//! Actual management wire requests retain ordinary delivery authority after
//! registry teardown/replacement. Session and request-response captures stay separate.

use super::*;
use domain::{EntityBinding, codec, keys};
use tokio::sync::Notify;

#[derive(Clone)]
struct PausedBound {
    actual: ActualBroker,
    reached: Arc<Notify>,
    release: Arc<Notify>,
    binding: Arc<StdMutex<Option<EntityBinding>>>,
}

impl Broker for PausedBound {
    async fn submit_bound(&self, binding: EntityBinding, kind: CommandKind) -> RawResult {
        *self.binding.lock().unwrap() = Some(binding.clone());
        self.reached.notify_one();
        self.release.notified().await;
        self.actual.submit_bound(binding, kind).await
    }
    async fn submit(&self, _: NamespaceName, _: EntityPath, _: CommandKind) -> RawResult {
        panic!("ordinary managed capabilities cannot submit by name")
    }
    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        std::future::pending().await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_management_wire_capabilities_survive_teardown_without_recapture() {
    for durable in [false, true] {
        // Complete, all delayed dispositions and patches, and ordinary renewal.
        for (operation, status, patch) in [
            (RENEW_LOCK_OPERATION, "", false),
            (UPDATE_DISPOSITION_OPERATION, "completed", false),
            (UPDATE_DISPOSITION_OPERATION, "abandoned", false),
            (UPDATE_DISPOSITION_OPERATION, "defered", false),
            (UPDATE_DISPOSITION_OPERATION, "suspended", false),
            (UPDATE_DISPOSITION_OPERATION, "abandoned", true),
            (UPDATE_DISPOSITION_OPERATION, "defered", true),
            (UPDATE_DISPOSITION_OPERATION, "suspended", true),
        ] {
            let mut actor = Actor::new(durable, false);
            let delivery = actor.receive();
            let token = delivery.lock.unwrap().token;
            let authority = actor.authority();
            let management = ConnectionManagement::new();
            let old = management
                .register_delivery(
                    LINK,
                    actor.entity.clone(),
                    delivery.sequence,
                    token,
                    authority.clone(),
                )
                .await;
            let broker = PausedBound {
                actual: actor.actual.as_ref().unwrap().clone(),
                reached: Arc::default(),
                release: Arc::default(),
                binding: Arc::default(),
            };
            let (_route, mut responses) = management
                .register_reply_route("authority-replies".to_owned())
                .await;
            let mut entries = vec![(LOCK_TOKENS, token_value(token))];
            if operation == UPDATE_DISPOSITION_OPERATION {
                entries.push(("disposition-status", Value::String(status.to_owned())));
                if patch {
                    entries.push((
                        "properties-to-modify",
                        Value::Map(amqp::OrderedMap::from_iter([(
                            Value::String("attempt".to_owned()),
                            Value::Int(2),
                        )])),
                    ));
                }
            }
            let mut message = request(operation, true, entries);
            message.properties = Some(Properties {
                message_id: Some(MessageId::Ulong(58)),
                reply_to: Some("authority-replies".into()),
                ..Default::default()
            });
            let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
            let LinkEndpoint::Receiver(receiver) = endpoint else {
                panic!("actual management receiver")
            };
            wire.request_message(&message).await;
            wire.barrier().await;
            let ns = actor.namespace.clone();
            let entity = actor.entity.clone();
            let serving_broker = broker.clone();
            let serving_management = management.clone();
            let serving = tokio::spawn(async move {
                serve_management_requests(
                    receiver,
                    ns,
                    entity,
                    serving_broker,
                    serving_management,
                    None,
                )
                .await
            });
            timeout(WAIT, broker.reached.notified()).await.unwrap();
            // The actual selected capability already owns its exact binding.
            // Receiver cleanup and reused registration must not replace it.
            management.unregister_delivery(&old).await;
            actor
                .store
                .as_ref()
                .unwrap()
                .apply(WriteBatch::default().put(
                    keys::entity_metadata(&actor.namespace, &actor.entity),
                    codec::encode(&(2_u64, 0_u32, false)).unwrap(),
                ))
                .unwrap();
            let fresh = crate::broker::ReceivingAuthority::Ordinary(
                actor
                    .actual
                    .as_ref()
                    .unwrap()
                    .handle
                    .bind_entity_blocking(actor.namespace.clone(), actor.entity.clone())
                    .unwrap(),
            );
            assert_ne!(authority, fresh);
            let replacement = management
                .register_delivery(LINK, actor.entity.clone(), delivery.sequence, token, fresh)
                .await;
            management.unregister_delivery(&old).await;
            let before = actor.snapshot();
            broker.release.notify_one();
            let response = timeout(WAIT, responses.recv()).await.unwrap().unwrap();
            assert_eq!(
                response.status_code, 404,
                "{operation} {status} patch={patch}"
            );
            assert_eq!(response.error_condition, Some(crate::NOT_FOUND));
            let crate::broker::ReceivingAuthority::Ordinary(original) = &authority else {
                unreachable!()
            };
            assert_eq!(broker.binding.lock().unwrap().as_ref(), Some(original));
            assert_eq!(
                management.deliveries.read().await.get(&replacement.key),
                Some(&replacement)
            );
            assert_eq!(actor.snapshot(), before);
            assert!(
                matches!(wire.control(1).await, amqp::Performative::Disposition(_)),
                "original request acknowledged after response routing"
            );
            wire.detach().await;
            timeout(WAIT, serving).await.unwrap().unwrap().unwrap();
            wire.stop().await;
            drop(broker);
            actor.reopen(&before);
        }
    }
}
