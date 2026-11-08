//! Detached ordinary capabilities must keep their original generation even
//! when the native link name and protocol registry key are reused.

use amqp::{
    AnnotationKey, ApplicationProperties, Array, Body, ClientConnection, ClientDelivery,
    ClientReceiver, ClientSender, ClientSession, Message, OrderedMap, Outcome, Properties, Uuid,
    Value,
};
use domain::{
    CommandKind, CommandOutcome, EntityBinding, EntityPath, NamespaceName, QueueConfig,
    QueueCounters, StateMachine, codec, keys,
};
use protocol_amqp::{Broker as ProtocolBroker, BrokerRejection};
use server::{Broker, BrokerHandle, LocalProposer, ManualClock};
use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use storage::{StateStore, WriteBatch};
use testkit::StoreProvider;
use tokio::{net::TcpListener, sync::Notify, time::timeout};

#[path = "../src/test_deadline.rs"]
mod test_deadline;
use test_deadline::WAIT;

#[derive(Clone)]
struct PausedSettlement {
    handle: BrokerHandle,
    armed: Arc<AtomicBool>,
    reached: Arc<Notify>,
    release: Arc<Notify>,
    retained: Arc<Mutex<Option<EntityBinding>>>,
}

impl ProtocolBroker for PausedSettlement {
    async fn bind_entity(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<EntityBinding, BrokerRejection> {
        ProtocolBroker::bind_entity(&self.handle, namespace, entity).await
    }
    async fn submit_bound(
        &self,
        binding: EntityBinding,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        if !matches!(kind, CommandKind::Receive { .. }) && self.armed.swap(false, Ordering::SeqCst)
        {
            *self.retained.lock().unwrap() = Some(binding.clone());
            self.reached.notify_one();
            self.release.notified().await;
        }
        ProtocolBroker::submit_bound(&self.handle, binding, kind).await
    }
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        ProtocolBroker::submit(&self.handle, namespace, entity, kind).await
    }
    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        self.handle.wait_until_deliverable(namespace, entity)
    }
}

fn token(delivery: &ClientDelivery) -> Uuid {
    match delivery
        .message()
        .delivery_annotations
        .as_ref()
        .and_then(|fields| fields.get(&AnnotationKey::from("x-opt-lock-token")))
    {
        Some(Value::Uuid(token)) => token.clone(),
        other => panic!("real ordinary delivery lock token: {other:?}"),
    }
}

fn request(operation: &str, status: &str, token: Uuid, patch: bool) -> Message {
    let mut body = OrderedMap::from_iter([
        (
            Value::String("lock-tokens".into()),
            Value::Array(Array::from(vec![Value::Uuid(token)])),
        ),
        (
            Value::String("disposition-status".into()),
            Value::String(status.into()),
        ),
    ]);
    if patch {
        body.insert(
            Value::String("properties-to-modify".into()),
            Value::Map(OrderedMap::from_iter([(
                Value::String("attempt".into()),
                Value::Int(2),
            )])),
        );
    }
    Message::builder()
        .properties(Properties {
            message_id: Some("retained-capability".to_owned().into()),
            reply_to: Some("replies".into()),
            ..Default::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(protocol_amqp::OPERATION_PROPERTY, operation)
                .insert(
                    protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY,
                    "reused-receiver",
                )
                .build(),
        )
        .body(Body::Value(Value::Map(body)))
        .build()
}

async fn response(receiver: &mut ClientReceiver, status: i32) -> Result<(), Box<dyn Error>> {
    let delivery = timeout(WAIT, receiver.recv()).await??;
    let fields = delivery.message().application_properties.as_ref().unwrap();
    assert_eq!(
        fields.get(protocol_amqp::STATUS_CODE_PROPERTY),
        Some(&Value::Int(status)),
        "{fields:?}"
    );
    if status == 404 {
        assert_eq!(
            fields.get(protocol_amqp::ERROR_CONDITION_PROPERTY),
            Some(&Value::Symbol(protocol_amqp::NOT_FOUND.into()))
        );
    }
    receiver.accept(&delivery).await?;
    Ok(())
}

async fn exercise<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let raw = provider.open()?;
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(raw.clone()),
        ManualClock::at(1_000),
    ));
    let handle = broker.handle();
    let namespace = NamespaceName::new("tenant")?;
    // One queue per case keeps original lock and capability identities distinct.
    for (index, (operation, status, patch)) in [
        (protocol_amqp::RENEW_LOCK_OPERATION, "", false),
        (
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            "completed",
            false,
        ),
        (
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            "abandoned",
            false,
        ),
        (
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            "defered",
            false,
        ),
        (
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            "suspended",
            false,
        ),
        (
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            "abandoned",
            true,
        ),
        (protocol_amqp::UPDATE_DISPOSITION_OPERATION, "defered", true),
        (
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            "suspended",
            true,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let entity = EntityPath::new(format!("queue-{index}"))?;
        handle
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )
            .await?;
        let original = handle
            .bind_entity(namespace.clone(), entity.clone())
            .await?;
        let send = |id: &str| CommandKind::Send {
            message_id: id.into(),
            body: id.as_bytes().to_vec(),
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        };
        handle
            .submit(namespace.clone(), entity.clone(), send("original"))
            .await?;
        let paused = PausedSettlement {
            handle: handle.clone(),
            armed: Arc::default(),
            reached: Arc::default(),
            release: Arc::default(),
            retained: Arc::default(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let mut service = protocol_amqp::AmqpListener::new(paused.clone(), namespace.clone())
            .into_tcp_service(listener);
        let retirement = service.retirement_handle();
        let service_task = tokio::spawn(async move {
            service.serve().await;
            service.finish().await;
            service.take_exit()
        });
        let mut connection = ClientConnection::builder()
            .container_id("retained-receiver-authority")
            .open(format!("amqp://{address}").as_str())
            .await?;
        let mut session = ClientSession::begin(&mut connection).await?;
        let mut receiver =
            ClientReceiver::attach(&mut session, "reused-receiver", entity.as_str()).await?;
        let original_delivery = timeout(WAIT, receiver.recv()).await??;
        let original_token = token(&original_delivery);
        let management_address = format!("{entity}/$management");
        let mut responses = ClientReceiver::builder()
            .name("responses")
            .source(management_address.clone())
            .target("replies")
            .attach(&mut session)
            .await?;
        let requests = ClientSender::attach(&mut session, "requests", management_address).await?;
        paused.armed.store(true, Ordering::SeqCst);
        let message = request(operation, status, original_token.clone(), patch);
        let sending = tokio::spawn(async move {
            let mut requests = requests;
            let outcome = requests.send(message).await;
            (requests, outcome)
        });
        timeout(WAIT, paused.reached.notified()).await?;
        receiver.close().await?;
        // Controlled same-kind recreation plus token-key collision. There is no
        // live administrative deletion or recreation in this test or feature.
        let counter_key = keys::queue_counters(&namespace, &entity);
        let mut counters: QueueCounters = codec::decode(&raw.get(&counter_key)?.unwrap())?;
        counters.next_lock_token = 1;
        raw.apply(
            WriteBatch::default()
                .put(
                    keys::entity_metadata(&namespace, &entity),
                    codec::encode(&(2_u64, 0_u32, false))?,
                )
                .put(counter_key, codec::encode(&counters)?),
        )?;
        let mut replacement =
            ClientReceiver::attach(&mut session, "reused-receiver", entity.as_str()).await?;
        handle
            .submit(namespace.clone(), entity.clone(), send("replacement"))
            .await?;
        let replacement_delivery = timeout(WAIT, replacement.recv()).await??;
        assert_eq!(
            token(&replacement_delivery),
            original_token,
            "registry key really reused"
        );
        let before = raw.snapshot()?;
        paused.release.notify_one();
        let (mut requests, outcome) = timeout(WAIT, sending).await??;
        assert!(
            matches!(outcome?, Outcome::Accepted(_)),
            "transport acknowledgement is not settlement success"
        );
        response(&mut responses, 404).await?;
        assert_eq!(paused.retained.lock().unwrap().as_ref(), Some(&original));
        assert_eq!(
            raw.snapshot()?,
            before,
            "detached old capability did not mutate the replacement incarnation"
        );
        // The old result/cleanup must not remove the replacement registration.
        assert!(matches!(
            requests
                .send(request(
                    protocol_amqp::RENEW_LOCK_OPERATION,
                    "",
                    original_token.clone(),
                    false
                ))
                .await?,
            Outcome::Accepted(_)
        ));
        response(&mut responses, 200).await?;
        assert!(matches!(
            requests
                .send(request(
                    protocol_amqp::UPDATE_DISPOSITION_OPERATION,
                    "completed",
                    original_token,
                    false
                ))
                .await?,
            Outcome::Accepted(_)
        ));
        response(&mut responses, 200).await?;
        connection.close().await?;
        retirement.request();
        assert!(timeout(WAIT, service_task).await??.is_some());
    }
    drop(handle);
    drop(broker);
    let committed = raw.snapshot()?;
    drop(raw);
    assert_eq!(provider.open()?.snapshot()?, committed);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_detached_ordinary_capabilities_reject_drift_and_preserve_reused_registration()
-> Result<(), Box<dyn Error>> {
    exercise(testkit::MemoryProvider::new()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_detached_ordinary_capabilities_reject_drift_and_preserve_reused_registration()
-> Result<(), Box<dyn Error>> {
    exercise(testkit::DurableProvider::temporary()?).await
}
