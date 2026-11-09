//! Retained request links, deferred locks and session capabilities over real
//! owner commands. Generation edits are owned fixtures, not a live Delete API.

use super::*;
use crate::listener::control_attachment_custody::{ControlContext, serve_control_attachment};
use domain::{EntityBinding, codec, keys};

#[derive(Clone)]
struct Witness {
    actual: ActualBroker,
    captures: Arc<AtomicUsize>,
    operations: Arc<StdMutex<Vec<(EntityBinding, CommandKind)>>>,
    paused: Arc<AtomicBool>,
    reached: Arc<Notify>,
    release: Arc<Notify>,
    after_deferred: Arc<AtomicBool>,
    after_complete: Arc<AtomicBool>,
}

impl Witness {
    fn new(actor: &Actor) -> Self {
        Self {
            actual: actor.actual.as_ref().unwrap().clone(),
            captures: Arc::default(),
            operations: Arc::default(),
            paused: Arc::default(),
            reached: Arc::default(),
            release: Arc::default(),
            after_deferred: Arc::default(),
            after_complete: Arc::default(),
        }
    }
}

impl Broker for Witness {
    async fn bind_entity(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<EntityBinding, BrokerRejection> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        self.actual.bind_entity(namespace, entity).await
    }
    async fn submit_bound(&self, binding: EntityBinding, kind: CommandKind) -> RawResult {
        self.operations
            .lock()
            .unwrap()
            .push((binding.clone(), kind.clone()));
        if self.paused.swap(false, Ordering::SeqCst) {
            self.reached.notify_one();
            self.release.notified().await;
        }
        let deferred = matches!(kind, CommandKind::ReceiveDeferred { .. });
        let complete = matches!(kind, CommandKind::Complete { .. });
        let result = self.actual.submit_bound(binding, kind).await;
        if (deferred && self.after_deferred.swap(false, Ordering::SeqCst))
            || (complete && self.after_complete.swap(false, Ordering::SeqCst))
        {
            self.reached.notify_one();
            self.release.notified().await;
        }
        result
    }
    async fn submit(&self, _: NamespaceName, _: EntityPath, _: CommandKind) -> RawResult {
        panic!("retained management must never invoke a name command")
    }
    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        std::future::pending().await
    }
}

fn counts(actor: &Actor) -> (usize, usize, usize) {
    let observations = &actor.store().observations;
    (
        observations.clock_calls.load(Ordering::SeqCst),
        observations.clock_reads.load(Ordering::SeqCst),
        observations.applies.load(Ordering::SeqCst),
    )
}

fn drift(actor: &Actor, target: &EntityPath) {
    let key = keys::entity_metadata(&actor.namespace, target);
    let (_, kind, tombstone): (u64, u32, bool) =
        codec::decode(&actor.store().get(&key).unwrap().unwrap()).unwrap();
    actor
        .store()
        .apply(WriteBatch::default().put(key, codec::encode(&(2_u64, kind, tombstone)).unwrap()))
        .unwrap();
}

fn capture(actor: &Actor, entity: &EntityPath) -> EntityBinding {
    actor
        .actual
        .as_ref()
        .unwrap()
        .handle
        .bind_entity_blocking(actor.namespace.clone(), entity.clone())
        .unwrap()
}

fn listen_authorization() -> Arc<crate::authorization::ConnectionAuthorization> {
    let rule = auth::SharedAccessRule::new(
        "listen",
        ResourceScope::namespace("tenant.servicebus.windows.net").unwrap(),
        auth::SharedAccessKey::new("secret").unwrap(),
        None,
        auth::PermissionSet::LISTEN,
    )
    .unwrap();
    let policy = auth::SharedAccessPolicy::new([rule]).unwrap();
    let grant = policy.authenticate_plain("listen", "secret").unwrap();
    crate::authorization::ConnectionAuthorization::new(
        crate::SharedAccessAuthentication::new(policy, "tenant.servicebus.windows.net").unwrap(),
        Some(grant),
    )
}

fn no_effects(actor: &Actor, before: &StoreSnapshot, observed: (usize, usize, usize)) {
    assert_eq!(
        counts(actor),
        observed,
        "no host clock, stored clock or apply on stale authority"
    );
    assert_eq!(
        &actor.snapshot(),
        before,
        "all records, indexes and checkpoint bytes remain exact"
    );
}

fn not_found(response: &ManagementResponse) {
    assert_eq!(response.status_code, 404, "{response:?}");
    assert_eq!(response.error_condition, Some(crate::NOT_FOUND));
}

fn value_map(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Map(
        entries
            .into_iter()
            .map(|(key, value)| (Value::String(key.into()), value))
            .collect(),
    )
}

fn peek_request(session: bool) -> Message {
    let mut entries = vec![
        ("from-sequence-number", Value::Long(0)),
        ("message-count", Value::Int(10)),
    ];
    if session {
        entries.push((SESSION_ID, Value::String(session_id().as_str().into())));
    }
    request(PEEK_MESSAGE_OPERATION, session, entries)
}

fn deferred_request(sequence: SequenceNumber, delete: bool, session: bool) -> Message {
    let mut entries = vec![
        (
            "sequence-numbers",
            Value::Array(Array::from(vec![Value::Long(sequence.as_u64() as i64)])),
        ),
        ("receiver-settle-mode", Value::Uint(u32::from(!delete))),
    ];
    if session {
        entries.push((SESSION_ID, Value::String(session_id().as_str().into())));
    }
    request(RECEIVE_BY_SEQUENCE_NUMBER_OPERATION, session, entries)
}

fn scheduled_request() -> Message {
    let mut message = Message {
        properties: Some(Properties {
            message_id: Some(MessageId::String("scheduled".into())),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut annotations = amqp::MessageAnnotations::default();
    annotations.insert(
        "x-opt-scheduled-enqueue-time",
        Value::Timestamp(AmqpTimestamp::from(50_000)),
    );
    message.message_annotations = Some(annotations);
    request(
        SCHEDULE_MESSAGE_OPERATION,
        false,
        [(
            "messages",
            Value::List(vec![value_map([
                ("message-id", Value::String("scheduled".into())),
                (
                    "message",
                    Value::Binary(amqp::encode_message(&message).unwrap().into()),
                ),
            ])]),
        )],
    )
}

fn requests() -> Vec<Message> {
    vec![
        peek_request(false),
        scheduled_request(),
        request(
            CANCEL_SCHEDULED_MESSAGE_OPERATION,
            false,
            [(
                "sequence-numbers",
                Value::Array(Array::from(vec![Value::Long(1)])),
            )],
        ),
        request(
            ADD_RULE_OPERATION,
            false,
            [
                ("rule-name", Value::String("fresh".into())),
                (
                    "rule-description",
                    value_map([(
                        "sql-filter",
                        value_map([("expression", Value::String("1=1".into()))]),
                    )]),
                ),
            ],
        ),
        request(
            REMOVE_RULE_OPERATION,
            false,
            [("rule-name", Value::String("$Default".into()))],
        ),
        request(
            ENUMERATE_RULES_OPERATION,
            false,
            [("skip", Value::Int(0)), ("top", Value::Int(10))],
        ),
    ]
}

fn prepare_subscription(actor: &Actor) -> EntityPath {
    let topic = EntityPath::new("events").unwrap();
    actor.intent_on(
        &topic,
        CommandKind::CreateTopic {
            config: domain::TopicConfig::default(),
        },
    );
    let name = domain::SubscriptionName::new("paid").unwrap();
    actor.intent_on(
        &topic,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: domain::SubscriptionConfig::default(),
        },
    );
    topic.subscription(&name).unwrap()
}

fn prepare_deferred(actor: &Actor, session: bool) -> (SequenceNumber, Option<SessionHold>) {
    let hold = session.then(|| actor.accept());
    let CommandOutcome::Sent { sequence } = actor.intent(CommandKind::Send {
        message_id: "deferred-authority".into(),
        body: b"original body".to_vec(),
        time_to_live_millis: None,
        session_id: session.then(session_id),
        scheduled_enqueue_at: None,
        envelope: None,
    }) else {
        panic!("real send")
    };
    let CommandOutcome::Received(Some(delivery)) = actor.intent(CommandKind::Receive {
        mode: ReceiveMode::PeekLock,
        lock_duration_millis: None,
        session: hold.clone(),
    }) else {
        panic!("real delivery")
    };
    actor.intent(CommandKind::Defer {
        sequence,
        lock_token: delivery.lock.unwrap().token,
        replacement_envelope: None,
    });
    (sequence, hold)
}

async fn process<B: Broker>(
    actor: &Actor,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
    message: &Message,
    limit: Option<u64>,
) -> ManagementResponse {
    process_bounded_request(
        message,
        MessageId::Ulong(60),
        &actor.namespace,
        entity,
        broker,
        management,
        None,
        ReplyRoute::Attached { limit },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn every_request_link_operation_keeps_original_capture_and_parsing_priority() {
    for durable in [false, true] {
        for (index, message) in requests().into_iter().enumerate() {
            let mut actor = Actor::new(durable, false);
            let target = if index >= 3 {
                prepare_subscription(&actor)
            } else {
                actor.entity.clone()
            };
            if index == 2 {
                actor.intent(CommandKind::Send {
                    message_id: "cancel-control".into(),
                    body: vec![],
                    time_to_live_millis: None,
                    session_id: None,
                    scheduled_enqueue_at: Some(domain::Timestamp::from_millis(50_000)),
                    envelope: None,
                });
            }
            let binding = capture(&actor, &target);
            let witness = Witness::new(&actor);
            let broker = RequestBroker::with_sender(witness.clone(), Arc::new(Ok(binding.clone())));
            let management = ConnectionManagement::new();
            drift(&actor, &target);
            let before = actor.snapshot();
            let observed = counts(&actor);
            let mut wake = Box::pin(
                actor
                    .actual
                    .as_ref()
                    .unwrap()
                    .handle
                    .wait_until_deliverable(&actor.namespace, &target),
            );
            pending_once(wake.as_mut()).await;
            not_found(&process(&actor, &target, &broker, &management, &message, None).await);
            let malformed = request(
                string_property(
                    message.application_properties.as_ref().unwrap(),
                    OPERATION_PROPERTY,
                )
                .unwrap(),
                false,
                [],
            );
            assert_eq!(
                process(&actor, &target, &broker, &management, &malformed, None)
                    .await
                    .status_code,
                400
            );
            assert_eq!(
                process(
                    &actor,
                    &target,
                    &broker,
                    &management,
                    &request("unknown-operation", false, []),
                    None
                )
                .await
                .status_code,
                400
            );
            no_effects(&actor, &before, observed);
            pending_once(wake.as_mut()).await;
            drop(wake);
            assert_eq!(
                witness.captures.load(Ordering::SeqCst),
                0,
                "no request-time owner lookup"
            );
            assert!(
                witness
                    .operations
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|(actual, _)| actual == &binding)
            );
            let fresh =
                RequestBroker::with_sender(witness.clone(), Arc::new(Ok(capture(&actor, &target))));
            let healthy = process(&actor, &target, &fresh, &management, &message, None).await;
            assert!(
                matches!(healthy.status_code, 200 | 204),
                "healthy operation {index}: {healthy:?}"
            );
            let committed = actor.snapshot();
            drop(fresh);
            drop(broker);
            drop(witness);
            actor.reopen(&committed);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn deferred_capabilities_without_associated_link_keep_receive_time_binding_for_all_operations()
 {
    for durable in [false, true] {
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
            let (sequence, _) = prepare_deferred(&actor, false);
            let witness = Witness::new(&actor);
            let binding = actor.authority().binding().clone();
            let broker = RequestBroker::with_sender(witness.clone(), Arc::new(Ok(binding.clone())));
            let management = ConnectionManagement::new();
            let response = process(
                &actor,
                &actor.entity,
                &broker,
                &management,
                &deferred_request(sequence, false, false),
                None,
            )
            .await;
            assert_eq!(response.status_code, 200);
            let row = management
                .request_response_deliveries
                .read()
                .await
                .values()
                .next()
                .unwrap()
                .clone();
            assert_eq!(
                row.managed.authority,
                DeliveryAuthority::RequestResponse(binding.clone())
            );
            let token = row.registration.key.lock_token;
            let mut entries = vec![(LOCK_TOKENS, token_value(token))];
            if operation == UPDATE_DISPOSITION_OPERATION {
                entries.push(("disposition-status", Value::String(status.into())));
                if patch {
                    entries.push((
                        "properties-to-modify",
                        value_map([("attempt", Value::Int(2))]),
                    ));
                }
            }
            let message = request(operation, false, entries);
            drift(&actor, &actor.entity);
            // A fresh management request link cannot upgrade the old lock.
            let fresh = RequestBroker::with_sender(
                witness.clone(),
                Arc::new(Ok(capture(&actor, &actor.entity))),
            );
            let before = actor.snapshot();
            let observed = counts(&actor);
            not_found(&process(&actor, &actor.entity, &fresh, &management, &message, None).await);
            no_effects(&actor, &before, observed);
            assert_eq!(
                management
                    .request_response_deliveries
                    .read()
                    .await
                    .get(&row.registration.key),
                Some(&row)
            );
            assert!(
                witness
                    .operations
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|(actual, _)| actual == &binding)
            );
            drop(fresh);
            drop(broker);
            drop(witness);
            actor.reopen(&before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn selected_session_state_renewal_and_optional_receive_never_use_request_link_or_replacement()
{
    for durable in [false, true] {
        for operation in [
            GET_SESSION_STATE_OPERATION,
            SET_SESSION_STATE_OPERATION,
            RENEW_SESSION_LOCK_OPERATION,
            PEEK_MESSAGE_OPERATION,
            RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
        ] {
            let mut actor = Actor::new(durable, true);
            let (sequence, hold) = prepare_deferred(&actor, true);
            let hold = hold.unwrap();
            let management = ConnectionManagement::new();
            let claim = management.claim_session(LINK, actor.entity.clone());
            let binding = actor.authority().binding().clone();
            let old = management
                .install_bound_session(&claim, hold.clone(), binding.clone(), || true)
                .await
                .unwrap();
            let witness = Witness::new(&actor);
            witness.paused.store(true, Ordering::SeqCst);
            let broker = RequestBroker::with_sender(
                witness.clone(),
                Arc::new(Err(BrokerRejection::Unavailable(
                    "unrelated request capture refusal".into(),
                ))),
            );
            let message = match operation {
                PEEK_MESSAGE_OPERATION => peek_request(true),
                RECEIVE_BY_SEQUENCE_NUMBER_OPERATION => deferred_request(sequence, false, true),
                SET_SESSION_STATE_OPERATION => state_request(),
                _ => request(
                    operation,
                    true,
                    [(SESSION_ID, Value::String(session_id().as_str().into()))],
                ),
            };
            let mut original = Box::pin(process(
                &actor,
                &actor.entity,
                &broker,
                &management,
                &message,
                None,
            ));
            tokio::select! { _ = original.as_mut() => panic!("selected operation held"), () = witness.reached.notified() => {} }
            drift(&actor, &actor.entity);
            let fresh = capture(&actor, &actor.entity);
            let claim = management.claim_session(LINK, actor.entity.clone());
            let replacement = management
                .install_bound_session(&claim, hold, fresh, || true)
                .await
                .unwrap();
            management.unregister_session(&old).await;
            let before = actor.snapshot();
            let observed = counts(&actor);
            witness.release.notify_one();
            not_found(&original.as_mut().await);
            drop(original);
            no_effects(&actor, &before, observed);
            assert_eq!(management.session(LINK).await, Some(replacement));
            assert_eq!(witness.operations.lock().unwrap()[0].0, binding);
            assert_eq!(witness.captures.load(Ordering::SeqCst), 0);
            drop(broker);
            drop(witness);
            actor.reopen(&before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn staged_deferred_delete_keeps_one_binding_through_hidden_receive_and_stale_complete() {
    for durable in [false, true] {
        for session in [false, true] {
            let mut actor = Actor::new(durable, session);
            let (sequence, hold) = prepare_deferred(&actor, session);
            let management = ConnectionManagement::new();
            if let Some(hold) = hold {
                install_hold(&management, &actor, hold).await;
            }
            let witness = Witness::new(&actor);
            witness.after_deferred.store(true, Ordering::SeqCst);
            let binding = actor.authority().binding().clone();
            let broker = RequestBroker::with_sender(witness.clone(), Arc::new(Ok(binding.clone())));
            let message = deferred_request(sequence, true, session);
            let mut original = Box::pin(process(
                &actor,
                &actor.entity,
                &broker,
                &management,
                &message,
                Some(128 * 1024),
            ));
            tokio::select! { _ = original.as_mut() => panic!("original receive result hidden"), () = witness.reached.notified() => {} }
            drift(&actor, &actor.entity);
            let before = actor.snapshot();
            let observed = counts(&actor);
            witness.release.notify_one();
            not_found(&original.as_mut().await);
            drop(original);
            no_effects(&actor, &before, observed);
            let operations = witness.operations.lock().unwrap().clone();
            assert!(matches!(
                &operations[..],
                [
                    (_, CommandKind::ReceiveDeferred { .. }),
                    (_, CommandKind::Complete { .. })
                ]
            ));
            assert!(operations.iter().all(|(actual, _)| actual == &binding));
            assert!(
                management
                    .request_response_deliveries
                    .read()
                    .await
                    .is_empty()
            );
            assert!(matches!(
                actor.message(sequence).unwrap().state,
                MessageState::Locked { .. }
            ));
            drop(broker);
            drop(witness);
            actor.reopen(&before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn deferred_reply_limits_and_partial_delete_keep_original_authority_without_hidden_deletion()
{
    for durable in [false, true] {
        for delete in [false, true] {
            let mut actor = Actor::new(durable, false);
            let (sequence, _) = prepare_deferred(&actor, false);
            let management = ConnectionManagement::new();
            let witness = Witness::new(&actor);
            let binding = actor.authority().binding().clone();
            let broker = RequestBroker::with_sender(witness.clone(), Arc::new(Ok(binding.clone())));
            let response = process(
                &actor,
                &actor.entity,
                &broker,
                &management,
                &deferred_request(sequence, delete, false),
                Some(1),
            )
            .await;
            assert_eq!(response.error_condition, Some(crate::MESSAGE_SIZE_EXCEEDED));
            assert!(matches!(
                &witness.operations.lock().unwrap()[..],
                [(_, CommandKind::ReceiveDeferred { .. })]
            ));
            assert!(matches!(
                actor.message(sequence).unwrap().state,
                MessageState::Locked { .. }
            ));
            assert!(
                management
                    .request_response_deliveries
                    .read()
                    .await
                    .is_empty()
            );
            assert_eq!(witness.operations.lock().unwrap()[0].0, binding);
            let committed = actor.snapshot();
            drop(broker);
            drop(witness);
            actor.reopen(&committed);
        }

        let mut actor = Actor::new(durable, false);
        let (first, _) = prepare_deferred(&actor, false);
        let (second, _) = prepare_deferred(&actor, false);
        let management = ConnectionManagement::new();
        let witness = Witness::new(&actor);
        witness.after_complete.store(true, Ordering::SeqCst);
        let binding = actor.authority().binding().clone();
        let broker = RequestBroker::with_sender(witness.clone(), Arc::new(Ok(binding.clone())));
        let message = request(
            RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            false,
            [
                (
                    "sequence-numbers",
                    Value::Array(Array::from(vec![
                        Value::Long(first.as_u64() as i64),
                        Value::Long(second.as_u64() as i64),
                    ])),
                ),
                ("receiver-settle-mode", Value::Uint(0)),
            ],
        );
        let mut original = Box::pin(process(
            &actor,
            &actor.entity,
            &broker,
            &management,
            &message,
            Some(128 * 1024),
        ));
        tokio::select! { _ = original.as_mut() => panic!("first committed deletion result is held"), () = witness.reached.notified() => {} }
        assert!(actor.message(first).is_none());
        drift(&actor, &actor.entity);
        let before = actor.snapshot();
        let observed = counts(&actor);
        witness.release.notify_one();
        let response = original.as_mut().await;
        drop(original);
        assert_eq!(response.status_code, 200);
        let Value::Map(body) = response.body else {
            panic!("partial response map")
        };
        let Some(Value::List(entries)) = body.get(&Value::String("messages".into())) else {
            panic!("partial response entries")
        };
        assert_eq!(
            entries.len(),
            1,
            "only the committed first deletion is visible"
        );
        no_effects(&actor, &before, observed);
        assert!(matches!(
            actor.message(second).unwrap().state,
            MessageState::Locked { .. }
        ));
        let operations = witness.operations.lock().unwrap().clone();
        assert!(matches!(
            &operations[..],
            [
                (_, CommandKind::ReceiveDeferred { .. }),
                (_, CommandKind::Complete { .. }),
                (_, CommandKind::Complete { .. })
            ]
        ));
        assert!(operations.iter().all(|(actual, _)| actual == &binding));
        assert!(
            management
                .request_response_deliveries
                .read()
                .await
                .is_empty()
        );
        drop(broker);
        drop(witness);
        actor.reopen(&before);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn generation_aliases_do_not_refresh_or_remove_a_new_deferred_registration() {
    for durable in [false, true] {
        for renew in [false, true] {
            let mut actor = Actor::new(durable, false);
            let delivery = actor.receive();
            let token = delivery.lock.unwrap().token;
            let binding = actor.authority().binding().clone();
            let management = ConnectionManagement::new();
            let ordinary = management
                .register_delivery(
                    LINK,
                    actor.entity.clone(),
                    delivery.sequence,
                    token,
                    actor.authority(),
                )
                .await;
            drift(&actor, &actor.entity);
            let replacement_binding = capture(&actor, &actor.entity);
            let replacement = management
                .register_request_response_delivery(
                    actor.entity.clone(),
                    delivery.clone(),
                    replacement_binding,
                )
                .await;
            let row = management
                .request_response_deliveries
                .read()
                .await
                .get(&replacement.key)
                .unwrap()
                .clone();
            let selected = management
                .managed_delivery(&actor.entity, Some(LINK), token)
                .await
                .unwrap();
            assert!(
                selected.request_response.is_none(),
                "different incarnation is not an alias"
            );
            assert_eq!(
                selected.managed.authority,
                DeliveryAuthority::Receiving(actor.authority())
            );
            let witness = Witness::new(&actor);
            let broker = RequestBroker::with_sender(
                witness.clone(),
                Arc::new(Ok(capture(&actor, &actor.entity))),
            );
            let mut entries = vec![(LOCK_TOKENS, token_value(token))];
            if !renew {
                entries.push(("disposition-status", Value::String("completed".into())));
            }
            let message = request(
                if renew {
                    RENEW_LOCK_OPERATION
                } else {
                    UPDATE_DISPOSITION_OPERATION
                },
                true,
                entries,
            );
            let before = actor.snapshot();
            let observed = counts(&actor);
            not_found(&process(&actor, &actor.entity, &broker, &management, &message, None).await);
            no_effects(&actor, &before, observed);
            assert_eq!(
                management
                    .request_response_deliveries
                    .read()
                    .await
                    .get(&replacement.key),
                Some(&row)
            );
            assert_eq!(
                management.deliveries.read().await.get(&ordinary.key),
                Some(&ordinary)
            );
            assert_eq!(witness.operations.lock().unwrap()[0].0, binding);
            drop(broker);
            drop(witness);
            actor.reopen(&before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn selected_deferred_result_keeps_conditional_cleanup_across_generation_and_registry_write_reuse()
 {
    for durable in [false, true] {
        for renew in [false, true] {
            let mut actor = Actor::new(durable, false);
            let delivery = actor.receive();
            let token = delivery.lock.unwrap().token;
            let management = ConnectionManagement::new();
            let original_binding = actor.authority().binding().clone();
            let old = management
                .register_request_response_delivery(
                    actor.entity.clone(),
                    delivery.clone(),
                    original_binding,
                )
                .await;
            let broker = actor.request_broker();
            actor.result_gate.arm();
            let mut entries = vec![(LOCK_TOKENS, token_value(token))];
            if !renew {
                entries.push(("disposition-status", Value::String("completed".into())));
            }
            let message = request(
                if renew {
                    RENEW_LOCK_OPERATION
                } else {
                    UPDATE_DISPOSITION_OPERATION
                },
                false,
                entries,
            );
            let mut original = PendingOperation::new(
                process(&actor, &actor.entity, &broker, &management, &message, None),
                broker.control(),
            );
            let raw = returned_while_pending(&mut original, &actor).await;
            assert!(matches!(
                raw,
                Ok(CommandOutcome::LockRenewed { .. } | CommandOutcome::Completed)
            ));
            drift(&actor, &actor.entity);
            let replacement = management
                .register_request_response_delivery(
                    actor.entity.clone(),
                    delivery.clone(),
                    capture(&actor, &actor.entity),
                )
                .await;
            assert_ne!(old, replacement);
            let held = management.request_response_deliveries.write().await;
            let expected = held.get(&replacement.key).unwrap().clone();
            actor.result_gate.release();
            // The exact owner result is already cached at its named result
            // frontier; its conditional refresh/unregister now waits on this
            // actual registry write lock. Retiring the borrower is not a retry.
            assert_eq!(returned_while_pending(&mut original, &actor).await, raw);
            original.retire();
            let before = actor.snapshot();
            let observed = counts(&actor);
            drop(held);
            let response = take_response(&mut original).await;
            assert_eq!(response.status_code, 200);
            no_effects(&actor, &before, observed);
            assert_eq!(
                management
                    .request_response_deliveries
                    .read()
                    .await
                    .get(&replacement.key),
                Some(&expected)
            );
            assert_eq!(
                actor.submissions.lock().unwrap().len(),
                1,
                "the selected original is never resubmitted"
            );
            drop(original);
            drop(broker);
            actor.reopen(&before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_control_attach_captures_once_before_accept_and_reused_link_does_not_upgrade_it() {
    for durable in [false, true] {
        for missing in [false, true] {
            let mut actor = Actor::new(durable, false);
            let target = if missing {
                EntityPath::new("not-created").unwrap()
            } else {
                actor.entity.clone()
            };
            let authorization = listen_authorization();
            let management = ConnectionManagement::new();
            let witness = Witness::new(&actor);
            let (mut wire, endpoint) =
                Wire::new(Role::Receiver, 32, ReceiverSettleMode::First).await;
            let LinkEndpoint::Sender(sender) = endpoint else {
                panic!("real reply sender")
            };
            let (route, responses) = management
                .register_reply_route("management-replies".into())
                .await;
            let reply_management = management.clone();
            let replies = tokio::spawn(async move {
                serve_management_replies(
                    sender,
                    "management-replies".into(),
                    route,
                    responses,
                    reply_management,
                    None,
                )
                .await
            });
            let (session, attach) = wire.offer_management_request(&target, 2).await;
            let admitted = serve_control_attachment(
                ControlContext {
                    session: &session,
                    broker: &witness,
                    namespace: &actor.namespace,
                    authorization: Some(&authorization),
                    management: &management,
                    retirement: None,
                },
                attach,
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(witness.captures.load(Ordering::SeqCst), 1);
            assert!(matches!(
                wire.control(1).await,
                amqp::Performative::Attach(_)
            ));
            assert!(matches!(wire.control(1).await, amqp::Performative::Flow(_)));
            for (id, message, expected) in [
                (1, peek_request(false), if missing { 404 } else { 204 }),
                (2, request("unknown", false, []), 400),
                (3, request(SCHEDULE_MESSAGE_OPERATION, false, []), 401),
            ] {
                let mut message = message;
                message.properties = Some(Properties {
                    message_id: Some(MessageId::Ulong(id)),
                    reply_to: Some("management-replies".into()),
                    ..Default::default()
                });
                wire.request_message_on(2, id as u32, &message).await;
                let response = wire.response_message().await;
                assert_eq!(
                    response
                        .application_properties
                        .unwrap()
                        .get(STATUS_CODE_PROPERTY),
                    Some(&Value::Int(expected))
                );
            }
            if missing {
                actor.intent_on(
                    &target,
                    CommandKind::CreateQueue {
                        config: QueueConfig::default(),
                    },
                );
            } else {
                drift(&actor, &target);
            }
            let before = actor.snapshot();
            let observed = counts(&actor);
            let mut message = peek_request(false);
            message.properties = Some(Properties {
                message_id: Some(MessageId::Ulong(4)),
                reply_to: Some("management-replies".into()),
                ..Default::default()
            });
            wire.request_message_on(2, 4, &message).await;
            let response = wire.response_message().await;
            assert_eq!(
                response
                    .application_properties
                    .unwrap()
                    .get(STATUS_CODE_PROPERTY),
                Some(&Value::Int(404))
            );
            assert_eq!(witness.captures.load(Ordering::SeqCst), 1);
            no_effects(&actor, &before, observed);
            // Close the declared receiver and reuse its exact name/handle. The old
            // capture is never upgraded; only this new attachment captures again.
            wire.detach_on(2).await;
            timeout(WAIT, admitted.task).await.unwrap().unwrap();
            timeout(WAIT, admitted.result)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            wire.restore_session(session);
            let (session, attach) = wire.offer_management_request(&target, 2).await;
            let admitted = serve_control_attachment(
                ControlContext {
                    session: &session,
                    broker: &witness,
                    namespace: &actor.namespace,
                    authorization: Some(&authorization),
                    management: &management,
                    retirement: None,
                },
                attach,
            )
            .await
            .unwrap()
            .unwrap();
            assert!(matches!(
                wire.control(1).await,
                amqp::Performative::Attach(_)
            ));
            assert!(matches!(wire.control(1).await, amqp::Performative::Flow(_)));
            assert_eq!(witness.captures.load(Ordering::SeqCst), 2);
            message.properties.as_mut().unwrap().message_id = Some(MessageId::Ulong(5));
            wire.request_message_on(2, 5, &message).await;
            assert_eq!(
                wire.response_message()
                    .await
                    .application_properties
                    .unwrap()
                    .get(STATUS_CODE_PROPERTY),
                Some(&Value::Int(204))
            );
            // The native FIFO barrier processes the reply's acceptance before its
            // explicit Detach. Join that original sender before stopping the driver.
            wire.barrier().await;
            wire.detach().await;
            timeout(WAIT, replies).await.unwrap().unwrap().unwrap();
            wire.stop().await;
            timeout(WAIT, admitted.task).await.unwrap().unwrap();
            timeout(WAIT, admitted.result)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            drop(session);
            drop(witness);
            let committed = actor.snapshot();
            actor.reopen(&committed);
        }
    }
}
