use std::sync::{
    Mutex as StdMutex,
    atomic::{AtomicU8, AtomicU64, Ordering},
};

use domain::{BrokerError, Command, QueueConfig, ReceiveMode, StateMachine, Timestamp};
use storage::{MemoryStore, StateStore, StoreSnapshot};

use super::*;

const LINK: &str = "reused-renewal-receiver";

#[derive(Clone)]
struct Owner {
    machine: Arc<StdMutex<StateMachine<MemoryStore>>>,
    now: Arc<AtomicU64>,
    submissions: Arc<StdMutex<Vec<CommandKind>>>,
    interleave: Arc<AtomicU8>,
    interleaved_snapshot: Arc<StdMutex<Option<StoreSnapshot>>>,
    management: Arc<ConnectionManagement>,
    binding: EntityBinding,
}

impl Owner {
    fn apply(&self, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.machine
            .lock()
            .expect("owner lock")
            .apply(&Command::new(
                self.binding.namespace().clone(),
                self.binding.target().clone(),
                Timestamp::from_millis(self.now.load(Ordering::Acquire)),
                kind,
            ))
    }

    fn snapshot(&self) -> StoreSnapshot {
        self.machine
            .lock()
            .expect("owner lock")
            .store()
            .snapshot()
            .expect("snapshot")
    }

    async fn takeover(&self, original: &SessionHold) -> SessionHold {
        self.apply(CommandKind::ReleaseSession {
            session: original.clone(),
        })
        .expect("release original");
        let CommandOutcome::SessionAccepted(Some(current)) = self
            .apply(CommandKind::AcceptSession {
                session_id: Some(original.session_id.clone()),
                lock_duration_millis: Some(1_000),
            })
            .expect("accept replacement")
        else {
            panic!("replacement session")
        };
        let hold = current.hold();
        assert_ne!(hold.token, original.token);
        self.management
            .register_session(
                LINK,
                self.binding.target().clone(),
                hold.clone(),
                self.binding.clone(),
            )
            .await;
        *self
            .interleaved_snapshot
            .lock()
            .expect("interleave snapshot") = Some(self.snapshot());
        hold
    }
}

impl Broker for Owner {
    crate::broker::fixture_binding_methods!();

    async fn rules(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        panic!("renewal must not read rules")
    }

    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: crate::Attachment,
    ) -> Result<Option<crate::EntityMetadata>, BrokerRejection> {
        panic!("renewal must not rebind")
    }

    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        panic!("management must not wait for delivery")
    }

    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        assert_eq!(&namespace, self.binding.namespace());
        assert_eq!(&entity, self.binding.target());
        self.submissions
            .lock()
            .expect("submissions")
            .push(kind.clone());
        if let CommandKind::RenewLockHeld {
            session: Some(original),
            ..
        } = &kind
            && self
                .interleave
                .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.takeover(original).await;
        }
        let result = self.apply(kind.clone()).map_err(BrokerRejection::Refused)?;
        if let CommandKind::ReceiveDeferredHeld {
            session: Some(original),
            ..
        } = &kind
            && self
                .interleave
                .compare_exchange(2, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.takeover(original).await;
        }
        Ok(result)
    }
}

fn fixture(
    sessions: bool,
    deferred: bool,
    duration: u64,
) -> (Owner, Option<SessionHold>, domain::Delivery) {
    let namespace = NamespaceName::new("tenant").expect("namespace");
    let entity = EntityPath::new("orders").expect("entity");
    let config = QueueConfig {
        requires_session: sessions,
        lock_duration_millis: 30_000,
        ..QueueConfig::default()
    };
    let binding = crate::broker::test_admission(
        namespace,
        crate::Attachment::Queue(entity),
        crate::EntityMetadata::Queue(config),
    )
    .binding;
    let owner = Owner {
        machine: Arc::new(StdMutex::new(StateMachine::new(MemoryStore::default()))),
        now: Arc::new(AtomicU64::new(100)),
        submissions: Arc::default(),
        interleave: Arc::new(AtomicU8::new(0)),
        interleaved_snapshot: Arc::default(),
        management: ConnectionManagement::new(),
        binding,
    };
    owner
        .apply(CommandKind::CreateQueue { config })
        .expect("create");
    let id = SessionId::new("original").expect("session");
    owner
        .apply(CommandKind::Send {
            message_id: "message".to_owned(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: Some(id.clone()),
        })
        .expect("send");
    owner.now.store(101, Ordering::Release);
    let hold = if sessions {
        let CommandOutcome::SessionAccepted(Some(accepted)) = owner
            .apply(CommandKind::AcceptSession {
                session_id: Some(id),
                lock_duration_millis: Some(duration),
            })
            .expect("accept")
        else {
            panic!("accepted session")
        };
        Some(accepted.hold())
    } else {
        None
    };
    owner.now.store(102, Ordering::Release);
    let CommandOutcome::Received(Some(delivery)) = owner
        .apply(CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(10_000),
            session: hold.clone(),
        })
        .expect("receive")
    else {
        panic!("received message")
    };
    if deferred {
        owner
            .apply(CommandKind::Defer {
                sequence: delivery.sequence,
                lock_token: delivery.lock.expect("message lock").token,
            })
            .expect("defer");
    }
    (owner, hold, delivery)
}

fn request(operation: &str, fields: impl IntoIterator<Item = (&'static str, Value)>) -> Message {
    let mut properties = ApplicationProperties::default();
    properties.insert(OPERATION_PROPERTY, operation.to_owned());
    properties.insert(ASSOCIATED_LINK_NAME_PROPERTY, LINK);
    Message {
        application_properties: Some(properties),
        body: Body::Value(Value::Map(
            fields
                .into_iter()
                .map(|(name, value)| (Value::String(name.to_owned()), value))
                .collect(),
        )),
        ..Message::default()
    }
}

fn renewal(token: LockToken) -> Message {
    request(
        RENEW_LOCK_OPERATION,
        [(
            LOCK_TOKENS,
            Value::Array(Array::from(vec![Value::Uuid(lock_token_uuid(token))])),
        )],
    )
}

async fn process(owner: &Owner, message: &Message, bytes: u64) -> ManagementResponse {
    let broker = BoundBroker::new(owner.clone(), owner.binding.clone());
    process_request(
        message,
        MessageId::Ulong(7),
        owner.binding.namespace(),
        owner.binding.target(),
        &broker,
        &owner.management,
        None,
        DeliveryBudget {
            max_bytes: bytes,
            per_message_overhead_bytes: 64,
        },
    )
    .await
}

#[tokio::test]
async fn renewal_uses_delivery_original_hold_after_same_name_session_replacement() {
    let (owner, original, delivery) = fixture(true, false, 10);
    let original = original.expect("original hold");
    let token = delivery.lock.expect("message lock").token;
    let registration = owner
        .management
        .register_delivery_owned_with_session(
            LINK,
            owner.binding.target().clone(),
            delivery.sequence,
            token,
            owner.binding.clone(),
            Some(original.clone()),
        )
        .expect("owned receipt");
    owner.now.store(111, Ordering::Release);
    let CommandOutcome::SessionAccepted(Some(current)) = owner
        .apply(CommandKind::AcceptSession {
            session_id: Some(original.session_id.clone()),
            lock_duration_millis: Some(1_000),
        })
        .expect("takeover")
    else {
        panic!("replacement")
    };
    owner
        .management
        .register_session(
            LINK,
            owner.binding.target().clone(),
            current.hold(),
            owner.binding.clone(),
        )
        .await;
    let before = owner.snapshot();
    let response = process(&owner, &renewal(token), 4_096).await;
    assert_eq!(response.status_code, 410);
    assert_eq!(response.error_condition, Some(crate::SESSION_LOCK_LOST));
    assert_eq!(owner.snapshot(), before);
    assert_eq!(
        owner
            .management
            .delivery(LINK, token)
            .await
            .expect("retained receipt")
            .session,
        Some(original.clone())
    );
    assert!(
        matches!(owner.submissions.lock().expect("submissions").last(), Some(CommandKind::RenewLockHeld { session: Some(hold), .. }) if hold == &original)
    );
    assert_eq!(
        owner
            .management
            .session(LINK)
            .await
            .expect("replacement association")
            .hold,
        current.hold()
    );
    drop(registration);
}

#[tokio::test]
async fn renewal_retains_original_receipt_when_takeover_occurs_after_lookup() {
    let (owner, original, delivery) = fixture(true, false, 1_000);
    let original = original.expect("original hold");
    let token = delivery.lock.expect("message lock").token;
    let registration = owner
        .management
        .register_delivery_owned_with_session(
            LINK,
            owner.binding.target().clone(),
            delivery.sequence,
            token,
            owner.binding.clone(),
            Some(original.clone()),
        )
        .expect("owned receipt");
    owner.now.store(103, Ordering::Release);
    owner.interleave.store(1, Ordering::Release);
    let response = process(&owner, &renewal(token), 4_096).await;
    assert_eq!(response.status_code, 410);
    assert_eq!(response.error_condition, Some(crate::SESSION_LOCK_LOST));
    assert_eq!(
        owner.snapshot(),
        owner
            .interleaved_snapshot
            .lock()
            .expect("interleave snapshot")
            .clone()
            .expect("baseline after takeover")
    );
    assert!(
        matches!(owner.submissions.lock().expect("submissions").last(), Some(CommandKind::RenewLockHeld { session: Some(hold), .. }) if hold == &original)
    );
    assert_eq!(
        owner
            .management
            .delivery(LINK, token)
            .await
            .expect("retained receipt")
            .session,
        Some(original)
    );
    drop(registration);
}

#[tokio::test]
async fn deferred_renewal_uses_hold_captured_before_owner_registry_interleave() {
    let (owner, original, delivery) = fixture(true, true, 1_000);
    let original = original.expect("original hold");
    owner
        .management
        .register_session(
            LINK,
            owner.binding.target().clone(),
            original.clone(),
            owner.binding.clone(),
        )
        .await;
    owner.now.store(103, Ordering::Release);
    owner.interleave.store(2, Ordering::Release);
    let response = process(
        &owner,
        &request(
            RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            [
                (
                    SESSION_ID,
                    Value::String(original.session_id.as_str().to_owned()),
                ),
                (
                    SEQUENCE_NUMBERS,
                    Value::Array(Array::from(vec![Value::Ulong(delivery.sequence.as_u64())])),
                ),
                (RECEIVER_SETTLE_MODE, Value::Uint(1)),
            ],
        ),
        4_096,
    )
    .await;
    assert_eq!(response.status_code, 200);
    let record = owner
        .machine
        .lock()
        .expect("owner lock")
        .message(
            owner.binding.namespace(),
            owner.binding.target(),
            delivery.sequence,
        )
        .expect("message read")
        .expect("message");
    let domain::MessageState::Locked { token, .. } = record.state else {
        panic!("deferred lock")
    };
    assert_eq!(
        owner
            .management
            .delivery(LINK, token)
            .await
            .expect("deferred receipt")
            .session,
        Some(original.clone())
    );
    assert_ne!(
        owner
            .management
            .session(LINK)
            .await
            .expect("replacement association")
            .hold
            .token,
        original.token
    );
    let before = owner.snapshot();
    let response = process(&owner, &renewal(token), 4_096).await;
    assert_eq!(response.status_code, 410);
    assert_eq!(response.error_condition, Some(crate::SESSION_LOCK_LOST));
    assert_eq!(owner.snapshot(), before);
    assert!(
        matches!(owner.submissions.lock().expect("submissions").last(), Some(CommandKind::RenewLockHeld { session: Some(hold), .. }) if hold == &original)
    );
    assert_eq!(
        owner
            .management
            .delivery(LINK, token)
            .await
            .expect("retained receipt")
            .session,
        Some(original)
    );
}

#[tokio::test]
async fn renewal_validation_and_binding_priority_precede_owner_submission() {
    let (owner, _, delivery) = fixture(false, false, 0);
    let token = delivery.lock.expect("message lock").token;
    for request in [
        request(RENEW_LOCK_OPERATION, []),
        request(
            RENEW_LOCK_OPERATION,
            [(LOCK_TOKENS, Value::Array(Array::from(Vec::<Value>::new())))],
        ),
        request(
            RENEW_LOCK_OPERATION,
            [(
                LOCK_TOKENS,
                Value::Array(Array::from(vec![
                    Value::Uuid(lock_token_uuid(token)),
                    Value::Uuid(lock_token_uuid(token)),
                ])),
            )],
        ),
    ] {
        assert_eq!(process(&owner, &request, 4_096).await.status_code, 400);
    }
    assert_eq!(process(&owner, &renewal(token), 8).await.status_code, 403);
    assert!(owner.submissions.lock().expect("submissions").is_empty());
    owner
        .management
        .register_delivery_with_session(
            LINK,
            EntityPath::new("another").expect("entity"),
            delivery.sequence,
            token,
            owner.binding.clone(),
            None,
        )
        .await;
    let before = owner.snapshot();
    let response = process(&owner, &renewal(token), 4_096).await;
    assert_eq!(response.status_code, 410);
    assert_eq!(response.error_condition, Some(crate::MESSAGE_LOCK_LOST));
    assert_eq!(owner.snapshot(), before);
    assert!(owner.submissions.lock().expect("submissions").is_empty());
    owner
        .management
        .register_delivery_with_session(
            LINK,
            owner.binding.target().clone(),
            delivery.sequence,
            token,
            owner.binding.clone(),
            None,
        )
        .await;
    owner.now.store(103, Ordering::Release);
    let response = process(&owner, &renewal(token), 4_096).await;
    assert_eq!(response.status_code, 200);
    assert!(matches!(
        owner.submissions.lock().expect("submissions").last(),
        Some(CommandKind::RenewLockHeld { session: None, .. })
    ));
    assert!(owner.management.delivery(LINK, token).await.is_some());
    assert_eq!(
        response.body,
        map_body(
            EXPIRATIONS,
            Value::Array(Array::from(vec![timestamp_value(Timestamp::from_millis(
                30_103
            ))]))
        )
    );
}
