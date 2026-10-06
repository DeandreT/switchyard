use std::sync::{
    Mutex as StdMutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use domain::{BrokerError, Command, QueueConfig, ReceiveMode, StateMachine, Timestamp};
use storage::{MemoryStore, StateStore};

use super::*;

const LINK: &str = "reused-receiver-name";

#[derive(Clone)]
struct Owner {
    machine: Arc<StdMutex<StateMachine<MemoryStore>>>,
    now: Arc<AtomicU64>,
    submissions: Arc<StdMutex<Vec<CommandKind>>>,
    takeover_after_receive: Arc<AtomicBool>,
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

    fn snapshot(&self) -> storage::StoreSnapshot {
        self.machine
            .lock()
            .expect("owner lock")
            .store()
            .snapshot()
            .expect("snapshot")
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
        panic!("settlement must not read rules")
    }

    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: crate::Attachment,
    ) -> Result<Option<crate::EntityMetadata>, BrokerRejection> {
        panic!("settlement must not rebind")
    }

    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        panic!("management must not wait for a ready message")
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
            .expect("submission lock")
            .push(kind.clone());
        let result = self.apply(kind.clone()).map_err(BrokerRejection::Refused)?;
        if let CommandKind::ReceiveDeferredHeld {
            session: Some(original),
            ..
        } = kind
            && self.takeover_after_receive.swap(false, Ordering::AcqRel)
        {
            // This is a controlled owner/registry interleave, not cloud evidence.
            self.apply(CommandKind::ReleaseSession {
                session: original.clone(),
            })
            .expect("release original");
            let before = self.snapshot();
            assert_eq!(
                self.apply(CommandKind::AcceptSession {
                    session_id: Some(original.session_id.clone()),
                    lock_duration_millis: Some(1_000),
                }),
                Err(BrokerError::SessionTakeoverPending {
                    session_id: original.session_id.clone()
                })
            );
            assert_eq!(self.snapshot(), before);
            let CommandOutcome::SessionAccepted(Some(replacement)) = self
                .apply(CommandKind::AcceptSession {
                    session_id: Some(SessionId::new("replacement").expect("different session")),
                    lock_duration_millis: Some(1_000),
                })
                .expect("takeover")
            else {
                panic!("replacement session")
            };
            self.management
                .register_session(LINK, entity, replacement.hold(), self.binding.clone())
                .await;
        }
        Ok(result)
    }
}

fn fixture(duration: u64, deferred: bool) -> (Owner, SessionHold, domain::Delivery) {
    let entity = EntityPath::new("orders").expect("entity");
    let namespace = NamespaceName::new("tenant").expect("namespace");
    let binding = crate::broker::test_admission(
        namespace.clone(),
        crate::Attachment::Queue(entity.clone()),
        crate::EntityMetadata::Queue(QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        }),
    )
    .binding;
    let management = ConnectionManagement::new();
    let owner = Owner {
        machine: Arc::new(StdMutex::new(StateMachine::new(MemoryStore::default()))),
        now: Arc::new(AtomicU64::new(100)),
        submissions: Arc::new(StdMutex::new(Vec::new())),
        takeover_after_receive: Arc::new(AtomicBool::new(false)),
        management,
        binding,
    };
    owner
        .apply(CommandKind::CreateQueue {
            config: QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            },
        })
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
    let CommandOutcome::SessionAccepted(Some(accepted)) = owner
        .apply(CommandKind::AcceptSession {
            session_id: Some(id),
            lock_duration_millis: Some(duration),
        })
        .expect("accept")
    else {
        panic!("accepted")
    };
    let hold = accepted.hold();
    owner.now.store(102, Ordering::Release);
    let CommandOutcome::Received(Some(delivery)) = owner
        .apply(CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(10_000),
            session: Some(hold.clone()),
        })
        .expect("receive")
    else {
        panic!("received")
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

async fn process(owner: &Owner, message: &Message) -> ManagementResponse {
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
            max_bytes: 4_096,
            per_message_overhead_bytes: 64,
        },
    )
    .await
}

fn settlement(token: LockToken, status: &str) -> Message {
    let updates = OrderedMap::from_iter([(
        Value::String("modified".to_owned()),
        Value::String("must not commit".to_owned()),
    )]);
    request(
        UPDATE_DISPOSITION_OPERATION,
        [
            (
                LOCK_TOKENS,
                Value::Array(Array::from(vec![Value::Uuid(lock_token_uuid(token))])),
            ),
            (DISPOSITION_STATUS, Value::String(status.to_owned())),
            (PROPERTIES_TO_MODIFY, Value::Map(updates)),
        ],
    )
}

#[tokio::test]
async fn reused_link_names_never_replace_a_deliverys_original_session_hold() {
    let (owner, original, delivery) = fixture(10, false);
    let token = delivery.lock.expect("message lock").token;
    owner
        .management
        .register_delivery_with_session(
            LINK,
            owner.binding.target().clone(),
            delivery.sequence,
            token,
            owner.binding.clone(),
            Some(original.clone()),
        )
        .await;
    owner.now.store(111, Ordering::Release);
    let before = owner.snapshot();
    assert_eq!(
        owner.apply(CommandKind::AcceptSession {
            session_id: Some(original.session_id.clone()),
            lock_duration_millis: Some(1_000),
        }),
        Err(BrokerError::SessionTakeoverPending {
            session_id: original.session_id.clone()
        })
    );
    assert_eq!(owner.snapshot(), before);
    let CommandOutcome::SessionAccepted(Some(replacement)) = owner
        .apply(CommandKind::AcceptSession {
            session_id: Some(SessionId::new("replacement").expect("different session")),
            lock_duration_millis: Some(1_000),
        })
        .expect("takeover")
    else {
        panic!("replacement")
    };
    assert_ne!(replacement.lock.token, original.token);
    assert_ne!(replacement.session_id, original.session_id);
    owner
        .management
        .register_session(
            LINK,
            owner.binding.target().clone(),
            replacement.hold(),
            owner.binding.clone(),
        )
        .await;
    owner.now.store(112, Ordering::Release);
    for status in ["completed", "abandoned", "defered", "suspended"] {
        let before = owner.snapshot();
        let response = process(&owner, &settlement(token, status)).await;
        assert_eq!(response.status_code, 410);
        assert_eq!(response.error_condition, Some(crate::SESSION_LOCK_LOST));
        assert_eq!(owner.snapshot(), before);
        assert_eq!(
            owner
                .management
                .delivery(LINK, token)
                .await
                .expect("receipt retained")
                .session,
            Some(original.clone())
        );
        let submissions = owner.submissions.lock().expect("submission lock");
        assert!(
            matches!(submissions.last(), Some(CommandKind::SettleHeld { session: Some(hold), .. }) if hold == &original)
        );
    }
    assert_eq!(
        owner
            .management
            .session(LINK)
            .await
            .expect("replacement association")
            .hold,
        replacement.hold()
    );
}

#[tokio::test]
async fn deferred_receipts_keep_the_hold_sampled_before_an_owner_registry_takeover() {
    let (owner, original, delivery) = fixture(1_000, true);
    owner
        .management
        .register_session(
            LINK,
            owner.binding.target().clone(),
            original.clone(),
            owner.binding.clone(),
        )
        .await;
    owner.takeover_after_receive.store(true, Ordering::Release);
    owner.now.store(103, Ordering::Release);
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
    )
    .await;
    assert_eq!(response.status_code, 200);
    let received = owner
        .submissions
        .lock()
        .expect("submission lock")
        .first()
        .cloned()
        .expect("held receive");
    assert!(
        matches!(received, CommandKind::ReceiveDeferredHeld { session: Some(hold), .. } if hold == original)
    );
    let stored = owner
        .machine
        .lock()
        .expect("owner lock")
        .message(
            owner.binding.namespace(),
            owner.binding.target(),
            delivery.sequence,
        )
        .expect("stored record")
        .expect("message");
    let domain::MessageState::Locked { token, .. } = stored.state else {
        panic!("new deferred lock")
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
            .expect("replacement")
            .hold
            .token,
        original.token
    );
    let before = owner.snapshot();
    let response = process(&owner, &settlement(token, "completed")).await;
    assert_eq!(response.status_code, 410);
    assert_eq!(response.error_condition, Some(crate::SESSION_LOCK_LOST));
    assert_eq!(owner.snapshot(), before);
    assert!(owner.management.delivery(LINK, token).await.is_some());
}
