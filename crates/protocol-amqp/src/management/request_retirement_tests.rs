//! Actual broker invocation, committed mutations, and post-result registry custody.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};

use amqp::{LinkEndpoint, Properties, ReceiverSettleMode, Role};
use domain::{
    DeliveryLock, MessageRecord, MessageState, QueueConfig, ReceiveMode, SessionId, SessionRecord,
    StateMachine,
};
use storage::{
    FjallStore, Key, MemoryStore, Mutation, StateStore, StorageError, StoreSnapshot,
    Value as StoredValue, WriteBatch,
};
use tokio::time::timeout;

use super::reply_retirement_tests::Wire;
use super::*;

const WAIT: Duration = Duration::from_secs(5);
const LINK: &str = "retained-management-receiver";
const STATE: &[u8] = b"original committed session state";
type RawResult = Result<CommandOutcome, BrokerRejection>;

#[derive(Default)]
struct ResultGate {
    paused: AtomicBool,
    entered: AtomicBool,
    released: Notify,
}

impl ResultGate {
    fn arm(&self) {
        self.entered.store(false, Ordering::SeqCst);
        self.paused.store(true, Ordering::SeqCst);
    }

    async fn wait(&self) {
        if !self.paused.load(Ordering::SeqCst) {
            return;
        }
        self.entered.store(true, Ordering::SeqCst);
        loop {
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if !self.paused.load(Ordering::SeqCst) {
                return;
            }
            released.await;
        }
    }

    fn release(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.released.notify_waiters();
    }
}

#[derive(Default)]
struct ApplyProgress {
    key: Vec<u8>,
    put: bool,
    before: bool,
    after: bool,
    allow_before: bool,
    allow_after: bool,
    commits: usize,
}

#[derive(Default)]
struct ApplyGate {
    progress: StdMutex<ApplyProgress>,
    released: Condvar,
    changed: Notify,
}

impl ApplyGate {
    fn arm(&self, key: Vec<u8>, put: bool) {
        *self.progress.lock().unwrap() = ApplyProgress {
            key,
            put,
            ..Default::default()
        };
    }

    fn matches(&self, batch: &WriteBatch) -> bool {
        let progress = self.progress.lock().unwrap();
        !progress.key.is_empty()
            && batch.mutations().iter().any(|mutation| match mutation {
                Mutation::Put { key, .. } => progress.put && *key == progress.key,
                Mutation::Delete { key } => !progress.put && *key == progress.key,
            })
    }

    fn pause(&self, after: bool) {
        let mut progress = self.progress.lock().unwrap();
        if after {
            progress.after = true;
            progress.commits += 1;
        } else {
            progress.before = true;
        }
        self.changed.notify_waiters();
        while !(if after {
            progress.allow_after
        } else {
            progress.allow_before
        }) {
            progress = self.released.wait(progress).unwrap();
        }
    }

    async fn reached(&self, after: bool) {
        timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let reached = {
                    let progress = self.progress.lock().unwrap();
                    if after {
                        progress.after
                    } else {
                        progress.before
                    }
                };
                if reached {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("the actual store apply reached its barrier");
    }

    fn release(&self, after: bool) {
        let mut progress = self.progress.lock().unwrap();
        if after {
            progress.allow_after = true;
        } else {
            progress.allow_before = true;
        }
        self.released.notify_all();
    }

    fn release_all(&self) {
        let mut progress = self
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        progress.allow_before = true;
        progress.allow_after = true;
        self.released.notify_all();
    }
}

#[derive(Clone)]
enum Backend {
    Memory(MemoryStore),
    Fjall(FjallStore),
}

#[derive(Clone)]
struct GatedStore {
    backend: Backend,
    gate: Arc<ApplyGate>,
}

impl StateStore for GatedStore {
    fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StorageError> {
        match &self.backend {
            Backend::Memory(store) => store.get(key),
            Backend::Fjall(store) => store.get(key),
        }
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let gated = self.gate.matches(&batch);
        if gated {
            self.gate.pause(false);
        }
        match &self.backend {
            Backend::Memory(store) => store.apply(batch)?,
            Backend::Fjall(store) => store.apply(batch)?,
        }
        if gated {
            self.gate.pause(true);
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        match &self.backend {
            Backend::Memory(store) => store.snapshot(),
            Backend::Fjall(store) => store.snapshot(),
        }
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, StoredValue)>, StorageError> {
        match &self.backend {
            Backend::Memory(store) => store.scan_from(prefix, start, limit),
            Backend::Fjall(store) => store.scan_from(prefix, start, limit),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Submission {
    kind: CommandKind,
    polled: bool,
    result: Option<RawResult>,
}

#[derive(Clone)]
struct ActualBroker {
    handle: server::BrokerHandle,
    submissions: Arc<StdMutex<Vec<Submission>>>,
    result_gate: Arc<ResultGate>,
}

impl Broker for ActualBroker {
    fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = RawResult> + Send {
        // This deliberately eager witness distinguishes method invocation from
        // future polling; the delegated command and its result are still real.
        let index = {
            let mut submissions = self.submissions.lock().unwrap();
            let index = submissions.len();
            submissions.push(Submission {
                kind: kind.clone(),
                polled: false,
                result: None,
            });
            index
        };
        async move {
            self.submissions.lock().unwrap()[index].polled = true;
            let result = self
                .handle
                .submit(namespace, entity, kind)
                .await
                .map_err(|error| match error {
                    server::SubmitError::Propose(server::ProposeError::Broker(error)) => {
                        BrokerRejection::Refused(error)
                    }
                    other => BrokerRejection::Unavailable(other.to_string()),
                });
            self.submissions.lock().unwrap()[index].result = Some(result.clone());
            self.result_gate.wait().await;
            result
        }
    }

    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

struct Actor {
    owner: Option<server::Broker>,
    actual: Option<ActualBroker>,
    store: Option<GatedStore>,
    memory: Option<MemoryStore>,
    directory: Option<tempfile::TempDir>,
    gate: Arc<ApplyGate>,
    result_gate: Arc<ResultGate>,
    submissions: Arc<StdMutex<Vec<Submission>>>,
    namespace: NamespaceName,
    entity: EntityPath,
    clock: server::ManualClock,
}

impl Actor {
    fn new(durable: bool, sessions: bool) -> Self {
        let gate = Arc::new(ApplyGate::default());
        let memory = (!durable).then(MemoryStore::default);
        let directory = durable.then(|| tempfile::tempdir().unwrap());
        let backend = match &directory {
            Some(directory) => Backend::Fjall(FjallStore::open(directory.path()).unwrap()),
            None => Backend::Memory(memory.as_ref().unwrap().clone()),
        };
        let store = GatedStore {
            backend,
            gate: Arc::clone(&gate),
        };
        let clock = server::ManualClock::at(1_000);
        let owner = server::Broker::spawn(server::LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let submissions = Arc::new(StdMutex::new(Vec::new()));
        let result_gate = Arc::new(ResultGate::default());
        let actual = ActualBroker {
            handle: owner.handle(),
            submissions: Arc::clone(&submissions),
            result_gate: Arc::clone(&result_gate),
        };
        let actor = Self {
            owner: Some(owner),
            actual: Some(actual),
            store: Some(store),
            memory,
            directory,
            gate,
            result_gate,
            submissions,
            namespace: NamespaceName::new("tenant").unwrap(),
            entity: EntityPath::new("orders").unwrap(),
            clock,
        };
        assert_eq!(
            actor.intent(CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_session: sessions,
                    ..Default::default()
                },
            }),
            CommandOutcome::QueueCreated
        );
        actor
    }

    fn intent(&self, kind: CommandKind) -> CommandOutcome {
        self.intent_on(&self.entity, kind)
    }

    fn intent_on(&self, entity: &EntityPath, kind: CommandKind) -> CommandOutcome {
        self.actual
            .as_ref()
            .unwrap()
            .handle
            .submit_blocking(self.namespace.clone(), entity.clone(), kind)
            .unwrap()
    }

    fn request_broker(&self) -> RequestBroker<ActualBroker> {
        RequestBroker::new(self.actual.as_ref().unwrap().clone())
    }

    fn accept(&self) -> SessionHold {
        match self.intent(CommandKind::AcceptSession {
            session_id: Some(session_id()),
            lock_duration_millis: None,
        }) {
            CommandOutcome::SessionAccepted(Some(accepted)) => accepted.hold(),
            other => panic!("a real named session was accepted: {other:?}"),
        }
    }

    fn receive(&self) -> Delivery {
        self.receive_on(&self.entity)
    }

    fn receive_on(&self, entity: &EntityPath) -> Delivery {
        let CommandOutcome::Sent { .. } = self.intent_on(
            entity,
            CommandKind::Send {
                message_id: "retained-message".to_owned(),
                body: b"original body".to_vec(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        ) else {
            panic!("a message was committed");
        };
        match self.intent_on(
            entity,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        ) {
            CommandOutcome::Received(Some(delivery)) => delivery,
            other => panic!("the broker returned an actual message lock: {other:?}"),
        }
    }

    fn store(&self) -> &GatedStore {
        self.store.as_ref().unwrap()
    }
    fn snapshot(&self) -> StoreSnapshot {
        self.store().snapshot().unwrap()
    }

    fn session(&self) -> SessionRecord {
        StateMachine::new(self.store().clone())
            .session(&self.namespace, &self.entity, &session_id())
            .unwrap()
            .unwrap()
    }

    fn message(&self, sequence: SequenceNumber) -> Option<MessageRecord> {
        self.message_on(&self.entity, sequence)
    }

    fn message_on(&self, entity: &EntityPath, sequence: SequenceNumber) -> Option<MessageRecord> {
        StateMachine::new(self.store().clone())
            .message(&self.namespace, entity, sequence)
            .unwrap()
    }

    fn assert_one(&self, kind: CommandKind, result: RawResult) {
        assert_eq!(
            *self.submissions.lock().unwrap(),
            vec![Submission {
                kind,
                polled: true,
                result: Some(result)
            }]
        );
    }

    fn reopen(&mut self, expected: &StoreSnapshot) {
        self.gate.release_all();
        self.result_gate.release();
        drop(self.actual.take());
        drop(self.owner.take());
        drop(self.store.take());
        let backend = match &self.directory {
            Some(directory) => Backend::Fjall(FjallStore::open(directory.path()).unwrap()),
            None => Backend::Memory(self.memory.as_ref().unwrap().clone()),
        };
        self.store = Some(GatedStore {
            backend,
            gate: Arc::clone(&self.gate),
        });
        assert_eq!(&self.snapshot(), expected);
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        self.gate.release_all();
        self.result_gate.release();
        drop(self.actual.take());
        drop(self.owner.take());
    }
}

fn session_id() -> SessionId {
    SessionId::new("retained-session").unwrap()
}

fn replacement_delivery(actor: &Actor) -> (EntityPath, Delivery) {
    let entity = EntityPath::new("replacement").unwrap();
    assert_eq!(
        actor.intent_on(
            &entity,
            CommandKind::CreateQueue {
                config: QueueConfig::default()
            }
        ),
        CommandOutcome::QueueCreated
    );
    let delivery = actor.receive_on(&entity);
    assert_eq!(delivery.sequence, SequenceNumber::new(1));
    assert_eq!(delivery.lock.unwrap().token, LockToken::new(1));
    (entity, delivery)
}

#[tokio::test(flavor = "current_thread")]
async fn actual_cross_entity_token_collision_excludes_the_unrelated_ordinary_alias() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        let original = actor.receive();
        let token = original.lock.unwrap().token;
        assert_eq!(original.sequence, SequenceNumber::new(1));
        assert_eq!(token, LockToken::new(1));
        let (other, replacement) = replacement_delivery(&actor);
        let management = ConnectionManagement::new();
        let ordinary = management
            .register_delivery(LINK, other.clone(), replacement.sequence, token)
            .await;
        let request_response = management
            .register_request_response_delivery(actor.entity.clone(), original.clone())
            .await;
        let selected = management
            .managed_delivery(&actor.entity, Some(LINK), token)
            .await
            .unwrap();
        assert_eq!(selected.managed.delivery, Some(original));
        assert!(selected.ordinary.is_none());
        assert_eq!(selected.request_response.as_ref(), Some(&request_response));
        let before = actor.snapshot();
        management.unregister_managed_delivery(&selected).await;
        assert_eq!(
            management.deliveries.read().await.get(&ordinary.key),
            Some(&ordinary)
        );
        assert!(
            management
                .request_response_delivery(&actor.entity, token)
                .await
                .is_none()
        );
        management.unregister_delivery(&ordinary).await;
        assert!(management.delivery(LINK, token).await.is_none());
        assert_eq!(actor.snapshot(), before);
        actor.reopen(&before);
        assert!(
            matches!(actor.message_on(&other, replacement.sequence).unwrap().state, MessageState::Locked { token: held, .. } if held == token)
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_equal_payload_registrations_have_distinct_owners_for_remove_and_refresh() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        let delivery = actor.receive();
        let lock = delivery.lock.unwrap();
        let management = ConnectionManagement::new();
        let old = management
            .register_delivery(LINK, actor.entity.clone(), delivery.sequence, lock.token)
            .await;
        let replacement = management
            .register_delivery(LINK, actor.entity.clone(), delivery.sequence, lock.token)
            .await;
        assert_eq!(old.managed, replacement.managed);
        assert_ne!(old, replacement);
        management.unregister_delivery(&old).await;
        assert_eq!(
            management.deliveries.read().await.get(&replacement.key),
            Some(&replacement)
        );
        let started = Instant::now();
        let old_rr = management
            .register_request_response_delivery_at(actor.entity.clone(), delivery.clone(), started)
            .await;
        let replacement_rr = management
            .register_request_response_delivery_at(actor.entity.clone(), delivery, started)
            .await;
        assert_ne!(old_rr, replacement_rr);
        let before_rr = management
            .request_response_deliveries
            .read()
            .await
            .get(&replacement_rr.key)
            .unwrap()
            .clone();
        management
            .refresh_request_response_delivery_at(
                Some(&old_rr),
                domain::Timestamp::from_millis(999_999),
                120_000,
                started + Duration::from_millis(1),
            )
            .await;
        management
            .unregister_request_response_delivery(&old_rr)
            .await;
        assert_eq!(
            management
                .request_response_deliveries
                .read()
                .await
                .get(&replacement_rr.key),
            Some(&before_rr)
        );
        management
            .unregister_request_response_delivery(&replacement_rr)
            .await;
        management.unregister_delivery(&replacement).await;
        let committed = actor.snapshot();
        actor.reopen(&committed);
        assert!(
            matches!(actor.message(replacement.managed.sequence).unwrap().state, MessageState::Locked { token, .. } if token == lock.token)
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_authority_captures_an_expired_alias_and_renews_it_before_purge() {
    for durable in [false, true] {
        let actor = Actor::new(durable, false);
        let delivery = actor.receive();
        let lock = delivery.lock.unwrap();
        let management = ConnectionManagement::new();
        management
            .register_delivery(LINK, actor.entity.clone(), delivery.sequence, lock.token)
            .await;
        let now = Instant::now();
        let started = now - Duration::from_millis(lock.lock_duration_millis);
        let registration = management
            .register_request_response_delivery_at(actor.entity.clone(), delivery.clone(), started)
            .await;
        let selected = management
            .managed_delivery(&actor.entity, Some(LINK), lock.token)
            .await
            .unwrap();
        assert!(selected.managed.delivery.is_none());
        assert_eq!(selected.request_response.as_ref(), Some(&registration));
        let renewed_until = domain::Timestamp::from_millis(lock.locked_until.as_millis() + 1);
        management
            .refresh_request_response_delivery_at(
                selected.request_response.as_ref(),
                renewed_until,
                lock.lock_duration_millis,
                now,
            )
            .await;
        let row = management
            .request_response_deliveries
            .read()
            .await
            .get(&registration.key)
            .unwrap()
            .clone();
        assert!(row.expires_at > now);
        assert_eq!(
            row.managed
                .delivery
                .as_ref()
                .unwrap()
                .lock
                .unwrap()
                .locked_until,
            renewed_until
        );
        let expired = management
            .register_request_response_delivery_at(actor.entity.clone(), delivery.clone(), started)
            .await;
        assert!(
            management
                .managed_delivery(&actor.entity, None, lock.token)
                .await
                .is_none()
        );
        assert!(
            !management
                .request_response_deliveries
                .read()
                .await
                .contains_key(&expired.key)
        );
        let expired = management
            .register_request_response_delivery_at(actor.entity.clone(), delivery, started)
            .await;
        management
            .refresh_request_response_delivery_at(
                None,
                renewed_until,
                lock.lock_duration_millis,
                now,
            )
            .await;
        assert!(
            !management
                .request_response_deliveries
                .read()
                .await
                .contains_key(&expired.key)
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_management_results_preserve_replaced_owners_after_success_and_definitive_loss() {
    for durable in [false, true] {
        for renew in [false, true] {
            for lost in [false, true] {
                for equal_payload in [false, true] {
                    let mut actor = Actor::new(durable, false);
                    let delivery = actor.receive();
                    let lock = delivery.lock.unwrap();
                    assert_eq!(delivery.sequence, SequenceNumber::new(1));
                    assert_eq!(lock.token, LockToken::new(1));
                    if lost {
                        actor.clock.set(lock.locked_until.as_millis());
                        assert_eq!(
                            actor.intent(CommandKind::ExpireLocks),
                            CommandOutcome::LocksExpired {
                                returned_to_ready: 1,
                                dead_lettered: 0
                            }
                        );
                    } else {
                        actor.clock.set(2_000);
                    }
                    let (other, other_delivery) = replacement_delivery(&actor);
                    let other_before = actor.message_on(&other, other_delivery.sequence).unwrap();
                    let management = ConnectionManagement::new();
                    let (old, old_rr) = register_lock(&management, &actor, &delivery).await;
                    let broker = actor.request_broker();
                    let message = if renew {
                        request(
                            RENEW_LOCK_OPERATION,
                            true,
                            [(LOCK_TOKENS, token_value(lock.token))],
                        )
                    } else {
                        request(
                            UPDATE_DISPOSITION_OPERATION,
                            true,
                            [
                                (LOCK_TOKENS, token_value(lock.token)),
                                ("disposition-status", Value::String("completed".to_owned())),
                            ],
                        )
                    };
                    let mut operation = PendingOperation::new(
                        process_request(
                            &message,
                            MessageId::Ulong(87),
                            &actor.namespace,
                            &actor.entity,
                            &broker,
                            &management,
                            None,
                        ),
                        broker.control(),
                    );
                    let raw = paused_broker_result(&mut operation, &actor).await;
                    let entity = if equal_payload {
                        actor.entity.clone()
                    } else {
                        other.clone()
                    };
                    let replacement = management
                        .register_delivery(LINK, entity, delivery.sequence, lock.token)
                        .await;
                    let replacement_rr = management
                        .register_request_response_delivery(actor.entity.clone(), delivery.clone())
                        .await;
                    assert_ne!(old, replacement);
                    assert_ne!(old_rr, replacement_rr);
                    let held = management.request_response_deliveries.write().await;
                    let row_before = held.get(&replacement_rr.key).unwrap().clone();
                    actor.result_gate.release();
                    assert_eq!(returned_while_pending(&mut operation, &actor).await, raw);
                    operation.retire();
                    let mut observer = Box::pin(operation.observe());
                    pending_once(observer.as_mut()).await;
                    drop(observer);
                    assert!(operation.take_packet().is_none());
                    drop(held);
                    let response = take_response(&mut operation).await;
                    let kind = if renew {
                        CommandKind::RenewLock {
                            sequence: delivery.sequence,
                            lock_token: lock.token,
                            lock_duration_millis: None,
                        }
                    } else {
                        CommandKind::Complete {
                            sequence: delivery.sequence,
                            lock_token: lock.token,
                        }
                    };
                    if lost {
                        let rejection =
                            BrokerRejection::Refused(domain::BrokerError::MessageNotLocked {
                                sequence: delivery.sequence,
                            });
                        assert_eq!(raw, Err(rejection.clone()));
                        assert_eq!(response.correlation_id, MessageId::Ulong(87));
                        assert_eq!(response.status_code, 410);
                        assert_eq!(response.status_description, rejection.to_string());
                        assert_eq!(response.error_condition, Some(crate::MESSAGE_LOCK_LOST));
                        assert_eq!(response.tracking_id.as_deref(), Some("original-tracking"));
                        assert_eq!(response.body, Value::Null);
                    } else if let Ok(CommandOutcome::LockRenewed { locked_until, .. }) = &raw {
                        assert_accepted(
                            &response,
                            &map_body(
                                EXPIRATIONS,
                                Value::Array(Array::from(vec![timestamp_value(*locked_until)])),
                            ),
                        );
                    } else {
                        assert_eq!(raw, Ok(CommandOutcome::Completed));
                        assert_accepted(&response, &Value::Null);
                    }
                    actor.assert_one(kind, raw);
                    assert_eq!(
                        management.deliveries.read().await.get(&replacement.key),
                        Some(&replacement)
                    );
                    assert_eq!(
                        management
                            .request_response_deliveries
                            .read()
                            .await
                            .get(&replacement_rr.key),
                        Some(&row_before)
                    );
                    assert_eq!(
                        actor.message_on(&other, other_delivery.sequence),
                        Some(other_before)
                    );
                    let committed = actor.snapshot();
                    drop(operation);
                    drop(broker);
                    actor.reopen(&committed);
                }
            }
        }
    }
}

fn request(
    operation: &str,
    associated: bool,
    entries: impl IntoIterator<Item = (&'static str, Value)>,
) -> Message {
    let mut properties = ApplicationProperties::default();
    properties.insert(OPERATION_PROPERTY, operation);
    properties.insert(TRACKING_ID_PROPERTY, "original-tracking");
    if associated {
        properties.insert(ASSOCIATED_LINK_NAME_PROPERTY, LINK);
    }
    let mut map = OrderedMap::new();
    for (name, value) in entries {
        map.insert(Value::String(name.to_owned()), value);
    }
    Message {
        application_properties: Some(properties),
        body: Body::Value(Value::Map(map)),
        ..Default::default()
    }
}

fn state_request() -> Message {
    request(
        SET_SESSION_STATE_OPERATION,
        true,
        [
            (SESSION_ID, Value::String(session_id().as_str().to_owned())),
            (SESSION_STATE, Value::Binary(Binary::from(STATE.to_vec()))),
        ],
    )
}

fn token_value(token: LockToken) -> Value {
    let mut bytes = [0_u8; 16];
    bytes[8..].copy_from_slice(&token.as_u64().to_be_bytes());
    Value::Array(Array::from(vec![Value::Uuid(Uuid::from(bytes))]))
}

async fn install_hold(management: &Arc<ConnectionManagement>, actor: &Actor, hold: SessionHold) {
    let claim = management.claim_session(LINK, actor.entity.clone());
    management
        .install_session(&claim, hold, || true)
        .await
        .expect("the actual grant was registered");
}

async fn register_lock(
    management: &ConnectionManagement,
    actor: &Actor,
    delivery: &Delivery,
) -> (DeliveryRegistration, RequestResponseDeliveryRegistration) {
    let ordinary = management
        .register_delivery(
            LINK,
            actor.entity.clone(),
            delivery.sequence,
            delivery.lock.unwrap().token,
        )
        .await;
    let request_response = management
        .register_request_response_delivery(actor.entity.clone(), delivery.clone())
        .await;
    (ordinary, request_response)
}

async fn pending_once<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    poll_fn(|context| {
        assert!(
            future.as_mut().poll(context).is_pending(),
            "the original is actually blocked"
        );
        Poll::Ready(())
    })
    .await;
}

async fn returned_while_pending(
    operation: &mut PendingOperation<'_, ManagementResponse>,
    actor: &Actor,
) -> RawResult {
    let mut observation = Box::pin(operation.observe());
    timeout(
        WAIT,
        poll_fn(|context| {
            assert!(
                observation.as_mut().poll(context).is_pending(),
                "the original retained operation is still pending"
            );
            let submissions = actor.submissions.lock().unwrap();
            assert!(submissions.len() <= 1, "the original was not resubmitted");
            match submissions
                .first()
                .and_then(|submission| submission.result.clone())
            {
                Some(result) => Poll::Ready(result),
                None => Poll::Pending,
            }
        }),
    )
    .await
    .expect("the actual broker result is retained while the original is pending")
}

async fn paused_broker_result(
    operation: &mut PendingOperation<'_, ManagementResponse>,
    actor: &Actor,
) -> RawResult {
    actor.result_gate.arm();
    let result = returned_while_pending(operation, actor).await;
    assert!(actor.result_gate.entered.load(Ordering::SeqCst));
    result
}

fn assert_accepted(response: &ManagementResponse, body: &Value) {
    assert_eq!(response.correlation_id, MessageId::Ulong(87));
    assert_eq!(response.status_code, 200);
    assert_eq!(response.status_description, "OK");
    assert_eq!(response.error_condition, None);
    assert_eq!(response.tracking_id.as_deref(), Some("original-tracking"));
    assert_eq!(&response.body, body);
}

async fn take_response(
    operation: &mut PendingOperation<'_, ManagementResponse>,
) -> ManagementResponse {
    let pointer = timeout(WAIT, operation.finish())
        .await
        .unwrap()
        .expect("begun original completes") as *const ManagementResponse;
    assert_eq!(
        operation.observe().await.unwrap() as *const ManagementResponse,
        pointer
    );
    let packet = operation
        .take_packet()
        .expect("the original packet is consumed once");
    assert!(packet.started);
    assert!(packet.retired);
    assert!(operation.take_packet().is_none());
    packet
        .result
        .expect("the exact original result was retained")
}

#[tokio::test(flavor = "current_thread")]
async fn retirement_before_first_poll_never_invokes_an_eager_broker_constructor() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, true);
        let hold = actor.accept();
        let before = actor.snapshot();
        let broker = actor.request_broker();
        let mut operation = PendingOperation::new(
            broker.submit(
                actor.namespace.clone(),
                actor.entity.clone(),
                CommandKind::SetSessionState {
                    session: hold.clone(),
                    state: STATE.to_vec(),
                },
            ),
            broker.control(),
        );
        assert!(actor.submissions.lock().unwrap().is_empty());
        assert!(operation.finish().await.is_none());
        let packet = operation.take_packet().unwrap();
        assert!(!packet.started);
        assert!(packet.retired);
        assert!(packet.result.is_none());
        assert!(operation.take_packet().is_none());
        assert!(actor.submissions.lock().unwrap().is_empty());
        drop(operation);
        drop(broker);
        actor.reopen(&before);
        assert_eq!(actor.session().lock.unwrap().token, hold.token);
        assert!(actor.session().state.is_empty());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn retirement_while_actual_session_preparation_is_locked_starts_no_broker_command() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, true);
        let hold = actor.accept();
        let management = ConnectionManagement::new();
        install_hold(&management, &actor, hold.clone()).await;
        let before = actor.snapshot();
        let held = management.sessions.write().await;
        let broker = actor.request_broker();
        let message = state_request();
        let mut operation = PendingOperation::new(
            process_request(
                &message,
                MessageId::Ulong(87),
                &actor.namespace,
                &actor.entity,
                &broker,
                &management,
                None,
            ),
            broker.control(),
        );
        let mut observation = Box::pin(operation.observe());
        pending_once(observation.as_mut()).await;
        drop(observation);
        assert!(!broker.control().started());
        assert!(actor.submissions.lock().unwrap().is_empty());
        assert!(operation.finish().await.is_none());
        let packet = operation.take_packet().unwrap();
        assert!(!packet.started && packet.retired && packet.result.is_none());
        drop(held);
        assert!(actor.submissions.lock().unwrap().is_empty());
        drop(operation);
        drop(broker);
        actor.reopen(&before);
        assert_eq!(actor.session().lock.unwrap().token, hold.token);
        assert!(actor.session().state.is_empty());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn retirement_while_actual_delivery_preparation_is_locked_starts_no_broker_command() {
    for durable in [false, true] {
        for request_response in [false, true] {
            let mut actor = Actor::new(durable, false);
            let delivery = actor.receive();
            let lock = delivery.lock.unwrap();
            let management = ConnectionManagement::new();
            register_lock(&management, &actor, &delivery).await;
            let before = actor.snapshot();
            let held: Box<dyn Send + '_> = if request_response {
                Box::new(management.request_response_deliveries.write().await)
            } else {
                Box::new(management.deliveries.write().await)
            };
            let broker = actor.request_broker();
            let message = request(
                RENEW_LOCK_OPERATION,
                true,
                [(LOCK_TOKENS, token_value(lock.token))],
            );
            let mut operation = PendingOperation::new(
                process_request(
                    &message,
                    MessageId::Ulong(87),
                    &actor.namespace,
                    &actor.entity,
                    &broker,
                    &management,
                    None,
                ),
                broker.control(),
            );
            let mut observation = Box::pin(operation.observe());
            pending_once(observation.as_mut()).await;
            drop(observation);
            assert!(!broker.control().started());
            assert!(operation.finish().await.is_none());
            let packet = operation.take_packet().unwrap();
            assert!(!packet.started && packet.retired && packet.result.is_none());
            drop(held);
            assert!(actor.submissions.lock().unwrap().is_empty());
            assert!(
                management
                    .managed_delivery(&actor.entity, Some(LINK), lock.token)
                    .await
                    .is_some()
            );
            drop(operation);
            drop(broker);
            actor.reopen(&before);
            assert!(
                matches!(actor.message(delivery.sequence).unwrap().state, MessageState::Locked { token, locked_until, .. } if token == lock.token && locked_until == lock.locked_until)
            );
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_begun_actual_mutation_retains_its_original_raw_result_before_and_after_apply() {
    for durable in [false, true] {
        for retire_after_apply in [false, true] {
            let mut actor = Actor::new(durable, true);
            let hold = actor.accept();
            let kind = CommandKind::SetSessionState {
                session: hold.clone(),
                state: STATE.to_vec(),
            };
            actor.gate.arm(
                domain::keys::session(&actor.namespace, &actor.entity, &hold.session_id),
                true,
            );
            let broker = actor.request_broker();
            let mut operation = PendingOperation::new(
                broker.submit(actor.namespace.clone(), actor.entity.clone(), kind.clone()),
                broker.control(),
            );
            let mut observation = Box::pin(operation.observe());
            pending_once(observation.as_mut()).await;
            drop(observation);
            actor.gate.reached(false).await;
            assert!(broker.control().started());
            assert_eq!(actor.submissions.lock().unwrap().len(), 1);
            assert!(actor.session().state.is_empty());
            if retire_after_apply {
                actor.gate.release(false);
                actor.gate.reached(true).await;
                assert_eq!(actor.session().state, STATE);
            }
            operation.retire();
            assert!(operation.take_packet().is_none());
            if !retire_after_apply {
                actor.gate.release(false);
                actor.gate.reached(true).await;
            }
            assert_eq!(actor.gate.progress.lock().unwrap().commits, 1);
            actor.gate.release(true);
            let expected = Ok(CommandOutcome::SessionStateSet);
            let pointer =
                timeout(WAIT, operation.finish()).await.unwrap().unwrap() as *const RawResult;
            assert_eq!(operation.observe().await.unwrap(), &expected);
            assert_eq!(
                operation.observe().await.unwrap() as *const RawResult,
                pointer
            );
            let packet = operation.take_packet().unwrap();
            assert!(packet.started && packet.retired);
            assert_eq!(packet.result, Some(expected.clone()));
            assert!(operation.take_packet().is_none());
            actor.assert_one(kind, expected);
            assert_eq!(actor.gate.progress.lock().unwrap().commits, 1);
            let committed = actor.snapshot();
            drop(operation);
            drop(broker);
            actor.reopen(&committed);
            assert_eq!(actor.session().state, STATE);
            assert_eq!(actor.session().lock.unwrap().token, hold.token);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_begun_session_state_request_drains_to_its_original_management_response() {
    for durable in [false, true] {
        for retire_after_apply in [false, true] {
            let mut actor = Actor::new(durable, true);
            let hold = actor.accept();
            let management = ConnectionManagement::new();
            install_hold(&management, &actor, hold.clone()).await;
            actor.gate.arm(
                domain::keys::session(&actor.namespace, &actor.entity, &hold.session_id),
                true,
            );
            let broker = actor.request_broker();
            let message = state_request();
            let mut operation = PendingOperation::new(
                process_request(
                    &message,
                    MessageId::Ulong(87),
                    &actor.namespace,
                    &actor.entity,
                    &broker,
                    &management,
                    None,
                ),
                broker.control(),
            );
            let mut observation = Box::pin(operation.observe());
            pending_once(observation.as_mut()).await;
            drop(observation);
            actor.gate.reached(false).await;
            if retire_after_apply {
                actor.gate.release(false);
                actor.gate.reached(true).await;
            }
            operation.retire();
            if !retire_after_apply {
                actor.gate.release(false);
                actor.gate.reached(true).await;
            }
            actor.gate.release(true);
            assert_accepted(&take_response(&mut operation).await, &Value::Null);
            actor.assert_one(
                CommandKind::SetSessionState {
                    session: hold.clone(),
                    state: STATE.to_vec(),
                },
                Ok(CommandOutcome::SessionStateSet),
            );
            assert_eq!(actor.gate.progress.lock().unwrap().commits, 1);
            let committed = actor.snapshot();
            drop(operation);
            drop(broker);
            actor.reopen(&committed);
            assert_eq!(actor.session().state, STATE);
            assert_eq!(actor.session().lock.unwrap().token, hold.token);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn retired_deferred_receive_drains_actual_post_result_delivery_registration() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        let original = actor.receive();
        assert_eq!(
            actor.intent(CommandKind::Defer {
                sequence: original.sequence,
                lock_token: original.lock.unwrap().token,
                replacement_envelope: None
            }),
            CommandOutcome::Deferred
        );
        let management = ConnectionManagement::new();
        let held = management.request_response_deliveries.write().await;
        let broker = actor.request_broker();
        let message = request(
            RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            false,
            [
                (
                    "sequence-numbers",
                    Value::Array(Array::from(vec![Value::Long(
                        i64::try_from(original.sequence.as_u64()).unwrap(),
                    )])),
                ),
                ("receiver-settle-mode", Value::Uint(1)),
            ],
        );
        let mut operation = PendingOperation::new(
            process_request(
                &message,
                MessageId::Ulong(87),
                &actor.namespace,
                &actor.entity,
                &broker,
                &management,
                None,
            ),
            broker.control(),
        );
        let raw = returned_while_pending(&mut operation, &actor).await;
        let Ok(CommandOutcome::DeferredReceived(deliveries)) = &raw else {
            panic!("actual deferred receive returned: {raw:?}");
        };
        assert_eq!(deliveries.len(), 1);
        let delivery = deliveries[0].clone();
        let lock = delivery.lock.unwrap();
        assert_ne!(lock.token, original.lock.unwrap().token);
        assert!(held.is_empty());
        operation.retire();
        let mut observation = Box::pin(operation.observe());
        pending_once(observation.as_mut()).await;
        drop(observation);
        assert!(operation.take_packet().is_none());
        drop(held);
        let response = take_response(&mut operation).await;
        let encoded =
            amqp::encode_message(&crate::message::write_delivery_from(&delivery, None).unwrap())
                .unwrap();
        let mut entry = OrderedMap::new();
        entry.insert(
            Value::String("message".to_owned()),
            Value::Binary(Binary::from(encoded)),
        );
        let mut token = [0_u8; 16];
        token[8..].copy_from_slice(&lock.token.as_u64().to_be_bytes());
        entry.insert(
            Value::String("lock-token".to_owned()),
            Value::Uuid(Uuid::from(token)),
        );
        assert_accepted(
            &response,
            &map_body("messages", Value::List(vec![Value::Map(entry)])),
        );
        let registered = management
            .request_response_delivery(&actor.entity, lock.token)
            .await
            .unwrap();
        assert_eq!(registered.entity, actor.entity);
        assert_eq!(registered.sequence, delivery.sequence);
        assert_eq!(registered.delivery, Some(delivery.clone()));
        actor.assert_one(
            CommandKind::ReceiveDeferred {
                sequences: vec![original.sequence],
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
            raw,
        );
        let committed = actor.snapshot();
        drop(operation);
        drop(broker);
        actor.reopen(&committed);
        let stored = actor.message(delivery.sequence).unwrap();
        assert_eq!(stored.body, delivery.body);
        assert_eq!(stored.delivery_count, delivery.delivery_count);
        assert!(
            matches!(stored.state, MessageState::Locked { token, locked_until, .. } if token == lock.token && locked_until == lock.locked_until)
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn retired_renewal_drains_actual_post_result_lock_refresh() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        let delivery = actor.receive();
        let old_lock = delivery.lock.unwrap();
        let management = ConnectionManagement::new();
        register_lock(&management, &actor, &delivery).await;
        actor.clock.set(2_000);
        let key = RequestResponseDeliveryKey {
            entity: actor.entity.clone(),
            lock_token: old_lock.token,
        };
        let broker = actor.request_broker();
        let message = request(
            RENEW_LOCK_OPERATION,
            true,
            [(LOCK_TOKENS, token_value(old_lock.token))],
        );
        let mut operation = PendingOperation::new(
            process_request(
                &message,
                MessageId::Ulong(87),
                &actor.namespace,
                &actor.entity,
                &broker,
                &management,
                None,
            ),
            broker.control(),
        );
        let raw = paused_broker_result(&mut operation, &actor).await;
        let held = management.request_response_deliveries.write().await;
        assert_eq!(
            held.get(&key)
                .unwrap()
                .managed
                .delivery
                .as_ref()
                .unwrap()
                .lock,
            Some(old_lock)
        );
        actor.result_gate.release();
        assert_eq!(returned_while_pending(&mut operation, &actor).await, raw);
        let Ok(CommandOutcome::LockRenewed {
            locked_until,
            lock_duration_millis,
        }) = &raw
        else {
            panic!("actual renewal returned: {raw:?}");
        };
        let renewed = DeliveryLock {
            token: old_lock.token,
            locked_until: *locked_until,
            lock_duration_millis: *lock_duration_millis,
        };
        assert!(renewed.locked_until > old_lock.locked_until);
        operation.retire();
        let mut observation = Box::pin(operation.observe());
        pending_once(observation.as_mut()).await;
        drop(observation);
        assert!(operation.take_packet().is_none());
        drop(held);
        let expected_body = map_body(
            EXPIRATIONS,
            Value::Array(Array::from(vec![timestamp_value(renewed.locked_until)])),
        );
        assert_accepted(&take_response(&mut operation).await, &expected_body);
        assert_eq!(
            management
                .request_response_delivery(&actor.entity, old_lock.token)
                .await
                .unwrap()
                .delivery
                .unwrap()
                .lock,
            Some(renewed)
        );
        actor.assert_one(
            CommandKind::RenewLock {
                sequence: delivery.sequence,
                lock_token: old_lock.token,
                lock_duration_millis: None,
            },
            raw,
        );
        let committed = actor.snapshot();
        drop(operation);
        drop(broker);
        actor.reopen(&committed);
        assert!(
            matches!(actor.message(delivery.sequence).unwrap().state, MessageState::Locked { token, locked_until, .. } if token == renewed.token && locked_until == renewed.locked_until)
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn retired_disposition_drains_actual_post_result_both_registry_removals() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        let delivery = actor.receive();
        let token = delivery.lock.unwrap().token;
        let management = ConnectionManagement::new();
        register_lock(&management, &actor, &delivery).await;
        let broker = actor.request_broker();
        let message = request(
            UPDATE_DISPOSITION_OPERATION,
            true,
            [
                (LOCK_TOKENS, token_value(token)),
                ("disposition-status", Value::String("completed".to_owned())),
            ],
        );
        let mut operation = PendingOperation::new(
            process_request(
                &message,
                MessageId::Ulong(87),
                &actor.namespace,
                &actor.entity,
                &broker,
                &management,
                None,
            ),
            broker.control(),
        );
        let raw = paused_broker_result(&mut operation, &actor).await;
        let held = management.request_response_deliveries.write().await;
        assert_eq!(held.len(), 1);
        actor.result_gate.release();
        assert_eq!(returned_while_pending(&mut operation, &actor).await, raw);
        assert_eq!(raw, Ok(CommandOutcome::Completed));
        assert!(actor.message(delivery.sequence).is_none());
        operation.retire();
        let mut observation = Box::pin(operation.observe());
        pending_once(observation.as_mut()).await;
        drop(observation);
        assert!(operation.take_packet().is_none());
        drop(held);
        assert_accepted(&take_response(&mut operation).await, &Value::Null);
        assert!(
            management
                .request_response_delivery(&actor.entity, token)
                .await
                .is_none()
        );
        assert!(
            management
                .managed_delivery(&actor.entity, Some(LINK), token)
                .await
                .is_none()
        );
        actor.assert_one(
            CommandKind::Complete {
                sequence: delivery.sequence,
                lock_token: token,
            },
            raw,
        );
        let committed = actor.snapshot();
        drop(operation);
        drop(broker);
        actor.reopen(&committed);
        assert!(actor.message(delivery.sequence).is_none());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn retired_actual_lock_loss_retains_the_refusal_and_drains_both_registry_removals() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        let delivery = actor.receive();
        let lock = delivery.lock.unwrap();
        let management = ConnectionManagement::new();
        register_lock(&management, &actor, &delivery).await;
        actor.clock.set(lock.locked_until.as_millis());
        assert_eq!(
            actor.intent(CommandKind::ExpireLocks),
            CommandOutcome::LocksExpired {
                returned_to_ready: 1,
                dead_lettered: 0
            },
        );
        let before = actor.snapshot();
        let broker = actor.request_broker();
        let message = request(
            RENEW_LOCK_OPERATION,
            true,
            [(LOCK_TOKENS, token_value(lock.token))],
        );
        let mut operation = PendingOperation::new(
            process_request(
                &message,
                MessageId::Ulong(87),
                &actor.namespace,
                &actor.entity,
                &broker,
                &management,
                None,
            ),
            broker.control(),
        );
        let raw = paused_broker_result(&mut operation, &actor).await;
        let held = management.request_response_deliveries.write().await;
        actor.result_gate.release();
        assert_eq!(returned_while_pending(&mut operation, &actor).await, raw);
        let rejection = BrokerRejection::Refused(domain::BrokerError::MessageNotLocked {
            sequence: delivery.sequence,
        });
        assert_eq!(raw, Err(rejection.clone()));
        operation.retire();
        let mut observation = Box::pin(operation.observe());
        pending_once(observation.as_mut()).await;
        drop(observation);
        assert!(operation.take_packet().is_none());
        drop(held);
        let response = take_response(&mut operation).await;
        assert_eq!(response.correlation_id, MessageId::Ulong(87));
        assert_eq!(response.status_code, 410);
        assert_eq!(response.status_description, rejection.to_string());
        assert_eq!(response.error_condition, Some(crate::MESSAGE_LOCK_LOST));
        assert_eq!(response.tracking_id.as_deref(), Some("original-tracking"));
        assert_eq!(response.body, Value::Null);
        assert!(
            management
                .request_response_delivery(&actor.entity, lock.token)
                .await
                .is_none()
        );
        assert!(
            management
                .managed_delivery(&actor.entity, Some(LINK), lock.token)
                .await
                .is_none()
        );
        actor.assert_one(
            CommandKind::RenewLock {
                sequence: delivery.sequence,
                lock_token: lock.token,
                lock_duration_millis: None,
            },
            raw,
        );
        assert_eq!(actor.snapshot(), before);
        drop(operation);
        drop(broker);
        actor.reopen(&before);
        assert_eq!(
            actor.message(delivery.sequence).unwrap().state,
            MessageState::Ready
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_request_detach_drains_begun_session_mutation_without_a_new_acknowledgement() {
    for durable in [false, true] {
        for retire_after_apply in [false, true] {
            let mut actor = Actor::new(durable, true);
            let hold = actor.accept();
            let management = ConnectionManagement::new();
            install_hold(&management, &actor, hold.clone()).await;
            let (_route, mut responses) = management
                .register_reply_route("mutation-reply".to_owned())
                .await;
            actor.gate.arm(
                domain::keys::session(&actor.namespace, &actor.entity, &hold.session_id),
                true,
            );
            let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
            let LinkEndpoint::Receiver(receiver) = endpoint else {
                panic!("an actual request-link receiver");
            };
            let mut message = state_request();
            message.properties = Some(Properties {
                message_id: Some(MessageId::Ulong(87)),
                reply_to: Some("mutation-reply".to_owned()),
                ..Default::default()
            });
            wire.request_message(&message).await;
            wire.barrier().await;
            let mut serving = Box::pin(serve_management_requests(
                receiver,
                actor.namespace.clone(),
                actor.entity.clone(),
                actor.actual.as_ref().unwrap().clone(),
                Arc::clone(&management),
                None,
            ));
            pending_once(serving.as_mut()).await;
            actor.gate.reached(false).await;
            assert_eq!(actor.submissions.lock().unwrap().len(), 1);
            assert!(actor.submissions.lock().unwrap()[0].polled);
            assert!(actor.submissions.lock().unwrap()[0].result.is_none());
            if retire_after_apply {
                actor.gate.release(false);
                actor.gate.reached(true).await;
                assert_eq!(actor.session().state, STATE);
            } else {
                assert!(actor.session().state.is_empty());
            }

            // Detach is observed by the real native owner while the original
            // command cannot return. Serving must retain that command to drain.
            wire.detach().await;
            pending_once(serving.as_mut()).await;
            wire.barrier().await;
            wire.no_frame_yet().await;
            if !retire_after_apply {
                actor.gate.release(false);
                actor.gate.reached(true).await;
            }
            assert_eq!(actor.gate.progress.lock().unwrap().commits, 1);
            actor.gate.release(true);
            timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
            wire.barrier().await;
            wire.no_frame_yet().await;
            assert!(matches!(
                responses.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            actor.assert_one(
                CommandKind::SetSessionState {
                    session: hold.clone(),
                    state: STATE.to_vec(),
                },
                Ok(CommandOutcome::SessionStateSet),
            );
            assert_eq!(actor.gate.progress.lock().unwrap().commits, 1);
            let committed = actor.snapshot();
            drop(serving);
            wire.stop().await;
            actor.reopen(&committed);
            assert_eq!(actor.session().state, STATE);
            assert_eq!(actor.session().lock.unwrap().token, hold.token);
        }
    }
}
