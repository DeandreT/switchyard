//! Actual owner-driven retirement, bounded progress, and error conservation.

use domain::{
    BrokerError, CommandKind, CommandOutcome, Delivery, EntityPath, MessageRecord, MessageState,
    NamespaceName, QueueConfig, ReceiveMode, ScheduledMessage, SequenceNumber, SessionHold,
    SessionId, StateMachine, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, keys,
};
use server::{
    Broker, BrokerHandle, LocalProposer, MAX_ROUNDS_PER_INDEX, ManualClock, ProposeError,
    SubmitError, SweepReport, TimerWorker,
};
use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use storage::{Mutation, StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Scan = (Vec<u8>, Vec<u8>, usize);

#[derive(Default)]
struct Observed {
    scans: Mutex<Vec<Scan>>,
    applies: AtomicUsize,
    commit_then_error: AtomicBool,
}

#[derive(Clone)]
struct ObservedStore<S> {
    inner: S,
    observed: Arc<Observed>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let retirement = batch.mutations().iter().any(|mutation| {
            matches!(mutation,
            Mutation::Delete { key } if key.first() == Some(&0x13))
        });
        self.observed.applies.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)?;
        if retirement
            && self
                .observed
                .commit_then_error
                .swap(false, Ordering::SeqCst)
        {
            return Err(StorageError::Backend {
                operation: "retirement commit",
                detail: "committed before reporting an error".into(),
            });
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        if matches!(prefix.first(), Some(0x14 | 0x15)) {
            let mut scans = self.observed.scans.lock().expect("tracking scans");
            assert!(scans.len() < 4_096);
            scans.push((prefix.to_vec(), start.to_vec(), limit));
        }
        self.inner.scan_from(prefix, start, limit)
    }
}

struct Node<P: StoreProvider> {
    store: ObservedStore<P::Store>,
    broker: Broker,
    clock: ManualClock,
    namespace: NamespaceName,
    entity: EntityPath,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    fn new(provider: P) -> TestResult<Self> {
        let store = ObservedStore {
            inner: provider.open()?,
            observed: Arc::default(),
        };
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let node = Self {
            store,
            broker,
            clock,
            namespace: NamespaceName::new("tenant")?,
            entity: EntityPath::new("orders")?,
            _provider: provider,
        };
        node.submit(CommandKind::CreateQueue {
            config: QueueConfig {
                requires_session: true,
                lock_duration_millis: 300_000,
                ..QueueConfig::default()
            },
        })?;
        Ok(node)
    }

    fn handle(&self) -> BrokerHandle {
        self.broker.handle()
    }
    fn machine(&self) -> StateMachine<ObservedStore<P::Store>> {
        StateMachine::new(self.store.clone())
    }
    fn submit(&self, kind: CommandKind) -> TestResult<CommandOutcome> {
        Ok(self
            .handle()
            .submit_blocking(self.namespace.clone(), self.entity.clone(), kind)?)
    }
    fn send(&self, id: &SessionId, name: &str, ttl: Option<u64>) -> TestResult<SequenceNumber> {
        let CommandOutcome::Sent { sequence } = self.submit(CommandKind::Send {
            message_id: name.into(),
            body: name.as_bytes().to_vec(),
            time_to_live_millis: ttl,
            session_id: Some(id.clone()),
        })?
        else {
            panic!("actual send")
        };
        Ok(sequence)
    }
    fn accept(&self, id: &SessionId, duration: u64) -> TestResult<SessionHold> {
        let CommandOutcome::SessionAccepted(Some(accepted)) =
            self.submit(CommandKind::AcceptSession {
                session_id: Some(id.clone()),
                lock_duration_millis: Some(duration),
            })?
        else {
            panic!("actual named session")
        };
        Ok(accepted.hold())
    }
    fn receive(&self, hold: &SessionHold) -> TestResult<Delivery> {
        let CommandOutcome::Received(Some(delivery)) = self.submit(CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(300_000),
            session: Some(hold.clone()),
        })?
        else {
            panic!("actual owned receive")
        };
        Ok(delivery)
    }
    fn release(&self, hold: &SessionHold) -> TestResult {
        assert_eq!(
            self.submit(CommandKind::ReleaseSession {
                session: hold.clone()
            })?,
            CommandOutcome::SessionReleased
        );
        Ok(())
    }
    fn record(&self, sequence: SequenceNumber) -> TestResult<MessageRecord> {
        Ok(self
            .machine()
            .message(&self.namespace, &self.entity, sequence)?
            .expect("actual message"))
    }
    fn scans(&self, prefix: &[u8]) -> Vec<Scan> {
        self.store
            .observed
            .scans
            .lock()
            .expect("tracking scans")
            .iter()
            .filter(|(seen, _, _)| seen == prefix)
            .cloned()
            .collect()
    }
    fn clear_scans(&self) {
        self.store
            .observed
            .scans
            .lock()
            .expect("tracking scans")
            .clear();
    }
    fn pending(&self, id: &SessionId) -> TestResult {
        let before = self.store.snapshot()?;
        assert!(
            matches!(self.handle().submit_blocking(self.namespace.clone(), self.entity.clone(),
            CommandKind::AcceptSession { session_id: Some(id.clone()), lock_duration_millis: None }),
            Err(SubmitError::Propose(ProposeError::Broker(BrokerError::SessionTakeoverPending { session_id })))
                if session_id == *id)
        );
        assert_eq!(self.store.snapshot()?, before);
        Ok(())
    }
}

fn ready_without_mutating_original(original: &MessageRecord, actual: &MessageRecord) {
    let mut expected = original.clone();
    expected.state = MessageState::Ready;
    assert_eq!(actual, &expected);
}

fn sweep_retires_lost_owned_locks_after_session_expiry<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let released = SessionId::new("released")?;
    let expired = SessionId::new("expired")?;
    let live = SessionId::new("live")?;
    let sequences = [
        node.send(&released, "released-message", None)?,
        node.send(&expired, "expired-message", None)?,
        node.send(&live, "live-message", None)?,
    ];
    let holds = [
        node.accept(&released, 100)?,
        node.accept(&expired, 100)?,
        node.accept(&live, 300_000)?,
    ];
    for hold in &holds {
        node.receive(hold)?;
    }
    node.submit(CommandKind::SetSessionState {
        session: holds[0].clone(),
        state: vec![0, 255, 7],
    })?;
    node.submit(CommandKind::SetSessionState {
        session: holds[1].clone(),
        state: vec![8, 0, 9],
    })?;
    let originals: Vec<_> = sequences
        .iter()
        .map(|sequence| node.record(*sequence))
        .collect::<TestResult<_>>()?;
    let live_before = node
        .machine()
        .session(&node.namespace, &node.entity, &live)?;
    let counters = node
        .store
        .get(&keys::queue_counters(&node.namespace, &node.entity))?;
    node.release(&holds[0])?;
    node.pending(&released)?;
    node.clock.set(1_100);
    let handle = node.handle();
    let timer = TimerWorker::new(&handle);
    assert_eq!(
        timer.sweep_once()?,
        SweepReport {
            queues_swept: 2,
            locks_returned_to_ready: 2,
            sessions_released: 1,
            ..SweepReport::default()
        }
    );
    for index in 0..2 {
        ready_without_mutating_original(&originals[index], &node.record(sequences[index])?);
        let id = &holds[index].session_id;
        for key in [
            keys::session_message_lock_reverse(&node.namespace, &node.entity, sequences[index]),
            keys::session_message_lock_forward(
                &node.namespace,
                &node.entity,
                id,
                Some(holds[index].token),
                sequences[index],
            ),
            keys::session_message_lock_summary(&node.namespace, &node.entity, id),
        ] {
            assert!(node.store.get(&key)?.is_none());
        }
    }
    assert_eq!(node.record(sequences[2])?, originals[2]);
    assert_eq!(
        node.machine()
            .session(&node.namespace, &node.entity, &live)?,
        live_before
    );
    assert_eq!(
        node.store
            .get(&keys::queue_counters(&node.namespace, &node.entity))?,
        counters
    );
    assert_eq!(
        node.machine()
            .session(&node.namespace, &node.entity, &released)?
            .expect("released state")
            .state,
        vec![0, 255, 7]
    );
    assert_eq!(
        node.machine()
            .session(&node.namespace, &node.entity, &expired)?
            .expect("expired state")
            .state,
        vec![8, 0, 9]
    );
    let fresh = node.accept(&released, 300_000)?;
    assert_ne!(fresh.token, holds[0].token);
    let delivered = node.receive(&fresh)?;
    assert_eq!(delivered.sequence, sequences[0]);
    assert_eq!(delivered.body, originals[0].body);
    assert_eq!(delivered.delivery_count, originals[0].delivery_count + 1);
    Ok(())
}

fn finite_rounds_follow_zero_progress_pages_and_bounded_cached_cursor<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let prefix = keys::session_message_lock_summary_prefix(&node.namespace, &node.entity);
    let live_count = domain::MAX_SESSION_RETIREMENT_GROUPS * MAX_ROUNDS_PER_INDEX;
    for index in 0..live_count {
        let id = SessionId::new(format!("s{index:04}"))?;
        node.send(&id, &format!("live-{index}"), None)?;
        let hold = node.accept(&id, 300_000)?;
        node.receive(&hold)?;
    }
    let last = SessionId::new(format!("s{live_count:04}"))?;
    let sequence = node.send(&last, "released-tail", None)?;
    let original = node.accept(&last, 300_000)?;
    node.receive(&original)?;
    node.release(&original)?;
    let locked = node.record(sequence)?;
    let handle = node.handle();
    let timer = TimerWorker::new(&handle);
    node.clear_scans();
    let before = node.store.snapshot()?;
    assert!(timer.sweep_once()?.is_idle());
    assert_eq!(node.store.snapshot()?, before);
    let scans = node.scans(&prefix);
    assert_eq!(scans.len(), live_count);
    assert!(scans.iter().all(|(_, _, limit)| *limit == 1));
    assert_eq!(scans[0].1, prefix);
    assert_eq!(node.record(sequence)?, locked);
    node.clear_scans();
    assert_eq!(timer.sweep_once()?.locks_returned_to_ready, 1);
    let scans = node.scans(&prefix);
    let previous = SessionId::new(format!("s{:04}", live_count - 1))?;
    let mut expected_start =
        keys::session_message_lock_summary(&node.namespace, &node.entity, &previous);
    expected_start.push(0);
    assert_eq!(
        scans.first().expect("resumed summary scan").1,
        expected_start
    );
    ready_without_mutating_original(&locked, &node.record(sequence)?);
    drop(timer);
    let another = SessionId::new(format!("s{:04}", live_count + 1))?;
    let tail = node.send(&another, "restart-tail", None)?;
    let hold = node.accept(&another, 300_000)?;
    node.receive(&hold)?;
    node.release(&hold)?;
    let tail_before = node.record(tail)?;
    let restarted = TimerWorker::new(&handle);
    node.clear_scans();
    assert!(restarted.sweep_once()?.is_idle());
    assert_eq!(node.scans(&prefix).len(), live_count);
    assert_eq!(node.scans(&prefix)[0].1, prefix);
    assert_eq!(node.record(tail)?, tail_before);
    assert_eq!(restarted.sweep_once()?.locks_returned_to_ready, 1);
    ready_without_mutating_original(&tail_before, &node.record(tail)?);
    Ok(())
}

fn late_retirement_error_preserves_original_outcome_and_other_family<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let id = SessionId::new("lost")?;
    let mut sequences = Vec::new();
    for index in 0..=domain::MAX_SESSION_RETIREMENT_ROWS {
        sequences.push(node.send(&id, &format!("backlog-{index}"), None)?);
    }
    let hold = node.accept(&id, 300_000)?;
    for _ in &sequences {
        node.receive(&hold)?;
    }
    node.release(&hold)?;
    let topic = EntityPath::new("events")?;
    node.handle().submit_blocking(
        node.namespace.clone(),
        topic.clone(),
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let sub = SubscriptionName::new("sink")?;
    node.handle().submit_blocking(
        node.namespace.clone(),
        topic.clone(),
        CommandKind::CreateSubscription {
            name: sub.clone(),
            config: SubscriptionConfig::default(),
        },
    )?;
    node.handle().submit_blocking(
        node.namespace.clone(),
        topic.clone(),
        CommandKind::Schedule {
            messages: vec![ScheduledMessage {
                message_id: "topic-after-failure".into(),
                body: vec![9],
                time_to_live_millis: None,
                session_id: None,
                enqueue_at: Timestamp::from_millis(1_001),
            }],
        },
    )?;
    let last = *sequences.last().expect("33 actual rows");
    let last_record = node.record(last)?;
    let MessageState::Locked { locked_until, .. } = &last_record.state else {
        panic!("actual lock")
    };
    let lock_key = keys::lock(&node.namespace, &node.entity, *locked_until, last);
    node.store
        .inner
        .apply(WriteBatch::default().put(lock_key.clone(), vec![1]))?;
    node.clock.set(1_001);
    let handle = node.handle();
    let timer = TimerWorker::new(&handle);
    assert!(matches!(
        timer.sweep_once(),
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::MalformedIndexKey
        )))
    ));
    for sequence in &sequences[..domain::MAX_SESSION_RETIREMENT_ROWS] {
        assert!(matches!(node.record(*sequence)?.state, MessageState::Ready));
    }
    assert_eq!(node.record(last)?, last_record);
    assert_eq!(
        node.machine()
            .ready_sequences(&node.namespace, &topic.subscription(&sub)?, 2)?
            .len(),
        1
    );
    node.store
        .inner
        .apply(WriteBatch::default().put(lock_key, Vec::new()))?;
    node.clear_scans();
    // Discovery advances past the failed queue before wrapping to it again.
    assert!(timer.sweep_once()?.is_idle());
    let scans_before = node.store.observed.scans.lock().expect("scans").len();
    assert_eq!(scans_before, 0);
    assert_eq!(timer.sweep_once()?.locks_returned_to_ready, 1);
    let forward_prefix =
        keys::session_message_lock_forward_prefix(&node.namespace, &node.entity, &id);
    let first = node
        .store
        .observed
        .scans
        .lock()
        .expect("scans")
        .iter()
        .find(|(prefix, _, _)| prefix.starts_with(&forward_prefix))
        .cloned()
        .expect("resumed owned scan");
    let mut expected = keys::session_message_lock_forward(
        &node.namespace,
        &node.entity,
        &id,
        Some(hold.token),
        sequences[domain::MAX_SESSION_RETIREMENT_ROWS - 1],
    );
    expected.push(0);
    assert_eq!(first.1, expected);
    assert!(matches!(node.record(last)?.state, MessageState::Ready));

    // A storage error may follow the actual commit; it must not publish an assumed result.
    let unknown = SessionId::new("unknown")?;
    let unknown_sequence = node.send(&unknown, "unknown-decision", None)?;
    let unknown_hold = node.accept(&unknown, 300_000)?;
    node.receive(&unknown_hold)?;
    node.release(&unknown_hold)?;
    node.store
        .observed
        .commit_then_error
        .store(true, Ordering::SeqCst);
    assert!(matches!(
        timer.sweep_once(),
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::Storage(StorageError::Backend {
                operation: "retirement commit",
                ..
            })
        )))
    ));
    assert!(matches!(
        node.record(unknown_sequence)?.state,
        MessageState::Ready
    ));
    assert!(timer.sweep_once()?.is_idle());
    assert!(timer.sweep_once()?.is_idle());
    assert!(
        node.store
            .get(&keys::session_message_lock_summary(
                &node.namespace,
                &node.entity,
                &unknown
            ))?
            .is_none()
    );

    let capped = SessionId::new("capped")?;
    let capped_sequence = node.send(&capped, "capped-state", None)?;
    let capped_hold = node.accept(&capped, 300_000)?;
    node.receive(&capped_hold)?;
    node.release(&capped_hold)?;
    let capped_message = node.record(capped_sequence)?;
    let state_key = keys::session(&node.namespace, &node.entity, &capped);
    let original_state = node
        .store
        .get(&state_key)?
        .expect("retained actual session");
    let mut oversized = node
        .machine()
        .session(&node.namespace, &node.entity, &capped)?
        .expect("session");
    // Private store setup exercises returned-byte admission, not public state-write admission.
    oversized.state = vec![0; domain::MAX_SESSION_RETIREMENT_READ_VALUE_BYTES];
    node.store
        .inner
        .apply(WriteBatch::default().put(state_key.clone(), domain::codec::encode(&oversized)?))?;
    drop(oversized);
    let applies = node.store.observed.applies.load(Ordering::SeqCst);
    let clock = node.machine().last_applied_time()?;
    assert!(matches!(
        timer.sweep_once(),
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::SessionRetirementTooLarge {
                limit: domain::SessionRetirementLimit::ReadValueBytes,
                maximum: domain::MAX_SESSION_RETIREMENT_READ_VALUE_BYTES,
            }
        )))
    ));
    assert_eq!(node.store.observed.applies.load(Ordering::SeqCst), applies);
    assert_eq!(node.machine().last_applied_time()?, clock);
    assert_eq!(node.record(capped_sequence)?, capped_message);
    node.store
        .inner
        .apply(WriteBatch::default().put(state_key, original_state))?;
    assert!(timer.sweep_once()?.is_idle());
    assert_eq!(timer.sweep_once()?.locks_returned_to_ready, 1);
    Ok(())
}

fn unowned_remainder_keeps_takeover_pending_and_idle_sweeps_write_nothing<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let id = SessionId::new("mixed")?;
    let owned_sequence = node.send(&id, "owned", None)?;
    let unowned_sequence = node.send(&id, "unowned", None)?;
    let hold = node.accept(&id, 300_000)?;
    let owned = node.receive(&hold)?;
    let deferred = node.receive(&hold)?;
    node.submit(CommandKind::Defer {
        sequence: deferred.sequence,
        lock_token: deferred.lock.expect("actual lock").token,
    })?;
    node.release(&hold)?;
    let CommandOutcome::DeferredReceived(deliveries) =
        node.submit(CommandKind::ReceiveDeferred {
            sequences: vec![unowned_sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(300_000),
            session_id: Some(id.clone()),
        })?
    else {
        panic!("actual trusted unowned receive")
    };
    assert_eq!(deliveries.len(), 1);
    let unowned = node.record(unowned_sequence)?;
    let unowned_key = keys::session_message_lock_forward(
        &node.namespace,
        &node.entity,
        &id,
        None,
        unowned_sequence,
    );
    let unowned_bytes = node.store.get(&unowned_key)?.expect("actual unowned row");
    let counters = node
        .store
        .get(&keys::queue_counters(&node.namespace, &node.entity))?;
    let session = node.machine().session(&node.namespace, &node.entity, &id)?;
    let handle = node.handle();
    let timer = TimerWorker::new(&handle);
    assert_eq!(timer.sweep_once()?.locks_returned_to_ready, 1);
    assert_eq!(
        node.record(owned_sequence)?.delivery_count,
        owned.delivery_count
    );
    assert_eq!(node.record(unowned_sequence)?, unowned);
    assert_eq!(node.store.get(&unowned_key)?, Some(unowned_bytes));
    assert_eq!(
        node.machine().session(&node.namespace, &node.entity, &id)?,
        session
    );
    assert_eq!(
        node.store
            .get(&keys::queue_counters(&node.namespace, &node.entity))?,
        counters
    );
    node.pending(&id)?;
    let ordinary = EntityPath::new("ordinary")?;
    handle.submit_blocking(
        node.namespace.clone(),
        ordinary.clone(),
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let before = node.store.snapshot()?;
    let applies = node.store.observed.applies.load(Ordering::SeqCst);
    assert!(timer.sweep_once()?.is_idle());
    assert_eq!(node.store.snapshot()?, before);
    assert_eq!(node.store.observed.applies.load(Ordering::SeqCst), applies);
    for entity in [ordinary.clone(), ordinary.dead_letter_queue()?] {
        assert!(
            matches!(handle.submit_blocking(node.namespace.clone(), entity,
            CommandKind::RetireSessionGenerationPage { after: None })?,
            CommandOutcome::SessionRetired(outcome) if outcome.returned_to_ready == 0 && matches!(outcome.page, domain::SessionRetirementPage::End))
        );
    }
    assert!(matches!(
        handle.submit_blocking(
            node.namespace.clone(),
            EntityPath::new("missing")?,
            CommandKind::RetireSessionGenerationPage { after: None }
        ),
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::QueueNotFound
        )))
    ));
    let MessageState::Locked { locked_until, .. } = unowned.state else {
        panic!("actual unowned deadline")
    };
    node.clock.set(locked_until.as_millis());
    let report = timer.sweep_once()?;
    assert_eq!(report.locks_returned_to_ready, 1);
    assert_eq!(report.sessions_released, 0);
    assert!(matches!(
        node.record(unowned_sequence)?.state,
        MessageState::Ready
    ));
    assert!(node.store.get(&unowned_key)?.is_none());
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend!(
    sweep_retires_lost_owned_locks_after_session_expiry,
    finite_rounds_follow_zero_progress_pages_and_bounded_cached_cursor,
    late_retirement_error_preserves_original_outcome_and_other_family,
    unowned_remainder_keeps_takeover_pending_and_idle_sweeps_write_nothing,
);
