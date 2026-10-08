//! Canonical elapsed session-lock rows are validated before the atomic batch commits.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use domain::{
    AcceptedSession, BrokerError, CodecError, Command, CommandKind, CommandOutcome, EntityPath,
    LockToken, NamespaceName, QueueConfig, ReceiveMode, SessionId, SessionLock, SessionRecord,
    StateMachine, TIMER_SCAN_LIMIT, Timestamp, codec, keys,
};
use storage::{StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Scan = (Vec<u8>, Vec<u8>, usize, usize);
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Clone)]
struct ObservedStore<S> {
    inner: S,
    applies: Arc<AtomicUsize>,
    gets: Arc<Mutex<Vec<Vec<u8>>>>,
    scans: Arc<Mutex<Vec<Scan>>>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.gets
            .lock()
            .expect("get observations")
            .push(key.to_vec());
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
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
        let rows = self.inner.scan_from(prefix, start, limit)?;
        self.scans.lock().expect("scan observations").push((
            prefix.to_vec(),
            start.to_vec(),
            limit,
            rows.len(),
        ));
        Ok(rows)
    }
}

struct Node<P: StoreProvider> {
    machine: StateMachine<ObservedStore<P::Store>>,
    namespace: NamespaceName,
    entity: EntityPath,
    provider: P,
}

impl<P: StoreProvider> Node<P> {
    fn new(provider: P) -> TestResult<Self> {
        let node = Self {
            machine: StateMachine::new(ObservedStore {
                inner: provider.open()?,
                applies: Arc::new(AtomicUsize::new(0)),
                gets: Arc::new(Mutex::new(Vec::new())),
                scans: Arc::new(Mutex::new(Vec::new())),
            }),
            namespace: NamespaceName::new("tenant")?,
            entity: EntityPath::new("orders")?,
            provider,
        };
        node.at(
            0,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_session: true,
                    ..QueueConfig::default()
                },
            },
        )?;
        Ok(node)
    }

    fn restart(self) -> TestResult<Self> {
        let Self {
            machine,
            namespace,
            entity,
            provider,
        } = self;
        let applies = machine.store().applies.clone();
        let gets = machine.store().gets.clone();
        let scans = machine.store().scans.clone();
        drop(machine);
        Ok(Self {
            machine: StateMachine::new(ObservedStore {
                inner: provider.open()?,
                applies,
                gets,
                scans,
            }),
            namespace,
            entity,
            provider,
        })
    }

    fn at(&self, at: u64, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.at_entity(&self.entity, at, kind)
    }

    fn at_entity(
        &self,
        entity: &EntityPath,
        at: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        ))
    }

    fn accept(&self, at: u64, name: &str, duration: u64) -> TestResult<AcceptedSession> {
        let CommandOutcome::SessionAccepted(Some(accepted)) = self.at(
            at,
            CommandKind::AcceptSession {
                session_id: Some(SessionId::new(name)?),
                lock_duration_millis: Some(duration),
            },
        )?
        else {
            panic!("named session was not accepted")
        };
        Ok(accepted)
    }

    fn state(&self, at: u64, accepted: &AcceptedSession, value: &[u8]) -> TestResult {
        assert_eq!(
            self.at(
                at,
                CommandKind::SetSessionState {
                    session: accepted.hold(),
                    state: value.to_vec(),
                }
            )?,
            CommandOutcome::SessionStateSet
        );
        Ok(())
    }

    fn message_lock(&self, at: u64, accepted: &AcceptedSession) -> TestResult {
        self.at(
            at,
            CommandKind::Send {
                message_id: accepted.session_id.as_str().to_owned(),
                body: vec![1, 2, 3],
                time_to_live_millis: None,
                session_id: Some(accepted.session_id.clone()),
            },
        )?;
        let CommandOutcome::Received(Some(delivery)) = self.at(
            at,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: Some(100),
                session: Some(accepted.hold()),
            },
        )?
        else {
            panic!("original held message was not delivered")
        };
        assert_eq!(delivery.session_id.as_ref(), Some(&accepted.session_id));
        assert!(delivery.lock.is_some());
        Ok(())
    }

    fn session_key(&self, session: &SessionId) -> Vec<u8> {
        keys::session(&self.namespace, &self.entity, session)
    }

    fn index_key(&self, session: &SessionId, deadline: u64) -> Vec<u8> {
        keys::session_lock(
            &self.namespace,
            &self.entity,
            Timestamp::from_millis(deadline),
            session,
        )
    }

    fn record(&self, session: &SessionId) -> TestResult<SessionRecord> {
        Ok(self
            .machine
            .session(&self.namespace, &self.entity, session)?
            .expect("stored session"))
    }

    fn tracking(&self) -> TestResult<Vec<Entries>> {
        Ok([
            keys::session_message_lock_reverse_prefix(&self.namespace, &self.entity),
            keys::session_message_lock_forward_prefix(
                &self.namespace,
                &self.entity,
                &SessionId::new("A")?,
            ),
            keys::session_message_lock_forward_prefix(
                &self.namespace,
                &self.entity,
                &SessionId::new("B")?,
            ),
            keys::session_message_lock_summary_prefix(&self.namespace, &self.entity),
        ]
        .into_iter()
        .map(|prefix| self.machine.store().scan_prefix(&prefix, 1_024))
        .collect::<Result<_, _>>()?)
    }

    fn raw(&self, batch: WriteBatch) -> TestResult {
        self.machine.store().inner.apply(batch)?;
        Ok(())
    }

    fn reset(&self) {
        self.machine.store().applies.store(0, Ordering::SeqCst);
        self.machine
            .store()
            .gets
            .lock()
            .expect("get observations")
            .clear();
        self.machine
            .store()
            .scans
            .lock()
            .expect("scan observations")
            .clear();
    }

    fn assert_scan(&self, metadata_probes: usize) {
        let prefix = keys::session_lock_prefix(&self.namespace, &self.entity);
        let mut mode_prefix = keys::topic_mode(&self.namespace, &self.entity);
        assert_eq!(mode_prefix.pop(), Some(0));
        mode_prefix.extend_from_slice(b"/subscriptions/");
        let calls = self
            .machine
            .store()
            .scans
            .lock()
            .expect("scan observations")
            .clone();
        let (metadata, runtime): (Vec<_>, Vec<_>) = calls
            .iter()
            .cloned()
            .partition(|scan| scan.0 == mode_prefix);
        assert_eq!(
            metadata,
            vec![(mode_prefix.clone(), mode_prefix, 1, 0); metadata_probes]
        );
        assert_eq!(&calls[runtime.len()..], metadata.as_slice());
        assert_eq!(
            runtime
                .into_iter()
                .map(|(prefix, start, limit, _)| (prefix, start, limit))
                .collect::<Vec<_>>(),
            vec![(prefix.clone(), prefix, TIMER_SCAN_LIMIT)]
        );
    }

    fn session_gets(&self) -> usize {
        let tag = self.session_key(&SessionId::new("A").expect("valid observation ID"))[0];
        self.machine
            .store()
            .gets
            .lock()
            .expect("get observations")
            .iter()
            .filter(|key| {
                key.first() == Some(&tag)
                    && keys::entity_scope_parts(key)
                        == Some((self.namespace.as_str(), self.entity.as_str()))
            })
            .count()
    }

    fn idle(&self, at: u64) -> TestResult {
        let snapshot = self.machine.store().snapshot()?;
        let clock = self.machine.last_applied_time()?;
        self.reset();
        assert_eq!(
            self.at(at, CommandKind::ExpireSessionLocks)?,
            CommandOutcome::SessionLocksExpired { released: 0 }
        );
        self.assert_scan(1);
        assert_eq!(self.session_gets(), 0);
        assert_eq!(self.machine.store().applies.load(Ordering::SeqCst), 0);
        assert_eq!(self.machine.store().snapshot()?, snapshot);
        assert_eq!(self.machine.last_applied_time()?, clock);
        Ok(())
    }

    fn refuses(&self, at: u64, expected: BrokerError) -> TestResult {
        let snapshot = self.machine.store().snapshot()?;
        let clock = self.machine.last_applied_time()?;
        self.reset();
        assert_eq!(self.at(at, CommandKind::ExpireSessionLocks), Err(expected));
        assert_eq!(self.machine.store().applies.load(Ordering::SeqCst), 0);
        self.assert_scan(0);
        assert_eq!(self.machine.store().snapshot()?, snapshot);
        assert_eq!(self.machine.last_applied_time()?, clock);
        Ok(())
    }
}

fn canonical_expiry_preserves_state_and_scoped_message_locks<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let accepted = node.accept(10, "A", 10)?;
    let future = node.accept(10, "future", 100)?;
    node.state(11, &accepted, b"opaque state")?;
    node.message_lock(12, &accepted)?;
    node.raw(WriteBatch::default().put(node.session_key(&future.session_id), vec![255]))?;
    let tracking = node.tracking()?;
    let counters_key = keys::queue_counters(&node.namespace, &node.entity);
    let counters = node
        .machine
        .store()
        .get(&counters_key)?
        .expect("actual queue counters");
    let message_prefix = keys::message_prefix(&node.namespace, &node.entity);
    let messages = node.machine.store().scan_prefix(&message_prefix, 1_024)?;
    let before = node.machine.store().snapshot()?;
    let clock = node.machine.last_applied_time()?;
    node.reset();
    assert_eq!(
        node.at(19, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 0 }
    );
    node.assert_scan(1);
    assert_eq!(node.session_gets(), 0);
    assert_eq!(node.machine.store().applies.load(Ordering::SeqCst), 0);
    assert_eq!(node.machine.store().snapshot()?, before);
    assert_eq!(node.machine.last_applied_time()?, clock);

    node.reset();
    assert_eq!(
        node.at(20, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 1 }
    );
    node.assert_scan(1);
    assert_eq!(node.session_gets(), 1);
    assert_eq!(node.machine.store().applies.load(Ordering::SeqCst), 1);
    assert_eq!(
        node.record(&accepted.session_id)?,
        SessionRecord {
            lock: None,
            state: b"opaque state".to_vec()
        }
    );
    assert_eq!(
        node.machine
            .store()
            .get(&node.index_key(&accepted.session_id, 20))?,
        None
    );
    assert_eq!(
        node.machine
            .store()
            .get(&node.index_key(&future.session_id, 110))?,
        Some(Vec::new())
    );
    assert_eq!(node.machine.store().get(&counters_key)?, Some(counters));
    assert_eq!(
        node.machine.store().scan_prefix(&message_prefix, 1_024)?,
        messages
    );
    assert_eq!(node.tracking()?, tracking);

    let plain = EntityPath::new("plain")?;
    node.at_entity(
        &plain,
        20,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let shadow = node.entity.dead_letter_queue()?;
    let missing = EntityPath::new("missing")?;
    for entity in [&node.entity, &plain, &shadow, &missing] {
        let snapshot = node.machine.store().snapshot()?;
        let clock = node.machine.last_applied_time()?;
        node.reset();
        assert_eq!(
            node.at_entity(entity, 21, CommandKind::ExpireSessionLocks)?,
            CommandOutcome::SessionLocksExpired { released: 0 }
        );
        assert_eq!(node.machine.store().applies.load(Ordering::SeqCst), 0);
        assert_eq!(node.session_gets(), 0);
        assert_eq!(node.machine.store().snapshot()?, snapshot);
        assert_eq!(node.machine.last_applied_time()?, clock);
    }
    Ok(())
}

fn stale_elapsed_index_cannot_clear_a_renewed_session_lock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let accepted = node.accept(10, "A", 10)?;
    node.state(11, &accepted, b"survives renewal")?;
    node.message_lock(12, &accepted)?;
    assert_eq!(
        node.at(
            15,
            CommandKind::RenewSessionLock {
                session: accepted.hold(),
                lock_duration_millis: Some(100)
            }
        )?,
        CommandOutcome::SessionLockRenewed {
            locked_until: Timestamp::from_millis(115)
        }
    );
    let old_key = node.index_key(&accepted.session_id, 20);
    assert_eq!(node.machine.store().get(&old_key)?, None);
    node.raw(WriteBatch::default().put(old_key.clone(), Vec::new()))?;
    node.refuses(20, BrokerError::MalformedIndexKey)?;
    assert_eq!(
        node.record(&accepted.session_id)?.lock,
        Some(SessionLock {
            token: accepted.lock.token,
            locked_until: Timestamp::from_millis(115)
        })
    );
    assert_eq!(
        node.at(
            20,
            CommandKind::GetSessionState {
                session: accepted.hold()
            }
        )?,
        CommandOutcome::SessionState(b"survives renewal".to_vec())
    );
    node.raw(WriteBatch::default().delete(old_key))?;
    let tracking = node.tracking()?;
    assert_eq!(
        node.at(115, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 1 }
    );
    assert_eq!(
        node.record(&accepted.session_id)?.state,
        b"survives renewal".to_vec()
    );
    assert_eq!(node.tracking()?, tracking);
    Ok(())
}

fn missing_absent_and_zero_token_records_refuse_expiry<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let accepted = node.accept(10, "A", 10)?;
    node.state(11, &accepted, b"original state")?;
    node.message_lock(12, &accepted)?;
    let key = node.session_key(&accepted.session_id);
    let original = node
        .machine
        .store()
        .get(&key)?
        .expect("original stored record");
    for damage in ["missing", "absent", "zero"] {
        let mut batch = WriteBatch::default();
        match damage {
            "missing" => batch.push_delete(key.clone()),
            "absent" => batch.push_put(
                key.clone(),
                codec::encode(&SessionRecord {
                    lock: None,
                    state: b"original state".to_vec(),
                })?,
            ),
            _ => batch.push_put(
                key.clone(),
                codec::encode(&SessionRecord {
                    lock: Some(SessionLock {
                        token: LockToken::new(0),
                        locked_until: accepted.lock.locked_until,
                    }),
                    state: b"original state".to_vec(),
                })?,
            ),
        }
        node.raw(batch)?;
        node.refuses(20, BrokerError::MalformedIndexKey)?;
        node.raw(WriteBatch::default().put(key.clone(), original.clone()))?;
    }
    assert_eq!(node.record(&accepted.session_id)?.lock, Some(accepted.lock));
    Ok(())
}

fn malformed_session_keys_and_values_preserve_error_priority<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let accepted = node.accept(10, "A", 10)?;
    let prefix = keys::session_lock_prefix(&node.namespace, &node.entity);
    let mut regressed_key = prefix.clone();
    regressed_key.extend_from_slice(&9_u64.to_be_bytes());
    regressed_key.push(255);
    node.raw(WriteBatch::default().put(regressed_key.clone(), Vec::new()))?;
    let snapshot = node.machine.store().snapshot()?;
    let clock = node.machine.last_applied_time()?;
    node.reset();
    assert_eq!(
        node.at(9, CommandKind::ExpireSessionLocks),
        Err(BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(10),
            proposed: Timestamp::from_millis(9),
        })
    );
    assert!(
        node.machine
            .store()
            .scans
            .lock()
            .expect("scan observations")
            .is_empty()
    );
    assert_eq!(node.session_gets(), 0);
    assert_eq!(node.machine.store().applies.load(Ordering::SeqCst), 0);
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    assert_eq!(node.machine.last_applied_time()?, clock);
    node.raw(WriteBatch::default().delete(regressed_key))?;
    for (suffix, expected) in [
        (vec![0; 7], BrokerError::MalformedIndexKey),
        (
            [19_u64.to_be_bytes().as_slice(), &[255]].concat(),
            BrokerError::MalformedIndexKey,
        ),
        (
            19_u64.to_be_bytes().to_vec(),
            BrokerError::Identifier(SessionId::new("").expect_err("empty ID")),
        ),
        (
            [19_u64.to_be_bytes().as_slice(), b"A\0bad"].concat(),
            BrokerError::Identifier(SessionId::new("A\0bad").expect_err("embedded separator")),
        ),
        (
            [19_u64.to_be_bytes().as_slice(), b"missing"].concat(),
            BrokerError::MalformedIndexKey,
        ),
    ] {
        let key = [prefix.as_slice(), suffix.as_slice()].concat();
        node.raw(WriteBatch::default().put(key.clone(), Vec::new()))?;
        if matches!(&expected, BrokerError::Identifier(_)) {
            node.idle(18)?;
        }
        node.refuses(19, expected)?;
        node.raw(WriteBatch::default().delete(key))?;
    }
    let index = node.index_key(&accepted.session_id, 20);
    let record_key = node.session_key(&accepted.session_id);
    let original = node
        .machine
        .store()
        .get(&record_key)?
        .expect("original record");
    node.raw(WriteBatch::default().put(index.clone(), vec![1]))?;
    node.refuses(20, BrokerError::MalformedIndexKey)?;
    for (raw, error) in [
        (Vec::new(), CodecError::EmptyEnvelope),
        (vec![255], CodecError::UnsupportedVersion { version: 255 }),
        (vec![codec::ACTIVE_VALUE_FORMAT, 255], CodecError::Decode),
    ] {
        node.raw(WriteBatch::default().put(record_key.clone(), raw))?;
        node.idle(19)?;
        node.refuses(20, BrokerError::Codec(error))?;
    }
    node.raw(
        WriteBatch::default()
            .put(record_key, original)
            .put(index, Vec::new()),
    )?;

    let neighbor = EntityPath::new("orders-neighbor")?;
    let foreign = keys::session_lock(
        &node.namespace,
        &neighbor,
        Timestamp::from_millis(19),
        &accepted.session_id,
    );
    node.raw(WriteBatch::default().put(foreign.clone(), vec![1]))?;
    assert_eq!(
        node.at(20, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 1 }
    );
    assert_eq!(node.machine.store().get(&foreign)?, Some(vec![1]));
    Ok(())
}

fn late_corruption_rolls_back_all_selected_session_clears<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let first = node.accept(10, "A", 10)?;
    let last = node.accept(10, "B", 10)?;
    node.state(11, &first, b"first state")?;
    node.state(11, &last, b"last state")?;
    node.message_lock(12, &first)?;
    node.message_lock(12, &last)?;
    let key = node.session_key(&last.session_id);
    let original = node
        .machine
        .store()
        .get(&key)?
        .expect("last original record");
    node.raw(WriteBatch::default().put(
        key.clone(),
        codec::encode(&SessionRecord {
            lock: None,
            state: b"last state".to_vec(),
        })?,
    ))?;
    node.refuses(20, BrokerError::MalformedIndexKey)?;
    assert_eq!(node.session_gets(), 2);
    assert_eq!(node.record(&first.session_id)?.lock, Some(first.lock));
    node.raw(WriteBatch::default().put(key, original))?;
    let tracking = node.tracking()?;
    assert_eq!(
        node.at(20, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 2 }
    );
    assert_eq!(
        node.record(&first.session_id)?,
        SessionRecord {
            lock: None,
            state: b"first state".to_vec()
        }
    );
    assert_eq!(
        node.record(&last.session_id)?,
        SessionRecord {
            lock: None,
            state: b"last state".to_vec()
        }
    );
    assert_eq!(node.tracking()?, tracking);
    Ok(())
}

fn bounded_session_expiry_resumes_after_restart_without_extra_scan<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let mut accepted = Vec::new();
    for index in 0..=TIMER_SCAN_LIMIT {
        accepted.push(node.accept(10, &format!("s-{index:03}"), 10)?);
    }
    for (index, session) in accepted.iter().enumerate() {
        node.state(11, session, &index.to_be_bytes())?;
    }
    let counters_key = keys::queue_counters(&node.namespace, &node.entity);
    let counters = node
        .machine
        .store()
        .get(&counters_key)?
        .expect("actual queue counters");
    node.reset();
    assert_eq!(
        node.at(20, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired {
            released: TIMER_SCAN_LIMIT as u32
        }
    );
    node.assert_scan(1);
    assert_eq!(node.session_gets(), TIMER_SCAN_LIMIT);
    assert_eq!(node.machine.store().applies.load(Ordering::SeqCst), 1);
    for (index, session) in accepted.iter().enumerate() {
        assert_eq!(
            node.record(&session.session_id)?,
            SessionRecord {
                lock: (index == TIMER_SCAN_LIMIT).then_some(session.lock),
                state: index.to_be_bytes().to_vec()
            }
        );
    }
    let snapshot = node.machine.store().snapshot()?;
    let node = node.restart()?;
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    node.reset();
    assert_eq!(
        node.at(20, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 1 }
    );
    node.assert_scan(1);
    assert_eq!(node.session_gets(), 1);
    assert_eq!(node.machine.store().applies.load(Ordering::SeqCst), 1);
    for (index, session) in accepted.iter().enumerate() {
        assert_eq!(
            node.record(&session.session_id)?,
            SessionRecord {
                lock: None,
                state: index.to_be_bytes().to_vec()
            }
        );
    }
    assert_eq!(node.machine.store().get(&counters_key)?, Some(counters));
    let snapshot = node.machine.store().snapshot()?;
    node.reset();
    assert_eq!(
        node.at(21, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 0 }
    );
    node.assert_scan(1);
    assert_eq!(node.session_gets(), 0);
    assert_eq!(node.machine.store().applies.load(Ordering::SeqCst), 0);
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(
            #[test]
            fn $case() -> super::TestResult {
                super::$case(::testkit::MemoryProvider::new())
            }
        )+ }
        mod durable { $(
            #[test]
            fn $case() -> super::TestResult {
                super::$case(::testkit::DurableProvider::temporary()?)
            }
        )+ }
    };
}

for_each_backend! {
    canonical_expiry_preserves_state_and_scoped_message_locks,
    stale_elapsed_index_cannot_clear_a_renewed_session_lock,
    missing_absent_and_zero_token_records_refuse_expiry,
    malformed_session_keys_and_values_preserve_error_priority,
    late_corruption_rolls_back_all_selected_session_clears,
    bounded_session_expiry_resumes_after_restart_without_extra_scan,
}
