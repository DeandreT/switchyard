//! Externally serialized logical apply/reopen controls over both real backends.
//! Raw edits are controlled corruption/ownership setup, not concurrent writers
//! or a power-cut/fsync fault model. All Fjall handles are dropped before reopen.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use domain::{
    BoundCommand, BrokerError, Command, CommandKind, CommandOutcome, CorrelationFilter,
    CorrelationValue, DurableProposal, EntityPath, FilterProperties, IndexedApplyError,
    IndexedApplyOutcome, IndexedWriter, LockToken, MAX_SUBSCRIPTION_RULES, MAX_TOPIC_SUBSCRIPTIONS,
    MessageEnvelope, MessageState, NamespaceName, QueueConfig, QueueConfigError, QueueConfigUpdate,
    ReceiveMode, RuleConfigError, RuleDefinition, RuleFilter, RuleName, SequenceNumber,
    StateMachine, SubscriptionConfig, SubscriptionConfigError, SubscriptionName, Timestamp,
    TopicConfig, TopicConfigError, codec, keys,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

type TestResult = Result<(), Box<dyn Error>>;
const PREFIX: &[u8] = b"\xF1switchyard/replay\0";

#[derive(Clone, Debug, Default)]
struct Trace {
    gets: Vec<Key>,
    scans: Vec<Key>,
    snapshots: usize,
    batches: Vec<WriteBatch>,
}

#[derive(Clone, Debug)]
struct Observed<S> {
    inner: S,
    trace: Arc<Mutex<Trace>>,
    apply_fault: Arc<AtomicUsize>,
    read_fault: Arc<Mutex<Option<Key>>>,
}

impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.trace.lock().unwrap().gets.push(key.to_vec());
        if self.read_fault.lock().unwrap().as_deref() == Some(key) {
            return Err(simulated_error());
        }
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.trace.lock().unwrap().batches.push(batch.clone());
        let fault = self.apply_fault.swap(0, Ordering::SeqCst);
        if fault == 1 {
            return Err(simulated_error());
        }
        self.inner.apply(batch)?;
        if fault == 2 {
            return Err(simulated_error());
        }
        Ok(())
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.trace.lock().unwrap().snapshots += 1;
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.trace.lock().unwrap().scans.push(prefix.to_vec());
        self.inner.scan_from(prefix, start, limit)
    }
}

fn simulated_error() -> StorageError {
    StorageError::Backend {
        operation: "simulate indexed apply boundary",
        detail: "controlled logical failure".to_owned(),
    }
}

struct Fixture<P: StoreProvider> {
    store: Option<Observed<P::Store>>,
    provider: P,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P) -> Self {
        let store = Some(Observed {
            inner: provider.open().unwrap(),
            trace: Arc::new(Mutex::new(Trace::default())),
            apply_fault: Arc::new(AtomicUsize::new(0)),
            read_fault: Arc::new(Mutex::new(None)),
        });
        Self { provider, store }
    }
    fn store(&self) -> &Observed<P::Store> {
        self.store.as_ref().unwrap()
    }
    fn machine(&self) -> StateMachine<Observed<P::Store>> {
        StateMachine::new(self.store().clone())
    }
    fn writer(&self) -> IndexedWriter<Observed<P::Store>> {
        IndexedWriter::open(self.store().clone()).unwrap()
    }
    fn snapshot(&self) -> Vec<(Key, Value)> {
        self.store().inner.snapshot().unwrap().entries().to_vec()
    }
    fn reset(&self) {
        *self.store().trace.lock().unwrap() = Trace::default();
    }
    fn trace(&self) -> Trace {
        self.store().trace.lock().unwrap().clone()
    }
    fn raw(&self, batch: WriteBatch) {
        self.store().inner.apply(batch).unwrap();
    }
    fn clear(&self) {
        let mut batch = WriteBatch::default();
        for (key, _) in self.snapshot() {
            batch.push_delete(key);
        }
        self.raw(batch);
    }
    fn reopen(&mut self) {
        // Memory retains shared state; Fjall reopens its actual directory only
        // after the caller and this fixture drop every original store handle.
        drop(self.store.take());
        self.store = Some(Observed {
            inner: self.provider.open().unwrap(),
            trace: Arc::new(Mutex::new(Trace::default())),
            apply_fault: Arc::new(AtomicUsize::new(0)),
            read_fault: Arc::new(Mutex::new(None)),
        });
    }
}

fn namespace() -> NamespaceName {
    NamespaceName::new("tenant").unwrap()
}
fn queue() -> EntityPath {
    EntityPath::new("orders").unwrap()
}
fn topic() -> EntityPath {
    EntityPath::new("events").unwrap()
}
fn subscription() -> EntityPath {
    topic()
        .subscription(&SubscriptionName::new("worker").unwrap())
        .unwrap()
}
fn command(entity: &EntityPath, at: u64, kind: CommandKind) -> Command {
    Command::new(
        namespace(),
        entity.clone(),
        Timestamp::from_millis(at),
        kind,
    )
}
fn proposal(entity: &EntityPath, at: u64, kind: CommandKind) -> DurableProposal {
    DurableProposal::unbound(command(entity, at, kind))
}
fn create_queue() -> CommandKind {
    CommandKind::CreateQueue {
        config: QueueConfig::default(),
    }
}
fn receive() -> CommandKind {
    CommandKind::Receive {
        mode: ReceiveMode::PeekLock,
        lock_duration_millis: Some(100),
        session: None,
    }
}
fn send(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.to_owned(),
        body: b"payload".to_vec(),
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_at: None,
        envelope: None,
    }
}
fn scheduled(id: &str, at: u64) -> CommandKind {
    let mut kind = send(id);
    if let CommandKind::Send {
        scheduled_enqueue_at,
        ..
    } = &mut kind
    {
        *scheduled_enqueue_at = Some(Timestamp::from_millis(at));
    }
    kind
}
fn applied(outcome: CommandOutcome) -> IndexedApplyOutcome {
    IndexedApplyOutcome::Applied(outcome)
}
fn key(tag: u8) -> Key {
    let mut key = PREFIX.to_vec();
    key.push(tag);
    key
}
fn hash(scope: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(scope);
    digest.update(bytes);
    digest.finalize().into()
}
fn proposal_hash(proposal: &DurableProposal) -> [u8; 32] {
    hash(
        b"switchyard indexed proposal v1\0",
        &proposal.encode().unwrap(),
    )
}
fn owner(version: u32) -> Value {
    let mut bytes = b"SWIA".to_vec();
    bytes.extend_from_slice(&version.to_be_bytes());
    bytes.extend_from_slice(&hash(b"switchyard indexed owner v1\0", &bytes));
    bytes
}
fn checkpoint(index: u64, proposal_hash: [u8; 32]) -> Value {
    let mut bytes = index.to_be_bytes().to_vec();
    bytes.extend_from_slice(&proposal_hash);
    bytes.extend_from_slice(&hash(b"switchyard indexed checkpoint v1\0", &bytes));
    bytes
}
fn domain_rows(rows: &[(Key, Value)]) -> Vec<(Key, Value)> {
    rows.iter()
        .filter(|(key, _)| key.first().is_some_and(|tag| *tag <= 0x11))
        .cloned()
        .collect()
}
fn effects(batch: &WriteBatch) -> WriteBatch {
    let mut filtered = WriteBatch::default();
    for mutation in batch.mutations() {
        match mutation {
            Mutation::Put { key, value } if !key.starts_with(PREFIX) => {
                filtered.push_put(key.clone(), value.clone())
            }
            Mutation::Delete { key } if !key.starts_with(PREFIX) => {
                filtered.push_delete(key.clone())
            }
            _ => {}
        }
    }
    filtered
}

fn exact_batch_parity<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let commands = [
        command(&queue(), 10, create_queue()),
        command(&queue(), 20, send("one")),
        command(&queue(), 30, receive()),
        command(
            &queue(),
            40,
            CommandKind::Complete {
                sequence: SequenceNumber::new(1),
                lock_token: LockToken::new(1),
            },
        ),
    ];
    let mut outcomes = Vec::new();
    fixture.reset();
    for command in &commands {
        outcomes.push(fixture.machine().apply(command)?);
    }
    let ordinary_batches = fixture.trace().batches;
    let ordinary_rows = fixture.snapshot();
    fixture.clear();
    let mut writer = fixture.writer();
    fixture.reset();
    for (offset, command) in commands.iter().enumerate() {
        assert_eq!(
            writer.apply(
                offset as u64 + 1,
                &DurableProposal::unbound(command.clone())
            )?,
            applied(outcomes[offset].clone())
        );
    }
    let indexed_batches = fixture.trace().batches;
    assert_eq!(indexed_batches.len(), ordinary_batches.len());
    for (offset, (indexed, ordinary)) in indexed_batches.iter().zip(&ordinary_batches).enumerate() {
        assert_eq!(effects(indexed), *ordinary);
        let metadata = indexed.mutations().len() - ordinary.mutations().len();
        assert_eq!(metadata, if offset == 0 { 2 } else { 1 });
        let original = DurableProposal::unbound(commands[offset].clone());
        assert_eq!(
            indexed.mutations().last(),
            Some(&Mutation::Put {
                key: key(1),
                value: checkpoint(offset as u64 + 1, proposal_hash(&original))
            })
        );
        if offset == 0 {
            assert_eq!(
                indexed.mutations()[ordinary.mutations().len()],
                Mutation::Put {
                    key: key(0),
                    value: owner(1)
                }
            );
        }
    }
    assert_eq!(domain_rows(&fixture.snapshot()), ordinary_rows);
    assert_eq!(writer.applied_index()?, 4);
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.writer().applied_index()?, 4);
    assert_eq!(domain_rows(&fixture.snapshot()), ordinary_rows);
    Ok(())
}

fn noop_refusal_and_clock<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let mut writer = fixture.writer();
    writer.apply(1, &proposal(&queue(), 10, create_queue()))?;
    let original_domain = domain_rows(&fixture.snapshot());
    for (index, at, kind) in [
        (2, 20, receive()),
        (3, 30, CommandKind::ExpireMessages),
        (4, 40, create_queue()),
        (5, 5, receive()),
    ] {
        let ordinary = fixture
            .machine()
            .apply(&command(&queue(), at, kind.clone()));
        fixture.reset();
        let result = writer.apply(index, &proposal(&queue(), at, kind))?;
        match ordinary {
            Ok(outcome) => assert_eq!(result, applied(outcome)),
            Err(error) => assert_eq!(result, IndexedApplyOutcome::Refused(error)),
        }
        let trace = fixture.trace();
        assert_eq!(trace.batches.len(), 1);
        assert_eq!(trace.batches[0].mutations().len(), 1);
        assert_eq!(domain_rows(&fixture.snapshot()), original_domain);
        assert_eq!(
            fixture.machine().last_applied_time()?,
            Timestamp::from_millis(10)
        );
    }
    // Ordinary no-op and rejection still make no logical apply at all.
    fixture.reset();
    fixture.machine().apply(&command(&queue(), 60, receive()))?;
    assert_eq!(
        fixture
            .machine()
            .apply(&command(&queue(), 60, create_queue())),
        Err(BrokerError::QueueAlreadyExists)
    );
    assert!(fixture.trace().batches.is_empty());
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.writer().applied_index()?, 5);
    assert_eq!(domain_rows(&fixture.snapshot()), original_domain);
    Ok(())
}

fn indexes_and_duplicate_reads<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let first = proposal(&queue(), 10, create_queue());
    let mut writer = fixture.writer();
    assert_eq!(writer.apply(0, &first), Err(IndexedApplyError::ZeroIndex));
    assert_eq!(
        writer.apply(2, &first),
        Err(IndexedApplyError::UnexpectedIndex {
            requested: 2,
            expected: 1
        })
    );
    writer.apply(1, &first)?;
    let refusal = proposal(&queue(), 5, receive());
    assert!(matches!(
        writer.apply(2, &refusal)?,
        IndexedApplyOutcome::Refused(BrokerError::ClockRegression { .. })
    ));
    fixture.reset();
    assert_eq!(
        writer.apply(2, &refusal)?,
        IndexedApplyOutcome::AlreadyApplied
    );
    assert_eq!(writer.applied_index()?, 2);
    let trace = fixture.trace();
    assert!(trace.gets.is_empty() && trace.scans.is_empty() && trace.batches.is_empty());
    assert_eq!(trace.snapshots, 0);
    assert_eq!(
        writer.apply(2, &first),
        Err(IndexedApplyError::ConflictingProposal { index: 2 })
    );
    assert_eq!(
        writer.apply(1, &first),
        Err(IndexedApplyError::UnexpectedIndex {
            requested: 1,
            expected: 3
        })
    );
    assert_eq!(
        writer.apply(4, &first),
        Err(IndexedApplyError::UnexpectedIndex {
            requested: 4,
            expected: 3
        })
    );
    let rows = fixture.snapshot();
    drop(writer);
    fixture.reopen();
    let mut writer = fixture.writer();
    fixture.reset();
    assert_eq!(
        writer.apply(2, &refusal)?,
        IndexedApplyOutcome::AlreadyApplied
    );
    assert!(
        fixture.trace().gets.is_empty()
            && fixture.trace().scans.is_empty()
            && fixture.trace().batches.is_empty()
    );
    assert_eq!(fixture.snapshot(), rows);
    drop(writer);
    // Checked raw metadata sets the arithmetic boundary; this is not a replay of MAX entries.
    fixture.raw(WriteBatch::default().put(key(1), checkpoint(u64::MAX - 1, proposal_hash(&first))));
    let mut writer = fixture.writer();
    assert_eq!(
        writer.apply(u64::MAX, &first)?,
        IndexedApplyOutcome::Refused(BrokerError::QueueAlreadyExists)
    );
    assert_eq!(
        writer.apply(u64::MAX, &first)?,
        IndexedApplyOutcome::AlreadyApplied
    );
    assert_eq!(
        writer.apply(1, &first),
        Err(IndexedApplyError::IndexExhausted)
    );
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.writer().applied_index()?, u64::MAX);
    Ok(())
}

fn empty_adoption_and_f0<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = Fixture::new(provider);
    assert_eq!(fixture.writer().applied_index()?, 0);
    assert!(fixture.snapshot().is_empty());
    for tag in 0x00..=0x11 {
        let occupied = vec![tag, 0xAA];
        fixture.raw(WriteBatch::default().put(occupied.clone(), vec![1]));
        fixture.reset();
        assert!(matches!(
            IndexedWriter::open(fixture.store().clone()),
            Err(IndexedApplyError::PopulatedStore)
        ));
        assert!(fixture.trace().batches.is_empty());
        assert_eq!(fixture.snapshot(), vec![(occupied.clone(), vec![1])]);
        fixture.raw(WriteBatch::default().delete(occupied));
    }
    // Known empty-journal framing is preserved byte-for-byte. This domain
    // control does not open, validate, commit or activate the journal API.
    let journal_root = b"\xF0switchyard/journal\0\0".to_vec();
    let journal_frontier = b"\xF0switchyard/journal\0\x01".to_vec();
    let mut frontier = 1_u32.to_be_bytes().to_vec();
    frontier.extend_from_slice(&0_u64.to_be_bytes());
    frontier.extend_from_slice(&hash(b"switchyard journal frontier v1\0", &frontier));
    fixture.raw(
        WriteBatch::default()
            .put(journal_root.clone(), 1_u32.to_be_bytes().to_vec())
            .put(journal_frontier.clone(), frontier.clone()),
    );
    let mut writer = fixture.writer();
    writer.apply(1, &proposal(&queue(), 10, create_queue()))?;
    assert_eq!(
        fixture.store().inner.get(&journal_root)?,
        Some(1_u32.to_be_bytes().to_vec())
    );
    assert_eq!(
        fixture.store().inner.get(&journal_frontier)?,
        Some(frontier)
    );
    assert_eq!(writer.applied_index()?, 1);
    Ok(())
}

fn malformed_ownership<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let good_owner = owner(1);
    let good_checkpoint = checkpoint(1, [0x42; 32]);
    let mut bad_owner_checksum = good_owner.clone();
    bad_owner_checksum[8] ^= 1;
    let mut bad_checkpoint_checksum = good_checkpoint.clone();
    bad_checkpoint_checksum[40] ^= 1;
    let mut owner_tail = good_owner.clone();
    owner_tail.push(0);
    let mut checkpoint_tail = good_checkpoint.clone();
    checkpoint_tail.push(0);
    let mut bad_magic = good_owner.clone();
    bad_magic[0] = b'X';
    let cases = vec![
        WriteBatch::default().put(key(0), good_owner.clone()),
        WriteBatch::default().put(key(1), good_checkpoint.clone()),
        WriteBatch::default().put(vec![0xF1], vec![0]),
        WriteBatch::default().put(PREFIX.to_vec(), vec![0]),
        WriteBatch::default().put(b"\xF1different-owner".to_vec(), vec![0]),
        WriteBatch::default()
            .put(key(0), bad_owner_checksum)
            .put(key(1), good_checkpoint.clone()),
        WriteBatch::default()
            .put(key(0), bad_magic)
            .put(key(1), good_checkpoint.clone()),
        WriteBatch::default()
            .put(key(0), good_owner[..39].to_vec())
            .put(key(1), good_checkpoint.clone()),
        WriteBatch::default()
            .put(key(0), owner_tail)
            .put(key(1), good_checkpoint.clone()),
        WriteBatch::default()
            .put(key(0), good_owner.clone())
            .put(key(1), bad_checkpoint_checksum),
        WriteBatch::default()
            .put(key(0), good_owner.clone())
            .put(key(1), good_checkpoint[..71].to_vec()),
        WriteBatch::default()
            .put(key(0), good_owner.clone())
            .put(key(1), checkpoint_tail),
        WriteBatch::default()
            .put(key(0), good_owner.clone())
            .put(key(1), checkpoint(0, [0x42; 32])),
        WriteBatch::default()
            .put(key(0), good_owner.clone())
            .put(key(1), good_checkpoint.clone())
            .put(key(2), vec![0]),
        WriteBatch::default()
            .put(key(0), good_owner.clone())
            .put(key(1), good_checkpoint.clone())
            .put(b"\xF1other".to_vec(), vec![0]),
    ];
    for case in cases {
        fixture.raw(case);
        let before = fixture.snapshot();
        fixture.reset();
        assert!(matches!(
            IndexedWriter::open(fixture.store().clone()),
            Err(IndexedApplyError::Corrupt { .. })
        ));
        assert_eq!(fixture.snapshot(), before);
        assert!(fixture.trace().batches.is_empty());
        fixture.reopen();
        assert!(matches!(
            IndexedWriter::open(fixture.store().clone()),
            Err(IndexedApplyError::Corrupt { .. })
        ));
        fixture.clear();
    }
    fixture.raw(
        WriteBatch::default()
            .put(key(0), owner(2))
            .put(key(1), good_checkpoint),
    );
    assert!(matches!(
        IndexedWriter::open(fixture.store().clone()),
        Err(IndexedApplyError::UnsupportedVersion {
            found: 2,
            expected: 1
        })
    ));
    Ok(())
}

fn apply_rows(before: &[(Key, Value)], batch: &WriteBatch) -> Vec<(Key, Value)> {
    let mut rows: BTreeMap<Key, Value> = before.iter().cloned().collect();
    for mutation in batch.mutations() {
        match mutation {
            Mutation::Put { key, value } => {
                rows.insert(key.clone(), value.clone());
            }
            Mutation::Delete { key } => {
                rows.remove(key);
            }
        }
    }
    rows.into_iter().collect()
}

fn ambiguous_apply_and_reopen<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    // First effect/no-op/refusal and later effect/no-op/refusal all use one
    // actual logical apply. The error wrapper never splits or cancels a batch.
    for case in 0..6 {
        for after_apply in [false, true] {
            fixture.clear();
            let mut writer = fixture.writer();
            let index = if case >= 3 {
                writer.apply(1, &proposal(&queue(), 10, create_queue()))?;
                2
            } else {
                1
            };
            let (kind, expected) = match case {
                0 => (create_queue(), applied(CommandOutcome::QueueCreated)),
                1 => (
                    CommandKind::ExpireSessionLocks,
                    applied(CommandOutcome::SessionLocksExpired { released: 0 }),
                ),
                2 => (
                    receive(),
                    IndexedApplyOutcome::Refused(BrokerError::QueueNotFound),
                ),
                3 => (
                    send("exactly-once"),
                    applied(CommandOutcome::Sent {
                        sequence: SequenceNumber::new(1),
                    }),
                ),
                4 => (receive(), applied(CommandOutcome::Received(None))),
                _ => (
                    create_queue(),
                    IndexedApplyOutcome::Refused(BrokerError::QueueAlreadyExists),
                ),
            };
            let original = proposal(&queue(), 30, kind);
            let before = fixture.snapshot();
            fixture.reset();
            fixture
                .store()
                .apply_fault
                .store(if after_apply { 2 } else { 1 }, Ordering::SeqCst);
            assert_eq!(
                writer.apply(index, &original),
                Err(IndexedApplyError::Storage(simulated_error()))
            );
            let trace = fixture.trace();
            assert_eq!(trace.batches.len(), 1);
            let complete = apply_rows(&before, &trace.batches[0]);
            assert_eq!(
                fixture.snapshot(),
                if after_apply {
                    complete.clone()
                } else {
                    before.clone()
                }
            );
            assert_eq!(writer.applied_index(), Err(IndexedApplyError::Unusable));
            assert_eq!(
                writer.apply(index, &original),
                Err(IndexedApplyError::Unusable)
            );
            assert_eq!(writer.apply(0, &original), Err(IndexedApplyError::Unusable));
            assert_eq!(fixture.trace().batches.len(), 1);
            drop(writer);
            fixture.reopen();
            assert_eq!(
                fixture.snapshot(),
                if after_apply {
                    complete.clone()
                } else {
                    before
                }
            );
            let mut writer = fixture.writer();
            assert_eq!(
                writer.applied_index()?,
                if after_apply { index } else { index - 1 }
            );
            fixture.reset();
            assert_eq!(
                writer.apply(index, &original)?,
                if after_apply {
                    IndexedApplyOutcome::AlreadyApplied
                } else {
                    expected
                }
            );
            assert_eq!(fixture.trace().batches.len(), usize::from(!after_apply));
            if after_apply {
                assert!(fixture.trace().gets.is_empty() && fixture.trace().scans.is_empty());
                assert_eq!(fixture.trace().snapshots, 0);
            }
            assert_eq!(fixture.snapshot(), complete);
            assert_eq!(writer.applied_index()?, index);
            if matches!(case, 1 | 2 | 4 | 5) {
                assert!(!trace.batches[0].mutations().iter().any(|mutation| matches!(mutation, Mutation::Put { key, .. } if *key == keys::clock())));
            }
            drop(writer);
        }
    }
    Ok(())
}

fn uncheckpointed<P: StoreProvider>(
    fixture: &Fixture<P>,
    writer: &mut IndexedWriter<Observed<P::Store>>,
    index: u64,
    original: &DurableProposal,
    error: BrokerError,
) {
    let before = fixture.snapshot();
    fixture.reset();
    assert_eq!(
        fixture.machine().apply(original.command()),
        Err(error.clone())
    );
    assert!(fixture.trace().batches.is_empty());
    assert_eq!(fixture.snapshot(), before);
    fixture.reset();
    assert_eq!(
        writer.apply(index, original),
        Err(IndexedApplyError::Domain(error))
    );
    assert_eq!(writer.applied_index().unwrap(), index - 1);
    assert!(fixture.trace().batches.is_empty());
    assert_eq!(fixture.snapshot(), before);
}

fn lock_and_schedule_origins<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let mut writer = fixture.writer();
    writer.apply(1, &proposal(&queue(), 10, create_queue()))?;
    writer.apply(2, &proposal(&queue(), 20, send("locked")))?;
    writer.apply(3, &proposal(&queue(), 30, send("ready")))?;
    writer.apply(4, &proposal(&queue(), 40, receive()))?;
    let sequence = SequenceNumber::new(2);
    assert_eq!(
        writer.apply(
            5,
            &proposal(
                &queue(),
                50,
                CommandKind::Complete {
                    sequence,
                    lock_token: LockToken::new(1)
                }
            )
        )?,
        IndexedApplyOutcome::Refused(BrokerError::MessageNotLocked { sequence })
    );
    // The first due lock is healthy and stages return-to-ready before the
    // second due index points at a Ready record and reports the same variant.
    fixture.raw(WriteBatch::default().put(
        keys::lock(
            &namespace(),
            &queue(),
            Timestamp::from_millis(200),
            sequence,
        ),
        Vec::new(),
    ));
    uncheckpointed(
        &fixture,
        &mut writer,
        6,
        &proposal(&queue(), 300, CommandKind::ExpireLocks),
        BrokerError::MessageNotLocked { sequence },
    );
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.writer().applied_index()?, 5);
    assert!(matches!(
        fixture
            .machine()
            .message(&namespace(), &queue(), SequenceNumber::new(1))?
            .unwrap()
            .state,
        MessageState::Locked { .. }
    ));
    fixture.clear();
    let mut writer = fixture.writer();
    writer.apply(1, &proposal(&queue(), 10, create_queue()))?;
    writer.apply(2, &proposal(&queue(), 20, scheduled("scheduled", 100)))?;
    writer.apply(3, &proposal(&queue(), 30, send("ready")))?;
    assert_eq!(
        writer.apply(
            4,
            &proposal(
                &queue(),
                40,
                CommandKind::CancelScheduled {
                    sequences: vec![sequence]
                }
            )
        )?,
        IndexedApplyOutcome::Refused(BrokerError::MessageNotScheduled { sequence })
    );
    fixture.raw(WriteBatch::default().put(
        keys::scheduled(
            &namespace(),
            &queue(),
            Timestamp::from_millis(150),
            sequence,
        ),
        Vec::new(),
    ));
    uncheckpointed(
        &fixture,
        &mut writer,
        5,
        &proposal(&queue(), 200, CommandKind::ActivateScheduled),
        BrokerError::MessageNotScheduled { sequence },
    );
    let mut record = fixture
        .machine()
        .message(&namespace(), &queue(), SequenceNumber::new(1))?
        .unwrap();
    record.scheduled_enqueue_at = None;
    fixture.raw(WriteBatch::default().put(
        keys::message(&namespace(), &queue(), record.sequence),
        codec::encode(&record)?,
    ));
    uncheckpointed(
        &fixture,
        &mut writer,
        5,
        &proposal(
            &queue(),
            200,
            CommandKind::CancelScheduled {
                sequences: vec![record.sequence],
            },
        ),
        BrokerError::ScheduledEnqueueTimeMissing {
            sequence: record.sequence,
        },
    );
    let before = fixture.snapshot();
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.writer().applied_index()?, 4);
    assert_eq!(fixture.snapshot(), before);
    Ok(())
}

fn duplicate_properties() -> FilterProperties {
    let value = CorrelationValue::new(vec![1]).unwrap();
    FilterProperties {
        application_properties: [
            ("Color".to_owned(), value.clone()),
            ("color".to_owned(), value),
        ]
        .into_iter()
        .collect(),
        ..FilterProperties::default()
    }
}

fn rule_config_origin<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let mut writer = fixture.writer();
    writer.apply(
        1,
        &proposal(
            &topic(),
            10,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
    )?;
    writer.apply(
        2,
        &proposal(
            &topic(),
            20,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("worker")?,
                config: SubscriptionConfig::default(),
            },
        ),
    )?;
    assert_eq!(
        writer.apply(
            3,
            &proposal(
                &subscription(),
                30,
                CommandKind::CreateRule {
                    name: RuleName::new("invalid")?,
                    filter: RuleFilter::Correlation(CorrelationFilter::default())
                }
            )
        )?,
        IndexedApplyOutcome::Refused(BrokerError::RuleConfig(
            RuleConfigError::EmptyCorrelationFilter
        ))
    );
    let mut invalid_input = send("invalid-projection");
    if let CommandKind::Send { envelope, .. } = &mut invalid_input {
        *envelope =
            Some(MessageEnvelope::new(vec![1]).with_filter_properties(duplicate_properties()));
    }
    assert_eq!(
        writer.apply(4, &proposal(&topic(), 40, invalid_input))?,
        IndexedApplyOutcome::Refused(BrokerError::RuleConfig(
            RuleConfigError::DuplicateApplicationProperty
        ))
    );
    writer.apply(
        5,
        &proposal(&topic(), 50, scheduled("valid-placeholder", 100)),
    )?;
    let mut record = fixture
        .machine()
        .message(&namespace(), &topic(), SequenceNumber::new(1))?
        .unwrap();
    record.envelope =
        Some(MessageEnvelope::new(vec![1]).with_filter_properties(duplicate_properties()));
    fixture.raw(WriteBatch::default().put(
        keys::message(&namespace(), &topic(), record.sequence),
        codec::encode(&record)?,
    ));
    uncheckpointed(
        &fixture,
        &mut writer,
        6,
        &proposal(&topic(), 200, CommandKind::ActivateScheduled),
        BrokerError::RuleConfig(RuleConfigError::DuplicateApplicationProperty),
    );
    let before = fixture.snapshot();
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.writer().applied_index()?, 5);
    assert_eq!(fixture.snapshot(), before);
    Ok(())
}

fn rule_caps_are_conservative<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let mut writer = fixture.writer();
    writer.apply(
        1,
        &proposal(
            &topic(),
            10,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
    )?;
    writer.apply(
        2,
        &proposal(
            &topic(),
            20,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("worker")?,
                config: SubscriptionConfig::default(),
            },
        ),
    )?;
    // Canonical raw fixtures fill the valid catalog without 2,000 fsyncs.
    // They are serialized setup, not an extra live domain writer.
    let mut batch = WriteBatch::default();
    for offset in 1..MAX_SUBSCRIPTION_RULES {
        let name = RuleName::new(format!("rule-{offset:04}"))?;
        let definition = RuleDefinition {
            name: name.clone(),
            filter: RuleFilter::True,
            created_at: Timestamp::from_millis(20),
        };
        batch.push_put(
            keys::subscription_rule(&namespace(), &subscription(), &name),
            codec::encode(&definition)?,
        );
    }
    fixture.raw(batch);
    assert_eq!(
        fixture
            .machine()
            .rules(&namespace(), &subscription(), 0, 1)?
            .len(),
        1
    );
    let create = proposal(
        &subscription(),
        30,
        CommandKind::CreateRule {
            name: RuleName::new("new-rule")?,
            filter: RuleFilter::True,
        },
    );
    let error = BrokerError::RuleLimitExceeded {
        maximum: MAX_SUBSCRIPTION_RULES,
    };
    uncheckpointed(&fixture, &mut writer, 3, &create, error.clone());
    let name = RuleName::new("overfull")?;
    fixture.raw(WriteBatch::default().put(
        keys::subscription_rule(&namespace(), &subscription(), &name),
        codec::encode(&RuleDefinition {
            name,
            filter: RuleFilter::True,
            created_at: Timestamp::from_millis(20),
        })?,
    ));
    uncheckpointed(&fixture, &mut writer, 3, &create, error.clone());
    uncheckpointed(
        &fixture,
        &mut writer,
        3,
        &proposal(
            &subscription(),
            30,
            CommandKind::ListRules {
                skip: 0,
                max_rules: 1,
            },
        ),
        error,
    );
    let before = fixture.snapshot();
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.writer().applied_index()?, 2);
    assert_eq!(fixture.snapshot(), before);
    Ok(())
}

fn subscription_caps_are_conservative<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let mut writer = fixture.writer();
    writer.apply(
        1,
        &proposal(
            &topic(),
            10,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
    )?;
    writer.apply(
        2,
        &proposal(
            &topic(),
            20,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("worker")?,
                config: SubscriptionConfig::default(),
            },
        ),
    )?;
    let backing = fixture
        .store()
        .inner
        .get(&keys::queue_config(&namespace(), &subscription()))?
        .unwrap();
    let shadow = fixture
        .store()
        .inner
        .get(&keys::queue_config(
            &namespace(),
            &subscription().dead_letter_queue()?,
        ))?
        .unwrap();
    let head = fixture
        .store()
        .inner
        .get(&keys::entity_metadata(&namespace(), &subscription()))?
        .unwrap();
    let default_name = RuleName::new(domain::DEFAULT_RULE_NAME)?;
    let default_rule = fixture
        .store()
        .inner
        .get(&keys::subscription_rule(
            &namespace(),
            &subscription(),
            &default_name,
        ))?
        .unwrap();
    let mut batch = WriteBatch::default();
    // Profiles, heads and default rules mirror the one actual-created child.
    for offset in 1..MAX_TOPIC_SUBSCRIPTIONS {
        let name = SubscriptionName::new(format!("child-{offset:04}"))?;
        let entity = topic().subscription(&name)?;
        batch.push_put(
            keys::topic_subscription(&namespace(), &topic(), &name),
            codec::encode(&entity)?,
        );
        batch.push_put(keys::queue_config(&namespace(), &entity), backing.clone());
        batch.push_put(
            keys::queue_config(&namespace(), &entity.dead_letter_queue()?),
            shadow.clone(),
        );
        batch.push_put(keys::entity_metadata(&namespace(), &entity), head.clone());
        batch.push_put(
            keys::subscription_rule(&namespace(), &entity, &default_name),
            default_rule.clone(),
        );
    }
    fixture.raw(batch);
    assert_eq!(
        fixture
            .machine()
            .subscriptions(&namespace(), &topic(), MAX_TOPIC_SUBSCRIPTIONS)?
            .len(),
        MAX_TOPIC_SUBSCRIPTIONS
    );
    let create = proposal(
        &topic(),
        30,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("new-child")?,
            config: SubscriptionConfig::default(),
        },
    );
    let error = BrokerError::SubscriptionLimitExceeded {
        maximum: MAX_TOPIC_SUBSCRIPTIONS,
    };
    uncheckpointed(&fixture, &mut writer, 3, &create, error.clone());
    let extra = SubscriptionName::new("overfull")?;
    fixture.raw(WriteBatch::default().put(
        keys::topic_subscription(&namespace(), &topic(), &extra),
        codec::encode(&topic().subscription(&extra)?)?,
    ));
    uncheckpointed(&fixture, &mut writer, 3, &create, error.clone());
    uncheckpointed(
        &fixture,
        &mut writer,
        3,
        &proposal(&topic(), 30, send("publish")),
        error,
    );
    let before = fixture.snapshot();
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.writer().applied_index()?, 2);
    assert_eq!(fixture.snapshot(), before);
    Ok(())
}

fn partial_receive_and_duplicate_failure<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let mut writer = fixture.writer();
    writer.apply(1, &proposal(&queue(), 10, create_queue()))?;
    let mut expiring = send("expires");
    if let CommandKind::Send {
        time_to_live_millis,
        ..
    } = &mut expiring
    {
        *time_to_live_millis = Some(1);
    }
    writer.apply(2, &proposal(&queue(), 20, expiring))?;
    let missing = SequenceNumber::new(2);
    fixture
        .raw(WriteBatch::default().put(keys::ready(&namespace(), &queue(), missing), Vec::new()));
    // The first expired message stages a DLQ enqueue before the later dangling
    // Ready entry fails. Neither the enqueue nor its deletes may survive.
    uncheckpointed(
        &fixture,
        &mut writer,
        3,
        &proposal(&queue(), 50, receive()),
        BrokerError::DanglingIndexEntry { sequence: missing },
    );
    assert!(
        fixture
            .machine()
            .message(
                &namespace(),
                &queue().dead_letter_queue()?,
                SequenceNumber::new(1)
            )?
            .is_none()
    );
    let before = fixture.snapshot();
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.snapshot(), before);
    fixture.clear();
    let mut writer = fixture.writer();
    let config = QueueConfig {
        requires_duplicate_detection: true,
        ..QueueConfig::default()
    };
    writer.apply(
        1,
        &proposal(&queue(), 10, CommandKind::CreateQueue { config }),
    )?;
    let counters = keys::queue_counters(&namespace(), &queue());
    *fixture.store().read_fault.lock().unwrap() = Some(counters.clone());
    uncheckpointed(
        &fixture,
        &mut writer,
        2,
        &proposal(&queue(), 20, send("staged-history")),
        BrokerError::Storage(simulated_error()),
    );
    let gets = fixture.trace().gets;
    let history = keys::duplicate_id(&namespace(), &queue(), "staged-history");
    assert!(
        gets.iter().position(|key| *key == history).unwrap()
            < gets.iter().position(|key| *key == counters).unwrap()
    );
    assert!(fixture.store().inner.get(&history)?.is_none());
    *fixture.store().read_fault.lock().unwrap() = None;
    // A preparation read error does not retire the writer; its same next index
    // remains usable once the controlled read fault is removed.
    writer.apply(2, &proposal(&queue(), 20, send("staged-history")))?;
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.writer().applied_index()?, 2);
    assert!(fixture.store().inner.get(&history)?.is_some());
    Ok(())
}

fn multiple_requested_refusal_conservation<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let mut writer = fixture.writer();
    writer.apply(1, &proposal(&queue(), 10, create_queue()))?;
    writer.apply(2, &proposal(&queue(), 20, scheduled("one", 100)))?;
    writer.apply(3, &proposal(&queue(), 30, scheduled("two", 110)))?;
    let sequences = vec![SequenceNumber::new(1), SequenceNumber::new(999)];
    let original = proposal(&queue(), 40, CommandKind::CancelScheduled { sequences });
    let before = domain_rows(&fixture.snapshot());
    fixture.reset();
    assert_eq!(
        fixture.machine().apply(original.command()),
        Err(BrokerError::MessageNotFound {
            sequence: SequenceNumber::new(999)
        })
    );
    assert!(fixture.trace().batches.is_empty());
    assert_eq!(
        writer.apply(4, &original)?,
        IndexedApplyOutcome::Refused(BrokerError::MessageNotFound {
            sequence: SequenceNumber::new(999)
        })
    );
    assert_eq!(fixture.trace().batches.len(), 1);
    assert_eq!(fixture.trace().batches[0].mutations().len(), 1);
    assert_eq!(domain_rows(&fixture.snapshot()), before);
    writer.apply(
        5,
        &proposal(
            &queue(),
            50,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)],
            },
        ),
    )?;
    assert!(
        fixture
            .machine()
            .message(&namespace(), &queue(), SequenceNumber::new(1))?
            .is_none()
    );
    assert!(
        fixture
            .machine()
            .message(&namespace(), &queue(), SequenceNumber::new(2))?
            .is_none()
    );
    drop(writer);
    fixture.clear();
    let mut writer = fixture.writer();
    writer.apply(1, &proposal(&queue(), 10, create_queue()))?;
    writer.apply(2, &proposal(&queue(), 20, send("one")))?;
    writer.apply(3, &proposal(&queue(), 30, send("two")))?;
    writer.apply(4, &proposal(&queue(), 40, receive()))?;
    writer.apply(
        5,
        &proposal(
            &queue(),
            50,
            CommandKind::Defer {
                sequence: SequenceNumber::new(1),
                lock_token: LockToken::new(1),
                replacement_envelope: None,
            },
        ),
    )?;
    writer.apply(6, &proposal(&queue(), 60, receive()))?;
    writer.apply(
        7,
        &proposal(
            &queue(),
            70,
            CommandKind::Defer {
                sequence: SequenceNumber::new(2),
                lock_token: LockToken::new(2),
                replacement_envelope: None,
            },
        ),
    )?;
    // These handlers validate the complete request BEFORE any staging. This is
    // multi-record refusal conservation, not a claimed staged-refusal frontier.
    let original = proposal(
        &queue(),
        80,
        CommandKind::ReceiveDeferred {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(999)],
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    );
    let before = domain_rows(&fixture.snapshot());
    fixture.reset();
    assert_eq!(
        fixture.machine().apply(original.command()),
        Err(BrokerError::MessageNotFound {
            sequence: SequenceNumber::new(999)
        })
    );
    assert!(fixture.trace().batches.is_empty());
    assert_eq!(
        writer.apply(8, &original)?,
        IndexedApplyOutcome::Refused(BrokerError::MessageNotFound {
            sequence: SequenceNumber::new(999)
        })
    );
    assert_eq!(fixture.trace().batches.len(), 1);
    assert_eq!(fixture.trace().batches[0].mutations().len(), 1);
    assert_eq!(domain_rows(&fixture.snapshot()), before);
    let expected = fixture.snapshot();
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.snapshot(), expected);
    let mut writer = fixture.writer();
    assert_eq!(writer.applied_index()?, 8);
    assert!(
        matches!(writer.apply(9, &proposal(&queue(), 90, CommandKind::ReceiveDeferred {
        sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)],
        mode: ReceiveMode::ReceiveAndDelete, lock_duration_millis: None, session: None,
    }))?, IndexedApplyOutcome::Applied(CommandOutcome::DeferredReceived(messages)) if messages.len() == 2)
    );
    assert!(
        fixture
            .machine()
            .message(&namespace(), &queue(), SequenceNumber::new(1))?
            .is_none()
    );
    assert!(
        fixture
            .machine()
            .message(&namespace(), &queue(), SequenceNumber::new(2))?
            .is_none()
    );
    Ok(())
}

fn incoming_config_refusals<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = Fixture::new(provider);
    let mut writer = fixture.writer();
    let invalid_queue = QueueConfig {
        lock_duration_millis: 0,
        ..QueueConfig::default()
    };
    assert_eq!(
        writer.apply(
            1,
            &proposal(
                &queue(),
                10,
                CommandKind::CreateQueue {
                    config: invalid_queue
                }
            )
        )?,
        IndexedApplyOutcome::Refused(BrokerError::QueueConfig(
            QueueConfigError::LockDurationTooShort
        ))
    );
    assert!(domain_rows(&fixture.snapshot()).is_empty());
    writer.apply(2, &proposal(&queue(), 20, create_queue()))?;
    let before = domain_rows(&fixture.snapshot());
    let invalid_update = QueueConfigUpdate {
        lock_duration_millis: Some(0),
        ..QueueConfigUpdate::default()
    };
    assert_eq!(
        writer.apply(
            3,
            &proposal(
                &queue(),
                30,
                CommandKind::UpdateQueue {
                    update: invalid_update
                }
            )
        )?,
        IndexedApplyOutcome::Refused(BrokerError::QueueConfig(
            QueueConfigError::LockDurationTooShort
        ))
    );
    assert_eq!(domain_rows(&fixture.snapshot()), before);
    let invalid_topic = TopicConfig {
        max_message_bytes: 0,
        ..TopicConfig::default()
    };
    assert_eq!(
        writer.apply(
            4,
            &proposal(
                &topic(),
                40,
                CommandKind::CreateTopic {
                    config: invalid_topic
                }
            )
        )?,
        IndexedApplyOutcome::Refused(BrokerError::TopicConfig(
            TopicConfigError::MaxMessageBytesTooSmall
        ))
    );
    assert_eq!(domain_rows(&fixture.snapshot()), before);
    writer.apply(
        5,
        &proposal(
            &topic(),
            50,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
    )?;
    let before = domain_rows(&fixture.snapshot());
    let invalid_subscription = SubscriptionConfig {
        lock_duration_millis: 0,
        ..SubscriptionConfig::default()
    };
    assert_eq!(
        writer.apply(
            6,
            &proposal(
                &topic(),
                60,
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new("invalid")?,
                    config: invalid_subscription
                }
            )
        )?,
        IndexedApplyOutcome::Refused(BrokerError::SubscriptionConfig(
            SubscriptionConfigError::LockDurationTooShort
        ))
    );
    assert_eq!(domain_rows(&fixture.snapshot()), before);
    assert_eq!(
        fixture.machine().last_applied_time()?,
        Timestamp::from_millis(50)
    );
    let mut too_large = send("cannot-encode");
    if let CommandKind::Send { body, .. } = &mut too_large {
        *body = vec![0; domain::MAX_DURABLE_PROPOSAL_BYTES + 1];
    }
    fixture.reset();
    assert_eq!(
        writer.apply(7, &proposal(&queue(), 70, too_large)),
        Err(IndexedApplyError::Proposal(
            domain::DurableProposalError::TooLarge
        ))
    );
    assert_eq!(writer.applied_index()?, 6);
    assert!(
        fixture.trace().gets.is_empty()
            && fixture.trace().scans.is_empty()
            && fixture.trace().batches.is_empty()
    );
    assert_eq!(
        writer.apply(7, &proposal(&queue(), 70, send("encodable")))?,
        applied(CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        })
    );
    Ok(())
}

// Mirrors only the private Queue head to inject generation/shape faults.
#[derive(Serialize)]
enum QueueKind {
    Queue,
}
#[derive(Serialize)]
struct QueueHead {
    generation: u64,
    kind: QueueKind,
    retired: bool,
}
fn queue_head(generation: u64) -> Value {
    codec::encode(&QueueHead {
        generation,
        kind: QueueKind::Queue,
        retired: false,
    })
    .unwrap()
}

fn bound_priority_and_duplicate<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let mut writer = fixture.writer();
    writer.apply(1, &proposal(&queue(), 100, create_queue()))?;
    let binding = fixture.machine().bind_entity(&namespace(), &queue())?;
    let bound = DurableProposal::bound(BoundCommand::new(
        binding.clone(),
        command(&queue(), 50, receive()),
    ))?;
    assert_eq!(
        writer.apply(2, &bound)?,
        IndexedApplyOutcome::Refused(BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(100),
            proposed: Timestamp::from_millis(50)
        })
    );
    let head_key = keys::entity_metadata(&namespace(), &queue());
    fixture.raw(
        WriteBatch::default()
            .put(head_key.clone(), queue_head(2))
            .put(keys::clock(), vec![255]),
    );
    fixture.reset();
    assert_eq!(
        writer.apply(3, &bound)?,
        IndexedApplyOutcome::Refused(BrokerError::StaleEntityBinding)
    );
    assert!(!fixture.trace().gets.contains(&keys::clock()));
    // Latest retries must not inspect either the now-stale authority or the
    // corrupt Clock. The external raw edit above is controlled fault setup.
    fixture.reset();
    assert_eq!(
        writer.apply(3, &bound)?,
        IndexedApplyOutcome::AlreadyApplied
    );
    assert!(
        fixture.trace().gets.is_empty()
            && fixture.trace().scans.is_empty()
            && fixture.trace().batches.is_empty()
    );
    assert_eq!(fixture.trace().snapshots, 0);
    assert_eq!(
        writer.apply(3, &DurableProposal::unbound(bound.command().clone())),
        Err(IndexedApplyError::ConflictingProposal { index: 3 })
    );
    assert!(
        fixture.trace().gets.is_empty()
            && fixture.trace().scans.is_empty()
            && fixture.trace().batches.is_empty()
    );
    fixture.raw(WriteBatch::default().put(head_key.clone(), vec![255]));
    let before = fixture.snapshot();
    fixture.reset();
    assert_eq!(
        writer.apply(4, &bound),
        Err(IndexedApplyError::Domain(
            BrokerError::EntityMetadataCorrupt
        ))
    );
    assert!(!fixture.trace().gets.contains(&keys::clock()));
    assert!(fixture.trace().batches.is_empty());
    assert_eq!(fixture.snapshot(), before);
    fixture.raw(WriteBatch::default().put(head_key, queue_head(1)));
    let clock_error = BrokerError::Codec(domain::CodecError::UnsupportedVersion { version: 255 });
    assert_eq!(
        writer.apply(4, &bound),
        Err(IndexedApplyError::Domain(clock_error))
    );
    assert_eq!(writer.applied_index()?, 3);
    let wrong_scope = BoundCommand::new(binding, command(&topic(), 40, receive()));
    fixture.reset();
    assert_eq!(
        fixture.machine().apply_bound(&wrong_scope),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert!(fixture.trace().gets.is_empty());
    assert_eq!(
        DurableProposal::bound(wrong_scope),
        Err(domain::DurableProposalError::InvalidAuthority)
    );
    let before = fixture.snapshot();
    drop(writer);
    fixture.reopen();
    let mut writer = fixture.writer();
    fixture.reset();
    assert_eq!(
        writer.apply(3, &bound)?,
        IndexedApplyOutcome::AlreadyApplied
    );
    assert!(
        fixture.trace().gets.is_empty()
            && fixture.trace().scans.is_empty()
            && fixture.trace().batches.is_empty()
    );
    assert_eq!(fixture.snapshot(), before);
    Ok(())
}

fn touched_corruption_and_collision_priority<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let mut writer = fixture.writer();
    writer.apply(1, &proposal(&queue(), 10, create_queue()))?;
    let config_key = keys::queue_config(&namespace(), &queue());
    let original_profile = fixture.store().inner.get(&config_key)?.unwrap();
    fixture.raw(WriteBatch::default().put(config_key.clone(), vec![255]));
    // Occupied-key collision keeps its ordinary priority; this does NOT claim
    // health of the profile bytes that this handler deliberately does not read.
    assert_eq!(
        writer.apply(2, &proposal(&queue(), 20, create_queue()))?,
        IndexedApplyOutcome::Refused(BrokerError::QueueAlreadyExists)
    );
    uncheckpointed(
        &fixture,
        &mut writer,
        3,
        &proposal(&queue(), 30, receive()),
        BrokerError::Codec(domain::CodecError::UnsupportedVersion { version: 255 }),
    );
    fixture.raw(WriteBatch::default().put(config_key, original_profile));
    *fixture.store().read_fault.lock().unwrap() = Some(keys::clock());
    uncheckpointed(
        &fixture,
        &mut writer,
        3,
        &proposal(&queue(), 30, receive()),
        BrokerError::Storage(simulated_error()),
    );
    *fixture.store().read_fault.lock().unwrap() = None;
    let mut bad_session_key = keys::session_lock_prefix(&namespace(), &queue());
    bad_session_key.extend_from_slice(&20_u64.to_be_bytes());
    fixture.raw(WriteBatch::default().put(bad_session_key.clone(), Vec::new()));
    uncheckpointed(
        &fixture,
        &mut writer,
        3,
        &proposal(&queue(), 30, CommandKind::ExpireSessionLocks),
        BrokerError::Identifier(domain::IdentifierError::Empty { kind: "session id" }),
    );
    fixture.raw(WriteBatch::default().delete(bad_session_key));
    drop(writer);
    fixture.clear();
    let mut writer = fixture.writer();
    writer.apply(
        1,
        &proposal(
            &topic(),
            10,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
    )?;
    writer.apply(
        2,
        &proposal(
            &topic(),
            20,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("worker")?,
                config: SubscriptionConfig::default(),
            },
        ),
    )?;
    let backing_key = keys::queue_config(&namespace(), &subscription());
    let backing = fixture.store().inner.get(&backing_key)?.unwrap();
    fixture.raw(WriteBatch::default().delete(backing_key.clone()));
    uncheckpointed(
        &fixture,
        &mut writer,
        3,
        &proposal(&topic(), 30, send("no-copy")),
        BrokerError::DanglingSubscription {
            entity: subscription(),
        },
    );
    fixture.raw(WriteBatch::default().put(backing_key, backing));
    let shadow_key = keys::queue_config(&namespace(), &subscription().dead_letter_queue()?);
    let shadow = fixture.store().inner.get(&shadow_key)?.unwrap();
    fixture.raw(WriteBatch::default().delete(shadow_key.clone()));
    uncheckpointed(
        &fixture,
        &mut writer,
        3,
        &proposal(&topic(), 30, send("no-copy")),
        BrokerError::TopicTopologyCorrupt,
    );
    fixture.raw(WriteBatch::default().put(shadow_key, shadow));
    let malformed = keys::topic_subscription_prefix(&namespace(), &topic());
    fixture.raw(WriteBatch::default().put(malformed, vec![1]));
    uncheckpointed(
        &fixture,
        &mut writer,
        3,
        &proposal(&topic(), 30, send("no-copy")),
        BrokerError::MalformedIndexKey,
    );
    let before = fixture.snapshot();
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.writer().applied_index()?, 2);
    assert_eq!(fixture.snapshot(), before);
    Ok(())
}

macro_rules! paired {
    ($memory:ident, $fjall:ident, $body:ident) => {
        #[test]
        fn $memory() -> TestResult {
            $body(MemoryProvider::new())
        }
        #[test]
        fn $fjall() -> TestResult {
            $body(DurableProvider::temporary()?)
        }
    };
}

paired!(
    memory_exact_atomic_batch_matches_ordinary_apply,
    fjall_exact_atomic_batch_matches_ordinary_apply,
    exact_batch_parity
);
paired!(
    memory_noop_and_refusal_checkpoint_without_clock,
    fjall_noop_and_refusal_checkpoint_without_clock,
    noop_refusal_and_clock
);
paired!(
    memory_checked_indexes_and_outcome_free_duplicate,
    fjall_checked_indexes_and_outcome_free_duplicate,
    indexes_and_duplicate_reads
);
paired!(
    memory_empty_adoption_all_tags_and_f0_coexistence,
    fjall_empty_adoption_all_tags_and_f0_coexistence,
    empty_adoption_and_f0
);
paired!(
    memory_malformed_partial_unknown_f1_refuses_open,
    fjall_malformed_partial_unknown_f1_refuses_open,
    malformed_ownership
);
paired!(
    memory_ambiguous_apply_is_atomic_and_reopen_resolves_retry,
    fjall_ambiguous_apply_is_atomic_and_reopen_resolves_retry,
    ambiguous_apply_and_reopen
);
paired!(
    memory_lock_and_schedule_error_origins_discard_staged_effects,
    fjall_lock_and_schedule_error_origins_discard_staged_effects,
    lock_and_schedule_origins
);
paired!(
    memory_rule_configuration_origin_is_context_safe,
    fjall_rule_configuration_origin_is_context_safe,
    rule_config_origin
);
paired!(
    memory_full_and_overfull_rule_caps_never_checkpoint,
    fjall_full_and_overfull_rule_caps_never_checkpoint,
    rule_caps_are_conservative
);
paired!(
    memory_full_and_overfull_subscription_caps_never_checkpoint,
    fjall_full_and_overfull_subscription_caps_never_checkpoint,
    subscription_caps_are_conservative
);
paired!(
    memory_partial_receive_and_duplicate_history_failures_discard,
    fjall_partial_receive_and_duplicate_history_failures_discard,
    partial_receive_and_duplicate_failure
);
paired!(
    memory_multi_record_refusals_conserve_every_effect,
    fjall_multi_record_refusals_conserve_every_effect,
    multiple_requested_refusal_conservation
);
paired!(
    memory_incoming_configs_checkpoint_only_known_refusals,
    fjall_incoming_configs_checkpoint_only_known_refusals,
    incoming_config_refusals
);
paired!(
    memory_bound_scope_authority_clock_and_duplicate_priority,
    fjall_bound_scope_authority_clock_and_duplicate_priority,
    bound_priority_and_duplicate
);
paired!(
    memory_touched_corruption_and_collision_error_priority,
    fjall_touched_corruption_and_collision_error_priority,
    touched_corruption_and_collision_priority
);
