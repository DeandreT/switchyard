#![cfg(feature = "capacity-conservation-test-support")]

#[path = "../../domain/tests/support/capacity_oracle.rs"]
mod oracle;
use cluster::{CapacityReplayHarness, CommittedReplay, Journal};
use domain::{
    BoundCommand, BrokerError, Command, CommandKind as C, DurableProposal, EntityPath,
    IndexedApplyError, IndexedApplyOutcome as Outcome, IndexedWriter, LockToken, MessageContent,
    NamespaceName, QueueCapacity, QueueConfig, RawMessage, ReceiveMode, SequenceNumber as S,
    Timestamp,
    capacity_conservation::{CapacityHarness, CapacityIndexedHarness, TestGenesis, TestOriginal},
    capacity_fence::CapacityIntent,
};
use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;
fn genesis() -> Result<TestGenesis> {
    Ok(TestGenesis::new(
        NamespaceName::new("tenant")?,
        EntityPath::new("orders")?,
        QueueConfig {
            capacity: QueueCapacity::Finite {
                max_size_bytes: 1024 * 1024,
            },
            max_message_bytes: 1024 * 1024,
            ..QueueConfig::default()
        },
    )?)
}
fn send(size: usize) -> C {
    C::SendContent {
        content: MessageContent::Raw(RawMessage {
            body: vec![3; size],
            ..RawMessage::default()
        }),
    }
}
fn originals<Ss: StateStore>(store: &Ss, g: &TestGenesis) -> Result<Vec<TestOriginal>> {
    let actor = CapacityHarness::open(store.clone(), g.clone())?;
    let binding = actor.bind(g.owner().parent())?;
    drop(actor);
    let mut result = Vec::new();
    for (time, kind) in [
        (1, send(600_000)),
        (2, send(600_000)),
        (
            4,
            C::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        ),
        (
            5,
            C::Defer {
                sequence: S::new(1),
                lock_token: LockToken::new(1),
                replacement_envelope: None,
            },
        ),
        (
            6,
            C::ReceiveDeferred {
                sequences: vec![S::new(1)],
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        ),
        (
            7,
            C::SendContent {
                content: MessageContent::Raw(RawMessage {
                    scheduled_enqueue_at: Some(Timestamp::from_millis(100)),
                    ..RawMessage::default()
                }),
            },
        ),
        (128, C::ActivateScheduled),
        (
            129,
            C::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        ),
    ] {
        let command = Command::new(
            g.owner().namespace().clone(),
            g.owner().parent().clone(),
            Timestamp::from_millis(time),
            kind,
        );
        result.push(TestOriginal::Owned(CapacityIntent::capture(
            g.owner().clone(),
            DurableProposal::bound(BoundCommand::new(binding.clone(), command))?,
        )?));
    }
    result.insert(
        2,
        TestOriginal::Ordinary(DurableProposal::unbound(Command::new(
            g.owner().namespace().clone(),
            EntityPath::new("other")?,
            Timestamp::from_millis(3),
            C::CreateQueue {
                config: QueueConfig::default(),
            },
        ))),
    );
    Ok(result)
}
fn append<Ss: StateStore>(store: &Ss, originals: &[TestOriginal]) -> Result {
    let mut journal = Journal::open(store.clone())?;
    for (i, original) in originals.iter().enumerate() {
        journal.append((i + 1) as u64, &original.encode()?)?;
        journal.commit((i + 1) as u64)?;
    }
    Ok(())
}
fn clear<Ss: StateStore>(store: &Ss) -> Result {
    let mut b = WriteBatch::default();
    for (k, _) in store.snapshot()?.entries() {
        b.push_delete(k.clone());
    }
    store.apply(b)?;
    Ok(())
}

fn replay<P: StoreProvider>(provider: P) -> Result {
    let g = genesis()?;
    let store = provider.open()?;
    g.initialize(&store)?;
    let originals = originals(&store, &g)?;
    append(&store, &originals)?;
    let initial = store.snapshot()?;
    assert!(CommittedReplay::open(store.clone()).is_err());
    assert_eq!(store.snapshot()?, initial);
    assert!(IndexedWriter::open(store.clone()).is_err());
    let counted = Counted {
        inner: store.clone(),
        reads: Arc::new(AtomicUsize::new(0)),
        failure: Arc::new(AtomicUsize::new(0)),
    };
    let mut writer = CapacityIndexedHarness::open(counted.clone(), g.clone())?;
    for (i, original) in originals.iter().enumerate() {
        let before = store.snapshot()?;
        let outcome = writer.apply((i + 1) as u64, original)?;
        if i == 1 {
            assert!(matches!(
                outcome,
                Outcome::Refused(BrokerError::RetainedCapacityExceeded { .. })
            ));
            let effects = store
                .snapshot()?
                .entries()
                .iter()
                .filter(|(k, _)| k.first() != Some(&0xF1))
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(
                effects,
                before
                    .entries()
                    .iter()
                    .filter(|(k, _)| k.first() != Some(&0xF1))
                    .cloned()
                    .collect::<Vec<_>>()
            );
        } else {
            assert!(matches!(outcome, Outcome::Applied(_)));
        }
        oracle::observe(store.snapshot()?.entries(), g.owner());
    }
    counted.reads.store(0, Ordering::SeqCst);
    assert_eq!(writer.apply(9, &originals[8])?, Outcome::AlreadyApplied);
    writer.verify_latest_applied(&originals[8])?;
    assert_eq!(counted.reads.load(Ordering::SeqCst), 0);
    let mut different = originals[8].encode()?;
    different[12..20].copy_from_slice(&2u64.to_be_bytes());
    // A bound V11 with changed generation is invalid at decode, not silently unbound.
    assert!(TestOriginal::decode(&different).is_err());
    assert!(matches!(
        writer.apply(9, &originals[0]),
        Err(IndexedApplyError::ConflictingProposal { .. })
    ));
    assert!(matches!(
        writer.apply(11, &originals[0]),
        Err(IndexedApplyError::UnexpectedIndex { .. })
    ));
    let complete = store.snapshot()?;
    drop(writer);
    drop(counted);
    drop(store);
    let store = provider.open()?;
    assert_eq!(store.snapshot()?, complete);
    let mut replay = CapacityReplayHarness::open(store.clone(), g.clone())?;
    assert_eq!(replay.applied_index()?, 9);
    assert_eq!(replay.replay_batch(64)?.processed, 0);
    drop(replay);
    assert!(
        CapacityHarness::open(store.clone(), g.clone()).is_err(),
        "no unindexed facade over an indexed owner"
    );
    // Actual journal originals replay from the exact same explicit fixture
    // genesis. No synthetic checkpoint or recovered outcome is inserted.
    clear(&store)?;
    g.initialize(&store)?;
    append(&store, &originals)?;
    drop(store);
    let store = provider.open()?;
    let mut replay = CapacityReplayHarness::open(store.clone(), g.clone())?;
    while replay.applied_index()? < 9 {
        assert_eq!(replay.replay_batch(1)?.processed, 1);
        oracle::observe(store.snapshot()?.entries(), g.owner());
    }
    assert_eq!(store.snapshot()?, complete);
    drop(replay);
    drop(store);
    let store = provider.open()?;
    oracle::observe(store.snapshot()?.entries(), g.owner());
    assert!(CommittedReplay::open(store.clone()).is_err());
    Ok(())
}

fn preflight<P: StoreProvider>(provider: P) -> Result {
    let g = genesis()?;
    let store = provider.open()?;
    g.initialize(&store)?;
    let original = originals(&store, &g)?.remove(0);
    for bad in [vec![1, 2, 3], {
        let mut b = original.encode()?;
        let n = u16::from_be_bytes(b[20..22].try_into()?) as usize;
        let p = u16::from_be_bytes(b[22..24].try_into()?) as usize;
        b[24 + n + p + 4..24 + n + p + 8].copy_from_slice(&11u32.to_be_bytes());
        b
    }] {
        let mut journal = Journal::open(store.clone())?;
        journal.append(1, &original.encode()?)?;
        journal.append(2, &bad)?;
        journal.commit(2)?;
        drop(journal);
        let before = store.snapshot()?;
        assert!(CapacityReplayHarness::open(store.clone(), g.clone()).is_err());
        assert_eq!(store.snapshot()?, before);
        oracle::observe(store.snapshot()?.entries(), g.owner());
        clear(&store)?;
        g.initialize(&store)?;
    }
    let mut journal = Journal::open(store.clone())?;
    journal.append(1, &original.encode()?)?;
    journal.commit(1)?;
    journal.append(2, &[0xff])?;
    drop(journal);
    let mut replay = CapacityReplayHarness::open(store.clone(), g.clone())?;
    assert_eq!(replay.replay_batch(64)?.processed, 1);
    drop(replay);
    oracle::observe(store.snapshot()?.entries(), g.owner());
    drop(store);
    let store = provider.open()?;
    let replay = CapacityReplayHarness::open(store.clone(), g.clone())?;
    assert_eq!(replay.applied_index()?, 1);
    drop(replay);
    // Exact-genesis exception is not arbitrary populated-state adoption.
    let mut b = WriteBatch::default();
    for (k, _) in store.snapshot()?.entries() {
        if k.first() == Some(&0xF1) {
            b.push_delete(k.clone());
        }
    }
    store.apply(b)?;
    let before = store.snapshot()?;
    assert!(CapacityIndexedHarness::open(store.clone(), g.clone()).is_err());
    assert_eq!(store.snapshot()?, before);
    Ok(())
}

#[derive(Clone, Debug)]
struct Counted<Ss> {
    inner: Ss,
    reads: Arc<AtomicUsize>,
    failure: Arc<AtomicUsize>,
}
impl<Ss: StateStore> StateStore for Counted<Ss> {
    fn get(&self, k: &[u8]) -> std::result::Result<Option<Value>, StorageError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(k)
    }
    fn scan_from(
        &self,
        p: &[u8],
        s: &[u8],
        l: usize,
    ) -> std::result::Result<Vec<(Key, Value)>, StorageError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(p, s, l)
    }
    fn snapshot(&self) -> std::result::Result<StoreSnapshot, StorageError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.snapshot()
    }
    fn apply(&self, b: WriteBatch) -> std::result::Result<(), StorageError> {
        let mode = self.failure.load(Ordering::SeqCst);
        if mode == 1 {
            return Err(fault());
        }
        self.inner.apply(b)?;
        if mode == 2 { Err(fault()) } else { Ok(()) }
    }
}
fn fault() -> StorageError {
    StorageError::Backend {
        operation: "injected indexed apply",
        detail: "fixture".into(),
    }
}
fn failures<P: StoreProvider>(provider: P) -> Result {
    let g = genesis()?;
    let store = provider.open()?;
    g.initialize(&store)?;
    let original = originals(&store, &g)?.remove(0);
    append(&store, std::slice::from_ref(&original))?;
    let before = store.snapshot()?;
    for mode in [1, 2] {
        let counted = Counted {
            inner: store.clone(),
            reads: Arc::new(AtomicUsize::new(0)),
            failure: Arc::new(AtomicUsize::new(0)),
        };
        let mut replay = CapacityReplayHarness::open(counted.clone(), g.clone())?;
        counted.failure.store(mode, Ordering::SeqCst);
        assert!(replay.replay_batch(1).is_err());
        assert!(replay.applied_index().is_err());
        drop(replay);
        drop(counted);
        let state = store.snapshot()?;
        oracle::observe(state.entries(), g.owner());
        if mode == 1 {
            assert_eq!(state, before);
        } else {
            assert!(
                state
                    .entries()
                    .iter()
                    .any(|(k, _)| k.first() == Some(&0xF1))
            );
        }
        let replay = CapacityReplayHarness::open(store.clone(), g.clone())?;
        assert_eq!(replay.applied_index()?, if mode == 1 { 0 } else { 1 });
        drop(replay);
    }
    let final_image = store.snapshot()?;
    drop(store);
    let store = provider.open()?;
    assert_eq!(store.snapshot()?, final_image);
    oracle::observe(final_image.entries(), g.owner());
    Ok(())
}
macro_rules! paired {
    ($m:ident,$f:ident,$body:ident) => {
        #[test]
        fn $m() -> Result {
            $body(MemoryProvider::new())
        }
        #[test]
        fn $f() -> Result {
            $body(DurableProvider::temporary()?)
        }
    };
}
paired!(
    memory_real_journal_replay,
    fjall_real_journal_replay,
    replay
);
paired!(
    memory_total_schema_preflight,
    fjall_total_schema_preflight,
    preflight
);
paired!(
    memory_ambiguous_replay_retirement,
    fjall_ambiguous_replay_retirement,
    failures
);

/// A controlled, consistent read mask used only to reach the application
/// boundary over an intentionally corrupt stopped-owner fixture. It is not a
/// concurrent raw writer, public adoption API, or replacement indexed evaluator.
#[derive(Clone, Debug)]
struct LedgerFault<Ss> {
    inner: Ss,
    replacement: Arc<std::sync::Mutex<Option<(Key, Value)>>>,
    applies: Arc<AtomicUsize>,
}
impl<Ss: StateStore> LedgerFault<Ss> {
    fn overlay(&self, key: &[u8], value: Value) -> Value {
        self.replacement
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(k, _)| k.as_slice() == key)
            .map_or(value, |(_, wrong)| wrong.clone())
    }
}
impl<Ss: StateStore> StateStore for LedgerFault<Ss> {
    fn get(&self, key: &[u8]) -> std::result::Result<Option<Value>, StorageError> {
        Ok(self.inner.get(key)?.map(|v| self.overlay(key, v)))
    }
    fn scan_from(
        &self,
        p: &[u8],
        s: &[u8],
        n: usize,
    ) -> std::result::Result<Vec<(Key, Value)>, StorageError> {
        Ok(self
            .inner
            .scan_from(p, s, n)?
            .into_iter()
            .map(|(k, v)| {
                let v = self.overlay(&k, v);
                (k, v)
            })
            .collect())
    }
    fn snapshot(&self) -> std::result::Result<StoreSnapshot, StorageError> {
        let memory = storage::MemoryStore::default();
        let mut b = WriteBatch::default();
        for (k, v) in self.inner.snapshot()?.entries() {
            b.push_put(k.clone(), self.overlay(k, v.clone()));
        }
        memory.apply(b)?;
        memory.snapshot()
    }
    fn apply(&self, b: WriteBatch) -> std::result::Result<(), StorageError> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(b)
    }
}

fn wrong_totals_application<P: StoreProvider>(provider: P) -> Result {
    use domain::retained_accounting::{RetainedUsage, keys::usage, usage_codec};
    let g = genesis()?;
    let store = provider.open()?;
    g.initialize(&store)?;
    let mut originals = originals(&store, &g)?;
    originals.truncate(2);
    append(&store, &originals)?;
    let faulted = LedgerFault {
        inner: store.clone(),
        replacement: Arc::new(std::sync::Mutex::new(None)),
        applies: Arc::new(AtomicUsize::new(0)),
    };
    let mut replay = CapacityReplayHarness::open(faulted.clone(), g.clone())?;
    assert_eq!(replay.replay_batch(1)?.applied_index, 1);
    let healthy = store.snapshot()?;
    let actual = oracle::observe(healthy.entries(), g.owner()).usage;
    let wrong = RetainedUsage::new(
        g.owner().generation(),
        actual.retained_bytes() + 1,
        actual.reserved_growth_bytes() - 1,
        actual.message_count(),
    )?;
    assert_eq!(wrong.committed_bytes()?, actual.committed_bytes()?);
    assert!(wrong.committed_bytes()? < 1024 * 1024);
    let encoded = usage_codec::encode(g.owner(), wrong)?;
    assert_eq!(
        usage_codec::decode(g.owner(), &encoded)?,
        wrong,
        "valid checksum/model/owner, not malformed usage"
    );
    let healthy_encoded = usage_codec::encode(g.owner(), actual)?;
    // Stop the real first owner and close every backend handle before seeding
    // actual, persistent wrong totals. The F1 checkpoint is genuine index 1.
    drop(replay);
    drop(faulted);
    drop(store);
    let store = provider.open()?;
    store.apply(WriteBatch::default().put(usage(g.owner()), encoded))?;
    let fault_image = store.snapshot()?;
    assert_eq!(
        oracle::recompute(fault_image.entries(), g.owner()).usage,
        actual
    );
    // Opening without a mask must already reject this fixture. To additionally
    // prove APPLICATION (not merely opener validation), an internal test store
    // supplies the previously validated ledger during open, then reveals the
    // actual corrupt bytes before the next typed indexed prepare. No public
    // validator callback or populated-image exception is introduced.
    assert!(CapacityReplayHarness::open(store.clone(), g.clone()).is_err());
    assert_eq!(store.snapshot()?, fault_image);
    let faulted = LedgerFault {
        inner: store.clone(),
        replacement: Arc::new(std::sync::Mutex::new(Some((
            usage(g.owner()),
            healthy_encoded.clone(),
        )))),
        applies: Arc::new(AtomicUsize::new(0)),
    };
    let mut replay = CapacityReplayHarness::open(faulted.clone(), g.clone())?;
    assert_eq!(replay.applied_index()?, 1);
    assert_eq!(faulted.applies.load(Ordering::SeqCst), 0);
    *faulted.replacement.lock().unwrap() = None;
    assert_eq!(faulted.snapshot()?, fault_image);
    // This reaches the real typed indexed prepare through replay_batch. The
    // second committed send would be a healthy quota refusal on a healthy image.
    assert!(matches!(
        replay.replay_batch(1),
        Err(cluster::ReplayError::Indexed(IndexedApplyError::Domain(
            BrokerError::CapacityStateCorrupt
        )))
    ));
    assert_eq!(
        faulted.applies.load(Ordering::SeqCst),
        0,
        "not even an F1-only refusal checkpoint"
    );
    assert_eq!(faulted.snapshot()?, fault_image);
    let checkpoints = |s: &StoreSnapshot| {
        s.entries()
            .iter()
            .filter(|(k, _)| k.first() == Some(&0xF1))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(checkpoints(&faulted.snapshot()?), checkpoints(&healthy));
    assert!(matches!(
        replay.applied_index(),
        Err(cluster::ReplayError::Unusable)
    ));
    assert!(matches!(
        replay.latest_applied_digest(),
        Err(cluster::ReplayError::Unusable)
    ));
    assert!(matches!(
        replay.replay_batch(1),
        Err(cluster::ReplayError::Unusable)
    ));
    // A seemingly healthy read surface does not resurrect the retired owner.
    *faulted.replacement.lock().unwrap() = Some((usage(g.owner()), healthy_encoded.clone()));
    assert!(matches!(
        replay.replay_batch(1),
        Err(cluster::ReplayError::Unusable)
    ));
    assert_eq!(faulted.applies.load(Ordering::SeqCst), 0);
    assert_eq!(store.snapshot()?, fault_image);
    drop(replay);
    drop(faulted);
    drop(store);
    // A physical reopen still finds the wrong totals and cannot serve. Only an
    // explicit stopped-owner fixture restoration followed by another cold open
    // can establish a new, healthy owner. No production repair path is added.
    let store = provider.open()?;
    assert_eq!(store.snapshot()?, fault_image);
    assert!(CapacityReplayHarness::open(store.clone(), g.clone()).is_err());
    assert_eq!(store.snapshot()?, fault_image);
    store.apply(WriteBatch::default().put(usage(g.owner()), healthy_encoded))?;
    assert_eq!(store.snapshot()?, healthy);
    drop(store);
    let store = provider.open()?;
    assert_eq!(store.snapshot()?, healthy);
    oracle::observe(healthy.entries(), g.owner());
    let mut reopened = CapacityReplayHarness::open(store.clone(), g.clone())?;
    assert_eq!(reopened.applied_index()?, 1);
    assert_eq!(reopened.replay_batch(1)?.applied_index, 2);
    assert_eq!(
        oracle::observe(store.snapshot()?.entries(), g.owner()).usage,
        actual
    );
    Ok(())
}
paired!(
    memory_valid_wrong_totals_are_fatal_in_typed_apply,
    fjall_valid_wrong_totals_are_fatal_in_typed_apply,
    wrong_totals_application
);
