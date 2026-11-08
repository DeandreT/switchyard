use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
};

use cluster::consensus::{
    ConsensusError, ConsensusIdentity, ConsensusStore, EntryKind, MAX_CONSENSUS_ENTRY_BYTES,
    MAX_CONSENSUS_PAGE_ENTRIES, MAX_CONSENSUS_PROPOSAL_BYTES, RetainedLogLimits, Voter, entry_kind,
};
use domain::{
    BoundCommand, Command, CommandKind, DurableProposal, EntityPath, NamespaceName, QueueConfig,
    StateMachine, Timestamp,
};
use protobuf::Message;
use raft::{
    GetEntriesContext, RawNode, Storage,
    eraftpb::{ConfChangeSingle, ConfChangeV2, Entry, EntryType, HardState},
};
use storage::{
    ConsensusBackend, ConsensusFjallStore, ConsensusMemoryStore, Key, Mutation, StorageError,
    Value, WriteBatch,
};

#[derive(Default)]
struct Control {
    fault: AtomicU8,
    writes: Mutex<Vec<WriteBatch>>,
}

struct Observed<B> {
    inner: B,
    control: Arc<Control>,
}

impl<B: ConsensusBackend> ConsensusBackend for Observed<B> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
    fn apply(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        let fault = self.control.fault.swap(0, Ordering::SeqCst);
        if fault == 1 {
            return Err(failure());
        }
        self.inner.apply(batch.clone())?;
        self.control.writes.lock().unwrap().push(batch);
        if fault == 2 {
            return Err(failure());
        }
        assert_ne!(fault, 3, "original after-write panic");
        Ok(())
    }
}

fn failure() -> StorageError {
    StorageError::Backend {
        operation: "injected consensus apply",
        detail: "ambiguous original result".into(),
    }
}

enum Backend {
    Memory(ConsensusMemoryStore),
    Fjall(ConsensusFjallStore),
}
impl ConsensusBackend for Backend {
    fn get(&self, k: &[u8]) -> Result<Option<Value>, StorageError> {
        match self {
            Self::Memory(b) => b.get(k),
            Self::Fjall(b) => b.get(k),
        }
    }
    fn scan_from(&self, p: &[u8], s: &[u8], n: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        match self {
            Self::Memory(b) => b.scan_from(p, s, n),
            Self::Fjall(b) => b.scan_from(p, s, n),
        }
    }
    fn apply(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        match self {
            Self::Memory(b) => b.apply(batch),
            Self::Fjall(b) => b.apply(batch),
        }
    }
}

fn fixture(durable: bool) -> (Observed<Backend>, Arc<Control>, Option<tempfile::TempDir>) {
    let directory = durable.then(|| tempfile::tempdir().unwrap());
    let inner = match &directory {
        Some(d) => Backend::Fjall(ConsensusFjallStore::open(d.path()).unwrap()),
        None => Backend::Memory(ConsensusMemoryStore::default()),
    };
    let control = Arc::new(Control::default());
    (
        Observed {
            inner,
            control: control.clone(),
        },
        control,
        directory,
    )
}

fn identity() -> ConsensusIdentity {
    ConsensusIdentity::new(
        "cluster-a".into(),
        "group-a".into(),
        NamespaceName::new("tenant").unwrap(),
        [
            Voter {
                id: 1,
                endpoint: "127.0.0.1:10101".parse().unwrap(),
            },
            Voter {
                id: 2,
                endpoint: "127.0.0.1:10102".parse().unwrap(),
            },
            Voter {
                id: 3,
                endpoint: "127.0.0.1:10103".parse().unwrap(),
            },
        ],
    )
    .unwrap()
}
fn hs(term: u64, vote: u64, commit: u64) -> HardState {
    HardState {
        term,
        vote,
        commit,
        ..Default::default()
    }
}
fn blank(index: u64, term: u64) -> Entry {
    Entry {
        index,
        term,
        ..Default::default()
    }
}
fn proposal_bytes() -> Vec<u8> {
    DurableProposal::unbound(Command::new(
        NamespaceName::new("tenant").unwrap(),
        EntityPath::new("orders").unwrap(),
        Timestamp::from_millis(1234),
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    ))
    .encode()
    .unwrap()
}
fn proposal(index: u64, term: u64) -> Entry {
    let mut e = blank(index, term);
    e.data = proposal_bytes().into();
    e
}
fn config(index: u64, term: u64) -> Entry {
    let mut e = blank(index, term);
    e.set_entry_type(EntryType::EntryConfChangeV2);
    e.data = ConfChangeV2::default().write_to_bytes().unwrap().into();
    e
}
fn rows(control: &Control) -> BTreeMap<Key, Value> {
    let mut result = BTreeMap::new();
    for batch in control.writes.lock().unwrap().iter() {
        for m in batch.mutations() {
            match m {
                Mutation::Put { key, value } => {
                    result.insert(key.clone(), value.clone());
                }
                Mutation::Delete { key } => {
                    result.remove(key);
                }
            }
        }
    }
    result
}

#[test]
fn immutable_identity_is_canonical_and_fixed_three() {
    let expected = identity();
    let mut voters = expected.voters().clone();
    voters.swap(0, 2);
    let reordered = ConsensusIdentity::new(
        "cluster-a".into(),
        "group-a".into(),
        NamespaceName::new("TENANT").unwrap(),
        voters.clone(),
    )
    .unwrap();
    assert_eq!(expected.fingerprint(), reordered.fingerprint());
    voters[0].id = voters[1].id;
    assert!(
        ConsensusIdentity::new(
            "a".into(),
            "b".into(),
            NamespaceName::new("tenant").unwrap(),
            voters
        )
        .is_err()
    );
    let mut voters = expected.voters().clone();
    voters[0].endpoint = voters[1].endpoint;
    assert!(
        ConsensusIdentity::new(
            "a".into(),
            "b".into(),
            NamespaceName::new("tenant").unwrap(),
            voters
        )
        .is_err()
    );
    let mut voters = expected.voters().clone();
    voters[0].id = 0;
    assert!(
        ConsensusIdentity::new(
            "a".into(),
            "b".into(),
            NamespaceName::new("tenant").unwrap(),
            voters
        )
        .is_err()
    );
    assert!(
        ConsensusStore::bootstrap(
            ConsensusMemoryStore::default(),
            expected,
            4,
            RetainedLogLimits::default()
        )
        .is_err()
    );
}

#[test]
fn durable_roundtrip_controls_and_application_mapping() {
    for durable in [false, true] {
        let (backend, control, directory) = fixture(durable);
        let mut store =
            ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default())
                .unwrap();
        let entries = vec![blank(1, 1), proposal(2, 1), blank(3, 1), proposal(4, 1)];
        store.persist(&entries, Some(&hs(1, 2, 4))).unwrap();
        assert_eq!(store.read_entries(1, 64).unwrap(), entries);
        assert_eq!(entry_kind(&entries[0]), EntryKind::Blank);
        assert_eq!(entry_kind(&config(3, 1)), EntryKind::Configuration);
        assert_eq!(store.application_ordinal(1).unwrap(), None);
        assert_eq!(store.application_ordinal(2).unwrap(), Some(1));
        assert_eq!(store.application_ordinal(3).unwrap(), None);
        assert_eq!(store.application_ordinal(4).unwrap(), Some(2));
        let before = rows(&control);
        let store = store.reopen().unwrap();
        assert_eq!(before, rows(&control));
        assert_eq!(store.hard_state().unwrap(), &hs(1, 2, 4));
        assert_eq!(Storage::term(&store, 0).unwrap(), 0);
        assert_eq!(Storage::first_index(&store).unwrap(), 1);
        assert!(matches!(
            Storage::snapshot(&store, 1, 2),
            Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable
            ))
        ));
        drop(store);
        if let Some(directory) = directory {
            let reopened = ConsensusStore::open(
                ConsensusFjallStore::open(directory.path()).unwrap(),
                identity(),
                1,
                RetainedLogLimits::default(),
            )
            .unwrap();
            assert_eq!(reopened.read_entries(1, 64).unwrap(), entries);
            assert_eq!(reopened.application_ordinal(4).unwrap(), Some(2));
        }
    }
}

#[test]
fn atomic_conflict_replacement_never_truncates_commit_or_matching_suffix() {
    for durable in [false, true] {
        let (backend, control, _directory) = fixture(durable);
        let mut store =
            ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default())
                .unwrap();
        store
            .persist(
                &[blank(1, 1), proposal(2, 1), blank(3, 1)],
                Some(&hs(1, 1, 1)),
            )
            .unwrap();
        store
            .persist(&[proposal(2, 2)], Some(&hs(2, 2, 1)))
            .unwrap();
        assert_eq!(store.last_index().unwrap(), 2);
        assert_eq!(
            store.read_entries(1, 64).unwrap(),
            vec![blank(1, 1), proposal(2, 2)]
        );
        store.persist(&[blank(3, 2)], None).unwrap();
        let before = rows(&control);
        store.persist(&[proposal(2, 2)], None).unwrap();
        assert_eq!(store.last_index().unwrap(), 3);
        assert_eq!(before, rows(&control));
        for (entries, state) in [
            (vec![blank(1, 3)], hs(3, 3, 1)),
            (vec![blank(2, 2)], hs(2, 2, 1)),
            (vec![blank(5, 2)], hs(2, 2, 1)),
            (vec![blank(4, 1)], hs(2, 2, 1)),
            (vec![], hs(1, 1, 1)),
            (vec![], hs(2, 3, 1)),
            (vec![], hs(2, 0, 1)),
            (vec![], hs(2, 2, 0)),
            (vec![], hs(2, 2, 4)),
            (vec![], hs(3, 99, 1)),
            (vec![], hs(u64::MAX, 0, 1)),
        ] {
            assert!(store.persist(&entries, Some(&state)).is_err());
            assert!(!store.is_retired());
            assert_eq!(before, rows(&control));
        }
        store.persist(&[], Some(&hs(3, 0, 3))).unwrap();
        assert_eq!(store.reopen().unwrap().hard_state().unwrap(), &hs(3, 0, 3));
    }
}

#[test]
fn same_term_bytes_conflict_after_replacement_boundary_refuses_unchanged() {
    for durable in [false, true] {
        let (backend, control, directory) = fixture(durable);
        let mut store =
            ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default())
                .unwrap();
        let original = vec![blank(1, 1), blank(2, 1), proposal(3, 2)];
        let state = hs(2, 1, 1);
        store.persist(&original, Some(&state)).unwrap();
        let before = rows(&control);
        let writes = control.writes.lock().unwrap().len();
        let bytes = store.retained_bytes().unwrap();

        // The differing-term boundary at 2 must not bypass the same-term
        // proposal/blank conflict at 3. This tests storage admission, not live
        // Raft output or a demonstrated quorum-safety failure.
        assert!(matches!(
            store.persist(&[blank(2, 2), blank(3, 2)], None),
            Err(ConsensusError::InvalidUpdate(
                "same-term entry bytes conflict"
            ))
        ));
        assert!(!store.is_retired());
        assert_eq!(control.writes.lock().unwrap().len(), writes);
        assert_eq!(rows(&control), before);
        assert_eq!(store.hard_state().unwrap(), &state);
        assert_eq!(store.last_index().unwrap(), 3);
        assert_eq!(store.retained_bytes().unwrap(), bytes);
        assert_eq!(store.read_entries(1, 64).unwrap(), original);

        let store = store.reopen().unwrap();
        assert_eq!(store.hard_state().unwrap(), &state);
        assert_eq!(store.read_entries(1, 64).unwrap(), original);
        assert_eq!(rows(&control), before);
        drop(store);
        if let Some(directory) = directory {
            let store = ConsensusStore::open(
                ConsensusFjallStore::open(directory.path()).unwrap(),
                identity(),
                1,
                RetainedLogLimits::default(),
            )
            .unwrap();
            assert_eq!(store.hard_state().unwrap(), &state);
            assert_eq!(store.read_entries(1, 64).unwrap(), original);
            assert_eq!(store.retained_bytes().unwrap(), bytes);
        }
    }
}

#[test]
fn later_overlaps_allow_matching_and_different_term_suffix_replacement() {
    for durable in [false, true] {
        let (backend, control, _directory) = fixture(durable);
        let mut store =
            ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default())
                .unwrap();
        store
            .persist(
                &[blank(1, 1), blank(2, 1), proposal(3, 2)],
                Some(&hs(2, 1, 1)),
            )
            .unwrap();
        // A later identical overlap must not move or erase the first boundary.
        store.persist(&[blank(2, 2), proposal(3, 2)], None).unwrap();
        assert_eq!(
            store.read_entries(1, 64).unwrap(),
            vec![blank(1, 1), blank(2, 2), proposal(3, 2)]
        );
        // Different-term overlaps remain replaceable strictly after commit.
        let replacement = [blank(2, 3), blank(3, 3)];
        store.persist(&replacement, Some(&hs(3, 1, 1))).unwrap();
        let before = rows(&control);
        let writes = control.writes.lock().unwrap().len();
        store.persist(&replacement, None).unwrap();
        assert_eq!(control.writes.lock().unwrap().len(), writes);
        assert_eq!(rows(&control), before);
        let store = store.reopen().unwrap();
        assert_eq!(store.hard_state().unwrap(), &hs(3, 1, 1));
        assert_eq!(
            store.read_entries(1, 64).unwrap(),
            vec![blank(1, 1), blank(2, 3), blank(3, 3)]
        );
    }
}

#[test]
fn ambiguous_errors_and_original_panics_retire_until_reopen() {
    for durable in [false, true] {
        for fault in [1, 2, 3] {
            let (backend, control, _directory) = fixture(durable);
            let mut store =
                ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default())
                    .unwrap();
            control.fault.store(fault, Ordering::SeqCst);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                store.persist(&[proposal(1, 1)], Some(&hs(1, 1, 1)))
            }));
            if fault == 3 {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert!(store.is_retired());
            assert!(matches!(
                store.read_entries(1, 1),
                Err(ConsensusError::Retired)
            ));
            assert!(matches!(store.hard_state(), Err(ConsensusError::Retired)));
            assert!(store.persist(&[], None).is_err());
            assert!(Storage::initial_state(&store).is_err());
            let reopened = store.reopen().unwrap();
            assert_eq!(
                reopened.last_index().unwrap(),
                if fault == 1 { 0 } else { 1 }
            );
            assert_eq!(
                reopened.hard_state().unwrap().commit,
                if fault == 1 { 0 } else { 1 }
            );
        }
    }
}

#[test]
fn retained_bounds_and_membership_refuse_without_mutation() {
    for durable in [false, true] {
        let (backend, control, _directory) = fixture(durable);
        let mut store = ConsensusStore::bootstrap(
            backend,
            identity(),
            1,
            RetainedLogLimits {
                max_entries: 2,
                ..RetainedLogLimits::default()
            },
        )
        .unwrap();
        let before = rows(&control);
        assert!(
            store
                .persist(&[blank(1, 1), blank(2, 1), blank(3, 1)], Some(&hs(1, 1, 0)))
                .is_err()
        );
        assert_eq!(before, rows(&control));
        let mut change = config(1, 1);
        let mut conf = ConfChangeV2::default();
        let single = ConfChangeSingle {
            node_id: 4,
            ..Default::default()
        };
        conf.changes.push(single);
        change.data = conf.write_to_bytes().unwrap().into();
        assert!(matches!(
            store.persist(&[change], Some(&hs(1, 1, 0))),
            Err(ConsensusError::MembershipChange)
        ));
        assert_eq!(before, rows(&control));
        assert!(matches!(
            store.persist(&[config(1, 1)], Some(&hs(1, 1, 0))),
            Err(ConsensusError::MembershipChange)
        ));
        let mut foreign = proposal(1, 1);
        foreign.data = DurableProposal::unbound(Command::new(
            NamespaceName::new("foreign").unwrap(),
            EntityPath::new("orders").unwrap(),
            Timestamp::from_millis(1234),
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ))
        .encode()
        .unwrap()
        .into();
        assert!(matches!(
            store.persist(&[foreign], Some(&hs(1, 1, 0))),
            Err(ConsensusError::ForeignIdentity)
        ));
        assert_eq!(before, rows(&control));
        store
            .persist(&[proposal(1, 1), blank(2, 1)], Some(&hs(1, 1, 2)))
            .unwrap();
        assert!(store.read_entries(0, 1).is_err());
        assert!(
            store
                .read_entries(1, MAX_CONSENSUS_PAGE_ENTRIES + 1)
                .is_err()
        );
        let entries =
            Storage::entries(&store, 1, 3, Some(0), GetEntriesContext::empty(false)).unwrap();
        assert_eq!(entries, vec![proposal(1, 1)]);
        assert!(matches!(
            Storage::entries(&store, 0, 1, None, GetEntriesContext::empty(false)),
            Err(raft::Error::Store(raft::StorageError::Compacted))
        ));
        assert!(matches!(
            Storage::entries(&store, 1, 4, None, GetEntriesContext::empty(false)),
            Err(raft::Error::Store(raft::StorageError::Unavailable))
        ));
        assert!(store.application_ordinal(3).is_err());
    }
}

#[test]
fn fresh_only_and_foreign_identity_refuse_without_writes() {
    for durable in [false, true] {
        for foreign in [
            b"\xF0local".as_slice(),
            b"\xF1local",
            b"\x01domain",
            b"\xF2unknown",
        ] {
            let (mut backend, control, _directory) = fixture(durable);
            backend
                .apply(WriteBatch::default().put(foreign.to_vec(), vec![1]))
                .unwrap();
            let before = rows(&control);
            assert!(
                ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default())
                    .is_err()
            );
            assert_eq!(before, rows(&control));
        }
        let (backend, control, directory) = fixture(durable);
        let store = ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default())
            .unwrap();
        let image = rows(&control);
        drop(store);
        for local in [2, 3] {
            let (mut target, target_control, _target_dir) = fixture(durable);
            let mut batch = WriteBatch::default();
            for (k, v) in &image {
                batch.push_put(k.clone(), v.clone());
            }
            target.apply(batch).unwrap();
            let before = rows(&target_control);
            assert!(matches!(
                ConsensusStore::open(target, identity(), local, RetainedLogLimits::default()),
                Err(ConsensusError::ForeignIdentity)
            ));
            assert_eq!(before, rows(&target_control));
        }
        if let Some(directory) = directory {
            let other = ConsensusIdentity::new(
                "other-cluster".into(),
                "group-a".into(),
                NamespaceName::new("tenant").unwrap(),
                identity().voters().clone(),
            )
            .unwrap();
            assert!(matches!(
                ConsensusStore::open(
                    ConsensusFjallStore::open(directory.path()).unwrap(),
                    other,
                    1,
                    RetainedLogLimits::default()
                ),
                Err(ConsensusError::ForeignIdentity)
            ));
        }
    }
}

#[test]
fn core_storage_and_exact_rawnode_api_contract_without_election() {
    let store = ConsensusStore::bootstrap(
        ConsensusMemoryStore::default(),
        identity(),
        1,
        RetainedLogLimits::default(),
    )
    .unwrap();
    let logger = slog::Logger::root(slog::Discard, slog::o!());
    let mut core = RawNode::new(
        &raft::Config {
            id: 1,
            ..Default::default()
        },
        store,
        &logger,
    )
    .unwrap();
    assert_eq!(
        core.store().initial_state().unwrap().conf_state.voters,
        vec![1, 2, 3]
    );
    // Compile and inspect the selected exact API; no campaign, peer, timer or
    // synthetic acknowledgement is used as a consensus demonstration.
    core.read_index(b"contract-probe".to_vec());
    if core.has_ready() {
        let ready = core.ready();
        assert!(ready.snapshot().is_empty());
        assert!(ready.entries().is_empty());
        assert!(ready.persisted_messages().is_empty());
        assert!(ready.read_states().is_empty());
        core.mut_store()
            .persist(ready.entries(), ready.hs())
            .unwrap();
        let light = core.advance_append(ready);
        assert!(light.committed_entries().is_empty());
        assert!(light.commit_index().is_none());
        core.advance_apply_to(0);
    }
}

#[test]
fn full_journal_sized_proposal_and_bound_authority_stay_exact() {
    let ns = NamespaceName::new("tenant").unwrap();
    let path = EntityPath::new("orders").unwrap();
    let machine = StateMachine::new(storage::MemoryStore::default());
    machine
        .apply(&Command::new(
            ns.clone(),
            path.clone(),
            Timestamp::from_millis(1),
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ))
        .unwrap();
    let binding = machine.bind_entity(&ns, &path).unwrap();
    let make = |size| {
        DurableProposal::bound(BoundCommand::new(
            binding.clone(),
            Command::new(
                ns.clone(),
                path.clone(),
                Timestamp::from_millis(9876),
                CommandKind::Send {
                    message_id: "historical".into(),
                    body: vec![7; size],
                    time_to_live_millis: Some(321),
                    session_id: None,
                    scheduled_enqueue_at: None,
                    envelope: None,
                },
            ),
        ))
        .unwrap()
        .encode()
    };
    let (mut low, mut high) = (0, MAX_CONSENSUS_PROPOSAL_BYTES);
    while low < high {
        let mid = (low + high).div_ceil(2);
        if make(mid).is_ok() {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    let bytes = make(low).unwrap();
    assert_eq!(bytes.len(), MAX_CONSENSUS_PROPOSAL_BYTES);
    let mut entry = blank(1, 1);
    entry.data = bytes.clone().into();
    entry.context = vec![0; cluster::consensus::MAX_CONSENSUS_CONTEXT_BYTES].into();
    assert!(entry.compute_size() as usize <= MAX_CONSENSUS_ENTRY_BYTES);
    for durable in [false, true] {
        let (backend, _control, _directory) = fixture(durable);
        let mut store =
            ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default())
                .unwrap();
        store
            .persist(std::slice::from_ref(&entry), Some(&hs(1, 1, 1)))
            .unwrap();
        let restored = store
            .reopen()
            .unwrap()
            .read_entries(1, 1)
            .unwrap()
            .remove(0);
        assert_eq!(restored.data.as_ref(), bytes);
        let original = DurableProposal::decode(&restored.data).unwrap();
        assert_eq!(original.encode().unwrap(), bytes);
        assert!(matches!(
            original.authority(),
            domain::DurableProposalAuthority::Bound(_)
        ));
        assert_eq!(original.command().issued_at, Timestamp::from_millis(9876));
    }
}

fn private_key(tag: u8) -> Vec<u8> {
    let mut k = b"\xF2switchyard/consensus\0".to_vec();
    k.push(tag);
    k
}
fn private_entry(index: u64) -> Vec<u8> {
    let mut k = private_key(2);
    k.extend_from_slice(&index.to_be_bytes());
    k
}
fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut bytes = 1u32.to_be_bytes().to_vec();
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(payload);
    let mut hash = Sha256::new();
    hash.update(b"switchyard consensus record v1\0");
    hash.update([tag]);
    hash.update(&bytes);
    bytes.extend_from_slice(&hash.finalize());
    bytes
}

#[test]
fn corruption_holes_versions_and_mixed_records_refuse_without_repair() {
    let (backend, control, _directory) = fixture(false);
    let mut owner =
        ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default()).unwrap();
    owner
        .persist(
            &[blank(1, 1), proposal(2, 1), blank(3, 1)],
            Some(&hs(1, 1, 2)),
        )
        .unwrap();
    let valid = rows(&control);
    drop(owner);
    for durable in [false, true] {
        for case in 0..18 {
            let (mut backend, control, directory) = fixture(durable);
            let mut batch = WriteBatch::default();
            for (k, v) in &valid {
                batch.push_put(k.clone(), v.clone());
            }
            backend.apply(batch).unwrap();
            let mutation = match case {
                0 => WriteBatch::default().delete(private_key(0)),
                1 => WriteBatch::default().delete(private_key(1)),
                2 => WriteBatch::default().delete(private_entry(1)),
                3 => WriteBatch::default().put(
                    private_entry(2),
                    frame(2, &proposal(3, 1).write_to_bytes().unwrap()),
                ),
                4 => WriteBatch::default().put(
                    private_key(1),
                    frame(1, &hs(1, 99, 2).write_to_bytes().unwrap()),
                ),
                5 => WriteBatch::default().put(
                    private_key(1),
                    frame(1, &hs(1, 1, 4).write_to_bytes().unwrap()),
                ),
                6 => WriteBatch::default().put(
                    private_key(1),
                    frame(1, &hs(0, 0, 0).write_to_bytes().unwrap()),
                ),
                7 => {
                    let mut raw = valid[&private_key(0)].clone();
                    raw[0..4].copy_from_slice(&2u32.to_be_bytes());
                    WriteBatch::default().put(private_key(0), raw)
                }
                8 => {
                    let mut raw = valid[&private_entry(2)].clone();
                    let last = raw.len() - 1;
                    raw[last] ^= 1;
                    WriteBatch::default().put(private_entry(2), raw)
                }
                9 => WriteBatch::default().put(private_key(9), vec![]),
                10 => WriteBatch::default().put(b"\xF2foreign".to_vec(), vec![]),
                11 => WriteBatch::default().put(b"\xF0local".to_vec(), vec![]),
                12 => {
                    let mut bad = proposal(2, 1);
                    bad.data = vec![1, 2, 3].into();
                    WriteBatch::default()
                        .put(private_entry(2), frame(2, &bad.write_to_bytes().unwrap()))
                }
                13 => WriteBatch::default().put(
                    private_entry(2),
                    frame(2, &proposal(2, 2).write_to_bytes().unwrap()),
                ),
                14 => WriteBatch::default().put(private_key(0), vec![0; 4096]),
                15 => WriteBatch::default().put(
                    private_entry(3),
                    frame(2, &blank(3, 0).write_to_bytes().unwrap()),
                ),
                16 => WriteBatch::default().put(
                    private_entry(2),
                    frame(2, &config(2, 1).write_to_bytes().unwrap()),
                ),
                17 => {
                    let mut entry = proposal(2, 1);
                    entry.data = DurableProposal::unbound(Command::new(
                        NamespaceName::new("foreign").unwrap(),
                        EntityPath::new("orders").unwrap(),
                        Timestamp::from_millis(1234),
                        CommandKind::CreateQueue {
                            config: QueueConfig::default(),
                        },
                    ))
                    .encode()
                    .unwrap()
                    .into();
                    WriteBatch::default()
                        .put(private_entry(2), frame(2, &entry.write_to_bytes().unwrap()))
                }
                _ => unreachable!(),
            };
            backend.apply(mutation).unwrap();
            let before = rows(&control);
            assert!(
                ConsensusStore::open(backend, identity(), 1, RetainedLogLimits::default()).is_err(),
                "case {case}"
            );
            assert_eq!(before, rows(&control));
            if let Some(directory) = directory {
                let backend = ConsensusFjallStore::open(directory.path()).unwrap();
                let mut actual = BTreeMap::new();
                let mut cursor = Vec::new();
                loop {
                    let page = backend.scan_from(b"", &cursor, 64).unwrap();
                    if page.is_empty() {
                        break;
                    }
                    for (k, v) in page {
                        cursor = k.clone();
                        cursor.push(0);
                        actual.insert(k, v);
                    }
                }
                assert_eq!(before, actual, "physical logical rows after refusal {case}");
            }
        }
    }
}

#[test]
fn bounded_pages_and_byte_admission_keep_all_committed_history() {
    let (backend, control, _directory) = fixture(false);
    let mut store =
        ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default()).unwrap();
    let first: Vec<_> = (1..=64).map(|i| blank(i, 1)).collect();
    store.persist(&first, Some(&hs(1, 1, 64))).unwrap();
    let second: Vec<_> = (65..=129).map(|i| blank(i, 1)).collect();
    let before = rows(&control);
    let over_limit: Vec<_> = (130..=4226).map(|i| blank(i, 1)).collect();
    assert!(store.persist(&over_limit, Some(&hs(1, 1, 129))).is_err());
    assert_eq!(before, rows(&control));
    store.persist(&second, Some(&hs(1, 1, 129))).unwrap();
    let store = store.reopen().unwrap();
    assert_eq!(store.read_entries(65, 64).unwrap().len(), 64);
    assert_eq!(
        Storage::entries(&store, 1, 130, None, GetEntriesContext::empty(false))
            .unwrap()
            .len(),
        129
    );
    assert_eq!(store.last_index().unwrap(), 129);
    // Logical byte quota independent of entry-count quota. An entry's entire
    // data and maximum context still fit; a second full-context entry refuses.
    let backend = ConsensusMemoryStore::default();
    let mut bounded = ConsensusStore::bootstrap(
        backend,
        identity(),
        1,
        RetainedLogLimits {
            max_entries: 8,
            max_bytes: (MAX_CONSENSUS_ENTRY_BYTES + 128) as u64,
        },
    )
    .unwrap();
    let mut entry = blank(1, 1);
    entry.context = vec![0; cluster::consensus::MAX_CONSENSUS_CONTEXT_BYTES].into();
    bounded
        .persist(&[entry.clone()], Some(&hs(1, 1, 1)))
        .unwrap();
    entry.index = 2;
    bounded
        .persist(&[entry.clone()], Some(&hs(1, 1, 2)))
        .unwrap();
    entry.index = 3;
    assert!(bounded.persist(&[entry], Some(&hs(1, 1, 3))).is_err());
    assert_eq!(bounded.hard_state().unwrap().commit, 2);
    assert_eq!(bounded.reopen().unwrap().last_index().unwrap(), 2);
}

#[test]
fn ambiguous_replacement_and_bootstrap_recover_only_original_persisted_state() {
    for durable in [false, true] {
        let (backend, control, _directory) = fixture(durable);
        let mut store =
            ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default())
                .unwrap();
        store
            .persist(
                &[blank(1, 1), proposal(2, 1), blank(3, 1)],
                Some(&hs(1, 1, 1)),
            )
            .unwrap();
        control.fault.store(2, Ordering::SeqCst);
        assert!(
            store
                .persist(&[proposal(2, 2)], Some(&hs(2, 2, 2)))
                .is_err()
        );
        assert!(store.is_retired());
        let reopened = store.reopen().unwrap();
        assert_eq!(
            reopened.read_entries(1, 64).unwrap(),
            vec![blank(1, 1), proposal(2, 2)]
        );
        assert_eq!(reopened.hard_state().unwrap(), &hs(2, 2, 2));
        assert_eq!(reopened.application_ordinal(2).unwrap(), Some(1));
    }
    for fault in [1, 2] {
        let (backend, control, directory) = fixture(true);
        control.fault.store(fault, Ordering::SeqCst);
        assert!(
            ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default())
                .is_err()
        );
        let backend = ConsensusFjallStore::open(directory.as_ref().unwrap().path()).unwrap();
        let opened = ConsensusStore::open(backend, identity(), 1, RetainedLogLimits::default());
        if fault == 1 {
            assert!(matches!(opened, Err(ConsensusError::MissingMetadata)));
        } else {
            assert_eq!(opened.unwrap().last_index().unwrap(), 0);
        }
    }
}

#[test]
fn cluster_offline_upgrade_refuses_consensus_profile_without_record_changes() {
    let (backend, control, directory) = fixture(true);
    let store =
        ConsensusStore::bootstrap(backend, identity(), 1, RetainedLogLimits::default()).unwrap();
    let before = rows(&control);
    drop(store);
    let directory = directory.unwrap();
    assert!(matches!(
        cluster::upgrade_store(directory.path()),
        Err(cluster::StoreUpgradeError::Storage(
            storage::StoreFormatUpgradeError::UnsupportedSource {
                found: 4,
                target: 3
            }
        ))
    ));
    let backend = ConsensusFjallStore::open(directory.path()).unwrap();
    let actual: BTreeMap<_, _> = backend
        .scan_from(b"", b"", 64)
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(before, actual);
}
