//! Offline typed-capture installation, not runtime recovery or authenticated
//! authority. Writers are dropped before install and all Fjall handles before reopen.

use std::collections::BTreeSet;

use cluster::{
    Journal, SnapshotCapture, SnapshotFormatError, SnapshotInstallError, SnapshotInstallLimits,
    SnapshotInstallOwner, SnapshotInstallPolicy, SnapshotLimit, SnapshotLimits,
};
use domain::{
    Command, CommandKind, CommandOutcome, DurableProposal, EntityPath, IndexedApplyOutcome,
    IndexedWriter, MessageEnvelope, NamespaceName, QueueConfig, QueueCounters, ReceiveMode,
    RuleFilter, RuleName, SessionId, StateMachine, SubscriptionConfig, SubscriptionName, Timestamp,
    TopicConfig, codec, keys,
};
use sha2::{Digest, Sha256};
use storage::{
    FjallStore, ImageInstallBackend, ImageInstallError, ImageInstallLimits, MemoryStore,
    SnapshotProvenance, StateStore, StoreSnapshot, WriteBatch,
};
use tempfile::TempDir;

const F0: &[u8] = b"\xF0switchyard/journal\0";
const F1: &[u8] = b"\xF1switchyard/replay\0";

fn limits() -> SnapshotInstallLimits {
    SnapshotInstallLimits {
        snapshot: SnapshotLimits {
            max_encoded_bytes: 16 * 1024 * 1024,
            max_records: 100_000,
            max_key_bytes: 4096,
            max_value_bytes: 2 * 1024 * 1024,
        },
        replacement: ImageInstallLimits {
            max_records: 100_000,
            max_key_bytes: 4096,
            max_value_bytes: 2 * 1024 * 1024,
            max_mutations: 200_000,
            max_mutation_bytes: 32 * 1024 * 1024,
        },
    }
}

fn ns() -> NamespaceName {
    NamespaceName::new("tenant").unwrap()
}
fn path(name: &str) -> EntityPath {
    EntityPath::new(name).unwrap()
}
fn proposal(at: u64, name: &str, kind: CommandKind) -> DurableProposal {
    DurableProposal::unbound(Command::new(
        ns(),
        path(name),
        Timestamp::from_millis(at),
        kind,
    ))
}
fn private_key(prefix: &[u8], tag: u8) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.push(tag);
    key
}
fn entry(index: u64) -> Vec<u8> {
    let mut key = private_key(F0, 2);
    key.extend(index.to_be_bytes());
    key
}

struct History<S: StateStore> {
    writer: IndexedWriter<S>,
    journal: Journal<S>,
    index: u64,
}
impl<S: StateStore> History<S> {
    fn new(store: &S) -> Self {
        Self {
            writer: IndexedWriter::open(store.clone()).unwrap(),
            journal: Journal::open(store.clone()).unwrap(),
            index: 0,
        }
    }
    fn apply(&mut self, entity: &str, kind: CommandKind) -> CommandOutcome {
        self.index += 1;
        let proposal = proposal(self.index, entity, kind);
        self.journal
            .append(self.index, &proposal.encode().unwrap())
            .unwrap();
        match self.writer.apply(self.index, &proposal).unwrap() {
            IndexedApplyOutcome::Applied(outcome) => outcome,
            other => panic!("fixture refused: {other:?}"),
        }
    }
    fn send(&mut self, entity: &str, id: &str, session: Option<SessionId>, scheduled: bool) {
        self.apply(
            entity,
            CommandKind::Send {
                message_id: id.into(),
                body: vec![0, 0xff, 0x80],
                time_to_live_millis: Some(100_000),
                session_id: session,
                scheduled_enqueue_at: scheduled.then(|| Timestamp::from_millis(50_000)),
                envelope: Some(MessageEnvelope::new(vec![0, 0xff, 7])),
            },
        );
    }
    fn receive(&mut self) -> domain::Delivery {
        match self.apply(
            "orders",
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: Some(10_000),
                session: None,
            },
        ) {
            CommandOutcome::Received(Some(delivery)) => delivery,
            other => panic!("fixture receive: {other:?}"),
        }
    }
}

fn rich<S: StateStore>(store: &S) {
    let mut h = History::new(store);
    h.apply(
        "orders",
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_duplicate_detection: true,
                duplicate_detection_history_millis: 100_000,
                ..QueueConfig::default()
            },
        },
    );
    h.send("orders", "deferred", None, false);
    let deferred = h.receive();
    h.apply(
        "orders",
        CommandKind::Defer {
            sequence: deferred.sequence,
            lock_token: deferred.lock.unwrap().token,
            replacement_envelope: None,
        },
    );
    h.send("orders", "locked", None, false);
    h.receive();
    h.send("orders", "ready", None, false);
    h.send("orders", "scheduled", None, true);
    h.apply(
        "sessions",
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            },
        },
    );
    let session = SessionId::new("one").unwrap();
    h.send("sessions", "session-ready", Some(session.clone()), false);
    let accepted = match h.apply(
        "sessions",
        CommandKind::AcceptSession {
            session_id: Some(session),
            lock_duration_millis: Some(10_000),
        },
    ) {
        CommandOutcome::SessionAccepted(Some(accepted)) => accepted,
        other => panic!("fixture session: {other:?}"),
    };
    h.apply(
        "sessions",
        CommandKind::SetSessionState {
            session: accepted.hold(),
            state: vec![0, 0xff, 0x80],
        },
    );
    h.apply(
        "events",
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    h.apply(
        "events",
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("worker").unwrap(),
            config: SubscriptionConfig::default(),
        },
    );
    h.apply(
        "events/subscriptions/worker",
        CommandKind::CreateRule {
            name: RuleName::new("extra").unwrap(),
            filter: RuleFilter::False,
        },
    );
    // Committed but unapplied work and opaque uncommitted tails must not replay.
    let next = h.index + 1;
    h.journal
        .append(
            next,
            &proposal(next, "orders", CommandKind::ExpireSessionLocks)
                .encode()
                .unwrap(),
        )
        .unwrap();
    h.journal.commit(next).unwrap();
    h.journal.append(next + 1, &[0xff, 0, 0x80]).unwrap();
    h.journal.append(next + 2, &[]).unwrap();
    drop(h);
    let rows = store.snapshot().unwrap();
    let tags = rows
        .entries()
        .iter()
        .map(|(key, _)| key[0])
        .collect::<BTreeSet<_>>();
    for tag in 0..=0x11 {
        assert!(tags.contains(&tag), "missing domain tag {tag:#x}");
    }
    assert!(tags.contains(&0xf0) && tags.contains(&0xf1));
}

fn small<S: StateStore>(store: &S, count: u64) {
    let mut h = History::new(store);
    h.apply(
        "old",
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    while h.index < count {
        h.apply("old", CommandKind::ExpireSessionLocks);
    }
    h.journal.commit(h.index).unwrap();
}

fn install_rich<S: ImageInstallBackend>(
    target: S,
    capture: &SnapshotCapture,
    populated: bool,
) -> S {
    if populated {
        small(&target, 40);
    }
    let before = target.snapshot().unwrap();
    let retained = before.clone();
    let immutable_source = capture.clone();
    let mut owner = SnapshotInstallOwner::new(target);
    let receipt = owner
        .install(capture, limits(), SnapshotInstallPolicy::default())
        .unwrap();
    if populated {
        assert_eq!(receipt.old_target_manifest.applied_index(), 40);
    } else {
        assert_eq!(receipt.old_target_manifest.applied_index(), 0);
    }
    assert_eq!(receipt.source_manifest, capture.manifest());
    assert_eq!(receipt.source_provenance, *capture.provenance());
    assert_eq!(receipt.replacement.deleted_records, before.entries().len());
    assert_eq!(
        owner.snapshot().unwrap().snapshot(),
        capture.image().snapshot()
    );
    assert_eq!(capture, &immutable_source);
    assert_eq!(retained, before);
    assert!(!owner.is_retired());
    // No anti-rollback gate: an older valid image may replace a later frontier.
    if populated {
        assert!(capture.manifest().applied_index() < receipt.old_target_manifest.applied_index());
    }
    assert_eq!(
        capture.manifest().committed_index(),
        capture.manifest().applied_index() + 1
    );
    assert_eq!(
        capture.manifest().last_appended_index(),
        capture.manifest().committed_index() + 2
    );
    owner.into_store().unwrap()
}

#[test]
fn memory_rich_empty_and_populated_targets_exact_recapture_no_replay() {
    let source = MemoryStore::default();
    rich(&source);
    let capture = SnapshotCapture::logical(&source, limits().snapshot).unwrap();
    for populated in [false, true] {
        let target = install_rich(MemoryStore::default(), &capture, populated);
        assert_eq!(
            SnapshotCapture::logical(&target, limits().snapshot).unwrap(),
            capture
        );
    }
}

#[test]
fn fjall_rich_empty_and_populated_targets_exact_full_reopen_no_replay() {
    let source_dir = TempDir::new().unwrap();
    let source = FjallStore::open(source_dir.path()).unwrap();
    rich(&source);
    let capture = SnapshotCapture::fjall(&source, limits().snapshot).unwrap();
    drop(source);
    for populated in [false, true] {
        let target_dir = TempDir::new().unwrap();
        drop(install_rich(
            FjallStore::open(target_dir.path()).unwrap(),
            &capture,
            populated,
        ));
        let reopened = FjallStore::open(target_dir.path()).unwrap();
        assert_eq!(
            SnapshotCapture::fjall(&reopened, limits().snapshot).unwrap(),
            capture
        );
        assert_eq!(reopened.snapshot().unwrap(), *capture.image().snapshot());
    }
}

fn policy<S: ImageInstallBackend>(target: S, capture: &SnapshotCapture, same_class: bool) -> S {
    let mut owner = SnapshotInstallOwner::new(target);
    let before = owner.snapshot().unwrap();
    let result = owner.install(capture, limits(), SnapshotInstallPolicy::default());
    if same_class {
        assert!(result.is_ok());
    } else {
        assert_eq!(result, Err(SnapshotInstallError::ProvenanceMismatch));
        assert_eq!(owner.snapshot().unwrap(), before);
        assert!(!owner.is_retired());
    }
    let receipt = owner
        .install(capture, limits(), SnapshotInstallPolicy::AllowCrossBackend)
        .unwrap();
    assert_eq!(receipt.source_provenance, *capture.provenance());
    assert_eq!(receipt.replacement.target_provenance, *before.provenance());
    assert_eq!(
        owner.snapshot().unwrap().snapshot(),
        capture.image().snapshot()
    );
    owner.into_store().unwrap()
}

#[test]
fn source_and_target_provenance_matrix_requires_explicit_cross_backend_opt_in() {
    let memory = MemoryStore::default();
    small(&memory, 1);
    let memory_capture = SnapshotCapture::logical(&memory, limits().snapshot).unwrap();
    let source_dir = TempDir::new().unwrap();
    let native = FjallStore::open(source_dir.path()).unwrap();
    small(&native, 1);
    let native_capture = SnapshotCapture::fjall(&native, limits().snapshot).unwrap();
    // Choosing logical capture on Fjall remains logical evidence.
    let logical_native = SnapshotCapture::logical(&native, limits().snapshot).unwrap();
    assert_eq!(memory_capture.encoded(), native_capture.encoded());
    assert_eq!(
        logical_native.provenance(),
        &SnapshotProvenance::LogicalOnly
    );
    drop(native);
    for capture in [&memory_capture, &native_capture, &logical_native] {
        let logical = matches!(capture.provenance(), SnapshotProvenance::LogicalOnly);
        let installed = policy(MemoryStore::default(), capture, logical);
        assert_eq!(
            SnapshotCapture::logical(&installed, limits().snapshot)
                .unwrap()
                .encoded(),
            capture.encoded()
        );
        let dir = TempDir::new().unwrap();
        drop(policy(
            FjallStore::open(dir.path()).unwrap(),
            capture,
            !logical,
        ));
        let reopened = FjallStore::open(dir.path()).unwrap();
        let recaptured = SnapshotCapture::fjall(&reopened, limits().snapshot).unwrap();
        assert_eq!(recaptured.encoded(), capture.encoded());
        assert!(matches!(
            recaptured.provenance(),
            SnapshotProvenance::Fjall(_)
        ));
        assert_eq!(
            capture.provenance(),
            if logical {
                &SnapshotProvenance::LogicalOnly
            } else {
                native_capture.provenance()
            }
        );
    }
}

fn corrupt_target<S: ImageInstallBackend>(target: S, case: usize, capture: &SnapshotCapture) -> S {
    if case == 11 {
        rich(&target);
    } else if case < 9 || case >= 12 {
        small(&target, 1);
    }
    let mutation = match case {
        0 => WriteBatch::default().put(keys::clock(), vec![0xff]),
        1 => WriteBatch::default().delete(keys::clock()),
        2 => WriteBatch::default().put(
            keys::queue_counters(&ns(), &path("old")),
            codec::encode(&QueueCounters {
                next_sequence: 0,
                next_lock_token: 0,
            })
            .unwrap(),
        ),
        3 => WriteBatch::default().put(b"\xF0other".to_vec(), vec![]),
        4 => WriteBatch::default().delete(private_key(F0, 0)),
        5 => WriteBatch::default().delete(entry(1)),
        6 => WriteBatch::default().delete(private_key(F1, 0)),
        7 => WriteBatch::default().put(private_key(F1, 1), vec![0; 72]),
        8 => WriteBatch::default().put(vec![0x12], vec![]),
        9 => WriteBatch::default().put(
            keys::clock(),
            codec::encode(&Timestamp::from_millis(1)).unwrap(),
        ),
        10 => {
            StateMachine::new(target.clone())
                .apply(
                    proposal(
                        1,
                        "unindexed",
                        CommandKind::CreateQueue {
                            config: QueueConfig::default(),
                        },
                    )
                    .command(),
                )
                .unwrap();
            WriteBatch::default()
        }
        11 => WriteBatch::default().put(
            keys::queue_counters(&ns(), &path("orders")),
            codec::encode(&QueueCounters {
                next_sequence: 1,
                next_lock_token: 1,
            })
            .unwrap(),
        ),
        12 => {
            let mut journal = Journal::open(target.clone()).unwrap();
            journal.append(2, &[0xff]).unwrap();
            journal.commit(2).unwrap();
            WriteBatch::default()
        }
        13 => {
            let mut checkpoint = target.get(&private_key(F1, 1)).unwrap().unwrap();
            checkpoint[8] ^= 1;
            let mut hash = Sha256::new();
            hash.update(b"switchyard indexed checkpoint v1\0");
            hash.update(&checkpoint[..40]);
            checkpoint[40..].copy_from_slice(&hash.finalize());
            WriteBatch::default().put(private_key(F1, 1), checkpoint)
        }
        _ => {
            let mut frontier = 1_u32.to_be_bytes().to_vec();
            frontier.extend(0_u64.to_be_bytes());
            let mut hash = Sha256::new();
            hash.update(b"switchyard journal frontier v1\0");
            hash.update(&frontier);
            frontier.extend_from_slice(&hash.finalize());
            WriteBatch::default().put(private_key(F0, 1), frontier)
        }
    };
    target.apply(mutation).unwrap();
    let before = target.snapshot().unwrap();
    let mut owner = SnapshotInstallOwner::new(target);
    let error = owner
        .install(capture, limits(), SnapshotInstallPolicy::AllowCrossBackend)
        .unwrap_err();
    assert!(
        matches!(error, SnapshotInstallError::Target(_)),
        "case {case}: {error:?}"
    );
    assert_eq!(owner.snapshot().unwrap().snapshot(), &before);
    assert!(!owner.is_retired());
    owner.into_store().unwrap()
}

#[test]
fn memory_full_old_target_domain_f0_f1_and_unindexed_refusals() {
    let source = MemoryStore::default();
    let capture = SnapshotCapture::logical(&source, limits().snapshot).unwrap();
    for case in 0..15 {
        corrupt_target(MemoryStore::default(), case, &capture);
    }
}

#[test]
fn fjall_full_old_target_domain_f0_f1_and_unindexed_refusals_reopen_unchanged() {
    let source = MemoryStore::default();
    let capture = SnapshotCapture::logical(&source, limits().snapshot).unwrap();
    for case in 0..15 {
        let dir = TempDir::new().unwrap();
        let target = corrupt_target(FjallStore::open(dir.path()).unwrap(), case, &capture);
        let before = target.snapshot().unwrap();
        drop(target);
        assert_eq!(
            FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
            before
        );
    }
}

fn bounded<S: ImageInstallBackend>(target: S, capture: &SnapshotCapture) {
    small(&target, 1);
    let before = target.snapshot().unwrap();
    let mut owner = SnapshotInstallOwner::new(target);
    let mut caps = limits();
    caps.replacement.max_records = 0;
    assert!(matches!(
        owner.install(capture, caps, SnapshotInstallPolicy::AllowCrossBackend),
        Err(SnapshotInstallError::Storage(
            ImageInstallError::LimitExceeded { .. }
        ))
    ));
    let mut caps = limits();
    caps.snapshot.max_encoded_bytes = 0;
    assert!(matches!(
        owner.install(capture, caps, SnapshotInstallPolicy::AllowCrossBackend),
        Err(SnapshotInstallError::Incoming(_))
    ));
    let mut caps = limits();
    caps.snapshot.max_records = 0;
    assert!(matches!(
        owner.install(capture, caps, SnapshotInstallPolicy::AllowCrossBackend),
        Err(SnapshotInstallError::Target(
            cluster::SnapshotCaptureError::Format(_)
        ))
    ));
    assert_eq!(owner.snapshot().unwrap().snapshot(), &before);
    assert!(!owner.is_retired());
}

#[test]
fn memory_and_fjall_install_bounds_precede_mutation() {
    let source = MemoryStore::default();
    let capture = SnapshotCapture::logical(&source, limits().snapshot).unwrap();
    bounded(MemoryStore::default(), &capture);
    let dir = TempDir::new().unwrap();
    bounded(FjallStore::open(dir.path()).unwrap(), &capture);
}

fn incoming_record_bounds<S: ImageInstallBackend>(target: S, capture: &SnapshotCapture) -> S {
    let records = capture.image().snapshot().entries();
    let mut exact = limits();
    exact.snapshot = SnapshotLimits {
        max_encoded_bytes: capture.encoded().len(),
        max_records: records.len(),
        max_key_bytes: records.iter().map(|(key, _)| key.len()).max().unwrap(),
        max_value_bytes: records.iter().map(|(_, value)| value.len()).max().unwrap(),
    };
    let mut owner = SnapshotInstallOwner::new(target);
    let before = owner.snapshot().unwrap();
    assert!(before.snapshot().entries().is_empty());
    for kind in [
        SnapshotLimit::Records,
        SnapshotLimit::KeyBytes,
        SnapshotLimit::ValueBytes,
    ] {
        let mut smaller = exact;
        let cap = match kind {
            SnapshotLimit::Records => &mut smaller.snapshot.max_records,
            SnapshotLimit::KeyBytes => &mut smaller.snapshot.max_key_bytes,
            SnapshotLimit::ValueBytes => &mut smaller.snapshot.max_value_bytes,
            SnapshotLimit::EncodedBytes => unreachable!(),
        };
        let requested = *cap;
        assert!(requested > 0);
        *cap -= 1;
        assert_eq!(
            owner.install(capture, smaller, SnapshotInstallPolicy::AllowCrossBackend),
            Err(SnapshotInstallError::Incoming(
                SnapshotFormatError::LimitExceeded {
                    kind,
                    requested: requested as u64,
                    maximum: requested - 1,
                }
            )),
            "incoming {kind:?} must use the install-time frame limit"
        );
        assert_eq!(owner.snapshot().unwrap(), before);
        assert!(!owner.is_retired());
    }
    let receipt = owner
        .install(capture, exact, SnapshotInstallPolicy::AllowCrossBackend)
        .unwrap();
    assert_eq!(receipt.source_manifest, capture.manifest());
    assert_eq!(receipt.source_provenance, *capture.provenance());
    assert_eq!(receipt.replacement.deleted_records, 0);
    assert_eq!(receipt.replacement.inserted_records, records.len());
    assert_eq!(
        owner.snapshot().unwrap().snapshot(),
        capture.image().snapshot()
    );
    assert!(!owner.is_retired());
    owner.into_store().unwrap()
}

#[test]
fn memory_and_fjall_incoming_record_key_and_value_exact_limits_and_one_over() {
    let source = MemoryStore::default();
    small(&source, 1);
    let capture = SnapshotCapture::logical(&source, limits().snapshot).unwrap();
    let target = incoming_record_bounds(MemoryStore::default(), &capture);
    assert_eq!(
        SnapshotCapture::logical(&target, limits().snapshot).unwrap(),
        capture
    );
    let dir = TempDir::new().unwrap();
    drop(incoming_record_bounds(
        FjallStore::open(dir.path()).unwrap(),
        &capture,
    ));
    let reopened = FjallStore::open(dir.path()).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), *capture.image().snapshot());
}
