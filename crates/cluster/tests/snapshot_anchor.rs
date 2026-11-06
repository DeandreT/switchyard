//! Synthetic pruning is fixture setup only. These controls do not establish
//! authenticated history, runtime recovery, or process-crash/power-cut safety.

use cluster::{
    CommittedReplay, Journal, JournalError, SNAPSHOT_ANCHOR_HASH_SCOPE, SnapshotAnchorError,
    SnapshotAnchorLimits, SnapshotAnchorOwner, SnapshotCapture, SnapshotInstallLimits,
    SnapshotInstallOwner, SnapshotInstallPolicy, SnapshotLimits,
};
use domain::{
    BoundCommand, Command, CommandKind, DurableProposal, DurableProposalAuthority, EntityPath,
    IndexedWriter, NamespaceName, QueueConfig, StateMachine, Timestamp,
};
use sha2::{Digest, Sha256};
use storage::{
    FjallStore, ImageInstallBackend, ImageInstallLimits, MemoryStore, StateStore, StoreSnapshot,
    WriteBatch,
};
use tempfile::TempDir;

const F0: &[u8] = b"\xF0switchyard/journal\0";
const F1: &[u8] = b"\xF1switchyard/replay\0";

fn caps() -> SnapshotAnchorLimits {
    SnapshotAnchorLimits {
        snapshot: SnapshotLimits {
            max_encoded_bytes: 8 * 1024 * 1024,
            max_records: 10_000,
            max_key_bytes: 4096,
            max_value_bytes: 2 * 1024 * 1024,
        },
        max_mutation_bytes: 2 * 1024 * 1024,
    }
}
fn install_caps() -> SnapshotInstallLimits {
    SnapshotInstallLimits {
        snapshot: caps().snapshot,
        replacement: ImageInstallLimits {
            max_records: 10_000,
            max_key_bytes: 4096,
            max_value_bytes: 2 * 1024 * 1024,
            max_mutations: 20_000,
            max_mutation_bytes: 16 * 1024 * 1024,
        },
    }
}
fn key(prefix: &[u8], tag: u8) -> Vec<u8> {
    [prefix, &[tag]].concat()
}
fn entry(index: u64) -> Vec<u8> {
    [key(F0, 2), index.to_be_bytes().to_vec()].concat()
}
fn digest(scope: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(scope);
    hash.update(bytes);
    hash.finalize().into()
}
fn resign_anchor(bytes: &mut [u8]) {
    let end = bytes.len() - 32;
    let hash = digest(SNAPSHOT_ANCHOR_HASH_SCOPE, &bytes[..end]);
    bytes[end..].copy_from_slice(&hash);
}
fn envelope(index: u64, payload: &[u8]) -> Vec<u8> {
    let mut bytes = 1_u32.to_be_bytes().to_vec();
    bytes.extend(index.to_be_bytes());
    bytes.extend((payload.len() as u32).to_be_bytes());
    let checksum = digest(
        b"switchyard journal entry v1\0",
        &[bytes.as_slice(), payload].concat(),
    );
    bytes.extend(checksum);
    bytes.extend(payload);
    bytes
}
fn frontier(index: u64) -> Vec<u8> {
    let mut bytes = 1_u32.to_be_bytes().to_vec();
    bytes.extend(index.to_be_bytes());
    bytes.extend(digest(b"switchyard journal frontier v1\0", &bytes));
    bytes
}
fn owner(version: u32) -> Vec<u8> {
    let mut bytes = b"SWIA".to_vec();
    bytes.extend(version.to_be_bytes());
    bytes.extend(digest(b"switchyard indexed owner v1\0", &bytes));
    bytes
}
fn proposal(at: u64, kind: CommandKind) -> DurableProposal {
    DurableProposal::unbound(Command::new(
        NamespaceName::new("tenant").unwrap(),
        EntityPath::new("orders").unwrap(),
        Timestamp::from_millis(at),
        kind,
    ))
}
fn seed<S: StateStore>(store: &S, committed: u64) -> DurableProposal {
    let mut journal = Journal::open(store.clone()).unwrap();
    let mut writer = IndexedWriter::open(store.clone()).unwrap();
    let first = proposal(
        10,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    journal.append(1, &first.encode().unwrap()).unwrap();
    writer.apply(1, &first).unwrap();
    let binding = StateMachine::new(store.clone())
        .bind_entity(
            &NamespaceName::new("tenant").unwrap(),
            &EntityPath::new("orders").unwrap(),
        )
        .unwrap();
    let second = DurableProposal::bound(BoundCommand::new(
        binding,
        proposal(
            20,
            CommandKind::Send {
                message_id: "original".into(),
                body: vec![0, 0xff, 7],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        )
        .command()
        .clone(),
    ))
    .unwrap();
    journal.append(2, &second.encode().unwrap()).unwrap();
    writer.apply(2, &second).unwrap();
    for index in 3..=committed {
        journal
            .append(
                index,
                &proposal(index + 20, CommandKind::ExpireSessionLocks)
                    .encode()
                    .unwrap(),
            )
            .unwrap();
    }
    journal.commit(committed).unwrap();
    journal.append(committed + 1, &[0xff, 0, 0x80]).unwrap();
    second
}
fn source(committed: u64) -> (SnapshotCapture, DurableProposal) {
    let store = MemoryStore::default();
    let original = seed(&store, committed);
    (
        SnapshotCapture::logical(&store, caps().snapshot).unwrap(),
        original,
    )
}
fn install<S: ImageInstallBackend>(store: S, capture: &SnapshotCapture) -> S {
    let mut owner = SnapshotInstallOwner::new(store);
    owner
        .install(
            capture,
            install_caps(),
            SnapshotInstallPolicy::AllowCrossBackend,
        )
        .unwrap();
    owner.into_store().unwrap()
}
fn attach<S: ImageInstallBackend>(store: S, capture: &SnapshotCapture) -> S {
    let before = store.snapshot().unwrap();
    let mut owner = SnapshotAnchorOwner::new(store);
    drop(owner.prepare(capture, caps()).unwrap());
    assert_eq!(owner.snapshot().unwrap().snapshot(), &before);
    assert!(!owner.is_retired());
    let receipt = owner.attach(capture, caps()).unwrap();
    assert_eq!(receipt.source_manifest, capture.manifest());
    assert_eq!(receipt.source_frame_digest, capture.frame_digest());
    assert_eq!(receipt.mutations, 3);
    let after = owner.snapshot().unwrap();
    assert_eq!(after.snapshot().entries().len(), before.entries().len() + 1);
    for (key, value) in before.entries() {
        if key != &self::key(F0, 0) && key != &self::key(F1, 0) {
            assert_eq!(
                after
                    .snapshot()
                    .entries()
                    .iter()
                    .find(|(k, _)| k == key)
                    .unwrap()
                    .1,
                *value
            );
        }
    }
    assert_eq!(
        owner.attach(capture, caps()),
        Err(SnapshotAnchorError::AlreadyAnchored)
    );
    assert!(!owner.is_retired());
    owner.into_store().unwrap()
}
fn inspect<S: StateStore>(
    store: &S,
    capture: &SnapshotCapture,
    original: &DurableProposal,
    floor: u64,
) {
    let journal = Journal::open(store.clone()).unwrap();
    let anchor = journal.snapshot_anchor().unwrap().unwrap();
    assert_eq!(anchor.applied_index(), 2);
    assert_eq!(anchor.removed_through(), floor);
    assert_eq!(anchor.source_format_version(), 1);
    assert_eq!(anchor.source_manifest(), capture.manifest());
    assert_eq!(anchor.source_frame_digest(), capture.frame_digest());
    assert_eq!(
        anchor.original_entry(),
        capture
            .image()
            .snapshot()
            .entries()
            .iter()
            .find(|(k, _)| *k == entry(2))
            .unwrap()
            .1
    );
    let decoded = DurableProposal::decode(&anchor.original_entry()[48..]).unwrap();
    assert_eq!(&decoded, original);
    assert_eq!(decoded.command().issued_at, Timestamp::from_millis(20));
    assert!(matches!(
        decoded.authority(),
        DurableProposalAuthority::Bound(_)
    ));
    let replay = CommittedReplay::open(store.clone()).unwrap();
    assert_eq!(replay.applied_index().unwrap(), 2);
    assert_eq!(
        replay.committed_index().unwrap(),
        capture.manifest().committed_index()
    );
    for from in 0..=floor {
        assert_eq!(
            journal.read_committed(from, 1),
            Err(JournalError::UnavailablePrefix {
                requested: from,
                removed_through: floor
            })
        );
    }
    let recapture = SnapshotCapture::logical(store, caps().snapshot).unwrap();
    assert_eq!(recapture.manifest(), capture.manifest());
    assert_ne!(recapture.frame_digest(), capture.frame_digest());
}
fn prune_fixture<S: StateStore>(store: &S, floor: u64) {
    let mut anchor = store.get(&key(F0, 3)).unwrap().unwrap();
    anchor[8..16].copy_from_slice(&floor.to_be_bytes());
    resign_anchor(&mut anchor);
    let mut batch = WriteBatch::default().put(key(F0, 3), anchor);
    for index in 1..=floor {
        batch.push_delete(entry(index));
    }
    store.apply(batch).unwrap();
}
fn progress<S: StateStore>(store: &S, committed: u64) {
    let old_owner = store.get(&key(F1, 0)).unwrap().unwrap();
    let anchor = store.get(&key(F0, 3)).unwrap().unwrap();
    let tail = store.get(&entry(committed + 1)).unwrap().unwrap();
    let mut replay = CommittedReplay::open(store.clone()).unwrap();
    let mut processed = 0;
    while replay.applied_index().unwrap() < committed {
        processed += replay.replay_batch(32).unwrap().processed;
    }
    assert_eq!(processed, committed as usize - 2);
    assert_eq!(replay.replay_batch(1).unwrap().processed, 0);
    drop(replay);
    assert_eq!(store.get(&key(F1, 0)).unwrap().unwrap(), old_owner);
    assert_eq!(store.get(&key(F0, 3)).unwrap().unwrap(), anchor);
    assert_eq!(store.get(&entry(committed + 1)).unwrap().unwrap(), tail);
    assert_eq!(
        CommittedReplay::open(store.clone())
            .unwrap()
            .applied_index()
            .unwrap(),
        committed
    );
    let mut journal = Journal::open(store.clone()).unwrap();
    journal.append(committed + 2, &[]).unwrap();
    assert_eq!(
        store.get(&key(F0, 0)).unwrap().unwrap(),
        2_u32.to_be_bytes()
    );
    drop(journal);
    SnapshotCapture::logical(store, caps().snapshot).unwrap();
}

#[test]
fn memory_exact_install_attach_prune_progress_and_recapture_install() {
    let (capture, original) = source(3);
    let store = attach(install(MemoryStore::default(), &capture), &capture);
    inspect(&store, &capture, &original, 0);
    let anchored = SnapshotCapture::logical(&store, caps().snapshot).unwrap();
    assert_eq!(
        install(MemoryStore::default(), &anchored)
            .snapshot()
            .unwrap(),
        store.snapshot().unwrap()
    );
    prune_fixture(&store, 2);
    inspect(&store, &capture, &original, 2);
    let pruned = SnapshotCapture::logical(&store, caps().snapshot).unwrap();
    let restored = install(MemoryStore::default(), &pruned);
    assert_eq!(restored.snapshot().unwrap(), store.snapshot().unwrap());
    progress(&restored, 3);
}

#[test]
fn fjall_exact_install_attach_prune_progress_full_handle_reopens() {
    let (capture, original) = source(3);
    let dir = TempDir::new().unwrap();
    drop(attach(
        install(FjallStore::open(dir.path()).unwrap(), &capture),
        &capture,
    ));
    let store = FjallStore::open(dir.path()).unwrap();
    inspect(&store, &capture, &original, 0);
    let anchored = SnapshotCapture::fjall(&store, caps().snapshot).unwrap();
    let anchored_dir = TempDir::new().unwrap();
    let anchored_copy = install(FjallStore::open(anchored_dir.path()).unwrap(), &anchored);
    assert_eq!(anchored_copy.snapshot().unwrap(), store.snapshot().unwrap());
    drop(anchored_copy);
    prune_fixture(&store, 2);
    drop(store);
    let store = FjallStore::open(dir.path()).unwrap();
    inspect(&store, &capture, &original, 2);
    let pruned = SnapshotCapture::fjall(&store, caps().snapshot).unwrap();
    drop(store);
    let restored_dir = TempDir::new().unwrap();
    drop(install(
        FjallStore::open(restored_dir.path()).unwrap(),
        &pruned,
    ));
    let restored = FjallStore::open(restored_dir.path()).unwrap();
    progress(&restored, 3);
    drop(restored);
    let reopened = FjallStore::open(restored_dir.path()).unwrap();
    assert_eq!(
        CommittedReplay::open(reopened.clone())
            .unwrap()
            .applied_index()
            .unwrap(),
        3
    );
    SnapshotCapture::fjall(&reopened, caps().snapshot).unwrap();
}

fn limits_and_mismatch<S: ImageInstallBackend>(store: S, capture: &SnapshotCapture) -> S {
    let store = install(store, capture);
    let before = store.snapshot().unwrap();
    let mut owner = SnapshotAnchorOwner::new(store);
    let original_len = before
        .entries()
        .iter()
        .find(|(key, _)| *key == entry(2))
        .unwrap()
        .1
        .len();
    let anchor_len = 116 + original_len + 32;
    let mut exact = caps();
    exact.max_mutation_bytes =
        key(F0, 0).len() + 4 + key(F1, 0).len() + 40 + key(F0, 3).len() + anchor_len;
    exact.snapshot.max_encoded_bytes = capture.encoded().len() + 16 + key(F0, 3).len() + anchor_len;
    exact.snapshot.max_records = before.entries().len() + 1;
    exact.snapshot.max_key_bytes = before
        .entries()
        .iter()
        .map(|(key, _)| key.len())
        .max()
        .unwrap();
    exact.snapshot.max_value_bytes = anchor_len.max(
        before
            .entries()
            .iter()
            .map(|(_, value)| value.len())
            .max()
            .unwrap(),
    );
    drop(owner.prepare(capture, exact).unwrap());
    for boundary in 0..5 {
        let mut smaller = exact;
        match boundary {
            0 => smaller.max_mutation_bytes -= 1,
            1 => smaller.snapshot.max_encoded_bytes -= 1,
            2 => smaller.snapshot.max_records -= 1,
            3 => smaller.snapshot.max_key_bytes -= 1,
            _ => smaller.snapshot.max_value_bytes -= 1,
        }
        assert!(
            owner.prepare(capture, smaller).is_err(),
            "boundary {boundary}"
        );
        assert!(!owner.is_retired());
    }
    let mut small = caps();
    small.max_mutation_bytes = 0;
    assert!(matches!(
        owner.attach(capture, small),
        Err(SnapshotAnchorError::MutationLimit { .. })
    ));
    let mut small = caps();
    small.snapshot.max_records = before.entries().len();
    assert!(matches!(
        owner.attach(capture, small),
        Err(SnapshotAnchorError::Format(_))
    ));
    assert_eq!(owner.snapshot().unwrap().snapshot(), &before);
    let store = owner.into_store().unwrap();
    // A valid later suffix is not reconciled, even with unchanged A/C/digest.
    {
        let mut journal = Journal::open(store.clone()).unwrap();
        journal.append(5, b"later").unwrap();
    }
    let changed = store.snapshot().unwrap();
    let mut owner = SnapshotAnchorOwner::new(store);
    assert_eq!(
        owner.attach(capture, caps()),
        Err(SnapshotAnchorError::TargetMismatch)
    );
    assert_eq!(owner.snapshot().unwrap().snapshot(), &changed);
    assert!(!owner.is_retired());
    let store = owner.into_store().unwrap();
    restore_fixture(&store, &before);
    store
        .apply(WriteBatch::default().put(entry(4), envelope(4, b"different opaque tail")))
        .unwrap();
    let changed = store.snapshot().unwrap();
    let mut owner = SnapshotAnchorOwner::new(store);
    assert_eq!(
        owner.attach(capture, caps()),
        Err(SnapshotAnchorError::TargetMismatch)
    );
    assert_eq!(owner.snapshot().unwrap().snapshot(), &changed);
    owner.into_store().unwrap()
}
#[test]
fn memory_and_fjall_exact_image_refusals_and_bounds_write_nothing() {
    let (capture, _) = source(3);
    limits_and_mismatch(MemoryStore::default(), &capture);
    let dir = TempDir::new().unwrap();
    let store = limits_and_mismatch(FjallStore::open(dir.path()).unwrap(), &capture);
    let before = store.snapshot().unwrap();
    drop(store);
    assert_eq!(
        FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
        before
    );
}

fn valid_domain_and_clock_mismatches<S: ImageInstallBackend>(
    store: S,
    capture: &SnapshotCapture,
) -> S {
    let mut store = install(store, capture);
    let before = store.snapshot().unwrap();
    for clock_only in [false, true] {
        restore_fixture(&store, &before);
        let (changed_key, changed_value) = if clock_only {
            (
                domain::keys::clock(),
                domain::codec::encode(&Timestamp::from_millis(21)).unwrap(),
            )
        } else {
            let prefix = domain::keys::message_prefix(
                &NamespaceName::new("tenant").unwrap(),
                &EntityPath::new("orders").unwrap(),
            );
            let (key, value) = before
                .entries()
                .iter()
                .find(|(key, _)| key.starts_with(&prefix))
                .unwrap();
            let mut message = domain::MessageRecord::decode(value).unwrap();
            message.body[0] ^= 0xff;
            (key.clone(), domain::codec::encode(&message).unwrap())
        };
        store
            .apply(WriteBatch::default().put(changed_key.clone(), changed_value))
            .unwrap();
        let changed = store.snapshot().unwrap();
        assert_ne!(changed, before);
        assert_eq!(changed.entries().len(), before.entries().len());
        for ((old_key, old_value), (new_key, new_value)) in
            before.entries().iter().zip(changed.entries())
        {
            assert_eq!(old_key, new_key);
            if old_key != &changed_key {
                // Includes every journal/frontier/checkpoint byte, not only
                // the manifest's unchanged P/C/L and latest proposal identity.
                assert_eq!(old_value, new_value);
            }
        }
        let valid_changed = SnapshotCapture::logical(&store, caps().snapshot).unwrap();
        assert_eq!(valid_changed.manifest(), capture.manifest());
        assert_ne!(valid_changed.frame_digest(), capture.frame_digest());
        let mut owner = SnapshotAnchorOwner::new(store);
        assert_eq!(
            owner.attach(capture, caps()),
            Err(SnapshotAnchorError::TargetMismatch)
        );
        assert_eq!(owner.snapshot().unwrap().snapshot(), &changed);
        assert!(!owner.is_retired());
        store = owner.into_store().unwrap();
    }
    store
}

#[test]
fn memory_and_fjall_valid_domain_and_clock_changes_are_not_the_exact_target() {
    let (capture, _) = source(3);
    valid_domain_and_clock_mismatches(MemoryStore::default(), &capture);
    let dir = TempDir::new().unwrap();
    let store = valid_domain_and_clock_mismatches(FjallStore::open(dir.path()).unwrap(), &capture);
    let changed = store.snapshot().unwrap();
    drop(store);
    assert_eq!(
        FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
        changed
    );
}

fn unapplied<S: ImageInstallBackend>(store: S) {
    for initialized in [false, true] {
        if initialized {
            let mut journal = Journal::open(store.clone()).unwrap();
            journal.append(1, b"opaque").unwrap();
        }
        let capture = SnapshotCapture::logical(&store, caps().snapshot).unwrap();
        let mut owner = SnapshotAnchorOwner::new(store.clone());
        assert_eq!(
            owner.attach(&capture, caps()),
            Err(SnapshotAnchorError::Unapplied)
        );
        assert_eq!(
            owner.snapshot().unwrap().snapshot(),
            capture.image().snapshot()
        );
        assert!(!owner.is_retired());
    }
}
#[test]
fn empty_and_unapplied_legacy_images_stay_valid_unanchored() {
    unapplied(MemoryStore::default());
    let dir = TempDir::new().unwrap();
    unapplied(FjallStore::open(dir.path()).unwrap());
}

fn restore_fixture<S: StateStore>(store: &S, image: &StoreSnapshot) {
    let mut batch = WriteBatch::default();
    for (key, _) in store.snapshot().unwrap().entries() {
        batch.push_delete(key.clone());
    }
    for (key, value) in image.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    store.apply(batch).unwrap();
}
fn corruptions<S: ImageInstallBackend>(store: S, capture: &SnapshotCapture) -> S {
    let store = attach(install(store, capture), capture);
    let base = store.snapshot().unwrap();
    for case in 0..23 {
        restore_fixture(&store, &base);
        let mut anchor = store.get(&key(F0, 3)).unwrap().unwrap();
        let batch = match case {
            0 => WriteBatch::default().delete(key(F0, 3)),
            1 => WriteBatch::default().put(key(F0, 0), 1_u32.to_be_bytes().to_vec()),
            2 => WriteBatch::default().put(key(F1, 0), owner(1)),
            3 => WriteBatch::default().delete(key(F1, 0)),
            4 => WriteBatch::default().delete(key(F1, 1)),
            5 => WriteBatch::default().put(key(F1, 0), owner(3)),
            6 => WriteBatch::default().put([key(F0, 3), vec![0]].concat(), vec![]),
            7 => WriteBatch::default().delete(entry(1)),
            8 => WriteBatch::default().delete(entry(33)),
            9 => WriteBatch::default().put(entry(1), envelope(1, &[0xff])), // old applied schema
            10 => WriteBatch::default().put(entry(33), envelope(33, &[0xff])),
            11 => WriteBatch::default().put(entry(71), vec![0xff]), // malformed opaque tail envelope
            12 => WriteBatch::default().put(key(F0, 1), frontier(2)), // source C regression
            13 => {
                anchor[4..8].copy_from_slice(&2_u32.to_be_bytes());
                resign_anchor(&mut anchor);
                WriteBatch::default().put(key(F0, 3), anchor)
            }
            14 => {
                anchor[8..16].copy_from_slice(&3_u64.to_be_bytes());
                resign_anchor(&mut anchor);
                WriteBatch::default().put(key(F0, 3), anchor)
            }
            15 => {
                anchor[16..20].copy_from_slice(&2_u32.to_be_bytes());
                resign_anchor(&mut anchor);
                WriteBatch::default().put(key(F0, 3), anchor)
            }
            16 => {
                anchor[20..24].copy_from_slice(&3_u32.to_be_bytes());
                resign_anchor(&mut anchor);
                WriteBatch::default().put(key(F0, 3), anchor)
            }
            17 => {
                anchor[24..32].copy_from_slice(&0_u64.to_be_bytes());
                resign_anchor(&mut anchor);
                WriteBatch::default().put(key(F0, 3), anchor)
            }
            18 => {
                anchor[48] ^= 1;
                resign_anchor(&mut anchor);
                WriteBatch::default().put(key(F0, 3), anchor)
            }
            19 => {
                let last = anchor.len() - 1;
                anchor[last] ^= 1;
                WriteBatch::default().put(key(F0, 3), anchor)
            }
            20 => {
                anchor[116 + 16] ^= 1;
                resign_anchor(&mut anchor);
                WriteBatch::default().put(key(F0, 3), anchor)
            }
            21 => {
                anchor.push(0);
                WriteBatch::default().put(key(F0, 3), anchor)
            }
            _ => {
                anchor[112..116].copy_from_slice(&u32::MAX.to_be_bytes());
                resign_anchor(&mut anchor);
                WriteBatch::default().put(key(F0, 3), anchor)
            }
        };
        store.apply(batch).unwrap();
        let before = store.snapshot().unwrap();
        assert!(
            SnapshotCapture::logical(&store, caps().snapshot).is_err(),
            "case {case}"
        );
        let mut owner = SnapshotAnchorOwner::new(store.clone());
        assert!(
            matches!(
                owner.attach(capture, caps()),
                Err(SnapshotAnchorError::Target(_))
            ),
            "case {case}"
        );
        assert_eq!(owner.snapshot().unwrap().snapshot(), &before);
        assert!(!owner.is_retired());
    }
    restore_fixture(&store, &base);
    store
}
#[test]
fn memory_all_retained_committed_pages_versions_and_corruption_refuse_atomically() {
    let (capture, _) = source(70);
    corruptions(MemoryStore::default(), &capture);
}
#[test]
fn fjall_all_retained_committed_pages_versions_and_corruption_refuse_atomically() {
    let (capture, _) = source(70);
    let dir = TempDir::new().unwrap();
    let store = corruptions(FjallStore::open(dir.path()).unwrap(), &capture);
    let before = store.snapshot().unwrap();
    drop(store);
    assert_eq!(
        FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
        before
    );
}

fn original_identity<S: ImageInstallBackend>(
    store: S,
    capture: &SnapshotCapture,
    original: &DurableProposal,
) {
    let store = attach(install(store, capture), capture);
    let base = store.snapshot().unwrap();
    for pruned in [false, true] {
        for time_only in [false, true] {
            restore_fixture(&store, &base);
            if pruned {
                prune_fixture(&store, 2);
            }
            let mut changed = original.command().clone();
            let changed = if time_only {
                changed.issued_at = Timestamp::from_millis(21);
                let DurableProposalAuthority::Bound(binding) = original.authority() else {
                    panic!("fixture must be bound")
                };
                DurableProposal::bound(BoundCommand::new(binding.clone(), changed)).unwrap()
            } else {
                DurableProposal::unbound(changed)
            };
            let replacement = envelope(2, &changed.encode().unwrap());
            let mut anchor = store.get(&key(F0, 3)).unwrap().unwrap();
            anchor.truncate(116);
            anchor[48..80].copy_from_slice(&IndexedWriter::<S>::proposal_digest(&changed).unwrap());
            anchor[112..116].copy_from_slice(&(replacement.len() as u32).to_be_bytes());
            anchor.extend(replacement);
            anchor.extend([0; 32]);
            resign_anchor(&mut anchor);
            store
                .apply(WriteBatch::default().put(key(F0, 3), anchor))
                .unwrap();
            let before = store.snapshot().unwrap();
            assert!(
                CommittedReplay::open(store.clone()).is_err(),
                "pruned={pruned} time_only={time_only}"
            );
            assert!(SnapshotCapture::logical(&store, caps().snapshot).is_err());
            assert_eq!(store.snapshot().unwrap(), before);
        }
    }
}
#[test]
fn memory_and_fjall_anchor_original_time_and_authority_cannot_replace_latest_identity() {
    let (capture, original) = source(3);
    original_identity(MemoryStore::default(), &capture, &original);
    let dir = TempDir::new().unwrap();
    original_identity(FjallStore::open(dir.path()).unwrap(), &capture, &original);
}

#[test]
fn memory_and_fjall_below_anchor_partial_floor_and_paged_progress() {
    let (capture, original) = source(70);
    fn run<S: ImageInstallBackend>(
        store: S,
        capture: &SnapshotCapture,
        original: &DurableProposal,
    ) {
        let store = attach(install(store, capture), capture);
        prune_fixture(&store, 1);
        inspect(&store, capture, original, 1);
        progress(&store, 70);
    }
    run(MemoryStore::default(), &capture, &original);
    let dir = TempDir::new().unwrap();
    run(FjallStore::open(dir.path()).unwrap(), &capture, &original);
}

fn exhausted_fixture<S: ImageInstallBackend>(store: S, capture: &SnapshotCapture) -> S {
    let store = attach(install(store, capture), capture);
    // Deliberately synthetic MAX-index image, not a claim that this history was
    // reached operationally. No production API can set a floor or checkpoint.
    let mut anchor = store.get(&key(F0, 3)).unwrap().unwrap();
    let original = envelope(u64::MAX, &anchor[116 + 48..anchor.len() - 32]);
    anchor.truncate(116);
    for offset in [8, 24, 32, 40] {
        anchor[offset..offset + 8].copy_from_slice(&u64::MAX.to_be_bytes());
    }
    anchor[112..116].copy_from_slice(&(original.len() as u32).to_be_bytes());
    anchor.extend(original);
    anchor.extend([0; 32]);
    resign_anchor(&mut anchor);
    let mut checkpoint = store.get(&key(F1, 1)).unwrap().unwrap();
    checkpoint[..8].copy_from_slice(&u64::MAX.to_be_bytes());
    let hash = digest(b"switchyard indexed checkpoint v1\0", &checkpoint[..40]);
    checkpoint[40..].copy_from_slice(&hash);
    let mut batch = WriteBatch::default()
        .put(key(F0, 3), anchor)
        .put(key(F0, 1), frontier(u64::MAX))
        .put(key(F1, 1), checkpoint);
    for index in 1..=4 {
        batch.push_delete(entry(index));
    }
    store.apply(batch).unwrap();
    let mut journal = Journal::open(store.clone()).unwrap();
    assert_eq!(journal.last_appended_index(), Ok(u64::MAX));
    assert_eq!(
        journal.append(u64::MAX, b""),
        Err(JournalError::IndexExhausted)
    );
    assert!(matches!(
        journal.read_committed(u64::MAX, 1),
        Err(JournalError::UnavailablePrefix { .. })
    ));
    drop(journal);
    let mut replay = CommittedReplay::open(store.clone()).unwrap();
    assert_eq!(replay.replay_batch(64).unwrap().processed, 0);
    drop(replay);
    SnapshotCapture::logical(&store, caps().snapshot).unwrap();
    store
}
#[test]
fn memory_and_fjall_synthetic_maximum_floor_and_index_exhaustion() {
    let (capture, _) = source(3);
    exhausted_fixture(MemoryStore::default(), &capture);
    let dir = TempDir::new().unwrap();
    let store = exhausted_fixture(FjallStore::open(dir.path()).unwrap(), &capture);
    let expected = store.snapshot().unwrap();
    drop(store);
    let reopened = FjallStore::open(dir.path()).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), expected);
    CommittedReplay::open(reopened).unwrap();
}

#[test]
fn legacy_unanchored_writes_keep_exact_v1_owner_and_container() {
    let store = MemoryStore::default();
    seed(&store, 3);
    assert_eq!(
        store.get(&key(F0, 0)).unwrap().unwrap(),
        1_u32.to_be_bytes()
    );
    assert_eq!(store.get(&key(F1, 0)).unwrap().unwrap(), owner(1));
    assert!(store.get(&key(F0, 3)).unwrap().is_none());
    let mut replay = CommittedReplay::open(store.clone()).unwrap();
    replay.replay_batch(1).unwrap();
    drop(replay);
    assert_eq!(
        store.get(&key(F0, 0)).unwrap().unwrap(),
        1_u32.to_be_bytes()
    );
    assert_eq!(store.get(&key(F1, 0)).unwrap().unwrap(), owner(1));
}

fn progressed_identity<S: ImageInstallBackend>(store: S, capture: &SnapshotCapture) {
    let store = attach(install(store, capture), capture);
    prune_fixture(&store, 2);
    {
        let mut replay = CommittedReplay::open(store.clone()).unwrap();
        replay.replay_batch(1).unwrap();
    }
    let base = store.snapshot().unwrap();
    for case in 0..4 {
        restore_fixture(&store, &base);
        let mutation = match case {
            0 => WriteBatch::default().put(
                entry(3),
                envelope(
                    3,
                    &proposal(99, CommandKind::ExpireSessionLocks)
                        .encode()
                        .unwrap(),
                ),
            ),
            1 => WriteBatch::default().delete(entry(3)),
            2 => {
                let mut checkpoint = capture
                    .image()
                    .snapshot()
                    .entries()
                    .iter()
                    .find(|(k, _)| *k == key(F1, 1))
                    .unwrap()
                    .1
                    .clone();
                checkpoint[..8].copy_from_slice(&1_u64.to_be_bytes());
                let hash = digest(b"switchyard indexed checkpoint v1\0", &checkpoint[..40]);
                checkpoint[40..].copy_from_slice(&hash);
                WriteBatch::default().put(key(F1, 1), checkpoint)
            }
            _ => WriteBatch::default().put(
                entry(1),
                capture
                    .image()
                    .snapshot()
                    .entries()
                    .iter()
                    .find(|(k, _)| *k == entry(1))
                    .unwrap()
                    .1
                    .clone(),
            ),
        };
        store.apply(mutation).unwrap();
        let before = store.snapshot().unwrap();
        assert!(
            CommittedReplay::open(store.clone()).is_err(),
            "progressed case {case}"
        );
        assert!(SnapshotCapture::logical(&store, caps().snapshot).is_err());
        assert_eq!(store.snapshot().unwrap(), before);
    }
}
#[test]
fn progressed_latest_requires_retained_p_and_checkpoint_cannot_regress_below_a() {
    let (capture, _) = source(3);
    progressed_identity(MemoryStore::default(), &capture);
    let dir = TempDir::new().unwrap();
    progressed_identity(FjallStore::open(dir.path()).unwrap(), &capture);
}
