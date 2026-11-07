//! Logical atomicity and reopen controls for offline compaction and read-only
//! recovery selection, not process-crash, power-cut, quorum or runtime recovery.

use cluster::{
    CommittedReplay, Journal, JournalError, SNAPSHOT_ANCHOR_HASH_SCOPE, SnapshotAnchorLimits,
    SnapshotAnchorOwner, SnapshotCapture, SnapshotCaptureError, SnapshotCompactionError,
    SnapshotCompactionLimits, SnapshotCompactionOwner, SnapshotInstallLimits, SnapshotInstallOwner,
    SnapshotInstallPolicy, SnapshotLimits, SnapshotRecoveryClass, select_recovery,
};
use domain::{Command, CommandKind, DurableProposal, EntityPath, IndexedWriter, NamespaceName};
use domain::{QueueConfig, Timestamp};
use sha2::{Digest, Sha256};
use storage::{
    FjallStore, ImageInstallBackend, ImageInstallLimits, MemoryStore, StateStore, StoreSnapshot,
    WriteBatch,
};
use tempfile::TempDir;

const F0: &[u8] = b"\xF0switchyard/journal\0";

fn snapshot_caps() -> SnapshotLimits {
    SnapshotLimits {
        max_encoded_bytes: 8 * 1024 * 1024,
        max_records: 10_000,
        max_key_bytes: 4096,
        max_value_bytes: 2 * 1024 * 1024,
    }
}
fn caps() -> SnapshotCompactionLimits {
    SnapshotCompactionLimits {
        snapshot: snapshot_caps(),
        max_deleted_entries: 10_000,
        max_mutation_bytes: 2 * 1024 * 1024,
    }
}
fn install_caps() -> SnapshotInstallLimits {
    SnapshotInstallLimits {
        snapshot: snapshot_caps(),
        replacement: ImageInstallLimits {
            max_records: 10_000,
            max_key_bytes: 4096,
            max_value_bytes: 2 * 1024 * 1024,
            max_mutations: 20_000,
            max_mutation_bytes: 16 * 1024 * 1024,
        },
    }
}
fn key(tag: u8) -> Vec<u8> {
    [F0, &[tag]].concat()
}
fn entry(index: u64) -> Vec<u8> {
    [key(2), index.to_be_bytes().to_vec()].concat()
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
fn proposal(at: u64, kind: CommandKind) -> DurableProposal {
    DurableProposal::unbound(Command::new(
        NamespaceName::new("tenant").unwrap(),
        EntityPath::new("orders").unwrap(),
        Timestamp::from_millis(at),
        kind,
    ))
}
/// A=P=2 (create, send), C=`committed` and an opaque tail at C+1.
fn source(committed: u64) -> SnapshotCapture {
    source_with(committed, true)
}
fn source_with(committed: u64, tail: bool) -> SnapshotCapture {
    let store = MemoryStore::default();
    let mut journal = Journal::open(store.clone()).unwrap();
    let mut writer = IndexedWriter::open(store.clone()).unwrap();
    let mut proposals = vec![
        proposal(
            10,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ),
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
        ),
    ];
    for index in 3..=committed {
        proposals.push(proposal(index + 20, CommandKind::ExpireSessionLocks));
    }
    for (offset, proposal) in proposals.iter().enumerate() {
        journal
            .append(offset as u64 + 1, &proposal.encode().unwrap())
            .unwrap();
    }
    journal.commit(committed).unwrap();
    if tail {
        journal.append(committed + 1, &[0xff, 0, 0x80]).unwrap();
    }
    for (offset, proposal) in proposals.iter().take(2).enumerate() {
        writer.apply(offset as u64 + 1, proposal).unwrap();
    }
    SnapshotCapture::logical(&store, snapshot_caps()).unwrap()
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
    let mut owner = SnapshotAnchorOwner::new(install(store, capture));
    let limits = SnapshotAnchorLimits {
        snapshot: snapshot_caps(),
        max_mutation_bytes: 2 * 1024 * 1024,
    };
    owner.attach(capture, limits).unwrap();
    owner.into_store().unwrap()
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
/// Synthetic 0 < B < A fixture setup; no production API removes a partial prefix.
fn prune_fixture<S: StateStore>(store: &S, floor: u64) {
    let mut anchor = store.get(&key(3)).unwrap().unwrap();
    anchor[8..16].copy_from_slice(&floor.to_be_bytes());
    resign_anchor(&mut anchor);
    let mut batch = WriteBatch::default().put(key(3), anchor);
    for index in 1..=floor {
        batch.push_delete(entry(index));
    }
    store.apply(batch).unwrap();
}
fn replay_one<S: StateStore>(store: &S) {
    let mut replay = CommittedReplay::open(store.clone()).unwrap();
    assert_eq!(replay.replay_batch(1).unwrap().processed, 1);
}

fn select<S: ImageInstallBackend>(
    store: &S,
    class: SnapshotRecoveryClass,
    floor: u64,
    [applied, committed, last]: [u64; 3],
) {
    let selection = select_recovery(store, snapshot_caps()).unwrap();
    assert_eq!(selection.class, class);
    assert_eq!(selection.removed_through, floor);
    assert_eq!(
        selection.anchor_applied_index,
        (class != SnapshotRecoveryClass::LegacyUnanchored).then_some(2)
    );
    assert_eq!(selection.manifest.applied_index(), applied);
    assert_eq!(selection.manifest.committed_index(), committed);
    assert_eq!(selection.manifest.last_appended_index(), last);
    assert_eq!(
        selection.provenance,
        *store.inspect_image().unwrap().provenance()
    );
}

/// Compacts B..=A and checks the exact two-row difference and replay of (P,C].
fn compact<S: ImageInstallBackend>(store: S, floor: u64, applied: u64, committed: u64) -> S {
    let before = store.snapshot().unwrap();
    let mut owner = SnapshotCompactionOwner::new(store);
    drop(owner.prepare(caps()).unwrap());
    assert_eq!(owner.snapshot().unwrap().snapshot(), &before);
    let receipt = owner.compact(caps()).unwrap();
    assert_eq!(receipt.applied_index, 2);
    assert_eq!(receipt.removed_through_before, floor);
    assert_eq!(receipt.removed_through_after, 2);
    assert_eq!(receipt.deleted_entries, (2 - floor) as usize);
    assert_eq!(receipt.mutations, receipt.deleted_entries + 1);
    assert_eq!(receipt.manifest.applied_index(), applied);
    assert_eq!(receipt.manifest.committed_index(), committed);
    let after = owner.snapshot().unwrap();
    assert_eq!(
        after.snapshot().entries().len(),
        before.entries().len() - receipt.deleted_entries
    );
    for (key, value) in before.entries() {
        let found = after.snapshot().entries().iter().find(|(k, _)| k == key);
        if (floor + 1..=2).any(|index| *key == entry(index)) {
            assert!(found.is_none());
        } else if *key == self::key(3) {
            // Only B and the checksum change in the anchor.
            let new = &found.unwrap().1;
            assert_eq!(new.len(), value.len());
            assert_eq!(new[8..16], 2_u64.to_be_bytes());
            assert_eq!(new[..8], value[..8]);
            assert_eq!(new[16..new.len() - 32], value[16..value.len() - 32]);
        } else {
            // Root/owner fences, frontier, checkpoint, retained entries, the
            // opaque tail, domain rows and Clock stay byte-exact.
            assert_eq!(found.unwrap().1, *value);
        }
    }
    assert_eq!(
        owner.compact(caps()),
        Err(SnapshotCompactionError::AlreadyCompacted)
    );
    assert_eq!(owner.snapshot().unwrap().snapshot(), after.snapshot());
    assert!(!owner.is_retired());
    owner.into_store().unwrap()
}
fn compacted_progress<S: ImageInstallBackend>(store: &S, applied: u64, committed: u64) {
    let tail = store.get(&entry(committed + 1)).unwrap().unwrap();
    let journal = Journal::open(store.clone()).unwrap();
    assert_eq!(journal.removed_through(), Ok(2));
    for from in 0..=2 {
        assert_eq!(
            journal.read_committed(from, 1),
            Err(JournalError::UnavailablePrefix {
                requested: from,
                removed_through: 2
            })
        );
    }
    drop(journal);
    let mut replay = CommittedReplay::open(store.clone()).unwrap();
    assert_eq!(replay.applied_index().unwrap(), applied);
    let mut processed = 0;
    while replay.applied_index().unwrap() < committed {
        processed += replay.replay_batch(64).unwrap().processed;
    }
    assert_eq!(processed as u64, committed - applied);
    assert_eq!(replay.replay_batch(1).unwrap().processed, 0);
    drop(replay);
    assert_eq!(store.get(&entry(committed + 1)).unwrap().unwrap(), tail);
    select(
        store,
        SnapshotRecoveryClass::Compacted,
        2,
        [committed, committed, committed + 1],
    );
    SnapshotCapture::logical(store, snapshot_caps()).unwrap();
}

#[test]
fn memory_compaction_at_applied_anchor_replays_only_committed_suffix() {
    let capture = source(3);
    let store = MemoryStore::default();
    select(
        &store,
        SnapshotRecoveryClass::LegacyUnanchored,
        0,
        [0, 0, 0],
    );
    let store = install(store, &capture);
    select(
        &store,
        SnapshotRecoveryClass::LegacyUnanchored,
        0,
        [2, 3, 4],
    );
    let store = attach(store, &capture);
    select(&store, SnapshotRecoveryClass::Anchored, 0, [2, 3, 4]);
    let store = compact(store, 0, 2, 3);
    select(&store, SnapshotRecoveryClass::Compacted, 2, [2, 3, 4]);
    // A compacted capture installs into an empty target and replays (P,C].
    let compacted = SnapshotCapture::logical(&store, snapshot_caps()).unwrap();
    let restored = install(MemoryStore::default(), &compacted);
    assert_eq!(restored.snapshot().unwrap(), store.snapshot().unwrap());
    select(&restored, SnapshotRecoveryClass::Compacted, 2, [2, 3, 4]);
    compacted_progress(&restored, 2, 3);
    compacted_progress(&store, 2, 3);
}

#[test]
fn fjall_compaction_survives_repeated_full_handle_reopens() {
    let capture = source(3);
    let dir = TempDir::new().unwrap();
    drop(attach(FjallStore::open(dir.path()).unwrap(), &capture));
    let store = FjallStore::open(dir.path()).unwrap();
    select(&store, SnapshotRecoveryClass::Anchored, 0, [2, 3, 4]);
    let expected = compact(store, 0, 2, 3).snapshot().unwrap();
    for _ in 0..3 {
        let store = FjallStore::open(dir.path()).unwrap();
        assert_eq!(store.snapshot().unwrap(), expected);
        select(&store, SnapshotRecoveryClass::Compacted, 2, [2, 3, 4]);
        assert_eq!(
            SnapshotCompactionOwner::new(store).compact(caps()),
            Err(SnapshotCompactionError::AlreadyCompacted)
        );
    }
    let store = FjallStore::open(dir.path()).unwrap();
    let compacted = SnapshotCapture::fjall(&store, snapshot_caps()).unwrap();
    drop(store);
    let restored_dir = TempDir::new().unwrap();
    drop(install(
        FjallStore::open(restored_dir.path()).unwrap(),
        &compacted,
    ));
    let restored = FjallStore::open(restored_dir.path()).unwrap();
    assert_eq!(restored.snapshot().unwrap(), expected);
    compacted_progress(&restored, 2, 3);
    let progressed = restored.snapshot().unwrap();
    drop(restored);
    for _ in 0..2 {
        let reopened = FjallStore::open(restored_dir.path()).unwrap();
        assert_eq!(reopened.snapshot().unwrap(), progressed);
        assert_eq!(
            CommittedReplay::open(reopened)
                .unwrap()
                .applied_index()
                .unwrap(),
            3
        );
    }
    compacted_progress(&FjallStore::open(dir.path()).unwrap(), 2, 3);
}

#[test]
fn memory_and_fjall_progressed_checkpoint_prunes_only_through_anchor() {
    fn run<S: ImageInstallBackend>(store: S) -> S {
        let capture = source(5);
        let store = attach(store, &capture);
        replay_one(&store);
        select(&store, SnapshotRecoveryClass::Anchored, 0, [3, 5, 6]);
        // P=3 > A=2: entry P must stay to verify the latest checkpoint.
        let store = compact(store, 0, 3, 5);
        assert!(store.get(&entry(3)).unwrap().is_some());
        compacted_progress(&store, 3, 5);
        store
    }
    run(MemoryStore::default());
    let dir = TempDir::new().unwrap();
    let expected = run(FjallStore::open(dir.path()).unwrap())
        .snapshot()
        .unwrap();
    let reopened = FjallStore::open(dir.path()).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), expected);
    CommittedReplay::open(reopened).unwrap();
}

#[test]
fn memory_and_fjall_partial_floor_deletes_only_the_remaining_prefix() {
    fn run<S: ImageInstallBackend>(store: S) -> S {
        let store = attach(store, &source(3));
        prune_fixture(&store, 1);
        select(&store, SnapshotRecoveryClass::Anchored, 1, [2, 3, 4]);
        let store = compact(store, 1, 2, 3);
        compacted_progress(&store, 2, 3);
        store
    }
    run(MemoryStore::default());
    let dir = TempDir::new().unwrap();
    let expected = run(FjallStore::open(dir.path()).unwrap())
        .snapshot()
        .unwrap();
    assert_eq!(
        FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
        expected
    );
}

fn refuse<S: ImageInstallBackend>(store: S, matches: fn(&SnapshotCompactionError) -> bool) -> S {
    let before = store.snapshot().unwrap();
    let mut owner = SnapshotCompactionOwner::new(store);
    let error = owner.compact(caps()).unwrap_err();
    assert!(matches(&error), "{error:?}");
    assert_eq!(owner.snapshot().unwrap().snapshot(), &before);
    assert!(!owner.is_retired());
    owner.into_store().unwrap()
}

#[test]
fn memory_and_fjall_unanchored_images_refuse_without_writes() {
    fn run<S: ImageInstallBackend>(store: S) -> S {
        let store = refuse(store, |error| *error == SnapshotCompactionError::Unanchored);
        let store = install(store, &source(3));
        refuse(store, |error| *error == SnapshotCompactionError::Unanchored)
    }
    run(MemoryStore::default());
    let dir = TempDir::new().unwrap();
    let expected = run(FjallStore::open(dir.path()).unwrap())
        .snapshot()
        .unwrap();
    assert_eq!(
        FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
        expected
    );
}

fn corruptions<S: ImageInstallBackend>(store: S) -> S {
    let mut store = attach(store, &source(3));
    let base = store.snapshot().unwrap();
    let original = store.get(&entry(2)).unwrap().unwrap();
    for case in 0..10 {
        restore_fixture(&store, &base);
        let mut anchor = store.get(&key(3)).unwrap().unwrap();
        let batch = match case {
            0 => {
                let last = anchor.len() - 1;
                anchor[last] ^= 1;
                WriteBatch::default().put(key(3), anchor)
            }
            1 => {
                anchor[8..16].copy_from_slice(&3_u64.to_be_bytes());
                resign_anchor(&mut anchor);
                WriteBatch::default().put(key(3), anchor)
            }
            // Divergent physical A, still a valid envelope and proposal.
            2 => WriteBatch::default().put(
                entry(2),
                envelope(
                    2,
                    &proposal(99, CommandKind::ExpireSessionLocks)
                        .encode()
                        .unwrap(),
                ),
            ),
            3 => WriteBatch::default().delete(entry(1)),
            4 => WriteBatch::default().delete(entry(2)),
            5 => WriteBatch::default().put(domain::keys::clock(), vec![0xff]),
            // A compacted image with a retained entry at/below its floor.
            6 => {
                anchor[8..16].copy_from_slice(&2_u64.to_be_bytes());
                resign_anchor(&mut anchor);
                WriteBatch::default()
                    .put(key(3), anchor)
                    .delete(entry(1))
                    .put(entry(2), original.clone())
            }
            // Corruption after A: a malformed opaque envelope at L.
            7 => WriteBatch::default().put(entry(4), vec![0xff]),
            // A gap at C+1 beneath a later valid tail envelope.
            8 => WriteBatch::default()
                .delete(entry(4))
                .put(entry(5), envelope(5, b"later")),
            // A committed (A,C] entry whose proposal cannot decode.
            _ => WriteBatch::default().put(entry(3), envelope(3, &[0xff])),
        };
        store.apply(batch).unwrap();
        assert!(
            select_recovery(&store, snapshot_caps()).is_err(),
            "case {case}"
        );
        store = refuse(store, |error| {
            matches!(error, SnapshotCompactionError::Target(_))
        });
    }
    restore_fixture(&store, &base);
    store
}
#[test]
fn memory_and_fjall_corrupt_divergent_and_invalid_images_refuse_without_writes() {
    corruptions(MemoryStore::default());
    let dir = TempDir::new().unwrap();
    let expected = corruptions(FjallStore::open(dir.path()).unwrap())
        .snapshot()
        .unwrap();
    assert_eq!(
        FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
        expected
    );
}

fn boundaries<S: ImageInstallBackend>(store: S) -> S {
    let mut store = attach(store, &source(3));
    let before = store.snapshot().unwrap();
    let records = before.entries();
    let anchor_len = store.get(&key(3)).unwrap().unwrap().len();
    let capture = SnapshotCapture::logical(&store, snapshot_caps()).unwrap();
    // Current-image caps bound validation; the compacted result is no larger.
    let exact = SnapshotCompactionLimits {
        snapshot: SnapshotLimits {
            max_encoded_bytes: capture.encoded().len(),
            max_records: records.len(),
            max_key_bytes: records.iter().map(|(key, _)| key.len()).max().unwrap(),
            max_value_bytes: records.iter().map(|(_, value)| value.len()).max().unwrap(),
        },
        max_deleted_entries: 2,
        max_mutation_bytes: entry(1).len() + entry(2).len() + key(3).len() + anchor_len,
    };
    let mut owner = SnapshotCompactionOwner::new(store);
    drop(owner.prepare(exact).unwrap());
    for boundary in 0..6 {
        let mut smaller = exact;
        match boundary {
            0 => smaller.max_deleted_entries -= 1,
            1 => smaller.max_mutation_bytes -= 1,
            2 => smaller.snapshot.max_encoded_bytes -= 1,
            3 => smaller.snapshot.max_records -= 1,
            4 => smaller.snapshot.max_key_bytes -= 1,
            _ => smaller.snapshot.max_value_bytes -= 1,
        }
        let error = owner.compact(smaller).unwrap_err();
        match boundary {
            0 => assert_eq!(
                error,
                SnapshotCompactionError::DeletionLimit {
                    requested: 2,
                    maximum: 1
                }
            ),
            1 => assert_eq!(
                error,
                SnapshotCompactionError::MutationLimit {
                    requested: exact.max_mutation_bytes,
                    maximum: exact.max_mutation_bytes - 1
                }
            ),
            _ => assert!(matches!(
                error,
                SnapshotCompactionError::Target(SnapshotCaptureError::Format(_))
            )),
        }
        assert!(!owner.is_retired());
        assert_eq!(owner.snapshot().unwrap().snapshot(), &before);
    }
    owner.compact(exact).unwrap();
    store = owner.into_store().unwrap();
    compacted_progress(&store, 2, 3);
    store
}
#[test]
fn memory_and_fjall_exact_and_one_over_caps() {
    boundaries(MemoryStore::default());
    let dir = TempDir::new().unwrap();
    let expected = boundaries(FjallStore::open(dir.path()).unwrap())
        .snapshot()
        .unwrap();
    assert_eq!(
        FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
        expected
    );
}

/// Appends and commits L+1 on a compacted store; replay applies only that entry.
fn append_after_compaction<S: StateStore>(store: &S, last: u64) {
    let next = proposal(99, CommandKind::ExpireSessionLocks)
        .encode()
        .unwrap();
    let mut journal = Journal::open(store.clone()).unwrap();
    assert_eq!(journal.last_appended_index(), Ok(last));
    for refused in [last, last + 2] {
        assert_eq!(
            journal.append(refused, &next),
            Err(JournalError::InvalidAppendIndex {
                found: refused,
                expected: last + 1
            })
        );
    }
    journal.append(last + 1, &next).unwrap();
    journal.commit(last + 1).unwrap();
    drop(journal);
    let mut replay = CommittedReplay::open(store.clone()).unwrap();
    assert_eq!(replay.applied_index().unwrap(), last);
    assert_eq!(replay.committed_index().unwrap(), last + 1);
    assert_eq!(replay.replay_batch(64).unwrap().processed, 1);
    assert_eq!(replay.replay_batch(1).unwrap().processed, 0);
    drop(replay);
    assert_eq!(
        CommittedReplay::open(store.clone())
            .unwrap()
            .applied_index()
            .unwrap(),
        last + 1
    );
    assert_eq!(
        Journal::open(store.clone()).unwrap().removed_through(),
        Ok(2)
    );
}

fn progress_after_compaction<S: ImageInstallBackend>(store: S, committed: u64) -> S {
    let store = attach(store, &source_with(committed, false));
    let store = compact(store, 0, 2, committed);
    if committed == 2 {
        // L=C=P=A: every entry is removed and the anchor original verifies P.
        assert!(
            store
                .snapshot()
                .unwrap()
                .entries()
                .iter()
                .all(|(key, _)| !key.starts_with(&self::key(2)))
        );
        let journal = Journal::open(store.clone()).unwrap();
        assert_eq!(journal.committed_index(), Ok(2));
        assert_eq!(journal.last_appended_index(), Ok(2));
    } else {
        replay_one(&store);
    }
    // P=C after compaction: there is nothing to replay.
    let mut replay = CommittedReplay::open(store.clone()).unwrap();
    assert_eq!(replay.applied_index().unwrap(), committed);
    assert_eq!(replay.replay_batch(64).unwrap().processed, 0);
    drop(replay);
    select(
        &store,
        SnapshotRecoveryClass::Compacted,
        2,
        [committed, committed, committed],
    );
    append_after_compaction(&store, committed);
    store
}
#[test]
fn memory_and_fjall_append_commit_and_replay_continue_after_compaction() {
    for committed in [2, 3] {
        progress_after_compaction(MemoryStore::default(), committed);
        let dir = TempDir::new().unwrap();
        let expected = progress_after_compaction(FjallStore::open(dir.path()).unwrap(), committed)
            .snapshot()
            .unwrap();
        let reopened = FjallStore::open(dir.path()).unwrap();
        assert_eq!(reopened.snapshot().unwrap(), expected);
        select(
            &reopened,
            SnapshotRecoveryClass::Compacted,
            2,
            [committed + 1; 3],
        );
        drop(reopened);
        append_after_compaction(&FjallStore::open(dir.path()).unwrap(), committed + 1);
    }
}
