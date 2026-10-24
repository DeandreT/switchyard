//! Neutral replacement tests. Snapshot copies are immutable observations, not
//! installation/recovery certificates or physical persistence evidence.
//! Private image-install tests additionally observe apply-call counts and native
//! commit sequences without relaxing the sealed backend/provenance boundary.

use storage::{
    FjallStore, ImageInstallBackend, ImageInstallError, ImageInstallLimit, ImageInstallLimits,
    MemoryStore, Mutation, OfflineImageOwner, StateStore, StoreSnapshot, WriteBatch,
};
use tempfile::TempDir;

fn generous() -> ImageInstallLimits {
    ImageInstallLimits {
        max_records: 100,
        max_key_bytes: 100,
        max_value_bytes: 100,
        max_mutations: 200,
        max_mutation_bytes: 10_000,
    }
}

fn image(rows: &[(&[u8], &[u8])]) -> StoreSnapshot {
    let store = MemoryStore::default();
    let mut batch = WriteBatch::default();
    for (key, value) in rows {
        batch.push_put(key.to_vec(), value.to_vec());
    }
    store.apply(batch).unwrap();
    store.snapshot().unwrap()
}

fn seed<S: StateStore>(store: &S, image: &StoreSnapshot) {
    let mut batch = WriteBatch::default();
    for (key, value) in image.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    store.apply(batch).unwrap();
}

fn replace<S: ImageInstallBackend>(store: S, old: &StoreSnapshot, new: &StoreSnapshot) -> S {
    seed(&store, old);
    let retained = store.snapshot().unwrap();
    let mut owner = OfflineImageOwner::new(store);
    let inspection = owner.inspect(new, generous()).unwrap();
    assert_eq!(inspection.image().snapshot(), old);
    let receipt = inspection.prepare().unwrap().apply().unwrap();
    assert_eq!(receipt.deleted_records, old.entries().len());
    assert_eq!(receipt.inserted_records, new.entries().len());
    assert_eq!(receipt.mutations, old.entries().len() + new.entries().len());
    assert_eq!(
        receipt.logical_mutation_bytes,
        old.entries().iter().map(|(k, _)| k.len()).sum::<usize>()
            + new
                .entries()
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
    );
    assert!(!owner.is_retired());
    assert_eq!(owner.snapshot().unwrap().snapshot(), new);
    assert_eq!(
        &retained, old,
        "retained old snapshots cannot observe a partial replacement"
    );
    owner.into_store().unwrap()
}

#[test]
fn memory_empty_populated_overlapping_and_disjoint() {
    for (old, new) in cases() {
        replace(MemoryStore::default(), &old, &new);
    }
}

#[test]
fn fjall_empty_populated_overlapping_and_disjoint_reopen() {
    for (old, new) in cases() {
        let dir = TempDir::new().unwrap();
        let store = replace(FjallStore::open(dir.path()).unwrap(), &old, &new);
        drop(store); // no clone, owner, or keyspace handle survives this close
        let reopened = FjallStore::open(dir.path()).unwrap();
        assert_eq!(reopened.snapshot().unwrap(), new);
        let captured = reopened.snapshot_with_provenance().unwrap();
        let storage::SnapshotProvenance::Fjall(meta) = captured.provenance() else {
            panic!("native evidence required")
        };
        assert_eq!(
            meta.format_marker(),
            &storage::ACTIVE_STORE_FORMAT.to_be_bytes()
        );
    }
}

fn cases() -> Vec<(StoreSnapshot, StoreSnapshot)> {
    let a = image(&[(b"a", b"old"), (b"shared", b"before")]);
    let b = image(&[(b"b", b"new"), (b"shared", b"after")]);
    vec![
        (image(&[]), image(&[])),
        (image(&[]), b.clone()),
        (a.clone(), image(&[])),
        (a.clone(), b),
        (a, image(&[(b"z", b"\0\xff\x80")])),
    ]
}

fn limits<S: ImageInstallBackend>(store: S) -> S {
    let old = image(&[(b"old", b"12345"), (b"same", b"")]);
    let new = image(&[(b"new", b"123456"), (b"same", b"next")]);
    seed(&store, &old);
    let mut owner = OfflineImageOwner::new(store);
    let before = owner.snapshot().unwrap();
    assert_eq!(before.snapshot(), &old);
    let exact = ImageInstallLimits {
        max_records: 2,
        max_key_bytes: 4,
        max_value_bytes: 6,
        max_mutations: 4,
        max_mutation_bytes: 24,
    };
    for kind in [
        ImageInstallLimit::Records,
        ImageInstallLimit::KeyBytes,
        ImageInstallLimit::ValueBytes,
        ImageInstallLimit::Mutations,
        ImageInstallLimit::MutationBytes,
    ] {
        let mut smaller = exact;
        let cap = match kind {
            ImageInstallLimit::Records => &mut smaller.max_records,
            ImageInstallLimit::KeyBytes => &mut smaller.max_key_bytes,
            ImageInstallLimit::ValueBytes => &mut smaller.max_value_bytes,
            ImageInstallLimit::Mutations => &mut smaller.max_mutations,
            ImageInstallLimit::MutationBytes => &mut smaller.max_mutation_bytes,
        };
        let requested = *cap;
        *cap -= 1;
        assert_eq!(
            owner.inspect(&new, smaller).err(),
            Some(ImageInstallError::LimitExceeded {
                kind,
                requested,
                maximum: requested - 1
            })
        );
        assert_eq!(owner.snapshot().unwrap(), before);
        assert!(!owner.is_retired());
    }
    // Dropping a fully checked/prepared operation is not a write.
    drop(owner.inspect(&new, exact).unwrap().prepare().unwrap());
    assert_eq!(owner.snapshot().unwrap(), before);
    assert!(!owner.is_retired());
    let receipt = owner
        .inspect(&new, exact)
        .unwrap()
        .prepare()
        .unwrap()
        .apply()
        .unwrap();
    assert_eq!(receipt.logical_mutation_bytes, 24);
    assert_eq!(owner.snapshot().unwrap().snapshot(), &new);
    owner.into_store().unwrap()
}

#[test]
fn memory_exact_limits_and_one_over() {
    limits(MemoryStore::default());
}

#[test]
fn fjall_exact_limits_and_one_over() {
    let dir = TempDir::new().unwrap();
    drop(limits(FjallStore::open(dir.path()).unwrap()));
    assert_eq!(
        FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
        image(&[(b"new", b"123456"), (b"same", b"next")])
    );
}

fn overfull_old<S: ImageInstallBackend>(store: S) {
    let old = image(&[(b"long-key", b"long-value"), (b"other", b"old")]);
    seed(&store, &old);
    let incoming = image(&[(b"x", b"y")]);
    let mut owner = OfflineImageOwner::new(store);
    let before = owner.snapshot().unwrap();
    for (kind, maximum) in [
        (ImageInstallLimit::Records, 1),
        (ImageInstallLimit::KeyBytes, 7),
        (ImageInstallLimit::ValueBytes, 9),
    ] {
        let mut cap = generous();
        match kind {
            ImageInstallLimit::Records => cap.max_records = maximum,
            ImageInstallLimit::KeyBytes => cap.max_key_bytes = maximum,
            ImageInstallLimit::ValueBytes => cap.max_value_bytes = maximum,
            _ => unreachable!(),
        }
        assert!(
            matches!(owner.inspect(&incoming, cap), Err(ImageInstallError::LimitExceeded { kind: actual, .. }) if actual == kind)
        );
        assert_eq!(owner.snapshot().unwrap(), before);
        assert!(!owner.is_retired());
    }
}

#[test]
fn memory_old_target_bounds_are_independent() {
    overfull_old(MemoryStore::default());
}

#[test]
fn fjall_old_target_bounds_are_independent() {
    let dir = TempDir::new().unwrap();
    overfull_old(FjallStore::open(dir.path()).unwrap());
    assert_eq!(
        FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
        image(&[(b"long-key", b"long-value"), (b"other", b"old")])
    );
}

#[test]
fn fjall_rejects_native_empty_and_oversized_incoming_keys_before_apply() {
    for key in [vec![], vec![1; usize::from(u16::MAX) + 1]] {
        let dir = TempDir::new().unwrap();
        let old = image(&[(b"old", b"retained")]);
        let store = FjallStore::open(dir.path()).unwrap();
        seed(&store, &old);
        let source = MemoryStore::default();
        source
            .apply(WriteBatch::default().put(key.clone(), vec![]))
            .unwrap();
        let incoming = source.snapshot().unwrap();
        let mut owner = OfflineImageOwner::new(store);
        let mut caps = generous();
        caps.max_key_bytes = usize::MAX;
        let error = owner.inspect(&incoming, caps).err().unwrap();
        if key.is_empty() {
            assert_eq!(error, ImageInstallError::EmptyNativeKey);
        } else {
            assert!(
                matches!(error, ImageInstallError::LimitExceeded { kind: ImageInstallLimit::KeyBytes, maximum, .. } if maximum == usize::from(u16::MAX))
            );
        }
        drop(owner);
        assert_eq!(
            FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
            old
        );
    }
}

#[test]
fn fjall_exact_native_key_limit_applies_and_reopens() {
    let dir = TempDir::new().unwrap();
    let source = MemoryStore::default();
    let key = vec![0xff; usize::from(u16::MAX)];
    source
        .apply(WriteBatch::default().put(key.clone(), vec![1]))
        .unwrap();
    let incoming = source.snapshot().unwrap();
    let mut owner = OfflineImageOwner::new(FjallStore::open(dir.path()).unwrap());
    let mut caps = generous();
    caps.max_key_bytes = key.len();
    caps.max_mutation_bytes = key.len() + 1;
    owner
        .inspect(&incoming, caps)
        .unwrap()
        .prepare()
        .unwrap()
        .apply()
        .unwrap();
    drop(owner);
    assert_eq!(
        FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
        incoming
    );
}

#[test]
fn mutation_order_is_explicit_in_the_public_batch_contract() {
    assert_eq!(
        WriteBatch::default()
            .delete(b"a".to_vec())
            .put(b"a".to_vec(), b"new".to_vec())
            .into_mutations(),
        vec![
            Mutation::Delete { key: b"a".to_vec() },
            Mutation::Put {
                key: b"a".to_vec(),
                value: b"new".to_vec()
            }
        ]
    );
}
