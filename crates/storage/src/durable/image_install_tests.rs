//! Native target controls inspect the original live handles. All raw mutations
//! are fixture setup, with no concurrent writer during installation inspection.

use std::collections::BTreeMap;

use tempfile::TempDir;

use super::*;
use crate::{
    ImageInstallError, ImageInstallLimit, ImageInstallLimits, MemoryStore, OfflineImageOwner,
};

#[derive(Debug, Eq, PartialEq)]
struct Observation {
    names: Vec<String>,
    rows: BTreeMap<String, Vec<(Key, Value)>>,
    next: u64,
    visible: u64,
}

fn observe(database: &Database) -> Observation {
    let mut names = database
        .list_keyspace_names()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    names.sort();
    let snapshot = database.snapshot();
    let rows = names
        .iter()
        .map(|name| {
            assert!(database.keyspace_exists(name));
            let handle = database
                .keyspace(name, KeyspaceCreateOptions::default)
                .unwrap();
            let rows = snapshot
                .iter(&handle)
                .map(|guard| read_entry(guard).unwrap())
                .collect();
            (name.clone(), rows)
        })
        .collect();
    Observation {
        names,
        rows,
        next: database.seqno(),
        visible: database.visible_seqno(),
    }
}

fn caps() -> ImageInstallLimits {
    ImageInstallLimits {
        max_records: 100,
        max_key_bytes: 100,
        max_value_bytes: 100,
        max_mutations: 200,
        max_mutation_bytes: 10_000,
    }
}

fn fixture(dir: &TempDir, populated: bool) -> FjallStore {
    let store = FjallStore::open(dir.path()).unwrap();
    if populated {
        store
            .apply(WriteBatch::default().put(b"original".to_vec(), vec![0, 0xff]))
            .unwrap();
    }
    store
}

fn refusal(store: FjallStore, dir: &TempDir) {
    let before = observe(&store.database);
    let cached_records = store.snapshot().unwrap();
    let cached_ids = (store.meta.id(), store.records.id());
    let incoming = MemoryStore::default();
    incoming
        .apply(WriteBatch::default().put(b"new".to_vec(), vec![1]))
        .unwrap();
    let incoming = incoming.snapshot().unwrap();
    let mut owner = OfflineImageOwner::new(store);
    assert!(matches!(
        owner.inspect(&incoming, caps()),
        Err(ImageInstallError::Storage(
            StorageError::CorruptMetadata { .. } | StorageError::UnsupportedStoreFormat { .. }
        ))
    ));
    assert!(!owner.is_retired());
    let store = owner.into_store().unwrap();
    assert_eq!(
        observe(&store.database),
        before,
        "refusal must not write records, metadata, inventory or commit sequence"
    );
    assert_eq!(store.snapshot().unwrap(), cached_records);
    assert_eq!((store.meta.id(), store.records.id()), cached_ids);
    drop(store);
    let database = Database::builder(dir.path()).open().unwrap();
    let after_reopen = observe(&database);
    assert_eq!(after_reopen.names, before.names);
    assert_eq!(after_reopen.rows, before.rows);
}

#[test]
fn install_missing_malformed_and_unsupported_markers_refuse_without_repair() {
    for populated in [false, true] {
        for marker in [
            None,
            Some(vec![]),
            Some(vec![2]),
            Some(vec![0, 0, 0, 2, 0]),
            Some(0_u32.to_be_bytes().to_vec()),
            Some(STORE_FORMAT_V1.to_be_bytes().to_vec()),
            Some(u32::MAX.to_be_bytes().to_vec()),
        ] {
            let dir = TempDir::new().unwrap();
            let store = fixture(&dir, populated);
            let mut batch = store
                .database
                .batch()
                .durability(Some(PersistMode::SyncAll));
            match marker {
                None => batch.remove(&store.meta, FORMAT_VERSION_KEY),
                Some(marker) => batch.insert(&store.meta, FORMAT_VERSION_KEY, marker),
            }
            batch.commit().unwrap();
            refusal(store, &dir);
        }
    }
}

#[test]
fn install_extra_metadata_and_foreign_empty_or_populated_keyspaces_refuse() {
    for populated in [false, true] {
        for key in [b"\0early".as_slice(), b"\xfflate".as_slice()] {
            let dir = TempDir::new().unwrap();
            let store = fixture(&dir, populated);
            let mut batch = store
                .database
                .batch()
                .durability(Some(PersistMode::SyncAll));
            batch.insert(&store.meta, key, vec![0, 0xff]);
            batch.commit().unwrap();
            refusal(store, &dir);
        }
        for foreign_populated in [false, true] {
            for name in ["aaa_foreign", "zzz_foreign"] {
                let dir = TempDir::new().unwrap();
                let store = fixture(&dir, populated);
                let foreign = store
                    .database
                    .keyspace(name, KeyspaceCreateOptions::default)
                    .unwrap();
                if foreign_populated {
                    let mut batch = store
                        .database
                        .batch()
                        .durability(Some(PersistMode::SyncAll));
                    batch.insert(&foreign, b"foreign", vec![0xff, 0]);
                    batch.commit().unwrap();
                }
                drop(foreign);
                refusal(store, &dir);
            }
        }
    }
}

#[test]
fn install_replaced_known_handles_refuse_without_adoption() {
    for name in [META_KEYSPACE, RECORDS_KEYSPACE] {
        let dir = TempDir::new().unwrap();
        let store = fixture(&dir, true);
        let original = if name == META_KEYSPACE {
            store.meta.clone()
        } else {
            store.records.clone()
        };
        store.database.delete_keyspace(original.clone()).unwrap();
        let current = store
            .database
            .keyspace(name, KeyspaceCreateOptions::default)
            .unwrap();
        assert_ne!(original.id(), current.id());
        let mut batch = store
            .database
            .batch()
            .durability(Some(PersistMode::SyncAll));
        if name == META_KEYSPACE {
            batch.insert(
                &current,
                FORMAT_VERSION_KEY,
                ACTIVE_STORE_FORMAT.to_be_bytes().to_vec(),
            );
        } else {
            batch.insert(&current, b"replacement", vec![0xff]);
        }
        batch.commit().unwrap();
        drop(original);
        drop(current);
        refusal(store, &dir);
    }
}

#[test]
fn install_preserves_current_marker_and_exact_inventory_across_full_reopen() {
    for populated in [false, true] {
        let dir = TempDir::new().unwrap();
        let store = fixture(&dir, populated);
        let before = observe(&store.database);
        let source = MemoryStore::default();
        source
            .apply(WriteBatch::default().put(b"new".to_vec(), vec![0, 0xff]))
            .unwrap();
        let incoming = source.snapshot().unwrap();
        let mut owner = OfflineImageOwner::new(store);
        owner
            .inspect(&incoming, caps())
            .unwrap()
            .prepare()
            .unwrap()
            .apply()
            .unwrap();
        drop(owner);
        let reopened = FjallStore::open(dir.path()).unwrap();
        let after = observe(&reopened.database);
        assert_eq!(after.names, before.names);
        assert_eq!(after.rows[META_KEYSPACE], before.rows[META_KEYSPACE]);
        assert_eq!(after.rows[RECORDS_KEYSPACE], incoming.entries());
    }
}

#[test]
fn install_limit_refusals_and_abandoned_preparation_preserve_native_sequences() {
    let dir = TempDir::new().unwrap();
    let store = fixture(&dir, true);
    let before = observe(&store.database);
    let source = MemoryStore::default();
    source
        .apply(WriteBatch::default().put(b"new".to_vec(), vec![1]))
        .unwrap();
    let incoming = source.snapshot().unwrap();
    let exact = ImageInstallLimits {
        max_records: 1,
        max_key_bytes: 8,
        max_value_bytes: 2,
        max_mutations: 2,
        max_mutation_bytes: 12,
    };
    let mut owner = OfflineImageOwner::new(store);
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
            owner.inspect(&incoming, smaller).err(),
            Some(ImageInstallError::LimitExceeded {
                kind,
                requested,
                maximum: requested - 1,
            })
        );
        assert!(!owner.is_retired());
        let store = owner.into_store().unwrap();
        assert_eq!(
            observe(&store.database),
            before,
            "{kind:?} refusal must not commit even an unchanged batch"
        );
        owner = OfflineImageOwner::new(store);
    }
    drop(owner.inspect(&incoming, exact).unwrap().prepare().unwrap());
    assert!(!owner.is_retired());
    let store = owner.into_store().unwrap();
    assert_eq!(
        observe(&store.database),
        before,
        "abandoning preparation must not advance native commit sequences"
    );
    let mut owner = OfflineImageOwner::new(store);
    owner
        .inspect(&incoming, exact)
        .unwrap()
        .prepare()
        .unwrap()
        .apply()
        .unwrap();
    let store = owner.into_store().unwrap();
    let after = observe(&store.database);
    assert!(after.next > before.next);
    assert!(after.visible > before.visible);
    assert_eq!(after.names, before.names);
    assert_eq!(after.rows[META_KEYSPACE], before.rows[META_KEYSPACE]);
    assert_eq!(after.rows[RECORDS_KEYSPACE], incoming.entries());
}
