use fjall::{Database, KeyspaceCreateOptions, PersistMode, Readable};
use storage::{
    ACTIVE_STORE_FORMAT, CONSENSUS_STORE_FORMAT, ConsensusBackend, ConsensusFjallStore,
    ConsensusMemoryStore, FjallStore, StateStore, StorageError, StoreFormatUpgradeError,
    StoreFormatUpgradeOwner, WriteBatch,
};

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

fn raw(
    directory: &std::path::Path,
    seed: bool,
    marker: Option<&[u8]>,
    metadata: Rows,
    records: Rows,
    extra: bool,
) -> (Rows, Rows, Vec<String>) {
    let database = Database::builder(directory).open().unwrap();
    let meta = database
        .keyspace("meta", KeyspaceCreateOptions::default)
        .unwrap();
    let data = database
        .keyspace("records", KeyspaceCreateOptions::default)
        .unwrap();
    if seed {
        if extra {
            database
                .keyspace("foreign", KeyspaceCreateOptions::default)
                .unwrap();
        }
        let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
        if let Some(marker) = marker {
            batch.insert(&meta, b"format_version", marker);
        }
        for (k, v) in metadata {
            batch.insert(&meta, k, v);
        }
        for (k, v) in records {
            batch.insert(&data, k, v);
        }
        batch.commit().unwrap();
    }
    let snapshot = database.snapshot();
    let rows = |space: &fjall::Keyspace| {
        snapshot
            .iter(space)
            .map(|v| {
                let (k, v) = v.into_inner().unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect()
    };
    let mut names: Vec<_> = database
        .list_keyspace_names()
        .iter()
        .map(ToString::to_string)
        .collect();
    names.sort();
    (rows(&meta), rows(&data), names)
}

#[test]
fn independent_profiles_and_same_directory_owner() {
    assert_eq!(ACTIVE_STORE_FORMAT, 3);
    assert_eq!(CONSENSUS_STORE_FORMAT, 4);
    let directory = tempfile::tempdir().unwrap();
    let mut backend = ConsensusFjallStore::open(directory.path()).unwrap();
    assert!(ConsensusFjallStore::open(directory.path()).is_err());
    assert!(FjallStore::open(directory.path()).is_err());
    backend
        .apply(WriteBatch::default().put(b"\xF2test".to_vec(), b"durable".to_vec()))
        .unwrap();
    assert_eq!(backend.get(b"\xF2test").unwrap().unwrap(), b"durable");
    drop(backend);
    let before = raw(directory.path(), false, None, vec![], vec![], false);
    assert!(matches!(
        FjallStore::open(directory.path()),
        Err(StorageError::UnsupportedStoreFormat {
            found: 4,
            expected: 3
        })
    ));
    let mut offline = StoreFormatUpgradeOwner::open_existing(directory.path()).unwrap();
    assert!(matches!(
        offline.inspect(),
        Err(StoreFormatUpgradeError::UnsupportedSource {
            found: 4,
            target: 3
        })
    ));
    drop(offline);
    assert_eq!(
        before,
        raw(directory.path(), false, None, vec![], vec![], false)
    );
    let backend = ConsensusFjallStore::open(directory.path()).unwrap();
    assert_eq!(backend.get(b"\xF2test").unwrap().unwrap(), b"durable");
}

#[test]
fn legacy_and_corrupt_inventory_refusals_preserve_logical_rows() {
    let cases = [
        (Some(3u32.to_be_bytes().to_vec()), vec![], vec![], false),
        (None, vec![], vec![(b"\xF0local".to_vec(), vec![1])], false),
        (None, vec![(b"unknown".to_vec(), vec![])], vec![], false),
        (
            Some(4u32.to_be_bytes().to_vec()),
            vec![(b"unknown".to_vec(), vec![])],
            vec![],
            false,
        ),
        (Some(vec![4]), vec![], vec![], false),
        (Some(99u32.to_be_bytes().to_vec()), vec![], vec![], false),
        (Some(4u32.to_be_bytes().to_vec()), vec![], vec![], true),
    ];
    for (marker, metadata, records, extra) in cases {
        let directory = tempfile::tempdir().unwrap();
        let before = raw(
            directory.path(),
            true,
            marker.as_deref(),
            metadata,
            records,
            extra,
        );
        for _ in 0..2 {
            assert!(ConsensusFjallStore::open(directory.path()).is_err());
            assert_eq!(
                before,
                raw(directory.path(), false, None, vec![], vec![], false)
            );
        }
    }
}

#[test]
fn missing_inventory_does_not_create_keyspaces() {
    let directory = tempfile::tempdir().unwrap();
    {
        let db = Database::builder(directory.path()).open().unwrap();
        db.keyspace("foreign", KeyspaceCreateOptions::default)
            .unwrap();
    }
    assert!(ConsensusFjallStore::open(directory.path()).is_err());
    let db = Database::builder(directory.path()).open().unwrap();
    let names = db.list_keyspace_names();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0].as_bytes(), b"foreign");
}

#[test]
fn memory_is_isolated_and_scans_are_bounded() {
    let mut backend = ConsensusMemoryStore::default();
    backend
        .apply(
            WriteBatch::default()
                .put(vec![1], vec![2])
                .put(vec![3], vec![4]),
        )
        .unwrap();
    assert_eq!(
        backend.scan_from(b"", b"", 1).unwrap(),
        vec![(vec![1], vec![2])]
    );
    assert!(backend.scan_from(b"", b"", 0).is_err());
    assert!(backend.scan_from(b"", b"", 65).is_err());
}

#[test]
fn ordinary_profile_three_behavior_is_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let store = FjallStore::open(directory.path()).unwrap();
    store
        .apply(WriteBatch::default().put(b"domain".to_vec(), vec![1]))
        .unwrap();
    drop(store);
    assert!(ConsensusFjallStore::open(directory.path()).is_err());
    let store = FjallStore::open(directory.path()).unwrap();
    assert_eq!(store.get(b"domain").unwrap(), Some(vec![1]));
    assert_eq!(
        match store.snapshot_with_provenance().unwrap().provenance() {
            storage::SnapshotProvenance::Fjall(m) => m.format_version(),
            _ => 0,
        },
        3
    );
}
