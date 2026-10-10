//! Private live fixtures mutate native metadata explicitly, not through StateStore.
//! Exclusive test ownership excludes every other writer and keyspace mutation.

use std::collections::BTreeMap;

use tempfile::TempDir;

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeImage {
    names: Vec<String>,
    rows: BTreeMap<String, Vec<(Key, Value)>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Observation {
    image: NativeImage,
    next_sequence: u64,
    visible_sequence: u64,
}

fn inventory(database: &Database) -> Vec<String> {
    let mut names: Vec<_> = database
        .list_keyspace_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect();
    names.sort();
    names
}

fn observe(database: &Database) -> Observation {
    let names = inventory(database);
    let snapshot = database.snapshot();
    let mut rows = BTreeMap::new();
    for name in &names {
        // Under exclusive test ownership the enumerated name still exists.
        // Opening this existing handle cannot create a missing keyspace.
        assert!(database.keyspace_exists(name));
        let keyspace = database
            .keyspace(name, KeyspaceCreateOptions::default)
            .unwrap();
        let entries = snapshot
            .iter(&keyspace)
            .map(|guard| read_entry(guard).unwrap())
            .collect();
        assert!(rows.insert(name.clone(), entries).is_none());
    }
    assert_eq!(inventory(database), names);
    Observation {
        image: NativeImage { names, rows },
        next_sequence: database.seqno(),
        visible_sequence: database.visible_seqno(),
    }
}

fn raw_reopen(directory: &Path) -> NativeImage {
    let database = Database::builder(directory).open().unwrap();
    let observed = observe(&database);
    drop(database);
    observed.image
}

fn cached_rows(store: &FjallStore) -> BTreeMap<String, Vec<(Key, Value)>> {
    let snapshot = store.database.snapshot();
    [
        (META_KEYSPACE, &store.meta),
        (RECORDS_KEYSPACE, &store.records),
    ]
    .into_iter()
    .map(|(name, keyspace)| {
        let entries = snapshot
            .iter(keyspace)
            .map(|guard| read_entry(guard).unwrap())
            .collect();
        (name.to_owned(), entries)
    })
    .collect()
}

fn binary_rows() -> Vec<(Key, Value)> {
    vec![
        (vec![0, 0xff], vec![0, 0xff, 0x80]),
        (b"format_version".to_vec(), vec![0xde, 0xad]),
        (b"zero".to_vec(), vec![]),
        (vec![0xff, 0, 1], vec![0x80, 0x7f]),
    ]
}

fn open_fixture(directory: &TempDir, populated: bool) -> FjallStore {
    let store = FjallStore::open(directory.path()).unwrap();
    if populated {
        let mut batch = WriteBatch::default();
        for (key, value) in binary_rows() {
            batch.push_put(key, value);
        }
        store.apply(batch).unwrap();
    }
    store
}

fn put_marker(store: &FjallStore, marker: Option<&[u8]>) {
    let mut batch = store
        .database
        .batch()
        .durability(Some(PersistMode::SyncAll));
    match marker {
        Some(bytes) => batch.insert(&store.meta, FORMAT_VERSION_KEY, bytes),
        None => batch.remove(&store.meta, FORMAT_VERSION_KEY),
    }
    batch.commit().unwrap();
}

fn put_meta(store: &FjallStore, key: &[u8], value: &[u8]) {
    let mut batch = store
        .database
        .batch()
        .durability(Some(PersistMode::SyncAll));
    batch.insert(&store.meta, key, value);
    batch.commit().unwrap();
}

fn assert_read_only_refusal(store: &FjallStore, expected: StorageError) -> NativeImage {
    // The baseline follows ordinary open and explicit raw fixture mutations.
    let before = observe(&store.database);
    assert_eq!(store.snapshot_with_provenance().unwrap_err(), expected);
    let after = observe(&store.database);
    assert_eq!(
        after, before,
        "inspection changed native names, rows or commit frontiers"
    );
    before.image
}

fn corrupt(detail: &str) -> StorageError {
    StorageError::CorruptMetadata {
        detail: detail.to_owned(),
    }
}

fn assert_image_after_supported_reopen(directory: &TempDir, expected: &NativeImage) {
    let store = FjallStore::open(directory.path()).unwrap();
    let before = observe(&store.database);
    assert_eq!(&before.image, expected);
    assert_read_only_refusal(
        &store,
        if expected.names.len() == 2 {
            corrupt("snapshot provenance requires only the format version marker")
        } else {
            corrupt("snapshot provenance requires exactly the meta and records keyspaces")
        },
    );
    drop(store);
    assert_eq!(&raw_reopen(directory.path()), expected);
}

#[test]
fn provenance_empty_and_populated_native_images_survive_clone_and_reopen() {
    for populated in [false, true] {
        let directory = TempDir::new().unwrap();
        let store = open_fixture(&directory, populated);
        let clone = store.clone();
        drop(store);
        let before = observe(&clone.database);
        assert_eq!(before.image.names, vec!["meta", "records"]);
        assert_eq!(
            before.image.rows["meta"],
            vec![(
                FORMAT_VERSION_KEY.to_vec(),
                ACTIVE_STORE_FORMAT.to_be_bytes().to_vec()
            )]
        );
        let captured = clone.snapshot_with_provenance().unwrap();
        assert_eq!(observe(&clone.database), before);
        assert_eq!(captured.snapshot().entries(), before.image.rows["records"]);
        assert_eq!(
            captured.snapshot().entries(),
            if populated { binary_rows() } else { vec![] }
        );
        let SnapshotProvenance::Fjall(metadata) = *captured.provenance() else {
            panic!("checked native capture lost Fjall provenance");
        };
        assert_eq!(metadata.format_version(), ACTIVE_STORE_FORMAT);
        assert_eq!(metadata.format_marker(), &ACTIVE_STORE_FORMAT.to_be_bytes());
        let retained = captured.clone();
        // Writes occur only after the completed exclusive inspection interval.
        clone
            .apply(WriteBatch::default().put(b"later".to_vec(), vec![]))
            .unwrap();
        assert_eq!(captured, retained);
        assert_eq!(metadata.format_marker(), &ACTIVE_STORE_FORMAT.to_be_bytes());
        let final_image = observe(&clone.database).image;
        drop(clone);
        assert_eq!(raw_reopen(directory.path()), final_image);
        let reopened = FjallStore::open(directory.path()).unwrap();
        let before_reopen_capture = observe(&reopened.database);
        assert_eq!(before_reopen_capture.image, final_image);
        let later = reopened.snapshot_with_provenance().unwrap();
        assert_eq!(observe(&reopened.database), before_reopen_capture);
        assert_eq!(later.snapshot().entries(), final_image.rows["records"]);
        assert_eq!(later.provenance(), captured.provenance());
        assert_eq!(captured, retained);
        drop(reopened);
        assert_eq!(raw_reopen(directory.path()), final_image);
    }
}

#[test]
fn provenance_missing_live_marker_refuses_without_metadata_repair() {
    for populated in [false, true] {
        let directory = TempDir::new().unwrap();
        let store = open_fixture(&directory, populated);
        put_marker(&store, None);
        let before = assert_read_only_refusal(
            &store,
            corrupt("snapshot provenance requires the format version marker"),
        );
        assert!(before.rows["meta"].is_empty());
        assert_eq!(
            before.rows["records"],
            if populated { binary_rows() } else { vec![] }
        );
        drop(store);
        assert_eq!(raw_reopen(directory.path()), before);
        if populated {
            assert!(matches!(
                FjallStore::open(directory.path()),
                Err(StorageError::CorruptMetadata { .. })
            ));
            assert_eq!(raw_reopen(directory.path()), before);
        }
        // Empty markerless ordinary open would legitimately stamp a new marker;
        // do not call it here and disguise that repair as inspection preservation.
    }
}

#[test]
fn provenance_live_marker_lengths_and_versions_refuse_with_exact_rows() {
    let mut cases = vec![
        (
            vec![],
            corrupt("format version record is 0 bytes, expected 4"),
        ),
        (
            vec![0, 0, 2],
            corrupt("format version record is 3 bytes, expected 4"),
        ),
        (
            vec![0, 0, 0, 2, 0],
            corrupt("format version record is 5 bytes, expected 4"),
        ),
    ];
    for version in [0_u32, STORE_FORMAT_V1, ACTIVE_STORE_FORMAT + 1, u32::MAX] {
        cases.push((
            version.to_be_bytes().to_vec(),
            StorageError::UnsupportedStoreFormat {
                found: version,
                expected: ACTIVE_STORE_FORMAT,
            },
        ));
    }
    for populated in [false, true] {
        for (marker, error) in &cases {
            let directory = TempDir::new().unwrap();
            let store = open_fixture(&directory, populated);
            put_marker(&store, Some(marker));
            let before = assert_read_only_refusal(&store, error.clone());
            assert_eq!(
                before.rows["meta"],
                vec![(FORMAT_VERSION_KEY.to_vec(), marker.clone())]
            );
            drop(store);
            assert_eq!(raw_reopen(directory.path()), before);
            // These marked malformed stores are refused, not repaired by open.
            let error_from_open = match FjallStore::open(directory.path()) {
                Ok(_) => panic!("ordinary open accepted an invalid marker"),
                Err(error) => error,
            };
            assert_eq!(&error_from_open, error);
            assert_eq!(raw_reopen(directory.path()), before);
        }
    }
}

#[test]
fn provenance_early_and_late_extra_meta_refuse_while_ordinary_open_stays_compatible() {
    for populated in [false, true] {
        for extra_key in [&b"\0early"[..], &b"\xfflate"[..]] {
            let directory = TempDir::new().unwrap();
            let store = open_fixture(&directory, populated);
            put_meta(&store, extra_key, &[]);
            let before = assert_read_only_refusal(
                &store,
                corrupt("snapshot provenance requires only the format version marker"),
            );
            assert_eq!(before.rows["meta"].len(), 2);
            assert!(
                before.rows["meta"]
                    .iter()
                    .any(|(key, value)| key.as_slice() == extra_key && value.is_empty())
            );
            assert_eq!(store.snapshot().unwrap().entries(), before.rows["records"]);
            drop(store);
            assert_eq!(raw_reopen(directory.path()), before);
            assert_image_after_supported_reopen(&directory, &before);
        }
    }
}

#[test]
fn provenance_empty_and_populated_foreign_names_refuse_without_inventory_changes() {
    for populated in [false, true] {
        for foreign_name in ["aaa_foreign", "zzz_foreign"] {
            for foreign_populated in [false, true] {
                let directory = TempDir::new().unwrap();
                let store = open_fixture(&directory, populated);
                let foreign = store
                    .database
                    .keyspace(foreign_name, KeyspaceCreateOptions::default)
                    .unwrap();
                if foreign_populated {
                    let mut batch = store
                        .database
                        .batch()
                        .durability(Some(PersistMode::SyncAll));
                    batch.insert(&foreign, b"binary\0key", vec![0, 0xff]);
                    batch.insert(&foreign, b"empty", Vec::<u8>::new());
                    batch.commit().unwrap();
                }
                let before = assert_read_only_refusal(
                    &store,
                    corrupt("snapshot provenance requires exactly the meta and records keyspaces"),
                );
                assert_eq!(before.names.len(), 3);
                assert!(before.names.iter().any(|name| name == foreign_name));
                assert_eq!(
                    before.rows[foreign_name],
                    if foreign_populated {
                        vec![
                            (b"binary\0key".to_vec(), vec![0, 0xff]),
                            (b"empty".to_vec(), vec![]),
                        ]
                    } else {
                        vec![]
                    }
                );
                assert_eq!(store.snapshot().unwrap().entries(), before.rows["records"]);
                drop(foreign);
                drop(store);
                assert_eq!(raw_reopen(directory.path()), before);
                assert_image_after_supported_reopen(&directory, &before);
            }
        }
    }
}

#[test]
fn provenance_foreign_only_directory_preserves_ordinary_initialization_policy() {
    for foreign_populated in [false, true] {
        let directory = TempDir::new().unwrap();
        let database = Database::builder(directory.path()).open().unwrap();
        let foreign = database
            .keyspace("foreign", KeyspaceCreateOptions::default)
            .unwrap();
        if foreign_populated {
            let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
            batch.insert(&foreign, b"retained", vec![0xff, 0]);
            batch.commit().unwrap();
        }
        let foreign_rows = observe(&database).image.rows["foreign"].clone();
        drop(foreign);
        drop(database);
        let store = FjallStore::open(directory.path()).unwrap();
        // Ordinary open's initialization is deliberately outside the baseline.
        let before = observe(&store.database);
        assert_eq!(before.image.names, vec!["foreign", "meta", "records"]);
        assert_eq!(before.image.rows["foreign"], foreign_rows);
        assert!(before.image.rows["records"].is_empty());
        assert_eq!(
            before.image.rows["meta"],
            vec![(
                FORMAT_VERSION_KEY.to_vec(),
                ACTIVE_STORE_FORMAT.to_be_bytes().to_vec()
            )]
        );
        let image = assert_read_only_refusal(
            &store,
            corrupt("snapshot provenance requires exactly the meta and records keyspaces"),
        );
        assert_eq!(image, before.image);
        drop(store);
        assert_eq!(raw_reopen(directory.path()), image);
        assert_image_after_supported_reopen(&directory, &image);
    }
}

fn replaced_known_keyspace_refuses_and_fresh_reopen_selects_current(name: &str) {
    let directory = TempDir::new().unwrap();
    let store = open_fixture(&directory, true);
    let original_rows = cached_rows(&store);
    let old = if name == META_KEYSPACE {
        store.meta.clone()
    } else {
        store.records.clone()
    };
    let old_id = old.id();
    // This is a prior, source-shaped topology mutation through private raw
    // aliases. No writer or topology mutation overlaps the inspection interval.
    store.database.delete_keyspace(old.clone()).unwrap();
    let replacement = store
        .database
        .keyspace(name, KeyspaceCreateOptions::default)
        .unwrap();
    assert_ne!(replacement.id(), old_id);
    let mut batch = store
        .database
        .batch()
        .durability(Some(PersistMode::SyncAll));
    if name == META_KEYSPACE {
        batch.insert(
            &replacement,
            FORMAT_VERSION_KEY,
            ACTIVE_STORE_FORMAT.to_be_bytes().to_vec(),
        );
    } else {
        batch.insert(&replacement, b"format_version", vec![0x80_u8, 0]);
        batch.insert(&replacement, b"replacement", Vec::<u8>::new());
        batch.insert(&replacement, vec![0xff_u8, 0], vec![0, 0xff]);
    }
    batch.commit().unwrap();
    let current_meta = store
        .database
        .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
        .unwrap();
    let current_records = store
        .database
        .keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)
        .unwrap();
    if name == META_KEYSPACE {
        assert_ne!(current_meta.id(), store.meta.id());
        assert_eq!(current_records.id(), store.records.id());
    } else {
        assert_eq!(current_meta.id(), store.meta.id());
        assert_ne!(current_records.id(), store.records.id());
    }
    let before = observe(&store.database);
    assert_eq!(before.image.names, vec!["meta", "records"]);
    assert_eq!(
        before.image.rows["meta"],
        vec![(
            FORMAT_VERSION_KEY.to_vec(),
            ACTIVE_STORE_FORMAT.to_be_bytes().to_vec()
        )]
    );
    assert_eq!(cached_rows(&store), original_rows);
    let image = assert_read_only_refusal(
        &store,
        corrupt("snapshot provenance requires the original meta and records keyspace handles"),
    );
    assert_eq!(image, before.image);
    assert_eq!(observe(&store.database), before);
    assert_eq!(cached_rows(&store), original_rows);
    assert_eq!(old.id(), old_id);
    assert_ne!(replacement.id(), old_id);
    let current_rows = image.rows["records"].clone();
    if name == RECORDS_KEYSPACE {
        assert_eq!(
            current_rows,
            vec![
                (b"format_version".to_vec(), vec![0x80, 0]),
                (b"replacement".to_vec(), vec![]),
                (vec![0xff, 0], vec![0, 0xff]),
            ]
        );
        assert_ne!(current_rows, original_rows["records"]);
    } else {
        assert_eq!(current_rows, original_rows["records"]);
    }
    // Drop both current and deleted-tree aliases before a real directory reopen.
    drop(current_meta);
    drop(current_records);
    drop(replacement);
    drop(old);
    drop(store);
    assert_eq!(raw_reopen(directory.path()), image);
    let reopened = FjallStore::open(directory.path()).unwrap();
    let reopen_before = observe(&reopened.database);
    assert_eq!(reopen_before.image, image);
    let captured = reopened.snapshot_with_provenance().unwrap();
    assert_eq!(captured.snapshot().entries(), current_rows);
    let SnapshotProvenance::Fjall(metadata) = *captured.provenance() else {
        panic!("fresh normal store did not select the valid current keyspaces");
    };
    assert_eq!(metadata.format_marker(), &ACTIVE_STORE_FORMAT.to_be_bytes());
    assert_eq!(observe(&reopened.database), reopen_before);
    drop(reopened);
    assert_eq!(raw_reopen(directory.path()), image);
}

#[test]
fn provenance_replaced_meta_handle_refuses_without_adopting_same_name() {
    replaced_known_keyspace_refuses_and_fresh_reopen_selects_current(META_KEYSPACE);
}

#[test]
fn provenance_replaced_records_handle_refuses_without_adopting_same_name() {
    replaced_known_keyspace_refuses_and_fresh_reopen_selects_current(RECORDS_KEYSPACE);
}
