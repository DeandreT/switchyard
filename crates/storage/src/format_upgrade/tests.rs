use std::{
    panic::{AssertUnwindSafe, catch_unwind, panic_any},
    sync::Arc,
};

use fjall::{KeyspaceCreateOptions, Readable};
use tempfile::TempDir;

use super::*;
use crate::{FjallStore, Key, SnapshotProvenance, StateStore, Value, WriteBatch, durable};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Fault {
    None,
    ErrorBefore,
    ErrorAfter,
    PanicBefore,
    PanicAfter,
}

const PAYLOAD: &str = "original marker-commit panic, not an upgrade replacement";

impl Fault {
    pub(super) fn before(self) -> Result<(), StoreFormatUpgradeError> {
        match self {
            Self::ErrorBefore => Err(StorageError::LockPoisoned.into()),
            Self::PanicBefore => panic_any(Arc::new(PAYLOAD)),
            _ => Ok(()),
        }
    }

    pub(super) fn after(self) -> Result<(), StoreFormatUpgradeError> {
        match self {
            Self::ErrorAfter => Err(StorageError::LockPoisoned.into()),
            Self::PanicAfter => panic_any(Arc::new(PAYLOAD)),
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Rows {
    meta: Vec<(Key, Value)>,
    records: Vec<(Key, Value)>,
}

fn marker(version: u32) -> (Key, Value) {
    (FORMAT_VERSION_KEY.to_vec(), version.to_be_bytes().to_vec())
}

fn records() -> Vec<(Key, Value)> {
    vec![
        (vec![0x00], vec![1, 0, 0, 0, 0, 0, 0, 0, 9]),
        (b"\x03opaque".to_vec(), Vec::new()),
        (b"\xF0switchyard/journal\0\0".to_vec(), vec![0xff; 3]),
        (b"\xF1tail".to_vec(), b"retained bytes".to_vec()),
    ]
}

/// Raw native writes, standing in for an older build or a corrupt directory.
fn seed(directory: &Path, rows: &Rows, extra_keyspace: Option<&str>) {
    let database = Database::builder(directory).open().unwrap();
    let meta = database
        .keyspace("meta", KeyspaceCreateOptions::default)
        .unwrap();
    let records = database
        .keyspace("records", KeyspaceCreateOptions::default)
        .unwrap();
    if let Some(name) = extra_keyspace {
        database
            .keyspace(name, KeyspaceCreateOptions::default)
            .unwrap();
    }
    let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
    for (key, value) in &rows.meta {
        batch.insert(&meta, key.as_slice(), value.as_slice());
    }
    for (key, value) in &rows.records {
        batch.insert(&records, key.as_slice(), value.as_slice());
    }
    batch.commit().unwrap();
}

/// Full native reopen. Recovery reads names before any create-or-open lookup.
fn read(directory: &Path) -> (Vec<String>, Rows) {
    let database = Database::recover(Database::builder(directory).into_config()).unwrap();
    let mut names = database
        .list_keyspace_names()
        .iter()
        .map(|name| String::from_utf8(name.as_bytes().to_vec()).unwrap())
        .collect::<Vec<_>>();
    names.sort();
    let mut rows = Rows::default();
    let snapshot = database.snapshot();
    if names.iter().any(|name| name == "meta") {
        let meta = database
            .keyspace("meta", KeyspaceCreateOptions::default)
            .unwrap();
        rows.meta = snapshot
            .iter(&meta)
            .map(durable::read_entry)
            .collect::<Result<_, _>>()
            .unwrap();
    }
    if names.iter().any(|name| name == "records") {
        let records = database
            .keyspace("records", KeyspaceCreateOptions::default)
            .unwrap();
        rows.records = snapshot
            .iter(&records)
            .map(durable::read_entry)
            .collect::<Result<_, _>>()
            .unwrap();
    }
    (names, rows)
}

fn known() -> Vec<String> {
    vec![String::from("meta"), String::from("records")]
}

fn version_two(records: Vec<(Key, Value)>) -> Rows {
    Rows {
        meta: vec![marker(STORE_FORMAT_V2)],
        records,
    }
}

fn refusal(directory: &Path) -> StoreFormatUpgradeError {
    match StoreFormatUpgradeOwner::open_existing(directory) {
        Err(error) => error,
        Ok(mut owner) => {
            let error = match owner.inspect() {
                Err(error) => error,
                Ok(_) => panic!("inspection must refuse"),
            };
            assert!(!owner.is_retired());
            error
        }
    }
}

#[test]
fn empty_and_populated_version_two_upgrade_only_the_marker() {
    for retained in [Vec::new(), records()] {
        let directory = TempDir::new().unwrap();
        seed(directory.path(), &version_two(retained.clone()), None);
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V2,
                expected: STORE_FORMAT_V3,
            }),
            "ordinary open never upgrades version 2"
        );
        let mut owner = StoreFormatUpgradeOwner::open_existing(directory.path()).unwrap();
        assert_eq!(owner.directory(), directory.path());
        let StoreFormatInspection::Eligible(prepared) = owner.inspect().unwrap() else {
            panic!("version 2 is eligible");
        };
        assert_eq!(prepared.image().snapshot().entries(), retained.as_slice());
        let SnapshotProvenance::Fjall(metadata) = prepared.image().provenance() else {
            panic!("native provenance");
        };
        assert_eq!(
            metadata.format_version(),
            STORE_FORMAT_V2,
            "never relabeled"
        );
        let seen = Arc::clone(prepared.image());
        prepared
            .commit_validated(|image| {
                assert!(Arc::ptr_eq(image, &seen), "the bound image is validated");
                Ok::<_, ()>(())
            })
            .unwrap();
        assert!(!owner.is_retired());
        assert!(matches!(
            owner.inspect().unwrap(),
            StoreFormatInspection::AlreadyCurrent
        ));
        drop(owner);

        let expected = Rows {
            meta: vec![marker(STORE_FORMAT_V3)],
            records: retained.clone(),
        };
        assert_eq!(read(directory.path()), (known(), expected));
        let store = FjallStore::open(directory.path()).unwrap();
        assert_eq!(store.snapshot().unwrap().entries(), retained.as_slice());
        let SnapshotProvenance::Fjall(metadata) =
            *store.snapshot_with_provenance().unwrap().provenance()
        else {
            panic!("native provenance");
        };
        assert_eq!(metadata.format_version(), STORE_FORMAT_V3);
    }
}

#[test]
fn version_three_repeats_are_already_current_without_writes() {
    let directory = TempDir::new().unwrap();
    let store = FjallStore::open(directory.path()).unwrap();
    store
        .apply(WriteBatch::default().put(b"\x03retained".to_vec(), vec![7]))
        .unwrap();
    drop(store);
    let before = read(directory.path());
    for _ in 0..2 {
        let mut owner = StoreFormatUpgradeOwner::open_existing(directory.path()).unwrap();
        let seqno = owner.database.seqno();
        for _ in 0..2 {
            assert!(matches!(
                owner.inspect().unwrap(),
                StoreFormatInspection::AlreadyCurrent
            ));
        }
        assert_eq!(owner.database.seqno(), seqno, "no native write");
        drop(owner);
        assert_eq!(read(directory.path()), before);
    }
}

#[test]
fn missing_or_non_database_directories_refuse_without_creation() {
    let parent = TempDir::new().unwrap();
    let missing = parent.path().join("absent");
    assert!(matches!(
        StoreFormatUpgradeOwner::open_existing(&missing),
        Err(StoreFormatUpgradeError::Storage(
            StorageError::Backend { .. }
        ))
    ));
    assert!(!missing.exists());
    let empty = parent.path().join("empty");
    std::fs::create_dir(&empty).unwrap();
    assert!(matches!(
        StoreFormatUpgradeOwner::open_existing(&empty),
        Err(StoreFormatUpgradeError::Storage(
            StorageError::Backend { .. }
        ))
    ));
    assert_eq!(std::fs::read_dir(&empty).unwrap().count(), 0);
}

#[test]
fn unsupported_unversioned_and_malformed_markers_refuse_without_mutation() {
    let corrupt = |detail: &str| {
        StoreFormatUpgradeError::Storage(StorageError::CorruptMetadata {
            detail: String::from(detail),
        })
    };
    let mut cases = Vec::new();
    for found in [0, durable::STORE_FORMAT_V1, STORE_FORMAT_V3 + 1, u32::MAX] {
        cases.push((
            vec![marker(found)],
            StoreFormatUpgradeError::UnsupportedSource {
                found,
                target: STORE_FORMAT_V3,
            },
        ));
    }
    cases.push((
        Vec::new(),
        corrupt("snapshot provenance requires the format version marker"),
    ));
    cases.push((
        vec![(b"unknown".to_vec(), Vec::new())],
        corrupt("snapshot provenance requires only the format version marker"),
    ));
    // Extra metadata refuses for either eligible or current markers; a valid
    // marker 3 never hides it behind already-current.
    for version in [STORE_FORMAT_V2, STORE_FORMAT_V3] {
        for extra in [b"a-other".to_vec(), b"other".to_vec()] {
            cases.push((
                vec![marker(version), (extra, Vec::new())],
                corrupt("snapshot provenance requires only the format version marker"),
            ));
        }
    }
    for width in [0, 3, 5] {
        cases.push((
            vec![(FORMAT_VERSION_KEY.to_vec(), vec![0; width])],
            corrupt(&format!(
                "format version record is {width} bytes, expected 4"
            )),
        ));
    }
    for (mut meta, expected) in cases {
        meta.sort();
        let directory = TempDir::new().unwrap();
        let rows = Rows {
            meta,
            records: records(),
        };
        seed(directory.path(), &rows, None);
        for _ in 0..2 {
            assert_eq!(refusal(directory.path()), expected);
            assert_eq!(read(directory.path()), (known(), rows.clone()));
        }
    }
}

#[test]
fn inventory_and_handle_corruption_refuses_without_creating_keyspaces() {
    let corrupt = |detail: &str| {
        StoreFormatUpgradeError::Storage(StorageError::CorruptMetadata {
            detail: String::from(detail),
        })
    };
    let inventory = corrupt("snapshot provenance requires exactly the meta and records keyspaces");

    for version in [STORE_FORMAT_V2, STORE_FORMAT_V3] {
        let directory = TempDir::new().unwrap();
        let rows = Rows {
            meta: vec![marker(version)],
            records: records(),
        };
        seed(directory.path(), &rows, Some("foreign"));
        assert_eq!(refusal(directory.path()), inventory, "marker {version}");
        let (names, found) = read(directory.path());
        assert_eq!(names, ["foreign", "meta", "records"]);
        assert_eq!(found, rows);
    }

    // Only `meta`: the missing records keyspace must not be created.
    let directory = TempDir::new().unwrap();
    {
        let database = Database::builder(directory.path()).open().unwrap();
        let meta = database
            .keyspace("meta", KeyspaceCreateOptions::default)
            .unwrap();
        let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
        batch.insert(&meta, FORMAT_VERSION_KEY, STORE_FORMAT_V2.to_be_bytes());
        batch.commit().unwrap();
    }
    assert_eq!(refusal(directory.path()), inventory);
    assert_eq!(read(directory.path()).0, ["meta"]);

    // Replaced handles and topology added after open are both refused.
    for replace in [true, false] {
        let directory = TempDir::new().unwrap();
        seed(directory.path(), &version_two(records()), None);
        let mut owner = StoreFormatUpgradeOwner::open_existing(directory.path()).unwrap();
        if replace {
            owner
                .database
                .delete_keyspace(owner.records.clone())
                .unwrap();
            owner
                .database
                .keyspace("records", KeyspaceCreateOptions::default)
                .unwrap();
        } else {
            owner
                .database
                .keyspace("late", KeyspaceCreateOptions::default)
                .unwrap();
        }
        let expected = if replace {
            corrupt("snapshot provenance requires the original meta and records keyspace handles")
        } else {
            inventory.clone()
        };
        assert_eq!(owner.inspect().err(), Some(expected));
        assert!(!owner.is_retired());
    }
}

#[test]
fn validation_refusal_writes_nothing_and_keeps_the_owner_usable() {
    let directory = TempDir::new().unwrap();
    seed(directory.path(), &version_two(records()), None);
    let mut owner = StoreFormatUpgradeOwner::open_existing(directory.path()).unwrap();
    owner.fault = Fault::PanicBefore; // Reaching the marker commit would panic.
    for _ in 0..2 {
        let StoreFormatInspection::Eligible(prepared) = owner.inspect().unwrap() else {
            panic!("version 2 is eligible");
        };
        assert_eq!(
            prepared.commit_validated(|image| {
                assert_eq!(image.snapshot().entries(), records().as_slice());
                Err("unsafe content")
            }),
            Err(StoreFormatCommitError::Validation("unsafe content"))
        );
        assert!(!owner.is_retired());
    }
    drop(owner);
    assert_eq!(read(directory.path()), (known(), version_two(records())));
}

#[test]
fn abandoned_preparation_writes_nothing_and_keeps_the_owner_usable() {
    let directory = TempDir::new().unwrap();
    seed(directory.path(), &version_two(records()), None);
    let mut owner = StoreFormatUpgradeOwner::open_existing(directory.path()).unwrap();
    for _ in 0..2 {
        let StoreFormatInspection::Eligible(prepared) = owner.inspect().unwrap() else {
            panic!("version 2 is eligible");
        };
        drop(prepared);
        assert!(!owner.is_retired());
    }
    drop(owner);
    assert_eq!(read(directory.path()), (known(), version_two(records())));
}

#[test]
fn commit_faults_retire_and_full_reopen_observes_old_or_new_marker() {
    for fault in [
        Fault::ErrorBefore,
        Fault::ErrorAfter,
        Fault::PanicBefore,
        Fault::PanicAfter,
    ] {
        let directory = TempDir::new().unwrap();
        seed(directory.path(), &version_two(records()), None);
        let mut owner = StoreFormatUpgradeOwner::open_existing(directory.path()).unwrap();
        owner.fault = fault;
        let StoreFormatInspection::Eligible(prepared) = owner.inspect().unwrap() else {
            panic!("version 2 is eligible");
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            prepared.commit_validated(|_| Ok::<_, ()>(()))
        }));
        match fault {
            Fault::ErrorBefore | Fault::ErrorAfter => assert_eq!(
                result.unwrap(),
                Err(StoreFormatCommitError::Upgrade(
                    StoreFormatUpgradeError::Storage(StorageError::LockPoisoned)
                ))
            ),
            Fault::PanicBefore | Fault::PanicAfter => {
                let payload = result.unwrap_err().downcast::<Arc<&str>>().unwrap();
                assert_eq!(**payload, PAYLOAD, "original panic escapes unchanged");
            }
            Fault::None => unreachable!(),
        }
        assert!(owner.is_retired(), "{fault:?}");
        assert!(matches!(
            owner.inspect(),
            Err(StoreFormatUpgradeError::Retired)
        ));
        drop(owner);

        let committed = matches!(fault, Fault::ErrorAfter | Fault::PanicAfter);
        let expected = Rows {
            meta: vec![marker(if committed {
                STORE_FORMAT_V3
            } else {
                STORE_FORMAT_V2
            })],
            records: records(),
        };
        assert_eq!(read(directory.path()), (known(), expected), "{fault:?}");
        assert_eq!(FjallStore::open(directory.path()).is_ok(), committed);
    }
}
