use std::path::Path;

use tempfile::TempDir;

use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;
type LogicalRows = Vec<(String, Key, Value)>;

fn rewrite_format(directory: &Path, format: u32) -> TestResult {
    let database = Database::builder(directory).worker_threads(1).open()?;
    let meta = database.keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)?;
    let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
    batch.insert(&meta, FORMAT_VERSION_KEY, format.to_be_bytes().to_vec());
    batch.commit()?;
    Ok(())
}

fn logical_rows(directory: &Path) -> Result<LogicalRows, Box<dyn std::error::Error>> {
    let database = Database::builder(directory).worker_threads(1).open()?;
    let mut rows = Vec::new();
    for name in [META_KEYSPACE, RECORDS_KEYSPACE] {
        let keyspace = database.keyspace(name, KeyspaceCreateOptions::default)?;
        for row in keyspace.iter() {
            let (key, value) = row.into_inner()?;
            rows.push((name.into(), key.to_vec(), value.to_vec()));
        }
    }
    Ok(rows)
}

#[test]
fn mandatory_topic_layout_eighteen_advances_every_derived_namespace_without_changing_seventeen() {
    assert_eq!(STORE_FORMAT_V17, 17);
    assert_eq!(STORE_FORMAT_V18, 18);
    assert_eq!(ACTIVE_STORE_FORMAT, STORE_FORMAT_V18);
    assert_eq!(ACTIVE_REPLICA_STORE_FORMAT, 0x8000_0012);
    assert_eq!(ACTIVE_CATALOG_REPLICA_STORE_FORMAT, 0xc000_0012);
    assert_eq!(ACTIVE_PROTECTED_STATE_STORE_FORMAT, 0xd000_0012);
    for namespace in [
        0,
        0x8000_0000,
        0xc000_0000,
        0xd000_0000,
        0xa000_0000,
        0xb000_0000,
    ] {
        let old = namespace | STORE_FORMAT_V17;
        let current = namespace | STORE_FORMAT_V18;
        assert_eq!(
            require_format_version(&current.to_be_bytes(), current),
            Ok(())
        );
        assert_eq!(require_format_version(&old.to_be_bytes(), old), Ok(()));
        assert_eq!(
            require_format_version(&old.to_be_bytes(), current),
            Err(StorageError::UnsupportedStoreFormat {
                found: old,
                expected: current
            })
        );
        assert_eq!(
            require_format_version(&current.to_be_bytes(), old),
            Err(StorageError::UnsupportedStoreFormat {
                found: current,
                expected: old
            })
        );
    }
    assert_eq!(
        require_format_version(&STORE_FORMAT_V17.to_be_bytes(), STORE_FORMAT_V16),
        Err(StorageError::UnsupportedStoreFormat {
            found: 17,
            expected: 16
        })
    );
}

#[test]
#[cfg(unix)]
fn previous_standalone_replica_catalog_and_protected_layouts_refuse_without_logical_rewrite()
-> TestResult {
    for role in 0..4 {
        let parent = TempDir::new()?;
        let directory = parent.path().join("selected");
        let (old, current) = match role {
            0 => {
                drop(FjallStore::open(&directory)?);
                (17, ACTIVE_STORE_FORMAT)
            }
            1 => {
                drop(FjallReplicaStore::open(&directory)?);
                (0x8000_0011, ACTIVE_REPLICA_STORE_FORMAT)
            }
            2 => {
                drop(FjallCatalogReplicaStore::open(&directory)?);
                (0xc000_0011, ACTIVE_CATALOG_REPLICA_STORE_FORMAT)
            }
            _ => {
                drop(FjallProtectedStateStore::create_new(&directory)?);
                (0xd000_0011, ACTIVE_PROTECTED_STATE_STORE_FORMAT)
            }
        };
        rewrite_format(&directory, old)?;
        let before = logical_rows(&directory)?;
        match role {
            0 => assert_eq!(
                FjallStore::open(&directory).err(),
                Some(StorageError::UnsupportedStoreFormat {
                    found: old,
                    expected: current
                })
            ),
            1 => assert_eq!(
                FjallReplicaStore::open(&directory).err(),
                Some(StorageError::UnsupportedStoreFormat {
                    found: old,
                    expected: current
                })
            ),
            2 => assert_eq!(
                FjallCatalogReplicaStore::open(&directory).err(),
                Some(StorageError::UnsupportedStoreFormat {
                    found: old,
                    expected: current
                })
            ),
            _ => assert_eq!(
                FjallProtectedStateStore::open_existing(&directory).err(),
                Some(FjallProtectedStateOpenError::InvalidLayout)
            ),
        }
        assert_eq!(logical_rows(&directory)?, before);
        rewrite_format(&directory, current)?;
        match role {
            0 => {
                drop(FjallStore::open(&directory)?);
            }
            1 => {
                drop(FjallReplicaStore::open(&directory)?);
            }
            2 => {
                drop(FjallCatalogReplicaStore::open(&directory)?);
            }
            _ => {
                drop(FjallProtectedStateStore::open_existing(&directory)?);
            }
        }
    }
    Ok(())
}
