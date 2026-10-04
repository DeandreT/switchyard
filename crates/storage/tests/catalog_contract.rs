use std::{error::Error, fmt::Debug};

use storage::{
    ACTIVE_CATALOG_REPLICA_STORE_FORMAT, ACTIVE_REPLICA_STORE_FORMAT, ACTIVE_STORE_FORMAT,
    BoundedStateStore, CatalogCommittedStore, CommittedStore, FjallCatalogReplicaStore,
    FjallReplicaStore, FjallStore, MAX_CATALOG_METADATA_BYTES, MemoryCatalogReplicaStore,
    ReadLimits, SnapshotCatalogReader, SnapshotCatalogRecord, StateStore, StorageError, WriteBatch,
};
use tempfile::TempDir;

#[path = "catalog_contract/crash.rs"]
mod crash;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

trait Fixture: Sized {
    type Writer: CatalogCommittedStore + Debug;

    fn new() -> TestResult<Self>;
    fn writer(&mut self) -> &mut Self::Writer;
}

struct MemoryFixture(MemoryCatalogReplicaStore);

impl Fixture for MemoryFixture {
    type Writer = MemoryCatalogReplicaStore;

    fn new() -> TestResult<Self> {
        Ok(Self(MemoryCatalogReplicaStore::new()))
    }

    fn writer(&mut self) -> &mut Self::Writer {
        &mut self.0
    }
}

struct FjallFixture {
    writer: FjallCatalogReplicaStore,
    _directory: TempDir,
}

impl Fixture for FjallFixture {
    type Writer = FjallCatalogReplicaStore;

    fn new() -> TestResult<Self> {
        let directory = TempDir::new()?;
        Ok(Self {
            writer: FjallCatalogReplicaStore::open(directory.path())?,
            _directory: directory,
        })
    }

    fn writer(&mut self) -> &mut Self::Writer {
        &mut self.writer
    }
}

fn absent_and_present_empty_slots_are_distinct_and_reader_writes_are_refused<F: Fixture>()
-> TestResult {
    let mut fixture = F::new()?;
    let reader = fixture.writer().reader();
    let catalog_reader = fixture.writer().catalog_reader();
    assert!(!fixture.writer().is_initialized()?);
    assert!(reader.snapshot()?.entries().is_empty());
    assert!(catalog_reader.read_catalog()?.is_none());
    for batch in [
        WriteBatch::default(),
        WriteBatch::default().put(b"business", b"forbidden"),
        WriteBatch::default().delete(b"business"),
    ] {
        assert_eq!(reader.apply(batch), Err(StorageError::ReplicaWriteRequired));
        assert!(!fixture.writer().is_initialized()?);
        assert!(reader.snapshot()?.entries().is_empty());
        assert!(catalog_reader.read_catalog()?.is_none());
    }
    fixture.writer().commit(WriteBatch::default())?;
    assert!(fixture.writer().is_initialized()?);
    assert!(catalog_reader.read_catalog()?.is_none());
    fixture
        .writer()
        .commit_with_catalog(WriteBatch::default(), SnapshotCatalogRecord::new(&[], &[])?)?;
    assert!(fixture.writer().is_initialized()?);
    let slot = catalog_reader
        .read_catalog()?
        .ok_or("missing present empty catalog")?;
    assert!(slot.metadata().is_empty());
    assert!(slot.artifact().is_empty());
    assert!(reader.snapshot()?.entries().is_empty());
    assert_eq!(
        reader.apply(WriteBatch::default()),
        Err(StorageError::ReplicaWriteRequired)
    );
    Ok(())
}

fn business_views_exclude_the_catalog_and_keep_ordered_batch_semantics<F: Fixture>() -> TestResult
where
    <F::Writer as storage::CommittedStore>::Reader: BoundedStateStore,
{
    let mut fixture = F::new()?;
    let reader = fixture.writer().reader();
    let metadata = vec![7; MAX_CATALOG_METADATA_BYTES];
    let artifact = vec![9; 16 * 1024];
    fixture.writer().commit_with_catalog(
        WriteBatch::default()
            .put(b"scope/b", b"second")
            .put(b"scope/a", b"stale")
            .delete(b"scope/a")
            .put(b"scope/a", b"final")
            .put(b"snapshot_meta", b"caller business record")
            .put(b"snapshot_image", b"another business record"),
        SnapshotCatalogRecord::new(&metadata, &artifact)?,
    )?;
    assert_eq!(reader.get(b"scope/a")?, Some(b"final".to_vec()));
    assert_eq!(reader.get(b"format_version")?, None);
    assert_eq!(reader.get(b"replica_profile")?, None);
    assert_eq!(reader.get(b"replica_initialized")?, None);
    assert_eq!(
        reader.get(b"snapshot_meta")?,
        Some(b"caller business record".to_vec())
    );
    assert_eq!(
        reader.scan_prefix(b"scope/", 9)?,
        vec![
            (b"scope/a".to_vec(), b"final".to_vec()),
            (b"scope/b".to_vec(), b"second".to_vec())
        ]
    );
    assert_eq!(
        reader.scan_from(b"scope/", b"scope/b", 1)?,
        vec![(b"scope/b".to_vec(), b"second".to_vec())]
    );
    let snapshot = reader.snapshot()?;
    assert_eq!(snapshot.entries().len(), 4);
    assert_eq!(
        reader.snapshot_bounded(ReadLimits {
            max_rows: 4,
            max_key_bytes: 32,
            max_value_bytes: 32,
            max_total_bytes: 256,
        })?,
        snapshot
    );
    assert_eq!(
        reader.snapshot_bounded(ReadLimits {
            max_rows: 3,
            max_key_bytes: 32,
            max_value_bytes: 32,
            max_total_bytes: 256,
        }),
        Err(StorageError::ReadLimitExceeded)
    );
    let slot = fixture
        .writer()
        .catalog_reader()
        .read_catalog()?
        .ok_or("missing catalog")?;
    assert_eq!(slot.metadata(), metadata);
    assert_eq!(slot.artifact(), artifact);
    Ok(())
}

fn ordinary_commits_preserve_an_older_catalog_without_interpreting_business_progress<F: Fixture>()
-> TestResult {
    let mut fixture = F::new()?;
    let reader = fixture.writer().reader();
    let catalog_reader = fixture.writer().catalog_reader();
    fixture.writer().commit_with_catalog(
        WriteBatch::default().put(b"opaque-business-progress", b"first"),
        SnapshotCatalogRecord::new(b"older metadata", b"older image")?,
    )?;
    let retained = catalog_reader
        .read_catalog()?
        .ok_or("missing old catalog")?;
    fixture
        .writer()
        .commit(WriteBatch::default().put(b"opaque-business-progress", b"newer"))?;
    fixture.writer().commit(WriteBatch::default())?;
    assert_eq!(
        reader.get(b"opaque-business-progress")?,
        Some(b"newer".to_vec())
    );
    let older = catalog_reader
        .read_catalog()?
        .ok_or("missing preserved catalog")?;
    assert_eq!(older.metadata(), retained.metadata());
    assert_eq!(older.artifact(), retained.artifact());
    fixture
        .writer()
        .commit(WriteBatch::default().delete(b"opaque-business-progress"))?;
    assert!(reader.snapshot()?.entries().is_empty());
    assert!(fixture.writer().is_initialized()?);
    let older = catalog_reader
        .read_catalog()?
        .ok_or("catalog vanished when business became empty")?;
    assert_eq!(older.metadata(), retained.metadata());
    assert_eq!(older.artifact(), retained.artifact());
    Ok(())
}

fn replacement_publishes_both_exact_components_and_retained_values_do_not_change<F: Fixture>()
-> TestResult {
    let mut fixture = F::new()?;
    let reader = fixture.writer().reader();
    let catalog_reader = fixture.writer().catalog_reader();
    fixture.writer().commit_with_catalog(
        WriteBatch::default().put(b"business", b"first"),
        SnapshotCatalogRecord::new(b"first metadata", b"first artifact")?,
    )?;
    let first = catalog_reader
        .read_catalog()?
        .ok_or("missing first catalog")?;
    fixture.writer().commit_with_catalog(
        WriteBatch::default().put(b"business", b"second"),
        SnapshotCatalogRecord::new(b"second metadata", b"second artifact")?,
    )?;
    let second = catalog_reader
        .clone()
        .read_catalog()?
        .ok_or("missing replacement")?;
    assert_eq!(first.metadata(), b"first metadata");
    assert_eq!(first.artifact(), b"first artifact");
    assert_eq!(second.metadata(), b"second metadata");
    assert_eq!(second.artifact(), b"second artifact");
    assert_eq!(reader.get(b"business")?, Some(b"second".to_vec()));
    Ok(())
}

fn catalog_readers_only_follow_their_originating_unique_writer<F: Fixture>() -> TestResult {
    let mut first = F::new()?;
    let mut second = F::new()?;
    let first_reader = first.writer().catalog_reader();
    let clone = first_reader.clone();
    let second_reader = second.writer().catalog_reader();
    first.writer().commit_with_catalog(
        WriteBatch::default(),
        SnapshotCatalogRecord::new(b"first", b"first")?,
    )?;
    assert!(second_reader.read_catalog()?.is_none());
    assert!(!second.writer().is_initialized()?);
    second.writer().commit_with_catalog(
        WriteBatch::default(),
        SnapshotCatalogRecord::new(b"second", b"second")?,
    )?;
    assert_eq!(
        first_reader
            .read_catalog()?
            .ok_or("missing first")?
            .metadata(),
        b"first"
    );
    assert_eq!(
        clone.read_catalog()?.ok_or("missing clone")?.artifact(),
        b"first"
    );
    assert_eq!(
        second_reader
            .read_catalog()?
            .ok_or("missing second")?
            .metadata(),
        b"second"
    );
    Ok(())
}

fn debug_redacts_records_and_both_catalog_components<F: Fixture>() -> TestResult
where
    <F::Writer as storage::CommittedStore>::Reader: Debug,
    <F::Writer as CatalogCommittedStore>::CatalogReader: Debug,
{
    let mut fixture = F::new()?;
    let key = b"private-business-key";
    let value = b"private-business-value";
    let metadata = b"private-catalog-metadata";
    let artifact = b"private-catalog-artifact";
    let input = SnapshotCatalogRecord::new(metadata, artifact)?;
    let input_debug = format!("{input:?}");
    fixture
        .writer()
        .commit_with_catalog(WriteBatch::default().put(key, value), input)?;
    let slot = fixture
        .writer()
        .catalog_reader()
        .read_catalog()?
        .ok_or("missing private catalog")?;
    for rendered in [
        format!("{:?}", fixture.writer()),
        format!("{:?}", fixture.writer().reader()),
        format!("{:?}", fixture.writer().catalog_reader()),
        format!("{slot:?}"),
        input_debug,
    ] {
        for private in [
            key.as_slice(),
            value.as_slice(),
            metadata.as_slice(),
            artifact.as_slice(),
        ] {
            assert!(!rendered.contains(std::str::from_utf8(private)?));
            assert!(!rendered.contains(&format!("{private:?}")));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Fault {
    Before,
    After,
}

fn controlled_catalog_commit<W: CatalogCommittedStore>(
    writer: &mut W,
    fault: Fault,
    batch: WriteBatch,
    catalog: SnapshotCatalogRecord<'_>,
) -> Result<(), StorageError> {
    if matches!(fault, Fault::After) {
        writer.commit_with_catalog(batch, catalog)?;
    }
    Err(StorageError::Backend {
        operation: "commit a controlled catalog batch",
        detail: "controlled failure before or after the complete commit".into(),
    })
}

fn commit_errors_can_leave_no_batch_or_the_complete_batch_without_retry<F: Fixture>() -> TestResult
{
    for initialized in [false, true] {
        for fault in [Fault::Before, Fault::After] {
            let mut fixture = F::new()?;
            if initialized {
                fixture.writer().commit_with_catalog(
                    WriteBatch::default().put(b"business", b"old"),
                    SnapshotCatalogRecord::new(b"old metadata", b"old artifact")?,
                )?;
            }
            let reader = fixture.writer().reader();
            let catalog_reader = fixture.writer().catalog_reader();
            assert!(matches!(
                controlled_catalog_commit(
                    fixture.writer(),
                    fault,
                    WriteBatch::default()
                        .put(b"business", b"new")
                        .put(b"second", b"complete"),
                    SnapshotCatalogRecord::new(b"new metadata", b"new artifact")?,
                ),
                Err(StorageError::Backend { .. })
            ));
            if matches!(fault, Fault::After) {
                assert!(fixture.writer().is_initialized()?);
                assert_eq!(reader.get(b"business")?, Some(b"new".to_vec()));
                assert_eq!(reader.get(b"second")?, Some(b"complete".to_vec()));
                let slot = catalog_reader
                    .read_catalog()?
                    .ok_or("missing entire committed catalog")?;
                assert_eq!(slot.metadata(), b"new metadata");
                assert_eq!(slot.artifact(), b"new artifact");
            } else {
                assert_eq!(fixture.writer().is_initialized()?, initialized);
                assert_eq!(
                    reader.get(b"business")?,
                    initialized.then(|| b"old".to_vec())
                );
                assert_eq!(reader.get(b"second")?, None);
                let slot = catalog_reader.read_catalog()?;
                if initialized {
                    let slot = slot.ok_or("old catalog disappeared")?;
                    assert_eq!(slot.metadata(), b"old metadata");
                    assert_eq!(slot.artifact(), b"old artifact");
                } else {
                    assert!(slot.is_none());
                }
            }
        }
    }
    Ok(())
}

macro_rules! backend_cases {
    ($fixture:ty) => {
        #[test]
        fn empty_slot_and_readonly_contracts() -> TestResult { absent_and_present_empty_slots_are_distinct_and_reader_writes_are_refused::<$fixture>() }
        #[test]
        fn business_views_are_complete_and_exclude_catalog() -> TestResult { business_views_exclude_the_catalog_and_keep_ordered_batch_semantics::<$fixture>() }
        #[test]
        fn ordinary_commits_preserve_older_catalog() -> TestResult { ordinary_commits_preserve_an_older_catalog_without_interpreting_business_progress::<$fixture>() }
        #[test]
        fn replacement_has_two_exact_immutable_components() -> TestResult { replacement_publishes_both_exact_components_and_retained_values_do_not_change::<$fixture>() }
        #[test]
        fn reader_origin_matches_unique_writer() -> TestResult { catalog_readers_only_follow_their_originating_unique_writer::<$fixture>() }
        #[test]
        fn debug_redacts_catalog_and_records() -> TestResult { debug_redacts_records_and_both_catalog_components::<$fixture>() }
        #[test]
        fn commit_error_does_not_prove_rollback() -> TestResult { commit_errors_can_leave_no_batch_or_the_complete_batch_without_retry::<$fixture>() }
    };
}

mod memory {
    use super::*;
    backend_cases!(MemoryFixture);
}

mod durable {
    use super::*;
    backend_cases!(FjallFixture);

    #[test]
    fn owned_catalog_bytes_survive_actual_all_handle_release_and_same_directory_reopens()
    -> TestResult {
        let directory = TempDir::new()?;
        let mut writer = FjallCatalogReplicaStore::open(directory.path())?;
        let business_reader = writer.reader();
        let catalog_reader = writer.catalog_reader();
        let clone = catalog_reader.clone();
        writer.commit_with_catalog(
            WriteBatch::default().put(b"business", b"captured"),
            SnapshotCatalogRecord::new(b"retained metadata", b"retained artifact")?,
        )?;
        let owned = catalog_reader
            .read_catalog()?
            .ok_or("missing retained catalog")?;
        let captured = business_reader.snapshot()?;
        writer.commit(WriteBatch::default().put(b"business", b"newer"))?;
        drop(writer);
        assert!(matches!(
            FjallCatalogReplicaStore::open(directory.path()),
            Err(StorageError::Backend { .. })
        ));
        drop(catalog_reader);
        drop(clone);
        assert!(matches!(
            FjallCatalogReplicaStore::open(directory.path()),
            Err(StorageError::Backend { .. })
        ));
        assert_eq!(business_reader.get(b"business")?, Some(b"newer".to_vec()));
        drop(business_reader);

        let mut reopened = FjallCatalogReplicaStore::open(directory.path())?;
        assert!(reopened.is_initialized()?);
        assert_eq!(reopened.reader().get(b"business")?, Some(b"newer".to_vec()));
        assert_eq!(
            captured.entries(),
            &[(b"business".to_vec(), b"captured".to_vec())]
        );
        let slot = reopened
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing reopened older catalog")?;
        assert_eq!(slot.metadata(), owned.metadata());
        assert_eq!(slot.artifact(), owned.artifact());
        reopened.commit(WriteBatch::default().delete(b"business"))?;
        drop(reopened);
        let reopened = FjallCatalogReplicaStore::open(directory.path())?;
        assert!(reopened.is_initialized()?);
        assert!(reopened.reader().snapshot()?.entries().is_empty());
        let slot = reopened
            .catalog_reader()
            .read_catalog()?
            .ok_or("catalog vanished after deletion and reopen")?;
        assert_eq!(slot.metadata(), owned.metadata());
        assert_eq!(slot.artifact(), owned.artifact());
        Ok(())
    }

    #[test]
    fn pristine_catalog_profile_and_empty_initialized_catalog_profile_are_refused_by_defaults()
    -> TestResult {
        for initialized in [false, true] {
            let directory = TempDir::new()?;
            let mut writer = FjallCatalogReplicaStore::open(directory.path())?;
            if initialized {
                writer.commit(WriteBatch::default())?;
            }
            drop(writer);
            assert_eq!(
                FjallStore::open(directory.path()).err(),
                Some(StorageError::ReplicaMetadataInStandalone)
            );
            assert_eq!(
                FjallReplicaStore::open(directory.path()).err(),
                Some(StorageError::UnsupportedStoreFormat {
                    found: ACTIVE_CATALOG_REPLICA_STORE_FORMAT,
                    expected: ACTIVE_REPLICA_STORE_FORMAT,
                })
            );
            let writer = FjallCatalogReplicaStore::open(directory.path())?;
            assert_eq!(writer.is_initialized()?, initialized);
            assert!(writer.catalog_reader().read_catalog()?.is_none());
            assert!(writer.reader().snapshot()?.entries().is_empty());
        }
        Ok(())
    }

    #[test]
    fn catalog_open_never_adopts_an_existing_empty_default_profile() -> TestResult {
        for replica in [false, true] {
            let directory = TempDir::new()?;
            let found = if replica {
                let mut writer = FjallReplicaStore::open(directory.path())?;
                writer.commit(WriteBatch::default())?;
                drop(writer);
                ACTIVE_REPLICA_STORE_FORMAT
            } else {
                drop(FjallStore::open(directory.path())?);
                ACTIVE_STORE_FORMAT
            };
            assert_eq!(
                FjallCatalogReplicaStore::open(directory.path()).err(),
                Some(StorageError::UnsupportedStoreFormat {
                    found,
                    expected: ACTIVE_CATALOG_REPLICA_STORE_FORMAT
                })
            );
            if replica {
                let writer = FjallReplicaStore::open(directory.path())?;
                assert!(writer.is_initialized()?);
                assert!(writer.reader().snapshot()?.entries().is_empty());
            } else {
                assert!(
                    FjallStore::open(directory.path())?
                        .snapshot()?
                        .entries()
                        .is_empty()
                );
            }
        }
        Ok(())
    }
}
