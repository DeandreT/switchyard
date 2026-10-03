use std::error::Error;

use storage::{
    ACTIVE_REPLICA_STORE_FORMAT, ACTIVE_STORE_FORMAT, CommittedStore, FjallReplicaStore,
    FjallStore, MemoryReplicaStore, StateStore, StorageError, WriteBatch,
};
use tempfile::TempDir;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

trait Fixture: Sized {
    type Writer: CommittedStore + std::fmt::Debug;

    fn new() -> TestResult<Self>;
    fn writer(&mut self) -> &mut Self::Writer;
}

struct MemoryFixture(MemoryReplicaStore);

impl Fixture for MemoryFixture {
    type Writer = MemoryReplicaStore;

    fn new() -> TestResult<Self> {
        Ok(Self(MemoryReplicaStore::new()))
    }

    fn writer(&mut self) -> &mut Self::Writer {
        &mut self.0
    }
}

struct FjallFixture {
    writer: FjallReplicaStore,
    _directory: TempDir,
}

impl Fixture for FjallFixture {
    type Writer = FjallReplicaStore;

    fn new() -> TestResult<Self> {
        let directory = TempDir::new()?;
        Ok(Self {
            writer: FjallReplicaStore::open(directory.path())?,
            _directory: directory,
        })
    }

    fn writer(&mut self) -> &mut Self::Writer {
        &mut self.writer
    }
}

fn every_reader_mutation_is_refused_without_initializing<F: Fixture>() -> TestResult {
    let mut fixture = F::new()?;
    let reader = fixture.writer().reader();
    let before = reader.snapshot()?;
    assert!(!fixture.writer().is_initialized()?);
    for batch in [
        WriteBatch::default(),
        WriteBatch::default().put(b"one", b"value"),
        WriteBatch::default().delete(b"one"),
        WriteBatch::default().put(b"one", b"value").delete(b"two"),
    ] {
        assert_eq!(reader.apply(batch), Err(StorageError::ReplicaWriteRequired));
        assert_eq!(reader.snapshot()?, before);
        assert!(!fixture.writer().is_initialized()?);
    }

    fixture
        .writer()
        .commit(WriteBatch::default().put(b"one", b"committed"))?;
    let committed = reader.snapshot()?;
    for view in [reader.clone(), fixture.writer().reader()] {
        assert_eq!(
            view.apply(WriteBatch::default().delete(b"one")),
            Err(StorageError::ReplicaWriteRequired)
        );
        assert_eq!(view.snapshot()?, committed);
    }
    assert!(fixture.writer().is_initialized()?);
    Ok(())
}

fn privileged_commits_preserve_atomic_batch_semantics<F: Fixture>() -> TestResult {
    let mut fixture = F::new()?;
    let reader = fixture.writer().reader();
    let clone = reader.clone();
    fixture.writer().commit(
        WriteBatch::default()
            .put(b"scope/a", b"first")
            .put(b"scope/b", b"second")
            .delete(b"scope/a")
            .put(b"scope/a", b"final")
            .put(b"other", b"outside"),
    )?;
    assert!(fixture.writer().is_initialized()?);
    assert_eq!(reader.get(b"scope/a")?, Some(b"final".to_vec()));
    assert_eq!(clone.snapshot()?, reader.snapshot()?);
    assert_eq!(
        reader.scan_prefix(b"scope/", 3)?,
        vec![
            (b"scope/a".to_vec(), b"final".to_vec()),
            (b"scope/b".to_vec(), b"second".to_vec()),
        ]
    );
    assert_eq!(
        clone.scan_from(b"scope/", b"scope/b", 1)?,
        vec![(b"scope/b".to_vec(), b"second".to_vec())]
    );
    fixture.writer().commit(
        WriteBatch::default()
            .delete(b"scope/a")
            .delete(b"scope/b")
            .delete(b"other"),
    )?;
    assert!(reader.snapshot()?.entries().is_empty());
    assert!(fixture.writer().is_initialized()?);
    Ok(())
}

fn empty_privileged_commit_is_an_initialization<F: Fixture>() -> TestResult {
    let mut fixture = F::new()?;
    let reader = fixture.writer().reader();
    assert!(!fixture.writer().is_initialized()?);
    assert!(reader.snapshot()?.entries().is_empty());
    fixture.writer().commit(WriteBatch::default())?;
    assert!(fixture.writer().is_initialized()?);
    assert!(reader.snapshot()?.entries().is_empty());
    assert_eq!(reader.get(b"replica_initialized")?, None);
    assert_eq!(reader.get(b"format_version")?, None);
    fixture.writer().commit(WriteBatch::default())?;
    assert!(fixture.writer().is_initialized()?);
    Ok(())
}

fn each_reader_matches_only_its_original_writer<F: Fixture>() -> TestResult {
    let mut first = F::new()?;
    let mut second = F::new()?;
    let first_reader = first.writer().reader();
    let second_reader = second.writer().reader();
    first
        .writer()
        .commit(WriteBatch::default().put(b"same", b"first"))?;
    assert_eq!(first_reader.get(b"same")?, Some(b"first".to_vec()));
    assert_eq!(second_reader.get(b"same")?, None);
    assert!(!second.writer().is_initialized()?);
    second
        .writer()
        .commit(WriteBatch::default().put(b"same", b"second"))?;
    assert_eq!(first_reader.get(b"same")?, Some(b"first".to_vec()));
    assert_eq!(second_reader.get(b"same")?, Some(b"second".to_vec()));
    Ok(())
}

fn replica_debug_never_exposes_record_keys_or_values<F: Fixture>() -> TestResult
where
    <F::Writer as CommittedStore>::Reader: std::fmt::Debug,
{
    let mut fixture = F::new()?;
    let key = "private-replica-record-key";
    let value = "private-replica-record-content";
    fixture
        .writer()
        .commit(WriteBatch::default().put(key.as_bytes(), value.as_bytes()))?;
    let reader = fixture.writer().reader();
    let reader_debug = format!("{reader:?}");
    let writer_debug = format!("{:?}", fixture.writer());
    assert_eq!(reader_debug, "ReplicaReader { .. }");
    for rendered in [&reader_debug, &writer_debug] {
        assert!(!rendered.contains(key));
        assert!(!rendered.contains(value));
        assert!(!rendered.contains(&format!("{:?}", key.as_bytes())));
        assert!(!rendered.contains(&format!("{:?}", value.as_bytes())));
    }
    assert_eq!(reader.get(key.as_bytes())?, Some(value.as_bytes().to_vec()));
    Ok(())
}

macro_rules! backend_cases {
    ($fixture:ty) => {
        #[test]
        fn reader_mutations_are_refused() -> TestResult {
            every_reader_mutation_is_refused_without_initializing::<$fixture>()
        }

        #[test]
        fn privileged_batches_are_atomic() -> TestResult {
            privileged_commits_preserve_atomic_batch_semantics::<$fixture>()
        }

        #[test]
        fn an_empty_commit_initializes() -> TestResult {
            empty_privileged_commit_is_an_initialization::<$fixture>()
        }

        #[test]
        fn reader_and_writer_origins_match() -> TestResult {
            each_reader_matches_only_its_original_writer::<$fixture>()
        }

        #[test]
        fn replica_debug_is_redacted() -> TestResult {
            replica_debug_never_exposes_record_keys_or_values::<$fixture>()
        }
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
    fn records_and_initialized_flag_survive_reopen_and_deletion() -> TestResult {
        let directory = TempDir::new()?;
        let mut writer = FjallReplicaStore::open(directory.path())?;
        writer.commit(
            WriteBatch::default()
                .put(b"one", b"first")
                .put(b"two", b"second"),
        )?;
        let reader = writer.reader();
        let committed = reader.snapshot()?;
        drop(reader);
        drop(writer);

        let mut writer = FjallReplicaStore::open(directory.path())?;
        assert!(writer.is_initialized()?);
        assert_eq!(writer.reader().snapshot()?, committed);
        writer.commit(WriteBatch::default().delete(b"one").delete(b"two"))?;
        drop(writer);

        let writer = FjallReplicaStore::open(directory.path())?;
        assert!(writer.is_initialized()?);
        assert!(writer.reader().snapshot()?.entries().is_empty());
        Ok(())
    }

    #[test]
    fn unopened_replica_records_remain_pristine_after_reopen() -> TestResult {
        let directory = TempDir::new()?;
        let writer = FjallReplicaStore::open(directory.path())?;
        assert!(!writer.is_initialized()?);
        drop(writer);
        let writer = FjallReplicaStore::open(directory.path())?;
        assert!(!writer.is_initialized()?);
        assert!(writer.reader().snapshot()?.entries().is_empty());
        Ok(())
    }

    #[test]
    fn read_only_clones_keep_the_directory_owned() -> TestResult {
        let directory = TempDir::new()?;
        let mut writer = FjallReplicaStore::open(directory.path())?;
        writer.commit(WriteBatch::default().put(b"one", b"value"))?;
        let reader = writer.reader();
        let clone = reader.clone();
        drop(writer);
        assert!(matches!(
            FjallReplicaStore::open(directory.path()),
            Err(StorageError::Backend { .. })
        ));
        assert_eq!(clone.get(b"one")?, Some(b"value".to_vec()));
        assert_eq!(
            reader.apply(WriteBatch::default()),
            Err(StorageError::ReplicaWriteRequired)
        );
        drop(reader);
        drop(clone);
        let writer = FjallReplicaStore::open(directory.path())?;
        assert!(writer.is_initialized()?);
        Ok(())
    }

    #[test]
    fn neither_mode_adopts_the_other_directory() -> TestResult {
        let standalone = TempDir::new()?;
        drop(FjallStore::open(standalone.path())?);
        assert_eq!(
            FjallReplicaStore::open(standalone.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: ACTIVE_STORE_FORMAT,
                expected: ACTIVE_REPLICA_STORE_FORMAT,
            })
        );
        assert!(
            FjallStore::open(standalone.path())?
                .snapshot()?
                .entries()
                .is_empty()
        );

        let replica = TempDir::new()?;
        drop(FjallReplicaStore::open(replica.path())?);
        assert_eq!(
            FjallStore::open(replica.path()).err(),
            Some(StorageError::ReplicaMetadataInStandalone)
        );
        assert!(!FjallReplicaStore::open(replica.path())?.is_initialized()?);
        Ok(())
    }
}
