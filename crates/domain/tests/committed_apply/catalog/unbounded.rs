use storage::{Key, SnapshotCatalogRecord, Value};

use super::*;

struct UnboundedWriter(MemoryCatalogReplicaStore);

#[derive(Clone)]
struct UnboundedReader(<MemoryCatalogReplicaStore as CommittedStore>::Reader);

impl StateStore for UnboundedReader {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.0.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.0.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.0.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.0.scan_from(prefix, start, limit)
    }
}

impl CommittedStore for UnboundedWriter {
    type Reader = UnboundedReader;

    fn reader(&self) -> Self::Reader {
        UnboundedReader(self.0.reader())
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.0.is_initialized()
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.0.commit(batch)
    }
}

impl CatalogCommittedStore for UnboundedWriter {
    type CatalogReader = <MemoryCatalogReplicaStore as CatalogCommittedStore>::CatalogReader;

    fn catalog_reader(&self) -> Self::CatalogReader {
        self.0.catalog_reader()
    }

    fn commit_with_catalog(
        &mut self,
        batch: WriteBatch,
        catalog: SnapshotCatalogRecord<'_>,
    ) -> Result<(), StorageError> {
        self.0.commit_with_catalog(batch, catalog)
    }
}

fn read_without_business_bound<W: CatalogCommittedStore>(
    machine: &mut CommittedStateMachine<W>,
) -> Result<Option<domain::RetainedCreateSendCatalog>, CommittedCatalogError> {
    machine.read_create_send_catalog()
}

#[test]
fn catalog_read_requires_no_bounded_business_reader_or_current_checkpoint_read() -> TestResult {
    let mut source = CommittedStateMachine::create(MemoryCatalogReplicaStore::new(), stream()?)?;
    let image = source.export_create_send_image()?;
    let captured = source.checkpoint()?;
    let (writer, control) = observed(UnboundedWriter(MemoryCatalogReplicaStore::new()));
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    control.inject_catalog(b"opaque unbounded-reader metadata", image.as_bytes())?;
    control.reset();
    let retained =
        read_without_business_bound(&mut machine)?.ok_or("missing unbounded-reader catalog")?;
    assert_catalog_read(&control);
    assert_eq!(retained.checkpoint(), &captured);
    assert_eq!(retained.image_bytes(), image.as_bytes());
    assert_eq!(retained.metadata(), b"opaque unbounded-reader metadata");
    assert_eq!(
        control.catalog_pointers(),
        vec![retained.image_bytes().as_ptr() as usize]
    );
    Ok(())
}
