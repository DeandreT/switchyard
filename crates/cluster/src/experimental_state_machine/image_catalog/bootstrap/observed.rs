use std::sync::{Arc, Mutex};

use storage::{
    BoundedStateStore, CatalogCommittedStore, CatalogReadError, CommittedStore, Key, ReadLimits,
    SnapshotCatalogReader, SnapshotCatalogRecord, StateStore, StorageError, StoreSnapshot,
    StoredSnapshotCatalog, Value, WriteBatch,
};

use super::captured;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Counts {
    pub factories: usize,
    pub initialized: usize,
    pub gets: usize,
    pub scans: usize,
    pub snapshots: usize,
    pub applies: usize,
    pub bounded: usize,
    pub commits: usize,
    pub catalog_factories: usize,
    pub catalog_reads: usize,
    pub catalog_commits: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) enum Fault {
    #[default]
    None,
    Initialized,
    Probe,
    CommitBefore,
    CommitAfter,
}

#[derive(Clone)]
pub(super) struct CatalogCall {
    pub batch: WriteBatch,
    pub metadata: Vec<u8>,
    pub metadata_ptr: usize,
    pub artifact_ptr: usize,
    pub artifact_digest: [u8; 32],
}

#[derive(Default)]
struct Observation {
    counts: Counts,
    calls: Vec<CatalogCall>,
    fault: Fault,
}

pub(super) struct Writer<W: CatalogCommittedStore> {
    writer: Arc<Mutex<W>>,
    observation: Arc<Mutex<Observation>>,
}
pub(super) struct Control<W: CatalogCommittedStore> {
    writer: Arc<Mutex<W>>,
    observation: Arc<Mutex<Observation>>,
}
#[derive(Clone)]
pub(super) struct Reader<R> {
    inner: R,
    observation: Arc<Mutex<Observation>>,
}
#[derive(Clone)]
pub(super) struct CatalogReader<R> {
    inner: R,
    observation: Arc<Mutex<Observation>>,
}

pub(super) fn observed<W: CatalogCommittedStore>(writer: W) -> (Writer<W>, Control<W>) {
    let writer = Arc::new(Mutex::new(writer));
    let observation = Arc::new(Mutex::new(Observation::default()));
    (
        Writer {
            writer: writer.clone(),
            observation: observation.clone(),
        },
        Control {
            writer,
            observation,
        },
    )
}

impl<W: CatalogCommittedStore> Control<W> {
    pub(super) fn counts(&self) -> Counts {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .clone()
    }
    pub(super) fn calls(&self) -> Vec<CatalogCall> {
        self.observation
            .lock()
            .expect("test observation")
            .calls
            .clone()
    }
    pub(super) fn reset(&self) {
        let mut state = self.observation.lock().expect("test observation");
        state.counts = Counts::default();
        state.calls.clear();
        state.fault = Fault::None;
    }
    pub(super) fn fault(&self, fault: Fault) {
        self.observation.lock().expect("test observation").fault = fault;
    }
    // These exact matching low-level capabilities bypass observation for evidence.
    pub(super) fn reader(&self) -> W::Reader {
        self.writer.lock().expect("test writer").reader()
    }
    pub(super) fn catalog_reader(&self) -> W::CatalogReader {
        self.writer.lock().expect("test writer").catalog_reader()
    }
    pub(super) fn initialized(&self) -> Result<bool, StorageError> {
        self.writer.lock().expect("test writer").is_initialized()
    }
    pub(super) fn inject(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.writer.lock().expect("test writer").commit(batch)
    }
    pub(super) fn writer(&self) -> Writer<W> {
        Writer {
            writer: self.writer.clone(),
            observation: self.observation.clone(),
        }
    }
}

impl<W: CatalogCommittedStore> CommittedStore for Writer<W> {
    type Reader = Reader<W::Reader>;
    fn reader(&self) -> Self::Reader {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .factories += 1;
        Reader {
            inner: self.writer.lock().expect("test writer").reader(),
            observation: self.observation.clone(),
        }
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        let fault = {
            let mut state = self.observation.lock().expect("test observation");
            state.counts.initialized += 1;
            if matches!(state.fault, Fault::Initialized) {
                std::mem::take(&mut state.fault)
            } else {
                Fault::None
            }
        };
        if matches!(fault, Fault::Initialized) {
            return Err(physical());
        }
        self.writer.lock().expect("test writer").is_initialized()
    }
    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .commits += 1;
        self.writer.lock().expect("test writer").commit(batch)
    }
}

impl<W: CatalogCommittedStore> CatalogCommittedStore for Writer<W> {
    type CatalogReader = CatalogReader<W::CatalogReader>;
    fn catalog_reader(&self) -> Self::CatalogReader {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .catalog_factories += 1;
        CatalogReader {
            inner: self.writer.lock().expect("test writer").catalog_reader(),
            observation: self.observation.clone(),
        }
    }
    fn commit_with_catalog(
        &mut self,
        batch: WriteBatch,
        record: SnapshotCatalogRecord<'_>,
    ) -> Result<(), StorageError> {
        let fault = {
            let mut state = self.observation.lock().expect("test observation");
            state.counts.catalog_commits += 1;
            state.calls.push(CatalogCall {
                batch: batch.clone(),
                metadata: record.metadata().to_vec(),
                metadata_ptr: record.metadata().as_ptr() as usize,
                artifact_ptr: record.artifact().as_ptr() as usize,
                artifact_digest: captured::digest(record.artifact()),
            });
            if matches!(state.fault, Fault::CommitBefore | Fault::CommitAfter) {
                std::mem::take(&mut state.fault)
            } else {
                Fault::None
            }
        };
        if matches!(fault, Fault::CommitBefore) {
            return Err(physical());
        }
        self.writer
            .lock()
            .expect("test writer")
            .commit_with_catalog(batch, record)?;
        if matches!(fault, Fault::CommitAfter) {
            return Err(physical());
        }
        Ok(())
    }
}

impl<R: StateStore> StateStore for Reader<R> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .gets += 1;
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let fault = {
            let mut state = self.observation.lock().expect("test observation");
            state.counts.scans += 1;
            if matches!(state.fault, Fault::Probe) {
                std::mem::take(&mut state.fault)
            } else {
                Fault::None
            }
        };
        if matches!(fault, Fault::Probe) {
            return Err(physical());
        }
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .snapshots += 1;
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .applies += 1;
        self.inner.apply(batch)
    }
}

impl<R: BoundedStateStore> BoundedStateStore for Reader<R> {
    fn snapshot_bounded(&self, limits: ReadLimits) -> Result<StoreSnapshot, StorageError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .bounded += 1;
        self.inner.snapshot_bounded(limits)
    }
}
impl<R: SnapshotCatalogReader> SnapshotCatalogReader for CatalogReader<R> {
    fn read_catalog(&self) -> Result<Option<StoredSnapshotCatalog>, CatalogReadError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .catalog_reads += 1;
        self.inner.read_catalog()
    }
}

fn physical() -> StorageError {
    StorageError::Backend {
        operation: "catalog bootstrap test",
        detail: "PRIVATE-backend-path".into(),
    }
}

// The associated reader intentionally does NOT implement BoundedStateStore.
pub(super) struct WithoutBoundedReader<W>(pub W);
#[derive(Clone)]
pub(super) struct OrdinaryReader<R>(R);
impl<W: CatalogCommittedStore> CommittedStore for WithoutBoundedReader<W> {
    type Reader = OrdinaryReader<W::Reader>;
    fn reader(&self) -> Self::Reader {
        OrdinaryReader(self.0.reader())
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.0.is_initialized()
    }
    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.0.commit(batch)
    }
}
impl<W: CatalogCommittedStore> CatalogCommittedStore for WithoutBoundedReader<W> {
    type CatalogReader = W::CatalogReader;
    fn catalog_reader(&self) -> Self::CatalogReader {
        self.0.catalog_reader()
    }
    fn commit_with_catalog(
        &mut self,
        batch: WriteBatch,
        record: SnapshotCatalogRecord<'_>,
    ) -> Result<(), StorageError> {
        self.0.commit_with_catalog(batch, record)
    }
}
impl<R: StateStore> StateStore for OrdinaryReader<R> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.0.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.0.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.0.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.0.apply(batch)
    }
}

pub(super) fn bootstrap_counts() -> Counts {
    Counts {
        factories: 1,
        initialized: 1,
        scans: 1,
        catalog_commits: 1,
        ..Counts::default()
    }
}
