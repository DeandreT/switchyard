use std::sync::{Arc, Mutex};

use storage::{
    BoundedStateStore, CatalogCommittedStore, CatalogReadError, CommittedStore, Key, ReadLimits,
    SnapshotCatalogReader, SnapshotCatalogRecord, StateStore, StorageError, StoreSnapshot,
    StoredSnapshotCatalog, Value, WriteBatch,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Counts {
    pub reader_factories: usize,
    pub gets: usize,
    pub scans: usize,
    pub ordinary: usize,
    pub bounded: usize,
    pub initialized: usize,
    pub commits: usize,
    pub applies: usize,
    pub catalog_factories: usize,
    pub catalog_reads: usize,
    pub catalog_commits: usize,
}

#[derive(Clone, Copy, Default)]
pub(super) enum Fault {
    #[default]
    None,
    CaptureLimit,
    CapturePhysical,
    CatalogLimit,
    CatalogNestedLimit,
    CatalogAllocation,
    CatalogPhysical,
    CatalogCorrupt,
    CommitBefore,
    CommitAfter,
    CommitLimit,
    CommitCorrupt,
}

#[derive(Clone)]
pub(super) struct Attempt {
    pub business: WriteBatch,
    pub metadata: Vec<u8>,
    pub artifact: Vec<u8>,
    pub artifact_pointer: usize,
}

#[derive(Default)]
struct Observation {
    counts: Counts,
    limits: Vec<ReadLimits>,
    attempts: Vec<Attempt>,
    catalog_pointers: Vec<usize>,
    fault: Fault,
}

pub(super) struct Writer<W> {
    inner: Arc<Mutex<W>>,
    observation: Arc<Mutex<Observation>>,
}

pub(super) struct Control<W> {
    inner: Arc<Mutex<W>>,
    observation: Arc<Mutex<Observation>>,
}

pub(super) fn observed<W: CatalogCommittedStore>(writer: W) -> (Writer<W>, Control<W>) {
    let inner = Arc::new(Mutex::new(writer));
    let observation = Arc::new(Mutex::new(Observation::default()));
    (
        Writer {
            inner: Arc::clone(&inner),
            observation: Arc::clone(&observation),
        },
        Control { inner, observation },
    )
}

impl<W: CatalogCommittedStore> Control<W> {
    pub(super) fn counts(&self) -> Counts {
        self.observation.lock().expect("test observation").counts
    }

    pub(super) fn limits(&self) -> Vec<ReadLimits> {
        self.observation
            .lock()
            .expect("test observation")
            .limits
            .clone()
    }

    pub(super) fn attempts(&self) -> Vec<Attempt> {
        self.observation
            .lock()
            .expect("test observation")
            .attempts
            .clone()
    }

    pub(super) fn catalog_pointers(&self) -> Vec<usize> {
        self.observation
            .lock()
            .expect("test observation")
            .catalog_pointers
            .clone()
    }

    pub(super) fn reset(&self) {
        let mut observation = self.observation.lock().expect("test observation");
        observation.counts = Counts::default();
        observation.limits.clear();
        observation.attempts.clear();
        observation.catalog_pointers.clear();
    }

    pub(super) fn fault(&self, fault: Fault) {
        self.observation.lock().expect("test observation").fault = fault;
    }

    // Trusted test witnesses bypass observed calls. They grant no public escape.
    pub(super) fn reader(&self) -> W::Reader {
        self.inner.lock().expect("test writer").reader()
    }

    pub(super) fn catalog(&self) -> Result<Option<StoredSnapshotCatalog>, CatalogReadError> {
        self.inner
            .lock()
            .expect("test writer")
            .catalog_reader()
            .read_catalog()
    }

    pub(super) fn inject_business(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.inner.lock().expect("test writer").commit(batch)
    }

    pub(super) fn inject_catalog(
        &self,
        metadata: &[u8],
        artifact: &[u8],
    ) -> Result<(), StorageError> {
        self.inner.lock().expect("test writer").commit_with_catalog(
            WriteBatch::default(),
            SnapshotCatalogRecord::new(metadata, artifact).expect("bounded test input"),
        )
    }

    // Used only after dropping the machine; this is not a real durable reopen.
    pub(super) fn recover_writer(&self) -> Writer<W> {
        Writer {
            inner: Arc::clone(&self.inner),
            observation: Arc::clone(&self.observation),
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
            .reader_factories += 1;
        Reader {
            inner: self.inner.lock().expect("test writer").reader(),
            observation: Arc::clone(&self.observation),
        }
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .initialized += 1;
        self.inner.lock().expect("test writer").is_initialized()
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .commits += 1;
        self.inner.lock().expect("test writer").commit(batch)
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
            inner: self.inner.lock().expect("test writer").catalog_reader(),
            observation: Arc::clone(&self.observation),
        }
    }

    fn commit_with_catalog(
        &mut self,
        business: WriteBatch,
        record: SnapshotCatalogRecord<'_>,
    ) -> Result<(), StorageError> {
        let fault = {
            let mut observation = self.observation.lock().expect("test observation");
            observation.counts.catalog_commits += 1;
            observation.attempts.push(Attempt {
                business: business.clone(),
                metadata: record.metadata().to_vec(),
                artifact: record.artifact().to_vec(),
                artifact_pointer: record.artifact().as_ptr() as usize,
            });
            match observation.fault {
                Fault::CommitBefore
                | Fault::CommitAfter
                | Fault::CommitLimit
                | Fault::CommitCorrupt => std::mem::take(&mut observation.fault),
                _ => Fault::None,
            }
        };
        match fault {
            Fault::CommitBefore => return Err(physical_error()),
            Fault::CommitLimit => return Err(StorageError::ReadLimitExceeded),
            Fault::CommitCorrupt => return Err(corrupt_error()),
            _ => {}
        }
        self.inner
            .lock()
            .expect("test writer")
            .commit_with_catalog(business, record)?;
        if matches!(fault, Fault::CommitAfter) {
            return Err(physical_error());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct Reader<R> {
    inner: R,
    observation: Arc<Mutex<Observation>>,
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

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .applies += 1;
        self.inner.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .ordinary += 1;
        Err(StorageError::Backend {
            operation: "unexpected allocating fallback",
            detail: "private-forbidden-fallback".into(),
        })
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .scans += 1;
        self.inner.scan_from(prefix, start, limit)
    }
}

impl<R: BoundedStateStore> BoundedStateStore for Reader<R> {
    fn snapshot_bounded(&self, limits: ReadLimits) -> Result<StoreSnapshot, StorageError> {
        let fault = {
            let mut observation = self.observation.lock().expect("test observation");
            observation.counts.bounded += 1;
            observation.limits.push(limits);
            match observation.fault {
                Fault::CaptureLimit | Fault::CapturePhysical => {
                    std::mem::take(&mut observation.fault)
                }
                _ => Fault::None,
            }
        };
        match fault {
            Fault::CaptureLimit => Err(StorageError::ReadLimitExceeded),
            Fault::CapturePhysical => Err(physical_error()),
            _ => self.inner.snapshot_bounded(limits),
        }
    }
}

#[derive(Clone)]
pub(super) struct CatalogReader<R> {
    inner: R,
    observation: Arc<Mutex<Observation>>,
}

impl<R: SnapshotCatalogReader> SnapshotCatalogReader for CatalogReader<R> {
    fn read_catalog(&self) -> Result<Option<StoredSnapshotCatalog>, CatalogReadError> {
        let fault = {
            let mut observation = self.observation.lock().expect("test observation");
            observation.counts.catalog_reads += 1;
            match observation.fault {
                Fault::CatalogLimit
                | Fault::CatalogNestedLimit
                | Fault::CatalogAllocation
                | Fault::CatalogPhysical
                | Fault::CatalogCorrupt => std::mem::take(&mut observation.fault),
                _ => Fault::None,
            }
        };
        match fault {
            Fault::CatalogLimit => return Err(CatalogReadError::LimitExceeded),
            Fault::CatalogNestedLimit => {
                return Err(CatalogReadError::Storage(StorageError::ReadLimitExceeded));
            }
            Fault::CatalogAllocation => return Err(CatalogReadError::Allocation),
            Fault::CatalogPhysical => return Err(CatalogReadError::Storage(physical_error())),
            Fault::CatalogCorrupt => return Err(CatalogReadError::Storage(corrupt_error())),
            _ => {}
        }
        let stored = self.inner.read_catalog()?;
        if let Some(stored) = &stored {
            self.observation
                .lock()
                .expect("test observation")
                .catalog_pointers
                .push(stored.artifact().as_ptr() as usize);
        }
        Ok(stored)
    }
}

fn physical_error() -> StorageError {
    StorageError::Backend {
        operation: "injected catalog I/O",
        detail: "private-backend-message-body-token-address".into(),
    }
}

fn corrupt_error() -> StorageError {
    StorageError::CorruptMetadata {
        detail: "private-corrupt-header-token-address".into(),
    }
}
