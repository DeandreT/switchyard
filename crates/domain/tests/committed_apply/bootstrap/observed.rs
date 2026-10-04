use std::sync::{Arc, Mutex};

use storage::{
    CatalogCommittedStore, CatalogReadError, CommittedStore, Key, SnapshotCatalogReader,
    SnapshotCatalogRecord, StateStore, StorageError, StoreSnapshot, StoredSnapshotCatalog, Value,
    WriteBatch,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Counts {
    pub reader_factories: usize,
    pub initialized: usize,
    pub gets: usize,
    pub scans: Vec<(Vec<u8>, Vec<u8>, usize)>,
    pub snapshots: usize,
    pub reader_applies: usize,
    pub commits: usize,
    pub catalog_reader_factories: usize,
    pub catalog_reads: usize,
    pub catalog_commits: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) enum Fault {
    #[default]
    None,
    Initialized,
    InitializedLimit,
    Scan,
    ScanLimit,
    CommitBefore,
    CommitAfter,
    CommitLimit,
    CommitCorrupt,
    ExitBefore,
    ExitAfter,
}

#[derive(Clone)]
pub(super) struct CatalogAttempt {
    pub metadata: Vec<u8>,
    pub artifact: Vec<u8>,
    pub artifact_pointer: usize,
}

struct Shared<W> {
    writer: W,
    force_uninitialized: bool,
    batches: Vec<WriteBatch>,
    catalog_attempts: Vec<CatalogAttempt>,
}

pub(super) struct Writer<W> {
    shared: Arc<Mutex<Shared<W>>>,
    counts: Arc<Mutex<Counts>>,
    fault: Arc<Mutex<Fault>>,
}

pub(super) struct Control<W> {
    shared: Arc<Mutex<Shared<W>>>,
    counts: Arc<Mutex<Counts>>,
    fault: Arc<Mutex<Fault>>,
}

pub(super) fn observed<W: CommittedStore>(writer: W) -> (Writer<W>, Control<W>) {
    let shared = Arc::new(Mutex::new(Shared {
        writer,
        force_uninitialized: false,
        batches: Vec::new(),
        catalog_attempts: Vec::new(),
    }));
    let counts = Arc::new(Mutex::new(Counts::default()));
    let fault = Arc::new(Mutex::new(Fault::None));
    (
        Writer {
            shared: shared.clone(),
            counts: counts.clone(),
            fault: fault.clone(),
        },
        Control {
            shared,
            counts,
            fault,
        },
    )
}

impl<W: CommittedStore> Control<W> {
    pub(super) fn counts(&self) -> Counts {
        self.counts.lock().expect("test counts lock").clone()
    }

    pub(super) fn reset(&self) {
        *self.counts.lock().expect("test counts lock") = Counts::default();
        let mut shared = self.shared.lock().expect("test writer lock");
        shared.batches.clear();
        shared.catalog_attempts.clear();
    }

    pub(super) fn reader(&self) -> W::Reader {
        self.shared
            .lock()
            .expect("test writer lock")
            .writer
            .reader()
    }

    pub(super) fn initialized(&self) -> Result<bool, StorageError> {
        self.shared
            .lock()
            .expect("test writer lock")
            .writer
            .is_initialized()
    }

    pub(super) fn writer(&self) -> Writer<W> {
        Writer {
            shared: self.shared.clone(),
            counts: self.counts.clone(),
            fault: self.fault.clone(),
        }
    }

    pub(super) fn fault(&self, fault: Fault) {
        *self.fault.lock().expect("test fault lock") = fault;
    }

    pub(super) fn force_uninitialized(&self) {
        self.shared
            .lock()
            .expect("test writer lock")
            .force_uninitialized = true;
    }

    pub(super) fn inject(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.shared
            .lock()
            .expect("test writer lock")
            .writer
            .commit(batch)
    }

    pub(super) fn batches(&self) -> Vec<WriteBatch> {
        self.shared
            .lock()
            .expect("test writer lock")
            .batches
            .clone()
    }
}

impl<W: CommittedStore> CommittedStore for Writer<W> {
    type Reader = Reader<W::Reader>;

    fn reader(&self) -> Self::Reader {
        self.counts
            .lock()
            .expect("test counts lock")
            .reader_factories += 1;
        Reader {
            inner: self
                .shared
                .lock()
                .expect("test writer lock")
                .writer
                .reader(),
            fault: self.fault.clone(),
            counts: self.counts.clone(),
        }
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.counts.lock().expect("test counts lock").initialized += 1;
        let mut fault = self.fault.lock().expect("test fault lock");
        match *fault {
            Fault::Initialized | Fault::InitializedLimit => {
                let error = if matches!(*fault, Fault::InitializedLimit) {
                    StorageError::ReadLimitExceeded
                } else {
                    physical_error()
                };
                *fault = Fault::None;
                return Err(error);
            }
            _ => {}
        }
        drop(fault);
        let shared = self.shared.lock().expect("test writer lock");
        if shared.force_uninitialized {
            return Ok(false);
        }
        shared.writer.is_initialized()
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.counts.lock().expect("test counts lock").commits += 1;
        let mut shared = self.shared.lock().expect("test writer lock");
        shared.batches.push(batch.clone());
        let fault = std::mem::take(&mut *self.fault.lock().expect("test fault lock"));
        match fault {
            Fault::CommitBefore => return Err(physical_error()),
            Fault::ExitBefore => std::process::exit(71),
            _ => {}
        }
        shared.writer.commit(batch)?;
        match fault {
            Fault::CommitAfter => Err(physical_error()),
            Fault::ExitAfter => std::process::exit(72),
            _ => Ok(()),
        }
    }
}

// Test evidence uses the matching raw reader, not these observed operations.
#[derive(Clone)]
pub(super) struct Reader<R> {
    inner: R,
    fault: Arc<Mutex<Fault>>,
    counts: Arc<Mutex<Counts>>,
}

impl<R: StateStore> StateStore for Reader<R> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.counts.lock().expect("test counts lock").gets += 1;
        self.inner.get(key)
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.counts.lock().expect("test counts lock").scans.push((
            prefix.to_vec(),
            start.to_vec(),
            limit,
        ));
        let mut fault = self.fault.lock().expect("test fault lock");
        match *fault {
            Fault::Scan | Fault::ScanLimit => {
                let error = if matches!(*fault, Fault::ScanLimit) {
                    StorageError::ReadLimitExceeded
                } else {
                    physical_error()
                };
                *fault = Fault::None;
                return Err(error);
            }
            _ => {}
        }
        drop(fault);
        self.inner.scan_from(prefix, start, limit)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.counts.lock().expect("test counts lock").snapshots += 1;
        Err(physical_error())
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.counts.lock().expect("test counts lock").reader_applies += 1;
        self.inner.apply(batch)
    }
}

fn physical_error() -> StorageError {
    StorageError::Backend {
        operation: "injected image bootstrap target",
        detail: "SECRET target path/body/backend detail".into(),
    }
}

impl<W: CatalogCommittedStore> Control<W> {
    pub(super) fn catalog(&self) -> Result<Option<StoredSnapshotCatalog>, CatalogReadError> {
        self.shared
            .lock()
            .expect("test writer lock")
            .writer
            .catalog_reader()
            .read_catalog()
    }

    pub(super) fn inject_catalog(
        &self,
        metadata: &[u8],
        artifact: &[u8],
    ) -> Result<(), StorageError> {
        self.shared
            .lock()
            .expect("test writer lock")
            .writer
            .commit_with_catalog(
                WriteBatch::default(),
                SnapshotCatalogRecord::new(metadata, artifact).expect("bounded catalog fixture"),
            )
    }

    pub(super) fn catalog_attempts(&self) -> Vec<CatalogAttempt> {
        self.shared
            .lock()
            .expect("test writer lock")
            .catalog_attempts
            .clone()
    }
}

impl<W: CatalogCommittedStore> CatalogCommittedStore for Writer<W> {
    type CatalogReader = CatalogReader<W::CatalogReader>;

    fn catalog_reader(&self) -> Self::CatalogReader {
        self.counts
            .lock()
            .expect("test counts lock")
            .catalog_reader_factories += 1;
        CatalogReader {
            inner: self
                .shared
                .lock()
                .expect("test writer lock")
                .writer
                .catalog_reader(),
            counts: Arc::clone(&self.counts),
        }
    }

    fn commit_with_catalog(
        &mut self,
        batch: WriteBatch,
        catalog: SnapshotCatalogRecord<'_>,
    ) -> Result<(), StorageError> {
        self.counts
            .lock()
            .expect("test counts lock")
            .catalog_commits += 1;
        let mut shared = self.shared.lock().expect("test writer lock");
        shared.batches.push(batch.clone());
        // Comparison copies are test-only, not production retention behavior.
        shared.catalog_attempts.push(CatalogAttempt {
            metadata: catalog.metadata().to_vec(),
            artifact: catalog.artifact().to_vec(),
            artifact_pointer: catalog.artifact().as_ptr() as usize,
        });
        let fault = std::mem::take(&mut *self.fault.lock().expect("test fault lock"));
        match fault {
            Fault::CommitBefore => return Err(physical_error()),
            Fault::CommitLimit => return Err(StorageError::ReadLimitExceeded),
            Fault::CommitCorrupt => {
                return Err(StorageError::CorruptMetadata {
                    detail: "SECRET catalog header/address".into(),
                });
            }
            Fault::ExitBefore => std::process::exit(71),
            _ => {}
        }
        shared.writer.commit_with_catalog(batch, catalog)?;
        match fault {
            Fault::CommitAfter => Err(physical_error()),
            Fault::ExitAfter => std::process::exit(72),
            _ => Ok(()),
        }
    }
}

#[derive(Clone)]
pub(super) struct CatalogReader<R> {
    inner: R,
    counts: Arc<Mutex<Counts>>,
}

impl<R: SnapshotCatalogReader> SnapshotCatalogReader for CatalogReader<R> {
    fn read_catalog(&self) -> Result<Option<StoredSnapshotCatalog>, CatalogReadError> {
        self.counts.lock().expect("test counts lock").catalog_reads += 1;
        self.inner.read_catalog()
    }
}
