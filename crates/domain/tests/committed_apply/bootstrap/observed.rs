use std::sync::{Arc, Mutex};

use storage::{CommittedStore, Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Counts {
    pub reader_factories: usize,
    pub initialized: usize,
    pub gets: usize,
    pub scans: Vec<(Vec<u8>, Vec<u8>, usize)>,
    pub snapshots: usize,
    pub reader_applies: usize,
    pub commits: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) enum Fault {
    #[default]
    None,
    Initialized,
    Scan,
    CommitBefore,
    CommitAfter,
    ExitBefore,
    ExitAfter,
}

struct Shared<W> {
    writer: W,
    force_uninitialized: bool,
    batches: Vec<WriteBatch>,
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
        if matches!(*fault, Fault::Initialized) {
            *fault = Fault::None;
            return Err(physical_error());
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
        if matches!(*fault, Fault::Scan) {
            *fault = Fault::None;
            return Err(physical_error());
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
