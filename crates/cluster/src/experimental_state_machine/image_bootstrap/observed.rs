use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use storage::{
    BoundedStateStore, CommittedStore, Key, ReadLimits, StateStore, StorageError, StoreSnapshot,
    Value, WriteBatch,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Counts {
    pub reader_factories: usize,
    pub initialized: usize,
    pub gets: usize,
    pub scans: Vec<(Vec<u8>, Vec<u8>, usize)>,
    pub snapshots: usize,
    pub bounded: usize,
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
}

struct Shared<W> {
    writer: W,
    force_uninitialized: bool,
    batches: Vec<WriteBatch>,
}

#[derive(Default)]
struct Drops {
    writers: AtomicUsize,
    readers: AtomicUsize,
}

pub(super) struct Writer<W> {
    shared: Arc<Mutex<Shared<W>>>,
    counts: Arc<Mutex<Counts>>,
    fault: Arc<Mutex<Fault>>,
    drops: Arc<Drops>,
}

pub(super) struct Control<W> {
    shared: Arc<Mutex<Shared<W>>>,
    counts: Arc<Mutex<Counts>>,
    fault: Arc<Mutex<Fault>>,
    drops: Arc<Drops>,
}

pub(super) fn observed<W: CommittedStore>(writer: W) -> (Writer<W>, Control<W>) {
    let control = Control {
        shared: Arc::new(Mutex::new(Shared {
            writer,
            force_uninitialized: false,
            batches: Vec::new(),
        })),
        counts: Arc::new(Mutex::new(Counts::default())),
        fault: Arc::new(Mutex::new(Fault::None)),
        drops: Arc::new(Drops::default()),
    };
    (control.writer(), control)
}

impl<W: CommittedStore> Control<W> {
    pub fn counts(&self) -> Counts {
        self.counts.lock().expect("test counts lock").clone()
    }

    pub fn reset(&self) {
        *self.counts.lock().expect("test counts lock") = Counts::default();
    }

    pub fn drops(&self) -> (usize, usize) {
        (
            self.drops.writers.load(Ordering::SeqCst),
            self.drops.readers.load(Ordering::SeqCst),
        )
    }

    pub fn reader(&self) -> W::Reader {
        self.shared
            .lock()
            .expect("test writer lock")
            .writer
            .reader()
    }

    pub fn initialized(&self) -> Result<bool, StorageError> {
        self.shared
            .lock()
            .expect("test writer lock")
            .writer
            .is_initialized()
    }

    pub fn writer(&self) -> Writer<W> {
        Writer {
            shared: self.shared.clone(),
            counts: self.counts.clone(),
            fault: self.fault.clone(),
            drops: self.drops.clone(),
        }
    }

    pub fn fault(&self, fault: Fault) {
        *self.fault.lock().expect("test fault lock") = fault;
    }

    pub fn force_uninitialized(&self) {
        self.shared
            .lock()
            .expect("test writer lock")
            .force_uninitialized = true;
    }

    pub fn inject(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.shared
            .lock()
            .expect("test writer lock")
            .writer
            .commit(batch)
    }

    pub fn batches(&self) -> Vec<WriteBatch> {
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
            drops: self.drops.clone(),
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
        if matches!(fault, Fault::CommitBefore) {
            return Err(physical_error());
        }
        shared.writer.commit(batch)?;
        if matches!(fault, Fault::CommitAfter) {
            return Err(physical_error());
        }
        Ok(())
    }
}

impl<W> Drop for Writer<W> {
    fn drop(&mut self) {
        self.drops.writers.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) struct Reader<R> {
    inner: R,
    fault: Arc<Mutex<Fault>>,
    counts: Arc<Mutex<Counts>>,
    drops: Arc<Drops>,
}

impl<R: Clone> Clone for Reader<R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            fault: self.fault.clone(),
            counts: self.counts.clone(),
            drops: self.drops.clone(),
        }
    }
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

impl<R: BoundedStateStore> BoundedStateStore for Reader<R> {
    fn snapshot_bounded(&self, limits: ReadLimits) -> Result<StoreSnapshot, StorageError> {
        self.counts.lock().expect("test counts lock").bounded += 1;
        self.inner.snapshot_bounded(limits)
    }
}

impl<R> Drop for Reader<R> {
    fn drop(&mut self) {
        self.drops.readers.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) fn bootstrap_counts() -> Counts {
    Counts {
        reader_factories: 1,
        initialized: 1,
        commits: 1,
        scans: vec![(Vec::new(), Vec::new(), 1)],
        ..Counts::default()
    }
}

fn physical_error() -> StorageError {
    StorageError::Backend {
        operation: "injected native image bootstrap target",
        detail: "PRIVATE target path/body/backend detail".into(),
    }
}
