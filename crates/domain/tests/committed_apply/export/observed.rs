use std::sync::{Arc, Mutex};

use storage::{
    BoundedStateStore, CommittedStore, Key, ReadLimits, StateStore, StorageError, StoreSnapshot,
    Value, WriteBatch,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Counts {
    pub gets: usize,
    pub scans: usize,
    pub ordinary: usize,
    pub bounded: usize,
    pub initialized: usize,
    pub commits: usize,
    pub applies: usize,
}

#[derive(Clone, Copy, Default)]
pub(super) enum Fault {
    #[default]
    None,
    ReadLimit,
    ReadPhysical,
    WriteBefore,
    WriteAfter,
}

#[derive(Default)]
struct Observation {
    counts: Counts,
    limits: Vec<ReadLimits>,
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

pub(super) fn observed<W: CommittedStore>(writer: W) -> (Writer<W>, Control<W>) {
    let inner = Arc::new(Mutex::new(writer));
    let observation = Arc::new(Mutex::new(Observation::default()));
    (
        Writer {
            inner: inner.clone(),
            observation: observation.clone(),
        },
        Control { inner, observation },
    )
}

impl<W: CommittedStore> Control<W> {
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

    pub(super) fn reset(&self) {
        let mut observation = self.observation.lock().expect("test observation");
        observation.counts = Counts::default();
        observation.limits.clear();
    }

    pub(super) fn fault(&self, fault: Fault) {
        self.observation.lock().expect("test observation").fault = fault;
    }

    pub(super) fn reader(&self) -> W::Reader {
        self.inner.lock().expect("test writer").reader()
    }

    pub(super) fn recover_writer(&self) -> Writer<W> {
        Writer {
            inner: self.inner.clone(),
            observation: self.observation.clone(),
        }
    }

    // Trusted corruption fixture only; no production writer escape is added.
    pub(super) fn inject(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.inner.lock().expect("test writer").commit(batch)
    }
}

impl<W: CommittedStore> CommittedStore for Writer<W> {
    type Reader = Reader<W::Reader>;

    fn reader(&self) -> Self::Reader {
        Reader {
            inner: self.inner.lock().expect("test writer").reader(),
            observation: self.observation.clone(),
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
        let fault = {
            let mut observation = self.observation.lock().expect("test observation");
            observation.counts.commits += 1;
            std::mem::take(&mut observation.fault)
        };
        if matches!(fault, Fault::WriteBefore) {
            return Err(physical_error());
        }
        self.inner.lock().expect("test writer").commit(batch)?;
        if matches!(fault, Fault::WriteAfter) {
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
            detail: "forbidden-test-fallback".into(),
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
                Fault::ReadLimit | Fault::ReadPhysical => std::mem::take(&mut observation.fault),
                _ => Fault::None,
            }
        };
        match fault {
            Fault::ReadLimit => Err(StorageError::ReadLimitExceeded),
            Fault::ReadPhysical => Err(physical_error()),
            _ => self.inner.snapshot_bounded(limits),
        }
    }
}

fn physical_error() -> StorageError {
    StorageError::Backend {
        operation: "injected read or write",
        detail: "secret-backend-detail".into(),
    }
}
