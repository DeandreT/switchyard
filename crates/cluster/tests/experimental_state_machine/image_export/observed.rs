use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};

use storage::{
    BoundedStateStore, CommittedStore, Key, ReadLimits, StateStore, StorageError, StoreSnapshot,
    Value, WriteBatch,
};
use tokio::sync::Notify;

use super::{DEADLINE, TestResult};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Counts {
    pub gets: usize,
    pub scans: usize,
    pub ordinary: usize,
    pub applies: usize,
    pub bounded: usize,
    pub initialized: usize,
    pub commits: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) enum Fault {
    #[default]
    None,
    ReadLimit,
    ReadPhysical,
    ReadPanic,
    CommitBefore,
    CommitAfter,
}

#[derive(Default)]
struct Observation {
    counts: Counts,
    limits: Vec<ReadLimits>,
    fault: Fault,
    gate: Option<Arc<ReadGate>>,
}

pub(super) struct Writer<W: CommittedStore> {
    writer: Arc<Mutex<W>>,
    reader: Reader<W::Reader>,
}

pub(super) struct Control<W: CommittedStore> {
    writer: Arc<Mutex<W>>,
    reader: Reader<W::Reader>,
}

#[derive(Clone)]
pub(super) struct Reader<R> {
    inner: R,
    observation: Arc<Mutex<Observation>>,
}

pub(super) fn observed<W: CommittedStore>(writer: W) -> (Writer<W>, Control<W>) {
    let reader = Reader {
        inner: writer.reader(),
        observation: Arc::new(Mutex::new(Observation::default())),
    };
    let writer = Arc::new(Mutex::new(writer));
    (
        Writer {
            writer: writer.clone(),
            reader: reader.clone(),
        },
        Control { writer, reader },
    )
}

impl<W: CommittedStore> Control<W> {
    pub(super) fn counts(&self) -> Counts {
        self.reader
            .observation
            .lock()
            .expect("test observation lock")
            .counts
            .clone()
    }

    pub(super) fn limits(&self) -> Vec<ReadLimits> {
        self.reader
            .observation
            .lock()
            .expect("test observation lock")
            .limits
            .clone()
    }

    pub(super) fn reset(&self) {
        let mut observed = self
            .reader
            .observation
            .lock()
            .expect("test observation lock");
        observed.counts = Counts::default();
        observed.limits.clear();
    }

    // Baseline evidence bypasses observations, not the matching read-only API.
    pub(super) fn reader(&self) -> W::Reader {
        self.reader.inner.clone()
    }

    pub(super) fn inject(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.writer.lock().expect("test writer lock").commit(batch)
    }

    pub(super) fn writer(&self) -> Writer<W> {
        Writer {
            writer: self.writer.clone(),
            reader: self.reader.clone(),
        }
    }

    pub(super) fn fault(&self, fault: Fault) {
        self.reader
            .observation
            .lock()
            .expect("test observation lock")
            .fault = fault;
    }

    pub(super) fn gate(&self) -> GateGuard {
        let gate = Arc::new(ReadGate {
            entered: AtomicBool::new(false),
            notify: Notify::new(),
            released: Mutex::new(false),
            wake: Condvar::new(),
        });
        let old = self
            .reader
            .observation
            .lock()
            .expect("test observation lock")
            .gate
            .replace(gate.clone());
        assert!(old.is_none(), "only one bounded capture gate may be armed");
        GateGuard(gate)
    }
}

impl<W: CommittedStore> CommittedStore for Writer<W> {
    type Reader = Reader<W::Reader>;

    fn reader(&self) -> Self::Reader {
        self.reader.clone()
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.reader
            .observation
            .lock()
            .expect("test observation lock")
            .counts
            .initialized += 1;
        self.writer
            .lock()
            .expect("test writer lock")
            .is_initialized()
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        let fault = {
            let mut observed = self
                .reader
                .observation
                .lock()
                .expect("test observation lock");
            observed.counts.commits += 1;
            if matches!(observed.fault, Fault::CommitBefore | Fault::CommitAfter) {
                std::mem::take(&mut observed.fault)
            } else {
                Fault::None
            }
        };
        if matches!(fault, Fault::CommitBefore) {
            return Err(physical_error());
        }
        self.writer
            .lock()
            .expect("test writer lock")
            .commit(batch)?;
        if matches!(fault, Fault::CommitAfter) {
            return Err(physical_error());
        }
        Ok(())
    }
}

impl<R: StateStore> StateStore for Reader<R> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.observation
            .lock()
            .expect("test observation lock")
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
        self.observation
            .lock()
            .expect("test observation lock")
            .counts
            .scans += 1;
        self.inner.scan_from(prefix, start, limit)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.observation
            .lock()
            .expect("test observation lock")
            .counts
            .ordinary += 1;
        Err(physical_error())
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observation
            .lock()
            .expect("test observation lock")
            .counts
            .applies += 1;
        self.inner.apply(batch)
    }
}

impl<R: BoundedStateStore> BoundedStateStore for Reader<R> {
    fn snapshot_bounded(&self, limits: ReadLimits) -> Result<StoreSnapshot, StorageError> {
        let (fault, gate) = {
            let mut observed = self.observation.lock().expect("test observation lock");
            observed.counts.bounded += 1;
            observed.limits.push(limits);
            let fault = if matches!(
                observed.fault,
                Fault::ReadLimit | Fault::ReadPhysical | Fault::ReadPanic
            ) {
                std::mem::take(&mut observed.fault)
            } else {
                Fault::None
            };
            (fault, observed.gate.take())
        };
        match fault {
            Fault::ReadLimit => return Err(StorageError::ReadLimitExceeded),
            Fault::ReadPhysical => return Err(physical_error()),
            _ => {}
        }
        let snapshot = self.inner.snapshot_bounded(limits)?;
        if let Some(gate) = gate {
            // The real complete snapshot is already captured. No backend,
            // observation, or writer lock is held through this fixture wait.
            gate.entered.store(true, Ordering::SeqCst);
            gate.notify.notify_waiters();
            let mut released = gate.released.lock().expect("test read gate lock");
            while !*released {
                released = gate.wake.wait(released).expect("test read gate lock");
            }
        }
        // All observation/backend/gate locks are released before the actual
        // owner must handle a panic and drop this captured snapshot.
        if matches!(fault, Fault::ReadPanic) {
            panic!("injected bounded image capture panic");
        }
        Ok(snapshot)
    }
}

fn physical_error() -> StorageError {
    StorageError::Backend {
        operation: "injected bounded capture",
        detail: "SECRET source/path/body detail".into(),
    }
}

struct ReadGate {
    entered: AtomicBool,
    notify: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}

pub(super) struct GateGuard(Arc<ReadGate>);

impl GateGuard {
    pub(super) async fn entered(&self) -> TestResult {
        tokio::time::timeout(DEADLINE, async {
            loop {
                let notified = self.0.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.0.entered.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        })
        .await?;
        Ok(())
    }

    pub(super) fn release(&self) {
        *self.0.released.lock().expect("test read gate lock") = true;
        self.0.wake.notify_all();
    }
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        self.release();
    }
}
