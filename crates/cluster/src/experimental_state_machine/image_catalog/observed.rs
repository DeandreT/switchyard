use std::sync::{Arc, Condvar, Mutex};

use storage::{
    BoundedStateStore, CatalogCommittedStore, CatalogReadError, CommittedStore, Key, ReadLimits,
    SnapshotCatalogReader, SnapshotCatalogRecord, StateStore, StorageError, StoreSnapshot,
    StoredSnapshotCatalog, Value, WriteBatch,
};
use tokio::sync::Notify;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::experimental_state_machine) struct Counts {
    pub reader_factories: usize,
    pub initialized: usize,
    pub gets: usize,
    pub scans: usize,
    pub snapshots: usize,
    pub applies: usize,
    pub bounded: usize,
    pub catalog_factories: usize,
    pub catalog_reads: usize,
    pub commits: usize,
    pub catalog_commits: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::experimental_state_machine) enum Fault {
    #[default]
    None,
    CapturePhysical,
    CaptureLimit,
    CapturePanic,
    CatalogPhysical,
    CatalogLimit,
    CatalogAllocation,
    CatalogPanic,
    CommitBefore,
    CommitLimit,
    CommitAfter,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::experimental_state_machine) enum GateKind {
    Capture,
    Catalog,
}

#[derive(Default)]
struct Observation {
    counts: Counts,
    limits: Vec<ReadLimits>,
    catalog_batches: Vec<WriteBatch>,
    committed_pointers: Vec<(usize, usize)>,
    read_pointers: Vec<(usize, usize)>,
    fault: Fault,
    gate: Option<(GateKind, Arc<ReadGate>)>,
}

pub(in crate::experimental_state_machine) struct Writer<W: CatalogCommittedStore> {
    writer: Arc<Mutex<W>>,
    observation: Arc<Mutex<Observation>>,
}

pub(in crate::experimental_state_machine) struct Control<W: CatalogCommittedStore> {
    writer: Arc<Mutex<W>>,
    observation: Arc<Mutex<Observation>>,
}

#[derive(Clone)]
pub(in crate::experimental_state_machine) struct Reader<R> {
    inner: R,
    observation: Arc<Mutex<Observation>>,
}

#[derive(Clone)]
pub(in crate::experimental_state_machine) struct CatalogReader<R> {
    inner: R,
    observation: Arc<Mutex<Observation>>,
}

pub(in crate::experimental_state_machine) fn observed<W: CatalogCommittedStore>(
    writer: W,
) -> (Writer<W>, Control<W>) {
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
    pub(in crate::experimental_state_machine) fn counts(&self) -> Counts {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .clone()
    }

    pub(in crate::experimental_state_machine) fn limits(&self) -> Vec<ReadLimits> {
        self.observation
            .lock()
            .expect("test observation")
            .limits
            .clone()
    }

    pub(in crate::experimental_state_machine) fn catalog_batches(&self) -> Vec<WriteBatch> {
        self.observation
            .lock()
            .expect("test observation")
            .catalog_batches
            .clone()
    }

    pub(in crate::experimental_state_machine) fn committed_pointers(&self) -> Vec<(usize, usize)> {
        self.observation
            .lock()
            .expect("test observation")
            .committed_pointers
            .clone()
    }

    pub(in crate::experimental_state_machine) fn read_pointers(&self) -> Vec<(usize, usize)> {
        self.observation
            .lock()
            .expect("test observation")
            .read_pointers
            .clone()
    }

    pub(in crate::experimental_state_machine) fn reset(&self) {
        let mut state = self.observation.lock().expect("test observation");
        assert!(state.gate.is_none(), "reset only after consuming the gate");
        state.counts = Counts::default();
        state.limits.clear();
        state.catalog_batches.clear();
        state.committed_pointers.clear();
        state.read_pointers.clear();
        state.fault = Fault::None;
    }

    // Matching raw capabilities are used only for evidence/injection and serial reopen.
    pub(in crate::experimental_state_machine) fn reader(&self) -> W::Reader {
        self.writer.lock().expect("test writer").reader()
    }

    pub(in crate::experimental_state_machine) fn catalog_reader(&self) -> W::CatalogReader {
        self.writer.lock().expect("test writer").catalog_reader()
    }

    pub(in crate::experimental_state_machine) fn writer(&self) -> Writer<W> {
        Writer {
            writer: self.writer.clone(),
            observation: self.observation.clone(),
        }
    }

    pub(in crate::experimental_state_machine) fn inject(
        &self,
        batch: WriteBatch,
    ) -> Result<(), StorageError> {
        self.writer.lock().expect("test writer").commit(batch)
    }

    pub(in crate::experimental_state_machine) fn retain(
        &self,
        metadata: &[u8],
        image: &[u8],
    ) -> Result<(), StorageError> {
        let record = SnapshotCatalogRecord::new(metadata, image)
            .map_err(|_| StorageError::ReadLimitExceeded)?;
        self.writer
            .lock()
            .expect("test writer")
            .commit_with_catalog(WriteBatch::default(), record)
    }

    pub(in crate::experimental_state_machine) fn fault(&self, fault: Fault) {
        self.observation.lock().expect("test observation").fault = fault;
    }

    pub(in crate::experimental_state_machine) fn gate(&self, kind: GateKind) -> GateGuard {
        let gate = Arc::new(ReadGate {
            state: Mutex::new(GateState::default()),
            notify: Notify::new(),
            wake: Condvar::new(),
        });
        let old = self
            .observation
            .lock()
            .expect("test observation")
            .gate
            .replace((kind, gate.clone()));
        assert!(old.is_none(), "one real read gate at a time");
        GateGuard(gate)
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
            inner: self.writer.lock().expect("test writer").reader(),
            observation: self.observation.clone(),
        }
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .initialized += 1;
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
        catalog: SnapshotCatalogRecord<'_>,
    ) -> Result<(), StorageError> {
        let fault = {
            let mut state = self.observation.lock().expect("test observation");
            state.counts.catalog_commits += 1;
            state.catalog_batches.push(batch.clone());
            state.committed_pointers.push((
                catalog.metadata().as_ptr() as usize,
                catalog.artifact().as_ptr() as usize,
            ));
            if matches!(
                state.fault,
                Fault::CommitBefore | Fault::CommitLimit | Fault::CommitAfter
            ) {
                std::mem::take(&mut state.fault)
            } else {
                Fault::None
            }
        };
        if matches!(fault, Fault::CommitBefore) {
            return Err(physical());
        }
        if matches!(fault, Fault::CommitLimit) {
            return Err(StorageError::ReadLimitExceeded);
        }
        self.writer
            .lock()
            .expect("test writer")
            .commit_with_catalog(batch, catalog)?;
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
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .scans += 1;
        self.inner.scan_from(prefix, start, limit)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.observation
            .lock()
            .expect("test observation")
            .counts
            .snapshots += 1;
        Err(physical())
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
        let (fault, gate) = {
            let mut state = self.observation.lock().expect("test observation");
            state.counts.bounded += 1;
            state.limits.push(limits);
            let fault = if matches!(
                state.fault,
                Fault::CapturePhysical | Fault::CaptureLimit | Fault::CapturePanic
            ) {
                std::mem::take(&mut state.fault)
            } else {
                Fault::None
            };
            let gate = take_gate(&mut state, GateKind::Capture);
            (fault, gate)
        };
        match fault {
            Fault::CapturePhysical => return Err(physical()),
            Fault::CaptureLimit => return Err(StorageError::ReadLimitExceeded),
            _ => {}
        }
        let result = self.inner.snapshot_bounded(limits)?;
        if let Some(gate) = gate {
            gate.block();
        }
        if matches!(fault, Fault::CapturePanic) {
            panic!("static catalog capture test panic");
        }
        Ok(result)
    }
}

impl<R: SnapshotCatalogReader> SnapshotCatalogReader for CatalogReader<R> {
    fn read_catalog(&self) -> Result<Option<StoredSnapshotCatalog>, CatalogReadError> {
        let (fault, gate) = {
            let mut state = self.observation.lock().expect("test observation");
            state.counts.catalog_reads += 1;
            let fault = if matches!(
                state.fault,
                Fault::CatalogPhysical
                    | Fault::CatalogLimit
                    | Fault::CatalogAllocation
                    | Fault::CatalogPanic
            ) {
                std::mem::take(&mut state.fault)
            } else {
                Fault::None
            };
            let gate = take_gate(&mut state, GateKind::Catalog);
            (fault, gate)
        };
        match fault {
            Fault::CatalogPhysical => return Err(CatalogReadError::Storage(physical())),
            Fault::CatalogLimit => return Err(CatalogReadError::LimitExceeded),
            Fault::CatalogAllocation => return Err(CatalogReadError::Allocation),
            _ => {}
        }
        let result = self.inner.read_catalog()?;
        if let Some(slot) = &result {
            self.observation
                .lock()
                .expect("test observation")
                .read_pointers
                .push((
                    slot.metadata().as_ptr() as usize,
                    slot.artifact().as_ptr() as usize,
                ));
        }
        if let Some(gate) = gate {
            gate.block();
        }
        if matches!(fault, Fault::CatalogPanic) {
            panic!("static catalog read test panic");
        }
        Ok(result)
    }
}

fn take_gate(state: &mut Observation, kind: GateKind) -> Option<Arc<ReadGate>> {
    if state.gate.as_ref().is_some_and(|(armed, _)| *armed == kind) {
        state.gate.take().map(|(_, gate)| gate)
    } else {
        None
    }
}

fn physical() -> StorageError {
    StorageError::Backend {
        operation: "native catalog test read",
        detail: "PRIVATE-backend-key-and-path".into(),
    }
}

#[derive(Default)]
struct GateState {
    entered: bool,
    released: bool,
}

struct ReadGate {
    state: Mutex<GateState>,
    notify: Notify,
    wake: Condvar,
}

impl ReadGate {
    fn block(&self) {
        let mut state = self.state.lock().expect("test read gate");
        state.entered = true;
        self.notify.notify_waiters();
        while !state.released {
            state = self.wake.wait(state).expect("test read gate");
        }
    }

    fn release(&self) {
        self.state.lock().expect("test read gate").released = true;
        self.wake.notify_all();
    }
}

pub(in crate::experimental_state_machine) struct GateGuard(Arc<ReadGate>);

impl GateGuard {
    pub(in crate::experimental_state_machine) async fn entered(&self) {
        loop {
            let wake = self.0.notify.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            if self.0.state.lock().expect("test read gate").entered {
                return;
            }
            wake.await;
        }
    }

    pub(in crate::experimental_state_machine) fn release(&self) {
        self.0.release();
    }
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        self.0.release();
    }
}
