use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use storage::{
    BoundedStateStore, CatalogCommittedStore, CatalogReadError, CommittedStore, ReadLimits,
    SnapshotCatalogReader, SnapshotCatalogRecord, StateStore, StorageError, StoreSnapshot,
    StoredSnapshotCatalog, WriteBatch,
};

#[derive(Default)]
pub(super) struct Control {
    pub commits: AtomicUsize,
    pub captures: AtomicUsize,
    pub catalog_commits: AtomicUsize,
    pub reads: AtomicUsize,
    pub catalog_reads: AtomicUsize,
    mode: Mutex<Mode>,
    read_mode: Mutex<Mode>,
    gate: Mutex<Option<Arc<GateState>>>,
}
#[derive(Clone, Copy, Default)]
pub(super) enum Mode {
    #[default]
    Healthy,
    Before,
    After,
    Panic,
    ExitBefore,
    ExitAfter,
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Target {
    Commit,
    Catalog,
    Capture,
    CatalogRead,
}
struct GateState {
    target: Target,
    entered: Mutex<bool>,
    released: Mutex<bool>,
    wake: Condvar,
    notify: tokio::sync::Notify,
}
pub(super) struct Gate(Arc<GateState>);
impl Gate {
    pub async fn entered(&self) -> Result<(), std::io::Error> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let notified = self.0.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if *self.0.entered.lock().unwrap() {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| std::io::Error::other("test-only local gate was not entered"))
    }
    pub fn release(&self) {
        *self.0.released.lock().unwrap() = true;
        self.0.wake.notify_all();
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.release();
    }
}
impl Control {
    pub fn fail(&self, mode: Mode) {
        *self.mode.lock().unwrap() = mode;
    }
    pub fn fail_read(&self, mode: Mode) {
        *self.read_mode.lock().unwrap() = mode;
    }
    pub fn gate(&self, target: Target) -> Gate {
        let state = Arc::new(GateState {
            target,
            entered: Mutex::new(false),
            released: Mutex::new(false),
            wake: Condvar::new(),
            notify: tokio::sync::Notify::new(),
        });
        *self.gate.lock().unwrap() = Some(state.clone());
        Gate(state)
    }
    fn pause(&self, target: Target) {
        let gate = {
            let mut gate = self.gate.lock().unwrap();
            if gate.as_ref().is_some_and(|gate| gate.target == target) {
                gate.take()
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            *gate.entered.lock().unwrap() = true;
            gate.notify.notify_waiters();
            let mut released = gate.released.lock().unwrap();
            while !*released {
                released = gate.wake.wait(released).unwrap();
            }
        }
    }
    fn mode(&self) -> Mode {
        *self.mode.lock().unwrap()
    }
    fn read_mode(&self) -> Mode {
        *self.read_mode.lock().unwrap()
    }
}

pub(super) struct Observed<W> {
    inner: W,
    pub control: Arc<Control>,
}
impl<W> Observed<W> {
    pub fn new(inner: W) -> (Self, Arc<Control>) {
        let control = Arc::new(Control::default());
        (
            Self {
                inner,
                control: control.clone(),
            },
            control,
        )
    }
}
pub(super) struct Reader<R> {
    inner: R,
    control: Arc<Control>,
}
impl<R: Clone> Clone for Reader<R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            control: self.control.clone(),
        }
    }
}
impl<R: StateStore> StateStore for Reader<R> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.control.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.inner.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.control.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        self.control.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
}
impl<R: BoundedStateStore> BoundedStateStore for Reader<R> {
    fn snapshot_bounded(&self, limits: ReadLimits) -> Result<StoreSnapshot, StorageError> {
        self.control.captures.fetch_add(1, Ordering::SeqCst);
        self.control.pause(Target::Capture);
        match self.control.read_mode() {
            Mode::Before | Mode::After => Err(physical()),
            Mode::Panic => panic!("test-only source capture panic"),
            Mode::Healthy | Mode::ExitBefore | Mode::ExitAfter => {
                self.inner.snapshot_bounded(limits)
            }
        }
    }
}
impl<W: CommittedStore> CommittedStore for Observed<W> {
    type Reader = Reader<W::Reader>;
    fn reader(&self) -> Self::Reader {
        Reader {
            inner: self.inner.reader(),
            control: self.control.clone(),
        }
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.inner.is_initialized()
    }
    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.control.commits.fetch_add(1, Ordering::SeqCst);
        self.control.pause(Target::Commit);
        let mode = self.control.mode();
        if matches!(mode, Mode::Before) {
            return Err(physical());
        }
        if matches!(mode, Mode::Panic) {
            panic!("test-only local log commit panic");
        }
        if matches!(mode, Mode::ExitBefore) {
            std::process::exit(91);
        }
        self.inner.commit(batch)?;
        if matches!(mode, Mode::ExitAfter) {
            std::process::exit(92);
        }
        if matches!(mode, Mode::After) {
            Err(physical())
        } else {
            Ok(())
        }
    }
}
impl<W: CatalogCommittedStore> CatalogCommittedStore for Observed<W> {
    type CatalogReader = CatalogReader<W::CatalogReader>;
    fn catalog_reader(&self) -> Self::CatalogReader {
        CatalogReader {
            inner: self.inner.catalog_reader(),
            control: self.control.clone(),
        }
    }
    fn commit_with_catalog(
        &mut self,
        batch: WriteBatch,
        catalog: SnapshotCatalogRecord<'_>,
    ) -> Result<(), StorageError> {
        self.control.catalog_commits.fetch_add(1, Ordering::SeqCst);
        self.control.pause(Target::Catalog);
        let mode = self.control.mode();
        if matches!(mode, Mode::Before) {
            return Err(physical());
        }
        if matches!(mode, Mode::Panic) {
            panic!("test-only local catalog commit panic");
        }
        if matches!(mode, Mode::ExitBefore) {
            std::process::exit(91);
        }
        self.inner.commit_with_catalog(batch, catalog)?;
        if matches!(mode, Mode::ExitAfter) {
            std::process::exit(92);
        }
        if matches!(mode, Mode::After) {
            Err(physical())
        } else {
            Ok(())
        }
    }
}
pub(super) struct CatalogReader<R> {
    inner: R,
    control: Arc<Control>,
}
impl<R: Clone> Clone for CatalogReader<R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            control: self.control.clone(),
        }
    }
}
impl<R: SnapshotCatalogReader> SnapshotCatalogReader for CatalogReader<R> {
    fn read_catalog(&self) -> Result<Option<StoredSnapshotCatalog>, CatalogReadError> {
        self.control.catalog_reads.fetch_add(1, Ordering::SeqCst);
        self.control.pause(Target::CatalogRead);
        match self.control.read_mode() {
            Mode::Before | Mode::After => Err(physical().into()),
            Mode::Panic => panic!("test-only source catalog read panic"),
            Mode::Healthy | Mode::ExitBefore | Mode::ExitAfter => self.inner.read_catalog(),
        }
    }
}
fn physical() -> StorageError {
    StorageError::Backend {
        operation: "test-only commit",
        detail: "PRIVATE backend failure".into(),
    }
}
