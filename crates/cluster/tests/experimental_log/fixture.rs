use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

use cluster::{ExperimentalLogStore, LogEntry, LogId, LogProfile};
use domain::{CommittedSend, CommittedStreamId, EntityPath, NamespaceName, Timestamp};
use openraft::{CommittedLeaderId, EntryPayload};
use storage::{CommittedStore, StorageError, WriteBatch};
use tokio::sync::Notify;

use super::{DEADLINE, TestResult};

pub(super) fn profile() -> TestResult<LogProfile> {
    Ok(LogProfile::new(7, CommittedStreamId::new([9; 16])?)?)
}

pub(super) fn id(term: u64, index: u64) -> LogId {
    LogId::new(CommittedLeaderId::new(term, 7), index)
}

pub(super) fn blank(index: u64) -> LogEntry {
    LogEntry {
        log_id: id(1, index),
        payload: EntryPayload::Blank,
    }
}

pub(super) fn send(index: u64, body: Vec<u8>) -> TestResult<LogEntry> {
    Ok(LogEntry {
        log_id: id(1, index),
        payload: EntryPayload::Normal(cluster::QueueLogCommand::send(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(2),
            CommittedSend {
                message_id: format!("message-{index}"),
                body,
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    })
}

pub(super) async fn pending<F: Future + ?Sized>(mut future: Pin<&mut F>) -> TestResult {
    poll_fn(|cx| match future.as_mut().poll(cx) {
        std::task::Poll::Pending => std::task::Poll::Ready(Ok(())),
        std::task::Poll::Ready(_) => std::task::Poll::Ready(Err(
            "operation completed before its controlled boundary".into(),
        )),
    })
    .await
}

pub(super) async fn workload(
    store: &ExperimentalLogStore,
    jobs: usize,
) -> TestResult<cluster::LogWorkload> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let observed = store.workload()?;
        if observed.accepted_jobs == jobs {
            return Ok(observed);
        }
        if Instant::now() >= deadline {
            return Err(
                "experimental log workload did not reach its expected bounded state".into(),
            );
        }
        tokio::task::yield_now().await;
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) enum Fault {
    #[default]
    None,
    Before,
    After,
    ExitBefore,
    ExitAfter,
    PanicBefore,
    PanicAfter,
}

struct SharedWriter<W> {
    writer: W,
    fault: Fault,
    gate: Option<Arc<CommitGate>>,
    last_batch: Option<WriteBatch>,
}

pub(super) struct ObservedWriter<W: CommittedStore> {
    writer: Arc<Mutex<SharedWriter<W>>>,
    reader: W::Reader,
    commits: Arc<AtomicUsize>,
}

pub(super) struct Control<W: CommittedStore> {
    writer: Arc<Mutex<SharedWriter<W>>>,
    reader: W::Reader,
    commits: Arc<AtomicUsize>,
}

pub(super) fn observed<W: CommittedStore>(writer: W) -> (ObservedWriter<W>, Control<W>) {
    let reader = writer.reader();
    let writer = Arc::new(Mutex::new(SharedWriter {
        writer,
        fault: Fault::None,
        gate: None,
        last_batch: None,
    }));
    let commits = Arc::new(AtomicUsize::new(0));
    (
        ObservedWriter {
            writer: writer.clone(),
            reader: reader.clone(),
            commits: commits.clone(),
        },
        Control {
            writer,
            reader,
            commits,
        },
    )
}

impl<W: CommittedStore> Control<W> {
    pub(super) fn commits(&self) -> usize {
        self.commits.load(Ordering::SeqCst)
    }

    pub(super) fn reader(&self) -> W::Reader {
        self.reader.clone()
    }

    pub(super) fn fault(&self, fault: Fault) {
        self.writer.lock().expect("test writer lock").fault = fault;
    }

    pub(super) fn gate(&self) -> GateGuard {
        let gate = Arc::new(CommitGate {
            entered: AtomicBool::new(false),
            entered_notify: Notify::new(),
            released: Mutex::new(false),
            release_notify: Condvar::new(),
        });
        self.writer.lock().expect("test writer lock").gate = Some(gate.clone());
        GateGuard(gate)
    }

    pub(super) fn last_batch(&self) -> WriteBatch {
        self.writer
            .lock()
            .expect("test writer lock")
            .last_batch
            .clone()
            .expect("observed commit batch")
    }

    pub(super) fn inject(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.writer
            .lock()
            .expect("test writer lock")
            .writer
            .commit(batch)
    }

    pub(super) fn recover_writer(&self) -> ObservedWriter<W> {
        ObservedWriter {
            writer: self.writer.clone(),
            reader: self.reader.clone(),
            commits: self.commits.clone(),
        }
    }
}

impl<W: CommittedStore> CommittedStore for ObservedWriter<W> {
    type Reader = W::Reader;

    fn reader(&self) -> Self::Reader {
        self.reader.clone()
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.writer
            .lock()
            .expect("test writer lock")
            .writer
            .is_initialized()
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        let (fault, gate) = {
            let mut writer = self.writer.lock().expect("test writer lock");
            writer.last_batch = Some(batch.clone());
            (std::mem::take(&mut writer.fault), writer.gate.take())
        };
        if let Some(gate) = gate {
            gate.entered.store(true, Ordering::SeqCst);
            gate.entered_notify.notify_waiters();
            let mut released = gate.released.lock().expect("test commit gate lock");
            while !*released {
                released = gate
                    .release_notify
                    .wait(released)
                    .expect("test commit gate lock");
            }
        }
        match fault {
            Fault::Before => return Err(injected_error()),
            Fault::ExitBefore => std::process::exit(77),
            Fault::PanicBefore => panic!("injected experimental log worker panic"),
            _ => {}
        }
        self.writer
            .lock()
            .expect("test writer lock")
            .writer
            .commit(batch)?;
        match fault {
            Fault::After => Err(injected_error()),
            Fault::ExitAfter => std::process::exit(78),
            Fault::PanicAfter => panic!("injected experimental log worker panic after commit"),
            _ => Ok(()),
        }
    }
}

fn injected_error() -> StorageError {
    StorageError::CorruptMetadata {
        detail: "injected experimental log write failure".into(),
    }
}

struct CommitGate {
    entered: AtomicBool,
    entered_notify: Notify,
    released: Mutex<bool>,
    release_notify: Condvar,
}

pub(super) struct GateGuard(Arc<CommitGate>);

impl GateGuard {
    pub(super) async fn entered(&self) -> TestResult {
        tokio::time::timeout(DEADLINE, async {
            loop {
                let notified = self.0.entered_notify.notified();
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
        *self.0.released.lock().expect("test commit gate lock") = true;
        self.0.release_notify.notify_all();
    }
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        self.release();
    }
}
