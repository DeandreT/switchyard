use std::{
    collections::{BTreeMap, BTreeSet},
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use domain::{
    CommittedCheckpoint, CommittedStreamId, EntityPath, NamespaceName, QueueConfig, Timestamp,
};
use openraft::{BasicNode, CommittedLeaderId, EntryPayload, Membership, storage::RaftStateMachine};
use storage::{
    CommittedStore, Key, MemoryReplicaStore, StateStore, StorageError, StoreSnapshot, Value,
    WriteBatch,
};
use tokio::sync::{Notify, oneshot};

use super::*;
use crate::{
    ExperimentalStateMachine, LogEntry, LogId, QueueLogCommand, experimental_owner::OwnerJoinError,
};

const DEADLINE: Duration = Duration::from_secs(20);
const READ_ERROR: usize = 1;
const READ_PANIC: usize = 2;
const WRITE_BEFORE: usize = 1;
const WRITE_AFTER: usize = 2;
const WRITE_PANIC: usize = 3;

fn stream() -> CommittedStreamId {
    CommittedStreamId::new([9; 16]).unwrap()
}

fn id(index: u64) -> LogId {
    LogId::new(CommittedLeaderId::new(1, 7), index)
}

fn blank(index: u64) -> LogEntry {
    LogEntry {
        log_id: id(index),
        payload: EntryPayload::Blank,
    }
}

fn membership(index: u64) -> LogEntry {
    LogEntry {
        log_id: id(index),
        payload: EntryPayload::Membership(Membership::new(
            vec![BTreeSet::from([1, 2, 3])],
            BTreeMap::from([
                (1, BasicNode::new("one")),
                (2, BasicNode::new("two")),
                (3, BasicNode::new("three")),
            ]),
        )),
    }
}

fn create(index: u64) -> LogEntry {
    LogEntry {
        log_id: id(index),
        payload: EntryPayload::Normal(QueueLogCommand::create_queue(
            NamespaceName::new("tenant").unwrap(),
            EntityPath::new("orders").unwrap(),
            Timestamp::from_millis(777),
            QueueConfig::default(),
        )),
    }
}

fn expected(entries: Vec<LogEntry>) -> CommittedCheckpoint {
    let mut state = StoreState::create(MemoryReplicaStore::new(), stream()).unwrap();
    state
        .apply(PreparedApply::from_entries(entries).unwrap())
        .unwrap();
    state.checkpoint().unwrap()
}

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(DEADLINE, future).await.unwrap()
}

async fn pending<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("operation crossed its controlled retirement boundary"),
    })
    .await;
}

#[derive(Default)]
struct Control {
    reads: AtomicUsize,
    read_fault: AtomicUsize,
    write_fault: AtomicUsize,
    commit_gate: Mutex<Option<Arc<Gate>>>,
    drop_gate: Mutex<Option<Arc<Gate>>>,
    drop_panics: AtomicBool,
}

struct ObservedWriter {
    inner: MemoryReplicaStore,
    control: Arc<Control>,
}

#[derive(Clone)]
struct ObservedReader {
    inner: <MemoryReplicaStore as CommittedStore>::Reader,
    control: Arc<Control>,
}

fn observed() -> (ObservedWriter, Arc<Control>) {
    let control = Arc::new(Control::default());
    (
        ObservedWriter {
            inner: MemoryReplicaStore::new(),
            control: control.clone(),
        },
        control,
    )
}

fn injected_error() -> StorageError {
    StorageError::CorruptMetadata {
        detail: "private retirement report failure".into(),
    }
}

impl CommittedStore for ObservedWriter {
    type Reader = ObservedReader;

    fn reader(&self) -> Self::Reader {
        ObservedReader {
            inner: self.inner.reader(),
            control: self.control.clone(),
        }
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.inner.is_initialized()
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        let gate = self.control.commit_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.wait()?;
        }
        let fault = self.control.write_fault.swap(0, Ordering::SeqCst);
        match fault {
            WRITE_BEFORE => return Err(injected_error()),
            WRITE_PANIC => panic!("controlled native owner-loop panic"),
            _ => {}
        }
        self.inner.commit(batch)?;
        if fault == WRITE_AFTER {
            Err(injected_error())
        } else {
            Ok(())
        }
    }
}

impl Drop for ObservedWriter {
    fn drop(&mut self) {
        let gate = self.control.drop_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.wait().unwrap();
        }
        assert!(
            !self.control.drop_panics.load(Ordering::SeqCst),
            "controlled native writer-Drop panic"
        );
    }
}

impl StateStore for ObservedReader {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.control.reads.fetch_add(1, Ordering::SeqCst);
        if key == [0x12] {
            match self.control.read_fault.load(Ordering::SeqCst) {
                READ_ERROR => return Err(injected_error()),
                READ_PANIC => panic!("controlled final checkpoint read panic"),
                _ => {}
            }
        }
        self.inner.get(key)
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.inner.apply(batch)
    }
}

struct Gate {
    entered: AtomicBool,
    notify: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}

impl Gate {
    fn wait(&self) -> Result<(), StorageError> {
        self.entered.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
        let released = self.released.lock().unwrap();
        let (released, _) = self
            .wake
            .wait_timeout_while(released, DEADLINE, |released| !*released)
            .unwrap();
        if *released {
            Ok(())
        } else {
            Err(injected_error())
        }
    }
}

struct GateGuard(Arc<Gate>);

impl GateGuard {
    fn new() -> Self {
        Self(Arc::new(Gate {
            entered: AtomicBool::new(false),
            notify: Notify::new(),
            released: Mutex::new(false),
            wake: Condvar::new(),
        }))
    }

    async fn entered(&self) {
        bounded(async {
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
        .await;
    }

    fn release(&self) {
        *self.0.released.lock().unwrap() = true;
        self.0.wake.notify_all();
    }
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        self.release();
    }
}

fn enqueue(handle: &Handle, entries: Vec<LogEntry>) -> oneshot::Receiver<PublishedResponse> {
    let (response, receiver) = oneshot::channel();
    handle
        .admission
        .enqueue(
            &handle.sender,
            Packet {
                operation: Some(Operation::Apply(
                    PreparedApply::from_entries(entries).unwrap(),
                )),
                response: Some(response),
                lease: None,
            },
        )
        .unwrap();
    receiver
}

#[tokio::test]
async fn final_checkpoint_includes_accepted_fifo_suffix_after_waiter_loss() {
    let (writer, control) = observed();
    let state = StoreState::create(writer, stream()).unwrap();
    let (handle, thread) = Handle::start(state).unwrap();
    let mut report = handle.enable_retirement_report().unwrap();
    handle
        .request(Operation::Apply(
            PreparedApply::from_entries([blank(0)]).unwrap(),
        ))
        .await
        .unwrap();
    let before = match handle.request(Operation::Checkpoint).await.unwrap() {
        Reply::Checkpoint(checkpoint) => checkpoint,
        _ => panic!("checkpoint response expected"),
    };
    let gate = GateGuard::new();
    *control.commit_gate.lock().unwrap() = Some(gate.0.clone());
    let first = enqueue(&handle, vec![membership(1)]);
    gate.entered().await;
    let second = enqueue(&handle, vec![create(2)]);
    assert_eq!(handle.workload().unwrap().accepted_jobs, 2);
    drop(first);
    drop(second);
    handle.close();
    assert_eq!(report.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    gate.release();
    assert_eq!(
        bounded(tokio::task::spawn_blocking(move || thread.join()))
            .await
            .unwrap()
            .unwrap(),
        Ok(())
    );
    let final_checkpoint = report.try_recv().unwrap().unwrap();
    assert_eq!(
        final_checkpoint,
        expected(vec![blank(0), membership(1), create(2)])
    );
    assert_ne!(final_checkpoint, *before);
    assert_eq!(
        final_checkpoint.highest_timestamp(),
        Timestamp::from_millis(777)
    );
    assert_eq!(handle.workload().unwrap().accepted_jobs, 0);
}

#[tokio::test]
async fn final_read_failure_and_panic_do_not_change_native_drain_status() {
    for (fault, error) in [
        (READ_ERROR, StateMachineError::Storage),
        (READ_PANIC, StateMachineError::Panicked),
    ] {
        let (writer, control) = observed();
        let store = ExperimentalStateMachine::create(writer, stream()).unwrap();
        let mut report = store.enable_retirement_report().unwrap();
        control.read_fault.store(fault, Ordering::SeqCst);
        assert_eq!(bounded(store.shutdown()).await, Ok(()));
        assert_eq!(report.try_recv().unwrap(), Err(error));
        assert!(!error.to_string().contains("private retirement"));
    }
}

#[tokio::test]
async fn physical_write_poison_before_or_after_commit_has_no_healthy_report() {
    for fault in [WRITE_BEFORE, WRITE_AFTER] {
        let (writer, control) = observed();
        let reader = writer.reader();
        let mut store = ExperimentalStateMachine::create(writer, stream()).unwrap();
        let baseline = reader.snapshot().unwrap();
        let mut report = store.enable_retirement_report().unwrap();
        let view = store.checkpoint_reader();
        control.write_fault.store(fault, Ordering::SeqCst);
        assert!(store.apply([blank(0)]).await.is_err());
        assert_eq!(view.checkpoint().await, Err(StateMachineError::Poisoned));
        let reads = control.reads.load(Ordering::SeqCst);
        assert_eq!(bounded(store.shutdown()).await, Ok(()));
        assert_eq!(report.try_recv().unwrap(), Err(StateMachineError::Poisoned));
        assert_eq!(control.reads.load(Ordering::SeqCst), reads);
        assert_eq!(
            reader.snapshot().unwrap() == baseline,
            fault == WRITE_BEFORE
        );
    }
}

#[tokio::test]
async fn published_report_does_not_cross_the_actual_writer_drop_join_barrier() {
    let (writer, control) = observed();
    let store = ExperimentalStateMachine::create(writer, stream()).unwrap();
    let mut report = store.enable_retirement_report().unwrap();
    let (store, owner) = store.into_runtime_parts().unwrap();
    let gate = GateGuard::new();
    *control.drop_gate.lock().unwrap() = Some(gate.0.clone());
    drop(store);
    gate.entered().await;
    assert_eq!(report.try_recv().unwrap().unwrap().stream(), stream());
    let mut join = Box::pin(owner.join());
    pending(join.as_mut()).await;
    gate.release();
    assert_eq!(bounded(join).await, Ok(()));
}

#[tokio::test]
async fn actual_writer_drop_panic_is_not_hidden_by_a_healthy_report() {
    let (writer, control) = observed();
    let store = ExperimentalStateMachine::create(writer, stream()).unwrap();
    let mut report = store.enable_retirement_report().unwrap();
    let (store, owner) = store.into_runtime_parts().unwrap();
    control.drop_panics.store(true, Ordering::SeqCst);
    drop(store);
    assert_eq!(
        bounded(owner.join()).await,
        Err(OwnerJoinError::ThreadPanicked)
    );
    assert!(report.try_recv().unwrap().is_ok());
}

#[tokio::test]
async fn actual_owner_loop_panic_retains_its_native_failure() {
    let (writer, control) = observed();
    let store = ExperimentalStateMachine::create(writer, stream()).unwrap();
    let mut report = store.enable_retirement_report().unwrap();
    let (mut store, owner) = store.into_runtime_parts().unwrap();
    control.write_fault.store(WRITE_PANIC, Ordering::SeqCst);
    assert!(store.apply([blank(0)]).await.is_err());
    drop(store);
    assert_eq!(
        bounded(owner.join()).await,
        Err(OwnerJoinError::Owner(StateMachineError::Panicked))
    );
    assert_eq!(report.try_recv().unwrap(), Err(StateMachineError::Panicked));
}

#[tokio::test]
async fn unarmed_adapter_and_pairing_shutdown_perform_no_final_read() {
    for paired in [false, true] {
        let (writer, control) = observed();
        let store = ExperimentalStateMachine::create(writer, stream()).unwrap();
        let reads = control.reads.load(Ordering::SeqCst);
        control.read_fault.store(READ_PANIC, Ordering::SeqCst);
        if paired {
            let (store, owner) = store.into_runtime_parts().unwrap();
            drop(store);
            assert_eq!(bounded(owner.join()).await, Ok(()));
        } else {
            assert_eq!(bounded(store.shutdown()).await, Ok(()));
        }
        assert_eq!(control.reads.load(Ordering::SeqCst), reads);
    }
}

#[tokio::test]
async fn duplicate_enable_cannot_replace_the_original_report_sink() {
    let store = ExperimentalStateMachine::create(MemoryReplicaStore::new(), stream()).unwrap();
    let mut report = store.enable_retirement_report().unwrap();
    assert!(matches!(
        store.enable_retirement_report(),
        Err(StateMachineError::Closed)
    ));
    assert_eq!(bounded(store.shutdown()).await, Ok(()));
    assert_eq!(report.try_recv().unwrap().unwrap().stream(), stream());
}

#[tokio::test]
async fn missing_report_sink_does_not_change_actual_native_join_status() {
    let (handle, thread) =
        Handle::start(StoreState::create(MemoryReplicaStore::new(), stream()).unwrap()).unwrap();
    let mut report = handle.enable_retirement_report().unwrap();
    *handle.report.lock().unwrap() = ReportState::Unarmed;
    handle.close();
    assert_eq!(
        bounded(tokio::task::spawn_blocking(move || thread.join()))
            .await
            .unwrap()
            .unwrap(),
        Ok(())
    );
    assert_eq!(report.try_recv(), Err(oneshot::error::TryRecvError::Closed));
    assert!(matches!(
        handle.enable_retirement_report(),
        Err(StateMachineError::Closed)
    ));
}

#[tokio::test]
async fn lost_report_receiver_does_not_change_actual_native_join_status() {
    let store = ExperimentalStateMachine::create(MemoryReplicaStore::new(), stream()).unwrap();
    drop(store.enable_retirement_report().unwrap());
    assert_eq!(bounded(store.shutdown()).await, Ok(()));
}

#[derive(Default)]
struct PanickingWake {
    wakes: AtomicUsize,
}

impl Wake for PanickingWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        panic!("controlled diagnostic report-waiter wake panic");
    }
}

#[tokio::test]
async fn report_waiter_wake_panic_does_not_change_normal_or_failed_native_join() {
    for owner_panics in [false, true] {
        let (writer, control) = observed();
        let store = ExperimentalStateMachine::create(writer, stream()).unwrap();
        let mut report = store.enable_retirement_report().unwrap();
        let wake = Arc::new(PanickingWake::default());
        let waker = Waker::from(wake.clone());
        {
            let mut context = Context::from_waker(&waker);
            assert!(Pin::new(&mut report).poll(&mut context).is_pending());
        }
        let (mut store, owner) = store.into_runtime_parts().unwrap();
        if owner_panics {
            control.write_fault.store(WRITE_PANIC, Ordering::SeqCst);
            assert!(store.apply([blank(0)]).await.is_err());
        }
        drop(store);
        let joined = bounded(owner.join()).await;
        assert_eq!(
            joined,
            if owner_panics {
                Err(OwnerJoinError::Owner(StateMachineError::Panicked))
            } else {
                Ok(())
            }
        );
        assert_eq!(wake.wakes.load(Ordering::SeqCst), 1);
        if owner_panics {
            assert!(!matches!(report.try_recv(), Ok(Ok(_))));
        }
    }
}

#[tokio::test]
async fn poisoned_report_sink_cannot_yield_a_healthy_checkpoint() {
    let (writer, control) = observed();
    let (handle, thread) = Handle::start(StoreState::create(writer, stream()).unwrap()).unwrap();
    let mut report = handle.enable_retirement_report().unwrap();
    let sink = handle.report.clone();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _guard = sink.lock().unwrap();
            panic!("controlled report-sink mutex poison");
        }))
        .is_err()
    );
    assert!(matches!(
        handle.enable_retirement_report(),
        Err(StateMachineError::Panicked)
    ));
    let reads = control.reads.load(Ordering::SeqCst);
    handle.close();
    assert_eq!(
        bounded(tokio::task::spawn_blocking(move || thread.join()))
            .await
            .unwrap()
            .unwrap(),
        Ok(())
    );
    assert_eq!(report.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    assert_eq!(control.reads.load(Ordering::SeqCst), reads);
    drop(sink);
    drop(handle);
    assert_eq!(report.try_recv(), Err(oneshot::error::TryRecvError::Closed));
}
