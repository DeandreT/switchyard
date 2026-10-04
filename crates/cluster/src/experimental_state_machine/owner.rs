use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

use domain::CommittedCheckpoint;
use flume::{Receiver, RecvTimeoutError, Sender};
use storage::CommittedStore;
use tokio::sync::oneshot;

use super::{
    AppliedState, LogApplication, MAX_STATE_MACHINE_OWNER_JOBS, StateMachineError,
    StateMachineImageExportError, StateMachineWorkload,
    budget::{Admission, Lease},
    input::PreparedApply,
    state::StoreState,
};

pub(super) enum Operation {
    Apply(PreparedApply),
    AppliedState,
    Checkpoint,
    ExportImage,
}

pub(super) enum Reply {
    Applications(Vec<LogApplication>),
    AppliedState(Box<AppliedState>),
    Checkpoint(Box<CommittedCheckpoint>),
    ImageExport(Result<domain::EncodedCommittedImage, StateMachineImageExportError>),
}

type Response = Result<Reply, StateMachineError>;
type PublishedResponse = (Response, oneshot::Receiver<()>);
type RetirementReport = Result<CommittedCheckpoint, StateMachineError>;

#[derive(Default)]
enum ReportState {
    #[default]
    Unarmed,
    Armed(oneshot::Sender<RetirementReport>),
    Finished,
}

pub(super) struct Packet {
    operation: Option<Operation>,
    response: Option<oneshot::Sender<PublishedResponse>>,
    lease: Option<Lease>,
}

impl Packet {
    pub(super) fn encoded_bytes(&self) -> usize {
        match self.operation.as_ref() {
            Some(Operation::Apply(entries)) => entries.encoded_bytes(),
            Some(Operation::ExportImage) => domain::MAX_COMMITTED_IMAGE_BYTES,
            _ => 64,
        }
    }

    pub(super) fn set_lease(&mut self, lease: Lease) {
        self.lease = Some(lease);
    }

    pub(super) fn finish(mut self, result: Response) {
        self.publish(result);
    }

    fn publish(&mut self, result: Response) {
        drop(self.operation.take());
        let Some(response) = self.response.take() else {
            drop(self.lease.take());
            return;
        };
        let (released, completion) = oneshot::channel();
        let _ = response.send((result, completion));
        // A returning caller crosses this barrier only after publication and
        // capacity refund. A lost waiter never retains or cancels the lease.
        drop(self.lease.take());
        let _ = released.send(());
    }
}

impl Drop for Packet {
    fn drop(&mut self) {
        self.publish(Err(StateMachineError::Panicked));
    }
}

#[derive(Clone)]
pub(super) struct Handle {
    sender: Sender<Packet>,
    admission: Arc<Admission>,
    report: Arc<Mutex<ReportState>>,
}

impl Handle {
    pub(super) fn start<W: CommittedStore>(
        state: StoreState<W>,
    ) -> Result<(Self, JoinHandle<Result<(), StateMachineError>>), StateMachineError> {
        let (sender, receiver) = flume::bounded(MAX_STATE_MACHINE_OWNER_JOBS);
        let admission = Arc::new(Admission::default());
        let owner_admission = Arc::clone(&admission);
        let report = Arc::new(Mutex::new(ReportState::default()));
        let owner_report = Arc::clone(&report);
        let thread = thread::Builder::new()
            .name("switchyard-committed-owner".into())
            .spawn(move || run(state, receiver, owner_admission, owner_report))
            .map_err(|_| StateMachineError::ThreadStart)?;
        Ok((
            Self {
                sender,
                admission,
                report,
            },
            thread,
        ))
    }

    pub(super) fn enable_retirement_report(
        &self,
    ) -> Result<oneshot::Receiver<RetirementReport>, StateMachineError> {
        let mut report = self
            .report
            .lock()
            .map_err(|_| StateMachineError::Panicked)?;
        if !matches!(*report, ReportState::Unarmed) || self.admission.is_closed() {
            return Err(StateMachineError::Closed);
        }
        let (sender, receiver) = oneshot::channel();
        *report = ReportState::Armed(sender);
        Ok(receiver)
    }

    pub(super) async fn request(&self, operation: Operation) -> Response {
        let (response, receiver) = oneshot::channel();
        self.admission.enqueue(
            &self.sender,
            Packet {
                operation: Some(operation),
                response: Some(response),
                lease: None,
            },
        )?;
        let (result, completion) = receiver.await.map_err(|_| StateMachineError::Panicked)?;
        completion.await.map_err(|_| StateMachineError::Panicked)?;
        result
    }

    pub(super) fn close(&self) {
        self.admission.close(StateMachineError::Closed);
    }

    pub(super) fn workload(&self) -> Result<StateMachineWorkload, StateMachineError> {
        self.admission.workload()
    }
}

fn run<W: CommittedStore>(
    mut state: StoreState<W>,
    receiver: Receiver<Packet>,
    admission: Arc<Admission>,
    report: Arc<Mutex<ReportState>>,
) -> Result<(), StateMachineError> {
    let result = catch_unwind(AssertUnwindSafe(|| {
        loop {
            if admission.is_closed() && receiver.is_empty() {
                break;
            }
            let mut packet = match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(packet) => packet,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            };
            let result = match packet.operation.take() {
                Some(Operation::Apply(entries)) => state.apply(entries).map(Reply::Applications),
                Some(Operation::AppliedState) => state
                    .applied_state()
                    .map(|state| Reply::AppliedState(Box::new(state))),
                Some(Operation::Checkpoint) => state
                    .checkpoint()
                    .map(|checkpoint| Reply::Checkpoint(Box::new(checkpoint))),
                Some(Operation::ExportImage) => {
                    Ok(Reply::ImageExport(state.export_create_send_image()))
                }
                None => Err(StateMachineError::Panicked),
            };
            packet.finish(result);
        }
    }));
    if result.is_err() {
        admission.close(StateMachineError::Panicked);
        for packet in receiver.try_iter() {
            packet.finish(Err(StateMachineError::Panicked));
        }
        if let Some(report) = take_report(&report) {
            publish_report(report, Err(StateMachineError::Panicked));
        }
        return Err(StateMachineError::Panicked);
    }
    admission.close(StateMachineError::Closed);
    if let Some(report) = take_report(&report) {
        // Continuity failure is not a failure to drain and join the owner.
        let checkpoint = catch_unwind(AssertUnwindSafe(|| state.checkpoint()))
            .unwrap_or(Err(StateMachineError::Panicked));
        publish_report(report, checkpoint);
    }
    Ok(())
}

fn publish_report(report: oneshot::Sender<RetirementReport>, checkpoint: RetirementReport) {
    // A diagnostic waiter's wake can panic without changing native join status.
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let _ = report.send(checkpoint);
    }));
}

fn take_report(report: &Mutex<ReportState>) -> Option<oneshot::Sender<RetirementReport>> {
    let mut report = report.lock().ok()?;
    match std::mem::replace(&mut *report, ReportState::Finished) {
        ReportState::Armed(sender) => Some(sender),
        ReportState::Unarmed | ReportState::Finished => None,
    }
}

#[cfg(test)]
mod retirement_tests;
