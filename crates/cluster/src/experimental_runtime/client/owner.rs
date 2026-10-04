use std::sync::Arc;

use tokio::{sync::mpsc, task::JoinHandle};

use super::super::node::{NodeStopCause, StopSignal};
use super::{
    budget::Admission,
    completion::{ActiveSlot, HeldCompletion, Job},
    fatal,
    write::{Context, Flow},
};

pub(in crate::experimental_runtime) struct ClientOwner {
    task: JoinHandle<()>,
    slot: Arc<ActiveSlot>,
    admission: Arc<Admission>,
    stop: StopSignal,
}

impl ClientOwner {
    pub(super) fn new(
        task: JoinHandle<()>,
        slot: Arc<ActiveSlot>,
        admission: Arc<Admission>,
        stop: StopSignal,
    ) -> Self {
        Self {
            task,
            slot,
            admission,
            stop,
        }
    }

    pub(in crate::experimental_runtime) async fn join(self) -> ClientExit {
        if self.task.await.is_err() {
            fatal(&self.admission, &self.stop);
        }
        ClientExit {
            held: self.slot.take_exit(),
        }
    }
}

pub(in crate::experimental_runtime) struct ClientExit {
    held: Option<HeldCompletion>,
}

impl ClientExit {
    /// Node cleanup calls this only after both actual native-owner joins.
    pub(in crate::experimental_runtime) fn finish(mut self) {
        if let Some(held) = self.held.take() {
            held.finish_unknown();
        }
    }
}

struct WorkerGuard {
    admission: Arc<Admission>,
    stop: StopSignal,
    normal: bool,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.admission.close();
        if !self.normal {
            self.stop.request(NodeStopCause::ClientOwnerLost);
        }
    }
}

pub(super) async fn run(
    context: Context,
    mut receiver: mpsc::Receiver<Job>,
    slot: Arc<ActiveSlot>,
) {
    let mut guard = WorkerGuard {
        admission: Arc::clone(&context.admission),
        stop: context.stop.clone(),
        normal: false,
    };
    loop {
        let job = tokio::select! {
            biased;
            _ = context.stop.requested() => break,
            _ = context.admission.closed() => break,
            job = receiver.recv() => match job { Some(job) => job, None => break },
        };
        if matches!(context.process(job, &slot).await, Flow::Retire) {
            break;
        }
    }
    context.admission.close();
    receiver.close();
    while let Some(job) = receiver.recv().await {
        job.finish(Err(super::QueueWriteError::KnownRejected(
            super::QueueWriteRejection::Closed,
        )));
    }
    guard.normal = true;
}
