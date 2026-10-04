use std::{fmt, sync::Arc};

use openraft::Raft;
use tokio::sync::{mpsc, oneshot};

use crate::{LogTypes, ReadOnlyLogReader, experimental_state_machine::HealthyCheckpointReader};

use super::{
    network::NodeGeneration,
    node::{NodeStopCause, StopSignal},
};

mod budget;
mod completion;
mod intent;
mod owner;
mod result;
mod write;

pub use budget::ClientWorkload;
pub use intent::QueueIntent;
pub(super) use owner::ClientOwner;
pub use result::{
    QueueWriteError, QueueWriteOutcome, QueueWriteRejection, QueueWriteResult, QueueWriteUnknown,
};

pub const MAX_CLIENT_JOBS: usize = 16;
pub const MAX_CLIENT_BYTES: usize = 4 * 1024 * 1024;
pub(super) const MAX_STAMP_AHEAD_MILLIS: u64 = 500;

type Response = Result<QueueWriteResult, QueueWriteError>;
type PublishedResponse = (Response, oneshot::Receiver<()>);

/// A bounded client ingress for one exact node generation, not a Raft handle.
#[derive(Clone)]
pub struct ExperimentalRaftHandle {
    sender: mpsc::Sender<completion::Job>,
    admission: Arc<budget::Admission>,
    generation: NodeGeneration,
    stop: StopSignal,
}

impl ExperimentalRaftHandle {
    /// First poll admits the intent. After admission, caller loss does not
    /// cancel it, retry it, or release its count and byte charge early.
    pub fn submit(
        &self,
        intent: QueueIntent,
    ) -> impl std::future::Future<Output = Response> + Send + 'static + use<> {
        let handle = self.clone();
        async move {
            if !handle.generation.is_live() || handle.stop.is_requested() {
                return Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed));
            }
            let lease = handle.admission.acquire(intent.encoded_bytes())?;
            let (reply, receiver) = oneshot::channel();
            let job = completion::Job::new(intent, reply, lease);
            if let Err(error) = handle.sender.try_send(job) {
                error
                    .into_inner()
                    .finish(Err(QueueWriteError::KnownRejected(
                        QueueWriteRejection::Closed,
                    )));
            }
            let (result, released) = receiver
                .await
                .map_err(|_| QueueWriteError::Unknown(QueueWriteUnknown::ResponseUnavailable))?;
            released
                .await
                .map_err(|_| QueueWriteError::Unknown(QueueWriteUnknown::ResponseUnavailable))?;
            result
        }
    }

    pub fn workload(&self) -> ClientWorkload {
        self.admission.workload()
    }

    pub(super) fn close_admission(&self) {
        self.admission.close();
    }
}

impl fmt::Debug for ExperimentalRaftHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExperimentalRaftHandle")
            .finish_non_exhaustive()
    }
}

pub(super) fn start(
    raft: Raft<LogTypes>,
    checkpoint: HealthyCheckpointReader,
    log: ReadOnlyLogReader,
    generation: NodeGeneration,
    stop: StopSignal,
    runtime: tokio::runtime::Handle,
) -> (ExperimentalRaftHandle, ClientOwner) {
    let (sender, receiver) = mpsc::channel(MAX_CLIENT_JOBS);
    let admission = Arc::new(budget::Admission::default());
    let slot = Arc::new(completion::ActiveSlot::default());
    let handle = ExperimentalRaftHandle {
        sender,
        admission: Arc::clone(&admission),
        generation: generation.clone(),
        stop: stop.clone(),
    };
    let context = write::Context {
        raft,
        checkpoint,
        log,
        generation,
        stop: stop.clone(),
        admission: Arc::clone(&admission),
    };
    let task = runtime.spawn(owner::run(context, receiver, Arc::clone(&slot)));
    (handle, ClientOwner::new(task, slot, admission, stop))
}

fn fatal(admission: &budget::Admission, stop: &StopSignal) {
    admission.close();
    stop.request(NodeStopCause::ClientOwnerLost);
}

#[cfg(test)]
mod tests;
