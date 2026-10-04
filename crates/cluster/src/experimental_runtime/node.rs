use std::{collections::BTreeMap, fmt};

use openraft::{BasicNode, RaftMetrics};
use tokio::sync::{mpsc, oneshot, watch};

use crate::ExperimentalReplicaStores;

use super::{
    Error,
    client::ExperimentalRaftHandle,
    continuity::EvidenceSlot,
    network::{NodeGeneration, PendingEndpoint},
};

mod lifecycle;
mod retirement;
mod stop;

pub(super) use retirement::NodeRetirement;
pub(super) use stop::{NodeStopCause, StopSignal};

#[cfg(test)]
mod tests;

pub(super) enum AdminRequest {
    Initialize(oneshot::Sender<Result<(), Error>>),
    Activate(oneshot::Sender<Result<(), Error>>),
}

pub(super) struct Node {
    client: ExperimentalRaftHandle,
    generation: NodeGeneration,
    metrics: watch::Receiver<RaftMetrics<u64, BasicNode>>,
    admin: mpsc::Sender<AdminRequest>,
    stop: StopSignal,
    completed: watch::Receiver<Option<Result<(), Error>>>,
    evidence: EvidenceSlot,
}

impl Node {
    pub(super) async fn start(
        prepared: ExperimentalReplicaStores,
        pending: PendingEndpoint,
        members: BTreeMap<u64, BasicNode>,
    ) -> Result<Self, Error> {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| Error::RuntimeUnavailable)?;
        let (finished, mut completed) = watch::channel(None);
        let supervisor_runtime = runtime.clone();
        let supervisor = runtime.spawn(lifecycle::start(
            prepared,
            pending,
            members,
            supervisor_runtime,
            finished,
            completed.clone(),
        ));
        match supervisor.await {
            Ok(result) => result,
            Err(_) => {
                // An unwind guard transfers cleanup before this supervisor
                // finishes. Do not publish startup loss ahead of its joins.
                wait_completed(&mut completed).await?;
                Err(Error::TaskFailed)
            }
        }
    }

    pub(super) fn client(&self) -> ExperimentalRaftHandle {
        self.client.clone()
    }

    pub(super) fn leader_hint(&self) -> Option<u64> {
        if self.stop.is_requested() || !self.generation.is_live() {
            None
        } else {
            self.metrics.borrow().current_leader
        }
    }

    pub(super) async fn initialize_fixed_membership(&self) -> Result<(), Error> {
        self.admin_request(AdminRequest::Initialize).await
    }

    pub(super) async fn activate(&self) -> Result<(), Error> {
        self.admin_request(AdminRequest::Activate).await
    }

    async fn admin_request(
        &self,
        request: fn(oneshot::Sender<Result<(), Error>>) -> AdminRequest,
    ) -> Result<(), Error> {
        if self.stop.is_requested() || !self.generation.is_live() {
            return Err(Error::Closed);
        }
        let (reply, receiver) = oneshot::channel();
        self.admin
            .send(request(reply))
            .await
            .map_err(|_| Error::Closed)?;
        receiver.await.map_err(|_| Error::TaskFailed)?
    }

    pub(super) fn request_stop(&self) {
        self.stop.request(NodeStopCause::Shutdown);
        self.client.close_admission();
        self.generation.retire();
    }

    pub(super) fn retire(self) -> NodeRetirement {
        self.request_stop();
        NodeRetirement::with_evidence(self.completed.clone(), self.evidence.clone())
    }

    #[cfg(test)]
    pub(super) async fn shutdown(self) -> Result<(), Error> {
        self.retire().join().await
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.request_stop();
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Node").finish_non_exhaustive()
    }
}

async fn wait_completed(
    completed: &mut watch::Receiver<Option<Result<(), Error>>>,
) -> Result<(), Error> {
    loop {
        if let Some(result) = *completed.borrow_and_update() {
            return result;
        }
        completed.changed().await.map_err(|_| Error::TaskFailed)?;
    }
}
