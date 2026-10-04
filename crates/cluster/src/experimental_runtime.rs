//! Actual fixed-three-node in-process experiment, not production activation.
//!
//! Admission bounds apply to owned client and RPC requests. The pinned engine's
//! internal queues are not a certified process-wide memory bound.

use std::{collections::BTreeMap, fmt};

use domain::CommittedStreamId;

use crate::ExperimentalReplicaStores;

mod client;
mod continuity;
mod network;
mod node;
mod rejoin;
mod startup;

#[cfg(test)]
mod partition_tests;

pub use client::{
    ClientWorkload, ExperimentalRaftHandle, QueueIntent, QueueWriteError, QueueWriteOutcome,
    QueueWriteRejection, QueueWriteResult, QueueWriteUnknown,
};
pub use rejoin::RejoinAdmissionError;

pub(crate) type Error = ReplicaRuntimeError;

/// Queued and running owned RPCs, not the pinned engine's internal queues.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportWorkload {
    pub accepted_jobs: usize,
    pub encoded_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReplicaRuntimeError {
    #[error("the fixed replica identities or streams do not match")]
    ProfileMismatch,
    #[error("the retained membership is not the fixed three-voter membership")]
    MembershipMismatch,
    #[error("replica history failed runtime validation")]
    InvalidHistory,
    #[error("creating a cluster requires three pristine storage pairs")]
    NotPristine,
    #[error("opening a cluster requires an existing initialized membership")]
    NotInitialized,
    #[error("the finite retained storage lacks startup headroom")]
    Headroom,
    #[error("replica runtime storage failed")]
    Storage,
    #[error("a replica runtime owner failed")]
    OwnerFailure,
    #[error("the replica runtime is closed")]
    Closed,
    #[error("the replica runtime requires a live Tokio runtime")]
    RuntimeUnavailable,
    #[error("the replica runtime task failed")]
    TaskFailed,
    #[error("the fixed replica runtime configuration is invalid")]
    Configuration,
    #[error("the fixed replica initialization failed")]
    Initialization,
    #[error("the replica coordination task failed")]
    CoreFailure,
    #[error("the fixed replica node is already running")]
    NodeRunning,
    #[error("a replica rejoin or its prior retirement is still in progress")]
    RejoinInProgress,
    #[error("the replica has no healthy joined continuity record")]
    ContinuityUnavailable,
}

/// Three real nodes with private storage, transport, and writable engine handles.
/// No snapshot, purge, arbitrary membership change, or raw engine is exposed.
///
/// ```compile_fail
/// fn raw_engine(cluster: &cluster::ExperimentalRaftCluster) {
///     let raft = cluster.raft();
/// }
/// ```
///
/// ```compile_fail
/// fn mutable_engine(handle: &cluster::ExperimentalRaftHandle) {
///     handle.change_membership([1, 2, 3]);
/// }
/// ```
pub struct ExperimentalRaftCluster {
    nodes: BTreeMap<u64, node::Node>,
    retiring: BTreeMap<u64, node::NodeRetirement>,
    rejoin: rejoin::RejoinState,
    runtime: tokio::runtime::Handle,
    routes: std::sync::Arc<network::Routes>,
    stream: CommittedStreamId,
}

impl ExperimentalRaftCluster {
    /// Initialize one fixed membership only after all three endpoints exist.
    /// An unpolled future performs no engine startup. Accepted startup survives
    /// caller loss while the captured Tokio runtime remains live.
    pub async fn create(stores: [ExperimentalReplicaStores; 3]) -> Result<Self, Error> {
        startup::start(stores, startup::Mode::Create).await
    }

    /// Recover existing fixed membership without initialization or repair.
    pub async fn open(stores: [ExperimentalReplicaStores; 3]) -> Result<Self, Error> {
        startup::start(stores, startup::Mode::Open).await
    }

    pub fn stream(&self) -> CommittedStreamId {
        self.stream
    }

    pub fn transport_workload(&self) -> Result<TransportWorkload, Error> {
        let workload = self.routes.workload()?;
        Ok(TransportWorkload {
            accepted_jobs: workload.accepted_jobs,
            encoded_bytes: workload.encoded_bytes,
        })
    }

    pub fn node_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.nodes.keys().copied()
    }

    pub fn handle(&self, node_id: u64) -> Option<ExperimentalRaftHandle> {
        self.nodes.get(&node_id).map(node::Node::client)
    }

    /// A routing observation only. The write owner separately proves leadership
    /// and local application before stamping a command.
    pub fn leader_hint(&self) -> Option<u64> {
        self.nodes
            .values()
            .filter_map(node::Node::leader_hint)
            .find(|id| self.nodes.contains_key(id))
    }

    /// Rejoin an original fixed voter using its exact healthy retired history.
    /// Factory refusal returns the untouched pair. An unpolled accepted future
    /// admits no attempt; after first poll, caller loss retains owned cleanup.
    /// Membership is never initialized again, and stale handles stay closed.
    pub fn rejoin_node(
        &mut self,
        node_id: u64,
        stores: ExperimentalReplicaStores,
    ) -> Result<
        impl std::future::Future<Output = Result<(), Error>> + Send + '_,
        RejoinAdmissionError,
    > {
        rejoin::admit(self, node_id, stores)
    }

    /// Stop one node and wait for its coordination and native storage owners.
    /// Repeated calls observe the same result; losing a waiter does not remove
    /// this node from the whole-cluster shutdown barrier.
    pub async fn stop_node(&mut self, node_id: u64) -> Result<(), Error> {
        if let Some(node) = self.nodes.remove(&node_id) {
            self.retiring.insert(node_id, node.retire());
        }
        let rejoining = self.rejoin.stop_pending(node_id).await;
        let stopping = self
            .retiring
            .get(&node_id)
            .ok_or(Error::Closed)?
            .clone()
            .join()
            .await;
        rejoining.and(stopping)
    }

    /// Start every node's shutdown before awaiting any node's drainage.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| Error::RuntimeUnavailable)?;
        self.rejoin.cancel_pending();
        for (id, node) in std::mem::take(&mut self.nodes) {
            self.retiring.insert(id, node.retire());
        }
        let retiring = std::mem::take(&mut self.retiring);
        let rejoining = std::mem::take(&mut self.rejoin);
        let task = runtime.spawn(async move {
            let (stopping, rejoining) = tokio::join!(
                startup::join_retirements(retiring),
                rejoining.join_pending(),
            );
            stopping.and(rejoining)
        });
        task.await.map_err(|_| Error::TaskFailed)?
    }
}

impl fmt::Debug for ExperimentalRaftCluster {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExperimentalRaftCluster")
            .finish_non_exhaustive()
    }
}
