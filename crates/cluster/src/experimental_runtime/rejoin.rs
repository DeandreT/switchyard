use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use domain::CommittedStreamId;
use openraft::BasicNode;
use tokio::{
    runtime::Handle,
    sync::{oneshot, watch},
    task::JoinHandle,
};

use crate::ExperimentalReplicaStores;

use super::{
    Error, ExperimentalRaftCluster, ReplicaRuntimeError,
    continuity::RetirementEvidence,
    network::{PendingEndpoint, Routes},
    node::{Node, NodeRetirement},
};

mod validation;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod ready_tests;

#[cfg(test)]
mod cleanup_tests;

/// Admission did not consume the supplied storage pair or start an engine.
pub struct RejoinAdmissionError {
    reason: ReplicaRuntimeError,
    stores: Box<ExperimentalReplicaStores>,
}

impl RejoinAdmissionError {
    fn new(reason: Error, stores: ExperimentalReplicaStores) -> Self {
        Self {
            reason,
            stores: Box::new(stores),
        }
    }

    pub fn reason(&self) -> ReplicaRuntimeError {
        self.reason
    }

    pub fn into_stores(self) -> ExperimentalReplicaStores {
        *self.stores
    }
}

impl fmt::Debug for RejoinAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RejoinAdmissionError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for RejoinAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "replica rejoin admission was refused: {}",
            self.reason
        )
    }
}

impl std::error::Error for RejoinAdmissionError {}

enum Floor {
    Healthy(Arc<RetirementEvidence>),
    Unavailable,
}

enum Continuity {
    NoEngineStart,
    RetiredNewGeneration(Arc<RetirementEvidence>),
    Unavailable,
}

struct Completion {
    cleanup: Result<(), Error>,
    continuity: Continuity,
}

/// One admitted operation, plus at most one continuity override per fixed ID.
#[derive(Default)]
pub(super) struct RejoinState {
    active: Option<AttemptReceipt>,
    floors: BTreeMap<u64, Floor>,
    cleanup_failure: Option<Error>,
}

impl RejoinState {
    fn absorb(&mut self, node_id: u64, completion: &Completion) -> Result<(), Error> {
        match &completion.continuity {
            Continuity::NoEngineStart => {}
            Continuity::RetiredNewGeneration(evidence) => {
                self.floors
                    .insert(node_id, Floor::Healthy(evidence.clone()));
            }
            Continuity::Unavailable => {
                self.floors.insert(node_id, Floor::Unavailable);
            }
        }
        if let Err(error) = completion.cleanup {
            self.cleanup_failure.get_or_insert(error);
        }
        completion.cleanup
    }

    fn fold_completed(&mut self) -> Result<(), Error> {
        let Some(active) = &self.active else {
            return Ok(());
        };
        let completion = active.completed().ok_or(Error::RejoinInProgress)?;
        let node_id = active.node_id;
        self.active.take();
        // Cleanup failures remain part of shutdown's aggregate result. They
        // do not invalidate an independently healthy continuity baseline.
        let _ = self.absorb(node_id, &completion);
        Ok(())
    }

    pub(super) fn cancel_pending(&self) {
        if let Some(active) = &self.active {
            active.cancel();
        }
    }

    /// A concurrent public rejoin is impossible; a canceled waiter can leave
    /// one independently owned attempt that this barrier must still retire.
    pub(super) async fn stop_pending(&mut self, node_id: u64) -> Result<(), Error> {
        let Some(active) = self
            .active
            .as_ref()
            .filter(|active| active.node_id == node_id)
            .cloned()
        else {
            return Ok(());
        };
        active.cancel();
        let completion = active.join().await;
        self.active.take();
        self.absorb(node_id, &completion)
    }

    /// The caller must retire every running node before awaiting this method.
    /// This consuming future belongs in the owned whole-cluster supervisor.
    pub(super) async fn join_pending(mut self) -> Result<(), Error> {
        if let Some(active) = self.active.take() {
            active.cancel();
            let node_id = active.node_id;
            let completion = active.join().await;
            let _ = self.absorb(node_id, &completion);
        }
        self.cleanup_failure.map_or(Ok(()), Err)
    }
}

#[derive(Clone)]
struct AttemptReceipt {
    node_id: u64,
    shared: Arc<AttemptShared>,
    completion: watch::Receiver<Option<Arc<Completion>>>,
}

struct AttemptShared {
    canceled: AtomicBool,
}

struct CompletionPublisher {
    completion: watch::Sender<Option<Arc<Completion>>>,
}

impl AttemptReceipt {
    fn new(node_id: u64) -> (Self, CompletionPublisher) {
        let (completion, receiver) = watch::channel(None);
        let receipt = Self {
            node_id,
            shared: Arc::new(AttemptShared {
                canceled: AtomicBool::new(false),
            }),
            completion: receiver,
        };
        (receipt, CompletionPublisher { completion })
    }

    fn cancel(&self) {
        self.shared.canceled.store(true, Ordering::Release);
    }

    fn completed(&self) -> Option<Arc<Completion>> {
        self.completion.borrow().clone().or_else(|| {
            self.completion
                .has_changed()
                .is_err()
                .then(unavailable_completion)
        })
    }

    async fn join(mut self) -> Arc<Completion> {
        loop {
            if let Some(completion) = self.completion.borrow_and_update().clone() {
                return completion;
            }
            if self.completion.changed().await.is_err() {
                return unavailable_completion();
            }
        }
    }
}

impl AttemptShared {
    fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::Acquire)
    }
}

impl CompletionPublisher {
    fn complete(self, completion: Completion) {
        drop(self.completion.send_replace(Some(Arc::new(completion))));
    }
}

fn unavailable_completion() -> Arc<Completion> {
    Arc::new(Completion {
        cleanup: Err(Error::TaskFailed),
        continuity: Continuity::Unavailable,
    })
}

struct Policy {
    cleanup_runtime: Handle,
    routes: Arc<Routes>,
    members: BTreeMap<u64, BasicNode>,
    floor: Arc<RetirementEvidence>,
    stream: CommittedStreamId,
}

fn policy(
    cluster: &mut ExperimentalRaftCluster,
    node_id: u64,
    stores: &ExperimentalReplicaStores,
) -> Result<Policy, Error> {
    cluster.rejoin.fold_completed()?;
    let members = cluster.routes.fixed_members();
    if !members.contains_key(&node_id)
        || stores.progress().node_id() != node_id
        || stores.progress().stream() != cluster.stream
    {
        return Err(Error::ProfileMismatch);
    }
    if cluster.nodes.contains_key(&node_id) {
        return Err(Error::NodeRunning);
    }
    let floor = match cluster.rejoin.floors.get(&node_id) {
        Some(Floor::Healthy(evidence)) => evidence.clone(),
        Some(Floor::Unavailable) => return Err(Error::ContinuityUnavailable),
        None => {
            let retirement = cluster.retiring.get(&node_id).ok_or(Error::Closed)?;
            retirement.joined_evidence().map_err(|error| match error {
                Error::Closed => Error::RejoinInProgress,
                error => error,
            })?
        }
    };
    Ok(Policy {
        cleanup_runtime: cluster.runtime.clone(),
        routes: cluster.routes.clone(),
        members,
        floor,
        stream: cluster.stream,
    })
}

/// Synchronous policy checks do not read storage or consume rejected input.
/// An unpolled accepted future does not admit an attempt or start an engine.
pub(super) fn admit<'a>(
    cluster: &'a mut ExperimentalRaftCluster,
    node_id: u64,
    stores: ExperimentalReplicaStores,
) -> Result<impl Future<Output = Result<(), Error>> + Send + 'a, RejoinAdmissionError> {
    let policy = match policy(cluster, node_id, &stores) {
        Ok(policy) => policy,
        Err(error) => return Err(RejoinAdmissionError::new(error, stores)),
    };
    Ok(async move {
        let (receipt, completion) = AttemptReceipt::new(node_id);
        let (reply, receiver) = oneshot::channel();
        let runtime = Handle::try_current();
        let execution_runtime = runtime.as_ref().ok().cloned();
        let runtime = execution_runtime
            .clone()
            .unwrap_or_else(|| policy.cleanup_runtime.clone());
        let owner = AttemptOwner {
            resources: Some(Resources {
                stores: Some(stores),
                pending: None,
                starting: None,
                node: None,
                retirement: None,
                shutting_down: None,
                engine_possible: false,
                failure: None,
            }),
            shared: receipt.shared.clone(),
            completion: Some(completion),
            runtime: runtime.clone(),
            reply: Some(reply),
        };
        cluster.rejoin.active = Some(receipt.clone());
        drop(runtime.spawn(async move {
            if execution_runtime.is_some() {
                owner.run(node_id, policy).await;
            } else {
                owner.fail(Error::RuntimeUnavailable).await;
            }
        }));
        let (result, mut waiting) = PublicationWait {
            guard: Some(WaitGuard {
                shared: receipt.shared.clone(),
                armed: true,
            }),
            receiver,
        }
        .receive()
        .await;
        let error = match result {
            Ok(Ok(ready)) => match ready.publish(cluster, node_id) {
                Ok(()) => {
                    waiting.disarm();
                    return Ok(());
                }
                Err(error) => error,
            },
            Ok(Err(error)) => error,
            Err(_) => Error::TaskFailed,
        };
        receipt.cancel();
        let completion = receipt.join().await;
        cluster.rejoin.active.take();
        let cleanup = cluster.rejoin.absorb(node_id, &completion);
        waiting.disarm();
        cleanup?;
        Err(error)
    })
}

struct WaitGuard {
    shared: Arc<AttemptShared>,
    armed: bool,
}

impl WaitGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        if self.armed {
            self.shared.canceled.store(true, Ordering::Release);
        }
    }
}

struct PublicationWait {
    guard: Option<WaitGuard>,
    receiver: oneshot::Receiver<Result<GuardedReadyNode, Error>>,
}

impl PublicationWait {
    async fn receive(
        mut self,
    ) -> (
        Result<Result<GuardedReadyNode, Error>, oneshot::error::RecvError>,
        WaitGuard,
    ) {
        let result = (&mut self.receiver).await;
        (result, self.guard.take().expect("owned publication waiter"))
    }
}

impl Drop for PublicationWait {
    fn drop(&mut self) {
        // Cancellation precedes destruction of an already-ready channel value.
        drop(self.guard.take());
    }
}

struct Resources {
    stores: Option<ExperimentalReplicaStores>,
    pending: Option<PendingEndpoint>,
    starting: Option<JoinHandle<Result<Node, Error>>>,
    node: Option<Node>,
    retirement: Option<NodeRetirement>,
    shutting_down: Option<JoinHandle<Result<(), Error>>>,
    engine_possible: bool,
    failure: Option<Error>,
}

struct AttemptOwner {
    resources: Option<Resources>,
    shared: Arc<AttemptShared>,
    completion: Option<CompletionPublisher>,
    runtime: Handle,
    reply: Option<oneshot::Sender<Result<GuardedReadyNode, Error>>>,
}

impl AttemptOwner {
    fn resources(&mut self) -> &mut Resources {
        self.resources.as_mut().expect("owned rejoin resources")
    }

    async fn run(mut self, node_id: u64, policy: Policy) {
        if self.shared.is_canceled() {
            self.fail(Error::Closed).await;
            return;
        }
        let result = validation::validate(
            self.resources()
                .stores
                .as_mut()
                .expect("unstarted rejoin stores"),
            node_id,
            policy.stream,
            &policy.members,
            &policy.floor,
        )
        .await;
        if let Err(error) = result {
            self.fail(error).await;
            return;
        }
        if self.shared.is_canceled() {
            self.fail(Error::Closed).await;
            return;
        }
        let pending = match policy.routes.begin_node(node_id) {
            Ok(pending) => pending,
            Err(error) => {
                self.fail(error).await;
                return;
            }
        };
        self.resources().pending = Some(pending);
        // Conservatively fence continuity once unique stores enter startup.
        // A failure here cannot silently revive the previous voter baseline.
        self.resources().engine_possible = true;
        let stores = self
            .resources()
            .stores
            .take()
            .expect("rejoin store handoff");
        let pending = self
            .resources()
            .pending
            .take()
            .expect("rejoin route handoff");
        self.resources().starting = Some(self.runtime.spawn(Node::start(
            stores,
            pending,
            policy.members,
        )));
        let result = self
            .resources()
            .starting
            .as_mut()
            .expect("owned rejoin startup")
            .await;
        self.resources().starting.take();
        match result {
            Ok(Ok(node)) => self.resources().node = Some(node),
            Ok(Err(error)) => {
                if error == Error::OwnerFailure {
                    self.resources().failure = Some(error);
                }
                self.fail(error).await;
                return;
            }
            Err(_) => {
                self.resources().failure = Some(Error::TaskFailed);
                self.fail(Error::TaskFailed).await;
                return;
            }
        }
        if self.shared.is_canceled() {
            self.fail(Error::Closed).await;
            return;
        }
        if let Err(error) = self
            .resources()
            .node
            .as_ref()
            .expect("started rejoin node")
            .activate()
            .await
        {
            self.fail(error).await;
            return;
        }
        if self.shared.is_canceled() {
            self.fail(Error::Closed).await;
            return;
        }
        let ready = GuardedReadyNode {
            node: self.resources().node.take(),
            shared: self.shared.clone(),
            runtime: self.runtime.clone(),
            completion: self.completion.take(),
        };
        self.resources.take();
        if let Some(reply) = self.reply.take() {
            // Receiver loss drops the guarded node, not just a raw Node value.
            let _ = reply.send(Ok(ready));
        }
    }

    async fn fail(mut self, error: Error) {
        let cleanup = RetirementCleanup {
            resources: self.resources.take(),
            completion: self.completion.take(),
            runtime: self.runtime.clone(),
            reply: self.reply.take(),
            error,
            polled: false,
        };
        cleanup.run().await;
    }
}

impl Drop for AttemptOwner {
    fn drop(&mut self) {
        if let Some(resources) = self.resources.take() {
            if let Some(node) = &resources.node {
                node.request_stop();
            }
            let cleanup = RetirementCleanup {
                resources: Some(resources),
                completion: self.completion.take(),
                runtime: self.runtime.clone(),
                reply: self.reply.take(),
                error: Error::TaskFailed,
                polled: false,
            };
            drop(self.runtime.spawn(cleanup.run()));
        }
    }
}

struct GuardedReadyNode {
    node: Option<Node>,
    shared: Arc<AttemptShared>,
    runtime: Handle,
    completion: Option<CompletionPublisher>,
}

impl GuardedReadyNode {
    fn publish(mut self, cluster: &mut ExperimentalRaftCluster, node_id: u64) -> Result<(), Error> {
        if cluster.nodes.contains_key(&node_id) || self.shared.is_canceled() {
            return Err(Error::Closed);
        }
        // No await or user callback separates ownership transfer and map entry.
        cluster
            .nodes
            .insert(node_id, self.node.take().expect("guarded ready node"));
        cluster.retiring.remove(&node_id);
        cluster.rejoin.floors.remove(&node_id);
        cluster.rejoin.active.take();
        Ok(())
    }
}

impl Drop for GuardedReadyNode {
    fn drop(&mut self) {
        if let Some(node) = self.node.take() {
            let retirement = node.retire();
            let cleanup = RetirementCleanup {
                resources: Some(Resources {
                    stores: None,
                    pending: None,
                    starting: None,
                    node: None,
                    retirement: Some(retirement),
                    shutting_down: None,
                    engine_possible: true,
                    failure: None,
                }),
                completion: self.completion.take(),
                runtime: self.runtime.clone(),
                reply: None,
                error: Error::Closed,
                polled: false,
            };
            drop(self.runtime.spawn(cleanup.run()));
        }
    }
}

async fn join_retirement(retirement: NodeRetirement) -> Completion {
    match retirement.clone().join().await {
        Ok(()) => match retirement.joined_evidence() {
            Ok(evidence) => Completion {
                cleanup: Ok(()),
                continuity: Continuity::RetiredNewGeneration(evidence),
            },
            Err(error) => Completion {
                cleanup: Err(error),
                continuity: Continuity::Unavailable,
            },
        },
        Err(error) => Completion {
            cleanup: Err(error),
            continuity: Continuity::Unavailable,
        },
    }
}

struct RetirementCleanup {
    resources: Option<Resources>,
    completion: Option<CompletionPublisher>,
    runtime: Handle,
    reply: Option<oneshot::Sender<Result<GuardedReadyNode, Error>>>,
    error: Error,
    polled: bool,
}

impl RetirementCleanup {
    fn resources(&mut self) -> &mut Resources {
        self.resources.as_mut().expect("owned retirement resources")
    }

    fn run(self) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(self.run_inner())
    }

    async fn run_inner(mut self) {
        self.polled = true;
        drop(self.resources().pending.take());
        if let Some(starting) = self.resources().starting.as_mut() {
            let result = starting.await;
            self.resources().starting.take();
            match result {
                Ok(Ok(node)) => self.resources().node = Some(node),
                Ok(Err(Error::OwnerFailure)) => {
                    self.resources().failure.get_or_insert(Error::OwnerFailure);
                }
                Err(_) => {
                    self.resources().failure.get_or_insert(Error::TaskFailed);
                }
                Ok(Err(_)) => {}
            }
        }
        if let Some(node) = self.resources().node.take() {
            self.resources().retirement = Some(node.retire());
        }
        let retirement = self.resources().retirement.clone();
        let mut completion = if let Some(retirement) = retirement {
            // Keep the original notice in this guard throughout the await.
            join_retirement(retirement).await
        } else {
            if let Some(stores) = self.resources().stores.take() {
                self.resources().shutting_down = Some(self.runtime.spawn(async move {
                    stores.shutdown().await.map_err(|_| Error::OwnerFailure)
                }));
            }
            if let Some(shutdown) = self.resources().shutting_down.as_mut() {
                let result = shutdown.await;
                self.resources().shutting_down.take();
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        self.resources().failure.get_or_insert(error);
                    }
                    Err(_) => {
                        self.resources().failure.get_or_insert(Error::TaskFailed);
                    }
                }
            }
            Completion {
                cleanup: Ok(()),
                continuity: if self.resources().engine_possible {
                    Continuity::Unavailable
                } else {
                    Continuity::NoEngineStart
                },
            }
        };
        if let Some(error) = self.resources().failure {
            completion.cleanup = Err(error);
        }
        // Every native join obligation is finished before publication.
        self.resources.take();
        let error = completion.cleanup.err().unwrap_or(self.error);
        if let Some(publisher) = self.completion.take() {
            publisher.complete(completion);
        }
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(Err(error));
        }
    }
}

impl Drop for RetirementCleanup {
    fn drop(&mut self) {
        if let Some(mut resources) = self.resources.take() {
            resources.failure.get_or_insert(Error::TaskFailed);
            if let Some(node) = &resources.node {
                node.request_stop();
            }
            if !self.polled {
                // A closed runtime can drop a newly spawned future inline.
                // Never recursively respawn an unpolled rescue or publish a
                // joined continuity claim without actual owner completion.
                drop(resources);
                return;
            }
            // Awaitables remain owned fields until their actual completion;
            // unwind transfers them, rather than dropping a taken join future.
            let cleanup = Self {
                resources: Some(resources),
                completion: self.completion.take(),
                runtime: self.runtime.clone(),
                reply: self.reply.take(),
                error: Error::TaskFailed,
                polled: false,
            };
            drop(self.runtime.spawn(cleanup.run()));
        }
    }
}
