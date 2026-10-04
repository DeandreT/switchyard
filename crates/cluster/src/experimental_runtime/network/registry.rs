use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use domain::CommittedStreamId;
use openraft::BasicNode;
use tokio::sync::Notify;

use super::super::{Error, node::StopSignal};
use super::NetworkFactory;
use super::admission::{Lease, RpcJob};
use super::supervisor::EndpointOwner;
use super::{MAX_RPC_BYTES, MAX_RPC_JOBS, MAX_TARGET_RPC_BYTES, RpcCause, RpcWorkload};
use crate::LogTypes;

pub(in crate::experimental_runtime) fn stable_label(node_id: u64) -> String {
    format!("switchyard-in-process-{node_id}")
}

#[derive(Clone)]
pub(in crate::experimental_runtime) struct NodeGeneration(Arc<GenerationState>);

struct GenerationState {
    id: u64,
    stream: CommittedStreamId,
    live: AtomicBool,
}

impl NodeGeneration {
    pub(in crate::experimental_runtime) fn retire(&self) {
        self.0.live.store(false, Ordering::Release);
    }

    pub(in crate::experimental_runtime) fn is_live(&self) -> bool {
        self.0.live.load(Ordering::Acquire)
    }

    pub(in crate::experimental_runtime) fn same_generation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(super) fn id(&self) -> u64 {
        self.0.id
    }
}

pub(in crate::experimental_runtime) struct Routes {
    stream: CommittedStreamId,
    members: BTreeMap<u64, BasicNode>,
    state: Mutex<RouteState>,
    changed: Notify,
}

struct RouteState {
    slots: BTreeMap<u64, Slot>,
    workload: RpcWorkload,
}

struct Slot {
    generation: Weak<GenerationState>,
    endpoint: Weak<Endpoint>,
    jobs: usize,
    bytes: usize,
}

pub(super) struct Endpoint {
    pub(super) generation: NodeGeneration,
    pub(super) sender: flume::Sender<RpcJob>,
}

pub(in crate::experimental_runtime) struct PendingEndpoint {
    routes: Arc<Routes>,
    endpoint: Arc<Endpoint>,
    receiver: Option<flume::Receiver<RpcJob>>,
    armed: bool,
}

impl Routes {
    pub(in crate::experimental_runtime) fn fixed_members(&self) -> BTreeMap<u64, BasicNode> {
        self.members.clone()
    }

    pub(in crate::experimental_runtime) fn new(
        stream: CommittedStreamId,
        ids: [u64; 3],
    ) -> Result<Arc<Self>, Error> {
        let members = ids
            .into_iter()
            .map(|id| (id, BasicNode::new(stable_label(id))))
            .collect::<BTreeMap<_, _>>();
        if members.len() != 3 {
            return Err(Error::ProfileMismatch);
        }
        let slots = members
            .keys()
            .map(|&id| {
                (
                    id,
                    Slot {
                        generation: Weak::new(),
                        endpoint: Weak::new(),
                        jobs: 0,
                        bytes: 0,
                    },
                )
            })
            .collect();
        Ok(Arc::new(Self {
            stream,
            members,
            state: Mutex::new(RouteState {
                slots,
                workload: RpcWorkload::default(),
            }),
            changed: Notify::new(),
        }))
    }

    pub(in crate::experimental_runtime) fn begin_node(
        self: &Arc<Self>,
        id: u64,
    ) -> Result<PendingEndpoint, Error> {
        let (sender, receiver) = flume::bounded(MAX_RPC_JOBS);
        let generation = NodeGeneration(Arc::new(GenerationState {
            id,
            stream: self.stream,
            live: AtomicBool::new(true),
        }));
        let endpoint = Arc::new(Endpoint {
            generation: generation.clone(),
            sender,
        });
        let mut state = self.state.lock().map_err(|_| Error::OwnerFailure)?;
        let slot = state.slots.get_mut(&id).ok_or(Error::ProfileMismatch)?;
        if slot
            .generation
            .upgrade()
            .is_some_and(|old| old.live.load(Ordering::Acquire))
            || slot.jobs != 0
        {
            return Err(Error::Closed);
        }
        slot.generation = Arc::downgrade(&generation.0);
        slot.endpoint = Weak::new();
        drop(state);
        Ok(PendingEndpoint {
            routes: self.clone(),
            endpoint,
            receiver: Some(receiver),
            armed: true,
        })
    }

    pub(super) fn matches_membership(
        &self,
        membership: &openraft::Membership<u64, BasicNode>,
    ) -> bool {
        membership
            == &openraft::Membership::new(
                vec![self.members.keys().copied().collect()],
                self.members.clone(),
            )
    }

    pub(super) fn admit(
        self: &Arc<Self>,
        source: &NodeGeneration,
        target: u64,
        label: &str,
        bytes: usize,
        make: impl FnOnce(Lease) -> RpcJob,
    ) -> Result<(), RpcCause> {
        let mut state = self.state.lock().map_err(|_| RpcCause::Unavailable)?;
        if source.0.stream != self.stream || !source.is_live() {
            return Err(RpcCause::Unavailable);
        }
        let source_slot = state.slots.get(&source.id()).ok_or(RpcCause::Unavailable)?;
        if !source_slot
            .generation
            .upgrade()
            .is_some_and(|generation| NodeGeneration(generation).same_generation(source))
        {
            return Err(RpcCause::Unavailable);
        }
        if self
            .members
            .get(&target)
            .is_none_or(|node| node.addr != label)
        {
            return Err(RpcCause::Unavailable);
        }
        let endpoint = state
            .slots
            .get(&target)
            .and_then(|slot| slot.endpoint.upgrade())
            .ok_or(RpcCause::Unavailable)?;
        if !endpoint.generation.is_live() {
            return Err(RpcCause::Unavailable);
        }
        let target_bytes = state.slots.get(&target).ok_or(RpcCause::Unavailable)?.bytes;
        let jobs = state
            .workload
            .accepted_jobs
            .checked_add(1)
            .ok_or(RpcCause::Busy)?;
        let total = state
            .workload
            .encoded_bytes
            .checked_add(bytes)
            .ok_or(RpcCause::Busy)?;
        let target_total = target_bytes.checked_add(bytes).ok_or(RpcCause::Busy)?;
        if jobs > MAX_RPC_JOBS || total > MAX_RPC_BYTES || target_total > MAX_TARGET_RPC_BYTES {
            return Err(RpcCause::Busy);
        }
        state.workload = RpcWorkload {
            accepted_jobs: jobs,
            encoded_bytes: total,
        };
        if let Some(slot) = state.slots.get_mut(&target) {
            slot.jobs += 1;
            slot.bytes = target_total;
        }
        let lease = Lease::new(self.clone(), endpoint.generation.clone(), bytes);
        let job = make(lease);
        // The admitted count also covers publication. A closing worker cannot
        // finish while this packet is between reservation and queue insertion.
        drop(state);
        let result = endpoint.sender.try_send(job);
        self.changed.notify_waiters();
        result.map_err(|failure| {
            failure.into_inner().refuse(RpcCause::Unavailable);
            RpcCause::Unavailable
        })
    }

    pub(super) fn refund(&self, generation: &NodeGeneration, bytes: usize) {
        if let Ok(mut state) = self.state.lock() {
            state.workload.accepted_jobs = state.workload.accepted_jobs.saturating_sub(1);
            state.workload.encoded_bytes = state.workload.encoded_bytes.saturating_sub(bytes);
            if let Some(slot) = state.slots.get_mut(&generation.id())
                && slot
                    .generation
                    .upgrade()
                    .is_some_and(|current| Arc::ptr_eq(&current, &generation.0))
            {
                slot.jobs = slot.jobs.saturating_sub(1);
                slot.bytes = slot.bytes.saturating_sub(bytes);
            }
        }
        self.changed.notify_waiters();
    }

    pub(in crate::experimental_runtime) fn workload(&self) -> Result<RpcWorkload, Error> {
        self.state
            .lock()
            .map(|state| state.workload)
            .map_err(|_| Error::OwnerFailure)
    }

    pub(super) fn target_jobs(&self, generation: &NodeGeneration) -> Result<usize, Error> {
        let state = self.state.lock().map_err(|_| Error::OwnerFailure)?;
        let slot = state.slots.get(&generation.id()).ok_or(Error::Closed)?;
        if !slot
            .generation
            .upgrade()
            .is_some_and(|current| Arc::ptr_eq(&current, &generation.0))
        {
            return Err(Error::Closed);
        }
        Ok(slot.jobs)
    }

    pub(super) fn changed(&self) -> &Notify {
        &self.changed
    }

    fn publish(&self, endpoint: &Arc<Endpoint>) -> Result<(), Error> {
        let mut state = self.state.lock().map_err(|_| Error::OwnerFailure)?;
        let slot = state
            .slots
            .get_mut(&endpoint.generation.id())
            .ok_or(Error::ProfileMismatch)?;
        if !endpoint.generation.is_live()
            || !slot
                .generation
                .upgrade()
                .is_some_and(|generation| Arc::ptr_eq(&generation, &endpoint.generation.0))
        {
            return Err(Error::Closed);
        }
        slot.endpoint = Arc::downgrade(endpoint);
        Ok(())
    }

    pub(super) fn close(&self, generation: &NodeGeneration) {
        match self.state.lock() {
            Ok(mut state) => {
                generation.0.live.store(false, Ordering::Release);
                if let Some(slot) = state.slots.get_mut(&generation.id())
                    && slot
                        .generation
                        .upgrade()
                        .is_some_and(|current| Arc::ptr_eq(&current, &generation.0))
                {
                    slot.endpoint = Weak::new();
                }
            }
            Err(_) => {
                generation.0.live.store(false, Ordering::Release);
            }
        }
        self.changed.notify_waiters();
    }
}

impl PendingEndpoint {
    pub(in crate::experimental_runtime) fn factory(&self) -> NetworkFactory {
        NetworkFactory::new(Arc::downgrade(&self.routes), self.generation())
    }

    pub(in crate::experimental_runtime) fn generation(&self) -> NodeGeneration {
        self.endpoint.generation.clone()
    }

    pub(in crate::experimental_runtime) fn attach(
        mut self,
        raft: openraft::Raft<LogTypes>,
        runtime: tokio::runtime::Handle,
        stop: StopSignal,
    ) -> Result<EndpointOwner, Error> {
        let receiver = self.receiver.take().ok_or(Error::Closed)?;
        let owner = EndpointOwner::start(
            self.routes.clone(),
            self.endpoint.clone(),
            receiver,
            raft,
            runtime,
            stop,
        );
        self.routes.publish(&self.endpoint)?;
        self.armed = false;
        Ok(owner)
    }

    #[cfg(test)]
    pub(super) fn test_parts(&mut self) -> (Arc<Endpoint>, flume::Receiver<RpcJob>) {
        self.routes
            .publish(&self.endpoint)
            .expect("publish exact test endpoint");
        (
            self.endpoint.clone(),
            self.receiver.take().expect("unique test receiver"),
        )
    }
}

impl Drop for PendingEndpoint {
    fn drop(&mut self) {
        if self.armed {
            self.routes.close(&self.endpoint.generation);
        }
    }
}

#[cfg(test)]
mod tests;
