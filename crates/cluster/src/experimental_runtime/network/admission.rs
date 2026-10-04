use std::sync::Arc;

use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use tokio::sync::oneshot;

use super::RpcCause;
use super::registry::{NodeGeneration, Routes};
use crate::LogTypes;

pub(super) struct Lease {
    routes: Arc<Routes>,
    generation: NodeGeneration,
    bytes: usize,
}

impl Lease {
    pub(super) fn new(routes: Arc<Routes>, generation: NodeGeneration, bytes: usize) -> Self {
        Self {
            routes,
            generation,
            bytes,
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.routes.refund(&self.generation, self.bytes);
    }
}

pub(super) enum RpcPayload {
    Append(AppendEntriesRequest<LogTypes>),
    Vote(VoteRequest<u64>),
}

pub(super) enum RpcResponse {
    Append(AppendEntriesResponse<u64>),
    Vote(VoteResponse<u64>),
}

pub(super) type Reply = oneshot::Sender<Result<RpcResponse, RpcCause>>;

pub(super) struct RpcJob {
    pub(super) payload: RpcPayload,
    pub(super) reply: Reply,
    pub(super) lease: Lease,
}

impl RpcJob {
    pub(super) fn refuse(self, cause: RpcCause) {
        let Self {
            payload,
            reply,
            lease,
        } = self;
        drop(payload);
        drop(lease);
        let _ = reply.send(Err(cause));
    }
}
