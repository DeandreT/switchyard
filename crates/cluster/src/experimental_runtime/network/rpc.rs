use std::sync::Weak;

use openraft::BasicNode;
use openraft::error::{
    InstallSnapshotError, PayloadTooLarge, RPCError, RaftError, Timeout, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tokio::sync::oneshot;

use super::admission::{RpcJob, RpcPayload, RpcResponse};
use super::registry::{NodeGeneration, Routes};
use super::{RpcCause, SCALAR_RPC_BYTES};
use crate::{LogTypes, MAX_APPEND_BYTES, MAX_APPEND_ENTRIES};

pub(in crate::experimental_runtime) struct NetworkFactory {
    routes: Weak<Routes>,
    source: NodeGeneration,
}

impl NetworkFactory {
    pub(super) fn new(routes: Weak<Routes>, source: NodeGeneration) -> Self {
        Self { routes, source }
    }
}

pub(in crate::experimental_runtime) struct NetworkClient {
    routes: Weak<Routes>,
    source: NodeGeneration,
    target: u64,
    label: String,
}

impl RaftNetworkFactory<LogTypes> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        NetworkClient {
            routes: self.routes.clone(),
            source: self.source.clone(),
            target,
            label: node.addr.clone(),
        }
    }
}

impl RaftNetwork<LogTypes> for NetworkClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<LogTypes>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let routes = self
            .routes
            .upgrade()
            .ok_or_else(|| unreachable(RpcCause::Unavailable))?;
        let bytes = validate_append(&routes, self.source.id(), &rpc)
            .map_err(AppendPreflightError::into_rpc_error)?;
        match self
            .call(
                routes,
                RpcPayload::Append(rpc),
                bytes,
                openraft::RPCTypes::AppendEntries,
                option,
            )
            .await?
        {
            RpcResponse::Append(response) => Ok(response),
            RpcResponse::Vote(_) => Err(unreachable(RpcCause::PeerFailed)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if rpc.vote.leader_id.node_id != self.source.id()
            || rpc
                .last_log_id
                .is_some_and(|id| id.leader_id.term > rpc.vote.leader_id.term)
        {
            return Err(unreachable(RpcCause::Invalid));
        }
        let routes = self
            .routes
            .upgrade()
            .ok_or_else(|| unreachable(RpcCause::Unavailable))?;
        match self
            .call(
                routes,
                RpcPayload::Vote(rpc),
                SCALAR_RPC_BYTES,
                openraft::RPCTypes::Vote,
                option,
            )
            .await?
        {
            RpcResponse::Vote(response) => Ok(response),
            RpcResponse::Append(_) => Err(unreachable(RpcCause::PeerFailed)),
        }
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<LogTypes>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        Err(unreachable(RpcCause::SnapshotUnsupported))
    }
}

impl NetworkClient {
    async fn call(
        &self,
        routes: std::sync::Arc<Routes>,
        payload: RpcPayload,
        bytes: usize,
        action: openraft::RPCTypes,
        option: RPCOption,
    ) -> Result<RpcResponse, RPCError<u64, BasicNode, RaftError<u64>>> {
        let (reply, response) = oneshot::channel();
        routes
            .admit(&self.source, self.target, &self.label, bytes, |lease| {
                RpcJob {
                    payload,
                    reply,
                    lease,
                }
            })
            .map_err(unreachable)?;
        match tokio::time::timeout(option.hard_ttl(), response).await {
            Ok(Ok(Ok(response))) => Ok(response),
            Ok(Ok(Err(cause))) => Err(unreachable(cause)),
            Ok(Err(_)) => Err(unreachable(RpcCause::PeerFailed)),
            Err(_) => Err(RPCError::Timeout(Timeout {
                action,
                id: self.source.id(),
                target: self.target,
                timeout: option.hard_ttl(),
            })),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AppendPreflightError {
    Invalid,
    PayloadTooLarge,
}

impl AppendPreflightError {
    fn into_rpc_error(self) -> RPCError<u64, BasicNode, RaftError<u64>> {
        match self {
            Self::Invalid => unreachable(RpcCause::Invalid),
            Self::PayloadTooLarge => {
                PayloadTooLarge::new_entries_hint(crate::MAX_REPLICA_PAYLOAD_ENTRIES).into()
            }
        }
    }
}

pub(super) fn validate_append(
    routes: &Routes,
    source: u64,
    rpc: &AppendEntriesRequest<LogTypes>,
) -> Result<usize, AppendPreflightError> {
    if rpc.vote.leader_id.node_id != source {
        return Err(AppendPreflightError::Invalid);
    }
    if rpc
        .prev_log_id
        .is_some_and(|id| id.leader_id.term > rpc.vote.leader_id.term)
    {
        return Err(AppendPreflightError::Invalid);
    }
    if rpc.entries.len() > MAX_APPEND_ENTRIES {
        return Err(AppendPreflightError::PayloadTooLarge);
    }
    let mut previous = rpc.prev_log_id;
    let mut bytes = 0usize;
    for entry in &rpc.entries {
        let expected = previous.map_or(Some(0), |id| id.index.checked_add(1));
        if expected != Some(entry.log_id.index)
            || previous.is_some_and(|id| entry.log_id <= id)
            || entry.log_id.leader_id.term > rpc.vote.leader_id.term
        {
            return Err(AppendPreflightError::Invalid);
        }
        if entry.log_id.index == 0
            && (entry.log_id != crate::LogId::default()
                || !matches!(&entry.payload, openraft::EntryPayload::Membership(_)))
        {
            return Err(AppendPreflightError::Invalid);
        }
        if let openraft::EntryPayload::Membership(membership) = &entry.payload
            && !routes.matches_membership(membership)
        {
            return Err(AppendPreflightError::Invalid);
        }
        let encoded = crate::experimental_log::validated_entry_len(entry)
            .map_err(|_| AppendPreflightError::Invalid)?;
        bytes = bytes
            .checked_add(encoded)
            .ok_or(AppendPreflightError::Invalid)?;
        if bytes > MAX_APPEND_BYTES {
            return Err(AppendPreflightError::PayloadTooLarge);
        }
        previous = Some(entry.log_id);
    }
    Ok(bytes.max(SCALAR_RPC_BYTES))
}

fn unreachable<E: std::error::Error>(cause: RpcCause) -> RPCError<u64, BasicNode, E> {
    Unreachable::new(&cause).into()
}

#[cfg(test)]
mod tests;
