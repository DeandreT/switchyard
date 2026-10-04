mod admission;
mod registry;
mod rpc;
mod supervisor;

#[cfg(test)]
mod tests;

pub(super) use registry::{NodeGeneration, PendingEndpoint, Routes, stable_label};
pub(super) use rpc::NetworkFactory;
pub(super) use supervisor::EndpointOwner;

pub(super) const MAX_RPC_JOBS: usize = 32;
pub(super) const MAX_RPC_BYTES: usize = 12 * 1024 * 1024;
pub(super) const MAX_TARGET_RPC_BYTES: usize = 4 * 1024 * 1024;
const SCALAR_RPC_BYTES: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum RpcCause {
    #[error("the in-process peer is unavailable")]
    Unavailable,
    #[error("the in-process peer work capacity is exhausted")]
    Busy,
    #[error("the in-process RPC is invalid")]
    Invalid,
    #[error("the in-process peer did not complete the RPC")]
    PeerFailed,
    #[error("in-process snapshot transport is unsupported")]
    SnapshotUnsupported,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct RpcWorkload {
    pub(super) accepted_jobs: usize,
    pub(super) encoded_bytes: usize,
}
