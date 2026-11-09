//! Bounded opt-in one-way TCP delivery of the pinned core's protobuf messages.
//! Protocol identity checks are NOT authentication. No elections, peer durable
//! acknowledgement, retry, quorum, application projection or runtime activation.

mod budget;
mod frame;
mod tcp;

pub use budget::{TransportStatus, Usage};
pub use frame::{FRAME_HEADER_BYTES, FRAME_VERSION, FrameCodec, MIN_FRAME_BYTES};
pub use tcp::{PeerHandler, PeerSender, PeerTransport, SendReceipt, TransportReport};

use std::time::Duration;

use thiserror::Error;

#[derive(Clone, Debug)]
pub struct TransportLimits {
    /// Header plus protobuf body. Future drivers must explicitly batch/chunk.
    pub max_frame_bytes: usize,
    pub max_entries: usize,
    /// Includes queued outbound reservations and completed-but-unjoined tasks.
    pub max_connections: usize,
    pub per_peer_connections: usize,
    pub max_operations: usize,
    pub per_peer_operations: usize,
    /// Logical allocation reservations, not physical RSS or a callback heap cap.
    pub max_bytes: usize,
    pub per_peer_bytes: usize,
    /// One absolute budget from accept/outbound admission through all network IO.
    /// Begun dispatch is joined without aborting it on this deadline.
    pub io_timeout: Duration,
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 4 * 1024 * 1024,
            max_entries: crate::consensus::MAX_CONSENSUS_PAGE_ENTRIES,
            max_connections: 16,
            per_peer_connections: 4,
            max_operations: 16,
            per_peer_operations: 4,
            max_bytes: 128 * 1024 * 1024,
            per_peer_bytes: 32 * 1024 * 1024,
            io_timeout: Duration::from_secs(5),
        }
    }
}

impl TransportLimits {
    pub fn validate(&self) -> Result<(), TransportError> {
        let maximum_charge = frame::allocation_charge(self.max_frame_bytes, self.max_entries)?;
        if !(MIN_FRAME_BYTES..=16 * 1024 * 1024).contains(&self.max_frame_bytes)
            || !(1..=crate::consensus::MAX_CONSENSUS_PAGE_ENTRIES).contains(&self.max_entries)
            || !(2..=1024).contains(&self.max_connections)
            || self.per_peer_connections == 0
            || self.per_peer_connections >= self.max_connections
            || !(2..=1024).contains(&self.max_operations)
            || self.per_peer_operations == 0
            || self.per_peer_operations >= self.max_operations
            || self.per_peer_bytes < maximum_charge
            || self.max_bytes
                < self
                    .per_peer_bytes
                    .checked_add(maximum_charge)
                    .ok_or(TransportError::Limit("byte quota overflow"))?
            || self.io_timeout.is_zero()
            || self.io_timeout > Duration::from_secs(3600)
        {
            return Err(TransportError::Limit("invalid transport profile"));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("transport is retired")]
    Retired,
    #[error("transport backpressure: {0}")]
    Busy(&'static str),
    #[error("transport bound: {0}")]
    Limit(&'static str),
    #[error("invalid peer protocol identity")]
    Identity,
    #[error("unsupported peer frame/version/message")]
    Unsupported,
    #[error("malformed peer frame: {0}")]
    Malformed(&'static str),
    #[error("peer IO deadline expired; delivery may be unknown")]
    Deadline,
    #[error("peer IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("peer protobuf: {0}")]
    Protobuf(#[from] protobuf::ProtobufError),
    #[error("peer handler: {0}")]
    Handler(&'static str),
    #[error("original peer task panicked")]
    TaskPanic,
    #[error("original peer task was unexpectedly canceled")]
    TaskCancelled,
    #[error("peer transport owner disappeared before original join")]
    OwnerLost,
}
