//! Opt-in persisted RawNode driver. No application projection, command outcomes,
//! consistent reads, primary routing, production startup or authenticated peers.

mod engine;
mod input;
mod node;
#[cfg(test)]
mod tests;

#[cfg(feature = "consensus-driver-testkit")]
pub mod testing;

pub use input::{AdmissionError, DriverHandle, ProposalAdmission, Receipt};
pub use node::ConsensusDriver;

use std::time::Duration;

use raft::StateRole;
use thiserror::Error;

use super::{ConsensusError, ConsensusIdentity, MAX_CONSENSUS_ENTRY_BYTES, RetainedLogLimits};
use crate::transport::{FrameCodec, TransportError, TransportLimits};

#[derive(Clone, Debug)]
pub struct DriverOptions {
    pub transport: TransportLimits,
    pub retained: RetainedLogLimits,
    pub max_events: usize,
    pub max_event_bytes: usize,
    /// Separate caps for staged Ready messages and retained local-IO receipts.
    /// Their combined diagnostic count is at most twice this bound.
    pub max_outbound: usize,
    /// Logical bytes (protobuf size + fixed overhead) per outbound stage.
    pub max_outbound_bytes: usize,
    pub inflight_per_peer: usize,
    pub max_uncommitted_bytes: u64,
    pub heartbeat_ticks: usize,
    pub election_ticks: usize,
    /// None permits explicit bounded tick events for deterministic local tests.
    /// Automatic timers skip missed ticks rather than accumulating a queue.
    pub tick_interval: Option<Duration>,
}

impl Default for DriverOptions {
    fn default() -> Self {
        Self {
            transport: TransportLimits::default(),
            retained: RetainedLogLimits::default(),
            max_events: 16,
            max_event_bytes: 64 * 1024 * 1024,
            max_outbound: 32,
            max_outbound_bytes: 128 * 1024 * 1024,
            inflight_per_peer: 2,
            max_uncommitted_bytes: 8 * 1024 * 1024,
            heartbeat_ticks: 1,
            election_ticks: 10,
            tick_interval: Some(Duration::from_millis(100)),
        }
    }
}

impl DriverOptions {
    fn validate(
        &self,
        identity: &ConsensusIdentity,
        local: u64,
    ) -> Result<FrameCodec, DriverError> {
        let codec = FrameCodec::new(identity.clone(), local, self.transport.clone())?;
        // Driver-specific stricter bounds are a subset of the frozen store
        // contract. Validate BEFORE calling a native backend factory.
        if !(4..=65536).contains(&self.retained.max_entries)
            || !((MAX_CONSENSUS_ENTRY_BYTES + 128) as u64..=1024 * 1024 * 1024)
                .contains(&self.retained.max_bytes)
            || !(1..=64).contains(&self.max_events)
            || self.max_event_bytes < 4 * self.transport.max_frame_bytes
            || self.max_event_bytes > 512 * 1024 * 1024
            || !(16..=128).contains(&self.max_outbound)
            || self.max_outbound_bytes < self.max_outbound * self.transport.max_frame_bytes
            || self.max_outbound_bytes > 1024 * 1024 * 1024
            || !(1..=4).contains(&self.inflight_per_peer)
            || !(MAX_CONSENSUS_ENTRY_BYTES as u64..=64 * 1024 * 1024)
                .contains(&self.max_uncommitted_bytes)
            || !(1..=100).contains(&self.heartbeat_ticks)
            || self.election_ticks < self.heartbeat_ticks * 5
            || self.election_ticks > 1000
            || self
                .tick_interval
                .is_some_and(|d| d < Duration::from_millis(10) || d > Duration::from_secs(60))
        {
            return Err(DriverError::Options);
        }
        self.core_config(local).validate()?;
        Ok(codec)
    }

    fn core_config(&self, id: u64) -> raft::Config {
        raft::Config {
            id,
            heartbeat_tick: self.heartbeat_ticks,
            election_tick: self.election_ticks,
            // Pinned core contract: zero means AT MOST ONE entry, retaining its
            // full size. Disabling batching also bounds wire entry count.
            max_size_per_msg: 0,
            batch_append: false,
            max_inflight_msgs: self.inflight_per_peer,
            max_uncommitted_size: self.max_uncommitted_bytes,
            max_committed_size_per_ready: 0,
            applied: 0,
            pre_vote: true,
            check_quorum: true,
            read_only_option: raft::ReadOnlyOption::Safe,
            ..Default::default()
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Role {
    #[default]
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}
impl From<StateRole> for Role {
    fn from(role: StateRole) -> Self {
        match role {
            StateRole::Follower => Self::Follower,
            StateRole::PreCandidate => Self::PreCandidate,
            StateRole::Candidate => Self::Candidate,
            StateRole::Leader => Self::Leader,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    Persistence,
    Core,
    Bounds,
    Panic,
}

/// Latest diagnostic metadata, NOT readiness, a linearizable read, a lease or
/// an application acknowledgement. Observed frontiers describe successful local
/// persistence only; after an ambiguous error the actual store may be ahead.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DriverStatus {
    pub role: Role,
    pub term: u64,
    pub leader_id: u64,
    pub observed_vote: u64,
    pub observed_commit: u64,
    pub observed_last: u64,
    pub applied: u64,
    pub processing: bool,
    pub retired: bool,
    pub finished: bool,
    pub storage_uncertain: bool,
    pub failure: Option<Failure>,
    pub accepting_proposals: bool,
    pub pending_events: usize,
    pub pending_event_bytes: usize,
    pub pending_outbound: usize,
    pub pending_outbound_bytes: usize,
    pub ready_persisted: u64,
    pub advanced: u64,
    pub light_commits_persisted: u64,
    pub peer_events: u64,
    pub ticks: u64,
    pub send_failures: u64,
    pub last_peer_from: u64,
    pub last_peer_term: u64,
    pub last_peer_index: u64,
    pub last_peer_kind: i32,
}

#[derive(Debug, Error)]
pub enum DriverError {
    #[error("invalid bounded driver options")]
    Options,
    #[error(transparent)]
    Persistence(#[from] ConsensusError),
    #[error(transparent)]
    Backend(#[from] storage::StorageError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Core(#[from] raft::Error),
    #[error("driver bound or invariant: {0}")]
    Bound(&'static str),
    #[error("original core/persistence operation panicked; payload retained")]
    Panic,
}
