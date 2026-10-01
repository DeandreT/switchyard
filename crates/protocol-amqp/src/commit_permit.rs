//! Runtime authority for one queued atomic commit, independent of its payload.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Instant,
};

/// A runtime decision, not a durable transaction record or retry token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum AtomicCommitState {
    Pending,
    Started,
    Aborted,
    Committed,
    Rejected,
    Indeterminate,
}

/// The owner's final knowledge after claiming a commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AtomicCommitDecision {
    Committed,
    Rejected,
    Indeterminate,
}

/// Why this ticket could not acquire owner authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AtomicCommitClaimError {
    #[error("atomic commit was aborted before the owner claimed it")]
    Aborted,
    #[error("atomic commit authority is no longer available")]
    Unavailable,
}

#[derive(Debug)]
struct PermitState {
    deadline: Instant,
    state: AtomicU8,
}

/// A clonable observation and pending-abort handle. Dropping a clone is inert.
///
/// This handle retains no commands, outcomes, storage errors, or message data.
/// A deadline limits acquisition of owner authority, not an already-started
/// physical commit. This runtime type is deliberately not serializable.
#[derive(Clone, Debug)]
pub struct AtomicCommitPermit(Arc<PermitState>);

impl AtomicCommitPermit {
    /// Creates one observer and the sole non-clonable submission ticket.
    pub fn new(deadline: Instant) -> (Self, AtomicCommitTicket) {
        let permit = Self(Arc::new(PermitState {
            deadline,
            state: AtomicU8::new(AtomicCommitState::Pending as u8),
        }));
        let ticket = AtomicCommitTicket {
            permit: permit.clone(),
        };
        (permit, ticket)
    }

    pub fn state(&self) -> AtomicCommitState {
        match self.0.state.load(Ordering::Acquire) {
            value if value == AtomicCommitState::Pending as u8 => AtomicCommitState::Pending,
            value if value == AtomicCommitState::Started as u8 => AtomicCommitState::Started,
            value if value == AtomicCommitState::Aborted as u8 => AtomicCommitState::Aborted,
            value if value == AtomicCommitState::Committed as u8 => AtomicCommitState::Committed,
            value if value == AtomicCommitState::Rejected as u8 => AtomicCommitState::Rejected,
            _ => AtomicCommitState::Indeterminate,
        }
    }

    /// Revokes only unclaimed authority. An abort cannot undo a started commit.
    pub fn abort(&self) -> bool {
        self.transition(AtomicCommitState::Pending, AtomicCommitState::Aborted)
    }

    /// Creates a pending-only cancellation guard to capture before a future polls.
    pub fn abort_on_drop(&self) -> AtomicCommitAbortGuard {
        AtomicCommitAbortGuard {
            permit: self.clone(),
        }
    }

    fn transition(&self, from: AtomicCommitState, to: AtomicCommitState) -> bool {
        self.0
            .state
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

/// Unique submission authority. An unclaimed ticket's destruction aborts it.
///
/// ```compile_fail
/// use protocol_amqp::AtomicCommitPermit;
/// let (_, ticket) = AtomicCommitPermit::new(std::time::Instant::now());
/// let _duplicate = ticket.clone();
/// ```
///
/// ```compile_fail
/// use protocol_amqp::AtomicCommitPermit;
/// let (_, ticket) = AtomicCommitPermit::new(std::time::Instant::now());
/// let _first = ticket.try_claim();
/// let _second = ticket.try_claim();
/// ```
#[derive(Debug)]
pub struct AtomicCommitTicket {
    permit: AtomicCommitPermit,
}

impl AtomicCommitTicket {
    pub fn permit(&self) -> &AtomicCommitPermit {
        &self.permit
    }

    /// The owner calls this once, before validation, stamping, or external I/O.
    /// The deadline is sampled here using the runtime's monotonic clock.
    pub fn try_claim(self) -> Result<AtomicCommitClaim, AtomicCommitClaimError> {
        self.try_claim_at(Instant::now())
    }

    fn try_claim_at(self, now: Instant) -> Result<AtomicCommitClaim, AtomicCommitClaimError> {
        if now >= self.permit.0.deadline {
            self.permit.abort();
        } else if self
            .permit
            .transition(AtomicCommitState::Pending, AtomicCommitState::Started)
        {
            return Ok(AtomicCommitClaim {
                permit: self.permit.clone(),
            });
        }
        match self.permit.state() {
            AtomicCommitState::Aborted => Err(AtomicCommitClaimError::Aborted),
            _ => Err(AtomicCommitClaimError::Unavailable),
        }
    }
}

impl Drop for AtomicCommitTicket {
    fn drop(&mut self) {
        self.permit.abort();
    }
}

/// A separate future-cancellation guard; observer clones never cancel by dropping.
///
/// ```compile_fail
/// use protocol_amqp::AtomicCommitPermit;
/// let (permit, _) = AtomicCommitPermit::new(std::time::Instant::now());
/// let guard = permit.abort_on_drop();
/// let _duplicate = guard.clone();
/// ```
#[derive(Debug)]
pub struct AtomicCommitAbortGuard {
    permit: AtomicCommitPermit,
}

impl Drop for AtomicCommitAbortGuard {
    fn drop(&mut self) {
        self.permit.abort();
    }
}

/// Exclusive owner authority. Dropping it without a decision is indeterminate.
///
/// ```compile_fail
/// use protocol_amqp::AtomicCommitPermit;
/// let (_, ticket) = AtomicCommitPermit::new(std::time::Instant::now());
/// let Ok(claim) = ticket.try_claim() else { return; };
/// let _duplicate = claim.clone();
/// ```
#[derive(Debug)]
pub struct AtomicCommitClaim {
    permit: AtomicCommitPermit,
}

impl AtomicCommitClaim {
    /// Finalizes once. Known success cannot be changed by later cancellation.
    pub fn finish(self, decision: AtomicCommitDecision) {
        let state = match decision {
            AtomicCommitDecision::Committed => AtomicCommitState::Committed,
            AtomicCommitDecision::Rejected => AtomicCommitState::Rejected,
            AtomicCommitDecision::Indeterminate => AtomicCommitState::Indeterminate,
        };
        self.permit.transition(AtomicCommitState::Started, state);
    }
}

impl Drop for AtomicCommitClaim {
    fn drop(&mut self) {
        self.permit
            .transition(AtomicCommitState::Started, AtomicCommitState::Indeterminate);
    }
}

#[cfg(test)]
mod tests;
