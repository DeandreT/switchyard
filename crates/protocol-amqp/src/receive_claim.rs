//! Pending-only admission for one trusted, expiry-fenced receive.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

const PENDING: u8 = 0;
const STARTED: u8 = 1;
const CANCELLED: u8 = 2;
const EXPIRED: u8 = 3;
const CLOCK_UNAVAILABLE: u8 = 4;

/// Runtime admission, not a durable result or proof of a completed receive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiveClaimState {
    Pending,
    Started,
    Cancelled,
}

/// Static reasons the owner did not acquire receive admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReceiveClaimError {
    #[error("the receive was cancelled before owner admission")]
    Cancelled,
    #[error("the receive authorization expired before owner admission")]
    AuthorizationExpired,
    #[error("the receive owner could not sample the authorization clock")]
    ClaimClockUnavailable,
}

/// An inert, payload-free observer and pending-only cancellation handle.
/// It retains no grant, transport, request, monotonic deadline, or result.
#[derive(Clone)]
pub struct ReceiveClaimPermit(Arc<AtomicU8>);

impl ReceiveClaimPermit {
    /// Creates the sole ticket from a trusted edge's numeric epoch snapshot.
    /// This constructor does not authenticate the snapshot or its resource.
    pub fn new(expiry_epoch_seconds: u64) -> (Self, ReceiveClaimTicket) {
        let permit = Self(Arc::new(AtomicU8::new(PENDING)));
        let ticket = ReceiveClaimTicket {
            permit: permit.clone(),
            expiry_epoch_seconds,
        };
        (permit, ticket)
    }

    pub fn state(&self) -> ReceiveClaimState {
        match self.0.load(Ordering::Acquire) {
            PENDING => ReceiveClaimState::Pending,
            STARTED => ReceiveClaimState::Started,
            _ => ReceiveClaimState::Cancelled,
        }
    }

    /// Cannot revoke Started admission or imply a physical rollback.
    pub fn cancel(&self) -> bool {
        self.transition(CANCELLED)
    }

    /// Capture this guard synchronously before returning an owned future.
    pub fn abort_on_drop(&self) -> ReceiveClaimAbortGuard {
        ReceiveClaimAbortGuard {
            permit: self.clone(),
        }
    }

    fn transition(&self, to: u8) -> bool {
        self.0
            .compare_exchange(PENDING, to, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn error(&self) -> ReceiveClaimError {
        match self.0.load(Ordering::Acquire) {
            EXPIRED => ReceiveClaimError::AuthorizationExpired,
            CLOCK_UNAVAILABLE => ReceiveClaimError::ClaimClockUnavailable,
            _ => ReceiveClaimError::Cancelled,
        }
    }
}

impl fmt::Debug for ReceiveClaimPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReceiveClaimPermit")
            .field("state", &self.state())
            .finish()
    }
}

/// Unique admission authority. Dropping it cancels only Pending admission.
///
/// ```compile_fail
/// let (_, ticket) = protocol_amqp::ReceiveClaimPermit::new(u64::MAX);
/// let _copy = ticket.clone();
/// ```
///
/// ```compile_fail
/// let (_, ticket) = protocol_amqp::ReceiveClaimPermit::new(u64::MAX);
/// let _first = ticket.try_claim();
/// let _second = ticket.try_claim();
/// ```
pub struct ReceiveClaimTicket {
    permit: ReceiveClaimPermit,
    expiry_epoch_seconds: u64,
}

impl ReceiveClaimTicket {
    pub fn permit(&self) -> &ReceiveClaimPermit {
        &self.permit
    }

    pub fn claim_expiry_epoch_seconds(&self) -> u64 {
        self.expiry_epoch_seconds
    }

    /// Samples UTC immediately before the Pending -> Started CAS. Equality
    /// refuses; later renewal cannot extend this snapshot. Started admission
    /// is irrevocable, but does not establish validation, commit, or delivery.
    pub fn try_claim(self) -> Result<(), ReceiveClaimError> {
        self.try_claim_at(SystemTime::now())
    }

    fn try_claim_at(self, now: SystemTime) -> Result<(), ReceiveClaimError> {
        let next = match now.duration_since(UNIX_EPOCH) {
            Ok(now) if now.as_secs() < self.expiry_epoch_seconds => STARTED,
            Ok(_) => EXPIRED,
            Err(_) => CLOCK_UNAVAILABLE,
        };
        if self.permit.transition(next) && next == STARTED {
            Ok(())
        } else {
            Err(self.permit.error())
        }
    }
}

impl fmt::Debug for ReceiveClaimTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReceiveClaimTicket")
            .field("state", &self.permit.state())
            .finish_non_exhaustive()
    }
}

impl Drop for ReceiveClaimTicket {
    fn drop(&mut self) {
        self.permit.cancel();
    }
}

/// Separate cancellation ownership; dropping an observer clone is inert.
///
/// ```compile_fail
/// let (permit, _) = protocol_amqp::ReceiveClaimPermit::new(u64::MAX);
/// let guard = permit.abort_on_drop();
/// let _copy = guard.clone();
/// ```
#[derive(Debug)]
pub struct ReceiveClaimAbortGuard {
    permit: ReceiveClaimPermit,
}

impl Drop for ReceiveClaimAbortGuard {
    fn drop(&mut self) {
        self.permit.cancel();
    }
}

#[cfg(test)]
mod tests;
