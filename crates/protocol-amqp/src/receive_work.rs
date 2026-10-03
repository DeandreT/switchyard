//! One exact route and unique admission ticket for a trusted receive owner.

use std::fmt;

use domain::{BrokerError, EntityBinding, EntityPath, ReceiveMode, SessionHold};

use crate::{ReceiveClaimError, ReceiveClaimPermit, ReceiveClaimTicket};

/// A static unavailable classification, never private storage diagnostics or
/// proof that a started receive rolled back.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReceiveOwnerUnavailableCause {
    #[error("owner stopped")]
    Stopped,
    #[error("owner response unavailable")]
    ResponseUnavailable,
    #[error("storage")]
    Storage,
    #[error("clock")]
    Clock,
    #[error("unexpected owner outcome")]
    UnexpectedOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReceiveSubmitError {
    #[error(transparent)]
    Claim(ReceiveClaimError),
    #[error("expiry-fenced receive admission is not implemented")]
    Unsupported,
    #[error(transparent)]
    Refused(BrokerError),
    #[error("the receive owner is unavailable ({0})")]
    OwnerUnavailable(ReceiveOwnerUnavailableCause),
}

/// An owned receive-only packet, not authentication or arbitrary command access.
/// Its binding and entity must come from the exact admitted endpoint. The
/// ticket is destroyed before route metadata on cancellation.
///
/// ```compile_fail
/// fn duplicate(work: protocol_amqp::OwnedReceiveSubmission) {
///     let _copy = work.clone();
/// }
/// ```
pub struct OwnedReceiveSubmission {
    ticket: ReceiveClaimTicket,
    binding: EntityBinding,
    entity: EntityPath,
    mode: ReceiveMode,
    session: Option<SessionHold>,
}

impl OwnedReceiveSubmission {
    pub fn new(
        binding: EntityBinding,
        entity: EntityPath,
        mode: ReceiveMode,
        session: Option<SessionHold>,
        ticket: ReceiveClaimTicket,
    ) -> Self {
        Self {
            ticket,
            binding,
            entity,
            mode,
            session,
        }
    }

    pub fn binding(&self) -> &EntityBinding {
        &self.binding
    }
    pub fn entity(&self) -> &EntityPath {
        &self.entity
    }
    pub fn permit(&self) -> &ReceiveClaimPermit {
        self.ticket.permit()
    }

    pub fn into_owner_parts(
        self,
    ) -> (
        ReceiveClaimTicket,
        EntityBinding,
        EntityPath,
        ReceiveMode,
        Option<SessionHold>,
    ) {
        (
            self.ticket,
            self.binding,
            self.entity,
            self.mode,
            self.session,
        )
    }
}

impl fmt::Debug for OwnedReceiveSubmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedReceiveSubmission")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
