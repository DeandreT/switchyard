use std::{fmt, sync::Arc};

use super::*;
use crate::{Body, Coordinator, TransactionCommand, TransactionId, TransactionalState};

mod book;
mod control_refusal;
mod endpoints;
mod group;
mod handler;
mod receiver_identity;
mod retirement;

pub(super) use book::NativeTransactionBook;
pub(super) use control_refusal::NativeControlRefusal;
pub(super) use endpoints::NativeCommand;
pub use endpoints::{
    CoordinatorEndpoint, CoordinatorRequest, TransactionalIngress, TransactionalReceiver,
};
pub(super) use group::NativePartialPosting;
pub(super) use group::NativeRetirementHook;
pub(super) use group::NativeRoute;
pub use group::{
    NativeClaim, NativeControllerIdentity, NativeReadySubmission, NativeReadyTicket,
    NativeTransactionIdentity, NativeTransactionResources, PendingDeclareReceipt, PreparedPosting,
    SealedDischargeReceipt, TransactionPostingReceipt,
};
pub(super) use handler::{handle_control_refusal, handle_native_command};
pub use receiver_identity::NativeReceiverIdentity;
pub use retirement::{NativePreparedWork, PreparedRetirement, TransactionRetirementReceipt};
pub(super) use retirement::{NativeRetirementAttempt, NativeRetirementCandidate};

pub const MAX_NATIVE_TRANSACTIONS: usize = 32;
pub const MAX_NATIVE_TRANSACTION_POSTINGS: usize = 100;
pub const MAX_NATIVE_TRANSACTION_CONTROL_BYTES: u64 = 4096;
const MAX_NATIVE_TERMINALS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeIngressPolicy {
    Disabled,
    Posting,
    PostingAndRetirement,
    WorkDefaults,
}

impl NativeIngressPolicy {
    pub(super) fn supports_retirement(self) -> bool {
        matches!(self, Self::PostingAndRetirement | Self::WorkDefaults)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeAttachKind {
    Ordinary,
    Coordinator(NativeCoordinatorProfile),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct NativeCoordinatorProfile {
    capabilities: u8,
    outcomes: u8,
    // Preserve the actor-approved nullness across mutable Attach adjustments.
    default_initial_delivery_count: bool,
}

impl NativeCoordinatorProfile {
    pub(super) fn defaults_initial_delivery_count(self) -> bool {
        self.default_initial_delivery_count
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeTransactionState {
    Pending,
    Sealed,
    Ready,
    OwnerStarted,
    Aborted,
    Committed,
    Rejected,
    Indeterminate,
    Faulted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeTransactionDecision {
    Committed,
    Rejected,
    Indeterminate,
}

/// A trusted local declaration refusal, without caller-supplied wire text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeDeclarationRefusal {
    ResourceLimit,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum NativeFault {
    Dropped,
    PartialAtSeal,
    Decode,
    Aborted,
    Inbox,
    Continuation,
    Flush,
    Closed,
    Stage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeTransactionError {
    Disabled,
    InvalidAttach,
    Unsupported,
    UnknownTransaction,
    Limit,
    NotReady,
    InvalidPreparedSet,
    InvalidDecision,
    Retired,
    Faulted(NativeFault),
}

impl NativeTransactionError {
    pub(super) fn condition(self) -> &'static str {
        match self {
            Self::Disabled | Self::Unsupported => "amqp:not-implemented",
            Self::UnknownTransaction => "amqp:transaction:unknown-id",
            Self::Limit => "amqp:resource-limit-exceeded",
            Self::Faulted(_) => "amqp:transaction:rollback",
            Self::Retired => "amqp:not-allowed",
            _ => "amqp:invalid-field",
        }
    }

    pub(super) fn description(self) -> &'static str {
        match self {
            Self::Disabled => "native transactional ingress is disabled",
            Self::InvalidAttach => "invalid native transaction endpoint approval",
            Self::Unsupported => "unsupported native transaction operation",
            Self::UnknownTransaction => "unknown native transaction identity",
            Self::Limit => "native transaction resource limit reached",
            Self::NotReady => "native transaction obligations are not ready",
            Self::InvalidPreparedSet => "native transaction prepared resources do not match",
            Self::InvalidDecision => "native transaction has no matching final owner decision",
            Self::Retired => "native transaction endpoint is retired",
            Self::Faulted(_) => "native transaction ingress has failed",
        }
    }
}

impl fmt::Display for NativeTransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl std::error::Error for NativeTransactionError {}

fn native_error(error: NativeTransactionError) -> EngineError {
    invalid_state(error.description())
}

pub(super) fn classify_attach(
    attach: &Attach,
    policy: NativeIngressPolicy,
) -> Result<NativeAttachKind, NativeTransactionError> {
    let Some(coordinator) = attach
        .target
        .as_ref()
        .and_then(|target| target.as_coordinator())
    else {
        if source_uses_transactions(attach.source.as_ref()) {
            return Err(NativeTransactionError::Unsupported);
        }
        return Ok(NativeAttachKind::Ordinary);
    };
    if policy == NativeIngressPolicy::Disabled {
        return Err(NativeTransactionError::Disabled);
    }
    if attach.role != Role::Sender
        || attach.snd_settle_mode == SenderSettleMode::Settled
        || has_recovery_state(attach)
        || (attach.initial_delivery_count.is_none() && policy != NativeIngressPolicy::WorkDefaults)
    {
        return Err(NativeTransactionError::InvalidAttach);
    }
    let capabilities = capability_bits(coordinator)?;
    let mut outcomes = 0;
    if let Some(source) = &attach.source {
        if source.dynamic
            || source.filter.is_some()
            || source.dynamic_node_properties.is_some()
            || transaction_state(source.default_outcome.as_ref())
        {
            return Err(NativeTransactionError::Unsupported);
        }
        if !matches!(
            source.default_outcome,
            None | Some(
                DeliveryState::Accepted(_)
                    | DeliveryState::Rejected(_)
                    | DeliveryState::Released(_)
                    | DeliveryState::Modified(_)
            )
        ) {
            return Err(NativeTransactionError::InvalidAttach);
        }
        if let Some(values) = &source.outcomes {
            if values.len() > 5 {
                return Err(NativeTransactionError::Limit);
            }
            for value in values.iter() {
                let bit = match value.as_str() {
                    "amqp:accepted:list" => 1,
                    "amqp:rejected:list" => 2,
                    "amqp:released:list" => 4,
                    "amqp:modified:list" => 8,
                    "amqp:declared:list" => 16,
                    _ => return Err(NativeTransactionError::Unsupported),
                };
                if outcomes & bit != 0 {
                    return Err(NativeTransactionError::InvalidAttach);
                }
                outcomes |= bit;
            }
        }
    }
    Ok(NativeAttachKind::Coordinator(NativeCoordinatorProfile {
        capabilities,
        outcomes,
        default_initial_delivery_count: attach.initial_delivery_count.is_none(),
    }))
}

fn capability_bits(coordinator: &Coordinator) -> Result<u8, NativeTransactionError> {
    let mut bits = 0;
    if let Some(values) = &coordinator.capabilities {
        if values.len() > 3 {
            return Err(NativeTransactionError::Limit);
        }
        for value in values.iter() {
            let bit = match value.as_str() {
                "amqp:local-transactions" => 1,
                "amqp:multi-txns-per-ssn" => 2,
                "amqp:multi-ssns-per-txn" => 4,
                _ => return Err(NativeTransactionError::Unsupported),
            };
            if bits & bit != 0 {
                return Err(NativeTransactionError::InvalidAttach);
            }
            bits |= bit;
        }
    }
    Ok(bits)
}

pub(super) fn validate_accept_kind(
    attach: &IncomingAttach,
    expected: NativeAttachKind,
) -> Result<(), EngineError> {
    let policy = match expected {
        NativeAttachKind::Coordinator(profile) if profile.defaults_initial_delivery_count() => {
            NativeIngressPolicy::WorkDefaults
        }
        _ => NativeIngressPolicy::Posting,
    };
    if attach.approval().kind() != expected
        || classify_attach(attach, policy).map_err(native_error)? != expected
    {
        return Err(native_error(NativeTransactionError::InvalidAttach));
    }
    Ok(())
}

fn control_command(message: &Message) -> Result<TransactionCommand, NativeTransactionError> {
    let Body::Value(value) = &message.body else {
        return Err(NativeTransactionError::Faulted(NativeFault::Decode));
    };
    TransactionCommand::try_from(value.clone())
        .map_err(|_| NativeTransactionError::Faulted(NativeFault::Decode))
}

#[cfg(test)]
mod tests;
