//! Portable owner results for trusted native/logical atomic handoffs.
//! This extension does not enable transaction handling on ordinary listeners.

use std::{fmt, future::Future};

use domain::{AtomicMessagingApplication, BrokerError};

use crate::{
    AtomicCommitClaimError, Broker, NativeTransactionError, NativeTransactionResources,
    OwnedNativeAtomicMessagingSubmission,
};

/// A static classification, not a claim that a physical failure rolled back.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NativeAtomicIndeterminateCause {
    #[error("storage")]
    Storage,
    #[error("unexpected owner outcome")]
    UnexpectedOutcome,
    #[error("owner work unavailable")]
    WorkUnavailable,
}

/// The owner's portable failure after applying its existing decision rules.
/// Physical storage failures belong in `Indeterminate`, not `Refused`.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NativeAtomicOwnerError {
    #[error(transparent)]
    NativeClaim(NativeTransactionError),
    #[error(transparent)]
    LogicalClaim(AtomicCommitClaimError),
    #[error(transparent)]
    Refused(BrokerError),
    #[error("the broker clock regressed")]
    ClockRegression,
    #[error("the broker owner stopped before producing an application")]
    OwnerStopped,
    #[error("the atomic owner decision is indeterminate ({0})")]
    Indeterminate(NativeAtomicIndeterminateCause),
}

/// No completion or native resources are available to this caller.
/// This does not establish whether an owner started or committed the job.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("the atomic owner response and native resources are unavailable")]
pub struct NativeAtomicResponseUnavailable;

/// One owner response with the unique native resources, including refusals.
/// The application decision is not proof that a wire response was flushed.
/// Some failure states make native `finish` refuse instead of emitting rollback.
///
/// ```compile_fail
/// fn duplicate(completion: protocol_amqp::NativeAtomicBrokerCompletion) {
///     let _copy = completion.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn consume_twice(completion: protocol_amqp::NativeAtomicBrokerCompletion) {
///     let _first = completion.into_parts();
///     let _second = completion.into_parts();
/// }
/// ```
pub struct NativeAtomicBrokerCompletion {
    application: Result<AtomicMessagingApplication, NativeAtomicOwnerError>,
    resources: NativeTransactionResources,
}

impl NativeAtomicBrokerCompletion {
    /// Joins parts returned by a trusted owner. This constructor establishes no
    /// authorization or correspondence between postings and logical commands.
    pub fn from_owner_parts(
        application: Result<AtomicMessagingApplication, NativeAtomicOwnerError>,
        resources: NativeTransactionResources,
    ) -> Self {
        Self {
            application,
            resources,
        }
    }

    pub fn application(&self) -> &Result<AtomicMessagingApplication, NativeAtomicOwnerError> {
        &self.application
    }

    pub fn into_parts(
        self,
    ) -> (
        Result<AtomicMessagingApplication, NativeAtomicOwnerError>,
        NativeTransactionResources,
    ) {
        (self.application, self.resources)
    }
}

impl fmt::Debug for NativeAtomicBrokerCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAtomicBrokerCompletion")
            .finish_non_exhaustive()
    }
}

/// An additive trusted handoff boundary; ordinary `Broker` implementations do
/// not need transaction support. Implementations must arm pending cancellation
/// synchronously, before the returned owned future can be polled or dropped.
pub trait NativeAtomicBroker: Broker {
    fn submit_native_atomic_messaging_owned(
        &self,
        submission: OwnedNativeAtomicMessagingSubmission,
    ) -> impl Future<Output = Result<NativeAtomicBrokerCompletion, NativeAtomicResponseUnavailable>>
    + Send
    + 'static;
}
