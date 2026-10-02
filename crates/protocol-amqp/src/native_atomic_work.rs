//! Co-owned native receipts and logical atomic work for trusted broker owners.

use std::fmt;

pub use amqp::{
    NativeClaim, NativeReadySubmission, NativeReadyTicket, NativeTransactionDecision,
    NativeTransactionError, NativeTransactionResources,
};

use crate::{AtomicCommitPermit, AtomicTransactionSubmission};

struct PendingLogicalAbort(Option<AtomicCommitPermit>);

impl Drop for PendingLogicalAbort {
    fn drop(&mut self) {
        if let Some(permit) = &self.0 {
            permit.abort();
        }
    }
}

/// Unique co-ownership of one native ready bundle and one logical submission.
///
/// This trusted pairing does not prove matching transaction IDs, controller
/// origins, message conversion, entity authorization, or SDK transaction support.
/// It only keeps both existing authorities and their resources in one handoff.
///
/// Destruction aborts unclaimed logical authority before faulting pending native
/// authority and releasing native payloads, then destroys logical work. Started
/// decisions cannot be revoked by this cancellation path.
///
/// ```compile_fail
/// fn duplicate(work: protocol_amqp::OwnedNativeAtomicMessagingSubmission) {
///     let _second = work.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn submit_twice(work: protocol_amqp::OwnedNativeAtomicMessagingSubmission) {
///     let _first = work.into_submissions();
///     let _second = work.into_submissions();
/// }
/// ```
pub struct OwnedNativeAtomicMessagingSubmission {
    // Field order cancels authority before native payloads and logical refunds.
    abort: PendingLogicalAbort,
    native: NativeReadySubmission,
    logical: AtomicTransactionSubmission,
}

impl OwnedNativeAtomicMessagingSubmission {
    pub fn new(native: NativeReadySubmission, logical: AtomicTransactionSubmission) -> Self {
        let abort = PendingLogicalAbort(Some(logical.permit().clone()));
        Self {
            abort,
            native,
            logical,
        }
    }

    pub fn permit(&self) -> &AtomicCommitPermit {
        self.logical.permit()
    }

    /// Transfers both unique submissions without canceling pending authority.
    ///
    /// The trusted owner must retain both resource scopes, claim native authority
    /// before logical authority and before I/O, and finalize or drop logical
    /// claims before native claims and either resource scope.
    pub fn into_submissions(self) -> (NativeReadySubmission, AtomicTransactionSubmission) {
        let Self {
            mut abort,
            native,
            logical,
        } = self;
        let _ = abort.0.take();
        (native, logical)
    }
}

impl fmt::Debug for OwnedNativeAtomicMessagingSubmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedNativeAtomicMessagingSubmission")
            .finish_non_exhaustive()
    }
}
