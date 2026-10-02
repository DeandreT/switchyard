use std::{fmt, future::Future};

use domain::{AtomicMessagingApplication, EntityBinding};
use protocol_amqp::{
    AtomicCommitClaim, AtomicCommitDecision, AtomicCommitTicket, AtomicMessagingOwnerWork,
    AtomicTransactionSubmission, NativeClaim, NativeTransactionDecision, NativeTransactionError,
    NativeTransactionResources, OwnedNativeAtomicMessagingSubmission,
};

use super::{guarded_atomic_messaging::commit_decision, *};

/// A native authority refusal or the logical owner's typed failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum NativeAtomicSubmitError {
    #[error(transparent)]
    NativeClaim(#[from] NativeTransactionError),
    #[error(transparent)]
    Guarded(#[from] GuardedAtomicSubmitError),
}

/// One owner response, retaining the unique native resources even on refusal.
///
/// A successful application is a broker decision, not a flushed wire response.
/// Consume this completion to finish its native resources asynchronously. A
/// native claim refusal can leave them faulted, in which case their `finish`
/// method refuses; their presence does not promise a wire rollback response.
///
/// ```compile_fail
/// fn duplicate(completion: server::NativeAtomicMessagingCompletion) {
///     let _second = completion.clone();
/// }
/// ```
pub struct NativeAtomicMessagingCompletion {
    application: Result<AtomicMessagingApplication, NativeAtomicSubmitError>,
    resources: NativeTransactionResources,
}

impl NativeAtomicMessagingCompletion {
    pub fn application(&self) -> &Result<AtomicMessagingApplication, NativeAtomicSubmitError> {
        &self.application
    }

    pub fn into_parts(
        self,
    ) -> (
        Result<AtomicMessagingApplication, NativeAtomicSubmitError>,
        NativeTransactionResources,
    ) {
        (self.application, self.resources)
    }
}

impl fmt::Debug for NativeAtomicMessagingCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAtomicMessagingCompletion")
            .finish_non_exhaustive()
    }
}

impl BrokerHandle {
    /// Queues a trusted native/logical pairing with both reservations intact.
    ///
    /// Cancellation is armed before the returned future's first poll. Outer
    /// errors mean the owner response and its resources are unavailable; known
    /// owner failures are returned inside a completion with their resources.
    /// This does not establish authorization or correspondence between native
    /// postings and logical commands, and it never performs native wire I/O.
    pub fn submit_native_atomic_messaging_owned(
        &self,
        submission: OwnedNativeAtomicMessagingSubmission,
    ) -> impl Future<Output = Result<NativeAtomicMessagingCompletion, NativeAtomicSubmitError>>
    + Send
    + 'static
    + use<> {
        let cancel = submission.permit().abort_on_drop();
        let requests = self.requests.clone();
        async move {
            let _cancel = cancel;
            let (reply, completion) = flume::bounded(1);
            if let Err(error) = requests
                .send_async(Request::ApplyNativeAtomicMessagingOwned {
                    submission: Box::new(submission),
                    reply,
                })
                .await
            {
                return recover_admission(error);
            }
            completion.recv_async().await.map_err(|_| broker_stopped())
        }
    }

    /// Blocking counterpart. Known decisions still require a separate native
    /// resource finish; losing the reply does not prove the group rolled back.
    pub fn submit_native_atomic_messaging_owned_blocking(
        &self,
        submission: OwnedNativeAtomicMessagingSubmission,
    ) -> Result<NativeAtomicMessagingCompletion, NativeAtomicSubmitError> {
        let _cancel = submission.permit().abort_on_drop();
        let (reply, completion) = flume::bounded(1);
        if let Err(error) = self
            .requests
            .send(Request::ApplyNativeAtomicMessagingOwned {
                submission: Box::new(submission),
                reply,
            })
        {
            return recover_admission(error);
        }
        completion.recv().map_err(|_| broker_stopped())
    }
}

// Both claims are declared after the resource scopes in the owner. Their field
// order publishes logical indeterminacy before native indeterminacy on unwind.
#[derive(Default)]
struct DualClaims {
    logical: Option<AtomicCommitClaim>,
    native: Option<NativeClaim>,
}

impl DualClaims {
    fn abort_native(&mut self) {
        if let Some(claim) = self.native.take() {
            claim.abort();
        }
    }

    fn finish(mut self, decision: AtomicCommitDecision) {
        if let Some(claim) = self.logical.take() {
            claim.finish(decision);
        }
        if let Some(claim) = self.native.take() {
            claim.finish(match decision {
                AtomicCommitDecision::Committed => NativeTransactionDecision::Committed,
                AtomicCommitDecision::Rejected => NativeTransactionDecision::Rejected,
                AtomicCommitDecision::Indeterminate => NativeTransactionDecision::Indeterminate,
            });
        }
    }
}

pub(super) fn apply_owned<S: StateStore, C: Clock>(
    proposer: &LocalProposer<S, C>,
    watchers: &Watchers,
    submission: OwnedNativeAtomicMessagingSubmission,
    reply: flume::Sender<NativeAtomicMessagingCompletion>,
) {
    let (native, logical) = submission.into_submissions();
    let (native_ticket, resources) = native.into_owner_parts();
    let (binding, logical_ticket, mut work) = logical_parts(logical);
    let mut claims = DualClaims::default();
    match native_ticket.try_claim() {
        Ok(claim) => claims.native = Some(claim),
        Err(error) => {
            drop(logical_ticket);
            let _ = reply.send(NativeAtomicMessagingCompletion {
                application: Err(error.into()),
                resources,
            });
            drop(work);
            return;
        }
    }
    match logical_ticket.try_claim() {
        Ok(claim) => claims.logical = Some(claim),
        Err(error) => {
            claims.abort_native();
            let _ = reply.send(NativeAtomicMessagingCompletion {
                application: Err(GuardedAtomicSubmitError::Permit(error).into()),
                resources,
            });
            drop(work);
            return;
        }
    }

    let application = if let Some(binding) = &binding {
        match work.with_commands(|kinds| proposer.propose_atomic_messaging(binding, kinds)) {
            Ok(application) => application,
            Err(_) => {
                claims.finish(AtomicCommitDecision::Indeterminate);
                let _ = reply.send(NativeAtomicMessagingCompletion {
                    application: Err(GuardedAtomicSubmitError::WorkUnavailable.into()),
                    resources,
                });
                drop(work);
                return;
            }
        }
    } else {
        Ok(AtomicMessagingApplication {
            outcomes: Vec::new(),
            enqueue_targets: Vec::new(),
        })
    };

    claims.finish(commit_decision(&application));
    if let (Some(binding), Ok(applied)) = (&binding, &application) {
        for target in &applied.enqueue_targets {
            watchers.notify(binding.namespace(), target);
        }
    }
    let _ = reply.send(NativeAtomicMessagingCompletion {
        application: application.map_err(|error| GuardedAtomicSubmitError::Propose(error).into()),
        resources,
    });
    drop(work);
}

fn logical_parts(
    submission: AtomicTransactionSubmission,
) -> (
    Option<EntityBinding>,
    AtomicCommitTicket,
    AtomicMessagingOwnerWork,
) {
    match submission {
        AtomicTransactionSubmission::Bound(submission) => {
            let (binding, ticket, work) = submission.into_owner_parts();
            (Some(binding), ticket, work)
        }
        AtomicTransactionSubmission::Empty(submission) => {
            let (ticket, work) = submission.into_owner_parts();
            (None, ticket, work)
        }
    }
}

fn recover_admission(
    error: flume::SendError<Box<Request>>,
) -> Result<NativeAtomicMessagingCompletion, NativeAtomicSubmitError> {
    let Request::ApplyNativeAtomicMessagingOwned { submission, .. } = *error.0 else {
        return Err(broker_stopped());
    };
    let submission = *submission;
    submission.permit().abort();
    let (native, logical) = submission.into_submissions();
    let (native_ticket, resources) = native.into_owner_parts();
    let (_, logical_ticket, work) = logical_parts(logical);
    drop(logical_ticket);
    drop(native_ticket);
    drop(work);
    Ok(NativeAtomicMessagingCompletion {
        application: Err(broker_stopped()),
        resources,
    })
}

fn broker_stopped() -> NativeAtomicSubmitError {
    GuardedAtomicSubmitError::BrokerStopped.into()
}
