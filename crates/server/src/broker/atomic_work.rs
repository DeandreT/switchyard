use std::future::Future;

use domain::AtomicMessagingApplication;
use protocol_amqp::{AtomicCommitDecision, OwnedAtomicMessagingSubmission};

use super::{guarded_atomic_messaging::commit_decision, *};

impl BrokerHandle {
    /// Queues trusted staged work with its resource reservation intact. The
    /// cancellation guard is armed even if this future is never polled.
    pub fn submit_atomic_messaging_owned(
        &self,
        submission: OwnedAtomicMessagingSubmission,
    ) -> impl Future<Output = Result<AtomicMessagingApplication, GuardedAtomicSubmitError>>
    + Send
    + 'static
    + use<> {
        let cancel = submission.permit().abort_on_drop();
        let requests = self.requests.clone();
        async move {
            let _cancel = cancel;
            let (reply, application) = flume::bounded(1);
            requests
                .send_async(Request::ApplyAtomicMessagingOwned { submission, reply })
                .await
                .map_err(|_| GuardedAtomicSubmitError::BrokerStopped)?;
            application
                .recv_async()
                .await
                .map_err(|_| GuardedAtomicSubmitError::BrokerStopped)?
        }
    }

    /// Blocking counterpart. Both entry points require the caller's own
    /// authorization and supply no wire transaction or retry-deduplication token.
    pub fn submit_atomic_messaging_owned_blocking(
        &self,
        submission: OwnedAtomicMessagingSubmission,
    ) -> Result<AtomicMessagingApplication, GuardedAtomicSubmitError> {
        let _cancel = submission.permit().abort_on_drop();
        let (reply, application) = flume::bounded(1);
        self.requests
            .send(Request::ApplyAtomicMessagingOwned { submission, reply })
            .map_err(|_| GuardedAtomicSubmitError::BrokerStopped)?;
        application
            .recv()
            .map_err(|_| GuardedAtomicSubmitError::BrokerStopped)?
    }
}

pub(super) fn apply_owned<S: StateStore, C: Clock>(
    proposer: &LocalProposer<S, C>,
    watchers: &Watchers,
    submission: OwnedAtomicMessagingSubmission,
    reply: flume::Sender<Result<AtomicMessagingApplication, GuardedAtomicSubmitError>>,
) {
    let (binding, ticket, mut work) = submission.into_owner_parts();
    let claim = match ticket.try_claim() {
        Ok(claim) => claim,
        Err(error) => {
            let _ = reply.send(Err(error.into()));
            drop(work);
            return;
        }
    };
    let application =
        match work.with_commands(|kinds| proposer.propose_atomic_messaging(&binding, kinds)) {
            Ok(application) => application,
            Err(_) => {
                claim.finish(AtomicCommitDecision::Indeterminate);
                let _ = reply.send(Err(GuardedAtomicSubmitError::WorkUnavailable));
                return;
            }
        };
    claim.finish(commit_decision(&application));
    if let Ok(applied) = &application {
        for target in &applied.enqueue_targets {
            watchers.notify(binding.namespace(), target);
        }
    }
    let _ = reply.send(application.map_err(GuardedAtomicSubmitError::Propose));
    // The reservation covers application, terminal publication, wakeups, and
    // reply delivery, including unwinding or an absent caller.
    drop(work);
}
