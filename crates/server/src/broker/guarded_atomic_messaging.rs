use std::future::Future;

use domain::{AtomicMessagingApplication, BrokerError, validate_atomic_messaging_kinds};
use protocol_amqp::{AtomicCommitClaimError, AtomicCommitDecision, AtomicCommitTicket};

use super::*;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GuardedAtomicSubmitError {
    #[error(transparent)]
    Permit(#[from] AtomicCommitClaimError),
    #[error(transparent)]
    Propose(#[from] ProposeError),
    #[error("the broker owner stopped; consult the commit permit for its decision")]
    BrokerStopped,
}

impl BrokerHandle {
    /// Executes one trusted atomic group with pending-only cancellation. The
    /// returned future owns its cancellation guard even before its first poll.
    pub fn submit_atomic_messaging_guarded(
        &self,
        binding: EntityBinding,
        kinds: Vec<CommandKind>,
        ticket: AtomicCommitTicket,
    ) -> impl Future<Output = Result<AtomicMessagingApplication, GuardedAtomicSubmitError>>
    + Send
    + 'static
    + use<> {
        let cancel = ticket.permit().abort_on_drop();
        let requests = self.requests.clone();
        async move {
            let _cancel = cancel;
            validate_atomic_messaging_kinds(&kinds).map_err(ProposeError::from)?;
            let (reply, application) = flume::bounded(1);
            requests
                .send_async(Request::ApplyAtomicMessagingGuarded {
                    binding,
                    kinds,
                    ticket,
                    reply,
                })
                .await
                .map_err(|_| GuardedAtomicSubmitError::BrokerStopped)?;
            application
                .recv_async()
                .await
                .map_err(|_| GuardedAtomicSubmitError::BrokerStopped)?
        }
    }

    /// Blocking counterpart of the guarded API. Neither entry point adds
    /// authorization, retry deduplication, or a wire transaction lifecycle.
    pub fn submit_atomic_messaging_guarded_blocking(
        &self,
        binding: EntityBinding,
        kinds: Vec<CommandKind>,
        ticket: AtomicCommitTicket,
    ) -> Result<AtomicMessagingApplication, GuardedAtomicSubmitError> {
        let _cancel = ticket.permit().abort_on_drop();
        validate_atomic_messaging_kinds(&kinds).map_err(ProposeError::from)?;
        let (reply, application) = flume::bounded(1);
        self.requests
            .send(Request::ApplyAtomicMessagingGuarded {
                binding,
                kinds,
                ticket,
                reply,
            })
            .map_err(|_| GuardedAtomicSubmitError::BrokerStopped)?;
        application
            .recv()
            .map_err(|_| GuardedAtomicSubmitError::BrokerStopped)?
    }
}

pub(super) fn apply_guarded<S: StateStore, C: Clock>(
    proposer: &LocalProposer<S, C>,
    watchers: &Watchers,
    binding: EntityBinding,
    kinds: Vec<CommandKind>,
    ticket: AtomicCommitTicket,
    reply: flume::Sender<Result<AtomicMessagingApplication, GuardedAtomicSubmitError>>,
) {
    let claim = match ticket.try_claim() {
        Ok(claim) => claim,
        Err(error) => {
            let _ = reply.send(Err(error.into()));
            return;
        }
    };
    let application = proposer.propose_atomic_messaging(&binding, kinds);
    // Publish the known decision before effects or replies, which may outlive
    // their callers or fail independently of a successful storage commit.
    claim.finish(commit_decision(&application));
    if let Ok(applied) = &application {
        for target in &applied.enqueue_targets {
            watchers.notify(binding.namespace(), target);
        }
    }
    let _ = reply.send(application.map_err(GuardedAtomicSubmitError::Propose));
}

fn commit_decision(
    application: &Result<AtomicMessagingApplication, ProposeError>,
) -> AtomicCommitDecision {
    match application {
        Ok(_) => AtomicCommitDecision::Committed,
        Err(
            ProposeError::Broker(BrokerError::Storage(_)) | ProposeError::UnexpectedOutcome { .. },
        ) => AtomicCommitDecision::Indeterminate,
        Err(ProposeError::Broker(_) | ProposeError::ClockWentBackward { .. }) => {
            AtomicCommitDecision::Rejected
        }
    }
}

#[cfg(test)]
mod tests;
