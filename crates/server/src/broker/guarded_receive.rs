use std::{future::Future, pin::Pin};

use domain::{BrokerError, Delivery};
use protocol_amqp::{
    OwnedReceiveSubmission, ReceiveClaimAbortGuard, ReceiveClaimPermit,
    ReceiveOwnerUnavailableCause, ReceiveSubmitError,
};

use super::*;

type ReceiveResult = Result<Option<Delivery>, ReceiveSubmitError>;
type AdmissionResult = Result<(), flume::SendError<Box<Request>>>;
type AdmissionFuture = Pin<Box<dyn Future<Output = AdmissionResult> + Send + 'static>>;

// Cancellation precedes destruction of the queued intent, including before
// this operation's first poll or while admission waits for capacity.
struct OwnedReceive {
    _cancel: ReceiveClaimAbortGuard,
    request: AdmissionFuture,
    permit: ReceiveClaimPermit,
    completion: flume::Receiver<ReceiveResult>,
}

impl OwnedReceive {
    async fn run(mut self) -> ReceiveResult {
        if let Err(error) = self.request.as_mut().await {
            self.permit.cancel();
            drop(error);
            return Err(ReceiveSubmitError::OwnerUnavailable(
                ReceiveOwnerUnavailableCause::Stopped,
            ));
        }
        self.completion.recv_async().await.map_err(|_| {
            ReceiveSubmitError::OwnerUnavailable(ReceiveOwnerUnavailableCause::ResponseUnavailable)
        })?
    }
}

impl BrokerHandle {
    /// Queues one trusted receive with pending-only cancellation and an expiry
    /// snapshot. This API does not authenticate that snapshot or its scope.
    ///
    /// The future owns its cancellation guard before its first poll. Once the
    /// owner claims admission, cancellation cannot undo a lock, deletion, or
    /// physical operation. A missing response is not proof of rollback.
    pub fn receive_fenced_owned(
        &self,
        submission: OwnedReceiveSubmission,
    ) -> impl Future<Output = ReceiveResult> + Send + 'static + use<> {
        let permit = submission.permit().clone();
        let cancel = permit.abort_on_drop();
        let requests = self.requests.clone();
        let (reply, completion) = flume::bounded(1);
        let operation = OwnedReceive {
            _cancel: cancel,
            request: Box::pin(async move {
                requests
                    .send_async(Request::ApplyReceiveOwned {
                        submission: Box::new(submission),
                        reply,
                    })
                    .await
            }),
            permit,
            completion,
        };
        async move { operation.run().await }
    }
}

pub(super) fn apply_owned<S: StateStore, C: Clock>(
    proposer: &LocalProposer<S, C>,
    watchers: &Watchers,
    submission: OwnedReceiveSubmission,
    reply: flume::Sender<ReceiveResult>,
) {
    let (ticket, binding, entity, mode, session) = submission.into_owner_parts();
    if let Err(error) = ticket.try_claim() {
        let _ = reply.send(Err(ReceiveSubmitError::Claim(error)));
        return;
    }
    let application = proposer.propose_fenced_with_effects(
        &binding,
        &entity,
        CommandKind::Receive {
            mode,
            lock_duration_millis: None,
            session,
        },
    );
    if let Ok(applied) = &application {
        if makes_deliverable(&applied.outcome) {
            watchers.notify(binding.namespace(), &entity);
        }
        if !entity.is_dead_letter_queue()
            && applied.dead_letters_enqueued
            && let Ok(shadow) = entity.dead_letter_queue()
        {
            watchers.notify(binding.namespace(), &shadow);
        }
    }
    let result = match application {
        Ok(applied) => match applied.outcome {
            CommandOutcome::Received(delivery) => Ok(delivery),
            _ => Err(ReceiveSubmitError::OwnerUnavailable(
                ReceiveOwnerUnavailableCause::UnexpectedOutcome,
            )),
        },
        Err(error) => Err(receive_error(error)),
    };
    let _ = reply.send(result);
}

fn receive_error(error: ProposeError) -> ReceiveSubmitError {
    match error {
        ProposeError::Broker(BrokerError::Storage(_)) => {
            ReceiveSubmitError::OwnerUnavailable(ReceiveOwnerUnavailableCause::Storage)
        }
        ProposeError::ClockWentBackward { .. } => {
            ReceiveSubmitError::OwnerUnavailable(ReceiveOwnerUnavailableCause::Clock)
        }
        ProposeError::UnexpectedOutcome { .. } => {
            ReceiveSubmitError::OwnerUnavailable(ReceiveOwnerUnavailableCause::UnexpectedOutcome)
        }
        ProposeError::Broker(error) => ReceiveSubmitError::Refused(error),
    }
}

#[cfg(test)]
mod tests;
