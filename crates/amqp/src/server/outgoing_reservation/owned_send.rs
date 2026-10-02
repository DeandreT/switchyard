use tokio::sync::mpsc;

use super::*;

// Field order cancels local admission before destroying an unpolled payload.
struct OwnedReservedSend {
    guard: ReservationGuard,
    reservation: ClaimedOutgoingSendReservation,
    message: Message,
    delivery_tag: DeliveryTag,
    channel: u16,
    handle: u32,
    identity: LinkIdentity,
    commands: mpsc::Sender<Command>,
}

impl OwnedReservedSend {
    async fn run(self) -> Result<PendingSettlement, EngineError> {
        if !self.reservation.belongs_to(&self.identity) {
            return Err(EngineError::SendReservationRevoked);
        }
        let Self {
            mut guard,
            reservation,
            message,
            delivery_tag,
            channel,
            handle,
            identity,
            commands,
        } = self;
        let (reply, outcome) = oneshot::channel();
        commands
            .send(Command::SendReserved {
                channel,
                handle,
                identity: identity.clone(),
                reservation,
                message: Box::new(message),
                delivery_tag,
                reply,
            })
            .await
            .map_err(|_| EngineError::Stopped)?;
        let outcome = outcome.await.map_err(|_| EngineError::Stopped)??;
        guard.disarm();
        Ok(PendingSettlement {
            outcome: outcome.outcome,
            identity,
            delivery_identity: outcome.delivery_identity,
            acknowledgement: outcome.acknowledgement,
            channel,
            handle,
            commands,
        })
    }
}

impl Sender {
    /// Creates an owned reserved send without borrowing this endpoint.
    ///
    /// Construction performs no transport IO or encoding and reserves no
    /// message bytes. The future owns the input message and a cancellation
    /// guard immediately, including before its first poll. Dropping it before
    /// the actor consumes the reservation returns local admission through
    /// shared state. Once consumed, cancellation does not undo the send or
    /// any broker operation the caller has already performed.
    ///
    /// On an unsettled sending link, success still waits for the ordinary peer
    /// terminal/default outcome, not merely the final Transfer flush. The
    /// existing source-settled completion remains unchanged. The returned
    /// settlement retains the exact original delivery and final-ACK authority.
    pub fn send_reserved_with_settlement_owned(
        &self,
        reservation: ClaimedOutgoingSendReservation,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> impl Future<Output = Result<PendingSettlement, EngineError>> + Send + 'static + use<> {
        let operation = OwnedReservedSend {
            guard: ReservationGuard::new(Arc::clone(&reservation.guard.control)),
            reservation,
            message,
            delivery_tag,
            channel: self.channel,
            handle: self.handle,
            identity: self.identity.clone(),
            commands: self.commands.clone(),
        };
        async move { operation.run().await }
    }
}

#[cfg(test)]
mod tests;
