use amqp::{ClaimedOutgoingSendReservation, PendingSettlement};
use futures_util::future::BoxFuture;
use tokio::sync::watch;

use super::{budget::ContentLease, *};
use crate::{management::DeliveryRegistration, settlement::held_settlement_command};

pub(super) type WorkFuture = BoxFuture<'static, Result<(), ReceiveExit>>;

struct Work<B> {
    sending: BoxFuture<'static, Result<PendingSettlement, EngineError>>,
    _registration: Option<DeliveryRegistration>,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: BoundBroker<B>,
    sequence: domain::SequenceNumber,
    lock: Option<domain::DeliveryLock>,
    session: Option<SessionHold>,
    authorization: Option<LinkAuthorization>,
    retired: watch::Receiver<bool>,
    _content: ContentLease,
}

impl<B: Broker> Work<B> {
    async fn run(mut self) -> Result<(), ReceiveExit> {
        let settlement = self.sending.as_mut().await?;
        let Some(lock) = self.lock else {
            // Receive-and-delete was irrevocable before the native send.
            self.transport_ack(settlement.accept()).await?;
            return Ok(());
        };
        if let Some(authorization) = &self.authorization {
            authorization
                .ensure()
                .await
                .map_err(|_| ReceiveExit::Unauthorized)?;
        }
        let kind = match held_settlement_command(
            self.sequence,
            lock.token,
            self.session.clone(),
            settlement.outcome().clone(),
        ) {
            Ok(kind) => kind,
            Err(error) => {
                let error = error_for(AmqpError::InvalidField, error.to_string());
                self.transport_ack(settlement.reject(error.clone())).await?;
                return Err(ReceiveExit::Refused(error));
            }
        };
        match self
            .broker
            .submit(self.namespace.clone(), self.entity.clone(), kind)
            .await
        {
            Ok(_) => self.transport_ack(settlement.accept()).await?,
            Err(rejection) => {
                warn!(sequence = %self.sequence, %rejection, "settlement refused, leaving the lock to expire");
                self.transport_ack(settlement.reject(rejection_error(&rejection)))
                    .await?;
                return Err(ReceiveExit::Broker(rejection));
            }
        }
        Ok(())
    }

    async fn transport_ack(
        &mut self,
        acknowledgement: impl std::future::Future<Output = Result<(), EngineError>>,
    ) -> Result<(), ReceiveExit> {
        // Retirement has already invalidated the exact native aliases. It must
        // not strand a known domain decision behind the actor's Detach flush.
        tokio::select! {
            biased;
            () = wait_retired(&mut self.retired) => Ok(()),
            result = acknowledgement => result.map_err(ReceiveExit::from),
        }
    }
}

async fn wait_retired(retired: &mut watch::Receiver<bool>) {
    while !*retired.borrow_and_update() {
        if retired.changed().await.is_err() {
            break;
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn start<B: Broker>(
    sender: &Sender,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &BoundBroker<B>,
    protocol: &ReceivingLinkProtocol,
    session: Option<&SessionHold>,
    delivery: Delivery,
    reservation: ClaimedOutgoingSendReservation,
    content: ContentLease,
    retired: watch::Receiver<bool>,
) -> Result<WorkFuture, ReceiveExit> {
    let sequence = delivery.sequence;
    let lock = delivery.lock;
    let registration = lock
        .map(|lock| {
            protocol.management.register_delivery_owned_with_session(
                sender.name(),
                entity.clone(),
                sequence,
                lock.token,
                broker.binding().clone(),
                session.cloned(),
            )
        })
        .transpose()
        .map_err(|_| {
            ReceiveExit::Refused(error_for(
                AmqpError::InternalError,
                "the delivery association could not be registered".to_owned(),
            ))
        })?;
    let tag = lock.map_or_else(
        || sequence.as_u64().to_be_bytes().to_vec().into(),
        |lock| lock_delivery_tag(lock.token),
    );
    let message = crate::write_delivery(&delivery);
    // Retain canonical lock metadata and the original session authority.
    drop(delivery);
    let sending = sender.send_reserved_with_settlement_owned(reservation, message, tag);
    let operation = Work {
        sending: Box::pin(sending),
        _registration: registration,
        namespace: namespace.clone(),
        entity: entity.clone(),
        broker: broker.clone(),
        sequence,
        lock,
        session: session.cloned(),
        authorization: protocol.authorization.clone(),
        retired,
        _content: content,
    };
    Ok(Box::pin(async move { operation.run().await }))
}

#[cfg(test)]
mod tests;
