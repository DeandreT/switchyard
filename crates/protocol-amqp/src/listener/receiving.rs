use std::future::Future;

use amqp::ClaimedOutgoingSendReservation;

use super::*;
use crate::settlement::settlement_command;

type LinkError = Box<dyn std::error::Error + Send + Sync>;

enum ReceiveExit {
    Detached,
    Unauthorized,
    Broker(BrokerRejection),
    Failed(LinkError),
}

impl From<EngineError> for ReceiveExit {
    fn from(error: EngineError) -> Self {
        match error {
            EngineError::RemoteClosed | EngineError::RemoteDetached | EngineError::Stopped => {
                Self::Detached
            }
            error => Self::Failed(error.into()),
        }
    }
}

pub(super) async fn serve_receiving_client<B: Broker>(
    mut sender: Sender,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: BoundBroker<B>,
    mode: ReceiveMode,
    session: Option<SessionHold>,
    protocol: ReceivingLinkProtocol,
) -> Result<(), LinkError> {
    let exit = match receive_until_stopped(
        &mut sender,
        &namespace,
        &entity,
        &broker,
        mode,
        session.as_ref(),
        &protocol,
    )
    .await
    {
        Ok(()) => ReceiveExit::Detached,
        Err(exit) => exit,
    };
    release_session(&broker, &namespace, &entity, session.as_ref()).await;
    match exit {
        ReceiveExit::Detached => {
            let _ = sender.close().await;
        }
        ReceiveExit::Unauthorized => {
            sender
                .close_with_error(unauthorized_error("the link's authorization has expired"))
                .await?;
        }
        ReceiveExit::Broker(rejection) => {
            sender.close_with_error(rejection_error(&rejection)).await?;
        }
        ReceiveExit::Failed(error) => return Err(error),
    }
    Ok(())
}

async fn receive_until_stopped<B: Broker>(
    sender: &mut Sender,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &BoundBroker<B>,
    mode: ReceiveMode,
    session: Option<&SessionHold>,
    protocol: &ReceivingLinkProtocol,
) -> Result<(), ReceiveExit> {
    let authorization = protocol.authorization.as_ref();
    loop {
        ensure_authorized(sender, authorization).await?;
        // The owned future does not borrow Sender, so detach remains watched.
        let reserving = sender.reserve_send();
        let reservation = watch_link(sender, authorization, reserving).await??;
        let reservation = match reservation.try_claim() {
            Ok(reservation) => reservation,
            Err(EngineError::SendReservationRevoked) => continue,
            Err(error) => return Err(error.into()),
        };
        ensure_authorized(sender, authorization).await?;

        // Arm before submitting, so enqueue between an empty reply and the
        // wait cannot be missed. One claimed slot admits exactly one Receive.
        let wakeup = broker.deliverable(namespace, entity);
        let receiving = broker.submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::Receive {
                mode,
                lock_duration_millis: None,
                session: session.cloned(),
            },
        );
        let outcome = watch_link(sender, authorization, receiving)
            .await?
            .map_err(ReceiveExit::Broker)?;
        match outcome {
            CommandOutcome::Received(Some(delivery)) => {
                if !settle(
                    sender,
                    namespace,
                    entity,
                    broker,
                    delivery,
                    reservation,
                    protocol,
                )
                .await
                .map_err(ReceiveExit::Failed)?
                {
                    return Err(ReceiveExit::Unauthorized);
                }
            }
            CommandOutcome::Received(None) => {
                // An empty lookup must not retain credit and strand drain.
                drop(reservation);
                watch_link(sender, authorization, async {
                    tokio::select! {
                        () = wakeup => {}
                        () = tokio::time::sleep(EMPTY_QUEUE_FALLBACK) => {}
                    }
                })
                .await?;
            }
            other => {
                return Err(ReceiveExit::Broker(BrokerRejection::Unavailable(format!(
                    "receive produced an unexpected outcome: {other:?}"
                ))));
            }
        }
    }
}

async fn ensure_authorized(
    sender: &mut Sender,
    authorization: Option<&LinkAuthorization>,
) -> Result<(), ReceiveExit> {
    let Some(authorization) = authorization else {
        return Ok(());
    };
    watch_link(sender, Some(authorization), authorization.ensure())
        .await?
        .map_err(|_| ReceiveExit::Unauthorized)
}

async fn watch_link<T>(
    sender: &mut Sender,
    authorization: Option<&LinkAuthorization>,
    future: impl Future<Output = T>,
) -> Result<T, ReceiveExit> {
    tokio::select! {
        biased;
        () = sender.on_detach() => Err(ReceiveExit::Detached),
        () = wait_until_link_unauthorized(authorization) => Err(ReceiveExit::Unauthorized),
        result = future => Ok(result),
    }
}

/// The broker lock or deletion already committed. Losing this operation does
/// not undo Receive; an unsettled lock remains recoverable through expiry.
async fn settle<B: Broker>(
    sender: &mut Sender,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &BoundBroker<B>,
    delivery: Delivery,
    reservation: ClaimedOutgoingSendReservation,
    protocol: &ReceivingLinkProtocol,
) -> Result<bool, LinkError> {
    let authorization = protocol.authorization.as_ref();
    let management = &protocol.management;
    let Some(lock) = delivery.lock else {
        let delivery_tag = sequence_delivery_tag(delivery.sequence);
        let sending =
            sender.send_reserved(reservation, crate::write_delivery(&delivery), delivery_tag);
        let sent = tokio::select! {
            outcome = sending => {
                outcome?;
                true
            }
            () = wait_until_link_unauthorized(authorization) => false,
        };
        return Ok(sent);
    };
    let sequence = delivery.sequence;
    let delivery_tag = lock_delivery_tag(lock.token);
    let link_name = sender.name().to_owned();
    management
        .register_delivery(
            &link_name,
            entity.clone(),
            sequence,
            lock.token,
            broker.binding().clone(),
        )
        .await;
    let outcome = {
        let sending = sender.send_reserved_with_settlement(
            reservation,
            crate::write_delivery(&delivery),
            delivery_tag,
        );
        tokio::select! {
            outcome = sending => Some(outcome),
            () = wait_until_link_unauthorized(authorization) => None,
        }
    };
    management
        .unregister_delivery(&link_name, lock.token, broker.binding())
        .await;
    let Some(outcome) = outcome else {
        return Ok(false);
    };
    let settlement = outcome?;
    if let Some(authorization) = authorization
        && authorization.ensure().await.is_err()
    {
        return Ok(false);
    }

    let kind = match settlement_command(sequence, lock.token, settlement.outcome().clone()) {
        Ok(kind) => kind,
        Err(error) => {
            let error = error_for(AmqpError::InvalidField, error.to_string());
            settlement.reject(error.clone()).await?;
            sender.close_with_error(error).await?;
            return Ok(true);
        }
    };

    if let Err(rejection) = broker.submit(namespace.clone(), entity.clone(), kind).await {
        warn!(%sequence, %rejection, "settlement refused, leaving the lock to expire");
        settlement.reject(rejection_error(&rejection)).await?;
        sender.close_with_error(rejection_error(&rejection)).await?;
    } else {
        settlement.accept().await?;
    }
    Ok(true)
}

fn sequence_delivery_tag(sequence: domain::SequenceNumber) -> DeliveryTag {
    sequence.as_u64().to_be_bytes().to_vec().into()
}
