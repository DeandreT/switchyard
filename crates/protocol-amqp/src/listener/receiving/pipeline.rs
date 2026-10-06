use amqp::ClaimedOutgoingSendReservation;
use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tokio::sync::watch;

use crate::{OwnedReceiveSubmission, ReceiveClaimError, ReceiveClaimPermit, ReceiveSubmitError};

use super::{
    budget::{ContentBudget, MAX_RECEIVING_WORK, projected_delivery_bytes},
    work, *,
};

struct Acquired {
    reservation: ClaimedOutgoingSendReservation,
    delivery: Box<Delivery>,
}

enum IntakeResult {
    Acquired(Acquired),
    Retry,
}

type IntakeFuture = BoxFuture<'static, Result<IntakeResult, ReceiveExit>>;

// Cancellation of the pending owner receive precedes returning native credit.
// Keep this whole packet captured while its receiving future is polled.
struct ReceivePacket<R> {
    receiving: BoxFuture<'static, Result<Option<Delivery>, ReceiveExit>>,
    reservation: R,
}

impl<R> ReceivePacket<R> {
    async fn run(mut self) -> Result<(R, Option<Delivery>), ReceiveExit> {
        let delivery = self.receiving.as_mut().await?;
        let Self {
            receiving,
            reservation,
        } = self;
        drop(receiving);
        Ok((reservation, delivery))
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn receive_until_stopped<B: Broker>(
    sender: &mut Sender,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &BoundBroker<B>,
    mode: ReceiveMode,
    session: Option<&SessionHold>,
    protocol: &ReceivingLinkProtocol,
) -> Result<(), ReceiveExit> {
    let budget = ContentBudget::default();
    let (retired, _) = watch::channel(false);
    let mut work = FuturesUnordered::<work::WorkFuture>::new();
    let mut intake: Option<IntakeFuture> = None;
    let mut parked: Option<(Acquired, usize)> = None;
    loop {
        if let Some((acquired, bytes)) = parked.take() {
            if let Some(content) = budget.try_acquire(bytes) {
                work.push(work::start(
                    sender,
                    namespace,
                    entity,
                    broker,
                    protocol,
                    session,
                    *acquired.delivery,
                    acquired.reservation,
                    content,
                    retired.subscribe(),
                )?);
            } else {
                parked = Some((acquired, bytes));
            }
        }
        if intake.is_none()
            && parked.is_none()
            && work.len() < MAX_RECEIVING_WORK
            && !budget.is_full()
        {
            intake = Some(start_intake(
                sender, namespace, entity, broker, mode, session, protocol,
            ));
        }
        tokio::select! {
            biased;
            () = sender.on_detach() => {
                return finish_detached(
                    sender, &retired, &mut work, &mut intake, &mut parked,
                    protocol.authorization.as_ref(),
                ).await;
            }
            () = wait_until_link_unauthorized(protocol.authorization.as_ref()) => {
                return Err(ReceiveExit::Unauthorized);
            }
            // Poll existing work before another lookup, after shutdown guards.
            Some(result) = work.next(), if !work.is_empty() => {
                if matches!(result, Err(ReceiveExit::Detached)) {
                    return finish_detached(
                        sender, &retired, &mut work, &mut intake, &mut parked,
                        protocol.authorization.as_ref(),
                    ).await;
                }
                result?;
            }
            result = poll_intake(&mut intake), if intake.is_some() => {
                intake = None;
                if matches!(result, Err(ReceiveExit::Detached)) {
                    return finish_detached(
                        sender, &retired, &mut work, &mut intake, &mut parked,
                        protocol.authorization.as_ref(),
                    ).await;
                }
                match result? {
                    IntakeResult::Acquired(acquired) => {
                        let bytes = projected_delivery_bytes(&acquired.delivery)?;
                        parked = Some((acquired, bytes));
                    }
                    IntakeResult::Retry => {}
                }
            }
        }
    }
}

async fn finish_detached(
    sender: &mut Sender,
    retired: &watch::Sender<bool>,
    work: &mut FuturesUnordered<work::WorkFuture>,
    intake: &mut Option<IntakeFuture>,
    parked: &mut Option<(Acquired, usize)>,
    authorization: Option<&LinkAuthorization>,
) -> Result<(), ReceiveExit> {
    // A retired-send reply can win the select after its detach branch was
    // sampled pending. Confirm the exact endpoint before broadcasting cleanup.
    tokio::select! {
        biased;
        () = wait_until_link_unauthorized(authorization) => return Err(ReceiveExit::Unauthorized),
        () = sender.on_detach() => {}
    }
    let _ = retired.send(true);
    drop(intake.take());
    drop(parked.take());
    drain_after_detach(work, authorization).await
}

async fn drain_after_detach(
    work: &mut FuturesUnordered<work::WorkFuture>,
    authorization: Option<&LinkAuthorization>,
) -> Result<(), ReceiveExit> {
    let mut failure = None;
    while !work.is_empty() {
        tokio::select! {
            biased;
            () = wait_until_link_unauthorized(authorization) => return Err(ReceiveExit::Unauthorized),
            Some(result) = work.next() => {
                if let Err(error) = result
                    && !matches!(error, ReceiveExit::Detached)
                    && failure.is_none()
                {
                    failure = Some(error);
                }
            }
        }
    }
    if failure.is_some() {
        warn!("receiving link detached with an unfinished settlement failure");
    }
    Err(failure.unwrap_or(ReceiveExit::Detached))
}

async fn poll_intake(intake: &mut Option<IntakeFuture>) -> Result<IntakeResult, ReceiveExit> {
    match intake {
        Some(intake) => intake.as_mut().await,
        None => std::future::pending().await,
    }
}

#[allow(clippy::too_many_arguments)]
fn start_intake<B: Broker>(
    sender: &Sender,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &BoundBroker<B>,
    mode: ReceiveMode,
    session: Option<&SessionHold>,
    protocol: &ReceivingLinkProtocol,
) -> IntakeFuture {
    let reserving = sender.reserve_send();
    let broker = broker.clone();
    let namespace = namespace.clone();
    let entity = entity.clone();
    let session = session.cloned();
    let authorization = protocol.authorization.clone();
    Box::pin(async move {
        ensure_authorized(authorization.as_ref()).await?;
        let reservation = reserving.await?;
        let reservation = match reservation.try_claim() {
            Ok(reservation) => reservation,
            Err(EngineError::SendReservationRevoked) => return Ok(IntakeResult::Retry),
            Err(error) => return Err(error.into()),
        };
        ensure_authorized(authorization.as_ref()).await?;
        // This future remains pinned across every other work completion.
        let wakeup = broker.deliverable(&namespace, &entity);
        let receiving: BoxFuture<'static, _> = if let Some(authorization) = &authorization {
            // This is the exact admitted Listen resource, not its family owner
            // or an unrelated grant. The owner will re-sample UTC at claim.
            let expiry = authorization
                .claim_expiry_epoch_seconds()
                .await
                .map_err(|_| ReceiveExit::Unauthorized)?;
            let (_, ticket) = ReceiveClaimPermit::new(expiry);
            let submission = OwnedReceiveSubmission::new(
                broker.binding().clone(),
                entity.clone(),
                mode,
                session,
                ticket,
            );
            let operation = broker.receive_fenced_owned(submission);
            Box::pin(async move { operation.await.map_err(receive_submit_error) })
        } else {
            // Unsecured listeners retain their existing command path.
            let broker = broker.clone();
            let namespace = namespace.clone();
            let entity = entity.clone();
            Box::pin(async move {
                match broker
                    .submit(
                        namespace,
                        entity,
                        CommandKind::Receive {
                            mode,
                            lock_duration_millis: None,
                            session,
                        },
                    )
                    .await
                    .map_err(ReceiveExit::Broker)?
                {
                    CommandOutcome::Received(delivery) => Ok(delivery),
                    _ => Err(ReceiveExit::Refused(error_for(
                        AmqpError::InternalError,
                        "receive produced an unexpected outcome".to_owned(),
                    ))),
                }
            })
        };
        let packet = ReceivePacket {
            receiving,
            reservation,
        };
        let (reservation, delivery) = packet.run().await?;
        match delivery {
            Some(delivery) => Ok(IntakeResult::Acquired(Acquired {
                reservation,
                delivery: Box::new(delivery),
            })),
            None => {
                // Empty Receive must not strand drain with a claimed credit.
                drop(reservation);
                tokio::select! {
                    () = wakeup => {}
                    () = tokio::time::sleep(EMPTY_QUEUE_FALLBACK) => {}
                }
                Ok(IntakeResult::Retry)
            }
        }
    })
}

fn receive_submit_error(error: ReceiveSubmitError) -> ReceiveExit {
    match error {
        ReceiveSubmitError::Claim(ReceiveClaimError::AuthorizationExpired) => {
            ReceiveExit::Unauthorized
        }
        ReceiveSubmitError::Unsupported => ReceiveExit::Refused(error_for(
            AmqpError::NotImplemented,
            "expiry-fenced receive admission is not implemented".to_owned(),
        )),
        ReceiveSubmitError::Refused(error) => ReceiveExit::Broker(BrokerRejection::Refused(error)),
        // Cancellation is not proof that this exact native sender retired.
        // In particular, never wait on on_detach for a healthy cancelled job.
        ReceiveSubmitError::Claim(_) | ReceiveSubmitError::OwnerUnavailable(_) => {
            ReceiveExit::Refused(error_for(
                AmqpError::InternalError,
                "the receive owner could not produce a result".to_owned(),
            ))
        }
    }
}

async fn ensure_authorized(authorization: Option<&LinkAuthorization>) -> Result<(), ReceiveExit> {
    if let Some(authorization) = authorization {
        authorization
            .ensure()
            .await
            .map_err(|_| ReceiveExit::Unauthorized)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
