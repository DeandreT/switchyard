use amqp::ClaimedOutgoingSendReservation;
use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tokio::sync::watch;

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
        let outcome = broker
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::Receive {
                    mode,
                    lock_duration_millis: None,
                    session,
                },
            )
            .await
            .map_err(ReceiveExit::Broker)?;
        match outcome {
            CommandOutcome::Received(Some(delivery)) => Ok(IntakeResult::Acquired(Acquired {
                reservation,
                delivery: Box::new(delivery),
            })),
            CommandOutcome::Received(None) => {
                // Empty Receive must not strand drain with a claimed credit.
                drop(reservation);
                tokio::select! {
                    () = wakeup => {}
                    () = tokio::time::sleep(EMPTY_QUEUE_FALLBACK) => {}
                }
                Ok(IntakeResult::Retry)
            }
            _ => Err(ReceiveExit::Refused(error_for(
                AmqpError::InternalError,
                "receive produced an unexpected outcome".to_owned(),
            ))),
        }
    })
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
