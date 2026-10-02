use amqp::{
    AmqpError, NativeSenderIdentity, PendingSettlement, SentDelivery, TransactionalDisposition,
    TransactionalSender,
};
use domain::ReceiveMode;

use super::super::groups::{HeldDelivery, RetirementCompletion};
use super::*;
use crate::{
    Broker,
    broker::BoundBroker,
    listener::{NextDeliveryError, error_for, lock_delivery_tag, next_delivery},
    settlement::settlement_command,
};

pub(in crate::listener::atomic_ingress) async fn consumer<B: NativeAtomicBroker>(
    mut sender: TransactionalSender,
    admission: QueueAdmission,
    broker: B,
    authorization: Option<LinkAuthorization>,
    events: mpsc::Sender<Event>,
    _permit: OwnedSemaphorePermit,
) -> Result<(), IngressError> {
    let identity = sender.sender_identity();
    let broker = BoundBroker::new(broker, admission.binding.clone());
    let namespace = admission.binding.namespace().clone();
    let entity = admission.binding.target().clone();
    let (close, mut closing) = mpsc::channel(1);
    let (reply, registered) = oneshot::channel();
    let registration = Event::RegisterConsumer {
        identity: identity.clone(),
        admission,
        authorization: authorization.clone(),
        close,
        reply,
    };
    let mut delivery = None;
    let mut sent: Option<SentDelivery> = None;
    let mut pending_disposition = None;
    let mut stop_reported = false;
    let result = async {
        let registered_event = tokio::select! {
            biased;
            () = producer_expired(authorization.as_ref()) => return Ok(Some(expired())),
            result = events.send(registration) => result,
        };
        if registered_event.is_err() { return Ok(None); }
        let registered = tokio::select! {
            biased;
            close = closing.recv() => return Ok(close_error(close)),
            () = sender.on_detach() => return Ok(None),
            () = producer_expired(authorization.as_ref()) => return Ok(Some(expired())),
            result = registered => result,
        };
        match registered {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Ok(Some(error)),
            Err(_) => return Ok(None),
        }

        loop {
            let fetched = tokio::select! {
                biased;
                close = closing.recv() => return Ok(close_error(close)),
                () = sender.on_detach() => return Ok(None),
                () = producer_expired(authorization.as_ref()) => return Ok(Some(expired())),
                fetched = next_delivery(&broker, &namespace, &entity, ReceiveMode::PeekLock, None, authorization.as_ref()) => fetched,
            };
            delivery = Some(match fetched {
                Ok(delivery) => delivery,
                Err(NextDeliveryError::Unauthorized) => return Ok(Some(expired())),
                Err(NextDeliveryError::Broker(error)) => return Ok(Some(rejection_error(&error))),
            });
            let current = delivery.as_ref().ok_or_else(|| std::io::Error::other("consumer delivery is unavailable"))?;
            let Some(lock) = current.lock else {
                return Ok(Some(error_for(AmqpError::InternalError, "peek-lock receive returned no lock".to_owned())));
            };
            let sequence = current.sequence;
            if let Some(authorization) = authorization.as_ref()
                && let Err(error) = authorization.ensure().await
            { return Ok(Some(error)); }

            // Keep the send future alive until pending logical authority is stopped.
            let published = {
                let sending = sender.send_with_dispositions(crate::write_delivery(current), lock_delivery_tag(lock.token));
                tokio::pin!(sending);
                tokio::select! {
                    biased;
                    close = closing.recv() => {
                        report_stop(&events, &identity, &mut stop_reported).await;
                        return Ok(close_error(close));
                    }
                    () = producer_expired(authorization.as_ref()) => {
                        report_stop(&events, &identity, &mut stop_reported).await;
                        return Ok(Some(expired()));
                    }
                    result = sending.as_mut() => result,
                }
            };
            sent = Some(match published {
                Ok(sent) => sent,
                Err(EngineError::RemoteClosed | EngineError::RemoteDetached | EngineError::Stopped) => return Ok(None),
                Err(error) => return Err(error.into()),
            });
            let original = sent.as_ref().ok_or_else(|| std::io::Error::other("consumer original is unavailable"))?
                .delivery_identity().clone();
            let (reply, held) = oneshot::channel();
            let registration = Event::RegisterHeld {
                source: identity.clone(),
                delivery: HeldDelivery { sequence, token: lock.token, original: original.clone() },
                reply,
            };
            let capacity = tokio::select! {
                biased;
                close = closing.recv() => return Ok(close_error(close)),
                () = sender.on_detach() => return Ok(None),
                () = producer_expired(authorization.as_ref()) => return Ok(Some(expired())),
                result = events.reserve() => result,
            };
            let Ok(capacity) = capacity else { return Ok(None); };
            capacity.send(registration);
            let held = tokio::select! {
                biased;
                close = closing.recv() => return Ok(close_error(close)),
                () = sender.on_detach() => return Ok(None),
                () = producer_expired(authorization.as_ref()) => return Ok(Some(expired())),
                result = held => result,
            };
            match held {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Ok(Some(error)),
                Err(_) => return Ok(None),
            }
            drop(delivery.take());

            loop {
                let original = sent.as_mut().ok_or_else(|| std::io::Error::other("consumer original is unavailable"))?;
                let disposition = tokio::select! {
                    biased;
                    close = closing.recv() => return Ok(close_error(close)),
                    () = sender.on_detach() => return Ok(None),
                    () = producer_expired(authorization.as_ref()) => return Ok(Some(expired())),
                    result = original.next_disposition() => result,
                };
                pending_disposition = Some(match disposition {
                    Ok(disposition) => disposition,
                    Err(EngineError::RemoteClosed | EngineError::RemoteDetached | EngineError::Stopped) => return Ok(None),
                    Err(error) => return Err(error.into()),
                });
                if let Some(authorization) = authorization.as_ref()
                    && let Err(error) = authorization.ensure().await
                { return Ok(Some(error)); }
                let original = sent.as_ref().ok_or_else(|| std::io::Error::other("consumer original is unavailable"))?
                    .delivery_identity();
                let exact = match pending_disposition.as_ref() {
                    Some(TransactionalDisposition::Ordinary(pending)) => pending.belongs_to_sender(&identity)
                        && pending.delivery_identity().same_delivery(original),
                    Some(TransactionalDisposition::Retirement(receipt)) => receipt.belongs_to_sender(&identity)
                        && receipt.delivery_identity().same_delivery(original),
                    None => false,
                };
                if !exact {
                    return Ok(Some(error_for(AmqpError::InvalidField,
                        "consumer disposition belongs to a different original delivery".to_owned())));
                }

                if matches!(pending_disposition, Some(TransactionalDisposition::Ordinary(_))) {
                    let Some(TransactionalDisposition::Ordinary(pending)) = pending_disposition.take() else {
                        return Err(std::io::Error::other("ordinary consumer disposition is unavailable").into());
                    };
                    let settlement = settle_ordinary(&broker, &namespace, &entity, sequence, lock.token, pending);
                    tokio::pin!(settlement);
                    let settled = tokio::select! {
                        biased;
                        close = closing.recv() => {
                            report_stop(&events, &identity, &mut stop_reported).await;
                            return Ok(close_error(close));
                        }
                        () = sender.on_detach() => {
                            report_stop(&events, &identity, &mut stop_reported).await;
                            return Ok(None);
                        }
                        () = producer_expired(authorization.as_ref()) => {
                            report_stop(&events, &identity, &mut stop_reported).await;
                            return Ok(Some(expired()));
                        }
                        result = settlement.as_mut() => result,
                    };
                    if let Some(error) = settled? { return Ok(Some(error)); }
                    let original = sent.as_ref().ok_or_else(|| std::io::Error::other("consumer original is unavailable"))?
                        .delivery_identity().clone();
                    let (reply, cleared) = oneshot::channel();
                    let capacity = tokio::select! {
                        biased;
                        close = closing.recv() => return Ok(close_error(close)),
                        () = sender.on_detach() => return Ok(None),
                        () = producer_expired(authorization.as_ref()) => return Ok(Some(expired())),
                        result = events.reserve() => result,
                    };
                    let Ok(capacity) = capacity else { return Ok(None); };
                    capacity.send(Event::ClearHeld { source: identity.clone(), original, reply });
                    tokio::select! {
                        biased;
                        close = closing.recv() => return Ok(close_error(close)),
                        () = sender.on_detach() => return Ok(None),
                        () = producer_expired(authorization.as_ref()) => return Ok(Some(expired())),
                        result = cleared => { if result.is_err() { return Ok(None); } },
                    }
                    drop(sent.take());
                    break;
                }

                let (reply, completion) = oneshot::channel();
                let capacity = tokio::select! {
                    biased;
                    close = closing.recv() => return Ok(close_error(close)),
                    () = sender.on_detach() => return Ok(None),
                    () = producer_expired(authorization.as_ref()) => return Ok(Some(expired())),
                    result = events.reserve() => result,
                };
                let Ok(capacity) = capacity else { return Ok(None); };
                let Some(TransactionalDisposition::Retirement(receipt)) = pending_disposition.take() else {
                    return Err(std::io::Error::other("consumer retirement disposition is unavailable").into());
                };
                capacity.send(Event::Retirement { source: identity.clone(), receipt, reply });
                let completed = tokio::select! {
                    biased;
                    close = closing.recv() => return Ok(close_error(close)),
                    () = sender.on_detach() => return Ok(None),
                    () = producer_expired(authorization.as_ref()) => return Ok(Some(expired())),
                    result = completion => result,
                };
                match completed {
                    Ok(RetirementCompletion::Rearmed) => continue,
                    Ok(RetirementCompletion::Committed) => {
                        drop(sent.take());
                        break;
                    }
                    Ok(RetirementCompletion::Refused(error)) => return Ok(Some(error)),
                    Err(_) => return Ok(None),
                }
            }
        }
    }.await;
    report_stop(&events, &identity, &mut stop_reported).await;
    drop(pending_disposition);
    drop(sent);
    drop(delivery);
    let closed = match &result {
        Ok(Some(error)) => sender.close_with_error(error.clone()).await,
        _ => sender.close().await,
    };
    complete_close(result, closed)
}

fn expired() -> AmqpProtocolError {
    unauthorized_error("the link's authorization has expired")
}

async fn report_stop(
    events: &mpsc::Sender<Event>,
    identity: &NativeSenderIdentity,
    reported: &mut bool,
) {
    if !*reported {
        stopped(events, WorkerIdentity::Consumer(identity.clone())).await;
        *reported = true;
    }
}

async fn settle_ordinary<B: Broker>(
    broker: &BoundBroker<B>,
    namespace: &domain::NamespaceName,
    entity: &domain::EntityPath,
    sequence: domain::SequenceNumber,
    token: domain::LockToken,
    pending: PendingSettlement,
) -> Result<Option<AmqpProtocolError>, IngressError> {
    let kind = match settlement_command(sequence, token, pending.outcome().clone()) {
        Ok(kind) => kind,
        Err(error) => {
            let error = error_for(AmqpError::InvalidField, error.to_string());
            pending.reject(error.clone()).await?;
            return Ok(Some(error));
        }
    };
    match broker.submit(namespace.clone(), entity.clone(), kind).await {
        Ok(_) => {
            pending.accept().await?;
            Ok(None)
        }
        Err(rejection) => {
            let error = rejection_error(&rejection);
            pending.reject(error.clone()).await?;
            Ok(Some(error))
        }
    }
}
