use std::sync::Arc;

use amqp::{
    CoordinatorEndpoint, EngineError, Error as AmqpProtocolError, ErrorCondition, RetainedDelivery,
    TransactionalIngress, TransactionalReceiver,
};
use serde_amqp::primitives::Symbol;
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};

use super::{Event, IngressError, QueueAdmission, WorkerClose, WorkerIdentity};
use crate::{
    NativeAtomicBroker,
    authorization::ConnectionAuthorization,
    batch::read_ingress,
    listener::{LinkAuthorization, rejection_error, unauthorized_error},
};

pub(super) async fn producer<B: NativeAtomicBroker>(
    mut receiver: TransactionalReceiver,
    admission: QueueAdmission,
    broker: B,
    authorization: Option<LinkAuthorization>,
    events: mpsc::Sender<Event>,
    _permit: OwnedSemaphorePermit,
) -> Result<(), IngressError> {
    let identity = receiver.receiver_identity();
    let (close, mut closing) = mpsc::channel(1);
    let (reply, registered) = oneshot::channel();
    let registration = Event::RegisterProducer {
        identity: identity.clone(),
        admission: QueueAdmission {
            binding: admission.binding.clone(),
            config: admission.config,
        },
        authorization: authorization.clone(),
        close,
        reply,
    };
    let mut pending_ingress = None;
    let result = async {
        let registration = tokio::select! {
            biased;
            () = producer_expired(authorization.as_ref()) => {
                return Ok(Some(unauthorized_error("the link's authorization has expired")));
            }
            result = events.send(registration) => result,
        };
        if registration.is_err() {
            return Ok(None);
        }
        let registered = tokio::select! {
            biased;
            close = closing.recv() => return Ok(close_error(close)),
            () = producer_expired(authorization.as_ref()) => {
                return Ok(Some(unauthorized_error("the link's authorization has expired")));
            }
            result = registered => result,
        };
        match registered {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Ok(Some(error)),
            Err(_) => return Ok(None),
        }

        loop {
            let ingress = tokio::select! {
                biased;
                close = closing.recv() => return Ok(close_error(close)),
                () = producer_expired(authorization.as_ref()) => {
                    return Ok(Some(unauthorized_error("the link's authorization has expired")));
                }
                received = receiver.recv() => received,
            };
            pending_ingress = Some(match ingress {
                Ok(ingress) => ingress,
                Err(
                    EngineError::RemoteClosed | EngineError::RemoteDetached | EngineError::Stopped,
                ) => return Ok(None),
                Err(error) => return Err(error.into()),
            });
            if let Some(authorization) = authorization.as_ref()
                && let Err(error) = authorization.ensure().await
            {
                return Ok(Some(error));
            }
            if let Some(TransactionalIngress::Ordinary(receipt)) = pending_ingress.as_ref() {
                ordinary(&receiver, &broker, &admission, receipt).await?;
                drop(pending_ingress.take());
                continue;
            }
            let capacity = tokio::select! {
                biased;
                close = closing.recv() => return Ok(close_error(close)),
                () = producer_expired(authorization.as_ref()) => {
                    return Ok(Some(unauthorized_error("the link's authorization has expired")));
                }
                result = events.reserve() => result,
            };
            let Ok(capacity) = capacity else {
                return Ok(None);
            };
            if let Some(TransactionalIngress::Posting(receipt)) = pending_ingress.take() {
                capacity.send(Event::Posting {
                    source: identity.clone(),
                    receipt,
                });
            }
        }
    }
    .await;
    // Publish pending invalidation before waiting for the detach flush.
    stopped(&events, WorkerIdentity::Producer(identity)).await;
    drop(pending_ingress);
    finish_receiver(&receiver, result).await
}

pub(super) async fn controller(
    mut endpoint: CoordinatorEndpoint,
    authorization: Option<Arc<ConnectionAuthorization>>,
    events: mpsc::Sender<Event>,
    _permit: OwnedSemaphorePermit,
) -> Result<(), IngressError> {
    let identity = endpoint.controller_identity().clone();
    let (close, mut closing) = mpsc::channel(1);
    let (reply, registered) = oneshot::channel();
    let mut pending_control = None;
    let result = async {
        let registration = Event::RegisterController { identity: identity.clone(), close, reply };
        let sent = tokio::select! {
            biased;
            () = controller_expired(authorization.as_ref()) => {
                return Ok(Some(unauthorized_error("the coordinator's authorization has expired")));
            }
            result = events.send(registration) => result,
        };
        if sent.is_err() { return Ok(None) }
        let registered = tokio::select! {
            biased;
            close = closing.recv() => return Ok(close_error(close)),
            () = controller_expired(authorization.as_ref()) => {
                return Ok(Some(unauthorized_error("the coordinator's authorization has expired")));
            }
            result = registered => result,
        };
        match registered {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Ok(Some(error)),
            Err(_) => return Ok(None),
        }
        loop {
            let request = tokio::select! {
                biased;
                close = closing.recv() => return Ok(close_error(close)),
                () = controller_expired(authorization.as_ref()) => {
                    return Ok(Some(unauthorized_error("the coordinator's authorization has expired")));
                }
                request = endpoint.recv() => request,
            };
            pending_control = Some(match request {
                Ok(request) => request,
                Err(EngineError::RemoteClosed | EngineError::RemoteDetached | EngineError::Stopped) => return Ok(None),
                Err(error) => return Err(error.into()),
            });
            if let Some(authorization) = authorization.as_ref()
                && !authorization.has_valid_grant().await
            {
                return Ok(Some(unauthorized_error("the coordinator's authorization has expired")));
            }
            let capacity = tokio::select! {
                biased;
                close = closing.recv() => return Ok(close_error(close)),
                () = controller_expired(authorization.as_ref()) => {
                    return Ok(Some(unauthorized_error("the coordinator's authorization has expired")));
                }
                result = events.reserve() => result,
            };
            let Ok(capacity) = capacity else { return Ok(None) };
            if let Some(request) = pending_control.take() {
                capacity.send(Event::Control { source: identity.clone(), request });
            }
        }
    }.await;
    stopped(&events, WorkerIdentity::Controller(identity)).await;
    drop(pending_control);
    let closed = match &result {
        Ok(Some(error)) => endpoint.close_with_error(error.clone()).await,
        _ => endpoint.close().await,
    };
    complete_close(result, closed)
}

async fn stopped(events: &mpsc::Sender<Event>, source: WorkerIdentity) {
    let (reply, stopped) = oneshot::channel();
    if events
        .send(Event::WorkerStopped { source, reply })
        .await
        .is_ok()
    {
        let _ = stopped.await;
    }
}

async fn ordinary<B: NativeAtomicBroker>(
    receiver: &TransactionalReceiver,
    broker: &B,
    admission: &QueueAdmission,
    receipt: &RetainedDelivery,
) -> Result<(), IngressError> {
    let kind = match read_ingress(receipt.message(), receipt.message_format()) {
        Ok(kind) => kind,
        Err(error) => {
            receiver
                .reject_retained(
                    receipt,
                    Some(AmqpProtocolError::new(
                        ErrorCondition::Custom(Symbol::from(error.condition())),
                        error.to_string(),
                        None,
                    )),
                )
                .await?;
            return Ok(());
        }
    };
    let outcome = broker
        .submit_fenced(
            admission.binding.clone(),
            admission.binding.target().clone(),
            kind,
        )
        .await;
    match outcome {
        Ok(_) => receiver.accept_retained(receipt).await?,
        Err(error) => {
            receiver
                .reject_retained(receipt, Some(rejection_error(&error)))
                .await?
        }
    }
    Ok(())
}

fn close_error(close: Option<WorkerClose>) -> Option<AmqpProtocolError> {
    match close {
        Some(WorkerClose::Close(error)) => error,
        None => None,
    }
}

async fn producer_expired(authorization: Option<&LinkAuthorization>) {
    match authorization {
        Some(authorization) => authorization.wait_until_unauthorized().await,
        None => std::future::pending().await,
    }
}

async fn controller_expired(authorization: Option<&Arc<ConnectionAuthorization>>) {
    match authorization {
        Some(authorization) => authorization.wait_until_no_valid_grant().await,
        None => std::future::pending().await,
    }
}

async fn finish_receiver(
    receiver: &TransactionalReceiver,
    result: Result<Option<AmqpProtocolError>, IngressError>,
) -> Result<(), IngressError> {
    let closed = match &result {
        Ok(Some(error)) => receiver.close_with_error(error.clone()).await,
        _ => receiver.close().await,
    };
    complete_close(result, closed)
}

fn complete_close(
    result: Result<Option<AmqpProtocolError>, IngressError>,
    closed: Result<(), EngineError>,
) -> Result<(), IngressError> {
    result?;
    match closed {
        Ok(())
        | Err(EngineError::RemoteClosed | EngineError::RemoteDetached | EngineError::Stopped) => {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}
