//! Explicit posting-only ingress; ordinary listeners retain their refusal policy.

use std::sync::Arc;

use amqp::{
    CoordinatorRequest, Error as AmqpProtocolError, NativeConnectionIdentity,
    NativeControllerIdentity, NativeReceiverIdentity, TransactionPostingReceipt,
};
use domain::{EntityBinding, QueueConfig};
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    task::JoinSet,
};

use super::LinkAuthorization;

mod groups;
mod operations;
mod owner;
mod routing;
mod workers;

#[cfg(test)]
mod tests;

const MAX_SESSIONS: usize = 32;
const MAX_LINKS: usize = 128;
const EVENT_CAPACITY: usize = 256;

struct QueueAdmission {
    binding: EntityBinding,
    config: QueueConfig,
}

enum WorkerClose {
    Close(Option<AmqpProtocolError>),
}

enum WorkerIdentity {
    Controller(NativeControllerIdentity),
    Producer(NativeReceiverIdentity),
}

enum Event {
    RegisterProducer {
        identity: NativeReceiverIdentity,
        admission: QueueAdmission,
        authorization: Option<LinkAuthorization>,
        close: mpsc::Sender<WorkerClose>,
        reply: oneshot::Sender<Result<(), AmqpProtocolError>>,
    },
    RegisterController {
        identity: NativeControllerIdentity,
        close: mpsc::Sender<WorkerClose>,
        reply: oneshot::Sender<Result<(), AmqpProtocolError>>,
    },
    Posting {
        source: NativeReceiverIdentity,
        receipt: TransactionPostingReceipt,
    },
    Control {
        source: NativeControllerIdentity,
        request: CoordinatorRequest,
    },
    WorkerStopped {
        source: WorkerIdentity,
        reply: oneshot::Sender<()>,
    },
    StopConnection {
        reply: oneshot::Sender<()>,
    },
}

fn same_connection(
    connection: &NativeConnectionIdentity,
    actual: Option<&NativeConnectionIdentity>,
) -> bool {
    connection.is_active()
        && actual.is_some_and(|actual| actual.is_active() && connection.same_connection(actual))
}

type IngressError = Box<dyn std::error::Error + Send + Sync>;

pub(super) async fn serve_atomic_posting_connection<B: crate::NativeAtomicBroker>(
    connection: &mut amqp::ServerConnection,
    namespace: domain::NamespaceName,
    broker: B,
    authorization: Option<Arc<crate::authorization::ConnectionAuthorization>>,
) -> Result<(), IngressError> {
    driver(connection, namespace, broker, authorization).await
}

struct Driver<B: crate::NativeAtomicBroker> {
    owner: owner::Owner<B>,
    sessions: JoinSet<Result<(), IngressError>>,
}

impl<B: crate::NativeAtomicBroker> Drop for Driver<B> {
    fn drop(&mut self) {
        // Cancel pending authority before aborting collectors or releasing receipts.
        self.owner.close();
        self.sessions.abort_all();
    }
}

async fn driver<B: crate::NativeAtomicBroker>(
    connection: &mut amqp::ServerConnection,
    namespace: domain::NamespaceName,
    broker: B,
    authorization: Option<Arc<crate::authorization::ConnectionAuthorization>>,
) -> Result<(), IngressError> {
    let (events, mut incoming) = mpsc::channel(EVENT_CAPACITY);
    let links = Arc::new(Semaphore::new(MAX_LINKS));
    let mut driver = Driver {
        owner: owner::Owner::new(connection.connection_identity().clone(), broker.clone()),
        sessions: JoinSet::new(),
    };
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let authorization_deadline = authorization.as_ref().and_then(|authorization| {
        tokio::time::Instant::now().checked_add(authorization.authorization_timeout())
    });
    if authorization.is_some() && authorization_deadline.is_none() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "the configured authorization timeout cannot be represented",
        )
        .into());
    }
    let mut awaiting_authorization = authorization.is_some();

    loop {
        tokio::select! {
            event = incoming.recv() => {
                let Some(event) = event else { break };
                match event {
                    Event::StopConnection { reply } => {
                        driver.owner.close();
                        let _ = reply.send(());
                        return Err(std::io::Error::other(
                            "an atomic posting ingress worker stopped unexpectedly",
                        ).into());
                    }
                    event => driver.owner.process(event),
                }
            }
            completion = driver.owner.next_operation() => {
                if let Some(completion) = completion {
                    driver.owner.accept_completion(completion);
                }
            }
            result = driver.sessions.join_next(), if !driver.sessions.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => return Err(error),
                    Some(Err(error)) => return Err(error.into()),
                    None => {}
                }
            }
            _ = tick.tick() => {
                driver.owner.tick();
                if !connection.connection_identity().is_active() {
                    break;
                }
                if awaiting_authorization {
                    if let Some(authorization) = authorization.as_ref()
                        && authorization.has_valid_grant().await
                    {
                        awaiting_authorization = false;
                    } else if authorization_deadline.is_some_and(|deadline| {
                        deadline <= tokio::time::Instant::now()
                    }) {
                        driver.owner.close();
                        connection.close_with_error(super::unauthorized_error(
                            "no CBS token was supplied before the authorization deadline",
                        )).await?;
                        return Ok(());
                    }
                }
            }
            session = connection.next_incoming_session() => {
                let Some(session) = session else { break };
                if driver.sessions.len() >= MAX_SESSIONS {
                    driver.owner.close();
                    connection.close_with_error(super::error_for(
                        amqp::AmqpError::ResourceLimitExceeded,
                        "atomic posting ingress session limit reached".to_owned(),
                    )).await?;
                    return Ok(());
                }
                let session = match connection.accept_session(session).await {
                    Ok(session) => session,
                    Err(amqp::EngineError::RemoteDetached) => continue,
                    Err(error) => return Err(error.into()),
                };
                driver.sessions.spawn(routing::serve_session(
                    session,
                    namespace.clone(),
                    broker.clone(),
                    authorization.clone(),
                    events.clone(),
                    Arc::clone(&links),
                ));
            }
        }
    }

    driver.owner.close();
    match connection.close().await {
        Ok(()) | Err(amqp::EngineError::RemoteClosed | amqp::EngineError::Stopped) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
