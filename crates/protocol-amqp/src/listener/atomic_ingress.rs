//! Explicit bounded native ingress; ordinary listeners retain their refusal policy.

use std::{future::Future, pin::Pin, sync::Arc};

use amqp::{
    CoordinatorRequest, Error as AmqpProtocolError, NativeConnectionIdentity,
    NativeControllerIdentity, NativeOutgoingDeliveryIdentity, NativeReceiverIdentity,
    NativeSenderIdentity, TransactionPostingReceipt, TransactionRetirementReceipt,
};
use domain::{EntityBinding, QueueConfig};
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    task::JoinSet,
};

use super::LinkAuthorization;
use groups::{HeldDelivery, RetirementCompletion};

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

#[derive(Clone, Copy, PartialEq, Eq)]
enum IngressMode {
    Posting,
    Messaging,
}

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
    Consumer(NativeSenderIdentity),
}

enum Event {
    RegisterConsumer {
        identity: NativeSenderIdentity,
        admission: QueueAdmission,
        authorization: Option<LinkAuthorization>,
        close: mpsc::Sender<WorkerClose>,
        reply: oneshot::Sender<Result<(), AmqpProtocolError>>,
    },
    RegisterHeld {
        source: NativeSenderIdentity,
        delivery: HeldDelivery,
        reply: oneshot::Sender<Result<(), AmqpProtocolError>>,
    },
    Retirement {
        source: NativeSenderIdentity,
        receipt: TransactionRetirementReceipt,
        reply: oneshot::Sender<RetirementCompletion>,
    },
    ClearHeld {
        source: NativeSenderIdentity,
        original: NativeOutgoingDeliveryIdentity,
        reply: oneshot::Sender<()>,
    },
    RegisterProducer {
        identity: NativeReceiverIdentity,
        admission: QueueAdmission,
        authorization: Option<LinkAuthorization>,
        close: mpsc::Sender<WorkerClose>,
        reply: oneshot::Sender<Result<(), AmqpProtocolError>>,
    },
    RegisterController {
        identity: NativeControllerIdentity,
        authorization: Option<Arc<crate::authorization::ConnectionAuthorization>>,
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
type SessionAdmission =
    Pin<Box<dyn Future<Output = Result<amqp::ServerSession, amqp::EngineError>> + Send>>;

pub(super) async fn serve_atomic_posting_connection<B: crate::NativeAtomicBroker>(
    connection: &mut amqp::ServerConnection,
    namespace: domain::NamespaceName,
    broker: B,
    authorization: Option<Arc<crate::authorization::ConnectionAuthorization>>,
) -> Result<(), IngressError> {
    driver(
        connection,
        namespace,
        broker,
        authorization,
        IngressMode::Posting,
    )
    .await
}

pub(super) async fn serve_atomic_messaging_connection<B: crate::NativeAtomicBroker>(
    connection: &mut amqp::ServerConnection,
    namespace: domain::NamespaceName,
    broker: B,
    authorization: Option<Arc<crate::authorization::ConnectionAuthorization>>,
) -> Result<(), IngressError> {
    driver(
        connection,
        namespace,
        broker,
        authorization,
        IngressMode::Messaging,
    )
    .await
}

struct Driver<B: crate::NativeAtomicBroker> {
    owner: owner::Owner<B>,
    sessions: JoinSet<Result<(), IngressError>>,
    admissions: FuturesUnordered<SessionAdmission>,
    events: mpsc::Sender<Event>,
    incoming: mpsc::Receiver<Event>,
    mode: IngressMode,
}

impl<B: crate::NativeAtomicBroker> Driver<B> {
    #[cfg(test)]
    fn new(connection: NativeConnectionIdentity, broker: B) -> Self {
        Self::with_mode(connection, broker, IngressMode::Posting)
    }

    fn with_mode(connection: NativeConnectionIdentity, broker: B, mode: IngressMode) -> Self {
        let (events, incoming) = mpsc::channel(EVENT_CAPACITY);
        Self {
            owner: owner::Owner::new(connection, broker),
            sessions: JoinSet::new(),
            admissions: FuturesUnordered::new(),
            events,
            incoming,
            mode,
        }
    }

    fn session_count(&self) -> usize {
        self.sessions.len() + self.admissions.len()
    }
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
    mode: IngressMode,
) -> Result<(), IngressError> {
    let driver = Driver::with_mode(
        connection.connection_identity().clone(),
        broker.clone(),
        mode,
    );
    run_driver(connection, namespace, broker, authorization, driver).await
}

async fn run_driver<B: crate::NativeAtomicBroker>(
    connection: &mut amqp::ServerConnection,
    namespace: domain::NamespaceName,
    broker: B,
    authorization: Option<Arc<crate::authorization::ConnectionAuthorization>>,
    mut driver: Driver<B>,
) -> Result<(), IngressError> {
    let links = Arc::new(Semaphore::new(MAX_LINKS));
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
    if driver.mode == IngressMode::Messaging
        && let (Some(authorization), Some(deadline)) =
            (authorization.as_ref(), authorization_deadline)
    {
        authorization.enable_initial_control_grace(deadline).await;
    }
    let mut awaiting_authorization = authorization.is_some();

    loop {
        tokio::select! {
            event = driver.incoming.recv() => {
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
            result = driver.admissions.next(), if !driver.admissions.is_empty() => {
                match result {
                    Some(Ok(session)) => {
                        driver.sessions.spawn(routing::serve_session(
                            session,
                            namespace.clone(),
                            broker.clone(),
                            authorization.clone(),
                            driver.events.clone(),
                            Arc::clone(&links),
                            driver.mode,
                        ));
                    }
                    Some(Err(amqp::EngineError::RemoteDetached)) | None => {}
                    Some(Err(error)) => return Err(error.into()),
                }
            }
            _ = tick.tick() => {
                driver.owner.tick();
                if !connection.connection_identity().is_active() {
                    break;
                }
                let initial_expired = if driver.mode == IngressMode::Messaging {
                    if let Some(authorization) = authorization.as_ref() {
                        matches!(
                            authorization.initial_control_state().await,
                            crate::authorization::InitialControlState::InitialExpired,
                        )
                    } else {
                        false
                    }
                } else {
                    false
                };
                if initial_expired {
                    driver.owner.close();
                    connection.close_with_error(super::unauthorized_error(
                        "no CBS token was supplied before the authorization deadline",
                    )).await?;
                    return Ok(());
                }
                if driver.mode == IngressMode::Posting && awaiting_authorization {
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
                if driver.session_count() >= MAX_SESSIONS {
                    driver.owner.close();
                    connection.close_with_error(super::error_for(
                        amqp::AmqpError::ResourceLimitExceeded,
                        "atomic posting ingress session limit reached".to_owned(),
                    )).await?;
                    return Ok(());
                }
                // Pending admission shares the collector budget without stalling its owner.
                driver.admissions.push(Box::pin(connection.accept_session(session)));
            }
        }
    }

    driver.owner.close();
    match connection.close().await {
        Ok(()) | Err(amqp::EngineError::RemoteClosed | amqp::EngineError::Stopped) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
