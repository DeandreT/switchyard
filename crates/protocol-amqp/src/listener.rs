//! The AMQP acceptor: connections in, commands out.
//!
//! One task per connection, session, and link. A link holds no broker state of
//! its own — a lock token it is carrying is the broker's, and if the link dies
//! holding one, the lock simply expires and the message is redelivered. That is
//! what makes an abrupt disconnect safe.

use std::time::Duration;

use domain::{
    AcceptedSession, CommandKind, CommandOutcome, Delivery, EntityPath, NamespaceName, ReceiveMode,
    SessionHold,
};
use amqp_runtime::{
    acceptor::{
        ConnectionAcceptor, LinkAcceptor, LinkEndpoint, ListenerSessionHandle, SessionAcceptor,
    },
    link::{LinkStateError, Receiver, RecvError, Sender},
    types::{
        definitions::{self, AmqpError, Role, SenderSettleMode},
        messaging::{Body, Outcome, TargetArchetype},
        performatives::Attach,
        primitives::Binary,
    },
};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::{
    Attachment, Broker, BrokerRejection, ProtocolError, SessionRequest, parse_attachment,
    read_incoming, read_session_filter, stamp_session_filter,
};

/// How long a receiving link waits on a wakeup before asking the broker anyway.
///
/// The wakeup is the mechanism; this is the net under it. A notification can be
/// lost when several links wait on one entity, so a waiter re-asks on a coarse
/// interval rather than trusting the signal absolutely.
const EMPTY_QUEUE_FALLBACK: Duration = Duration::from_secs(3);

/// How often a link that holds a session renews its lock.
///
/// The broker renews on the receiver's behalf while the link is open, because
/// the management operations the SDKs renew through do not exist yet. Well
/// under the shortest configurable lock a client is likely to use.
const SESSION_RENEW_INTERVAL: Duration = Duration::from_secs(15);

pub struct AmqpListener<B> {
    broker: B,
    namespace: NamespaceName,
    container_id: String,
}

impl<B: Broker> AmqpListener<B> {
    pub fn new(broker: B, namespace: NamespaceName) -> Self {
        Self {
            broker,
            namespace,
            container_id: String::from("switchyard"),
        }
    }

    /// Accepts connections until the listener fails.
    ///
    /// A connection that fails takes only itself down: one client's protocol
    /// error is not the node's.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (stream, peer) = listener.accept().await?;
            debug!(%peer, "connection accepted");

            let broker = self.broker.clone();
            let namespace = self.namespace.clone();
            let container_id = self.container_id.clone();
            tokio::spawn(async move {
                if let Err(error) = serve_connection(stream, container_id, namespace, broker).await
                {
                    warn!(%peer, %error, "connection ended");
                }
            });
        }
    }
}

async fn serve_connection<B: Broker>(
    stream: TcpStream,
    container_id: String,
    namespace: NamespaceName,
    broker: B,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut connection = ConnectionAcceptor::new(container_id).accept(stream).await?;

    while let Some(incoming) = connection.next_incoming_session().await {
        let session = SessionAcceptor::default()
            .accept_incoming_session(incoming, &mut connection)
            .await?;
        let broker = broker.clone();
        let namespace = namespace.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_session(session, namespace, broker).await {
                warn!(%error, "session ended");
            }
        });
    }

    // The loop ends when the connection is closing. A client that closed first
    // still gets the answering close from the engine; reporting its hang-up as
    // this node's error would make every clean disconnect look like a failure.
    match connection.close().await {
        Ok(()) | Err(amqp_runtime::connection::Error::RemoteClosed) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn serve_session<B: Broker>(
    mut session: ListenerSessionHandle,
    namespace: NamespaceName,
    broker: B,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let acceptor = LinkAcceptor::default();

    while let Some(mut attach) = session.next_incoming_attach().await {
        // The address has to be read before the attach is consumed. A client
        // sending names its entity in the target; one receiving names it in the
        // source.
        let address = attach
            .target
            .as_ref()
            .and_then(|target| match target.as_ref() {
                TargetArchetype::Target(target) => target.address.clone(),
            })
            .or_else(|| {
                attach
                    .source
                    .as_ref()
                    .and_then(|source| source.address.clone())
            })
            .unwrap_or_default();

        // A receiver that asks for pre-settled transfers is asking for
        // at-most-once: the broker deletes before sending and a lost transfer
        // stays lost. Anything else gets peek-lock.
        let mode = match attach.snd_settle_mode {
            SenderSettleMode::Settled => ReceiveMode::ReceiveAndDelete,
            SenderSettleMode::Unsettled | SenderSettleMode::Mixed => ReceiveMode::PeekLock,
        };

        // Everything that can refuse the link is decided before the attach is
        // accepted, so a granted session can be stamped into the source the
        // acceptor echoes — the echo is how a next-available receiver learns
        // which session it got.
        let plan = plan_link(&broker, &namespace, &address, &attach).await;
        if let Ok((_, Some(accepted))) = &plan
            && let Some(source) = attach.source.as_deref_mut()
        {
            stamp_session_filter(source, &accepted.session_id);
        }

        let endpoint = acceptor
            .accept_incoming_attach(attach, &mut session)
            .await?;
        let (entity, accepted) = match plan {
            Ok(plan) => plan,
            Err(error) => {
                // Refusing the link rather than the connection: another link on
                // the same session may be perfectly valid.
                warn!(%address, condition = ?error.condition, "refusing link");
                detach_with(endpoint, error).await;
                continue;
            }
        };

        info!(%address, entity = %entity, session = accepted.as_ref().map(|accepted| accepted.session_id.as_str()), "link attached");
        let broker = broker.clone();
        let namespace = namespace.clone();
        match endpoint {
            // The client sends; this end receives.
            LinkEndpoint::Receiver(receiver) => {
                tokio::spawn(async move {
                    if let Err(error) =
                        serve_sending_client(receiver, namespace, entity, broker).await
                    {
                        warn!(%error, "sending link ended");
                    }
                });
            }
            // The client receives; this end sends.
            LinkEndpoint::Sender(sender) => {
                let hold = accepted.map(|accepted| accepted.hold());
                tokio::spawn(async move {
                    if let Err(error) =
                        serve_receiving_client(sender, namespace, entity, broker, mode, hold).await
                    {
                        warn!(%error, "receiving link ended");
                    }
                });
            }
        }
    }
    Ok(())
}

/// Everything an attach needs decided before it is answered: the entity it
/// reaches, and the session lock it holds if its source asked for one.
async fn plan_link<B: Broker>(
    broker: &B,
    namespace: &NamespaceName,
    address: &str,
    attach: &Attach,
) -> Result<(EntityPath, Option<AcceptedSession>), definitions::Error> {
    let entity = resolve_entity(address)
        .map_err(|error| error_for(AmqpError::InvalidField, error.to_string()))?;

    // Only a receiving link takes a session lock; a sender names a session per
    // message instead.
    if attach.role != Role::Receiver {
        return Ok((entity, None));
    }
    let session_id = match read_session_filter(attach.source.as_deref())
        .map_err(|error| error_for(AmqpError::InvalidField, error.to_string()))?
    {
        SessionRequest::None => return Ok((entity, None)),
        SessionRequest::NextAvailable => None,
        SessionRequest::Named(session_id) => Some(session_id),
    };

    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::AcceptSession {
                session_id,
                lock_duration_millis: None,
            },
        )
        .await
    {
        Ok(CommandOutcome::SessionAccepted(Some(accepted))) => Ok((entity, Some(accepted))),
        // Nothing to grant is what Service Bus reports as a timeout: the client
        // did nothing wrong and simply asks again.
        Ok(CommandOutcome::SessionAccepted(None)) => Err(definitions::Error::new(
            definitions::ErrorCondition::Custom(crate::TIMEOUT.into()),
            String::from("no session is available to accept"),
            None,
        )),
        Ok(other) => Err(error_for(
            AmqpError::InternalError,
            format!("accepting a session produced an unexpected outcome: {other:?}"),
        )),
        Err(rejection) => Err(rejection_error(&rejection)),
    }
}

/// The entity a link may attach to, or why it may not.
fn resolve_entity(address: &str) -> Result<EntityPath, ProtocolError> {
    match parse_attachment(address)? {
        Attachment::Queue(entity) => Ok(entity),
        Attachment::DeadLetter(entity) => Err(ProtocolError::InvalidAddress {
            address: address.to_owned(),
            detail: format!("receiving from the dead-letter queue of {entity} is not implemented"),
        }),
        Attachment::Subscription { topic, .. } => Err(ProtocolError::InvalidAddress {
            address: address.to_owned(),
            detail: format!("subscriptions of topic {topic} are not implemented"),
        }),
    }
}

/// Drives a link the client sends on: every transfer becomes one send command.
async fn serve_sending_client<B: Broker>(
    mut receiver: Receiver,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: B,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let delivery = match receiver.recv::<Body<Binary>>().await {
            Ok(delivery) => delivery,
            // The client hung up. Its detach is waiting for an answer, and a
            // dropped handle would leave it waiting; closing sends it.
            Err(RecvError::LinkStateError(
                LinkStateError::RemoteClosed
                | LinkStateError::RemoteDetached
                | LinkStateError::RemoteClosedWithError(_)
                | LinkStateError::RemoteDetachedWithError(_),
            )) => {
                let _ = receiver.close().await;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        let incoming = match read_incoming(delivery.message()) {
            Ok(incoming) => incoming,
            Err(error) => {
                // The client's message, the client's fault: reject this transfer
                // and keep the link.
                receiver
                    .reject(
                        &delivery,
                        error_for(AmqpError::InvalidField, error.to_string()),
                    )
                    .await?;
                continue;
            }
        };

        let outcome = broker
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::Send {
                    message_id: incoming.message_id,
                    body: incoming.body,
                    time_to_live_millis: incoming.time_to_live_millis,
                    session_id: incoming.session_id,
                },
            )
            .await;

        // Accepting only after the command committed is what makes the
        // acknowledgement mean the message is durable.
        match outcome {
            Ok(_) => receiver.accept(&delivery).await?,
            Err(rejection) => {
                receiver
                    .reject(&delivery, rejection_error(&rejection))
                    .await?
            }
        }
    }
}

/// Drives a link the client receives on: fetch, deliver, then settle as the
/// client's disposition says.
async fn serve_receiving_client<B: Broker>(
    mut sender: Sender,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: B,
    mode: ReceiveMode,
    session: Option<SessionHold>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut renew = tokio::time::interval(SESSION_RENEW_INTERVAL);
    renew.tick().await; // the first tick is immediate, and the lock is fresh

    loop {
        // The link is watched the whole time a message is being waited for. A
        // client that detaches while the queue is empty is waiting for an
        // answer, and a task that only polls the broker would never send one.
        let fetched = tokio::select! {
            biased;
            _ = sender.on_detach() => {
                release_session(&broker, &namespace, &entity, session.as_ref()).await;
                let _ = sender.close().await;
                return Ok(());
            }
            _ = renew.tick(), if session.is_some() => {
                let Some(hold) = session.as_ref() else { continue };
                match broker
                    .submit(
                        namespace.clone(),
                        entity.clone(),
                        CommandKind::RenewSessionLock {
                            session: hold.clone(),
                            lock_duration_millis: None,
                        },
                    )
                    .await
                {
                    Ok(_) => continue,
                    // The lock is already gone, so there is nothing to release.
                    Err(rejection) => {
                        sender.close_with_error(rejection_error(&rejection)).await?;
                        return Ok(());
                    }
                }
            }
            fetched = next_delivery(&broker, &namespace, &entity, mode, session.as_ref()) => fetched,
        };

        match fetched {
            Ok(delivery) => settle(&mut sender, &namespace, &entity, &broker, delivery).await?,
            Err(rejection) => {
                release_session(&broker, &namespace, &entity, session.as_ref()).await;
                sender.close_with_error(rejection_error(&rejection)).await?;
                return Ok(());
            }
        }
    }
}

/// Frees the session a link held, so the next receiver need not wait out the
/// lock. Failure is survivable: expiry frees it anyway.
async fn release_session<B: Broker>(
    broker: &B,
    namespace: &NamespaceName,
    entity: &EntityPath,
    session: Option<&SessionHold>,
) {
    let Some(hold) = session else { return };
    if let Err(rejection) = broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::ReleaseSession {
                session: hold.clone(),
            },
        )
        .await
    {
        debug!(session = %hold.session_id, %rejection, "session not released, leaving it to expire");
    }
}

/// The next message the queue will part with, however long that takes.
async fn next_delivery<B: Broker>(
    broker: &B,
    namespace: &NamespaceName,
    entity: &EntityPath,
    mode: ReceiveMode,
    session: Option<&SessionHold>,
) -> Result<Delivery, BrokerRejection> {
    loop {
        // Armed before the receive: a message that lands between the empty
        // answer below and the wait leaves a stored notification, so the wait
        // returns at once instead of sleeping on a queue that is not empty.
        let wakeup = broker.deliverable(namespace, entity);
        let outcome = broker
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::Receive {
                    mode,
                    lock_duration_millis: None,
                    session: session.cloned(),
                },
            )
            .await?;

        match outcome {
            CommandOutcome::Received(Some(delivery)) => return Ok(delivery),
            CommandOutcome::Received(None) => {
                tokio::select! {
                    () = wakeup => {}
                    () = tokio::time::sleep(EMPTY_QUEUE_FALLBACK) => {}
                }
            }
            other => {
                // A receive that produced anything else means the broker and the
                // edge disagree about the command, which is not a client problem.
                return Err(BrokerRejection::Unavailable(format!(
                    "receive produced an unexpected outcome: {other:?}"
                )));
            }
        }
    }
}

/// Hands one message to the client and applies whatever it said about it.
///
/// The lock is already committed, so a client that never answers costs a
/// redelivery rather than a lost message.
async fn settle<B: Broker>(
    sender: &mut Sender,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    delivery: Delivery,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(lock) = delivery.lock else {
        // Receive-and-delete: the message is already gone, so there is nothing
        // to settle after the transfer.
        sender.send(crate::write_delivery(&delivery)).await?;
        return Ok(());
    };
    let sequence = delivery.sequence;
    let outcome = sender.send(crate::write_delivery(&delivery)).await?;

    let kind = match outcome {
        Outcome::Accepted(_) => CommandKind::Complete {
            sequence,
            lock_token: lock.token,
        },
        // Rejected means the client will never process it, so it goes to the
        // dead-letter queue rather than round again.
        Outcome::Rejected(rejected) => CommandKind::DeadLetter {
            sequence,
            lock_token: lock.token,
            reason: String::from("RejectedByReceiver"),
            description: rejected
                .error
                .and_then(|error| error.description)
                .unwrap_or_else(|| String::from("the receiver rejected the message")),
        },
        // Released and modified both mean "not now": back to the queue, with the
        // delivery count already incremented by the receive.
        Outcome::Released(_) | Outcome::Modified(_) => CommandKind::Abandon {
            sequence,
            lock_token: lock.token,
        },
    };

    if let Err(rejection) = broker.submit(namespace.clone(), entity.clone(), kind).await {
        // A settlement that fails is not fatal to the link: the lock expires and
        // the message comes round again.
        warn!(%sequence, %rejection, "settlement failed, leaving the lock to expire");
    }
    Ok(())
}

async fn detach_with(endpoint: LinkEndpoint, error: definitions::Error) {
    match endpoint {
        LinkEndpoint::Sender(sender) => {
            let _ = sender.close_with_error(error).await;
        }
        LinkEndpoint::Receiver(receiver) => {
            let _ = receiver.close_with_error(error).await;
        }
    }
}

fn error_for(condition: AmqpError, description: String) -> definitions::Error {
    definitions::Error::new(condition, description, None)
}

/// The wire error a broker rejection becomes, carrying the condition an SDK
/// keys its behaviour off.
fn rejection_error(rejection: &BrokerRejection) -> definitions::Error {
    definitions::Error::new(
        definitions::ErrorCondition::Custom(rejection.condition().into()),
        rejection.to_string(),
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_plain_queue_address_attaches() {
        assert_eq!(
            resolve_entity("orders").expect("a queue attaches").as_str(),
            "orders"
        );
        // Recognised and refused, rather than treated as a queue of that name.
        for address in [
            "orders/$deadletterqueue",
            "billing/Subscriptions/accounting",
        ] {
            assert!(
                resolve_entity(address).is_err(),
                "{address} should not attach yet"
            );
        }
    }

    #[test]
    fn a_rejection_reaches_the_wire_as_its_condition() {
        let rejection = BrokerRejection::Refused(domain::BrokerError::QueueNotFound);
        let error = rejection_error(&rejection);
        assert_eq!(
            error.condition,
            definitions::ErrorCondition::Custom(crate::NOT_FOUND.into())
        );
    }
}
