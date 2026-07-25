//! The AMQP acceptor: connections in, commands out.
//!
//! One task per connection, session, and link. A link holds no broker state of
//! its own — a lock token it is carrying is the broker's, and if the link dies
//! holding one, the lock simply expires and the message is redelivered. That is
//! what makes an abrupt disconnect safe.

use std::time::Duration;

use domain::{CommandKind, CommandOutcome, Delivery, EntityPath, NamespaceName, ReceiveMode};
use amqp_runtime::{
    acceptor::{
        ConnectionAcceptor, LinkAcceptor, LinkEndpoint, ListenerSessionHandle, SessionAcceptor,
    },
    link::{LinkStateError, Receiver, RecvError, Sender},
    types::{
        definitions::{self, AmqpError},
        messaging::{Body, Outcome, TargetArchetype},
        primitives::Binary,
    },
};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::{Attachment, Broker, BrokerRejection, ProtocolError, parse_attachment, read_incoming};

/// How long a receiving link waits before asking again once a queue came back
/// empty.
///
/// Polling is crude. It is here because the state machine has no way to say "a
/// message arrived" yet; when it does, this becomes a wait on that signal.
const EMPTY_QUEUE_BACKOFF: Duration = Duration::from_millis(50);

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

    while let Some(attach) = session.next_incoming_attach().await {
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

        let endpoint = acceptor
            .accept_incoming_attach(attach, &mut session)
            .await?;
        let entity = match resolve_entity(&address) {
            Ok(entity) => entity,
            Err(error) => {
                // Refusing the link rather than the connection: another link on
                // the same session may be perfectly valid.
                warn!(%address, %error, "refusing link");
                detach_with(endpoint, AmqpError::InvalidField, error.to_string()).await;
                continue;
            }
        };

        info!(%address, entity = %entity, "link attached");
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
                tokio::spawn(async move {
                    if let Err(error) =
                        serve_receiving_client(sender, namespace, entity, broker).await
                    {
                        warn!(%error, "receiving link ended");
                    }
                });
            }
        }
    }
    Ok(())
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        // The link is watched the whole time a message is being waited for. A
        // client that detaches while the queue is empty is waiting for an
        // answer, and a task that only polls the broker would never send one.
        let fetched = tokio::select! {
            biased;
            _ = sender.on_detach() => {
                let _ = sender.close().await;
                return Ok(());
            }
            fetched = next_delivery(&broker, &namespace, &entity) => fetched,
        };

        match fetched {
            Ok(delivery) => settle(&mut sender, &namespace, &entity, &broker, delivery).await?,
            Err(rejection) => {
                sender.close_with_error(rejection_error(&rejection)).await?;
                return Ok(());
            }
        }
    }
}

/// The next message the queue will part with, however long that takes.
async fn next_delivery<B: Broker>(
    broker: &B,
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> Result<Delivery, BrokerRejection> {
    loop {
        let outcome = broker
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            )
            .await?;

        match outcome {
            CommandOutcome::Received(Some(delivery)) => return Ok(delivery),
            CommandOutcome::Received(None) => tokio::time::sleep(EMPTY_QUEUE_BACKOFF).await,
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

async fn detach_with(endpoint: LinkEndpoint, condition: AmqpError, description: String) {
    let error = error_for(condition, description);
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
