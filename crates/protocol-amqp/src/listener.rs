//! The AMQP acceptor: connections in, commands out.
//!
//! One task per connection, session, and link. A link holds no broker state of
//! its own — a lock token it is carrying is the broker's, and if the link dies
//! holding one, the lock simply expires and the message is redelivered. That is
//! what makes an abrupt disconnect safe.

use std::{sync::Arc, time::Duration};

use amqp::{
    AmqpError, Attach, DeliveryState, DeliveryTag, EngineError, Error as AmqpProtocolError,
    ErrorCondition, Fields, LinkEndpoint, Outcome, Receiver, Role, Sender, SenderSettleMode,
    ServerConnection, ServerSession,
};
use auth::{Permission, ResourceScope};
use domain::{
    AcceptedSession, CommandKind, CommandOutcome, Delivery, EntityPath, LockToken, NamespaceName,
    ReceiveMode, SessionHold,
};
use rustls::ServerConfig;
use serde_amqp::{Value, primitives::Symbol};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use crate::{
    Attachment, Broker, BrokerRejection, ProtocolError, SessionRequest, SharedAccessAuthentication,
    authorization::{ConnectionAuthorization, SharedAccessSaslAcceptor},
    cbs::{serve_cbs_replies, serve_cbs_requests},
    management::{
        ConnectionManagement, ManagementAuthorization, serve_management_replies,
        serve_management_requests,
    },
    parse_attachment, read_incoming, read_session_filter, stamp_session_filter,
};

/// How long a receiving link waits on a wakeup before asking the broker anyway.
///
/// The wakeup is the mechanism; this is the net under it. A notification can be
/// lost when several links wait on one entity, so a waiter re-asks on a coarse
/// interval rather than trusting the signal absolutely.
const EMPTY_QUEUE_FALLBACK: Duration = Duration::from_secs(3);

const LOCKED_UNTIL_UTC_PROPERTY: &str = "com.microsoft:locked-until-utc";
const DOTNET_UNIX_EPOCH_TICKS: u64 = 621_355_968_000_000_000;
const DOTNET_TICKS_PER_MILLISECOND: u64 = 10_000;

pub struct AmqpListener<B> {
    broker: B,
    namespace: NamespaceName,
    container_id: String,
    tls_acceptor: Option<TlsAcceptor>,
    shared_access_authentication: Option<SharedAccessAuthentication>,
}

impl<B: Broker> AmqpListener<B> {
    pub fn new(broker: B, namespace: NamespaceName) -> Self {
        Self {
            broker,
            namespace,
            container_id: String::from("switchyard"),
            tls_acceptor: None,
            shared_access_authentication: None,
        }
    }

    /// Secures accepted sockets before AMQP and SASL negotiation begin.
    pub fn with_tls(mut self, config: ServerConfig) -> Self {
        self.tls_acceptor = Some(TlsAcceptor::from(std::sync::Arc::new(config)));
        self
    }

    /// Requires SASL MSSBCBS/ANONYMOUS plus CBS, or valid SASL PLAIN
    /// credentials, before entity links are accepted.
    pub fn with_shared_access_authentication(
        mut self,
        authentication: SharedAccessAuthentication,
    ) -> Self {
        self.shared_access_authentication = Some(authentication);
        self
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
            let tls_acceptor = self.tls_acceptor.clone();
            let shared_access_authentication = self.shared_access_authentication.clone();
            tokio::spawn(async move {
                let result = match tls_acceptor {
                    Some(acceptor) => match acceptor.accept(stream).await {
                        Ok(stream) => {
                            debug!(%peer, "TLS established");
                            serve_connection(
                                stream,
                                container_id,
                                namespace,
                                broker,
                                shared_access_authentication,
                            )
                            .await
                        }
                        Err(error) => Err(error.into()),
                    },
                    None => {
                        serve_connection(
                            stream,
                            container_id,
                            namespace,
                            broker,
                            shared_access_authentication,
                        )
                        .await
                    }
                };
                if let Err(error) = result {
                    warn!(%peer, %error, "connection ended");
                }
            });
        }
    }
}

async fn serve_connection<Io, B>(
    stream: Io,
    container_id: String,
    namespace: NamespaceName,
    broker: B,
    shared_access_authentication: Option<SharedAccessAuthentication>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    B: Broker,
{
    let (connection, authorization) = match shared_access_authentication {
        Some(config) => {
            let sasl_acceptor = SharedAccessSaslAcceptor::new(&config);
            let connection = ServerConnection::accept(
                stream,
                container_id,
                Some(Arc::new(sasl_acceptor.clone())),
            )
            .await?;
            let authorization = ConnectionAuthorization::new(config, sasl_acceptor.grant());
            (connection, Some(authorization))
        }
        None => (
            ServerConnection::accept(stream, container_id, None).await?,
            None,
        ),
    };
    serve_open_connection(connection, namespace, broker, authorization).await
}

async fn serve_open_connection<B: Broker>(
    mut connection: ServerConnection,
    namespace: NamespaceName,
    broker: B,
    authorization: Option<Arc<ConnectionAuthorization>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let management = ConnectionManagement::new();
    let mut awaiting_authorization = authorization.is_some();
    let timeout = authorization
        .as_ref()
        .map(|authorization| tokio::time::sleep(authorization.authorization_timeout()));
    tokio::pin!(timeout);

    loop {
        let incoming = if awaiting_authorization {
            let authorization = authorization
                .as_ref()
                .expect("authorization is present while it is awaited");
            tokio::select! {
                biased;
                () = authorization.wait_for_grant() => {
                    awaiting_authorization = false;
                    continue;
                }
                () = async {
                    match timeout.as_mut().as_pin_mut() {
                        Some(timeout) => timeout.await,
                        None => std::future::pending().await,
                    }
                } => {
                    connection
                        .close_with_error(unauthorized_error(
                            "no CBS token was supplied before the authorization deadline",
                        ))
                        .await?;
                    return Ok(());
                }
                incoming = connection.next_incoming_session() => incoming,
            }
        } else {
            connection.next_incoming_session().await
        };
        let Some(incoming) = incoming else { break };

        let session = connection.accept_session(incoming).await?;
        let broker = broker.clone();
        let namespace = namespace.clone();
        let authorization = authorization.clone();
        let management = Arc::clone(&management);
        tokio::spawn(async move {
            if let Err(error) =
                serve_session(session, namespace, broker, authorization, management).await
            {
                warn!(%error, ?error, "session ended");
            }
        });
    }

    // The loop ends when the connection is closing. A client that closed first
    // still gets the answering close from the engine; reporting its hang-up as
    // this node's error would make every clean disconnect look like a failure.
    match connection.close().await {
        Ok(()) | Err(EngineError::RemoteClosed | EngineError::Stopped) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn serve_session<B: Broker>(
    mut session: ServerSession,
    namespace: NamespaceName,
    broker: B,
    authorization: Option<Arc<ConnectionAuthorization>>,
    management: Arc<ConnectionManagement>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    while let Some(mut attach) = session.next_incoming_attach().await {
        let source_address = attach
            .source
            .as_ref()
            .and_then(|source| source.address.clone())
            .unwrap_or_default();
        let target_address = attach
            .target
            .as_ref()
            .and_then(|target| target.address.clone())
            .unwrap_or_default();

        if let Some(authorization) = authorization.as_ref()
            && (target_address == crate::CBS_NODE || source_address == crate::CBS_NODE)
        {
            // The Microsoft duplex CBS link omits this sender field. Azure
            // accepts it as zero, while the AMQP engine enforces the MUST.
            if attach.role == Role::Sender && attach.initial_delivery_count.is_none() {
                attach.initial_delivery_count = Some(0);
            }
            debug!(?attach, "accepting CBS link");
            let endpoint = session
                .accept_attach(attach, crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64)
                .await?;
            let authorization = Arc::clone(authorization);
            match (target_address.as_str(), source_address.as_str(), endpoint) {
                (crate::CBS_NODE, _, LinkEndpoint::Receiver(receiver)) => {
                    tokio::spawn(async move {
                        if let Err(error) = serve_cbs_requests(receiver, authorization).await {
                            warn!(%error, "CBS request link ended");
                        }
                    });
                }
                (_, crate::CBS_NODE, LinkEndpoint::Sender(sender))
                    if !target_address.is_empty() =>
                {
                    let (route, responses) = authorization
                        .register_reply_route(target_address.clone())
                        .await;
                    tokio::spawn(async move {
                        if let Err(error) = serve_cbs_replies(
                            sender,
                            target_address,
                            route,
                            responses,
                            authorization,
                        )
                        .await
                        {
                            warn!(%error, "CBS response link ended");
                        }
                    });
                }
                (_, _, endpoint) => {
                    detach_with(
                        endpoint,
                        error_for(AmqpError::InvalidField, "invalid CBS link".into()),
                    )
                    .await;
                }
            }
            continue;
        }

        let address = address_for_role(&attach.role, &source_address, &target_address);
        if let Some(entity) = management_entity(address) {
            if attach.role == Role::Sender && attach.initial_delivery_count.is_none() {
                attach.initial_delivery_count = Some(0);
            }
            let plan = match entity {
                Ok(entity) => {
                    let link_authorization = match authorization.as_ref() {
                        Some(authorization) => match authorization
                            .authorize_entity(entity.as_str(), Permission::Manage)
                            .await
                        {
                            Ok(resource) => Some(ManagementAuthorization::new(
                                Arc::clone(authorization),
                                resource,
                            )),
                            Err(_) => {
                                let endpoint = session
                                    .accept_attach(
                                        attach,
                                        crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64,
                                    )
                                    .await?;
                                detach_with(
                                    endpoint,
                                    unauthorized_error(format!(
                                        "Manage is not authorized for {entity}"
                                    )),
                                )
                                .await;
                                continue;
                            }
                        },
                        None => None,
                    };
                    Ok((entity, link_authorization))
                }
                Err(error) => Err(error_for(AmqpError::InvalidField, error.to_string())),
            };

            debug!(%address, ?attach, "accepting management link");
            let endpoint = session
                .accept_attach(attach, crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64)
                .await?;
            let (entity, link_authorization) = match plan {
                Ok(plan) => plan,
                Err(error) => {
                    detach_with(endpoint, error).await;
                    continue;
                }
            };
            match (target_address.as_str(), source_address.as_str(), endpoint) {
                (target, _, LinkEndpoint::Receiver(receiver)) if target == address => {
                    let namespace = namespace.clone();
                    let broker = broker.clone();
                    let management = Arc::clone(&management);
                    tokio::spawn(async move {
                        if let Err(error) = serve_management_requests(
                            receiver,
                            namespace,
                            entity,
                            broker,
                            management,
                            link_authorization,
                        )
                        .await
                        {
                            warn!(%error, "management request link ended");
                        }
                    });
                }
                (_, source, LinkEndpoint::Sender(sender))
                    if source == address && !target_address.is_empty() =>
                {
                    let (route, responses) = management
                        .register_reply_route(target_address.clone())
                        .await;
                    let management = Arc::clone(&management);
                    tokio::spawn(async move {
                        if let Err(error) = serve_management_replies(
                            sender,
                            target_address,
                            route,
                            responses,
                            management,
                            link_authorization,
                        )
                        .await
                        {
                            warn!(%error, "management response link ended");
                        }
                    });
                }
                (_, _, endpoint) => {
                    detach_with(
                        endpoint,
                        error_for(AmqpError::InvalidField, "invalid management link".into()),
                    )
                    .await;
                }
            }
            continue;
        }

        // The address has to be read before the attach is consumed. A client
        // sending names its entity in the target; one receiving names it in the
        // source. The other terminus may carry a generated link address.
        debug!(%address, ?attach, "accepting entity link");

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
        let plan = plan_link(
            &broker,
            &namespace,
            address,
            &attach,
            authorization.as_ref(),
        )
        .await;
        if let Ok((_, Some(accepted), _)) = &plan
            && let Some(source) = attach.source.as_mut()
        {
            stamp_session_filter(source, &accepted.session_id);
        }

        let response_properties = plan
            .as_ref()
            .ok()
            .and_then(|(_, accepted, _)| accepted.as_ref())
            .map(session_attach_properties);
        let endpoint = session
            .accept_attach_with_properties(
                attach,
                crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64,
                response_properties,
            )
            .await?;
        let (entity, accepted, link_authorization) = match plan {
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
                    if let Err(error) = serve_sending_client(
                        receiver,
                        namespace,
                        entity,
                        broker,
                        link_authorization,
                    )
                    .await
                    {
                        warn!(%error, "sending link ended");
                    }
                });
            }
            // The client receives; this end sends.
            LinkEndpoint::Sender(sender) => {
                let hold = accepted.map(|accepted| accepted.hold());
                let link_name = sender.name().to_owned();
                if let Some(hold) = hold.as_ref() {
                    management
                        .register_session(&link_name, entity.clone(), hold.clone())
                        .await;
                }
                let connection_management = Arc::clone(&management);
                tokio::spawn(async move {
                    let result = serve_receiving_client(
                        sender,
                        namespace,
                        entity.clone(),
                        broker,
                        mode,
                        hold.clone(),
                        ReceivingLinkProtocol {
                            authorization: link_authorization,
                            management: Arc::clone(&connection_management),
                        },
                    )
                    .await;
                    if let Some(hold) = hold.as_ref() {
                        connection_management
                            .unregister_session(&link_name, hold)
                            .await;
                    }
                    if let Err(error) = result {
                        warn!(%error, "receiving link ended");
                    }
                });
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
struct LinkAuthorization {
    connection: Arc<ConnectionAuthorization>,
    resource: ResourceScope,
    permission: Permission,
}

struct ReceivingLinkProtocol {
    authorization: Option<LinkAuthorization>,
    management: Arc<ConnectionManagement>,
}

fn session_attach_properties(accepted: &AcceptedSession) -> Fields {
    let millis = accepted.lock.locked_until.as_millis();
    let ticks =
        DOTNET_UNIX_EPOCH_TICKS.saturating_add(millis.saturating_mul(DOTNET_TICKS_PER_MILLISECOND));
    let mut properties = Fields::new();
    properties.insert(
        Symbol::from(LOCKED_UNTIL_UTC_PROPERTY),
        Value::Long(i64::try_from(ticks).unwrap_or(i64::MAX)),
    );
    properties
}

impl LinkAuthorization {
    async fn ensure(&self) -> Result<(), AmqpProtocolError> {
        self.connection
            .authorize_resource(&self.resource, self.permission)
            .await
            .map_err(|_| unauthorized_error("the link's authorization has expired"))
    }

    async fn wait_until_unauthorized(&self) {
        self.connection
            .wait_until_unauthorized(&self.resource, self.permission)
            .await;
    }
}

/// Everything an attach needs decided before it is answered: the entity it
/// reaches, and the session lock it holds if its source asked for one.
async fn plan_link<B: Broker>(
    broker: &B,
    namespace: &NamespaceName,
    address: &str,
    attach: &Attach,
    authorization: Option<&Arc<ConnectionAuthorization>>,
) -> Result<
    (
        EntityPath,
        Option<AcceptedSession>,
        Option<LinkAuthorization>,
    ),
    AmqpProtocolError,
> {
    let entity = resolve_entity(address, attach.role.clone())
        .map_err(|error| error_for(AmqpError::InvalidField, error.to_string()))?;
    let link_authorization = match authorization {
        Some(authorization) => {
            let permission = match attach.role {
                Role::Sender => Permission::Send,
                Role::Receiver => Permission::Listen,
            };
            let resource = authorization
                .authorize_entity(entity.as_str(), permission)
                .await
                .map_err(|_| {
                    unauthorized_error(format!("{permission:?} is not authorized for {entity}"))
                })?;
            Some(LinkAuthorization {
                connection: Arc::clone(authorization),
                resource,
                permission,
            })
        }
        None => None,
    };

    // Only a receiving link takes a session lock; a sender names a session per
    // message instead.
    if attach.role != Role::Receiver {
        return Ok((entity, None, link_authorization));
    }
    let session_id = match read_session_filter(attach.source.as_ref())
        .map_err(|error| error_for(AmqpError::InvalidField, error.to_string()))?
    {
        SessionRequest::None => return Ok((entity, None, link_authorization)),
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
        Ok(CommandOutcome::SessionAccepted(Some(accepted))) => {
            Ok((entity, Some(accepted), link_authorization))
        }
        // Nothing to grant is what Service Bus reports as a timeout: the client
        // did nothing wrong and simply asks again.
        Ok(CommandOutcome::SessionAccepted(None)) => Err(AmqpProtocolError::new(
            ErrorCondition::Custom(Symbol::from(crate::TIMEOUT)),
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
///
/// A dead-letter address resolves to the shadow queue for a receiver and is
/// refused for a sender: the only way in is dead-lettering.
fn resolve_entity(address: &str, role: Role) -> Result<EntityPath, ProtocolError> {
    match parse_attachment(address)? {
        Attachment::Queue(entity) => Ok(entity),
        Attachment::DeadLetter(entity) if role == Role::Receiver => entity
            .dead_letter_queue()
            .map_err(|error| ProtocolError::InvalidAddress {
                address: address.to_owned(),
                detail: error.to_string(),
            }),
        Attachment::DeadLetter(entity) => Err(ProtocolError::InvalidAddress {
            address: address.to_owned(),
            detail: format!("the dead-letter queue of {entity} cannot be sent to"),
        }),
        Attachment::Subscription { topic, .. } => Err(ProtocolError::InvalidAddress {
            address: address.to_owned(),
            detail: format!("subscriptions of topic {topic} are not implemented"),
        }),
    }
}

fn address_for_role<'a>(role: &Role, source: &'a str, target: &'a str) -> &'a str {
    match role {
        Role::Sender => target,
        Role::Receiver => source,
    }
}

fn management_entity(address: &str) -> Option<Result<EntityPath, ProtocolError>> {
    let entity = address.strip_suffix("/$management")?;
    Some(
        EntityPath::new(entity).map_err(|error| ProtocolError::InvalidAddress {
            address: address.to_owned(),
            detail: error.to_string(),
        }),
    )
}

/// Drives a link the client sends on: every transfer becomes one send command.
async fn serve_sending_client<B: Broker>(
    mut receiver: Receiver,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: B,
    authorization: Option<LinkAuthorization>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let received = async {
            match authorization.as_ref() {
                Some(authorization) => {
                    tokio::select! {
                        result = receiver.recv() => Some(result),
                        () = authorization.wait_until_unauthorized() => None,
                    }
                }
                None => Some(receiver.recv().await),
            }
        }
        .await;
        let Some(received) = received else {
            receiver
                .close_with_error(unauthorized_error("the link's authorization has expired"))
                .await?;
            return Ok(());
        };
        let delivery = match received {
            Ok(delivery) => delivery,
            // The client hung up. Its detach is waiting for an answer, and a
            // dropped handle would leave it waiting; closing sends it.
            Err(EngineError::RemoteClosed | EngineError::RemoteDetached | EngineError::Stopped) => {
                let _ = receiver.close().await;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(authorization) = authorization.as_ref()
            && let Err(error) = authorization.ensure().await
        {
            receiver.close_with_error(error).await?;
            return Ok(());
        }
        let incoming = match read_incoming(delivery.message()) {
            Ok(incoming) => incoming,
            Err(error) => {
                // The client's message, the client's fault: reject this transfer
                // and keep the link.
                receiver
                    .reject(
                        &delivery,
                        Some(error_for(AmqpError::InvalidField, error.to_string())),
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
                    .reject(&delivery, Some(rejection_error(&rejection)))
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
    protocol: ReceivingLinkProtocol,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ReceivingLinkProtocol {
        authorization,
        management,
    } = protocol;

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
            () = wait_until_link_unauthorized(authorization.as_ref()), if authorization.is_some() => {
                release_session(&broker, &namespace, &entity, session.as_ref()).await;
                sender
                    .close_with_error(unauthorized_error("the link's authorization has expired"))
                    .await?;
                return Ok(());
            }
            fetched = next_delivery(
                &broker,
                &namespace,
                &entity,
                mode,
                session.as_ref(),
                authorization.as_ref(),
            ) => fetched,
        };

        match fetched {
            Ok(delivery) => {
                if !settle(
                    &mut sender,
                    &namespace,
                    &entity,
                    &broker,
                    delivery,
                    authorization.as_ref(),
                    &management,
                )
                .await?
                {
                    release_session(&broker, &namespace, &entity, session.as_ref()).await;
                    sender
                        .close_with_error(unauthorized_error(
                            "the link's authorization expired before settlement",
                        ))
                        .await?;
                    return Ok(());
                }
            }
            Err(NextDeliveryError::Broker(rejection)) => {
                release_session(&broker, &namespace, &entity, session.as_ref()).await;
                sender.close_with_error(rejection_error(&rejection)).await?;
                return Ok(());
            }
            Err(NextDeliveryError::Unauthorized) => {
                release_session(&broker, &namespace, &entity, session.as_ref()).await;
                sender
                    .close_with_error(unauthorized_error("the link's authorization has expired"))
                    .await?;
                return Ok(());
            }
        }
    }
}

async fn wait_until_link_unauthorized(authorization: Option<&LinkAuthorization>) {
    match authorization {
        Some(authorization) => authorization.wait_until_unauthorized().await,
        None => std::future::pending().await,
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
    authorization: Option<&LinkAuthorization>,
) -> Result<Delivery, NextDeliveryError> {
    loop {
        if let Some(authorization) = authorization
            && authorization.ensure().await.is_err()
        {
            return Err(NextDeliveryError::Unauthorized);
        }
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
            .await
            .map_err(NextDeliveryError::Broker)?;

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
                return Err(NextDeliveryError::Broker(BrokerRejection::Unavailable(
                    format!("receive produced an unexpected outcome: {other:?}"),
                )));
            }
        }
    }
}

enum NextDeliveryError {
    Broker(BrokerRejection),
    Unauthorized,
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
    authorization: Option<&LinkAuthorization>,
    management: &ConnectionManagement,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let Some(lock) = delivery.lock else {
        let delivery_tag = sequence_delivery_tag(delivery.sequence);
        // Receive-and-delete: the message is already gone, so there is nothing
        // to settle after the transfer.
        let sent = match authorization {
            Some(authorization) => {
                tokio::select! {
                    outcome = sender.send(crate::write_delivery(&delivery), delivery_tag.clone()) => {
                        outcome?;
                        true
                    }
                    () = authorization.wait_until_unauthorized() => false,
                }
            }
            None => {
                sender
                    .send(crate::write_delivery(&delivery), delivery_tag)
                    .await?;
                true
            }
        };
        return Ok(sent);
    };
    let sequence = delivery.sequence;
    let delivery_tag = lock_delivery_tag(lock.token);
    let link_name = sender.name().to_owned();
    management
        .register_delivery(&link_name, entity.clone(), sequence, lock.token)
        .await;
    let outcome = match authorization {
        Some(authorization) => {
            tokio::select! {
                outcome = sender.send_unconfirmed(crate::write_delivery(&delivery), delivery_tag.clone()) => Some(outcome),
                () = authorization.wait_until_unauthorized() => None,
            }
        }
        None => Some(
            sender
                .send_unconfirmed(crate::write_delivery(&delivery), delivery_tag)
                .await,
        ),
    };
    management.unregister_delivery(&link_name, lock.token).await;
    let Some(outcome) = outcome else {
        return Ok(false);
    };
    let outcome = outcome?;
    if let Some(authorization) = authorization
        && authorization.ensure().await.is_err()
    {
        sender
            .confirm(DeliveryState::Rejected(amqp::Rejected {
                error: Some(unauthorized_error(
                    "the link's authorization expired before settlement committed",
                )),
            }))
            .await?;
        return Ok(false);
    }

    // Service Bus treats the sender's second-mode disposition as the result of
    // applying the requested settlement operation, not as an echo of that
    // request. Accepted means Complete, Abandon, or DeadLetter committed.
    let confirmation = DeliveryState::Accepted(amqp::Accepted);

    let kind = match outcome {
        Outcome::Accepted(_) => CommandKind::Complete {
            sequence,
            lock_token: lock.token,
        },
        // Rejected means the client will never process it, so it goes to the
        // dead-letter queue rather than round again.
        Outcome::Rejected(rejected) => {
            let (reason, description) = dead_letter_details(rejected);
            CommandKind::DeadLetter {
                sequence,
                lock_token: lock.token,
                reason,
                description,
            }
        }
        // Released and modified both mean "not now": back to the queue, with the
        // delivery count already incremented by the receive.
        Outcome::Released(_) | Outcome::Modified(_) => CommandKind::Abandon {
            sequence,
            lock_token: lock.token,
        },
    };

    match broker.submit(namespace.clone(), entity.clone(), kind).await {
        Ok(_) => sender.confirm(confirmation).await?,
        Err(rejection) => {
            sender
                .confirm(DeliveryState::Rejected(amqp::Rejected {
                    error: Some(rejection_error(&rejection)),
                }))
                .await?;
            // A settlement that fails is not fatal to the link: the lock expires
            // and the message comes round again.
            warn!(%sequence, %rejection, "settlement failed, leaving the lock to expire");
        }
    }
    Ok(true)
}

fn lock_delivery_tag(token: LockToken) -> DeliveryTag {
    let mut tag = [0_u8; 16];
    tag[8..].copy_from_slice(&token.as_u64().to_be_bytes());
    tag.to_vec().into()
}

fn sequence_delivery_tag(sequence: domain::SequenceNumber) -> DeliveryTag {
    sequence.as_u64().to_be_bytes().to_vec().into()
}

/// Reads the Service Bus dead-letter contract from a rejected delivery.
///
/// The official clients put an application-supplied reason and description in
/// the AMQP error's info map. A generic AMQP client may reject without either,
/// in which case the stable Switchyard fallback still explains how the message
/// reached the dead-letter queue.
fn dead_letter_details(rejected: amqp::Rejected) -> (String, String) {
    let Some(error) = rejected.error else {
        return (
            String::from("RejectedByReceiver"),
            String::from("the receiver rejected the message"),
        );
    };

    let reason = error
        .info
        .as_ref()
        .and_then(|info| string_field(info, crate::DEAD_LETTER_REASON_PROPERTY))
        .unwrap_or_else(|| String::from("RejectedByReceiver"));
    let description = error
        .info
        .as_ref()
        .and_then(|info| string_field(info, crate::DEAD_LETTER_DESCRIPTION_PROPERTY))
        .or(error.description)
        .unwrap_or_else(|| String::from("the receiver rejected the message"));
    (reason, description)
}

fn string_field(fields: &Fields, name: &str) -> Option<String> {
    fields
        .get(&Symbol::from(name))
        .and_then(|value| match value {
            Value::String(value) => Some(value.clone()),
            _ => None,
        })
}

async fn detach_with(endpoint: LinkEndpoint, error: AmqpProtocolError) {
    match endpoint {
        LinkEndpoint::Sender(sender) => {
            let _ = sender.close_with_error(error).await;
        }
        LinkEndpoint::Receiver(receiver) => {
            let _ = receiver.close_with_error(error).await;
        }
    }
}

fn error_for(condition: AmqpError, description: String) -> AmqpProtocolError {
    AmqpProtocolError::new(condition, description, None)
}

fn unauthorized_error(description: impl Into<String>) -> AmqpProtocolError {
    error_for(AmqpError::UnauthorizedAccess, description.into())
}

/// The wire error a broker rejection becomes, carrying the condition an SDK
/// keys its behaviour off.
fn rejection_error(rejection: &BrokerRejection) -> AmqpProtocolError {
    AmqpProtocolError::new(
        ErrorCondition::Custom(Symbol::from(rejection.condition())),
        rejection.to_string(),
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_resolve_by_role() {
        assert_eq!(
            resolve_entity("orders", Role::Sender)
                .expect("a queue accepts senders")
                .as_str(),
            "orders"
        );
        // The dead-letter address is a real queue for a receiver and a refusal
        // for a sender: the only way in is dead-lettering.
        assert_eq!(
            resolve_entity("orders/$deadletterqueue", Role::Receiver)
                .expect("the shadow queue accepts receivers")
                .as_str(),
            "orders/$deadletterqueue"
        );
        assert!(resolve_entity("orders/$deadletterqueue", Role::Sender).is_err());
        assert!(resolve_entity("billing/Subscriptions/accounting", Role::Receiver).is_err());
    }

    #[test]
    fn entity_addresses_come_from_the_authoritative_terminus() {
        assert_eq!(
            address_for_role(&Role::Sender, "generated-source", "orders"),
            "orders"
        );
        assert_eq!(
            address_for_role(&Role::Receiver, "orders", "generated-target"),
            "orders"
        );
    }

    #[test]
    fn a_lock_token_is_a_guid_sized_delivery_tag() {
        let tag = lock_delivery_tag(LockToken::new(42));
        assert_eq!(tag.len(), 16);
        assert_eq!(&tag[8..], &42_u64.to_be_bytes());
    }

    #[test]
    fn a_rejection_reaches_the_wire_as_its_condition() {
        let rejection = BrokerRejection::Refused(domain::BrokerError::QueueNotFound);
        let error = rejection_error(&rejection);
        assert_eq!(
            error.condition,
            ErrorCondition::Custom(Symbol::from(crate::NOT_FOUND))
        );
    }

    #[test]
    fn a_service_bus_rejection_keeps_its_dead_letter_details() {
        let mut info = Fields::default();
        info.insert(
            Symbol::from(crate::DEAD_LETTER_REASON_PROPERTY),
            Value::String(String::from("InvalidOrder")),
        );
        info.insert(
            Symbol::from(crate::DEAD_LETTER_DESCRIPTION_PROPERTY),
            Value::String(String::from("the order has no customer")),
        );
        let rejected = amqp::Rejected {
            error: Some(AmqpProtocolError::new(
                ErrorCondition::Custom(Symbol::from("com.microsoft:dead-letter")),
                "the receiver rejected the message",
                Some(info),
            )),
        };

        assert_eq!(
            dead_letter_details(rejected),
            (
                String::from("InvalidOrder"),
                String::from("the order has no customer")
            )
        );
    }

    #[test]
    fn a_generic_rejection_gets_stable_dead_letter_details() {
        assert_eq!(
            dead_letter_details(amqp::Rejected::default()),
            (
                String::from("RejectedByReceiver"),
                String::from("the receiver rejected the message")
            )
        );
    }
}
