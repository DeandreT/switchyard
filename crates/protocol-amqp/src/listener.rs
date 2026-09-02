//! The AMQP acceptor: connections in, commands out.
//!
//! One task per connection, session, and link. A link holds no broker state of
//! its own — a lock token it is carrying is the broker's, and if the link dies
//! holding one, the lock simply expires and the message is redelivered. That is
//! what makes an abrupt disconnect safe.

use std::sync::Arc;

use amqp::{
    AmqpError, Attach, EngineError, Error as AmqpProtocolError, ErrorCondition, Fields,
    LinkEndpoint, Receiver, Role, SenderSettleMode, ServerConnection, ServerSession,
};
use auth::{Permission, ResourceScope};
use domain::{
    AcceptedSession, CommandKind, CommandOutcome, EntityPath, NamespaceName, ReceiveMode,
};
use rustls::ServerConfig;
use serde_amqp::{Value, primitives::Symbol};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use crate::{
    Attachment, Broker, BrokerRejection, IncomingMessages, ProtocolError, SessionRequest,
    SharedAccessAuthentication,
    authorization::{ConnectionAuthorization, SharedAccessSaslAcceptor},
    cbs::{serve_cbs_replies, serve_cbs_requests},
    management::{
        ConnectionManagement, ManagementAuthorization, serve_management_replies,
        serve_management_requests,
    },
    parse_attachment, read_incoming_messages, read_session_filter, stamp_session_filter,
    websocket::accept_amqp_websocket,
};

mod settlement;

use settlement::serve_receiving_client;

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

    /// Accepts AMQP tunneled through the Service Bus WebSocket endpoint.
    ///
    /// TLS, when configured, is established before the HTTP upgrade. The
    /// broker process requires TLS for this transport; permitting an unwrapped
    /// stream here keeps the protocol binding independently testable.
    pub async fn serve_websockets(self, listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (stream, peer) = listener.accept().await?;
            debug!(%peer, "WebSocket connection accepted");

            let broker = self.broker.clone();
            let namespace = self.namespace.clone();
            let container_id = self.container_id.clone();
            let tls_acceptor = self.tls_acceptor.clone();
            let shared_access_authentication = self.shared_access_authentication.clone();
            tokio::spawn(async move {
                let result = match tls_acceptor {
                    Some(acceptor) => match acceptor.accept(stream).await {
                        Ok(stream) => {
                            debug!(%peer, "WebSocket TLS established");
                            match accept_amqp_websocket(stream).await {
                                Ok(stream) => {
                                    debug!(%peer, "AMQP WebSocket upgraded");
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
                            }
                        }
                        Err(error) => Err(error.into()),
                    },
                    None => match accept_amqp_websocket(stream).await {
                        Ok(stream) => {
                            debug!(%peer, "AMQP WebSocket upgraded");
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
                };
                if let Err(error) = result {
                    warn!(%peer, %error, "WebSocket connection ended");
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

        let session = match connection.accept_session(incoming).await {
            Ok(session) => session,
            // End may overtake application acceptance, and the channel may
            // already belong to a newer Begin. That stale work is session-
            // scoped and must not close the replacement connection.
            Err(EngineError::RemoteDetached) => continue,
            Err(error) => return Err(error.into()),
        };
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
            let endpoint = match session
                .accept_attach(attach, crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64)
                .await
            {
                Ok(endpoint) => endpoint,
                Err(EngineError::RemoteDetached) => continue,
                Err(error) => return Err(error.into()),
            };
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
                            .authorize_entity_any(
                                entity.as_str(),
                                &[Permission::Send, Permission::Listen],
                            )
                            .await
                        {
                            Ok(resource) => Some(ManagementAuthorization::new(
                                Arc::clone(authorization),
                                resource,
                            )),
                            Err(_) => {
                                let endpoint = match session
                                    .accept_attach(
                                        attach,
                                        crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64,
                                    )
                                    .await
                                {
                                    Ok(endpoint) => endpoint,
                                    Err(EngineError::RemoteDetached) => continue,
                                    Err(error) => return Err(error.into()),
                                };
                                detach_with(
                                    endpoint,
                                    unauthorized_error(format!(
                                        "neither Send nor Listen is authorized for {entity}"
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
            let endpoint = match session
                .accept_attach(attach, crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64)
                .await
            {
                Ok(endpoint) => endpoint,
                Err(EngineError::RemoteDetached) => continue,
                Err(error) => return Err(error.into()),
            };
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
        let endpoint = match session
            .accept_attach_with_properties(
                attach,
                crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64,
                response_properties,
            )
            .await
        {
            Ok(endpoint) => endpoint,
            Err(error) => {
                // Planning a session receiver acquires its broker hold before
                // the attach can be accepted. If the peer detached meanwhile,
                // that hold belongs to no link and must not block its
                // replacement until lock expiry.
                if let Ok((entity, Some(accepted), _)) = &plan {
                    let hold = accepted.hold();
                    settlement::release_session(&broker, &namespace, entity, Some(&hold)).await;
                }
                match error {
                    EngineError::RemoteDetached => continue,
                    error => return Err(error.into()),
                }
            }
        };
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
/// Queue and topic paths share the same wire shape; the domain decides which
/// one a plain path names. Subscription paths are unambiguous and are
/// canonicalized here so the SDK's `Subscriptions` spelling reaches the same
/// durable entity key as administration. Dead-letter addresses resolve to a
/// shadow only for receivers: the only way in is dead-lettering.
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
        Attachment::Subscription {
            topic,
            subscription,
        } if role == Role::Receiver => {
            topic
                .subscription(&subscription)
                .map_err(|error| ProtocolError::InvalidAddress {
                    address: address.to_owned(),
                    detail: error.to_string(),
                })
        }
        Attachment::Subscription { topic, .. } => Err(ProtocolError::InvalidAddress {
            address: address.to_owned(),
            detail: format!("subscriptions of topic {topic} cannot be sent to"),
        }),
        Attachment::SubscriptionDeadLetter {
            topic,
            subscription,
        } if role == Role::Receiver => topic
            .subscription(&subscription)
            .and_then(|entity| entity.dead_letter_queue())
            .map_err(|error| ProtocolError::InvalidAddress {
                address: address.to_owned(),
                detail: error.to_string(),
            }),
        Attachment::SubscriptionDeadLetter { topic, .. } => Err(ProtocolError::InvalidAddress {
            address: address.to_owned(),
            detail: format!("the dead-letter queue of a subscription of {topic} cannot be sent to"),
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
    const MANAGEMENT_SUFFIX: &str = "/$management";
    let split = address.len().checked_sub(MANAGEMENT_SUFFIX.len())?;
    let entity = address.get(..split)?;
    let suffix = address.get(split..)?;
    if !suffix.eq_ignore_ascii_case(MANAGEMENT_SUFFIX) {
        return None;
    }
    // Management links use the same well-known entity suffixes as data links.
    // In particular, the official .NET client spells the DLQ segment with
    // capitals here even though Switchyard's shadow key is canonicalized. The
    // management suffix itself is another case-insensitive identity segment.
    Some(resolve_entity(entity, Role::Receiver))
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
            // The engine already answered a remote Detach. A late local close
            // could detach a replacement link that reused this handle.
            Err(EngineError::RemoteClosed | EngineError::RemoteDetached | EngineError::Stopped) => {
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
        let incoming = match read_incoming_messages(
            delivery.message_format(),
            delivery.message(),
            delivery.encoded_message(),
        ) {
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

        let (command, expected) = match incoming {
            IncomingMessages::Single(incoming) => (
                CommandKind::Send {
                    message_id: incoming.message_id,
                    body: incoming.body,
                    time_to_live_millis: incoming.time_to_live_millis,
                    session_id: incoming.session_id,
                    scheduled_enqueue_at: incoming.scheduled_enqueue_at,
                    envelope: Some(incoming.envelope),
                },
                SendOutcome::Single,
            ),
            IncomingMessages::Batch(messages) => {
                let count = messages.len();
                (
                    CommandKind::SendBatch {
                        messages: messages.into_iter().map(Into::into).collect(),
                    },
                    SendOutcome::Batch(count),
                )
            }
        };
        let outcome = broker
            .submit(namespace.clone(), entity.clone(), command)
            .await;

        // Accepting only after the command committed is what makes the
        // acknowledgement mean the message is durable.
        match outcome {
            Ok(outcome) if expected.matches(&outcome) => receiver.accept(&delivery).await?,
            Ok(other) => {
                receiver
                    .reject(
                        &delivery,
                        Some(error_for(
                            AmqpError::InternalError,
                            format!("send produced an unexpected outcome: {other:?}"),
                        )),
                    )
                    .await?
            }
            Err(rejection) => {
                receiver
                    .reject(&delivery, Some(rejection_error(&rejection)))
                    .await?
            }
        }
    }
}

#[derive(Clone, Copy)]
enum SendOutcome {
    Single,
    Batch(usize),
}

impl SendOutcome {
    fn matches(self, outcome: &CommandOutcome) -> bool {
        match (self, outcome) {
            (
                Self::Single,
                CommandOutcome::Sent { .. } | CommandOutcome::DuplicateSuppressed { .. },
            ) => true,
            (Self::Single, CommandOutcome::Published { sequences, .. }) => sequences.len() == 1,
            (Self::Batch(expected), CommandOutcome::BatchSent { sequences, stored }) => {
                sequences.len() == expected
                    && u32::try_from(expected).is_ok_and(|expected| *stored <= expected)
            }
            (Self::Batch(expected), CommandOutcome::Published { sequences, .. }) => {
                sequences.len() == expected
            }
            _ => false,
        }
    }
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
        assert_eq!(
            resolve_entity("billing/Subscriptions/accounting", Role::Receiver)
                .expect("a subscription accepts receivers")
                .as_str(),
            "billing/subscriptions/accounting"
        );
        assert_eq!(
            resolve_entity(
                "billing/Subscriptions/accounting/$DeadLetterQueue",
                Role::Receiver,
            )
            .expect("a subscription dead-letter queue accepts receivers")
            .as_str(),
            "billing/subscriptions/accounting/$deadletterqueue"
        );
        assert!(resolve_entity("billing/Subscriptions/accounting", Role::Sender).is_err());
        assert!(
            resolve_entity(
                "billing/Subscriptions/accounting/$DeadLetterQueue",
                Role::Sender,
            )
            .is_err()
        );
    }

    #[test]
    fn maximum_topic_and_subscription_components_resolve_with_their_dlq() {
        let topic = "t".repeat(domain::MAX_ENTITY_PATH_BYTES);
        let subscription = "s".repeat(domain::MAX_SUBSCRIPTION_NAME_CHARACTERS);
        let address = format!("{topic}/Subscriptions/{subscription}/$DeadLetterQueue");
        let resolved = resolve_entity(&address, Role::Receiver)
            .expect("separately valid maximum components form a valid composite address");
        assert_eq!(
            resolved.as_str(),
            format!("{topic}/subscriptions/{subscription}/$deadletterqueue")
        );
        assert!(resolved.as_str().len() > domain::MAX_ENTITY_PATH_BYTES);
    }

    #[test]
    fn management_addresses_canonicalize_every_identity_segment() {
        assert_eq!(
            management_entity("OrDeRs/$DeadLetterQueue/$MaNaGeMeNt")
                .expect("management suffix")
                .expect("the DLQ management entity resolves")
                .as_str(),
            "orders/$deadletterqueue"
        );
        assert_eq!(
            management_entity("ORDERS/$MANAGEMENT")
                .expect("management suffix")
                .expect("the queue management entity resolves")
                .as_str(),
            "orders"
        );
        assert_eq!(
            management_entity("BiLlInG/Subscriptions/AcCoUnTiNg/$Management")
                .expect("management suffix")
                .expect("the subscription management entity resolves")
                .as_str(),
            "billing/subscriptions/accounting"
        );
        assert_eq!(
            management_entity("Billing/Subscriptions/Accounting/$DeadLetterQueue/$MANAGEMENT")
                .expect("management suffix")
                .expect("the subscription DLQ management entity resolves")
                .as_str(),
            "billing/subscriptions/accounting/$deadletterqueue"
        );
        assert!(management_entity("éxxxxxxxxxxx").is_none());
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
    fn a_rejection_reaches_the_wire_as_its_condition() {
        let rejection = BrokerRejection::Refused(domain::BrokerError::QueueNotFound);
        let error = rejection_error(&rejection);
        assert_eq!(
            error.condition,
            ErrorCondition::Custom(Symbol::from(crate::NOT_FOUND))
        );
    }

    #[test]
    fn a_send_accepts_suppression_and_a_batch_requires_one_sequence_per_child() {
        assert!(
            SendOutcome::Single.matches(&CommandOutcome::DuplicateSuppressed {
                sequence: domain::SequenceNumber::new(1)
            })
        );
        assert!(SendOutcome::Batch(2).matches(&CommandOutcome::BatchSent {
            sequences: vec![
                domain::SequenceNumber::new(1),
                domain::SequenceNumber::new(2)
            ],
            stored: 1,
        }));
        assert!(!SendOutcome::Batch(2).matches(&CommandOutcome::BatchSent {
            sequences: vec![domain::SequenceNumber::new(1)],
            stored: 1,
        }));
        assert!(!SendOutcome::Batch(2).matches(&CommandOutcome::BatchSent {
            sequences: vec![
                domain::SequenceNumber::new(1),
                domain::SequenceNumber::new(2)
            ],
            stored: 3,
        }));
        assert!(!SendOutcome::Batch(1).matches(&CommandOutcome::Sent {
            sequence: domain::SequenceNumber::new(1)
        }));
        assert!(SendOutcome::Single.matches(&CommandOutcome::Published {
            sequences: vec![domain::SequenceNumber::new(1)],
            subscriptions: vec![],
        }));
        assert!(!SendOutcome::Single.matches(&CommandOutcome::Published {
            sequences: vec![
                domain::SequenceNumber::new(1),
                domain::SequenceNumber::new(2),
            ],
            subscriptions: vec![],
        }));
        assert!(SendOutcome::Batch(2).matches(&CommandOutcome::Published {
            sequences: vec![
                domain::SequenceNumber::new(1),
                domain::SequenceNumber::new(2),
            ],
            subscriptions: vec![],
        }));
    }
}
