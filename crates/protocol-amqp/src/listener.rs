//! The AMQP acceptor: connections in, commands out.
//!
//! One task per connection, session, and link. A link holds no broker state of
//! its own — a lock token it is carrying is the broker's, and if the link dies
//! holding one, the lock simply expires and the message is redelivered. That is
//! what makes an abrupt disconnect safe.

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use amqp::{
    AmqpError, Attach, DeliveryTag, EngineError, Error as AmqpProtocolError, ErrorCondition,
    Fields, LinkEndpoint, MessageFormatDecoders, Receiver, Role, Sender, SenderSettleMode,
    ServerConnection, ServerSession,
};
use auth::{Permission, ResourceScope};
use domain::{
    AcceptedSession, CommandKind, CommandOutcome, Delivery, EntityPath, LockToken, NamespaceName,
    ReceiveMode, SessionHold,
};
use rustls::ServerConfig;
use serde_amqp::{Value, primitives::Symbol};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use crate::{
    Attachment, Broker, BrokerRejection, ProtocolError, SessionRequest, SharedAccessAuthentication,
    authorization::ConnectionAuthorization,
    batch::read_ingress,
    broker::BoundBroker,
    cbs::{serve_cbs_replies, serve_cbs_requests},
    management::{
        ConnectionManagement, ManagementAuthorization, serve_management_replies,
        serve_management_requests,
    },
    parse_attachment, read_session_filter, stamp_session_filter,
};

mod atomic_ingress;
mod connection;
#[cfg(test)]
mod owned_tasks;
mod receiving;
mod retained_connection;
mod routing;
mod session_paging;
#[cfg(test)]
mod session_paging_tests;
mod websocket;

pub use retained_connection::{
    RetainedConnectionJoinReport, RetainedConnectionOutcome, RetainedConnectionOutcomes,
    RetainedConnectionOwner, RetainedConnectionRequest, RetainedConnectionResult,
    RetainedConnectionStartCause, RetainedConnectionStartError, RetainedConnectionStarter,
    RetainedConnectionTaskJoins,
};

pub use atomic_ingress::{
    RetainedAtomicMessagingAdmissionOutcome, RetainedAtomicMessagingBuildError,
    RetainedAtomicMessagingControl, RetainedAtomicMessagingDrain, RetainedAtomicMessagingLimits,
    RetainedAtomicMessagingLimitsError, RetainedAtomicMessagingOwner,
    RetainedAtomicMessagingProgress, RetainedAtomicMessagingReport,
    RetainedAtomicMessagingSessionOutcome, RetainedAtomicMessagingStarter,
    RetainedAtomicMessagingWorkerBranch, RetainedAtomicMessagingWorkerOutcome,
};

use receiving::serve_receiving_client;
use routing::{management_target, plan_link, plan_management};

#[cfg(test)]
use routing::management_entity;

/// How long a receiving link waits on a wakeup before asking the broker anyway.
///
/// The wakeup is the mechanism; this is the net under it. A notification can be
/// lost when several links wait on one entity, so a waiter re-asks on a coarse
/// interval rather than trusting the signal absolutely.
const EMPTY_QUEUE_FALLBACK: Duration = Duration::from_secs(3);
const DEFAULT_MAX_CONNECTIONS: usize = 128;
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

const LOCKED_UNTIL_UTC_PROPERTY: &str = "com.microsoft:locked-until-utc";
const DOTNET_UNIX_EPOCH_TICKS: u64 = 621_355_968_000_000_000;
const DOTNET_TICKS_PER_MILLISECOND: u64 = 10_000;

pub struct AmqpListener<B> {
    broker: B,
    namespace: NamespaceName,
    container_id: String,
    tls_acceptor: Option<TlsAcceptor>,
    shared_access_authentication: Option<SharedAccessAuthentication>,
    max_connections: NonZeroUsize,
    handshake_timeout: Duration,
    connection_options: amqp::ConnectionOptions,
    websocket: bool,
}

impl<B: Broker> AmqpListener<B> {
    pub fn new(broker: B, namespace: NamespaceName) -> Self {
        Self {
            broker,
            namespace,
            container_id: String::from("switchyard"),
            tls_acceptor: None,
            shared_access_authentication: None,
            max_connections: NonZeroUsize::new(DEFAULT_MAX_CONNECTIONS)
                .expect("the default connection limit is positive"),
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            connection_options: amqp::ConnectionOptions::default(),
            websocket: false,
        }
    }

    /// Limits live sockets, including connections still negotiating security.
    /// Values above the semaphore's supported maximum are clamped to that maximum.
    pub fn with_max_connections(mut self, maximum: NonZeroUsize) -> Self {
        self.max_connections = maximum;
        self
    }

    /// One absolute deadline covers TLS, HTTP upgrade, SASL, and AMQP Open.
    /// A zero duration immediately expires the negotiation deadline.
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Advertises half the receive-silence deadline. Zero disables that check.
    /// Positive values below one second are refused before accepting sockets.
    pub fn with_idle_timeout_millis(mut self, millis: u32) -> Self {
        self.connection_options = self.connection_options.idle_timeout_millis(millis);
        self
    }

    /// Bounds each running frame's write and flush. Zero refuses writes immediately.
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.connection_options = self.connection_options.write_timeout(timeout);
        self
    }

    /// Secures accepted sockets before AMQP and SASL negotiation begin.
    pub fn with_tls(mut self, mut config: ServerConfig) -> Self {
        if self.websocket {
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
        }
        self.tls_acceptor = Some(TlsAcceptor::from(std::sync::Arc::new(config)));
        self
    }

    /// Accepts AMQP over binary WebSockets at `/$servicebus/websocket/`.
    /// TLS, authentication, and connection admission settings remain unchanged.
    pub fn with_websocket(mut self) -> Self {
        self.websocket = true;
        if let Some(acceptor) = &self.tls_acceptor {
            let mut config = (**acceptor.config()).clone();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            self.tls_acceptor = Some(TlsAcceptor::from(Arc::new(config)));
        }
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
}
async fn serve_open_connection<B: Broker>(
    connection: &mut ServerConnection,
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
            .and_then(amqp::TargetTerminus::as_target)
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
        if let Some(target) = management_target(address) {
            if attach.role == Role::Sender && attach.initial_delivery_count.is_none() {
                attach.initial_delivery_count = Some(0);
            }
            let plan = match target {
                Ok(target) => {
                    plan_management(&broker, &namespace, target, authorization.as_ref()).await
                }
                Err(error) => Err(error_for(AmqpError::InvalidField, error.to_string())),
            };

            let plan = match plan {
                Ok(plan) => Ok::<_, AmqpProtocolError>(plan),
                Err(error) => match session.reject_attach(attach, error).await {
                    Ok(()) | Err(EngineError::RemoteDetached) => continue,
                    Err(error) => return Err(error.into()),
                },
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
            let (entity, link_authorization, bound) = match plan {
                Ok(plan) => plan,
                Err(error) => {
                    detach_with(endpoint, error).await;
                    continue;
                }
            };
            match (target_address.as_str(), source_address.as_str(), endpoint) {
                (target, _, LinkEndpoint::Receiver(receiver)) if target == address => {
                    let namespace = namespace.clone();
                    let broker = bound;
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
                        .register_reply_route(
                            target_address.clone(),
                            sender.max_message_size(),
                            bound.binding().clone(),
                        )
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
            Some((&session, &attach)),
        )
        .await;
        let plan = match plan {
            Ok(plan) => Ok::<_, session_paging::PlanningFailure>(plan),
            Err(failure) => {
                warn!(%address, condition = ?failure.primary.condition, "refusing link");
                let refusal = session
                    .reject_attach(attach, failure.primary.refusal())
                    .await;
                failure.report();
                if failure.cleanup_failed() {
                    return Err(Box::new(failure));
                }
                match refusal {
                    Ok(()) | Err(EngineError::RemoteDetached) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
        };
        if let Ok((_, Some(accepted), _, _)) = &plan
            && let Some(source) = attach.source.as_mut()
        {
            stamp_session_filter(source, &accepted.session_id);
        }

        let response_properties = plan
            .as_ref()
            .ok()
            .and_then(|(_, accepted, _, _)| accepted.as_ref())
            .map(session_attach_properties);
        let decoders = if attach.role == Role::Sender && plan.is_ok() {
            MessageFormatDecoders::default().with_decoder(
                crate::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
                amqp::decode_message,
            )?
        } else {
            MessageFormatDecoders::default()
        };
        let endpoint = match session
            .accept_attach_with_decoders(
                attach,
                crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64,
                response_properties,
                decoders,
            )
            .await
        {
            Ok(endpoint) => endpoint,
            Err(error) => {
                if let Ok((entity, Some(accepted), _, bound)) = &plan {
                    let failure = session_paging::AcceptanceFailure {
                        primary: error,
                        release: session_paging::release_accepted(
                            bound, &namespace, entity, accepted,
                        )
                        .await,
                    };
                    failure.report();
                    if failure.cleanup_failed() {
                        return Err(Box::new(failure));
                    }
                    if matches!(failure.primary, EngineError::RemoteDetached) {
                        continue;
                    }
                    return Err(Box::new(failure));
                }
                if matches!(error, EngineError::RemoteDetached) {
                    continue;
                }
                return Err(error.into());
            }
        };
        let (entity, accepted, link_authorization, broker) = match plan {
            Ok(plan) => plan,
            Err(error) => {
                // Refusing the link rather than the connection: another link on
                // the same session may be perfectly valid.
                warn!(%address, condition = ?error.primary.condition, "refusing link");
                detach_with(endpoint, error.primary.refusal()).await;
                error.report();
                if error.cleanup_failed() {
                    return Err(Box::new(error));
                }
                continue;
            }
        };

        info!(%address, entity = %entity, session = accepted.as_ref().map(|accepted| accepted.session_id.as_str()), "link attached");
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
                        .register_session(
                            &link_name,
                            entity.clone(),
                            hold.clone(),
                            broker.binding().clone(),
                        )
                        .await;
                }
                let connection_management = Arc::clone(&management);
                tokio::spawn(async move {
                    let binding = broker.binding().clone();
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
                            .unregister_session(&link_name, hold, &binding)
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

    async fn claim_expiry_epoch_seconds(&self) -> Result<u64, AmqpProtocolError> {
        self.connection
            .claim_expiry_epoch_seconds(&self.resource, self.permission)
            .await
            .map_err(|_| unauthorized_error("the link's authorization has expired"))
    }

    async fn wait_until_unauthorized(&self) {
        self.connection
            .wait_until_unauthorized(&self.resource, self.permission)
            .await;
    }
}

fn address_for_role<'a>(role: &Role, source: &'a str, target: &'a str) -> &'a str {
    match role {
        Role::Sender => target,
        Role::Receiver => source,
    }
}

/// Drives a link the client sends on: every transfer becomes one send command.
async fn serve_sending_client<B: Broker>(
    mut receiver: Receiver,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: BoundBroker<B>,
    authorization: Option<LinkAuthorization>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let received = async {
            match authorization.as_ref() {
                Some(authorization) => {
                    tokio::select! {
                        result = receiver.recv_retained() => Some(result),
                        () = authorization.wait_until_unauthorized() => None,
                    }
                }
                None => Some(receiver.recv_retained().await),
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
        let kind = match read_ingress(delivery.message(), delivery.message_format()) {
            Ok(kind) => kind,
            Err(error) => {
                // The client's message, the client's fault: reject this transfer
                // and keep the link.
                receiver
                    .reject_retained(
                        &delivery,
                        Some(AmqpProtocolError::new(
                            ErrorCondition::Custom(Symbol::from(error.condition())),
                            error.to_string(),
                            None,
                        )),
                    )
                    .await?;
                continue;
            }
        };

        let outcome = broker.submit(namespace.clone(), entity.clone(), kind).await;

        // Accepting only after the command committed is what makes the
        // acknowledgement mean the message is durable.
        match outcome {
            Ok(_) => receiver.accept_retained(&delivery).await?,
            Err(rejection) => {
                receiver
                    .reject_retained(&delivery, Some(rejection_error(&rejection)))
                    .await?
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
    broker: &BoundBroker<B>,
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
    broker: &BoundBroker<B>,
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

fn lock_delivery_tag(token: LockToken) -> DeliveryTag {
    let mut tag = [0_u8; 16];
    tag[8..].copy_from_slice(&token.as_u64().to_be_bytes());
    tag.to_vec().into()
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
mod session_startup_tests;

#[cfg(test)]
mod session_provenance_tests;

#[cfg(test)]
mod binding_tests;

#[cfg(test)]
mod pending_refusal_tests;

#[cfg(test)]
mod retained_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_resolve_to_canonical_entities() {
        assert_eq!(
            parse_attachment("orders")
                .expect("valid address")
                .canonical_entity()
                .expect("a queue address")
                .as_str(),
            "orders"
        );
        // The dead-letter address is a real queue for a receiver and a refusal
        // for a sender: the only way in is dead-lettering.
        assert_eq!(
            parse_attachment("orders/$deadletterqueue")
                .expect("valid address")
                .canonical_entity()
                .expect("the shadow queue address")
                .as_str(),
            "orders/$deadletterqueue"
        );
        assert_eq!(
            parse_attachment("billing/Subscriptions/accounting")
                .expect("valid address")
                .canonical_entity()
                .expect("canonical subscription")
                .as_str(),
            "billing/subscriptions/accounting",
        );
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
    fn management_dead_letter_paths_use_the_same_canonical_queue_as_receivers() {
        for address in [
            "Orders/$deadletterqueue/$management",
            "Orders/$DeadLetterQueue/$management",
        ] {
            assert_eq!(
                management_entity(address)
                    .expect("a management address")
                    .expect("a valid dead-letter address")
                    .as_str(),
                "Orders/$deadletterqueue"
            );
        }
        assert_eq!(
            management_entity("Orders/$management")
                .expect("a management address")
                .expect("a valid queue")
                .as_str(),
            "Orders"
        );
        assert!(management_entity("Orders").is_none());
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
}
