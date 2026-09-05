use std::{collections::HashMap, sync::Arc, time::Duration};

use amqp::{
    AmqpError, ApplicationProperties, Body, Error as AmqpProtocolError, Message, MessageId,
    Receiver, Sender,
};
use auth::{Permission, ResourceScope};
use domain::{
    CommandKind, CommandOutcome, Delivery, EntityPath, LockToken, NamespaceName, SequenceNumber,
    SessionHold,
};
use serde_amqp::{
    Value,
    primitives::{Array, Binary, OrderedMap, Timestamp as AmqpTimestamp, Uuid},
};
use tokio::{
    sync::{Mutex, Notify, RwLock, mpsc},
    time::Instant,
};
use tracing::debug;

use crate::{Broker, BrokerRejection, authorization::ConnectionAuthorization};

mod deferred;
mod peek;
mod response;
mod rules;
mod scheduled;

use self::response::ManagementResponse;

pub use deferred::{RECEIVE_BY_SEQUENCE_NUMBER_OPERATION, UPDATE_DISPOSITION_OPERATION};
pub use peek::PEEK_MESSAGE_OPERATION;
pub use rules::{ADD_RULE_OPERATION, ENUMERATE_RULES_OPERATION, REMOVE_RULE_OPERATION};
pub use scheduled::{CANCEL_SCHEDULED_MESSAGE_OPERATION, SCHEDULE_MESSAGE_OPERATION};

pub const RENEW_LOCK_OPERATION: &str = "com.microsoft:renew-lock";
pub const RENEW_SESSION_LOCK_OPERATION: &str = "com.microsoft:renew-session-lock";
pub const GET_SESSION_STATE_OPERATION: &str = "com.microsoft:get-session-state";
pub const SET_SESSION_STATE_OPERATION: &str = "com.microsoft:set-session-state";
pub const OPERATION_PROPERTY: &str = "operation";
pub const ASSOCIATED_LINK_NAME_PROPERTY: &str = "associated-link-name";
pub const STATUS_CODE_PROPERTY: &str = "statusCode";
pub const STATUS_DESCRIPTION_PROPERTY: &str = "statusDescription";
pub const ERROR_CONDITION_PROPERTY: &str = "errorCondition";
pub const TRACKING_ID_PROPERTY: &str = "com.microsoft:tracking-id";
pub const LOCK_TOKENS: &str = "lock-tokens";
pub const EXPIRATIONS: &str = "expirations";
pub const EXPIRATION: &str = "expiration";
pub const SESSION_ID: &str = "session-id";
pub const SESSION_STATE: &str = "session-state";

const REPLY_ROUTE_TIMEOUT: Duration = Duration::from_secs(2);
const REPLY_BUFFER: usize = 16;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DeliveryKey {
    link_name: String,
    lock_token: LockToken,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RequestResponseDeliveryKey {
    entity: EntityPath,
    lock_token: LockToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManagedDelivery {
    entity: EntityPath,
    sequence: SequenceNumber,
    /// Present for deliveries returned inside a management response. It is the
    /// sender envelope before broker overlays, needed if a later request asks
    /// to change application properties while settling the lock.
    delivery: Option<Delivery>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RequestResponseDelivery {
    managed: ManagedDelivery,
    /// A protocol-local deadline. Domain timestamps may come from a replay or
    /// test clock, so they are never compared with this process's wall clock.
    expires_at: Instant,
}

fn request_response_deadline(now: Instant, lock_duration_millis: u64) -> Instant {
    now.checked_add(Duration::from_millis(lock_duration_millis))
        // Queue validation caps lock durations well below Instant's range. If
        // a future caller violates that invariant, fail closed rather than
        // retaining an immortal registry entry.
        .unwrap_or(now)
}

fn purge_request_response_deliveries(
    deliveries: &mut HashMap<RequestResponseDeliveryKey, RequestResponseDelivery>,
    now: Instant,
) {
    deliveries.retain(|_, delivery| delivery.expires_at > now);
}

fn definitive_message_lock_loss(rejection: &BrokerRejection) -> bool {
    matches!(
        rejection,
        BrokerRejection::Refused(
            domain::BrokerError::MessageNotFound { .. }
                | domain::BrokerError::MessageNotLocked { .. }
                | domain::BrokerError::LockTokenMismatch { .. }
                | domain::BrokerError::LockExpired { .. }
        )
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManagedSession {
    entity: EntityPath,
    hold: SessionHold,
}

#[derive(Debug, Default)]
struct ReplyRoutes {
    senders: HashMap<String, mpsc::Sender<ManagementResponse>>,
}

/// Protocol-only state shared by every session on one AMQP connection.
///
/// Delivery tags and dynamic reply addresses have connection scope. Neither is
/// replicated broker state, and both disappear when the connection does.
#[derive(Debug, Default)]
pub(crate) struct ConnectionManagement {
    deliveries: RwLock<HashMap<DeliveryKey, ManagedDelivery>>,
    request_response_deliveries:
        RwLock<HashMap<RequestResponseDeliveryKey, RequestResponseDelivery>>,
    sessions: RwLock<HashMap<String, ManagedSession>>,
    routes: Mutex<ReplyRoutes>,
    route_changed: Notify,
}

impl ConnectionManagement {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) async fn register_delivery(
        &self,
        link_name: &str,
        entity: EntityPath,
        sequence: SequenceNumber,
        lock_token: LockToken,
    ) {
        self.deliveries.write().await.insert(
            DeliveryKey {
                link_name: link_name.to_owned(),
                lock_token,
            },
            ManagedDelivery {
                entity,
                sequence,
                delivery: None,
            },
        );
    }

    pub(crate) async fn unregister_delivery(&self, link_name: &str, lock_token: LockToken) {
        self.deliveries.write().await.remove(&DeliveryKey {
            link_name: link_name.to_owned(),
            lock_token,
        });
    }

    async fn delivery(&self, link_name: &str, lock_token: LockToken) -> Option<ManagedDelivery> {
        self.deliveries
            .read()
            .await
            .get(&DeliveryKey {
                link_name: link_name.to_owned(),
                lock_token,
            })
            .cloned()
    }

    async fn register_request_response_delivery(&self, entity: EntityPath, delivery: Delivery) {
        self.register_request_response_delivery_at(entity, delivery, Instant::now())
            .await;
    }

    async fn register_request_response_delivery_at(
        &self,
        entity: EntityPath,
        delivery: Delivery,
        now: Instant,
    ) {
        let sequence = delivery.sequence;
        let lock = delivery
            .lock
            .expect("only a locked management delivery is registered");
        let mut deliveries = self.request_response_deliveries.write().await;
        purge_request_response_deliveries(&mut deliveries, now);
        deliveries.insert(
            RequestResponseDeliveryKey {
                entity: entity.clone(),
                lock_token: lock.token,
            },
            RequestResponseDelivery {
                managed: ManagedDelivery {
                    entity,
                    sequence,
                    delivery: Some(delivery),
                },
                expires_at: request_response_deadline(now, lock.lock_duration_millis),
            },
        );
    }

    async fn request_response_delivery(
        &self,
        entity: &EntityPath,
        lock_token: LockToken,
    ) -> Option<ManagedDelivery> {
        self.request_response_delivery_at(entity, lock_token, Instant::now())
            .await
    }

    async fn request_response_delivery_at(
        &self,
        entity: &EntityPath,
        lock_token: LockToken,
        now: Instant,
    ) -> Option<ManagedDelivery> {
        let mut deliveries = self.request_response_deliveries.write().await;
        purge_request_response_deliveries(&mut deliveries, now);
        deliveries
            .get(&RequestResponseDeliveryKey {
                entity: entity.clone(),
                lock_token,
            })
            .map(|delivery| delivery.managed.clone())
    }

    async fn refresh_request_response_delivery(
        &self,
        entity: &EntityPath,
        lock_token: LockToken,
        locked_until: domain::Timestamp,
        lock_duration_millis: u64,
    ) {
        self.refresh_request_response_delivery_at(
            entity,
            lock_token,
            locked_until,
            lock_duration_millis,
            Instant::now(),
        )
        .await;
    }

    async fn refresh_request_response_delivery_at(
        &self,
        entity: &EntityPath,
        lock_token: LockToken,
        locked_until: domain::Timestamp,
        lock_duration_millis: u64,
        now: Instant,
    ) {
        let key = RequestResponseDeliveryKey {
            entity: entity.clone(),
            lock_token,
        };
        let mut deliveries = self.request_response_deliveries.write().await;
        if let Some(registered) = deliveries.get_mut(&key) {
            registered.expires_at = request_response_deadline(now, lock_duration_millis);
            if let Some(delivery) = registered.managed.delivery.as_mut() {
                delivery.lock = Some(domain::DeliveryLock {
                    token: lock_token,
                    locked_until,
                    lock_duration_millis,
                });
            }
        }
        // Update the successfully renewed target before purging: the broker is
        // authoritative even if its old local deadline elapsed in flight.
        purge_request_response_deliveries(&mut deliveries, now);
    }

    async fn unregister_request_response_delivery(
        &self,
        entity: &EntityPath,
        lock_token: LockToken,
    ) {
        self.request_response_deliveries
            .write()
            .await
            .remove(&RequestResponseDeliveryKey {
                entity: entity.clone(),
                lock_token,
            });
    }

    async fn unregister_managed_delivery(
        &self,
        entity: &EntityPath,
        link_name: Option<&str>,
        lock_token: LockToken,
    ) {
        self.unregister_request_response_delivery(entity, lock_token)
            .await;
        if let Some(link_name) = link_name {
            self.unregister_delivery(link_name, lock_token).await;
        }
    }

    /// Finds a delivery managed through either an ordinary receive link or a
    /// request/response receive. The .NET client omits `associated-link-name`
    /// when it retrieves a deferred message before opening its receive link,
    /// so the entity-scoped registry is the authority for that path.
    async fn managed_delivery(
        &self,
        entity: &EntityPath,
        link_name: Option<&str>,
        lock_token: LockToken,
    ) -> Option<ManagedDelivery> {
        if let Some(link_name) = link_name
            && let Some(delivery) = self.delivery(link_name, lock_token).await
            && &delivery.entity == entity
        {
            return Some(delivery);
        }
        self.request_response_delivery(entity, lock_token).await
    }

    pub(crate) async fn register_session(
        &self,
        link_name: &str,
        entity: EntityPath,
        hold: SessionHold,
    ) {
        self.sessions
            .write()
            .await
            .insert(link_name.to_owned(), ManagedSession { entity, hold });
    }

    pub(crate) async fn unregister_session(&self, link_name: &str, hold: &SessionHold) {
        let mut sessions = self.sessions.write().await;
        if sessions
            .get(link_name)
            .is_some_and(|session| &session.hold == hold)
        {
            sessions.remove(link_name);
        }
    }

    async fn session(&self, link_name: &str) -> Option<ManagedSession> {
        self.sessions.read().await.get(link_name).cloned()
    }

    pub(crate) async fn register_reply_route(
        &self,
        address: String,
    ) -> (
        mpsc::Sender<ManagementResponse>,
        mpsc::Receiver<ManagementResponse>,
    ) {
        let (sender, receiver) = mpsc::channel(REPLY_BUFFER);
        self.routes
            .lock()
            .await
            .senders
            .insert(address, sender.clone());
        self.route_changed.notify_waiters();
        (sender, receiver)
    }

    async fn unregister_reply_route(
        &self,
        address: &str,
        sender: &mpsc::Sender<ManagementResponse>,
    ) {
        let mut routes = self.routes.lock().await;
        if routes
            .senders
            .get(address)
            .is_some_and(|current| current.same_channel(sender))
        {
            routes.senders.remove(address);
        }
    }

    async fn route_response(
        &self,
        address: &str,
        response: ManagementResponse,
    ) -> Result<(), RouteError> {
        let deadline = tokio::time::Instant::now() + REPLY_ROUTE_TIMEOUT;
        loop {
            let changed = self.route_changed.notified();
            let route = self.routes.lock().await.senders.get(address).cloned();
            if let Some(route) = route {
                return route.send(response).await.map_err(|_| RouteError);
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|_| RouteError)?;
        }
    }
}

#[derive(Clone)]
pub(crate) struct ManagementAuthorization {
    connection: Arc<ConnectionAuthorization>,
    resource: ResourceScope,
}

impl ManagementAuthorization {
    pub(crate) fn new(connection: Arc<ConnectionAuthorization>, resource: ResourceScope) -> Self {
        Self {
            connection,
            resource,
        }
    }

    async fn ensure(&self, permission: Permission) -> Result<(), AmqpProtocolError> {
        self.connection
            .authorize_resource(&self.resource, permission)
            .await
            .map_err(|_| unauthorized_error("the management link's authorization has expired"))
    }

    async fn ensure_any(&self) -> Result<(), AmqpProtocolError> {
        self.connection
            .authorize_resource_any(&self.resource, &[Permission::Send, Permission::Listen])
            .await
            .map_err(|_| unauthorized_error("the management link's authorization has expired"))
    }

    async fn wait_until_unauthorized(&self) {
        self.connection
            .wait_until_unauthorized_any(&self.resource, &[Permission::Send, Permission::Listen])
            .await;
    }
}

pub(crate) async fn serve_management_requests<B: Broker>(
    mut receiver: Receiver,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: B,
    management: Arc<ConnectionManagement>,
    authorization: Option<ManagementAuthorization>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let received = match authorization.as_ref() {
            Some(authorization) => {
                tokio::select! {
                    result = receiver.recv() => Some(result),
                    () = authorization.wait_until_unauthorized() => None,
                }
            }
            None => Some(receiver.recv().await),
        };
        let Some(received) = received else {
            receiver
                .close_with_error(unauthorized_error(
                    "the management link's authorization has expired",
                ))
                .await?;
            return Ok(());
        };
        let delivery = match received {
            Ok(delivery) => delivery,
            Err(
                amqp::EngineError::RemoteClosed
                | amqp::EngineError::RemoteDetached
                | amqp::EngineError::Stopped,
            ) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if let Some(authorization) = authorization.as_ref()
            && let Err(error) = authorization.ensure_any().await
        {
            receiver.close_with_error(error).await?;
            return Ok(());
        }

        let Some(properties) = delivery.message().properties.as_ref() else {
            receiver.reject(&delivery, None).await?;
            continue;
        };
        let (Some(message_id), Some(reply_to)) =
            (properties.message_id.clone(), properties.reply_to.clone())
        else {
            receiver.reject(&delivery, None).await?;
            continue;
        };

        let response = process_request(
            delivery.message(),
            message_id,
            &namespace,
            &entity,
            &broker,
            &management,
            authorization.as_ref(),
        )
        .await;
        debug!(correlation_id = ?response.correlation_id, %reply_to, status_code = response.status_code, "management request processed");
        receiver.accept(&delivery).await?;
        if management
            .route_response(&reply_to, response)
            .await
            .is_err()
        {
            debug!(%reply_to, "management reply route disappeared");
        } else {
            debug!(%reply_to, "management response routed");
        }
    }
}

async fn process_request<B: Broker>(
    message: &Message,
    message_id: MessageId,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
    authorization: Option<&ManagementAuthorization>,
) -> ManagementResponse {
    let tracking_id = message
        .application_properties
        .as_ref()
        .and_then(|properties| string_property(properties, TRACKING_ID_PROPERTY))
        .map(str::to_owned);
    let Some(properties) = message.application_properties.as_ref() else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "application properties are required",
        );
    };
    let Some(operation) = string_property(properties, OPERATION_PROPERTY) else {
        return ManagementResponse::bad_request(message_id, tracking_id, "operation is required");
    };
    let permission = management_permission(operation);
    if let Some(authorization) = authorization
        && authorization.ensure(permission).await.is_err()
    {
        return ManagementResponse::unauthorized(
            message_id,
            tracking_id,
            format!("{permission:?} is not authorized for this management operation"),
        );
    }
    match operation {
        RENEW_LOCK_OPERATION => {
            renew_message_lock(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        RENEW_SESSION_LOCK_OPERATION => {
            renew_session_lock(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        GET_SESSION_STATE_OPERATION => {
            get_session_state(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        SET_SESSION_STATE_OPERATION => {
            set_session_state(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        RECEIVE_BY_SEQUENCE_NUMBER_OPERATION => {
            deferred::receive_by_sequence_number(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        UPDATE_DISPOSITION_OPERATION => {
            deferred::update_disposition(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        PEEK_MESSAGE_OPERATION => {
            peek::peek(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        ADD_RULE_OPERATION => {
            rules::add(message, message_id, tracking_id, namespace, entity, broker).await
        }
        REMOVE_RULE_OPERATION => {
            rules::remove(message, message_id, tracking_id, namespace, entity, broker).await
        }
        ENUMERATE_RULES_OPERATION => {
            rules::enumerate(message, message_id, tracking_id, namespace, entity, broker).await
        }
        SCHEDULE_MESSAGE_OPERATION => {
            scheduled::schedule(message, message_id, tracking_id, namespace, entity, broker).await
        }
        CANCEL_SCHEDULED_MESSAGE_OPERATION => {
            scheduled::cancel(message, message_id, tracking_id, namespace, entity, broker).await
        }
        _ => ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "unsupported management operation",
        ),
    }
}

fn management_permission(operation: &str) -> Permission {
    match operation {
        SCHEDULE_MESSAGE_OPERATION | CANCEL_SCHEDULED_MESSAGE_OPERATION => Permission::Send,
        _ => Permission::Listen,
    }
}

#[allow(clippy::too_many_arguments)]
async fn renew_message_lock<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
) -> ManagementResponse {
    let Some(properties) = message.application_properties.as_ref() else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "application properties are required",
        );
    };
    let link_name = string_property(properties, ASSOCIATED_LINK_NAME_PROPERTY);
    let Some(tokens) = lock_tokens(&message.body) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "lock-tokens must be an AMQP value array of UUIDs",
        );
    };
    if tokens.len() != 1 {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "exactly one lock token is required",
        );
    }

    let lock_token = tokens[0];
    let Some(delivery) = management
        .managed_delivery(entity, link_name, lock_token)
        .await
    else {
        return ManagementResponse::lock_lost(
            message_id,
            tracking_id,
            "the lock token is not active for this entity",
        );
    };

    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::RenewLock {
                sequence: delivery.sequence,
                lock_token,
                lock_duration_millis: None,
            },
        )
        .await
    {
        Ok(CommandOutcome::LockRenewed {
            locked_until,
            lock_duration_millis,
        }) => {
            management
                .refresh_request_response_delivery(
                    entity,
                    lock_token,
                    locked_until,
                    lock_duration_millis,
                )
                .await;
            ManagementResponse::accepted(
                message_id,
                tracking_id,
                map_body(
                    EXPIRATIONS,
                    Value::Array(Array::from(vec![timestamp_value(locked_until)])),
                ),
            )
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("renewing a lock produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => {
            if definitive_message_lock_loss(&rejection) {
                management
                    .unregister_managed_delivery(entity, link_name, lock_token)
                    .await;
                return ManagementResponse::lock_lost(
                    message_id,
                    tracking_id,
                    rejection.to_string(),
                );
            }
            ManagementResponse::from_rejection(message_id, tracking_id, &rejection)
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SessionLookupError {
    BadRequest(&'static str),
    LockLost(&'static str),
}

async fn requested_session(
    message: &Message,
    entity: &EntityPath,
    management: &ConnectionManagement,
) -> Result<ManagedSession, SessionLookupError> {
    let properties =
        message
            .application_properties
            .as_ref()
            .ok_or(SessionLookupError::BadRequest(
                "application properties are required",
            ))?;
    let link_name = string_property(properties, ASSOCIATED_LINK_NAME_PROPERTY).ok_or(
        SessionLookupError::BadRequest("the associated receive link name is required"),
    )?;
    let session_id = string_map_value(&message.body, SESSION_ID).ok_or(
        SessionLookupError::BadRequest("session-id must be an AMQP value string"),
    )?;
    let session = management
        .session(link_name)
        .await
        .ok_or(SessionLookupError::LockLost(
            "the associated link does not hold a session",
        ))?;
    if &session.entity != entity || session.hold.session_id.as_str() != session_id {
        return Err(SessionLookupError::LockLost(
            "the associated link does not hold the named session",
        ));
    }
    Ok(session)
}

fn session_lookup_response(
    message_id: MessageId,
    tracking_id: Option<String>,
    error: SessionLookupError,
) -> ManagementResponse {
    match error {
        SessionLookupError::BadRequest(description) => {
            ManagementResponse::bad_request(message_id, tracking_id, description)
        }
        SessionLookupError::LockLost(description) => {
            ManagementResponse::session_lock_lost(message_id, tracking_id, description)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn renew_session_lock<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
) -> ManagementResponse {
    let session = match requested_session(message, entity, management).await {
        Ok(session) => session,
        Err(error) => return session_lookup_response(message_id, tracking_id, error),
    };
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::RenewSessionLock {
                session: session.hold,
                lock_duration_millis: None,
            },
        )
        .await
    {
        Ok(CommandOutcome::SessionLockRenewed { locked_until }) => ManagementResponse::accepted(
            message_id,
            tracking_id,
            map_body(EXPIRATION, timestamp_value(locked_until)),
        ),
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("renewing a session lock produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

#[allow(clippy::too_many_arguments)]
async fn get_session_state<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
) -> ManagementResponse {
    let session = match requested_session(message, entity, management).await {
        Ok(session) => session,
        Err(error) => return session_lookup_response(message_id, tracking_id, error),
    };
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::GetSessionState {
                session: session.hold,
            },
        )
        .await
    {
        Ok(CommandOutcome::SessionState(state)) => {
            let state = if state.is_empty() {
                Value::Null
            } else {
                Value::Binary(Binary::from(state))
            };
            ManagementResponse::accepted(message_id, tracking_id, map_body(SESSION_STATE, state))
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("reading session state produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

#[allow(clippy::too_many_arguments)]
async fn set_session_state<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
) -> ManagementResponse {
    let session = match requested_session(message, entity, management).await {
        Ok(session) => session,
        Err(error) => return session_lookup_response(message_id, tracking_id, error),
    };
    let state = match map_value(&message.body, SESSION_STATE) {
        Some(Value::Binary(state)) => state.to_vec(),
        Some(Value::Null) => Vec::new(),
        _ => {
            return ManagementResponse::bad_request(
                message_id,
                tracking_id,
                "session-state must be an AMQP binary value or null",
            );
        }
    };
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::SetSessionState {
                session: session.hold,
                state,
            },
        )
        .await
    {
        Ok(CommandOutcome::SessionStateSet) => {
            ManagementResponse::accepted(message_id, tracking_id, Value::Null)
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("setting session state produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

fn string_property<'a>(properties: &'a ApplicationProperties, name: &str) -> Option<&'a str> {
    match properties.get(name) {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
}

fn map_value<'a>(body: &'a Body, name: &str) -> Option<&'a Value> {
    let Body::Value(Value::Map(map)) = body else {
        return None;
    };
    map.iter().find_map(|(key, value)| match key {
        Value::String(key) if key == name => Some(value),
        Value::Symbol(key) if key.as_str() == name => Some(value),
        _ => None,
    })
}

fn string_map_value<'a>(body: &'a Body, name: &str) -> Option<&'a str> {
    match map_value(body, name) {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
}

fn map_body(name: &str, value: Value) -> Value {
    let mut map = OrderedMap::new();
    map.insert(Value::String(name.to_owned()), value);
    Value::Map(map)
}

fn timestamp_value(timestamp: domain::Timestamp) -> Value {
    Value::Timestamp(AmqpTimestamp::from_milliseconds(
        i64::try_from(timestamp.as_millis()).unwrap_or(i64::MAX),
    ))
}

fn lock_tokens(body: &Body) -> Option<Vec<LockToken>> {
    let value = map_value(body, LOCK_TOKENS)?;
    let Value::Array(values) = value else {
        return None;
    };
    values
        .iter()
        .map(|value| match value {
            Value::Uuid(value) => lock_token(value),
            _ => None,
        })
        .collect()
}

fn lock_token(uuid: &Uuid) -> Option<LockToken> {
    let bytes = uuid.as_inner();
    if bytes[..8] != [0; 8] {
        return None;
    }
    Some(LockToken::new(u64::from_be_bytes(
        bytes[8..].try_into().ok()?,
    )))
}

pub(crate) async fn serve_management_replies(
    mut sender: Sender,
    address: String,
    route: mpsc::Sender<ManagementResponse>,
    mut responses: mpsc::Receiver<ManagementResponse>,
    management: Arc<ConnectionManagement>,
    authorization: Option<ManagementAuthorization>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        tokio::select! {
            _ = sender.on_detach() => {
                management.unregister_reply_route(&address, &route).await;
                return Ok(());
            }
            () = wait_until_unauthorized(authorization.as_ref()), if authorization.is_some() => {
                management.unregister_reply_route(&address, &route).await;
                sender.close_with_error(unauthorized_error(
                    "the management link's authorization has expired",
                )).await?;
                return Ok(());
            }
            response = responses.recv() => {
                let Some(response) = response else { return Ok(()) };
                let tag = response_delivery_tag(&response.correlation_id);
                debug!(?response.correlation_id, status_code = response.status_code, "sending management response");
                sender.send(response.into_message(), tag).await?;
                debug!("management response sent");
            }
        }
    }
}

async fn wait_until_unauthorized(authorization: Option<&ManagementAuthorization>) {
    match authorization {
        Some(authorization) => authorization.wait_until_unauthorized().await,
        None => std::future::pending().await,
    }
}

fn response_delivery_tag(message_id: &MessageId) -> Binary {
    Binary::from(format!("{message_id:?}").into_bytes())
}

fn unauthorized_error(description: impl Into<String>) -> AmqpProtocolError {
    AmqpProtocolError::new(AmqpError::UnauthorizedAccess, description.into(), None)
}

#[derive(Clone, Copy, Debug)]
struct RouteError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_tokens_are_read_from_guid_sized_delivery_tags() {
        let mut bytes = [0_u8; 16];
        bytes[8..].copy_from_slice(&42_u64.to_be_bytes());
        let mut map = OrderedMap::new();
        map.insert(
            Value::String(String::from(LOCK_TOKENS)),
            Value::Array(Array::from(vec![Value::Uuid(Uuid::from(bytes))])),
        );

        assert_eq!(
            lock_tokens(&Body::Value(Value::Map(map))),
            Some(vec![LockToken::new(42)])
        );
    }

    #[test]
    fn a_success_response_uses_the_management_contract_shapes() {
        let response = ManagementResponse::accepted(
            MessageId::Ulong(7),
            Some(String::from("trace-1")),
            map_body(
                EXPIRATIONS,
                Value::Array(Array::from(vec![timestamp_value(
                    domain::Timestamp::from_millis(12_345),
                )])),
            ),
        )
        .into_message();

        assert_eq!(
            response
                .application_properties
                .as_ref()
                .and_then(|properties| properties.get(STATUS_CODE_PROPERTY)),
            Some(&Value::Int(200))
        );
        let Body::Value(Value::Map(body)) = response.body else {
            panic!("the response must carry an AMQP value map");
        };
        assert_eq!(
            body.iter().find_map(|(key, value)| {
                (key == &Value::String(String::from(EXPIRATIONS))).then_some(value)
            }),
            Some(&Value::Array(Array::from(vec![Value::Timestamp(
                AmqpTimestamp::from_milliseconds(12_345)
            )])))
        );
    }

    #[test]
    fn scheduling_uses_send_permission_and_receive_management_uses_listen() {
        assert_eq!(
            management_permission(SCHEDULE_MESSAGE_OPERATION),
            Permission::Send
        );
        assert_eq!(
            management_permission(CANCEL_SCHEDULED_MESSAGE_OPERATION),
            Permission::Send
        );
        for operation in [
            RENEW_LOCK_OPERATION,
            RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            UPDATE_DISPOSITION_OPERATION,
            PEEK_MESSAGE_OPERATION,
            RENEW_SESSION_LOCK_OPERATION,
            GET_SESSION_STATE_OPERATION,
            SET_SESSION_STATE_OPERATION,
            ADD_RULE_OPERATION,
            REMOVE_RULE_OPERATION,
            ENUMERATE_RULES_OPERATION,
        ] {
            assert_eq!(management_permission(operation), Permission::Listen);
        }
    }
}
