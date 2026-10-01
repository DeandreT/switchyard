use std::{collections::HashMap, sync::Arc, time::Duration};

use amqp::{
    AmqpError, ApplicationProperties, Body, Error as AmqpProtocolError, Message,
    MessageDecodeBudget, MessageId, Properties, Receiver, Sender, decode_message_with_budget,
    encode_message,
};
use auth::{Permission, ResourceScope};
use domain::{
    CommandKind, CommandOutcome, DeliveryBudget, EntityPath, LockToken, NamespaceName,
    ScheduledEnvelope, SequenceNumber, SessionHold, SessionId, SettlementDisposition,
};
use serde_amqp::{
    Value,
    primitives::{Array, Binary, OrderedMap, Symbol, Timestamp as AmqpTimestamp, Uuid},
};
use tokio::sync::{Mutex, Notify, RwLock, mpsc};
use tracing::debug;

use crate::{
    Broker, BrokerRejection,
    authorization::ConnectionAuthorization,
    message::{read_incoming, write_delivery, write_peek_delivery},
    settlement::{dead_letter_disposition, read_properties_to_modify},
};

mod rules;
pub use rules::{
    ADD_RULE_OPERATION, ENUMERATE_RULES_OPERATION, REMOVE_RULE_OPERATION, RULE_DESCRIPTION,
    RULE_NAME, RULES,
};

pub const PEEK_MESSAGE_OPERATION: &str = "com.microsoft:peek-message";
pub const SCHEDULE_MESSAGE_OPERATION: &str = "com.microsoft:schedule-message";
pub const CANCEL_SCHEDULED_MESSAGE_OPERATION: &str = "com.microsoft:cancel-scheduled-message";
pub const RECEIVE_BY_SEQUENCE_NUMBER_OPERATION: &str = "com.microsoft:receive-by-sequence-number";
pub const RENEW_LOCK_OPERATION: &str = "com.microsoft:renew-lock";
pub const RENEW_SESSION_LOCK_OPERATION: &str = "com.microsoft:renew-session-lock";
pub const UPDATE_DISPOSITION_OPERATION: &str = "com.microsoft:update-disposition";
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
pub const FROM_SEQUENCE_NUMBER: &str = "from-sequence-number";
pub const MESSAGE_COUNT: &str = "message-count";
pub const MESSAGES: &str = "messages";
pub const MESSAGE: &str = "message";
pub const LOCK_TOKEN: &str = "lock-token";
pub const SEQUENCE_NUMBERS: &str = "sequence-numbers";
pub const RECEIVER_SETTLE_MODE: &str = "receiver-settle-mode";
pub const DISPOSITION_STATUS: &str = "disposition-status";
pub const PROPERTIES_TO_MODIFY: &str = "properties-to-modify";
pub const DEAD_LETTER_REASON: &str = "deadletter-reason";
pub const DEAD_LETTER_DESCRIPTION: &str = "deadletter-description";
pub const SESSION_ID: &str = "session-id";
pub const SESSION_STATE: &str = "session-state";

const REPLY_ROUTE_TIMEOUT: Duration = Duration::from_secs(2);
const REPLY_BUFFER: usize = 16;
const MAX_MANAGEMENT_RESPONSE_BYTES: u64 = 4 * 1024 * 1024;
const RESPONSE_ENTRY_OVERHEAD_BYTES: u64 = 64;
const RESPONSE_WRAPPER_RESERVE_BYTES: u64 = 256;
const RESPONSE_SIZE_DESCRIPTION: &str = "the requested response exceeds the reply capacity";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DeliveryKey {
    link_name: String,
    lock_token: LockToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManagedDelivery {
    entity: EntityPath,
    sequence: SequenceNumber,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManagedSession {
    entity: EntityPath,
    hold: SessionHold,
}

#[derive(Debug, Default)]
struct ReplyRoutes {
    senders: HashMap<String, ReplyRoute>,
}

#[derive(Clone, Debug)]
struct ReplyRoute {
    sender: mpsc::Sender<ManagementResponse>,
    max_message_size: u64,
}

/// Protocol-only state shared by every session on one AMQP connection.
///
/// Delivery tags and dynamic reply addresses have connection scope. Neither is
/// replicated broker state, and both disappear when the connection does.
#[derive(Debug, Default)]
pub(crate) struct ConnectionManagement {
    deliveries: RwLock<HashMap<DeliveryKey, ManagedDelivery>>,
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
            ManagedDelivery { entity, sequence },
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
        max_message_size: Option<u64>,
    ) -> (
        mpsc::Sender<ManagementResponse>,
        mpsc::Receiver<ManagementResponse>,
    ) {
        let (sender, receiver) = mpsc::channel(REPLY_BUFFER);
        self.routes.lock().await.senders.insert(
            address,
            ReplyRoute {
                sender: sender.clone(),
                max_message_size: max_message_size
                    .filter(|limit| *limit != 0)
                    .unwrap_or(u64::MAX)
                    .min(MAX_MANAGEMENT_RESPONSE_BYTES),
            },
        );
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
            .is_some_and(|current| current.sender.same_channel(sender))
        {
            routes.senders.remove(address);
        }
    }

    async fn reply_route(&self, address: &str) -> Result<ReplyRoute, RouteError> {
        let deadline = tokio::time::Instant::now() + REPLY_ROUTE_TIMEOUT;
        loop {
            let changed = self.route_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let route = self.routes.lock().await.senders.get(address).cloned();
            if let Some(route) = route {
                if !route.sender.is_closed() {
                    return Ok(route);
                }
                self.unregister_reply_route(address, &route.sender).await;
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

    async fn ensure(&self) -> Result<(), AmqpProtocolError> {
        self.connection
            .authorize_resource_any(&self.resource, &[Permission::Send, Permission::Listen])
            .await
            .map_err(|_| unauthorized_error("the management link's authorization has expired"))
    }

    async fn ensure_permission(&self, permission: Permission) -> Result<(), AmqpProtocolError> {
        self.connection
            .authorize_resource(&self.resource, permission)
            .await
            .map_err(|_| unauthorized_error("the management operation is not authorized"))
    }

    async fn wait_until_unauthorized(&self) {
        self.connection
            .wait_until_unauthorized_any(&self.resource, &[Permission::Send, Permission::Listen])
            .await;
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ManagementResponse {
    correlation_id: MessageId,
    status_code: i32,
    status_description: String,
    error_condition: Option<&'static str>,
    tracking_id: Option<String>,
    body: Value,
}

impl ManagementResponse {
    fn accepted(correlation_id: MessageId, tracking_id: Option<String>, body: Value) -> Self {
        Self {
            correlation_id,
            status_code: 200,
            status_description: String::from("OK"),
            error_condition: None,
            tracking_id,
            body,
        }
    }

    fn bad_request(
        correlation_id: MessageId,
        tracking_id: Option<String>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            correlation_id,
            status_code: 400,
            status_description: description.into(),
            error_condition: None,
            tracking_id,
            body: Value::Null,
        }
    }

    fn invalid_field(
        correlation_id: MessageId,
        tracking_id: Option<String>,
        description: impl Into<String>,
    ) -> Self {
        let mut response = Self::bad_request(correlation_id, tracking_id, description);
        response.error_condition = Some(crate::INVALID_FIELD);
        response
    }

    fn from_rejection(
        correlation_id: MessageId,
        tracking_id: Option<String>,
        rejection: &BrokerRejection,
    ) -> Self {
        let condition = rejection.condition();
        let status_code = match condition {
            crate::MESSAGE_LOCK_LOST | crate::SESSION_LOCK_LOST => 410,
            crate::NOT_FOUND | crate::condition::MESSAGE_NOT_FOUND => 404,
            crate::ENTITY_ALREADY_EXISTS => 409,
            crate::NOT_IMPLEMENTED
                if matches!(
                    rejection,
                    BrokerRejection::Refused(domain::BrokerError::SqlRuleCompilation(
                        domain::SqlCompileError::Unsupported { .. }
                    ))
                ) =>
            {
                501
            }
            crate::MESSAGE_SIZE_EXCEEDED | crate::RESOURCE_LIMIT_EXCEEDED => 403,
            crate::INVALID_FIELD | crate::NOT_ALLOWED | crate::PRECONDITION_FAILED => 400,
            crate::RESOURCE_LOCKED => 503,
            _ => 500,
        };
        Self {
            correlation_id,
            status_code,
            status_description: rejection.to_string(),
            error_condition: Some(condition),
            tracking_id,
            body: Value::Null,
        }
    }

    fn lock_lost(
        correlation_id: MessageId,
        tracking_id: Option<String>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            correlation_id,
            status_code: 410,
            status_description: description.into(),
            error_condition: Some(crate::MESSAGE_LOCK_LOST),
            tracking_id,
            body: Value::Null,
        }
    }

    fn too_large(correlation_id: MessageId, tracking_id: Option<String>) -> Self {
        Self {
            correlation_id,
            status_code: 403,
            status_description: RESPONSE_SIZE_DESCRIPTION.to_owned(),
            error_condition: Some(crate::MESSAGE_SIZE_EXCEEDED),
            tracking_id,
            body: Value::Null,
        }
    }

    fn message_not_found(correlation_id: MessageId, tracking_id: Option<String>) -> Self {
        Self {
            correlation_id,
            status_code: 404,
            status_description: "the requested deferred messages were not found".to_owned(),
            error_condition: Some(crate::condition::MESSAGE_NOT_FOUND),
            tracking_id,
            body: Value::Null,
        }
    }

    fn session_lock_lost(
        correlation_id: MessageId,
        tracking_id: Option<String>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            correlation_id,
            status_code: 410,
            status_description: description.into(),
            error_condition: Some(crate::SESSION_LOCK_LOST),
            tracking_id,
            body: Value::Null,
        }
    }

    fn internal(
        correlation_id: MessageId,
        tracking_id: Option<String>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            correlation_id,
            status_code: 500,
            status_description: description.into(),
            error_condition: Some(crate::INTERNAL_ERROR),
            tracking_id,
            body: Value::Null,
        }
    }

    fn unauthorized(correlation_id: MessageId, tracking_id: Option<String>) -> Self {
        Self {
            correlation_id,
            status_code: 401,
            status_description: "the management operation is not authorized".to_owned(),
            error_condition: Some("amqp:unauthorized-access"),
            tracking_id,
            body: Value::Null,
        }
    }

    fn into_message(self) -> Message {
        let mut application_properties = ApplicationProperties::default();
        application_properties.insert(STATUS_CODE_PROPERTY, self.status_code);
        application_properties.insert(STATUS_DESCRIPTION_PROPERTY, self.status_description);
        if let Some(condition) = self.error_condition {
            application_properties.insert(ERROR_CONDITION_PROPERTY, Symbol::from(condition));
        }
        if let Some(tracking_id) = self.tracking_id {
            application_properties.insert(TRACKING_ID_PROPERTY, tracking_id);
        }

        Message {
            properties: Some(Properties {
                correlation_id: Some(self.correlation_id),
                ..Properties::default()
            }),
            application_properties: Some(application_properties),
            body: Body::Value(self.body),
            ..Message::default()
        }
    }
}

fn response_budget(
    correlation_id: &MessageId,
    tracking_id: Option<&str>,
    max_message_size: u64,
) -> Result<Option<DeliveryBudget>, std::io::Error> {
    let success = ManagementResponse::accepted(
        correlation_id.clone(),
        tracking_id.map(str::to_owned),
        map_body(MESSAGES, Value::List(Vec::new())),
    );
    let refusal =
        ManagementResponse::too_large(correlation_id.clone(), tracking_id.map(str::to_owned));
    // Reserve the actual correlation/tracking fields and enough additional
    // wrapper space for the other response body shapes and diagnostic fields.
    let wrapper_bytes = (encode_message(&success.into_message())?.len() as u64)
        .max(encode_message(&refusal.into_message())?.len() as u64)
        .saturating_add(RESPONSE_WRAPPER_RESERVE_BYTES);
    Ok(max_message_size
        .checked_sub(wrapper_bytes)
        .map(|max_bytes| DeliveryBudget {
            max_bytes,
            per_message_overhead_bytes: RESPONSE_ENTRY_OVERHEAD_BYTES,
        }))
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
            ) => {
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

        let route = match management.reply_route(&reply_to).await {
            Ok(route) => route,
            Err(_) => {
                receiver
                    .reject(
                        &delivery,
                        Some(AmqpProtocolError::new(
                            AmqpError::PreconditionFailed,
                            "the management reply link is not attached",
                            None,
                        )),
                    )
                    .await?;
                continue;
            }
        };
        let tracking_id = delivery
            .message()
            .application_properties
            .as_ref()
            .and_then(|properties| string_property(properties, TRACKING_ID_PROPERTY));
        let Some(budget) = response_budget(&message_id, tracking_id, route.max_message_size)?
        else {
            receiver
                .reject(
                    &delivery,
                    Some(AmqpProtocolError::new(
                        amqp::ErrorCondition::Custom(Symbol::from(crate::MESSAGE_SIZE_EXCEEDED)),
                        RESPONSE_SIZE_DESCRIPTION,
                        None,
                    )),
                )
                .await?;
            continue;
        };
        let mut response = process_request(
            delivery.message(),
            message_id,
            &namespace,
            &entity,
            &broker,
            &management,
            authorization.as_ref(),
            budget,
        )
        .await;
        if encode_message(&response.clone().into_message())?.len() as u64 > route.max_message_size {
            response = ManagementResponse::too_large(response.correlation_id, response.tracking_id);
        }
        debug!(correlation_id = ?response.correlation_id, %reply_to, status_code = response.status_code, "management request processed");
        receiver.accept(&delivery).await?;
        if route.sender.send(response).await.is_err() {
            debug!(%reply_to, "management reply route disappeared");
        } else {
            debug!(%reply_to, "management response routed");
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_request<B: Broker>(
    message: &Message,
    message_id: MessageId,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
    authorization: Option<&ManagementAuthorization>,
    budget: DeliveryBudget,
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
    let permission = match operation {
        SCHEDULE_MESSAGE_OPERATION | CANCEL_SCHEDULED_MESSAGE_OPERATION => Permission::Send,
        _ => Permission::Listen,
    };
    if let Some(authorization) = authorization
        && authorization.ensure_permission(permission).await.is_err()
    {
        return ManagementResponse::unauthorized(message_id, tracking_id);
    }
    match operation {
        ADD_RULE_OPERATION | REMOVE_RULE_OPERATION | ENUMERATE_RULES_OPERATION => {
            rules::process(
                operation,
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                budget,
            )
            .await
        }
        SCHEDULE_MESSAGE_OPERATION => {
            schedule_messages(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                budget,
            )
            .await
        }
        CANCEL_SCHEDULED_MESSAGE_OPERATION => {
            cancel_scheduled_messages(message, message_id, tracking_id, namespace, entity, broker)
                .await
        }
        PEEK_MESSAGE_OPERATION => {
            peek_messages(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                budget,
            )
            .await
        }
        RECEIVE_BY_SEQUENCE_NUMBER_OPERATION => {
            receive_by_sequence_number(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
                budget,
            )
            .await
        }
        RENEW_LOCK_OPERATION => {
            renew_message_lock(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
                budget,
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
        UPDATE_DISPOSITION_OPERATION => {
            update_disposition(
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
        _ => ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "unsupported management operation",
        ),
    }
}

#[allow(clippy::too_many_arguments)]
async fn schedule_messages<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    budget: DeliveryBudget,
) -> ManagementResponse {
    let messages = match scheduled_messages(&message.body) {
        Ok(messages) => messages,
        Err(description) => {
            return description.into_response(message_id, tracking_id);
        }
    };
    if (messages.len() as u64).saturating_mul(9) > budget.max_bytes {
        return ManagementResponse::too_large(message_id, tracking_id);
    }
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::ScheduleEnvelopes { messages },
        )
        .await
    {
        Ok(CommandOutcome::Scheduled { sequences }) => ManagementResponse::accepted(
            message_id,
            tracking_id,
            map_body(
                SEQUENCE_NUMBERS,
                Value::Array(Array::from(
                    sequences
                        .into_iter()
                        .map(|sequence| {
                            Value::Long(i64::try_from(sequence.as_u64()).unwrap_or(i64::MAX))
                        })
                        .collect::<Vec<_>>(),
                )),
            ),
        ),
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("scheduling messages produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

#[derive(Debug, thiserror::Error)]
enum ScheduleRequestError {
    #[error("{0}")]
    Malformed(String),
    #[error(transparent)]
    Message(#[from] crate::ProtocolError),
    #[error(transparent)]
    Refused(#[from] domain::BrokerError),
}

impl ScheduleRequestError {
    fn into_response(
        self,
        correlation_id: MessageId,
        tracking_id: Option<String>,
    ) -> ManagementResponse {
        match self {
            Self::Refused(error) => ManagementResponse::from_rejection(
                correlation_id,
                tracking_id,
                &BrokerRejection::Refused(error),
            ),
            Self::Message(error @ crate::ProtocolError::MessageTooLarge { .. }) => {
                ManagementResponse {
                    correlation_id,
                    status_code: 403,
                    status_description: error.to_string(),
                    error_condition: Some(crate::MESSAGE_SIZE_EXCEEDED),
                    tracking_id,
                    body: Value::Null,
                }
            }
            other => {
                ManagementResponse::invalid_field(correlation_id, tracking_id, other.to_string())
            }
        }
    }
}

fn scheduled_messages(body: &Body) -> Result<Vec<ScheduledEnvelope>, ScheduleRequestError> {
    let Some(Value::List(entries)) = map_value(body, MESSAGES) else {
        return Err(ScheduleRequestError::Malformed(
            "messages must be an AMQP list of maps".to_owned(),
        ));
    };
    if entries.is_empty() {
        return Err(ScheduleRequestError::Malformed(
            "at least one message is required".to_owned(),
        ));
    }
    if entries.len() > domain::MAX_INGRESS_BATCH_MESSAGES {
        return Err(domain::BrokerError::IngressBatchLimitExceeded {
            limit: domain::IngressBatchLimit::Messages,
            actual: entries.len(),
            maximum: domain::MAX_INGRESS_BATCH_MESSAGES,
        }
        .into());
    }
    let mut messages = Vec::with_capacity(entries.len());
    let mut decode_budget = MessageDecodeBudget::default();
    for entry in entries {
        let Value::Map(entry) = entry else {
            return Err(ScheduleRequestError::Malformed(
                "each scheduled message must be an AMQP map".to_owned(),
            ));
        };
        let Some(Value::Binary(encoded)) = entry.get(&Value::String(MESSAGE.to_owned())) else {
            return Err(ScheduleRequestError::Malformed(
                "each scheduled message must contain a binary message".to_owned(),
            ));
        };
        crate::validate_standard_message_size(encoded.len())?;
        let decoded = decode_message_with_budget(encoded, &mut decode_budget)
            .map_err(|error| ScheduleRequestError::Malformed(error.to_string()))?;
        let incoming = read_incoming(&decoded)?;
        let enqueue_at = incoming.scheduled_enqueue_time.ok_or_else(|| {
            ScheduleRequestError::Malformed(
                "each scheduled message must specify its enqueue timestamp".to_owned(),
            )
        })?;
        messages.push(ScheduledEnvelope {
            message_id: incoming.message_id,
            body: incoming.body,
            time_to_live_millis: incoming.time_to_live_millis,
            session_id: incoming.session_id,
            enqueue_at,
            envelope: incoming.envelope,
        });
    }
    Ok(messages)
}

async fn cancel_scheduled_messages<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
) -> ManagementResponse {
    let Some(sequences) = sequence_numbers(&message.body).filter(|sequences| !sequences.is_empty())
    else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "sequence-numbers must contain at least one non-negative integer",
        );
    };
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::CancelScheduled { sequences },
        )
        .await
    {
        Ok(CommandOutcome::ScheduledCancelled { .. }) => {
            ManagementResponse::accepted(message_id, tracking_id, Value::Null)
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("cancelling scheduled messages produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

#[allow(clippy::too_many_arguments)]
async fn receive_by_sequence_number<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
    budget: DeliveryBudget,
) -> ManagementResponse {
    let link_name = message
        .application_properties
        .as_ref()
        .and_then(|properties| string_property(properties, ASSOCIATED_LINK_NAME_PROPERTY));
    let Some(sequences) = sequence_numbers(&message.body) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "sequence-numbers must be an AMQP array or list of non-negative integer values",
        );
    };
    let mode = match unsigned_map_value(&message.body, RECEIVER_SETTLE_MODE) {
        Some(0) => domain::ReceiveMode::ReceiveAndDelete,
        Some(1) => domain::ReceiveMode::PeekLock,
        _ => {
            return ManagementResponse::bad_request(
                message_id,
                tracking_id,
                "receiver-settle-mode must be 0 or 1",
            );
        }
    };
    let session = match map_value(&message.body, SESSION_ID) {
        Some(Value::String(session_id)) => {
            if let Err(error) = SessionId::new(session_id) {
                return ManagementResponse::bad_request(
                    message_id,
                    tracking_id,
                    format!("session-id is invalid: {error}"),
                );
            }
            match requested_session(message, entity, management).await {
                Ok(session) => Some(session.hold),
                Err(error) => return session_lookup_response(message_id, tracking_id, error),
            }
        }
        Some(_) => {
            return ManagementResponse::bad_request(
                message_id,
                tracking_id,
                "session-id must be an AMQP value string",
            );
        }
        None => None,
    };

    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::ReceiveDeferredHeld {
                sequences,
                mode,
                lock_duration_millis: None,
                session,
                budget,
            },
        )
        .await
    {
        Ok(CommandOutcome::DeferredReceived(deliveries)) => {
            // The successful command may have removed expired deferred records.
            // Report absence only after that cleanup has committed.
            if deliveries.is_empty() {
                return ManagementResponse::message_not_found(message_id, tracking_id);
            }
            let mut messages = Vec::with_capacity(deliveries.len());
            for delivery in deliveries {
                let encoded = match encode_message(&write_delivery(&delivery)) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        return ManagementResponse::internal(
                            message_id,
                            tracking_id,
                            format!("encoding a deferred message failed: {error}"),
                        );
                    }
                };
                let lock = delivery.lock;
                if let (Some(link_name), Some(lock)) = (link_name, lock) {
                    management
                        .register_delivery(link_name, entity.clone(), delivery.sequence, lock.token)
                        .await;
                }

                let mut entry = OrderedMap::new();
                entry.insert(
                    Value::String(MESSAGE.to_owned()),
                    Value::Binary(Binary::from(encoded)),
                );
                if let Some(lock) = lock {
                    entry.insert(
                        Value::String(LOCK_TOKEN.to_owned()),
                        Value::Uuid(lock_token_uuid(lock.token)),
                    );
                }
                messages.push(Value::Map(entry));
            }
            ManagementResponse::accepted(
                message_id,
                tracking_id,
                map_body(MESSAGES, Value::List(messages)),
            )
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("receiving deferred messages produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

#[allow(clippy::too_many_arguments)]
async fn update_disposition<B: Broker>(
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
    let Some(link_name) = string_property(properties, ASSOCIATED_LINK_NAME_PROPERTY) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "the associated receive link name is required",
        );
    };
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
    let Some(status) = string_map_value(&message.body, DISPOSITION_STATUS) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "disposition-status must be an AMQP string value",
        );
    };

    let lock_token = tokens[0];
    let Some(delivery) = management.delivery(link_name, lock_token).await else {
        return ManagementResponse::lock_lost(
            message_id,
            tracking_id,
            "the lock token is not active on the associated link",
        );
    };
    if &delivery.entity != entity {
        return ManagementResponse::lock_lost(
            message_id,
            tracking_id,
            "the lock token belongs to another entity",
        );
    }

    let mut properties_to_modify =
        match read_properties_to_modify(map_value(&message.body, PROPERTIES_TO_MODIFY)) {
            Ok(properties) => properties,
            Err(error) => {
                return ManagementResponse::invalid_field(
                    message_id,
                    tracking_id,
                    error.to_string(),
                );
            }
        };
    let disposition = match status {
        "completed" => SettlementDisposition::Complete,
        "abandoned" => SettlementDisposition::Abandon,
        "defered" => SettlementDisposition::Defer,
        "suspended" => {
            for name in [DEAD_LETTER_REASON, DEAD_LETTER_DESCRIPTION] {
                if map_value(&message.body, name)
                    .is_some_and(|value| !matches!(value, Value::String(_) | Value::Null))
                {
                    return ManagementResponse::invalid_field(
                        message_id,
                        tracking_id,
                        format!("{name} must be a string"),
                    );
                }
            }
            match dead_letter_disposition(
                string_map_value(&message.body, DEAD_LETTER_REASON).map(str::to_owned),
                string_map_value(&message.body, DEAD_LETTER_DESCRIPTION).map(str::to_owned),
                &mut properties_to_modify,
                "DeadLetteredByReceiver",
                "the receiver dead-lettered the message",
            ) {
                Ok(disposition) => disposition,
                Err(error) => {
                    return ManagementResponse::invalid_field(
                        message_id,
                        tracking_id,
                        error.to_string(),
                    );
                }
            }
        }
        _ => {
            return ManagementResponse::bad_request(
                message_id,
                tracking_id,
                format!("unsupported disposition-status {status:?}"),
            );
        }
    };
    let kind = CommandKind::Settle {
        sequence: delivery.sequence,
        lock_token,
        disposition,
        properties_to_modify,
    };

    match broker.submit(namespace.clone(), entity.clone(), kind).await {
        Ok(
            CommandOutcome::Completed
            | CommandOutcome::Abandoned { .. }
            | CommandOutcome::Deferred
            | CommandOutcome::DeadLettered,
        ) => {
            management.unregister_delivery(link_name, lock_token).await;
            ManagementResponse::accepted(message_id, tracking_id, Value::Null)
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("updating disposition produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

#[allow(clippy::too_many_arguments)]
async fn peek_messages<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    budget: DeliveryBudget,
) -> ManagementResponse {
    let Some(from_sequence) = unsigned_map_value(&message.body, FROM_SEQUENCE_NUMBER) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "from-sequence-number must be a non-negative AMQP integer value",
        );
    };
    let Some(message_count) = unsigned_map_value(&message.body, MESSAGE_COUNT) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "message-count must be a non-negative AMQP integer value",
        );
    };
    let Ok(message_count) = u32::try_from(message_count) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "message-count exceeds the supported maximum",
        );
    };
    let session_id = match map_value(&message.body, SESSION_ID) {
        Some(Value::String(session_id)) => match SessionId::new(session_id) {
            Ok(session_id) => Some(session_id),
            Err(error) => {
                return ManagementResponse::bad_request(
                    message_id,
                    tracking_id,
                    format!("session-id is invalid: {error}"),
                );
            }
        },
        Some(_) => {
            return ManagementResponse::bad_request(
                message_id,
                tracking_id,
                "session-id must be an AMQP value string",
            );
        }
        None => None,
    };

    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::PeekBounded {
                from_sequence: SequenceNumber::new(from_sequence),
                max_messages: message_count,
                session_id,
                budget,
            },
        )
        .await
    {
        Ok(CommandOutcome::Peeked(deliveries)) => {
            let mut messages = Vec::with_capacity(deliveries.len());
            for delivery in deliveries {
                let encoded = match encode_message(&write_peek_delivery(&delivery)) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        return ManagementResponse::internal(
                            message_id,
                            tracking_id,
                            format!("encoding a peeked message failed: {error}"),
                        );
                    }
                };
                let mut entry = OrderedMap::new();
                entry.insert(
                    Value::String(MESSAGE.to_owned()),
                    Value::Binary(Binary::from(encoded)),
                );
                messages.push(Value::Map(entry));
            }
            ManagementResponse::accepted(
                message_id,
                tracking_id,
                map_body(MESSAGES, Value::List(messages)),
            )
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("peeking messages produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
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
    budget: DeliveryBudget,
) -> ManagementResponse {
    let Some(properties) = message.application_properties.as_ref() else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "application properties are required",
        );
    };
    let Some(link_name) = string_property(properties, ASSOCIATED_LINK_NAME_PROPERTY) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "the associated receive link name is required",
        );
    };
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
    if budget.max_bytes < 9 {
        return ManagementResponse::too_large(message_id, tracking_id);
    }

    let lock_token = tokens[0];
    let Some(delivery) = management.delivery(link_name, lock_token).await else {
        return ManagementResponse::lock_lost(
            message_id,
            tracking_id,
            "the lock token is not active on the associated link",
        );
    };
    if &delivery.entity != entity {
        return ManagementResponse::lock_lost(
            message_id,
            tracking_id,
            "the lock token belongs to another entity",
        );
    }

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
        Ok(CommandOutcome::LockRenewed { locked_until }) => ManagementResponse::accepted(
            message_id,
            tracking_id,
            map_body(
                EXPIRATIONS,
                Value::Array(Array::from(vec![timestamp_value(locked_until)])),
            ),
        ),
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("renewing a lock produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
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

fn unsigned_map_value(body: &Body, name: &str) -> Option<u64> {
    unsigned_value(map_value(body, name)?)
}

fn unsigned_value(value: &Value) -> Option<u64> {
    match value {
        Value::Ubyte(value) => Some(u64::from(*value)),
        Value::Ushort(value) => Some(u64::from(*value)),
        Value::Uint(value) => Some(u64::from(*value)),
        Value::Ulong(value) => Some(*value),
        Value::Byte(value) => u64::try_from(*value).ok(),
        Value::Short(value) => u64::try_from(*value).ok(),
        Value::Int(value) => u64::try_from(*value).ok(),
        Value::Long(value) => u64::try_from(*value).ok(),
        _ => None,
    }
}

fn sequence_numbers(body: &Body) -> Option<Vec<SequenceNumber>> {
    let value = map_value(body, SEQUENCE_NUMBERS)?;
    match value {
        Value::Array(values) => values
            .iter()
            .map(|value| unsigned_value(value).map(SequenceNumber::new))
            .collect(),
        Value::List(values) => values
            .iter()
            .map(|value| unsigned_value(value).map(SequenceNumber::new))
            .collect(),
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

fn lock_token_uuid(token: LockToken) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes[8..].copy_from_slice(&token.as_u64().to_be_bytes());
    Uuid::from(bytes)
}

pub(crate) async fn serve_management_replies(
    mut sender: Sender,
    address: String,
    route: mpsc::Sender<ManagementResponse>,
    mut responses: mpsc::Receiver<ManagementResponse>,
    management: Arc<ConnectionManagement>,
    authorization: Option<ManagementAuthorization>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut next_delivery_tag = 0_u64;
    loop {
        tokio::select! {
            _ = sender.on_detach() => {
                management.unregister_reply_route(&address, &route).await;
                let _ = sender.close().await;
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
                let tag = Binary::from(next_delivery_tag.to_be_bytes().to_vec());
                next_delivery_tag = next_delivery_tag.wrapping_add(1);
                debug!(?response.correlation_id, status_code = response.status_code, "sending management response");
                if let Err(error) = sender.send(response.into_message(), tag).await {
                    management.unregister_reply_route(&address, &route).await;
                    return Err(error.into());
                }
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

fn unauthorized_error(description: impl Into<String>) -> AmqpProtocolError {
    AmqpProtocolError::new(AmqpError::UnauthorizedAccess, description.into(), None)
}

#[derive(Clone, Copy, Debug)]
struct RouteError;

#[cfg(test)]
mod peek_session_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_budgets_reserve_typed_correlation_and_tracking_fields() {
        let ids = [
            MessageId::Ulong(u64::MAX),
            MessageId::Uuid(Uuid::from([7; 16])),
            MessageId::String("correlation".repeat(60)),
            MessageId::Binary(Binary::from(vec![8; 700])),
        ];
        for id in ids {
            let limit = 4_096;
            let tracking = "tracking".repeat(80);
            let plain = response_budget(&id, None, limit)
                .expect("valid wrapper")
                .expect("fits");
            let budget = response_budget(&id, Some(&tracking), limit)
                .expect("valid wrapper")
                .expect("fits");
            assert!(budget.max_bytes < plain.max_bytes);
            assert_eq!(
                budget.per_message_overhead_bytes,
                RESPONSE_ENTRY_OVERHEAD_BYTES
            );
            let payload_bytes = budget.max_bytes - budget.per_message_overhead_bytes;
            let mut entry = OrderedMap::new();
            entry.insert(
                Value::String(MESSAGE.to_owned()),
                Value::Binary(Binary::from(vec![0; payload_bytes as usize])),
            );
            entry.insert(
                Value::String(LOCK_TOKEN.to_owned()),
                Value::Uuid(Uuid::from([1; 16])),
            );
            let response = ManagementResponse::accepted(
                id.clone(),
                Some(tracking.clone()),
                map_body(MESSAGES, Value::List(vec![Value::Map(entry)])),
            );
            assert!(
                encode_message(&response.into_message())
                    .expect("valid response")
                    .len() as u64
                    <= limit
            );
            let refusal = ManagementResponse::too_large(id, Some(tracking));
            assert!(
                encode_message(&refusal.into_message())
                    .expect("valid refusal")
                    .len() as u64
                    <= limit
            );
        }
    }

    #[test]
    fn a_reply_too_small_for_its_wrapper_has_no_delivery_budget() {
        let id = MessageId::String("large correlation".repeat(100));
        assert_eq!(
            response_budget(&id, Some("tracking"), 128).expect("valid wrapper"),
            None
        );
    }

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

    fn scheduled_request(encoded: Vec<u8>) -> Body {
        let entry = [(
            Value::String(MESSAGE.to_owned()),
            Value::Binary(encoded.into()),
        )]
        .into_iter()
        .collect();
        Body::Value(map_body(MESSAGES, Value::List(vec![Value::Map(entry)])))
    }

    #[test]
    fn a_scheduled_inner_message_size_failure_keeps_the_quota_condition() {
        let error = scheduled_messages(&scheduled_request(vec![
            0;
            crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES
                + 1
        ]))
        .expect_err("the inner message exceeds the quota");
        assert!(matches!(
            error,
            ScheduleRequestError::Message(crate::ProtocolError::MessageTooLarge { .. })
        ));
        let response = error.into_response(MessageId::Ulong(1), Some("trace".to_owned()));
        assert_eq!(response.status_code, 403);
        assert_eq!(response.error_condition, Some(crate::MESSAGE_SIZE_EXCEEDED));
        assert_eq!(response.tracking_id.as_deref(), Some("trace"));
        assert_eq!(response.correlation_id, MessageId::Ulong(1));
    }

    #[test]
    fn a_malformed_scheduled_message_remains_an_invalid_request() {
        let error = scheduled_messages(&scheduled_request(vec![0]))
            .expect_err("the inner message is not valid AMQP");
        let response = error.into_response(MessageId::Ulong(1), None);
        assert_eq!(response.status_code, 400);
        assert_eq!(response.error_condition, Some(crate::INVALID_FIELD));
    }

    #[test]
    fn scheduled_message_count_is_refused_before_decoding_entries() {
        let count = domain::MAX_INGRESS_BATCH_MESSAGES + 1;
        let body = Body::Value(map_body(MESSAGES, Value::List(vec![Value::Null; count])));
        let error = scheduled_messages(&body).expect_err("the batch exceeds its entry quota");
        assert!(matches!(
            error,
            ScheduleRequestError::Refused(domain::BrokerError::IngressBatchLimitExceeded {
                limit: domain::IngressBatchLimit::Messages,
                actual,
                maximum: domain::MAX_INGRESS_BATCH_MESSAGES,
            }) if actual == count
        ));
        let response = error.into_response(MessageId::Ulong(7), Some("trace".to_owned()));
        assert_eq!(response.status_code, 403);
        assert_eq!(
            response.error_condition,
            Some(crate::RESOURCE_LIMIT_EXCEEDED)
        );
        assert_eq!(response.correlation_id, MessageId::Ulong(7));
        assert_eq!(response.tracking_id.as_deref(), Some("trace"));
    }

    #[test]
    fn scheduled_messages_share_one_inner_value_budget() {
        let mut annotations = amqp::Annotations::new();
        annotations.insert(
            Symbol::from(crate::message::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(AmqpTimestamp::from_milliseconds(3_000)),
        );
        let message = Message {
            message_annotations: Some(annotations),
            ..Message::default()
        };
        let mut encoded = encode_message(&message).expect("valid scheduling annotations");
        // The null constructor represents 40,000 array members without payload bytes.
        encoded.extend_from_slice(&[0x00, 0x53, 0x77, 0xf0]);
        encoded.extend_from_slice(&5_u32.to_be_bytes());
        encoded.extend_from_slice(&40_000_u32.to_be_bytes());
        encoded.push(0x40);
        amqp::decode_message(&encoded).expect("each compact message fits alone");
        scheduled_messages(&scheduled_request(encoded.clone()))
            .expect("one decoded message fits the domain limits");
        let entry = Value::Map(
            [(
                Value::String(MESSAGE.to_owned()),
                Value::Binary(encoded.into()),
            )]
            .into_iter()
            .collect(),
        );
        let body = Body::Value(map_body(MESSAGES, Value::List(vec![entry; 4])));
        let error = scheduled_messages(&body).expect_err("the cumulative decode quota is exceeded");
        let response = error.into_response(MessageId::Ulong(8), None);
        assert_eq!(response.status_code, 400);
        assert_eq!(response.error_condition, Some(crate::INVALID_FIELD));
        assert_eq!(response.correlation_id, MessageId::Ulong(8));
    }

    #[test]
    fn a_domain_size_failure_uses_the_sdk_quota_status() {
        let response = ManagementResponse::from_rejection(
            MessageId::Ulong(1),
            None,
            &BrokerRejection::Refused(domain::BrokerError::MessageTooLarge {
                body_bytes: 101,
                maximum_bytes: 100,
            }),
        );
        assert_eq!(response.status_code, 403);
        assert_eq!(response.error_condition, Some(crate::MESSAGE_SIZE_EXCEEDED));
    }

    #[test]
    fn exhausted_identifiers_use_the_quota_status_and_preserve_correlation() {
        for counter in [
            domain::QueueCounterKind::Sequence,
            domain::QueueCounterKind::LockToken,
        ] {
            let response = ManagementResponse::from_rejection(
                MessageId::Ulong(7),
                Some("trace".to_owned()),
                &BrokerRejection::Refused(domain::BrokerError::QueueCounterExhausted { counter }),
            );
            assert_eq!(response.status_code, 403);
            assert_eq!(
                response.error_condition,
                Some(crate::RESOURCE_LIMIT_EXCEEDED)
            );
            assert_eq!(response.correlation_id, MessageId::Ulong(7));
            assert_eq!(response.tracking_id.as_deref(), Some("trace"));
            assert_eq!(response.body, Value::Null);
        }
    }
}
