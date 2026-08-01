use std::{collections::HashMap, sync::Arc, time::Duration};

use amqp::{
    AmqpError, ApplicationProperties, Body, Error as AmqpProtocolError, Message, MessageId,
    Properties, Receiver, Sender,
};
use auth::{Permission, ResourceScope};
use domain::{CommandKind, CommandOutcome, EntityPath, LockToken, NamespaceName, SequenceNumber};
use serde_amqp::{
    Value,
    primitives::{Array, Binary, OrderedMap, Symbol, Timestamp as AmqpTimestamp, Uuid},
};
use tokio::sync::{Mutex, Notify, RwLock, mpsc};
use tracing::debug;

use crate::{Broker, BrokerRejection, authorization::ConnectionAuthorization};

pub const RENEW_LOCK_OPERATION: &str = "com.microsoft:renew-lock";
pub const OPERATION_PROPERTY: &str = "operation";
pub const ASSOCIATED_LINK_NAME_PROPERTY: &str = "associated-link-name";
pub const STATUS_CODE_PROPERTY: &str = "statusCode";
pub const STATUS_DESCRIPTION_PROPERTY: &str = "statusDescription";
pub const ERROR_CONDITION_PROPERTY: &str = "errorCondition";
pub const TRACKING_ID_PROPERTY: &str = "com.microsoft:tracking-id";
pub const LOCK_TOKENS: &str = "lock-tokens";
pub const EXPIRATIONS: &str = "expirations";

const REPLY_ROUTE_TIMEOUT: Duration = Duration::from_secs(2);
const REPLY_BUFFER: usize = 16;

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

    async fn ensure(&self) -> Result<(), AmqpProtocolError> {
        self.connection
            .authorize_resource(&self.resource, Permission::Manage)
            .await
            .map_err(|_| unauthorized_error("the management link's authorization has expired"))
    }

    async fn wait_until_unauthorized(&self) {
        self.connection
            .wait_until_unauthorized(&self.resource, Permission::Manage)
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
    expirations: Vec<domain::Timestamp>,
}

impl ManagementResponse {
    fn accepted(
        correlation_id: MessageId,
        tracking_id: Option<String>,
        expirations: Vec<domain::Timestamp>,
    ) -> Self {
        Self {
            correlation_id,
            status_code: 200,
            status_description: String::from("OK"),
            error_condition: None,
            tracking_id,
            expirations,
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
            expirations: Vec::new(),
        }
    }

    fn from_rejection(
        correlation_id: MessageId,
        tracking_id: Option<String>,
        rejection: &BrokerRejection,
    ) -> Self {
        let condition = rejection.condition();
        let status_code = match condition {
            crate::MESSAGE_LOCK_LOST => 410,
            crate::NOT_FOUND => 404,
            crate::NOT_ALLOWED | crate::PRECONDITION_FAILED => 400,
            crate::RESOURCE_LOCKED => 503,
            _ => 500,
        };
        Self {
            correlation_id,
            status_code,
            status_description: rejection.to_string(),
            error_condition: Some(condition),
            tracking_id,
            expirations: Vec::new(),
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
            expirations: Vec::new(),
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
            expirations: Vec::new(),
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

        let body = if self.expirations.is_empty() {
            Body::Value(Value::Null)
        } else {
            let expirations: Vec<Value> = self
                .expirations
                .into_iter()
                .map(|timestamp| {
                    Value::Timestamp(AmqpTimestamp::from_milliseconds(
                        i64::try_from(timestamp.as_millis()).unwrap_or(i64::MAX),
                    ))
                })
                .collect();
            let mut map = OrderedMap::new();
            map.insert(
                Value::String(String::from(EXPIRATIONS)),
                Value::Array(Array::from(expirations)),
            );
            Body::Value(Value::Map(map))
        };

        Message {
            properties: Some(Properties {
                correlation_id: Some(self.correlation_id),
                ..Properties::default()
            }),
            application_properties: Some(application_properties),
            body,
            ..Message::default()
        }
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

        let response = process_request(
            delivery.message(),
            message_id,
            &namespace,
            &entity,
            &broker,
            &management,
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
    if string_property(properties, OPERATION_PROPERTY) != Some(RENEW_LOCK_OPERATION) {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "unsupported management operation",
        );
    }
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

    let mut deliveries = Vec::with_capacity(tokens.len());
    for token in tokens {
        let Some(delivery) = management.delivery(link_name, token).await else {
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
        deliveries.push((delivery, token));
    }

    let mut expirations = Vec::with_capacity(deliveries.len());
    for (delivery, lock_token) in deliveries {
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
            Ok(CommandOutcome::LockRenewed { locked_until }) => expirations.push(locked_until),
            Ok(other) => {
                return ManagementResponse::internal(
                    message_id,
                    tracking_id,
                    format!("renewing a lock produced an unexpected outcome: {other:?}"),
                );
            }
            Err(rejection) => {
                return ManagementResponse::from_rejection(message_id, tracking_id, &rejection);
            }
        }
    }

    ManagementResponse::accepted(message_id, tracking_id, expirations)
}

fn string_property<'a>(properties: &'a ApplicationProperties, name: &str) -> Option<&'a str> {
    match properties.get(name) {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
}

fn lock_tokens(body: &Body) -> Option<Vec<LockToken>> {
    let Body::Value(Value::Map(map)) = body else {
        return None;
    };
    let value = map.iter().find_map(|(key, value)| match key {
        Value::String(key) if key == LOCK_TOKENS => Some(value),
        Value::Symbol(key) if key.as_str() == LOCK_TOKENS => Some(value),
        _ => None,
    })?;
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
            vec![domain::Timestamp::from_millis(12_345)],
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
}
