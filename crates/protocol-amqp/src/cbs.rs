use std::sync::Arc;

use amqp::{ApplicationProperties, Body, Message, MessageId, Properties, Receiver, Sender};
use serde_amqp::{Value, primitives::Binary};
use tokio::sync::mpsc;
use tracing::debug;

use crate::authorization::ConnectionAuthorization;

pub(crate) const PUT_TOKEN_OPERATION: &str = "put-token";
pub(crate) const SAS_TOKEN_TYPE: &str = "servicebus.windows.net:sastoken";

const OPERATION_PROPERTY: &str = "operation";
const TOKEN_TYPE_PROPERTY: &str = "type";
const AUDIENCE_PROPERTY: &str = "name";
const STATUS_CODE_PROPERTY: &str = "status-code";
const STATUS_DESCRIPTION_PROPERTY: &str = "status-description";

#[derive(Clone, Debug)]
pub(crate) struct CbsResponse {
    correlation_id: MessageId,
    status_code: i32,
    status_description: String,
}

impl CbsResponse {
    fn accepted(correlation_id: MessageId) -> Self {
        Self {
            correlation_id,
            status_code: 202,
            status_description: String::from("Accepted"),
        }
    }

    fn bad_request(correlation_id: MessageId, description: impl Into<String>) -> Self {
        Self {
            correlation_id,
            status_code: 400,
            status_description: description.into(),
        }
    }

    fn unauthorized(correlation_id: MessageId) -> Self {
        Self {
            correlation_id,
            status_code: 401,
            status_description: String::from("Unauthorized"),
        }
    }

    fn into_message(self) -> Message {
        let mut application_properties = ApplicationProperties::default();
        application_properties.insert(STATUS_CODE_PROPERTY, self.status_code);
        application_properties.insert(STATUS_DESCRIPTION_PROPERTY, self.status_description);
        Message {
            properties: Some(Properties {
                correlation_id: Some(self.correlation_id),
                ..Properties::default()
            }),
            application_properties: Some(application_properties),
            body: Body::Value(Value::Null),
            ..Message::default()
        }
    }
}

pub(crate) async fn serve_cbs_requests(
    mut receiver: Receiver,
    authorization: Arc<ConnectionAuthorization>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let delivery = match receiver.recv().await {
            Ok(delivery) => delivery,
            Err(
                amqp::EngineError::RemoteClosed
                | amqp::EngineError::RemoteDetached
                | amqp::EngineError::Stopped,
            ) => return Ok(()),
            Err(error) => return Err(error.into()),
        };

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

        let response = process_request(delivery.message(), message_id, &authorization).await;
        // CBS requests are usually pre-settled, so accepting those is a no-op,
        // while unsettled diagnostic clients still get their outcome.
        receiver.accept(&delivery).await?;
        if authorization
            .route_response(&reply_to, response)
            .await
            .is_err()
        {
            debug!(%reply_to, "CBS reply route disappeared");
        }
    }
}

async fn process_request(
    message: &Message,
    message_id: MessageId,
    authorization: &ConnectionAuthorization,
) -> CbsResponse {
    let Some(properties) = message.application_properties.as_ref() else {
        return CbsResponse::bad_request(message_id, "application properties are required");
    };
    if string_property(properties, OPERATION_PROPERTY) != Some(PUT_TOKEN_OPERATION)
        || string_property(properties, TOKEN_TYPE_PROPERTY) != Some(SAS_TOKEN_TYPE)
    {
        return CbsResponse::bad_request(message_id, "unsupported CBS operation or token type");
    }
    let Some(audience) = string_property(properties, AUDIENCE_PROPERTY) else {
        return CbsResponse::bad_request(message_id, "the token audience is required");
    };
    let Body::Value(Value::String(token)) = &message.body else {
        return CbsResponse::bad_request(message_id, "the SAS token must be an AMQP value string");
    };

    match authorization.validate_and_add(token, audience).await {
        Ok(()) => CbsResponse::accepted(message_id),
        Err(_) => CbsResponse::unauthorized(message_id),
    }
}

fn string_property<'a>(properties: &'a ApplicationProperties, name: &str) -> Option<&'a str> {
    match properties.get(name) {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
}

pub(crate) async fn serve_cbs_replies(
    mut sender: Sender,
    address: String,
    route: mpsc::Sender<CbsResponse>,
    mut responses: mpsc::Receiver<CbsResponse>,
    authorization: Arc<ConnectionAuthorization>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        tokio::select! {
            _ = sender.on_detach() => {
                authorization.unregister_reply_route(&address, &route).await;
                return Ok(());
            }
            response = responses.recv() => {
                let Some(response) = response else { return Ok(()) };
                let tag = cbs_delivery_tag(&response.correlation_id);
                sender.send(response.into_message(), tag).await?;
            }
        }
    }
}

fn cbs_delivery_tag(message_id: &MessageId) -> Binary {
    Binary::from(format!("{message_id:?}").into_bytes())
}
