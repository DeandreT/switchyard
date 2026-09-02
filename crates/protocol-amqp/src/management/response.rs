use amqp::{ApplicationProperties, Body, Message, MessageId, Properties};
use serde_amqp::{Value, primitives::Symbol};

use crate::BrokerRejection;

use super::{
    ERROR_CONDITION_PROPERTY, STATUS_CODE_PROPERTY, STATUS_DESCRIPTION_PROPERTY,
    TRACKING_ID_PROPERTY,
};

#[derive(Clone, Debug)]
pub(crate) struct ManagementResponse {
    pub(super) correlation_id: MessageId,
    pub(super) status_code: i32,
    pub(super) status_description: String,
    pub(super) error_condition: Option<&'static str>,
    pub(super) tracking_id: Option<String>,
    pub(super) body: Value,
}

impl ManagementResponse {
    pub(super) fn accepted(
        correlation_id: MessageId,
        tracking_id: Option<String>,
        body: Value,
    ) -> Self {
        Self {
            correlation_id,
            status_code: 200,
            status_description: String::from("OK"),
            error_condition: None,
            tracking_id,
            body,
        }
    }

    pub(super) fn no_content(correlation_id: MessageId, tracking_id: Option<String>) -> Self {
        Self {
            correlation_id,
            status_code: 204,
            status_description: String::from("No Content"),
            error_condition: None,
            tracking_id,
            body: Value::Null,
        }
    }

    pub(super) fn bad_request(
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

    pub(super) fn unauthorized(
        correlation_id: MessageId,
        tracking_id: Option<String>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            correlation_id,
            status_code: 401,
            status_description: description.into(),
            error_condition: Some("amqp:unauthorized-access"),
            tracking_id,
            body: Value::Null,
        }
    }

    pub(super) fn from_rejection(
        correlation_id: MessageId,
        tracking_id: Option<String>,
        rejection: &BrokerRejection,
    ) -> Self {
        let condition = rejection.condition();
        let status_code = match condition {
            crate::MESSAGE_LOCK_LOST | crate::SESSION_LOCK_LOST => 410,
            crate::NOT_FOUND => 404,
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

    pub(super) fn lock_lost(
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

    pub(super) fn session_lock_lost(
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

    pub(super) fn internal(
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

    pub(super) fn into_message(self) -> Message {
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

#[cfg(test)]
mod tests {
    use domain::BrokerError;

    use super::*;

    #[test]
    fn an_invalid_message_field_is_a_bad_management_request() {
        let rejection = BrokerRejection::Refused(BrokerError::MessageIdTooLong {
            characters: domain::MAX_MESSAGE_ID_CHARACTERS + 1,
            maximum: domain::MAX_MESSAGE_ID_CHARACTERS,
        });

        let response = ManagementResponse::from_rejection(MessageId::Ulong(1), None, &rejection);

        assert_eq!(response.status_code, 400);
        assert_eq!(response.error_condition, Some(crate::INVALID_FIELD));
    }
}
