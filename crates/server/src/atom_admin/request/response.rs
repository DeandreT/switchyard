use http_body_util::Full;
use hyper::{
    Response, StatusCode,
    body::Bytes,
    header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue},
};
use quick_xml::{
    Writer,
    events::{BytesEnd, BytesStart, BytesText, Event},
};

use super::super::xml::subscriptions::{self, SubscriptionXmlError};
use super::super::xml::{self, AtomXmlError};
use crate::{AtomQueueOwnerError, AtomSubscriptionOwnerError, ProposeError, SubmitError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RequestFailure {
    Authentication,
    BadRequest,
    MethodNotAllowed,
    HeaderTooLarge,
    NotFound,
    TopicNotFound,
    SubscriptionNotFound,
    SubscriptionConflict,
    Conflict,
    Quota,
    Unavailable,
    Internal,
    Xml(AtomXmlError),
    SubscriptionXml(SubscriptionXmlError),
}

impl From<AtomXmlError> for RequestFailure {
    fn from(error: AtomXmlError) -> Self {
        Self::Xml(error)
    }
}

impl From<AtomQueueOwnerError> for RequestFailure {
    fn from(error: AtomQueueOwnerError) -> Self {
        match error {
            AtomQueueOwnerError::Submit(error) => error.into(),
            AtomQueueOwnerError::UnsupportedDefinition | AtomQueueOwnerError::InvalidPageBounds => {
                Self::BadRequest
            }
            AtomQueueOwnerError::WorkLimitExceeded => Self::Unavailable,
        }
    }
}

impl From<SubscriptionXmlError> for RequestFailure {
    fn from(error: SubscriptionXmlError) -> Self {
        Self::SubscriptionXml(error)
    }
}

impl From<AtomSubscriptionOwnerError> for RequestFailure {
    fn from(error: AtomSubscriptionOwnerError) -> Self {
        match error {
            AtomSubscriptionOwnerError::Submit(error) => error.into(),
            AtomSubscriptionOwnerError::UnsupportedDefinition => {
                Self::SubscriptionXml(SubscriptionXmlError::UnsupportedDefinition)
            }
        }
    }
}

impl From<SubmitError> for RequestFailure {
    fn from(error: SubmitError) -> Self {
        match error {
            SubmitError::BrokerStopped => Self::Unavailable,
            SubmitError::Propose(ProposeError::ClockWentBackward { .. }) => Self::Unavailable,
            SubmitError::Propose(ProposeError::UnexpectedOutcome { .. }) => Self::Internal,
            SubmitError::Propose(ProposeError::Broker(error)) => {
                use domain::BrokerError;
                match error {
                    BrokerError::QueueNotFound => Self::NotFound,
                    BrokerError::TopicNotFound => Self::TopicNotFound,
                    BrokerError::SubscriptionNotFound => Self::SubscriptionNotFound,
                    BrokerError::SubscriptionAlreadyExists => Self::SubscriptionConflict,
                    BrokerError::QueueAlreadyExists | BrokerError::EntityPathAlreadyExists => {
                        Self::Conflict
                    }
                    BrokerError::QueueCapacityFull => Self::Quota,
                    BrokerError::InvalidQueueCapacity
                    | BrokerError::QueueCapacityNotSupported
                    | BrokerError::QueuePropertyIsImmutable { .. }
                    | BrokerError::EntityKindMismatch
                    | BrokerError::DeadLetterQueueIsReserved
                    | BrokerError::SubscriptionPathIsReserved => Self::BadRequest,
                    BrokerError::QueueCapacityWorkLimitExceeded
                    | BrokerError::SubscriptionLimitExceeded { .. }
                    | BrokerError::EntityDeleteTooLarge { .. }
                    | BrokerError::ClockRegression { .. } => Self::Unavailable,
                    // Desired input is validated before owner admission; remaining
                    // codec/configuration failures are stored-state failures.
                    _ => Self::Internal,
                }
            }
        }
    }
}

impl RequestFailure {
    fn status(self) -> StatusCode {
        match self {
            Self::Authentication => StatusCode::UNAUTHORIZED,
            Self::BadRequest
            | Self::Xml(
                AtomXmlError::Malformed
                | AtomXmlError::InvalidDefinition
                | AtomXmlError::UnsupportedDefinition,
            )
            | Self::SubscriptionXml(
                SubscriptionXmlError::Malformed
                | SubscriptionXmlError::InvalidDefinition
                | SubscriptionXmlError::UnsupportedDefinition,
            ) => StatusCode::BAD_REQUEST,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::HeaderTooLarge => StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            Self::NotFound | Self::TopicNotFound | Self::SubscriptionNotFound => {
                StatusCode::NOT_FOUND
            }
            Self::Conflict | Self::SubscriptionConflict => StatusCode::CONFLICT,
            Self::Quota => StatusCode::FORBIDDEN,
            Self::Unavailable
            | Self::Xml(AtomXmlError::WorkLimitExceeded)
            | Self::SubscriptionXml(SubscriptionXmlError::WorkLimitExceeded) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::Internal
            | Self::Xml(AtomXmlError::ReplyLimitExceeded)
            | Self::SubscriptionXml(SubscriptionXmlError::ReplyLimitExceeded) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    fn public_text(self) -> (&'static str, &'static str) {
        match self {
            Self::Authentication => ("Unauthorized", "Management authorization is required."),
            Self::BadRequest => (
                "InvalidRequest",
                "The request is not supported by this administration profile.",
            ),
            Self::MethodNotAllowed => {
                ("MethodNotAllowed", "The requested method is not supported.")
            }
            Self::HeaderTooLarge => ("RequestHeaderLimit", "The request headers exceed a limit."),
            Self::NotFound => ("EntityNotFound", "The requested queue was not found."),
            Self::TopicNotFound => ("EntityNotFound", "The requested topic was not found."),
            Self::SubscriptionNotFound => (
                "EntityNotFound",
                "The requested subscription was not found.",
            ),
            Self::SubscriptionConflict => (
                "EntityAlreadyExists",
                "The requested subscription already exists.",
            ),
            Self::Conflict => ("EntityAlreadyExists", "The requested queue already exists."),
            Self::Quota => ("QuotaExceeded", "The queue capacity would be exceeded."),
            Self::Unavailable => (
                "ServiceBusy",
                "The request could not be completed within the service limits.",
            ),
            Self::Internal | Self::Xml(_) | Self::SubscriptionXml(_) => {
                ("InternalError", "The request could not be completed.")
            }
        }
    }

    pub(super) fn into_response(self) -> Response<Full<Bytes>> {
        let body = match self {
            Self::Xml(error) => xml::encode_error(error),
            Self::SubscriptionXml(error) => {
                subscriptions::encode_error(error).map_err(|_| AtomXmlError::ReplyLimitExceeded)
            }
            _ => write_error(self.public_text()).map_err(|_| AtomXmlError::ReplyLimitExceeded),
        };
        match body {
            Ok(body) => response(self.status(), body, "application/xml"),
            Err(_) => response(StatusCode::INTERNAL_SERVER_ERROR,
                b"<Error><Code>InternalError</Code><Detail>The request could not be completed.</Detail></Error>".to_vec(),
                "application/xml"),
        }
    }
}

fn write_error((code, detail): (&'static str, &'static str)) -> std::io::Result<Vec<u8>> {
    let mut writer = Writer::new(Vec::new());
    writer.write_event(Event::Start(BytesStart::new("Error")))?;
    for (name, value) in [("Code", code), ("Detail", detail)] {
        writer.write_event(Event::Start(BytesStart::new(name)))?;
        writer.write_event(Event::Text(BytesText::new(value)))?;
        writer.write_event(Event::End(BytesEnd::new(name)))?;
    }
    writer.write_event(Event::End(BytesEnd::new("Error")))?;
    Ok(writer.into_inner())
}

pub(super) fn response(
    status: StatusCode,
    body: Vec<u8>,
    content_type: &'static str,
) -> Response<Full<Bytes>> {
    let length = body.len() as u64;
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONNECTION, HeaderValue::from_static("close"));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
        .headers_mut()
        .insert(CONTENT_LENGTH, HeaderValue::from(length));
    response
}
