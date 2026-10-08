use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::Arc,
};

use amqp::{
    ApplicationProperties, Body, DeliveryState, EngineError, Message, MessageId, Outcome,
    Properties, Receiver, Sender,
};
use futures_util::FutureExt;
use serde_amqp::{Value, primitives::Binary};
use tokio::sync::mpsc;
use tracing::debug;

use crate::authorization::ConnectionAuthorization;

mod custody;

use custody::{
    OperationControl, PanicPayload, PendingOperation, PumpPoint, ReplyCustody, RequestCustody,
    TokenValidation,
};

type CbsError = Box<dyn std::error::Error + Send + Sync>;

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
    let detached = receiver.on_detach_owned();
    tokio::pin!(detached);
    loop {
        let received = tokio::select! {
            biased;
            () = &mut detached => return Ok(()),
            received = receiver.recv() => received,
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

        // The owner outlives the borrowed pump, but not the next mutable recv.
        let mut custody = RequestCustody::default();
        let result = AssertUnwindSafe(cbs_request_pump(
            &receiver,
            &delivery,
            &authorization,
            &mut custody,
            detached.as_mut(),
        ))
        .catch_unwind()
        .await;
        let cleanup = AssertUnwindSafe(custody.finish()).catch_unwind().await;
        let diagnostics = if cleanup.is_ok() {
            catch_unwind(AssertUnwindSafe(|| custody.report())).err()
        } else {
            None
        };
        let cleanup_panic = cleanup
            .err()
            .or_else(|| custody.take_cleanup_panic())
            .or(diagnostics);
        let retired = finish_pump(result, cleanup_panic, custody.take_native_error())?;
        if retired {
            return Ok(());
        }
    }
}

async fn cbs_request_pump<'a>(
    receiver: &'a Receiver,
    delivery: &'a amqp::Delivery,
    authorization: &'a ConnectionAuthorization,
    custody: &mut RequestCustody<'a>,
    mut detached: Pin<&mut (impl Future<Output = ()> + Send)>,
) -> Result<bool, CbsError> {
    let correlation = delivery
        .message()
        .properties
        .as_ref()
        .and_then(|properties| {
            Some((properties.message_id.clone()?, properties.reply_to.clone()?))
        });
    let Some((message_id, reply_to)) = correlation else {
        custody.native = Some(native_operation(receiver.reject(delivery, None)));
        if observe_or_retire(
            custody.native.as_mut().unwrap(),
            detached.as_mut(),
            PumpPoint::RequestNative,
        )
        .await
        {
            return Ok(true);
        }
        custody.capture_native();
        if let Some(error) = custody.take_native_error() {
            return Err(error.into());
        }
        return Ok(false);
    };

    let control = OperationControl::new();
    custody.token = Some(PendingOperation::new(
        process_request(
            delivery.message(),
            message_id,
            authorization,
            control.clone(),
        ),
        control,
    ));
    #[cfg(test)]
    custody::pump_checkpoint(PumpPoint::RequestPrepared).await;
    if observe_or_retire(
        custody.token.as_mut().unwrap(),
        detached.as_mut(),
        PumpPoint::RequestToken,
    )
    .await
    {
        return Ok(true);
    }
    custody.capture_token();
    custody.response = Some(
        custody
            .token_packet
            .as_mut()
            .unwrap()
            .result
            .take()
            .flatten()
            .expect("active CBS request has a response"),
    );
    #[cfg(test)]
    custody::pump_checkpoint(PumpPoint::RequestResponse).await;
    // CBS requests are usually pre-settled, so accepting those is a no-op,
    // while unsettled diagnostic clients still get their outcome.
    custody.native = Some(native_operation(receiver.accept(delivery)));
    if observe_or_retire(
        custody.native.as_mut().unwrap(),
        detached.as_mut(),
        PumpPoint::RequestNative,
    )
    .await
    {
        return Ok(true);
    }
    custody.capture_native();
    if let Some(error) = custody.take_native_error() {
        return Err(error.into());
    }
    let routed = tokio::select! {
        biased;
        () = detached.as_mut() => return Ok(true),
        () = custody::pump_fault(PumpPoint::RequestRouting) => unreachable!("CBS fault checkpoint panics"),
        routed = authorization.route_response(&reply_to, custody.response.as_ref().unwrap().clone()) => routed,
    };
    if routed.is_err() {
        debug!(%reply_to, "CBS reply route disappeared");
    }
    Ok(false)
}

async fn process_request(
    message: &Message,
    message_id: MessageId,
    authorization: &ConnectionAuthorization,
    control: OperationControl,
) -> Option<CbsResponse> {
    let Some(properties) = message.application_properties.as_ref() else {
        return Some(CbsResponse::bad_request(
            message_id,
            "application properties are required",
        ));
    };
    if string_property(properties, OPERATION_PROPERTY) != Some(PUT_TOKEN_OPERATION)
        || string_property(properties, TOKEN_TYPE_PROPERTY) != Some(SAS_TOKEN_TYPE)
    {
        return Some(CbsResponse::bad_request(
            message_id,
            "unsupported CBS operation or token type",
        ));
    }
    let Some(audience) = string_property(properties, AUDIENCE_PROPERTY) else {
        return Some(CbsResponse::bad_request(
            message_id,
            "the token audience is required",
        ));
    };
    let Body::Value(Value::String(token)) = &message.body else {
        return Some(CbsResponse::bad_request(
            message_id,
            "the SAS token must be an AMQP value string",
        ));
    };

    Some(
        match TokenValidation::new(authorization, token, audience, control).await? {
            Ok(()) => CbsResponse::accepted(message_id),
            Err(_) => CbsResponse::unauthorized(message_id),
        },
    )
}

fn string_property<'a>(properties: &'a ApplicationProperties, name: &str) -> Option<&'a str> {
    match properties.get(name) {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
}

pub(crate) async fn serve_cbs_replies(
    sender: Sender,
    address: String,
    route: mpsc::Sender<CbsResponse>,
    responses: mpsc::Receiver<CbsResponse>,
    authorization: Arc<ConnectionAuthorization>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut custody = ReplyCustody::new(responses, address, route, &authorization);
    let result = AssertUnwindSafe(cbs_reply_loop(&sender, &mut custody))
        .catch_unwind()
        .await;
    let cleanup = AssertUnwindSafe(custody.finish()).catch_unwind().await;
    let diagnostics = if cleanup.is_ok() {
        catch_unwind(AssertUnwindSafe(|| custody.report())).err()
    } else {
        None
    };
    let cleanup_panic = cleanup
        .err()
        .or_else(|| custody.take_cleanup_panic())
        .or(diagnostics);
    finish_pump(result, cleanup_panic, custody.take_native_error())
}

async fn cbs_reply_loop<'a>(
    sender: &'a Sender,
    custody: &mut ReplyCustody<'a>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let detached = sender.on_detach_owned();
    tokio::pin!(detached);
    loop {
        #[cfg(test)]
        custody::pump_checkpoint(PumpPoint::ReplyIdle).await;
        let response = tokio::select! {
            biased;
            () = &mut detached => return Ok(()),
            response = custody.responses.recv() => response,
        };
        let Some(response) = response else {
            return Ok(());
        };
        let control = OperationControl::new();
        custody.original = Some(PendingOperation::new(
            send_cbs_response(sender, response, control.clone()),
            control,
        ));
        #[cfg(test)]
        custody::pump_checkpoint(PumpPoint::ReplyPrepared).await;
        if observe_or_retire(
            custody.original.as_mut().unwrap(),
            detached.as_mut(),
            PumpPoint::ReplyNative,
        )
        .await
        {
            return Ok(());
        }
        custody.capture_packet();
        #[cfg(test)]
        custody::pump_checkpoint(PumpPoint::ReplyResult).await;
        if let Some(error) = custody.take_native_error() {
            return Err(error.into());
        }
        custody.packet = None;
    }
}

async fn send_cbs_response(
    sender: &Sender,
    response: CbsResponse,
    control: OperationControl,
) -> Result<Outcome, EngineError> {
    if !control.begin() {
        return Err(EngineError::Stopped);
    }
    let tag = cbs_delivery_tag(&response.correlation_id);
    let pending = sender.send_pending(response.into_message(), tag).await?;
    let observed = pending.await?;
    let (_, outcome, confirmation) = observed.into_parts();
    if let Some(confirmation) = confirmation
        && control.begin()
    {
        let state = match &outcome {
            Outcome::Accepted(value) => DeliveryState::Accepted(value.clone()),
            Outcome::Rejected(value) => DeliveryState::Rejected(value.clone()),
            Outcome::Released(value) => DeliveryState::Released(value.clone()),
            Outcome::Modified(value) => DeliveryState::Modified(value.clone()),
        };
        confirmation.confirm(state).await?;
    }
    Ok(outcome)
}

async fn observe_or_retire<T: Send>(
    original: &mut PendingOperation<'_, T>,
    detached: Pin<&mut (impl Future<Output = ()> + Send)>,
    point: PumpPoint,
) -> bool {
    tokio::select! {
        biased;
        () = detached => { original.retire(); true }
        () = custody::pump_fault(point) => unreachable!("CBS fault checkpoint panics"),
        _ = original.observe() => false,
    }
}

fn native_operation<'a, T: Send + 'a>(
    actual: impl Future<Output = Result<T, EngineError>> + Send + 'a,
) -> PendingOperation<'a, Result<T, EngineError>> {
    let control = OperationControl::new();
    let frontier = control.clone();
    PendingOperation::new(
        async move {
            if !frontier.begin() {
                return Err(EngineError::Stopped);
            }
            actual.await
        },
        control,
    )
}

fn finish_pump<T>(
    primary: std::thread::Result<Result<T, CbsError>>,
    cleanup_panic: Option<PanicPayload>,
    native_error: Option<EngineError>,
) -> Result<T, CbsError> {
    match primary {
        Err(payload) => resume_unwind(payload),
        Ok(Err(error)) => Err(error),
        Ok(Ok(value)) => {
            if let Some(error) = native_error {
                return Err(error.into());
            }
            if let Some(payload) = cleanup_panic {
                resume_unwind(payload);
            }
            Ok(value)
        }
    }
}

fn cbs_delivery_tag(message_id: &MessageId) -> Binary {
    Binary::from(format!("{message_id:?}").into_bytes())
}

#[cfg(test)]
mod request_retirement_tests;

#[cfg(test)]
mod reply_retirement_tests;
