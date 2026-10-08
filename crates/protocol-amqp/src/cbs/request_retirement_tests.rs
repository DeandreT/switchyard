//! Concrete token validation/store custody and actual CBS request retirement.

use std::time::{SystemTime, UNIX_EPOCH};

use amqp::{Accepted, LinkEndpoint, Performative, ReceiverSettleMode, Role};
use auth::{
    Permission, PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::time::timeout;

use super::custody::{PUMP_FAULT, PumpFault};
use super::reply_retirement_tests::{ADDRESS, WAIT, Wire, pending_once};
use super::*;
use crate::SharedAccessAuthentication;

const HOST: &str = "tenant.servicebus.windows.net";
const AUDIENCE: &str = "amqps://tenant.servicebus.windows.net/orders";

pub(super) fn authorization() -> Arc<ConnectionAuthorization> {
    let rules = [
        ("sender", PermissionSet::SEND),
        ("listener", PermissionSet::LISTEN),
        ("manager", PermissionSet::MANAGE),
    ]
    .map(|(name, permissions)| {
        SharedAccessRule::new(
            name,
            ResourceScope::namespace(HOST).unwrap(),
            SharedAccessKey::new("secret").unwrap(),
            None,
            permissions,
        )
        .unwrap()
    });
    let policy = SharedAccessPolicy::new(rules).unwrap();
    ConnectionAuthorization::new(SharedAccessAuthentication::new(policy, HOST).unwrap(), None)
}

fn expiry() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600
}

fn signed_token(rule: &str, path: &str, expiry: u64) -> String {
    let encoded = format!("amqps%3A%2F%2F{HOST}%2F{path}");
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    mac.update(format!("{encoded}\n{expiry}").as_bytes());
    let signature = STANDARD
        .encode(mac.finalize().into_bytes())
        .replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D");
    format!("SharedAccessSignature sr={encoded}&sig={signature}&se={expiry}&skn={rule}")
}

pub(super) fn request(token: &str, id: u64) -> Message {
    let mut properties = ApplicationProperties::default();
    properties.insert(OPERATION_PROPERTY, PUT_TOKEN_OPERATION);
    properties.insert(TOKEN_TYPE_PROPERTY, SAS_TOKEN_TYPE);
    properties.insert(AUDIENCE_PROPERTY, AUDIENCE);
    Message {
        properties: Some(Properties {
            message_id: Some(MessageId::Ulong(id)),
            reply_to: Some(ADDRESS.to_owned()),
            ..Properties::default()
        }),
        application_properties: Some(properties),
        body: Body::Value(Value::String(token.to_owned())),
        ..Message::default()
    }
}

fn assert_response(response: CbsResponse, id: u64, status: i32, description: &str) {
    let message = response.into_message();
    assert_eq!(
        message.properties.unwrap().correlation_id,
        Some(MessageId::Ulong(id))
    );
    let properties = message.application_properties.unwrap();
    assert_eq!(
        properties.get(STATUS_CODE_PROPERTY),
        Some(&Value::Int(status))
    );
    assert_eq!(
        properties.get(STATUS_DESCRIPTION_PROPERTY),
        Some(&Value::String(description.to_owned()))
    );
    assert!(matches!(message.body, Body::Value(Value::Null)));
}

pub(super) fn assert_outer_panic(result: std::thread::Result<Result<(), CbsError>>) {
    let payload = result.expect_err("original outer pump panic is resumed");
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"controlled outer CBS pump panic")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn outer_request_panic_before_token_poll_stores_nothing_and_starts_no_acknowledgement() {
    for settled in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("CBS request receiver")
        };
        let authorization = authorization();
        let token = signed_token("sender", "orders", expiry());
        let (_route, mut responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        let held = authorization.grant_write_lock().await;
        wire.request_message(&request(&token, 42), settled).await;
        wire.barrier().await;
        let fault = PumpFault::new(PumpPoint::RequestPrepared);
        let mut serving = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(serve_cbs_requests(receiver, Arc::clone(&authorization)))
                    .catch_unwind(),
            ),
        );
        pending_once(serving.as_mut()).await;
        timeout(WAIT, fault.reached.notified()).await.unwrap();
        assert!(held.is_empty());
        fault.trigger.notify_one();
        assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
        assert!(held.is_empty());
        assert!(matches!(
            responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop(held);
        assert!(authorization.grant_snapshot().await.is_empty());
        wire.barrier().await;
        wire.no_frame_yet().await;
        authorization
            .validate_and_add(&token, AUDIENCE)
            .await
            .unwrap();
        assert_eq!(authorization.grant_snapshot().await.len(), 1);
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outer_request_panic_drains_begun_token_store_without_acknowledgement_or_rollback() {
    for settled in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("CBS request receiver")
        };
        let authorization = authorization();
        let expires = expiry();
        let token = signed_token("sender", "orders", expires);
        let (_route, mut responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        let held = authorization.grant_write_lock().await;
        wire.request_message(&request(&token, 42), settled).await;
        wire.barrier().await;
        let fault = PumpFault::new(PumpPoint::RequestToken);
        let mut serving = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(serve_cbs_requests(receiver, Arc::clone(&authorization)))
                    .catch_unwind(),
            ),
        );
        pending_once(serving.as_mut()).await;
        timeout(WAIT, fault.reached.notified()).await.unwrap();
        assert!(held.is_empty());
        fault.trigger.notify_one();
        for _ in 0..2 {
            pending_once(serving.as_mut()).await;
            assert!(
                held.is_empty(),
                "the same original is still waiting on the held grant row"
            );
        }
        assert!(matches!(
            responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop(held);
        assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
        let grants = authorization.grant_snapshot().await;
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].subject(), "sender");
        assert_eq!(grants[0].expires_at_epoch_seconds(), expires);
        assert_eq!(
            grants[0].scope(),
            &ResourceScope::entity(HOST, "orders").unwrap()
        );
        assert_eq!(grants[0].permissions(), PermissionSet::SEND);
        assert!(matches!(
            responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        wire.barrier().await;
        wire.no_frame_yet().await;
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outer_request_panic_retains_cached_response_and_retires_routing_without_new_native_work() {
    for settled in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("CBS request receiver")
        };
        let authorization = authorization();
        let token = signed_token("sender", "orders", expiry());
        let (_route, mut responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        wire.request_message(&request(&token, 42), settled).await;
        wire.barrier().await;
        let fault = PumpFault::new(PumpPoint::RequestResponse);
        let mut serving = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(serve_cbs_requests(receiver, Arc::clone(&authorization)))
                    .catch_unwind(),
            ),
        );
        pending_once(serving.as_mut()).await;
        timeout(WAIT, fault.reached.notified()).await.unwrap();
        let before = authorization.grant_snapshot().await;
        assert_eq!(
            before.len(),
            1,
            "the actual token result was cached before the fault"
        );
        fault.trigger.notify_one();
        assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
        assert_eq!(authorization.grant_snapshot().await, before);
        assert!(matches!(
            responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        wire.barrier().await;
        wire.no_frame_yet().await;
        wire.stop().await;
    }
    for full in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("CBS request receiver")
        };
        let authorization = authorization();
        let old = if full {
            let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
            for id in 0..16 {
                route
                    .send(CbsResponse::accepted(MessageId::Ulong(id)))
                    .await
                    .unwrap();
            }
            assert_eq!(route.capacity(), 0);
            Some((route, responses))
        } else {
            None
        };
        let token = signed_token("sender", "orders", expiry());
        wire.request_message(&request(&token, 42), false).await;
        wire.barrier().await;
        let fault = PumpFault::new(PumpPoint::RequestRouting);
        let mut serving = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(serve_cbs_requests(receiver, Arc::clone(&authorization)))
                    .catch_unwind(),
            ),
        );
        pending_once(serving.as_mut()).await;
        let Performative::Disposition(accepted) = wire.control(1).await else {
            panic!("original request acceptance")
        };
        assert_eq!(accepted.first, 1);
        assert_eq!(accepted.state, Some(DeliveryState::Accepted(Accepted)));
        wire.barrier().await;
        pending_once(serving.as_mut()).await;
        timeout(WAIT, fault.reached.notified()).await.unwrap();
        let stored = authorization.grant_snapshot().await;
        assert_eq!(stored.len(), 1);
        let (_replacement, mut replacement_responses) =
            authorization.register_reply_route(ADDRESS.to_owned()).await;
        fault.trigger.notify_one();
        assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
        assert_eq!(authorization.grant_snapshot().await, stored);
        assert!(matches!(
            replacement_responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        if let Some((route, mut responses)) = old {
            assert_eq!(route.capacity(), 0);
            for id in 0..16 {
                assert_eq!(
                    responses.try_recv().unwrap().correlation_id,
                    MessageId::Ulong(id)
                );
            }
            assert!(matches!(
                responses.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }
        authorization
            .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
            .await
            .unwrap();
        assert_eq!(
            replacement_responses.recv().await.unwrap().correlation_id,
            MessageId::Ulong(999)
        );
        wire.barrier().await;
        wire.no_frame_yet().await;
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn request_owner_keeps_original_token_and_cached_packet_across_cancelled_finishes() {
    let authorization = authorization();
    let token = signed_token("sender", "orders", expiry());
    let message = request(&token, 42);
    let held = authorization.grant_write_lock().await;
    let control = OperationControl::new();
    let mut custody = RequestCustody::default();
    custody.token = Some(PendingOperation::new(
        process_request(
            &message,
            MessageId::Ulong(42),
            &authorization,
            control.clone(),
        ),
        control.clone(),
    ));
    pending_once(Box::pin(custody.token.as_mut().unwrap().observe()).as_mut()).await;
    assert!(control.started());
    for _ in 0..2 {
        pending_once(Box::pin(custody.finish()).as_mut()).await;
        assert!(custody.token.is_some() && custody.token_packet.is_none());
        assert!(control.started() && control.is_retired());
        assert!(held.is_empty());
    }
    drop(held);
    timeout(WAIT, custody.finish()).await.unwrap();
    assert!(custody.token.is_none() && custody.native.is_none());
    let packet = custody.token_packet.as_ref().unwrap();
    assert!(packet.started && packet.retired && !packet.panicked);
    let response = packet.result.as_ref().unwrap().as_ref().unwrap();
    assert_eq!(response.correlation_id, MessageId::Ulong(42));
    assert_eq!(response.status_code, 202);
    timeout(WAIT, custody.finish()).await.unwrap();
    assert_eq!(
        custody
            .token_packet
            .as_ref()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .status_code,
        202
    );
    assert_eq!(authorization.grant_snapshot().await.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn panicked_original_is_terminal_and_keeps_metadata_without_a_fabricated_result() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for started in [false, true] {
        let polls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&polls);
        let control = OperationControl::new();
        let frontier = control.clone();
        let mut original = PendingOperation::new(
            std::future::poll_fn(move |_| -> std::task::Poll<Result<(), EngineError>> {
                count.fetch_add(1, Ordering::SeqCst);
                if started {
                    assert!(frontier.begin());
                }
                std::panic::panic_any("actual original CBS future panic");
            }),
            control,
        );
        let payload = AssertUnwindSafe(original.observe())
            .catch_unwind()
            .await
            .unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"actual original CBS future panic")
        );
        assert!(original.finish().await.is_none());
        assert!(original.finish().await.is_none());
        let packet = original.take_packet().unwrap();
        assert_eq!(packet.started, started);
        assert!(packet.retired && packet.panicked && packet.result.is_none());
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "a poisoned original must never be repolled"
        );
    }

    // Custody primitive only: two started slots, not an actual native task tree.
    let polls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&polls);
    let control = OperationControl::new();
    let frontier = control.clone();
    let mut custody = RequestCustody::default();
    custody.token = Some(PendingOperation::new(
        std::future::poll_fn(move |_| -> std::task::Poll<Option<CbsResponse>> {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                assert!(frontier.begin());
                return std::task::Poll::Pending;
            }
            std::panic::panic_any("secondary original CBS future panic");
        }),
        control,
    ));
    let (release, held) = tokio::sync::oneshot::channel();
    let native_starts = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&native_starts);
    let control = OperationControl::new();
    let frontier = control.clone();
    custody.native = Some(PendingOperation::new(
        async move {
            assert!(frontier.begin());
            count.fetch_add(1, Ordering::SeqCst);
            held.await.unwrap();
            Err(EngineError::InvalidState(
                "secondary native result".to_owned(),
            ))
        },
        control,
    ));
    pending_once(Box::pin(custody.token.as_mut().unwrap().observe()).as_mut()).await;
    pending_once(Box::pin(custody.native.as_mut().unwrap().observe()).as_mut()).await;
    for _ in 0..2 {
        pending_once(Box::pin(custody.finish()).as_mut()).await;
        assert!(custody.token.is_none() && custody.native.is_some());
        let packet = custody.token_packet.as_ref().unwrap();
        assert!(packet.started && packet.retired && packet.panicked && packet.result.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(native_starts.load(Ordering::SeqCst), 1);
    }
    release.send(()).unwrap();
    timeout(WAIT, custody.finish()).await.unwrap();
    assert!(
        matches!(custody.native_packet.as_ref().unwrap().result.as_ref(), Some(Err(EngineError::InvalidState(value))) if value == "secondary native result")
    );
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(native_starts.load(Ordering::SeqCst), 1);
    let identity = Arc::new(());
    let primary = catch_unwind(|| std::panic::panic_any(Arc::clone(&identity))).unwrap_err();
    let resumed = catch_unwind(AssertUnwindSafe(|| {
        finish_pump::<()>(
            Err(primary),
            custody.take_cleanup_panic(),
            custody.take_native_error(),
        )
    }))
    .unwrap_err()
    .downcast::<Arc<()>>()
    .unwrap();
    assert!(Arc::ptr_eq(&identity, &resumed));
    let original_error: CbsError =
        EngineError::InvalidState("original CBS error".to_owned()).into();
    let returned = finish_pump::<()>(
        Ok(Err(original_error)),
        Some(Box::new("secondary diagnostic panic")),
        Some(EngineError::Stopped),
    )
    .unwrap_err();
    assert!(
        matches!(returned.downcast_ref::<EngineError>(), Some(EngineError::InvalidState(value)) if value == "original CBS error")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn concrete_validation_is_inert_until_first_poll_and_unpolled_retirement_stores_nothing() {
    let authorization = authorization();
    let token = signed_token("sender", "orders", expiry());
    let held = authorization.grant_write_lock().await;
    let control = OperationControl::new();
    let mut original = PendingOperation::new(
        TokenValidation::new(&authorization, &token, AUDIENCE, control.clone()),
        control.clone(),
    );
    assert!(!control.started());
    assert!(original.finish().await.is_none());
    let packet = original.take_packet().unwrap();
    assert!(!packet.started && packet.retired && packet.result.is_none());
    assert!(held.is_empty());
    drop(held);
    assert!(authorization.grant_snapshot().await.is_empty());
    // The same finite signed token is valid; the empty store was retirement,
    // not a hidden validation refusal.
    authorization
        .validate_and_add(&token, AUDIENCE)
        .await
        .unwrap();
    assert_eq!(authorization.grant_snapshot().await.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_validated_token_store_and_raw_result_survive_cancelled_borrowed_finishes() {
    let authorization = authorization();
    let expires = expiry();
    let token = signed_token("sender", "orders", expires);
    let held = authorization.grant_write_lock().await;
    let control = OperationControl::new();
    let mut original = PendingOperation::new(
        TokenValidation::new(&authorization, &token, AUDIENCE, control.clone()),
        control.clone(),
    );
    pending_once(Box::pin(original.observe()).as_mut()).await;
    assert!(control.started() && !control.is_retired());
    assert!(held.is_empty());
    for _ in 0..2 {
        pending_once(Box::pin(original.finish()).as_mut()).await;
        assert!(control.started() && control.is_retired());
    }
    assert!(original.take_packet().is_none());
    drop(held);
    assert!(matches!(
        timeout(WAIT, original.finish()).await.unwrap(),
        Some(Some(Ok(())))
    ));
    let packet = original.take_packet().unwrap();
    assert!(packet.started && packet.retired);
    assert!(matches!(packet.result, Some(Some(Ok(())))));
    assert!(original.take_packet().is_none());
    let grants = authorization.grant_snapshot().await;
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].subject(), "sender");
    assert_eq!(
        grants[0].scope(),
        &ResourceScope::entity(HOST, "orders").unwrap()
    );
    assert_eq!(grants[0].expires_at_epoch_seconds(), expires);
    assert_eq!(grants[0].permissions(), PermissionSet::SEND);
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_cbs_preparation_keeps_field_precedence_and_never_starts_token_validation() {
    let authorization = authorization();
    let held = authorization.grant_write_lock().await;
    let mut valid_properties = ApplicationProperties::default();
    valid_properties.insert(OPERATION_PROPERTY, PUT_TOKEN_OPERATION);
    valid_properties.insert(TOKEN_TYPE_PROPERTY, SAS_TOKEN_TYPE);
    let mut audience_properties = valid_properties.clone();
    audience_properties.insert(AUDIENCE_PROPERTY, AUDIENCE);
    let cases = [
        (Message::default(), "application properties are required"),
        (
            Message {
                application_properties: Some(ApplicationProperties::default()),
                ..Message::default()
            },
            "unsupported CBS operation or token type",
        ),
        (
            Message {
                application_properties: Some(valid_properties),
                ..Message::default()
            },
            "the token audience is required",
        ),
        (
            Message {
                application_properties: Some(audience_properties),
                ..Message::default()
            },
            "the SAS token must be an AMQP value string",
        ),
    ];
    for (message, description) in cases {
        let control = OperationControl::new();
        let response = timeout(
            WAIT,
            process_request(
                &message,
                MessageId::Ulong(42),
                &authorization,
                control.clone(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!control.started());
        assert_response(response, 42, 400, description);
        assert!(held.is_empty());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_invalid_sas_is_refused_before_grant_lock_without_exposing_policy_errors() {
    let authorization = authorization();
    let held = authorization.grant_write_lock().await;
    let tokens = [
        "invalid-token".to_owned(),
        signed_token("sender", "orders", 1),
        signed_token("unknown", "orders", expiry()),
        signed_token("sender", "orders", expiry()).replace("sig=", "sig=broken"),
    ];
    for token in tokens {
        let control = OperationControl::new();
        let message = request(&token, 42);
        let response = timeout(
            WAIT,
            process_request(
                &message,
                MessageId::Ulong(42),
                &authorization,
                control.clone(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(control.started());
        assert_response(response, 42, 401, "Unauthorized");
        assert!(held.is_empty());
    }
    let invalid = signed_token("unknown", "orders", 1);
    assert!(matches!(
        timeout(
            WAIT,
            authorization.validate_and_add(&invalid, "invalid-audience")
        )
        .await
        .unwrap(),
        Err(auth::SasError::Expired)
    ));
    drop(held);
    assert!(authorization.grant_snapshot().await.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_request_detach_drains_validated_token_store_without_a_new_acknowledgement() {
    let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
    let LinkEndpoint::Receiver(receiver) = endpoint else {
        panic!("actual CBS request receiver")
    };
    let authorization = authorization();
    let expires = expiry();
    let token = signed_token("sender", "orders", expires);
    let (_route, mut responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    let held = authorization.grant_write_lock().await;
    wire.request_message(&request(&token, 42), false).await;
    wire.barrier().await;
    let mut serving = Box::pin(serve_cbs_requests(receiver, Arc::clone(&authorization)));
    pending_once(serving.as_mut()).await;
    assert!(held.is_empty());
    wire.detach().await;
    pending_once(serving.as_mut()).await;
    assert!(held.is_empty());
    assert!(matches!(
        responses.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    drop(held);
    timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
    let grants = authorization.grant_snapshot().await;
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].subject(), "sender");
    assert_eq!(grants[0].expires_at_epoch_seconds(), expires);
    assert_eq!(
        grants[0].scope(),
        &ResourceScope::entity(HOST, "orders").unwrap()
    );
    assert!(matches!(
        responses.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    wire.barrier().await;
    wire.no_frame_yet().await;
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_full_seventeenth_route_and_missing_route_retire_without_relookup_or_token_rollback()
{
    for full in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("actual CBS request receiver")
        };
        let authorization = authorization();
        let old = if full {
            let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
            for id in 0..16 {
                route
                    .send(CbsResponse::accepted(MessageId::Ulong(id)))
                    .await
                    .unwrap();
            }
            assert_eq!(route.capacity(), 0);
            Some((route, responses))
        } else {
            None
        };
        let token = signed_token("sender", "orders", expiry());
        wire.request_message(&request(&token, 42), false).await;
        wire.barrier().await;
        let mut serving = Box::pin(serve_cbs_requests(receiver, Arc::clone(&authorization)));
        pending_once(serving.as_mut()).await;
        let Performative::Disposition(accepted) = wire.control(1).await else {
            panic!("original diagnostic request acceptance")
        };
        assert_eq!(accepted.first, 1);
        assert_eq!(accepted.state, Some(DeliveryState::Accepted(Accepted)));
        wire.barrier().await;
        pending_once(serving.as_mut()).await;
        let stored = authorization.grant_snapshot().await;
        assert_eq!(stored.len(), 1);
        let (_replacement, mut replacement_responses) =
            authorization.register_reply_route(ADDRESS.to_owned()).await;
        wire.detach().await;
        timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
        assert_eq!(authorization.grant_snapshot().await, stored);
        assert!(matches!(
            replacement_responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        if let Some((route, mut responses)) = old {
            assert_eq!(route.capacity(), 0);
            for id in 0..16 {
                assert_eq!(
                    responses.try_recv().unwrap().correlation_id,
                    MessageId::Ulong(id)
                );
            }
            assert!(matches!(
                responses.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }
        authorization
            .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
            .await
            .unwrap();
        assert_eq!(
            replacement_responses.recv().await.unwrap().correlation_id,
            MessageId::Ulong(999)
        );
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_selected_old_route_failure_never_retries_response_on_replacement() {
    let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
    let LinkEndpoint::Receiver(receiver) = endpoint else {
        panic!("actual CBS request receiver")
    };
    let authorization = authorization();
    let (old, mut old_responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    for id in 0..16 {
        old.send(CbsResponse::accepted(MessageId::Ulong(id)))
            .await
            .unwrap();
    }
    let token = signed_token("sender", "orders", expiry());
    wire.request_message(&request(&token, 42), false).await;
    wire.barrier().await;
    let mut serving = Box::pin(serve_cbs_requests(receiver, Arc::clone(&authorization)));
    pending_once(serving.as_mut()).await;
    assert!(matches!(
        wire.control(1).await,
        Performative::Disposition(_)
    ));
    wire.barrier().await;
    pending_once(serving.as_mut()).await;
    let stored = authorization.grant_snapshot().await;
    let (_replacement, mut replacement_responses) =
        authorization.register_reply_route(ADDRESS.to_owned()).await;
    old_responses.close();
    pending_once(serving.as_mut()).await;
    assert!(matches!(
        replacement_responses.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(authorization.grant_snapshot().await, stored);
    wire.detach().await;
    timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
    assert!(matches!(
        replacement_responses.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn healthy_pre_settled_token_request_preserves_correlated_reply_without_native_ack() {
    let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
    let LinkEndpoint::Receiver(receiver) = endpoint else {
        panic!("actual CBS request receiver")
    };
    let authorization = authorization();
    let (_route, mut responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    let token = signed_token("listener", "orders", expiry());
    wire.request_message(&request(&token, 42), true).await;
    wire.barrier().await;
    let mut serving = Box::pin(serve_cbs_requests(receiver, Arc::clone(&authorization)));
    pending_once(serving.as_mut()).await;
    assert_response(
        timeout(WAIT, responses.recv()).await.unwrap().unwrap(),
        42,
        202,
        "Accepted",
    );
    let grants = authorization.grant_snapshot().await;
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].permissions(), PermissionSet::LISTEN);
    wire.barrier().await;
    wire.no_frame_yet().await;
    wire.detach().await;
    timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_finite_grants_keep_rule_permissions_scope_and_exact_refresh_replacement() {
    for (rule, permissions, allowed, denied) in [
        (
            "sender",
            PermissionSet::SEND,
            Permission::Send,
            Permission::Listen,
        ),
        (
            "listener",
            PermissionSet::LISTEN,
            Permission::Listen,
            Permission::Send,
        ),
        (
            "manager",
            PermissionSet::MANAGE,
            Permission::Manage,
            Permission::Audit,
        ),
    ] {
        let authorization = authorization();
        let expires = expiry();
        let message = request(&signed_token(rule, "orders", expires), 42);
        let response = process_request(
            &message,
            MessageId::Ulong(42),
            &authorization,
            OperationControl::new(),
        )
        .await
        .unwrap();
        assert_response(response, 42, 202, "Accepted");
        let grants = authorization.grant_snapshot().await;
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].subject(), rule);
        assert_eq!(
            grants[0].scope(),
            &ResourceScope::entity(HOST, "orders").unwrap()
        );
        assert_eq!(grants[0].expires_at_epoch_seconds(), expires);
        assert_eq!(grants[0].permissions(), permissions);
        assert!(
            authorization
                .authorize_entity("orders", allowed)
                .await
                .is_ok()
        );
        assert!(
            authorization
                .authorize_entity("orders", denied)
                .await
                .is_err()
        );
        assert!(
            authorization
                .authorize_entity("other", allowed)
                .await
                .is_err()
        );
        assert!(
            authorization
                .authorize_entity("orders", Permission::Cluster)
                .await
                .is_err()
        );
    }
    let authorization = authorization();
    let expires = expiry();
    for (rule, path) in [
        ("sender", "orders"),
        ("sender", "other"),
        ("listener", "orders"),
    ] {
        let token = signed_token(rule, path, expires);
        authorization
            .validate_and_add(&token, &format!("amqps://{HOST}/{path}"))
            .await
            .unwrap();
    }
    let before = authorization.grant_snapshot().await;
    assert_eq!(before.len(), 3);
    let refreshed = request(&signed_token("sender", "orders", expires + 60), 42);
    assert_response(
        process_request(
            &refreshed,
            MessageId::Ulong(42),
            &authorization,
            OperationControl::new(),
        )
        .await
        .unwrap(),
        42,
        202,
        "Accepted",
    );
    let after = authorization.grant_snapshot().await;
    assert_eq!(after.len(), 3);
    let orders = ResourceScope::entity(HOST, "orders").unwrap();
    let matching: Vec<_> = after
        .iter()
        .filter(|grant| grant.subject() == "sender" && grant.scope() == &orders)
        .collect();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].expires_at_epoch_seconds(), expires + 60);
    for untouched in before
        .into_iter()
        .filter(|grant| grant.subject() != "sender" || grant.scope() != &orders)
    {
        assert!(after.contains(&untouched));
    }
}
