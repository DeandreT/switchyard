//! CBS cleanup notices. Concurrent request slots and injected original poison
//! are qualified custody compositions, not ordinary two-slot admission.

use std::{
    panic::{catch_unwind, panic_any},
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::*;
use crate::listener::connection_custody::ConnectionRetirementRequest;

const AUDIENCE: &str = "amqps://tenant.servicebus.windows.net/orders";

fn signed_token(rule: &str, path: &str, expiry: u64) -> String {
    let encoded = format!("amqps%3A%2F%2Ftenant.servicebus.windows.net%2F{path}");
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    mac.update(format!("{encoded}\n{expiry}").as_bytes());
    let signature = STANDARD
        .encode(mac.finalize().into_bytes())
        .replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D");
    format!("SharedAccessSignature sr={encoded}&sig={signature}&se={expiry}&skn={rule}")
}

fn expiry() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600
}

fn counted<'a, T>(
    actual: impl Future<Output = T> + Send + 'a,
    polls: Arc<AtomicUsize>,
) -> impl Future<Output = T> + Send + 'a {
    let mut actual = Box::pin(actual);
    poll_fn(move |context| {
        polls.fetch_add(1, Ordering::SeqCst);
        actual.as_mut().poll(context)
    })
}

fn poisoned<'a, T: Send + 'a>(
    actual: impl Future<Output = T> + Send + 'a,
    after_result: bool,
    reached: Arc<Notify>,
    trigger: Arc<Notify>,
    payload: Arc<str>,
    polls: Arc<AtomicUsize>,
) -> impl Future<Output = T> + Send + 'a {
    let mut actual = Box::pin(actual);
    let mut trigger = Box::pin(async move { trigger.notified().await });
    let mut reached_once = false;
    let mut result = None;
    poll_fn(move |context| {
        polls.fetch_add(1, Ordering::SeqCst);
        if trigger.as_mut().poll(context).is_ready() {
            panic_any(Arc::clone(&payload));
        }
        if result.is_some() {
            return Poll::Pending;
        }
        match actual.as_mut().poll(context) {
            Poll::Pending => {
                if !after_result && !reached_once {
                    reached_once = true;
                    reached.notify_one();
                }
            }
            Poll::Ready(value) => {
                assert!(
                    after_result,
                    "injected pre-result fault needs held original"
                );
                result = Some(value);
                reached.notify_one();
            }
        }
        Poll::Pending
    })
}

fn assert_payload(payload: PanicPayload, expected: &Arc<str>) {
    let actual = payload.downcast::<Arc<str>>().unwrap();
    assert!(Arc::ptr_eq(&actual, expected));
}

fn original_poison<T: Send>(
    payload: Arc<str>,
    polls: Arc<AtomicUsize>,
) -> impl Future<Output = T> + Send {
    counted(async move { panic_any(payload) }, polls)
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_request_grant_poison_stops_actual_ack_or_reject_without_write_release() {
    for after_result in [false, true] {
        for reject in [false, true] {
            let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
            let LinkEndpoint::Receiver(mut receiver) = endpoint else {
                panic!("actual request receiver")
            };
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let authorization = authorization();
            let expires = expiry();
            authorization
                .validate_and_add(&signed_token("listener", "orders", expires), AUDIENCE)
                .await
                .unwrap();
            let before = authorization.grant_snapshot().await;
            let mut grant_guard = Some(authorization.grant_write_lock().await);
            let token = signed_token("sender", "orders", expires);
            let message = super::super::request_retirement_tests::request(&token, 42);
            wire.request_message(&message, false).await;
            wire.barrier().await;
            let delivery = timeout(WAIT, receiver.recv()).await.unwrap().unwrap();
            let control = OperationControl::new();
            let reached = Arc::new(Notify::new());
            let trigger = Arc::new(Notify::new());
            let payload: Arc<str> = Arc::from("controlled original CBS validation poison");
            let token_polls = Arc::new(AtomicUsize::new(0));
            let mut custody = RequestCustody::default();
            custody.token = Some(PendingOperation::new(
                poisoned(
                    process_request(
                        &message,
                        MessageId::Ulong(42),
                        &authorization,
                        control.clone(),
                    ),
                    after_result,
                    Arc::clone(&reached),
                    Arc::clone(&trigger),
                    Arc::clone(&payload),
                    Arc::clone(&token_polls),
                ),
                control.clone(),
            ));
            {
                let mut original = Box::pin(custody.token.as_mut().unwrap().observe());
                pending_once(Box::pin(tokio::task::unconstrained(original.as_mut())).as_mut())
                    .await;
            }
            assert!(control.started());
            assert_eq!(grant_guard.as_ref().unwrap().as_slice(), before.as_slice());
            if after_result {
                drop(grant_guard.take());
                let mut original = Box::pin(custody.token.as_mut().unwrap().observe());
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("poison gate holds raw response") },
                    () = reached.notified() => {},
                }}).await.unwrap();
                let grants = authorization.grant_snapshot().await;
                assert_eq!(grants.len(), before.len() + 1);
                assert_eq!(grants[0], before[0]);
                let installed = grants.last().unwrap();
                assert_eq!(installed.subject(), "sender");
                assert_eq!(
                    installed.scope(),
                    &auth::ResourceScope::parse(AUDIENCE).unwrap()
                );
                assert_eq!(installed.expires_at_epoch_seconds(), expires);
                assert_eq!(installed.permissions(), auth::PermissionSet::SEND);
            } else {
                timeout(WAIT, reached.notified()).await.unwrap();
            }
            let expected = if after_result {
                authorization.grant_snapshot().await
            } else {
                before.clone()
            };
            wire.writes.arm(false);
            let native_polls = Arc::new(AtomicUsize::new(0));
            custody.native = Some(native_operation(counted(
                async {
                    if reject {
                        receiver.reject(&delivery, None).await
                    } else {
                        receiver.accept(&delivery).await
                    }
                },
                Arc::clone(&native_polls),
            )));
            {
                let mut original = Box::pin(custody.native.as_mut().unwrap().observe());
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("actual native disposition held") },
                    () = wire.writes.reached() => {},
                }}).await.unwrap();
            }
            {
                let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
                pending_once(finish.as_mut()).await;
            }
            assert!(!notice.is_requested());
            trigger.notify_one();
            let ((), ()) = timeout(WAIT, async {
                tokio::join!(
                    notice.observer(),
                    custody.finish_with_retirement(Some(&notice))
                )
            })
            .await
            .unwrap();
            assert!(wire.writes.state.lock().unwrap().held);
            assert!(custody.token_packet.as_ref().unwrap().panicked);
            assert!(custody.token_packet.as_ref().unwrap().result.is_none());
            assert!(matches!(
                custody.native_packet.as_ref().unwrap().result,
                Some(Err(EngineError::Stopped))
            ));
            let token_poll_count = token_polls.load(Ordering::SeqCst);
            let native_poll_count = native_polls.load(Ordering::SeqCst);
            let native_result = custody
                .native_packet
                .as_ref()
                .unwrap()
                .result
                .as_ref()
                .unwrap() as *const Result<(), EngineError>;
            custody.finish_with_retirement(Some(&notice)).await;
            notice.request();
            assert_eq!(token_polls.load(Ordering::SeqCst), token_poll_count);
            assert_eq!(native_polls.load(Ordering::SeqCst), native_poll_count);
            assert_eq!(
                custody
                    .native_packet
                    .as_ref()
                    .unwrap()
                    .result
                    .as_ref()
                    .unwrap() as *const Result<(), EngineError>,
                native_result
            );
            let cleanup_panic = custody.take_cleanup_panic().unwrap();
            assert!(Arc::ptr_eq(
                cleanup_panic.downcast_ref::<Arc<str>>().unwrap(),
                &payload
            ));
            let error = finish_pump(
                Ok(Ok(true)),
                Some(cleanup_panic),
                custody.take_native_error(),
            )
            .unwrap_err();
            assert!(matches!(
                error.downcast_ref::<EngineError>(),
                Some(EngineError::Stopped)
            ));
            drop(grant_guard.take());
            assert_eq!(authorization.grant_snapshot().await, expected);
            timeout(WAIT, wire.connection.shutdown())
                .await
                .unwrap()
                .unwrap();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_reply_write_or_confirmation_error_notifies_before_cancelled_route_cleanup() {
    for confirmation in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::Second).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual reply sender")
        };
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let authorization = authorization();
        let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        let mut custody =
            ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &authorization);
        let polls = Arc::new(AtomicUsize::new(0));
        let control = OperationControl::new();
        custody.original = Some(PendingOperation::new(
            counted(
                send_cbs_response(
                    &sender,
                    CbsResponse::accepted(MessageId::Ulong(42)),
                    control.clone(),
                ),
                Arc::clone(&polls),
            ),
            control,
        ));
        if confirmation {
            {
                let mut original = Box::pin(custody.original.as_mut().unwrap().observe());
                let (transfer, message) = timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("reply needs peer disposition") },
                    value = wire.response_transfer() => value,
                }}).await.unwrap();
                assert_eq!(
                    message.properties.unwrap().correlation_id,
                    Some(MessageId::Ulong(42))
                );
                wire.accept_response(transfer.delivery_id.unwrap()).await;
                wire.barrier().await;
            }
        }
        wire.writes.arm(false);
        {
            let mut original = Box::pin(custody.original.as_mut().unwrap().observe());
            timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("actual reply native write held") },
                () = wire.writes.reached() => {},
            }}).await.unwrap();
        }
        let (_replacement, mut replacement_responses) =
            authorization.register_reply_route(ADDRESS.to_owned()).await;
        let route_guard = authorization.reply_route_lock().await;
        wire.writes.fail_held_write();
        {
            let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
            timeout(WAIT, async {
                tokio::select! {
                    () = finish.as_mut() => panic!("captured route lock holds cleanup"),
                    () = notice.observer() => {},
                }
            })
            .await
            .unwrap();
        }
        let error = custody
            .packet
            .as_ref()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err();
        if confirmation {
            assert!(
                matches!(error, EngineError::InvalidState(value) if value == "controlled CBS native write failure")
            );
        } else {
            assert!(matches!(error, EngineError::Stopped));
        }
        let error_identity = error as *const EngineError;
        let poll_count = polls.load(Ordering::SeqCst);
        assert!(route.is_closed());
        assert!(wire.writes.state.lock().unwrap().held);
        {
            let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
            pending_once(finish.as_mut()).await;
        }
        notice.request();
        assert_eq!(polls.load(Ordering::SeqCst), poll_count);
        assert_eq!(
            custody
                .packet
                .as_ref()
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err() as *const EngineError,
            error_identity
        );
        drop(route_guard);
        timeout(WAIT, custody.finish_with_retirement(Some(&notice)))
            .await
            .unwrap();
        assert!(custody.take_cleanup_panic().is_none());
        assert!(custody.take_native_error().is_some());
        authorization
            .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
            .await
            .unwrap();
        assert_eq!(
            replacement_responses.recv().await.unwrap().correlation_id,
            MessageId::Ulong(999)
        );
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_no_credit_reply_error_notifies_before_same_conditional_unregister_retry() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 0, ReceiverSettleMode::First).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("actual no-credit reply sender")
    };
    let notice = ConnectionRetirementRequest::capture(&wire.connection);
    let authorization = authorization();
    let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    let mut custody =
        ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &authorization);
    let control = OperationControl::new();
    let polls = Arc::new(AtomicUsize::new(0));
    custody.original = Some(PendingOperation::new(
        counted(
            send_cbs_response(
                &sender,
                CbsResponse::accepted(MessageId::Ulong(42)),
                control.clone(),
            ),
            Arc::clone(&polls),
        ),
        control.clone(),
    ));
    {
        let mut original = Box::pin(custody.original.as_mut().unwrap().observe());
        pending_once(Box::pin(tokio::task::unconstrained(original.as_mut())).as_mut()).await;
    }
    assert!(control.started());
    wire.barrier().await;
    wire.detach().await;
    let (_replacement, mut replacement_responses) =
        authorization.register_reply_route(ADDRESS.to_owned()).await;
    let route_guard = authorization.reply_route_lock().await;
    {
        let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
        timeout(WAIT, async {
            tokio::select! {
                () = finish.as_mut() => panic!("original route lock holds unregister"),
                () = notice.observer() => {},
            }
        })
        .await
        .unwrap();
    }
    assert!(matches!(
        custody.packet.as_ref().unwrap().result.as_ref(),
        Some(Err(EngineError::RemoteDetached))
    ));
    let error_identity = custody
        .packet
        .as_ref()
        .unwrap()
        .result
        .as_ref()
        .unwrap()
        .as_ref()
        .unwrap_err() as *const EngineError;
    let poll_count = polls.load(Ordering::SeqCst);
    {
        let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
        pending_once(finish.as_mut()).await;
    }
    assert_eq!(polls.load(Ordering::SeqCst), poll_count);
    assert_eq!(
        custody
            .packet
            .as_ref()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err() as *const EngineError,
        error_identity
    );
    drop(route_guard);
    custody.finish_with_retirement(Some(&notice)).await;
    assert!(route.is_closed());
    authorization
        .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
        .await
        .unwrap();
    assert_eq!(
        replacement_responses.recv().await.unwrap().correlation_id,
        MessageId::Ulong(999)
    );
    let error = finish_pump(
        Ok(Ok(())),
        custody.take_cleanup_panic(),
        custody.take_native_error(),
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<EngineError>(),
        Some(EngineError::RemoteDetached)
    ));
    timeout(WAIT, wire.connection.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_reply_original_poison_is_cached_before_route_wait_and_never_repolled() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::First).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("actual reply sender")
    };
    let notice = ConnectionRetirementRequest::capture(&wire.connection);
    let authorization = authorization();
    let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    let mut custody =
        ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &authorization);
    let payload: Arc<str> = Arc::from("controlled original CBS reply poison");
    let polls = Arc::new(AtomicUsize::new(0));
    let reached = Arc::new(Notify::new());
    let trigger = Arc::new(Notify::new());
    let control = OperationControl::new();
    custody.original = Some(PendingOperation::new(
        poisoned(
            send_cbs_response(
                &sender,
                CbsResponse::accepted(MessageId::Ulong(42)),
                control.clone(),
            ),
            false,
            Arc::clone(&reached),
            Arc::clone(&trigger),
            Arc::clone(&payload),
            Arc::clone(&polls),
        ),
        control,
    ));
    wire.writes.arm(false);
    {
        let mut original = Box::pin(custody.original.as_mut().unwrap().observe());
        timeout(WAIT, async { tokio::select! {
            result = original.as_mut() => { let _ = result; panic!("actual Transfer write held") },
            () = wire.writes.reached() => {},
        }}).await.unwrap();
    }
    let (_replacement, mut replacement_responses) =
        authorization.register_reply_route(ADDRESS.to_owned()).await;
    let route_guard = authorization.reply_route_lock().await;
    trigger.notify_one();
    {
        let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
        timeout(WAIT, async {
            tokio::select! {
                () = finish.as_mut() => panic!("route lock remains held after poison"),
                () = notice.observer() => {},
            }
        })
        .await
        .unwrap();
    }
    assert!(custody.packet.as_ref().unwrap().panicked);
    assert!(custody.packet.as_ref().unwrap().result.is_none());
    assert!(wire.writes.state.lock().unwrap().held);
    let poll_count = polls.load(Ordering::SeqCst);
    {
        let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
        pending_once(finish.as_mut()).await;
    }
    assert_eq!(polls.load(Ordering::SeqCst), poll_count);
    drop(route_guard);
    custody.finish_with_retirement(Some(&notice)).await;
    assert_payload(custody.take_cleanup_panic().unwrap(), &payload);
    assert!(custody.take_native_error().is_none());
    assert!(route.is_closed());
    authorization
        .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
        .await
        .unwrap();
    assert_eq!(
        replacement_responses.recv().await.unwrap().correlation_id,
        MessageId::Ulong(999)
    );
    timeout(WAIT, wire.connection.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn actual_request_native_cleanup_error_is_retained_once_and_keeps_cbs_error_policy() {
    for reject in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(mut receiver) = endpoint else {
            panic!("actual request receiver")
        };
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        wire.request_message(&Message::default(), false).await;
        wire.barrier().await;
        let delivery = receiver.recv().await.unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        let mut custody = RequestCustody::default();
        custody.native = Some(native_operation(counted(
            async {
                if reject {
                    receiver.reject(&delivery, None).await
                } else {
                    receiver.accept(&delivery).await
                }
            },
            Arc::clone(&polls),
        )));
        wire.writes.arm(false);
        {
            let mut original = Box::pin(custody.native.as_mut().unwrap().observe());
            timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("actual disposition write held") },
                () = wire.writes.reached() => {},
            }}).await.unwrap();
        }
        assert!(!notice.is_requested());
        wire.writes.fail_held_write();
        timeout(WAIT, custody.finish_with_retirement(Some(&notice)))
            .await
            .unwrap();
        assert!(notice.is_requested());
        assert!(wire.writes.state.lock().unwrap().held);
        assert!(matches!(
            custody.native_packet.as_ref().unwrap().result.as_ref(),
            Some(Err(EngineError::Stopped))
        ));
        let error_identity = custody
            .native_packet
            .as_ref()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err() as *const EngineError;
        let poll_count = polls.load(Ordering::SeqCst);
        custody.finish_with_retirement(Some(&notice)).await;
        assert_eq!(polls.load(Ordering::SeqCst), poll_count);
        assert_eq!(
            custody
                .native_packet
                .as_ref()
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err() as *const EngineError,
            error_identity
        );
        assert!(custody.take_cleanup_panic().is_none());
        let error = finish_pump(Ok(Ok(true)), None, custody.take_native_error()).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<EngineError>(),
            Some(EngineError::Stopped)
        ));
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn previously_completed_actual_errors_do_not_become_cleanup_first_notices() {
    for captured in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual reply sender")
        };
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let authorization = authorization();
        let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        let mut custody =
            ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &authorization);
        let polls = Arc::new(AtomicUsize::new(0));
        let control = OperationControl::new();
        custody.original = Some(PendingOperation::new(
            counted(
                send_cbs_response(
                    &sender,
                    CbsResponse::accepted(MessageId::Ulong(42)),
                    control.clone(),
                ),
                Arc::clone(&polls),
            ),
            control,
        ));
        {
            let mut original = Box::pin(custody.original.as_mut().unwrap().observe());
            pending_once(Box::pin(tokio::task::unconstrained(original.as_mut())).as_mut()).await;
        }
        wire.barrier().await;
        wire.detach().await;
        assert!(matches!(
            custody.original.as_mut().unwrap().observe().await,
            Some(Err(EngineError::RemoteDetached))
        ));
        if captured {
            custody.capture_packet();
        }
        let (_replacement, mut replacement_responses) =
            authorization.register_reply_route(ADDRESS.to_owned()).await;
        let route_guard = authorization.reply_route_lock().await;
        {
            let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
            pending_once(finish.as_mut()).await;
        }
        assert!(
            !notice.is_requested(),
            "uncaptured terminal result is not a new drain result"
        );
        assert!(matches!(
            custody.packet.as_ref().unwrap().result.as_ref(),
            Some(Err(EngineError::RemoteDetached))
        ));
        let poll_count = polls.load(Ordering::SeqCst);
        drop(route_guard);
        custody.finish_with_retirement(Some(&notice)).await;
        assert!(!notice.is_requested());
        assert_eq!(polls.load(Ordering::SeqCst), poll_count);
        authorization
            .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
            .await
            .unwrap();
        assert_eq!(
            replacement_responses.recv().await.unwrap().correlation_id,
            MessageId::Ulong(999)
        );
        wire.barrier().await;
        wire.stop().await;

        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(mut receiver) = endpoint else {
            panic!("actual request receiver")
        };
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        wire.request_message(&Message::default(), false).await;
        wire.barrier().await;
        let delivery = receiver.recv().await.unwrap();
        wire.detach().await;
        let mut custody = RequestCustody::default();
        custody.native = Some(native_operation(receiver.accept(&delivery)));
        assert!(matches!(
            custody.native.as_mut().unwrap().observe().await,
            Some(Err(EngineError::InvalidState(_)))
        ));
        if captured {
            custody.capture_native();
        }
        custody.finish_with_retirement(Some(&notice)).await;
        assert!(!notice.is_requested());
        assert!(matches!(
            custody.take_native_error(),
            Some(EngineError::InvalidState(_))
        ));
        wire.barrier().await;
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_previously_poisoned_originals_remain_terminal_without_new_notice() {
    for reply in [false, true] {
        let (mut wire, _endpoint) = Wire::new(Role::Receiver, 0, ReceiverSettleMode::First).await;
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let payload: Arc<str> = Arc::from("controlled already-caught original CBS poison");
        let polls = Arc::new(AtomicUsize::new(0));
        let control = OperationControl::new();
        assert!(control.begin());
        if reply {
            let authorization = authorization();
            let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
            let mut custody =
                ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &authorization);
            custody.original = Some(PendingOperation::new(
                original_poison(Arc::clone(&payload), Arc::clone(&polls)),
                control,
            ));
            let raw = AssertUnwindSafe(custody.original.as_mut().unwrap().observe())
                .catch_unwind()
                .await
                .unwrap_err();
            assert_payload(raw, &payload);
            let route_guard = authorization.reply_route_lock().await;
            {
                let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
                pending_once(finish.as_mut()).await;
            }
            assert!(!notice.is_requested());
            assert!(custody.packet.as_ref().unwrap().panicked);
            drop(route_guard);
            custody.finish_with_retirement(Some(&notice)).await;
            assert!(custody.take_cleanup_panic().is_none());
        } else {
            let mut custody = RequestCustody::default();
            custody.token = Some(PendingOperation::new(
                original_poison(Arc::clone(&payload), Arc::clone(&polls)),
                control,
            ));
            let raw = AssertUnwindSafe(custody.token.as_mut().unwrap().observe())
                .catch_unwind()
                .await
                .unwrap_err();
            assert_payload(raw, &payload);
            custody.finish_with_retirement(Some(&notice)).await;
            assert!(custody.token_packet.as_ref().unwrap().panicked);
            assert!(custody.take_cleanup_panic().is_none());
        }
        assert!(!notice.is_requested());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        wire.barrier().await;
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_cbs_refusal_statuses_installed_grant_and_idle_detach_do_not_notify() {
    for status in [400, 401, 202] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("actual request receiver")
        };
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let authorization = authorization();
        let (_route, mut responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        let expires = expiry();
        let token = if status == 202 {
            signed_token("sender", "orders", expires)
        } else {
            "invalid-token".to_owned()
        };
        let mut message = super::super::request_retirement_tests::request(&token, 42);
        if status == 400 {
            message.application_properties = None;
        }
        wire.request_message(&message, true).await;
        wire.barrier().await;
        let mut serving = Box::pin(serve_cbs_requests_with_retirement(
            receiver,
            Arc::clone(&authorization),
            Some(notice.clone()),
        ));
        let response = timeout(WAIT, async { tokio::select! {
            result = serving.as_mut() => { let _ = result; panic!("healthy CBS request waits for next delivery") },
            response = responses.recv() => response.unwrap(),
        }}).await.unwrap();
        assert_eq!(response.correlation_id, MessageId::Ulong(42));
        assert_eq!(response.status_code, status);
        let grants = authorization.grant_snapshot().await;
        assert_eq!(grants.len(), usize::from(status == 202));
        if let Some(grant) = grants.first() {
            assert_eq!(grant.subject(), "sender");
            assert_eq!(
                grant.scope(),
                &auth::ResourceScope::parse(AUDIENCE).unwrap()
            );
            assert_eq!(grant.expires_at_epoch_seconds(), expires);
        }
        wire.detach().await;
        timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
        assert!(!notice.is_requested());
        assert_eq!(authorization.grant_snapshot().await, grants);
        wire.barrier().await;
        wire.stop().await;
    }

    for reject in [false, true] {
        let (mut wire, endpoint) =
            Wire::new(Role::Receiver, u32::from(reject), ReceiverSettleMode::First).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual reply sender")
        };
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let authorization = authorization();
        let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        if reject {
            route
                .send(CbsResponse::accepted(MessageId::Ulong(42)))
                .await
                .unwrap();
        }
        let mut serving = Box::pin(serve_cbs_replies_with_retirement(
            sender,
            ADDRESS.to_owned(),
            route.clone(),
            responses,
            Arc::clone(&authorization),
            Some(notice.clone()),
        ));
        if reject {
            let (transfer, message) = timeout(WAIT, async { tokio::select! {
                result = serving.as_mut() => { let _ = result; panic!("reply waits for ordinary remote refusal") },
                value = wire.response_transfer() => value,
            }}).await.unwrap();
            assert_eq!(
                message.properties.unwrap().correlation_id,
                Some(MessageId::Ulong(42))
            );
            write_frame(
                &mut wire.peer,
                &frame(
                    CHANNEL,
                    Performative::Disposition(Disposition {
                        role: Role::Receiver,
                        first: transfer.delivery_id.unwrap(),
                        last: None,
                        settled: true,
                        state: Some(DeliveryState::Rejected(amqp::Rejected { error: None })),
                        batchable: false,
                    }),
                ),
            )
            .await
            .unwrap();
            wire.barrier().await;
        } else {
            pending_once(serving.as_mut()).await;
        }
        wire.detach().await;
        timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
        assert!(route.is_closed());
        assert!(!notice.is_requested());
        wire.barrier().await;
        wire.stop().await;
    }
}

struct PanicDiagnostics {
    events: Arc<AtomicUsize>,
    payload: Arc<str>,
}

impl tracing::Subscriber for PanicDiagnostics {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {
        self.events.fetch_add(1, Ordering::SeqCst);
        panic_any(Arc::clone(&self.payload));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test(flavor = "current_thread")]
async fn unstarted_actual_work_and_reached_reporting_only_panic_do_not_notify() {
    let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
    let LinkEndpoint::Receiver(mut receiver) = endpoint else {
        panic!("actual request receiver")
    };
    let notice = ConnectionRetirementRequest::capture(&wire.connection);
    let authorization = authorization();
    let token = signed_token("sender", "orders", expiry());
    let message = super::super::request_retirement_tests::request(&token, 42);
    wire.request_message(&message, false).await;
    wire.barrier().await;
    let delivery = receiver.recv().await.unwrap();
    let validation_polls = Arc::new(AtomicUsize::new(0));
    let native_polls = Arc::new(AtomicUsize::new(0));
    let control = OperationControl::new();
    let mut custody = RequestCustody::default();
    custody.token = Some(PendingOperation::new(
        counted(
            process_request(
                &message,
                MessageId::Ulong(42),
                &authorization,
                control.clone(),
            ),
            Arc::clone(&validation_polls),
        ),
        control,
    ));
    custody.native = Some(native_operation(counted(
        receiver.reject(&delivery, None),
        Arc::clone(&native_polls),
    )));
    custody.finish_with_retirement(Some(&notice)).await;
    assert_eq!(validation_polls.load(Ordering::SeqCst), 0);
    assert_eq!(native_polls.load(Ordering::SeqCst), 0);
    assert!(!custody.token_packet.as_ref().unwrap().started);
    assert!(!custody.native_packet.as_ref().unwrap().started);
    assert!(!notice.is_requested());
    assert!(authorization.grant_snapshot().await.is_empty());
    drop(custody);
    receiver.accept(&delivery).await.unwrap();
    let Performative::Disposition(disposition) = wire.control(CHANNEL).await else {
        panic!("actual independent healthy disposition")
    };
    assert_eq!(disposition.first, 1);
    assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
    wire.stop().await;

    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::First).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("actual unstarted reply sender")
    };
    let notice = ConnectionRetirementRequest::capture(&wire.connection);
    let authorization = super::authorization();
    let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    let reply_polls = Arc::new(AtomicUsize::new(0));
    let control = OperationControl::new();
    let mut custody =
        ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &authorization);
    custody.original = Some(PendingOperation::new(
        counted(
            send_cbs_response(
                &sender,
                CbsResponse::accepted(MessageId::Ulong(42)),
                control.clone(),
            ),
            Arc::clone(&reply_polls),
        ),
        control,
    ));
    custody.finish_with_retirement(Some(&notice)).await;
    assert_eq!(reply_polls.load(Ordering::SeqCst), 0);
    assert!(!custody.packet.as_ref().unwrap().started);
    assert!(!notice.is_requested());
    assert!(route.is_closed());
    drop(custody);
    let mut independent = Box::pin(send_cbs_response(
        &sender,
        CbsResponse::accepted(MessageId::Ulong(999)),
        OperationControl::new(),
    ));
    let (transfer, message) = timeout(WAIT, async { tokio::select! {
        result = independent.as_mut() => { let _ = result; panic!("independent actual reply waits for disposition") },
        value = wire.response_transfer() => value,
    }}).await.unwrap();
    assert_eq!(
        message.properties.unwrap().correlation_id,
        Some(MessageId::Ulong(999))
    );
    wire.accept_response(transfer.delivery_id.unwrap()).await;
    assert!(matches!(
        timeout(WAIT, independent.as_mut()).await.unwrap().unwrap(),
        Outcome::Accepted(_)
    ));
    assert!(!notice.is_requested());
    wire.stop().await;

    let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
    let LinkEndpoint::Receiver(receiver) = endpoint else {
        panic!("actual reporting request receiver")
    };
    let notice = ConnectionRetirementRequest::capture(&wire.connection);
    let authorization = super::authorization();
    let (_route, mut responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    wire.request_message(
        &super::super::request_retirement_tests::request("invalid-token", 42),
        true,
    )
    .await;
    wire.barrier().await;
    let events = Arc::new(AtomicUsize::new(0));
    let payload: Arc<str> = Arc::from("controlled reporting-only CBS panic");
    let dispatch = tracing::Dispatch::new(PanicDiagnostics {
        events: Arc::clone(&events),
        payload: Arc::clone(&payload),
    });
    std::thread::spawn(tracing::callsite::rebuild_interest_cache)
        .join()
        .unwrap();
    let mut serving = Box::pin(
        AssertUnwindSafe(serve_cbs_requests_with_retirement(
            receiver,
            Arc::clone(&authorization),
            Some(notice.clone()),
        ))
        .catch_unwind(),
    );
    let mut observed = Box::pin(poll_fn(|context| {
        tracing::dispatcher::with_default(&dispatch, || serving.as_mut().poll(context))
    }));
    let raw = timeout(WAIT, observed.as_mut()).await.unwrap().unwrap_err();
    assert_payload(raw, &payload);
    assert_eq!(events.load(Ordering::SeqCst), 1);
    assert_eq!(responses.recv().await.unwrap().status_code, 401);
    assert!(!notice.is_requested());
    assert!(authorization.grant_snapshot().await.is_empty());
    wire.barrier().await;
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_fresh_original_poisons_keep_both_payloads_and_existing_resolver_priority() {
    let (mut wire, _endpoint) = Wire::new(Role::Receiver, 0, ReceiverSettleMode::First).await;
    let notice = ConnectionRetirementRequest::capture(&wire.connection);
    let token_payload: Arc<str> = Arc::from("controlled original token cleanup poison");
    let native_payload: Arc<str> = Arc::from("controlled original native cleanup poison");
    let token_polls = Arc::new(AtomicUsize::new(0));
    let native_polls = Arc::new(AtomicUsize::new(0));
    let token_control = OperationControl::new();
    let native_control = OperationControl::new();
    assert!(token_control.begin());
    assert!(native_control.begin());
    let mut custody = RequestCustody::default();
    custody.token = Some(PendingOperation::new(
        original_poison(Arc::clone(&token_payload), Arc::clone(&token_polls)),
        token_control,
    ));
    custody.native = Some(PendingOperation::new(
        original_poison(Arc::clone(&native_payload), Arc::clone(&native_polls)),
        native_control,
    ));
    custody.finish_with_retirement(Some(&notice)).await;
    assert!(notice.is_requested());
    assert!(custody.token_packet.as_ref().unwrap().panicked);
    assert!(custody.native_packet.as_ref().unwrap().panicked);
    custody.finish_with_retirement(Some(&notice)).await;
    assert_eq!(token_polls.load(Ordering::SeqCst), 1);
    assert_eq!(native_polls.load(Ordering::SeqCst), 1);
    let token_panic = custody.take_cleanup_panic().unwrap();
    assert!(Arc::ptr_eq(
        token_panic.downcast_ref::<Arc<str>>().unwrap(),
        &token_payload
    ));
    assert_payload(custody.take_cleanup_panic().unwrap(), &native_payload);
    assert!(custody.take_cleanup_panic().is_none());
    let raw = catch_unwind(AssertUnwindSafe(|| {
        finish_pump(Ok(Ok(())), Some(token_panic), None)
    }))
    .unwrap_err();
    assert_payload(raw, &token_payload);

    let primary: Arc<str> = Arc::from("controlled primary CBS pump panic");
    let raw = catch_unwind(AssertUnwindSafe(|| {
        finish_pump::<()>(
            Err(Box::new(Arc::clone(&primary))),
            Some(Box::new(Arc::clone(&native_payload))),
            Some(EngineError::Stopped),
        )
    }))
    .unwrap_err();
    assert_payload(raw, &primary);
    let error = finish_pump::<()>(
        Ok(Err(Box::new(EngineError::InvalidState(
            "primary CBS error".to_owned(),
        )))),
        Some(Box::new(Arc::clone(&native_payload))),
        Some(EngineError::Stopped),
    )
    .unwrap_err();
    assert!(
        matches!(error.downcast_ref::<EngineError>(), Some(EngineError::InvalidState(value)) if value == "primary CBS error")
    );
    timeout(WAIT, wire.connection.shutdown())
        .await
        .unwrap()
        .unwrap();
}
