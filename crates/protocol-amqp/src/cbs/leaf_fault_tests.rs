//! Captured real-leaf notices, not task-family or concrete parent join evidence.

use super::*;
use crate::listener::connection_custody::ConnectionRetirementRequest;

#[tokio::test(flavor = "current_thread")]
async fn cbs_request_fault_notice_interrupts_original_accept_or_reject_write() {
    for reject in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("request receiver")
        };
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let authorization = authorization();
        let (_route, mut responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        let message = if reject {
            Message::default()
        } else {
            super::super::request_retirement_tests::request("invalid-token", 42)
        };
        wire.request_message(&message, false).await;
        wire.barrier().await;
        wire.writes.arm(false);
        let fault = PumpFault::new(PumpPoint::RequestNative);
        let mut original = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(serve_cbs_requests_with_retirement(
                    receiver,
                    Arc::clone(&authorization),
                    Some(notice.clone()),
                ))
                .catch_unwind(),
            ),
        );
        timeout(WAIT, async { tokio::select! {
            result = original.as_mut() => { let _ = result; panic!("original CBS native write held") },
            () = wire.writes.reached() => {},
        }}).await.unwrap();
        timeout(WAIT, fault.reached.notified()).await.unwrap();
        assert!(!notice.is_requested());
        fault.trigger.notify_one();
        let ((), result) = timeout(WAIT, async {
            tokio::join!(notice.observer(), original.as_mut())
        })
        .await
        .unwrap();
        assert_outer_panic(result);
        drop(original);
        assert!(wire.writes.state.lock().unwrap().held);
        assert!(notice.is_requested());
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(authorization.grant_snapshot().await.is_empty());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cbs_reply_fault_notice_interrupts_original_write_or_second_confirmation() {
    for confirmation in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::Second).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("reply sender")
        };
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let authorization = authorization();
        let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        route
            .send(CbsResponse::accepted(MessageId::Ulong(42)))
            .await
            .unwrap();
        if !confirmation {
            wire.writes.arm(false);
        }
        let fault = PumpFault::new(PumpPoint::ReplyNative);
        let mut original = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(serve_cbs_replies_with_retirement(
                    sender,
                    ADDRESS.to_owned(),
                    route.clone(),
                    responses,
                    Arc::clone(&authorization),
                    Some(notice.clone()),
                ))
                .catch_unwind(),
            ),
        );
        if confirmation {
            let (transfer, message) = timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("reply waits for actual disposition") },
                transferred = wire.response_transfer() => transferred,
            }}).await.unwrap();
            assert_eq!(
                message.properties.unwrap().correlation_id,
                Some(MessageId::Ulong(42))
            );
            wire.accept_response(transfer.delivery_id.unwrap()).await;
            wire.barrier().await;
            wire.writes.arm(false);
        }
        timeout(WAIT, async { tokio::select! {
            result = original.as_mut() => { let _ = result; panic!("original reply or confirmation held") },
            () = wire.writes.reached() => {},
        }}).await.unwrap();
        timeout(WAIT, fault.reached.notified()).await.unwrap();
        let (_replacement, mut replacement_responses) =
            authorization.register_reply_route(ADDRESS.to_owned()).await;
        assert!(!notice.is_requested());
        fault.trigger.notify_one();
        let ((), result) = timeout(WAIT, async {
            tokio::join!(notice.observer(), original.as_mut())
        })
        .await
        .unwrap();
        assert_outer_panic(result);
        drop(original);
        assert!(wire.writes.state.lock().unwrap().held);
        assert!(route.is_closed());
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        authorization
            .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
            .await
            .unwrap();
        assert_eq!(
            replacement_responses.recv().await.unwrap().correlation_id,
            MessageId::Ulong(999)
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cbs_reply_healthy_response_and_detach_do_not_publish_fault() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::First).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("reply sender")
    };
    let notice = ConnectionRetirementRequest::capture(&wire.connection);
    let authorization = authorization();
    let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    route
        .send(CbsResponse::accepted(MessageId::Ulong(42)))
        .await
        .unwrap();
    let mut original = Box::pin(serve_cbs_replies_with_retirement(
        sender,
        ADDRESS.to_owned(),
        route.clone(),
        responses,
        Arc::clone(&authorization),
        Some(notice.clone()),
    ));
    let (transfer, message) = timeout(WAIT, async { tokio::select! {
        result = original.as_mut() => { let _ = result; panic!("healthy reply waits for disposition") },
        transferred = wire.response_transfer() => transferred,
    }}).await.unwrap();
    assert_eq!(
        message.properties.unwrap().correlation_id,
        Some(MessageId::Ulong(42))
    );
    wire.accept_response(transfer.delivery_id.unwrap()).await;
    wire.barrier().await;
    wire.detach().await;
    timeout(WAIT, original.as_mut()).await.unwrap().unwrap();
    drop(original);
    assert!(!notice.is_requested());
    wire.barrier().await;
    wire.stop().await;
}
