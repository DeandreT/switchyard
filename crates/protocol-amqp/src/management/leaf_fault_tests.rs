//! Captured control-leaf Stop fronts; original parent joins are tested separately.

use super::*;
use crate::listener::connection_custody::ConnectionRetirementRequest;

impl Wire {
    pub(in crate::management) fn retirement_request(&self) -> ConnectionRetirementRequest {
        ConnectionRetirementRequest::capture(&self.connection)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn management_request_fault_notice_interrupts_original_accept_or_reject_write() {
    for reject in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("request receiver")
        };
        let notice = wire.retirement_request();
        let management = ConnectionManagement::new();
        let (_route, mut responses) = management.register_reply_route(ADDRESS.to_owned()).await;
        if reject {
            wire.request_message(&Message::default()).await;
        } else {
            wire.request().await;
        }
        wire.barrier().await;
        wire.writes.arm(false);
        let fault = PumpFault::new(PumpPoint::RequestNative);
        let mut original = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(serve_management_requests_with_retirement(
                    receiver,
                    NamespaceName::new("tenant").unwrap(),
                    EntityPath::new("orders").unwrap(),
                    NoBroker,
                    management,
                    None,
                    Some(notice.clone()),
                ))
                .catch_unwind(),
            ),
        );
        timeout(WAIT, async { tokio::select! {
            result = original.as_mut() => { let _ = result; panic!("original request native write held") },
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
        super::assert_outer_panic(result);
        drop(original);
        assert!(wire.writes.state.lock().unwrap().held);
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn management_reply_fault_notice_interrupts_write_or_second_confirmation_and_keeps_route() {
    for confirmation in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::Second).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("reply sender")
        };
        let notice = wire.retirement_request();
        let management = ConnectionManagement::new();
        let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
        route.send(response(42)).await.unwrap();
        if !confirmation {
            wire.writes.arm(false);
        }
        let fault = PumpFault::new(PumpPoint::ReplyNative);
        let mut original = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(serve_management_replies_with_retirement(
                    sender,
                    ADDRESS.to_owned(),
                    route.clone(),
                    responses,
                    Arc::clone(&management),
                    None,
                    Some(notice.clone()),
                ))
                .catch_unwind(),
            ),
        );
        if confirmation {
            let transfer = timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("reply waits for actual disposition") },
                transfer = super::transfer(&mut wire) => transfer,
            }}).await.unwrap();
            super::accept(&mut wire, transfer.delivery_id.unwrap()).await;
            wire.barrier().await;
            wire.writes.arm(false);
        }
        timeout(WAIT, async { tokio::select! {
            result = original.as_mut() => { let _ = result; panic!("original reply or confirmation held") },
            () = wire.writes.reached() => {},
        }}).await.unwrap();
        timeout(WAIT, fault.reached.notified()).await.unwrap();
        let (_replacement, mut replacement_responses) =
            management.register_reply_route(ADDRESS.to_owned()).await;
        assert!(!notice.is_requested());
        fault.trigger.notify_one();
        let ((), result) = timeout(WAIT, async {
            tokio::join!(notice.observer(), original.as_mut())
        })
        .await
        .unwrap();
        super::assert_outer_panic(result);
        drop(original);
        assert!(wire.writes.state.lock().unwrap().held);
        assert!(route.is_closed());
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        management
            .route_response(ADDRESS, response(999))
            .await
            .unwrap();
        assert_eq!(
            replacement_responses.recv().await.unwrap().correlation_id,
            MessageId::Ulong(999)
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn management_healthy_response_and_detach_do_not_publish_fault() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::First).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("reply sender")
    };
    let notice = wire.retirement_request();
    let management = ConnectionManagement::new();
    let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
    route.send(response(42)).await.unwrap();
    let mut original = Box::pin(serve_management_replies_with_retirement(
        sender,
        ADDRESS.to_owned(),
        route,
        responses,
        management,
        None,
        Some(notice.clone()),
    ));
    let transfer = timeout(WAIT, async {
        tokio::select! {
            result = original.as_mut() => { let _ = result; panic!("healthy reply remains open") },
            transfer = super::transfer(&mut wire) => transfer,
        }
    })
    .await
    .unwrap();
    super::accept(&mut wire, transfer.delivery_id.unwrap()).await;
    wire.barrier().await;
    wire.detach().await;
    timeout(WAIT, original.as_mut()).await.unwrap().unwrap();
    drop(original);
    assert!(!notice.is_requested());
    wire.barrier().await;
    wire.stop().await;
}
