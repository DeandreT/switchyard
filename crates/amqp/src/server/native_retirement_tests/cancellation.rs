use std::future::{Future, poll_fn};
use std::task::Poll;

use super::*;

async fn false_discharge_is_rollback(fixture: &mut Fixture, transaction: TransactionId) {
    let id = fixture
        .command(
            CONTROL,
            TransactionCommand::Discharge(Discharge {
                txn_id: transaction,
                fail: Some(false),
            }),
        )
        .await;
    let disposition = fixture.peer.disposition(CONTROL, Role::Receiver, id).await;
    assert!(disposition.settled);
    let Some(DeliveryState::Rejected(rejected)) = disposition.state else {
        panic!("lost consumer must not permit an empty native commit");
    };
    assert_eq!(
        rejected
            .error
            .expect("known rollback refusal")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:transaction:rollback"
    );
}

#[tokio::test]
async fn cancel_before_final_transfer_flush_preserves_transport_but_revokes_consumer_authority() {
    let mut fixture = Fixture::with_policy(Policy::Work, 512).await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let transaction = txn(25);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let message = Message {
        body: Body::Data(vec![vec![42; 2_000].into()]),
        ..Message::default()
    };
    fixture
        .gate
        .final_transfer(fixture.peer.local(SEND), handle);
    let mut sending = Box::pin(sender.send_with_dispositions(message.clone(), TAG.to_vec().into()));
    let id = bounded("complete outgoing bytes before final flush cancellation", async {
        tokio::select! {
            result = sending.as_mut() => panic!("sent handle before gated final flush: {}", result.is_ok()),
            id = fixture.peer.outgoing(SEND, handle, &message, TAG) => id,
        }
    }).await;
    fixture.gate.wait().await;
    assert_eq!(observer.state(), NativeTransactionState::Pending);
    drop(sending);
    fixture.gate.release();
    fixture.peer.barrier(SEND).await;
    assert_eq!(observer.state(), NativeTransactionState::Pending);
    fixture.peer.retirement(SEND, id, &transaction).await;
    let (channel, frame) = fixture.peer.control().await;
    assert_eq!(channel, fixture.peer.local(SEND));
    let Performative::Detach(detach) = frame else {
        panic!("canceled consumer cannot accept retirement publication");
    };
    assert_eq!(detach.handle, handle);
    assert!(detach.closed);
    assert_eq!(
        detach
            .error
            .expect("closed disposition inbox")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:resource-limit-exceeded"
    );
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    fixture
        .peer
        .send(
            SEND,
            Performative::Detach(Detach {
                handle: HANDLE,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await;
    false_discharge_is_rollback(&mut fixture, transaction).await;
    assert!(coordinator.controller_identity().is_active());
    fixture.shutdown().await;
}

#[tokio::test]
async fn dropping_ready_sent_handle_reply_faults_actual_association_before_queued_receipt_drop() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let transaction = txn(26);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let message = message();
    let mut sending = Box::pin(sender.send_with_dispositions(message.clone(), TAG.to_vec().into()));
    poll_fn(|cx| {
        assert!(sending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let id = fixture.peer.outgoing(SEND, handle, &message, TAG).await;
    fixture.peer.barrier(SEND).await;
    // Leave the ready SentDelivery inside its oneshot while the actual actor associates it.
    fixture.peer.retirement(SEND, id, &transaction).await;
    fixture.peer.barrier(SEND).await;
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &transaction, false)
        .await;
    assert_eq!(observer.state(), NativeTransactionState::Sealed);
    {
        let waiting = discharged.receipt.wait_ready();
        tokio::pin!(waiting);
        poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    drop(sending);
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    assert!(matches!(
        bounded(
            "lost ready reply faults unprepared native association",
            discharged.receipt.wait_ready()
        )
        .await,
        Err(NativeTransactionError::Faulted(NativeFault::Dropped))
    ));
    assert!(matches!(
        discharged.receipt.prepare_work(Vec::new()),
        Err(NativeTransactionError::NotReady)
    ));
    fixture.peer.barrier(SEND).await;
    false_discharge_is_rollback(&mut fixture, transaction).await;
    assert!(coordinator.controller_identity().is_active());
    fixture.shutdown().await;
}
