use std::future::Future;

use amqp::{Accepted, DeliveryState, Outcome, Performative, ReceiverSettleMode, Role};

use super::fixture::{CHANNEL, Fixture, HANDLE, ReplyWake, bounded, message, pending};

fn owned<F: Future + Send + 'static>(future: F) -> F {
    future
}

#[tokio::test]
async fn unpolled_and_canceled_reservation_futures_do_not_spend_or_strand_credit() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "cancel",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    let unpolled = {
        let borrowed = &sender;
        owned(borrowed.reserve_send())
    };
    drop(unpolled);
    fixture.peer.barrier(CHANNEL).await;
    let mut canceled = Box::pin(sender.reserve_send());
    pending(canceled.as_mut()).await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 0, false).await;
    drop(canceled);
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    assert_eq!(snapshot.delivery_count, Some(0));
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("canceled waiter refunded")
        .try_claim()
        .expect("fresh local claim");
    let responding = async {
        let transfer = fixture.peer.transfer(CHANNEL, local, b"after-cancel").await;
        assert_eq!(transfer.delivery_id, Some(0));
        fixture.peer.accepted(CHANNEL, 0, true).await;
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            sender.send_reserved(claimed, message(), b"after-cancel".to_vec().into()),
            responding
        )
    })
    .await;
    assert!(result.is_ok());
    fixture.shutdown().await;
}

#[tokio::test]
async fn dropping_an_unconsumed_ready_reply_refunds_its_unique_reservation() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "reply-loss",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    let mut lost = Box::pin(sender.reserve_send());
    let reply = ReplyWake::poll_pending(lost.as_mut());
    reply.wait().await;
    // The actor replied, but the future has not taken the unique token.
    drop(lost);
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    assert_eq!(snapshot.delivery_count, Some(0));
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("lost reply refunded")
        .try_claim()
        .expect("replacement claim");
    let responding = async {
        let transfer = fixture.peer.transfer(CHANNEL, local, b"after-loss").await;
        assert_eq!(transfer.delivery_id, Some(0));
        fixture.peer.accepted(CHANNEL, 0, true).await;
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            sender.send_reserved(claimed, message(), b"after-loss".to_vec().into()),
            responding
        )
    })
    .await;
    assert_eq!(
        result.expect("send after lost reply"),
        Outcome::Accepted(Accepted)
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn unused_reserved_and_claimed_tokens_refund_without_allocating_delivery_ids() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "unused",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    let reserved = bounded(sender.reserve_send())
        .await
        .expect("unused reservation");
    drop(reserved);
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("refunded reservation")
        .try_claim()
        .expect("unused claim");
    drop(claimed);
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    assert_eq!(snapshot.delivery_count, Some(0));
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("refunded claim")
        .try_claim()
        .expect("real send claim");
    let responding = async {
        let transfer = fixture.peer.transfer(CHANNEL, local, b"first-real").await;
        assert_eq!(transfer.delivery_id, Some(0));
        fixture.peer.accepted(CHANNEL, 0, true).await;
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            sender.send_reserved(claimed, message(), b"first-real".to_vec().into()),
            responding
        )
    })
    .await;
    assert!(result.is_ok());
    fixture.shutdown().await;
}

#[tokio::test]
async fn reserved_send_preserves_exact_second_mode_acknowledgement() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "second",
            ReceiverSettleMode::Second,
            None,
        )
        .await;
    let observer = sender.sender_identity();
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("second-mode reservation")
        .try_claim()
        .expect("second-mode claim");
    let responding = async {
        let transfer = fixture.peer.transfer(CHANNEL, local, b"second").await;
        assert_eq!(transfer.delivery_id, Some(0));
        fixture.peer.accepted(CHANNEL, 0, false).await;
    };
    let (receipt, ()) = bounded(async {
        tokio::join!(
            sender.send_reserved_with_settlement(claimed, message(), b"second".to_vec().into()),
            responding
        )
    })
    .await;
    let receipt = receipt.expect("actual second-mode outcome");
    assert_eq!(receipt.outcome(), &Outcome::Accepted(Accepted));
    assert!(receipt.belongs_to_sender(&observer));
    assert!(receipt.delivery_identity().belongs_to_sender(&observer));
    let (result, (channel, response)) =
        bounded(async { tokio::join!(receipt.accept(), fixture.peer.control()) }).await;
    result.expect("sender acknowledgement flushed");
    assert_eq!(channel, fixture.peer.local_channel(CHANNEL));
    let Performative::Disposition(acknowledgement) = response else {
        panic!("second-mode sender acknowledgement");
    };
    assert_eq!(acknowledgement.role, Role::Sender);
    assert_eq!(acknowledgement.first, 0);
    assert!(acknowledgement.last.is_none());
    assert!(acknowledgement.settled);
    assert_eq!(
        acknowledgement.state,
        Some(DeliveryState::Accepted(Accepted))
    );
    fixture.shutdown().await;
}
