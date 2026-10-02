use std::future::Future;

use amqp::{
    Accepted, Body, DeliveryState, EngineError, Message, Outcome, PendingSettlement, Performative,
    ReceiverSettleMode, Released, Role,
};

use super::fixture::{CHANNEL, Fixture, HANDLE, ReplyWake, bounded, message, pending};

fn owned<F: Future<Output = Result<PendingSettlement, EngineError>> + Send + 'static>(
    future: F,
) -> F {
    future
}

fn distinct_message(label: &[u8]) -> Message {
    Message {
        body: Body::Data(vec![label.to_vec().into()]),
        ..Message::default()
    }
}

async fn acknowledge(fixture: &mut Fixture, receipt: &PendingSettlement, id: u32) {
    let (result, (channel, response)) =
        bounded(async { tokio::join!(receipt.accept(), fixture.peer.control()) }).await;
    result.expect("exact sender acknowledgement flushed");
    assert_eq!(channel, fixture.peer.local_channel(CHANNEL));
    let Performative::Disposition(acknowledgement) = response else {
        panic!("exact second-mode acknowledgement");
    };
    assert_eq!(acknowledgement.role, Role::Sender);
    assert_eq!(acknowledgement.first, id);
    assert!(acknowledgement.last.is_none());
    assert!(acknowledgement.settled);
    assert_eq!(
        acknowledgement.state,
        Some(DeliveryState::Accepted(Accepted))
    );
}

#[tokio::test]
async fn owned_same_link_sends_correlate_reverse_outcomes_and_exact_second_mode_acks() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "owned-pair",
            ReceiverSettleMode::Second,
            None,
        )
        .await;
    let origin = sender.sender_identity();
    fixture.peer.flow(CHANNEL, HANDLE, 0, 2, false).await;
    let first_claim = bounded(sender.reserve_send())
        .await
        .expect("first slot")
        .try_claim()
        .expect("first claim");
    let second_claim = bounded(sender.reserve_send())
        .await
        .expect("second slot")
        .try_claim()
        .expect("second claim");
    let first_message = distinct_message(b"first owned original");
    let second_message = distinct_message(b"second owned original");
    let mut first = Box::pin(owned({
        let borrowed = &sender;
        borrowed.send_reserved_with_settlement_owned(
            first_claim,
            first_message.clone(),
            b"first".to_vec().into(),
        )
    }));
    let mut second = Box::pin(owned(sender.send_reserved_with_settlement_owned(
        second_claim,
        second_message.clone(),
        b"second".to_vec().into(),
    )));
    {
        let detaching = sender.on_detach();
        tokio::pin!(detaching);
        pending(detaching.as_mut()).await;
    }
    let mut another = Box::pin(sender.reserve_send());
    pending(another.as_mut()).await;
    drop(another);
    pending(first.as_mut()).await;
    pending(second.as_mut()).await;
    let first_transfer = fixture
        .peer
        .transfer_message(CHANNEL, local, b"first", &first_message)
        .await;
    let second_transfer = fixture
        .peer
        .transfer_message(CHANNEL, local, b"second", &second_message)
        .await;
    assert_eq!(first_transfer.delivery_id, Some(0));
    assert_eq!(second_transfer.delivery_id, Some(1));
    fixture.peer.barrier(CHANNEL).await;
    pending(first.as_mut()).await;
    pending(second.as_mut()).await;

    fixture
        .peer
        .outcome(CHANNEL, 1, false, DeliveryState::Released(Released))
        .await;
    let second_receipt = bounded(second).await.expect("second peer outcome first");
    pending(first.as_mut()).await;
    fixture.peer.accepted(CHANNEL, 0, false).await;
    let first_receipt = bounded(first).await.expect("first peer outcome last");
    assert_eq!(first_receipt.outcome(), &Outcome::Accepted(Accepted));
    assert_eq!(second_receipt.outcome(), &Outcome::Released(Released));
    assert!(first_receipt.belongs_to_sender(&origin));
    assert!(second_receipt.belongs_to_sender(&origin));
    let first_original = first_receipt.delivery_identity().clone();
    let second_original = second_receipt.delivery_identity().clone();
    assert!(!first_original.same_delivery(&second_original));
    assert!(first_original.belongs_to_sender(&origin));
    assert!(second_original.belongs_to_sender(&origin));
    acknowledge(&mut fixture, &second_receipt, 1).await;
    acknowledge(&mut fixture, &first_receipt, 0).await;
    assert!(first_original.same_delivery(first_receipt.delivery_identity()));
    assert!(second_original.same_delivery(second_receipt.delivery_identity()));
    assert_eq!(
        fixture
            .peer
            .flow(CHANNEL, HANDLE, 2, 0, false)
            .await
            .delivery_count,
        Some(2)
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_owned_unsettled_result_stays_pending_after_full_transfer_flush() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "outcome-not-flush",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("slot")
        .try_claim()
        .expect("claim");
    let mut sending = Box::pin(sender.send_reserved_with_settlement_owned(
        claimed,
        message(),
        b"unsettled".to_vec().into(),
    ));
    pending(sending.as_mut()).await;
    let transfer = fixture.peer.transfer(CHANNEL, local, b"unsettled").await;
    assert_eq!(transfer.delivery_id, Some(0));
    assert!(!transfer.more);
    fixture.peer.barrier(CHANNEL).await;
    pending(sending.as_mut()).await;
    fixture.peer.accepted(CHANNEL, 0, true).await;
    let receipt = bounded(sending)
        .await
        .expect("actual terminal peer outcome");
    assert_eq!(receipt.outcome(), &Outcome::Accepted(Accepted));
    bounded(receipt.accept())
        .await
        .expect("first-mode local completion");
    fixture.peer.barrier(CHANNEL).await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn dropping_an_unpolled_owned_packet_refunds_before_any_native_enqueue() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "unpolled-packet",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("slot")
        .try_claim()
        .expect("claim");
    let packet = owned({
        let borrowed = &sender;
        borrowed.send_reserved_with_settlement_owned(claimed, message(), b"unused".to_vec().into())
    });
    drop(packet);
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    assert_eq!(snapshot.delivery_count, Some(0));
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("unpolled refund")
        .try_claim()
        .expect("fresh claim");
    let mut sending = Box::pin(sender.send_reserved_with_settlement_owned(
        claimed,
        message(),
        b"unused".to_vec().into(),
    ));
    pending(sending.as_mut()).await;
    assert_eq!(
        fixture
            .peer
            .transfer(CHANNEL, local, b"unused")
            .await
            .delivery_id,
        Some(0)
    );
    fixture.peer.accepted(CHANNEL, 0, true).await;
    assert!(bounded(sending).await.is_ok());
    fixture.shutdown().await;
}

#[tokio::test]
async fn dropping_a_queued_owned_request_before_actor_consumption_refunds_the_slot() {
    let (mut fixture, gate) = Fixture::gated().await;
    let mut session = fixture.session(CHANNEL).await;
    let (sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "queued-packet",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("slot")
        .try_claim()
        .expect("claim");
    gate.arm(fixture.peer.local_channel(CHANNEL));
    fixture.peer.barrier(CHANNEL).await;
    gate.wait().await;
    let mut canceled = Box::pin(sender.send_reserved_with_settlement_owned(
        claimed,
        message(),
        b"queued-cancel".to_vec().into(),
    ));
    pending(canceled.as_mut()).await;
    drop(canceled);
    let mut replacement = Box::pin(sender.reserve_send());
    pending(replacement.as_mut()).await;
    gate.release();
    let claimed = bounded(replacement)
        .await
        .expect("queued packet refunded")
        .try_claim()
        .expect("replacement claim");
    assert_eq!(
        fixture
            .peer
            .flow(CHANNEL, HANDLE, 0, 1, false)
            .await
            .delivery_count,
        Some(0)
    );
    let mut sending = Box::pin(sender.send_reserved_with_settlement_owned(
        claimed,
        message(),
        b"queued-cancel".to_vec().into(),
    ));
    pending(sending.as_mut()).await;
    assert_eq!(
        fixture
            .peer
            .transfer(CHANNEL, local, b"queued-cancel")
            .await
            .delivery_id,
        Some(0)
    );
    fixture.peer.accepted(CHANNEL, 0, true).await;
    assert!(bounded(sending).await.is_ok());
    fixture.shutdown().await;
}

#[tokio::test]
async fn dropping_an_actor_admitted_owned_send_does_not_undo_its_queued_original() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "admitted-packet",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    fixture
        .peer
        .grant_with_window(CHANNEL, HANDLE, 0, 2, false, true, 0)
        .await;
    assert_eq!(
        fixture.peer.link_flow(CHANNEL).await.delivery_count,
        Some(0)
    );
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("first slot")
        .try_claim()
        .expect("first claim");
    let mut admitted = Box::pin(sender.send_reserved_with_settlement_owned(
        claimed,
        message(),
        b"admitted".to_vec().into(),
    ));
    pending(admitted.as_mut()).await;
    // The later command's reply proves the preceding SendReserved was processed
    // while the closed session window still prevented its first Transfer.
    let next = bounded(sender.reserve_send())
        .await
        .expect("later command fence");
    drop(admitted);
    drop(next);
    fixture
        .peer
        .grant_with_window(CHANNEL, HANDLE, 0, 1, false, true, 0)
        .await;
    assert_eq!(
        fixture.peer.link_flow(CHANNEL).await.delivery_count,
        Some(0)
    );
    fixture
        .peer
        .grant(CHANNEL, HANDLE, 0, 1, false, false)
        .await;
    assert_eq!(
        fixture
            .peer
            .transfer(CHANNEL, local, b"admitted")
            .await
            .delivery_id,
        Some(0)
    );
    fixture.peer.flow(CHANNEL, HANDLE, 1, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("second local slot")
        .try_claim()
        .expect("second local claim");
    assert!(matches!(
        bounded(sender.send_reserved_with_settlement_owned(
            claimed,
            message(),
            b"admitted".to_vec().into()
        ))
        .await,
        Err(EngineError::InvalidState(_))
    ));
    fixture.peer.accepted(CHANNEL, 0, true).await;
    fixture.peer.barrier(CHANNEL).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("released original tag")
        .try_claim()
        .expect("healthy next claim");
    let mut sending = Box::pin(sender.send_reserved_with_settlement_owned(
        claimed,
        message(),
        b"admitted".to_vec().into(),
    ));
    pending(sending.as_mut()).await;
    assert_eq!(
        fixture
            .peer
            .transfer(CHANNEL, local, b"admitted")
            .await
            .delivery_id,
        Some(1)
    );
    fixture.peer.accepted(CHANNEL, 1, true).await;
    assert!(bounded(sending).await.is_ok());
    fixture.shutdown().await;
}

#[tokio::test]
async fn dropping_an_unseen_owned_outcome_does_not_release_a_second_mode_alias() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "unseen-outcome",
            ReceiverSettleMode::Second,
            None,
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("slot")
        .try_claim()
        .expect("claim");
    let mut unseen = Box::pin(sender.send_reserved_with_settlement_owned(
        claimed,
        message(),
        b"unseen".to_vec().into(),
    ));
    let reply = ReplyWake::poll_pending(unseen.as_mut());
    assert_eq!(
        fixture
            .peer
            .transfer(CHANNEL, local, b"unseen")
            .await
            .delivery_id,
        Some(0)
    );
    fixture.peer.accepted(CHANNEL, 0, false).await;
    reply.wait().await;
    drop(unseen);
    fixture.peer.flow(CHANNEL, HANDLE, 1, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("new credit")
        .try_claim()
        .expect("new claim");
    assert!(matches!(
        bounded(sender.send_reserved_with_settlement_owned(
            claimed,
            message(),
            b"unseen".to_vec().into()
        ))
        .await,
        Err(EngineError::InvalidState(_))
    ));
    fixture.peer.accepted(CHANNEL, 0, true).await;
    fixture.peer.barrier(CHANNEL).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("exact remote settlement released alias")
        .try_claim()
        .expect("next claim");
    let mut sending = Box::pin(sender.send_reserved_with_settlement_owned(
        claimed,
        message(),
        b"unseen".to_vec().into(),
    ));
    pending(sending.as_mut()).await;
    assert_eq!(
        fixture
            .peer
            .transfer(CHANNEL, local, b"unseen")
            .await
            .delivery_id,
        Some(1)
    );
    fixture.peer.accepted(CHANNEL, 1, false).await;
    let receipt = bounded(sending).await.expect("healthy new original");
    acknowledge(&mut fixture, &receipt, 1).await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_owned_packet_cannot_revive_a_reused_link_or_session_origin() {
    for retire_session in [false, true] {
        let mut fixture = Fixture::new().await;
        let mut session = fixture.session(CHANNEL).await;
        let (old, old_handle) = fixture
            .sender(
                &mut session,
                CHANNEL,
                HANDLE,
                "same-owned-link",
                ReceiverSettleMode::First,
                None,
            )
            .await;
        let old_origin = old.sender_identity();
        fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
        let claimed = bounded(old.reserve_send())
            .await
            .expect("old slot")
            .try_claim()
            .expect("old claim");
        let old_packet = owned(old.send_reserved_with_settlement_owned(
            claimed,
            message(),
            b"reused".to_vec().into(),
        ));
        if retire_session {
            fixture.peer.end(CHANNEL).await;
            session = fixture.session(CHANNEL).await;
        } else {
            fixture.peer.detach(CHANNEL, HANDLE, old_handle).await;
        }
        let (replacement, local) = fixture
            .sender(
                &mut session,
                CHANNEL,
                HANDLE,
                "same-owned-link",
                ReceiverSettleMode::First,
                None,
            )
            .await;
        assert_eq!(old_handle, local);
        assert!(!old_origin.is_active());
        assert!(!old_origin.same_sender(&replacement.sender_identity()));
        fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
        assert!(matches!(
            bounded(old_packet).await,
            Err(EngineError::SendReservationRevoked)
        ));
        assert_eq!(
            fixture
                .peer
                .flow(CHANNEL, HANDLE, 0, 1, false)
                .await
                .delivery_count,
            Some(0)
        );
        let claimed = bounded(replacement.reserve_send())
            .await
            .expect("replacement credit")
            .try_claim()
            .expect("replacement claim");
        let mut sending = Box::pin(replacement.send_reserved_with_settlement_owned(
            claimed,
            message(),
            b"reused".to_vec().into(),
        ));
        pending(sending.as_mut()).await;
        assert_eq!(
            fixture
                .peer
                .transfer(CHANNEL, local, b"reused")
                .await
                .delivery_id,
            Some(0)
        );
        fixture.peer.accepted(CHANNEL, 0, true).await;
        assert!(bounded(sending).await.is_ok());
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn an_owned_future_does_not_keep_a_shutdown_connection_active() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (sender, _) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "closed-owned",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("slot")
        .try_claim()
        .expect("claim");
    let packet =
        sender.send_reserved_with_settlement_owned(claimed, message(), b"closed".to_vec().into());
    let connection = fixture.connection.connection_identity().clone();
    let origin = sender.sender_identity();
    fixture.shutdown().await;
    assert!(!connection.is_active());
    assert!(!origin.is_active());
    assert!(matches!(
        bounded(packet).await,
        Err(EngineError::SendReservationRevoked)
    ));
}
