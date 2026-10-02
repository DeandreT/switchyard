use amqp::{Accepted, EngineError, Outcome, ReceiverSettleMode};

use super::fixture::{CHANNEL, Fixture, HANDLE, bounded, message, pending};

#[tokio::test]
async fn reservation_waits_for_actual_peer_credit_without_advancing_counters() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "waiting",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    let reservation = sender.reserve_send();
    tokio::pin!(reservation);
    pending(reservation.as_mut()).await;
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 0, 0, false).await;
    assert_eq!(snapshot.handle, Some(local));
    assert_eq!(snapshot.delivery_count, Some(0));
    assert_eq!(snapshot.link_credit, Some(0));
    pending(reservation.as_mut()).await;

    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    assert_eq!(snapshot.delivery_count, Some(0));
    let claimed = bounded(reservation)
        .await
        .expect("credited reservation")
        .try_claim()
        .expect("unique local claim");
    let responding = async {
        let transfer = fixture.peer.transfer(CHANNEL, local, b"first").await;
        assert_eq!(transfer.delivery_id, Some(0));
        fixture.peer.accepted(CHANNEL, 0, true).await;
    };
    let (outcome, ()) = bounded(async {
        tokio::join!(
            sender.send_reserved(claimed, message(), b"first".to_vec().into()),
            responding
        )
    })
    .await;
    assert_eq!(outcome.expect("reserved send"), Outcome::Accepted(Accepted));
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 1, 0, false).await;
    assert_eq!(snapshot.delivery_count, Some(1));
    fixture.shutdown().await;
}

#[tokio::test]
async fn reservations_are_bounded_by_credit_and_replayed_flow_mints_no_extra_slot() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "bounded",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 3, false).await;
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(
            bounded(sender.reserve_send())
                .await
                .expect("available slot"),
        );
    }
    let extra = sender.reserve_send();
    tokio::pin!(extra);
    pending(extra.as_mut()).await;
    for _ in 0..2 {
        let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 0, 3, false).await;
        assert_eq!(snapshot.delivery_count, Some(0));
        pending(extra.as_mut()).await;
    }
    drop(reservations.remove(1));
    reservations.push(bounded(extra).await.expect("one refunded slot"));
    for (id, reservation) in reservations.into_iter().enumerate() {
        let id = u32::try_from(id).expect("small delivery index");
        let tag = format!("reserved-{id}").into_bytes();
        let claimed = reservation.try_claim().expect("separate unique claim");
        let responding = async {
            let transfer = fixture.peer.transfer(CHANNEL, local, &tag).await;
            assert_eq!(transfer.delivery_id, Some(id));
            fixture.peer.accepted(CHANNEL, id, true).await;
        };
        let (result, ()) = bounded(async {
            tokio::join!(
                sender.send_reserved(claimed, message(), tag.clone().into()),
                responding
            )
        })
        .await;
        assert_eq!(
            result.expect("reserved outcome"),
            Outcome::Accepted(Accepted)
        );
    }
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 3, 0, false).await;
    assert_eq!(snapshot.delivery_count, Some(3));
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_ordinary_send_cannot_take_a_reserved_link_credit_slot() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "ordinary",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    let reservation = bounded(sender.reserve_send()).await.expect("held slot");
    let sending = sender.send(message(), b"ordinary".to_vec().into());
    tokio::pin!(sending);
    pending(sending.as_mut()).await;
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    assert_eq!(snapshot.delivery_count, Some(0));
    pending(sending.as_mut()).await;
    drop(reservation);
    let transfer = fixture.peer.transfer(CHANNEL, local, b"ordinary").await;
    assert_eq!(transfer.delivery_id, Some(0));
    fixture.peer.accepted(CHANNEL, 0, true).await;
    assert_eq!(
        bounded(sending).await.expect("ordinary outcome"),
        Outcome::Accepted(Accepted)
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_admitted_reserved_row_keeps_its_credit_ahead_of_an_older_ordinary_row() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "queued-priority",
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
        .expect("reserved row admission")
        .try_claim()
        .expect("claimed reserved row");
    let spare = bounded(sender.reserve_send())
        .await
        .expect("remaining protected slot");

    // Native commands are FIFO; dropping the ordinary outcome waiter does not
    // remove its already-enqueued send. The closed window prevents either row
    // from starting before the later credit-reduction frame is processed.
    let mut ordinary = Box::pin(sender.send(message(), b"older".to_vec().into()));
    pending(ordinary.as_mut()).await;
    fixture.peer.window(CHANNEL, 0).await;
    drop(ordinary);
    let mut reserved =
        Box::pin(sender.send_reserved(claimed, message(), b"reserved-first".to_vec().into()));
    pending(reserved.as_mut()).await;
    fixture
        .peer
        .grant_with_window(CHANNEL, HANDLE, 0, 1, false, true, 0)
        .await;
    assert_eq!(
        fixture.peer.link_flow(CHANNEL).await.delivery_count,
        Some(0)
    );
    assert!(matches!(
        spare.try_claim(),
        Err(EngineError::SendReservationRevoked)
    ));
    fixture
        .peer
        .grant(CHANNEL, HANDLE, 0, 1, false, false)
        .await;
    let first = fixture
        .peer
        .transfer(CHANNEL, local, b"reserved-first")
        .await;
    assert_eq!(first.delivery_id, Some(0));
    fixture.peer.accepted(CHANNEL, 0, true).await;
    assert_eq!(
        bounded(reserved.as_mut())
            .await
            .expect("reserved first outcome"),
        Outcome::Accepted(Accepted)
    );
    drop(reserved);
    let exhausted = fixture.peer.flow(CHANNEL, HANDLE, 1, 0, false).await;
    assert_eq!(exhausted.delivery_count, Some(1));

    fixture
        .peer
        .grant(CHANNEL, HANDLE, 1, 1, false, false)
        .await;
    let second = fixture.peer.transfer(CHANNEL, local, b"older").await;
    assert_eq!(second.delivery_id, Some(1));
    fixture.peer.accepted(CHANNEL, 1, true).await;
    fixture.peer.barrier(CHANNEL).await;
    fixture.peer.flow(CHANNEL, HANDLE, 2, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("released legacy tag capacity")
        .try_claim()
        .expect("healthy third claim");
    let responding = async {
        let transfer = fixture.peer.transfer(CHANNEL, local, b"older").await;
        assert_eq!(transfer.delivery_id, Some(2));
        fixture.peer.accepted(CHANNEL, 2, true).await;
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            sender.send_reserved(claimed, message(), b"older".to_vec().into()),
            responding
        )
    })
    .await;
    assert!(result.is_ok());
    assert_eq!(
        fixture
            .peer
            .flow(CHANNEL, HANDLE, 3, 0, false)
            .await
            .delivery_count,
        Some(3)
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn credit_retraction_revokes_unclaimed_slots_before_payload_admission() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "revoked",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 2, false).await;
    let first = bounded(sender.reserve_send()).await.expect("first slot");
    let second = bounded(sender.reserve_send()).await.expect("second slot");
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 0, 0, false).await;
    assert_eq!(snapshot.delivery_count, Some(0));
    assert!(matches!(
        first.try_claim(),
        Err(EngineError::SendReservationRevoked)
    ));
    assert!(matches!(
        second.try_claim(),
        Err(EngineError::SendReservationRevoked)
    ));
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("fresh credit")
        .try_claim()
        .expect("fresh claim");
    let responding = async {
        let transfer = fixture.peer.transfer(CHANNEL, local, b"fresh").await;
        assert_eq!(transfer.delivery_id, Some(0));
        fixture.peer.accepted(CHANNEL, 0, true).await;
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            sender.send_reserved(claimed, message(), b"fresh".to_vec().into()),
            responding
        )
    })
    .await;
    assert!(result.is_ok());
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_claim_is_local_admission_not_permission_to_ignore_current_wire_credit() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "claimed",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("reservation")
        .try_claim()
        .expect("claim before retract");
    fixture.peer.flow(CHANNEL, HANDLE, 0, 0, false).await;
    let sending = sender.send_reserved(claimed, message(), b"regranted".to_vec().into());
    tokio::pin!(sending);
    pending(sending.as_mut()).await;
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 0, 0, false).await;
    assert_eq!(snapshot.delivery_count, Some(0));
    pending(sending.as_mut()).await;
    fixture
        .peer
        .grant(CHANNEL, HANDLE, 0, 1, false, false)
        .await;
    let transfer = fixture.peer.transfer(CHANNEL, local, b"regranted").await;
    assert_eq!(transfer.delivery_id, Some(0));
    fixture.peer.accepted(CHANNEL, 0, true).await;
    assert!(bounded(sending).await.is_ok());
    fixture.shutdown().await;
}

#[tokio::test]
async fn dropping_an_empty_reservation_unblocks_exact_drain_completion() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "drain",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("reservation")
        .try_claim()
        .expect("empty local admission");
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 0, 1, true).await;
    assert_eq!(snapshot.delivery_count, Some(0));
    assert_eq!(snapshot.link_credit, Some(1));
    assert!(snapshot.drain);
    drop(claimed);
    let completed = fixture.peer.link_flow(CHANNEL).await;
    assert_eq!(completed.handle, Some(local));
    assert_eq!(completed.delivery_count, Some(1));
    assert_eq!(completed.link_credit, Some(0));
    assert!(completed.drain);
    fixture.peer.flow(CHANNEL, HANDLE, 1, 1, false).await;
    let claimed = bounded(sender.reserve_send())
        .await
        .expect("post-drain reservation")
        .try_claim()
        .expect("post-drain claim");
    let responding = async {
        let transfer = fixture.peer.transfer(CHANNEL, local, b"after-drain").await;
        assert_eq!(transfer.delivery_id, Some(0));
        fixture.peer.accepted(CHANNEL, 0, true).await;
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            sender.send_reserved(claimed, message(), b"after-drain".to_vec().into()),
            responding
        )
    })
    .await;
    assert!(result.is_ok());
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 2, 0, false).await;
    assert_eq!(snapshot.delivery_count, Some(2));
    fixture.shutdown().await;
}
