use amqp::{EngineError, ReceiverSettleMode};

use super::fixture::{CHANNEL, Fixture, HANDLE, OTHER_CHANNEL, bounded, message, pending};

#[tokio::test]
async fn a_foreign_sender_claim_is_refused_before_message_encoding() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (mut original, original_handle) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "original",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    let (mut foreign, foreign_handle) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE + 1,
            "foreign",
            ReceiverSettleMode::First,
            Some(1),
        )
        .await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    fixture.peer.flow(CHANNEL, HANDLE + 1, 0, 1, false).await;
    let claimed = bounded(original.reserve_send())
        .await
        .expect("original reservation")
        .try_claim()
        .expect("original claim");
    assert!(matches!(
        bounded(foreign.send_reserved(claimed, message(), b"wrong".to_vec().into())).await,
        Err(EngineError::SendReservationRevoked)
    ));
    let snapshot = fixture.peer.flow(CHANNEL, HANDLE + 1, 0, 1, false).await;
    assert_eq!(snapshot.delivery_count, Some(0));
    let foreign_claim = bounded(foreign.reserve_send())
        .await
        .expect("foreign own credit")
        .try_claim()
        .expect("foreign own claim");
    assert!(matches!(
        bounded(foreign.send_reserved(foreign_claim, message(), b"oversized".to_vec().into()))
            .await,
        Err(EngineError::MessageSizeExceeded {
            maximum_bytes: 1,
            ..
        })
    ));
    fixture
        .peer
        .oversized_detach(CHANNEL, HANDLE + 1, foreign_handle)
        .await;
    let claimed = bounded(original.reserve_send())
        .await
        .expect("foreign rejection refunded original")
        .try_claim()
        .expect("original replacement claim");
    let responding = async {
        let transfer = fixture
            .peer
            .transfer(CHANNEL, original_handle, b"healthy")
            .await;
        assert_eq!(transfer.delivery_id, Some(0));
        fixture.peer.accepted(CHANNEL, 0, true).await;
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            original.send_reserved(claimed, message(), b"healthy".to_vec().into()),
            responding
        )
    })
    .await;
    assert!(result.is_ok());
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_claim_cannot_cross_sessions_even_when_both_local_handles_are_zero() {
    let mut fixture = Fixture::new().await;
    let mut first = fixture.session(CHANNEL).await;
    let mut second = fixture.session(OTHER_CHANNEL).await;
    let (original, old_handle) = fixture
        .sender(
            &mut first,
            CHANNEL,
            HANDLE,
            "first-session",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    let (mut foreign, foreign_handle) = fixture
        .sender(
            &mut second,
            OTHER_CHANNEL,
            HANDLE,
            "second-session",
            ReceiverSettleMode::First,
            Some(1),
        )
        .await;
    assert_eq!(old_handle, foreign_handle);
    fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
    fixture.peer.flow(OTHER_CHANNEL, HANDLE, 0, 1, false).await;
    let claimed = bounded(original.reserve_send())
        .await
        .expect("first-session reservation")
        .try_claim()
        .expect("first-session claim");
    assert!(matches!(
        bounded(foreign.send_reserved(claimed, message(), b"cross-session".to_vec().into())).await,
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
    assert_eq!(
        fixture
            .peer
            .flow(OTHER_CHANNEL, HANDLE, 0, 1, false)
            .await
            .delivery_count,
        Some(0)
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn reused_link_and_session_aliases_do_not_revive_a_retired_claim() {
    for retire_session in [false, true] {
        let mut fixture = Fixture::new().await;
        let mut session = fixture.session(CHANNEL).await;
        let (old, old_handle) = fixture
            .sender(
                &mut session,
                CHANNEL,
                HANDLE,
                "same-link",
                ReceiverSettleMode::First,
                None,
            )
            .await;
        let origin = old.sender_identity();
        fixture.peer.flow(CHANNEL, HANDLE, 0, 2, false).await;
        let unclaimed = bounded(old.reserve_send())
            .await
            .expect("old unclaimed slot");
        let claimed = bounded(old.reserve_send())
            .await
            .expect("old claimed slot")
            .try_claim()
            .expect("old claim");
        if retire_session {
            fixture.peer.end(CHANNEL).await;
            session = fixture.session(CHANNEL).await;
        } else {
            fixture.peer.detach(CHANNEL, HANDLE, old_handle).await;
        }
        assert!(!origin.is_active());
        assert!(matches!(
            unclaimed.try_claim(),
            Err(EngineError::SendReservationRevoked)
        ));
        let (mut replacement, replacement_handle) = fixture
            .sender(
                &mut session,
                CHANNEL,
                HANDLE,
                "same-link",
                ReceiverSettleMode::First,
                Some(1),
            )
            .await;
        assert_eq!(old_handle, replacement_handle);
        assert!(!origin.same_sender(&replacement.sender_identity()));
        fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
        assert!(matches!(
            bounded(replacement.send_reserved(claimed, message(), b"stale".to_vec().into())).await,
            Err(EngineError::SendReservationRevoked)
        ));
        let snapshot = fixture.peer.flow(CHANNEL, HANDLE, 0, 1, false).await;
        assert_eq!(snapshot.delivery_count, Some(0));
        let own = bounded(replacement.reserve_send())
            .await
            .expect("replacement slot")
            .try_claim()
            .expect("replacement claim");
        assert!(matches!(
            bounded(replacement.send_reserved(own, message(), b"own-size".to_vec().into())).await,
            Err(EngineError::MessageSizeExceeded {
                maximum_bytes: 1,
                ..
            })
        ));
        fixture
            .peer
            .oversized_detach(CHANNEL, HANDLE, replacement_handle)
            .await;
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn exact_link_retirement_finishes_a_waiting_reservation_without_payload_or_transfer() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let (sender, local) = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "retired-waiter",
            ReceiverSettleMode::First,
            None,
        )
        .await;
    let origin = sender.sender_identity();
    let waiting = sender.reserve_send();
    tokio::pin!(waiting);
    pending(waiting.as_mut()).await;
    fixture.peer.flow(CHANNEL, HANDLE, 0, 0, false).await;
    fixture.peer.detach(CHANNEL, HANDLE, local).await;
    assert!(!origin.is_active());
    assert!(matches!(
        bounded(waiting).await,
        Err(EngineError::RemoteDetached)
    ));
    fixture.peer.barrier(CHANNEL).await;
    fixture.shutdown().await;
}
