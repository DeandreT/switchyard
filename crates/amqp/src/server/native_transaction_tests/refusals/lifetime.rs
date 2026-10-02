use std::{
    future::{Future, poll_fn},
    task::Poll,
};

use super::*;

#[tokio::test]
async fn unpolled_refusal_futures_fail_closed_without_a_wire_decision() {
    for declare in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = controller(
            &mut fixture,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            ReceiverSettleMode::First,
            true,
        )
        .await;
        let _healthy = fixture.session(HEALTHY_CHANNEL).await;
        if declare {
            let receipt = declare_receipt(
                &mut fixture,
                &mut coordinator,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                0,
            )
            .await;
            drop(receipt.refuse(NativeDeclarationRefusal::Unavailable));
            assert!(!coordinator.controller_identity().is_active());
        } else {
            let transaction = id(122);
            let observer = registered(
                &mut fixture,
                &mut coordinator,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                0,
                &transaction,
                ReceiverSettleMode::First,
            )
            .await;
            let sealed = seal(
                &mut fixture,
                &mut coordinator,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                1,
                &transaction,
            )
            .await;
            drop(sealed.refuse_staging());
            assert_eq!(observer.state(), NativeTransactionState::Faulted);
        }
        fixture.peer.barrier(HEALTHY_CHANNEL).await;
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn canceled_negative_waiter_does_not_cancel_the_actor_owned_refusal() {
    for declare in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = controller(
            &mut fixture,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            ReceiverSettleMode::First,
            true,
        )
        .await;
        let transaction = id(123);
        let observer = registered(
            &mut fixture,
            &mut coordinator,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            0,
            &transaction,
            ReceiverSettleMode::First,
        )
        .await;
        fixture
            .gate
            .block_refusal(CONTROL_CHANNEL, 1, CONTROL_HANDLE, true);
        if declare {
            let receipt = declare_receipt(
                &mut fixture,
                &mut coordinator,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                1,
            )
            .await;
            let mut refusing = Box::pin(receipt.refuse(NativeDeclarationRefusal::Unavailable));
            poll_fn(|cx| {
                assert!(refusing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            fixture.gate.wait().await;
            negative(
                &mut fixture,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                1,
                ReceiverSettleMode::First,
                true,
                "native transaction declaration is unavailable",
            )
            .await;
            drop(refusing);
            assert_eq!(observer.state(), NativeTransactionState::Pending);
        } else {
            let sealed = seal(
                &mut fixture,
                &mut coordinator,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                1,
                &transaction,
            )
            .await;
            let mut refusing = Box::pin(sealed.refuse_staging());
            poll_fn(|cx| {
                assert!(refusing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            fixture.gate.wait().await;
            negative(
                &mut fixture,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                1,
                ReceiverSettleMode::First,
                true,
                STAGING_DESCRIPTION,
            )
            .await;
            assert_eq!(observer.state(), NativeTransactionState::Aborted);
            drop(refusing);
        }
        fixture.gate.unblock();
        fixture.peer.barrier(CONTROL_CHANNEL).await;
        assert!(
            coordinator.controller_identity().is_active(),
            "canceled reply is not a new controller close"
        );
        let healthy_id = id(124);
        registered(
            &mut fixture,
            &mut coordinator,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            1,
            &healthy_id,
            ReceiverSettleMode::First,
        )
        .await;
        empty_commit(
            &mut fixture,
            &mut coordinator,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            2,
            &healthy_id,
            ReceiverSettleMode::First,
        )
        .await;
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn negative_flush_failure_is_not_a_successful_declaration_or_commit() {
    for (declare, rejected) in [(true, true), (false, true), (false, false)] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = controller(
            &mut fixture,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            ReceiverSettleMode::First,
            rejected,
        )
        .await;
        let transaction = id(125);
        let observer = registered(
            &mut fixture,
            &mut coordinator,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            0,
            &transaction,
            ReceiverSettleMode::First,
        )
        .await;
        fixture
            .gate
            .fail_refusal(CONTROL_CHANNEL, 1, CONTROL_HANDLE, rejected);
        let result = if declare {
            let receipt = declare_receipt(
                &mut fixture,
                &mut coordinator,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                1,
            )
            .await;
            let (result, ()) = tokio::join!(
                receipt.refuse(NativeDeclarationRefusal::Unavailable),
                negative(
                    &mut fixture,
                    CONTROL_CHANNEL,
                    CONTROL_HANDLE,
                    1,
                    ReceiverSettleMode::First,
                    rejected,
                    "native transaction declaration is unavailable"
                )
            );
            result
        } else {
            let sealed = seal(
                &mut fixture,
                &mut coordinator,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                1,
                &transaction,
            )
            .await;
            let (result, ()) = tokio::join!(
                sealed.refuse_staging(),
                negative(
                    &mut fixture,
                    CONTROL_CHANNEL,
                    CONTROL_HANDLE,
                    1,
                    ReceiverSettleMode::First,
                    rejected,
                    STAGING_DESCRIPTION
                )
            );
            result
        };
        assert!(
            result.is_err(),
            "accepted bytes are not a successful negative flush"
        );
        bounded("failed actor cleanup", fixture.connection.shutdown()).await;
        assert!(!fixture.connection.connection_identity().is_active());
        assert_ne!(observer.state(), NativeTransactionState::Committed);
    }
}

#[tokio::test]
async fn retired_or_sender_settled_control_refusal_never_acknowledges_a_replacement() {
    for retired in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = controller(
            &mut fixture,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            ReceiverSettleMode::First,
            true,
        )
        .await;
        let _healthy = fixture.session(HEALTHY_CHANNEL).await;
        let receipt = declare_receipt(
            &mut fixture,
            &mut coordinator,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            0,
        )
        .await;
        if retired {
            fixture.peer.detach(CONTROL_CHANNEL, CONTROL_HANDLE).await;
        } else {
            sender_ack(&mut fixture, CONTROL_CHANNEL, 0).await;
        }
        let (result, ()) = tokio::join!(
            receipt.refuse(NativeDeclarationRefusal::Unavailable),
            fixture.peer.barrier(HEALTHY_CHANNEL)
        );
        assert!(result.is_err());
        assert!(fixture.connection.connection_identity().is_active());
        let (_other, mut other) = controller(
            &mut fixture,
            OTHER_CHANNEL,
            OTHER_HANDLE,
            ReceiverSettleMode::First,
            true,
        )
        .await;
        let transaction = id(126);
        registered(
            &mut fixture,
            &mut other,
            OTHER_CHANNEL,
            OTHER_HANDLE,
            0,
            &transaction,
            ReceiverSettleMode::First,
        )
        .await;
        empty_commit(
            &mut fixture,
            &mut other,
            OTHER_CHANNEL,
            OTHER_HANDLE,
            1,
            &transaction,
            ReceiverSettleMode::First,
        )
        .await;
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn stale_staging_control_faults_pending_authority_without_post_cleanup() {
    for retired in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = controller(
            &mut fixture,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            ReceiverSettleMode::First,
            true,
        )
        .await;
        let (_data, mut receiver) = fixture.receiver().await;
        let _healthy = fixture.session(HEALTHY_CHANNEL).await;
        let transaction = id(127);
        let observer = registered(
            &mut fixture,
            &mut coordinator,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            0,
            &transaction,
            ReceiverSettleMode::First,
        )
        .await;
        let message = Message::data(b"pending post must not be cleaned by stale control".to_vec());
        let held = posting(&mut fixture, &mut receiver, &transaction, 0, &message).await;
        let sealed = seal(
            &mut fixture,
            &mut coordinator,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            1,
            &transaction,
        )
        .await;
        if retired {
            fixture.peer.detach(CONTROL_CHANNEL, CONTROL_HANDLE).await;
        } else {
            sender_ack(&mut fixture, CONTROL_CHANNEL, 1).await;
        }
        let (result, ()) = tokio::join!(
            sealed.refuse_staging(),
            fixture.peer.barrier(HEALTHY_CHANNEL)
        );
        assert!(result.is_err());
        assert_eq!(
            observer.state(),
            NativeTransactionState::Faulted,
            "consumed stale receipt still drops fail-closed"
        );
        assert_eq!(held.message(), &message);
        ordinary_reuse(&mut fixture, &mut receiver, 1, ReceiverSettleMode::First).await;
        assert!(fixture.connection.connection_identity().is_active());
        fixture.connection.shutdown().await;
    }
}
