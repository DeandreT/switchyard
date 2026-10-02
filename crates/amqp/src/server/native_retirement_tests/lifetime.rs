use std::future::{Future, poll_fn};
use std::task::Poll;

use super::*;

async fn ordinary_finish(fixture: &mut Fixture, sent: &mut Sent) {
    fixture
        .peer
        .outcome(
            SEND,
            sent.id,
            None,
            DeliveryState::Accepted(Accepted),
            false,
        )
        .await;
    let TransactionalDisposition::Ordinary(receipt) = bounded(
        "rearmed original ordinary outcome",
        sent.delivery.next_disposition(),
    )
    .await
    .expect("live original inbox") else {
        panic!("ordinary outcome after retirement rollback");
    };
    assert!(
        receipt
            .delivery_identity()
            .same_delivery(sent.delivery.delivery_identity())
    );
    let (result, disposition) = bounded("ordinary exact-original ACK", async {
        tokio::join!(
            receipt.accept(),
            fixture.peer.disposition(SEND, Role::Sender, sent.id)
        )
    })
    .await;
    result.expect("ordinary ACK flushed");
    assert!(disposition.settled);
    assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
}

#[tokio::test]
async fn controller_close_reconciles_queued_and_prepared_receipts_before_late_attempt_cleanup() {
    for receipt_stage in 0..3 {
        let mut fixture = Fixture::new().await;
        let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
        let mut session = fixture.session(SEND).await;
        let (mut sender, handle) = fixture
            .sender(&mut session, SEND, HANDLE, "retirement-sender")
            .await;
        let first_id = txn(8);
        let first_observer = fixture.declare(&mut coordinator, CONTROL, &first_id).await;
        let mut sent = fixture.send(&mut sender, SEND, handle).await;
        let original = sent.delivery.delivery_identity().clone();
        let mut held_receipt = None;
        let mut held_prepared = None;
        if receipt_stage == 0 {
            fixture.peer.retirement(SEND, sent.id, &first_id).await;
            fixture.peer.barrier(SEND).await;
        } else {
            let first = fixture.retirement(&mut sent, &first_id).await;
            if receipt_stage == 2 {
                held_prepared = Some(fixture.provisional(first, &first_id, sent.id).await);
            } else {
                held_receipt = Some(first);
            }
        }
        fixture.peer.detach(CONTROL, HANDLE, 0).await;
        assert_eq!(first_observer.state(), NativeTransactionState::Faulted);
        assert!(!coordinator.controller_identity().is_active());
        fixture.peer.barrier(SEND).await;
        if receipt_stage == 0 {
            let TransactionalDisposition::Retirement(first) = bounded(
                "queued old receipt survives controller retirement",
                sent.delivery.next_disposition(),
            )
            .await
            .expect("original queued event") else {
                panic!("queued retirement receipt");
            };
            assert_eq!(first.transaction_id(), &first_id);
            held_receipt = Some(first);
        }
        let (_other_session, mut other_controller) = fixture.coordinator(HEALTHY).await;
        let second_id = txn(9);
        let second_observer = fixture
            .declare(&mut other_controller, HEALTHY, &second_id)
            .await;
        let receipt = fixture.retirement(&mut sent, &second_id).await;
        assert!(receipt.delivery_identity().same_delivery(&original));
        let second = fixture.provisional(receipt, &second_id, sent.id).await;
        if let Some(first) = held_receipt.take() {
            assert!(
                bounded(
                    "stale old attempt provisional refusal",
                    first.provisional_accept()
                )
                .await
                .is_err()
            );
        }
        drop(held_prepared.take());
        fixture.peer.barrier(SEND).await;
        assert_eq!(second_observer.state(), NativeTransactionState::Pending);
        let discharged = fixture
            .discharge(&mut other_controller, HEALTHY, &second_id, false)
            .await;
        let ready = discharged
            .receipt
            .prepare_work(vec![NativePreparedWork::Retirement(second)])
            .expect("current controller attempt only");
        let (ticket, resources) = ready.into_owner_parts();
        ticket
            .try_claim()
            .expect("current attempt native claim")
            .finish(NativeTransactionDecision::Committed);
        let expected = [(fixture.peer.local(SEND), Role::Sender, sent.id)];
        super::lifecycle::finish_committed(
            &mut fixture,
            resources,
            (HEALTHY, discharged.id),
            &expected,
        )
        .await;
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn dropped_unclaimed_bundle_faults_group_but_actor_restores_original_without_finish() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let transaction = txn(10);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let mut sent = fixture.send(&mut sender, SEND, handle).await;
    let receipt = fixture.retirement(&mut sent, &transaction).await;
    let prepared = fixture.provisional(receipt, &transaction, sent.id).await;
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &transaction, false)
        .await;
    let ready = discharged
        .receipt
        .prepare_work(vec![NativePreparedWork::Retirement(prepared)])
        .expect("exact native ready bundle");
    let (ticket, resources) = ready.into_owner_parts();
    drop(resources);
    assert!(matches!(
        ticket.try_claim(),
        Err(NativeTransactionError::Faulted(NativeFault::Dropped))
    ));
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    fixture.peer.barrier(SEND).await;
    ordinary_finish(&mut fixture, &mut sent).await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn started_claim_cannot_restore_or_ack_a_replacement_sender_route() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let transaction = txn(11);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let mut sent = fixture.send(&mut sender, SEND, handle).await;
    let original = sent.delivery.delivery_identity().clone();
    let receipt = fixture.retirement(&mut sent, &transaction).await;
    let prepared = fixture.provisional(receipt, &transaction, sent.id).await;
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &transaction, false)
        .await;
    let ready = discharged
        .receipt
        .prepare_work(vec![NativePreparedWork::Retirement(prepared)])
        .expect("ready retirement");
    let (ticket, resources) = ready.into_owner_parts();
    let claim = ticket.try_claim().expect("native owner started");
    fixture.peer.detach(SEND, HANDLE, handle).await;
    assert_eq!(observer.state(), NativeTransactionState::OwnerStarted);
    let (mut replacement, replacement_handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    assert_eq!(handle, replacement_handle);
    let mut replacement_sent = fixture
        .send(&mut replacement, SEND, replacement_handle)
        .await;
    assert!(!original.same_delivery(replacement_sent.delivery.delivery_identity()));
    assert!(!original.belongs_to_sender(&replacement.sender_identity()));
    claim.finish(NativeTransactionDecision::Indeterminate);
    assert_eq!(observer.state(), NativeTransactionState::Indeterminate);
    assert!(
        bounded("unknown native terminal finalization", resources.finish())
            .await
            .is_err()
    );
    let (channel, frame) = fixture.peer.control().await;
    assert_eq!(channel, fixture.peer.local(CONTROL));
    let Performative::Detach(detach) = frame else {
        panic!("unknown control decision must not succeed");
    };
    assert_eq!(
        detach
            .error
            .expect("indeterminate controller refusal")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:internal-error"
    );
    fixture.peer.barrier(SEND).await;
    ordinary_finish(&mut fixture, &mut replacement_sent).await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn retirement_provisional_flush_is_required_before_ready_mixed_authority() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let transaction = txn(12);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let mut sent = fixture.send(&mut sender, SEND, handle).await;
    let receipt = fixture.retirement(&mut sent, &transaction).await;
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &transaction, false)
        .await;
    assert_eq!(observer.state(), NativeTransactionState::Sealed);
    fixture.gate.retirement(fixture.peer.local(SEND), sent.id);
    let provisional = receipt.provisional_accept();
    tokio::pin!(provisional);
    let disposition = bounded("provisional bytes before flush", async {
        tokio::select! {
            result = provisional.as_mut() => panic!("provisional returned before gated flush: {}", result.is_ok()),
            frame = fixture.peer.disposition(SEND, Role::Sender, sent.id) => frame,
        }
    }).await;
    assert!(!disposition.settled);
    fixture.gate.wait().await;
    assert_eq!(observer.state(), NativeTransactionState::Sealed);
    let prepared = {
        let waiting = discharged.receipt.wait_ready();
        tokio::pin!(waiting);
        poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        fixture.gate.release();
        let prepared = bounded("provisional flush completed", provisional.as_mut())
            .await
            .expect("prepared only after flush");
        bounded("sealed retirement ready", waiting.as_mut())
            .await
            .expect("Ready after provisional flush");
        prepared
    };
    let ready = discharged
        .receipt
        .prepare_work(vec![NativePreparedWork::Retirement(prepared)])
        .expect("exact prepared retirement");
    let (ticket, resources) = ready.into_owner_parts();
    ticket
        .try_claim()
        .expect("now claimable")
        .finish(NativeTransactionDecision::Committed);
    let expected = [(fixture.peer.local(SEND), Role::Sender, sent.id)];
    super::lifecycle::finish_committed(
        &mut fixture,
        resources,
        (CONTROL, discharged.id),
        &expected,
    )
    .await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn sent_handle_requires_final_outgoing_transfer_flush() {
    let mut fixture = Fixture::with_policy(Policy::Work, 512).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    fixture
        .gate
        .final_transfer(fixture.peer.local(SEND), handle);
    let message = Message {
        body: Body::Data(vec![vec![42; 2_000].into()]),
        ..Message::default()
    };
    let sending = sender.send_with_dispositions(message.clone(), TAG.to_vec().into());
    tokio::pin!(sending);
    let id = bounded("outgoing fragments before final flush", async {
        tokio::select! {
            result = sending.as_mut() => panic!("sent handle before final flush: {}", result.is_ok()),
            id = fixture.peer.outgoing(SEND, handle, &message, TAG) => id,
        }
    }).await;
    fixture.gate.wait().await;
    poll_fn(|cx| {
        assert!(sending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    fixture.gate.release();
    let sent = bounded("fully flushed sent handle", sending.as_mut())
        .await
        .expect("native sent handle");
    assert_eq!(sent.delivery_identity().id(), id);
    fixture.shutdown().await;
}

#[tokio::test]
async fn dropped_receipt_or_failed_provisional_flush_cannot_become_ready_empty_work() {
    for fail_flush in [false, true] {
        let mut fixture = Fixture::new().await;
        let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
        let mut session = fixture.session(SEND).await;
        let (mut sender, handle) = fixture
            .sender(&mut session, SEND, HANDLE, "retirement-sender")
            .await;
        let transaction = txn(13);
        let observer = fixture
            .declare(&mut coordinator, CONTROL, &transaction)
            .await;
        let mut sent = fixture.send(&mut sender, SEND, handle).await;
        let receipt = fixture.retirement(&mut sent, &transaction).await;
        if fail_flush {
            fixture.gate.retirement(fixture.peer.local(SEND), sent.id);
            let provisional = receipt.provisional_accept();
            tokio::pin!(provisional);
            bounded("failed provisional bytes", async {
                tokio::select! {
                    result = provisional.as_mut() => panic!("flush returned early: {}", result.is_ok()),
                    _ = fixture.peer.disposition(SEND, Role::Sender, sent.id) => {}
                }
            }).await;
            fixture.gate.wait().await;
            fixture.gate.fail();
            assert!(
                bounded("provisional IO failure", provisional.as_mut())
                    .await
                    .is_err()
            );
        } else {
            drop(receipt);
            fixture.peer.barrier(SEND).await;
            ordinary_finish(&mut fixture, &mut sent).await;
        }
        assert_eq!(observer.state(), NativeTransactionState::Faulted);
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn canceled_provisional_reply_faults_and_rearms_after_actor_flush_without_empty_commit() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let transaction = txn(17);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let mut sent = fixture.send(&mut sender, SEND, handle).await;
    let receipt = fixture.retirement(&mut sent, &transaction).await;
    fixture.gate.retirement(fixture.peer.local(SEND), sent.id);
    let mut provisional = Box::pin(receipt.provisional_accept());
    bounded("actor-owned provisional frame", async {
        tokio::select! {
            result = provisional.as_mut() => panic!("provisional returned before flush gate: {}", result.is_ok()),
            _ = fixture.peer.disposition(SEND, Role::Sender, sent.id) => {}
        }
    }).await;
    fixture.gate.wait().await;
    drop(provisional);
    fixture.gate.release();
    fixture.peer.barrier(SEND).await;
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    ordinary_finish(&mut fixture, &mut sent).await;
    fixture.shutdown().await;
}
