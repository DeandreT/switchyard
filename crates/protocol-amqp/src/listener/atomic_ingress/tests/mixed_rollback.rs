use std::{sync::atomic::Ordering, time::Duration};

use amqp::{DeliveryState, NativeTransactionState, Outcome, Role, TransactionalState};
use tokio::{sync::oneshot, time::timeout};

use super::super::{Event, groups::RetirementCompletion, operations::Operation};

mod fixture;
use fixture::{CONTROL, DEADLINE, Fixture, POST, SEND, TestResult};

async fn no_operation(fixture: &mut Fixture) {
    assert!(
        timeout(Duration::from_millis(20), fixture.owner.next_operation())
            .await
            .is_err()
    );
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 0);
}

async fn finish_rollback(fixture: &mut Fixture, control: u32, posting: Option<u32>) -> TestResult {
    let response = async {
        if let Some(id) = posting {
            let disposition = fixture.peer.disposition(POST, Role::Receiver, id).await?;
            assert!(disposition.settled);
            assert!(disposition.state.is_none());
        }
        let disposition = fixture
            .peer
            .disposition(CONTROL, Role::Receiver, control)
            .await?;
        assert!(disposition.settled);
        assert!(matches!(
            disposition.state,
            Some(DeliveryState::Accepted(_))
        ));
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (operation, response) =
        tokio::join!(timeout(DEADLINE, fixture.owner.next_operation()), response);
    response?;
    let operation = operation?.ok_or("actual rollback completion missing")?;
    assert!(matches!(
        &operation,
        Operation::Finished {
            result: Ok(()),
            completion: RetirementCompletion::Rearmed,
            ..
        }
    ));
    fixture.owner.accept_completion(operation);
    Ok(())
}

#[tokio::test]
async fn actual_true_control_waits_for_both_late_native_collectors() -> TestResult {
    let mut fixture = Fixture::new().await?;
    let mut sent = fixture.held().await?;
    let original = sent.delivery_identity().clone();
    let transaction = fixture.declare(0).await?;
    let posting = fixture.posting(&transaction, 0).await?;
    let retirement = fixture.retirement(&mut sent, &transaction).await?;
    fixture.discharge(&transaction, 1, true).await?;
    assert_eq!(
        fixture
            .observer
            .as_ref()
            .expect("declared observer")
            .state(),
        NativeTransactionState::Aborted
    );
    no_operation(&mut fixture).await;
    fixture.peer.barrier().await?;
    fixture.owner.process(Event::Posting {
        source: fixture.receiver.receiver_identity(),
        receipt: posting,
    });
    no_operation(&mut fixture).await;
    let (reply, mut completion) = oneshot::channel();
    fixture.owner.process(Event::Retirement {
        source: fixture.sender.sender_identity(),
        receipt: retirement,
        reply,
    });
    assert!(matches!(
        completion.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    finish_rollback(&mut fixture, 1, Some(0)).await?;
    assert!(matches!(completion.await?, RetirementCompletion::Rearmed));
    assert!(sent.delivery_identity().same_delivery(&original));
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 0);

    // The same native delivery can join the next transaction without a resend.
    let next = fixture.declare(2).await?;
    let retirement = fixture.retirement(&mut sent, &next).await?;
    let (reply, mut completion) = oneshot::channel();
    fixture.owner.process(Event::Retirement {
        source: fixture.sender.sender_identity(),
        receipt: retirement,
        reply,
    });
    assert!(matches!(
        completion.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    fixture.discharge(&next, 3, true).await?;
    let operation = timeout(DEADLINE, fixture.owner.next_operation())
        .await?
        .ok_or("checked retirement missing")?;
    assert!(matches!(&operation, Operation::RetirementChecked { .. }));
    fixture.owner.accept_completion(operation);
    finish_rollback(&mut fixture, 3, None).await?;
    assert!(matches!(completion.await?, RetirementCompletion::Rearmed));
    assert!(sent.delivery_identity().same_delivery(&original));
    fixture.shutdown().await
}

#[tokio::test]
async fn true_rollback_waits_for_the_actual_prepared_callback() -> TestResult {
    let mut fixture = Fixture::new().await?;
    let mut sent = fixture.held().await?;
    let transaction = fixture.declare(0).await?;
    let receipt = fixture.retirement(&mut sent, &transaction).await?;
    let (reply, mut completion) = oneshot::channel();
    fixture.owner.process(Event::Retirement {
        source: fixture.sender.sender_identity(),
        receipt,
        reply,
    });
    let checked = timeout(DEADLINE, fixture.owner.next_operation())
        .await?
        .ok_or("checked retirement missing")?;
    assert!(matches!(
        &checked,
        Operation::RetirementChecked { result: Ok(()), .. }
    ));
    fixture.owner.accept_completion(checked);
    let (prepared, disposition) = tokio::join!(
        timeout(DEADLINE, fixture.owner.next_operation()),
        fixture.peer.disposition(SEND, Role::Sender, 0),
    );
    let prepared = prepared?.ok_or("prepared retirement missing")?;
    assert!(matches!(
        &prepared,
        Operation::RetirementPrepared { result: Ok(_), .. }
    ));
    let disposition = disposition?;
    assert!(!disposition.settled);
    assert!(
        matches!(disposition.state, Some(DeliveryState::Transactional(TransactionalState {
        txn_id, outcome: Some(Outcome::Accepted(_)),
    })) if txn_id == transaction)
    );
    fixture.discharge(&transaction, 1, true).await?;
    no_operation(&mut fixture).await;
    assert!(matches!(
        completion.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    fixture.peer.barrier().await?;
    fixture.owner.accept_completion(prepared);
    finish_rollback(&mut fixture, 1, None).await?;
    assert!(matches!(completion.await?, RetirementCompletion::Rearmed));
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 0);
    fixture.shutdown().await
}

#[tokio::test]
async fn delayed_true_rollback_posting_does_not_close_the_next_transaction() -> TestResult {
    let mut fixture = Fixture::new().await?;
    let transaction = fixture.declare(0).await?;
    let posting = fixture.posting(&transaction, 0).await?;
    fixture.discharge(&transaction, 1, true).await?;
    no_operation(&mut fixture).await;
    fixture.owner.process(Event::Posting {
        source: fixture.receiver.receiver_identity(),
        receipt: posting,
    });
    finish_rollback(&mut fixture, 1, Some(0)).await?;
    let next = fixture.declare(2).await?;
    let posting = fixture.posting(&next, 1).await?;
    fixture.owner.process(Event::Posting {
        source: fixture.receiver.receiver_identity(),
        receipt: posting,
    });
    fixture.discharge(&next, 3, true).await?;
    let operation = timeout(DEADLINE, fixture.owner.next_operation())
        .await?
        .ok_or("checked posting missing")?;
    assert!(matches!(&operation, Operation::PostingChecked { .. }));
    fixture.owner.accept_completion(operation);
    finish_rollback(&mut fixture, 3, Some(1)).await?;
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 0);
    fixture.shutdown().await
}
