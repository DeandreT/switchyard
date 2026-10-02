use std::{sync::atomic::Ordering, time::Duration};

use amqp::{DeliveryState, NativeTransactionState, Outcome, TransactionId, TransactionalState};
use tokio::time::timeout;

use super::operations::Operation;

mod admissions;
mod begin_gate;
mod fixture;
mod recorder;

use fixture::{CONTROL, DEADLINE, Fixture, POST, TestResult};

fn provisional(disposition: amqp::Disposition, transaction: &TransactionId) {
    assert!(!disposition.settled);
    assert!(matches!(disposition.state,
        Some(DeliveryState::Transactional(TransactionalState { txn_id, outcome: Some(Outcome::Accepted(_)) }))
            if &txn_id == transaction
    ));
}

async fn drive_provisional(fixture: &mut Fixture, transaction: &TransactionId) -> TestResult {
    let owner = &mut fixture.owner;
    let response = fixture.peer.disposition(POST, 0);
    tokio::pin!(response);
    loop {
        tokio::select! {
            disposition = &mut response => {
                provisional(disposition?, transaction);
                return Ok(());
            }
            operation = owner.next_operation() => {
                owner.accept_completion(operation.ok_or("native posting operation missing")?);
            }
        }
    }
}

async fn drive_finish(fixture: &mut Fixture) -> TestResult {
    let owner = &mut fixture.owner;
    let response = async {
        let posting = fixture.peer.disposition(POST, 0).await?;
        assert!(posting.settled);
        assert!(matches!(posting.state, Some(DeliveryState::Accepted(_))));
        let control = fixture.peer.disposition(CONTROL, 1).await?;
        assert!(control.settled);
        assert!(matches!(control.state, Some(DeliveryState::Accepted(_))));
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    tokio::pin!(response);
    timeout(DEADLINE, async {
        loop {
            tokio::select! {
                response = &mut response => return response,
                operation = owner.next_operation() => {
                    owner.accept_completion(operation.ok_or("native finalization operation missing")?);
                }
            }
        }
    }).await?
}

#[tokio::test]
async fn discharge_can_reach_owner_before_earlier_actual_posting_event() -> TestResult {
    let mut fixture = Fixture::new().await?;
    let transaction = fixture.declare().await?;
    let posting = fixture
        .posting(&transaction, b"late collector event")
        .await?;
    let sealed = fixture.sealed(&transaction).await?;
    assert_eq!(sealed.state(), NativeTransactionState::Sealed);
    fixture.discharge(sealed);
    assert!(
        timeout(Duration::from_millis(20), fixture.owner.next_operation())
            .await
            .is_err()
    );
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 0);
    fixture.peer.barrier().await?;
    fixture.stage(posting);
    drive_provisional(&mut fixture, &transaction).await?;
    drive_finish(&mut fixture).await?;
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 1);
    assert_eq!(
        fixture
            .recorder
            .bodies
            .lock()
            .expect("body observer")
            .as_slice(),
        &[vec![b"late collector event".to_vec()]]
    );
    assert_eq!(
        fixture
            .observer
            .as_ref()
            .expect("declared observer")
            .state(),
        NativeTransactionState::Committed
    );
    fixture.shutdown().await
}

#[tokio::test]
async fn native_ready_waits_for_the_actual_last_prepared_return() -> TestResult {
    let mut fixture = Fixture::new().await?;
    let transaction = fixture.declare().await?;
    let posting = fixture
        .posting(&transaction, b"held prepared callback")
        .await?;
    fixture.stage(posting);
    let held = timeout(DEADLINE, async {
        loop {
            let operation = fixture
                .owner
                .next_operation()
                .await
                .ok_or("native preparation operation missing")?;
            if matches!(&operation, Operation::Prepared { result: Ok(_), .. }) {
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(operation);
            }
            fixture.owner.accept_completion(operation);
        }
    })
    .await??;
    provisional(fixture.peer.disposition(POST, 0).await?, &transaction);
    let sealed = fixture.sealed(&transaction).await?;
    assert_eq!(sealed.state(), NativeTransactionState::Ready);
    fixture.discharge(sealed);
    let ready = timeout(DEADLINE, fixture.owner.next_operation())
        .await?
        .ok_or("native readiness operation missing")?;
    assert!(matches!(&ready, Operation::Ready { result: Ok(()), .. }));
    fixture.owner.accept_completion(ready);
    assert!(
        timeout(Duration::from_millis(20), fixture.owner.next_operation())
            .await
            .is_err()
    );
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 0);
    fixture.peer.barrier().await?;
    fixture.owner.accept_completion(held);
    drive_finish(&mut fixture).await?;
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 1);
    fixture.shutdown().await
}

#[tokio::test]
async fn connection_owner_close_aborts_queued_pair_before_claiming() -> TestResult {
    let mut fixture = Fixture::new().await?;
    let transaction = fixture.declare().await?;
    let posting = fixture
        .posting(&transaction, b"queued cancellation")
        .await?;
    fixture.stage(posting);
    drive_provisional(&mut fixture, &transaction).await?;
    let sealed = fixture.sealed(&transaction).await?;
    fixture.discharge(sealed);
    timeout(DEADLINE, async {
        while fixture.recorder.handoffs.load(Ordering::Relaxed) == 0 {
            let operation = fixture
                .owner
                .next_operation()
                .await
                .expect("owned operation");
            fixture.owner.accept_completion(operation);
        }
    })
    .await?;
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 0);
    fixture.owner.close();
    let applied = timeout(DEADLINE, fixture.owner.next_operation())
        .await?
        .ok_or("queued application operation missing")?;
    assert!(
        matches!(&applied, Operation::Applied { result: Ok(completion), .. } if completion.application().is_err())
    );
    fixture.owner.accept_completion(applied);
    assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 0);
    assert!(
        fixture
            .recorder
            .bodies
            .lock()
            .expect("body observer")
            .is_empty()
    );
    assert_eq!(
        fixture
            .observer
            .as_ref()
            .expect("declared observer")
            .state(),
        NativeTransactionState::Aborted
    );
    fixture.shutdown().await
}

#[tokio::test]
async fn idle_empty_declaration_expiry_closes_its_actual_native_controller() -> TestResult {
    let mut fixture = Fixture::new().await?;
    let _transaction = fixture.declare().await?;
    assert_eq!(
        fixture
            .observer
            .as_ref()
            .expect("declared observer")
            .state(),
        NativeTransactionState::Pending
    );
    let deadline = std::time::Instant::now()
        .checked_add(crate::ATOMIC_TRANSACTION_TIMEOUT)
        .expect("test deadline");
    fixture.owner.tick_at(deadline);
    fixture.close_expired_controller().await?;
    assert!(!fixture.coordinator.controller_identity().is_active());
    assert_eq!(
        fixture
            .observer
            .as_ref()
            .expect("declared observer")
            .state(),
        NativeTransactionState::Faulted
    );
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 0);
    fixture.shutdown().await
}
