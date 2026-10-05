use super::{fixture::*, observed::Mode, *};
use crate::experimental_local_compaction::frontier::{Frontier, PairIdentity};
use std::sync::atomic::Ordering;

pub(super) async fn read_poison_before_claim<L, S>(log: L, state: S) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    late_failure(log, state, false, false).await
}
pub(super) async fn export_poison_before_claim<L, S>(log: L, state: S) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    late_failure(log, state, true, false).await
}
pub(super) async fn read_panic_before_claim<L, S>(log: L, state: S) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    late_failure(log, state, false, true).await
}
pub(super) async fn export_panic_before_claim<L, S>(log: L, state: S) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    late_failure(log, state, true, true).await
}

async fn late_failure<L, S>(log_writer: L, state_writer: S, export: bool, panic: bool) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    let (log, mut state, log_control, control) =
        seed_with_export(log_writer, state_writer, 2, 4).await?;
    let stale_read = state.read_create_send_catalog();
    let stale_export = state.export_create_send_image();
    let identity = PairIdentity::new();
    let (frontier, publisher) = Frontier::new(identity.clone());
    let result: TestResult = async {
        let checkpoint = state
            .seal_local_compaction(identity.clone(), publisher)
            .await?;
        log.seal(identity.clone(), Box::new(checkpoint.clone()), None)
            .await?;
        let attempt = frontier.begin()?;
        let receipt = state
            .build_local_compaction(identity, 7, attempt, Box::new(checkpoint))
            .await?;
        let permit = log.permit(frontier.clone(), receipt).await?;
        let commits = log_control.commits.load(Ordering::SeqCst);
        control.fail_read(if panic { Mode::Panic } else { Mode::Before });
        if export {
            assert!(stale_export.await.is_err());
        } else {
            assert!(stale_read.await.is_err());
        }
        // The completed poisoned/panicking read cannot leave old authority live.
        assert_eq!(
            frontier.receipt(attempt).await.err(),
            Some(LocalCompactionError::OwnerFailure)
        );
        assert_eq!(
            log.compact(permit).await.err(),
            Some(LocalCompactionError::OwnerFailure)
        );
        assert_eq!(log_control.commits.load(Ordering::SeqCst), commits);
        Ok(())
    }
    .await;
    let (a, b) = tokio::join!(log.shutdown(), state.shutdown());
    result?;
    a?;
    if panic {
        assert_eq!(b, Err(crate::StateMachineError::Panicked));
    } else {
        b?;
    }
    Ok(())
}

#[tokio::test]
async fn publisher_loss_and_close_wake_waiters() -> TestResult {
    let (frontier, publisher) = Frontier::new(PairIdentity::new());
    let attempt = frontier.begin()?;
    let observer = frontier.clone();
    let waiter = tokio::spawn(async move { observer.receipt(attempt).await });
    drop(publisher);
    assert_eq!(waiter.await?.err(), Some(LocalCompactionError::Closed));
    assert_eq!(frontier.begin(), Err(LocalCompactionError::Closed));
    Ok(())
}

#[tokio::test]
async fn consumed_and_foreign_generation_permits_refuse() -> TestResult {
    let (log, state, _, _) = seed(
        MemoryReplicaStore::new(),
        MemoryCatalogReplicaStore::new(),
        2,
        4,
    )
    .await?;
    let identity = PairIdentity::new();
    let (frontier, publisher) = Frontier::new(identity.clone());
    let result: TestResult = async {
        let checkpoint = state
            .seal_local_compaction(identity.clone(), publisher)
            .await?;
        let attempt = frontier.begin()?;
        let receipt = state
            .build_local_compaction(identity.clone(), 7, attempt, Box::new(checkpoint))
            .await?;
        let first = frontier.permit(receipt.clone(), 0, [1; 32], vec![2])?;
        let second = frontier.permit(receipt.clone(), 0, [1; 32], vec![2])?;
        assert!(
            first
                .claim(&identity, stream()?, 7, 0, [1; 32], &[2])
                .is_ok()
        );
        assert_eq!(
            second
                .claim(&identity, stream()?, 7, 0, [1; 32], &[2])
                .err(),
            Some(LocalCompactionError::InvalidPair)
        );
        let (other, _publisher) = Frontier::new(PairIdentity::new());
        other.begin()?;
        assert_eq!(
            other.permit(receipt, 0, [1; 32], vec![2]).err(),
            Some(LocalCompactionError::InvalidPair)
        );
        Ok(())
    }
    .await;
    let (a, b) = tokio::join!(log.shutdown(), state.shutdown());
    result?;
    a?;
    b?;
    Ok(())
}

#[tokio::test]
async fn stale_ordinal_and_terminal_before_claim_refuse() -> TestResult {
    let (log, state, _, _) = seed(
        MemoryReplicaStore::new(),
        MemoryCatalogReplicaStore::new(),
        2,
        4,
    )
    .await?;
    let identity = PairIdentity::new();
    let (frontier, publisher) = Frontier::new(identity.clone());
    let result: TestResult = async {
        let checkpoint = state
            .seal_local_compaction(identity.clone(), publisher)
            .await?;
        let attempt = frontier.begin()?;
        let receipt = state
            .build_local_compaction(identity.clone(), 7, attempt, Box::new(checkpoint))
            .await?;
        let stale = frontier.permit(receipt.clone(), 0, [1; 32], vec![2])?;
        assert_eq!(
            stale.claim(&identity, stream()?, 7, 1, [1; 32], &[2]).err(),
            Some(LocalCompactionError::InvalidPair)
        );
        let pending = frontier.permit(receipt, 0, [1; 32], vec![2])?;
        frontier.close(LocalCompactionError::Closed);
        assert_eq!(
            pending
                .claim(&identity, stream()?, 7, 0, [1; 32], &[2])
                .err(),
            Some(LocalCompactionError::Closed)
        );
        Ok(())
    }
    .await;
    let (a, b) = tokio::join!(log.shutdown(), state.shutdown());
    result?;
    a?;
    b?;
    Ok(())
}
