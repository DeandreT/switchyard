use std::{sync::Arc, time::Duration};

use domain::{CommittedStateMachine, CommittedStreamId};
use storage::MemoryReplicaStore;

use crate::{
    LogProfile, LogVote,
    experimental_log::{FinalLogReport, LogRetention},
};

use super::{
    AttemptReceipt, Completion, Continuity, Error, Floor, RejoinState, RetirementEvidence,
    WaitGuard,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(5);

fn comparison_evidence(node_id: u64, term: u64) -> TestResult<Arc<RetirementEvidence>> {
    let stream = CommittedStreamId::new([73; 16])?;
    let machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream)?;
    let checkpoint = machine.checkpoint()?;
    // These units test receipt ownership and floor selection, not native
    // retirement. Production creates authoritative floors only after joins.
    let log = FinalLogReport {
        profile: LogProfile::new(node_id, stream)?,
        vote: Some(LogVote::new(term, node_id)),
        retention: LogRetention {
            last_present: None,
            last_purged: None,
            retained_entries: 0,
            retained_bytes: 0,
        },
        prefixes: Vec::new(),
        membership_events: Vec::new(),
    };
    Ok(RetirementEvidence::checked(
        node_id, stream, log, checkpoint,
    )?)
}

#[tokio::test]
async fn a_lost_unique_publisher_is_unavailable_not_an_infinite_receipt_wait() -> TestResult {
    let (receipt, publisher) = AttemptReceipt::new(7);
    let clone = receipt.clone();
    drop(publisher);
    let completion = receipt.completed().expect("lost publisher is terminal");
    assert_eq!(completion.cleanup, Err(Error::TaskFailed));
    assert!(matches!(completion.continuity, Continuity::Unavailable));
    let completion = tokio::time::timeout(DEADLINE, clone.join()).await?;
    assert_eq!(completion.cleanup, Err(Error::TaskFailed));
    assert!(matches!(completion.continuity, Continuity::Unavailable));
    Ok(())
}

#[tokio::test]
async fn publisher_loss_cannot_be_hidden_by_a_retained_cancellation_guard() -> TestResult {
    let (receipt, publisher) = AttemptReceipt::new(7);
    let guard = WaitGuard {
        shared: receipt.shared.clone(),
        armed: true,
    };
    drop(publisher);
    let completion = tokio::time::timeout(DEADLINE, receipt.join()).await?;
    assert_eq!(completion.cleanup, Err(Error::TaskFailed));
    assert!(matches!(completion.continuity, Continuity::Unavailable));
    drop(guard);
    Ok(())
}

#[tokio::test]
async fn published_completion_survives_the_unique_sender_drop() -> TestResult {
    let (receipt, publisher) = AttemptReceipt::new(7);
    receipt.cancel();
    assert!(receipt.shared.is_canceled());
    assert!(receipt.completed().is_none());
    publisher.complete(Completion {
        cleanup: Ok(()),
        continuity: Continuity::NoEngineStart,
    });
    let completion = tokio::time::timeout(DEADLINE, receipt.join()).await?;
    assert_eq!(completion.cleanup, Ok(()));
    assert!(matches!(completion.continuity, Continuity::NoEngineStart));
    Ok(())
}

#[tokio::test]
async fn pre_engine_cleanup_failure_is_sticky_only_for_shutdown_aggregation() -> TestResult {
    let first = comparison_evidence(7, 1)?;
    let other = comparison_evidence(8, 1)?;
    let mut state = RejoinState::default();
    state.floors.insert(7, Floor::Healthy(first.clone()));
    state.floors.insert(8, Floor::Healthy(other.clone()));
    let (receipt, publisher) = AttemptReceipt::new(7);
    state.active = Some(receipt);
    publisher.complete(Completion {
        cleanup: Err(Error::OwnerFailure),
        continuity: Continuity::NoEngineStart,
    });
    assert_eq!(state.fold_completed(), Ok(()));
    assert!(state.active.is_none());
    assert_eq!(state.cleanup_failure, Some(Error::OwnerFailure));
    for (id, expected) in [(7, first), (8, other)] {
        assert!(matches!(state.floors.get(&id), Some(Floor::Healthy(actual))
            if Arc::ptr_eq(actual, &expected)));
    }
    assert_eq!(
        tokio::time::timeout(DEADLINE, state.join_pending()).await?,
        Err(Error::OwnerFailure)
    );
    Ok(())
}

#[test]
fn completed_new_generation_replaces_old_floor_even_with_a_sticky_diagnostic() -> TestResult {
    let old = comparison_evidence(7, 1)?;
    let new = comparison_evidence(7, 2)?;
    let mut state = RejoinState::default();
    state.floors.insert(7, Floor::Healthy(old));
    let (receipt, publisher) = AttemptReceipt::new(7);
    state.active = Some(receipt);
    publisher.complete(Completion {
        cleanup: Err(Error::TaskFailed),
        continuity: Continuity::RetiredNewGeneration(new.clone()),
    });
    assert_eq!(state.fold_completed(), Ok(()));
    assert_eq!(state.cleanup_failure, Some(Error::TaskFailed));
    assert!(matches!(state.floors.get(&7), Some(Floor::Healthy(actual))
        if Arc::ptr_eq(actual, &new)));
    Ok(())
}

#[test]
fn post_handoff_unavailable_fences_only_that_original_voter() -> TestResult {
    let other = comparison_evidence(8, 1)?;
    let mut state = RejoinState::default();
    state
        .floors
        .insert(7, Floor::Healthy(comparison_evidence(7, 1)?));
    state.floors.insert(8, Floor::Healthy(other.clone()));
    let (receipt, publisher) = AttemptReceipt::new(7);
    state.active = Some(receipt);
    publisher.complete(Completion {
        cleanup: Ok(()),
        continuity: Continuity::Unavailable,
    });
    assert_eq!(state.fold_completed(), Ok(()));
    assert!(matches!(state.floors.get(&7), Some(Floor::Unavailable)));
    assert!(matches!(state.floors.get(&8), Some(Floor::Healthy(actual))
        if Arc::ptr_eq(actual, &other)));
    Ok(())
}

#[test]
fn a_lost_publisher_is_folded_into_unavailable_instead_of_in_progress() {
    let mut state = RejoinState::default();
    let (receipt, publisher) = AttemptReceipt::new(7);
    state.active = Some(receipt);
    drop(publisher);
    assert_eq!(state.fold_completed(), Ok(()));
    assert!(state.active.is_none());
    assert!(matches!(state.floors.get(&7), Some(Floor::Unavailable)));
    assert_eq!(state.cleanup_failure, Some(Error::TaskFailed));
}

#[test]
fn an_incomplete_publisher_preserves_the_single_active_attempt() {
    let mut state = RejoinState::default();
    let (receipt, publisher) = AttemptReceipt::new(7);
    state.active = Some(receipt);
    assert_eq!(state.fold_completed(), Err(Error::RejoinInProgress));
    assert!(state.active.is_some());
    assert!(state.floors.is_empty());
    drop(publisher);
}
