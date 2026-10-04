use openraft::storage::RaftStateMachine;
use storage::{CommittedStore, FjallReplicaStore, StateStore, StorageError, WriteBatch};

use super::{
    construction::finish,
    fixture::*,
    observed::{Fault, bootstrap_counts, observed},
    *,
};

#[tokio::test]
async fn joined_native_bootstrap_releases_all_handles_before_exact_fjall_reopen() -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let source = selected(true)?;
    {
        let (writer, control) = observed(FjallReplicaStore::open(directory.path())?);
        let mut machine =
            ExperimentalStateMachine::bootstrap_create_send_image(writer, source.request())?;
        let result = async {
            assert_eq!(control.counts(), bootstrap_counts());
            assert_eq!(control.reader().snapshot()?, source.snapshot);
            assert_eq!(machine.applied_state().await?.1.membership(), &members());
            Ok(())
        }
        .await;
        finish(machine, result).await?;
        assert_eq!(control.drops(), (1, 1));
    }
    // The owner joined and the observing control and every physical reader were
    // released before the directory is opened independently again.
    {
        let writer = FjallReplicaStore::open(directory.path())?;
        let reader = writer.reader();
        assert_eq!(reader.snapshot()?, source.snapshot);
        assert!(matches!(
            reader.apply(WriteBatch::default()),
            Err(StorageError::ReplicaWriteRequired)
        ));
        let mut machine = ExperimentalStateMachine::open_with_image_export(writer, stream()?)?;
        let result = async {
            assert_eq!(
                machine.export_create_send_image().await?.as_bytes(),
                source.image.as_bytes()
            );
            assert_eq!(
                machine.apply([next_send()?]).await?,
                vec![crate::LogApplication::Sent { sequence: 3 }]
            );
            Ok(())
        }
        .await;
        finish(machine, result).await?;
        drop(reader);
    }
    {
        let writer = FjallReplicaStore::open(directory.path())?;
        let reader = writer.reader();
        let business = domain::StateMachine::new(reader.clone());
        let row = business
            .message(&namespace()?, &entity()?, domain::SequenceNumber::new(3))?
            .ok_or("missing resumed original")?;
        assert_eq!(row.body, b"PRIVATE-next-body");
        assert!(
            business
                .message(&namespace()?, &entity()?, domain::SequenceNumber::new(2))?
                .is_none()
        );
        assert!(
            business
                .message(&namespace()?, &entity()?, domain::SequenceNumber::new(4))?
                .is_none()
        );
        let mut machine = ExperimentalStateMachine::open(writer, stream()?)?;
        let result = async {
            assert_eq!(machine.applied_state().await?.0.map(|id| id.index), Some(5));
            Ok(())
        }
        .await;
        finish(machine, result).await?;
    }
    Ok(())
}

async fn physical_reopen(after_commit: bool) -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let source = selected(false)?;
    {
        let (writer, control) = observed(FjallReplicaStore::open(directory.path())?);
        control.fault(if after_commit {
            Fault::CommitAfter
        } else {
            Fault::CommitBefore
        });
        assert_eq!(
            ExperimentalStateMachine::bootstrap_create_send_image(writer, source.request()).err(),
            Some(StateMachineImageBootstrapError::Domain(
                CommittedImageBootstrapError::CommitUnknown
            ))
        );
        assert_eq!(control.counts(), bootstrap_counts());
        assert_eq!(control.drops(), (1, 1));
    }
    {
        let writer = FjallReplicaStore::open(directory.path())?;
        let reader = writer.reader();
        assert_eq!(writer.is_initialized()?, after_commit);
        if after_commit {
            assert_eq!(reader.snapshot()?, source.snapshot);
            let mut machine = ExperimentalStateMachine::open_with_image_export(writer, stream()?)?;
            let result = async {
                assert_eq!(machine.applied_state().await?.0.map(|id| id.index), Some(4));
                assert_eq!(
                    machine.export_create_send_image().await?.as_bytes(),
                    source.image.as_bytes()
                );
                Ok(())
            }
            .await;
            finish(machine, result).await?;
        } else {
            assert!(reader.snapshot()?.entries().is_empty());
            assert_eq!(
                ExperimentalStateMachine::open(writer, stream()?).err(),
                Some(StateMachineError::InvalidState)
            );
        }
        drop(reader);
    }
    // There is no bootstrap retry after the unknown result in either branch.
    // A second full-release reopen pins the durable decision once more.
    let writer = FjallReplicaStore::open(directory.path())?;
    assert_eq!(writer.is_initialized()?, after_commit);
    let snapshot = writer.reader().snapshot()?;
    if after_commit {
        assert_eq!(snapshot, source.snapshot);
    } else {
        assert!(snapshot.entries().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn unknown_before_actual_commit_reopens_pristine_without_a_retry() -> TestResult {
    physical_reopen(false).await
}

#[tokio::test]
async fn unknown_after_actual_commit_reopens_complete_without_a_retry() -> TestResult {
    physical_reopen(true).await
}

#[tokio::test]
async fn simulated_thread_start_failure_drops_known_committed_state_before_fjall_reopen()
-> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let source = selected(true)?;
    {
        let (writer, control) = observed(FjallReplicaStore::open(directory.path())?);
        let result = super::super::bootstrap_with_starter(writer, source.request(), |state| {
            assert_eq!(control.counts(), bootstrap_counts());
            assert!(state.image_export.is_none());
            drop(state);
            Err(StateMachineError::ThreadStart)
        });
        assert_eq!(
            result.err(),
            Some(StateMachineImageBootstrapError::OwnerStartAfterCommit)
        );
        assert_eq!(control.counts(), bootstrap_counts());
        assert_eq!(control.drops(), (1, 1));
        assert!(control.initialized()?);
        assert_eq!(control.reader().snapshot()?, source.snapshot);
    }
    // The starter deliberately refused; no OS thread was exhausted or launched
    // in that attempt. Its consumed state and the test control are now gone.
    {
        let writer = FjallReplicaStore::open(directory.path())?;
        assert!(writer.is_initialized()?);
        assert_eq!(writer.reader().snapshot()?, source.snapshot);
        let mut machine = ExperimentalStateMachine::open_with_image_export(writer, stream()?)?;
        let result = async {
            assert_eq!(
                machine.export_create_send_image().await?.as_bytes(),
                source.image.as_bytes()
            );
            assert_eq!(machine.applied_state().await?.0.map(|id| id.index), Some(4));
            Ok(())
        }
        .await;
        finish(machine, result).await?;
    }
    let writer = FjallReplicaStore::open(directory.path())?;
    assert_eq!(writer.reader().snapshot()?, source.snapshot);
    Ok(())
}
