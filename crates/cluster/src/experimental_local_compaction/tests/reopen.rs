use super::{fixture::*, observed::Target, *};
use crate::experimental_local_compaction::owner;
use openraft::storage::RaftStateMachine;
use std::sync::atomic::Ordering;

pub(super) async fn reacquire(
    log_path: &std::path::Path,
    state_path: &std::path::Path,
) -> TestResult {
    let log = ExperimentalCompactionLogStore::open(FjallReplicaStore::open(log_path)?, profile()?)?;
    let reopened = (|| -> TestResult<_> {
        Ok(ExperimentalStateMachine::open_with_snapshot_catalog(
            FjallCatalogReplicaStore::open(state_path)?,
            stream()?,
        )?)
    })();
    let state = match reopened {
        Ok(state) => state,
        Err(error) => {
            log.shutdown().await?;
            return Err(error);
        }
    };
    let (a, b) = tokio::join!(log.shutdown(), state.shutdown());
    a?;
    b?;
    Ok(())
}

#[tokio::test]
async fn unpolled_prepare_drop_joins_both_and_reacquires() -> TestResult {
    let log_dir = testkit::DurableProvider::temporary()?;
    let state_dir = testkit::DurableProvider::temporary()?;
    let (log, state, log_control, state_control) = seed(
        FjallReplicaStore::open(log_dir.path())?,
        FjallCatalogReplicaStore::open(state_dir.path())?,
        2,
        4,
    )
    .await?;
    let before = (
        log_control.commits.load(Ordering::SeqCst),
        state_control.captures.load(Ordering::SeqCst),
        state_control.catalog_commits.load(Ordering::SeqCst),
        log_control.reads.load(Ordering::SeqCst),
        state_control.reads.load(Ordering::SeqCst),
        state_control.catalog_reads.load(Ordering::SeqCst),
    );
    let (future, exit) = owner::prepare_observed(7, log, state, tokio::runtime::Handle::current());
    // No first poll, source query, capture, or retention precedes this drop.
    drop(future);
    assert_eq!(
        wait_exit(exit).await?,
        Err(LocalCompactionError::TaskFailed)
    );
    assert_eq!(
        before,
        (
            log_control.commits.load(Ordering::SeqCst),
            state_control.captures.load(Ordering::SeqCst),
            state_control.catalog_commits.load(Ordering::SeqCst),
            log_control.reads.load(Ordering::SeqCst),
            state_control.reads.load(Ordering::SeqCst),
            state_control.catalog_reads.load(Ordering::SeqCst)
        )
    );
    reacquire(log_dir.path(), state_dir.path()).await?;
    reacquire(log_dir.path(), state_dir.path()).await
}

#[tokio::test]
async fn first_polled_prepare_caller_loss_joins_both_and_reacquires() -> TestResult {
    let log_dir = testkit::DurableProvider::temporary()?;
    let state_dir = testkit::DurableProvider::temporary()?;
    let (log, state, _, control) = seed(
        FjallReplicaStore::open(log_dir.path())?,
        FjallCatalogReplicaStore::open(state_dir.path())?,
        2,
        4,
    )
    .await?;
    let gate = control.gate(Target::CatalogRead);
    let (future, exit) = owner::prepare_observed(7, log, state, tokio::runtime::Handle::current());
    let waiter = tokio::spawn(future);
    if let Err(error) = gate.entered().await {
        waiter.abort();
        let _ = waiter.await;
        gate.release();
        let _ = wait_exit(exit).await?;
        return Err(error.into());
    }
    waiter.abort();
    let _ = waiter.await;
    gate.release();
    wait_exit(exit).await??;
    reacquire(log_dir.path(), state_dir.path()).await?;
    reacquire(log_dir.path(), state_dir.path()).await
}

#[tokio::test]
async fn source_panic_retires_both_before_same_directory_reopen() -> TestResult {
    let log_dir = testkit::DurableProvider::temporary()?;
    let state_dir = testkit::DurableProvider::temporary()?;
    let (log, state, _, control) = seed(
        FjallReplicaStore::open(log_dir.path())?,
        FjallCatalogReplicaStore::open(state_dir.path())?,
        2,
        4,
    )
    .await?;
    let mut pair = pair(log, state).await?;
    control.fail_read(super::observed::Mode::Panic);
    let result = pair.compact().await;
    let joined = pair.shutdown().await;
    assert_eq!(result.err(), Some(LocalCompactionError::OwnerFailure));
    assert_eq!(joined, Err(LocalCompactionError::OwnerFailure));
    reacquire(log_dir.path(), state_dir.path()).await?;
    reacquire(log_dir.path(), state_dir.path()).await
}

#[tokio::test]
async fn two_distinct_cycles_require_joined_reopen_and_new_generation() -> TestResult {
    let log_dir = testkit::DurableProvider::temporary()?;
    let state_dir = testkit::DurableProvider::temporary()?;
    let (log, state, _, _) = seed(
        FjallReplicaStore::open(log_dir.path())?,
        FjallCatalogReplicaStore::open(state_dir.path())?,
        2,
        4,
    )
    .await?;
    let mut first = pair(log, state).await?;
    let old_observer = first.handle.observer();
    let result: TestResult<_> = async { Ok(first.compact().await?) }.await;
    let joined = first.shutdown().await;
    let first_progress = result?;
    joined?;
    assert_eq!(first_progress.ordinal(), 1);

    let log =
        ExperimentalCompactionLogStore::open(FjallReplicaStore::open(log_dir.path())?, profile()?)?;
    let reopened = (|| -> TestResult<_> {
        Ok(ExperimentalStateMachine::open_with_snapshot_catalog(
            FjallCatalogReplicaStore::open(state_dir.path())?,
            stream()?,
        )?)
    })();
    let mut state = match reopened {
        Ok(state) => state,
        Err(error) => {
            log.shutdown().await?;
            return Err(error);
        }
    };
    let result: TestResult<_> = async {
        let suffix = log.read_limited(0, u64::MAX).await?;
        assert_eq!(
            suffix
                .iter()
                .map(|entry| entry.log_id.index)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
        state.apply(suffix).await?;
        Ok(state.checkpoint().await?)
    }
    .await;
    let (log, state, expected) = sources_or_cleanup(result, log, state).await?;
    let mut second = pair(log, state).await?;
    let result: TestResult<_> = async {
        assert_eq!(old_observer.begin(), Err(LocalCompactionError::Closed));
        let current = second.compact().await?;
        assert_eq!(current.ordinal(), 2);
        assert_eq!(current.retained_entries(), 0);
        assert_eq!(current.checkpoint(), &expected);
        assert_eq!(first_progress.checkpoint().last().unwrap().id.index, 2);
        Ok(current)
    }
    .await;
    let joined = second.shutdown().await;
    let second_progress = result?;
    joined?;

    let (log_writer, log_control) =
        super::observed::Observed::new(FjallReplicaStore::open(log_dir.path())?);
    let log = ExperimentalCompactionLogStore::open(log_writer, profile()?)?;
    let reopened = (|| -> TestResult<_> {
        let (writer, control) =
            super::observed::Observed::new(FjallCatalogReplicaStore::open(state_dir.path())?);
        Ok((
            ExperimentalStateMachine::open_with_snapshot_catalog(writer, stream()?)?,
            control,
        ))
    })();
    let (mut state, state_control) = match reopened {
        Ok(state) => state,
        Err(error) => {
            log.shutdown().await?;
            return Err(error);
        }
    };
    let result: TestResult = async {
        assert!(log.read_limited(0, u64::MAX).await?.is_empty());
        assert_eq!(state.checkpoint().await?, expected);
        assert_eq!(second_progress.checkpoint(), &expected);
        let retained = state.read_create_send_catalog().await?.unwrap();
        let image = domain::DecodedCommittedImage::decode(retained.image_bytes())?;
        let messages = image
            .rows()
            .filter(|row| row.key().first() == Some(&3))
            .map(|row| domain::MessageRecord::decode(row.value()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(messages.len(), 3);
        assert_eq!(
            messages
                .iter()
                .map(|message| message.message_id.as_str())
                .collect::<Vec<_>>(),
            vec!["PRIVATE-2", "PRIVATE-3", "PRIVATE-4"]
        );
        assert_eq!(
            messages
                .iter()
                .map(|message| message.body.as_slice())
                .collect::<Vec<_>>(),
            vec![&[2u8; 32][..], &[3u8; 32][..], &[4u8; 32][..]]
        );
        Ok(())
    }
    .await;
    let (log, state, ()) = sources_or_cleanup(result, log, state).await?;
    let mut cached_pair = pair(log, state).await?;
    let result: TestResult = async {
        let before_reads = (
            log_control.reads.load(Ordering::SeqCst),
            state_control.reads.load(Ordering::SeqCst),
            state_control.catalog_reads.load(Ordering::SeqCst),
        );
        assert_eq!(cached_pair.compact().await?.ordinal(), 2);
        assert_eq!(cached_pair.compact().await?.ordinal(), 2);
        assert_eq!(log_control.commits.load(Ordering::SeqCst), 0);
        assert_eq!(state_control.captures.load(Ordering::SeqCst), 0);
        assert_eq!(state_control.catalog_commits.load(Ordering::SeqCst), 0);
        assert_eq!(
            before_reads,
            (
                log_control.reads.load(Ordering::SeqCst),
                state_control.reads.load(Ordering::SeqCst),
                state_control.catalog_reads.load(Ordering::SeqCst)
            )
        );
        Ok(())
    }
    .await;
    let joined = cached_pair.shutdown().await;
    result?;
    joined?;
    reacquire(log_dir.path(), state_dir.path()).await
}
