use super::{fixture::*, *};
use crate::LogEntry;
use openraft::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use storage::StateStore;

pub(super) async fn whole_prefix_and_cached_no_write<L, S>(
    log_writer: L,
    state_writer: S,
) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    let source = state_writer.reader();
    let log_source = log_writer.reader();
    let (log, state, log_control, state_control) = seed(log_writer, state_writer, 2, 4).await?;
    let reader = log.test_reader();
    let setup: TestResult<_> = async { Ok((source.snapshot()?, state.checkpoint().await?)) }.await;
    let (log, state, (original, checkpoint)) = sources_or_cleanup(setup, log, state).await?;
    let mut pair = pair(log, state).await?;
    let result: TestResult = async {
        let before_log = log_control
            .commits
            .load(std::sync::atomic::Ordering::SeqCst);
        let before_capture = state_control
            .captures
            .load(std::sync::atomic::Ordering::SeqCst);
        let before_retain = state_control
            .catalog_commits
            .load(std::sync::atomic::Ordering::SeqCst);
        let before_state_reads = state_control
            .reads
            .load(std::sync::atomic::Ordering::SeqCst);
        let progress = pair.compact().await?;
        assert_eq!(
            log_control
                .commits
                .load(std::sync::atomic::Ordering::SeqCst),
            before_log + 1
        );
        assert_eq!(
            state_control
                .captures
                .load(std::sync::atomic::Ordering::SeqCst),
            before_capture + 1
        );
        assert_eq!(
            state_control
                .catalog_commits
                .load(std::sync::atomic::Ordering::SeqCst),
            before_retain + 1
        );
        assert_eq!(
            state_control
                .reads
                .load(std::sync::atomic::Ordering::SeqCst),
            before_state_reads
        );
        assert_eq!(progress.checkpoint(), &checkpoint);
        assert_eq!(progress.ordinal(), 1);
        assert_eq!(progress.retained_entries(), 2);
        assert_eq!(
            reader
                .read(0, u64::MAX)
                .await?
                .iter()
                .map(|entry| entry.log_id.index)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert_eq!(source.snapshot()?, original);
        let log_after = log_source.snapshot()?;
        let commits = log_control
            .commits
            .load(std::sync::atomic::Ordering::SeqCst);
        let captures = state_control
            .captures
            .load(std::sync::atomic::Ordering::SeqCst);
        let retains = state_control
            .catalog_commits
            .load(std::sync::atomic::Ordering::SeqCst);
        let log_reads = log_control.reads.load(std::sync::atomic::Ordering::SeqCst);
        let state_reads = state_control
            .reads
            .load(std::sync::atomic::Ordering::SeqCst);
        let catalog_reads = state_control
            .catalog_reads
            .load(std::sync::atomic::Ordering::SeqCst);
        let repeated = pair.compact().await?;
        assert_eq!(repeated.ordinal(), 1);
        assert_eq!(repeated.snapshot_meta(), progress.snapshot_meta());
        assert_eq!(log_source.snapshot()?, log_after);
        assert_eq!(
            log_control
                .commits
                .load(std::sync::atomic::Ordering::SeqCst),
            commits
        );
        assert_eq!(
            state_control
                .captures
                .load(std::sync::atomic::Ordering::SeqCst),
            captures
        );
        assert_eq!(
            state_control
                .catalog_commits
                .load(std::sync::atomic::Ordering::SeqCst),
            retains
        );
        assert_eq!(
            log_control.reads.load(std::sync::atomic::Ordering::SeqCst),
            log_reads
        );
        assert_eq!(
            state_control
                .reads
                .load(std::sync::atomic::Ordering::SeqCst),
            state_reads
        );
        assert_eq!(
            state_control
                .catalog_reads
                .load(std::sync::atomic::Ordering::SeqCst),
            catalog_reads
        );
        Ok(())
    }
    .await;
    let joined = pair.shutdown().await;
    result?;
    joined?;
    Ok(())
}

pub(super) async fn retained_generic_builder_is_denied<L, S>(
    log_writer: L,
    state_writer: S,
) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    let (log, mut state, _, control) = seed(log_writer, state_writer, 2, 4).await?;
    let old_builder = state.build_create_send_catalog();
    let mut old_standalone_builder = state.create_send_snapshot_builder();
    let mut pair = pair(log, state).await?;
    let before = control
        .catalog_commits
        .load(std::sync::atomic::Ordering::SeqCst);
    let result: TestResult = async {
        assert_eq!(
            old_builder.await.err(),
            Some(crate::StateMachineCatalogError::Owner(
                crate::StateMachineError::Closed
            ))
        );
        assert!(old_standalone_builder.build_snapshot().await.is_err());
        assert_eq!(
            control
                .catalog_commits
                .load(std::sync::atomic::Ordering::SeqCst),
            before
        );
        let progress = pair.compact().await?;
        assert_eq!(progress.ordinal(), 1);
        Ok(())
    }
    .await;
    let joined = pair.shutdown().await;
    result?;
    joined?;
    Ok(())
}

pub(super) async fn content_mismatch_refuses_before_retention<L, S>(
    log_writer: L,
    state_writer: S,
) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    let (log, mut state, _, control) = seed(log_writer, state_writer, 1, 2).await?;
    let result: TestResult = async {
        state
            .apply([LogEntry {
                log_id: id(2),
                payload: openraft::EntryPayload::Normal(crate::QueueLogCommand::send(
                    domain::NamespaceName::new("tenant")?,
                    domain::EntityPath::new("orders")?,
                    domain::Timestamp::from_millis(12),
                    domain::CommittedSend {
                        message_id: "PRIVATE-wrong".into(),
                        body: b"different".to_vec(),
                        time_to_live_millis: None,
                        session_id: None,
                    },
                )),
            }])
            .await?;
        Ok(())
    }
    .await;
    if let Err(error) = result {
        let (a, b) = tokio::join!(log.shutdown(), state.shutdown());
        a?;
        b?;
        return Err(error);
    }
    let refused = pair(log, state).await;
    assert_eq!(
        refused
            .err()
            .and_then(|error| error.downcast::<LocalCompactionError>().ok())
            .map(|error| *error),
        Some(LocalCompactionError::InvalidHistory)
    );
    assert_eq!(
        control
            .catalog_commits
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    Ok(())
}
