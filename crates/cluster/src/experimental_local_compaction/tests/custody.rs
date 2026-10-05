use super::{fixture::*, observed::Target, *};
use storage::StateStore;

pub(super) async fn caller_loss_and_busy<L, S>(log_writer: L, state_writer: S) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    let (log, state, _, control) = seed(log_writer, state_writer, 2, 4).await?;
    let reader = log.test_reader();
    let mut pair = pair(log, state).await?;
    let gate = control.gate(Target::Catalog);
    let waiter = tokio::spawn(pair.compact());
    if let Err(error) = gate.entered().await {
        gate.release();
        let _ = waiter.await;
        pair.shutdown().await?;
        return Err(error.into());
    }
    waiter.abort();
    let _ = waiter.await;
    let result: TestResult = async {
        assert_eq!(pair.compact().await.err(), Some(LocalCompactionError::Busy));
        assert_eq!(
            reader.vote().await?,
            Some(crate::LogVote::new_committed(1, 7))
        );
        assert_eq!(reader.read(0, u64::MAX).await?.len(), 5);
        assert_eq!(
            control
                .catalog_commits
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        Ok(())
    }
    .await;
    // Close wins before the destructive claim, but must still drain the source.
    let mut shutdown = Box::pin(pair.shutdown());
    let ready = probe(shutdown.as_mut()).await;
    gate.release();
    let joined = match ready {
        Some(joined) => joined,
        None => shutdown.await,
    };
    result?;
    joined?;
    assert_eq!(
        reader.read(0, u64::MAX).await.err(),
        Some(LocalCompactionError::Closed)
    );
    Ok(())
}

pub(super) async fn close_before_claim<L, S>(log_writer: L, state_writer: S) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    let log_source = log_writer.reader();
    let (log, state, log_control, state_control) = seed(log_writer, state_writer, 2, 4).await?;
    let setup: TestResult<_> = log_source.snapshot().map_err(Into::into);
    let (log, state, before) = sources_or_cleanup(setup, log, state).await?;
    let mut pair = pair(log, state).await?;
    let gate = state_control.gate(Target::Catalog);
    let waiter = tokio::spawn(pair.compact());
    if let Err(error) = gate.entered().await {
        gate.release();
        let _ = waiter.await;
        pair.shutdown().await?;
        return Err(error.into());
    }
    let commits = log_control
        .commits
        .load(std::sync::atomic::Ordering::SeqCst);
    let mut shutdown = Box::pin(pair.shutdown());
    let ready = probe(shutdown.as_mut()).await;
    let pending = ready.is_none();
    gate.release();
    let completed = waiter.await;
    let joined = match ready {
        Some(result) => result,
        None => shutdown.await,
    };
    assert!(pending);
    assert_eq!(completed?.err(), Some(LocalCompactionError::Closed));
    joined?;
    assert_eq!(log_source.snapshot()?, before);
    assert_eq!(
        log_control
            .commits
            .load(std::sync::atomic::Ordering::SeqCst),
        commits
    );
    Ok(())
}

pub(super) async fn close_after_claim<L, S>(log_writer: L, state_writer: S) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    let log_source = log_writer.reader();
    let (log, state, control, _) = seed(log_writer, state_writer, 2, 4).await?;
    let mut pair = pair(log, state).await?;
    let gate = control.gate(Target::Commit);
    let waiter = tokio::spawn(pair.compact());
    if let Err(error) = gate.entered().await {
        gate.release();
        let _ = waiter.await;
        pair.shutdown().await?;
        return Err(error.into());
    }
    let mut shutdown = Box::pin(pair.shutdown());
    let ready = probe(shutdown.as_mut()).await;
    let pending = ready.is_none();
    gate.release();
    let completed = waiter.await;
    let joined = match ready {
        Some(result) => result,
        None => shutdown.await,
    };
    assert!(pending);
    assert_eq!(completed??.ordinal(), 1);
    joined?;
    assert!(
        log_source
            .snapshot()?
            .entries()
            .iter()
            .all(|(key, _)| key[0] != 0x10 || u64::from_be_bytes(key[1..].try_into().unwrap()) > 2)
    );
    Ok(())
}
