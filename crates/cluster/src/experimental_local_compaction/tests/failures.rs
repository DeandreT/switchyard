use super::{fixture::*, observed::Mode, *};
use storage::StateStore;

async fn failed<L, S>(log_writer: L, state_writer: S, source: bool, mode: Mode) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    let log_reader = log_writer.reader();
    let business = state_writer.reader();
    let (log, state, log_control, state_control) = seed(log_writer, state_writer, 2, 4).await?;
    let setup: TestResult<_> = (|| Ok((log_reader.snapshot()?, business.snapshot()?)))();
    let (log, state, (old_log, old_business)) = sources_or_cleanup(setup, log, state).await?;
    let mut pair = pair(log, state).await?;
    if source {
        state_control.fail(mode);
    } else {
        log_control.fail(mode);
    }
    let result = pair.compact().await;
    let joined = pair.shutdown().await;
    assert_eq!(business.snapshot()?, old_business);
    if matches!(mode, Mode::Panic) {
        assert_eq!(result.err(), Some(LocalCompactionError::OwnerFailure));
        assert_eq!(joined, Err(LocalCompactionError::OwnerFailure));
    } else {
        assert_eq!(result.err(), Some(LocalCompactionError::CommitUnknown));
        assert_eq!(joined, Err(LocalCompactionError::CommitUnknown));
    }
    if source || matches!(mode, Mode::Before | Mode::Panic) {
        assert_eq!(log_reader.snapshot()?, old_log);
    } else {
        assert_ne!(log_reader.snapshot()?, old_log);
    }
    Ok(())
}
pub(super) async fn catalog_unknown_before<L, S>(l: L, s: S) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    failed(l, s, true, Mode::Before).await
}
pub(super) async fn catalog_unknown_after<L, S>(l: L, s: S) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    failed(l, s, true, Mode::After).await
}
pub(super) async fn log_unknown_before<L, S>(l: L, s: S) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    failed(l, s, false, Mode::Before).await
}
pub(super) async fn log_unknown_after<L, S>(l: L, s: S) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    failed(l, s, false, Mode::After).await
}
pub(super) async fn log_panic_joins_both<L, S>(l: L, s: S) -> TestResult
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    failed(l, s, false, Mode::Panic).await
}
