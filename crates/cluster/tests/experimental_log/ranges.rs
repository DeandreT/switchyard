use std::ops::Bound;

use cluster::{ExperimentalLogStore, LogEntry, LogVote};
use openraft::{
    EntryPayload, RaftLogReader,
    storage::{RaftLogStorage, RaftLogStorageExt},
};
use storage::{CommittedStore, StateStore};

use super::{TestResult, fixture::*};

async fn append_conflicts_gaps_overlap_and_atomic_truncate_purge<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    store
        .blocking_append([blank(0), send(1, b"original".to_vec())?, blank(2)])
        .await?;
    store
        .blocking_append([send(1, b"original".to_vec())?, blank(2), blank(3)])
        .await?;
    let before = control.reader().snapshot()?;
    let commits = control.commits();
    store.blocking_append([blank(2), blank(3)]).await?;
    assert_eq!(control.commits(), commits);
    for entries in [
        vec![send(1, b"conflict".to_vec())?],
        vec![blank(5)],
        vec![blank(4), blank(6)],
        vec![LogEntry {
            log_id: id(0, 4),
            payload: EntryPayload::Blank,
        }],
    ] {
        assert!(store.blocking_append(entries).await.is_err());
        assert_eq!(control.commits(), commits);
        assert_eq!(control.reader().snapshot()?, before);
    }
    let vote = LogVote::new_committed(4, 7);
    store.save_vote(&vote).await?;
    let commits = control.commits();
    store.truncate(id(1, 2)).await?;
    assert_eq!(control.commits(), commits + 1);
    assert_eq!(control.last_batch().mutations().len(), 3);
    assert_eq!(
        store.try_get_log_entries(..).await?,
        vec![blank(0), send(1, b"original".to_vec())?]
    );
    assert_eq!(store.get_log_state().await?.last_log_id, Some(id(1, 1)));
    assert_eq!(store.read_vote().await?, Some(vote));
    store
        .blocking_append([LogEntry {
            log_id: id(2, 2),
            payload: EntryPayload::Blank,
        }])
        .await?;
    store.purge(id(1, 0)).await?;
    let state = store.get_log_state().await?;
    assert_eq!(state.last_purged_log_id, Some(id(1, 0)));
    assert_eq!(state.last_log_id, Some(id(2, 2)));
    let before = control.reader().snapshot()?;
    let commits = control.commits();
    assert!(store.truncate(id(1, 0)).await.is_err());
    assert!(store.purge(id(3, 0)).await.is_err());
    assert!(store.blocking_append([blank(0)]).await.is_err());
    assert_eq!(control.commits(), commits);
    assert_eq!(control.reader().snapshot()?, before);
    store.purge(id(3, 20)).await?;
    assert!(store.try_get_log_entries(..).await?.is_empty());
    let state = store.get_log_state().await?;
    assert_eq!(state.last_purged_log_id, Some(id(3, 20)));
    assert_eq!(state.last_log_id, Some(id(3, 20)));
    assert_eq!(store.read_vote().await?, Some(vote));
    let commits = control.commits();
    store.purge(id(3, 20)).await?;
    store.purge(id(1, 1)).await?;
    assert_eq!(control.commits(), commits);
    let next = LogEntry {
        log_id: id(3, 21),
        payload: EntryPayload::Blank,
    };
    store.blocking_append([next.clone()]).await?;
    let snapshot = control.reader().snapshot()?;
    store.shutdown().await?;
    let mut reopened = ExperimentalLogStore::open(control.recover_writer(), profile()?)?;
    assert_eq!(reopened.try_get_log_entries(..).await?, vec![next]);
    assert_eq!(
        reopened.get_log_state().await?.last_purged_log_id,
        Some(id(3, 20))
    );
    assert_eq!(control.reader().snapshot()?, snapshot);
    reopened.shutdown().await?;
    Ok(())
}

async fn full_ranges_are_complete_limited_ranges_are_nonempty_and_max_is_checked<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    let entries = (0..70).map(blank).collect::<Vec<_>>();
    for chunk in entries.chunks(32) {
        store.blocking_append(chunk.to_vec()).await?;
    }
    assert_eq!(store.try_get_log_entries(..).await?, entries);
    assert_eq!(store.try_get_log_entries(4..8).await?, entries[4..8]);
    assert_eq!(store.try_get_log_entries(4..=8).await?, entries[4..=8]);
    assert!(store.try_get_log_entries(8..8).await?.is_empty());
    assert_eq!(store.limited_get_log_entries(0, 70).await?, entries[..32]);
    assert_eq!(
        store.limited_get_log_entries(69, 70).await?,
        vec![blank(69)]
    );
    assert!(store.limited_get_log_entries(70, 70).await?.is_empty());
    assert!(store.limited_get_log_entries(71, 72).await.is_err());

    store.purge(id(2, u64::MAX - 1)).await?;
    let last = LogEntry {
        log_id: id(2, u64::MAX),
        payload: EntryPayload::Blank,
    };
    store.blocking_append([last.clone()]).await?;
    assert_eq!(
        store.try_get_log_entries(u64::MAX..=u64::MAX).await?,
        vec![last.clone()]
    );
    assert_eq!(
        store.try_get_log_entries(..=u64::MAX).await?,
        vec![last.clone()]
    );
    assert!(
        store
            .try_get_log_entries((Bound::Excluded(u64::MAX), Bound::Unbounded))
            .await?
            .is_empty()
    );
    assert!(store.try_get_log_entries(..u64::MAX).await?.is_empty());
    let before = control.reader().snapshot()?;
    let commits = control.commits();
    assert!(store.blocking_append([blank(0)]).await.is_err());
    assert_eq!(control.commits(), commits);
    assert_eq!(control.reader().snapshot()?, before);
    store.shutdown().await?;
    let mut reopened = ExperimentalLogStore::open(control.recover_writer(), profile()?)?;
    assert_eq!(reopened.try_get_log_entries(..).await?, vec![last]);
    reopened.shutdown().await?;
    Ok(())
}

for_each_backend!(
    append_conflicts_gaps_overlap_and_atomic_truncate_purge,
    full_ranges_are_complete_limited_ranges_are_nonempty_and_max_is_checked,
);
