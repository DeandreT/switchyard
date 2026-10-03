use cluster::{
    ExperimentalLogStore, MAX_APPEND_ENTRIES, MAX_LIMITED_ENTRIES, MAX_LOG_BODY_BYTES,
    MAX_RETAINED_ENTRIES,
};
use openraft::{
    RaftLogReader,
    storage::{RaftLogStorage, RaftLogStorageExt},
};
use storage::{CommittedStore, StateStore};

use super::{TestResult, fixture::*};

async fn append_count_body_and_encoded_byte_limits_refuse_before_owner_io<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    let before = control.reader().snapshot()?;
    assert!(
        store
            .blocking_append((0..=MAX_APPEND_ENTRIES as u64).map(blank))
            .await
            .is_err()
    );
    assert!(
        store
            .blocking_append([send(0, vec![0; MAX_LOG_BODY_BYTES + 1])?])
            .await
            .is_err()
    );
    let too_many_bytes = (0..MAX_APPEND_ENTRIES as u64)
        .map(|index| send(index, vec![1; MAX_LOG_BODY_BYTES / 2]))
        .collect::<TestResult<Vec<_>>>()?;
    assert!(store.blocking_append(too_many_bytes).await.is_err());
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(control.commits(), 1);
    assert_eq!(workload(&store, 0).await?.encoded_bytes, 0);
    let valid = (0..MAX_APPEND_ENTRIES as u64)
        .map(blank)
        .collect::<Vec<_>>();
    store.blocking_append(valid.clone()).await?;
    assert_eq!(store.try_get_log_entries(..).await?, valid);
    store.shutdown().await?;
    Ok(())
}

async fn retained_entry_capacity_is_exact_and_truncation_releases_it<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    let entries = (0..MAX_RETAINED_ENTRIES).map(blank).collect::<Vec<_>>();
    for chunk in entries.chunks(MAX_APPEND_ENTRIES) {
        store.blocking_append(chunk.to_vec()).await?;
    }
    let before = control.reader().snapshot()?;
    let commits = control.commits();
    assert!(
        store
            .blocking_append([blank(MAX_RETAINED_ENTRIES)])
            .await
            .is_err()
    );
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(control.commits(), commits);
    assert_eq!(store.try_get_log_entries(..).await?, entries);
    assert_eq!(
        store
            .limited_get_log_entries(0, MAX_RETAINED_ENTRIES)
            .await?
            .len(),
        MAX_LIMITED_ENTRIES
    );
    store.truncate(id(1, MAX_RETAINED_ENTRIES - 1)).await?;
    store
        .blocking_append([blank(MAX_RETAINED_ENTRIES - 1)])
        .await?;
    assert_eq!(store.try_get_log_entries(..).await?, entries);
    store.shutdown().await?;
    Ok(())
}

async fn limited_byte_prefix_and_retained_encoded_bytes_have_independent_caps<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)
        .map_err(|error| format!("byte-limit create: {error}"))?;
    for first in (0..255).step_by(15) {
        let entries = (first..first + 15)
            .map(|index| send(index, vec![0x41; MAX_LOG_BODY_BYTES]))
            .collect::<TestResult<Vec<_>>>()?;
        store
            .blocking_append(entries)
            .await
            .map_err(|error| format!("initial append chunk starting at {first}: {error}"))?;
    }
    let limited = store
        .limited_get_log_entries(0, 20)
        .await
        .map_err(|error| format!("limited byte-prefix read: {error}"))?;
    assert_eq!(limited.len(), 15);
    assert_eq!(limited.first().map(|entry| entry.log_id), Some(id(1, 0)));
    assert_eq!(limited.last().map(|entry| entry.log_id), Some(id(1, 14)));
    drop(limited);
    let before = control
        .reader()
        .snapshot()
        .map_err(|error| format!("before byte-cap refusal snapshot: {error}"))?;
    let commits = control.commits();
    assert!(
        store
            .blocking_append([send(255, vec![0x41; MAX_LOG_BODY_BYTES])?])
            .await
            .is_err()
    );
    assert_eq!(
        control
            .reader()
            .snapshot()
            .map_err(|error| format!("after byte-cap refusal snapshot: {error}"))?,
        before
    );
    assert_eq!(control.commits(), commits);
    let full = store
        .try_get_log_entries(..)
        .await
        .map_err(|error| format!("full retained byte-limit read: {error}"))?;
    assert_eq!(full.len(), 255);
    assert_eq!(full.last().map(|entry| entry.log_id), Some(id(1, 254)));
    assert_eq!(
        workload(&store, 0)
            .await
            .map_err(|error| format!("full-read admission release: {error}"))?
            .encoded_bytes,
        0
    );
    drop(full);
    drop(before);
    store
        .purge(id(1, 14))
        .await
        .map_err(|error| format!("purge freeing retained bytes: {error}"))?;
    store
        .blocking_append([send(255, vec![0x41; MAX_LOG_BODY_BYTES])?])
        .await
        .map_err(|error| format!("final append at 255 after purge: {error}"))?;
    store
        .shutdown()
        .await
        .map_err(|error| format!("byte-limit shutdown: {error}"))?;
    Ok(())
}

for_each_backend!(
    append_count_body_and_encoded_byte_limits_refuse_before_owner_io,
    retained_entry_capacity_is_exact_and_truncation_releases_it,
    limited_byte_prefix_and_retained_encoded_bytes_have_independent_caps,
);
