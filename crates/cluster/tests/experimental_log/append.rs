use cluster::{ExperimentalLogStore, LogVote};
use openraft::{
    RaftLogReader,
    storage::{RaftLogStorage, RaftLogStorageExt},
};
use storage::{CommittedStore, StateStore};

use super::{TestResult, fixture::*};

async fn blocking_append_success_follows_actual_durable_completion<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    let before = control.reader().snapshot()?;
    let gate = control.gate();
    let entry = send(0, b"exact durable entry".to_vec())?;
    let mut append = Box::pin(store.blocking_append([entry.clone()]));
    pending(append.as_mut()).await?;
    gate.entered().await?;
    pending(append.as_mut()).await?;
    assert_eq!(control.reader().snapshot()?, before);
    gate.release();
    append.await?;
    assert_eq!(store.try_get_log_entries(..).await?, vec![entry]);
    assert_eq!(store.get_log_state().await?.last_log_id, Some(id(1, 0)));
    assert_eq!(control.commits(), 2);
    assert_eq!(workload(&store, 0).await?.encoded_bytes, 0);
    store.shutdown().await?;
    Ok(())
}

async fn actual_blocking_append_waits_for_commit_and_preserves_fifo_after_caller_loss<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    let before = control.reader().snapshot()?;
    let gate = control.gate();
    let mut append =
        Box::pin(store.blocking_append([send(0, b"retained through caller loss".to_vec())?]));
    pending(append.as_mut()).await?;
    gate.entered().await?;
    pending(append.as_mut()).await?;
    assert_eq!(control.reader().snapshot()?, before);
    drop(append);
    let held = workload(&store, 1).await?;
    assert!(held.encoded_bytes > 0);

    let vote = LogVote::new_committed(3, 7);
    let mut save = Box::pin(store.save_vote(&vote));
    pending(save.as_mut()).await?;
    drop(save);
    assert_eq!(
        workload(&store, 2).await?.encoded_bytes,
        held.encoded_bytes + 64
    );
    assert_eq!(control.reader().snapshot()?, before);
    gate.release();
    assert_eq!(store.read_vote().await?, Some(vote));
    assert_eq!(store.get_log_state().await?.last_log_id, Some(id(1, 0)));
    assert_eq!(
        store.try_get_log_entries(..).await?,
        vec![send(0, b"retained through caller loss".to_vec())?]
    );
    assert_eq!(workload(&store, 0).await?.encoded_bytes, 0);
    assert_eq!(control.commits(), 3);
    store.shutdown().await?;
    let mut reopened = ExperimentalLogStore::open(control.recover_writer(), profile()?)?;
    assert_eq!(reopened.read_vote().await?, Some(vote));
    assert_eq!(reopened.get_log_state().await?.last_log_id, Some(id(1, 0)));
    reopened.shutdown().await?;
    Ok(())
}

async fn append_commit_failures_never_report_success_and_poison_the_owner<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    store.blocking_append([blank(0)]).await?;
    let before = control.reader().snapshot()?;
    control.fault(Fault::Before);
    assert!(
        store
            .blocking_append([send(1, b"before".to_vec())?])
            .await
            .is_err()
    );
    assert_eq!(control.reader().snapshot()?, before);
    let commits = control.commits();
    assert!(store.read_vote().await.is_err());
    assert!(store.blocking_append([blank(1)]).await.is_err());
    assert_eq!(control.commits(), commits);
    store.shutdown().await?;

    let mut store = ExperimentalLogStore::open(control.recover_writer(), profile()?)?;
    store
        .blocking_append([send(1, b"before".to_vec())?])
        .await?;
    control.fault(Fault::After);
    assert!(
        store
            .blocking_append([send(2, b"after".to_vec())?])
            .await
            .is_err()
    );
    let physical = control.reader().snapshot()?;
    assert_ne!(physical, before);
    let commits = control.commits();
    assert!(store.try_get_log_entries(..).await.is_err());
    assert_eq!(control.commits(), commits);
    store.shutdown().await?;

    let mut reopened = ExperimentalLogStore::open(control.recover_writer(), profile()?)?;
    let entries = vec![
        blank(0),
        send(1, b"before".to_vec())?,
        send(2, b"after".to_vec())?,
    ];
    assert_eq!(reopened.try_get_log_entries(..).await?, entries);
    let commits = control.commits();
    reopened.blocking_append([entries[2].clone()]).await?;
    assert_eq!(control.commits(), commits);
    assert_eq!(control.reader().snapshot()?, physical);
    reopened.shutdown().await?;
    Ok(())
}

for_each_backend!(
    blocking_append_success_follows_actual_durable_completion,
    actual_blocking_append_waits_for_commit_and_preserves_fifo_after_caller_loss,
    append_commit_failures_never_report_success_and_poison_the_owner,
);
