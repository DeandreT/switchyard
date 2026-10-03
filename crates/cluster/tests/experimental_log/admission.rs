use std::{future::Future, pin::Pin};

use cluster::{ExperimentalLogStore, LogStorageError, MAX_LOG_BODY_BYTES, MAX_LOG_OWNER_JOBS};
use openraft::{RaftLogReader, storage::RaftLogStorageExt};
use storage::{CommittedStore, StateStore};

use super::{TestResult, fixture::*};

type ReadFuture = Pin<
    Box<dyn Future<Output = Result<Vec<cluster::LogEntry>, openraft::StorageError<u64>>> + Send>,
>;

async fn queued_caller_loss_retains_count_capacity_until_actual_completion<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    let before = control.reader().snapshot()?;
    let reader = store.log_reader();
    let gate = control.gate();
    let mut append = Box::pin(store.blocking_append([blank(0)]));
    pending(append.as_mut()).await?;
    gate.entered().await?;
    drop(append);
    let held = workload(&store, 1).await?;

    let mut reads: Vec<ReadFuture> = Vec::new();
    for _ in 1..MAX_LOG_OWNER_JOBS {
        let mut reader = reader.clone();
        let mut read: ReadFuture = Box::pin(async move { reader.try_get_log_entries(..).await });
        pending(read.as_mut()).await?;
        reads.push(read);
    }
    assert_eq!(
        workload(&store, MAX_LOG_OWNER_JOBS).await?.encoded_bytes,
        held.encoded_bytes + 64 * (MAX_LOG_OWNER_JOBS - 1)
    );
    let mut rejected = reader.clone();
    assert!(rejected.try_get_log_entries(..).await.is_err());
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(control.commits(), 2);
    drop(reads);
    assert_eq!(
        workload(&store, MAX_LOG_OWNER_JOBS).await?.accepted_jobs,
        MAX_LOG_OWNER_JOBS
    );
    assert!(rejected.try_get_log_entries(..).await.is_err());
    gate.release();
    workload(&store, 0).await?;
    assert_eq!(store.try_get_log_entries(..).await?, vec![blank(0)]);
    store.shutdown().await?;
    Ok(())
}

async fn queued_encoded_bytes_remain_charged_after_caller_loss<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    let before = control.reader().snapshot()?;
    let entries = (0..10)
        .map(|index| send(index, vec![0x61; MAX_LOG_BODY_BYTES]))
        .collect::<TestResult<Vec<_>>>()?;
    let gate = control.gate();
    let mut append = Box::pin(store.blocking_append(entries.clone()));
    pending(append.as_mut()).await?;
    gate.entered().await?;
    drop(append);
    let held = workload(&store, 1).await?;
    assert!(held.encoded_bytes > 10 * MAX_LOG_BODY_BYTES);
    let next = (10..17)
        .map(|index| send(index, vec![0x62; MAX_LOG_BODY_BYTES]))
        .collect::<TestResult<Vec<_>>>()?;
    assert!(store.blocking_append(next).await.is_err());
    assert_eq!(workload(&store, 1).await?, held);
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(control.commits(), 2);
    gate.release();
    workload(&store, 0).await?;
    assert_eq!(store.try_get_log_entries(..).await?, entries);
    store.blocking_append([blank(10)]).await?;
    store.shutdown().await?;
    Ok(())
}

async fn owner_panics_fail_current_and_queued_callbacks_without_success<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    for fault in [Fault::PanicBefore, Fault::PanicAfter] {
        let before = control.reader().snapshot()?;
        let reader = store.log_reader();
        let gate = control.gate();
        control.fault(fault);
        let mut append = Box::pin(store.blocking_append([blank(0)]));
        pending(append.as_mut()).await?;
        gate.entered().await?;
        let mut queued_reader = reader.clone();
        let mut read = Box::pin(async move { queued_reader.try_get_log_entries(..).await });
        pending(read.as_mut()).await?;
        gate.release();
        assert!(append.await.is_err());
        assert!(read.await.is_err());
        assert_eq!(store.shutdown().await, Err(LogStorageError::Panicked));
        let after = control.reader().snapshot()?;
        match fault {
            Fault::PanicBefore => assert_eq!(after, before),
            Fault::PanicAfter => assert_ne!(after, before),
            _ => return Err("unexpected panic test fault".into()),
        }
        store = ExperimentalLogStore::open(control.recover_writer(), profile()?)?;
        let expected = if matches!(fault, Fault::PanicAfter) {
            vec![blank(0)]
        } else {
            Vec::new()
        };
        assert_eq!(store.try_get_log_entries(..).await?, expected);
    }
    store.shutdown().await?;
    Ok(())
}

async fn shutdown_joins_accepted_work_even_when_its_original_waiter_is_gone<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    let reader = store.log_reader();
    let before = control.reader().snapshot()?;
    let gate = control.gate();
    let mut append = Box::pin(store.blocking_append([blank(0)]));
    pending(append.as_mut()).await?;
    gate.entered().await?;
    drop(append);
    let mut shutdown = Box::pin(store.shutdown());
    pending(shutdown.as_mut()).await?;
    let mut reader = reader;
    assert!(reader.try_get_log_entries(..).await.is_err());
    assert_eq!(control.reader().snapshot()?, before);
    gate.release();
    shutdown.await?;
    let mut reopened = ExperimentalLogStore::open(control.recover_writer(), profile()?)?;
    assert_eq!(reopened.try_get_log_entries(..).await?, vec![blank(0)]);
    reopened.shutdown().await?;
    Ok(())
}

for_each_backend!(
    queued_caller_loss_retains_count_capacity_until_actual_completion,
    queued_encoded_bytes_remain_charged_after_caller_loss,
    owner_panics_fail_current_and_queued_callbacks_without_success,
    shutdown_joins_accepted_work_even_when_its_original_waiter_is_gone,
);
