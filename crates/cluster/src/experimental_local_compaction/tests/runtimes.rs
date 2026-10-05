use super::{fixture::*, observed::Target, *};
use crate::experimental_local_compaction::owner;
use std::{
    future::Future,
    task::{Context, Waker},
    time::Duration,
};

#[tokio::test]
async fn distinct_current_runtime_death_before_worker_poll_joins_on_live_fallback() -> TestResult {
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
        log_control.reads.load(std::sync::atomic::Ordering::SeqCst),
        state_control
            .reads
            .load(std::sync::atomic::Ordering::SeqCst),
        state_control
            .catalog_reads
            .load(std::sync::atomic::Ordering::SeqCst),
    );
    let (future, exit) = owner::prepare_observed(7, log, state, tokio::runtime::Handle::current());
    let polled = tokio::task::spawn_blocking(move || -> Result<bool, std::io::Error> {
        let current = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let entered = current.enter();
        let mut future = Box::pin(future);
        let pending = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();
        drop(entered);
        // No event loop ever drives the worker queued by the accepted first poll.
        drop(current);
        drop(future);
        Ok(pending)
    })
    .await;
    let joined = wait_exit(exit).await;
    assert!(polled??);
    assert_eq!(joined?, Err(LocalCompactionError::TaskFailed));
    assert_eq!(
        before,
        (
            log_control.reads.load(std::sync::atomic::Ordering::SeqCst),
            state_control
                .reads
                .load(std::sync::atomic::Ordering::SeqCst),
            state_control
                .catalog_reads
                .load(std::sync::atomic::Ordering::SeqCst)
        )
    );
    super::reopen::reacquire(log_dir.path(), state_dir.path()).await?;
    super::reopen::reacquire(log_dir.path(), state_dir.path()).await
}

struct AlternateRuntime {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<(), std::io::Error>>>,
}
impl AlternateRuntime {
    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
    async fn joined(&mut self) -> TestResult {
        self.stop();
        let task = self.task.as_mut().ok_or("missing alternate runtime task")?;
        let result = task.await;
        self.task = None;
        result??;
        Ok(())
    }
}
impl Drop for AlternateRuntime {
    fn drop(&mut self) {
        self.stop();
    }
}

#[tokio::test]
async fn distinct_current_runtime_death_during_cleanup_preserves_both_fallback_joins() -> TestResult
{
    let log_dir = testkit::DurableProvider::temporary()?;
    let state_dir = testkit::DurableProvider::temporary()?;
    let (log, mut state, _, control) = seed(
        FjallReplicaStore::open(log_dir.path())?,
        FjallCatalogReplicaStore::open(state_dir.path())?,
        2,
        4,
    )
    .await?;
    let old_read = state.read_create_send_catalog();
    let (future, observation) =
        owner::prepare_observed_with_cleanup(7, log, state, tokio::runtime::Handle::current());
    let (ready, receiver) = tokio::sync::oneshot::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
        let current = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        current.block_on(async move {
            let _ = ready.send(future.await);
            let _ = stopped.await;
        });
        drop(current);
        Ok(())
    });
    let mut alternate = AlternateRuntime {
        stop: Some(stop),
        task: Some(task),
    };
    let pair = match receiver.await {
        Ok(Ok(pair)) => pair,
        result => {
            let joined = alternate.joined().await;
            let drained = wait_exit(observation.exit).await;
            joined?;
            let _ = drained?;
            return Err(format!(
                "alternate preparation did not complete: {}",
                match result {
                    Ok(Err(error)) => error.to_string(),
                    _ => "reply lost".into(),
                }
            )
            .into());
        }
    };
    let gate = control.gate(Target::CatalogRead);
    let reader = tokio::spawn(old_read);
    if let Err(error) = gate.entered().await {
        gate.release();
        let read = reader.await;
        let joined = pair.shutdown().await;
        let alternate_joined = alternate.joined().await;
        read??;
        let _ = joined;
        alternate_joined?;
        return Err(error.into());
    }
    let mut shutdown = Box::pin(pair.shutdown());
    let started = tokio::time::timeout(Duration::from_secs(5), observation.started).await;
    if !matches!(started, Ok(Ok(()))) {
        gate.release();
        let read = reader.await;
        let joined = shutdown.await;
        let alternate_joined = alternate.joined().await;
        read??;
        let _ = joined;
        alternate_joined?;
        return Err("alternate cleanup did not publish its bounded start marker".into());
    }
    // BOTH tokens have moved into explicit fallback tasks while the state owner
    // is still blocked. Current-runtime death must not own or detach those joins.
    alternate.stop();
    let ended = tokio::time::timeout(Duration::from_secs(5), alternate.joined()).await;
    let ended_without_releasing_gate = ended.is_ok();
    let pending = probe(shutdown.as_mut()).await;
    let waited_for_state_owner = pending.is_none();
    gate.release();
    let read = reader.await;
    let joined = match pending {
        Some(result) => result,
        None => shutdown.await,
    };
    let drained = wait_exit(observation.exit).await;
    if let Ok(result) = ended {
        result?;
    } else {
        alternate.joined().await?;
    }
    read??;
    assert!(ended_without_releasing_gate);
    assert!(waited_for_state_owner);
    assert_eq!(joined, Err(LocalCompactionError::TaskFailed));
    assert_eq!(drained?, Err(LocalCompactionError::TaskFailed));
    super::reopen::reacquire(log_dir.path(), state_dir.path()).await?;
    super::reopen::reacquire(log_dir.path(), state_dir.path()).await
}
