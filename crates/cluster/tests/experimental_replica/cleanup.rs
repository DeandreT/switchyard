use cluster::{
    ExperimentalLogStore, ExperimentalReplicaStores, ExperimentalStateMachine,
    ReplicaPreparationError as Error,
};
use openraft::{
    RaftLogReader,
    storage::{RaftLogStorage, RaftStateMachine},
};
use storage::{CommittedStore, StateStore};

use super::{DEADLINE, TestResult, fixture::*};

async fn owner_read_failure_or_panic_retires_both_adapters_before_failure_returns<
    W: CommittedStore,
>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let (mut log, mut state, log_control, state_control) =
        seed(log_writer, state_writer, &[initial()], 0).await?;
    for (on_log, panic) in [(false, false), (true, false), (false, true), (true, true)] {
        let snapshots = (
            log_control.reader().snapshot()?,
            state_control.reader().snapshot()?,
        );
        let commits = (log_control.commits(), state_control.commits());
        let mut old_log_reader = log.log_reader();
        let target = if on_log { &log_control } else { &state_control };
        if panic {
            target.panic_next_read();
        } else {
            target.error_next_read();
        }
        let result = ExperimentalReplicaStores::prepare(NODE, log, state).await;
        match result {
            Err(error) => {
                assert_eq!(
                    error,
                    if panic {
                        Error::OwnerFailure
                    } else {
                        Error::Storage
                    }
                );
                let text = error.to_string();
                assert!(!text.contains("private replica"));
                assert!(!text.contains("tenant"));
                assert!(!text.contains("orders"));
            }
            Ok(prepared) => {
                prepared.shutdown().await?;
                return Err("a failed owner was accepted".into());
            }
        }
        assert!(log_control.retired() && state_control.retired());
        assert!(old_log_reader.try_get_log_entries(..).await.is_err());
        assert_eq!((log_control.commits(), state_control.commits()), commits);
        assert_eq!(
            (
                log_control.reader().snapshot()?,
                state_control.reader().snapshot()?
            ),
            snapshots
        );
        log = ExperimentalLogStore::open(log_control.recover_writer(), profile()?)?;
        state = ExperimentalStateMachine::open(state_control.recover_writer(), stream()?)?;
    }
    let prepared = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
    prepared.shutdown().await?;
    Ok(())
}

async fn once_polled_preparation_survives_caller_loss_then_retires_unclaimed_pair<
    W: CommittedStore,
>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let (log, state, log_control, state_control) =
        seed(log_writer, state_writer, &[initial()], 0).await?;
    let mut old_log_reader = log.log_reader();
    let snapshots = (
        log_control.reader().snapshot()?,
        state_control.reader().snapshot()?,
    );
    let commits = (log_control.commits(), state_control.commits());
    let state_reads = state_control.reads();
    let gate = state_control.gate_next_read();
    let mut future = Box::pin(ExperimentalReplicaStores::prepare(NODE, log, state));
    pending(future.as_mut()).await?;
    gate.entered().await?;
    assert!(state_control.reads() > state_reads);
    drop(future);
    assert!(!log_control.retired() && !state_control.retired());
    assert_eq!(
        old_log_reader.try_get_log_entries(..).await?,
        vec![initial()]
    );
    assert_eq!((log_control.commits(), state_control.commits()), commits);
    gate.release();
    // The probes observe backing-writer retirement, not an independent native-thread join receipt.
    owners_retired(&log_control, &state_control).await?;
    assert!(old_log_reader.try_get_log_entries(..).await.is_err());
    assert_eq!((log_control.commits(), state_control.commits()), commits);
    assert_eq!(
        (
            log_control.reader().snapshot()?,
            state_control.reader().snapshot()?
        ),
        snapshots
    );
    let log = ExperimentalLogStore::open(log_control.recover_writer(), profile()?)?;
    let state = ExperimentalStateMachine::open(state_control.recover_writer(), stream()?)?;
    ExperimentalReplicaStores::prepare(NODE, log, state)
        .await?
        .shutdown()
        .await?;
    Ok(())
}

async fn log_owner_failure_still_waits_for_the_other_accepted_owner_query_to_finish<
    W: CommittedStore,
>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let (log, mut state, log_control, state_control) =
        seed(log_writer, state_writer, &[initial()], 0).await?;
    let snapshots = (
        log_control.reader().snapshot()?,
        state_control.reader().snapshot()?,
    );
    let commits = (log_control.commits(), state_control.commits());
    let mut old_log_reader = log.log_reader();
    let gate = state_control.gate_next_read();
    let mut query = Box::pin(state.applied_state());
    pending(query.as_mut()).await?;
    gate.entered().await?;
    drop(query);
    log_control.panic_next_read();
    let mut preparation = Box::pin(ExperimentalReplicaStores::prepare(NODE, log, state));
    pending(preparation.as_mut()).await?;
    tokio::time::timeout(DEADLINE, async {
        while !log_control.retired() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(old_log_reader.try_get_log_entries(..).await.is_err());
    assert!(!state_control.retired());
    pending(preparation.as_mut()).await?;
    assert_eq!((log_control.commits(), state_control.commits()), commits);
    assert_eq!(
        (
            log_control.reader().snapshot()?,
            state_control.reader().snapshot()?
        ),
        snapshots
    );
    gate.release();
    match preparation.await {
        Err(error) => assert_eq!(error, Error::OwnerFailure),
        Ok(prepared) => {
            prepared.shutdown().await?;
            return Err("the failed log owner was accepted".into());
        }
    }
    assert!(log_control.retired() && state_control.retired());
    assert_eq!((log_control.commits(), state_control.commits()), commits);
    assert_eq!(
        (
            log_control.reader().snapshot()?,
            state_control.reader().snapshot()?
        ),
        snapshots
    );
    let log = ExperimentalLogStore::open(log_control.recover_writer(), profile()?)?;
    let state = ExperimentalStateMachine::open(state_control.recover_writer(), stream()?)?;
    ExperimentalReplicaStores::prepare(NODE, log, state)
        .await?
        .shutdown()
        .await?;
    Ok(())
}

async fn prepared_drop_and_polled_shutdown_loss_keep_owned_cleanup_running<W: CommittedStore>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let (mut log, mut state, log_control, state_control) =
        seed(log_writer, state_writer, &[initial()], 0).await?;
    for lose_shutdown in [false, true] {
        let snapshots = (
            log_control.reader().snapshot()?,
            state_control.reader().snapshot()?,
        );
        let commits = (log_control.commits(), state_control.commits());
        let mut reader = log.log_reader();
        let prepared = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
        if lose_shutdown {
            let mut shutdown = Box::pin(prepared.shutdown());
            pending(shutdown.as_mut()).await?;
            drop(shutdown);
        } else {
            drop(prepared);
        }
        owners_retired(&log_control, &state_control).await?;
        assert!(reader.try_get_log_entries(..).await.is_err());
        assert_eq!((log_control.commits(), state_control.commits()), commits);
        assert_eq!(
            (
                log_control.reader().snapshot()?,
                state_control.reader().snapshot()?
            ),
            snapshots
        );
        log = ExperimentalLogStore::open(log_control.recover_writer(), profile()?)?;
        state = ExperimentalStateMachine::open(state_control.recover_writer(), stream()?)?;
    }
    ExperimentalReplicaStores::prepare(NODE, log, state)
        .await?
        .shutdown()
        .await?;
    Ok(())
}

async fn unpolled_preparation_drop_does_not_start_preflight_or_promise_joined_cleanup<
    W: CommittedStore,
>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let (log, state, log_control, state_control) =
        seed(log_writer, state_writer, &[initial()], 0).await?;
    let snapshots = (
        log_control.reader().snapshot()?,
        state_control.reader().snapshot()?,
    );
    let counts = (
        log_control.commits(),
        state_control.commits(),
        log_control.reads(),
        state_control.reads(),
    );
    drop(Box::pin(ExperimentalReplicaStores::prepare(
        NODE, log, state,
    )));
    owners_retired(&log_control, &state_control).await?;
    assert_eq!(
        (
            log_control.commits(),
            state_control.commits(),
            log_control.reads(),
            state_control.reads()
        ),
        counts
    );
    assert_eq!(
        (
            log_control.reader().snapshot()?,
            state_control.reader().snapshot()?
        ),
        snapshots
    );
    Ok(())
}

async fn lone_initial_vote_exception_ends_as_soon_as_that_membership_is_applied<
    W: CommittedStore,
>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let (mut log, state, log_control, state_control) =
        seed(log_writer, state_writer, &[initial()], 1).await?;
    let snapshots = (
        log_control.reader().snapshot()?,
        state_control.reader().snapshot()?,
    );
    let commits = (log_control.commits(), state_control.commits());
    match ExperimentalReplicaStores::prepare(NODE, log, state).await {
        Err(error) => assert_eq!(error, Error::VoteMismatch),
        Ok(prepared) => {
            prepared.shutdown().await?;
            return Err("uncommitted initial vote was accepted for applied membership".into());
        }
    }
    assert!(log_control.retired() && state_control.retired());
    assert_eq!((log_control.commits(), state_control.commits()), commits);
    assert_eq!(
        (
            log_control.reader().snapshot()?,
            state_control.reader().snapshot()?
        ),
        snapshots
    );
    log = ExperimentalLogStore::open(log_control.recover_writer(), profile()?)?;
    let state = ExperimentalStateMachine::open(state_control.recover_writer(), stream()?)?;
    log.save_vote(&cluster::LogVote::new_committed(0, 0))
        .await?;
    let prepared = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
    assert_eq!(prepared.progress().applied(), Some(Default::default()));
    prepared.shutdown().await?;
    Ok(())
}

for_each_backend!(
    owner_read_failure_or_panic_retires_both_adapters_before_failure_returns,
    once_polled_preparation_survives_caller_loss_then_retires_unclaimed_pair,
    log_owner_failure_still_waits_for_the_other_accepted_owner_query_to_finish,
    prepared_drop_and_polled_shutdown_loss_keep_owned_cleanup_running,
    unpolled_preparation_drop_does_not_start_preflight_or_promise_joined_cleanup,
    lone_initial_vote_exception_ends_as_soon_as_that_membership_is_applied,
);
