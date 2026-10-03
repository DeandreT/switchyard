use cluster::{
    ExperimentalStateMachine, LogApplication as A, MAX_APPLY_BYTES, MAX_LOG_BODY_BYTES,
    MAX_STATE_MACHINE_OWNER_JOBS, StateMachineError,
};
use domain::QueueConfig;
use openraft::storage::RaftStateMachine;
use storage::{CommittedStore, StateStore};

use super::{DEADLINE, TestResult, fixture::*};

async fn caller_loss_keeps_count_leases_and_blocking_io_off_the_async_executor<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    let before = control.reader().snapshot()?;
    let gate = control.gate_after(1);
    let mut apply = Box::pin(machine.apply([create(0, 1, QueueConfig::default())?]));
    pending(apply.as_mut()).await?;
    gate.entered().await?;
    drop(apply);
    let held = workload(&machine, 1).await?;
    let progress = tokio::spawn(async {
        tokio::task::yield_now().await;
        42
    });
    assert_eq!(tokio::time::timeout(DEADLINE, progress).await??, 42);
    assert_eq!(control.reader().snapshot()?, before);
    for _ in 1..MAX_STATE_MACHINE_OWNER_JOBS {
        let mut read = Box::pin(machine.applied_state());
        pending(read.as_mut()).await?;
        drop(read);
    }
    assert_eq!(
        workload(&machine, MAX_STATE_MACHINE_OWNER_JOBS)
            .await?
            .encoded_bytes,
        held.encoded_bytes + 64 * (MAX_STATE_MACHINE_OWNER_JOBS - 1)
    );
    assert!(machine.applied_state().await.is_err());
    assert_eq!(
        workload(&machine, MAX_STATE_MACHINE_OWNER_JOBS)
            .await?
            .accepted_jobs,
        MAX_STATE_MACHINE_OWNER_JOBS
    );
    assert_eq!(control.reader().snapshot()?, before);
    gate.release();
    workload(&machine, 0).await?;
    assert_eq!(machine.applied_state().await?.0, Some(id(1, 0)));
    assert_eq!(
        machine.apply([send(1, 2, vec![1])?]).await?,
        vec![A::Sent { sequence: 1 }]
    );
    machine.shutdown().await?;
    Ok(())
}

async fn large_packet_bytes_remain_charged_after_losing_the_apply_waiter<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    let before = control.reader().snapshot()?;
    let mut entries = vec![create(0, 1, QueueConfig::default())?];
    for index in 1..200 {
        entries.push(send(index, 2, vec![0x41; MAX_LOG_BODY_BYTES])?);
    }
    let gate = control.gate_after(1);
    let mut apply = Box::pin(machine.apply(entries));
    pending(apply.as_mut()).await?;
    gate.entered().await?;
    drop(apply);
    let held = workload(&machine, 1).await?;
    assert!(held.encoded_bytes > 199 * MAX_LOG_BODY_BYTES);
    assert!(held.encoded_bytes <= MAX_APPLY_BYTES);
    let next = (200..270)
        .map(|index| send(index, 2, vec![0x42; MAX_LOG_BODY_BYTES]))
        .collect::<TestResult<Vec<_>>>()?;
    assert!(machine.apply(next).await.is_err());
    assert_eq!(workload(&machine, 1).await?, held);
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(control.counts().commits, 2);
    gate.release();
    assert_eq!(machine.applied_state().await?.0, Some(id(1, 199)));
    assert_eq!(workload(&machine, 0).await?.encoded_bytes, 0);
    assert_eq!(
        message(&control.reader(), 199)?
            .ok_or("missing accepted large-packet tail")?
            .body,
        vec![0x41; MAX_LOG_BODY_BYTES]
    );
    assert_eq!(message(&control.reader(), 200)?, None);
    machine.shutdown().await?;
    Ok(())
}

async fn joined_shutdown_finishes_accepted_multi_entry_work_after_caller_loss<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    let gate = control.gate_after(2);
    let mut apply = Box::pin(machine.apply([
        create(0, 1, QueueConfig::default())?,
        send(1, 2, vec![1])?,
        send(2, 3, vec![2])?,
    ]));
    pending(apply.as_mut()).await?;
    gate.entered().await?;
    drop(apply);
    let charged = workload(&machine, 1).await?;
    assert!(charged.encoded_bytes > 0);
    assert_eq!(message(&control.reader(), 1)?, None);
    let mut shutdown = Box::pin(machine.shutdown());
    pending(shutdown.as_mut()).await?;
    gate.release();
    shutdown.await?;
    let mut reopened = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
    assert_eq!(reopened.applied_state().await?.0, Some(id(1, 2)));
    assert_eq!(
        message(&control.reader(), 1)?
            .ok_or("missing drained first allocation")?
            .body,
        vec![1]
    );
    assert_eq!(
        message(&control.reader(), 2)?
            .ok_or("missing drained next allocation")?
            .body,
        vec![2]
    );
    reopened.shutdown().await?;
    Ok(())
}

async fn owner_panic_fails_active_and_queued_work_without_discarding_a_durable_prefix<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    for fault in [Fault::PanicBefore, Fault::PanicAfter] {
        let before = control.reader().snapshot()?;
        let gate = control.gate_after(1);
        control.fault_after(1, fault);
        let mut apply = Box::pin(machine.apply([create(0, 1, QueueConfig::default())?]));
        pending(apply.as_mut()).await?;
        gate.entered().await?;
        drop(apply);
        let mut read = Box::pin(machine.applied_state());
        pending(read.as_mut()).await?;
        gate.release();
        assert!(read.await.is_err());
        assert_eq!(machine.shutdown().await, Err(StateMachineError::Panicked));
        let after = control.reader().snapshot()?;
        if matches!(fault, Fault::PanicBefore) {
            assert_eq!(after, before);
        } else {
            assert_ne!(after, before);
        }
        machine = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
        assert_eq!(
            machine.applied_state().await?.0,
            if matches!(fault, Fault::PanicAfter) {
                Some(id(1, 0))
            } else {
                None
            }
        );
    }
    machine.shutdown().await?;
    Ok(())
}

for_each_backend!(
    caller_loss_keeps_count_leases_and_blocking_io_off_the_async_executor,
    large_packet_bytes_remain_charged_after_losing_the_apply_waiter,
    joined_shutdown_finishes_accepted_multi_entry_work_after_caller_loss,
    owner_panic_fails_active_and_queued_work_without_discarding_a_durable_prefix,
);
