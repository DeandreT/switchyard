use domain::{
    BrokerError, CommittedApplyError, CommittedStateMachine, FiniteQueueCapacity,
    QueueCapacityCommandV1, QueueConfig, StateMachine, Timestamp, keys,
};
use storage::{CommittedStore, StateStore, WriteBatch};

use super::{TestResult, fixture::*};

fn finite_owner_is_outside_legacy_committed_work_without_checkpoint_advance<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let source = storage::MemoryStore::default();
    StateMachine::new(source.clone()).apply_queue_capacity(
        &QueueCapacityCommandV1::CreateFinite {
            namespace: namespace()?,
            entity: entity()?,
            issued_at: Timestamp::UNIX_EPOCH,
            config: QueueConfig::default(),
            limit: FiniteQueueCapacity::new(1_000)?,
        },
    )?;
    let mut batch = WriteBatch::default();
    for (key, value) in source.snapshot()?.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    control.inject(batch)?;
    let before = control.reader().snapshot()?;
    let checkpoint = machine.checkpoint()?;
    let commits = control.counts().commits;
    for work in [
        create(1, QueueConfig::default())?,
        send(1, "one", b"payload")?,
    ] {
        assert_eq!(
            machine.apply_committed(&update(&machine, 0)?, &work),
            Err(CommittedApplyError::BusinessState(
                BrokerError::QueueCapacityNotSupported
            ))
        );
        assert_eq!(machine.checkpoint()?, checkpoint);
        assert_eq!(control.counts().commits, commits);
        assert_eq!(control.reader().snapshot()?, before);
    }
    Ok(())
}

fn missing_mode_is_fatal_before_legacy_refusal_or_send_commit<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    apply(&mut machine, 0, &create(1, QueueConfig::default())?)?;
    control.inject(
        WriteBatch::default().delete(keys::queue_capacity_mode(&namespace()?, &entity()?)),
    )?;
    let before = control.reader().snapshot()?;
    let checkpoint = machine.checkpoint()?;
    let commits = control.counts().commits;
    for work in [
        create(2, QueueConfig::default())?,
        send(2, "one", b"payload")?,
    ] {
        assert_eq!(
            machine.apply_committed(&update(&machine, 1)?, &work),
            Err(CommittedApplyError::BusinessState(
                BrokerError::QueueCapacityCorrupt
            ))
        );
        assert_eq!(machine.checkpoint()?, checkpoint);
        assert_eq!(control.counts().commits, commits);
        assert_eq!(control.reader().snapshot()?, before);
    }
    Ok(())
}

for_each_backend! {
    finite_owner_is_outside_legacy_committed_work_without_checkpoint_advance,
    missing_mode_is_fatal_before_legacy_refusal_or_send_commit,
}
