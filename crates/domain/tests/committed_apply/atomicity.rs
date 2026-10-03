use domain::{
    BrokerError, CommittedApplication, CommittedApplyError, CommittedApplyResult,
    CommittedStateMachine, QueueConfig, Timestamp, keys,
};
use storage::{CommittedStore, Mutation, StateStore};

use super::{TestResult, fixture::*};

fn business_and_checkpoint_share_one_privileged_batch<W: CommittedStore>(writer: W) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let checkpoint_key = checkpoint_key(&control.reader())?;
    let before = control.counts().commits;
    apply(&mut machine, 0, &create(1, QueueConfig::default())?)?;
    assert_eq!(control.counts().commits, before + 1);
    let creation = control.batches().pop().ok_or("missing creation batch")?;
    for key in [
        checkpoint_key.clone(),
        keys::clock(),
        keys::queue_config(&namespace()?, &entity()?),
        keys::queue_config(&namespace()?, &entity()?.dead_letter_queue()?),
        keys::entity_incarnation(&namespace()?, &entity()?),
    ] {
        assert_eq!(creation.mutations().iter().filter(|mutation| matches!(mutation, Mutation::Put { key: actual, .. } if actual == &key)).count(), 1);
    }
    let before = control.counts().commits;
    apply(&mut machine, 1, &send(2, "one", b"payload")?)?;
    assert_eq!(control.counts().commits, before + 1);
    let sending = control.batches().pop().ok_or("missing sending batch")?;
    for key in [
        checkpoint_key.clone(),
        keys::clock(),
        keys::queue_counters(&namespace()?, &entity()?),
        keys::message(&namespace()?, &entity()?, domain::SequenceNumber::new(1)),
        keys::ready(&namespace()?, &entity()?, domain::SequenceNumber::new(1)),
    ] {
        assert_eq!(sending.mutations().iter().filter(|mutation| matches!(mutation, Mutation::Put { key: actual, .. } if actual == &key)).count(), 1);
    }

    let before = control.reader().snapshot()?;
    let (_, application) = applied(apply(&mut machine, 2, &create(8, QueueConfig::default())?)?)?;
    assert_eq!(
        application,
        CommittedApplication::Refused(BrokerError::QueueAlreadyExists)
    );
    let refusal = control.batches().pop().ok_or("missing refusal batch")?;
    assert_eq!(refusal.mutations().len(), 1);
    assert!(matches!(&refusal.mutations()[0], Mutation::Put { key, .. } if key == &checkpoint_key));
    let after = control.reader().snapshot()?;
    let business = |snapshot: &storage::StoreSnapshot| {
        snapshot
            .entries()
            .iter()
            .filter(|(key, _)| key != &checkpoint_key)
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(business(&before), business(&after));
    assert_eq!(
        machine.checkpoint()?.highest_timestamp(),
        Timestamp::from_millis(8)
    );
    Ok(())
}

fn physical_failures_poison_without_inventing_rollback<W: CommittedStore>(writer: W) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    apply(&mut machine, 0, &create(1, QueueConfig::default())?)?;
    let work = send(2, "before", b"first")?;
    let request = update(&machine, 1)?;
    let before = control.reader().snapshot()?;
    control.fault(CommitFault::Before);
    assert!(matches!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Storage(_))
    ));
    assert_eq!(control.reader().snapshot()?, before);
    let counts = control.counts();
    assert_eq!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Poisoned)
    );
    assert_eq!(control.counts(), counts);
    drop(machine);
    let mut machine = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
    assert!(matches!(
        machine.apply_committed(&request, &work)?,
        CommittedApplyResult::Applied { .. }
    ));
    assert_eq!(
        record(&machine.reader(), 1)?
            .ok_or("missing first message")?
            .body,
        b"first"
    );

    let work = send(3, "after", b"second")?;
    let request = update(&machine, 2)?;
    control.fault(CommitFault::After);
    assert!(matches!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Storage(_))
    ));
    let position = machine
        .checkpoint()?
        .last()
        .ok_or("missing physically committed position")?;
    assert_eq!(position.id, request.entry);
    assert_eq!(
        record(&machine.reader(), 2)?
            .ok_or("missing physically committed message")?
            .body,
        b"second"
    );
    let physical = control.reader().snapshot()?;
    let counts = control.counts();
    assert_eq!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Poisoned)
    );
    assert_eq!(control.counts(), counts);
    drop(machine);
    let mut recovered = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
    assert_eq!(
        recovered.apply_committed(&request, &work),
        Ok(CommittedApplyResult::AlreadyApplied { position })
    );
    assert_eq!(control.reader().snapshot()?, physical);
    assert_eq!(
        counters(&recovered.reader())?
            .ok_or("missing counters")?
            .next_sequence,
        3
    );
    Ok(())
}

fn checkpoint_only_refusal_write_failures_are_still_indeterminate<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let checkpoint_key = checkpoint_key(&control.reader())?;
    apply(&mut machine, 0, &create(1, QueueConfig::default())?)?;
    let before = control.reader().snapshot()?;
    let request = update(&machine, 1)?;
    let work = create(8, QueueConfig::default())?;
    control.fault(CommitFault::Before);
    assert!(matches!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Storage(_))
    ));
    assert_eq!(control.reader().snapshot()?, before);
    let counts = control.counts();
    assert_eq!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Poisoned)
    );
    assert_eq!(control.counts(), counts);
    drop(machine);
    let mut machine = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
    let (_, application) = applied(machine.apply_committed(&request, &work)?)?;
    assert_eq!(
        application,
        CommittedApplication::Refused(BrokerError::QueueAlreadyExists)
    );

    let request = update(&machine, 2)?;
    let work = create(9, QueueConfig::default())?;
    control.fault(CommitFault::After);
    assert!(matches!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Storage(_))
    ));
    let checkpoint = machine.checkpoint()?;
    let position = checkpoint
        .last()
        .ok_or("missing physically persisted refusal progress")?;
    assert_eq!(position.id, request.entry);
    assert_eq!(checkpoint.highest_timestamp(), Timestamp::from_millis(9));
    let batch = control
        .batches()
        .pop()
        .ok_or("missing physical refusal batch")?;
    assert_eq!(batch.mutations().len(), 1);
    assert!(matches!(&batch.mutations()[0], Mutation::Put { key, .. } if key == &checkpoint_key));
    let physical = control.reader().snapshot()?;
    let business = |snapshot: &storage::StoreSnapshot| {
        snapshot
            .entries()
            .iter()
            .filter(|(key, _)| key != &checkpoint_key)
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(business(&before), business(&physical));
    let counts = control.counts();
    assert_eq!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Poisoned)
    );
    assert_eq!(control.counts(), counts);
    drop(machine);
    let mut recovered = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
    assert_eq!(
        recovered.apply_committed(&request, &work),
        Ok(CommittedApplyResult::AlreadyApplied { position })
    );
    assert_eq!(control.reader().snapshot()?, physical);
    assert_eq!(
        domain::StateMachine::new(recovered.reader()).last_applied_time()?,
        Timestamp::from_millis(1)
    );
    Ok(())
}

for_each_backend!(
    business_and_checkpoint_share_one_privileged_batch,
    physical_failures_poison_without_inventing_rollback,
    checkpoint_only_refusal_write_failures_are_still_indeterminate,
);
