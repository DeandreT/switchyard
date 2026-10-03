use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, CommittedApplication, CommittedApplyError,
    CommittedQueueWork, CommittedStateMachine, QueueConfig, SequenceNumber, StateMachine,
    Timestamp,
};
use storage::{CommittedStore, StateStore, StorageError, WriteBatch};

use super::{TestResult, fixture::*};

fn initialization_and_readers_are_fail_closed<W: CommittedStore>(writer: W) -> TestResult {
    let (writer, control) = observed(writer);
    assert!(matches!(
        CommittedStateMachine::open(writer, stream()?),
        Err(CommittedApplyError::NotInitialized)
    ));
    assert_eq!(control.counts().commits, 0);
    let machine = CommittedStateMachine::create(control.recover_writer(), stream()?)?;
    let checkpoint = machine.checkpoint()?;
    assert_eq!(checkpoint.stream(), stream()?);
    assert_eq!(checkpoint.last(), None);
    assert_eq!(checkpoint.previous(), None);
    assert_eq!(checkpoint.highest_timestamp(), Timestamp::UNIX_EPOCH);
    assert_eq!(checkpoint.membership(), None);
    assert_eq!(control.counts().commits, 1);
    assert_eq!(control.reader().snapshot()?.entries().len(), 1);

    let reader = machine.reader();
    assert_eq!(
        reader.apply(WriteBatch::default()),
        Err(StorageError::ReplicaWriteRequired)
    );
    let command = Command::new(
        namespace()?,
        entity()?,
        Timestamp::UNIX_EPOCH,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    assert_eq!(
        StateMachine::new(reader.clone()).apply(&command),
        Err(BrokerError::Storage(StorageError::ReplicaWriteRequired))
    );
    assert_eq!(control.reader().snapshot()?.entries().len(), 1);
    drop(reader);
    drop(machine);
    assert!(matches!(
        CommittedStateMachine::create(control.recover_writer(), stream()?),
        Err(CommittedApplyError::NotPristine)
    ));
    assert_eq!(control.counts().commits, 1);
    let reopened = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
    assert_eq!(reopened.checkpoint()?, checkpoint);
    Ok(())
}

fn queue_send_and_normal_refusal_preserve_watermark<W: CommittedStore>(writer: W) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let (_, application) = applied(apply(&mut machine, 0, &send(11, "missing", b"body")?)?)?;
    assert_eq!(
        application,
        CommittedApplication::Refused(BrokerError::QueueNotFound)
    );
    assert_eq!(
        machine.checkpoint()?.highest_timestamp(),
        Timestamp::from_millis(11)
    );
    assert_eq!(
        StateMachine::new(machine.reader()).last_applied_time()?,
        Timestamp::UNIX_EPOCH
    );

    let (_, application) = applied(apply(
        &mut machine,
        1,
        &create(12, QueueConfig::default())?,
    )?)?;
    let CommittedApplication::Queue(application) = application else {
        return Err("expected queue creation".into());
    };
    assert_eq!(application.outcome, CommandOutcome::QueueCreated);
    assert!(!application.dead_letters_enqueued);
    assert_eq!(application.subscription_enqueues, None);
    assert_eq!(application.entity_deletions, None);

    let (_, application) = applied(apply(&mut machine, 2, &send(13, "one", b"payload")?)?)?;
    let CommittedApplication::Queue(application) = application else {
        return Err("expected queue send".into());
    };
    assert_eq!(
        application.outcome,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    let message = record(&machine.reader(), 1)?.ok_or("missing committed message")?;
    assert_eq!(message.message_id, "one");
    assert_eq!(message.body, b"payload");
    assert_eq!(message.enqueued_at, Timestamp::from_millis(13));
    assert_eq!(message.delivery_count, 0);
    assert_eq!(message.envelope, None);
    assert_eq!(
        counters(&machine.reader())?
            .ok_or("missing counters")?
            .next_sequence,
        2
    );

    let (_, application) = applied(apply(
        &mut machine,
        3,
        &create(20, QueueConfig::default())?,
    )?)?;
    assert_eq!(
        application,
        CommittedApplication::Refused(BrokerError::QueueAlreadyExists)
    );
    assert_eq!(
        machine.checkpoint()?.highest_timestamp(),
        Timestamp::from_millis(20)
    );
    assert_eq!(
        StateMachine::new(machine.reader()).last_applied_time()?,
        Timestamp::from_millis(13)
    );
    assert_eq!(control.counts().commits, 5);
    Ok(())
}

fn blank_and_membership_entries_advance_without_business<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let payload = vec![0, 255, 17, 3];
    let (first, application) = applied(apply(
        &mut machine,
        0,
        &CommittedQueueWork::Membership {
            schema_version: 1,
            payload: payload.clone(),
        },
    )?)?;
    assert_eq!(application, CommittedApplication::CheckpointOnly);
    let membership = machine
        .checkpoint()?
        .membership()
        .cloned()
        .ok_or("missing membership")?;
    assert_eq!(membership.source, first.id);
    assert_eq!(membership.schema_version, 1);
    assert_eq!(membership.payload, payload);

    let (blank, application) = applied(apply(&mut machine, 1, &CommittedQueueWork::Blank)?)?;
    assert_eq!(application, CommittedApplication::CheckpointOnly);
    let checkpoint = machine.checkpoint()?;
    assert_eq!(checkpoint.last(), Some(blank));
    assert_eq!(checkpoint.previous(), Some(first));
    assert_eq!(checkpoint.membership(), Some(&membership));
    assert_eq!(checkpoint.highest_timestamp(), Timestamp::UNIX_EPOCH);
    assert_eq!(control.reader().snapshot()?.entries().len(), 1);

    let (replacement, application) = applied(apply(
        &mut machine,
        2,
        &CommittedQueueWork::Membership {
            schema_version: 2,
            payload: Vec::new(),
        },
    )?)?;
    assert_eq!(application, CommittedApplication::CheckpointOnly);
    let checkpoint = machine.checkpoint()?;
    let membership = checkpoint
        .membership()
        .ok_or("missing replacement membership")?;
    assert_eq!(membership.source, replacement.id);
    assert_eq!(membership.schema_version, 2);
    assert!(membership.payload.is_empty());
    assert_eq!(control.reader().snapshot()?.entries().len(), 1);
    assert_eq!(control.counts().commits, 4);
    Ok(())
}

fn invalid_caller_configuration_and_clock_regression_are_normal_refusals<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let checkpoint_key = checkpoint_key(&control.reader())?;
    let (_, application) = applied(apply(
        &mut machine,
        0,
        &create(
            10,
            QueueConfig {
                max_message_bytes: 0,
                ..QueueConfig::default()
            },
        )?,
    )?)?;
    assert!(matches!(
        application,
        CommittedApplication::Refused(BrokerError::QueueConfig(_))
    ));
    let refusal = control
        .batches()
        .pop()
        .ok_or("missing input refusal batch")?;
    assert_eq!(refusal.mutations().len(), 1);
    assert!(
        matches!(&refusal.mutations()[0], storage::Mutation::Put { key, .. } if key == &checkpoint_key)
    );
    assert_eq!(control.reader().snapshot()?.entries().len(), 1);
    assert_eq!(
        machine.checkpoint()?.highest_timestamp(),
        Timestamp::from_millis(10)
    );
    assert_eq!(
        StateMachine::new(machine.reader()).last_applied_time()?,
        Timestamp::UNIX_EPOCH
    );

    let (_, application) = applied(apply(&mut machine, 1, &create(9, QueueConfig::default())?)?)?;
    assert_eq!(
        application,
        CommittedApplication::Refused(BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(10),
            proposed: Timestamp::from_millis(9),
        })
    );
    assert_eq!(
        control
            .batches()
            .last()
            .ok_or("missing clock refusal batch")?
            .mutations()
            .len(),
        1
    );
    assert_eq!(
        machine.checkpoint()?.highest_timestamp(),
        Timestamp::from_millis(10)
    );
    assert_eq!(
        StateMachine::new(machine.reader()).last_applied_time()?,
        Timestamp::UNIX_EPOCH
    );
    apply(&mut machine, 2, &create(10, QueueConfig::default())?)?;
    assert_eq!(
        StateMachine::new(machine.reader()).last_applied_time()?,
        Timestamp::from_millis(10)
    );
    Ok(())
}

for_each_backend!(
    initialization_and_readers_are_fail_closed,
    queue_send_and_normal_refusal_preserve_watermark,
    blank_and_membership_entries_advance_without_business,
    invalid_caller_configuration_and_clock_regression_are_normal_refusals,
);
