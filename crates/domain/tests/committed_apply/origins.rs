use domain::{
    BrokerError, Command, CommandKind, CommittedApplication, CommittedApplyError,
    CommittedStateMachine, DeleteEntityTarget, EntityIncarnation, EntityIncarnationKind,
    MAX_SEQUENCE_NUMBER, QueueConfig, QueueCounterKind, QueueCounters, SequenceNumber,
    StateMachine, Timestamp, TopicConfig, codec, keys,
};
use storage::{CommittedStore, StateStore, WriteBatch};

use super::{TestResult, fixture::*};

fn typed_queue_work_cannot_enter_topic_fanout<W: CommittedStore>(writer: W) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let checkpoint_key = checkpoint_key(&control.reader())?;
    let source = storage::MemoryStore::default();
    StateMachine::new(source.clone()).apply(&Command::new(
        namespace()?,
        entity()?,
        Timestamp::UNIX_EPOCH,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    ))?;
    let mut batch = WriteBatch::default();
    for (key, value) in source.snapshot()?.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    control.inject(batch)?;
    let topic_key = keys::topic_config(&namespace()?, &entity()?);
    let original_topic = control
        .reader()
        .get(&topic_key)?
        .ok_or("missing valid topic configuration")?;
    control.inject(WriteBatch::default().put(
        topic_key.clone(),
        codec::encode(&TopicConfig {
            max_message_bytes: 0,
            ..TopicConfig::default()
        })?,
    ))?;
    let invalid_before = control.reader().snapshot()?;
    let commits = control.counts().commits;
    let request = update(&machine, 0)?;
    assert!(matches!(
        machine.apply_committed(&request, &send(1, "one", b"payload")?),
        Err(CommittedApplyError::BusinessState(
            BrokerError::TopicConfig(_)
        ))
    ));
    assert_eq!(control.counts().commits, commits);
    assert_eq!(control.reader().snapshot()?, invalid_before);
    control.inject(WriteBatch::default().put(topic_key, original_topic))?;
    let before = control.reader().snapshot()?;
    let (_, application) = applied(apply(&mut machine, 0, &send(1, "one", b"payload")?)?)?;
    assert_eq!(
        application,
        CommittedApplication::Refused(BrokerError::EntityKindMismatch)
    );
    assert_eq!(record(&machine.reader(), 1)?, None);
    assert_eq!(counters(&machine.reader())?, None);
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
        control
            .batches()
            .last()
            .ok_or("missing refusal")?
            .mutations()
            .len(),
        1
    );
    Ok(())
}

fn missing_and_regressed_counters_cannot_overwrite_records<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let config = QueueConfig {
        requires_duplicate_detection: true,
        ..QueueConfig::default()
    };
    apply(&mut machine, 0, &create(1, config)?)?;
    apply(&mut machine, 1, &send(2, "duplicate", b"one")?)?;
    apply(&mut machine, 2, &send(3, "duplicate", b"must not replace")?)?;
    apply(&mut machine, 3, &send(4, "three", b"three")?)?;
    assert_eq!(
        record(&machine.reader(), 1)?
            .ok_or("missing first record")?
            .body,
        b"one"
    );
    assert_eq!(record(&machine.reader(), 2)?, None);
    assert_eq!(
        record(&machine.reader(), 3)?
            .ok_or("missing third record")?
            .body,
        b"three"
    );
    let key = keys::queue_counters(&namespace()?, &entity()?);
    let original = control.reader().get(&key)?.ok_or("missing counter")?;
    let request = update(&machine, 4)?;
    for corruption in [
        WriteBatch::default().delete(key.clone()),
        WriteBatch::default().put(
            key.clone(),
            codec::encode(&QueueCounters {
                next_sequence: 1,
                next_lock_token: 1,
            })?,
        ),
        WriteBatch::default().put(
            key.clone(),
            codec::encode(&QueueCounters {
                next_sequence: 2,
                next_lock_token: 1,
            })?,
        ),
    ] {
        control.inject(corruption)?;
        let before = control.reader().snapshot()?;
        let commits = control.counts().commits;
        assert_eq!(
            machine.apply_committed(&request, &send(5, "replacement", b"overwrite")?),
            Err(CommittedApplyError::BusinessState(
                BrokerError::DanglingEntityMetadata
            ))
        );
        assert_eq!(control.counts().commits, commits);
        assert_eq!(control.reader().snapshot()?, before);
        control.inject(WriteBatch::default().put(key.clone(), original.clone()))?;
    }
    machine.apply_committed(&request, &send(5, "four", b"four")?)?;
    assert_eq!(
        record(&machine.reader(), 4)?
            .ok_or("missing healthy fourth record")?
            .body,
        b"four"
    );
    assert_eq!(
        record(&machine.reader(), 1)?
            .ok_or("missing original first record")?
            .body,
        b"one"
    );
    assert_eq!(
        record(&machine.reader(), 3)?
            .ok_or("missing original third record")?
            .body,
        b"three"
    );

    let sentinel = keys::message(
        &namespace()?,
        &entity()?,
        SequenceNumber::new(MAX_SEQUENCE_NUMBER + 1),
    );
    let retained = control
        .reader()
        .get(&keys::message(
            &namespace()?,
            &entity()?,
            SequenceNumber::new(4),
        ))?
        .ok_or("missing retained message bytes")?;
    control.inject(
        WriteBatch::default()
            .put(
                key,
                codec::encode(&QueueCounters {
                    next_sequence: MAX_SEQUENCE_NUMBER + 1,
                    next_lock_token: 1,
                })?,
            )
            .put(sentinel, retained),
    )?;
    let before = control.reader().snapshot()?;
    let commits = control.counts().commits;
    let request = update(&machine, 5)?;
    assert_eq!(
        machine.apply_committed(&request, &send(6, "exhausted", b"never admitted")?),
        Err(CommittedApplyError::BusinessState(
            BrokerError::DanglingEntityMetadata
        ))
    );
    assert_eq!(control.counts().commits, commits);
    assert_eq!(control.reader().snapshot()?, before);
    Ok(())
}

fn removed_topology_cannot_adopt_surviving_business_records<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    apply(&mut machine, 0, &create(1, QueueConfig::default())?)?;
    apply(&mut machine, 1, &send(2, "one", b"retained")?)?;
    let shadow = entity()?.dead_letter_queue()?;
    control.inject(
        WriteBatch::default()
            .delete(keys::queue_config(&namespace()?, &entity()?))
            .delete(keys::queue_config(&namespace()?, &shadow))
            .delete(keys::entity_incarnation(&namespace()?, &entity()?)),
    )?;
    let before = control.reader().snapshot()?;
    let commits = control.counts().commits;
    let request = update(&machine, 2)?;
    assert_eq!(
        machine.apply_committed(&request, &create(3, QueueConfig::default())?),
        Err(CommittedApplyError::BusinessState(
            BrokerError::DanglingEntityMetadata
        ))
    );
    assert_eq!(control.counts().commits, commits);
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(
        record(&machine.reader(), 1)?
            .ok_or("orphaned message was overwritten")?
            .body,
        b"retained"
    );
    Ok(())
}

fn fresh_orphan_primary_and_shadow_state_is_not_adopted<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let request = update(&machine, 0)?;
    let shadow = entity()?.dead_letter_queue()?;
    for key in [
        keys::ready(&namespace()?, &entity()?, SequenceNumber::new(1)),
        keys::ready(&namespace()?, &shadow, SequenceNumber::new(1)),
        keys::queue_counters(&namespace()?, &entity()?),
        keys::queue_counters(&namespace()?, &shadow),
    ] {
        control.inject(
            WriteBatch::default().put(key.clone(), codec::encode(&QueueCounters::default())?),
        )?;
        let before = control.reader().snapshot()?;
        let commits = control.counts().commits;
        assert_eq!(
            machine.apply_committed(&request, &create(1, QueueConfig::default())?),
            Err(CommittedApplyError::BusinessState(
                BrokerError::DanglingEntityMetadata
            ))
        );
        assert_eq!(control.counts().commits, commits);
        assert_eq!(control.reader().snapshot()?, before);
        control.inject(WriteBatch::default().delete(key))?;
    }
    apply(&mut machine, 0, &create(1, QueueConfig::default())?)?;
    Ok(())
}

fn legitimate_retired_counter_fences_survive_recreation<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let checkpoint_key = checkpoint_key(&control.reader())?;
    apply(&mut machine, 0, &create(1, QueueConfig::default())?)?;
    apply(&mut machine, 1, &send(2, "one", b"purged")?)?;

    // Produce the real standalone deletion state, then install only its business
    // records through the fixture's privileged writer without changing progress.
    let business = storage::MemoryStore::default();
    let before = control.reader().snapshot()?;
    let mut copy = WriteBatch::default();
    for (key, value) in before
        .entries()
        .iter()
        .filter(|(key, _)| key != &checkpoint_key)
    {
        copy.push_put(key.clone(), value.clone());
    }
    business.apply(copy)?;
    StateMachine::new(business.clone()).apply(&Command::new(
        namespace()?,
        entity()?,
        Timestamp::from_millis(2),
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    ))?;
    let deleted = business.snapshot()?;
    let mut replace = WriteBatch::default();
    for (key, _) in before
        .entries()
        .iter()
        .filter(|(key, _)| key != &checkpoint_key)
    {
        replace.push_delete(key.clone());
    }
    for (key, value) in deleted.entries() {
        replace.push_put(key.clone(), value.clone());
    }
    control.inject(replace)?;
    assert_eq!(record(&machine.reader(), 1)?, None);
    assert_eq!(
        counters(&machine.reader())?
            .ok_or("deleted queue lost counter fence")?
            .next_sequence,
        2
    );
    let incarnation_key = keys::entity_incarnation(&namespace()?, &entity()?);
    let retired: EntityIncarnation = codec::decode(
        &control
            .reader()
            .get(&incarnation_key)?
            .ok_or("missing retired identity")?,
    )?;
    assert_eq!(
        retired,
        EntityIncarnation::new(1, EntityIncarnationKind::Queue, true)?
    );

    let shadow_counter = keys::queue_counters(&namespace()?, &entity()?.dead_letter_queue()?);
    assert_eq!(control.reader().get(&shadow_counter)?, None);
    let primary_counter = keys::queue_counters(&namespace()?, &entity()?);
    let original_counter = control
        .reader()
        .get(&primary_counter)?
        .ok_or("missing retained counter")?;
    let request = update(&machine, 2)?;
    for (corrupt, malformed) in [
        (Vec::new(), true),
        (
            codec::encode(&QueueCounters {
                next_sequence: 0,
                next_lock_token: 1,
            })?,
            false,
        ),
        (
            codec::encode(&QueueCounters {
                next_sequence: MAX_SEQUENCE_NUMBER + 2,
                next_lock_token: 1,
            })?,
            false,
        ),
        (
            codec::encode(&QueueCounters {
                next_sequence: 2,
                next_lock_token: 0,
            })?,
            false,
        ),
    ] {
        control.inject(WriteBatch::default().put(primary_counter.clone(), corrupt))?;
        let before = control.reader().snapshot()?;
        let commits = control.counts().commits;
        let result = machine.apply_committed(&request, &create(3, QueueConfig::default())?);
        if malformed {
            assert!(matches!(
                result,
                Err(CommittedApplyError::BusinessState(BrokerError::Codec(_)))
            ));
        } else {
            assert_eq!(
                result,
                Err(CommittedApplyError::BusinessState(
                    BrokerError::DanglingEntityMetadata
                ))
            );
        }
        assert_eq!(control.counts().commits, commits);
        assert_eq!(control.reader().snapshot()?, before);
        control
            .inject(WriteBatch::default().put(primary_counter.clone(), original_counter.clone()))?;
    }
    apply(&mut machine, 2, &create(3, QueueConfig::default())?)?;
    apply(&mut machine, 3, &send(4, "two", b"new generation")?)?;
    assert_eq!(record(&machine.reader(), 1)?, None);
    assert_eq!(
        record(&machine.reader(), 2)?
            .ok_or("missing recreated queue message")?
            .body,
        b"new generation"
    );
    assert_eq!(
        counters(&machine.reader())?
            .ok_or("missing recreated counter")?
            .next_sequence,
        3
    );
    let live: EntityIncarnation = codec::decode(
        &control
            .reader()
            .get(&incarnation_key)?
            .ok_or("missing new identity")?,
    )?;
    assert_eq!(
        live,
        EntityIncarnation::new(2, EntityIncarnationKind::Queue, false)?
    );
    Ok(())
}

fn valid_counter_exhaustion_commits_refusal_but_invalid_counters_are_fatal<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let checkpoint_key = checkpoint_key(&control.reader())?;
    apply(&mut machine, 0, &create(1, QueueConfig::default())?)?;
    let key = keys::queue_counters(&namespace()?, &entity()?);
    let exhausted = codec::encode(&QueueCounters {
        next_sequence: MAX_SEQUENCE_NUMBER + 1,
        next_lock_token: 1,
    })?;
    control.inject(WriteBatch::default().put(key.clone(), exhausted.clone()))?;
    let before = control.reader().snapshot()?;
    let commits = control.counts().commits;
    let (_, application) = applied(apply(&mut machine, 1, &send(2, "one", b"not retained")?)?)?;
    assert_eq!(
        application,
        CommittedApplication::Refused(BrokerError::QueueCounterExhausted {
            counter: QueueCounterKind::Sequence
        })
    );
    assert_eq!(control.counts().commits, commits + 1);
    let batch = control
        .batches()
        .pop()
        .ok_or("missing exhaustion refusal")?;
    assert_eq!(batch.mutations().len(), 1);
    assert!(
        matches!(&batch.mutations()[0], storage::Mutation::Put { key, .. } if key == &checkpoint_key)
    );
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
        StateMachine::new(machine.reader()).last_applied_time()?,
        Timestamp::from_millis(1)
    );
    assert_eq!(
        machine.checkpoint()?.highest_timestamp(),
        Timestamp::from_millis(2)
    );

    let request = update(&machine, 2)?;
    for counters in [
        QueueCounters {
            next_sequence: 0,
            next_lock_token: 1,
        },
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER + 2,
            next_lock_token: 1,
        },
        QueueCounters {
            next_sequence: 1,
            next_lock_token: 0,
        },
    ] {
        control.inject(WriteBatch::default().put(key.clone(), codec::encode(&counters)?))?;
        let before = control.reader().snapshot()?;
        let commits = control.counts().commits;
        assert_eq!(
            machine.apply_committed(&request, &send(3, "invalid", b"not retained")?),
            Err(CommittedApplyError::BusinessState(
                BrokerError::DanglingEntityMetadata
            ))
        );
        assert_eq!(control.counts().commits, commits);
        assert_eq!(control.reader().snapshot()?, before);
    }
    Ok(())
}

for_each_backend!(
    typed_queue_work_cannot_enter_topic_fanout,
    missing_and_regressed_counters_cannot_overwrite_records,
    removed_topology_cannot_adopt_surviving_business_records,
    fresh_orphan_primary_and_shadow_state_is_not_adopted,
    legitimate_retired_counter_fences_survive_recreation,
    valid_counter_exhaustion_commits_refusal_but_invalid_counters_are_fatal,
);
