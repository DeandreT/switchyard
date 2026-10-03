use domain::{
    BrokerError, CommittedApplyError, CommittedCheckpoint, CommittedEntryId, CommittedEntryMark,
    CommittedQueueWork, CommittedStateMachine, CommittedStreamId, MAX_COMMITTED_CHECKPOINT_BYTES,
    QueueConfig, Timestamp, codec, keys,
};
use serde::Serialize;
use storage::{CommittedStore, StateStore, WriteBatch};

use super::{TestResult, fixture::*};

fn initialized_progress_cannot_be_missing_or_adopted<W: CommittedStore>(writer: W) -> TestResult {
    let (writer, control) = observed(writer);
    let machine = CommittedStateMachine::create(writer, stream()?)?;
    let key = checkpoint_key(&control.reader())?;
    let bytes = control.reader().get(&key)?.ok_or("missing baseline")?;
    drop(machine);
    let mut unsupported = bytes.clone();
    unsupported[4] = 2;
    let mut trailing = bytes.clone();
    trailing.push(0);
    for corrupt in [
        Vec::new(),
        unsupported,
        trailing,
        bytes[..bytes.len() - 1].to_vec(),
        vec![0; MAX_COMMITTED_CHECKPOINT_BYTES + 1],
    ] {
        control.inject(WriteBatch::default().put(key.clone(), corrupt))?;
        let before = control.reader().snapshot()?;
        let commits = control.counts().commits;
        assert!(matches!(
            CommittedStateMachine::open(control.recover_writer(), stream()?),
            Err(CommittedApplyError::CorruptCheckpoint)
        ));
        assert_eq!(control.counts().commits, commits);
        assert_eq!(control.reader().snapshot()?, before);
    }
    control.inject(WriteBatch::default().delete(key.clone()))?;
    let commits = control.counts().commits;
    assert!(matches!(
        CommittedStateMachine::open(control.recover_writer(), stream()?),
        Err(CommittedApplyError::CorruptCheckpoint)
    ));
    assert!(matches!(
        CommittedStateMachine::create(control.recover_writer(), stream()?),
        Err(CommittedApplyError::NotPristine)
    ));
    assert_eq!(control.counts().commits, commits);
    control.inject(WriteBatch::default().put(key, bytes))?;
    assert!(matches!(
        CommittedStateMachine::open(control.recover_writer(), CommittedStreamId::new([9; 16])?),
        Err(CommittedApplyError::WrongStream)
    ));
    assert_eq!(control.counts().commits, commits);
    Ok(())
}

#[derive(Serialize)]
struct StoredCheckpoint<'a> {
    stream: CommittedStreamId,
    last: Option<CommittedEntryMark>,
    previous: Option<CommittedEntryMark>,
    highest_timestamp: u64,
    membership: Option<StoredMembership<'a>>,
}

#[derive(Serialize)]
struct StoredMembership<'a> {
    source: CommittedEntryId,
    schema_version: u16,
    payload: &'a [u8],
}

fn bad_membership_source(checkpoint: &CommittedCheckpoint) -> TestResult<Vec<u8>> {
    let membership = checkpoint.membership().ok_or("missing membership")?;
    let wire = StoredCheckpoint {
        stream: checkpoint.stream(),
        last: checkpoint.last(),
        previous: checkpoint.previous(),
        highest_timestamp: checkpoint.highest_timestamp().as_millis(),
        membership: Some(StoredMembership {
            source: CommittedEntryId {
                node_id: membership.source.node_id + 1,
                ..membership.source
            },
            schema_version: membership.schema_version,
            payload: &membership.payload,
        }),
    };
    let mut bytes = b"SWYC\x01".to_vec();
    bytes.extend_from_slice(&postcard::to_stdvec(&wire)?);
    Ok(bytes)
}

fn retained_previous_membership_source_must_match_exactly<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let key = checkpoint_key(&control.reader())?;
    apply(
        &mut machine,
        0,
        &CommittedQueueWork::Membership {
            schema_version: 1,
            payload: vec![1, 2, 3],
        },
    )?;
    apply(&mut machine, 1, &CommittedQueueWork::Blank)?;
    let checkpoint = machine.checkpoint()?;
    assert_eq!(
        checkpoint.membership().ok_or("missing membership")?.source,
        checkpoint.previous().ok_or("missing previous")?.id
    );
    let request = update(&machine, 2)?;
    control.inject(WriteBatch::default().put(key, bad_membership_source(&checkpoint)?))?;
    let before = control.reader().snapshot()?;
    let counts = control.counts();
    assert_eq!(
        machine.apply_committed(&request, &create(1, QueueConfig::default())?),
        Err(CommittedApplyError::CorruptCheckpoint)
    );
    assert_eq!(control.counts().commits, counts.commits);
    assert_eq!(control.counts().scans, counts.scans);
    assert_eq!(control.counts().gets, counts.gets + 1);
    assert_eq!(control.reader().snapshot()?, before);
    Ok(())
}

fn corrupt_business_metadata_never_becomes_a_normal_refusal<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    apply(&mut machine, 0, &create(1, QueueConfig::default())?)?;
    let request = update(&machine, 1)?;
    let config_key = keys::queue_config(&namespace()?, &entity()?);
    let original = control
        .reader()
        .get(&config_key)?
        .ok_or("missing configuration")?;
    let invalid = QueueConfig {
        max_message_bytes: 0,
        ..QueueConfig::default()
    };
    control.inject(WriteBatch::default().put(config_key.clone(), codec::encode(&invalid)?))?;
    let before = control.reader().snapshot()?;
    let commits = control.counts().commits;
    assert!(matches!(
        machine.apply_committed(&request, &send(2, "one", b"payload")?),
        Err(CommittedApplyError::BusinessState(
            BrokerError::QueueConfig(_)
        ))
    ));
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(control.counts().commits, commits);
    control.inject(WriteBatch::default().put(config_key, original))?;
    for key in [
        keys::queue_config(&namespace()?, &entity()?.dead_letter_queue()?),
        keys::entity_incarnation(&namespace()?, &entity()?),
    ] {
        let original = control
            .reader()
            .get(&key)?
            .ok_or("missing topology record")?;
        control.inject(WriteBatch::default().delete(key.clone()))?;
        let before = control.reader().snapshot()?;
        let commits = control.counts().commits;
        assert_eq!(
            machine.apply_committed(&request, &send(2, "one", b"payload")?),
            Err(CommittedApplyError::BusinessState(
                BrokerError::DanglingEntityMetadata
            ))
        );
        assert_eq!(control.reader().snapshot()?, before);
        assert_eq!(control.counts().commits, commits);
        control.inject(WriteBatch::default().put(key, original))?;
    }
    assert!(matches!(
        machine.apply_committed(&request, &send(2, "one", b"payload")?)?,
        domain::CommittedApplyResult::Applied { .. }
    ));
    assert_eq!(
        record(&machine.reader(), 1)?
            .ok_or("missing healthy message")?
            .message_id,
        "one"
    );
    Ok(())
}

fn touched_clock_incarnation_and_history_corruption_is_fatal<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    apply(
        &mut machine,
        0,
        &create(
            1,
            QueueConfig {
                requires_duplicate_detection: true,
                ..QueueConfig::default()
            },
        )?,
    )?;
    apply(&mut machine, 1, &send(2, "one", b"original")?)?;
    let request = update(&machine, 2)?;
    let work = send(3, "one", b"duplicate")?;
    let clock_key = keys::clock();
    let original_clock = control.reader().get(&clock_key)?.ok_or("missing clock")?;
    for (bytes, ahead) in [
        (Vec::new(), false),
        (codec::encode(&Timestamp::from_millis(3))?, true),
    ] {
        control.inject(WriteBatch::default().put(clock_key.clone(), bytes))?;
        let before = control.reader().snapshot()?;
        let commits = control.counts().commits;
        let result = machine.apply_committed(&request, &work);
        if ahead {
            assert_eq!(
                result,
                Err(CommittedApplyError::BusinessClockAhead {
                    applied: Timestamp::from_millis(3),
                    watermark: Timestamp::from_millis(2),
                })
            );
        } else {
            assert!(matches!(
                result,
                Err(CommittedApplyError::BusinessState(BrokerError::Codec(_)))
            ));
        }
        assert_eq!(control.counts().commits, commits);
        assert_eq!(control.reader().snapshot()?, before);
        control.inject(WriteBatch::default().put(clock_key.clone(), original_clock.clone()))?;
    }
    for key in [
        keys::entity_incarnation(&namespace()?, &entity()?),
        keys::duplicate_history(&namespace()?, &entity()?, "one"),
    ] {
        let original = control
            .reader()
            .get(&key)?
            .ok_or("missing metadata to corrupt")?;
        let original_counters = counters(&machine.reader())?;
        control.inject(WriteBatch::default().put(key.clone(), Vec::new()))?;
        let before = control.reader().snapshot()?;
        let commits = control.counts().commits;
        assert!(matches!(
            machine.apply_committed(&request, &work),
            Err(CommittedApplyError::BusinessState(BrokerError::Codec(_)))
        ));
        assert_eq!(control.counts().commits, commits);
        assert_eq!(control.reader().snapshot()?, before);
        assert_eq!(counters(&machine.reader())?, original_counters);
        control.inject(WriteBatch::default().put(key, original))?;
    }
    machine.apply_committed(&request, &send(3, "two", b"healthy")?)?;
    assert_eq!(
        record(&machine.reader(), 1)?
            .ok_or("missing original")?
            .body,
        b"original"
    );
    assert_eq!(
        record(&machine.reader(), 2)?
            .ok_or("missing healthy continuation")?
            .body,
        b"healthy"
    );
    Ok(())
}

for_each_backend!(
    initialized_progress_cannot_be_missing_or_adopted,
    retained_previous_membership_source_must_match_exactly,
    corrupt_business_metadata_never_becomes_a_normal_refusal,
    touched_clock_incarnation_and_history_corruption_is_fatal,
);
