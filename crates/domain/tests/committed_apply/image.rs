use domain::{
    BROKER_HEADER_RESERVE_BYTES, BrokerError, CommandOutcome, CommittedApplication,
    CommittedCheckpoint, CommittedImageRole, CommittedQueueCommand, CommittedQueueWork,
    CommittedSend, CommittedStateMachine, DecodedCommittedImage, EncodedCommittedImage,
    MAX_COMMITTED_BODY_BYTES, MAX_COMMITTED_IMAGE_BYTES, MAX_COMMITTED_IMAGE_KEY_BYTES,
    MAX_COMMITTED_IMAGE_ROWS, MAX_COMMITTED_IMAGE_VALUE_BYTES, MAX_MESSAGE_ID_LENGTH,
    MAX_SESSION_ID_BYTES, MessageState, QueueConfig, QueueCounters, SequenceNumber, SessionId,
    StateMachine, Timestamp, ValidatedCreateSendLayout17Image, codec, keys,
};
use storage::{
    BoundedStateStore, CommittedStore, FjallReplicaStore, ReadLimits, StateStore, StorageError,
    StoreSnapshot, WriteBatch,
};

use super::{TestResult, fixture::*};

const CREATE_TIME: u64 = 100;
const SEND_TIME: u64 = 101;
const DUPLICATE_TIME: u64 = 102;
const REFUSAL_TIME: u64 = 103;
const MESSAGE_TTL: u64 = 123;
const HISTORY_WINDOW: u64 = 20_000;

fn limits() -> ReadLimits {
    ReadLimits {
        max_rows: MAX_COMMITTED_IMAGE_ROWS,
        max_key_bytes: MAX_COMMITTED_IMAGE_KEY_BYTES,
        max_value_bytes: MAX_COMMITTED_IMAGE_VALUE_BYTES,
        max_total_bytes: MAX_COMMITTED_IMAGE_BYTES,
    }
}

fn bounded<R: BoundedStateStore>(reader: &R) -> TestResult<StoreSnapshot> {
    Ok(reader.snapshot_bounded(limits())?)
}

fn configuration() -> QueueConfig {
    QueueConfig {
        max_message_bytes: MAX_COMMITTED_BODY_BYTES + BROKER_HEADER_RESERVE_BYTES,
        default_time_to_live_millis: Some(1_000),
        requires_session: true,
        requires_duplicate_detection: true,
        duplicate_detection_history_time_window_millis: HISTORY_WINDOW,
        ..QueueConfig::default()
    }
}

fn maximum_message_id() -> String {
    "\u{0800}".repeat(MAX_MESSAGE_ID_LENGTH)
}

fn maximum_session() -> TestResult<SessionId> {
    Ok(SessionId::new("s".repeat(MAX_SESSION_ID_BYTES))?)
}

fn maximum_body() -> Vec<u8> {
    (0..MAX_COMMITTED_BODY_BYTES)
        .map(|offset| (offset % 251) as u8)
        .collect()
}

fn send_with_session(
    time: u64,
    id: &str,
    body: &[u8],
    session: Option<SessionId>,
) -> TestResult<CommittedQueueWork> {
    Ok(CommittedQueueWork::Queue(CommittedQueueCommand::send(
        namespace()?,
        entity()?,
        Timestamp::from_millis(time),
        CommittedSend {
            message_id: id.to_owned(),
            body: body.to_vec(),
            time_to_live_millis: Some(MESSAGE_TTL),
            session_id: session,
        },
    )))
}

fn assert_sent(application: CommittedApplication, sequence: u64) -> TestResult {
    let CommittedApplication::Queue(application) = application else {
        return Err("expected a committed queue send".into());
    };
    assert_eq!(
        application.outcome,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(sequence)
        }
    );
    assert!(!application.dead_letters_enqueued);
    assert_eq!(application.subscription_enqueues, None);
    assert_eq!(application.entity_deletions, None);
    Ok(())
}

fn populate<W>(machine: &mut CommittedStateMachine<W>) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (_, created) = applied(apply(machine, 0, &create(CREATE_TIME, configuration())?)?)?;
    let CommittedApplication::Queue(created) = created else {
        return Err("expected a committed queue creation".into());
    };
    assert_eq!(created.outcome, CommandOutcome::QueueCreated);

    let message_id = maximum_message_id();
    assert_eq!(message_id.encode_utf16().count(), MAX_MESSAGE_ID_LENGTH);
    assert_eq!(message_id.len(), 3 * MAX_MESSAGE_ID_LENGTH);
    let session = maximum_session()?;
    assert_eq!(session.as_str().len(), MAX_SESSION_ID_BYTES);
    let body = maximum_body();
    assert_eq!(body.len(), MAX_COMMITTED_BODY_BYTES);

    let (_, sent) = applied(apply(
        machine,
        1,
        &send_with_session(SEND_TIME, &message_id, &body, Some(session.clone()))?,
    )?)?;
    assert_sent(sent, 1)?;
    let (duplicate_mark, duplicate) = applied(apply(
        machine,
        2,
        &send_with_session(DUPLICATE_TIME, &message_id, &body, Some(session.clone()))?,
    )?)?;
    assert_sent(duplicate, 2)?;

    let reader = machine.reader();
    let original = record(&reader, 1)?.ok_or("missing original maximum-body message")?;
    assert_eq!(original.message_id, message_id);
    assert_eq!(original.body, body);
    assert_eq!(original.session_id, Some(session.clone()));
    assert_eq!(original.enqueued_at, Timestamp::from_millis(SEND_TIME));
    assert_eq!(
        original.expires_at,
        Some(Timestamp::from_millis(SEND_TIME + MESSAGE_TTL))
    );
    assert_eq!(original.delivery_count, 0);
    assert_eq!(original.state, MessageState::Ready);
    assert_eq!(original.dead_letter, None);
    assert_eq!(original.scheduled_enqueue_time, None);
    assert_eq!(original.envelope, None);
    assert_eq!(record(&reader, 2)?, None);
    assert_eq!(
        counters(&reader)?,
        Some(QueueCounters {
            next_sequence: 3,
            next_lock_token: 1
        })
    );
    assert_eq!(
        StateMachine::new(reader.clone()).last_applied_time()?,
        Timestamp::from_millis(DUPLICATE_TIME)
    );
    let before_refusal = bounded(&reader)?;
    let checkpoint_key = before_refusal
        .entries()
        .iter()
        .find(|(key, _)| key.as_slice() == [0x12])
        .map(|(key, _)| key.clone())
        .ok_or("missing checkpoint row")?;

    let (refusal_mark, refused) = applied(apply(
        machine,
        3,
        &send_with_session(REFUSAL_TIME, "missing-session", b"never-enqueued", None)?,
    )?)?;
    assert_eq!(
        refused,
        CommittedApplication::Refused(BrokerError::SessionRequired)
    );
    let checkpoint = machine.checkpoint()?;
    assert_eq!(checkpoint.last(), Some(refusal_mark));
    assert_eq!(checkpoint.previous(), Some(duplicate_mark));
    assert_eq!(
        checkpoint.highest_timestamp(),
        Timestamp::from_millis(REFUSAL_TIME)
    );
    assert_eq!(
        StateMachine::new(reader.clone()).last_applied_time()?,
        Timestamp::from_millis(DUPLICATE_TIME)
    );
    let after_refusal = bounded(&reader)?;
    assert_eq!(
        before_refusal
            .entries()
            .iter()
            .filter(|(key, _)| key != &checkpoint_key)
            .collect::<Vec<_>>(),
        after_refusal
            .entries()
            .iter()
            .filter(|(key, _)| key != &checkpoint_key)
            .collect::<Vec<_>>()
    );
    assert_ne!(
        before_refusal
            .entries()
            .iter()
            .find(|(key, _)| key == &checkpoint_key),
        after_refusal
            .entries()
            .iter()
            .find(|(key, _)| key == &checkpoint_key)
    );

    let namespace = namespace()?;
    let entity = entity()?;
    let sequence = SequenceNumber::new(1);
    let expires = Timestamp::from_millis(SEND_TIME + MESSAGE_TTL);
    let history_expires = Timestamp::from_millis(SEND_TIME + HISTORY_WINDOW);
    assert_eq!(
        reader.get(&keys::session_ready(
            &namespace, &entity, &session, sequence
        ))?,
        Some(Vec::new())
    );
    assert_eq!(
        reader.get(&keys::ready(&namespace, &entity, sequence))?,
        None
    );
    assert_eq!(
        reader.get(&keys::expiry(&namespace, &entity, expires, sequence))?,
        Some(Vec::new())
    );
    let history = reader
        .get(&keys::duplicate_history(&namespace, &entity, &message_id))?
        .ok_or("missing maximum-id duplicate history")?;
    assert_eq!(codec::decode::<Timestamp>(&history)?, history_expires);
    assert_eq!(
        reader.get(&keys::duplicate_history_expiry(
            &namespace,
            &entity,
            history_expires,
            &message_id
        ))?,
        Some(Vec::new())
    );
    let view = StateMachine::new(reader.clone());
    assert_eq!(
        view.queue_config(&namespace, &entity)?,
        Some(configuration())
    );
    assert_eq!(
        view.queue_config(&namespace, &entity.dead_letter_queue()?)?,
        Some(configuration().dead_letter_shadow())
    );
    let mut expected_keys = vec![
        keys::clock(),
        keys::queue_config(&namespace, &entity),
        keys::queue_config(&namespace, &entity.dead_letter_queue()?),
        keys::queue_counters(&namespace, &entity),
        keys::message(&namespace, &entity, sequence),
        keys::expiry(&namespace, &entity, expires, sequence),
        keys::session_ready(&namespace, &entity, &session, sequence),
        keys::duplicate_history(&namespace, &entity, &message_id),
        keys::duplicate_history_expiry(&namespace, &entity, history_expires, &message_id),
        keys::entity_incarnation(&namespace, &entity),
        checkpoint_key,
        keys::queue_capacity_mode(&namespace, &entity),
    ];
    expected_keys.sort();
    assert_eq!(
        after_refusal
            .entries()
            .iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>(),
        expected_keys.iter().collect::<Vec<_>>()
    );
    Ok(())
}

fn encode_exact<R: BoundedStateStore>(
    reader: &R,
    checkpoint: &CommittedCheckpoint,
) -> TestResult<(StoreSnapshot, EncodedCommittedImage)> {
    let source = bounded(reader)?;
    let encoded = EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendLayout17V1,
        stream()?,
        &source,
    )?;
    {
        let decoded = DecodedCommittedImage::decode(encoded.as_bytes())?;
        assert_eq!(decoded.role(), CommittedImageRole::CreateSendLayout17V1);
        assert_eq!(decoded.stream(), stream()?);
        assert_eq!(decoded.checkpoint(), checkpoint);
        assert_eq!(decoded.row_count(), source.entries().len());
        assert_eq!(decoded.encoded_bytes(), encoded.as_bytes());
        assert_eq!(
            decoded
                .rows()
                .map(|row| (row.key(), row.value()))
                .collect::<Vec<_>>(),
            source
                .entries()
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect::<Vec<_>>()
        );
        for row in decoded.rows() {
            let key_offset = row
                .key()
                .as_ptr()
                .addr()
                .checked_sub(encoded.as_bytes().as_ptr().addr())
                .ok_or("decoded key does not borrow the encoded image")?;
            let value_offset = row
                .value()
                .as_ptr()
                .addr()
                .checked_sub(encoded.as_bytes().as_ptr().addr())
                .ok_or("decoded value does not borrow the encoded image")?;
            assert!(key_offset < encoded.len());
            assert!(value_offset <= encoded.len());
            assert!(
                key_offset
                    .checked_add(row.key().len())
                    .is_some_and(|end| end <= encoded.len())
            );
            assert!(
                value_offset
                    .checked_add(row.value().len())
                    .is_some_and(|end| end <= encoded.len())
            );
            assert!(std::ptr::eq(
                row.key().as_ptr(),
                encoded.as_bytes()[key_offset..].as_ptr()
            ));
            assert!(std::ptr::eq(
                row.value().as_ptr(),
                encoded.as_bytes()[value_offset..].as_ptr()
            ));
        }
        let validated = ValidatedCreateSendLayout17Image::validate(decoded)?;
        assert_eq!(validated.checkpoint(), checkpoint);
        assert_eq!(validated.rows().count(), source.entries().len());
    }
    assert!(encoded.len() <= MAX_COMMITTED_IMAGE_BYTES);
    assert_eq!(bounded(reader)?, source);
    assert_eq!(
        reader.apply(WriteBatch::default()),
        Err(StorageError::ReplicaWriteRequired)
    );
    assert_eq!(
        reader.apply(
            WriteBatch::default().put(keys::clock(), codec::encode(&Timestamp::from_millis(999))?)
        ),
        Err(StorageError::ReplicaWriteRequired)
    );
    assert_eq!(bounded(reader)?, source);
    Ok((source, encoded))
}

fn initial_checkpoint_container_preserves_complete_bounded_source<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let machine = CommittedStateMachine::create(writer, stream()?)?;
    let checkpoint = machine.checkpoint()?;
    assert_eq!(checkpoint.last(), None);
    assert_eq!(checkpoint.previous(), None);
    assert_eq!(checkpoint.highest_timestamp(), Timestamp::UNIX_EPOCH);
    assert_eq!(checkpoint.membership(), None);
    let reader = machine.reader();
    let (source, encoded) = encode_exact(&reader, &checkpoint)?;
    assert_eq!(source.entries().len(), 1);
    assert_eq!(source.entries()[0].0, [0x12]);
    assert!(encoded.len() > source.entries()[0].1.len());
    drop(reader);
    drop(machine);
    Ok(())
}

fn maximum_committed_rows_duplicate_holes_and_refusal_watermark_are_exact<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    populate(&mut machine)?;
    let checkpoint = machine.checkpoint()?;
    let reader = machine.reader();
    let (source, _) = encode_exact(&reader, &checkpoint)?;
    assert_eq!(source.entries().len(), 12);
    assert_eq!(machine.checkpoint()?, checkpoint);
    drop(reader);
    drop(machine);
    Ok(())
}

#[test]
fn durable_source_and_container_are_identical_after_all_old_handles_drop() -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let mut machine =
        CommittedStateMachine::create(FjallReplicaStore::open(directory.path())?, stream()?)?;
    populate(&mut machine)?;
    let checkpoint = machine.checkpoint()?;
    let reader = machine.reader();
    let (source, encoded) = encode_exact(&reader, &checkpoint)?;
    drop(reader);
    drop(machine);

    let reopened =
        CommittedStateMachine::open(FjallReplicaStore::open(directory.path())?, stream()?)?;
    assert_eq!(reopened.checkpoint()?, checkpoint);
    let reader = reopened.reader();
    let (recovered_source, recovered_encoded) = encode_exact(&reader, &checkpoint)?;
    assert_eq!(recovered_source, source);
    assert_eq!(recovered_encoded.as_bytes(), encoded.as_bytes());
    assert_eq!(record(&reader, 2)?, None);
    assert_eq!(
        counters(&reader)?,
        Some(QueueCounters {
            next_sequence: 3,
            next_lock_token: 1
        })
    );
    assert_eq!(
        StateMachine::new(reader.clone()).last_applied_time()?,
        Timestamp::from_millis(DUPLICATE_TIME)
    );
    drop(reader);
    drop(reopened);
    Ok(())
}

for_each_backend!(
    initial_checkpoint_container_preserves_complete_bounded_source,
    maximum_committed_rows_duplicate_holes_and_refusal_watermark_are_exact,
);
