use domain::{
    CommittedApplication, CommittedImageRole, CommittedImageValidationError, CommittedQueueCommand,
    CommittedQueueWork, CommittedSend, CommittedStateMachine, DecodedCommittedImage,
    EncodedCommittedImage, EntityIncarnation, EntityIncarnationKind, MAX_COMMITTED_BODY_BYTES,
    MAX_COMMITTED_IMAGE_BYTES, MAX_COMMITTED_IMAGE_KEY_BYTES, MAX_COMMITTED_IMAGE_ROWS,
    MAX_COMMITTED_IMAGE_VALUE_BYTES, MAX_SEQUENCE_NUMBER, MessageRecord, MessageState, QueueConfig,
    QueueCounters, SequenceNumber, SessionId, Timestamp, ValidatedCreateSendImage, codec, keys,
};
use storage::{
    BoundedStateStore, CommittedStore, MemoryStore, ReadLimits, StateStore, StoreSnapshot,
    WriteBatch,
};

use super::{TestResult, fixture::*};

fn bounded<R: BoundedStateStore>(reader: &R) -> TestResult<StoreSnapshot> {
    Ok(reader.snapshot_bounded(ReadLimits {
        max_rows: MAX_COMMITTED_IMAGE_ROWS,
        max_key_bytes: MAX_COMMITTED_IMAGE_KEY_BYTES,
        max_value_bytes: MAX_COMMITTED_IMAGE_VALUE_BYTES,
        max_total_bytes: MAX_COMMITTED_IMAGE_BYTES,
    })?)
}

fn validate_snapshot(
    snapshot: &StoreSnapshot,
) -> TestResult<Result<(usize, usize), CommittedImageValidationError>> {
    let image =
        EncodedCommittedImage::encode(CommittedImageRole::CreateSendV1, stream()?, snapshot)?;
    let decoded = DecodedCommittedImage::decode(image.as_bytes())?;
    Ok(ValidatedCreateSendImage::validate(decoded)
        .map(|image| (image.queue_count(), image.message_count())))
}

fn mutate(snapshot: &StoreSnapshot, mutations: WriteBatch) -> TestResult<StoreSnapshot> {
    let store = MemoryStore::default();
    let mut baseline = WriteBatch::default();
    for (key, value) in snapshot.entries() {
        baseline.push_put(key.clone(), value.clone());
    }
    store.apply(baseline)?;
    store.apply(mutations)?;
    Ok(store.snapshot()?)
}

fn put<T: serde::Serialize>(key: Vec<u8>, value: &T) -> TestResult<WriteBatch> {
    Ok(WriteBatch::default().put(key, codec::encode(value)?))
}

fn message_key(sequence: u64) -> TestResult<Vec<u8>> {
    Ok(keys::message(
        &namespace()?,
        &entity()?,
        SequenceNumber::new(sequence),
    ))
}

fn retained(snapshot: &StoreSnapshot, sequence: u64) -> TestResult<MessageRecord> {
    let key = message_key(sequence)?;
    let (_, value) = snapshot
        .entries()
        .iter()
        .find(|(found, _)| *found == key)
        .ok_or("missing fixture message")?;
    Ok(codec::decode(value)?)
}

fn input(
    time: u64,
    id: &str,
    body: &[u8],
    ttl: Option<u64>,
    session: Option<SessionId>,
) -> TestResult<CommittedQueueWork> {
    Ok(CommittedQueueWork::Queue(CommittedQueueCommand::send(
        namespace()?,
        entity()?,
        Timestamp::from_millis(time),
        CommittedSend {
            message_id: id.into(),
            body: body.to_vec(),
            time_to_live_millis: ttl,
            session_id: session,
        },
    )))
}

fn populated<W>(writer: W) -> TestResult<(CommittedStateMachine<W>, StoreSnapshot)>
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let config = QueueConfig {
        requires_session: true,
        requires_duplicate_detection: true,
        default_time_to_live_millis: Some(1000),
        max_message_bytes: MAX_COMMITTED_BODY_BYTES,
        duplicate_detection_history_time_window_millis: 20_000,
        ..QueueConfig::default()
    };
    applied(apply(&mut machine, 0, &create(100, config)?)?)?;
    let id = "\u{0800}".repeat(domain::MAX_MESSAGE_ID_LENGTH);
    let session = SessionId::new("s".repeat(domain::MAX_SESSION_ID_BYTES))?;
    let body = (0..MAX_COMMITTED_BODY_BYTES)
        .map(|offset| (offset % 251) as u8)
        .collect::<Vec<_>>();
    applied(apply(
        &mut machine,
        1,
        &input(110, &id, &body, Some(123), Some(session.clone()))?,
    )?)?;
    // Allocated sequence 2 is suppressed; all retained messages remain Ready.
    applied(apply(
        &mut machine,
        2,
        &input(111, &id, b"suppressed", None, Some(session.clone()))?,
    )?)?;
    applied(apply(
        &mut machine,
        3,
        &input(20_110, &id, b"second", Some(0), Some(session))?,
    )?)?;
    // A committed business refusal advances the watermark, not the ordinary
    // clock or history. The retained history is legitimately expired.
    let (_, application) = applied(apply(&mut machine, 4, &create(50_000, config)?)?)?;
    assert!(matches!(
        application,
        CommittedApplication::Refused(domain::BrokerError::QueueAlreadyExists)
    ));
    let snapshot = bounded(&machine.reader())?;
    Ok((machine, snapshot))
}

fn actual_maximum_records_holes_expired_latest_history_and_clock_gap_validate<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (machine, snapshot) = populated(writer)?;
    assert_eq!(validate_snapshot(&snapshot)?, Ok((1, 2)));
    assert_eq!(
        counters(&machine.reader())?
            .ok_or("missing counters")?
            .next_sequence,
        4
    );
    assert_eq!(record(&machine.reader(), 2)?, None);
    assert_eq!(
        machine.checkpoint()?.highest_timestamp(),
        Timestamp::from_millis(50_000)
    );
    let encoded =
        EncodedCommittedImage::encode(CommittedImageRole::CreateSendV1, stream()?, &snapshot)?;
    let image =
        ValidatedCreateSendImage::validate(DecodedCommittedImage::decode(encoded.as_bytes())?)?;
    assert_eq!(image.rows().count(), snapshot.entries().len());
    assert_eq!(image.stream(), stream()?);
    assert_eq!(image.checkpoint(), &machine.checkpoint()?);
    assert_eq!(bounded(&machine.reader())?, snapshot);
    assert!(!format!("{image:?}").contains(&"\u{0800}".repeat(8)));
    Ok(())
}

fn initial_and_refusal_only_images_need_no_business_clock<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    assert_eq!(validate_snapshot(&bounded(&machine.reader())?)?, Ok((0, 0)));
    let (_, application) = applied(apply(&mut machine, 0, &send(777, "missing", b"body")?)?)?;
    assert!(matches!(
        application,
        CommittedApplication::Refused(domain::BrokerError::QueueNotFound)
    ));
    assert_eq!(validate_snapshot(&bounded(&machine.reader())?)?, Ok((0, 0)));
    assert_eq!(machine.reader().get(&keys::clock())?, None);
    applied(apply(
        &mut machine,
        1,
        &CommittedQueueWork::Membership {
            schema_version: 999,
            payload: b"opaque-not-cluster-membership".to_vec(),
        },
    )?)?;
    assert_eq!(validate_snapshot(&bounded(&machine.reader())?)?, Ok((0, 0)));
    Ok(())
}

fn ordinary_anonymous_zero_ttl_and_saturated_deadlines_validate<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let config = QueueConfig {
        default_time_to_live_millis: Some(1),
        requires_duplicate_detection: true,
        ..QueueConfig::default()
    };
    applied(apply(&mut machine, 0, &create(0, config)?)?)?;
    applied(apply(
        &mut machine,
        1,
        &input(u64::MAX - 1, "", b"zero", Some(0), None)?,
    )?)?;
    applied(apply(
        &mut machine,
        2,
        &input(u64::MAX, "", b"saturated", Some(100), None)?,
    )?)?;
    let snapshot = bounded(&machine.reader())?;
    assert_eq!(validate_snapshot(&snapshot)?, Ok((1, 2)));
    assert_eq!(
        record(&machine.reader(), 1)?
            .ok_or("missing message")?
            .expires_at,
        Some(Timestamp::from_millis(u64::MAX - 1))
    );
    assert_eq!(
        record(&machine.reader(), 2)?
            .ok_or("missing message")?
            .expires_at,
        Some(Timestamp::from_millis(u64::MAX))
    );
    Ok(())
}

fn literal_scopes_and_nul_ids_remain_independent_across_primary_queues<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let config = QueueConfig {
        requires_duplicate_detection: true,
        ..QueueConfig::default()
    };
    applied(apply(&mut machine, 0, &create(100, config)?)?)?;
    let other_namespace = domain::NamespaceName::new("Tenant")?;
    let other_entity = domain::EntityPath::new("/Orders/$Management/literal")?;
    let other_create = CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
        other_namespace.clone(),
        other_entity.clone(),
        Timestamp::from_millis(101),
        config,
    ));
    applied(apply(&mut machine, 1, &other_create)?)?;
    applied(apply(
        &mut machine,
        2,
        &send(102, "id\0with\0tail", b"first")?,
    )?)?;
    let other_send = CommittedQueueWork::Queue(CommittedQueueCommand::send(
        other_namespace,
        other_entity,
        Timestamp::from_millis(103),
        CommittedSend {
            message_id: "id\0with\0tail".into(),
            body: b"second".to_vec(),
            time_to_live_millis: None,
            session_id: None,
        },
    ));
    applied(apply(&mut machine, 3, &other_send)?)?;
    let snapshot = bounded(&machine.reader())?;
    assert_eq!(validate_snapshot(&snapshot)?, Ok((2, 2)));
    assert_eq!(bounded(&machine.reader())?, snapshot);
    Ok(())
}

fn config_incarnation_counter_and_scope_mutations_are_refused<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (_machine, snapshot) = populated(writer)?;
    let ns = namespace()?;
    let queue = entity()?;
    let shadow = queue.dead_letter_queue()?;
    let primary = keys::queue_config(&ns, &queue);
    let incarnation = keys::entity_incarnation(&ns, &queue);
    let counter_key = keys::queue_counters(&ns, &queue);
    let mut patches = vec![
        WriteBatch::default().delete(primary.clone()),
        WriteBatch::default().delete(keys::queue_config(&ns, &shadow)),
        WriteBatch::default().delete(incarnation.clone()),
        put(keys::queue_config(&ns, &shadow), &QueueConfig::default())?,
        put(
            primary,
            &QueueConfig {
                max_delivery_count: 0,
                ..QueueConfig::default()
            },
        )?,
        put(
            incarnation.clone(),
            &(0_u64, EntityIncarnationKind::Queue, false),
        )?,
        put(
            incarnation.clone(),
            &EntityIncarnation::new(2, EntityIncarnationKind::Queue, false)?,
        )?,
        put(
            incarnation.clone(),
            &EntityIncarnation::new(1, EntityIncarnationKind::Queue, true)?,
        )?,
        put(
            incarnation,
            &EntityIncarnation::new(1, EntityIncarnationKind::Topic, false)?,
        )?,
        WriteBatch::default().delete(counter_key.clone()),
        put(
            counter_key.clone(),
            &QueueCounters {
                next_sequence: 2,
                next_lock_token: 1,
            },
        )?,
        put(
            counter_key.clone(),
            &QueueCounters {
                next_sequence: 0,
                next_lock_token: 1,
            },
        )?,
        put(
            counter_key.clone(),
            &QueueCounters {
                next_sequence: MAX_SEQUENCE_NUMBER + 2,
                next_lock_token: 1,
            },
        )?,
        put(
            counter_key.clone(),
            &QueueCounters {
                next_sequence: 4,
                next_lock_token: 0,
            },
        )?,
        put(
            counter_key.clone(),
            &QueueCounters {
                next_sequence: 4,
                next_lock_token: 2,
            },
        )?,
        put(
            keys::queue_counters(&ns, &shadow),
            &QueueCounters::default(),
        )?,
    ];
    let mut wrong_scope = counter_key;
    wrong_scope[1] = b'T';
    patches.push(put(
        wrong_scope,
        &QueueCounters {
            next_sequence: 4,
            next_lock_token: 1,
        },
    )?);
    for patch in patches {
        assert!(validate_snapshot(&mutate(&snapshot, patch)?)?.is_err());
    }
    // The upper sentinel is numerically valid. The view certifies consistency,
    // not whether each historical sequence hole came from a genuine request.
    assert_eq!(
        validate_snapshot(&mutate(
            &snapshot,
            put(
                keys::queue_counters(&ns, &queue),
                &QueueCounters {
                    next_sequence: MAX_SEQUENCE_NUMBER + 1,
                    next_lock_token: 1
                }
            )?
        )?)?,
        Ok((1, 2))
    );
    Ok(())
}

fn exact_ready_expiry_indices_and_queue_message_agreement_are_required<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (_machine, snapshot) = populated(writer)?;
    let ns = namespace()?;
    let queue = entity()?;
    let message = retained(&snapshot, 1)?;
    let session = message
        .session_id
        .as_ref()
        .ok_or("missing fixture session")?;
    let ready = keys::session_ready(&ns, &queue, session, SequenceNumber::new(1));
    let expiry = keys::expiry(
        &ns,
        &queue,
        message.expires_at.ok_or("missing expiry")?,
        SequenceNumber::new(1),
    );
    let patches = [
        WriteBatch::default().delete(ready.clone()),
        WriteBatch::default().put(ready.clone(), vec![0]),
        WriteBatch::default().put(keys::ready(&ns, &queue, SequenceNumber::new(1)), Vec::new()),
        WriteBatch::default().delete(expiry.clone()),
        WriteBatch::default().put(
            keys::expiry(
                &ns,
                &queue,
                Timestamp::from_millis(999),
                SequenceNumber::new(1),
            ),
            Vec::new(),
        ),
        WriteBatch::default().put(
            keys::session_ready(&ns, &queue, session, SequenceNumber::new(2)),
            Vec::new(),
        ),
        WriteBatch::default().put(
            keys::expiry(
                &ns,
                &queue,
                Timestamp::from_millis(999),
                SequenceNumber::new(2),
            ),
            Vec::new(),
        ),
    ];
    for patch in patches {
        assert!(validate_snapshot(&mutate(&snapshot, patch)?)?.is_err());
    }
    let mut wrong_sequence = message.clone();
    wrong_sequence.sequence = SequenceNumber::new(2);
    let mut wrong_session = message.clone();
    wrong_session.session_id = Some(SessionId::new("wrong")?);
    let mut missing_session = message.clone();
    missing_session.session_id = None;
    let mut ttl_above_ceiling = message;
    ttl_above_ceiling.expires_at = Some(Timestamp::from_millis(1111));
    for value in [
        wrong_sequence,
        wrong_session,
        missing_session,
        ttl_above_ceiling,
    ] {
        assert!(validate_snapshot(&mutate(&snapshot, put(message_key(1)?, &value)?)?)?.is_err());
    }
    Ok(())
}

fn latest_retained_id_history_and_exact_deadline_pairs_are_required<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (_machine, snapshot) = populated(writer)?;
    let ns = namespace()?;
    let queue = entity()?;
    let latest = retained(&snapshot, 3)?;
    let history = keys::duplicate_history(&ns, &queue, &latest.message_id);
    let index = keys::duplicate_history_expiry(
        &ns,
        &queue,
        Timestamp::from_millis(40_110),
        &latest.message_id,
    );
    for patch in [
        WriteBatch::default().delete(history.clone()),
        WriteBatch::default().delete(index.clone()),
        put(history.clone(), &Timestamp::from_millis(20_110))?,
        WriteBatch::default().put(index.clone(), vec![11, 0]),
        WriteBatch::default().put(
            keys::duplicate_history_expiry(
                &ns,
                &queue,
                Timestamp::from_millis(20_110),
                &latest.message_id,
            ),
            Vec::new(),
        ),
        put(
            keys::duplicate_history(&ns, &queue, "unknown"),
            &Timestamp::from_millis(40_110),
        )?,
    ] {
        assert!(validate_snapshot(&mutate(&snapshot, patch)?)?.is_err());
    }
    // Keep every index and final history individually consistent, but put a
    // second retained copy inside the first copy's deduplication window.
    let mut overlap = latest;
    overlap.enqueued_at = Timestamp::from_millis(111);
    overlap.expires_at = Some(Timestamp::from_millis(111));
    let mut patch = put(message_key(3)?, &overlap)?;
    patch.push_delete(keys::expiry(
        &ns,
        &queue,
        Timestamp::from_millis(20_110),
        SequenceNumber::new(3),
    ));
    patch.push_put(
        keys::expiry(
            &ns,
            &queue,
            Timestamp::from_millis(111),
            SequenceNumber::new(3),
        ),
        Vec::new(),
    );
    patch.push_put(history, codec::encode(&Timestamp::from_millis(20_111))?);
    patch.push_delete(index);
    patch.push_put(
        keys::duplicate_history_expiry(
            &ns,
            &queue,
            Timestamp::from_millis(20_111),
            &overlap.message_id,
        ),
        Vec::new(),
    );
    assert_eq!(
        validate_snapshot(&mutate(&snapshot, patch)?)?,
        Err(CommittedImageValidationError::InconsistentHistory)
    );
    Ok(())
}

fn closed_profile_legacy_formats_and_supported_corruption_are_distinguished<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (_machine, snapshot) = populated(writer)?;
    let ns = namespace()?;
    let queue = entity()?;
    let mut message = retained(&snapshot, 1)?;
    message.state = MessageState::Deferred;
    assert_eq!(
        validate_snapshot(&mutate(&snapshot, put(message_key(1)?, &message)?)?)?,
        Err(CommittedImageValidationError::UnsupportedProfile)
    );
    let config_key = keys::queue_config(&ns, &queue);
    let value = snapshot
        .entries()
        .iter()
        .find(|(key, _)| *key == config_key)
        .ok_or("missing configuration")?
        .1
        .clone();
    let mut legacy = value.clone();
    legacy[0] = 10;
    assert_eq!(
        validate_snapshot(&mutate(
            &snapshot,
            WriteBatch::default().put(config_key.clone(), legacy)
        )?)?,
        Err(CommittedImageValidationError::UnsupportedProfile)
    );
    let mut trailing = value;
    trailing.push(0);
    assert_eq!(
        validate_snapshot(&mutate(
            &snapshot,
            WriteBatch::default().put(config_key, trailing)
        )?)?,
        Err(CommittedImageValidationError::InvalidRecord)
    );
    assert_eq!(
        validate_snapshot(&mutate(
            &snapshot,
            WriteBatch::default().put(
                keys::session(&ns, &queue, &SessionId::new("broader")?),
                Vec::new()
            )
        )?)?,
        Err(CommittedImageValidationError::UnsupportedProfile)
    );
    Ok(())
}

fn clock_missing_ahead_and_retained_enqueue_regression_are_refused<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (_machine, snapshot) = populated(writer)?;
    for patch in [
        WriteBatch::default().delete(keys::clock()),
        put(keys::clock(), &Timestamp::from_millis(50_001))?,
        put(keys::clock(), &Timestamp::from_millis(109))?,
    ] {
        assert!(validate_snapshot(&mutate(&snapshot, patch)?)?.is_err());
    }
    let mut earlier = retained(&snapshot, 3)?;
    earlier.enqueued_at = Timestamp::from_millis(109);
    earlier.expires_at = Some(Timestamp::from_millis(109));
    assert!(validate_snapshot(&mutate(&snapshot, put(message_key(3)?, &earlier)?)?)?.is_err());
    Ok(())
}

fn retained_enqueue_order_is_checked_without_ttl_or_history<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let config = QueueConfig {
        requires_session: false,
        requires_duplicate_detection: false,
        default_time_to_live_millis: None,
        ..QueueConfig::default()
    };
    applied(apply(&mut machine, 0, &create(100, config)?)?)?;
    applied(apply(&mut machine, 1, &send(110, "first", b"first")?)?)?;
    applied(apply(&mut machine, 2, &send(111, "second", b"second")?)?)?;
    let snapshot = bounded(&machine.reader())?;
    assert_eq!(validate_snapshot(&snapshot)?, Ok((1, 2)));
    assert!(
        snapshot
            .entries()
            .iter()
            .all(|(key, _)| { !matches!(key.first().copied(), Some(0x06 | 0x0c | 0x0d)) }),
        "the monotonicity fixture must have no expiry or history rows"
    );
    let mut second = retained(&snapshot, 2)?;
    assert_eq!(second.enqueued_at, Timestamp::from_millis(111));
    assert_eq!(second.expires_at, None);
    // This is the only changed field. Ordinary ready indexes contain no time,
    // and this fixture has no TTL or dedup history that could mask the error.
    second.enqueued_at = Timestamp::from_millis(109);
    let changed = mutate(&snapshot, put(message_key(2)?, &second)?)?;
    assert_eq!(
        validate_snapshot(&changed)?,
        Err(CommittedImageValidationError::InconsistentMessage),
    );
    assert_eq!(bounded(&machine.reader())?, snapshot);
    Ok(())
}

fn nonempty_dedup_id_is_reaccepted_at_the_saturated_deadline<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    const ID: &str = "same-id-at-saturated-deadline";
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let config = QueueConfig {
        requires_duplicate_detection: true,
        duplicate_detection_history_time_window_millis: 20_000,
        default_time_to_live_millis: None,
        ..QueueConfig::default()
    };
    applied(apply(&mut machine, 0, &create(0, config)?)?)?;
    applied(apply(&mut machine, 1, &send(u64::MAX, ID, b"first")?)?)?;
    // The retained deadline is MAX, and MAX > MAX is false. A second real
    // Send therefore retains another original rather than suppressing it.
    applied(apply(&mut machine, 2, &send(u64::MAX, ID, b"second")?)?)?;
    let reader = machine.reader();
    let first = record(&reader, 1)?.ok_or("missing first saturated original")?;
    let second = record(&reader, 2)?.ok_or("missing second saturated original")?;
    for (message, sequence, body) in [
        (&first, 1, b"first".as_slice()),
        (&second, 2, b"second".as_slice()),
    ] {
        assert_eq!(message.sequence, SequenceNumber::new(sequence));
        assert_eq!(message.message_id, ID);
        assert_eq!(message.body.as_slice(), body);
        assert_eq!(message.enqueued_at, Timestamp::from_millis(u64::MAX));
        assert_eq!(message.expires_at, None);
        assert_eq!(message.delivery_count, 0);
        assert!(matches!(message.state, MessageState::Ready));
    }
    assert_eq!(record(&reader, 3)?, None);
    assert_eq!(
        counters(&reader)?,
        Some(QueueCounters {
            next_sequence: 3,
            next_lock_token: 1
        })
    );
    let snapshot = bounded(&reader)?;
    assert_eq!(validate_snapshot(&snapshot)?, Ok((1, 2)));
    let history_key = keys::duplicate_history(&namespace()?, &entity()?, ID);
    let history_expiry_key = keys::duplicate_history_expiry(
        &namespace()?,
        &entity()?,
        Timestamp::from_millis(u64::MAX),
        ID,
    );
    let deadline = codec::encode(&Timestamp::from_millis(u64::MAX))?;
    let history_rows: Vec<_> = snapshot
        .entries()
        .iter()
        .filter(|(key, _)| matches!(key.first().copied(), Some(0x0c | 0x0d)))
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect();
    assert_eq!(
        history_rows,
        vec![
            (history_key.as_slice(), deadline.as_slice()),
            (history_expiry_key.as_slice(), &[][..]),
        ]
    );
    assert_eq!(bounded(&reader)?, snapshot);
    Ok(())
}

for_each_backend!(
    actual_maximum_records_holes_expired_latest_history_and_clock_gap_validate,
    initial_and_refusal_only_images_need_no_business_clock,
    ordinary_anonymous_zero_ttl_and_saturated_deadlines_validate,
    literal_scopes_and_nul_ids_remain_independent_across_primary_queues,
    config_incarnation_counter_and_scope_mutations_are_refused,
    exact_ready_expiry_indices_and_queue_message_agreement_are_required,
    latest_retained_id_history_and_exact_deadline_pairs_are_required,
    closed_profile_legacy_formats_and_supported_corruption_are_distinguished,
    clock_missing_ahead_and_retained_enqueue_regression_are_refused,
    retained_enqueue_order_is_checked_without_ttl_or_history,
    nonempty_dedup_id_is_reaccepted_at_the_saturated_deadline,
);
