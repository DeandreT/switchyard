//! Bounded ingress batches validate completely and commit as one queue mutation.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use domain::{
    AnnotationKey, BrokerError, CommandKind, CommandOutcome, Delivery, EntityPath,
    IngressBatchLimit, IngressEnvelope, MAX_INGRESS_BATCH_CONTENT_BYTES,
    MAX_INGRESS_BATCH_MESSAGES, MAX_INGRESS_BATCH_VALUE_ITEMS, MAX_SEQUENCE_NUMBER, MessageBody,
    MessageEnvelope, MessageHeader, MessageIdentifier, MessageProperties, MessageState,
    MessageValue, NamespaceName, QueueConfig, QueueConfigUpdate, QueueCounterKind, QueueCounters,
    QueueTimeToLiveUpdate, ReceiveMode, ScheduledEnvelope, SequenceNumber, SessionHold, SessionId,
    Timestamp, codec, keys,
};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn member(id: &str) -> IngressEnvelope {
    IngressEnvelope {
        message_id: id.to_owned(),
        body: b"payload".to_vec(),
        time_to_live_millis: None,
        session_id: None,
        envelope: MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String(id.to_owned())),
                ..MessageProperties::default()
            },
            body: MessageBody::Data(vec![b"payload".to_vec()]),
            ..MessageEnvelope::default()
        },
        scheduled_enqueue_time: None,
    }
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    messages: Vec<IngressEnvelope>,
) -> TestResult<Vec<SequenceNumber>> {
    let application = fixture
        .machine
        .apply_with_effects(&fixture.command(millis, CommandKind::SendBatch { messages }))?;
    assert!(!application.dead_letters_enqueued);
    let CommandOutcome::BatchSent { sequences } = application.outcome else {
        panic!("batch result");
    };
    Ok(sequences)
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    session: Option<SessionHold>,
) -> TestResult<Option<Delivery>> {
    let CommandOutcome::Received(delivery) = fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session,
        },
    )?
    else {
        panic!("receive result");
    };
    Ok(delivery)
}

fn counters<P: StoreProvider>(fixture: &QueueFixture<P>) -> TestResult<QueueCounters> {
    let value = fixture
        .machine
        .store()
        .get(&keys::queue_counters(&fixture.namespace, &fixture.entity))?
        .expect("allocated counters");
    Ok(codec::decode(&value)?)
}

fn reject<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    messages: Vec<IngressEnvelope>,
    expected: BrokerError,
) -> TestResult {
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture
            .machine
            .apply_with_effects(&fixture.command(millis, CommandKind::SendBatch { messages })),
        Err(expected)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn independent_content_survives_fifo_delivery_and_restart<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let mut first = member("first");
    first.envelope.header = Some(MessageHeader {
        durable: true,
        priority: 7,
        first_acquirer: true,
    });
    first.envelope.properties.correlation_id = Some(MessageIdentifier::Ulong(19));
    first.envelope.properties.subject = Some("own subject".to_owned());
    first.envelope.application_properties = BTreeMap::from([
        ("empty".to_owned(), MessageValue::Null),
        ("attempt".to_owned(), MessageValue::Uint(2)),
    ]);
    first.envelope.message_annotations.insert(
        AnnotationKey::Symbol("private:trace".to_owned()),
        MessageValue::Uuid([3; 16]),
    );
    first
        .envelope
        .footer
        .insert(AnnotationKey::Ulong(8), MessageValue::Binary(vec![9, 8]));
    let mut second = member("second");
    second.body = vec![4];
    second.envelope.properties = MessageProperties::default();
    second.envelope.body = MessageBody::Value(MessageValue::String("typed body".to_owned()));
    let originals = [first, second];
    assert_eq!(
        send(&fixture, 1, originals.to_vec())?,
        vec![SequenceNumber::new(1), SequenceNumber::new(2)]
    );
    let before = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    for (index, original) in originals.into_iter().enumerate() {
        let sequence = SequenceNumber::new(index as u64 + 1);
        let record = fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?
            .expect("stored member");
        assert_eq!(record.state, MessageState::Ready);
        assert_eq!(record.scheduled_enqueue_time, None);
        assert_eq!(record.envelope.as_deref(), Some(&original.envelope));
        let encoded = fixture
            .machine
            .store()
            .get(&keys::message(
                &fixture.namespace,
                &fixture.entity,
                sequence,
            ))?
            .expect("encoded record");
        assert_eq!(encoded[0], codec::ACTIVE_VALUE_FORMAT);
        let delivery = receive(&fixture, 2, None)?.expect("FIFO member");
        assert_eq!(delivery.sequence, sequence);
        assert_eq!(delivery.body, original.body);
        assert_eq!(delivery.envelope.as_deref(), Some(&original.envelope));
        assert_eq!(delivery.scheduled_enqueue_time, None);
    }
    assert!(receive(&fixture, 2, None)?.is_none());
    assert_eq!(counters(&fixture)?.next_sequence, 3);
    assert_eq!(counters(&fixture)?.next_lock_token, 1);
    Ok(())
}

fn mixed_schedules_capture_ttl_without_inventing_ordinary_timestamps<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            default_time_to_live_millis: Some(50),
            ..QueueConfig::default()
        },
    )?;
    let mut messages: Vec<_> = ["ordinary", "future", "past", "current"]
        .into_iter()
        .map(member)
        .collect();
    for message in &mut messages {
        message.time_to_live_millis = Some(75);
    }
    messages[1].scheduled_enqueue_time = Some(Timestamp::from_millis(100));
    messages[2].scheduled_enqueue_time = Some(Timestamp::from_millis(5));
    messages[3].scheduled_enqueue_time = Some(Timestamp::from_millis(10));
    let future_content = messages[1].envelope.clone();
    let sequences = send(&fixture, 10, messages)?;
    for (index, timestamp) in [None, Some(100), Some(5), Some(10)].into_iter().enumerate() {
        let record = fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequences[index])?
            .expect("member");
        assert_eq!(
            record.scheduled_enqueue_time,
            timestamp.map(Timestamp::from_millis)
        );
        assert_eq!(record.enqueued_at, Timestamp::from_millis(10));
        assert_eq!(
            record.expires_at,
            (index != 1).then(|| Timestamp::from_millis(60))
        );
        if index == 1 {
            assert_eq!(
                record.state,
                MessageState::Scheduled {
                    enqueue_at: Timestamp::from_millis(100),
                    time_to_live_millis: Some(50)
                }
            );
        } else {
            assert_eq!(record.state, MessageState::Ready);
        }
    }
    for expected in [1, 3, 4] {
        assert_eq!(
            receive(&fixture, 11, None)?.expect("ready member").sequence,
            SequenceNumber::new(expected)
        );
    }
    assert!(receive(&fixture, 11, None)?.is_none());
    fixture.at(
        12,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 1 }),
                ..QueueConfigUpdate::default()
            },
        },
    )?;
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequences[1])?
            .is_none()
    );
    let delivery = receive(&fixture, 100, None)?.expect("activated member");
    assert_eq!(delivery.sequence, SequenceNumber::new(5));
    assert_eq!(delivery.enqueued_at, Timestamp::from_millis(100));
    assert_eq!(delivery.expires_at, Some(Timestamp::from_millis(150)));
    assert_eq!(delivery.time_to_live_millis, Some(50));
    assert_eq!(
        delivery.scheduled_enqueue_time,
        Some(Timestamp::from_millis(100))
    );
    assert_eq!(delivery.envelope.as_deref(), Some(&future_content));
    Ok(())
}

fn batch_records_match_existing_rich_send_and_schedule_semantics<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let config = QueueConfig {
        default_time_to_live_millis: Some(50),
        ..QueueConfig::default()
    };
    let fixture = QueueFixture::new(provider, "tenant", "batch", config)?;
    let individual_entity = EntityPath::new("individual")?;
    let mut command = fixture.command(0, CommandKind::CreateQueue { config });
    command.entity = individual_entity.clone();
    fixture.machine.apply(&command)?;
    let mut members: Vec<_> = ["ordinary", "future", "past", "current"]
        .into_iter()
        .map(member)
        .collect();
    for (message, timestamp) in members.iter_mut().zip([None, Some(100), Some(5), Some(10)]) {
        message.scheduled_enqueue_time = timestamp.map(Timestamp::from_millis);
        message.time_to_live_millis = Some(75);
    }
    let sequences = send(&fixture, 10, members.clone())?;
    for (message, sequence) in members.into_iter().zip(sequences) {
        command.issued_at = Timestamp::from_millis(10);
        command.kind = match message.scheduled_enqueue_time {
            None => CommandKind::SendEnvelope {
                message_id: message.message_id,
                body: message.body,
                time_to_live_millis: message.time_to_live_millis,
                session_id: message.session_id,
                envelope: Box::new(message.envelope),
            },
            Some(enqueue_at) => CommandKind::ScheduleEnvelopes {
                messages: vec![ScheduledEnvelope {
                    message_id: message.message_id,
                    body: message.body,
                    time_to_live_millis: message.time_to_live_millis,
                    session_id: message.session_id,
                    enqueue_at,
                    envelope: message.envelope,
                }],
            },
        };
        fixture.machine.apply(&command)?;
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, sequence)?,
            fixture
                .machine
                .message(&fixture.namespace, &individual_entity, sequence)?
        );
    }
    Ok(())
}

fn zero_and_unlimited_lifetimes_match_immediate_and_future_enqueue<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let mut messages: Vec<_> = [
        "unlimited",
        "zero",
        "future-zero",
        "past-zero",
        "future-unlimited",
    ]
    .into_iter()
    .map(member)
    .collect();
    for index in [1, 2, 3] {
        messages[index].time_to_live_millis = Some(0);
    }
    messages[2].scheduled_enqueue_time = Some(Timestamp::from_millis(20));
    messages[3].scheduled_enqueue_time = Some(Timestamp::from_millis(5));
    messages[4].scheduled_enqueue_time = Some(Timestamp::from_millis(20));
    send(&fixture, 10, messages)?;
    assert_eq!(
        receive(&fixture, 10, None)?.expect("unlimited").expires_at,
        None
    );
    assert!(receive(&fixture, 10, None)?.is_none());
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, SequenceNumber::new(2))?
            .is_none()
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, SequenceNumber::new(4))?
            .is_none()
    );
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(20, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 2 }
    );
    let zero = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, SequenceNumber::new(6))?
        .expect("activated zero TTL");
    assert_eq!(zero.expires_at, Some(Timestamp::from_millis(20)));
    let delivery = receive(&fixture, 20, None)?.expect("activated unlimited");
    assert_eq!(delivery.sequence, SequenceNumber::new(7));
    assert_eq!(delivery.expires_at, None);
    assert_eq!(delivery.time_to_live_millis, None);
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, SequenceNumber::new(6))?
            .is_none()
    );
    assert_eq!(counters(&fixture)?.next_sequence, 8);
    Ok(())
}

fn every_member_is_validated_before_duplicates_or_mutation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            max_message_bytes: 200,
            ..QueueConfig::default()
        },
    )?;
    send(&fixture, 1, vec![member("known")])?;
    let before = fixture.machine.store().snapshot()?;
    let mut invalid = member(&"x".repeat(129));
    reject(
        &fixture,
        10,
        vec![member("new"), invalid.clone()],
        BrokerError::MessageIdTooLong {
            length: 129,
            maximum: 128,
        },
    )?;
    invalid.message_id = "known".to_owned();
    reject(
        &fixture,
        10,
        vec![member("new"), invalid],
        BrokerError::MessageIdTooLong {
            length: 129,
            maximum: 128,
        },
    )?;
    for invalid in [
        {
            let mut value = member("known");
            value
                .envelope
                .application_properties
                .insert("bad".to_owned(), MessageValue::List(vec![]));
            value
        },
        {
            let mut value = member("known");
            value.envelope.body = MessageBody::Value(MessageValue::Array(vec![]));
            value
        },
        {
            let mut value = member("known");
            value.body.resize(201, 0);
            value
        },
    ] {
        assert!(
            fixture
                .at(
                    10,
                    CommandKind::SendBatch {
                        messages: vec![member("new"), invalid]
                    }
                )
                .is_err()
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        send(&fixture, 2, vec![member("new"), member("other")])?,
        vec![SequenceNumber::new(2), SequenceNumber::new(3)]
    );
    assert_eq!(counters(&fixture)?.next_sequence, 4);
    Ok(())
}

fn session_presence_and_same_session_validation_are_atomic<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    let cart = SessionId::new("cart")?;
    let other = SessionId::new("other")?;
    let with_session = |id: &str, session: &SessionId| {
        let mut value = member(id);
        value.session_id = Some(session.clone());
        value
    };
    reject(
        &fixture,
        1,
        vec![
            with_session("one", &cart),
            with_session("two", &other),
            member("missing"),
        ],
        BrokerError::SessionRequired,
    )?;
    reject(
        &fixture,
        1,
        vec![with_session("one", &cart), with_session("two", &other)],
        BrokerError::BatchSessionMismatch,
    )?;
    let before = fixture.machine.store().snapshot()?;
    assert!(send(&fixture, 1, vec![])?.is_empty());
    assert_eq!(fixture.machine.store().snapshot()?, before);
    send(
        &fixture,
        1,
        vec![with_session("one", &cart), with_session("two", &cart)],
    )?;
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        2,
        CommandKind::AcceptSession {
            session_id: Some(cart.clone()),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("session accepted");
    };
    for expected in [1, 2] {
        let delivery = receive(&fixture, 3, Some(accepted.hold()))?.expect("session member");
        assert_eq!(delivery.sequence, SequenceNumber::new(expected));
        assert_eq!(delivery.session_id.as_ref(), Some(&cart));
    }
    let mut unrelated = fixture.command(
        3,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    unrelated.entity = EntityPath::new("ordinary")?;
    fixture.machine.apply(&unrelated)?;
    unrelated.kind = CommandKind::SendBatch {
        messages: vec![with_session("one", &cart), with_session("two", &other)],
    };
    assert_eq!(
        fixture.machine.apply(&unrelated)?,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)]
        }
    );
    for (sequence, session) in [(1, &cart), (2, &other)] {
        let record = fixture
            .machine
            .message(
                &fixture.namespace,
                &unrelated.entity,
                SequenceNumber::new(sequence),
            )?
            .expect("ordinary metadata member");
        assert_eq!(record.session_id.as_ref(), Some(session));
    }
    Ok(())
}

fn deduplication_spans_mixed_members_without_refreshing_history<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    send(&fixture, 1, vec![member("known")])?;
    let history_key = keys::duplicate_history(&fixture.namespace, &fixture.entity, "known");
    let original_deadline = fixture.machine.store().get(&history_key)?;
    let mut future = member("new");
    future.scheduled_enqueue_time = Some(Timestamp::from_millis(100));
    let mut suppressed_future = member("known");
    suppressed_future.scheduled_enqueue_time = Some(Timestamp::from_millis(100));
    let sequences = send(
        &fixture,
        2,
        vec![
            member("new"),
            future,
            suppressed_future,
            member(""),
            member(""),
        ],
    )?;
    assert_eq!(
        sequences,
        (2..=6).map(SequenceNumber::new).collect::<Vec<_>>()
    );
    for absent in [3, 4] {
        assert!(
            fixture
                .machine
                .message(
                    &fixture.namespace,
                    &fixture.entity,
                    SequenceNumber::new(absent)
                )?
                .is_none()
        );
    }
    assert_eq!(
        fixture.machine.store().get(&history_key)?,
        original_deadline
    );
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 0 }
    );
    for expected in [1, 2, 5, 6] {
        assert_eq!(
            receive(&fixture, 100, None)?
                .expect("accepted member")
                .sequence,
            SequenceNumber::new(expected)
        );
    }
    let mut future = member("scheduled");
    future.scheduled_enqueue_time = Some(Timestamp::from_millis(200));
    let sequence = send(&fixture, 100, vec![future])?[0];
    fixture.at(
        100,
        CommandKind::CancelScheduled {
            sequences: vec![sequence],
        },
    )?;
    let fixture = fixture.restart()?;
    let cancelled_duplicate = send(&fixture, 101, vec![member("scheduled")])?[0];
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, cancelled_duplicate)?
            .is_none()
    );
    assert_eq!(counters(&fixture)?.next_sequence, 9);
    Ok(())
}

fn exhausted_middle_allocation_rolls_back_history_and_every_record<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::queue_counters(&fixture.namespace, &fixture.entity),
        codec::encode(&QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER,
            next_lock_token: 1,
        })?,
    ))?;
    reject(
        &fixture,
        10,
        vec![member("same"), member("same")],
        BrokerError::QueueCounterExhausted {
            counter: QueueCounterKind::Sequence,
        },
    )?;
    let before = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        send(&fixture, 1, vec![member("same")])?,
        vec![SequenceNumber::new(MAX_SEQUENCE_NUMBER)]
    );
    let before = fixture.machine.store().snapshot()?;
    assert!(send(&fixture, 100, vec![])?.is_empty());
    assert_eq!(fixture.machine.store().snapshot()?, before);
    reject(
        &fixture,
        2,
        vec![member("same")],
        BrokerError::QueueCounterExhausted {
            counter: QueueCounterKind::Sequence,
        },
    )?;
    Ok(())
}

fn message_count_limit_accepts_the_boundary_and_rejects_before_mutation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    reject(
        &fixture,
        1,
        vec![member(""); MAX_INGRESS_BATCH_MESSAGES + 1],
        BrokerError::IngressBatchLimitExceeded {
            limit: IngressBatchLimit::Messages,
            actual: MAX_INGRESS_BATCH_MESSAGES + 1,
            maximum: MAX_INGRESS_BATCH_MESSAGES,
        },
    )?;
    let sequences = send(&fixture, 1, vec![member(""); MAX_INGRESS_BATCH_MESSAGES])?;
    assert_eq!(sequences.len(), MAX_INGRESS_BATCH_MESSAGES);
    assert_eq!(sequences.first(), Some(&SequenceNumber::new(1)));
    assert_eq!(
        sequences.last(),
        Some(&SequenceNumber::new(MAX_INGRESS_BATCH_MESSAGES as u64))
    );
    assert_eq!(
        counters(&fixture)?.next_sequence,
        MAX_INGRESS_BATCH_MESSAGES as u64 + 1
    );
    assert_eq!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::ready_prefix(&fixture.namespace, &fixture.entity),
                MAX_INGRESS_BATCH_MESSAGES + 1,
            )?
            .len(),
        MAX_INGRESS_BATCH_MESSAGES
    );
    Ok(())
}

fn aggregate_value_nodes_are_bounded_before_content_clones<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let mut first = member("first");
    let mut second = member("second");
    let children = MAX_INGRESS_BATCH_VALUE_ITEMS / 2 - 1;
    first.envelope.body =
        MessageBody::Value(MessageValue::Array(vec![MessageValue::Null; children]));
    second.envelope.body =
        MessageBody::Value(MessageValue::Array(vec![MessageValue::Null; children]));
    let mut extra = member("extra");
    extra.envelope.body = MessageBody::Value(MessageValue::Null);
    reject(
        &fixture,
        1,
        vec![first.clone(), second.clone(), extra],
        BrokerError::IngressBatchLimitExceeded {
            limit: IngressBatchLimit::ValueItems,
            actual: MAX_INGRESS_BATCH_VALUE_ITEMS + 1,
            maximum: MAX_INGRESS_BATCH_VALUE_ITEMS,
        },
    )?;
    let sequences = send(&fixture, 1, vec![first, second])?;
    let fixture = fixture.restart()?;
    for sequence in sequences {
        let record = fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?
            .expect("array member");
        let MessageBody::Value(MessageValue::Array(values)) =
            &record.envelope.expect("typed content").body
        else {
            panic!("array content");
        };
        assert_eq!(values.len(), children);
        assert!(values.iter().all(|value| *value == MessageValue::Null));
    }
    Ok(())
}

fn retained_bytes_include_compatibility_body_identifier_and_session<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: MAX_INGRESS_BATCH_CONTENT_BYTES,
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    let mut exact = member("normalized");
    exact.session_id = Some(SessionId::new("cart")?);
    exact.envelope = MessageEnvelope {
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::Binary(vec![4, 3])),
            ..MessageProperties::default()
        },
        ..MessageEnvelope::default()
    };
    let typed_and_identifiers = exact.envelope.content_size()
        + exact.message_id.len()
        + exact.session_id.as_ref().expect("session").as_str().len();
    exact.body = vec![0; MAX_INGRESS_BATCH_CONTENT_BYTES - typed_and_identifiers];
    let mut excess = exact.clone();
    excess.body.push(0);
    reject(
        &fixture,
        1,
        vec![excess],
        BrokerError::IngressBatchLimitExceeded {
            limit: IngressBatchLimit::ContentBytes,
            actual: MAX_INGRESS_BATCH_CONTENT_BYTES + 1,
            maximum: MAX_INGRESS_BATCH_CONTENT_BYTES,
        },
    )?;
    let sequence = send(&fixture, 1, vec![exact])?[0];
    let fixture = fixture.restart()?;
    let record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, sequence)?
        .expect("exact boundary member");
    assert_eq!(
        record.body.len() + typed_and_identifiers,
        MAX_INGRESS_BATCH_CONTENT_BYTES
    );
    assert_eq!(
        record.envelope.as_deref().expect("typed body").body,
        MessageBody::Empty
    );
    assert_eq!(
        record.session_id.as_ref().expect("session").as_str(),
        "cart"
    );
    Ok(())
}

fn empty_batches_validate_the_target_without_applying_clock_or_counters<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let before = fixture.machine.store().snapshot()?;
    assert!(send(&fixture, 100, vec![])?.is_empty());
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert!(
        fixture
            .machine
            .store()
            .get(&keys::queue_counters(&fixture.namespace, &fixture.entity))?
            .is_none()
    );
    for (entity, expected) in [
        (EntityPath::new("missing")?, BrokerError::QueueNotFound),
        (
            fixture.entity.dead_letter_queue()?,
            BrokerError::DeadLetterQueueIsReserved,
        ),
        (
            EntityPath::new("missing/$deadletterqueue")?,
            BrokerError::DeadLetterQueueIsReserved,
        ),
    ] {
        let mut command = fixture.command(100, CommandKind::SendBatch { messages: vec![] });
        command.entity = entity;
        assert_eq!(fixture.machine.apply_with_effects(&command), Err(expected));
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    assert_eq!(
        send(&fixture, 1, vec![member("first")])?,
        vec![SequenceNumber::new(1)]
    );
    let before = fixture.machine.store().snapshot()?;
    reject(
        &fixture,
        0,
        vec![],
        BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(1),
            proposed: Timestamp::from_millis(0),
        },
    )?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert!(send(&fixture, 100, vec![])?.is_empty());
    assert_eq!(
        send(&fixture, 2, vec![member("second")])?,
        vec![SequenceNumber::new(2)]
    );
    Ok(())
}

fn namespaces_and_entities_keep_batch_sequences_and_history_isolated<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let config = QueueConfig {
        requires_duplicate_detection: true,
        ..QueueConfig::default()
    };
    let mut fixture = QueueFixture::new(provider, "tenant", "orders", config)?;
    let scopes = [
        (NamespaceName::new("tenant")?, EntityPath::new("orders")?),
        (NamespaceName::new("neighbor")?, EntityPath::new("orders")?),
        (NamespaceName::new("tenant")?, EntityPath::new("other")?),
    ];
    for (index, (namespace, entity)) in scopes.iter().enumerate() {
        fixture.namespace = namespace.clone();
        fixture.entity = entity.clone();
        if index != 0 {
            fixture.at(1, CommandKind::CreateQueue { config })?;
        }
        assert_eq!(
            send(&fixture, 1, vec![member("same"), member("same")])?,
            vec![SequenceNumber::new(1), SequenceNumber::new(2)]
        );
        assert!(
            fixture
                .machine
                .message(namespace, entity, SequenceNumber::new(1))?
                .is_some()
        );
        assert!(
            fixture
                .machine
                .message(namespace, entity, SequenceNumber::new(2))?
                .is_none()
        );
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::duplicate_history(namespace, entity, "same"))?
                .is_some()
        );
        assert_eq!(counters(&fixture)?.next_sequence, 3);
        let shadow = entity.dead_letter_queue()?;
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::queue_counters(namespace, &shadow))?
                .is_none()
        );
        assert!(
            fixture
                .machine
                .store()
                .scan_prefix(&keys::ready_prefix(namespace, &shadow), 1)?
                .is_empty()
        );
    }
    let fixture = fixture.restart()?;
    for (namespace, entity) in scopes {
        assert!(
            fixture
                .machine
                .message(&namespace, &entity, SequenceNumber::new(1))?
                .is_some()
        );
    }
    Ok(())
}

#[derive(Debug, Default)]
struct Observations {
    reads: Vec<Key>,
    commits: usize,
    counter_writes: usize,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    fail_next: Arc<AtomicBool>,
    observations: Arc<Mutex<Observations>>,
    counter_key: Key,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.observations
            .lock()
            .expect("observations")
            .reads
            .push(key.to_vec());
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        {
            let mut observations = self.observations.lock().expect("observations");
            observations.commits += 1;
            observations.counter_writes += batch.mutations().iter().filter(|mutation| {
                matches!(mutation, Mutation::Put { key, .. } if key == &self.counter_key)
            }).count();
        }
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected failure".to_owned(),
            });
        }
        self.inner.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
}

struct ObservedProvider<P> {
    inner: P,
    fail_next: Arc<AtomicBool>,
    observations: Arc<Mutex<Observations>>,
    counter_key: Key,
}

impl<P: StoreProvider> StoreProvider for ObservedProvider<P> {
    type Store = ObservedStore<P::Store>;

    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(ObservedStore {
            inner: self.inner.open()?,
            fail_next: self.fail_next.clone(),
            observations: self.observations.clone(),
            counter_key: self.counter_key.clone(),
        })
    }
}

fn one_commit_and_counter_write_retries_storage_failure_without_losing_sequences<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let observations = Arc::new(Mutex::new(Observations::default()));
    let fail_next = Arc::new(AtomicBool::new(false));
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("orders")?;
    let counter_key = keys::queue_counters(&namespace, &entity);
    let config_key = keys::queue_config(&namespace, &entity);
    let fixture = QueueFixture::new(
        ObservedProvider {
            inner: provider,
            fail_next: fail_next.clone(),
            observations: observations.clone(),
            counter_key: counter_key.clone(),
        },
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    let mut future = member("future");
    future.scheduled_enqueue_time = Some(Timestamp::from_millis(100));
    let messages = vec![member("same"), member("same"), future];
    let before = fixture.machine.store().snapshot()?;
    *observations.lock().expect("observations") = Observations::default();
    fail_next.store(true, Ordering::Relaxed);
    reject(
        &fixture,
        10,
        messages.clone(),
        BrokerError::Storage(StorageError::Backend {
            operation: "commit",
            detail: "injected failure".to_owned(),
        }),
    )?;
    {
        let observed = observations.lock().expect("observations");
        assert_eq!(observed.commits, 1);
        assert_eq!(observed.counter_writes, 1);
        assert_eq!(
            observed
                .reads
                .iter()
                .filter(|key| *key == &config_key)
                .count(),
            2
        );
        assert_eq!(
            observed
                .reads
                .iter()
                .filter(|key| *key == &counter_key)
                .count(),
            1
        );
    }
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    *observations.lock().expect("observations") = Observations::default();
    assert_eq!(
        send(&fixture, 1, messages)?,
        vec![
            SequenceNumber::new(1),
            SequenceNumber::new(2),
            SequenceNumber::new(3)
        ]
    );
    {
        let observed = observations.lock().expect("observations");
        assert_eq!(observed.commits, 1);
        assert_eq!(observed.counter_writes, 1);
        assert_eq!(
            observed
                .reads
                .iter()
                .filter(|key| *key == &config_key)
                .count(),
            2
        );
        assert_eq!(
            observed
                .reads
                .iter()
                .filter(|key| *key == &counter_key)
                .count(),
            1
        );
    }
    assert_eq!(counters(&fixture)?.next_sequence, 4);
    assert!(
        fixture
            .machine
            .message(&namespace, &entity, SequenceNumber::new(2))?
            .is_none()
    );
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    assert!(
        fixture
            .machine
            .message(&namespace, &entity, SequenceNumber::new(3))?
            .is_none()
    );
    assert!(
        fixture
            .machine
            .message(&namespace, &entity, SequenceNumber::new(4))?
            .is_some()
    );
    Ok(())
}

#[test]
fn batch_command_is_appended_without_changing_existing_serialized_shapes() -> TestResult {
    for (kind, expected) in [
        (
            CommandKind::Send {
                message_id: "id".to_owned(),
                body: vec![1],
                time_to_live_millis: None,
                session_id: None,
            },
            vec![1, 2, 105, 100, 1, 1, 0, 0],
        ),
        (CommandKind::Schedule { messages: vec![] }, vec![2, 0]),
        (
            CommandKind::ScheduleEnvelopes { messages: vec![] },
            vec![23, 0],
        ),
        (
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate::default(),
            },
            vec![28, 0, 0, 0, 0, 0, 0, 0, 0],
        ),
        (CommandKind::SendBatch { messages: vec![] }, vec![29, 0]),
    ] {
        assert_eq!(postcard::to_stdvec(&kind)?, expected);
        assert_eq!(postcard::from_bytes::<CommandKind>(&expected)?, kind);
    }
    let mut rich = member("rich");
    rich.scheduled_enqueue_time = Some(Timestamp::from_millis(20));
    let command = CommandKind::SendBatch {
        messages: vec![member("ordinary"), rich],
    };
    let encoded = postcard::to_stdvec(&command)?;
    assert_eq!(encoded[0], 29);
    assert_eq!(postcard::from_bytes::<CommandKind>(&encoded)?, command);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    independent_content_survives_fifo_delivery_and_restart,
    mixed_schedules_capture_ttl_without_inventing_ordinary_timestamps,
    batch_records_match_existing_rich_send_and_schedule_semantics,
    zero_and_unlimited_lifetimes_match_immediate_and_future_enqueue,
    every_member_is_validated_before_duplicates_or_mutation,
    session_presence_and_same_session_validation_are_atomic,
    deduplication_spans_mixed_members_without_refreshing_history,
    exhausted_middle_allocation_rolls_back_history_and_every_record,
    message_count_limit_accepts_the_boundary_and_rejects_before_mutation,
    aggregate_value_nodes_are_bounded_before_content_clones,
    retained_bytes_include_compatibility_body_identifier_and_session,
    empty_batches_validate_the_target_without_applying_clock_or_counters,
    namespaces_and_entities_keep_batch_sequences_and_history_isolated,
    one_commit_and_counter_write_retries_storage_failure_without_losing_sequences,
}
