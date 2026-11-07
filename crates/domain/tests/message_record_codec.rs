//! Frozen message-record formats: V1 goldens equal the former generic value
//! encoding, V2 goldens are hand-framed, and both survive ordinary lifecycle
//! rewrites and snapshot validation without changing version.

use std::{collections::BTreeMap, error::Error};

use domain::{
    CodecError, Command, CommandKind, CommandOutcome, CorrelationValue, DeadLetterInfo,
    DeadLetterReason, Delivery, DeliveryOrigin, DurableProposal, DurableProposalError, EntityPath,
    FilterProperties, LockToken, MessageEnvelope, MessageInput, MessageRecord, MessageRecordFormat,
    MessageState, NamespaceName, OverlayProperty, QueueConfig, ReceiveMode, SequenceNumber,
    SessionId, Timestamp, codec, keys,
    snapshot_validation::{SnapshotStateError, validate_message_rows},
};
use serde::Serialize;
use storage::{StateStore, WriteBatch};
use testkit::{MemoryProvider, QueueFixture};

type TestResult = Result<(), Box<dyn Error>>;

/// The exact shape `MessageRecord` derived before its codec was frozen. The
/// public field types still carry their own serde, so encoding this through
/// the generic value codec reproduces the former bytes.
#[derive(Serialize)]
struct FormerRecord<'a> {
    sequence: SequenceNumber,
    message_id: &'a String,
    body: &'a Vec<u8>,
    enqueued_at: Timestamp,
    scheduled_enqueue_at: Option<Timestamp>,
    expires_at: Option<Timestamp>,
    delivery_count: u32,
    state: &'a MessageState,
    session_id: &'a Option<SessionId>,
    dead_letter: &'a Option<DeadLetterInfo>,
    envelope: &'a Option<MessageEnvelope>,
}

fn former_encoding(record: &MessageRecord) -> Vec<u8> {
    codec::encode(&FormerRecord {
        sequence: record.sequence,
        message_id: &record.message_id,
        body: &record.body,
        enqueued_at: record.enqueued_at,
        scheduled_enqueue_at: record.scheduled_enqueue_at,
        expires_at: record.expires_at,
        delivery_count: record.delivery_count,
        state: &record.state,
        session_id: &record.session_id,
        dead_letter: &record.dead_letter,
        envelope: &record.envelope,
    })
    .unwrap()
}

fn base(sequence: u64) -> MessageRecord {
    MessageRecord {
        sequence: SequenceNumber::new(sequence),
        message_id: format!("m-{sequence}"),
        body: Vec::new(),
        enqueued_at: Timestamp::from_millis(0),
        scheduled_enqueue_at: None,
        expires_at: None,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: None,
        dead_letter: None,
        envelope: None,
        format: MessageRecordFormat::V1,
    }
}

fn session() -> Option<SessionId> {
    Some(SessionId::new("s-1").unwrap())
}

fn locked(sequence: u64, origin: DeliveryOrigin) -> MessageRecord {
    MessageRecord {
        delivery_count: 2,
        state: MessageState::Locked {
            token: LockToken::new(7),
            locked_until: Timestamp::from_millis(1_234),
            origin,
        },
        ..base(sequence)
    }
}

fn dead_lettered(sequence: u64, reason: DeadLetterReason) -> MessageRecord {
    MessageRecord {
        dead_letter: Some(DeadLetterInfo {
            reason,
            description: String::from("why"),
            dead_lettered_at: Timestamp::from_millis(12),
        }),
        ..base(sequence)
    }
}

fn projection() -> FilterProperties {
    FilterProperties {
        correlation_id: Some(String::from("c")),
        message_id: Some(String::from("m")),
        to: Some(String::from("t")),
        reply_to: Some(String::from("r")),
        subject: Some(String::from("s")),
        session_id: Some(String::from("s-1")),
        reply_to_session_id: Some(String::from("rs")),
        content_type: Some(String::from("ct")),
        application_properties: BTreeMap::from([
            (
                String::from("a"),
                CorrelationValue::new(vec![0x40]).unwrap(),
            ),
            (
                String::from("b"),
                CorrelationValue::new(vec![0x52, 5]).unwrap(),
            ),
        ]),
    }
}

const AMQP: [u8; 4] = [0, 0x53, 0x77, 0x40];

/// Representative V1 records covering every state, origin and dead-letter
/// reason, with and without a session, an envelope and its projection. The
/// hex was captured from the generic encoder before the codec was frozen.
fn v1_goldens() -> Vec<(MessageRecord, &'static str)> {
    vec![
        (base(1), "0101036d2d31000000000000000000"),
        (
            MessageRecord {
                body: b"hi".to_vec(),
                enqueued_at: Timestamp::from_millis(1_000),
                scheduled_enqueue_at: Some(Timestamp::from_millis(900)),
                expires_at: Some(Timestamp::from_millis(5_000)),
                delivery_count: 1,
                state: MessageState::Deferred,
                session_id: session(),
                ..base(2)
            },
            "0102036d2d32026869e80701840701882701010103732d310000",
        ),
        (
            MessageRecord {
                scheduled_enqueue_at: Some(Timestamp::from_millis(2_000)),
                expires_at: Some(Timestamp::from_millis(3_000)),
                state: MessageState::Scheduled,
                ..base(u64::MAX)
            },
            "01ffffffffffffffffff01166d2d3138343436373434303733373039353531363135\
             000001d00f01b8170002000000",
        ),
        (
            locked(4, DeliveryOrigin::Ready),
            "0104036d2d3400000000020307d20900000000",
        ),
        (
            locked(5, DeliveryOrigin::Deferred),
            "0105036d2d3500000000020307d20901000000",
        ),
        (
            MessageRecord {
                session_id: session(),
                ..locked(6, DeliveryOrigin::Scheduled)
            },
            "0106036d2d3600000000020307d209020103732d310000",
        ),
        (
            dead_lettered(7, DeadLetterReason::MaxDeliveryCountExceeded),
            "0107036d2d37000000000000000100037768790c00",
        ),
        (
            MessageRecord {
                session_id: session(),
                ..dead_lettered(8, DeadLetterReason::TimeToLiveExpired)
            },
            "0108036d2d380000000000000103732d310101037768790c00",
        ),
        (
            dead_lettered(9, DeadLetterReason::Application(String::from("Custom"))),
            "0109036d2d3900000000000000010206437573746f6d037768790c00",
        ),
        (
            MessageRecord {
                body: vec![1, 2],
                envelope: Some(MessageEnvelope::new(AMQP.to_vec())),
                ..base(10)
            },
            "010a046d2d313002010200000000000000010400537740000000000000000000",
        ),
        (
            MessageRecord {
                session_id: session(),
                envelope: Some(
                    MessageEnvelope::new(AMQP.to_vec()).with_filter_properties(projection()),
                ),
                ..locked(11, DeliveryOrigin::Deferred)
            },
            "010b046d2d313100000000020307d209010103732d310001040053774001016301016d\
             0101740101720101730103732d31010272730102637402016101400162025205",
        ),
    ]
}

fn overlay(entries: &[(&str, &[u8])]) -> Vec<OverlayProperty> {
    entries
        .iter()
        .map(|(name, value)| OverlayProperty {
            name: (*name).to_owned(),
            value: value.to_vec(),
        })
        .collect()
}

/// Hand-framed V2 records: raw bytes, AMQP with an empty overlay, and an
/// overlay holding an explicit null before a compound list value.
fn v2_goldens() -> Vec<(MessageRecord, &'static str)> {
    vec![
        (
            MessageRecord {
                body: vec![1, 2],
                format: MessageRecordFormat::V2,
                ..base(1)
            },
            "0201036d2d310000000000000000020102",
        ),
        (
            MessageRecord {
                state: MessageState::Deferred,
                session_id: session(),
                envelope: Some(MessageEnvelope::new(AMQP.to_vec())),
                format: MessageRecordFormat::V2,
                ..base(2)
            },
            "0202036d2d3200000000010103732d310001040053774000000000000000000000",
        ),
        (
            MessageRecord {
                envelope: Some(
                    MessageEnvelope::new(AMQP.to_vec())
                        .with_filter_properties(FilterProperties {
                            subject: Some(String::from("s")),
                            application_properties: BTreeMap::from([(
                                String::from("a"),
                                CorrelationValue::new(vec![0x40]).unwrap(),
                            )]),
                            ..FilterProperties::default()
                        })
                        .with_overlay(overlay(&[
                            ("z", &[0x40]),
                            ("a", &[0xc0, 0x03, 0x01, 0x50, 0x05]),
                        ])),
                ),
                format: MessageRecordFormat::V2,
                ..MessageRecord {
                    dead_letter: dead_lettered(3, DeadLetterReason::Application("Custom".into()))
                        .dead_letter,
                    ..locked(3, DeliveryOrigin::Deferred)
                }
            },
            "0203036d2d3300000002\
             0307d20901\
             00\
             010206437573746f6d037768790c\
             01040053774002017a0140016105c003015005\
             00000000010173000000\
             0101610140",
        ),
    ]
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&text[index..index + 2], 16).unwrap())
        .collect()
}

#[test]
fn v1_goldens_equal_the_frozen_and_former_generic_encoders() -> TestResult {
    for (record, golden) in v1_goldens() {
        let encoded = codec::message::encode(&record)?;
        assert_eq!(hex(&encoded), golden, "{}", record.message_id);
        assert_eq!(encoded, former_encoding(&record), "{}", record.message_id);
        assert_eq!(MessageRecord::decode(&encoded)?, record);
    }
    Ok(())
}

#[test]
fn v2_goldens_round_trip_exactly() -> TestResult {
    for (record, golden) in v2_goldens() {
        let encoded = codec::message::encode(&record)?;
        assert_eq!(hex(&encoded), golden, "{}", record.message_id);
        let decoded = MessageRecord::decode(&unhex(golden))?;
        assert_eq!(decoded, record);
        assert_eq!(codec::message::encode(&decoded)?, encoded);
    }
    let compound = &v2_goldens()[2].0;
    let names: Vec<_> = compound
        .envelope
        .as_ref()
        .unwrap()
        .overlay()
        .iter()
        .collect();
    assert_eq!(names[0].name, "z", "overlay keeps first-insertion order");
    assert_eq!(names[0].value, [0x40], "an explicit null is a stored value");
    Ok(())
}

#[test]
fn malformed_and_unrepresentable_records_are_refused() {
    assert_eq!(MessageRecord::decode(&[]), Err(CodecError::EmptyEnvelope));
    for version in [0, 3, 255] {
        assert_eq!(
            MessageRecord::decode(&[version, 0]),
            Err(CodecError::UnsupportedVersion { version })
        );
    }
    let raw = &v2_goldens()[0].1;
    assert_eq!(
        MessageRecord::decode(&unhex(&format!("{raw}00"))),
        Err(CodecError::Decode),
        "V2 framing is exact"
    );
    assert_eq!(
        MessageRecord::decode(&unhex(&raw[..raw.len() - 2])),
        Err(CodecError::Decode)
    );
    // Two overlay entries both named "z", then an empty projection.
    let duplicate = format!(
        "0201036d2d31{}010400537740{}{}{}",
        "00".repeat(7),
        "02",
        "017a0140".repeat(2),
        "00".repeat(9)
    );
    assert_eq!(
        MessageRecord::decode(&unhex(&duplicate)),
        Err(CodecError::Decode)
    );
    let unique = duplicate.replacen("017a0140017a0140", "017a0140017b0140", 1);
    assert!(MessageRecord::decode(&unhex(&unique)).is_ok());

    let amqp = |overlay: Vec<OverlayProperty>| MessageRecord {
        envelope: Some(MessageEnvelope::new(AMQP.to_vec()).with_overlay(overlay)),
        format: MessageRecordFormat::V2,
        ..base(1)
    };
    for record in [
        amqp(overlay(&[("z", &[0x40]), ("z", &[0x41])])),
        amqp(overlay(&[("z", &[])])),
        MessageRecord {
            body: vec![1],
            ..amqp(Vec::new())
        },
        MessageRecord {
            format: MessageRecordFormat::V1,
            ..amqp(overlay(&[("z", &[0x40])]))
        },
    ] {
        assert_eq!(codec::message::encode(&record), Err(CodecError::Encode));
    }
}

#[test]
fn v1_runtime_reads_keep_trailing_byte_tolerance() -> TestResult {
    let (record, golden) = &v1_goldens()[0];
    assert_eq!(
        &MessageRecord::decode(&unhex(&format!("{golden}00")))?,
        record
    );
    Ok(())
}

#[test]
fn proposals_refuse_envelopes_with_overlays() {
    let command = Command::new(
        NamespaceName::new("tenant").unwrap(),
        EntityPath::new("orders").unwrap(),
        Timestamp::from_millis(1),
        CommandKind::SendBatch {
            messages: vec![MessageInput {
                message_id: String::from("m"),
                envelope: Some(
                    MessageEnvelope::new(AMQP.to_vec()).with_overlay(overlay(&[("z", &[0x40])])),
                ),
                ..MessageInput::default()
            }],
        },
    );
    assert_eq!(
        DurableProposal::unbound(command).encode(),
        Err(DurableProposalError::UnsupportedCommandVersion(1))
    );
}

fn stored(fixture: &QueueFixture<MemoryProvider>, sequence: u64) -> Option<Vec<u8>> {
    fixture
        .machine
        .store()
        .get(&keys::message(
            &fixture.namespace,
            &fixture.entity,
            SequenceNumber::new(sequence),
        ))
        .unwrap()
}

fn assert_versions(fixture: &QueueFixture<MemoryProvider>, expected: &[(u64, Option<u8>)]) {
    for (sequence, version) in expected {
        let raw = stored(fixture, *sequence);
        assert_eq!(raw.as_ref().map(|raw| raw[0]), *version, "{sequence}");
        if let Some(raw) = raw {
            let record = MessageRecord::decode(&raw).unwrap();
            assert_eq!(codec::message::encode(&record).unwrap(), raw);
            if *version == Some(1) {
                assert_eq!(former_encoding(&record), raw);
            }
        }
    }
}

fn received(outcome: CommandOutcome) -> Delivery {
    match outcome {
        CommandOutcome::Received(Some(delivery)) => delivery,
        other => panic!("expected delivery, got {other:?}"),
    }
}

/// Rewrites a sent message as a V2 row, as a later producer would store it.
fn rewrite_as_v2(fixture: &QueueFixture<MemoryProvider>, sequence: u64, amqp: bool) {
    let mut record = fixture
        .machine
        .message(
            &fixture.namespace,
            &fixture.entity,
            SequenceNumber::new(sequence),
        )
        .unwrap()
        .unwrap();
    record.format = MessageRecordFormat::V2;
    if amqp {
        record.body.clear();
        record.envelope =
            Some(MessageEnvelope::new(AMQP.to_vec()).with_overlay(overlay(&[("z", &[0x40])])));
    }
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().put(
            keys::message(&fixture.namespace, &fixture.entity, record.sequence),
            codec::message::encode(&record).unwrap(),
        ))
        .unwrap();
}

#[test]
fn mixed_v1_and_v2_rows_keep_their_versions_through_settlement() -> TestResult {
    let fixture = QueueFixture::new(
        MemoryProvider::new(),
        "tenant",
        "orders",
        QueueConfig {
            lock_duration_millis: 100,
            ..QueueConfig::default()
        },
    )?;
    for id in ["m-1", "m-2", "m-3"] {
        fixture.at(
            1,
            CommandKind::Send {
                message_id: id.to_owned(),
                body: id.as_bytes().to_vec(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        )?;
    }
    rewrite_as_v2(&fixture, 2, true);
    rewrite_as_v2(&fixture, 3, false);
    assert_versions(&fixture, &[(1, Some(1)), (2, Some(2)), (3, Some(2))]);
    let image = fixture.machine.store().snapshot()?.entries().to_vec();
    assert_eq!(validate_message_rows(&image)?.message_rows(), 3);

    let CommandOutcome::Peeked(peeked) = fixture.at(
        2,
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(1),
            max_messages: 3,
            session: None,
        },
    )?
    else {
        panic!("expected peek");
    };
    assert_eq!(peeked.len(), 3);
    assert!(
        peeked[1].body.is_empty(),
        "V2 AMQP content has no separate body"
    );
    assert_eq!(peeked[1].envelope.as_ref().unwrap().overlay().len(), 1);
    assert_eq!(peeked[2].body, b"m-3");

    let receive = |at| {
        received(
            fixture
                .at(
                    at,
                    CommandKind::Receive {
                        mode: ReceiveMode::PeekLock,
                        lock_duration_millis: None,
                        session: None,
                    },
                )
                .unwrap(),
        )
    };
    let deliveries: Vec<_> = (0..3).map(|_| receive(3)).collect();
    assert_versions(&fixture, &[(1, Some(1)), (2, Some(2)), (3, Some(2))]);
    let token = |delivery: &Delivery| delivery.lock.unwrap().token;

    fixture.at(
        4,
        CommandKind::RenewLock {
            sequence: deliveries[1].sequence,
            lock_token: token(&deliveries[1]),
            lock_duration_millis: None,
        },
    )?;
    fixture.at(
        5,
        CommandKind::Complete {
            sequence: deliveries[0].sequence,
            lock_token: token(&deliveries[0]),
        },
    )?;
    for delivery in &deliveries[1..] {
        fixture.at(
            6,
            CommandKind::Abandon {
                sequence: delivery.sequence,
                lock_token: token(delivery),
                replacement_envelope: None,
            },
        )?;
    }
    assert_versions(&fixture, &[(1, None), (2, Some(2)), (3, Some(2))]);
    let image = fixture.machine.store().snapshot()?.entries().to_vec();
    assert_eq!(validate_message_rows(&image)?.message_rows(), 2);

    let again = receive(7);
    assert_eq!(again.sequence, SequenceNumber::new(2));
    assert_eq!(again.delivery_count, 2);
    assert_eq!(again.envelope.as_ref().unwrap().overlay()[0].name, "z");
    fixture.at(
        8,
        CommandKind::Complete {
            sequence: again.sequence,
            lock_token: token(&again),
        },
    )?;
    assert_versions(&fixture, &[(2, None), (3, Some(2))]);
    Ok(())
}

#[test]
fn snapshot_validation_rejects_noncanonical_rows_of_either_version() -> TestResult {
    let fixture = QueueFixture::with_defaults(MemoryProvider::new(), "tenant", "orders")?;
    fixture.at(
        1,
        CommandKind::Send {
            message_id: String::from("m-1"),
            body: vec![1, 2],
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )?;
    rewrite_as_v2(&fixture, 1, false);
    let image = fixture.machine.store().snapshot()?.entries().to_vec();
    assert_eq!(validate_message_rows(&image)?.message_rows(), 1);

    let key = keys::message(&fixture.namespace, &fixture.entity, SequenceNumber::new(1));
    let row = image.iter().position(|(stored, _)| stored == &key).unwrap();
    let mut v1 = MessageRecord::decode(&image[row].1)?;
    v1.format = MessageRecordFormat::V1;
    let v1 = codec::message::encode(&v1)?;
    let mut nonminimal_v2 = image[row].1.clone();
    // Sequence 1 as a two-byte varint is accepted by postcard but not canonical.
    nonminimal_v2.splice(1..2, [0x81, 0x00]);
    assert_eq!(MessageRecord::decode(&nonminimal_v2)?.sequence.as_u64(), 1);
    let mut trailing_v1 = v1.clone();
    trailing_v1.push(0);
    let mut nonminimal_v1 = v1.clone();
    nonminimal_v1.splice(1..2, [0x81, 0x00]);
    assert_eq!(MessageRecord::decode(&nonminimal_v1)?.sequence.as_u64(), 1);

    let mut accepted = image.clone();
    accepted[row].1 = v1;
    assert_eq!(validate_message_rows(&accepted)?.message_rows(), 1);
    for (value, expected) in [
        (nonminimal_v2, "state value is not canonical"),
        (nonminimal_v1, "state value is not canonical"),
        (trailing_v1, "state value is not canonical"),
        (vec![], "state value cannot be decoded"),
        (vec![2], "state value cannot be decoded"),
        (vec![9, 0], "state value cannot be decoded"),
    ] {
        let mut broken = image.clone();
        broken[row].1 = value;
        assert_eq!(
            validate_message_rows(&broken).unwrap_err(),
            SnapshotStateError::InvalidValue {
                row,
                detail: expected
            }
        );
    }
    Ok(())
}

fn queue(config: QueueConfig) -> QueueFixture<MemoryProvider> {
    QueueFixture::new(MemoryProvider::new(), "tenant", "orders", config).unwrap()
}

fn send(
    fixture: &QueueFixture<MemoryProvider>,
    id: &str,
    ttl: Option<u64>,
    session_id: Option<SessionId>,
) -> SequenceNumber {
    match fixture
        .at(
            1,
            CommandKind::Send {
                message_id: id.to_owned(),
                body: id.as_bytes().to_vec(),
                time_to_live_millis: ttl,
                session_id,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        )
        .unwrap()
    {
        CommandOutcome::Sent { sequence } => sequence,
        other => panic!("expected send, got {other:?}"),
    }
}

fn lock(
    fixture: &QueueFixture<MemoryProvider>,
    at: u64,
    session: Option<domain::SessionHold>,
) -> Delivery {
    received(
        fixture
            .at(
                at,
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session,
                },
            )
            .unwrap(),
    )
}

/// The stored record and its leading version byte, in the queue or its DLQ.
fn row(
    fixture: &QueueFixture<MemoryProvider>,
    dead_letter: bool,
    sequence: SequenceNumber,
) -> Option<(u8, MessageRecord)> {
    let entity = if dead_letter {
        fixture.entity.dead_letter_queue().unwrap()
    } else {
        fixture.entity.clone()
    };
    let raw = fixture
        .machine
        .store()
        .get(&keys::message(&fixture.namespace, &entity, sequence))
        .unwrap()?;
    let record = MessageRecord::decode(&raw).unwrap();
    assert_eq!(codec::message::encode(&record).unwrap(), raw);
    if raw[0] == 1 {
        assert_eq!(former_encoding(&record), raw);
    }
    Some((raw[0], record))
}

fn validated_rows(fixture: &QueueFixture<MemoryProvider>) -> usize {
    let image = fixture
        .machine
        .store()
        .snapshot()
        .unwrap()
        .entries()
        .to_vec();
    validate_message_rows(&image).unwrap().message_rows()
}

#[derive(Clone, Copy, Debug)]
enum Disposition {
    Abandon,
    Defer,
    DeadLetter,
}

#[test]
fn a_replacement_envelope_becomes_v2_content_and_leaves_v1_unchanged() -> TestResult {
    for disposition in [
        Disposition::Abandon,
        Disposition::Defer,
        Disposition::DeadLetter,
    ] {
        let fixture = queue(QueueConfig {
            lock_duration_millis: 100,
            ..QueueConfig::default()
        });
        let sequences: Vec<_> = ["m-1", "m-2", "m-3"]
            .into_iter()
            .map(|id| send(&fixture, id, None, None))
            .collect();
        rewrite_as_v2(&fixture, 2, false);
        rewrite_as_v2(&fixture, 3, true);
        let replacement = MessageEnvelope::new(b"replacement".to_vec());
        let deliveries: Vec<_> = (0..3).map(|_| lock(&fixture, 2, None)).collect();
        for delivery in deliveries {
            let sequence = delivery.sequence;
            let lock_token = delivery.lock.unwrap().token;
            let replacement_envelope = Some(replacement.clone());
            fixture.at(
                3,
                match disposition {
                    Disposition::Abandon => CommandKind::Abandon {
                        sequence,
                        lock_token,
                        replacement_envelope,
                    },
                    Disposition::Defer => CommandKind::Defer {
                        sequence,
                        lock_token,
                        replacement_envelope,
                    },
                    Disposition::DeadLetter => CommandKind::DeadLetter {
                        sequence,
                        lock_token,
                        reason: String::from("Custom"),
                        description: String::new(),
                        replacement_envelope,
                    },
                },
            )?;
        }
        let dead_letter = matches!(disposition, Disposition::DeadLetter);
        for (index, sequence) in sequences.into_iter().enumerate() {
            let (version, record) = row(&fixture, dead_letter, sequence).unwrap();
            assert_eq!(
                record.envelope.as_ref(),
                Some(&replacement),
                "{disposition:?}"
            );
            assert!(record.envelope.as_ref().unwrap().overlay().is_empty());
            if index == 0 {
                assert_eq!(version, 1, "{disposition:?}");
                assert_eq!(record.body, b"m-1", "V1 keeps its body beside the envelope");
            } else {
                assert_eq!(version, 2, "{disposition:?}");
                assert!(record.body.is_empty(), "{disposition:?}");
            }
        }
        assert_eq!(validated_rows(&fixture), 3, "{disposition:?}");
    }
    Ok(())
}

#[test]
fn v2_rows_keep_their_version_through_deferral_and_dead_lettering() -> TestResult {
    let fixture = queue(QueueConfig {
        lock_duration_millis: 100,
        max_delivery_count: 1,
        ..QueueConfig::default()
    });
    let explicit = send(&fixture, "m-1", None, None);
    let exhausted = send(&fixture, "m-2", None, None);
    let expiring = send(&fixture, "m-3", Some(50), None);
    rewrite_as_v2(&fixture, 1, true);
    rewrite_as_v2(&fixture, 2, false);
    rewrite_as_v2(&fixture, 3, false);
    assert_eq!(validated_rows(&fixture), 3);

    let first = lock(&fixture, 2, None);
    assert_eq!(first.sequence, explicit);
    fixture.at(
        3,
        CommandKind::Defer {
            sequence: explicit,
            lock_token: first.lock.unwrap().token,
            replacement_envelope: None,
        },
    )?;
    let (version, record) = row(&fixture, false, explicit).unwrap();
    assert_eq!((version, record.state), (2, MessageState::Deferred));
    assert_eq!(validated_rows(&fixture), 3);

    let CommandOutcome::DeferredReceived(deferred) = fixture.at(
        4,
        CommandKind::ReceiveDeferred {
            sequences: vec![explicit],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("expected deferred receive");
    };
    assert_eq!(deferred[0].origin, DeliveryOrigin::Deferred);
    assert_eq!(deferred[0].envelope.as_ref().unwrap().overlay().len(), 1);
    let (version, record) = row(&fixture, false, explicit).unwrap();
    assert_eq!(version, 2);
    assert!(matches!(
        record.state,
        MessageState::Locked {
            origin: DeliveryOrigin::Deferred,
            ..
        }
    ));
    assert_eq!(validated_rows(&fixture), 3);
    fixture.at(
        5,
        CommandKind::DeadLetter {
            sequence: explicit,
            lock_token: deferred[0].lock.unwrap().token,
            reason: String::from("Custom"),
            description: String::from("why"),
            replacement_envelope: None,
        },
    )?;

    let second = lock(&fixture, 6, None);
    assert_eq!(second.sequence, exhausted);
    assert_eq!(
        fixture.at(
            7,
            CommandKind::Abandon {
                sequence: exhausted,
                lock_token: second.lock.unwrap().token,
                replacement_envelope: None,
            },
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true
        }
    );
    assert_eq!(
        fixture.at(100, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 1 }
    );

    for (sequence, reason) in [
        (
            explicit,
            DeadLetterReason::Application(String::from("Custom")),
        ),
        (exhausted, DeadLetterReason::MaxDeliveryCountExceeded),
        (expiring, DeadLetterReason::TimeToLiveExpired),
    ] {
        assert!(row(&fixture, false, sequence).is_none());
        let (version, record) = row(&fixture, true, sequence).unwrap();
        assert_eq!(version, 2, "{sequence}");
        assert_eq!(record.dead_letter.unwrap().reason, reason);
    }
    let (_, amqp) = row(&fixture, true, explicit).unwrap();
    assert!(amqp.body.is_empty());
    assert_eq!(amqp.envelope.unwrap().overlay()[0].name, "z");
    assert_eq!(row(&fixture, true, expiring).unwrap().1.body, b"m-3");
    assert_eq!(validated_rows(&fixture), 3);
    Ok(())
}

#[test]
fn session_v2_rows_validate_while_locked_and_after_settlement() -> TestResult {
    let fixture = queue(QueueConfig {
        lock_duration_millis: 100,
        requires_session: true,
        ..QueueConfig::default()
    });
    let first = send(&fixture, "m-1", None, session());
    let second = send(&fixture, "m-2", None, session());
    rewrite_as_v2(&fixture, 1, true);
    rewrite_as_v2(&fixture, 2, false);
    assert_eq!(validated_rows(&fixture), 2);

    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        2,
        CommandKind::AcceptSession {
            session_id: session(),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("expected session");
    };
    let hold = accepted.hold();
    let delivery = lock(&fixture, 3, Some(hold.clone()));
    assert_eq!(delivery.sequence, first);
    assert_eq!(delivery.session_id, session());
    assert_eq!(row(&fixture, false, first).unwrap().0, 2);
    assert_eq!(validated_rows(&fixture), 2);
    fixture.at(
        4,
        CommandKind::Complete {
            sequence: first,
            lock_token: delivery.lock.unwrap().token,
        },
    )?;
    let delivery = lock(&fixture, 5, Some(hold));
    assert_eq!(delivery.sequence, second);
    fixture.at(
        6,
        CommandKind::Defer {
            sequence: second,
            lock_token: delivery.lock.unwrap().token,
            replacement_envelope: None,
        },
    )?;
    let (version, record) = row(&fixture, false, second).unwrap();
    assert_eq!((version, record.state), (2, MessageState::Deferred));
    assert_eq!(record.session_id, session());
    assert_eq!(validated_rows(&fixture), 1);
    Ok(())
}
