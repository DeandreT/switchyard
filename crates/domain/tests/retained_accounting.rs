use domain::{
    DeadLetterInfo, DeadLetterReason, DeliveryOrigin, EntityPath, FilterProperties, LockToken,
    MessageEnvelope, MessageRecord, MessageRecordFormat, MessageState, NamespaceName,
    OverlayProperty, SequenceNumber, Timestamp, codec, keys,
    retained_accounting::{
        AccountingError, CapacityFramingBudget, CapacityOwner, ReservationDelta, RetainedUsage,
        calculate_batch, encoded_row_bytes, keys as accounting_keys, reserve_message, usage_codec,
    },
};
use storage::{Key, StateStore, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

fn owner() -> CapacityOwner {
    CapacityOwner::new(
        NamespaceName::new("Tenant").unwrap(),
        EntityPath::new("Orders").unwrap(),
        7,
    )
    .unwrap()
}

fn record(format: MessageRecordFormat, amqp: bool) -> MessageRecord {
    let envelope = amqp.then(|| {
        let mut properties = FilterProperties {
            subject: Some("subject".into()),
            ..FilterProperties::default()
        };
        properties.application_properties.insert(
            "a".into(),
            domain::CorrelationValue::new(vec![0x40]).unwrap(),
        );
        let envelope = MessageEnvelope::new(vec![0, 0x53, 0x75, 0xa0, 3, 1, 2, 3])
            .with_filter_properties(properties);
        if format == MessageRecordFormat::V1 {
            envelope
        } else {
            envelope.with_overlay(vec![OverlayProperty {
                name: "patched".into(),
                value: vec![0x40],
            }])
        }
    });
    MessageRecord {
        sequence: SequenceNumber::new(1),
        message_id: "id".into(),
        body: if amqp && format != MessageRecordFormat::V1 {
            vec![]
        } else {
            vec![1, 2, 3]
        },
        enqueued_at: Timestamp::from_millis(0),
        scheduled_enqueue_at: Some(Timestamp::from_millis(1)),
        expires_at: Some(Timestamp::from_millis(127)),
        delivery_count: 0,
        state: MessageState::Scheduled,
        session_id: None,
        dead_letter: None,
        envelope,
        format,
    }
}

fn rows(owner: &CapacityOwner, entity: &EntityPath, record: &MessageRecord) -> Vec<(Key, Value)> {
    let n = owner.namespace();
    let s = record.sequence;
    let mut rows = vec![(
        keys::message(n, entity, s),
        codec::message::encode(record).unwrap(),
    )];
    match record.state {
        MessageState::Ready => {
            rows.push((keys::ready(n, entity, s), vec![]));
            if let Some(t) = record.expires_at {
                rows.push((keys::expiry(n, entity, t, s), vec![]));
            }
        }
        MessageState::Deferred => rows.push((keys::deferred(n, entity, s), vec![])),
        MessageState::Scheduled => rows.push((
            keys::scheduled(n, entity, record.scheduled_enqueue_at.unwrap(), s),
            vec![],
        )),
        MessageState::Locked { locked_until, .. } => {
            rows.push((keys::lock(n, entity, locked_until, s), vec![]))
        }
    }
    rows
}

fn independent_bytes(rows: &[(Key, Value)]) -> u64 {
    rows.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum()
}

#[test]
fn exact_fixture_charges_only_six_tags_parent_and_shadow() {
    let owner = owner();
    let n = owner.namespace();
    let s = SequenceNumber::new(1);
    let t = Timestamp::from_millis(2);
    let mut batch = WriteBatch::default();
    for e in [owner.parent(), owner.shadow()] {
        for key in [
            keys::message(n, e, s),
            keys::ready(n, e, s),
            keys::lock(n, e, t, s),
            keys::expiry(n, e, t, s),
            keys::deferred(n, e, s),
            keys::scheduled(n, e, t, s),
        ] {
            batch.push_put(key, vec![0xaa, 0xbb]);
        }
    }
    // Explicit frozen fixture: parent scope 15, shadow scope 32, three
    // 8-byte and three 16-byte suffixes; each value is two bytes.
    let expected = 6 * 15 + 72 + 12 + 6 * 32 + 72 + 12;
    for tag in [
        0x00, 0x01, 0x02, 0x08, 0x09, 0x0A, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0xF0, 0xF1,
    ] {
        batch.push_put(vec![tag], vec![7; 100]);
    }
    let deltas = calculate_batch(std::slice::from_ref(&owner), &[], &batch).unwrap();
    assert_eq!(deltas[0].added_bytes, expected);
    assert_eq!(deltas[0].added_messages, 2);
    assert_eq!(deltas[0].removed_bytes, 0);
    assert_eq!(encoded_row_bytes(b"abc", b"defg"), Ok(7));
}

#[test]
fn final_mutation_and_each_original_key_count_once() {
    let owner = owner();
    let key = keys::message(owner.namespace(), owner.parent(), SequenceNumber::new(1));
    let absent = keys::ready(owner.namespace(), owner.parent(), SequenceNumber::new(100));
    let before = vec![(key.clone(), vec![1; 13])];
    let batch = WriteBatch::default()
        .put(key.clone(), vec![2; 500])
        .delete(key.clone())
        .put(key.clone(), vec![3; 7])
        .delete(absent.clone())
        .put(absent.clone(), vec![1])
        .delete(absent);
    let delta = &calculate_batch(std::slice::from_ref(&owner), &before, &batch).unwrap()[0];
    assert_eq!(delta.removed_bytes, key.len() as u64 + 13);
    assert_eq!(delta.added_bytes, key.len() as u64 + 7);
    assert_eq!((delta.removed_messages, delta.added_messages), (1, 1));
    let usage = RetainedUsage::new(7, delta.removed_bytes, 10, 1).unwrap();
    let updated = usage
        .apply(
            delta,
            ReservationDelta {
                removed: 10,
                added: 16,
            },
        )
        .unwrap();
    assert_eq!(updated.committed_bytes(), usage.committed_bytes());
    assert_eq!(before[0].1, vec![1; 13]);
    let delete = WriteBatch::default().delete(key.clone()).delete(key);
    let delta = &calculate_batch(&[owner], &before, &delete).unwrap()[0];
    assert_eq!(
        usage
            .apply(
                delta,
                ReservationDelta {
                    removed: 10,
                    added: 0
                }
            )
            .unwrap()
            .message_count(),
        0
    );
}

#[test]
fn refuses_ambiguous_corrupt_unknown_and_unowned_inputs() {
    let owner = owner();
    let row = (
        keys::ready(owner.namespace(), owner.parent(), SequenceNumber::new(1)),
        vec![],
    );
    assert_eq!(
        calculate_batch(
            std::slice::from_ref(&owner),
            &[row.clone(), row.clone()],
            &WriteBatch::default()
        ),
        Err(AccountingError::DuplicateBeforeImage)
    );
    assert_eq!(
        calculate_batch(&[owner.clone(), owner.clone()], &[], &WriteBatch::default()),
        Err(AccountingError::DuplicateOwner)
    );
    for bad in [
        vec![],
        vec![0x13],
        b"\x03tenant\0orders\0".to_vec(),
        b"\x04Tenant\0orders\0\0\0\0\0\0\0\0\x01".to_vec(),
    ] {
        assert_eq!(
            calculate_batch(
                std::slice::from_ref(&owner),
                &[],
                &WriteBatch::default().put(bad, vec![])
            ),
            Err(AccountingError::InvalidKey)
        );
    }
    let foreign = keys::ready(
        owner.namespace(),
        &EntityPath::new("orders-neighbor").unwrap(),
        SequenceNumber::new(1),
    );
    assert_eq!(
        calculate_batch(
            std::slice::from_ref(&owner),
            &[],
            &WriteBatch::default()
                .put(foreign.clone(), vec![])
                .delete(foreign)
        ),
        Err(AccountingError::UnownedKey)
    );
    assert_eq!(
        calculate_batch(&[], &[row], &WriteBatch::default()),
        Err(AccountingError::UnownedKey)
    );
    assert_eq!(
        CapacityOwner::new(owner.namespace().clone(), owner.shadow().clone(), 7),
        Err(AccountingError::InvalidOwner)
    );
    assert_eq!(
        CapacityOwner::new(owner.namespace().clone(), owner.parent().clone(), 0),
        Err(AccountingError::InvalidOwner)
    );
}

#[test]
fn unsigned_totals_refuse_overflow_underflow_and_generation_mismatch() {
    assert_eq!(
        RetainedUsage::new(1, u64::MAX, 1, 1),
        Err(AccountingError::Overflow)
    );
    assert_eq!(
        RetainedUsage::new(1, 1, 0, 0),
        Err(AccountingError::CorruptUsage)
    );
    assert_eq!(
        RetainedUsage::new(1, 0, 0, 1),
        Err(AccountingError::CorruptUsage)
    );
    let owner = owner();
    let row = (
        keys::message(owner.namespace(), owner.parent(), SequenceNumber::new(1)),
        vec![1],
    );
    let deltas = calculate_batch(
        &[owner],
        std::slice::from_ref(&row),
        &WriteBatch::default().delete(row.0.clone()),
    )
    .unwrap();
    assert_eq!(
        RetainedUsage::new(7, 1, 0, 1)
            .unwrap()
            .apply(&deltas[0], ReservationDelta::default()),
        Err(AccountingError::Underflow)
    );
    assert_eq!(
        RetainedUsage::new(8, 1, 0, 1)
            .unwrap()
            .apply(&deltas[0], ReservationDelta::default()),
        Err(AccountingError::GenerationMismatch)
    );
    let mut delta = deltas[0].clone();
    delta.removed_bytes = 0;
    delta.removed_messages = 0;
    delta.added_bytes = 1;
    assert_eq!(
        RetainedUsage::new(7, u64::MAX, 0, 1)
            .unwrap()
            .apply(&delta, ReservationDelta::default()),
        Err(AccountingError::Overflow)
    );
    delta.added_bytes = 0;
    delta.added_messages = 1;
    assert_eq!(
        RetainedUsage::new(7, u64::MAX, 0, u64::MAX)
            .unwrap()
            .apply(&delta, ReservationDelta::default()),
        Err(AccountingError::Overflow)
    );
    delta.added_messages = 0;
    assert_eq!(
        RetainedUsage::new(7, 1, 1, 1).unwrap().apply(
            &delta,
            ReservationDelta {
                removed: 2,
                added: 0
            }
        ),
        Err(AccountingError::Underflow)
    );
}

#[test]
fn usage_codec_is_fixed_canonical_key_and_generation_bound() {
    let owner = owner();
    let usage = RetainedUsage::new(7, 100, 200, 1).unwrap();
    let bytes = usage_codec::encode(&owner, usage).unwrap();
    assert_eq!(accounting_keys::usage(&owner), b"\x12tenant\0orders\0");
    assert_eq!(bytes.len(), 72);
    assert_eq!(&bytes[..8], b"SYBU\0\x01\0\x01");
    assert_eq!(
        &bytes[8..40],
        &[
            7_u64.to_be_bytes(),
            100_u64.to_be_bytes(),
            200_u64.to_be_bytes(),
            1_u64.to_be_bytes()
        ]
        .concat()
    );
    assert_eq!(usage_codec::decode(&owner, &bytes), Ok(usage));
    for i in 0..bytes.len() {
        let mut bad = bytes.clone();
        bad[i] ^= 1;
        assert!(usage_codec::decode(&owner, &bad).is_err(), "byte {i}");
    }
    assert!(usage_codec::decode(&owner, &bytes[..71]).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(usage_codec::decode(&owner, &trailing).is_err());
    let newer = CapacityOwner::new(owner.namespace().clone(), owner.parent().clone(), 8).unwrap();
    assert_eq!(
        usage_codec::decode(&newer, &bytes),
        Err(AccountingError::GenerationMismatch)
    );
    assert_eq!(
        usage_codec::encode(&newer, usage),
        Err(AccountingError::GenerationMismatch)
    );
    let foreign = CapacityOwner::new(
        owner.namespace().clone(),
        EntityPath::new("other").unwrap(),
        7,
    )
    .unwrap();
    assert_eq!(
        usage_codec::decode(&foreign, &bytes),
        Err(AccountingError::CorruptUsage)
    );
    assert!(matches!(
        domain::snapshot_validation::validate_catalog(&[(accounting_keys::usage(&owner), bytes)]),
        Err(domain::snapshot_validation::SnapshotCatalogError::UnsupportedTag { tag: 0x12, .. })
    ));
}

#[test]
fn current_encoding_bounds_cover_all_automatic_states_and_varint_edges() {
    let owner = owner();
    for format in [
        MessageRecordFormat::V1,
        MessageRecordFormat::V2,
        MessageRecordFormat::V3,
    ] {
        for amqp in [false, true] {
            let original = record(format, amqp);
            let initial = rows(&owner, owner.parent(), &original);
            let prepaid = reserve_message(
                &owner,
                &initial,
                CapacityFramingBudget::AdditionalMessageValueBytes(37),
            )
            .unwrap();
            assert_eq!(prepaid.retained_bytes, independent_bytes(&initial));
            assert_eq!(
                prepaid.ceiling_bytes,
                prepaid.retained_bytes + prepaid.reserved_growth_bytes
            );
            let unframed = reserve_message(
                &owner,
                &initial,
                CapacityFramingBudget::CurrentUnframedEncoding,
            )
            .unwrap();
            assert_eq!(prepaid.ceiling_bytes, unframed.ceiling_bytes + 37);
            for number in [0, 127, 128, 16383, 16384, u32::MAX as u64, u64::MAX] {
                let mut next = original.clone();
                next.sequence = SequenceNumber::new(number);
                next.enqueued_at = Timestamp::from_millis(number);
                next.expires_at = Some(Timestamp::from_millis(number));
                next.scheduled_enqueue_at = Some(Timestamp::from_millis(number));
                next.delivery_count = number.min(u32::MAX as u64) as u32;
                for state in [
                    MessageState::Ready,
                    MessageState::Deferred,
                    MessageState::Scheduled,
                    MessageState::Locked {
                        token: LockToken::new(number),
                        locked_until: Timestamp::from_millis(number),
                        origin: DeliveryOrigin::Ready,
                    },
                    MessageState::Locked {
                        token: LockToken::new(number),
                        locked_until: Timestamp::from_millis(number),
                        origin: DeliveryOrigin::Deferred,
                    },
                ] {
                    next.state = state;
                    let current = rows(&owner, owner.parent(), &next);
                    let reservation = reserve_message(
                        &owner,
                        &current,
                        CapacityFramingBudget::AdditionalMessageValueBytes(37),
                    )
                    .unwrap();
                    assert_eq!(reservation.ceiling_bytes, prepaid.ceiling_bytes);
                    assert!(independent_bytes(&current) + 37 <= prepaid.ceiling_bytes);
                    assert_eq!(next.envelope, original.envelope);
                    assert_eq!(next.body, original.body);
                }
                next.expires_at = None;
                for (reason, description) in [
                    (
                        DeadLetterReason::TimeToLiveExpired,
                        "the message exceeded its time to live",
                    ),
                    (
                        DeadLetterReason::TimeToLiveExpired,
                        "the deferred message exceeded its time to live",
                    ),
                    (
                        DeadLetterReason::MaxDeliveryCountExceeded,
                        "the message reached its maximum delivery count",
                    ),
                ] {
                    next.dead_letter = Some(DeadLetterInfo {
                        reason,
                        description: description.into(),
                        dead_lettered_at: Timestamp::from_millis(number),
                    });
                    for state in [
                        MessageState::Ready,
                        MessageState::Deferred,
                        MessageState::Locked {
                            token: LockToken::new(number),
                            locked_until: Timestamp::from_millis(number),
                            origin: DeliveryOrigin::Ready,
                        },
                    ] {
                        next.state = state;
                        let current = rows(&owner, owner.shadow(), &next);
                        let reserved = reserve_message(
                            &owner,
                            &current,
                            CapacityFramingBudget::AdditionalMessageValueBytes(37),
                        )
                        .unwrap();
                        assert!(reserved.ceiling_bytes <= prepaid.ceiling_bytes);
                        assert!(independent_bytes(&current) + 37 <= prepaid.ceiling_bytes);
                    }
                }
            }
        }
    }
}

#[test]
fn reservation_refuses_incomplete_corrupt_session_and_unbounded_rewrite_assumptions() {
    let owner = owner();
    let original = record(MessageRecordFormat::V3, true);
    let initial = rows(&owner, owner.parent(), &original);
    assert!(
        reserve_message(
            &owner,
            &initial[..1],
            CapacityFramingBudget::CurrentUnframedEncoding
        )
        .is_err()
    );
    let mut corrupt = initial.clone();
    corrupt[0].1.push(0);
    assert_eq!(
        reserve_message(
            &owner,
            &corrupt,
            CapacityFramingBudget::CurrentUnframedEncoding
        ),
        Err(AccountingError::InvalidMessage)
    );
    let mut future = initial.clone();
    future[0].1[0] = 4;
    assert_eq!(
        reserve_message(
            &owner,
            &future,
            CapacityFramingBudget::CurrentUnframedEncoding
        ),
        Err(AccountingError::InvalidMessage)
    );
    corrupt = initial.clone();
    corrupt[1].1.push(0);
    assert_eq!(
        reserve_message(
            &owner,
            &corrupt,
            CapacityFramingBudget::CurrentUnframedEncoding
        ),
        Err(AccountingError::InvalidMessage)
    );
    assert_eq!(
        reserve_message(
            &owner,
            &initial,
            CapacityFramingBudget::AdditionalMessageValueBytes(u64::MAX)
        ),
        Err(AccountingError::Overflow)
    );
    let mut session = original.clone();
    session.session_id = Some(domain::SessionId::new("s").unwrap());
    assert_eq!(
        reserve_message(
            &owner,
            &rows(&owner, owner.parent(), &session),
            CapacityFramingBudget::CurrentUnframedEncoding
        ),
        Err(AccountingError::InvalidMessage)
    );
    let previous = reserve_message(
        &owner,
        &initial,
        CapacityFramingBudget::CurrentUnframedEncoding,
    )
    .unwrap();
    let mut changed = original;
    changed.envelope = Some(
        changed
            .envelope
            .unwrap()
            .with_overlay(vec![OverlayProperty {
                name: "external".into(),
                value: vec![1; 10000],
            }]),
    );
    let rewritten = reserve_message(
        &owner,
        &rows(&owner, owner.parent(), &changed),
        CapacityFramingBudget::CurrentUnframedEncoding,
    )
    .unwrap();
    assert!(rewritten.ceiling_bytes > previous.ceiling_bytes);
    // Already admitted application metadata remains bounded by its actual
    // encoding, however large; automatic lock/renew never appends to it.
    changed.state = MessageState::Ready;
    changed.expires_at = None;
    changed.dead_letter = Some(DeadLetterInfo {
        reason: DeadLetterReason::Application("x".repeat(1000)),
        description: "y".repeat(1000),
        dead_lettered_at: Timestamp::from_millis(1),
    });
    assert!(
        reserve_message(
            &owner,
            &rows(&owner, owner.shadow(), &changed),
            CapacityFramingBudget::CurrentUnframedEncoding
        )
        .unwrap()
        .ceiling_bytes
            > rewritten.ceiling_bytes
    );
}

fn backend_rows<P: StoreProvider>(provider: P) {
    let owner = owner();
    let store = provider.open().unwrap();
    let original = rows(
        &owner,
        owner.parent(),
        &record(MessageRecordFormat::V1, true),
    );
    let mut batch = WriteBatch::default();
    for (key, value) in &original {
        batch.push_put(key.clone(), value.clone());
    }
    let delta = calculate_batch(std::slice::from_ref(&owner), &[], &batch).unwrap();
    store.apply(batch).unwrap();
    let image = store.snapshot().unwrap();
    assert_eq!(image.entries(), original);
    let bound = reserve_message(
        &owner,
        image.entries(),
        CapacityFramingBudget::CurrentUnframedEncoding,
    )
    .unwrap();
    assert_eq!(delta[0].added_bytes, independent_bytes(image.entries()));
    let usage =
        RetainedUsage::new(7, bound.retained_bytes, bound.reserved_growth_bytes, 1).unwrap();
    let bytes = usage_codec::encode(&owner, usage).unwrap();
    // All calculations, including refusal, leave every raw row unchanged.
    assert!(
        reserve_message(
            &owner,
            &original[..1],
            CapacityFramingBudget::CurrentUnframedEncoding
        )
        .is_err()
    );
    assert_eq!(store.snapshot().unwrap(), image);
    store
        .apply(WriteBatch::default().put(accounting_keys::usage(&owner), bytes.clone()))
        .unwrap();
    drop(store);
    let reopened = provider.open().unwrap();
    assert_eq!(
        reopened
            .get(&accounting_keys::usage(&owner))
            .unwrap()
            .unwrap(),
        bytes
    );
    assert_eq!(
        usage_codec::decode(
            &owner,
            &reopened
                .get(&accounting_keys::usage(&owner))
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        usage
    );
    for (key, value) in original {
        assert_eq!(reopened.get(&key).unwrap(), Some(value));
    }
}

#[test]
fn memory_raw_rows_and_read_only_calculations() {
    backend_rows(MemoryProvider::new());
}

#[test]
fn fjall_raw_rows_and_reopen() {
    backend_rows(DurableProvider::temporary().unwrap());
}

#[derive(Clone, Copy)]
enum AutomaticCase {
    MaxDelivery,
    LockedTtl,
    DeferredTtl,
}

fn actual_lifecycle<P: StoreProvider>(provider: P, case: AutomaticCase) {
    use domain::{Command, CommandKind, CommandOutcome, QueueConfig, ReceiveMode};
    let ttl_dlq = !matches!(case, AutomaticCase::MaxDelivery);
    let fixture = testkit::QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            lock_duration_millis: 1,
            max_delivery_count: 1,
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )
    .unwrap();
    let owner = owner();
    let source = record(MessageRecordFormat::V1, true);
    fixture
        .at(
            1,
            CommandKind::Send {
                message_id: source.message_id,
                body: source.body,
                time_to_live_millis: Some(if ttl_dlq { 10 } else { 1000 }),
                session_id: None,
                scheduled_enqueue_at: Some(Timestamp::from_millis(2)),
                envelope: source.envelope,
            },
        )
        .unwrap();
    let check = || {
        let before = fixture.machine.store().snapshot().unwrap();
        let message_rows: Vec<_> = before
            .entries()
            .iter()
            .filter(|(k, _)| matches!(k.first(), Some(0x03..=0x07 | 0x0B)))
            .cloned()
            .collect();
        let bound = reserve_message(
            &owner,
            &message_rows,
            CapacityFramingBudget::CurrentUnframedEncoding,
        )
        .unwrap();
        assert_eq!(bound.retained_bytes, independent_bytes(&message_rows));
        // A full raw before-image retains profiles, Clock and counters. Pure
        // accounting neither changes them nor counts them toward capacity.
        let delta = calculate_batch(
            std::slice::from_ref(&owner),
            before.entries(),
            &WriteBatch::default(),
        )
        .unwrap();
        assert_eq!(delta[0].removed_bytes + delta[0].added_bytes, 0);
        assert_eq!(fixture.machine.store().snapshot().unwrap(), before);
        bound
    };
    let prepaid = check();
    fixture.at(2, CommandKind::ActivateScheduled).unwrap();
    assert_eq!(check().ceiling_bytes, prepaid.ceiling_bytes);
    let CommandOutcome::Received(Some(delivery)) = fixture
        .at(
            3,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        )
        .unwrap()
    else {
        panic!("delivery")
    };
    assert_eq!(check().ceiling_bytes, prepaid.ceiling_bytes);
    fixture
        .at(
            3,
            CommandKind::RenewLock {
                sequence: delivery.sequence,
                lock_token: delivery.lock.unwrap().token,
                lock_duration_millis: None,
            },
        )
        .unwrap();
    assert_eq!(check().ceiling_bytes, prepaid.ceiling_bytes);
    if matches!(case, AutomaticCase::DeferredTtl) {
        fixture
            .at(
                3,
                CommandKind::Defer {
                    sequence: delivery.sequence,
                    lock_token: delivery.lock.unwrap().token,
                    replacement_envelope: None,
                },
            )
            .unwrap();
        assert_eq!(check().ceiling_bytes, prepaid.ceiling_bytes);
        fixture
            .at(
                20,
                CommandKind::ReceiveDeferred {
                    sequences: vec![delivery.sequence],
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            )
            .unwrap();
    } else {
        fixture
            .at(if ttl_dlq { 20 } else { 5 }, CommandKind::ExpireLocks)
            .unwrap();
    }
    assert!(check().ceiling_bytes <= prepaid.ceiling_bytes);
    let shadow = fixture
        .machine
        .message(owner.namespace(), owner.shadow(), delivery.sequence)
        .unwrap()
        .unwrap();
    let dead_letter = shadow.dead_letter.unwrap();
    assert_eq!(
        dead_letter.reason,
        if ttl_dlq {
            DeadLetterReason::TimeToLiveExpired
        } else {
            DeadLetterReason::MaxDeliveryCountExceeded
        }
    );
    assert_eq!(
        dead_letter.description,
        match case {
            AutomaticCase::MaxDelivery => "the message reached its maximum delivery count",
            AutomaticCase::LockedTtl => "the message exceeded its time to live",
            AutomaticCase::DeferredTtl => "the deferred message exceeded its time to live",
        }
    );
    let CommandOutcome::Received(Some(dlq)) = fixture
        .machine
        .apply(&Command::new(
            owner.namespace().clone(),
            owner.shadow().clone(),
            Timestamp::from_millis(21),
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        ))
        .unwrap()
    else {
        panic!("DLQ delivery")
    };
    assert!(check().ceiling_bytes <= prepaid.ceiling_bytes);
    fixture
        .machine
        .apply(&Command::new(
            owner.namespace().clone(),
            owner.shadow().clone(),
            Timestamp::from_millis(21),
            CommandKind::Complete {
                sequence: dlq.sequence,
                lock_token: dlq.lock.unwrap().token,
            },
        ))
        .unwrap();
    assert!(
        fixture
            .machine
            .store()
            .scan_prefix(&keys::message_prefix(owner.namespace(), owner.shadow()), 1)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn actual_automatic_lifecycle_stays_within_prepaid_bound_memory() {
    actual_lifecycle(MemoryProvider::new(), AutomaticCase::MaxDelivery);
    actual_lifecycle(MemoryProvider::new(), AutomaticCase::LockedTtl);
    actual_lifecycle(MemoryProvider::new(), AutomaticCase::DeferredTtl);
}

#[test]
fn actual_automatic_lifecycle_stays_within_prepaid_bound_fjall() {
    actual_lifecycle(
        DurableProvider::temporary().unwrap(),
        AutomaticCase::MaxDelivery,
    );
    actual_lifecycle(
        DurableProvider::temporary().unwrap(),
        AutomaticCase::LockedTtl,
    );
    actual_lifecycle(
        DurableProvider::temporary().unwrap(),
        AutomaticCase::DeferredTtl,
    );
}

#[test]
fn equal_generations_in_separate_owners_never_share_deltas() {
    let first = owner();
    let second = CapacityOwner::new(
        first.namespace().clone(),
        EntityPath::new("other").unwrap(),
        first.generation(),
    )
    .unwrap();
    let left = keys::message(first.namespace(), first.shadow(), SequenceNumber::new(1));
    let right = keys::message(second.namespace(), second.parent(), SequenceNumber::new(1));
    let before = vec![(left.clone(), vec![1; 100])];
    let batch = WriteBatch::default()
        .delete(left.clone())
        .put(right.clone(), vec![2; 40]);
    let deltas = calculate_batch(&[first, second], &before, &batch).unwrap();
    assert_eq!(
        (deltas[0].removed_bytes, deltas[0].added_bytes),
        (left.len() as u64 + 100, 0)
    );
    assert_eq!(
        (deltas[1].removed_bytes, deltas[1].added_bytes),
        (0, right.len() as u64 + 40)
    );
}
