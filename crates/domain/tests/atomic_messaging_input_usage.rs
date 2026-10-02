use std::collections::BTreeMap;

use domain::{
    AnnotationKey, AtomicMessagingInputUsage, AtomicMessagingLimit, BrokerError, CommandKind,
    IngressEnvelope, LockToken, MAX_ATOMIC_MESSAGING_ACTIONS, MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
    MAX_ATOMIC_MESSAGING_MESSAGES, MAX_ATOMIC_MESSAGING_VALUE_ITEMS, MAX_MESSAGE_VALUE_DEPTH,
    MessageBody, MessageEnvelope, MessageValue, SequenceNumber, SessionId, SettlementDisposition,
    Timestamp, validate_atomic_messaging_kinds,
};

fn send(id: &str, body: Vec<u8>) -> CommandKind {
    CommandKind::Send {
        message_id: id.to_owned(),
        body,
        time_to_live_millis: None,
        session_id: None,
    }
}

fn envelope_send(envelope: MessageEnvelope) -> CommandKind {
    CommandKind::SendEnvelope {
        message_id: String::new(),
        body: vec![],
        time_to_live_millis: None,
        session_id: None,
        envelope: Box::new(envelope),
    }
}

fn member(id: &str) -> IngressEnvelope {
    IngressEnvelope {
        message_id: id.to_owned(),
        body: vec![1, 2],
        time_to_live_millis: Some(100),
        session_id: None,
        envelope: MessageEnvelope::default(),
        scheduled_enqueue_time: None,
    }
}

fn complete() -> CommandKind {
    CommandKind::Complete {
        sequence: SequenceNumber::new(1),
        lock_token: LockToken::new(1),
    }
}

fn limit(limit: AtomicMessagingLimit, maximum: usize) -> BrokerError {
    BrokerError::AtomicMessagingTooLarge { limit, maximum }
}

fn counts(usage: AtomicMessagingInputUsage) -> (usize, usize, usize, usize) {
    (
        usage.actions(),
        usage.messages(),
        usage.content_bytes(),
        usage.value_items(),
    )
}

fn fold(kinds: &[CommandKind]) -> Result<AtomicMessagingInputUsage, BrokerError> {
    let mut usage = AtomicMessagingInputUsage::default();
    for kind in kinds {
        usage.try_extend(kind)?;
    }
    Ok(usage)
}

fn nested(depth: usize) -> MessageValue {
    let mut value = MessageValue::Null;
    for _ in 0..depth {
        value = MessageValue::List(vec![value]);
    }
    value
}

fn nonzero_usage() -> AtomicMessagingInputUsage {
    fold(&[
        complete(),
        envelope_send(MessageEnvelope {
            body: MessageBody::Value(MessageValue::Bool(true)),
            ..MessageEnvelope::default()
        }),
    ])
    .expect("valid nonzero seed")
}

fn assert_refusal_is_atomic(kind: &CommandKind, expected: BrokerError) {
    let mut usage = nonzero_usage();
    let before = usage;
    assert_eq!(usage.try_extend(kind), Err(expected));
    assert_eq!(usage, before, "refusal must preserve every counter");
    usage
        .try_extend(&complete())
        .expect("healthy extension after refusal");
    assert_eq!(
        counts(usage),
        (
            before.actions() + 1,
            before.messages(),
            before.content_bytes(),
            before.value_items(),
        )
    );
}

#[test]
fn usage_is_copy_and_default_has_no_payload_or_counts() {
    fn public_traits<T: Copy + Clone + Default + std::fmt::Debug + Eq>() {}
    public_traits::<AtomicMessagingInputUsage>();
    let usage = AtomicMessagingInputUsage::default();
    assert_eq!(counts(usage), (0, 0, 0, 0));
    let mut extended = usage;
    extended
        .try_extend(&complete())
        .expect("one settlement action");
    assert_eq!(counts(usage), (0, 0, 0, 0));
    assert_eq!(counts(extended), (1, 0, 0, 0));
}

#[test]
fn empty_batches_count_actions_without_messages_content_or_values() {
    let empty = CommandKind::SendBatch { messages: vec![] };
    let mut usage = AtomicMessagingInputUsage::default();
    usage.try_extend(&empty).expect("empty batch");
    assert_eq!(counts(usage), (1, 0, 0, 0));
    assert_eq!(validate_atomic_messaging_kinds(&[]), Ok(()));
    assert_eq!(fold(&[]), Ok(AtomicMessagingInputUsage::default()));
}

#[test]
fn action_limit_is_exact_and_refused_action_cannot_change_counts() {
    let mut usage = AtomicMessagingInputUsage::default();
    for _ in 0..MAX_ATOMIC_MESSAGING_ACTIONS {
        usage.try_extend(&complete()).expect("action within limit");
    }
    assert_eq!(counts(usage), (MAX_ATOMIC_MESSAGING_ACTIONS, 0, 0, 0));
    let before = usage;
    assert_eq!(
        usage.try_extend(&send("never-counted", vec![1])),
        Err(limit(
            AtomicMessagingLimit::Actions,
            MAX_ATOMIC_MESSAGING_ACTIONS
        ))
    );
    assert_eq!(usage, before);
}

#[test]
fn message_limit_counts_repeated_ids_and_late_batch_preflight_is_atomic() {
    let mut usage = AtomicMessagingInputUsage::default();
    let first = CommandKind::SendBatch {
        messages: vec![member("same"); MAX_ATOMIC_MESSAGING_MESSAGES / 2],
    };
    usage
        .try_extend(&first)
        .expect("first half of logical sends");
    let before = usage;
    let mut too_many = vec![member("same"); MAX_ATOMIC_MESSAGING_MESSAGES / 2 + 1];
    too_many.last_mut().expect("last member").session_id =
        Some(SessionId::new("unsupported").expect("session identifier"));
    assert_eq!(
        usage.try_extend(&CommandKind::SendBatch { messages: too_many }),
        Err(limit(
            AtomicMessagingLimit::Messages,
            MAX_ATOMIC_MESSAGING_MESSAGES
        ))
    );
    assert_eq!(
        usage, before,
        "batch length wins before unsupported late member"
    );
    usage
        .try_extend(&first)
        .expect("exact logical message limit");
    assert_eq!(usage.messages(), MAX_ATOMIC_MESSAGING_MESSAGES);
    assert_eq!(usage.actions(), 2);
    let before = usage;
    assert_eq!(
        usage.try_extend(&send("same", vec![])),
        Err(limit(
            AtomicMessagingLimit::Messages,
            MAX_ATOMIC_MESSAGING_MESSAGES
        ))
    );
    assert_eq!(usage, before);
    usage
        .try_extend(&complete())
        .expect("settlement does not consume a send slot");
    assert_eq!(usage.messages(), MAX_ATOMIC_MESSAGING_MESSAGES);
    assert_eq!(usage.actions(), 3);
}

#[test]
fn content_limit_is_shared_and_exact_including_message_ids() {
    let half = MAX_ATOMIC_MESSAGING_CONTENT_BYTES / 2;
    let mut usage = AtomicMessagingInputUsage::default();
    usage
        .try_extend(&send("a", vec![7; half - 1]))
        .expect("first half");
    let before = usage;
    assert_eq!(
        usage.try_extend(&send("bb", vec![9; half - 1])),
        Err(limit(
            AtomicMessagingLimit::ContentBytes,
            MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
        ))
    );
    assert_eq!(usage, before);
    usage
        .try_extend(&send("b", vec![9; half - 1]))
        .expect("exact byte limit");
    assert_eq!(counts(usage), (2, 2, MAX_ATOMIC_MESSAGING_CONTENT_BYTES, 0));
    let before = usage;
    assert_eq!(
        usage.try_extend(&send("", vec![0])),
        Err(limit(
            AtomicMessagingLimit::ContentBytes,
            MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
        ))
    );
    assert_eq!(usage, before);
    usage
        .try_extend(&complete())
        .expect("content-free healthy action");
    assert_eq!(usage.content_bytes(), MAX_ATOMIC_MESSAGING_CONTENT_BYTES);
}

#[test]
fn value_limit_is_shared_and_exact_including_container_nodes() {
    let half = MAX_ATOMIC_MESSAGING_VALUE_ITEMS / 2;
    let values = envelope_send(MessageEnvelope {
        body: MessageBody::Value(MessageValue::Array(vec![MessageValue::Null; half - 1])),
        ..MessageEnvelope::default()
    });
    let mut usage = AtomicMessagingInputUsage::default();
    usage.try_extend(&values).expect("first half of values");
    usage.try_extend(&values).expect("exact value limit");
    assert_eq!(usage.value_items(), MAX_ATOMIC_MESSAGING_VALUE_ITEMS);
    assert_eq!(usage.messages(), 2);
    let before = usage;
    assert_eq!(
        usage.try_extend(&envelope_send(MessageEnvelope {
            body: MessageBody::Value(MessageValue::Null),
            ..MessageEnvelope::default()
        })),
        Err(limit(
            AtomicMessagingLimit::ValueItems,
            MAX_ATOMIC_MESSAGING_VALUE_ITEMS,
        ))
    );
    assert_eq!(usage, before);
    usage
        .try_extend(&complete())
        .expect("value-free healthy action");
    assert_eq!(usage.value_items(), MAX_ATOMIC_MESSAGING_VALUE_ITEMS);
}

#[test]
fn late_batch_session_and_each_scheduled_timestamp_leave_all_counts_unchanged() {
    let mut late = member("last");
    late.session_id = Some(SessionId::new("session").expect("session identifier"));
    assert_refusal_is_atomic(
        &CommandKind::SendBatch {
            messages: vec![member("first"), late],
        },
        BrokerError::AtomicMessagingOperationNotSupported,
    );
    for timestamp in [0, 1, u64::MAX] {
        let mut late = member("last");
        late.scheduled_enqueue_time = Some(Timestamp::from_millis(timestamp));
        assert_refusal_is_atomic(
            &CommandKind::SendBatch {
                messages: vec![member("first"), late],
            },
            BrokerError::AtomicMessagingOperationNotSupported,
        );
    }
}

#[test]
fn late_batch_content_and_value_overflows_do_not_keep_earlier_members() {
    let mut oversized = member("last");
    oversized.body = vec![0; MAX_ATOMIC_MESSAGING_CONTENT_BYTES];
    assert_refusal_is_atomic(
        &CommandKind::SendBatch {
            messages: vec![member("first"), oversized],
        },
        limit(
            AtomicMessagingLimit::ContentBytes,
            MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
        ),
    );
    let mut too_many_values = member("last");
    too_many_values.envelope.body = MessageBody::Value(MessageValue::Array(vec![
        MessageValue::Null;
        MAX_ATOMIC_MESSAGING_VALUE_ITEMS
            - 1
    ]));
    assert_refusal_is_atomic(
        &CommandKind::SendBatch {
            messages: vec![member("first"), too_many_values],
        },
        limit(
            AtomicMessagingLimit::ValueItems,
            MAX_ATOMIC_MESSAGING_VALUE_ITEMS,
        ),
    );
}

#[test]
fn single_session_sends_and_unsupported_actions_do_not_charge_input() {
    let session = SessionId::new("session").expect("session identifier");
    let mut legacy = send("not-counted", vec![1, 2]);
    if let CommandKind::Send { session_id, .. } = &mut legacy {
        *session_id = Some(session.clone());
    }
    assert_refusal_is_atomic(&legacy, BrokerError::AtomicMessagingOperationNotSupported);
    let mut rich = envelope_send(MessageEnvelope::default());
    if let CommandKind::SendEnvelope { session_id, .. } = &mut rich {
        *session_id = Some(session);
    }
    assert_refusal_is_atomic(&rich, BrokerError::AtomicMessagingOperationNotSupported);
    assert_refusal_is_atomic(
        &CommandKind::RenewLock {
            sequence: SequenceNumber::new(1),
            lock_token: LockToken::new(1),
            lock_duration_millis: None,
        },
        BrokerError::AtomicMessagingOperationNotSupported,
    );
}

#[test]
fn late_depth_failure_rolls_back_counts_and_the_exact_depth_remains_usable() {
    let deep = envelope_send(MessageEnvelope {
        body: MessageBody::Value(nested(MAX_MESSAGE_VALUE_DEPTH + 1)),
        ..MessageEnvelope::default()
    });
    let mut usage = nonzero_usage();
    let before = usage;
    assert!(matches!(
        usage.try_extend(&deep),
        Err(BrokerError::InvalidMessageContent { .. })
    ));
    assert_eq!(usage, before);
    let valid = envelope_send(MessageEnvelope {
        body: MessageBody::Value(nested(MAX_MESSAGE_VALUE_DEPTH)),
        ..MessageEnvelope::default()
    });
    usage.try_extend(&valid).expect("exact depth limit");
    assert_eq!(usage.actions(), before.actions() + 1);
    assert_eq!(usage.messages(), before.messages() + 1);
    assert_eq!(
        usage.value_items(),
        before.value_items() + MAX_MESSAGE_VALUE_DEPTH + 1
    );

    let mut late = member("last");
    late.envelope.body = MessageBody::Value(nested(MAX_MESSAGE_VALUE_DEPTH + 1));
    let mut usage = nonzero_usage();
    let before = usage;
    assert!(matches!(
        usage.try_extend(&CommandKind::SendBatch {
            messages: vec![member("first"), late]
        }),
        Err(BrokerError::InvalidMessageContent { .. })
    ));
    assert_eq!(usage, before);
    usage
        .try_extend(&complete())
        .expect("healthy extension after late depth failure");

    let patch = CommandKind::Settle {
        sequence: SequenceNumber::new(1),
        lock_token: LockToken::new(1),
        disposition: SettlementDisposition::Abandon,
        properties_to_modify: BTreeMap::from([
            ("a-first".into(), MessageValue::Bool(true)),
            ("z-last".into(), nested(MAX_MESSAGE_VALUE_DEPTH + 1)),
        ]),
    };
    let before = usage;
    assert!(matches!(
        usage.try_extend(&patch),
        Err(BrokerError::InvalidMessageContent { .. })
    ));
    assert_eq!(usage, before);
    usage
        .try_extend(&complete())
        .expect("healthy extension after late patch depth failure");
}

#[test]
fn settlement_patches_details_and_rich_envelope_share_exact_accounting() {
    let properties = BTreeMap::from([
        ("binary".to_owned(), MessageValue::Binary(vec![1, 2, 3])),
        (
            "map".to_owned(),
            MessageValue::Map(vec![(MessageValue::Null, MessageValue::Bool(true))]),
        ),
    ]);
    let patch_bytes: usize = properties
        .iter()
        .map(|(key, value)| key.len() + value.content_size())
        .sum();
    let patch = CommandKind::Settle {
        sequence: SequenceNumber::new(1),
        lock_token: LockToken::new(1),
        disposition: SettlementDisposition::DeadLetter {
            reason: "reason".into(),
            description: "detail".into(),
        },
        properties_to_modify: properties,
    };
    let mut usage = AtomicMessagingInputUsage::default();
    usage
        .try_extend(&patch)
        .expect("borrowed patch and details");
    assert_eq!(counts(usage), (1, 0, patch_bytes + 12, 4));
    let rich = MessageEnvelope {
        application_properties: BTreeMap::from([("p".into(), MessageValue::Bool(true))]),
        message_annotations: BTreeMap::from([(
            AnnotationKey::Symbol("a".into()),
            MessageValue::Null,
        )]),
        footer: BTreeMap::from([(AnnotationKey::Symbol("f".into()), MessageValue::Null)]),
        body: MessageBody::Sequence(vec![
            vec![MessageValue::List(vec![MessageValue::Null])],
            vec![],
        ]),
        ..MessageEnvelope::default()
    };
    let rich_bytes = rich.content_size();
    usage
        .try_extend(&envelope_send(rich))
        .expect("rich envelope");
    assert_eq!(counts(usage), (2, 1, patch_bytes + 12 + rich_bytes, 9));
    for disposition in [
        SettlementDisposition::Complete,
        SettlementDisposition::Abandon,
        SettlementDisposition::Defer,
    ] {
        usage
            .try_extend(&CommandKind::Settle {
                sequence: SequenceNumber::new(1),
                lock_token: LockToken::new(1),
                disposition,
                properties_to_modify: BTreeMap::new(),
            })
            .expect("content-free settlement");
    }
    assert_eq!(counts(usage), (5, 1, patch_bytes + 12 + rich_bytes, 9));
}

#[test]
fn settlement_byte_boundaries_refuse_atomically_without_send_slots() {
    let mut patch = CommandKind::Settle {
        sequence: SequenceNumber::new(1),
        lock_token: LockToken::new(1),
        disposition: SettlementDisposition::Abandon,
        properties_to_modify: BTreeMap::from([(
            "k".into(),
            MessageValue::Binary(vec![0; MAX_ATOMIC_MESSAGING_CONTENT_BYTES - 6]),
        )]),
    };
    let mut usage = AtomicMessagingInputUsage::default();
    usage
        .try_extend(&patch)
        .expect("key plus binary is exact byte limit");
    assert_eq!(counts(usage), (1, 0, MAX_ATOMIC_MESSAGING_CONTENT_BYTES, 1));
    if let CommandKind::Settle {
        properties_to_modify,
        ..
    } = &mut patch
    {
        properties_to_modify.insert(
            "k".into(),
            MessageValue::Binary(vec![0; MAX_ATOMIC_MESSAGING_CONTENT_BYTES - 5]),
        );
    }
    let mut usage = AtomicMessagingInputUsage::default();
    assert_eq!(
        usage.try_extend(&patch),
        Err(limit(
            AtomicMessagingLimit::ContentBytes,
            MAX_ATOMIC_MESSAGING_CONTENT_BYTES
        ))
    );
    assert_eq!(usage, AtomicMessagingInputUsage::default());
    usage
        .try_extend(&complete())
        .expect("healthy retry after patch refusal");
    assert_eq!(counts(usage), (1, 0, 0, 0));

    let mut details = CommandKind::DeadLetter {
        sequence: SequenceNumber::new(1),
        lock_token: LockToken::new(1),
        reason: "r".repeat(MAX_ATOMIC_MESSAGING_CONTENT_BYTES / 2),
        description: "d".repeat(MAX_ATOMIC_MESSAGING_CONTENT_BYTES / 2),
    };
    let mut usage = AtomicMessagingInputUsage::default();
    usage.try_extend(&details).expect("exact detail byte limit");
    assert_eq!(counts(usage), (1, 0, MAX_ATOMIC_MESSAGING_CONTENT_BYTES, 0));
    if let CommandKind::DeadLetter { description, .. } = &mut details {
        description.push('d');
    }
    let mut usage = AtomicMessagingInputUsage::default();
    assert_eq!(
        usage.try_extend(&details),
        Err(limit(
            AtomicMessagingLimit::ContentBytes,
            MAX_ATOMIC_MESSAGING_CONTENT_BYTES
        ))
    );
    assert_eq!(usage, AtomicMessagingInputUsage::default());
    usage
        .try_extend(&complete())
        .expect("healthy retry after details refusal");
}

#[test]
fn empty_section_floor_refuses_before_invalid_value_traversal() {
    for body in [
        MessageBody::Data(vec![vec![], vec![]]),
        MessageBody::Sequence(vec![vec![]]),
    ] {
        let mut usage = AtomicMessagingInputUsage::default();
        usage
            .try_extend(&send("", vec![0; MAX_ATOMIC_MESSAGING_CONTENT_BYTES - 15]))
            .expect("leave fifteen bytes");
        let before = usage;
        let envelope = MessageEnvelope {
            application_properties: BTreeMap::from([(
                "deep".into(),
                nested(MAX_MESSAGE_VALUE_DEPTH + 1),
            )]),
            body,
            ..MessageEnvelope::default()
        };
        assert_eq!(
            usage.try_extend(&envelope_send(envelope)),
            Err(limit(
                AtomicMessagingLimit::ContentBytes,
                MAX_ATOMIC_MESSAGING_CONTENT_BYTES
            ))
        );
        assert_eq!(usage, before);
        usage
            .try_extend(&complete())
            .expect("healthy extension after floor refusal");
    }
    for body in [
        MessageBody::Data(vec![vec![], vec![]]),
        MessageBody::Sequence(vec![vec![], vec![]]),
    ] {
        let envelope = MessageEnvelope {
            body,
            ..MessageEnvelope::default()
        };
        let bytes = envelope.content_size();
        let usage = fold(&[envelope_send(envelope)]).expect("empty sections within byte budget");
        assert_eq!(counts(usage), (1, 1, bytes, 0));
    }
}

#[test]
fn splitting_a_batch_preserves_payload_totals_but_changes_action_count() {
    let mut messages = vec![member("same"), member("same"), member("other")];
    messages[1].envelope.body = MessageBody::Value(MessageValue::List(vec![
        MessageValue::Null,
        MessageValue::Bool(true),
    ]));
    messages[2]
        .envelope
        .application_properties
        .insert("p".into(), MessageValue::String("value".into()));
    let split = messages
        .iter()
        .map(|message| CommandKind::SendEnvelope {
            message_id: message.message_id.clone(),
            body: message.body.clone(),
            time_to_live_millis: message.time_to_live_millis,
            session_id: message.session_id.clone(),
            envelope: Box::new(message.envelope.clone()),
        })
        .collect::<Vec<_>>();
    let batch = fold(&[CommandKind::SendBatch { messages }]).expect("one batch");
    let separate = fold(&split).expect("individual messages");
    assert_eq!(batch.actions(), 1);
    assert_eq!(separate.actions(), 3);
    assert_eq!(batch.messages(), separate.messages());
    assert_eq!(batch.content_bytes(), separate.content_bytes());
    assert_eq!(batch.value_items(), separate.value_items());
}

#[test]
fn whole_list_and_incremental_validation_share_the_same_kind_core() {
    let mut scheduled = member("late");
    scheduled.scheduled_enqueue_time = Some(Timestamp::from_millis(0));
    let lists = vec![
        vec![],
        vec![
            send("x", vec![1]),
            complete(),
            CommandKind::SendBatch { messages: vec![] },
        ],
        vec![CommandKind::SendBatch {
            messages: vec![member("same"); MAX_ATOMIC_MESSAGING_MESSAGES],
        }],
        vec![CommandKind::SendBatch {
            messages: vec![member("same"); MAX_ATOMIC_MESSAGING_MESSAGES + 1],
        }],
        vec![
            complete(),
            CommandKind::SendBatch {
                messages: vec![member("first"), scheduled],
            },
        ],
        vec![envelope_send(MessageEnvelope {
            body: MessageBody::Value(nested(MAX_MESSAGE_VALUE_DEPTH + 1)),
            ..MessageEnvelope::default()
        })],
        vec![send("", vec![0; MAX_ATOMIC_MESSAGING_CONTENT_BYTES + 1])],
        vec![
            complete(),
            CommandKind::RenewLock {
                sequence: SequenceNumber::new(1),
                lock_token: LockToken::new(1),
                lock_duration_millis: None,
            },
        ],
        vec![complete(); MAX_ATOMIC_MESSAGING_ACTIONS],
    ];
    for kinds in lists {
        assert_eq!(
            validate_atomic_messaging_kinds(&kinds),
            fold(&kinds).map(|_| ())
        );
    }
}

#[test]
fn whole_list_action_precheck_intentionally_precedes_an_early_kind_refusal() {
    let unsupported = CommandKind::RenewLock {
        sequence: SequenceNumber::new(1),
        lock_token: LockToken::new(1),
        lock_duration_millis: None,
    };
    let mut kinds = vec![complete(); MAX_ATOMIC_MESSAGING_ACTIONS + 1];
    kinds[0] = unsupported;
    assert_eq!(
        validate_atomic_messaging_kinds(&kinds),
        Err(limit(
            AtomicMessagingLimit::Actions,
            MAX_ATOMIC_MESSAGING_ACTIONS
        ))
    );
    assert_eq!(
        fold(&kinds),
        Err(BrokerError::AtomicMessagingOperationNotSupported)
    );
}
