use super::*;
use crate::{LockToken, SequenceNumber, Timestamp, validate_atomic_messaging_kinds};

fn populated_usage() -> AtomicMessagingInputUsage {
    AtomicMessagingInputUsage {
        actions: 2,
        messages: 1,
        content_bytes: 7,
        value_items: 3,
    }
}

fn assert_failed_extension(
    mut usage: AtomicMessagingInputUsage,
    kind: &CommandKind,
    expected: BrokerError,
) {
    let original = usage;
    assert_eq!(usage.try_extend(kind), Err(expected));
    assert_eq!(usage, original);
}

fn ingress() -> crate::IngressEnvelope {
    crate::IngressEnvelope {
        message_id: String::from("id"),
        body: vec![1],
        time_to_live_millis: None,
        session_id: None,
        envelope: MessageEnvelope::default(),
        scheduled_enqueue_time: None,
    }
}

fn send(envelope: MessageEnvelope) -> CommandKind {
    CommandKind::SendEnvelope {
        message_id: String::new(),
        body: Vec::new(),
        time_to_live_millis: None,
        session_id: None,
        envelope: Box::new(envelope),
    }
}

#[test]
fn shared_value_limit_includes_a_later_settlement_patch() -> Result<(), BrokerError> {
    let envelope = MessageEnvelope {
        body: MessageBody::Value(MessageValue::List(vec![
            MessageValue::Null;
            Limit::ValueItems.maximum() - 1
        ])),
        ..MessageEnvelope::default()
    };
    let first = send(envelope);
    validate_atomic_messaging_kinds(std::slice::from_ref(&first))?;
    let patch = CommandKind::Settle {
        sequence: SequenceNumber::new(1),
        lock_token: LockToken::new(1),
        disposition: SettlementDisposition::Complete,
        properties_to_modify: BTreeMap::from([(String::from("value"), MessageValue::Null)]),
    };
    assert_eq!(
        validate_atomic_messaging_kinds(&[first, patch]),
        Err(Limit::ValueItems.exceeded())
    );
    Ok(())
}

#[test]
fn empty_body_sections_are_bounded_before_iteration() {
    for sequence in [false, true] {
        let envelope = MessageEnvelope {
            body: if sequence {
                MessageBody::Sequence(vec![Vec::new(); Limit::ContentBytes.maximum() / 19 + 1])
            } else {
                MessageBody::Data(vec![Vec::new(); Limit::ContentBytes.maximum() / 15 + 1])
            },
            ..MessageEnvelope::default()
        };
        assert_eq!(
            validate_atomic_messaging_kinds(&[send(envelope)]),
            Err(Limit::ContentBytes.exceeded())
        );
    }
}

#[test]
fn depth_is_checked_before_descending_and_any_scheduled_timestamp_is_refused() {
    let mut value = MessageValue::Null;
    for _ in 0..=MAX_MESSAGE_VALUE_DEPTH {
        value = MessageValue::List(vec![value]);
    }
    assert!(matches!(
        validate_atomic_messaging_kinds(&[send(MessageEnvelope {
            body: MessageBody::Value(value),
            ..MessageEnvelope::default()
        })]),
        Err(BrokerError::InvalidMessageContent { .. })
    ));
    let batch = CommandKind::SendBatch {
        messages: vec![crate::IngressEnvelope {
            message_id: String::new(),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: None,
            envelope: MessageEnvelope::default(),
            scheduled_enqueue_time: Some(Timestamp::UNIX_EPOCH),
        }],
    };
    assert_eq!(
        validate_atomic_messaging_kinds(&[batch]),
        Err(BrokerError::AtomicMessagingOperationNotSupported)
    );
}

#[test]
fn section_overhead_preflight_tracks_the_content_tally() {
    for body in [
        MessageBody::Data(vec![Vec::new()]),
        MessageBody::Sequence(vec![Vec::new()]),
    ] {
        let expected = if matches!(body, MessageBody::Data(_)) {
            15
        } else {
            19
        };
        let envelope = MessageEnvelope {
            body,
            ..MessageEnvelope::default()
        };
        assert_eq!(
            envelope.content_size() - MessageEnvelope::default().content_size(),
            expected
        );
    }
}

#[test]
fn failed_extensions_preserve_all_usage_after_partial_charges() {
    let usage = populated_usage();
    assert_failed_extension(
        usage,
        &CommandKind::ExpireMessages,
        BrokerError::AtomicMessagingOperationNotSupported,
    );
    let mut late_scheduled = ingress();
    late_scheduled.scheduled_enqueue_time = Some(Timestamp::UNIX_EPOCH);
    assert_failed_extension(
        usage,
        &CommandKind::SendBatch {
            messages: vec![ingress(), late_scheduled],
        },
        BrokerError::AtomicMessagingOperationNotSupported,
    );
    let mut late_session = ingress();
    late_session.session_id = Some(crate::SessionId::new("session").expect("session"));
    assert_failed_extension(
        usage,
        &CommandKind::SendBatch {
            messages: vec![ingress(), late_session],
        },
        BrokerError::AtomicMessagingOperationNotSupported,
    );
    assert_failed_extension(
        AtomicMessagingInputUsage {
            content_bytes: Limit::ContentBytes.maximum() - 4,
            ..usage
        },
        &CommandKind::DeadLetter {
            sequence: SequenceNumber::new(1),
            lock_token: LockToken::new(1),
            reason: String::from("four"),
            description: String::from("overflow"),
        },
        Limit::ContentBytes.exceeded(),
    );
    assert_failed_extension(
        AtomicMessagingInputUsage {
            value_items: Limit::ValueItems.maximum() - 1,
            ..usage
        },
        &send(MessageEnvelope {
            body: MessageBody::Value(MessageValue::List(vec![MessageValue::Null])),
            ..MessageEnvelope::default()
        }),
        Limit::ValueItems.exceeded(),
    );
}

#[test]
fn failed_depth_and_empty_section_preflights_preserve_usage() {
    let mut value = MessageValue::Null;
    for _ in 0..=MAX_MESSAGE_VALUE_DEPTH {
        value = MessageValue::List(vec![value]);
    }
    let mut usage = populated_usage();
    let original = usage;
    assert!(matches!(
        usage.try_extend(&send(MessageEnvelope {
            body: MessageBody::Value(value),
            ..MessageEnvelope::default()
        })),
        Err(BrokerError::InvalidMessageContent { .. })
    ));
    assert_eq!(usage, original);
    for body in [
        MessageBody::Data(vec![Vec::new(); Limit::ContentBytes.maximum() / 15 + 1]),
        MessageBody::Sequence(vec![Vec::new(); Limit::ContentBytes.maximum() / 19 + 1]),
    ] {
        assert_failed_extension(
            usage,
            &send(MessageEnvelope {
                body,
                ..MessageEnvelope::default()
            }),
            Limit::ContentBytes.exceeded(),
        );
    }
}

#[test]
fn action_and_batch_message_caps_retain_their_rejection_priority() {
    assert_failed_extension(
        AtomicMessagingInputUsage {
            actions: Limit::Actions.maximum(),
            ..populated_usage()
        },
        &CommandKind::ExpireMessages,
        Limit::Actions.exceeded(),
    );
    let mut scheduled = ingress();
    scheduled.scheduled_enqueue_time = Some(Timestamp::UNIX_EPOCH);
    assert_failed_extension(
        AtomicMessagingInputUsage {
            messages: Limit::Messages.maximum(),
            ..populated_usage()
        },
        &CommandKind::SendBatch {
            messages: vec![scheduled],
        },
        Limit::Messages.exceeded(),
    );
    let kinds = vec![CommandKind::ExpireMessages; Limit::Actions.maximum() + 1];
    assert_eq!(
        validate_atomic_messaging_kinds(&kinds),
        Err(Limit::Actions.exceeded())
    );
    assert_eq!(
        validate_atomic_messaging_kinds(&kinds[..Limit::Actions.maximum()]),
        Err(BrokerError::AtomicMessagingOperationNotSupported)
    );
}

#[test]
fn empty_batch_and_settlements_consume_actions_without_logical_sends() {
    let mut usage = AtomicMessagingInputUsage::default();
    assert_eq!(usage.actions(), 0);
    assert_eq!(usage.messages(), 0);
    assert_eq!(usage.content_bytes(), 0);
    assert_eq!(usage.value_items(), 0);
    for kind in [
        CommandKind::SendBatch {
            messages: Vec::new(),
        },
        CommandKind::Complete {
            sequence: SequenceNumber::new(1),
            lock_token: LockToken::new(1),
        },
        CommandKind::Abandon {
            sequence: SequenceNumber::new(1),
            lock_token: LockToken::new(1),
        },
        CommandKind::Defer {
            sequence: SequenceNumber::new(1),
            lock_token: LockToken::new(1),
        },
    ] {
        usage.try_extend(&kind).expect("supported action");
    }
    assert_eq!(usage.actions(), 4);
    assert_eq!(usage.messages(), 0);
    assert_eq!(usage.content_bytes(), 0);
    assert_eq!(usage.value_items(), 0);
}
