use super::*;
use crate::{LockToken, SequenceNumber, Timestamp, validate_atomic_messaging_kinds};

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
