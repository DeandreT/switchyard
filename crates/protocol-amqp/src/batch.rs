//! A producer batch is a transport wrapper, not a message to retain.

use std::io;

use amqp::{Body, Message, MessageDecodeBudget, decode_message_with_budget};
use domain::{
    BrokerError, CommandKind, IngressBatchLimit, IngressEnvelope, MAX_INGRESS_BATCH_MESSAGES,
    ScheduledEnvelope,
};

use crate::{ProtocolError, parse_session_id, read_incoming, validate_standard_message_size};

pub const SERVICE_BUS_BATCH_MESSAGE_FORMAT: u32 = 0x8001_3700;

#[derive(Debug, thiserror::Error)]
pub(crate) enum BatchDecodeError {
    #[error("a batch must contain one or more AMQP Data sections")]
    InvalidBody,
    #[error("producer message format {0:#010x} is not supported")]
    UnsupportedFormat(u32),
    #[error("invalid embedded batch message: {0}")]
    Malformed(#[from] io::Error),
    #[error(transparent)]
    Message(#[from] ProtocolError),
    #[error(transparent)]
    Refused(#[from] BrokerError),
}

impl BatchDecodeError {
    pub fn condition(&self) -> &'static str {
        match self {
            Self::UnsupportedFormat(_) => "amqp:not-implemented",
            Self::Refused(error) => crate::condition_for(error),
            Self::Message(ProtocolError::MessageTooLarge { .. }) => crate::MESSAGE_SIZE_EXCEEDED,
            _ => crate::INVALID_FIELD,
        }
    }
}

pub(crate) fn read_ingress(
    message: &Message,
    format: u32,
) -> Result<CommandKind, BatchDecodeError> {
    match format {
        SERVICE_BUS_BATCH_MESSAGE_FORMAT => Ok(CommandKind::SendBatch {
            messages: read_batch(message)?,
        }),
        0 => {
            let incoming = read_incoming(message)?;
            Ok(match incoming.scheduled_enqueue_time {
                Some(enqueue_at) => CommandKind::ScheduleEnvelopes {
                    messages: vec![ScheduledEnvelope {
                        message_id: incoming.message_id,
                        body: incoming.body,
                        time_to_live_millis: incoming.time_to_live_millis,
                        session_id: incoming.session_id,
                        enqueue_at,
                        envelope: incoming.envelope,
                    }],
                },
                None => CommandKind::SendEnvelope {
                    message_id: incoming.message_id,
                    body: incoming.body,
                    time_to_live_millis: incoming.time_to_live_millis,
                    session_id: incoming.session_id,
                    envelope: incoming.envelope.into(),
                },
            })
        }
        other => Err(BatchDecodeError::UnsupportedFormat(other)),
    }
}

pub(crate) fn read_batch(message: &Message) -> Result<Vec<IngressEnvelope>, BatchDecodeError> {
    let Body::Data(sections) = &message.body else {
        return Err(BatchDecodeError::InvalidBody);
    };
    if sections.is_empty() {
        return Err(BatchDecodeError::InvalidBody);
    }
    if sections.len() > MAX_INGRESS_BATCH_MESSAGES {
        return Err(BrokerError::IngressBatchLimitExceeded {
            limit: IngressBatchLimit::Messages,
            actual: sections.len(),
            maximum: MAX_INGRESS_BATCH_MESSAGES,
        }
        .into());
    }
    let outer_session = message
        .properties
        .as_ref()
        .and_then(|properties| properties.group_id.as_deref())
        .map(parse_session_id)
        .transpose()?;
    let mut messages = Vec::with_capacity(sections.len());
    let mut budget = MessageDecodeBudget::default();
    for encoded in sections {
        validate_standard_message_size(encoded.len())?;
        let decoded = decode_message_with_budget(encoded, &mut budget)?;
        let incoming = read_incoming(&decoded)?;
        if let (Some(outer), Some(inner)) = (&outer_session, &incoming.session_id)
            && outer != inner
        {
            return Err(BrokerError::BatchSessionMismatch.into());
        }
        messages.push(IngressEnvelope {
            message_id: incoming.message_id,
            body: incoming.body,
            time_to_live_millis: incoming.time_to_live_millis,
            session_id: incoming.session_id,
            envelope: incoming.envelope,
            scheduled_enqueue_time: incoming.scheduled_enqueue_time,
        });
    }
    Ok(messages)
}

#[cfg(test)]
mod tests {
    use amqp::{Annotations, Header, MessageId, Properties, Symbol, Value, encode_message};
    use domain::{MessageBody, SessionId, Timestamp};
    use serde_amqp::primitives::Timestamp as AmqpTimestamp;

    use super::*;

    fn inner(id: &str, session: Option<&str>) -> Message {
        Message {
            properties: Some(Properties {
                message_id: Some(MessageId::String(id.to_owned())),
                group_id: session.map(str::to_owned),
                ..Properties::default()
            }),
            ..Message::data(id.as_bytes().to_vec())
        }
    }

    fn outer(messages: &[Message]) -> Message {
        Message {
            body: Body::Data(
                messages
                    .iter()
                    .map(|message| {
                        encode_message(message)
                            .expect("standard inner message")
                            .into()
                    })
                    .collect(),
            ),
            ..Message::default()
        }
    }

    fn encoded_outer(encoded: Vec<Vec<u8>>) -> Message {
        Message {
            body: Body::Data(encoded.into_iter().map(Into::into).collect()),
            ..Message::default()
        }
    }

    #[test]
    fn single_and_multiple_members_keep_their_independent_content() {
        for count in [1, 3] {
            let messages = (0..count)
                .map(|index| {
                    let mut message = inner(&format!("member-{index}"), None);
                    message.header = Some(Header {
                        priority: index as u8,
                        ttl: Some(2_000 + index),
                        ..Header::default()
                    });
                    message.properties.as_mut().expect("properties").subject =
                        Some(format!("subject-{index}"));
                    message.application_properties = Some(amqp::ApplicationProperties(
                        [(format!("property-{index}"), Value::Uint(index))]
                            .into_iter()
                            .collect(),
                    ));
                    message
                })
                .collect::<Vec<_>>();
            let batch = read_batch(&outer(&messages)).expect("valid batch");
            for (index, member) in batch.into_iter().enumerate() {
                let expected = read_incoming(&messages[index]).expect("valid inner message");
                assert_eq!(member.message_id, expected.message_id);
                assert_eq!(member.body, expected.body);
                assert_eq!(member.time_to_live_millis, expected.time_to_live_millis);
                assert_eq!(member.envelope, expected.envelope);
                assert_eq!(member.scheduled_enqueue_time, None);
            }
        }
    }

    #[test]
    fn outer_metadata_does_not_fill_anonymous_or_missing_inner_content() {
        let mut wrapper = outer(&[Message::default(), inner("own-id", None)]);
        wrapper.header = Some(Header {
            durable: true,
            ttl: Some(999),
            ..Header::default()
        });
        wrapper.properties = Some(Properties {
            message_id: Some(MessageId::String("transport-id".to_owned())),
            correlation_id: Some(MessageId::String("transport-correlation".to_owned())),
            subject: Some("outer-subject".to_owned()),
            ..Properties::default()
        });
        wrapper.application_properties = Some(amqp::ApplicationProperties(
            [("transport-only".to_owned(), Value::Bool(true))]
                .into_iter()
                .collect(),
        ));
        let mut annotations = Annotations::new();
        annotations.insert(
            Symbol::from(crate::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(AmqpTimestamp::from_milliseconds(9_000)),
        );
        wrapper.message_annotations = Some(annotations);
        let batch = read_batch(&wrapper).expect("metadata is not inherited");
        assert!(batch[0].message_id.is_empty());
        assert_eq!(batch[0].envelope.properties.message_id, None);
        assert_eq!(batch[0].envelope.body, MessageBody::Empty);
        assert_eq!(batch[1].message_id, "own-id");
        for member in batch {
            assert_eq!(member.time_to_live_millis, None);
            assert_eq!(member.scheduled_enqueue_time, None);
            assert_eq!(member.envelope.header, None);
            assert_eq!(member.envelope.properties.correlation_id, None);
            assert_eq!(member.envelope.properties.subject, None);
            assert!(member.envelope.application_properties.is_empty());
            assert!(member.envelope.message_annotations.is_empty());
        }
    }

    #[test]
    fn only_nonempty_data_wrappers_are_batches() {
        for body in [
            Body::Empty,
            Body::Data(Vec::new()),
            Body::Value(Value::Null),
            Body::Sequence(Vec::new()),
        ] {
            let error = read_batch(&Message {
                body,
                ..Message::default()
            })
            .expect_err("wrong wrapper");
            assert!(matches!(error, BatchDecodeError::InvalidBody));
            assert_eq!(error.condition(), crate::INVALID_FIELD);
        }
        let batch =
            read_batch(&encoded_outer(vec![Vec::new()])).expect("empty standard inner message");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].envelope.body, MessageBody::Empty);
    }

    #[test]
    fn entry_count_is_checked_before_decoding_or_member_allocation() {
        let error = read_batch(&encoded_outer(vec![
            vec![0];
            MAX_INGRESS_BATCH_MESSAGES + 1
        ]))
        .expect_err("entry quota precedes malformed inner content");
        assert!(
            matches!(error, BatchDecodeError::Refused(BrokerError::IngressBatchLimitExceeded {
            limit: IngressBatchLimit::Messages,
            actual,
            maximum: MAX_INGRESS_BATCH_MESSAGES,
        }) if actual == MAX_INGRESS_BATCH_MESSAGES + 1)
        );
        assert_eq!(error.condition(), crate::RESOURCE_LIMIT_EXCEEDED);
        assert_eq!(
            read_batch(&encoded_outer(vec![Vec::new(); MAX_INGRESS_BATCH_MESSAGES]))
                .expect("exact count quota")
                .len(),
            MAX_INGRESS_BATCH_MESSAGES,
        );
    }

    #[test]
    fn a_later_malformed_or_oversized_message_cannot_return_a_partial_batch() {
        let valid = encode_message(&inner("valid", None)).expect("valid member");
        for invalid in [vec![0], [valid.as_slice(), &[0]].concat()] {
            let error = read_batch(&encoded_outer(vec![valid.clone(), invalid]))
                .expect_err("the complete batch must decode");
            assert!(matches!(error, BatchDecodeError::Malformed(_)));
            assert_eq!(error.condition(), crate::INVALID_FIELD);
        }
        let error = read_batch(&encoded_outer(vec![
            valid,
            vec![0; crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES + 1],
        ]))
        .expect_err("inner wire size quota");
        assert!(matches!(
            error,
            BatchDecodeError::Message(ProtocolError::MessageTooLarge { .. })
        ));
        assert_eq!(error.condition(), crate::MESSAGE_SIZE_EXCEEDED);
    }

    #[test]
    fn inner_data_content_is_not_recursively_unwrapped() {
        let nested = encode_message(&inner("not-a-member", None)).expect("nested payload");
        let message = Message::data(nested.clone());
        let batch = read_batch(&outer(&[message])).expect("one standard data message");
        assert_eq!(batch.len(), 1);
        assert!(batch[0].message_id.is_empty());
        assert_eq!(batch[0].body, nested);
        assert_eq!(
            batch[0].envelope.body,
            MessageBody::Data(vec![batch[0].body.clone()])
        );
    }

    #[test]
    fn optional_outer_session_must_agree_but_never_repairs_a_missing_member() {
        let mut wrapper = outer(&[inner("first", Some("cart")), inner("second", None)]);
        wrapper.properties = Some(Properties {
            group_id: Some("cart".to_owned()),
            ..Properties::default()
        });
        let batch =
            read_batch(&wrapper).expect("queue policy will reject the absent inner session");
        assert_eq!(
            batch[0].session_id,
            Some(SessionId::new("cart").expect("session"))
        );
        assert_eq!(batch[1].session_id, None);
        wrapper
            .properties
            .as_mut()
            .expect("outer properties")
            .group_id = Some("other".to_owned());
        let error = read_batch(&wrapper).expect_err("outer and named inner sessions disagree");
        assert!(matches!(
            error,
            BatchDecodeError::Refused(BrokerError::BatchSessionMismatch)
        ));
        assert_eq!(error.condition(), crate::INVALID_FIELD);
        wrapper
            .properties
            .as_mut()
            .expect("outer properties")
            .group_id = Some(String::new());
        assert!(matches!(
            read_batch(&wrapper),
            Err(BatchDecodeError::Message(
                ProtocolError::InvalidSessionId { .. }
            ))
        ));
    }

    #[test]
    fn every_members_optional_schedule_is_preserved_without_inheritance() {
        let mut future = inner("future", None);
        let mut annotations = Annotations::new();
        annotations.insert(
            Symbol::from(crate::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(AmqpTimestamp::from_milliseconds(3_000)),
        );
        future.message_annotations = Some(annotations.clone());
        annotations.insert(
            Symbol::from(crate::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(AmqpTimestamp::from_milliseconds(1)),
        );
        let mut past = inner("past", None);
        past.message_annotations = Some(annotations);
        let batch =
            read_batch(&outer(&[inner("ordinary", None), future, past])).expect("mixed ingress");
        assert_eq!(batch[0].scheduled_enqueue_time, None);
        assert_eq!(
            batch[1].scheduled_enqueue_time,
            Some(Timestamp::from_millis(3_000))
        );
        assert_eq!(
            batch[2].scheduled_enqueue_time,
            Some(Timestamp::from_millis(1))
        );
    }

    #[test]
    fn transfer_format_not_body_shape_selects_atomic_batch_ingress() {
        let wrapper = outer(&[inner("first", None), inner("second", None)]);
        let CommandKind::SendEnvelope { body, .. } =
            read_ingress(&wrapper, 0).expect("standard data body")
        else {
            panic!("standard Data sections must not be unwrapped");
        };
        let Body::Data(sections) = &wrapper.body else {
            panic!("wrapper")
        };
        assert_eq!(
            body,
            sections
                .iter()
                .flat_map(|section| section.iter().copied())
                .collect::<Vec<_>>()
        );
        let CommandKind::SendBatch { messages } =
            read_ingress(&wrapper, SERVICE_BUS_BATCH_MESSAGE_FORMAT)
                .expect("registered batch format")
        else {
            panic!("one atomic command");
        };
        assert_eq!(messages.len(), 2);
        let error = read_ingress(&wrapper, 0x8001_3701)
            .expect_err("a different format version is not standard");
        assert_eq!(error.condition(), "amqp:not-implemented");
    }

    fn compact_array(count: u32, named_descriptor: Option<&str>) -> Vec<u8> {
        let mut constructor = Vec::new();
        if let Some(name) = named_descriptor {
            constructor.extend_from_slice(&[0x00, 0xb3]);
            constructor
                .extend_from_slice(&u32::try_from(name.len()).expect("fixture").to_be_bytes());
            constructor.extend_from_slice(name.as_bytes());
        }
        constructor.push(0x40);
        let mut encoded = vec![0x00, 0x53, 0x77, 0xf0];
        encoded.extend_from_slice(
            &(4 + u32::try_from(constructor.len()).expect("fixture")).to_be_bytes(),
        );
        encoded.extend_from_slice(&count.to_be_bytes());
        encoded.extend_from_slice(&constructor);
        encoded
    }

    #[test]
    fn independently_valid_compact_members_share_the_value_count_allowance() {
        let encoded = compact_array(40_000, None);
        amqp::decode_message(&encoded).expect("independently fitting member");
        let error = read_batch(&encoded_outer(vec![encoded; 4])).expect_err("shared value quota");
        assert!(matches!(error, BatchDecodeError::Malformed(_)));
        assert!(error.to_string().contains("element limit"));
    }

    #[test]
    fn shared_descriptor_expansion_is_charged_across_members() {
        let encoded = compact_array(80, Some(&"x".repeat(2_048)));
        amqp::decode_message(&encoded).expect("independently fitting descriptor clones");
        let wrapper = encoded_outer(vec![encoded; 26]);
        assert!(
            encode_message(&wrapper).expect("outer wire encoding").len()
                < crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES
        );
        let error = read_batch(&wrapper).expect_err("shared copied-byte quota");
        assert!(matches!(error, BatchDecodeError::Malformed(_)));
        assert!(error.to_string().contains("expanded value byte limit"));
    }
}
