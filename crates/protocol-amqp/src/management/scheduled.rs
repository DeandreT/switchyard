//! Service Bus request/response scheduling and cancellation operations.

use amqp::{Body, Message, MessageId, decode_message};
use domain::{
    CommandKind, CommandOutcome, EntityPath, MessageContent, NamespaceName, SequenceNumber,
};
use serde_amqp::{Value, primitives::Array};

use crate::{Broker, validate_standard_message_size};

use super::{ManagementResponse, map_body, map_value};

pub const SCHEDULE_MESSAGE_OPERATION: &str = "com.microsoft:schedule-message";
pub const CANCEL_SCHEDULED_MESSAGE_OPERATION: &str = "com.microsoft:cancel-scheduled-message";

const MESSAGES: &str = "messages";
const MESSAGE: &str = "message";
const MESSAGE_ID: &str = "message-id";
const SESSION_ID: &str = "session-id";
const PARTITION_KEY: &str = "partition-key";
const VIA_PARTITION_KEY: &str = "via-partition-key";
const SEQUENCE_NUMBERS: &str = "sequence-numbers";

pub(super) async fn schedule<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
) -> ManagementResponse {
    let messages = match scheduled_messages(&message.body) {
        Ok(messages) => messages,
        Err(description) => {
            return ManagementResponse::bad_request(message_id, tracking_id, description);
        }
    };
    let expected = messages.len();
    // Parsing and the operation's Send authorization precede capture. The
    // request-link adapter retains this binding; it never recaptures on drift.
    let binding = match broker.bind_entity(namespace.clone(), entity.clone()).await {
        Ok(binding) => binding,
        Err(rejection) => {
            return ManagementResponse::from_rejection(message_id, tracking_id, &rejection);
        }
    };
    let sequences = match broker
        .submit_bound(binding, CommandKind::SendContentBatch { messages })
        .await
    {
        Ok(CommandOutcome::BatchSent { sequences, stored })
            if sequences.len() == expected
                && u32::try_from(expected).is_ok_and(|expected| stored <= expected) =>
        {
            sequences
        }
        Ok(CommandOutcome::Published {
            sequences,
            subscriptions,
        }) if sequences.len() == expected && subscriptions.is_empty() => sequences,
        Ok(other) => {
            return ManagementResponse::internal(
                message_id,
                tracking_id,
                format!("scheduling messages produced an unexpected outcome: {other:?}"),
            );
        }
        Err(rejection) => {
            return ManagementResponse::from_rejection(message_id, tracking_id, &rejection);
        }
    };
    match sequence_values(&sequences) {
        Ok(values) => ManagementResponse::accepted(
            message_id,
            tracking_id,
            map_body(SEQUENCE_NUMBERS, Value::Array(Array::from(values))),
        ),
        Err(description) => ManagementResponse::internal(message_id, tracking_id, description),
    }
}

pub(super) async fn cancel<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
) -> ManagementResponse {
    let sequences = match sequence_numbers(&message.body) {
        Ok(sequences) => sequences,
        Err(description) => {
            return ManagementResponse::bad_request(message_id, tracking_id, description);
        }
    };
    let expected = u32::try_from(sequences.len()).unwrap_or(u32::MAX);
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::CancelScheduled { sequences },
        )
        .await
    {
        Ok(CommandOutcome::ScheduledCancelled { cancelled }) if cancelled == expected => {
            ManagementResponse::accepted(message_id, tracking_id, Value::Null)
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("cancelling scheduled messages produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

fn scheduled_messages(body: &Body) -> Result<Vec<MessageContent>, String> {
    let Some(Value::List(entries)) = map_value(body, MESSAGES) else {
        return Err(String::from(
            "messages must be a nonempty AMQP value list of maps",
        ));
    };
    if entries.is_empty() {
        return Err(String::from(
            "messages must be a nonempty AMQP value list of maps",
        ));
    }

    let contents = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| scheduled_message(entry, index))
        .collect::<Result<Vec<_>, _>>()?;
    // Apply derives the batch against one shared projection budget, so check
    // it the same way before submission.
    MessageContent::derive_batch(&contents)
        .map_err(|error| format!("messages are invalid together: {error}"))?;
    Ok(contents)
}

fn scheduled_message(entry: &Value, index: usize) -> Result<MessageContent, String> {
    let Value::Map(entry) = entry else {
        return Err(format!("messages[{index}] must be an AMQP map"));
    };
    let encoded = match entry_value(entry, MESSAGE) {
        Some(Value::Binary(encoded)) => encoded.as_slice(),
        _ => return Err(format!("messages[{index}].message must be AMQP binary")),
    };
    validate_standard_message_size(encoded.len()).map_err(|error| error.to_string())?;
    // The message arrives in the client's management request: received checks.
    decode_message(encoded).map_err(|error| {
        format!("messages[{index}].message is not a complete AMQP message: {error}")
    })?;
    // The cross-checks use the derivation the state machine applies, so the
    // request is checked against exactly the fields the stored record gets.
    let content = MessageContent::AmqpTextIds(encoded.to_vec());
    let incoming = content
        .derive()
        .map_err(|error| format!("messages[{index}].message is invalid: {error}"))?;
    if incoming.scheduled_enqueue_at.is_none() {
        return Err(format!(
            "messages[{index}].message requires x-opt-scheduled-enqueue-time"
        ));
    }

    let message_id =
        string_entry(entry, MESSAGE_ID, index, true)?.expect("a required string entry is present");
    if message_id
        != incoming
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.filter_properties().message_id.as_deref())
            .unwrap_or_default()
    {
        return Err(format!(
            "messages[{index}].message-id must match the encoded AMQP message-id"
        ));
    }
    let session_id = string_entry(entry, SESSION_ID, index, false)?;
    if session_id.as_deref() != incoming.session_id.as_ref().map(domain::SessionId::as_str) {
        return Err(format!(
            "messages[{index}].session-id must match the encoded AMQP group-id"
        ));
    }
    // These routing fields are meaningful only on partitioned entities, which
    // are outside Switchyard's current scope, but their wire types are still
    // validated so malformed requests are never silently reinterpreted.
    let _ = string_entry(entry, PARTITION_KEY, index, false)?;
    let _ = string_entry(entry, VIA_PARTITION_KEY, index, false)?;

    Ok(content)
}

fn string_entry(
    entry: &serde_amqp::primitives::OrderedMap<Value, Value>,
    name: &str,
    index: usize,
    required: bool,
) -> Result<Option<String>, String> {
    match entry_value(entry, name) {
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(format!("messages[{index}].{name} must be an AMQP string")),
        None if required => Err(format!("messages[{index}].{name} is required")),
        None => Ok(None),
    }
}

fn entry_value<'a>(
    entry: &'a serde_amqp::primitives::OrderedMap<Value, Value>,
    name: &str,
) -> Option<&'a Value> {
    entry.iter().find_map(|(key, value)| match key {
        Value::String(key) if key == name => Some(value),
        Value::Symbol(key) if key.as_str() == name => Some(value),
        _ => None,
    })
}

fn sequence_numbers(body: &Body) -> Result<Vec<SequenceNumber>, &'static str> {
    let Some(Value::Array(values)) = map_value(body, SEQUENCE_NUMBERS) else {
        return Err("sequence-numbers must be a nonempty AMQP value array of positive longs");
    };
    if values.is_empty() {
        return Err("sequence-numbers must be a nonempty AMQP value array of positive longs");
    }
    values
        .iter()
        .map(|value| match value {
            Value::Long(value) if *value > 0 => u64::try_from(*value)
                .map(SequenceNumber::new)
                .map_err(|_| "sequence-numbers must contain positive longs"),
            _ => Err("sequence-numbers must contain positive longs"),
        })
        .collect()
}

fn sequence_values(sequences: &[SequenceNumber]) -> Result<Vec<Value>, &'static str> {
    sequences
        .iter()
        .map(|sequence| {
            i64::try_from(sequence.as_u64())
                .map(Value::Long)
                .map_err(|_| "a scheduled sequence number exceeds the AMQP long range")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use amqp::{AnnotationKey, MessageAnnotations, Properties, encode_message};
    use domain::{CommandOutcome, Timestamp};
    use serde_amqp::primitives::{Binary, OrderedMap, Timestamp as AmqpTimestamp};

    use super::*;

    #[derive(Clone)]
    struct RecordingBroker {
        commands: Arc<Mutex<Vec<CommandKind>>>,
        outcome: CommandOutcome,
    }

    impl RecordingBroker {
        fn returning(outcome: CommandOutcome) -> Self {
            Self {
                commands: Arc::new(Mutex::new(Vec::new())),
                outcome,
            }
        }

        fn commands(&self) -> Vec<CommandKind> {
            self.commands.lock().expect("command recorder").clone()
        }
    }

    impl Broker for RecordingBroker {
        async fn bind_entity(
            &self,
            namespace: NamespaceName,
            entity: EntityPath,
        ) -> Result<domain::EntityBinding, crate::BrokerRejection> {
            Ok(crate::broker::test_binding(namespace, entity))
        }

        async fn submit_bound(
            &self,
            _binding: domain::EntityBinding,
            kind: CommandKind,
        ) -> Result<CommandOutcome, crate::BrokerRejection> {
            self.commands.lock().expect("command recorder").push(kind);
            Ok(self.outcome.clone())
        }

        fn submit(
            &self,
            _namespace: NamespaceName,
            _entity: EntityPath,
            kind: CommandKind,
        ) -> impl std::future::Future<Output = Result<CommandOutcome, crate::BrokerRejection>> + Send
        {
            let commands = Arc::clone(&self.commands);
            let outcome = self.outcome.clone();
            async move {
                commands.lock().expect("command recorder").push(kind);
                Ok(outcome)
            }
        }

        async fn deliverable(&self, _namespace: &NamespaceName, _entity: &EntityPath) {
            std::future::pending().await
        }
    }

    fn names() -> (NamespaceName, EntityPath) {
        (
            NamespaceName::new("tenant").expect("namespace"),
            EntityPath::new("orders").expect("entity"),
        )
    }

    fn encoded(message_id: &str, scheduled_at: i64) -> Vec<u8> {
        let mut message = Message::data(message_id.as_bytes().to_vec());
        message.properties = Some(Properties {
            message_id: Some(message_id.to_owned().into()),
            ..Properties::default()
        });
        let mut annotations = MessageAnnotations::default();
        annotations.insert(
            AnnotationKey::from("x-opt-scheduled-enqueue-time"),
            Value::Timestamp(AmqpTimestamp::from_milliseconds(scheduled_at)),
        );
        message.message_annotations = Some(annotations);
        encode_message(&message).expect("message encodes")
    }

    fn schedule_request(entries: Vec<Value>) -> Message {
        Message {
            body: Body::Value(map_body(MESSAGES, Value::List(entries))),
            ..Message::default()
        }
    }

    fn entry(message_id: &str, encoded: Vec<u8>) -> Value {
        let mut entry = OrderedMap::new();
        entry.insert(
            Value::String(MESSAGE.into()),
            Value::Binary(Binary::from(encoded)),
        );
        entry.insert(
            Value::String(MESSAGE_ID.into()),
            Value::String(message_id.into()),
        );
        Value::Map(entry)
    }

    #[test]
    fn schedule_cross_checks_filter_text_not_the_duplicate_identity() {
        for (id, expected) in [
            (Some(MessageId::String("literal".into())), "literal"),
            (Some(MessageId::Ulong(u64::MAX)), "18446744073709551615"),
            (
                Some(MessageId::Uuid(serde_amqp::primitives::Uuid::from(
                    [0xab; 16],
                ))),
                "abababab-abab-abab-abab-abababababab",
            ),
            (
                Some(MessageId::Binary(vec![0, 0xab, 0xff].into())),
                "00ABFF",
            ),
            (None, ""),
        ] {
            let mut message = decode_message(&encoded("source", 12_000)).unwrap();
            message.properties.as_mut().unwrap().message_id = id;
            let bytes = encode_message(&message).unwrap();
            let content = scheduled_message(&entry(expected, bytes.clone()), 0).unwrap();
            assert_eq!(content, MessageContent::AmqpTextIds(bytes));
            assert!(
                scheduled_message(&entry("wrong", encode_message(&message).unwrap()), 0).is_err()
            );
        }
    }

    #[tokio::test]
    async fn schedule_decodes_complete_messages_and_returns_long_sequences() {
        let broker = RecordingBroker::returning(CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(7), SequenceNumber::new(8)],
            stored: 1,
        });
        let (namespace, entity) = names();
        let response = schedule(
            &schedule_request(vec![
                entry("one", encoded("one", 12_000)),
                entry("two", encoded("two", 13_000)),
            ]),
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
        )
        .await
        .into_message();

        assert_eq!(
            response
                .application_properties
                .as_ref()
                .and_then(|p| p.get("statusCode")),
            Some(&Value::Int(200))
        );
        assert_eq!(
            map_value(&response.body, SEQUENCE_NUMBERS),
            Some(&Value::Array(Array::from(vec![
                Value::Long(7),
                Value::Long(8)
            ])))
        );
        let commands = broker.commands();
        let [CommandKind::SendContentBatch { messages }] = commands.as_slice() else {
            panic!("expected one atomic scheduled batch")
        };
        assert_eq!(
            messages,
            &[
                MessageContent::AmqpTextIds(encoded("one", 12_000)),
                MessageContent::AmqpTextIds(encoded("two", 13_000))
            ]
        );
        let derived = MessageContent::derive_batch(messages).expect("the batch derives");
        assert_eq!(
            derived[0].scheduled_enqueue_at,
            Some(Timestamp::from_millis(12_000))
        );
        assert_eq!(derived[1].message_id, "two");
    }

    fn status(response: &Message) -> Option<&Value> {
        response
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get("statusCode"))
    }

    /// A scheduled message whose application property is the array
    /// `["é", "x"]`, correctly encoded with byte lengths.
    fn non_ascii_array(message_id: &str) -> Vec<u8> {
        let mut message = Message {
            body: Body::Empty,
            ..Message::default()
        };
        message.properties = Some(Properties {
            message_id: Some(message_id.to_owned().into()),
            ..Properties::default()
        });
        let mut annotations = MessageAnnotations::default();
        annotations.insert(
            AnnotationKey::from("x-opt-scheduled-enqueue-time"),
            Value::Timestamp(AmqpTimestamp::from_milliseconds(12_000)),
        );
        message.message_annotations = Some(annotations);
        let mut bytes = encode_message(&message).expect("message encodes");
        bytes.extend([
            0x00, 0x53, 0x74, // application-properties
            0xc1, 0x0d, 0x02, // map8, size 13, count 2
            0xa1, 0x01, b'k', // key "k"
            0xe0, 0x07, 0x02, 0xa1, 0x02, 0xc3, 0xa9, 0x01, 0x78, // ["é","x"]
            0x00, 0x53, 0x75, 0xa0, 0x01, b'b', // data "b"
        ]);
        bytes
    }

    #[tokio::test]
    async fn a_non_ascii_string_array_refuses_the_whole_schedule_before_submission() {
        let encoded_array = non_ascii_array("two");
        // The client's bytes are valid AMQP; the refusal is the derivation's.
        decode_message(&encoded_array).expect("the received checks accept it");
        let broker = RecordingBroker::returning(CommandOutcome::BatchSent {
            sequences: vec![],
            stored: 0,
        });
        let (namespace, entity) = names();
        let response = schedule(
            &schedule_request(vec![
                entry("one", encoded("one", 12_000)),
                entry("two", encoded_array),
            ]),
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
        )
        .await
        .into_message();

        assert_eq!(status(&response), Some(&Value::Int(400)));
        assert!(broker.commands().is_empty());
    }

    #[tokio::test]
    async fn the_request_identifiers_must_match_the_derived_fields() {
        let mut grouped = Message::data(b"grouped".to_vec());
        grouped.properties = Some(Properties {
            message_id: Some(MessageId::Ulong(42)),
            group_id: Some("cart".to_owned()),
            ..Properties::default()
        });
        let mut annotations = MessageAnnotations::default();
        annotations.insert(
            AnnotationKey::from("x-opt-scheduled-enqueue-time"),
            Value::Timestamp(AmqpTimestamp::from_milliseconds(12_000)),
        );
        grouped.message_annotations = Some(annotations);
        let grouped = encode_message(&grouped).expect("message encodes");
        let with_session = |message_id: &str, session: Option<&str>| {
            let Value::Map(mut map) = entry(message_id, grouped.clone()) else {
                unreachable!("entries are maps")
            };
            if let Some(session) = session {
                map.insert(
                    Value::String(SESSION_ID.into()),
                    Value::String(session.into()),
                );
            }
            Value::Map(map)
        };
        let (namespace, entity) = names();
        for (request, accepted) in [
            (with_session("42", Some("cart")), true),
            (with_session("43", Some("cart")), false),
            (with_session("42", Some("other")), false),
            (with_session("42", None), false),
        ] {
            let broker = RecordingBroker::returning(CommandOutcome::BatchSent {
                sequences: vec![SequenceNumber::new(1)],
                stored: 1,
            });
            let response = schedule(
                &schedule_request(vec![request]),
                MessageId::Ulong(1),
                None,
                &namespace,
                &entity,
                &broker,
            )
            .await
            .into_message();
            let expected = if accepted { 200 } else { 400 };
            assert_eq!(status(&response), Some(&Value::Int(expected)));
            assert_eq!(broker.commands().len(), usize::from(accepted));
        }
    }

    #[tokio::test]
    async fn a_suppressed_schedule_still_returns_its_sequence_slot() {
        let broker = RecordingBroker::returning(CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(9)],
            stored: 0,
        });
        let (namespace, entity) = names();
        let response = schedule(
            &schedule_request(vec![entry("duplicate", encoded("duplicate", 12_000))]),
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
        )
        .await
        .into_message();

        assert_eq!(
            response
                .application_properties
                .as_ref()
                .and_then(|properties| properties.get("statusCode")),
            Some(&Value::Int(200))
        );
        assert_eq!(
            map_value(&response.body, SEQUENCE_NUMBERS),
            Some(&Value::Array(Array::from(vec![Value::Long(9)])))
        );
    }

    #[tokio::test]
    async fn a_scheduled_topic_publish_returns_its_placeholder_sequences() {
        let broker = RecordingBroker::returning(CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(11), SequenceNumber::new(12)],
            subscriptions: Vec::new(),
        });
        let (namespace, entity) = names();
        let response = schedule(
            &schedule_request(vec![
                entry("one", encoded("one", 12_000)),
                entry("two", encoded("two", 13_000)),
            ]),
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
        )
        .await
        .into_message();

        assert_eq!(
            response
                .application_properties
                .as_ref()
                .and_then(|properties| properties.get("statusCode")),
            Some(&Value::Int(200))
        );
        assert_eq!(
            map_value(&response.body, SEQUENCE_NUMBERS),
            Some(&Value::Array(Array::from(vec![
                Value::Long(11),
                Value::Long(12)
            ])))
        );
    }

    #[tokio::test]
    async fn one_malformed_entry_rejects_the_whole_schedule_before_submission() {
        let broker = RecordingBroker::returning(CommandOutcome::BatchSent {
            sequences: vec![],
            stored: 0,
        });
        let (namespace, entity) = names();
        let response = schedule(
            &schedule_request(vec![
                entry("one", encoded("one", 12_000)),
                entry("two", b"not an AMQP message".to_vec()),
            ]),
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
        )
        .await
        .into_message();

        assert_eq!(
            response
                .application_properties
                .as_ref()
                .and_then(|p| p.get("statusCode")),
            Some(&Value::Int(400))
        );
        assert!(broker.commands().is_empty());
    }

    #[tokio::test]
    async fn cancel_requires_positive_longs_and_submits_one_atomic_command() {
        let broker =
            RecordingBroker::returning(CommandOutcome::ScheduledCancelled { cancelled: 2 });
        let (namespace, entity) = names();
        let request = Message {
            body: Body::Value(map_body(
                SEQUENCE_NUMBERS,
                Value::Array(Array::from(vec![Value::Long(7), Value::Long(9)])),
            )),
            ..Message::default()
        };
        let response = cancel(
            &request,
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
        )
        .await
        .into_message();

        assert_eq!(
            response
                .application_properties
                .as_ref()
                .and_then(|p| p.get("statusCode")),
            Some(&Value::Int(200))
        );
        assert_eq!(
            broker.commands(),
            vec![CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(7), SequenceNumber::new(9)]
            }]
        );
    }
}
