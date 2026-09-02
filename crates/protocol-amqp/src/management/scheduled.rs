//! Service Bus request/response scheduling and cancellation operations.

use amqp::{Body, Message, MessageId, decode_message};
use domain::{
    CommandKind, CommandOutcome, EntityPath, MessageInput, NamespaceName, SequenceNumber,
};
use serde_amqp::{Value, primitives::Array};

use crate::{Broker, read_incoming, validate_standard_message_size};

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
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::SendBatch { messages },
        )
        .await
    {
        Ok(CommandOutcome::BatchSent { sequences, stored })
            if sequences.len() == expected
                && u32::try_from(expected).is_ok_and(|expected| stored <= expected) =>
        {
            match sequence_values(&sequences) {
                Ok(values) => ManagementResponse::accepted(
                    message_id,
                    tracking_id,
                    map_body(SEQUENCE_NUMBERS, Value::Array(Array::from(values))),
                ),
                Err(description) => {
                    ManagementResponse::internal(message_id, tracking_id, description)
                }
            }
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("scheduling messages produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
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

fn scheduled_messages(body: &Body) -> Result<Vec<MessageInput>, String> {
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

    entries
        .iter()
        .enumerate()
        .map(|(index, entry)| scheduled_message(entry, index))
        .collect()
}

fn scheduled_message(entry: &Value, index: usize) -> Result<MessageInput, String> {
    let Value::Map(entry) = entry else {
        return Err(format!("messages[{index}] must be an AMQP map"));
    };
    let encoded = match entry_value(entry, MESSAGE) {
        Some(Value::Binary(encoded)) => encoded.as_slice(),
        _ => return Err(format!("messages[{index}].message must be AMQP binary")),
    };
    validate_standard_message_size(encoded.len()).map_err(|error| error.to_string())?;
    let decoded = decode_message(encoded).map_err(|error| {
        format!("messages[{index}].message is not a complete AMQP message: {error}")
    })?;
    let incoming = read_incoming(&decoded, encoded)
        .map_err(|error| format!("messages[{index}].message is invalid: {error}"))?;
    if incoming.scheduled_enqueue_at.is_none() {
        return Err(format!(
            "messages[{index}].message requires x-opt-scheduled-enqueue-time"
        ));
    }

    let message_id =
        string_entry(entry, MESSAGE_ID, index, true)?.expect("a required string entry is present");
    if message_id != incoming.message_id {
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

    Ok(incoming.into())
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
        let [CommandKind::SendBatch { messages }] = commands.as_slice() else {
            panic!("expected one atomic scheduled batch")
        };
        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages[0].scheduled_enqueue_at,
            Some(Timestamp::from_millis(12_000))
        );
        assert_eq!(messages[1].message_id, "two");
        assert!(messages[0].envelope.is_some());
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
