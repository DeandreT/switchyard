//! Service Bus request/response browsing without acquiring message locks.

use amqp::{Body, Message, MessageId, encode_message};
use domain::{
    CommandKind, CommandOutcome, Delivery, EntityPath, NamespaceName, SequenceNumber, SessionHold,
};
use serde_amqp::{
    Value,
    primitives::{Binary, OrderedMap},
};

use crate::Broker;

use super::{
    ConnectionManagement, ManagementResponse, SESSION_ID, map_body, map_value, requested_session,
    session_lookup_response,
};

pub const PEEK_MESSAGE_OPERATION: &str = "com.microsoft:peek-message";

const FROM_SEQUENCE_NUMBER: &str = "from-sequence-number";
const MESSAGE_COUNT: &str = "message-count";
const MESSAGES: &str = "messages";
const MESSAGE: &str = "message";

#[allow(clippy::too_many_arguments)]
pub(super) async fn peek<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
) -> ManagementResponse {
    let (from_sequence, max_messages) = match peek_request(&message.body) {
        Ok(request) => request,
        Err(description) => {
            return ManagementResponse::bad_request(message_id, tracking_id, description);
        }
    };
    let session = match optional_session(
        message,
        message_id.clone(),
        tracking_id.clone(),
        entity,
        management,
    )
    .await
    {
        Ok(session) => session,
        Err(response) => return response,
    };

    let result = broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::Peek {
                from_sequence,
                max_messages,
                session,
            },
        )
        .await;
    let deliveries = match result {
        Ok(CommandOutcome::Peeked(deliveries)) => deliveries,
        Ok(other) => {
            return ManagementResponse::internal(
                message_id,
                tracking_id,
                format!("peeking messages produced an unexpected outcome: {other:?}"),
            );
        }
        Err(rejection) => {
            return ManagementResponse::from_rejection(message_id, tracking_id, &rejection);
        }
    };

    if deliveries.is_empty() {
        return ManagementResponse::no_content(message_id, tracking_id);
    }
    let entries = match encode_deliveries(&deliveries) {
        Ok(entries) => entries,
        Err(error) => {
            return ManagementResponse::internal(
                message_id,
                tracking_id,
                format!("a peeked message could not be encoded: {error}"),
            );
        }
    };
    ManagementResponse::accepted(
        message_id,
        tracking_id,
        map_body(MESSAGES, Value::List(entries)),
    )
}

fn peek_request(body: &Body) -> Result<(SequenceNumber, u32), &'static str> {
    let from_sequence = match map_value(body, FROM_SEQUENCE_NUMBER) {
        Some(Value::Long(value)) if *value >= 0 => {
            SequenceNumber::new(u64::try_from(*value).expect("a nonnegative i64 fits in u64"))
        }
        _ => return Err("from-sequence-number must be a nonnegative AMQP long"),
    };
    let max_messages = match map_value(body, MESSAGE_COUNT) {
        Some(Value::Int(value)) if *value > 0 => {
            u32::try_from(*value).expect("a positive i32 fits in u32")
        }
        _ => return Err("message-count must be a positive AMQP int"),
    };
    Ok((from_sequence, max_messages))
}

async fn optional_session(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    entity: &EntityPath,
    management: &ConnectionManagement,
) -> Result<Option<SessionHold>, ManagementResponse> {
    if map_value(&message.body, SESSION_ID).is_none() {
        return Ok(None);
    }
    requested_session(message, entity, management)
        .await
        .map(|session| Some(session.hold))
        .map_err(|error| session_lookup_response(message_id, tracking_id, error))
}

fn encode_deliveries(deliveries: &[Delivery]) -> Result<Vec<Value>, crate::ProtocolError> {
    deliveries
        .iter()
        .map(|delivery| {
            // Direct DLQ browsing has no DeadLetterSource. Azure reserves it
            // for messages auto-forwarded out of a DLQ.
            let message = crate::message::write_peeked_delivery_from(delivery, None)?;
            let encoded = encode_message(&message).map_err(|error| {
                crate::ProtocolError::InvalidEnvelope {
                    detail: error.to_string(),
                }
            })?;
            let mut entry = OrderedMap::new();
            entry.insert(
                Value::String(String::from(MESSAGE)),
                Value::Binary(Binary::from(encoded)),
            );
            Ok(Value::Map(entry))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use amqp::{
        AnnotationKey, ApplicationProperties, DeliveryAnnotations, MessageAnnotations, Properties,
        decode_message,
    };
    use domain::{
        DeadLetterInfo, DeadLetterReason, DeliveryLock, DeliveryOrigin, LockToken, MessageEnvelope,
        SessionId, Timestamp,
    };
    use serde_amqp::primitives::Uuid;

    use super::*;
    use crate::ASSOCIATED_LINK_NAME_PROPERTY;

    #[derive(Clone)]
    struct RecordingBroker {
        commands: Arc<Mutex<Vec<CommandKind>>>,
        deliveries: Arc<Vec<Delivery>>,
    }

    impl RecordingBroker {
        fn returning(deliveries: Vec<Delivery>) -> Self {
            Self {
                commands: Arc::new(Mutex::new(Vec::new())),
                deliveries: Arc::new(deliveries),
            }
        }

        fn commands(&self) -> Vec<CommandKind> {
            self.commands
                .lock()
                .expect("the command recorder is not poisoned")
                .clone()
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
            let deliveries = Arc::clone(&self.deliveries);
            async move {
                commands
                    .lock()
                    .expect("the command recorder is not poisoned")
                    .push(kind);
                Ok(CommandOutcome::Peeked(deliveries.as_ref().clone()))
            }
        }

        fn deliverable(
            &self,
            _namespace: &NamespaceName,
            _entity: &EntityPath,
        ) -> impl std::future::Future<Output = ()> + Send {
            std::future::pending()
        }
    }

    fn body(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Body {
        let mut map = OrderedMap::new();
        for (name, value) in entries {
            map.insert(Value::String(name.to_owned()), value);
        }
        Body::Value(Value::Map(map))
    }

    fn request(from_sequence: i64, max_messages: i32) -> Message {
        Message {
            body: body([
                (FROM_SEQUENCE_NUMBER, Value::Long(from_sequence)),
                (MESSAGE_COUNT, Value::Int(max_messages)),
            ]),
            ..Message::default()
        }
    }

    fn namespace() -> NamespaceName {
        NamespaceName::new("local").expect("a valid namespace")
    }

    fn entity() -> EntityPath {
        EntityPath::new("orders").expect("a valid entity")
    }

    fn delivery(origin: DeliveryOrigin) -> Delivery {
        Delivery {
            sequence: SequenceNumber::new(7),
            message_id: String::from("order-7"),
            body: b"payload".to_vec(),
            enqueued_at: Timestamp::from_millis(10),
            scheduled_enqueue_at: None,
            expires_at: Some(Timestamp::from_millis(1_010)),
            delivery_count: 4,
            origin,
            lock: None,
            session_id: None,
            dead_letter: None,
            envelope: None,
        }
    }

    #[test]
    fn request_requires_the_exact_signed_wire_types_and_bounds() {
        assert_eq!(
            peek_request(&request(0, 500).body),
            Ok((SequenceNumber::new(0), 500))
        );

        for body in [
            body([(MESSAGE_COUNT, Value::Int(1))]),
            body([
                (FROM_SEQUENCE_NUMBER, Value::Long(-1)),
                (MESSAGE_COUNT, Value::Int(1)),
            ]),
            body([
                (FROM_SEQUENCE_NUMBER, Value::Ulong(0)),
                (MESSAGE_COUNT, Value::Int(1)),
            ]),
        ] {
            assert_eq!(
                peek_request(&body),
                Err("from-sequence-number must be a nonnegative AMQP long")
            );
        }
        for value in [Value::Int(0), Value::Int(-1), Value::Uint(1)] {
            assert_eq!(
                peek_request(&body([
                    (FROM_SEQUENCE_NUMBER, Value::Long(0)),
                    (MESSAGE_COUNT, value),
                ])),
                Err("message-count must be a positive AMQP int")
            );
        }
        assert_eq!(
            peek_request(&body([(FROM_SEQUENCE_NUMBER, Value::Long(0))])),
            Err("message-count must be a positive AMQP int")
        );
    }

    #[tokio::test]
    async fn a_cross_session_peek_maps_to_the_domain_command_and_empty_is_204() {
        let broker = RecordingBroker::returning(Vec::new());
        let management = ConnectionManagement::new();
        let mut request = request(12, 500);
        let mut properties = ApplicationProperties::default();
        properties.insert(crate::OPERATION_PROPERTY, PEEK_MESSAGE_OPERATION);
        properties.insert(crate::TRACKING_ID_PROPERTY, "trace-1");
        request.application_properties = Some(properties);
        let response = super::super::process_request(
            &request,
            MessageId::Ulong(1),
            &namespace(),
            &entity(),
            &broker,
            &management,
            None,
        )
        .await;

        assert_eq!(response.status_code, 204);
        assert_eq!(response.status_description, "No Content");
        assert_eq!(response.tracking_id.as_deref(), Some("trace-1"));
        assert_eq!(response.body, Value::Null);
        let wire_response = response.into_message();
        assert_eq!(wire_response.body, Body::Value(Value::Null));
        assert_eq!(
            wire_response
                .application_properties
                .as_ref()
                .and_then(|properties| properties.get(crate::STATUS_CODE_PROPERTY)),
            Some(&Value::Int(204))
        );
        assert_eq!(
            broker.commands(),
            vec![CommandKind::Peek {
                from_sequence: SequenceNumber::new(12),
                max_messages: 500,
                session: None,
            }]
        );
    }

    #[tokio::test]
    async fn a_named_session_must_resolve_through_the_associated_live_link() {
        let broker = RecordingBroker::returning(Vec::new());
        let management = ConnectionManagement::new();
        let entity = entity();
        let hold = SessionHold::new(
            SessionId::new("cart-1").expect("a valid session"),
            LockToken::new(9),
        );
        management
            .register_session("receiver-1", entity.clone(), hold.clone())
            .await;
        let mut request = request(0, 1);
        request.body = body([
            (FROM_SEQUENCE_NUMBER, Value::Long(0)),
            (MESSAGE_COUNT, Value::Int(1)),
            (SESSION_ID, Value::String(String::from("cart-1"))),
        ]);
        let mut properties = ApplicationProperties::default();
        properties.insert(ASSOCIATED_LINK_NAME_PROPERTY, "receiver-1");
        request.application_properties = Some(properties);

        let response = peek(
            &request,
            MessageId::Ulong(1),
            None,
            &namespace(),
            &entity,
            &broker,
            &management,
        )
        .await;
        assert_eq!(response.status_code, 204);
        assert_eq!(
            broker.commands(),
            vec![CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 1,
                session: Some(hold),
            }]
        );

        request.application_properties = None;
        let rejected = peek(
            &request,
            MessageId::Ulong(2),
            None,
            &namespace(),
            &entity,
            &broker,
            &management,
        )
        .await;
        assert_eq!(rejected.status_code, 400);
        assert_eq!(broker.commands().len(), 1);
    }

    #[tokio::test]
    async fn a_session_name_that_the_associated_link_does_not_hold_is_lock_lost() {
        let broker = RecordingBroker::returning(Vec::new());
        let management = ConnectionManagement::new();
        let entity = entity();
        management
            .register_session(
                "receiver-1",
                entity.clone(),
                SessionHold::new(
                    SessionId::new("cart-1").expect("a valid session"),
                    LockToken::new(9),
                ),
            )
            .await;
        let mut request = request(0, 1);
        request.body = body([
            (FROM_SEQUENCE_NUMBER, Value::Long(0)),
            (MESSAGE_COUNT, Value::Int(1)),
            (SESSION_ID, Value::String(String::from("cart-2"))),
        ]);
        let mut properties = ApplicationProperties::default();
        properties.insert(ASSOCIATED_LINK_NAME_PROPERTY, "receiver-1");
        request.application_properties = Some(properties);

        let response = peek(
            &request,
            MessageId::Ulong(1),
            None,
            &namespace(),
            &entity,
            &broker,
            &management,
        )
        .await;
        assert_eq!(response.status_code, 410);
        assert_eq!(response.error_condition, Some(crate::SESSION_LOCK_LOST));
        assert!(broker.commands().is_empty());
    }

    #[tokio::test]
    async fn response_embeds_lockless_messages_with_peek_counts_and_broker_overlays() {
        let mut stored = Message::data(b"payload".to_vec());
        stored.properties = Some(Properties {
            message_id: Some(String::from("sender-id").into()),
            ..Properties::default()
        });
        let mut message_annotations = MessageAnnotations::default();
        message_annotations.insert("x-opt-locked-until", Value::Timestamp(99.into()));
        message_annotations.insert("x-opt-message-state", Value::Int(2));
        stored.message_annotations = Some(message_annotations);
        let mut delivery_annotations = DeliveryAnnotations::default();
        delivery_annotations.insert("x-opt-lock-token", Value::Uuid(Uuid::from([9_u8; 16])));
        stored.delivery_annotations = Some(delivery_annotations);
        stored.application_properties = Some(
            ApplicationProperties::builder()
                .insert("custom", "kept")
                .build(),
        );

        let mut peeked = delivery(DeliveryOrigin::Deferred);
        peeked.lock = Some(DeliveryLock {
            token: LockToken::new(42),
            locked_until: Timestamp::from_millis(500),
            lock_duration_millis: 60_000,
        });
        peeked.dead_letter = Some(DeadLetterInfo {
            reason: DeadLetterReason::Application(String::from("InvalidOrder")),
            description: String::from("missing customer"),
            dead_lettered_at: Timestamp::from_millis(20),
        });
        peeked.envelope = Some(MessageEnvelope::new(
            encode_message(&stored).expect("the stored message encodes"),
        ));
        let broker = RecordingBroker::returning(vec![peeked]);
        let management = ConnectionManagement::new();
        let dead_letter = EntityPath::new("orders/$deadletterqueue").expect("a valid entity");

        let response = peek(
            &request(0, 1),
            MessageId::Ulong(1),
            None,
            &namespace(),
            &dead_letter,
            &broker,
            &management,
        )
        .await;
        assert_eq!(response.status_code, 200);
        let Value::Map(body) = response.body else {
            panic!("the response body must be a map")
        };
        let Value::List(entries) = body
            .get(&Value::String(String::from(MESSAGES)))
            .expect("the response contains messages")
        else {
            panic!("messages must be an AMQP list")
        };
        assert_eq!(entries.len(), 1);
        let Value::Map(entry) = &entries[0] else {
            panic!("a message entry must be a map")
        };
        assert_eq!(entry.len(), 1);
        let Value::Binary(encoded) = entry
            .get(&Value::String(String::from(MESSAGE)))
            .expect("the entry contains encoded message bytes")
        else {
            panic!("message must be AMQP binary")
        };
        let message = decode_message(encoded).expect("the embedded message decodes");

        assert_eq!(
            message.header.as_ref().map(|header| header.delivery_count),
            Some(4)
        );
        assert_eq!(
            message
                .message_annotations
                .as_ref()
                .and_then(|annotations| annotations.get(&AnnotationKey::from("x-opt-locked-until"))),
            None
        );
        assert_eq!(
            message
                .delivery_annotations
                .as_ref()
                .and_then(|annotations| annotations.get(&AnnotationKey::from("x-opt-lock-token"))),
            None
        );
        assert_eq!(
            message.message_annotations.as_ref().and_then(
                |annotations| annotations.get(&AnnotationKey::from("x-opt-message-state"))
            ),
            Some(&Value::Int(1))
        );
        assert_eq!(
            message.message_annotations.as_ref().and_then(
                |annotations| annotations.get(&AnnotationKey::from("x-opt-deadletter-source"))
            ),
            None,
            "direct DLQ browsing must not claim an auto-forward source"
        );
        let properties = message
            .application_properties
            .expect("application properties exist");
        assert_eq!(
            properties.get("custom"),
            Some(&Value::String(String::from("kept")))
        );
        assert_eq!(
            properties.get(crate::DEAD_LETTER_REASON_PROPERTY),
            Some(&Value::String(String::from("InvalidOrder")))
        );
        assert_eq!(
            properties.get(crate::DEAD_LETTER_DESCRIPTION_PROPERTY),
            Some(&Value::String(String::from("missing customer")))
        );
    }
}
