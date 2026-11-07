//! Service Bus request/response operations for deferred messages.

use amqp::{Body, Fields, Message, MessageId, encode_message};
use domain::{
    CommandKind, CommandOutcome, Delivery, EntityPath, LockToken, NamespaceName, ReceiveMode,
    SequenceNumber, SessionHold,
};
use serde_amqp::{
    Value,
    primitives::{Binary, OrderedMap, Symbol, Uuid},
};

use crate::Broker;

use super::{
    ASSOCIATED_LINK_NAME_PROPERTY, ConnectionManagement, ManagementResponse, SESSION_ID, map_body,
    map_value, requested_session, session_lookup_response, string_map_value, string_property,
};

pub const RECEIVE_BY_SEQUENCE_NUMBER_OPERATION: &str = "com.microsoft:receive-by-sequence-number";
pub const UPDATE_DISPOSITION_OPERATION: &str = "com.microsoft:update-disposition";

const SEQUENCE_NUMBERS: &str = "sequence-numbers";
const RECEIVER_SETTLE_MODE: &str = "receiver-settle-mode";
const MESSAGES: &str = "messages";
const MESSAGE: &str = "message";
const LOCK_TOKEN: &str = "lock-token";
const DISPOSITION_STATUS: &str = "disposition-status";
const PROPERTIES_TO_MODIFY: &str = "properties-to-modify";
const DEAD_LETTER_REASON: &str = "deadletter-reason";
const DEAD_LETTER_DESCRIPTION: &str = "deadletter-description";

#[allow(clippy::too_many_arguments)]
pub(super) async fn receive_by_sequence_number<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
) -> ManagementResponse {
    let Some(sequences) = sequence_numbers(&message.body) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "sequence-numbers must be an AMQP value array of positive longs",
        );
    };
    let Some(mode) = receiver_settle_mode(&message.body) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "receiver-settle-mode must be uint 0 or 1",
        );
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
            CommandKind::ReceiveDeferred {
                sequences,
                mode,
                lock_duration_millis: None,
                session,
            },
        )
        .await;
    let deliveries = match result {
        Ok(CommandOutcome::DeferredReceived(deliveries)) => deliveries,
        Ok(other) => {
            return ManagementResponse::internal(
                message_id,
                tracking_id,
                format!("receiving deferred messages produced an unexpected outcome: {other:?}"),
            );
        }
        Err(rejection) => {
            return ManagementResponse::from_rejection(message_id, tracking_id, &rejection);
        }
    };

    let entries = match encode_deliveries(&deliveries) {
        Ok(entries) => entries,
        Err(error) => {
            return ManagementResponse::internal(
                message_id,
                tracking_id,
                format!("a deferred message could not be encoded: {error}"),
            );
        }
    };
    for delivery in deliveries {
        if delivery.lock.is_some() {
            management
                .register_request_response_delivery(entity.clone(), delivery)
                .await;
        }
    }

    ManagementResponse::accepted(
        message_id,
        tracking_id,
        map_body(MESSAGES, Value::List(entries)),
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn update_disposition<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
) -> ManagementResponse {
    let Some(tokens) = super::lock_tokens(&message.body) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "lock-tokens must be an AMQP value array of UUIDs",
        );
    };
    if tokens.len() != 1 {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "exactly one lock token is required",
        );
    }
    if let Err(response) = optional_session(
        message,
        message_id.clone(),
        tracking_id.clone(),
        entity,
        management,
    )
    .await
    {
        return response;
    }

    let link_name = message
        .application_properties
        .as_ref()
        .and_then(|properties| string_property(properties, ASSOCIATED_LINK_NAME_PROPERTY));
    let lock_token = tokens[0];
    let Some(delivery) = management
        .managed_delivery(entity, link_name, lock_token)
        .await
    else {
        return ManagementResponse::lock_lost(
            message_id,
            tracking_id,
            "the lock token is not active for this entity",
        );
    };
    let properties = match properties_to_modify(&message.body) {
        Ok(properties) => properties,
        Err(description) => {
            return ManagementResponse::bad_request(message_id, tracking_id, description);
        }
    };
    let Some(status) = string_map_value(&message.body, DISPOSITION_STATUS) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "disposition-status must be an AMQP value string",
        );
    };
    let (kind, expected) = match disposition_command(
        status,
        message,
        delivery.managed.sequence,
        lock_token,
        properties.as_ref(),
    ) {
        Ok(disposition) => disposition,
        Err(DispositionError::Request(description)) => {
            return ManagementResponse::bad_request(message_id, tracking_id, description);
        }
        Err(DispositionError::Patch(error)) => {
            return ManagementResponse::invalid_field(message_id, tracking_id, error.to_string());
        }
    };

    match broker.submit(namespace.clone(), entity.clone(), kind).await {
        Ok(outcome) if expected.matches(&outcome) => {
            management.unregister_managed_delivery(&delivery).await;
            ManagementResponse::accepted(message_id, tracking_id, Value::Null)
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("updating a disposition produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => {
            if super::definitive_message_lock_loss(&rejection) {
                management.unregister_managed_delivery(&delivery).await;
                return ManagementResponse::lock_lost(
                    message_id,
                    tracking_id,
                    rejection.to_string(),
                );
            }
            ManagementResponse::from_rejection(message_id, tracking_id, &rejection)
        }
    }
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

fn sequence_numbers(body: &Body) -> Option<Vec<SequenceNumber>> {
    let Value::Array(values) = map_value(body, SEQUENCE_NUMBERS)? else {
        return None;
    };
    values
        .iter()
        .map(|value| match value {
            Value::Long(value) if *value > 0 => u64::try_from(*value).ok().map(SequenceNumber::new),
            _ => None,
        })
        .collect()
}

fn receiver_settle_mode(body: &Body) -> Option<ReceiveMode> {
    match map_value(body, RECEIVER_SETTLE_MODE) {
        Some(Value::Uint(0)) => Some(ReceiveMode::ReceiveAndDelete),
        Some(Value::Uint(1)) => Some(ReceiveMode::PeekLock),
        _ => None,
    }
}

fn encode_deliveries(deliveries: &[Delivery]) -> Result<Vec<Value>, crate::ProtocolError> {
    deliveries
        .iter()
        .map(|delivery| {
            let message = crate::message::write_delivery_from(delivery, None)?;
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
            if let Some(lock) = delivery.lock {
                entry.insert(
                    Value::String(String::from(LOCK_TOKEN)),
                    Value::Uuid(lock_uuid(lock.token)),
                );
            }
            Ok(Value::Map(entry))
        })
        .collect()
}

fn lock_uuid(lock_token: LockToken) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes[8..].copy_from_slice(&lock_token.as_u64().to_be_bytes());
    Uuid::from(bytes)
}

fn properties_to_modify(body: &Body) -> Result<Option<Fields>, &'static str> {
    let Some(value) = map_value(body, PROPERTIES_TO_MODIFY) else {
        return Ok(None);
    };
    let Value::Map(properties) = value else {
        return Err("properties-to-modify must be an AMQP value map");
    };
    let mut fields = Fields::new();
    for (name, value) in properties {
        let name = match name {
            Value::String(name) => name.as_str(),
            Value::Symbol(name) => name.as_str(),
            _ => return Err("properties-to-modify keys must be strings or symbols"),
        };
        fields.insert(Symbol::from(name), value.clone());
    }
    Ok(Some(fields))
}

/// Why a disposition request cannot become a broker command.
#[derive(Debug)]
enum DispositionError {
    /// The request itself is malformed.
    Request(&'static str),
    /// A property change no peer could send.
    Patch(crate::ProtocolError),
}

fn disposition_command(
    status: &str,
    message: &Message,
    sequence: SequenceNumber,
    lock_token: LockToken,
    properties: Option<&Fields>,
) -> Result<(CommandKind, ExpectedDisposition), DispositionError> {
    let has_properties = properties.is_some_and(|properties| !properties.is_empty());
    if status == "completed" {
        return if has_properties {
            Err(DispositionError::Request(
                "properties-to-modify is not supported for completion",
            ))
        } else {
            Ok((
                CommandKind::Complete {
                    sequence,
                    lock_token,
                },
                ExpectedDisposition::Completed,
            ))
        };
    }
    if !matches!(status, "defered" | "abandoned" | "suspended") {
        return Err(DispositionError::Request(
            "disposition-status must be completed, abandoned, defered, or suspended",
        ));
    }
    // The broker applies a patch to the locked copy wherever its lock came
    // from, so a link-received lock can change properties too. Without a
    // property change the legacy command leaves the copy's bytes unchanged.
    let patch = properties
        .map(crate::message::property_patch)
        .transpose()
        .map_err(DispositionError::Patch)?
        .filter(|patch| !patch.is_empty());
    Ok(match (status, patch) {
        ("defered", Some(patch)) => (
            CommandKind::DeferWithPatch {
                sequence,
                lock_token,
                patch,
            },
            ExpectedDisposition::Deferred,
        ),
        ("defered", None) => (
            CommandKind::Defer {
                sequence,
                lock_token,
                replacement_envelope: None,
            },
            ExpectedDisposition::Deferred,
        ),
        ("abandoned", Some(patch)) => (
            CommandKind::AbandonWithPatch {
                sequence,
                lock_token,
                patch,
            },
            ExpectedDisposition::Abandoned,
        ),
        ("abandoned", None) => (
            CommandKind::Abandon {
                sequence,
                lock_token,
                replacement_envelope: None,
            },
            ExpectedDisposition::Abandoned,
        ),
        (_, patch) => {
            let reason = optional_string(message, DEAD_LETTER_REASON)
                .unwrap_or("RejectedByReceiver")
                .to_owned();
            let description = optional_string(message, DEAD_LETTER_DESCRIPTION)
                .unwrap_or("the receiver rejected the message")
                .to_owned();
            let kind = match patch {
                Some(patch) => CommandKind::DeadLetterWithPatch {
                    sequence,
                    lock_token,
                    reason,
                    description,
                    patch,
                },
                None => CommandKind::DeadLetter {
                    sequence,
                    lock_token,
                    reason,
                    description,
                    replacement_envelope: None,
                },
            };
            (kind, ExpectedDisposition::DeadLettered)
        }
    })
}

fn optional_string<'a>(message: &'a Message, name: &str) -> Option<&'a str> {
    match map_value(&message.body, name) {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum ExpectedDisposition {
    Completed,
    Abandoned,
    DeadLettered,
    Deferred,
}

impl ExpectedDisposition {
    fn matches(self, outcome: &CommandOutcome) -> bool {
        matches!(
            (self, outcome),
            (Self::Completed, CommandOutcome::Completed)
                | (Self::Abandoned, CommandOutcome::Abandoned { .. })
                | (Self::DeadLettered, CommandOutcome::DeadLettered)
                | (Self::Deferred, CommandOutcome::Deferred)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_amqp::primitives::Array;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct RecordingBroker {
        commands: Arc<Mutex<Vec<CommandKind>>>,
        rejection: Arc<Mutex<Option<crate::BrokerRejection>>>,
    }

    impl RecordingBroker {
        fn refusing(rejection: crate::BrokerRejection) -> Self {
            Self {
                rejection: Arc::new(Mutex::new(Some(rejection))),
                ..Self::default()
            }
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
            let rejection = Arc::clone(&self.rejection);
            async move {
                commands
                    .lock()
                    .expect("the command recorder is not poisoned")
                    .push(kind.clone());
                if let Some(rejection) = rejection
                    .lock()
                    .expect("the rejection recorder is not poisoned")
                    .clone()
                {
                    return Err(rejection);
                }
                let outcome = match &kind {
                    CommandKind::Complete { .. } => CommandOutcome::Completed,
                    CommandKind::Defer { .. } | CommandKind::DeferWithPatch { .. } => {
                        CommandOutcome::Deferred
                    }
                    CommandKind::Abandon { .. } | CommandKind::AbandonWithPatch { .. } => {
                        CommandOutcome::Abandoned {
                            dead_lettered: false,
                        }
                    }
                    CommandKind::DeadLetter { .. } | CommandKind::DeadLetterWithPatch { .. } => {
                        CommandOutcome::DeadLettered
                    }
                    _ => panic!("unexpected test command: {kind:?}"),
                };
                Ok(outcome)
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

    #[test]
    fn receive_request_uses_signed_sequence_numbers_and_uint_settle_mode() {
        let request = body([
            (
                SEQUENCE_NUMBERS,
                Value::Array(Array::from(vec![Value::Long(7), Value::Long(9)])),
            ),
            (RECEIVER_SETTLE_MODE, Value::Uint(1)),
        ]);

        assert_eq!(
            sequence_numbers(&request),
            Some(vec![SequenceNumber::new(7), SequenceNumber::new(9)])
        );
        assert_eq!(receiver_settle_mode(&request), Some(ReceiveMode::PeekLock));
    }

    #[test]
    fn receive_delete_is_settle_mode_zero() {
        let request = body([(RECEIVER_SETTLE_MODE, Value::Uint(0))]);
        assert_eq!(
            receiver_settle_mode(&request),
            Some(ReceiveMode::ReceiveAndDelete)
        );
    }

    #[test]
    fn malformed_sequence_arrays_are_not_reinterpreted() {
        for value in [
            Value::List(vec![Value::Long(1)]),
            Value::Array(Array::from(vec![Value::Long(0)])),
            Value::Array(Array::from(vec![Value::String(String::from("1"))])),
        ] {
            assert_eq!(sequence_numbers(&body([(SEQUENCE_NUMBERS, value)])), None);
        }
    }

    #[test]
    fn the_service_bus_defer_status_keeps_its_historical_spelling() {
        let (kind, expected) = disposition_command(
            "defered",
            &Message::default(),
            SequenceNumber::new(7),
            LockToken::new(9),
            None,
        )
        .expect("defered is the wire spelling");
        assert_eq!(
            kind,
            CommandKind::Defer {
                sequence: SequenceNumber::new(7),
                lock_token: LockToken::new(9),
                replacement_envelope: None,
            }
        );
        assert!(expected.matches(&CommandOutcome::Deferred));
        assert!(
            disposition_command(
                "deferred",
                &Message::default(),
                SequenceNumber::new(7),
                LockToken::new(9),
                None,
            )
            .is_err()
        );
    }

    fn patch_entry(name: &str, value: Value) -> domain::OverlayProperty {
        domain::OverlayProperty {
            name: name.to_owned(),
            value: serde_amqp::to_vec(&value).expect("the patch value encodes"),
        }
    }

    #[test]
    fn management_dispositions_send_property_changes_as_patches() {
        let sequence = SequenceNumber::new(7);
        let lock_token = LockToken::new(9);
        let mut properties = Fields::new();
        properties.insert(Symbol::from("reviewed-by"), Value::String("sdk".into()));
        // Broker-owned dead-letter names never become patch entries.
        properties.insert(
            Symbol::from(crate::DEAD_LETTER_DESCRIPTION_PROPERTY),
            Value::String("forged".into()),
        );
        let patch = vec![patch_entry("reviewed-by", Value::String("sdk".into()))];

        let (abandon, abandon_outcome) = disposition_command(
            "abandoned",
            &Message::default(),
            sequence,
            lock_token,
            Some(&properties),
        )
        .expect("abandon supports properties-to-modify");
        assert_eq!(
            abandon,
            CommandKind::AbandonWithPatch {
                sequence,
                lock_token,
                patch: patch.clone(),
            }
        );
        assert!(abandon_outcome.matches(&CommandOutcome::Abandoned {
            dead_lettered: false,
        }));

        let (defer, defer_outcome) = disposition_command(
            "defered",
            &Message::default(),
            sequence,
            lock_token,
            Some(&properties),
        )
        .expect("defer supports properties-to-modify");
        assert_eq!(
            defer,
            CommandKind::DeferWithPatch {
                sequence,
                lock_token,
                patch: patch.clone(),
            }
        );
        assert!(defer_outcome.matches(&CommandOutcome::Deferred));

        let dead_letter_request = Message {
            body: body([
                (DEAD_LETTER_REASON, Value::String("InvalidOrder".into())),
                (
                    DEAD_LETTER_DESCRIPTION,
                    Value::String("the order is incomplete".into()),
                ),
            ]),
            ..Message::default()
        };
        let (dead_letter, dead_letter_outcome) = disposition_command(
            "suspended",
            &dead_letter_request,
            sequence,
            lock_token,
            Some(&properties),
        )
        .expect("dead-letter supports properties-to-modify");
        assert_eq!(
            dead_letter,
            CommandKind::DeadLetterWithPatch {
                sequence,
                lock_token,
                reason: String::from("InvalidOrder"),
                description: String::from("the order is incomplete"),
                patch,
            }
        );
        assert!(dead_letter_outcome.matches(&CommandOutcome::DeadLettered));
    }

    #[test]
    fn management_dispositions_without_property_changes_keep_legacy_commands() {
        let sequence = SequenceNumber::new(7);
        let lock_token = LockToken::new(9);
        let empty = Fields::new();
        let mut reserved_only = Fields::new();
        reserved_only.insert(
            Symbol::from(crate::DEAD_LETTER_REASON_PROPERTY),
            Value::String("forged".into()),
        );
        for properties in [None, Some(&empty), Some(&reserved_only)] {
            let (abandon, _) = disposition_command(
                "abandoned",
                &Message::default(),
                sequence,
                lock_token,
                properties,
            )
            .expect("abandon is valid");
            assert_eq!(
                abandon,
                CommandKind::Abandon {
                    sequence,
                    lock_token,
                    replacement_envelope: None,
                }
            );
        }
        let mut properties = Fields::new();
        properties.insert(Symbol::from("reviewed-by"), Value::String("sdk".into()));
        assert!(matches!(
            disposition_command(
                "completed",
                &Message::default(),
                sequence,
                lock_token,
                Some(&properties),
            ),
            Err(DispositionError::Request(_))
        ));
    }

    fn locked_delivery(sequence: u64, token: u64, lock_duration_millis: u64) -> Delivery {
        Delivery {
            sequence: SequenceNumber::new(sequence),
            message_id: format!("deferred-{sequence}"),
            body: Vec::new(),
            enqueued_at: domain::Timestamp::from_millis(10),
            scheduled_enqueue_at: None,
            expires_at: None,
            delivery_count: 1,
            lock: Some(domain::DeliveryLock {
                token: LockToken::new(token),
                locked_until: domain::Timestamp::from_millis(100),
                lock_duration_millis,
            }),
            session_id: None,
            dead_letter: None,
            envelope: None,
            origin: domain::DeliveryOrigin::Deferred,
        }
    }

    #[tokio::test]
    async fn request_response_locks_expire_and_are_purged_monotonically() {
        let management = ConnectionManagement::new();
        let entity = EntityPath::new("orders").expect("valid entity");
        let started = tokio::time::Instant::now();
        management
            .register_request_response_delivery_at(
                entity.clone(),
                locked_delivery(7, 9, 1_000),
                started,
            )
            .await;

        assert!(
            management
                .request_response_delivery_at(
                    &entity,
                    LockToken::new(9),
                    started + std::time::Duration::from_millis(999),
                )
                .await
                .is_some()
        );
        assert_eq!(
            management
                .request_response_delivery_at(
                    &entity,
                    LockToken::new(9),
                    started + std::time::Duration::from_millis(1_000),
                )
                .await,
            None
        );
        assert!(
            management
                .request_response_deliveries
                .read()
                .await
                .is_empty()
        );

        // Insertion is also a purge point, bounding a continuously used
        // connection to entries whose monotonic lifetimes are still active.
        management
            .register_request_response_delivery_at(
                entity.clone(),
                locked_delivery(8, 10, 1),
                started,
            )
            .await;
        management
            .register_request_response_delivery_at(
                entity,
                locked_delivery(9, 11, 1_000),
                started + std::time::Duration::from_millis(1),
            )
            .await;
        assert_eq!(management.request_response_deliveries.read().await.len(), 1);
    }

    #[tokio::test]
    async fn a_successful_renewal_rearms_the_monotonic_registry_deadline() {
        let management = ConnectionManagement::new();
        let entity = EntityPath::new("orders").expect("valid entity");
        let started = tokio::time::Instant::now();
        let registration = management
            .register_request_response_delivery_at(
                entity.clone(),
                locked_delivery(7, 9, 1_000),
                started,
            )
            .await;
        management
            .refresh_request_response_delivery_at(
                Some(&registration),
                2_000,
                started + std::time::Duration::from_millis(900),
            )
            .await;

        let renewed = management
            .request_response_delivery_at(
                &entity,
                LockToken::new(9),
                started + std::time::Duration::from_millis(2_899),
            )
            .await
            .expect("the renewed registry entry remains live");
        assert_eq!(renewed.sequence, SequenceNumber::new(7));
        assert_eq!(
            management
                .request_response_delivery_at(
                    &entity,
                    LockToken::new(9),
                    started + std::time::Duration::from_millis(2_900),
                )
                .await,
            None
        );
    }

    #[tokio::test]
    async fn a_request_response_lock_needs_no_associated_link() {
        let management = ConnectionManagement::new();
        let entity = EntityPath::new("orders").expect("valid entity");
        let delivery = Delivery {
            sequence: SequenceNumber::new(7),
            message_id: String::from("deferred"),
            body: Vec::new(),
            enqueued_at: domain::Timestamp::from_millis(10),
            scheduled_enqueue_at: None,
            expires_at: None,
            delivery_count: 1,
            lock: Some(domain::DeliveryLock {
                token: LockToken::new(9),
                locked_until: domain::Timestamp::from_millis(100),
                lock_duration_millis: 60_000,
            }),
            session_id: None,
            dead_letter: None,
            envelope: None,
            origin: domain::DeliveryOrigin::Deferred,
        };
        let entries = encode_deliveries(std::slice::from_ref(&delivery))
            .expect("the deferred response entry encodes");
        let [Value::Map(entry)] = entries.as_slice() else {
            panic!("one delivery must produce one AMQP map entry")
        };
        assert!(matches!(
            entry.get(&Value::String(String::from(MESSAGE))),
            Some(Value::Binary(encoded))
                if amqp::decode_message(encoded).is_ok()
        ));
        assert_eq!(
            entry.get(&Value::String(String::from(LOCK_TOKEN))),
            Some(&Value::Uuid(lock_uuid(LockToken::new(9))))
        );
        management
            .register_request_response_delivery(entity.clone(), delivery)
            .await;

        assert_eq!(
            management
                .managed_delivery(&entity, None, LockToken::new(9))
                .await
                .map(|delivery| delivery.managed.sequence),
            Some(SequenceNumber::new(7))
        );
        assert_eq!(
            management
                .managed_delivery(
                    &EntityPath::new("other").expect("valid entity"),
                    None,
                    LockToken::new(9),
                )
                .await,
            None
        );
    }

    #[tokio::test]
    async fn a_committed_request_response_settlement_removes_its_registry_entry() {
        let management = ConnectionManagement::new();
        let entity = EntityPath::new("orders").expect("valid entity");
        let namespace = NamespaceName::new("tenant").expect("valid namespace");
        let delivery = Delivery {
            sequence: SequenceNumber::new(7),
            message_id: String::from("deferred"),
            body: Vec::new(),
            enqueued_at: domain::Timestamp::from_millis(10),
            scheduled_enqueue_at: None,
            expires_at: None,
            delivery_count: 1,
            lock: Some(domain::DeliveryLock {
                token: LockToken::new(9),
                locked_until: domain::Timestamp::from_millis(100),
                lock_duration_millis: 60_000,
            }),
            session_id: None,
            dead_letter: None,
            envelope: None,
            origin: domain::DeliveryOrigin::Deferred,
        };
        management
            .register_request_response_delivery(entity.clone(), delivery)
            .await;
        let request = Message {
            body: body([
                (
                    crate::LOCK_TOKENS,
                    Value::Array(Array::from(vec![Value::Uuid(lock_uuid(LockToken::new(9)))])),
                ),
                (DISPOSITION_STATUS, Value::String(String::from("completed"))),
            ]),
            ..Message::default()
        };
        let broker = RecordingBroker::default();

        let response = update_disposition(
            &request,
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
            &management,
        )
        .await;

        assert_eq!(response.status_code, 200);
        assert_eq!(
            broker
                .commands
                .lock()
                .expect("the command recorder is not poisoned")
                .as_slice(),
            &[CommandKind::Complete {
                sequence: SequenceNumber::new(7),
                lock_token: LockToken::new(9),
            }]
        );
        assert_eq!(
            management
                .managed_delivery(&entity, None, LockToken::new(9))
                .await,
            None
        );
    }

    #[tokio::test]
    async fn a_missing_managed_message_is_lock_lost_and_removes_the_registry_entry() {
        let management = ConnectionManagement::new();
        let entity = EntityPath::new("orders").expect("valid entity");
        let namespace = NamespaceName::new("tenant").expect("valid namespace");
        management
            .register_request_response_delivery(entity.clone(), locked_delivery(7, 9, 60_000))
            .await;
        let request = Message {
            body: body([
                (
                    crate::LOCK_TOKENS,
                    Value::Array(Array::from(vec![Value::Uuid(lock_uuid(LockToken::new(9)))])),
                ),
                (DISPOSITION_STATUS, Value::String(String::from("completed"))),
            ]),
            ..Message::default()
        };
        let broker = RecordingBroker::refusing(crate::BrokerRejection::Refused(
            domain::BrokerError::MessageNotFound {
                sequence: SequenceNumber::new(7),
            },
        ));

        let response = update_disposition(
            &request,
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
            &management,
        )
        .await;

        assert_eq!(response.status_code, 410);
        assert_eq!(response.error_condition, Some(crate::MESSAGE_LOCK_LOST));
        assert_eq!(
            management
                .managed_delivery(&entity, None, LockToken::new(9))
                .await,
            None
        );
    }

    #[tokio::test]
    async fn a_link_received_lock_can_modify_properties_through_management() {
        let management = ConnectionManagement::new();
        let entity = EntityPath::new("orders").expect("valid entity");
        let namespace = NamespaceName::new("tenant").expect("valid namespace");
        let link = "receiver-link";
        management
            .register_delivery(
                link,
                entity.clone(),
                SequenceNumber::new(7),
                LockToken::new(9),
            )
            .await;
        let mut properties = OrderedMap::new();
        properties.insert(
            Value::String(String::from("reviewed-by")),
            Value::String(String::from("sdk")),
        );
        let request = Message {
            application_properties: Some(
                amqp::ApplicationProperties::builder()
                    .insert(ASSOCIATED_LINK_NAME_PROPERTY, link)
                    .build(),
            ),
            body: body([
                (
                    crate::LOCK_TOKENS,
                    Value::Array(Array::from(vec![Value::Uuid(lock_uuid(LockToken::new(9)))])),
                ),
                (DISPOSITION_STATUS, Value::String(String::from("abandoned"))),
                (PROPERTIES_TO_MODIFY, Value::Map(properties)),
            ]),
            ..Message::default()
        };
        let broker = RecordingBroker::default();

        let response = update_disposition(
            &request,
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
            &management,
        )
        .await;

        assert_eq!(response.status_code, 200, "{}", response.status_description);
        assert_eq!(
            broker
                .commands
                .lock()
                .expect("the command recorder is not poisoned")
                .as_slice(),
            &[CommandKind::AbandonWithPatch {
                sequence: SequenceNumber::new(7),
                lock_token: LockToken::new(9),
                patch: vec![patch_entry("reviewed-by", Value::String("sdk".into()))],
            }]
        );
    }

    /// The real broker behind the management handler, as the listener runs it.
    #[derive(Clone)]
    struct RealBroker(server::BrokerHandle);

    impl Broker for RealBroker {
        fn submit(
            &self,
            namespace: NamespaceName,
            entity: EntityPath,
            kind: CommandKind,
        ) -> impl std::future::Future<Output = Result<CommandOutcome, crate::BrokerRejection>> + Send
        {
            let handle = self.0.clone();
            async move {
                handle
                    .submit(namespace, entity, kind)
                    .await
                    .map_err(|error| match error {
                        server::SubmitError::Propose(server::ProposeError::Broker(error)) => {
                            crate::BrokerRejection::Refused(error)
                        }
                        other => crate::BrokerRejection::Unavailable(other.to_string()),
                    })
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

    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_patch_value_is_invalid_field_and_leaves_the_copy_and_lock() {
        let owner = server::Broker::spawn(server::LocalProposer::new(
            domain::StateMachine::new(storage::MemoryStore::default()),
            server::ManualClock::at(1_000),
        ));
        let broker = RealBroker(owner.handle());
        let namespace = NamespaceName::new("tenant").expect("valid namespace");
        let entity = EntityPath::new("orders").expect("valid entity");
        let submit = |kind| {
            owner
                .handle()
                .submit_blocking(namespace.clone(), entity.clone(), kind)
                .expect("the setup command commits")
        };
        submit(CommandKind::CreateQueue {
            config: domain::QueueConfig::default(),
        });
        let original = amqp::Message::builder()
            .application_properties(
                amqp::ApplicationProperties::builder()
                    .insert("stage", "received")
                    .build(),
            )
            .body(Body::Data(vec![b"kept".to_vec().into()]))
            .build();
        submit(CommandKind::SendContent {
            content: domain::MessageContent::Amqp(
                amqp::encode_message(&original).expect("the original encodes"),
            ),
        });
        let CommandOutcome::Received(Some(delivery)) = submit(CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        }) else {
            panic!("the message was locked")
        };
        let token = delivery.lock.expect("a peek lock").token;
        let link = "receiver-link";
        let management = ConnectionManagement::new();
        management
            .register_delivery(link, entity.clone(), delivery.sequence, token)
            .await;
        let peek = || {
            submit(CommandKind::Peek {
                from_sequence: delivery.sequence,
                max_messages: 1,
                session: None,
            })
        };
        let before = peek();
        let request = |properties: OrderedMap<Value, Value>| Message {
            application_properties: Some(
                amqp::ApplicationProperties::builder()
                    .insert(ASSOCIATED_LINK_NAME_PROPERTY, link)
                    .build(),
            ),
            body: body([
                (
                    crate::LOCK_TOKENS,
                    Value::Array(Array::from(vec![Value::Uuid(lock_uuid(token))])),
                ),
                (DISPOSITION_STATUS, Value::String(String::from("abandoned"))),
                (PROPERTIES_TO_MODIFY, Value::Map(properties)),
            ]),
            ..Message::default()
        };
        let one = |value: Value| {
            let mut properties = OrderedMap::new();
            properties.insert(
                Value::String(String::from("stage")),
                Value::String(String::from("patched")),
            );
            properties.insert(Value::String(String::from("refused")), value);
            properties
        };

        for (refused, why) in [
            // Inside the size headroom, but larger than any filterable property
            // value may be: the broker's content checks refuse it.
            (
                Value::Binary(vec![0_u8; 300 * 1024].into()),
                "domain content refusal",
            ),
            // A shape the AMQP codec cannot encode as one received value.
            (
                Value::Array(Array::from(vec![Value::String(String::from("café"))])),
                "protocol patch refusal",
            ),
        ] {
            let response = update_disposition(
                &request(one(refused)),
                MessageId::Ulong(1),
                None,
                &namespace,
                &entity,
                &broker,
                &management,
            )
            .await;
            assert_eq!(
                response.status_code, 400,
                "{why}: {}",
                response.status_description
            );
            assert_eq!(
                response.error_condition,
                Some(crate::INVALID_FIELD),
                "{why}"
            );
            assert_eq!(peek(), before, "{why}: the copy and its lock are unchanged");
            assert!(
                management
                    .managed_delivery(&entity, Some(link), token)
                    .await
                    .is_some(),
                "{why}: the lock stays registered"
            );
        }

        // The lock is still live: an unpatched abandon settles it.
        let response = update_disposition(
            &request(OrderedMap::new()),
            MessageId::Ulong(2),
            None,
            &namespace,
            &entity,
            &broker,
            &management,
        )
        .await;
        assert_eq!(response.status_code, 200, "{}", response.status_description);
        let CommandOutcome::Received(Some(redelivered)) = submit(CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        }) else {
            panic!("the abandoned message comes round again")
        };
        assert_eq!(
            redelivered
                .envelope
                .map(|envelope| envelope.overlay().to_vec()),
            Some(Vec::new()),
            "no refused patch reached the copy"
        );
    }
}
