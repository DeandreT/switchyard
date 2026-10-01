use std::sync::{Arc, Mutex};

use auth::{PermissionSet, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::MAX_SESSION_ID_BYTES;

use super::*;
use crate::{Attachment, EntityMetadata, authorization::SharedAccessAuthentication};

const HOST: &str = "tenant.servicebus.windows.net";
const ENTITY: &str = "Orders/subscriptions/Alpha";
const BUDGET: DeliveryBudget = DeliveryBudget {
    max_bytes: 4_096,
    per_message_overhead_bytes: 64,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Submission {
    namespace: NamespaceName,
    entity: EntityPath,
    kind: CommandKind,
}

#[derive(Clone, Default)]
struct ObservedBroker {
    submissions: Arc<Mutex<Vec<Submission>>>,
}

impl ObservedBroker {
    fn submissions(&self) -> Vec<Submission> {
        self.submissions.lock().expect("observation lock").clone()
    }
}

impl Broker for ObservedBroker {
    crate::broker::fixture_binding_methods!();

    async fn rules(
        &self,
        _namespace: NamespaceName,
        _topic: EntityPath,
        _subscription: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        Err(BrokerRejection::Unavailable("unexpected rule read".into()))
    }

    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        panic!("management peek must not query link metadata")
    }

    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        assert!(matches!(kind, CommandKind::PeekBounded { .. }));
        self.submissions
            .lock()
            .expect("observation lock")
            .push(Submission {
                namespace,
                entity,
                kind,
            });
        Ok(CommandOutcome::Peeked(Vec::new()))
    }

    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        panic!("management peek must not wait for deliveries")
    }
}

fn request(session_id: Option<Value>) -> Message {
    let mut body = OrderedMap::new();
    body.insert(
        Value::String(FROM_SEQUENCE_NUMBER.to_owned()),
        Value::Ulong(3),
    );
    body.insert(Value::String(MESSAGE_COUNT.to_owned()), Value::Uint(2));
    if let Some(session_id) = session_id {
        body.insert(Value::String(SESSION_ID.to_owned()), session_id);
    }
    let mut properties = ApplicationProperties::default();
    properties.insert(
        OPERATION_PROPERTY.to_owned(),
        Value::String(PEEK_MESSAGE_OPERATION.to_owned()),
    );
    properties.insert(
        TRACKING_ID_PROPERTY.to_owned(),
        Value::String("peek-trace".to_owned()),
    );
    Message {
        application_properties: Some(properties),
        body: Body::Value(Value::Map(body)),
        ..Message::default()
    }
}

async fn process(
    message: &Message,
    broker: &ObservedBroker,
    management: &ConnectionManagement,
    authorization: Option<&ManagementAuthorization>,
) -> ManagementResponse {
    let entity = EntityPath::new(ENTITY).expect("entity");
    let bound = BoundBroker::new(broker.clone(), test_binding(&entity));
    process_request(
        message,
        MessageId::Ulong(7),
        &NamespaceName::new("tenant").expect("namespace"),
        &entity,
        &bound,
        management,
        authorization,
        BUDGET,
    )
    .await
}

fn expected_submission(session_id: Option<&str>) -> Submission {
    Submission {
        namespace: NamespaceName::new("tenant").expect("namespace"),
        entity: EntityPath::new(ENTITY).expect("entity"),
        kind: CommandKind::PeekBounded {
            from_sequence: SequenceNumber::new(3),
            max_messages: 2,
            session_id: session_id.map(|id| SessionId::new(id).expect("session identifier")),
            budget: BUDGET,
        },
    }
}

fn authorization(permissions: PermissionSet, granted_entity: &str) -> ManagementAuthorization {
    let granted_resource = ResourceScope::entity(HOST, granted_entity).expect("granted resource");
    let policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "peek-rule",
        granted_resource,
        SharedAccessKey::new("peek-secret").expect("key"),
        None,
        permissions,
    )
    .expect("rule")])
    .expect("policy");
    let grant = policy
        .authenticate_plain("peek-rule", "peek-secret")
        .expect("grant");
    let connection = ConnectionAuthorization::new(
        SharedAccessAuthentication::new(policy, HOST).expect("authentication"),
        Some(grant),
    );
    ManagementAuthorization::new(
        connection,
        ResourceScope::entity(HOST, format!("{ENTITY}/$management"))
            .expect("requested resource")
            .into_amqp_scope(),
    )
}

#[tokio::test]
async fn absent_or_string_session_identifier_submits_without_an_associated_link_or_hold() {
    for session_id in [None, Some("Session-A")] {
        let broker = ObservedBroker::default();
        let management = ConnectionManagement::new();
        let message = request(session_id.map(|id| Value::String(id.to_owned())));
        let response = process(&message, &broker, &management, None).await;

        assert_eq!(response.status_code, 200);
        assert_eq!(response.correlation_id, MessageId::Ulong(7));
        assert_eq!(response.tracking_id.as_deref(), Some("peek-trace"));
        assert_eq!(response.body, map_body(MESSAGES, Value::List(Vec::new())));
        assert_eq!(broker.submissions(), [expected_submission(session_id)]);
        assert!(management.sessions.read().await.is_empty());
        assert!(management.deliveries.read().await.is_empty());
    }
}

#[tokio::test]
async fn present_nonstring_session_identifier_is_bad_request_before_submission() {
    let malformed = [
        Value::Null,
        Value::Int(1),
        Value::Symbol(Symbol::from("Session-A")),
        Value::List(vec![Value::String("Session-A".to_owned())]),
        Value::Map(OrderedMap::new()),
    ];
    for value in malformed {
        let broker = ObservedBroker::default();
        let management = ConnectionManagement::new();
        let response = process(&request(Some(value.clone())), &broker, &management, None).await;

        assert_eq!(response.status_code, 400, "{value:?}");
        assert!(response.status_description.contains("session-id"));
        assert_eq!(response.correlation_id, MessageId::Ulong(7));
        assert_eq!(response.tracking_id.as_deref(), Some("peek-trace"));
        assert_eq!(response.body, Value::Null);
        assert!(broker.submissions().is_empty());
        assert!(management.sessions.read().await.is_empty());
        assert!(management.deliveries.read().await.is_empty());
    }
}

#[tokio::test]
async fn invalid_string_session_identifier_is_bad_request_before_submission() {
    for value in [
        String::new(),
        "Session\0A".to_owned(),
        "S".repeat(MAX_SESSION_ID_BYTES + 1),
    ] {
        let broker = ObservedBroker::default();
        let management = ConnectionManagement::new();
        let response = process(
            &request(Some(Value::String(value))),
            &broker,
            &management,
            None,
        )
        .await;

        assert_eq!(response.status_code, 400);
        assert!(
            response
                .status_description
                .contains("session-id is invalid")
        );
        assert_eq!(response.correlation_id, MessageId::Ulong(7));
        assert_eq!(response.tracking_id.as_deref(), Some("peek-trace"));
        assert!(broker.submissions().is_empty());
    }
}

#[tokio::test]
async fn peek_requires_listen_on_the_bound_management_resource_before_body_parsing() {
    let endpoint = format!("{ENTITY}/$management");
    let listen = authorization(PermissionSet::LISTEN, &endpoint);
    let broker = ObservedBroker::default();
    let management = ConnectionManagement::new();
    let response = process(&request(None), &broker, &management, Some(&listen)).await;
    assert_eq!(response.status_code, 200);
    assert_eq!(broker.submissions(), [expected_submission(None)]);

    for denied in [
        authorization(PermissionSet::SEND, &endpoint),
        authorization(
            PermissionSet::LISTEN,
            "Orders/subscriptions/Beta/$management",
        ),
    ] {
        for session_id in [None, Some(Value::Null)] {
            let broker = ObservedBroker::default();
            let response = process(&request(session_id), &broker, &management, Some(&denied)).await;
            assert_eq!(response.status_code, 401);
            assert!(broker.submissions().is_empty());
        }
    }
}

#[tokio::test]
async fn a_foreign_associated_link_neither_redirects_nor_locks_a_peek() {
    let management = ConnectionManagement::new();
    let foreign_entity = EntityPath::new("Orders/subscriptions/Beta").expect("foreign entity");
    let hold = SessionHold::new(
        SessionId::new("Foreign-Session").expect("foreign session"),
        LockToken::new(99),
    );
    let foreign_binding = test_binding(&foreign_entity);
    management
        .register_session(
            "foreign-link",
            foreign_entity.clone(),
            hold.clone(),
            foreign_binding.clone(),
        )
        .await;
    management
        .register_delivery(
            "foreign-link",
            foreign_entity.clone(),
            SequenceNumber::new(77),
            LockToken::new(99),
            foreign_binding.clone(),
        )
        .await;

    for session_id in [None, Some("Session-A")] {
        let broker = ObservedBroker::default();
        let mut message = request(session_id.map(|id| Value::String(id.to_owned())));
        message
            .application_properties
            .as_mut()
            .expect("application properties")
            .insert(
                ASSOCIATED_LINK_NAME_PROPERTY.to_owned(),
                Value::String("foreign-link".to_owned()),
            );
        let response = process(&message, &broker, &management, None).await;

        assert_eq!(response.status_code, 200);
        assert_eq!(broker.submissions(), [expected_submission(session_id)]);
        assert_eq!(
            management.session("foreign-link").await,
            Some(ManagedSession {
                entity: foreign_entity.clone(),
                hold: hold.clone(),
                binding: foreign_binding.clone(),
            })
        );
        assert_eq!(
            management
                .delivery("foreign-link", LockToken::new(99))
                .await,
            Some(ManagedDelivery {
                entity: foreign_entity.clone(),
                sequence: SequenceNumber::new(77),
                binding: foreign_binding.clone(),
            })
        );
    }
}
