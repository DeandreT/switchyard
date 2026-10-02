use auth::{PermissionSet, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};

use super::*;
use crate::{Attachment, EntityMetadata, authorization::SharedAccessAuthentication};

mod action_bindings;

const ENTITY: &str = "Orders/subscriptions/Alpha";
const HOST: &str = "tenant.servicebus.windows.net";
const BUDGET: DeliveryBudget = DeliveryBudget {
    max_bytes: 4096,
    per_message_overhead_bytes: 64,
};

#[derive(Clone)]
struct NoOwnerWork;

impl Broker for NoOwnerWork {
    crate::broker::fixture_binding_methods!();

    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        panic!("provenance refusal must precede metadata reads")
    }

    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        panic!("provenance refusal must precede command submission")
    }

    async fn rules(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        panic!("provenance refusal must precede rule reads")
    }

    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        panic!("management must not wait for deliveries")
    }
}

fn identity(generation: u64) -> EntityBinding {
    let old = test_binding(&EntityPath::new(ENTITY).expect("entity"));
    EntityBinding::new(
        old.namespace().clone(),
        old.target().clone(),
        old.owner().clone(),
        old.kind(),
        generation,
    )
    .expect("identity")
}

fn request(operation: &str, fields: impl IntoIterator<Item = (&'static str, Value)>) -> Message {
    let mut properties = ApplicationProperties::default();
    properties.insert(OPERATION_PROPERTY, operation.to_owned());
    properties.insert(ASSOCIATED_LINK_NAME_PROPERTY, "owner");
    Message {
        application_properties: Some(properties),
        body: Body::Value(Value::Map(
            fields
                .into_iter()
                .map(|(key, value)| (Value::String(key.to_owned()), value))
                .collect(),
        )),
        ..Message::default()
    }
}

fn authorization(permissions: PermissionSet) -> ManagementAuthorization {
    let endpoint = format!("{ENTITY}/$management");
    let policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "manager",
        ResourceScope::entity(HOST, &endpoint).expect("scope"),
        SharedAccessKey::new("secret").expect("key"),
        None,
        permissions,
    )
    .expect("rule")])
    .expect("policy");
    let grant = policy
        .authenticate_plain("manager", "secret")
        .expect("grant");
    let connection = ConnectionAuthorization::new(
        SharedAccessAuthentication::new(policy, HOST).expect("authentication"),
        Some(grant),
    );
    ManagementAuthorization::new(
        connection,
        ResourceScope::entity(HOST, endpoint)
            .expect("endpoint")
            .into_amqp_scope(),
    )
}

async fn process(
    message: &Message,
    binding: EntityBinding,
    management: &ConnectionManagement,
) -> ManagementResponse {
    let broker = BoundBroker::new(NoOwnerWork, binding.clone());
    process_request(
        message,
        MessageId::Ulong(7),
        binding.namespace(),
        binding.target(),
        &broker,
        management,
        None,
        BUDGET,
    )
    .await
}

#[tokio::test]
async fn a_reply_route_requires_exact_target_namespace_kind_and_generation() {
    let current = identity(2);
    let queue = EntityPath::new("Orders").expect("queue");
    let mismatches = [
        identity(1),
        test_binding(&EntityPath::new("Orders/subscriptions/Beta").expect("sibling")),
        EntityBinding::new(
            NamespaceName::new("other").expect("namespace"),
            current.target().clone(),
            current.owner().clone(),
            current.kind(),
            2,
        )
        .expect("foreign"),
        EntityBinding::new(
            current.namespace().clone(),
            queue.clone(),
            queue.clone(),
            domain::EntityIncarnationKind::Queue,
            2,
        )
        .expect("queue"),
        EntityBinding::new(
            current.namespace().clone(),
            queue.clone(),
            queue,
            domain::EntityIncarnationKind::Topic,
            2,
        )
        .expect("topic"),
    ];
    let message = request(PEEK_MESSAGE_OPERATION, []);
    let management = ConnectionManagement::default();
    let (_, _receiver) = management
        .register_reply_route("exact".into(), None, current.clone())
        .await;
    let exact = management.reply_route("exact").await.expect("route");
    assert!(
        validate_reply_binding(&message, &current, &exact.binding, None)
            .await
            .is_ok()
    );
    for (index, mismatch) in mismatches.into_iter().enumerate() {
        let address = format!("old-{index}");
        let (_, mut receiver) = management
            .register_reply_route(address.clone(), None, mismatch)
            .await;
        let route = management.reply_route(&address).await.expect("route");
        let error = validate_reply_binding(&message, &current, &route.binding, None)
            .await
            .unwrap_err();
        assert_eq!(error.condition.as_symbol(), Symbol::from(crate::NOT_FOUND));
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}

#[tokio::test]
async fn denied_operation_permission_precedes_a_reply_identity_refusal() {
    let send = authorization(PermissionSet::SEND);
    let listen = authorization(PermissionSet::LISTEN);
    let current = identity(2);
    let old = identity(1);
    for (operation, grant, condition) in [
        (PEEK_MESSAGE_OPERATION, &send, "amqp:unauthorized-access"),
        (
            SCHEDULE_MESSAGE_OPERATION,
            &listen,
            "amqp:unauthorized-access",
        ),
        (PEEK_MESSAGE_OPERATION, &listen, crate::NOT_FOUND),
        (SCHEDULE_MESSAGE_OPERATION, &send, crate::NOT_FOUND),
    ] {
        let error = validate_reply_binding(&request(operation, []), &current, &old, Some(grant))
            .await
            .unwrap_err();
        assert_eq!(error.condition.as_symbol(), Symbol::from(condition));
    }
}

#[tokio::test]
async fn a_same_path_session_from_an_old_incarnation_cannot_renew_read_write_or_receive_deferred() {
    let entity = EntityPath::new(ENTITY).expect("entity");
    let hold = SessionHold::new(SessionId::new("A").expect("session"), LockToken::new(9));
    let management = ConnectionManagement::default();
    management
        .register_session("owner", entity.clone(), hold.clone(), identity(1))
        .await;
    for operation in [
        RENEW_SESSION_LOCK_OPERATION,
        GET_SESSION_STATE_OPERATION,
        SET_SESSION_STATE_OPERATION,
        RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
    ] {
        let message = request(
            operation,
            [
                (SESSION_ID, Value::String("A".into())),
                (SESSION_STATE, Value::Binary(Binary::from(vec![1]))),
                (
                    SEQUENCE_NUMBERS,
                    Value::Array(Array::from(vec![Value::Long(7)])),
                ),
                (RECEIVER_SETTLE_MODE, Value::Uint(1)),
            ],
        );
        let response = process(&message, identity(2), &management).await;
        assert_eq!(response.status_code, 410, "{operation}");
        assert_eq!(response.error_condition, Some(crate::SESSION_LOCK_LOST));
        assert_eq!(
            management.session("owner").await,
            Some(ManagedSession {
                entity: entity.clone(),
                hold: hold.clone(),
                binding: identity(1),
            })
        );
    }
}

#[tokio::test]
async fn same_path_and_session_id_are_accepted_only_with_an_exact_binding() {
    let entity = EntityPath::new(ENTITY).expect("entity");
    let hold = SessionHold::new(SessionId::new("A").expect("session"), LockToken::new(9));
    let management = ConnectionManagement::default();
    management
        .register_session("owner", entity.clone(), hold.clone(), identity(2))
        .await;
    let message = request(
        GET_SESSION_STATE_OPERATION,
        [(SESSION_ID, Value::String("A".into()))],
    );
    let session = requested_session(&message, &entity, &identity(2), &management)
        .await
        .expect("exact session");
    assert_eq!(session.hold, hold);
    assert!(matches!(
        requested_session(&message, &entity, &identity(1), &management).await,
        Err(SessionLookupError::LockLost(_))
    ));
}

#[tokio::test]
async fn same_path_old_delivery_receipts_cannot_renew_or_settle_and_remain_registered() {
    let entity = EntityPath::new(ENTITY).expect("entity");
    let token = LockToken::new(9);
    let management = ConnectionManagement::default();
    management
        .register_delivery(
            "owner",
            entity.clone(),
            SequenceNumber::new(7),
            token,
            identity(1),
        )
        .await;
    for operation in [RENEW_LOCK_OPERATION, UPDATE_DISPOSITION_OPERATION] {
        let message = request(
            operation,
            [
                (
                    LOCK_TOKENS,
                    Value::Array(Array::from(vec![Value::Uuid(lock_token_uuid(token))])),
                ),
                (DISPOSITION_STATUS, Value::String("completed".into())),
            ],
        );
        let response = process(&message, identity(2), &management).await;
        assert_eq!(response.status_code, 410, "{operation}");
        assert_eq!(response.error_condition, Some(crate::MESSAGE_LOCK_LOST));
        assert_eq!(
            management.delivery("owner", token).await,
            Some(ManagedDelivery {
                entity: entity.clone(),
                sequence: SequenceNumber::new(7),
                binding: identity(1),
            })
        );
    }
}

#[tokio::test]
async fn late_delivery_cleanup_cannot_remove_a_reused_name_and_token_on_another_binding() {
    let old = identity(1);
    for current in [
        identity(2),
        test_binding(&EntityPath::new("Orders/subscriptions/Beta").expect("sibling")),
    ] {
        let management = ConnectionManagement::default();
        let token = LockToken::new(9);
        management
            .register_delivery(
                "reused",
                old.target().clone(),
                SequenceNumber::new(7),
                token,
                old.clone(),
            )
            .await;
        management
            .register_delivery(
                "reused",
                current.target().clone(),
                SequenceNumber::new(8),
                token,
                current.clone(),
            )
            .await;
        management.unregister_delivery("reused", token, &old).await;
        assert_eq!(
            management.delivery("reused", token).await,
            Some(ManagedDelivery {
                entity: current.target().clone(),
                sequence: SequenceNumber::new(8),
                binding: current.clone(),
            })
        );
        management
            .unregister_delivery("reused", token, &current)
            .await;
        assert!(management.delivery("reused", token).await.is_none());
    }
}

#[tokio::test]
async fn late_session_cleanup_cannot_remove_a_reused_name_and_hold_on_another_binding() {
    let old = identity(1);
    let hold = SessionHold::new(SessionId::new("A").expect("session"), LockToken::new(9));
    for current in [
        identity(2),
        test_binding(&EntityPath::new("Orders/subscriptions/Beta").expect("sibling")),
    ] {
        let management = ConnectionManagement::default();
        management
            .register_session("reused", old.target().clone(), hold.clone(), old.clone())
            .await;
        management
            .register_session(
                "reused",
                current.target().clone(),
                hold.clone(),
                current.clone(),
            )
            .await;
        management.unregister_session("reused", &hold, &old).await;
        assert_eq!(
            management.session("reused").await,
            Some(ManagedSession {
                entity: current.target().clone(),
                hold: hold.clone(),
                binding: current.clone(),
            })
        );
        management
            .unregister_session("reused", &hold, &current)
            .await;
        assert!(management.session("reused").await.is_none());
    }
}
