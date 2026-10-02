use std::sync::{Arc, Mutex};

use auth::{PermissionSet, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::MessageValue;

use super::*;
use crate::{Attachment, EntityMetadata, authorization::SharedAccessAuthentication};

const ENTITY: &str = "Orders/subscriptions/Alpha";
const HOST: &str = "tenant.servicebus.windows.net";
const BUDGET: DeliveryBudget = DeliveryBudget {
    max_bytes: 64 * 1024,
    per_message_overhead_bytes: 64,
};

type Submission = (NamespaceName, EntityPath, CommandKind);
type Read = (NamespaceName, EntityPath, SubscriptionName);

#[derive(Clone, Default)]
struct ObservedBroker {
    submissions: Arc<Mutex<Vec<Submission>>>,
    reads: Arc<Mutex<Vec<Read>>>,
    definitions: Arc<Mutex<Vec<RuleDefinition>>>,
    rejection: Option<BrokerRejection>,
}

impl Broker for ObservedBroker {
    crate::broker::fixture_binding_methods!();

    async fn rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        self.reads
            .lock()
            .expect("rule reads")
            .push((namespace, topic, subscription));
        if let Some(error) = &self.rejection {
            return Err(error.clone());
        }
        Ok(self.definitions.lock().expect("rules").clone())
    }

    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        panic!("rule operation must not query link metadata again")
    }

    async fn submit(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let outcome = match &kind {
            CommandKind::CreateRule { .. } | CommandKind::CreateRuleWithAction { .. } => {
                CommandOutcome::RuleCreated
            }
            CommandKind::DeleteRule { .. } => CommandOutcome::RuleDeleted,
            _ => panic!("unexpected rule command: {kind:?}"),
        };
        self.submissions
            .lock()
            .expect("rule submissions")
            .push((namespace, topic, kind));
        if let Some(error) = &self.rejection {
            return Err(error.clone());
        }
        Ok(outcome)
    }

    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        panic!("rule operations must not wait for deliveries")
    }
}

fn map(entries: impl IntoIterator<Item = (&'static str, Value)>) -> OrderedMap<Value, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (Value::String(key.into()), value))
        .collect()
}

fn request(operation: &str, body: OrderedMap<Value, Value>) -> Message {
    let mut properties = ApplicationProperties::default();
    properties.insert(OPERATION_PROPERTY, operation.to_owned());
    properties.insert(TRACKING_ID_PROPERTY, "rule-trace");
    Message {
        application_properties: Some(properties),
        body: Body::Value(Value::Map(body)),
        ..Message::default()
    }
}

fn add(name: &str, filter_name: &'static str, filter: Value) -> Message {
    request(
        ADD_RULE_OPERATION,
        map([
            (RULE_NAME, Value::String(name.into())),
            (
                RULE_DESCRIPTION,
                Value::Map(map([
                    (RULE_NAME, Value::String(name.into())),
                    (filter_name, filter),
                    (SQL_ACTION, Value::Null),
                ])),
            ),
        ]),
    )
}

fn sql(name: &str, expression: &str) -> Message {
    add(
        name,
        SQL_FILTER,
        Value::Map(map([(EXPRESSION, Value::String(expression.into()))])),
    )
}

fn enumeration(top: Value, skip: Value) -> Message {
    request(
        ENUMERATE_RULES_OPERATION,
        map([("top", top), ("skip", skip)]),
    )
}

async fn process_request_for(
    message: &Message,
    entity: &str,
    broker: &ObservedBroker,
    authorization: Option<&ManagementAuthorization>,
    budget: DeliveryBudget,
) -> ManagementResponse {
    let bound = BoundBroker::new(
        broker.clone(),
        test_binding(&EntityPath::new(ENTITY).expect("bound entity")),
    );
    process_request(
        message,
        MessageId::Ulong(7),
        &NamespaceName::new("tenant").expect("namespace"),
        &EntityPath::new(entity).expect("entity"),
        &bound,
        &ConnectionManagement::default(),
        authorization,
        budget,
    )
    .await
}

fn definition(name: &str, filter: RuleFilter) -> RuleDefinition {
    RuleDefinition {
        name: RuleName::new(name).expect("name"),
        filter,
        created_at: Timestamp::from_millis(1_000),
        action: None,
    }
}

#[tokio::test]
async fn action_metadata_is_represented_without_changing_plain_or_empty_pages() {
    let broker = ObservedBroker::default();
    let plain = definition("plain", RuleFilter::True);
    let mut action = definition("annotated", RuleFilter::True);
    action.action = Some(domain::SqlAction::new("REMOVE [private-action]").expect("action"));
    *broker.definitions.lock().expect("rules") = vec![action, plain];
    for skip in [0, 1, 2] {
        let response = process_request_for(
            &enumeration(Value::Int(1), Value::Int(skip)),
            ENTITY,
            &broker,
            None,
            BUDGET,
        )
        .await;
        assert_eq!(response.status_code, 200);
        assert_eq!(response.error_condition, None);
        let Value::Map(body) = &response.body else {
            panic!("rule body")
        };
        let Some(Value::List(entries)) = get(body, RULES) else {
            panic!("rule entries")
        };
        if skip == 2 {
            assert!(entries.is_empty());
            continue;
        }
        let [Value::Map(entry)] = entries.as_slice() else {
            panic!("one rule")
        };
        let rule = fields(
            get(entry, RULE_DESCRIPTION).expect("description"),
            RULE_DESCRIPTION_CODE,
        );
        if skip == 0 {
            assert_eq!(
                fields(&rule[1], SQL_ACTION_CODE),
                [
                    Value::String("REMOVE [private-action]".into()),
                    Value::Int(20)
                ]
            );
        } else {
            assert!(fields(&rule[1], EMPTY_ACTION_CODE).is_empty());
        }
    }
    assert!(broker.submissions.lock().expect("commands").is_empty());
    assert_eq!(broker.reads.lock().expect("reads").len(), 3);
}

fn authorization(permissions: PermissionSet, granted: &str) -> ManagementAuthorization {
    let policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "rule-manager",
        ResourceScope::entity(HOST, granted).expect("scope"),
        SharedAccessKey::new("secret").expect("key"),
        None,
        permissions,
    )
    .expect("rule")])
    .expect("policy");
    let grant = policy
        .authenticate_plain("rule-manager", "secret")
        .expect("grant");
    let connection = ConnectionAuthorization::new(
        SharedAccessAuthentication::new(policy, HOST).expect("authentication"),
        Some(grant),
    );
    ManagementAuthorization::new(
        connection,
        ResourceScope::entity(HOST, format!("{ENTITY}/$management"))
            .expect("scope")
            .into_amqp_scope(),
    )
}

#[tokio::test]
async fn exact_true_false_aliases_submit_typed_parent_commands() {
    for (expression, filter) in [("1=1", RuleFilter::True), ("1=0", RuleFilter::False)] {
        let broker = ObservedBroker::default();
        let response =
            process_request_for(&sql("match", expression), ENTITY, &broker, None, BUDGET).await;
        assert_eq!(response.status_code, 200);
        assert_eq!(response.correlation_id, MessageId::Ulong(7));
        assert_eq!(response.tracking_id.as_deref(), Some("rule-trace"));
        assert_eq!(
            *broker.submissions.lock().expect("commands"),
            [(
                NamespaceName::new("tenant").expect("namespace"),
                EntityPath::new("Orders").expect("topic"),
                CommandKind::CreateRule {
                    subscription: SubscriptionName::new("Alpha").expect("subscription"),
                    name: RuleName::new("match").expect("rule"),
                    filter
                }
            )]
        );
        assert!(broker.reads.lock().expect("reads").is_empty());
    }
}

#[tokio::test]
async fn sdk_correlation_null_system_fields_and_scalar_properties_keep_their_types() {
    let mut fields = map(SYSTEM_FIELDS.into_iter().map(|field| (field, Value::Null)));
    fields.insert(Value::String("label".into()), Value::String("Order".into()));
    fields.insert(
        Value::String(FILTER_PROPERTIES.into()),
        Value::Map(map([
            ("nullable", Value::Null),
            ("int", Value::Int(1)),
            ("long", Value::Long(1)),
            ("bool", Value::Bool(true)),
            ("binary", Value::Binary(vec![1, 2].into())),
        ])),
    );
    let broker = ObservedBroker::default();
    let response = process_request_for(
        &add("typed", CORRELATION_FILTER, Value::Map(fields)),
        ENTITY,
        &broker,
        None,
        BUDGET,
    )
    .await;
    assert_eq!(response.status_code, 200);
    let submissions = broker.submissions.lock().expect("commands");
    let CommandKind::CreateRule {
        filter: RuleFilter::Correlation(filter),
        ..
    } = &submissions[0].2
    else {
        panic!("correlation")
    };
    assert_eq!(filter.subject.as_deref(), Some("Order"));
    assert_eq!(filter.correlation_id, None);
    assert_eq!(filter.properties["nullable"], MessageValue::Null);
    assert_eq!(filter.properties["int"], MessageValue::Int(1));
    assert_eq!(filter.properties["long"], MessageValue::Long(1));
    assert_eq!(
        filter.properties["binary"],
        MessageValue::Binary(vec![1, 2])
    );
}

#[test]
fn empty_correlation_is_admitted_without_inventing_a_missing_condition() {
    assert_eq!(
        create_rule(&add(
            "empty",
            CORRELATION_FILTER,
            Value::Map(map([(FILTER_PROPERTIES, Value::Map(OrderedMap::new()))]))
        ))
        .expect("empty correlation")
        .1,
        RuleFilter::Correlation(CorrelationFilter::default())
    );
}

#[tokio::test]
async fn malformed_and_unsupported_inputs_never_reach_the_owner() {
    let mut mismatch = sql("outer", "1=1");
    let Body::Value(Value::Map(body)) = &mut mismatch.body else {
        unreachable!()
    };
    let Value::Map(description) = body
        .get_mut(&Value::String(RULE_DESCRIPTION.into()))
        .expect("description")
    else {
        unreachable!()
    };
    description.insert(
        Value::String(RULE_NAME.into()),
        Value::String("inner".into()),
    );
    let mut action = sql("action", "1=1");
    let Body::Value(Value::Map(body)) = &mut action.body else {
        unreachable!()
    };
    let Value::Map(description) = body
        .get_mut(&Value::String(RULE_DESCRIPTION.into()))
        .expect("description")
    else {
        unreachable!()
    };
    description.insert(
        Value::String(SQL_ACTION.into()),
        Value::Map(map([(
            EXPRESSION,
            Value::String("SET colour = 'red'".into()),
        )])),
    );
    let cases = [
        (sql("invalid/name", "1=1"), 400),
        (mismatch, 400),
        (sql("sql", "newid()=NULL"), 501),
        (action, 501),
        (
            add(
                "bad-system",
                CORRELATION_FILTER,
                Value::Map(map([("label", Value::Int(1))])),
            ),
            400,
        ),
        (
            add(
                "compound",
                CORRELATION_FILTER,
                Value::Map(map([(
                    FILTER_PROPERTIES,
                    Value::Map(map([("list", Value::List(vec![]))])),
                )])),
            ),
            501,
        ),
        (enumeration(Value::Uint(100), Value::Int(0)), 400),
        (enumeration(Value::Int(101), Value::Int(0)), 400),
        (enumeration(Value::Int(0), Value::Int(0)), 400),
        (enumeration(Value::Int(100), Value::Int(-1)), 400),
    ];
    for (message, status) in cases {
        let broker = ObservedBroker::default();
        let response = process_request_for(&message, ENTITY, &broker, None, BUDGET).await;
        assert_eq!(response.status_code, status, "{message:?}");
        assert_eq!(
            response.error_condition,
            Some(if status == 501 {
                crate::NOT_IMPLEMENTED
            } else {
                crate::INVALID_FIELD
            })
        );
        assert!(broker.submissions.lock().expect("commands").is_empty());
        assert!(broker.reads.lock().expect("reads").is_empty());
    }
}

mod actions;
mod sql;

#[tokio::test]
async fn wrong_targets_are_refused_before_mutation_or_rule_reads() {
    for entity in [
        "Orders",
        "queue",
        "Orders/subscriptions/Alpha/$deadletterqueue",
    ] {
        for message in [
            sql("match", "1=1"),
            enumeration(Value::Int(100), Value::Int(0)),
        ] {
            let broker = ObservedBroker::default();
            let response = process_request_for(&message, entity, &broker, None, BUDGET).await;
            assert_eq!(response.status_code, 400);
            assert_eq!(response.error_condition, Some(crate::INVALID_FIELD));
            assert!(broker.submissions.lock().expect("commands").is_empty());
            assert!(broker.reads.lock().expect("reads").is_empty());
        }
    }
}

#[tokio::test]
async fn listen_is_required_before_parsing_or_owner_reads() {
    for (permissions, scope, expected) in [
        (PermissionSet::LISTEN, format!("{ENTITY}/$management"), 200),
        (PermissionSet::SEND, format!("{ENTITY}/$management"), 401),
        (
            PermissionSet::LISTEN,
            "Orders/subscriptions/beta/$management".to_owned(),
            401,
        ),
        (PermissionSet::LISTEN, "Orders/$management".to_owned(), 401),
    ] {
        let authorization = authorization(permissions, &scope);
        for message in [
            sql("match", "1=1"),
            enumeration(Value::Int(100), Value::Int(0)),
        ] {
            let broker = ObservedBroker::default();
            let response =
                process_request_for(&message, ENTITY, &broker, Some(&authorization), BUDGET).await;
            assert_eq!(response.status_code, expected);
            if expected == 401 {
                assert!(broker.submissions.lock().expect("commands").is_empty());
                assert!(broker.reads.lock().expect("reads").is_empty());
            }
        }
    }
}

#[tokio::test]
async fn a_foreign_associated_link_name_cannot_redirect_a_rule_operation() {
    let mut message = sql("target", "1=1");
    message
        .application_properties
        .as_mut()
        .expect("properties")
        .insert(ASSOCIATED_LINK_NAME_PROPERTY, "valid-beta-receiver");
    let management = ConnectionManagement::default();
    management
        .register_session(
            "valid-beta-receiver",
            EntityPath::new("Orders/subscriptions/beta").expect("sibling"),
            SessionHold {
                session_id: SessionId::new("A").expect("session"),
                token: LockToken::new(9),
            },
            test_binding(&EntityPath::new("Orders/subscriptions/beta").expect("sibling")),
        )
        .await;
    let broker = ObservedBroker::default();
    let bound = BoundBroker::new(
        broker.clone(),
        test_binding(&EntityPath::new(ENTITY).expect("bound entity")),
    );
    let authorization = authorization(PermissionSet::LISTEN, &format!("{ENTITY}/$management"));
    let response = process_request(
        &message,
        MessageId::Ulong(7),
        &NamespaceName::new("tenant").expect("namespace"),
        &EntityPath::new(ENTITY).expect("entity"),
        &bound,
        &management,
        Some(&authorization),
        BUDGET,
    )
    .await;
    assert_eq!(response.status_code, 200);
    assert!(
        matches!(&broker.submissions.lock().expect("commands")[0].2, CommandKind::CreateRule { subscription, .. } if subscription.as_str() == "Alpha")
    );
    assert_eq!(
        management
            .session("valid-beta-receiver")
            .await
            .expect("sibling hold")
            .entity
            .as_str(),
        "Orders/subscriptions/beta"
    );
}

#[tokio::test]
async fn impossible_variable_payloads_and_combined_condition_counts_refuse_before_submission() {
    let too_large = add(
        "large",
        CORRELATION_FILTER,
        Value::Map(map([(
            FILTER_PROPERTIES,
            Value::Map(map([(
                "binary",
                Value::Binary(vec![0; domain::MAX_RULE_BYTES + 1].into()),
            )])),
        )])),
    );
    let mut too_many = map([(
        FILTER_PROPERTIES,
        Value::Map(
            (0..domain::MAX_CORRELATION_RULE_CONDITIONS)
                .map(|index| (Value::String(format!("key-{index}")), Value::Null))
                .collect(),
        ),
    )]);
    too_many.insert(Value::String("label".into()), Value::String("extra".into()));
    for (message, status, condition) in [
        (too_large, 403, crate::MESSAGE_SIZE_EXCEEDED),
        (
            add("many", CORRELATION_FILTER, Value::Map(too_many)),
            400,
            crate::INVALID_FIELD,
        ),
    ] {
        let broker = ObservedBroker::default();
        let response = process_request_for(&message, ENTITY, &broker, None, BUDGET).await;
        assert_eq!(response.status_code, status);
        assert_eq!(response.error_condition, Some(condition));
        assert!(broker.submissions.lock().expect("commands").is_empty());
        assert!(broker.reads.lock().expect("reads").is_empty());
    }
}

#[tokio::test]
async fn string_symbol_alias_fields_are_not_silently_selected() {
    let mut message = sql("named", "1=1");
    let Body::Value(Value::Map(body)) = &mut message.body else {
        unreachable!()
    };
    body.insert(
        Value::Symbol(Symbol::from(RULE_NAME)),
        Value::String("other".into()),
    );
    let broker = ObservedBroker::default();
    let response = process_request_for(&message, ENTITY, &broker, None, BUDGET).await;
    assert_eq!(response.status_code, 400);
    assert_eq!(response.error_condition, Some(crate::INVALID_FIELD));
    assert!(broker.submissions.lock().expect("commands").is_empty());
    assert!(broker.reads.lock().expect("reads").is_empty());
}

#[tokio::test]
async fn unrepresentable_committed_timestamps_are_an_internal_wire_failure() {
    let broker = ObservedBroker::default();
    let mut rule = definition("future", RuleFilter::True);
    rule.created_at = Timestamp::from_millis(u64::MAX);
    *broker.definitions.lock().expect("rules") = vec![rule];
    let response = process_request_for(
        &enumeration(Value::Int(100), Value::Int(0)),
        ENTITY,
        &broker,
        None,
        BUDGET,
    )
    .await;
    assert_eq!(response.status_code, 500);
    assert_eq!(response.error_condition, Some(crate::INTERNAL_ERROR));
    assert!(broker.submissions.lock().expect("commands").is_empty());
}

#[tokio::test]
async fn removal_and_domain_failures_keep_sdk_status_conditions() {
    let message = request(
        REMOVE_RULE_OPERATION,
        map([(RULE_NAME, Value::String("$Default".into()))]),
    );
    let broker = ObservedBroker::default();
    assert_eq!(
        process_request_for(&message, ENTITY, &broker, None, BUDGET)
            .await
            .status_code,
        200
    );
    assert!(
        matches!(&broker.submissions.lock().expect("commands")[0].2, CommandKind::DeleteRule { subscription, name } if subscription.as_str() == "Alpha" && name.as_str() == "$Default")
    );
    for (error, status, condition) in [
        (
            BrokerError::RuleAlreadyExists,
            409,
            crate::ENTITY_ALREADY_EXISTS,
        ),
        (BrokerError::RuleNotFound, 404, crate::NOT_FOUND),
        (BrokerError::SubscriptionNotFound, 404, crate::NOT_FOUND),
        (
            BrokerError::RuleLimitExceeded { maximum: 32 },
            403,
            crate::RESOURCE_LIMIT_EXCEEDED,
        ),
        (
            BrokerError::RuleTooLarge {
                maximum_bytes: 64 * 1024,
            },
            403,
            crate::MESSAGE_SIZE_EXCEEDED,
        ),
        (
            BrokerError::DanglingRuleMetadata,
            500,
            crate::INTERNAL_ERROR,
        ),
    ] {
        let broker = ObservedBroker {
            rejection: Some(BrokerRejection::Refused(error)),
            ..ObservedBroker::default()
        };
        let response = process_request_for(&message, ENTITY, &broker, None, BUDGET).await;
        assert_eq!(response.status_code, status);
        assert_eq!(response.error_condition, Some(condition));
    }
}

fn fields(value: &Value, code: u64) -> &[Value] {
    let Value::Described(description) = value else {
        panic!("described list")
    };
    assert_eq!(description.descriptor, Descriptor::Code(code));
    let Value::List(fields) = &description.value else {
        panic!("list")
    };
    fields
}

#[tokio::test]
async fn enumeration_is_clock_free_and_uses_exact_pinned_described_lists() {
    let correlation = CorrelationFilter {
        subject: Some("Order".into()),
        properties: BTreeMap::from([
            ("nullable".into(), MessageValue::Null),
            ("number".into(), MessageValue::Long(7)),
        ]),
        ..CorrelationFilter::default()
    };
    let broker = ObservedBroker::default();
    *broker.definitions.lock().expect("rules") = vec![
        definition("a", RuleFilter::True),
        definition("b", RuleFilter::False),
        definition("c", RuleFilter::Correlation(correlation)),
    ];
    let response = process_request_for(
        &enumeration(Value::Int(100), Value::Int(0)),
        ENTITY,
        &broker,
        None,
        BUDGET,
    )
    .await;
    assert_eq!(response.status_code, 200);
    assert!(broker.submissions.lock().expect("commands").is_empty());
    assert_eq!(
        *broker.reads.lock().expect("reads"),
        [(
            NamespaceName::new("tenant").expect("namespace"),
            EntityPath::new("Orders").expect("topic"),
            SubscriptionName::new("Alpha").expect("subscription")
        )]
    );
    let Value::Map(body) = &response.body else {
        panic!("map")
    };
    let Some(Value::List(entries)) = get(body, RULES) else {
        panic!("rules")
    };
    assert_eq!(entries.len(), 3);
    for (index, (entry, filter_code)) in entries
        .iter()
        .zip([0x000001370000007, 0x000001370000008, 0x000001370000009])
        .enumerate()
    {
        let Value::Map(entry) = entry else {
            panic!("entry")
        };
        let rule = fields(
            get(entry, RULE_DESCRIPTION).expect("rule"),
            0x0000013700000004,
        );
        assert_eq!(rule.len(), 4);
        let filter = fields(&rule[0], filter_code);
        assert_eq!(fields(&rule[1], 0x0000013700000005), []);
        assert_eq!(rule[2], Value::String(["a", "b", "c"][index].into()));
        assert_eq!(rule[3], Value::Timestamp(1_000.into()));
        if index == 2 {
            assert_eq!(filter.len(), 9);
            assert_eq!(filter[4], Value::String("Order".into()));
            assert_eq!(filter[0], Value::Null);
            assert_eq!(
                filter[8],
                Value::Map(map([("nullable", Value::Null), ("number", Value::Long(7))]))
            );
        } else {
            assert!(filter.is_empty());
        }
    }
    let encoded = encode_message(&response.into_message()).expect("encode");
    let decoded = amqp::decode_message(&encoded).expect("decode described lists");
    assert!(matches!(decoded.body, Body::Value(Value::Map(_))));
}

#[tokio::test]
async fn complete_requested_page_is_refused_not_truncated_when_reply_is_small() {
    let broker = ObservedBroker::default();
    *broker.definitions.lock().expect("rules") = vec![
        definition("a", RuleFilter::True),
        definition("b", RuleFilter::False),
    ];
    let response = process_request_for(
        &enumeration(Value::Int(100), Value::Int(0)),
        ENTITY,
        &broker,
        None,
        DeliveryBudget {
            max_bytes: 1,
            ..BUDGET
        },
    )
    .await;
    assert_eq!(response.status_code, 403);
    assert_eq!(response.error_condition, Some(crate::MESSAGE_SIZE_EXCEEDED));
    assert_eq!(response.body, Value::Null);
    let page = process_request_for(
        &enumeration(Value::Int(1), Value::Int(1)),
        ENTITY,
        &broker,
        None,
        BUDGET,
    )
    .await;
    let Value::Map(body) = page.body else {
        panic!("map")
    };
    let Some(Value::List(entries)) = get(&body, RULES) else {
        panic!("rules")
    };
    assert_eq!(entries.len(), 1);
    let page = process_request_for(
        &enumeration(Value::Int(100), Value::Int(i32::MAX)),
        ENTITY,
        &broker,
        None,
        BUDGET,
    )
    .await;
    assert_eq!(page.status_code, 200);
    assert_eq!(page.body, map_body(RULES, Value::List(vec![])));
}
