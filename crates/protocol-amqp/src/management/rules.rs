//! Service Bus subscription rule management over the entity management node.

use std::collections::BTreeMap;

use amqp::{Body, Message, MessageId};
use domain::{
    CommandKind, CommandOutcome, CorrelationFilter, EntityPath, NamespaceName, RuleDefinition,
    RuleFilter, RuleName,
};
use serde_amqp::{Value, described::Described, descriptor::Descriptor, primitives::OrderedMap};

use crate::{Broker, message::correlation_value};

use super::{ManagementResponse, map_body, map_value, timestamp_value};

pub const ADD_RULE_OPERATION: &str = "com.microsoft:add-rule";
pub const REMOVE_RULE_OPERATION: &str = "com.microsoft:remove-rule";
pub const ENUMERATE_RULES_OPERATION: &str = "com.microsoft:enumerate-rules";

const RULE_NAME: &str = "rule-name";
const RULE_DESCRIPTION: &str = "rule-description";
const RULES: &str = "rules";
const SQL_FILTER: &str = "sql-filter";
const CORRELATION_FILTER: &str = "correlation-filter";
const SQL_RULE_ACTION: &str = "sql-rule-action";
const EXPRESSION: &str = "expression";
const CORRELATION_ID: &str = "correlation-id";
const MESSAGE_ID: &str = "message-id";
const TO: &str = "to";
const REPLY_TO: &str = "reply-to";
const LABEL: &str = "label";
const SESSION_ID: &str = "session-id";
const REPLY_TO_SESSION_ID: &str = "reply-to-session-id";
const CONTENT_TYPE: &str = "content-type";
const PROPERTIES: &str = "properties";
const SKIP: &str = "skip";
const TOP: &str = "top";

const RULE_DESCRIPTION_CODE: u64 = 0x0000_0137_0000_0004;
const EMPTY_RULE_ACTION_CODE: u64 = 0x0000_0137_0000_0005;
const TRUE_FILTER_CODE: u64 = 0x0000_0013_7000_0007;
const FALSE_FILTER_CODE: u64 = 0x0000_0013_7000_0008;
const CORRELATION_FILTER_CODE: u64 = 0x0000_0013_7000_0009;
const EMPTY_RULE_ACTION_NAME: &str = "com.microsoft:empty-rule-action:list";

pub(super) async fn add<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
) -> ManagementResponse {
    let (name, filter) = match add_request(&message.body) {
        Ok(request) => request,
        Err(description) => {
            return ManagementResponse::bad_request(message_id, tracking_id, description);
        }
    };
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::CreateRule { name, filter },
        )
        .await
    {
        Ok(CommandOutcome::RuleCreated) => {
            ManagementResponse::accepted(message_id, tracking_id, Value::Null)
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("adding a rule produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

pub(super) async fn remove<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
) -> ManagementResponse {
    let name = match rule_name(&message.body) {
        Ok(name) => name,
        Err(description) => {
            return ManagementResponse::bad_request(message_id, tracking_id, description);
        }
    };
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::DeleteRule { name },
        )
        .await
    {
        Ok(CommandOutcome::RuleDeleted) => {
            ManagementResponse::accepted(message_id, tracking_id, Value::Null)
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("removing a rule produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

pub(super) async fn enumerate<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
) -> ManagementResponse {
    let (skip, max_rules) = match page_request(&message.body) {
        Ok(page) => page,
        Err(description) => {
            return ManagementResponse::bad_request(message_id, tracking_id, description);
        }
    };
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::ListRules { skip, max_rules },
        )
        .await
    {
        Ok(CommandOutcome::RulesListed { rules }) if rules.is_empty() => {
            ManagementResponse::no_content(message_id, tracking_id)
        }
        Ok(CommandOutcome::RulesListed { rules }) => match rules_value(&rules) {
            Ok(rules) => ManagementResponse::accepted(
                message_id,
                tracking_id,
                map_body(RULES, Value::List(rules)),
            ),
            Err(description) => ManagementResponse::internal(message_id, tracking_id, description),
        },
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("enumerating rules produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

fn add_request(body: &Body) -> Result<(RuleName, RuleFilter), String> {
    let name = rule_name(body)?;
    let description = match map_value(body, RULE_DESCRIPTION) {
        Some(Value::Map(description)) => description,
        _ => return Err(String::from("rule-description must be an AMQP map")),
    };
    if let Some(value) = entry_value(description, RULE_NAME) {
        match value {
            Value::String(inner) if inner.eq_ignore_ascii_case(name.as_str()) => {}
            Value::String(_) => {
                return Err(String::from(
                    "rule-description.rule-name must match rule-name",
                ));
            }
            _ => {
                return Err(String::from(
                    "rule-description.rule-name must be an AMQP string",
                ));
            }
        }
    }
    ensure_actionless(description)?;

    let filter = match (
        entry_value(description, SQL_FILTER),
        entry_value(description, CORRELATION_FILTER),
    ) {
        (Some(sql), None) => sql_filter(sql)?,
        (None, Some(correlation)) => correlation_filter(correlation)?,
        (None, None) => {
            return Err(String::from(
                "rule-description requires one supported rule filter",
            ));
        }
        (Some(_), Some(_)) => {
            return Err(String::from(
                "rule-description must contain exactly one rule filter",
            ));
        }
    };
    filter.validate().map_err(|error| error.to_string())?;
    Ok((name, filter))
}

fn rule_name(body: &Body) -> Result<RuleName, String> {
    let value = match map_value(body, RULE_NAME) {
        Some(Value::String(value)) => value.clone(),
        _ => return Err(String::from("rule-name must be an AMQP string")),
    };
    RuleName::new(value).map_err(|error| error.to_string())
}

fn sql_filter(value: &Value) -> Result<RuleFilter, String> {
    let Value::Map(filter) = value else {
        return Err(String::from("sql-filter must be an AMQP map"));
    };
    match entry_value(filter, EXPRESSION) {
        Some(Value::String(expression)) if expression == "1=1" => Ok(RuleFilter::True),
        Some(Value::String(expression)) if expression == "1=0" => Ok(RuleFilter::False),
        Some(Value::String(_)) => Err(String::from(
            "only the SQL filter expressions 1=1 and 1=0 are supported",
        )),
        _ => Err(String::from("sql-filter.expression must be an AMQP string")),
    }
}

fn correlation_filter(value: &Value) -> Result<RuleFilter, String> {
    let Value::Map(filter) = value else {
        return Err(String::from("correlation-filter must be an AMQP map"));
    };
    let application_properties = match entry_value(filter, PROPERTIES) {
        None | Some(Value::Null) => BTreeMap::new(),
        Some(Value::Map(properties)) => correlation_properties(properties)?,
        Some(_) => {
            return Err(String::from(
                "correlation-filter.properties must be an AMQP map",
            ));
        }
    };
    Ok(RuleFilter::Correlation(CorrelationFilter {
        correlation_id: optional_string(filter, CORRELATION_ID)?,
        message_id: optional_string(filter, MESSAGE_ID)?,
        to: optional_string(filter, TO)?,
        reply_to: optional_string(filter, REPLY_TO)?,
        subject: optional_string(filter, LABEL)?,
        session_id: optional_string(filter, SESSION_ID)?,
        reply_to_session_id: optional_string(filter, REPLY_TO_SESSION_ID)?,
        content_type: optional_string(filter, CONTENT_TYPE)?,
        application_properties,
    }))
}

fn correlation_properties(
    properties: &OrderedMap<Value, Value>,
) -> Result<BTreeMap<String, domain::CorrelationValue>, String> {
    let mut parsed = BTreeMap::new();
    for (key, value) in properties {
        let name = match key {
            Value::String(name) => name.clone(),
            Value::Symbol(name) => name.as_str().to_owned(),
            _ => {
                return Err(String::from(
                    "correlation application-property names must be strings",
                ));
            }
        };
        if !is_scalar(value) {
            return Err(format!(
                "correlation application property {name:?} must be an AMQP scalar"
            ));
        }
        let encoded = correlation_value(value).map_err(|error| error.to_string())?;
        if parsed.insert(name.clone(), encoded).is_some() {
            return Err(format!(
                "correlation application property {name:?} is repeated"
            ));
        }
    }
    Ok(parsed)
}

fn is_scalar(value: &Value) -> bool {
    matches!(
        value,
        Value::Bool(_)
            | Value::Ubyte(_)
            | Value::Ushort(_)
            | Value::Uint(_)
            | Value::Ulong(_)
            | Value::Byte(_)
            | Value::Short(_)
            | Value::Int(_)
            | Value::Long(_)
            | Value::Float(_)
            | Value::Double(_)
            | Value::Decimal32(_)
            | Value::Decimal64(_)
            | Value::Decimal128(_)
            | Value::Char(_)
            | Value::Timestamp(_)
            | Value::Uuid(_)
            | Value::Binary(_)
            | Value::String(_)
            | Value::Symbol(_)
    )
}

fn optional_string(map: &OrderedMap<Value, Value>, name: &str) -> Result<Option<String>, String> {
    match entry_value(map, name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(format!("correlation-filter.{name} must be an AMQP string")),
    }
}

fn ensure_actionless(description: &OrderedMap<Value, Value>) -> Result<(), String> {
    match entry_value(description, SQL_RULE_ACTION) {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Map(action)) if action.is_empty() => Ok(()),
        Some(value) if is_empty_action(value) => Ok(()),
        Some(_) => Err(String::from("SQL rule actions are not supported")),
    }
}

fn is_empty_action(value: &Value) -> bool {
    let Value::Described(action) = value else {
        return false;
    };
    let descriptor_matches = match &action.descriptor {
        Descriptor::Code(code) => *code == EMPTY_RULE_ACTION_CODE,
        Descriptor::Name(name) => name.as_str() == EMPTY_RULE_ACTION_NAME,
    };
    descriptor_matches && matches!(&action.value, Value::List(fields) if fields.is_empty())
}

fn page_request(body: &Body) -> Result<(u32, u32), String> {
    let skip = nonnegative_int(body, SKIP)?;
    let top = nonnegative_int(body, TOP)?;
    if top == 0 {
        return Err(String::from("top must be a positive AMQP int"));
    }
    Ok((skip, top))
}

fn nonnegative_int(body: &Body, name: &str) -> Result<u32, String> {
    match map_value(body, name) {
        Some(Value::Int(value)) if *value >= 0 => Ok(*value as u32),
        _ => Err(format!("{name} must be a nonnegative AMQP int")),
    }
}

fn rules_value(rules: &[RuleDefinition]) -> Result<Vec<Value>, String> {
    rules.iter().map(rule_value).collect()
}

fn rule_value(rule: &RuleDefinition) -> Result<Value, String> {
    let filter = filter_value(&rule.filter)?;
    let action = described(EMPTY_RULE_ACTION_CODE, Value::List(Vec::new()));
    let displayed_name = if rule.name.as_str() == domain::DEFAULT_RULE_NAME {
        "$Default"
    } else {
        rule.name.display_name()
    };
    let description = described(
        RULE_DESCRIPTION_CODE,
        Value::List(vec![
            filter,
            action,
            Value::String(displayed_name.to_owned()),
            timestamp_value(rule.created_at),
        ]),
    );
    let mut entry = OrderedMap::new();
    entry.insert(Value::String(RULE_DESCRIPTION.into()), description);
    Ok(Value::Map(entry))
}

fn filter_value(filter: &RuleFilter) -> Result<Value, String> {
    match filter {
        RuleFilter::True => Ok(described(TRUE_FILTER_CODE, Value::List(Vec::new()))),
        RuleFilter::False => Ok(described(FALSE_FILTER_CODE, Value::List(Vec::new()))),
        RuleFilter::Correlation(filter) => {
            let mut properties = OrderedMap::new();
            for (name, encoded) in &filter.application_properties {
                let value = serde_amqp::from_slice(encoded.as_bytes()).map_err(|error| {
                    format!(
                        "stored correlation application property {name:?} is invalid AMQP: {error}"
                    )
                })?;
                properties.insert(Value::String(name.clone()), value);
            }
            Ok(described(
                CORRELATION_FILTER_CODE,
                Value::List(vec![
                    string_or_null(&filter.correlation_id),
                    string_or_null(&filter.message_id),
                    string_or_null(&filter.to),
                    string_or_null(&filter.reply_to),
                    string_or_null(&filter.subject),
                    string_or_null(&filter.session_id),
                    string_or_null(&filter.reply_to_session_id),
                    string_or_null(&filter.content_type),
                    Value::Map(properties),
                ]),
            ))
        }
    }
}

fn string_or_null(value: &Option<String>) -> Value {
    value.clone().map(Value::String).unwrap_or(Value::Null)
}

fn described(code: u64, value: Value) -> Value {
    Value::Described(Box::new(Described {
        descriptor: Descriptor::Code(code),
        value,
    }))
}

fn entry_value<'a>(map: &'a OrderedMap<Value, Value>, name: &str) -> Option<&'a Value> {
    map.iter().find_map(|(key, value)| match key {
        Value::String(key) if key == name => Some(value),
        Value::Symbol(key) if key.as_str() == name => Some(value),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use domain::{BrokerError, CorrelationValue, Timestamp};

    use crate::BrokerRejection;

    use super::*;

    #[derive(Clone)]
    struct RecordingBroker {
        commands: Arc<Mutex<Vec<CommandKind>>>,
        result: Result<CommandOutcome, BrokerRejection>,
    }

    impl RecordingBroker {
        fn returning(result: Result<CommandOutcome, BrokerRejection>) -> Self {
            Self {
                commands: Arc::new(Mutex::new(Vec::new())),
                result,
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
        ) -> impl std::future::Future<Output = Result<CommandOutcome, BrokerRejection>> + Send
        {
            let commands = Arc::clone(&self.commands);
            let result = self.result.clone();
            async move {
                commands.lock().expect("command recorder").push(kind);
                result
            }
        }

        async fn deliverable(&self, _namespace: &NamespaceName, _entity: &EntityPath) {
            std::future::pending().await
        }
    }

    fn names() -> (NamespaceName, EntityPath) {
        (
            NamespaceName::new("tenant").expect("namespace"),
            EntityPath::new("orders/subscriptions/paid").expect("entity"),
        )
    }

    fn map(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
        Value::Map(
            entries
                .into_iter()
                .map(|(key, value)| (Value::String(key.into()), value))
                .collect(),
        )
    }

    fn add_message(name: &str, filter_name: &'static str, filter: Value) -> Message {
        Message {
            body: Body::Value(map([
                (RULE_NAME, Value::String(name.into())),
                (
                    RULE_DESCRIPTION,
                    map([
                        (filter_name, filter),
                        (SQL_RULE_ACTION, Value::Null),
                        (RULE_NAME, Value::String(name.into())),
                    ]),
                ),
            ])),
            ..Message::default()
        }
    }

    #[tokio::test]
    async fn add_decodes_the_sdk_true_filter_map() {
        let broker = RecordingBroker::returning(Ok(CommandOutcome::RuleCreated));
        let (namespace, entity) = names();
        let message = add_message(
            "AllOrders",
            SQL_FILTER,
            map([(EXPRESSION, Value::String("1=1".into()))]),
        );

        let response = add(
            &message,
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
        )
        .await;

        assert_eq!(response.status_code, 200);
        assert_eq!(
            broker.commands(),
            vec![CommandKind::CreateRule {
                name: RuleName::new("allorders").expect("rule name"),
                filter: RuleFilter::True,
            }]
        );
    }

    #[tokio::test]
    async fn add_preserves_supported_correlation_fields_and_scalar_types() {
        let broker = RecordingBroker::returning(Ok(CommandOutcome::RuleCreated));
        let (namespace, entity) = names();
        let application = map([
            ("region", Value::String("west".into())),
            ("attempt", Value::Int(7)),
        ]);
        let message = add_message(
            "Correlated",
            CORRELATION_FILTER,
            map([
                (CORRELATION_ID, Value::String("correlation".into())),
                (MESSAGE_ID, Value::String("message".into())),
                (TO, Value::String("orders".into())),
                (REPLY_TO, Value::String("replies".into())),
                (LABEL, Value::String("created".into())),
                (REPLY_TO_SESSION_ID, Value::String("reply-session".into())),
                (CONTENT_TYPE, Value::String("application/json".into())),
                (PROPERTIES, application),
            ]),
        );

        let response = add(
            &message,
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
        )
        .await;

        assert_eq!(response.status_code, 200);
        let commands = broker.commands();
        let [
            CommandKind::CreateRule {
                filter: RuleFilter::Correlation(filter),
                ..
            },
        ] = commands.as_slice()
        else {
            panic!("expected one correlation rule command")
        };
        assert_eq!(filter.correlation_id.as_deref(), Some("correlation"));
        assert_eq!(filter.message_id.as_deref(), Some("message"));
        assert_eq!(filter.to.as_deref(), Some("orders"));
        assert_eq!(filter.reply_to.as_deref(), Some("replies"));
        assert_eq!(filter.subject.as_deref(), Some("created"));
        assert_eq!(filter.session_id, None);
        assert_eq!(filter.reply_to_session_id.as_deref(), Some("reply-session"));
        assert_eq!(filter.content_type.as_deref(), Some("application/json"));
        assert_eq!(
            filter.application_properties.get("attempt"),
            Some(&correlation_value(&Value::Int(7)).expect("scalar encodes"))
        );
    }

    #[tokio::test]
    async fn session_id_predicates_are_rejected_until_topic_sessions_exist() {
        let broker = RecordingBroker::returning(Ok(CommandOutcome::RuleCreated));
        let (namespace, entity) = names();
        let message = add_message(
            "Session",
            CORRELATION_FILTER,
            map([(SESSION_ID, Value::String("session".into()))]),
        );

        let response = add(
            &message,
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
        )
        .await;

        assert_eq!(response.status_code, 400);
        assert!(broker.commands().is_empty());
    }

    #[tokio::test]
    async fn unsupported_sql_and_actions_are_bad_requests_before_submission() {
        let broker = RecordingBroker::returning(Ok(CommandOutcome::RuleCreated));
        let (namespace, entity) = names();
        let unsupported = add_message(
            "sql",
            SQL_FILTER,
            map([(EXPRESSION, Value::String("priority > 10".into()))]),
        );
        let mut action_description = OrderedMap::new();
        action_description.insert(
            Value::String(SQL_FILTER.into()),
            map([(EXPRESSION, Value::String("1=0".into()))]),
        );
        action_description.insert(
            Value::String(SQL_RULE_ACTION.into()),
            map([(EXPRESSION, Value::String("SET copied = 1".into()))]),
        );
        let action = Message {
            body: Body::Value(map([
                (RULE_NAME, Value::String("action".into())),
                (RULE_DESCRIPTION, Value::Map(action_description)),
            ])),
            ..Message::default()
        };

        for message in [&unsupported, &action] {
            let response = add(
                message,
                MessageId::Ulong(1),
                None,
                &namespace,
                &entity,
                &broker,
            )
            .await;
            assert_eq!(response.status_code, 400);
        }
        assert!(broker.commands().is_empty());
    }

    #[tokio::test]
    async fn enumerate_emits_vendor_described_rules_and_default_display_name() {
        let typed = correlation_value(&Value::Long(7)).expect("scalar encodes");
        let rules = vec![
            RuleDefinition {
                name: RuleName::new("$default").expect("default name"),
                filter: RuleFilter::True,
                created_at: Timestamp::from_millis(1_000),
            },
            RuleDefinition {
                name: RuleName::new("Typed").expect("rule name"),
                filter: RuleFilter::Correlation(CorrelationFilter {
                    subject: Some("created".into()),
                    application_properties: BTreeMap::from([("attempt".into(), typed)]),
                    ..CorrelationFilter::default()
                }),
                created_at: Timestamp::from_millis(2_000),
            },
        ];
        let broker = RecordingBroker::returning(Ok(CommandOutcome::RulesListed { rules }));
        let (namespace, entity) = names();
        let message = Message {
            body: Body::Value(map([(SKIP, Value::Int(0)), (TOP, Value::Int(100))])),
            ..Message::default()
        };

        let response = enumerate(
            &message,
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
        )
        .await;

        assert_eq!(response.status_code, 200);
        assert_eq!(
            broker.commands(),
            vec![CommandKind::ListRules {
                skip: 0,
                max_rules: 100,
            }]
        );
        let Body::Value(body) = response.into_message().body else {
            panic!("expected an AMQP value body")
        };
        let Value::List(entries) = entry_value(
            match &body {
                Value::Map(map) => map,
                _ => panic!("expected response map"),
            },
            RULES,
        )
        .expect("rules value") else {
            panic!("expected rules list")
        };
        let Value::Map(default_entry) = &entries[0] else {
            panic!("expected rule map")
        };
        let Value::Described(default_description) =
            entry_value(default_entry, RULE_DESCRIPTION).expect("rule description")
        else {
            panic!("expected described rule")
        };
        assert_eq!(
            default_description.descriptor,
            Descriptor::Code(RULE_DESCRIPTION_CODE)
        );
        let Value::List(default_fields) = &default_description.value else {
            panic!("expected described list")
        };
        assert_eq!(default_fields[2], Value::String("$Default".into()));
        assert!(matches!(
            &default_fields[0],
            Value::Described(filter)
                if filter.descriptor == Descriptor::Code(TRUE_FILTER_CODE)
                    && filter.value == Value::List(Vec::new())
        ));
        assert!(matches!(
            &default_fields[1],
            Value::Described(action)
                if action.descriptor == Descriptor::Code(EMPTY_RULE_ACTION_CODE)
                    && action.value == Value::List(Vec::new())
        ));

        let Value::Map(typed_entry) = &entries[1] else {
            panic!("expected rule map")
        };
        let Value::Described(typed_description) =
            entry_value(typed_entry, RULE_DESCRIPTION).expect("rule description")
        else {
            panic!("expected described rule")
        };
        let Value::List(typed_fields) = &typed_description.value else {
            panic!("expected described list")
        };
        assert_eq!(typed_fields[2], Value::String("Typed".into()));
        let Value::Described(filter) = &typed_fields[0] else {
            panic!("expected described filter")
        };
        assert_eq!(filter.descriptor, Descriptor::Code(CORRELATION_FILTER_CODE));
        let Value::List(fields) = &filter.value else {
            panic!("expected correlation fields")
        };
        assert_eq!(fields.len(), 9);
        assert_eq!(fields[4], Value::String("created".into()));
        let Value::Map(properties) = &fields[8] else {
            panic!("expected correlation properties")
        };
        assert_eq!(
            entry_value(properties, "attempt"),
            Some(&Value::Long(7)),
            "the AMQP long type must survive enumeration"
        );
    }

    #[tokio::test]
    async fn duplicate_and_missing_rules_use_service_bus_conditions() {
        let (namespace, entity) = names();
        let add_broker = RecordingBroker::returning(Err(BrokerRejection::Refused(
            BrokerError::RuleAlreadyExists {
                name: RuleName::new("same").expect("rule name"),
            },
        )));
        let add_response = add(
            &add_message(
                "same",
                SQL_FILTER,
                map([(EXPRESSION, Value::String("1=1".into()))]),
            ),
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &add_broker,
        )
        .await;
        assert_eq!(add_response.status_code, 409);
        assert_eq!(
            add_response.error_condition,
            Some(crate::ENTITY_ALREADY_EXISTS)
        );

        let missing_broker =
            RecordingBroker::returning(Err(BrokerRejection::Refused(BrokerError::RuleNotFound {
                name: RuleName::new("gone").expect("rule name"),
            })));
        let missing_response = remove(
            &Message {
                body: Body::Value(map([(RULE_NAME, Value::String("gone".into()))])),
                ..Message::default()
            },
            MessageId::Ulong(2),
            None,
            &namespace,
            &entity,
            &missing_broker,
        )
        .await;
        assert_eq!(missing_response.status_code, 404);
        assert_eq!(missing_response.error_condition, Some(crate::NOT_FOUND));
    }

    #[test]
    fn empty_correlation_filters_and_compound_values_are_invalid() {
        let empty = add_message(CORRELATION_ID, CORRELATION_FILTER, map([]));
        assert!(add_request(&empty.body).is_err());

        let compound = add_message(
            "compound",
            CORRELATION_FILTER,
            map([(PROPERTIES, map([("nested", Value::List(Vec::new()))]))]),
        );
        assert!(add_request(&compound.body).is_err());
    }

    #[test]
    fn raw_amqp_rule_names_follow_the_service_bus_resource_grammar() {
        for name in [
            "   ",
            "bad/name",
            "bad\\name",
            "bad@name",
            "bad?name",
            "bad#name",
            "bad*name",
            "bad name",
            ".leading",
            "trailing-",
            "café",
        ] {
            let message = add_message(
                name,
                SQL_FILTER,
                map([(EXPRESSION, Value::String("1=1".into()))]),
            );
            assert!(add_request(&message.body).is_err(), "accepted {name:?}");
        }
    }

    #[test]
    fn stored_correlation_values_must_still_be_valid_amqp() {
        let malformed = RuleFilter::Correlation(CorrelationFilter {
            application_properties: BTreeMap::from([(
                "bad".into(),
                CorrelationValue::new(vec![0xff]).expect("small opaque value"),
            )]),
            ..CorrelationFilter::default()
        });
        assert!(filter_value(&malformed).is_err());
    }
}
