//! Service Bus subscription rule management over the entity management node.

use std::collections::BTreeMap;

use amqp::{Body, Message, MessageId};
use domain::{
    CommandKind, CommandOutcome, CorrelationFilter, EntityPath, NamespaceName, RuleDefinition,
    RuleFilter, RuleName, SqlActionProgram, SqlRuleAction, SqlRuleFilter,
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
const COMPATIBILITY_LEVEL: &str = "compatibility-level";
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
const SQL_RULE_ACTION_CODE: u64 = 0x0000_0137_0000_0006;
const SQL_FILTER_CODE: u64 = 0x0000_0013_7000_0006;
const TRUE_FILTER_CODE: u64 = 0x0000_0013_7000_0007;
const FALSE_FILTER_CODE: u64 = 0x0000_0013_7000_0008;
const CORRELATION_FILTER_CODE: u64 = 0x0000_0013_7000_0009;
const EMPTY_RULE_ACTION_NAME: &str = "com.microsoft:empty-rule-action:list";
/// The only SQL dialect level Service Bus defines; enumerated SQL filters
/// report it, and add requests may name it.
const SQL_COMPATIBILITY_LEVEL: i32 = 20;

pub(super) async fn add<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
) -> ManagementResponse {
    let (name, filter, action) = match add_request(&message.body) {
        Ok(request) => request,
        Err(description) => {
            return ManagementResponse::bad_request(message_id, tracking_id, description);
        }
    };
    let kind = match action {
        Some(action) => CommandKind::CreateRuleWithAction {
            name,
            filter,
            action,
        },
        None => CommandKind::CreateRule { name, filter },
    };
    match super::submit_request(broker, namespace, entity, kind).await {
        Ok(CommandOutcome::RuleCreated) => {
            ManagementResponse::accepted(message_id, tracking_id, Value::Null)
        }
        Ok(_) => ManagementResponse::internal(
            message_id,
            tracking_id,
            "adding a rule produced an unexpected outcome",
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
    match super::submit_request(broker, namespace, entity, CommandKind::DeleteRule { name }).await {
        Ok(CommandOutcome::RuleDeleted) => {
            ManagementResponse::accepted(message_id, tracking_id, Value::Null)
        }
        Ok(_) => ManagementResponse::internal(
            message_id,
            tracking_id,
            "removing a rule produced an unexpected outcome",
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

#[cfg(test)]
pub(super) async fn enumerate<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
) -> ManagementResponse {
    enumerate_bounded(
        message,
        message_id,
        tracking_id,
        namespace,
        entity,
        broker,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn enumerate_bounded<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    reply_limit: Option<u64>,
) -> ManagementResponse {
    let (skip, max_rules) = match page_request(&message.body) {
        Ok(page) => page,
        Err(description) => {
            return ManagementResponse::bad_request(message_id, tracking_id, description);
        }
    };
    match super::submit_request(
        broker,
        namespace,
        entity,
        CommandKind::ListRules { skip, max_rules },
    )
    .await
    {
        Ok(CommandOutcome::RulesListed { rules }) if rules.is_empty() => {
            ManagementResponse::no_content(message_id, tracking_id)
        }
        Ok(CommandOutcome::RulesListed { rules }) => {
            project_rules(&rules, message_id, tracking_id, reply_limit)
        }
        Ok(_) => ManagementResponse::internal(
            message_id,
            tracking_id,
            "enumerating rules produced an unexpected outcome",
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

fn add_request(body: &Body) -> Result<(RuleName, RuleFilter, Option<SqlRuleAction>), String> {
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
    let action = action_request(description)?;

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
    Ok((name, filter, action))
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
    match entry_value(filter, COMPATIBILITY_LEVEL) {
        None | Some(Value::Null) => {}
        Some(Value::Int(level)) if *level == SQL_COMPATIBILITY_LEVEL => {}
        Some(_) => {
            return Err(format!(
                "sql-filter.compatibility-level must be the AMQP int {SQL_COMPATIBILITY_LEVEL}"
            ));
        }
    }
    // The SDKs send their true and false filters as these exact expressions;
    // keeping them as the dedicated filters preserves their enumerated type.
    // Any other text is retained verbatim and compiled by validation.
    match entry_value(filter, EXPRESSION) {
        Some(Value::String(expression)) if expression == "1=1" => Ok(RuleFilter::True),
        Some(Value::String(expression)) if expression == "1=0" => Ok(RuleFilter::False),
        Some(Value::String(expression)) => {
            Ok(RuleFilter::Sql(SqlRuleFilter::new(expression.clone())))
        }
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
            return Err("correlation application property must be an AMQP scalar".into());
        }
        let encoded = correlation_value(value).map_err(|error| error.to_string())?;
        if parsed.insert(name.clone(), encoded).is_some() {
            return Err("correlation application property is repeated".into());
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

fn action_request(description: &OrderedMap<Value, Value>) -> Result<Option<SqlRuleAction>, String> {
    match entry_value(description, SQL_RULE_ACTION) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Map(action)) if action.is_empty() => Ok(None),
        Some(value) if is_empty_action(value) => Ok(None),
        Some(Value::Map(action)) => {
            let mut seen = std::collections::BTreeSet::new();
            for (key, _) in action {
                let key = match key {
                    Value::String(key) => key.as_str(),
                    Value::Symbol(key) => key.as_str(),
                    _ => return Err("invalid SQL action setting".into()),
                };
                if ![EXPRESSION, COMPATIBILITY_LEVEL].contains(&key) || !seen.insert(key) {
                    return Err("invalid SQL action setting".into());
                }
            }
            match entry_value(action, COMPATIBILITY_LEVEL) {
                None | Some(Value::Null) | Some(Value::Int(SQL_COMPATIBILITY_LEVEL)) => {}
                _ => {
                    return Err(
                        "sql-rule-action.compatibility-level must be the AMQP int 20".into(),
                    );
                }
            }
            let Some(Value::String(source)) = entry_value(action, EXPRESSION) else {
                return Err("sql-rule-action.expression must be an AMQP string".into());
            };
            // Compile borrowed text before retaining it, binding or submitting.
            SqlActionProgram::compile(source).map_err(|_| "SQL action cannot be compiled")?;
            Ok(Some(SqlRuleAction::new(source.as_str())))
        }
        Some(_) => Err(String::from("invalid SQL rule action shape")),
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
    if top == 0 || top > domain::MAX_RULE_PAGE {
        return Err(String::from("top must be an AMQP int between 1 and 100"));
    }
    Ok((skip, top))
}

fn nonnegative_int(body: &Body, name: &str) -> Result<u32, String> {
    match map_value(body, name) {
        Some(Value::Int(value)) if *value >= 0 => Ok(*value as u32),
        _ => Err(format!("{name} must be a nonnegative AMQP int")),
    }
}

fn project_rules(
    rules: &[RuleDefinition],
    message_id: MessageId,
    tracking_id: Option<String>,
    reply_limit: Option<u64>,
) -> ManagementResponse {
    let maximum = reply_limit
        .unwrap_or(u64::MAX)
        .min(crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64);
    let too_large = || ManagementResponse {
        correlation_id: message_id.clone(),
        status_code: 403,
        status_description: "rule page exceeds the bounded response size limit".into(),
        error_condition: Some(crate::MESSAGE_SIZE_EXCEEDED),
        tracking_id: tracking_id.clone(),
        body: Value::Null,
    };
    // One page, not an unbounded projection followed by silent truncation. The
    // fixed envelope is encoded once; extra container-width growth costs <= 16.
    let empty = ManagementResponse::accepted(
        message_id.clone(),
        tracking_id.clone(),
        map_body(RULES, Value::List(Vec::new())),
    );
    let Ok(encoded) = amqp::encode_message(&empty.into_message()) else {
        return ManagementResponse::internal(
            message_id,
            tracking_id,
            "rule response cannot be encoded",
        );
    };
    let mut charged = encoded.len() as u64 + 16;
    let mut entries = Vec::new();
    if rules.len() > domain::MAX_RULE_PAGE as usize || charged > maximum {
        return too_large();
    }
    for rule in rules {
        // Bound an entry before cloning any retained source or property bytes.
        // ListRules already validates catalog grammar; recheck source at this
        // projection boundary rather than fabricating EmptyRuleAction on damage.
        if let Some(action) = &rule.action
            && action.compile().is_err()
        {
            return ManagementResponse::internal(
                message_id,
                tracking_id,
                "stored SQL action is invalid",
            );
        }
        let mut bound = 256usize;
        let mut add = |size: usize| {
            bound = bound.saturating_add(size);
        };
        add(rule.name.display_name().len());
        if let Some(action) = &rule.action {
            add(action.expression().len());
        }
        match &rule.filter {
            RuleFilter::Sql(filter) => add(filter.expression().len()),
            RuleFilter::Correlation(filter) => {
                for value in [
                    &filter.correlation_id,
                    &filter.message_id,
                    &filter.to,
                    &filter.reply_to,
                    &filter.subject,
                    &filter.session_id,
                    &filter.reply_to_session_id,
                    &filter.content_type,
                ]
                .into_iter()
                .flatten()
                {
                    add(value.len().saturating_add(5));
                }
                for (key, value) in &filter.application_properties {
                    add(key
                        .len()
                        .saturating_add(value.as_bytes().len())
                        .saturating_add(10));
                }
            }
            _ => {}
        }
        if bound as u64 > maximum {
            return too_large();
        }
        let entry = match rule_value(rule) {
            Ok(entry) => entry,
            Err(_) => {
                return ManagementResponse::internal(
                    message_id,
                    tracking_id,
                    "stored rule cannot be projected",
                );
            }
        };
        let Ok(size) = serde_amqp::serialized_size(&entry) else {
            return ManagementResponse::internal(
                message_id,
                tracking_id,
                "rule response cannot be encoded",
            );
        };
        let Some(next) = charged.checked_add(size as u64) else {
            return too_large();
        };
        if next > maximum {
            return too_large();
        }
        charged = next;
        entries.push(entry);
    }
    ManagementResponse::accepted(
        message_id,
        tracking_id,
        map_body(RULES, Value::List(entries)),
    )
}

fn rule_value(rule: &RuleDefinition) -> Result<Value, String> {
    let filter = filter_value(&rule.filter)?;
    let action = match &rule.action {
        None => described(EMPTY_RULE_ACTION_CODE, Value::List(Vec::new())),
        Some(action) => {
            action
                .compile()
                .map_err(|_| "stored SQL action is invalid")?;
            described(
                SQL_RULE_ACTION_CODE,
                Value::List(vec![
                    Value::String(action.expression().to_owned()),
                    Value::Int(SQL_COMPATIBILITY_LEVEL),
                ]),
            )
        }
    };
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
        RuleFilter::Sql(filter) => Ok(described(
            SQL_FILTER_CODE,
            Value::List(vec![
                Value::String(filter.expression().to_owned()),
                Value::Int(SQL_COMPATIBILITY_LEVEL),
            ]),
        )),
        RuleFilter::Correlation(filter) => {
            let mut properties = OrderedMap::new();
            for (name, encoded) in &filter.application_properties {
                // Correlation properties admitted over AMQP are scalars. Prove
                // that bounded, nonrecursive shape before the serde decoder can
                // encounter a forged retained container/count.
                let scalar_limits = message_content::Limits {
                    max_bytes: crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES,
                    max_depth: 1,
                    max_nodes: 1,
                    max_work: crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES + 16,
                };
                message_content::decode_value(encoded.as_bytes(), scalar_limits)
                    .map_err(|_| "stored correlation scalar is invalid")?;
                let value = serde_amqp::from_slice(encoded.as_bytes())
                    .map_err(|_| "stored correlation scalar is invalid")?;
                if !is_scalar(&value) {
                    return Err("stored correlation scalar is invalid".into());
                }
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
        async fn bind_entity(
            &self,
            namespace: NamespaceName,
            entity: EntityPath,
        ) -> Result<domain::EntityBinding, BrokerRejection> {
            Ok(crate::broker::test_binding(namespace, entity))
        }
        async fn submit(
            &self,
            _namespace: NamespaceName,
            _entity: EntityPath,
            _kind: CommandKind,
        ) -> Result<CommandOutcome, BrokerRejection> {
            panic!("bound outcome fixture cannot submit by name")
        }
        fn submit_bound(
            &self,
            _binding: domain::EntityBinding,
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
    async fn add_retains_sql_expression_text_verbatim() {
        let broker = RecordingBroker::returning(Ok(CommandOutcome::RuleCreated));
        let (namespace, entity) = names();
        let expression = "Priority >= 5 AND sys.Label = 'created'  ";
        for filter in [
            map([(EXPRESSION, Value::String(expression.into()))]),
            map([
                (EXPRESSION, Value::String(expression.into())),
                (COMPATIBILITY_LEVEL, Value::Int(SQL_COMPATIBILITY_LEVEL)),
            ]),
        ] {
            let response = add(
                &add_message("Urgent", SQL_FILTER, filter),
                MessageId::Ulong(1),
                None,
                &namespace,
                &entity,
                &broker,
            )
            .await;
            assert_eq!(response.status_code, 200);
        }
        let expected = CommandKind::CreateRule {
            name: RuleName::new("urgent").expect("rule name"),
            filter: RuleFilter::Sql(SqlRuleFilter::new(expression)),
        };
        assert_eq!(broker.commands(), vec![expected.clone(), expected]);
    }

    #[tokio::test]
    async fn add_keeps_the_sdk_false_expression_a_false_filter() {
        let broker = RecordingBroker::returning(Ok(CommandOutcome::RuleCreated));
        let (namespace, entity) = names();
        let message = add_message(
            "Blocked",
            SQL_FILTER,
            map([(EXPRESSION, Value::String("1=0".into()))]),
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
                name: RuleName::new("blocked").expect("rule name"),
                filter: RuleFilter::False,
            }]
        );
    }

    #[tokio::test]
    async fn uncompilable_sql_is_a_bad_request_naming_the_compile_error() {
        let broker = RecordingBroker::returning(Ok(CommandOutcome::RuleCreated));
        let (namespace, entity) = names();
        for (expression, expected) in [
            ("Priority >", "invalid SQL predicate syntax"),
            ("newid() = 'x'", "unsupported SQL predicate"),
            ("sys.Unknown = 1", "unsupported SQL predicate"),
        ] {
            let response = add(
                &add_message(
                    "broken",
                    SQL_FILTER,
                    map([(EXPRESSION, Value::String(expression.into()))]),
                ),
                MessageId::Ulong(1),
                None,
                &namespace,
                &entity,
                &broker,
            )
            .await;
            assert_eq!(response.status_code, 400, "{expression}");
            assert!(
                response
                    .status_description
                    .starts_with("SQL filter cannot be compiled: "),
                "{expression}: {}",
                response.status_description
            );
            assert!(
                response.status_description.contains(expected),
                "{expression}: {}",
                response.status_description
            );
        }
        assert!(broker.commands().is_empty());
    }

    #[tokio::test]
    async fn unsupported_actions_and_other_compatibility_levels_refuse_before_submission() {
        let broker = RecordingBroker::returning(Ok(CommandOutcome::RuleCreated));
        let (namespace, entity) = names();
        let other_level = add_message(
            "level",
            SQL_FILTER,
            map([
                (EXPRESSION, Value::String("Priority > 10".into())),
                (COMPATIBILITY_LEVEL, Value::Int(10)),
            ]),
        );
        let mut action_description = OrderedMap::new();
        action_description.insert(
            Value::String(SQL_FILTER.into()),
            map([(EXPRESSION, Value::String("Priority > 10".into()))]),
        );
        action_description.insert(
            Value::String(SQL_RULE_ACTION.into()),
            map([(EXPRESSION, Value::String("SET copied = newid()".into()))]),
        );
        let action = Message {
            body: Body::Value(map([
                (RULE_NAME, Value::String("action".into())),
                (RULE_DESCRIPTION, Value::Map(action_description)),
            ])),
            ..Message::default()
        };

        for (message, expected) in [
            (&other_level, "compatibility-level"),
            (&action, "SQL action cannot be compiled"),
        ] {
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
            assert!(
                response.status_description.contains(expected),
                "{}",
                response.status_description
            );
        }
        assert!(broker.commands().is_empty());
    }

    #[tokio::test]
    async fn the_topic_sql_rule_quota_is_a_resource_limit() {
        let broker = RecordingBroker::returning(Err(BrokerRejection::Refused(
            BrokerError::SqlRuleLimitExceeded {
                maximum: domain::MAX_TOPIC_SQL_RULES,
            },
        )));
        let (namespace, entity) = names();
        let response = add(
            &add_message(
                "one-too-many",
                SQL_FILTER,
                map([(EXPRESSION, Value::String("Priority > 10".into()))]),
            ),
            MessageId::Ulong(1),
            None,
            &namespace,
            &entity,
            &broker,
        )
        .await;

        assert_eq!(response.status_code, 403);
        assert_eq!(
            response.error_condition,
            Some(crate::RESOURCE_LIMIT_EXCEEDED)
        );
        assert_eq!(broker.commands().len(), 1);
    }

    #[tokio::test]
    async fn remove_deletes_a_sql_rule_by_name() {
        let broker = RecordingBroker::returning(Ok(CommandOutcome::RuleDeleted));
        let (namespace, entity) = names();
        let response = remove(
            &Message {
                body: Body::Value(map([(RULE_NAME, Value::String("Urgent".into()))])),
                ..Message::default()
            },
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
            vec![CommandKind::DeleteRule {
                name: RuleName::new("urgent").expect("rule name"),
            }]
        );
    }

    #[tokio::test]
    async fn enumerate_emits_vendor_described_rules_and_default_display_name() {
        let typed = correlation_value(&Value::Long(7)).expect("scalar encodes");
        let rules = vec![
            RuleDefinition {
                action: None,
                name: RuleName::new("$default").expect("default name"),
                filter: RuleFilter::True,
                created_at: Timestamp::from_millis(1_000),
            },
            RuleDefinition {
                action: None,
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
        // A tiny forged retained array may advertise an enormous implicit
        // element count. It must fail bounded scalar proof before serde sees it.
        let forged = RuleFilter::Correlation(CorrelationFilter {
            application_properties: BTreeMap::from([(
                "forged".into(),
                CorrelationValue::new(vec![0xf0, 0, 0, 0, 5, 0xff, 0xff, 0xff, 0xff, 0x41])
                    .unwrap(),
            )]),
            ..CorrelationFilter::default()
        });
        assert!(filter_value(&forged).is_err());
    }

    #[tokio::test]
    async fn enumerate_projects_sql_rules_with_their_exact_text() {
        let expression = "Priority >= 5 AND sys.Label = 'created'  ";
        let rules = vec![
            RuleDefinition {
                action: None,
                name: RuleName::new("Urgent").expect("rule name"),
                filter: RuleFilter::Sql(SqlRuleFilter::new(expression)),
                created_at: Timestamp::from_millis(1_000),
            },
            RuleDefinition {
                action: None,
                name: RuleName::new("Blocked").expect("rule name"),
                filter: RuleFilter::False,
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
        let Body::Value(Value::Map(body)) = response.into_message().body else {
            panic!("expected an AMQP map body")
        };
        let Some(Value::List(entries)) = entry_value(&body, RULES) else {
            panic!("expected rules list")
        };
        let filters: Vec<&Described<Value>> = entries
            .iter()
            .map(|entry| {
                let Value::Map(entry) = entry else {
                    panic!("expected rule map")
                };
                let Some(Value::Described(description)) = entry_value(entry, RULE_DESCRIPTION)
                else {
                    panic!("expected described rule")
                };
                let Value::List(fields) = &description.value else {
                    panic!("expected described list")
                };
                let Value::Described(filter) = &fields[0] else {
                    panic!("expected described filter")
                };
                filter.as_ref()
            })
            .collect();
        assert_eq!(filters[0].descriptor, Descriptor::Code(SQL_FILTER_CODE));
        assert_eq!(
            filters[0].value,
            Value::List(vec![
                Value::String(expression.into()),
                Value::Int(SQL_COMPATIBILITY_LEVEL),
            ])
        );
        assert_eq!(filters[1].descriptor, Descriptor::Code(FALSE_FILTER_CODE));
    }

    fn with_action(action: Option<Value>) -> Message {
        let mut message = add_message(
            "Action",
            SQL_FILTER,
            map([(EXPRESSION, Value::String("1=1".into()))]),
        );
        let Body::Value(Value::Map(body)) = &mut message.body else {
            unreachable!()
        };
        let Value::Map(description) = body
            .get_mut(&Value::String(RULE_DESCRIPTION.into()))
            .unwrap()
        else {
            unreachable!()
        };
        description.swap_remove(&Value::String(SQL_RULE_ACTION.into()));
        if let Some(action) = action {
            description.insert(Value::String(SQL_RULE_ACTION.into()), action);
        }
        message
    }

    #[tokio::test]
    async fn actions_compile_before_submission_and_preserve_exact_source_and_projection() {
        let source = " SET user.flag = TRUE; REMOVE user.old; ";
        for level in [None, Some(Value::Null), Some(Value::Int(20))] {
            let mut action = OrderedMap::new();
            action.insert(
                Value::Symbol(EXPRESSION.into()),
                Value::String(source.into()),
            );
            if let Some(level) = level {
                action.insert(Value::String(COMPATIBILITY_LEVEL.into()), level);
            }
            let broker = RecordingBroker::returning(Ok(CommandOutcome::RuleCreated));
            let (namespace, entity) = names();
            let response = add(
                &with_action(Some(Value::Map(action))),
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
                vec![CommandKind::CreateRuleWithAction {
                    name: RuleName::new("Action").unwrap(),
                    filter: RuleFilter::True,
                    action: SqlRuleAction::new(source)
                }]
            );
        }
        let value = rule_value(&RuleDefinition {
            name: RuleName::new("Action").unwrap(),
            filter: RuleFilter::True,
            created_at: Timestamp::from_millis(1),
            action: Some(SqlRuleAction::new(source)),
        })
        .unwrap();
        let Value::Map(entry) = value else { panic!() };
        let Some(Value::Described(description)) = entry_value(&entry, RULE_DESCRIPTION) else {
            panic!()
        };
        assert_eq!(
            description.descriptor,
            Descriptor::Code(RULE_DESCRIPTION_CODE)
        );
        let Value::List(fields) = &description.value else {
            panic!()
        };
        assert_eq!(fields.len(), 4);
        assert_eq!(
            fields[1],
            described(
                SQL_RULE_ACTION_CODE,
                Value::List(vec![Value::String(source.into()), Value::Int(20)])
            )
        );
        assert_ne!(SQL_RULE_ACTION_CODE, SQL_FILTER_CODE);
    }

    #[test]
    fn all_existing_actionless_forms_remain_actionless() {
        for action in [
            None,
            Some(Value::Null),
            Some(map([])),
            Some(described(EMPTY_RULE_ACTION_CODE, Value::List(vec![]))),
            Some(Value::Described(Box::new(Described {
                descriptor: Descriptor::Name(EMPTY_RULE_ACTION_NAME.into()),
                value: Value::List(vec![]),
            }))),
        ] {
            assert_eq!(add_request(&with_action(action).body).unwrap().2, None);
        }
    }

    #[tokio::test]
    async fn action_errors_are_fixed_and_do_not_submit_or_retain_uncompiled_source() {
        let (namespace, entity) = names();
        let broker = RecordingBroker::returning(Ok(CommandOutcome::RuleCreated));
        for source in [
            "",
            "   ",
            "SET secret = 'sensitive'; SET sys.Label = 'private'",
            "SET x = NULL",
            "SET x = @parameter",
            "REMOVE x;".repeat(33).as_str(),
            &format!("SET x = '{}'", "😀".repeat(512)),
        ] {
            let response = add(
                &with_action(Some(map([(EXPRESSION, Value::String(source.into()))]))),
                MessageId::Ulong(1),
                None,
                &namespace,
                &entity,
                &broker,
            )
            .await;
            assert_eq!(response.status_code, 400);
            assert_eq!(response.status_description, "SQL action cannot be compiled");
        }
        for action in [
            map([(EXPRESSION, Value::Null)]),
            map([
                (EXPRESSION, Value::String("REMOVE x".into())),
                ("parameters", map([])),
            ]),
            map([
                (EXPRESSION, Value::String("REMOVE x".into())),
                (COMPATIBILITY_LEVEL, Value::Long(20)),
            ]),
            map([
                (EXPRESSION, Value::String("REMOVE x".into())),
                (COMPATIBILITY_LEVEL, Value::Int(19)),
            ]),
            described(
                SQL_RULE_ACTION_CODE,
                Value::List(vec![Value::String("REMOVE x".into()), Value::Int(20)]),
            ),
            described(EMPTY_RULE_ACTION_CODE, Value::List(vec![Value::Null])),
        ] {
            assert_eq!(
                add(
                    &with_action(Some(action)),
                    MessageId::Ulong(1),
                    None,
                    &namespace,
                    &entity,
                    &broker
                )
                .await
                .status_code,
                400
            );
        }
        assert!(broker.commands().is_empty());
        assert!(
            add_request(
                &with_action(Some(map([(
                    EXPRESSION,
                    Value::String("REMOVE x;".repeat(32))
                )])))
                .body
            )
            .is_ok()
        );
        let boundary = format!("SET x = '{}'", "😀".repeat(507)); // 1,024 UTF-16 units
        assert_eq!(boundary.encode_utf16().count(), 1024);
        assert!(
            add_request(&with_action(Some(map([(EXPRESSION, Value::String(boundary))]))).body)
                .is_ok()
        );
    }

    #[test]
    fn enumeration_is_bounded_and_never_truncates_or_hides_corrupt_actions() {
        for top in [0, 101, i32::MAX] {
            assert!(
                page_request(&Body::Value(map([
                    (SKIP, Value::Int(0)),
                    (TOP, Value::Int(top))
                ])))
                .is_err()
            );
        }
        for (skip, top) in [
            (Value::Int(-1), Value::Int(1)),
            (Value::Int(0), Value::Long(20)),
            (Value::Uint(0), Value::Int(1)),
        ] {
            assert!(page_request(&Body::Value(map([(SKIP, skip), (TOP, top)]))).is_err());
        }
        let rules = vec![
            RuleDefinition {
                name: RuleName::new("Action").unwrap(),
                filter: RuleFilter::True,
                created_at: Timestamp::from_millis(1),
                action: Some(SqlRuleAction::new("REMOVE old"))
            };
            100
        ];
        let small = project_rules(&rules, MessageId::Ulong(1), None, Some(512));
        assert_eq!(small.error_condition, Some(crate::MESSAGE_SIZE_EXCEEDED));
        assert_eq!(small.body, Value::Null);
        let full = project_rules(&rules, MessageId::Ulong(1), None, None);
        assert_eq!(full.status_code, 200);
        assert!(
            amqp::encode_message(&full.into_message()).unwrap().len()
                <= crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES
        );
        let mut corrupt = rules[0].clone();
        corrupt.action = Some(SqlRuleAction::new("SET sys.Label = 'do not echo'"));
        let response = project_rules(&[corrupt], MessageId::Ulong(1), None, None);
        assert_eq!(response.status_code, 500);
        assert_eq!(response.status_description, "stored SQL action is invalid");
        assert_eq!(response.body, Value::Null);
        let mut large = rules[0].clone();
        large.filter = RuleFilter::Correlation(CorrelationFilter {
            application_properties: BTreeMap::from([(
                "large".into(),
                correlation_value(&Value::String("x".repeat(64 * 1024))).unwrap(),
            )]),
            ..CorrelationFilter::default()
        });
        let response = project_rules(&vec![large; 100], MessageId::Ulong(1), None, None);
        assert_eq!(response.error_condition, Some(crate::MESSAGE_SIZE_EXCEEDED));
        assert_eq!(
            response.body,
            Value::Null,
            "never return a successful partial page"
        );
    }
}
