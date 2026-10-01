use std::collections::BTreeMap;

use domain::{
    BrokerError, CorrelationFilter, RuleDefinition, RuleFilter, RuleName, SqlCompileError,
    SqlCompileLimit, SqlFilter, SubscriptionName, Timestamp,
};
use serde_amqp::{described::Described, descriptor::Descriptor};

use super::*;

pub const ADD_RULE_OPERATION: &str = "com.microsoft:add-rule";
pub const REMOVE_RULE_OPERATION: &str = "com.microsoft:remove-rule";
pub const ENUMERATE_RULES_OPERATION: &str = "com.microsoft:enumerate-rules";
pub const RULE_NAME: &str = "rule-name";
pub const RULE_DESCRIPTION: &str = "rule-description";
pub const RULES: &str = "rules";

const SQL_FILTER: &str = "sql-filter";
const CORRELATION_FILTER: &str = "correlation-filter";
const SQL_ACTION: &str = "sql-rule-action";
const EXPRESSION: &str = "expression";
const FILTER_PROPERTIES: &str = "properties";
const SYSTEM_FIELDS: [&str; 8] = [
    "correlation-id",
    "message-id",
    "to",
    "reply-to",
    "label",
    "session-id",
    "reply-to-session-id",
    "content-type",
];
const MAX_RULE_PAGE_SIZE: i32 = 100;

// These literals deliberately retain the different widths used by the SDK's
// rule-description/action and filter codecs.
const RULE_DESCRIPTION_CODE: u64 = 0x0000013700000004;
const EMPTY_ACTION_CODE: u64 = 0x0000013700000005;
const TRUE_FILTER_CODE: u64 = 0x000001370000007;
const FALSE_FILTER_CODE: u64 = 0x000001370000008;
const SQL_FILTER_CODE: u64 = 0x000001370000006;
const CORRELATION_FILTER_CODE: u64 = 0x000001370000009;

#[derive(Debug)]
enum RuleRequestError {
    Invalid(String),
    Unsupported(&'static str),
    Internal(&'static str),
    Domain(BrokerError),
}

impl RuleRequestError {
    fn invalid(description: impl Into<String>) -> Self {
        Self::Invalid(description.into())
    }

    fn response(self, id: MessageId, tracking: Option<String>) -> ManagementResponse {
        match self {
            Self::Invalid(description) => {
                ManagementResponse::invalid_field(id, tracking, description)
            }
            Self::Domain(error) => {
                ManagementResponse::from_rejection(id, tracking, &BrokerRejection::Refused(error))
            }
            Self::Internal(description) => ManagementResponse::internal(id, tracking, description),
            Self::Unsupported(description) => ManagementResponse {
                correlation_id: id,
                status_code: 501,
                status_description: description.to_owned(),
                error_condition: Some(crate::NOT_IMPLEMENTED),
                tracking_id: tracking,
                body: Value::Null,
            },
        }
    }
}

impl From<BrokerError> for RuleRequestError {
    fn from(error: BrokerError) -> Self {
        Self::Domain(error)
    }
}

fn target(entity: &EntityPath) -> Result<(EntityPath, SubscriptionName), RuleRequestError> {
    match crate::parse_attachment(entity.as_str()) {
        Ok(crate::Attachment::Subscription {
            topic,
            subscription,
        }) => Ok((topic, subscription)),
        _ => Err(RuleRequestError::invalid(
            "rule operations require a subscription management endpoint",
        )),
    }
}

fn body_map(message: &Message) -> Result<&OrderedMap<Value, Value>, RuleRequestError> {
    match &message.body {
        Body::Value(Value::Map(body)) => Ok(body),
        _ => Err(RuleRequestError::invalid(
            "a rule request requires an AMQP map body",
        )),
    }
}

fn get<'a>(map: &'a OrderedMap<Value, Value>, key: &str) -> Option<&'a Value> {
    map.get(&Value::String(key.to_owned()))
}

fn known_fields(map: &OrderedMap<Value, Value>, allowed: &[&str]) -> Result<(), RuleRequestError> {
    if map
        .keys()
        .any(|key| !matches!(key, Value::String(key) if allowed.contains(&key.as_str())))
    {
        return Err(RuleRequestError::invalid(
            "a rule map contains an unsupported field",
        ));
    }
    Ok(())
}

fn name(map: &OrderedMap<Value, Value>) -> Result<RuleName, RuleRequestError> {
    let Some(Value::String(value)) = get(map, RULE_NAME) else {
        return Err(RuleRequestError::invalid("rule-name must be a string"));
    };
    if value
        .encode_utf16()
        .take(domain::MAX_RULE_NAME_LENGTH + 1)
        .count()
        > domain::MAX_RULE_NAME_LENGTH
    {
        return Err(RuleRequestError::invalid(
            "rule-name exceeds the supported length",
        ));
    }
    RuleName::new(value.clone()).map_err(|error| RuleRequestError::invalid(error.to_string()))
}

fn optional_string(
    map: &OrderedMap<Value, Value>,
    field: &str,
) -> Result<Option<String>, RuleRequestError> {
    match get(map, field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(RuleRequestError::invalid(format!(
            "correlation {field} must be a string or null"
        ))),
    }
}

fn correlation_filter(value: &Value) -> Result<RuleFilter, RuleRequestError> {
    let Value::Map(map) = value else {
        return Err(RuleRequestError::invalid(
            "correlation-filter must be a map",
        ));
    };
    if map.keys().any(|key| !matches!(key, Value::String(key) if SYSTEM_FIELDS.contains(&key.as_str()) || key == FILTER_PROPERTIES)) {
        return Err(RuleRequestError::invalid("a correlation filter contains an unsupported field"));
    }
    // Variable payload bytes are a lower bound on the stored rule. Refuse an
    // already-impossible field set before copying strings or binary values.
    let mut payload_bytes = SYSTEM_FIELDS
        .iter()
        .filter_map(|field| match get(map, field) {
            Some(Value::String(value)) => Some(value.len()),
            _ => None,
        })
        .fold(0_usize, usize::saturating_add);
    if let Some(Value::Map(entries)) = get(map, FILTER_PROPERTIES) {
        for (key, value) in entries.iter() {
            if let Value::String(key) = key {
                payload_bytes = payload_bytes.saturating_add(key.len());
            }
            payload_bytes = payload_bytes.saturating_add(match value {
                Value::String(value) => value.len(),
                Value::Symbol(value) => value.as_str().len(),
                Value::Binary(value) => value.len(),
                _ => 0,
            });
        }
    }
    if payload_bytes > domain::MAX_RULE_BYTES {
        return Err(BrokerError::RuleTooLarge {
            maximum_bytes: domain::MAX_RULE_BYTES,
        }
        .into());
    }
    let mut properties = BTreeMap::new();
    if let Some(value) = get(map, FILTER_PROPERTIES) {
        let Value::Map(entries) = value else {
            return Err(RuleRequestError::invalid(
                "correlation properties must be a map",
            ));
        };
        if entries.len() > domain::MAX_CORRELATION_RULE_CONDITIONS {
            return Err(RuleRequestError::invalid(
                "too many correlation property conditions",
            ));
        }
        for (key, value) in entries.iter() {
            let Value::String(key) = key else {
                return Err(RuleRequestError::invalid(
                    "correlation property names must be strings",
                ));
            };
            if matches!(
                value,
                Value::List(_) | Value::Map(_) | Value::Array(_) | Value::Described(_)
            ) {
                return Err(RuleRequestError::Unsupported(
                    "compound correlation property conditions are not implemented",
                ));
            }
            if properties
                .insert(key.clone(), crate::message::read_value(value))
                .is_some()
            {
                return Err(RuleRequestError::invalid(
                    "correlation property names must be unique",
                ));
            }
        }
    }
    let filter = RuleFilter::Correlation(CorrelationFilter {
        correlation_id: optional_string(map, SYSTEM_FIELDS[0])?,
        message_id: optional_string(map, SYSTEM_FIELDS[1])?,
        to: optional_string(map, SYSTEM_FIELDS[2])?,
        reply_to: optional_string(map, SYSTEM_FIELDS[3])?,
        subject: optional_string(map, SYSTEM_FIELDS[4])?,
        session_id: optional_string(map, SYSTEM_FIELDS[5])?,
        reply_to_session_id: optional_string(map, SYSTEM_FIELDS[6])?,
        content_type: optional_string(map, SYSTEM_FIELDS[7])?,
        properties,
    });
    filter.validate()?;
    Ok(filter)
}

fn create_rule(message: &Message) -> Result<(RuleName, RuleFilter), RuleRequestError> {
    let body = body_map(message)?;
    known_fields(body, &[RULE_NAME, RULE_DESCRIPTION])?;
    let rule_name = name(body)?;
    let Some(Value::Map(description)) = get(body, RULE_DESCRIPTION) else {
        return Err(RuleRequestError::invalid("rule-description must be a map"));
    };
    known_fields(
        description,
        &[RULE_NAME, SQL_FILTER, CORRELATION_FILTER, SQL_ACTION],
    )?;
    if name(description)? != rule_name {
        return Err(RuleRequestError::invalid(
            "the outer and described rule names must agree",
        ));
    }
    match get(description, SQL_ACTION) {
        None | Some(Value::Null) => {}
        Some(Value::Map(action)) => {
            known_fields(action, &[EXPRESSION])?;
            if !matches!(get(action, EXPRESSION), Some(Value::String(_))) {
                return Err(RuleRequestError::invalid(
                    "sql-rule-action requires a string expression",
                ));
            }
            return Err(RuleRequestError::Unsupported(
                "SQL rule actions are not implemented",
            ));
        }
        _ => {
            return Err(RuleRequestError::invalid(
                "sql-rule-action must be a map or null",
            ));
        }
    }
    let filter = match (
        get(description, SQL_FILTER),
        get(description, CORRELATION_FILTER),
    ) {
        (Some(Value::Map(sql)), None) => {
            known_fields(sql, &[EXPRESSION])?;
            match get(sql, EXPRESSION) {
                Some(Value::String(expression)) if expression == "1=1" => RuleFilter::True,
                Some(Value::String(expression)) if expression == "1=0" => RuleFilter::False,
                Some(Value::String(expression)) => sql_filter(expression)?,
                _ => {
                    return Err(RuleRequestError::invalid(
                        "sql-filter requires a string expression",
                    ));
                }
            }
        }
        (None, Some(correlation)) => correlation_filter(correlation)?,
        _ => {
            return Err(RuleRequestError::invalid(
                "rule-description requires exactly one filter map",
            ));
        }
    };
    let rule = RuleDefinition {
        name: rule_name,
        filter,
        created_at: Timestamp::UNIX_EPOCH,
    };
    rule.encoded_size()?;
    Ok((rule.name, rule.filter))
}

fn sql_filter(expression: &str) -> Result<RuleFilter, RuleRequestError> {
    // Refuse an impossible borrowed source before the typed constructor copies it.
    for (actual, maximum, kind) in [
        (
            expression.len(),
            domain::MAX_SQL_EXPRESSION_BYTES,
            SqlCompileLimit::SourceBytes,
        ),
        (
            expression
                .encode_utf16()
                .take(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1)
                .count(),
            domain::MAX_SQL_EXPRESSION_UTF16_UNITS,
            SqlCompileLimit::SourceUtf16Units,
        ),
    ] {
        if actual > maximum {
            return Err(
                BrokerError::SqlRuleCompilation(SqlCompileError::Limit { kind, maximum }).into(),
            );
        }
    }
    SqlFilter::new(expression)
        .map(RuleFilter::Sql)
        .map_err(|error| BrokerError::SqlRuleCompilation(error).into())
}

fn remove_rule(message: &Message) -> Result<RuleName, RuleRequestError> {
    let body = body_map(message)?;
    known_fields(body, &[RULE_NAME])?;
    name(body)
}

fn page(message: &Message) -> Result<(usize, usize), RuleRequestError> {
    let body = body_map(message)?;
    known_fields(body, &["top", "skip"])?;
    match (get(body, "top"), get(body, "skip")) {
        (Some(Value::Int(top)), Some(Value::Int(skip)))
            if (1..=MAX_RULE_PAGE_SIZE).contains(top) && *skip >= 0 =>
        {
            Ok((*top as usize, *skip as usize))
        }
        _ => Err(RuleRequestError::invalid(
            "top must be an int from 1 to 100 and skip a non-negative int",
        )),
    }
}

fn described(code: u64, fields: Vec<Value>) -> Value {
    Value::Described(Box::new(Described {
        descriptor: Descriptor::Code(code),
        value: Value::List(fields),
    }))
}

fn encoded_filter(filter: &RuleFilter) -> Value {
    match filter {
        RuleFilter::True => described(TRUE_FILTER_CODE, vec![]),
        RuleFilter::False => described(FALSE_FILTER_CODE, vec![]),
        RuleFilter::Sql(filter) => described(
            SQL_FILTER_CODE,
            vec![
                Value::String(filter.expression().to_owned()),
                Value::Int(20),
            ],
        ),
        RuleFilter::Correlation(filter) => {
            let mut fields: Vec<_> = [
                filter.correlation_id.as_ref(),
                filter.message_id.as_ref(),
                filter.to.as_ref(),
                filter.reply_to.as_ref(),
                filter.subject.as_ref(),
                filter.session_id.as_ref(),
                filter.reply_to_session_id.as_ref(),
                filter.content_type.as_ref(),
            ]
            .into_iter()
            .map(|value| value.map_or(Value::Null, |value| Value::String(value.clone())))
            .collect();
            fields.push(Value::Map(
                filter
                    .properties
                    .iter()
                    .map(|(key, value)| {
                        (
                            Value::String(key.clone()),
                            crate::message::write_value(value),
                        )
                    })
                    .collect(),
            ));
            described(CORRELATION_FILTER_CODE, fields)
        }
    }
}

fn encoded_rule(rule: &RuleDefinition) -> Result<Value, RuleRequestError> {
    let created = i64::try_from(rule.created_at.as_millis()).map_err(|_| {
        RuleRequestError::Internal("rule creation timestamp cannot be represented on AMQP")
    })?;
    Ok(Value::Map(
        [(
            Value::String(RULE_DESCRIPTION.to_owned()),
            described(
                RULE_DESCRIPTION_CODE,
                vec![
                    encoded_filter(&rule.filter),
                    described(EMPTY_ACTION_CODE, vec![]),
                    Value::String(rule.name.as_str().to_owned()),
                    Value::Timestamp(created.into()),
                ],
            ),
        )]
        .into_iter()
        .collect(),
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn process<B: Broker>(
    operation: &str,
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    budget: DeliveryBudget,
) -> ManagementResponse {
    let (topic, subscription) = match target(entity) {
        Ok(target) => target,
        Err(error) => return error.response(message_id, tracking_id),
    };
    let kind = match operation {
        ADD_RULE_OPERATION => match create_rule(message) {
            Ok((name, filter)) => CommandKind::CreateRule {
                subscription,
                name,
                filter,
            },
            Err(error) => return error.response(message_id, tracking_id),
        },
        REMOVE_RULE_OPERATION => match remove_rule(message) {
            Ok(name) => CommandKind::DeleteRule { subscription, name },
            Err(error) => return error.response(message_id, tracking_id),
        },
        ENUMERATE_RULES_OPERATION => {
            let (top, skip) = match page(message) {
                Ok(page) => page,
                Err(error) => return error.response(message_id, tracking_id),
            };
            let rules = match broker.rules(namespace.clone(), topic, subscription).await {
                Ok(rules) => rules,
                Err(error) => {
                    return ManagementResponse::from_rejection(message_id, tracking_id, &error);
                }
            };
            let entries = match rules
                .iter()
                .skip(skip)
                .take(top)
                .map(encoded_rule)
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(entries) => entries,
                Err(error) => return error.response(message_id, tracking_id),
            };
            let body = map_body(RULES, Value::List(entries));
            return match serde_amqp::to_vec(&body) {
                Ok(encoded) if encoded.len() as u64 <= budget.max_bytes => {
                    ManagementResponse::accepted(message_id, tracking_id, body)
                }
                Ok(_) => ManagementResponse::too_large(message_id, tracking_id),
                Err(_) => ManagementResponse::internal(
                    message_id,
                    tracking_id,
                    "rule response encoding failed",
                ),
            };
        }
        _ => unreachable!("only rule operations are dispatched here"),
    };
    match broker.submit(namespace.clone(), topic, kind).await {
        Ok(CommandOutcome::RuleCreated | CommandOutcome::RuleDeleted) => {
            ManagementResponse::accepted(message_id, tracking_id, Value::Null)
        }
        Ok(_) => ManagementResponse::internal(
            message_id,
            tracking_id,
            "rule mutation returned an unexpected outcome",
        ),
        Err(error) => ManagementResponse::from_rejection(message_id, tracking_id, &error),
    }
}

#[cfg(test)]
mod tests;
