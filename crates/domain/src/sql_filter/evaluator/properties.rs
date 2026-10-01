use crate::MessageIdentifier;

use super::*;

pub(super) struct Summary {
    count: usize,
    key_bytes: usize,
}

impl Summary {
    pub(super) fn new(
        message: SqlMessageContext<'_>,
        budget: &mut SqlEvaluationBudget,
    ) -> Result<Self, SqlEvaluationError> {
        let properties = message.envelope.map(|value| &value.application_properties);
        let count = properties.map_or(0, std::collections::BTreeMap::len);
        budget.charge_work(count)?;
        let mut key_bytes = 0_usize;
        if let Some(properties) = properties {
            for key in properties.keys() {
                key_bytes = key_bytes.saturating_add(key.len());
            }
        }
        budget.charge_bytes(key_bytes.saturating_mul(3))?;
        Ok(Self { count, key_bytes })
    }

    pub(super) fn charge_lookup(
        &self,
        property: &SqlProperty,
        budget: &mut SqlEvaluationBudget,
    ) -> Result<(), SqlEvaluationError> {
        match property {
            SqlProperty::User(name) => {
                // Unicode lowercase may expand a scalar into multiple scalars.
                // Three times all source bytes bounds both compared streams.
                let bytes = name
                    .len()
                    .saturating_mul(self.count)
                    .saturating_add(self.key_bytes)
                    .saturating_mul(3);
                budget.charge_work(self.count.saturating_add(1).saturating_add(bytes))?;
                budget.charge_bytes(bytes)
            }
            SqlProperty::System(_) => budget.charge_work(1),
        }
    }
}

pub(super) struct PropertyValue<'a> {
    pub(super) exists: bool,
    pub(super) value: Value<'a>,
}

pub(super) fn lookup<'a>(
    message: SqlMessageContext<'a>,
    property: &SqlProperty,
) -> Result<PropertyValue<'a>, SqlEvaluationError> {
    match property {
        SqlProperty::User(name) => {
            let mut found = None;
            if let Some(envelope) = message.envelope {
                for (key, value) in &envelope.application_properties {
                    if lowercase_equal(key, name) {
                        if found.is_some() {
                            return Err(SqlEvaluationError::AmbiguousProperty);
                        }
                        found = Some(value);
                    }
                }
            }
            match found {
                Some(value) => Ok(PropertyValue {
                    exists: true,
                    value: message_value(value)?,
                }),
                None => Ok(PropertyValue {
                    exists: false,
                    value: Value::Unknown,
                }),
            }
        }
        SqlProperty::System(property) => system(message, *property),
    }
}

fn lowercase_equal(left: &str, right: &str) -> bool {
    left.chars()
        .flat_map(char::to_lowercase)
        .eq(right.chars().flat_map(char::to_lowercase))
}

fn system(
    message: SqlMessageContext<'_>,
    property: SqlSystemProperty,
) -> Result<PropertyValue<'_>, SqlEvaluationError> {
    let properties = message.envelope.map(|envelope| &envelope.properties);
    let value = match property {
        SqlSystemProperty::MessageId => Some(message.message_id),
        SqlSystemProperty::SessionId => message.session_id.map(SessionId::as_str),
        SqlSystemProperty::CorrelationId => {
            match properties.and_then(|properties| properties.correlation_id.as_ref()) {
                Some(MessageIdentifier::String(value)) => Some(value.as_str()),
                Some(_) => {
                    return Ok(PropertyValue {
                        exists: true,
                        value: Value::Unsupported,
                    });
                }
                None => None,
            }
        }
        SqlSystemProperty::To => properties.and_then(|value| value.to.as_deref()),
        SqlSystemProperty::ReplyTo => properties.and_then(|value| value.reply_to.as_deref()),
        SqlSystemProperty::Subject => properties.and_then(|value| value.subject.as_deref()),
        SqlSystemProperty::ReplyToSessionId => {
            properties.and_then(|value| value.reply_to_group_id.as_deref())
        }
        SqlSystemProperty::ContentType => {
            properties.and_then(|value| value.content_type.as_deref())
        }
    };
    Ok(PropertyValue {
        exists: value.is_some(),
        value: value.map_or(Value::Null, Value::String),
    })
}
