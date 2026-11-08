//! Per-command rule compilation and per-message subscription routing.
//!
//! Rules are compiled once per fanout command from the rule rows read at that
//! command's log position, so ordinary sends and scheduled activations route
//! against the same durable snapshot every replica reads. Matching is
//! actionless: any rule that evaluates to `TRUE` delivers one copy. When no
//! rule matches and at least one SQL rule failed to evaluate, the subscription
//! receives one dead-lettered copy instead. This mirrors Service Bus's default
//! of dead-lettering on filter evaluation exceptions; the reason, description
//! and the rule that a match still delivers are Switchyard's chosen behavior.
//! A subscription whose `dead_lettering_on_filter_evaluation_exceptions` is
//! false discards that copy instead. Otherwise the subscription is not offered
//! the message.

use std::{collections::BTreeMap, sync::Arc};

use message_content::{Value, decode_value};
use storage::WriteBatch;

use crate::{
    BrokerError, CONTENT_LIMITS, CodecError, CorrelationFilter, DeadLetterInfo, DeadLetterReason,
    EntityPath, FILTER_EVALUATION_DEAD_LETTER_REASON, FilterProperties, MessageRecord,
    MessageState, NamespaceName, RuleFilter, RuleName, SqlEvaluationBudget, SqlEvaluationError,
    SqlEvaluationLimit, SqlMessageContext, SqlProgram, SqlProperty, SqlRuleFilter,
    SqlSystemProperty, SqlSystemValue, SqlTruth, SqlValue, Timestamp, codec, keys,
};

use super::rules::CheckedRule;

/// Programs compiled during one command, keyed by exact expression text.
/// Never stored: a program is a pure function of its text and the current
/// grammar, so sharing one between identical rules cannot change a result.
#[derive(Default)]
pub(super) struct ProgramCache(BTreeMap<String, Option<Arc<SqlProgram>>>);

impl ProgramCache {
    /// `None` when the text does not compile under the default budget.
    pub(super) fn program(&mut self, expression: &str) -> Option<Arc<SqlProgram>> {
        if let Some(program) = self.0.get(expression) {
            return program.clone();
        }
        let program = SqlRuleFilter::new(expression).compile().ok().map(Arc::new);
        self.0.insert(expression.to_owned(), program.clone());
        program
    }
}

/// One subscription's rules and filter-error setting, ready to evaluate.
/// Never stored.
pub(super) struct CompiledRules {
    rules: Vec<CompiledRule>,
    dead_letter_on_failure: bool,
}

enum CompiledRule {
    True,
    False,
    Correlation(CorrelationFilter),
    Sql {
        name: RuleName,
        program: Arc<SqlProgram>,
    },
}

impl CompiledRules {
    /// Takes rows from the bounded canonical rule reader, which compiled
    /// every SQL filter once, and the subscription's
    /// `dead_lettering_on_filter_evaluation_exceptions` setting.
    pub(super) fn new(
        rules: Vec<CheckedRule>,
        dead_letter_on_failure: bool,
    ) -> Result<Self, BrokerError> {
        let rules = rules
            .into_iter()
            .map(|(definition, program)| match (definition.filter, program) {
                (RuleFilter::True, None) => Ok(CompiledRule::True),
                (RuleFilter::False, None) => Ok(CompiledRule::False),
                (RuleFilter::Correlation(filter), None) => Ok(CompiledRule::Correlation(filter)),
                (RuleFilter::Sql(_), Some(program)) => Ok(CompiledRule::Sql {
                    name: definition.name,
                    program,
                }),
                _ => Err(BrokerError::EntityMetadataCorrupt),
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            rules,
            dead_letter_on_failure,
        })
    }

    pub(super) fn uses_sql(&self) -> bool {
        self.rules
            .iter()
            .any(|rule| matches!(rule, CompiledRule::Sql { .. }))
    }

    pub(super) fn route(&self, message: &RoutingInput<'_>) -> Route {
        let mut failure = None;
        for rule in &self.rules {
            match rule {
                CompiledRule::True => return Route::Deliver,
                CompiledRule::False => {}
                CompiledRule::Correlation(filter) => {
                    if filter.matches(message.properties) {
                        return Route::Deliver;
                    }
                }
                CompiledRule::Sql { name, program } => {
                    let mut budget = SqlEvaluationBudget::default();
                    match program.evaluate(message.context(), &mut budget) {
                        Ok(SqlTruth::True) => return Route::Deliver,
                        Ok(SqlTruth::False | SqlTruth::Unknown) => {}
                        // The first failure in canonical rule-name order is
                        // reported; a later match still delivers. A
                        // subscription that discards filter-error copies is
                        // skipped exactly as when no rule matched.
                        Err(_) if !self.dead_letter_on_failure => {}
                        Err(error) => {
                            failure.get_or_insert_with(|| {
                                format!(
                                    "rule {}: {}",
                                    name.display_name(),
                                    evaluation_failure(error)
                                )
                            });
                        }
                    }
                }
            }
        }
        failure.map_or(Route::Skip, Route::DeadLetter)
    }
}

/// The routing decision for one message and one subscription.
pub(super) enum Route {
    Deliver,
    /// No rule matched and a SQL filter failed with this description.
    DeadLetter(String),
    Skip,
}

/// Application-property values decoded from the stored filter projection.
///
/// Projection values are canonical AMQP, so one that does not decode is
/// corruption and refuses the command. A decoded value whose AMQP type the SQL
/// profile does not support is `Unsupported`: a filter that references it
/// fails, and one that does not is unaffected.
pub(super) struct TypedProperties<'a> {
    properties: &'a FilterProperties,
    values: Vec<(&'a str, Value)>,
}

impl<'a> TypedProperties<'a> {
    /// Decodes only when some subscription has a SQL rule.
    pub(super) fn new(properties: &'a FilterProperties, decode: bool) -> Result<Self, BrokerError> {
        let values = if decode {
            properties
                .application_properties
                .iter()
                .map(|(name, value)| {
                    decode_value(value.as_bytes(), CONTENT_LIMITS)
                        .map(|value| (name.as_str(), value))
                        .map_err(|_| BrokerError::Codec(CodecError::Decode))
                })
                .collect::<Result<_, _>>()?
        } else {
            Vec::new()
        };
        Ok(Self { properties, values })
    }

    pub(super) fn input(&self) -> RoutingInput<'_> {
        let properties = self.properties;
        let text =
            |value: &'a Option<String>| value.as_deref().map_or(SqlValue::Null, SqlValue::String);
        RoutingInput {
            properties,
            application: self
                .values
                .iter()
                .map(|(name, value)| SqlProperty {
                    name,
                    value: sql_value(value),
                })
                .collect(),
            system: [
                (SqlSystemProperty::CorrelationId, &properties.correlation_id),
                (SqlSystemProperty::MessageId, &properties.message_id),
                (SqlSystemProperty::To, &properties.to),
                (SqlSystemProperty::ReplyTo, &properties.reply_to),
                (SqlSystemProperty::Subject, &properties.subject),
                (SqlSystemProperty::SessionId, &properties.session_id),
                (
                    SqlSystemProperty::ReplyToSessionId,
                    &properties.reply_to_session_id,
                ),
                (SqlSystemProperty::ContentType, &properties.content_type),
            ]
            .map(|(property, value)| SqlSystemValue {
                property,
                value: text(value),
            }),
        }
    }
}

/// Borrowed evaluation input shared by every subscription of one message.
pub(super) struct RoutingInput<'a> {
    properties: &'a FilterProperties,
    application: Vec<SqlProperty<'a>>,
    system: [SqlSystemValue<'a>; 8],
}

impl RoutingInput<'_> {
    fn context(&self) -> SqlMessageContext<'_> {
        SqlMessageContext {
            application_properties: &self.application,
            system_properties: &self.system,
        }
    }
}

fn sql_value(value: &Value) -> SqlValue<'_> {
    match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Bool(*value),
        Value::Byte(value) => SqlValue::Byte(*value),
        Value::UByte(value) => SqlValue::Ubyte(*value),
        Value::Short(value) => SqlValue::Short(*value),
        Value::UShort(value) => SqlValue::Ushort(*value),
        Value::Int(value) => SqlValue::Int(*value),
        Value::UInt(value) => SqlValue::Uint(*value),
        Value::Long(value) => SqlValue::Long(*value),
        Value::ULong(value) => SqlValue::Ulong(*value),
        Value::Float(bits) => SqlValue::Float(f32::from_bits(*bits)),
        Value::Double(bits) => SqlValue::Double(f64::from_bits(*bits)),
        Value::String(text) => SqlValue::String(&text.text),
        // Symbols, characters, timestamps, identifiers, decimals, binary and
        // compound values are retained but not comparable in this profile.
        _ => SqlValue::Unsupported,
    }
}

/// The replicated description text of each evaluation error. These strings
/// are written into dead-letter records, so they are fixed here rather than
/// derived from an error's `Display` or `Debug` output.
fn evaluation_failure(error: SqlEvaluationError) -> &'static str {
    match error {
        SqlEvaluationError::TypeMismatch => "SQL operands have incompatible types",
        SqlEvaluationError::ArithmeticOverflow => {
            "SQL integral arithmetic overflows its result type"
        }
        SqlEvaluationError::DivideByZero => "SQL integral division or remainder has a zero divisor",
        SqlEvaluationError::UnsupportedValue => "SQL operand uses an unsupported input value",
        SqlEvaluationError::AmbiguousProperty => "SQL property lookup is ambiguous",
        SqlEvaluationError::MissingSystemProperty => "SQL system property input is missing",
        SqlEvaluationError::StringOrderingUnsupported => "SQL string ordering is unsupported",
        SqlEvaluationError::NonPredicate => "SQL result is not a Boolean predicate",
        SqlEvaluationError::InvalidLikeEscape => {
            "SQL LIKE escape must contain exactly one Unicode scalar"
        }
        SqlEvaluationError::InvalidLikePattern => "SQL LIKE pattern is malformed",
        SqlEvaluationError::Limit { kind, .. } => match kind {
            SqlEvaluationLimit::WorkUnits => "SQL evaluation exceeds its work limit",
            SqlEvaluationLimit::ComparisonBytes => {
                "SQL evaluation exceeds its comparison byte limit"
            }
            SqlEvaluationLimit::LikePatternBytes => {
                "SQL evaluation exceeds its LIKE pattern byte limit"
            }
            SqlEvaluationLimit::RegexBytes => "SQL evaluation exceeds its LIKE regex byte limit",
        },
    }
}

/// Stages one filter-error copy in the subscription's dead-letter queue under
/// the topic-assigned sequence, as an ordinary ready message without lifetime
/// or session, exactly as other dead-lettering does. Returns the queue path.
pub(super) fn stage_filter_dead_letter(
    namespace: &NamespaceName,
    subscription: &EntityPath,
    mut record: MessageRecord,
    description: String,
    at: Timestamp,
    batch: &mut WriteBatch,
) -> Result<EntityPath, BrokerError> {
    let dead_letter_queue = subscription.dead_letter_queue()?;
    record.state = MessageState::Ready;
    record.expires_at = None;
    record.session_id = None;
    record.dead_letter = Some(DeadLetterInfo {
        reason: DeadLetterReason::Application(FILTER_EVALUATION_DEAD_LETTER_REASON.to_owned()),
        description,
        dead_lettered_at: at,
    });
    batch.push_put(
        keys::message(namespace, &dead_letter_queue, record.sequence),
        codec::message::encode(&record)?,
    );
    batch.push_put(
        keys::ready(namespace, &dead_letter_queue, record.sequence),
        Vec::new(),
    );
    Ok(dead_letter_queue)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicated_failure_descriptions_are_frozen() {
        let limit = |kind| SqlEvaluationError::Limit { kind, maximum: 1 };
        let cases = [
            (
                SqlEvaluationError::TypeMismatch,
                "SQL operands have incompatible types",
            ),
            (
                SqlEvaluationError::ArithmeticOverflow,
                "SQL integral arithmetic overflows its result type",
            ),
            (
                SqlEvaluationError::DivideByZero,
                "SQL integral division or remainder has a zero divisor",
            ),
            (
                SqlEvaluationError::UnsupportedValue,
                "SQL operand uses an unsupported input value",
            ),
            (
                SqlEvaluationError::AmbiguousProperty,
                "SQL property lookup is ambiguous",
            ),
            (
                SqlEvaluationError::MissingSystemProperty,
                "SQL system property input is missing",
            ),
            (
                SqlEvaluationError::StringOrderingUnsupported,
                "SQL string ordering is unsupported",
            ),
            (
                SqlEvaluationError::NonPredicate,
                "SQL result is not a Boolean predicate",
            ),
            (
                SqlEvaluationError::InvalidLikeEscape,
                "SQL LIKE escape must contain exactly one Unicode scalar",
            ),
            (
                SqlEvaluationError::InvalidLikePattern,
                "SQL LIKE pattern is malformed",
            ),
            (
                limit(SqlEvaluationLimit::WorkUnits),
                "SQL evaluation exceeds its work limit",
            ),
            (
                limit(SqlEvaluationLimit::ComparisonBytes),
                "SQL evaluation exceeds its comparison byte limit",
            ),
            (
                limit(SqlEvaluationLimit::LikePatternBytes),
                "SQL evaluation exceeds its LIKE pattern byte limit",
            ),
            (
                limit(SqlEvaluationLimit::RegexBytes),
                "SQL evaluation exceeds its LIKE regex byte limit",
            ),
        ];
        for (error, text) in cases {
            assert_eq!(evaluation_failure(error), text);
        }
        // The limit's maximum never reaches replicated text.
        assert_eq!(
            evaluation_failure(SqlEvaluationError::Limit {
                kind: SqlEvaluationLimit::WorkUnits,
                maximum: usize::MAX,
            }),
            "SQL evaluation exceeds its work limit"
        );
    }
}
