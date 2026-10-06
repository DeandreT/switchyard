use crate::{
    CorrelationFilter, MAX_RULE_BYTES, MAX_SUBSCRIPTION_RULE_BYTES, MAX_SUBSCRIPTION_RULES,
    MAX_TOPIC_RULE_COMPARISON_BYTES, MAX_TOPIC_RULE_MATCH_WORK, RuleDefinition, RuleFilter,
    RuleMatchLimit, RuleName, SqlCompileBudget, SqlCompileError, SqlCompileLimit,
    SqlEvaluationBudget, SqlEvaluationError, SqlEvaluationLimit, SqlMessageContext, SqlProgram,
    SubscriptionName,
};

use super::*;
use crate::rule::{CheckedSqlAction, SqlActionProgram};

struct StoredRules {
    definitions: Vec<RuleDefinition>,
    bytes: usize,
}

impl<S: StateStore> StateMachine<S> {
    /// The complete, sorted rule set. An empty set deliberately matches nothing.
    /// This metadata query never reads or advances the applied clock.
    pub fn rules(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        subscription: &SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerError> {
        Ok(self
            .load_rules(
                namespace,
                topic,
                subscription,
                &mut SqlCompileBudget::default(),
            )?
            .definitions)
    }

    fn read_rule_definitions(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        subscription: &SubscriptionName,
    ) -> Result<StoredRules, BrokerError> {
        self.require_rule_subscription(namespace, topic, subscription)?;
        let prefix = keys::rule_prefix(namespace, topic, subscription);
        let entries = self
            .store
            .scan_prefix(&prefix, MAX_SUBSCRIPTION_RULES + 1)?;
        if entries.len() > MAX_SUBSCRIPTION_RULES {
            return Err(BrokerError::RuleLimitExceeded {
                maximum: MAX_SUBSCRIPTION_RULES,
            });
        }
        let mut total = 0_usize;
        for (_, bytes) in &entries {
            total = total.saturating_add(bytes.len());
            if bytes.len() > MAX_RULE_BYTES || total > MAX_SUBSCRIPTION_RULE_BYTES {
                return Err(BrokerError::DanglingRuleMetadata);
            }
        }
        let mut rules = Vec::with_capacity(entries.len());
        for (key, bytes) in entries {
            let name =
                keys::rule_name_parts(&prefix, &key).ok_or(BrokerError::MalformedIndexKey)?;
            let rule = RuleDefinition::decode(&bytes)?;
            if rule.name.as_str() != name
                || keys::rule(namespace, topic, subscription, &rule.name) != key
                || rule.validate().is_err()
            {
                return Err(BrokerError::DanglingRuleMetadata);
            }
            rules.push(rule);
        }
        Ok(StoredRules {
            definitions: rules,
            bytes: total,
        })
    }

    pub(super) fn load_rules(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        subscription: &SubscriptionName,
        budget: &mut SqlCompileBudget,
    ) -> Result<LoadedRules, BrokerError> {
        let stored = self.read_rule_definitions(namespace, topic, subscription)?;
        let definitions = stored.definitions;
        let mut programs = Vec::with_capacity(definitions.len());
        let mut actions = Vec::with_capacity(definitions.len());
        for definition in &definitions {
            let program = match &definition.filter {
                RuleFilter::Sql(filter) => Some(
                    SqlProgram::compile_with_budget(filter.expression(), budget)
                        .map_err(stored_compile_error)?,
                ),
                _ => None,
            };
            programs.push(program);
            actions.push(
                definition
                    .action
                    .as_ref()
                    .map(|action| {
                        SqlActionProgram::compile_version_with_budget(
                            action.expression(),
                            action.semantic_version(),
                            budget,
                        )
                        .map_err(stored_action_compile_error)
                    })
                    .transpose()?,
            );
        }
        Ok(LoadedRules {
            definitions,
            programs,
            actions,
            stored_bytes: stored.bytes,
        })
    }

    fn require_rule_subscription(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        subscription: &SubscriptionName,
    ) -> Result<(), BrokerError> {
        Self::require_primary_entity_path(topic)?;
        if self
            .store
            .get(&keys::queue_config(namespace, topic))?
            .is_some()
            && self
                .store
                .get(&keys::topic_config(namespace, topic))?
                .is_some()
        {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        if self
            .subscription_config(namespace, topic, subscription)?
            .is_none()
        {
            if !self
                .store
                .scan_prefix(&keys::rule_prefix(namespace, topic, subscription), 1)?
                .is_empty()
            {
                return Err(BrokerError::DanglingRuleMetadata);
            }
            return Err(if self.topic_config(namespace, topic)?.is_some() {
                BrokerError::SubscriptionNotFound
            } else {
                BrokerError::TopicNotFound
            });
        }
        let entity = topic.subscription(subscription)?;
        let shadow = entity.dead_letter_queue()?;
        if self
            .store
            .get(&keys::topic_config(namespace, &entity))?
            .is_some()
            || self
                .store
                .get(&keys::topic_config(namespace, &shadow))?
                .is_some()
        {
            return Err(BrokerError::DanglingSubscriptionMetadata);
        }
        Ok(())
    }

    pub(super) fn create_rule(
        &self,
        command: &Command,
        subscription: &SubscriptionName,
        name: &RuleName,
        filter: &RuleFilter,
        action: Option<&crate::SqlAction>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        // Count the borrowed definition before copying any rule payload.
        let rules = self.load_rules(
            &command.namespace,
            &command.entity,
            subscription,
            &mut SqlCompileBudget::default(),
        )?;
        if rules.definitions.iter().any(|rule| rule.name == *name) {
            return Err(BrokerError::RuleAlreadyExists);
        }
        if rules.definitions.len() == MAX_SUBSCRIPTION_RULES {
            return Err(BrokerError::RuleLimitExceeded {
                maximum: MAX_SUBSCRIPTION_RULES,
            });
        }
        filter.validate()?;
        if let RuleFilter::Sql(filter) = filter {
            SqlProgram::compile(filter.expression()).map_err(BrokerError::SqlRuleCompilation)?;
        }
        if let Some(action) = action {
            action
                .validate_source()
                .map_err(BrokerError::SqlActionCompilation)?;
            SqlActionProgram::compile_version(action.expression(), action.semantic_version())
                .map_err(BrokerError::SqlActionCompilation)?;
        }
        let size = rule_definition_size(name, filter, command.issued_at, action)?;
        let total = rules.stored_bytes.saturating_add(size);
        if total > MAX_SUBSCRIPTION_RULE_BYTES {
            return Err(BrokerError::RuleSetTooLarge {
                maximum_bytes: MAX_SUBSCRIPTION_RULE_BYTES,
            });
        }
        let rule = RuleDefinition {
            name: name.clone(),
            filter: filter.clone(),
            created_at: command.issued_at,
            action: action.cloned(),
        };
        batch.push_put(
            keys::rule(&command.namespace, &command.entity, subscription, name),
            codec::encode(&rule)?,
        );
        Ok(CommandOutcome::RuleCreated)
    }

    pub(super) fn delete_rule(
        &self,
        command: &Command,
        subscription: &SubscriptionName,
        name: &RuleName,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        if !self
            .rules(&command.namespace, &command.entity, subscription)?
            .iter()
            .any(|rule| rule.name == *name)
        {
            return Err(BrokerError::RuleNotFound);
        }
        batch.push_delete(keys::rule(
            &command.namespace,
            &command.entity,
            subscription,
            name,
        ));
        Ok(CommandOutcome::RuleDeleted)
    }
}

fn stored_compile_error(error: SqlCompileError) -> BrokerError {
    match error {
        SqlCompileError::Limit {
            kind:
                SqlCompileLimit::AggregateSourceBytes
                | SqlCompileLimit::AggregateTokens
                | SqlCompileLimit::AggregateNodes,
            ..
        } => BrokerError::SqlRuleCompilation(error),
        _ => BrokerError::DanglingRuleMetadata,
    }
}

fn stored_action_compile_error(error: SqlCompileError) -> BrokerError {
    match error {
        SqlCompileError::Limit {
            kind:
                SqlCompileLimit::AggregateSourceBytes
                | SqlCompileLimit::AggregateTokens
                | SqlCompileLimit::AggregateNodes,
            ..
        } => BrokerError::SqlActionCompilation(error),
        _ => BrokerError::DanglingRuleMetadata,
    }
}

pub(super) struct LoadedRules {
    definitions: Vec<RuleDefinition>,
    programs: Vec<Option<SqlProgram>>,
    actions: Vec<Option<SqlActionProgram>>,
    stored_bytes: usize,
}

impl LoadedRules {
    pub(super) fn action(&self, index: usize) -> Result<(&SqlActionProgram, &str), BrokerError> {
        let program = self
            .actions
            .get(index)
            .and_then(Option::as_ref)
            .ok_or(BrokerError::DanglingRuleMetadata)?;
        let rule = self
            .definitions
            .get(index)
            .ok_or(BrokerError::DanglingRuleMetadata)?;
        Ok((program, rule.name.as_str()))
    }
}

#[derive(Debug)]
pub(super) enum SubscriptionMatch {
    NoMatch,
    Matched {
        no_action: bool,
        actions: Vec<MatchedAction>,
    },
    FilterError(SqlEvaluationError),
}

#[derive(Debug)]
pub(super) struct MatchedAction {
    pub(super) rule: usize,
    pub(super) checked: CheckedSqlAction,
}

#[derive(Clone, Copy, Default)]
pub(super) struct RuleMatchBudget {
    evaluation: SqlEvaluationBudget,
}

impl RuleMatchBudget {
    /// Precharges possible filter comparisons and action-planning visits before
    /// matching short circuits. These counters are not allocation or time limits.
    pub(super) fn charge(
        &mut self,
        message: MessageInput<'_>,
        subscriptions: &[LoadedRules],
    ) -> Result<(), BrokerError> {
        let properties = message
            .envelope
            .map(|envelope| &envelope.application_properties);
        let property_count = properties.map_or(0, BTreeMap::len);
        let (key_bytes, value_bytes) = properties.map_or((0, 0), |properties| {
            properties
                .iter()
                .fold((0_usize, 0_usize), |(keys, values), (key, value)| {
                    (
                        keys.saturating_add(key.len()),
                        values.saturating_add(scalar_bytes(value)),
                    )
                })
        });
        let candidates = system_candidates(message);
        let value_items = message
            .envelope
            .map_or(Ok(0), MessageEnvelope::validate_value_limits)?;
        let has_actions = subscriptions
            .iter()
            .any(|rules| rules.actions.iter().any(Option::is_some));
        let (
            application_items,
            annotation_items,
            sequence_sections,
            data_sections,
            metadata_entries,
        ) = if has_actions {
            message.envelope.map_or((0, 0, 0, 0, 0), |envelope| {
                let application_items = envelope
                    .application_properties
                    .values()
                    .fold(0_usize, |items, value| {
                        items.saturating_add(MessageEnvelope::action_value_items(value))
                    });
                let annotation_items = envelope
                    .message_annotations
                    .values()
                    .fold(0_usize, |items, value| {
                        items.saturating_add(MessageEnvelope::action_value_items(value))
                    });
                let (sequence_sections, data_sections) = match &envelope.body {
                    crate::MessageBody::Sequence(sections) => (sections.len(), 0),
                    crate::MessageBody::Data(sections) => (0, sections.len()),
                    _ => (0, 0),
                };
                let metadata_entries = property_count
                    .saturating_mul(3)
                    .saturating_add(envelope.message_annotations.len().saturating_mul(2))
                    .saturating_add(envelope.footer.len());
                (
                    application_items,
                    annotation_items,
                    sequence_sections,
                    data_sections,
                    metadata_entries,
                )
            })
        } else {
            (0, 0, 0, 0, 0)
        };
        if has_actions {
            // This shared profiling walks all values once, then app/annotation
            // values once each; flattening also visits empty sequence sections.
            self.add_work(
                value_items
                    .saturating_add(application_items)
                    .saturating_add(annotation_items)
                    .saturating_add(sequence_sections)
                    .saturating_add(property_count),
            )?;
        }
        for rules in subscriptions {
            for rule in &rules.definitions {
                self.add_work(1)?;
                if let RuleFilter::Correlation(filter) = &rule.filter {
                    for (expected, actual) in filter.system_conditions().into_iter().zip(candidates)
                    {
                        if let Some(expected) = expected {
                            self.add_work(1)?;
                            self.add_bytes(
                                expected.len().saturating_add(actual.map_or(0, str::len)),
                            )?;
                        }
                    }
                    for (key, value) in &filter.properties {
                        self.add_work(property_count.saturating_add(1))?;
                        self.add_bytes(
                            key.len()
                                .saturating_mul(property_count)
                                .saturating_add(key_bytes)
                                .saturating_add(scalar_bytes(value))
                                .saturating_add(value_bytes),
                        )?;
                    }
                }
            }
            for program in rules.actions.iter().flatten() {
                let overlay_count = program.targets().len();
                // Baseline sizing/counting: 2N+2A+H+2S+D. Original target
                // sizing/counting visits each first-unmodified app subtree at
                // most once (2A); final RuleName may independently revisit 2A.
                // Two positional property tallies each visit two IDs and six
                // strings. Other fixed scalar bookkeeping is not a node walk.
                self.add_work(
                    value_items
                        .saturating_mul(2)
                        .saturating_add(application_items.saturating_mul(6))
                        .saturating_add(annotation_items)
                        .saturating_add(sequence_sections.saturating_mul(2))
                        .saturating_add(data_sections)
                        .saturating_add(metadata_entries)
                        .saturating_add(16),
                )?;
                // Four overlay scans per SET plus the final RuleName scan;
                // two original-map lookups per SET plus the final lookup.
                self.add_work(
                    overlay_count
                        .saturating_mul(overlay_count)
                        .saturating_mul(4)
                        .saturating_add(overlay_count)
                        .saturating_add(
                            overlay_count
                                .saturating_mul(2)
                                .saturating_add(1)
                                .saturating_mul(property_count),
                        )
                        .saturating_add(overlay_count.saturating_mul(3)),
                )?;
                let overlay_key_bytes = program
                    .targets()
                    .iter()
                    .fold(0_usize, |bytes, key| bytes.saturating_add(key.len()));
                let literal_bytes = program.literal_bytes();
                for target in program.targets() {
                    self.add_bytes(
                        target
                            .len()
                            .saturating_mul(property_count)
                            .saturating_add(key_bytes)
                            .saturating_mul(2)
                            .saturating_add(
                                target
                                    .len()
                                    .saturating_mul(overlay_count)
                                    .saturating_add(overlay_key_bytes)
                                    .saturating_mul(4),
                            )
                            .saturating_add(value_bytes)
                            .saturating_add(literal_bytes),
                    )?;
                }
                self.add_bytes(
                    "RuleName"
                        .len()
                        .saturating_mul(overlay_count)
                        .saturating_add(overlay_key_bytes)
                        .saturating_add("RuleName".len().saturating_mul(property_count))
                        .saturating_add(key_bytes),
                )?;
            }
        }
        Ok(())
    }

    fn add_work(&mut self, amount: usize) -> Result<(), BrokerError> {
        self.evaluation
            .charge_work(amount)
            .map_err(evaluation_limit)
    }

    fn add_bytes(&mut self, amount: usize) -> Result<(), BrokerError> {
        self.evaluation
            .charge_bytes(amount)
            .map_err(evaluation_limit)
    }
}

pub(super) fn matching_subscriptions(
    message: MessageInput<'_>,
    subscriptions: &[LoadedRules],
    budget: &mut RuleMatchBudget,
) -> Result<Vec<SubscriptionMatch>, BrokerError> {
    budget.charge(message, subscriptions)?;
    let mut matches = Vec::with_capacity(subscriptions.len());
    for rules in subscriptions {
        let mut no_action = false;
        let mut actions = Vec::new();
        let mut first_error = None;
        for (index, (rule, program)) in rules.definitions.iter().zip(&rules.programs).enumerate() {
            let matched = match &rule.filter {
                RuleFilter::True => true,
                RuleFilter::False => false,
                RuleFilter::Correlation(filter) => correlation_matches(message, filter),
                RuleFilter::Sql(_) => {
                    let program = program.as_ref().ok_or(BrokerError::DanglingRuleMetadata)?;
                    match program.evaluate(
                        SqlMessageContext {
                            message_id: message.message_id,
                            session_id: message.session_id,
                            envelope: message.envelope,
                        },
                        &mut budget.evaluation,
                    ) {
                        Ok(truth) => truth.is_match(),
                        Err(error @ SqlEvaluationError::Limit { .. }) => {
                            return Err(evaluation_limit(error));
                        }
                        Err(error) => {
                            first_error.get_or_insert(error);
                            false
                        }
                    }
                }
            };
            if matched {
                if rule.action.is_some() {
                    actions.push(index);
                } else {
                    no_action = true;
                }
            }
        }
        matches.push(match first_error {
            Some(error) => SubscriptionMatch::FilterError(error),
            None if no_action || !actions.is_empty() => {
                let actions = actions
                    .into_iter()
                    .map(|rule| {
                        let (program, _) = rules.action(rule)?;
                        let checked = program.check(
                            message.message_id,
                            message.body.len(),
                            message.envelope,
                        )?;
                        Ok(MatchedAction { rule, checked })
                    })
                    .collect::<Result<Vec<_>, BrokerError>>()?;
                SubscriptionMatch::Matched { no_action, actions }
            }
            None => SubscriptionMatch::NoMatch,
        });
    }
    Ok(matches)
}

fn evaluation_limit(error: SqlEvaluationError) -> BrokerError {
    let (limit, maximum) = match error {
        SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            ..
        } => (RuleMatchLimit::WorkUnits, MAX_TOPIC_RULE_MATCH_WORK),
        SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            ..
        } => (
            RuleMatchLimit::ComparisonBytes,
            MAX_TOPIC_RULE_COMPARISON_BYTES,
        ),
        SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::LikePatternBytes,
            ..
        } => (
            RuleMatchLimit::LikePatternBytes,
            crate::MAX_SQL_LIKE_PATTERN_BYTES,
        ),
        SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::RegexEngineBytes,
            ..
        } => (
            RuleMatchLimit::RegexEngineBytes,
            crate::MAX_SQL_REGEX_ENGINE_BYTES,
        ),
        _ => return BrokerError::DanglingRuleMetadata,
    };
    BrokerError::TopicRuleMatchTooLarge { limit, maximum }
}

fn correlation_matches(message: MessageInput<'_>, filter: &CorrelationFilter) -> bool {
    if filter
        .system_conditions()
        .into_iter()
        .zip(system_candidates(message))
        .any(|(expected, actual)| expected.is_some_and(|expected| actual != Some(expected)))
    {
        return false;
    }
    filter
        .properties
        .iter()
        .all(|(expected_key, expected_value)| {
            message
                .envelope
                .and_then(|envelope| {
                    envelope
                        .application_properties
                        .iter()
                        .find(|(key, _)| *key == expected_key)
                        .map(|(_, value)| value)
                })
                .is_some_and(|value| value == expected_value)
        })
}

fn system_candidates<'a>(message: MessageInput<'a>) -> [Option<&'a str>; 8] {
    let properties = message.envelope.map(|envelope| &envelope.properties);
    [
        properties.and_then(|properties| match &properties.correlation_id {
            Some(MessageIdentifier::String(value)) => Some(value.as_str()),
            _ => None,
        }),
        Some(message.message_id),
        properties.and_then(|properties| properties.to.as_deref()),
        properties.and_then(|properties| properties.reply_to.as_deref()),
        properties.and_then(|properties| properties.subject.as_deref()),
        message.session_id.map(SessionId::as_str),
        properties.and_then(|properties| properties.reply_to_group_id.as_deref()),
        properties.and_then(|properties| properties.content_type.as_deref()),
    ]
}

fn scalar_bytes(value: &MessageValue) -> usize {
    match value {
        MessageValue::Null => 1,
        MessageValue::Bool(_) | MessageValue::Ubyte(_) | MessageValue::Byte(_) => 2,
        MessageValue::Ushort(_) | MessageValue::Short(_) => 3,
        MessageValue::Uint(_)
        | MessageValue::Int(_)
        | MessageValue::Float(_)
        | MessageValue::Decimal32(_)
        | MessageValue::Char(_) => 5,
        MessageValue::Ulong(_)
        | MessageValue::Long(_)
        | MessageValue::Double(_)
        | MessageValue::Decimal64(_)
        | MessageValue::Timestamp(_) => 9,
        MessageValue::Decimal128(_) | MessageValue::Uuid(_) => 17,
        MessageValue::String(value) | MessageValue::Symbol(value) => value.len().saturating_add(5),
        MessageValue::Binary(value) => value.len().saturating_add(5),
        // A scalar predicate cannot compare compound children or descriptors.
        MessageValue::List(_)
        | MessageValue::Map(_)
        | MessageValue::Array(_)
        | MessageValue::Described { .. } => 0,
    }
}

fn rule_definition_size(
    name: &RuleName,
    filter: &RuleFilter,
    created_at: Timestamp,
    action: Option<&crate::SqlAction>,
) -> Result<usize, BrokerError> {
    RuleName::validate(name.as_str())?;
    // Structs and tuples have the same positional postcard field encoding.
    let size: usize = postcard::serialize_with_flavor(
        &(name, filter, created_at, action),
        postcard::ser_flavors::Size::default(),
    )
    .map_err(|_| crate::CodecError::Encode)?;
    let size = size.saturating_add(1);
    if size > MAX_RULE_BYTES {
        return Err(BrokerError::RuleTooLarge {
            maximum_bytes: MAX_RULE_BYTES,
        });
    }
    Ok(size)
}

#[cfg(test)]
mod tests;
