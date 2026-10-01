use crate::{
    CorrelationFilter, MAX_RULE_BYTES, MAX_SUBSCRIPTION_RULE_BYTES, MAX_SUBSCRIPTION_RULES,
    MAX_TOPIC_RULE_COMPARISON_BYTES, MAX_TOPIC_RULE_MATCH_WORK, RuleDefinition, RuleFilter,
    RuleMatchLimit, RuleName, SubscriptionName,
};

use super::*;

impl<S: StateStore> StateMachine<S> {
    /// The complete, sorted rule set. An empty set deliberately matches nothing.
    /// This metadata query never reads or advances the applied clock.
    pub fn rules(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        subscription: &SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerError> {
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
            let rule: RuleDefinition = codec::decode(&bytes)?;
            if rule.name.as_str() != name
                || keys::rule(namespace, topic, subscription, &rule.name) != key
                || rule.encoded_size().is_err()
            {
                return Err(BrokerError::DanglingRuleMetadata);
            }
            rules.push(rule);
        }
        Ok(rules)
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
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        // Count the borrowed definition before copying any rule payload.
        let rules = self.rules(&command.namespace, &command.entity, subscription)?;
        if rules.iter().any(|rule| rule.name == *name) {
            return Err(BrokerError::RuleAlreadyExists);
        }
        if rules.len() == MAX_SUBSCRIPTION_RULES {
            return Err(BrokerError::RuleLimitExceeded {
                maximum: MAX_SUBSCRIPTION_RULES,
            });
        }
        filter.validate()?;
        let size = rule_definition_size(name, filter, command.issued_at)?;
        let total = rules.iter().try_fold(size, |total, rule| {
            Ok::<_, BrokerError>(total.saturating_add(rule.encoded_size()?))
        })?;
        if total > MAX_SUBSCRIPTION_RULE_BYTES {
            return Err(BrokerError::RuleSetTooLarge {
                maximum_bytes: MAX_SUBSCRIPTION_RULE_BYTES,
            });
        }
        let rule = RuleDefinition {
            name: name.clone(),
            filter: filter.clone(),
            created_at: command.issued_at,
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

#[derive(Clone, Copy, Default)]
pub(super) struct RuleMatchBudget {
    work: usize,
    bytes: usize,
}

impl RuleMatchBudget {
    /// Charges every possible comparison, independently of match short circuits.
    /// Custom keys are scanned linearly after this admission so the upper bound
    /// does not depend on a library map's internal search strategy.
    pub(super) fn charge(
        &mut self,
        message: MessageInput<'_>,
        subscriptions: &[Vec<RuleDefinition>],
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
        for rules in subscriptions {
            for rule in rules {
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
        }
        Ok(())
    }

    fn add_work(&mut self, amount: usize) -> Result<(), BrokerError> {
        self.work = self.work.saturating_add(amount);
        if self.work > MAX_TOPIC_RULE_MATCH_WORK {
            return Err(BrokerError::TopicRuleMatchTooLarge {
                limit: RuleMatchLimit::WorkUnits,
                maximum: MAX_TOPIC_RULE_MATCH_WORK,
            });
        }
        Ok(())
    }

    fn add_bytes(&mut self, amount: usize) -> Result<(), BrokerError> {
        self.bytes = self.bytes.saturating_add(amount);
        if self.bytes > MAX_TOPIC_RULE_COMPARISON_BYTES {
            return Err(BrokerError::TopicRuleMatchTooLarge {
                limit: RuleMatchLimit::ComparisonBytes,
                maximum: MAX_TOPIC_RULE_COMPARISON_BYTES,
            });
        }
        Ok(())
    }
}

pub(super) fn matching_subscriptions(
    message: MessageInput<'_>,
    subscriptions: &[Vec<RuleDefinition>],
) -> u32 {
    let mut mask = 0;
    for (index, rules) in subscriptions.iter().enumerate() {
        if rules.iter().any(|rule| match &rule.filter {
            RuleFilter::True => true,
            RuleFilter::False => false,
            RuleFilter::Correlation(filter) => correlation_matches(message, filter),
        }) {
            mask |= 1_u32 << index;
        }
    }
    mask
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
) -> Result<usize, BrokerError> {
    RuleName::validate(name.as_str())?;
    // Structs and tuples have the same positional postcard field encoding.
    let size: usize = postcard::serialize_with_flavor(
        &(name, filter, created_at),
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
