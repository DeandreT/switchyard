//! Durable subscription filters/actions and bounded rule browsing.
//!
//! Every filter, SQL included, is validated and canonicalized before anything
//! is staged, so an invalid definition refuses without mutation.

use storage::{StateStore, WriteBatch};

use std::sync::Arc;

use crate::{
    BrokerError, Command, CommandOutcome, EntityPath, MAX_RULE_PAGE, MAX_SUBSCRIPTION_RULES,
    MAX_TOPIC_SQL_RULES, NamespaceName, RuleDefinition, RuleFilter, RuleName, SqlProgram, codec,
    identifier::SUBSCRIPTION_PATH_SEGMENT, keys,
};

use super::{PreparationFailure, StateMachine, topic_routing::ProgramCache};

/// One validated rule row, with its compiled program when it is a SQL rule.
pub(super) type CheckedRule = (
    RuleDefinition,
    Option<Arc<SqlProgram>>,
    Option<Arc<crate::SqlActionProgram>>,
);

impl<S: StateStore> StateMachine<S> {
    /// Lists durable rules in canonical-name order.
    pub fn rules(
        &self,
        namespace: &NamespaceName,
        subscription: &EntityPath,
        skip: u32,
        max_rules: u32,
    ) -> Result<Vec<RuleDefinition>, BrokerError> {
        validate_page(max_rules)?;
        self.ensure_subscription(namespace, subscription)?;
        let rules = self.all_rules(namespace, subscription)?;
        Ok(rules
            .into_iter()
            .skip(usize::try_from(skip).unwrap_or(usize::MAX))
            .take(max_rules as usize)
            .collect())
    }

    pub(super) fn create_rule(
        &self,
        command: &Command,
        name: &RuleName,
        filter: &RuleFilter,
        action: Option<&crate::SqlRuleAction>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, PreparationFailure> {
        self.ensure_subscription(&command.namespace, &command.entity)?;
        let filter = filter.canonicalized().map_err(BrokerError::from)?;
        if let Some(action) = action {
            action
                .compile()
                .map_err(crate::RuleConfigError::SqlAction)
                .map_err(BrokerError::from)?;
        }
        let key = keys::subscription_rule(&command.namespace, &command.entity, name);
        if self.store().get(&key).map_err(BrokerError::from)?.is_some() {
            return Err(BrokerError::RuleAlreadyExists { name: name.clone() }.into());
        }
        if self.all_rules(&command.namespace, &command.entity)?.len() >= MAX_SUBSCRIPTION_RULES {
            // The bounded reader already rejected overfull or invalid touched rows.
            return Err(PreparationFailure::HealthyRuleCapacity);
        }
        if action.is_some() || !filter.is_legacy_representable() {
            let count = self.topic_sql_rules(&command.namespace, &command.entity)?;
            if count > MAX_TOPIC_SQL_RULES {
                // Overfull is already established without compiling stored
                // sources. It is an ordinary fail-stop, never a healthy origin.
                return Err(BrokerError::SqlRuleLimitExceeded {
                    maximum: MAX_TOPIC_SQL_RULES,
                }
                .into());
            }
            if count == MAX_TOPIC_SQL_RULES {
                // Version counting alone is not a health proof. Only a complete
                // canonical/compiled exact-full catalog can checkpoint a refusal.
                // Keep below-quota creation's existing unselected-reader boundary.
                return Err(
                    match self.checked_topic_sql_rules(&command.namespace, &command.entity)? {
                        MAX_TOPIC_SQL_RULES => PreparationFailure::HealthySqlRuleCapacity,
                        count if count > MAX_TOPIC_SQL_RULES => BrokerError::SqlRuleLimitExceeded {
                            maximum: MAX_TOPIC_SQL_RULES,
                        }
                        .into(),
                        _ => BrokerError::EntityMetadataCorrupt.into(),
                    },
                );
            }
        }

        let definition = RuleDefinition {
            action: action.cloned(),
            name: name.clone(),
            filter,
            created_at: command.issued_at,
        };
        batch.push_put(
            key,
            codec::rule::encode(&definition).map_err(BrokerError::from)?,
        );
        Ok(CommandOutcome::RuleCreated)
    }

    pub(super) fn delete_rule(
        &self,
        command: &Command,
        name: &RuleName,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        self.ensure_subscription(&command.namespace, &command.entity)?;
        let key = keys::subscription_rule(&command.namespace, &command.entity, name);
        if self.store().get(&key)?.is_none() {
            return Err(BrokerError::RuleNotFound { name: name.clone() });
        }
        batch.push_delete(key);
        Ok(CommandOutcome::RuleDeleted)
    }

    pub(super) fn list_rules(
        &self,
        command: &Command,
        skip: u32,
        max_rules: u32,
    ) -> Result<CommandOutcome, BrokerError> {
        Ok(CommandOutcome::RulesListed {
            rules: self.rules(&command.namespace, &command.entity, skip, max_rules)?,
        })
    }

    pub(super) fn stage_default_rule(
        &self,
        command: &Command,
        subscription: &EntityPath,
        batch: &mut WriteBatch,
    ) -> Result<(), BrokerError> {
        let definition = RuleDefinition::default_at(command.issued_at);
        batch.push_put(
            keys::subscription_rule(&command.namespace, subscription, &definition.name),
            codec::rule::encode(&definition)?,
        );
        Ok(())
    }

    pub(super) fn all_rules(
        &self,
        namespace: &NamespaceName,
        subscription: &EntityPath,
    ) -> Result<Vec<RuleDefinition>, BrokerError> {
        Ok(self
            .checked_rules(namespace, subscription, &mut ProgramCache::default())?
            .into_iter()
            .map(|(definition, _, _)| definition)
            .collect())
    }

    /// Stored SQL/action rows across every subscription of the subscription's
    /// topic. V2 SQL and V3 action records each consume one quota slot, regardless
    /// of filter kind. Rows are counted by their record version without compiling
    /// them; the selected subscription's rows were already fully validated.
    fn topic_sql_rules(
        &self,
        namespace: &NamespaceName,
        subscription: &EntityPath,
    ) -> Result<usize, BrokerError> {
        let (topic, _) = subscription
            .as_str()
            .split_once(SUBSCRIPTION_PATH_SEGMENT)
            .ok_or(BrokerError::SubscriptionNotFound)?;
        let topic = EntityPath::from_internal(topic.to_owned())?;
        let mut count = 0_usize;
        for sibling in self.subscriptions(namespace, &topic, usize::MAX)? {
            count = count.saturating_add(
                self.store()
                    .scan_prefix(
                        &keys::subscription_rule_prefix(namespace, &sibling),
                        MAX_SUBSCRIPTION_RULES + 1,
                    )?
                    .iter()
                    .filter(|(_, value)| matches!(value.first(), Some(2 | 3)))
                    .count(),
            );
            if count > MAX_TOPIC_SQL_RULES {
                // No later sibling can make an overfull catalog healthy.
                return Ok(count);
            }
        }
        Ok(count)
    }

    /// Proves every bounded sibling rule row before the healthy quota origin
    /// can be minted. Overfull or malformed catalogs never become client refusals.
    fn checked_topic_sql_rules(
        &self,
        namespace: &NamespaceName,
        subscription: &EntityPath,
    ) -> Result<usize, BrokerError> {
        let (topic, _) = subscription
            .as_str()
            .split_once(SUBSCRIPTION_PATH_SEGMENT)
            .ok_or(BrokerError::SubscriptionNotFound)?;
        let topic = EntityPath::from_internal(topic.to_owned())?;
        let mut cache = ProgramCache::default();
        let mut count = 0usize;
        for sibling in self.subscriptions(namespace, &topic, usize::MAX)? {
            count = count.saturating_add(
                self.checked_rules(namespace, &sibling, &mut cache)?
                    .iter()
                    .filter(|(definition, _, _)| {
                        definition.action.is_some()
                            || matches!(definition.filter, RuleFilter::Sql(_))
                    })
                    .count(),
            );
            if count > MAX_TOPIC_SQL_RULES {
                // Defensive bound even if the raw count and canonical proof
                // disagree: never retain programs for further siblings.
                return Err(BrokerError::SqlRuleLimitExceeded {
                    maximum: MAX_TOPIC_SQL_RULES,
                });
            }
        }
        Ok(count)
    }

    /// The bounded canonical rule reader. Each distinct SQL expression is
    /// compiled once through `cache`, which also serves later routing in the
    /// same command.
    pub(super) fn checked_rules(
        &self,
        namespace: &NamespaceName,
        subscription: &EntityPath,
        cache: &mut ProgramCache,
    ) -> Result<Vec<CheckedRule>, BrokerError> {
        let rules = self
            .store()
            .scan_prefix(
                &keys::subscription_rule_prefix(namespace, subscription),
                MAX_SUBSCRIPTION_RULES + 1,
            )?
            .into_iter()
            .map(|(key, value)| {
                codec::rule::decode(&value)
                    .map(|definition| (key, definition))
                    .map_err(BrokerError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if rules.len() > MAX_SUBSCRIPTION_RULES {
            return Err(BrokerError::RuleLimitExceeded {
                maximum: MAX_SUBSCRIPTION_RULES,
            });
        }
        rules
            .into_iter()
            .map(|(key, definition)| {
                if key != keys::subscription_rule(namespace, subscription, &definition.name) {
                    return Err(BrokerError::EntityMetadataCorrupt);
                }
                let program = match &definition.filter {
                    // Expression text is canonical by construction; it only
                    // has to compile under the current grammar and limits.
                    RuleFilter::Sql(filter) => Some(
                        cache
                            .program(filter.expression())
                            .ok_or(BrokerError::EntityMetadataCorrupt)?,
                    ),
                    filter => {
                        if filter
                            .canonicalized()
                            .map_err(|_| BrokerError::EntityMetadataCorrupt)?
                            != *filter
                        {
                            return Err(BrokerError::EntityMetadataCorrupt);
                        }
                        None
                    }
                };
                let action = definition
                    .action
                    .as_ref()
                    .map(|action| {
                        cache
                            .action(action.expression())
                            .ok_or(BrokerError::EntityMetadataCorrupt)
                    })
                    .transpose()?;
                Ok((definition, program, action))
            })
            .collect()
    }

    fn ensure_subscription(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<(), BrokerError> {
        if entity.is_dead_letter_queue()
            || !entity.is_subscription()
            || self.queue_config(namespace, entity)?.is_none()
        {
            return Err(BrokerError::SubscriptionNotFound);
        }
        Ok(())
    }
}

fn validate_page(max_rules: u32) -> Result<(), BrokerError> {
    if max_rules == 0 {
        return Err(BrokerError::EmptyRulePage);
    }
    if max_rules > MAX_RULE_PAGE {
        return Err(BrokerError::RulePageTooLarge {
            requested: max_rules,
            maximum: MAX_RULE_PAGE,
        });
    }
    Ok(())
}
