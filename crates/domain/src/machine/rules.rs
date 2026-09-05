//! Durable actionless subscription rules and bounded rule browsing.

use storage::{StateStore, WriteBatch};

use crate::{
    BrokerError, Command, CommandOutcome, EntityPath, MAX_RULE_PAGE, MAX_SUBSCRIPTION_RULES,
    NamespaceName, RuleDefinition, RuleFilter, RuleName, codec, keys,
};

use super::StateMachine;

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
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        self.ensure_subscription(&command.namespace, &command.entity)?;
        let filter = filter.canonicalized()?;
        let key = keys::subscription_rule(&command.namespace, &command.entity, name);
        if self.store().get(&key)?.is_some() {
            return Err(BrokerError::RuleAlreadyExists { name: name.clone() });
        }
        if self.all_rules(&command.namespace, &command.entity)?.len() >= MAX_SUBSCRIPTION_RULES {
            return Err(BrokerError::RuleLimitExceeded {
                maximum: MAX_SUBSCRIPTION_RULES,
            });
        }

        let definition = RuleDefinition {
            name: name.clone(),
            filter,
            created_at: command.issued_at,
        };
        batch.push_put(key, codec::encode(&definition)?);
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
            codec::encode(&definition)?,
        );
        Ok(())
    }

    pub(super) fn all_rules(
        &self,
        namespace: &NamespaceName,
        subscription: &EntityPath,
    ) -> Result<Vec<RuleDefinition>, BrokerError> {
        let rules = self
            .store()
            .scan_prefix(
                &keys::subscription_rule_prefix(namespace, subscription),
                MAX_SUBSCRIPTION_RULES + 1,
            )?
            .into_iter()
            .map(|(_, value)| codec::decode(&value).map_err(BrokerError::from))
            .collect::<Result<Vec<_>, _>>()?;
        if rules.len() > MAX_SUBSCRIPTION_RULES {
            return Err(BrokerError::RuleLimitExceeded {
                maximum: MAX_SUBSCRIPTION_RULES,
            });
        }
        Ok(rules)
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
