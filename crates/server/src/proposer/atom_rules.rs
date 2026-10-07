use domain::{EntityBinding, RuleDefinition, RuleName, SubscriptionName};

use super::*;
use crate::atom_admin::xml::{rules, subscriptions};
use crate::{AtomRuleDefinition, AtomRuleOwnerError, AtomSubscriptionOwnerError, SubmitError};

impl<S: StateStore, C: Clock> LocalProposer<S, C> {
    /// Returns the admitted static definition without a postcommit rule read.
    pub fn create_atom_rule(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        subscription: &SubscriptionName,
        definition: AtomRuleDefinition,
    ) -> Result<AtomRuleDefinition, AtomRuleOwnerError> {
        rules::validate_definition(&definition)
            .map_err(|_| AtomRuleOwnerError::UnsupportedDefinition)?;
        let binding = self.atom_rule_admission(namespace, topic, subscription)?;
        let outcome = self
            .propose_fenced_with_effects(
                &binding,
                topic,
                CommandKind::CreateRule {
                    subscription: subscription.clone(),
                    name: definition.name.clone(),
                    filter: definition.filter.clone(),
                },
            )?
            .outcome;
        if outcome != CommandOutcome::RuleCreated {
            return Err(ProposeError::UnexpectedOutcome {
                outcome: format!("{outcome:?}"),
            }
            .into());
        }
        Ok(definition)
    }

    /// Reads complete stored rule health before looking up the selected name.
    pub fn get_atom_rule(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        subscription: &SubscriptionName,
        name: &RuleName,
    ) -> Result<Option<AtomRuleDefinition>, AtomRuleOwnerError> {
        let binding = self.atom_rule_admission(namespace, topic, subscription)?;
        let definitions = self.rules_fenced(&binding, topic, subscription)?;
        rules::validate_name(name).map_err(|_| AtomRuleOwnerError::UnsupportedDefinition)?;
        definitions
            .into_iter()
            .find(|definition| definition.name == *name)
            .map(project_rule)
            .transpose()
    }

    /// Validates the entire complete bounded set before pagination, including
    /// rules outside the requested window. No partial page escapes a refusal.
    pub fn list_atom_rules(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        subscription: &SubscriptionName,
        skip: usize,
        top: usize,
    ) -> Result<Vec<AtomRuleDefinition>, AtomRuleOwnerError> {
        if skip > 1_000 || !(1..=100).contains(&top) {
            return Err(AtomRuleOwnerError::InvalidPageBounds);
        }
        let binding = self.atom_rule_admission(namespace, topic, subscription)?;
        let definitions = self.rules_fenced(&binding, topic, subscription)?;
        let definitions = definitions
            .into_iter()
            .map(project_rule)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(definitions.into_iter().skip(skip).take(top).collect())
    }

    /// Keeps original planner validation, including opaque healthy native rules
    /// and the stamped RuleNotFound path. It does not require a wire projection.
    pub fn delete_atom_rule(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        subscription: &SubscriptionName,
        name: &RuleName,
    ) -> Result<CommandOutcome, AtomRuleOwnerError> {
        rules::validate_name(name).map_err(|_| AtomRuleOwnerError::UnsupportedDefinition)?;
        let binding = self.atom_rule_admission(namespace, topic, subscription)?;
        let outcome = self
            .propose_fenced_with_effects(
                &binding,
                topic,
                CommandKind::DeleteRule {
                    subscription: subscription.clone(),
                    name: name.clone(),
                },
            )?
            .outcome;
        if outcome != CommandOutcome::RuleDeleted {
            return Err(ProposeError::UnexpectedOutcome {
                outcome: format!("{outcome:?}"),
            }
            .into());
        }
        Ok(outcome)
    }

    fn atom_rule_admission(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        subscription: &SubscriptionName,
    ) -> Result<EntityBinding, AtomRuleOwnerError> {
        let Some((binding, config)) =
            self.atom_subscription_admission(namespace, topic, subscription)?
        else {
            // Native bind-first absence is not an orphan-rule or ledger proof.
            return Err(BrokerError::SubscriptionNotFound.into());
        };
        super::atom_subscriptions::validate_names(topic, subscription)?;
        subscriptions::validate_config(&config)
            .map_err(|_| AtomRuleOwnerError::UnsupportedDefinition)?;
        Ok(binding)
    }
}

fn project_rule(rule: RuleDefinition) -> Result<AtomRuleDefinition, AtomRuleOwnerError> {
    if rule.action.is_some() {
        return Err(AtomRuleOwnerError::UnsupportedDefinition);
    }
    let definition = AtomRuleDefinition {
        name: rule.name,
        filter: rule.filter,
    };
    rules::validate_definition(&definition)
        .map_err(|_| AtomRuleOwnerError::UnsupportedDefinition)?;
    Ok(definition)
}

impl From<AtomSubscriptionOwnerError> for AtomRuleOwnerError {
    fn from(error: AtomSubscriptionOwnerError) -> Self {
        match error {
            AtomSubscriptionOwnerError::Submit(error) => Self::Submit(error),
            AtomSubscriptionOwnerError::UnsupportedDefinition => Self::UnsupportedDefinition,
        }
    }
}

impl From<ProposeError> for AtomRuleOwnerError {
    fn from(error: ProposeError) -> Self {
        Self::Submit(SubmitError::Propose(error))
    }
}

impl From<BrokerError> for AtomRuleOwnerError {
    fn from(error: BrokerError) -> Self {
        Self::from(ProposeError::Broker(error))
    }
}
