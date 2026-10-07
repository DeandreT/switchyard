use domain::{RuleFilter, RuleName, SubscriptionName};

use super::*;

/// The complete static rule profile; no action or creation date is projected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AtomRuleDefinition {
    pub name: RuleName,
    pub filter: RuleFilter,
}

/// Closed-profile failures for ordinary subscription rule administration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum AtomRuleOwnerError {
    #[error(transparent)]
    Submit(#[from] SubmitError),
    #[error("the rule definition is not supported by this administration profile")]
    UnsupportedDefinition,
    #[error("the rule page bounds are not supported by this administration profile")]
    InvalidPageBounds,
}

impl BrokerHandle {
    /// Creates one rule and returns its prepared static definition.
    pub async fn create_atom_rule(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
        definition: AtomRuleDefinition,
    ) -> Result<AtomRuleDefinition, AtomRuleOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::CreateAtomRule {
                namespace,
                topic,
                subscription,
                definition: Box::new(definition),
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        result
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
    }

    /// Blocking equivalent of the same single owner request.
    pub fn create_atom_rule_blocking(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
        definition: AtomRuleDefinition,
    ) -> Result<AtomRuleDefinition, AtomRuleOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::CreateAtomRule {
                namespace,
                topic,
                subscription,
                definition: Box::new(definition),
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result.recv().map_err(|_| SubmitError::BrokerStopped)?
    }

    /// Reads one supported rule after complete current-set validation.
    pub async fn get_atom_rule(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
        name: RuleName,
    ) -> Result<Option<AtomRuleDefinition>, AtomRuleOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::GetAtomRule {
                namespace,
                topic,
                subscription,
                name,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        result
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
    }

    /// Blocking equivalent; the read stamps no command and applies no batch.
    pub fn get_atom_rule_blocking(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
        name: RuleName,
    ) -> Result<Option<AtomRuleDefinition>, AtomRuleOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::GetAtomRule {
                namespace,
                topic,
                subscription,
                name,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result.recv().map_err(|_| SubmitError::BrokerStopped)?
    }

    /// Projects the complete supported set before selecting a bounded page.
    pub async fn list_atom_rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
        skip: usize,
        top: usize,
    ) -> Result<Vec<AtomRuleDefinition>, AtomRuleOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::ListAtomRules {
                namespace,
                topic,
                subscription,
                skip,
                top,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        result
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
    }

    /// Blocking equivalent; a short page follows complete-set exhaustion proof.
    pub fn list_atom_rules_blocking(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
        skip: usize,
        top: usize,
    ) -> Result<Vec<AtomRuleDefinition>, AtomRuleOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::ListAtomRules {
                namespace,
                topic,
                subscription,
                skip,
                top,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result.recv().map_err(|_| SubmitError::BrokerStopped)?
    }

    /// Deletes one named rule through the current subscription fence.
    pub async fn delete_atom_rule(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
        name: RuleName,
    ) -> Result<CommandOutcome, AtomRuleOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::DeleteAtomRule {
                namespace,
                topic,
                subscription,
                name,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        result
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
    }

    /// Blocking equivalent; rule changes publish no delivery/removal wakeups.
    pub fn delete_atom_rule_blocking(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
        name: RuleName,
    ) -> Result<CommandOutcome, AtomRuleOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::DeleteAtomRule {
                namespace,
                topic,
                subscription,
                name,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result.recv().map_err(|_| SubmitError::BrokerStopped)?
    }
}
