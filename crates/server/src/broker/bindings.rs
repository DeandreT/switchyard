use domain::{RuleDefinition, SubscriptionName};

use super::*;

impl BrokerHandle {
    /// Captures validated topology and its identity in a single owner request.
    pub fn bind_blocking(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, SubmitError> {
        let (reply, admission) = flume::bounded(1);
        self.requests
            .send(Request::BindEntity {
                namespace,
                target,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        admission
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    pub async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, SubmitError> {
        let (reply, admission) = flume::bounded(1);
        self.requests
            .send_async(Request::BindEntity {
                namespace,
                target,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        admission
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    pub fn submit_fenced_blocking(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, SubmitError> {
        let (reply, outcome) = flume::bounded(1);
        self.requests
            .send(Request::Apply {
                namespace: binding.namespace().clone(),
                entity,
                binding: Some(binding),
                kind: Box::new(kind),
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        outcome
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    pub async fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, SubmitError> {
        let (reply, outcome) = flume::bounded(1);
        self.requests
            .send_async(Request::Apply {
                namespace: binding.namespace().clone(),
                entity,
                binding: Some(binding),
                kind: Box::new(kind),
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        outcome
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    pub fn rules_fenced_blocking(
        &self,
        binding: EntityBinding,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, SubmitError> {
        let (reply, rules) = flume::bounded(1);
        self.requests
            .send(Request::ListRules {
                namespace: binding.namespace().clone(),
                topic,
                subscription,
                binding: Some(binding),
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        rules
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    pub async fn rules_fenced(
        &self,
        binding: EntityBinding,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, SubmitError> {
        let (reply, rules) = flume::bounded(1);
        self.requests
            .send_async(Request::ListRules {
                namespace: binding.namespace().clone(),
                topic,
                subscription,
                binding: Some(binding),
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        rules
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }
}
