use domain::{RuleDefinition, SubscriptionDefinition, SubscriptionName};

use super::*;

impl BrokerHandle {
    /// Binds complete native metadata and its incarnation in one clock-free owner turn.
    pub async fn bind_admin(
        &self,
        namespace: NamespaceName,
        target: AdminTarget,
    ) -> Result<Option<EntityAdmission>, SubmitError> {
        let (reply, admission) = flume::bounded(1);
        self.requests
            .send_async(Request::BindAdminEntity {
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

    /// Reads complete, bounded subscription rules without a command stamp.
    pub fn rules_blocking(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, SubmitError> {
        let (reply, rules) = flume::bounded(1);
        self.requests
            .send(Request::ListRules {
                namespace,
                topic,
                subscription,
                binding: None,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        rules
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Reads complete rules without blocking the caller's executor.
    pub async fn rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, SubmitError> {
        let (reply, rules) = flume::bounded(1);
        self.requests
            .send_async(Request::ListRules {
                namespace,
                topic,
                subscription,
                binding: None,
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

    /// Reads validated native entity metadata without a command stamp.
    pub fn admin_entity_metadata_blocking(
        &self,
        namespace: NamespaceName,
        target: AdminTarget,
    ) -> Result<Option<EntityMetadata>, SubmitError> {
        let (reply, metadata) = flume::bounded(1);
        self.requests
            .send(Request::GetAdminEntityMetadata {
                namespace,
                target,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        metadata
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Reads validated native entity metadata without blocking the executor.
    pub async fn admin_entity_metadata(
        &self,
        namespace: NamespaceName,
        target: AdminTarget,
    ) -> Result<Option<EntityMetadata>, SubmitError> {
        let (reply, metadata) = flume::bounded(1);
        self.requests
            .send_async(Request::GetAdminEntityMetadata {
                namespace,
                target,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        metadata
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Reads all bounded subscription membership in one owner turn.
    pub fn subscriptions_blocking(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
    ) -> Result<Vec<SubscriptionDefinition>, SubmitError> {
        let (reply, subscriptions) = flume::bounded(1);
        self.requests
            .send(Request::ListSubscriptions {
                namespace,
                topic,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        subscriptions
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Reads all bounded subscription membership without blocking the executor.
    pub async fn subscriptions(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
    ) -> Result<Vec<SubscriptionDefinition>, SubmitError> {
        let (reply, subscriptions) = flume::bounded(1);
        self.requests
            .send_async(Request::ListSubscriptions {
                namespace,
                topic,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        subscriptions
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }
}
