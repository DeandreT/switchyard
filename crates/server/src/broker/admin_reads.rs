use domain::SubscriptionDefinition;

use super::*;

impl BrokerHandle {
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
