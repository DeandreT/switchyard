use domain::{FiniteQueueCapacity, QueueCapacityView};

use super::*;

impl BrokerHandle {
    pub async fn create_finite_queue(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        config: QueueConfig,
        limit: FiniteQueueCapacity,
    ) -> Result<QueueCapacityView, SubmitError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::CreateFiniteQueue {
                namespace,
                entity,
                config,
                limit,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        result
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    pub fn create_finite_queue_blocking(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        config: QueueConfig,
        limit: FiniteQueueCapacity,
    ) -> Result<QueueCapacityView, SubmitError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::CreateFiniteQueue {
                namespace,
                entity,
                config,
                limit,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    pub async fn set_queue_capacity_limit_fenced(
        &self,
        binding: EntityBinding,
        limit: FiniteQueueCapacity,
    ) -> Result<QueueCapacityView, SubmitError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::SetQueueCapacityLimit {
                binding,
                limit,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        result
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    pub fn set_queue_capacity_limit_fenced_blocking(
        &self,
        binding: EntityBinding,
        limit: FiniteQueueCapacity,
    ) -> Result<QueueCapacityView, SubmitError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::SetQueueCapacityLimit {
                binding,
                limit,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    pub async fn set_finite_queue_definition_fenced(
        &self,
        binding: EntityBinding,
        config: QueueConfig,
        limit: FiniteQueueCapacity,
    ) -> Result<QueueCapacityView, SubmitError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::SetFiniteQueueDefinition {
                binding,
                config,
                limit,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        result
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    pub fn set_finite_queue_definition_fenced_blocking(
        &self,
        binding: EntityBinding,
        config: QueueConfig,
        limit: FiniteQueueCapacity,
    ) -> Result<QueueCapacityView, SubmitError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::SetFiniteQueueDefinition {
                binding,
                config,
                limit,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    pub async fn describe_queue_capacity(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<Option<QueueCapacityView>, SubmitError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::DescribeQueueCapacity {
                namespace,
                entity,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        result
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    pub fn describe_queue_capacity_blocking(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<Option<QueueCapacityView>, SubmitError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::DescribeQueueCapacity {
                namespace,
                entity,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }
}
