use domain::{FiniteQueueCapacity, QueueCapacityView};

use super::*;

/// Failures of the trusted, by-name Atom queue owner operations.
/// This envelope grants neither HTTP authorization nor native capacity fields.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum AtomQueueOwnerError {
    #[error(transparent)]
    Submit(#[from] SubmitError),
    #[error("the queue definition is not supported by Atom administration")]
    UnsupportedDefinition,
    #[error("the Atom queue page bounds are invalid")]
    InvalidPageBounds,
    #[error("the Atom queue page exceeds a read work limit")]
    WorkLimitExceeded,
}

impl From<ProposeError> for AtomQueueOwnerError {
    fn from(error: ProposeError) -> Self {
        Self::Submit(SubmitError::Propose(error))
    }
}

impl From<domain::BrokerError> for AtomQueueOwnerError {
    fn from(error: domain::BrokerError) -> Self {
        ProposeError::from(error).into()
    }
}

impl BrokerHandle {
    pub async fn get_atom_finite_queue(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<Option<QueueCapacityView>, AtomQueueOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::GetAtomFiniteQueue {
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
    }

    pub fn get_atom_finite_queue_blocking(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<Option<QueueCapacityView>, AtomQueueOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::GetAtomFiniteQueue {
                namespace,
                entity,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result.recv().map_err(|_| SubmitError::BrokerStopped)?
    }

    pub async fn update_atom_finite_queue(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        config: QueueConfig,
        limit: FiniteQueueCapacity,
    ) -> Result<QueueCapacityView, AtomQueueOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::UpdateAtomFiniteQueue {
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
    }

    pub fn update_atom_finite_queue_blocking(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        config: QueueConfig,
        limit: FiniteQueueCapacity,
    ) -> Result<QueueCapacityView, AtomQueueOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::UpdateAtomFiniteQueue {
                namespace,
                entity,
                config,
                limit,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result.recv().map_err(|_| SubmitError::BrokerStopped)?
    }

    pub async fn delete_atom_finite_queue(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<CommandOutcome, AtomQueueOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::DeleteAtomFiniteQueue {
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
    }

    pub fn delete_atom_finite_queue_blocking(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<CommandOutcome, AtomQueueOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::DeleteAtomFiniteQueue {
                namespace,
                entity,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result.recv().map_err(|_| SubmitError::BrokerStopped)?
    }

    pub async fn atom_finite_queues_page(
        &self,
        namespace: NamespaceName,
        skip: usize,
        top: usize,
    ) -> Result<Vec<QueueCapacityView>, AtomQueueOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::AtomFiniteQueuesPage {
                namespace,
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

    pub fn atom_finite_queues_page_blocking(
        &self,
        namespace: NamespaceName,
        skip: usize,
        top: usize,
    ) -> Result<Vec<QueueCapacityView>, AtomQueueOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::AtomFiniteQueuesPage {
                namespace,
                skip,
                top,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result.recv().map_err(|_| SubmitError::BrokerStopped)?
    }
}
