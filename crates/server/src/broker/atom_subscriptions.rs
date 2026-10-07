use domain::{SubscriptionConfig, SubscriptionName};

use super::*;

/// Closed-profile failures for ordinary subscription administration only.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum AtomSubscriptionOwnerError {
    #[error(transparent)]
    Submit(#[from] SubmitError),
    #[error("the subscription definition is not supported by this administration profile")]
    UnsupportedDefinition,
}

impl BrokerHandle {
    /// Creates one ordinary subscription, returning the prepared configuration.
    pub async fn create_atom_subscription(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        name: SubscriptionName,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionConfig, AtomSubscriptionOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::CreateAtomSubscription {
                namespace,
                topic,
                name,
                config,
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
    pub fn create_atom_subscription_blocking(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        name: SubscriptionName,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionConfig, AtomSubscriptionOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::CreateAtomSubscription {
                namespace,
                topic,
                name,
                config,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result.recv().map_err(|_| SubmitError::BrokerStopped)?
    }

    /// Reads a supported subscription without a command timestamp or mutation.
    pub async fn get_atom_subscription(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        name: SubscriptionName,
    ) -> Result<Option<SubscriptionConfig>, AtomSubscriptionOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::GetAtomSubscription {
                namespace,
                topic,
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

    /// Blocking equivalent of the same single owner request.
    pub fn get_atom_subscription_blocking(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        name: SubscriptionName,
    ) -> Result<Option<SubscriptionConfig>, AtomSubscriptionOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::GetAtomSubscription {
                namespace,
                topic,
                name,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result.recv().map_err(|_| SubmitError::BrokerStopped)?
    }

    /// Resolves the current child identity and deletes it in the same owner turn.
    pub async fn delete_atom_subscription(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        name: SubscriptionName,
    ) -> Result<CommandOutcome, AtomSubscriptionOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::DeleteAtomSubscription {
                namespace,
                topic,
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

    /// Blocking equivalent; removal wakeups are published only after commit.
    pub fn delete_atom_subscription_blocking(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        name: SubscriptionName,
    ) -> Result<CommandOutcome, AtomSubscriptionOwnerError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send(Request::DeleteAtomSubscription {
                namespace,
                topic,
                name,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        result.recv().map_err(|_| SubmitError::BrokerStopped)?
    }
}
