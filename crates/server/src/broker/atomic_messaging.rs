use domain::{AtomicMessagingApplication, validate_atomic_messaging_kinds};

use super::*;

impl BrokerHandle {
    /// Applies one trusted atomic queue group on the owner thread. Callers
    /// must supply their own authorization; cancellation does not cancel a commit.
    pub fn submit_atomic_messaging_blocking(
        &self,
        binding: EntityBinding,
        kinds: Vec<CommandKind>,
    ) -> Result<AtomicMessagingApplication, SubmitError> {
        validate_atomic_messaging_kinds(&kinds)
            .map_err(ProposeError::from)
            .map_err(SubmitError::Propose)?;
        let (reply, application) = flume::bounded(1);
        self.requests
            .send(Request::ApplyAtomicMessaging {
                binding,
                kinds,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        application
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Applies one trusted atomic queue group without blocking the executor.
    /// Dropping the returned future does not make retrying an applied group safe.
    pub async fn submit_atomic_messaging(
        &self,
        binding: EntityBinding,
        kinds: Vec<CommandKind>,
    ) -> Result<AtomicMessagingApplication, SubmitError> {
        validate_atomic_messaging_kinds(&kinds)
            .map_err(ProposeError::from)
            .map_err(SubmitError::Propose)?;
        let (reply, application) = flume::bounded(1);
        self.requests
            .send_async(Request::ApplyAtomicMessaging {
                binding,
                kinds,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        application
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }
}
