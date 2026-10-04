use tokio::runtime::Handle;

use crate::{
    ExperimentalLogStore, ExperimentalStateMachine, LogStorageError, StateMachineError,
    experimental_owner::RetiredOwner,
};

use super::{ReplicaPreparationError, ReplicaProgress};

pub(crate) struct RuntimeParts {
    pub(crate) log: ExperimentalLogStore,
    pub(crate) state: ExperimentalStateMachine,
    pub(crate) log_join: RetiredOwner<LogStorageError>,
    pub(crate) state_join: RetiredOwner<StateMachineError>,
    pub(crate) progress: ReplicaProgress,
    pub(crate) config: openraft::Config,
}

pub(super) struct OwnedStores {
    parts: Option<Parts>,
    runtime: Handle,
}

struct Parts {
    log: ExperimentalLogStore,
    state: ExperimentalStateMachine,
    log_join: RetiredOwner<LogStorageError>,
    state_join: RetiredOwner<StateMachineError>,
}

impl OwnedStores {
    pub(super) fn into_raft_parts(
        mut self,
        progress: ReplicaProgress,
        config: openraft::Config,
    ) -> Result<RuntimeParts, ReplicaPreparationError> {
        let Parts {
            log,
            state,
            log_join,
            state_join,
        } = self.parts.take().ok_or(ReplicaPreparationError::Closed)?;
        Ok(RuntimeParts {
            log,
            state,
            log_join,
            state_join,
            progress,
            config,
        })
    }

    pub(super) async fn new(
        log: ExperimentalLogStore,
        state: ExperimentalStateMachine,
        runtime: Handle,
    ) -> Result<Self, ReplicaPreparationError> {
        let (log, log_join) = match log.into_runtime_parts() {
            Ok(parts) => parts,
            Err(_) => {
                state
                    .shutdown()
                    .await
                    .map_err(|_| ReplicaPreparationError::OwnerFailure)?;
                return Err(ReplicaPreparationError::Closed);
            }
        };
        let (state, state_join) = match state.into_runtime_parts() {
            Ok(parts) => parts,
            Err(_) => {
                drop(log);
                log_join
                    .join()
                    .await
                    .map_err(|_| ReplicaPreparationError::OwnerFailure)?;
                return Err(ReplicaPreparationError::Closed);
            }
        };
        Ok(Self {
            parts: Some(Parts {
                log,
                state,
                log_join,
                state_join,
            }),
            runtime,
        })
    }

    pub(super) fn adapters(
        &mut self,
    ) -> Result<(&mut ExperimentalLogStore, &mut ExperimentalStateMachine), ReplicaPreparationError>
    {
        let parts = self.parts.as_mut().ok_or(ReplicaPreparationError::Closed)?;
        Ok((&mut parts.log, &mut parts.state))
    }

    pub(super) async fn shutdown(mut self) -> Result<(), ReplicaPreparationError> {
        let parts = self.parts.take().ok_or(ReplicaPreparationError::Closed)?;
        let supervisor = self.runtime.spawn(retire(parts));
        supervisor
            .await
            .map_err(|_| ReplicaPreparationError::TaskFailed)?
    }
}

impl Drop for OwnedStores {
    fn drop(&mut self) {
        if let Some(parts) = self.parts.take() {
            // Publication loss also retires the owned pair. No cleanup waiter
            // or storage authority is retained by the prepared observations.
            drop(self.runtime.spawn(retire(parts)));
        }
    }
}

async fn retire(parts: Parts) -> Result<(), ReplicaPreparationError> {
    let Parts {
        log,
        state,
        log_join,
        state_join,
    } = parts;
    drop(log);
    drop(state);
    let (log_result, state_result) = tokio::join!(log_join.join(), state_join.join());
    if log_result.is_err() || state_result.is_err() {
        Err(ReplicaPreparationError::OwnerFailure)
    } else {
        Ok(())
    }
}
