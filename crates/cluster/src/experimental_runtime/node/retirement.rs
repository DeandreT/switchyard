use std::fmt;

use tokio::sync::watch;

use super::super::continuity::{EvidenceSlot, RetirementEvidence};
use super::{Error, wait_completed};

/// Completion of the owned lifecycle cleanup, with no close or engine handle.
#[derive(Clone)]
pub(in crate::experimental_runtime) struct NodeRetirement {
    completed: watch::Receiver<Option<Result<(), Error>>>,
    evidence: Option<EvidenceSlot>,
}

impl NodeRetirement {
    #[cfg(test)]
    pub(super) fn new(completed: watch::Receiver<Option<Result<(), Error>>>) -> Self {
        Self {
            completed,
            evidence: None,
        }
    }

    pub(super) fn with_evidence(
        completed: watch::Receiver<Option<Result<(), Error>>>,
        evidence: EvidenceSlot,
    ) -> Self {
        Self {
            completed,
            evidence: Some(evidence),
        }
    }

    pub(in crate::experimental_runtime) fn joined_evidence(
        &self,
    ) -> Result<std::sync::Arc<RetirementEvidence>, Error> {
        (*self.completed.borrow()).ok_or(Error::Closed)??;
        self.evidence.as_ref().ok_or(Error::OwnerFailure)?.get()
    }

    pub(in crate::experimental_runtime) async fn join(mut self) -> Result<(), Error> {
        wait_completed(&mut self.completed).await?;
        if self.evidence.is_some() {
            self.joined_evidence()?;
        }
        Ok(())
    }
}

impl fmt::Debug for NodeRetirement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeRetirement")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
