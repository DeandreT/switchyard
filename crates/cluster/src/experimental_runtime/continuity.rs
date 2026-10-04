use std::sync::{Arc, Mutex};

use domain::{CommittedCheckpoint, CommittedStreamId};

use crate::experimental_log::FinalLogReport;

use super::Error;

/// Immutable data only; no engine, writer, query, or close authority survives.
#[derive(Clone, Eq, PartialEq)]
pub(super) struct RetirementEvidence {
    pub(super) log: FinalLogReport,
    pub(super) checkpoint: CommittedCheckpoint,
}

impl RetirementEvidence {
    pub(super) fn checked(
        node_id: u64,
        stream: CommittedStreamId,
        log: FinalLogReport,
        checkpoint: CommittedCheckpoint,
    ) -> Result<Arc<Self>, Error> {
        if log.profile().node_id() != node_id
            || log.profile().stream() != stream
            || log.retention().last_purged.is_some()
            || !log.matches_checkpoint(&checkpoint)
        {
            return Err(Error::OwnerFailure);
        }
        Ok(Arc::new(Self { log, checkpoint }))
    }
}

#[derive(Clone, Default)]
pub(super) struct EvidenceSlot(Arc<Mutex<Option<Arc<RetirementEvidence>>>>);

impl EvidenceSlot {
    pub(super) fn publish(&self, evidence: Arc<RetirementEvidence>) -> Result<(), Error> {
        let mut slot = self.0.lock().map_err(|_| Error::OwnerFailure)?;
        if slot.is_some() {
            return Err(Error::OwnerFailure);
        }
        *slot = Some(evidence);
        Ok(())
    }

    pub(super) fn get(&self) -> Result<Arc<RetirementEvidence>, Error> {
        self.0
            .lock()
            .map_err(|_| Error::OwnerFailure)?
            .clone()
            .ok_or(Error::OwnerFailure)
    }
}
