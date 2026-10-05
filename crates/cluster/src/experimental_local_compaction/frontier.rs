use std::sync::{Arc, Mutex};

use domain::{CommittedCheckpoint, CommittedStreamId};
use openraft::{BasicNode, SnapshotMeta};
use tokio::sync::Notify;

use super::LocalCompactionError as Error;
use crate::experimental_state_machine::EncodedNativeSnapshotMetadata;

#[derive(Clone)]
pub(crate) struct PairIdentity(Arc<()>);

impl PairIdentity {
    pub(crate) fn new() -> Self {
        Self(Arc::new(()))
    }
    pub(crate) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

pub(crate) struct ReceiptData {
    pub(crate) identity: PairIdentity,
    pub(crate) attempt: u64,
    pub(crate) node_id: u64,
    pub(crate) checkpoint: CommittedCheckpoint,
    pub(crate) metadata: EncodedNativeSnapshotMetadata,
    pub(crate) projection: SnapshotMeta<u64, BasicNode>,
}

pub(crate) type Receipt = Arc<ReceiptData>;

struct State {
    terminal: Option<Error>,
    attempt: u64,
    receipt: Option<Receipt>,
    refusal: Option<Error>,
    claimed: bool,
}

struct Shared {
    identity: PairIdentity,
    state: Mutex<State>,
    changed: Notify,
}

#[derive(Clone)]
pub(crate) struct Frontier(Arc<Shared>);

/// Unique publisher: observers and receipts cannot keep it alive.
pub(crate) struct Publisher {
    frontier: Frontier,
}

pub(crate) struct PurgePermit {
    frontier: Frontier,
    receipt: Receipt,
    ordinal: u64,
    baseline_checksum: [u8; 32],
    progress: Vec<u8>,
}

impl Frontier {
    pub(crate) fn new(identity: PairIdentity) -> (Self, Publisher) {
        let frontier = Self(Arc::new(Shared {
            identity,
            state: Mutex::new(State {
                terminal: None,
                attempt: 0,
                receipt: None,
                refusal: None,
                claimed: false,
            }),
            changed: Notify::new(),
        }));
        (frontier.clone(), Publisher { frontier })
    }

    pub(crate) fn begin(&self) -> Result<u64, Error> {
        let mut state = self.0.state.lock().map_err(|_| Error::TaskFailed)?;
        if let Some(error) = state.terminal {
            return Err(error);
        }
        let Some(attempt) = state.attempt.checked_add(1) else {
            state.terminal = Some(Error::Exhausted);
            drop(state);
            self.0.changed.notify_waiters();
            return Err(Error::Exhausted);
        };
        state.attempt = attempt;
        state.receipt = None;
        state.refusal = None;
        state.claimed = false;
        Ok(attempt)
    }

    pub(crate) fn close(&self, error: Error) {
        let mut state = match self.0.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.terminal.is_none() {
            state.terminal = Some(error);
        }
        drop(state);
        self.0.changed.notify_waiters();
    }

    pub(crate) async fn receipt(&self, attempt: u64) -> Result<Receipt, Error> {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.0.state.lock().map_err(|_| Error::TaskFailed)?;
                if let Some(error) = state.terminal {
                    return Err(error);
                }
                if attempt != state.attempt {
                    return Err(Error::InvalidPair);
                }
                if let Some(error) = state.refusal {
                    return Err(error);
                }
                if let Some(receipt) = &state.receipt {
                    return Ok(Arc::clone(receipt));
                }
            }
            changed.await;
        }
    }

    pub(crate) fn permit(
        &self,
        receipt: Receipt,
        ordinal: u64,
        baseline_checksum: [u8; 32],
        progress: Vec<u8>,
    ) -> Result<PurgePermit, Error> {
        let state = self.0.state.lock().map_err(|_| Error::TaskFailed)?;
        if let Some(error) = state.terminal {
            return Err(error);
        }
        if !receipt.identity.same(&self.0.identity)
            || receipt.attempt != state.attempt
            || state
                .receipt
                .as_ref()
                .is_none_or(|current| !Arc::ptr_eq(current, &receipt))
            || state.claimed
        {
            return Err(Error::InvalidPair);
        }
        Ok(PurgePermit {
            frontier: self.clone(),
            receipt,
            ordinal,
            baseline_checksum,
            progress,
        })
    }
}

impl Publisher {
    pub(crate) fn terminal(&self, error: Error) {
        self.frontier.close(error);
    }
    pub(crate) fn allows_build(&self, attempt: u64) -> bool {
        self.frontier.0.state.lock().is_ok_and(|state| {
            state.terminal.is_none()
                && state.attempt == attempt
                && state.receipt.is_none()
                && state.refusal.is_none()
        })
    }
    pub(crate) fn refuse(&self, attempt: u64, error: Error) {
        let mut state = match self.frontier.0.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                drop(poisoned.into_inner());
                self.frontier.close(Error::TaskFailed);
                return;
            }
        };
        if state.terminal.is_none() && state.attempt == attempt && state.receipt.is_none() {
            state.refusal = Some(error);
        }
        drop(state);
        self.frontier.0.changed.notify_waiters();
    }
    pub(crate) fn publish(&self, receipt: Receipt) -> Result<(), Error> {
        let mut state = self
            .frontier
            .0
            .state
            .lock()
            .map_err(|_| Error::TaskFailed)?;
        if let Some(error) = state.terminal {
            return Err(error);
        }
        if !receipt.identity.same(&self.frontier.0.identity)
            || receipt.attempt != state.attempt
            || state.receipt.is_some()
        {
            return Err(Error::InvalidPair);
        }
        state.receipt = Some(receipt);
        drop(state);
        self.frontier.0.changed.notify_waiters();
        Ok(())
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.frontier.close(Error::Closed);
    }
}

impl PurgePermit {
    pub(crate) fn receipt(&self) -> &Receipt {
        &self.receipt
    }
    pub(crate) fn exhausted(self) {
        self.frontier.close(Error::Exhausted);
    }

    /// This is the destructive admission boundary, not queue publication.
    /// No storage call, await, or caller callback runs while the lock is held.
    pub(crate) fn claim(
        self,
        identity: &PairIdentity,
        stream: CommittedStreamId,
        node_id: u64,
        ordinal: u64,
        checksum: [u8; 32],
        progress: &[u8],
    ) -> Result<Receipt, Error> {
        let mut state = self
            .frontier
            .0
            .state
            .lock()
            .map_err(|_| Error::TaskFailed)?;
        if let Some(error) = state.terminal {
            return Err(error);
        }
        if !identity.same(&self.frontier.0.identity)
            || !identity.same(&self.receipt.identity)
            || self.receipt.node_id != node_id
            || self.receipt.checkpoint.stream() != stream
            || self.receipt.attempt != state.attempt
            || state.claimed
            || self.ordinal != ordinal
            || self.baseline_checksum != checksum
            || self.progress != progress
            || state
                .receipt
                .as_ref()
                .is_none_or(|current| !Arc::ptr_eq(current, &self.receipt))
        {
            return Err(Error::InvalidPair);
        }
        state.claimed = true;
        Ok(self.receipt)
    }
}

#[cfg(test)]
mod tests;
