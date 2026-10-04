use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::{MAX_CLIENT_BYTES, MAX_CLIENT_JOBS, QueueWriteError, QueueWriteRejection};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClientWorkload {
    pub accepted_jobs: usize,
    pub encoded_bytes: usize,
}

#[derive(Default)]
struct State {
    workload: ClientWorkload,
    closed: bool,
}

#[derive(Default)]
pub(super) struct Admission {
    state: Mutex<State>,
    changed: Notify,
}

impl Admission {
    pub(super) fn acquire(self: &Arc<Self>, bytes: usize) -> Result<Lease, QueueWriteError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| QueueWriteError::KnownRejected(QueueWriteRejection::Closed))?;
        if state.closed {
            return Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed));
        }
        let total = state
            .workload
            .encoded_bytes
            .checked_add(bytes)
            .filter(|total| *total <= MAX_CLIENT_BYTES);
        let Some(total) = total.filter(|_| state.workload.accepted_jobs < MAX_CLIENT_JOBS) else {
            return Err(QueueWriteError::KnownRejected(
                QueueWriteRejection::Capacity,
            ));
        };
        state.workload.accepted_jobs += 1;
        state.workload.encoded_bytes = total;
        Ok(Lease {
            admission: Arc::clone(self),
            bytes,
        })
    }

    pub(super) fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
        self.changed.notify_waiters();
    }

    pub(super) fn is_closed(&self) -> bool {
        self.state.lock().map_or(true, |state| state.closed)
    }

    pub(super) fn workload(&self) -> ClientWorkload {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .workload
    }

    pub(super) async fn closed(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_closed() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests;

pub(super) struct Lease {
    admission: Arc<Admission>,
    bytes: usize,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self
            .admission
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match (
            state.workload.accepted_jobs.checked_sub(1),
            state.workload.encoded_bytes.checked_sub(self.bytes),
        ) {
            (Some(jobs), Some(bytes)) => {
                state.workload = ClientWorkload {
                    accepted_jobs: jobs,
                    encoded_bytes: bytes,
                }
            }
            _ => state.closed = true,
        }
    }
}
