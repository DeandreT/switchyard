use std::sync::{Arc, Mutex};

use flume::{Sender, TrySendError};

use super::{LogStorageError, LogWorkload, MAX_LOG_OWNER_BYTES, MAX_LOG_OWNER_JOBS, owner::Packet};

#[derive(Default)]
struct AdmissionState {
    closed: Option<LogStorageError>,
    jobs: usize,
    bytes: usize,
}

#[derive(Default)]
pub(super) struct Admission {
    state: Mutex<AdmissionState>,
}

impl Admission {
    pub(super) fn enqueue(
        self: &Arc<Self>,
        sender: &Sender<Packet>,
        mut packet: Packet,
    ) -> Result<(), LogStorageError> {
        let bytes = packet.encoded_bytes();
        // Admission, FIFO publication, and shutdown share this gate. In
        // particular, shutdown cannot see an empty queue between claiming a
        // lease and publishing its packet.
        let result = match self.state.lock() {
            Err(_) => Err((LogStorageError::Poisoned, packet)),
            Ok(mut state) => {
                if let Some(error) = state.closed {
                    Err((error, packet))
                } else if state.jobs >= MAX_LOG_OWNER_JOBS
                    || bytes > MAX_LOG_OWNER_BYTES.saturating_sub(state.bytes)
                {
                    Err((LogStorageError::Busy, packet))
                } else {
                    state.jobs += 1;
                    state.bytes += bytes;
                    packet.set_lease(Lease {
                        admission: Arc::clone(self),
                        bytes,
                    });
                    match sender.try_send(packet) {
                        Ok(()) => Ok(()),
                        Err(TrySendError::Full(packet)) => Err((LogStorageError::Busy, packet)),
                        Err(TrySendError::Disconnected(packet)) => {
                            state.closed = Some(LogStorageError::Closed);
                            Err((LogStorageError::Closed, packet))
                        }
                    }
                }
            }
        };
        match result {
            Ok(()) => Ok(()),
            Err((error, packet)) => {
                packet.finish(Err(error));
                Err(error)
            }
        }
    }

    pub(super) fn close(&self, error: LogStorageError) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.closed.is_none() {
            state.closed = Some(error);
        }
    }

    pub(super) fn is_closed(&self) -> bool {
        self.state
            .lock()
            .map_or(true, |state| state.closed.is_some())
    }

    pub(super) fn workload(&self) -> Result<LogWorkload, LogStorageError> {
        let state = self.state.lock().map_err(|_| LogStorageError::Poisoned)?;
        Ok(LogWorkload {
            accepted_jobs: state.jobs,
            encoded_bytes: state.bytes,
        })
    }
}

pub(super) struct Lease {
    admission: Arc<Admission>,
    bytes: usize,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = match self.admission.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.jobs -= 1;
        state.bytes -= self.bytes;
    }
}
