use std::sync::{Arc, Mutex};

use flume::{Sender, TrySendError};

use super::{
    MAX_STATE_MACHINE_OWNER_BYTES, MAX_STATE_MACHINE_OWNER_JOBS, StateMachineError,
    StateMachineWorkload, owner::Packet,
};

#[derive(Default)]
struct AdmissionState {
    closed: Option<StateMachineError>,
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
    ) -> Result<(), StateMachineError> {
        let bytes = packet.encoded_bytes();
        // Publication and shutdown share the gate, including the interval
        // between claiming capacity and making its packet visible to the owner.
        let result = match self.state.lock() {
            Err(_) => Err((StateMachineError::Poisoned, packet)),
            Ok(mut state) => {
                if let Some(error) = state.closed {
                    Err((error, packet))
                } else if state.jobs >= MAX_STATE_MACHINE_OWNER_JOBS
                    || bytes > MAX_STATE_MACHINE_OWNER_BYTES.saturating_sub(state.bytes)
                {
                    Err((StateMachineError::Busy, packet))
                } else {
                    state.jobs += 1;
                    state.bytes += bytes;
                    packet.set_lease(Lease {
                        admission: Arc::clone(self),
                        bytes,
                    });
                    match sender.try_send(packet) {
                        Ok(()) => Ok(()),
                        Err(TrySendError::Full(packet)) => Err((StateMachineError::Busy, packet)),
                        Err(TrySendError::Disconnected(packet)) => {
                            state.closed = Some(StateMachineError::Closed);
                            Err((StateMachineError::Closed, packet))
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

    pub(super) fn close(&self, error: StateMachineError) {
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

    pub(super) fn workload(&self) -> Result<StateMachineWorkload, StateMachineError> {
        let state = self.state.lock().map_err(|_| StateMachineError::Poisoned)?;
        Ok(StateMachineWorkload {
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
