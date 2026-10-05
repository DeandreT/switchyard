use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize},
};

use super::super::retained_session::controls::{Controls as SessionControls, Gate};

#[derive(Default)]
pub(super) struct Controls {
    pub(super) sessions: [Arc<SessionControls>; 2],
    pub(super) admission_ready: [Gate; 2],
    pub(super) session_ready: [Gate; 2],
    pub(super) row_ready: Gate,
    pub(super) close_ack: Gate,
    pub(super) row_panic: AtomicBool,
    pub(super) observed_sessions: AtomicUsize,
    pub(super) observed_rows: AtomicUsize,
    pub(super) worker_stopped: AtomicUsize,
    pub(super) authority_closed: AtomicBool,
    pub(super) receiver_torn_down: AtomicBool,
    pub(super) owner_torn_down: AtomicBool,
}

impl Controls {
    pub(super) fn release_observers(&self) {
        for gate in self.admission_ready.iter().chain(self.session_ready.iter()) {
            gate.release();
        }
        self.row_ready.release();
        self.close_ack.release();
    }
    pub(super) fn release_all(&self) {
        self.release_observers();
        for controls in &self.sessions {
            controls.release_all();
        }
    }
}
