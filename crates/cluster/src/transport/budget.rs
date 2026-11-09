use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::consensus::ConsensusIdentity;

use super::{FRAME_HEADER_BYTES, TransportError, TransportLimits};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Usage {
    pub connections: usize,
    pub operations: usize,
    pub bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransportReport {
    pub joined: u64,
    pub dispatched: u64,
    pub written: u64,
    pub refused: u64,
    pub deadlines: u64,
    pub io_errors: u64,
    pub handler_errors: u64,
    pub panics: u64,
    pub cancelled: u64,
    pub retired_queued: u64,
    pub listener_errors: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TransportStatus {
    pub retired: bool,
    pub global: Usage,
    /// Same stable ID order as ConsensusIdentity::voters().
    pub peers: [Usage; 3],
    pub dispatching: usize,
    pub started: u64,
    /// Actual socket WouldBlock observations, not an elapsed-time inference.
    pub blocked_writes: u64,
    pub report: TransportReport,
}

pub(super) struct Shared {
    pub limits: TransportLimits,
    identity: ConsensusIdentity,
    state: Mutex<TransportStatus>,
    updates: watch::Sender<TransportStatus>,
    stop: watch::Sender<bool>,
}

pub(super) type LeaseRef = Arc<Mutex<Lease>>;

pub(super) struct Lease {
    shared: Arc<Shared>,
    peer: Option<usize>,
    bytes: usize,
    pub dispatching: bool,
}

impl Shared {
    pub fn new(identity: ConsensusIdentity, limits: TransportLimits) -> Arc<Self> {
        let (updates, _) = watch::channel(TransportStatus::default());
        let (stop, _) = watch::channel(false);
        Arc::new(Self {
            limits,
            identity,
            state: Mutex::new(TransportStatus::default()),
            updates,
            stop,
        })
    }
    fn publish(&self, state: &TransportStatus) {
        self.updates.send_replace(state.clone());
    }
    pub fn status(&self) -> TransportStatus {
        self.state.lock().expect("budget lock").clone()
    }
    pub fn subscribe(&self) -> watch::Receiver<TransportStatus> {
        self.updates.subscribe()
    }
    pub fn stop_receiver(&self) -> watch::Receiver<bool> {
        self.stop.subscribe()
    }
    pub fn retire(&self) {
        let mut state = self.state.lock().expect("budget lock");
        state.retired = true;
        self.publish(&state);
        self.stop.send_replace(true);
    }
    fn peer(&self, id: u64) -> Result<usize, TransportError> {
        self.identity
            .voters()
            .iter()
            .position(|v| v.id == id)
            .ok_or(TransportError::Identity)
    }
    pub fn can_accept(&self) -> bool {
        let state = self.state.lock().expect("budget lock");
        !state.retired
            && fits(
                state.global,
                FRAME_HEADER_BYTES,
                self.limits.max_connections,
                self.limits.max_operations,
                self.limits.max_bytes,
            )
    }
    pub fn reserve(
        self: &Arc<Self>,
        peer: Option<u64>,
        bytes: usize,
    ) -> Result<LeaseRef, TransportError> {
        let peer = peer.map(|id| self.peer(id)).transpose()?;
        let mut state = self.state.lock().expect("budget lock");
        if state.retired {
            return Err(TransportError::Retired);
        }
        if !fits(
            state.global,
            bytes,
            self.limits.max_connections,
            self.limits.max_operations,
            self.limits.max_bytes,
        ) {
            return Err(TransportError::Busy("global quota"));
        }
        if let Some(p) = peer {
            if !fits(
                state.peers[p],
                bytes,
                self.limits.per_peer_connections,
                self.limits.per_peer_operations,
                self.limits.per_peer_bytes,
            ) {
                return Err(TransportError::Busy("peer quota"));
            }
            add(&mut state.peers[p], bytes);
        }
        add(&mut state.global, bytes);
        self.publish(&state);
        Ok(Arc::new(Mutex::new(Lease {
            shared: self.clone(),
            peer,
            bytes,
            dispatching: false,
        })))
    }
    /// Queue admission and dispatch admission share the same retirement gate.
    pub fn if_live<T>(&self, action: impl FnOnce() -> T) -> Result<T, TransportError> {
        let state = self.state.lock().expect("budget lock");
        if state.retired {
            drop(state);
            return Err(TransportError::Retired);
        }
        Ok(action())
    }
    pub fn blocked_write(&self) {
        let mut state = self.state.lock().expect("budget lock");
        state.blocked_writes = state.blocked_writes.saturating_add(1);
        self.publish(&state);
    }
    pub fn listener_error(&self) {
        let mut state = self.state.lock().expect("budget lock");
        state.report.listener_errors = state.report.listener_errors.saturating_add(1);
        self.publish(&state);
    }
    pub fn retired_queued(&self) {
        let mut state = self.state.lock().expect("budget lock");
        state.report.retired_queued = state.report.retired_queued.saturating_add(1);
        self.publish(&state);
    }
    pub fn joined(&self, dispatched: bool, outbound: bool, result: &Result<(), TransportError>) {
        let mut state = self.state.lock().expect("budget lock");
        let report = &mut state.report;
        report.joined = report.joined.saturating_add(1);
        if dispatched {
            report.dispatched = report.dispatched.saturating_add(1);
        }
        match result {
            Ok(()) if outbound => report.written = report.written.saturating_add(1),
            Ok(()) => {}
            Err(TransportError::TaskPanic) => report.panics = report.panics.saturating_add(1),
            Err(TransportError::TaskCancelled) => {
                report.cancelled = report.cancelled.saturating_add(1)
            }
            Err(_) if dispatched => report.handler_errors = report.handler_errors.saturating_add(1),
            Err(TransportError::Deadline) => report.deadlines = report.deadlines.saturating_add(1),
            Err(TransportError::Io(_)) => report.io_errors = report.io_errors.saturating_add(1),
            Err(_) => report.refused = report.refused.saturating_add(1),
        }
        self.publish(&state);
    }
}

impl Lease {
    /// Header identity is checked before charging a declared peer and allocating
    /// a body. No wait queue can be grown by that peer on quota exhaustion.
    pub fn identify(&mut self, id: u64, bytes: usize) -> Result<(), TransportError> {
        let peer = self.shared.peer(id)?;
        let mut state = self.shared.state.lock().expect("budget lock");
        if state.retired {
            return Err(TransportError::Retired);
        }
        let delta = bytes
            .checked_sub(self.bytes)
            .ok_or(TransportError::Limit("allocation charge"))?;
        if self.peer.is_some()
            || state
                .global
                .bytes
                .checked_add(delta)
                .is_none_or(|b| b > self.shared.limits.max_bytes)
            || !fits(
                state.peers[peer],
                bytes,
                self.shared.limits.per_peer_connections,
                self.shared.limits.per_peer_operations,
                self.shared.limits.per_peer_bytes,
            )
        {
            return Err(TransportError::Busy("inbound peer/byte quota"));
        }
        state.global.bytes += delta;
        add(&mut state.peers[peer], bytes);
        self.peer = Some(peer);
        self.bytes = bytes;
        self.shared.publish(&state);
        Ok(())
    }
    /// The synchronous gate establishes begun dispatch before invoking any
    /// trusted callback code. Retirement never aborts a callback admitted here.
    pub fn begin_dispatch(&mut self) -> Result<(), TransportError> {
        let mut state = self.shared.state.lock().expect("budget lock");
        if state.retired {
            return Err(TransportError::Retired);
        }
        self.dispatching = true;
        state.dispatching += 1;
        state.started = state.started.saturating_add(1);
        self.shared.publish(&state);
        Ok(())
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().expect("budget lock");
        sub(&mut state.global, self.bytes);
        if let Some(peer) = self.peer {
            sub(&mut state.peers[peer], self.bytes);
        }
        if self.dispatching {
            state.dispatching -= 1;
        }
        self.shared.publish(&state);
    }
}

fn fits(
    usage: Usage,
    bytes: usize,
    connections: usize,
    operations: usize,
    maximum_bytes: usize,
) -> bool {
    usage.connections < connections
        && usage.operations < operations
        && usage
            .bytes
            .checked_add(bytes)
            .is_some_and(|v| v <= maximum_bytes)
}
fn add(usage: &mut Usage, bytes: usize) {
    usage.connections += 1;
    usage.operations += 1;
    usage.bytes += bytes;
}
fn sub(usage: &mut Usage, bytes: usize) {
    usage.connections -= 1;
    usage.operations -= 1;
    usage.bytes -= bytes;
}
