use std::{
    collections::TryReserveError,
    error::Error,
    fmt,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

use amqp::{EngineError, ServerSession};
use tokio::{
    sync::Notify,
    task::{Id, JoinError},
};

use super::{admission::Original, collector::SessionExit, worker_history::Packet};
use crate::listener::RetainedConnectionJoinReport;

pub(super) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetainedAtomicMessagingDrain {
    Live,
    PeerEnd,
    External,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetainedAtomicMessagingWorkerBranch {
    Controller,
    CbsRequests,
    CbsReplies,
    Consumer,
    Producer,
}

impl From<super::super::routing::Branch> for RetainedAtomicMessagingWorkerBranch {
    fn from(branch: super::super::routing::Branch) -> Self {
        use super::super::routing::Branch;
        match branch {
            Branch::Controller => Self::Controller,
            Branch::CbsRequests => Self::CbsRequests,
            Branch::CbsReplies => Self::CbsReplies,
            Branch::Consumer => Self::Consumer,
            Branch::Producer => Self::Producer,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetainedAtomicMessagingLimitsError {
    SessionAttempts,
    WorkerHistory,
}

impl fmt::Display for RetainedAtomicMessagingLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SessionAttempts => "session attempt limit must be in 1..=32",
            Self::WorkerHistory => "worker history limit must be in 1..=128",
        })
    }
}
impl Error for RetainedAtomicMessagingLimitsError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetainedAtomicMessagingLimits {
    sessions: usize,
    workers: usize,
}

impl RetainedAtomicMessagingLimits {
    pub fn new(
        session_attempts: usize,
        worker_history: usize,
    ) -> Result<Self, RetainedAtomicMessagingLimitsError> {
        if !(1..=32).contains(&session_attempts) {
            return Err(RetainedAtomicMessagingLimitsError::SessionAttempts);
        }
        if !(1..=128).contains(&worker_history) {
            return Err(RetainedAtomicMessagingLimitsError::WorkerHistory);
        }
        Ok(Self {
            sessions: session_attempts,
            workers: worker_history,
        })
    }
    pub fn session_attempts(&self) -> usize {
        self.sessions
    }
    pub fn worker_history(&self) -> usize {
        self.workers
    }
}

pub struct RetainedAtomicMessagingBuildError<A> {
    pub(super) anchor: A,
    pub(super) reserve: TryReserveError,
}

impl<A> RetainedAtomicMessagingBuildError<A> {
    pub fn anchor(&self) -> &A {
        &self.anchor
    }
    pub fn reserve_error(&self) -> Option<&TryReserveError> {
        Some(&self.reserve)
    }
    pub fn into_parts(self) -> (A, Option<TryReserveError>) {
        (self.anchor, Some(self.reserve))
    }
}
impl<A> fmt::Debug for RetainedAtomicMessagingBuildError<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedAtomicMessagingBuildError")
            .finish_non_exhaustive()
    }
}
impl<A> fmt::Display for RetainedAtomicMessagingBuildError<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("retained atomic messaging history allocation refused")
    }
}
impl<A> Error for RetainedAtomicMessagingBuildError<A> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.reserve)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Reason {
    PeerEnd,
    Expired,
    History,
    Worker,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RetainedAtomicMessagingProgress {
    pub(super) stop_requested: bool,
    pub(super) sealed: bool,
    pub(super) authority_closed: bool,
    pub(super) bound: bool,
    pub(super) bridge_done: bool,
    pub(super) reported: bool,
    pub(super) session_attempts: usize,
    pub(super) session_joins: usize,
    pub(super) worker_launches: usize,
    pub(super) worker_joins: usize,
    pub(super) reason: Option<Reason>,
}

impl RetainedAtomicMessagingProgress {
    pub fn stop_requested(&self) -> bool {
        self.stop_requested
    }
    pub fn authority_closed(&self) -> bool {
        self.authority_closed
    }
    pub fn bound(&self) -> bool {
        self.bound
    }
    pub fn reported(&self) -> bool {
        self.reported
    }
    pub fn session_attempts(&self) -> usize {
        self.session_attempts
    }
    pub fn session_joins(&self) -> usize {
        self.session_joins
    }
    pub fn worker_launches(&self) -> usize {
        self.worker_launches
    }
    pub fn worker_joins(&self) -> usize {
        self.worker_joins
    }
}

pub(super) struct Data {
    state: Mutex<RetainedAtomicMessagingProgress>,
    pub(super) changed: Notify,
}

impl Data {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RetainedAtomicMessagingProgress::default()),
            changed: Notify::new(),
        })
    }
    pub(super) fn progress(&self) -> RetainedAtomicMessagingProgress {
        *locked(&self.state)
    }
    pub(super) fn update(&self, update: impl FnOnce(&mut RetainedAtomicMessagingProgress)) {
        update(&mut locked(&self.state));
        self.changed.notify_waiters();
    }
    pub(super) fn natural(&self, reason: Reason) {
        self.update(|state| {
            state.sealed = true;
            state.reason.get_or_insert(reason);
        });
    }
}

/// Inert observations and requests; driving the retained owner is required.
#[derive(Clone)]
pub struct RetainedAtomicMessagingControl {
    pub(super) data: Arc<Data>,
}

impl RetainedAtomicMessagingControl {
    pub fn request_stop(&self) {
        self.data.update(|state| state.stop_requested = true);
    }
    pub fn progress(&self) -> RetainedAtomicMessagingProgress {
        self.data.progress()
    }
}
impl fmt::Debug for RetainedAtomicMessagingControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedAtomicMessagingControl")
            .field("progress", &self.progress())
            .finish()
    }
}

pub struct RetainedAtomicMessagingAdmissionOutcome {
    pub(super) ordinal: usize,
    pub(super) error: Option<EngineError>,
    pub(super) launched: Option<(Id, usize)>,
    pub(super) _original: Option<Original>,
    pub(super) _incoming: Option<amqp::IncomingSession>,
    pub(super) _unlaunched: Option<ServerSession>,
}
impl RetainedAtomicMessagingAdmissionOutcome {
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }
    pub fn engine_error(&self) -> Option<&EngineError> {
        self.error.as_ref()
    }
    pub fn launched(&self) -> Option<(Id, usize)> {
        self.launched
    }
}
impl fmt::Debug for RetainedAtomicMessagingAdmissionOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedAtomicMessagingAdmissionOutcome")
            .field("ordinal", &self.ordinal)
            .field("launched", &self.launched.is_some())
            .field("error", &self.error.is_some())
            .finish_non_exhaustive()
    }
}

pub struct RetainedAtomicMessagingSessionOutcome {
    pub(super) id: Id,
    pub(super) ordinal: usize,
    pub(super) abort_requested: bool,
    pub(super) drain: RetainedAtomicMessagingDrain,
    pub(super) original: Result<SessionExit, JoinError>,
}
impl RetainedAtomicMessagingSessionOutcome {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }
    pub fn abort_requested(&self) -> bool {
        self.abort_requested
    }
    pub fn drain(&self) -> RetainedAtomicMessagingDrain {
        self.drain
    }
    pub fn join_error(&self) -> Option<&JoinError> {
        self.original.as_ref().err()
    }
    pub fn routing_error(&self) -> Option<&(dyn Error + Send + Sync + 'static)> {
        match &self.original {
            Ok(SessionExit::Routing(error)) => Some(error.as_ref()),
            Ok(SessionExit::Closed(reason)) => Some(reason),
            _ => None,
        }
    }
    pub fn worker_failure_index(&self) -> Option<usize> {
        match &self.original {
            Ok(SessionExit::WorkerFailed(row)) => Some(*row),
            _ => None,
        }
    }
    pub fn is_completed(&self) -> bool {
        matches!(self.original, Ok(SessionExit::Completed))
    }
}
impl fmt::Debug for RetainedAtomicMessagingSessionOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedAtomicMessagingSessionOutcome")
            .field("id", &self.id)
            .field("ordinal", &self.ordinal)
            .field("abort_requested", &self.abort_requested)
            .field("drain", &self.drain)
            .field("completed", &self.is_completed())
            .finish_non_exhaustive()
    }
}

pub struct RetainedAtomicMessagingWorkerOutcome {
    pub(super) id: Id,
    pub(super) ordinal: usize,
    pub(super) branch: RetainedAtomicMessagingWorkerBranch,
    pub(super) abort_requested: bool,
    pub(super) drain: RetainedAtomicMessagingDrain,
    pub(super) original: Result<Result<(), super::super::IngressError>, JoinError>,
}
impl RetainedAtomicMessagingWorkerOutcome {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }
    pub fn branch(&self) -> RetainedAtomicMessagingWorkerBranch {
        self.branch
    }
    pub fn abort_requested(&self) -> bool {
        self.abort_requested
    }
    pub fn drain(&self) -> RetainedAtomicMessagingDrain {
        self.drain
    }
    pub fn join_error(&self) -> Option<&JoinError> {
        self.original.as_ref().err()
    }
    pub fn worker_error(&self) -> Option<&(dyn Error + Send + Sync + 'static)> {
        match &self.original {
            Ok(Err(error)) => Some(error.as_ref()),
            _ => None,
        }
    }
}
impl fmt::Debug for RetainedAtomicMessagingWorkerOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedAtomicMessagingWorkerOutcome")
            .field("id", &self.id)
            .field("ordinal", &self.ordinal)
            .field("branch", &self.branch)
            .field("abort_requested", &self.abort_requested)
            .field("drain", &self.drain)
            .finish_non_exhaustive()
    }
}

/// Original covered-role results. This is not broker health or a cleanup certificate.
pub struct RetainedAtomicMessagingReport<A> {
    pub(super) socket: RetainedConnectionJoinReport<()>,
    pub(super) anchor: A,
    pub(super) admissions: Vec<RetainedAtomicMessagingAdmissionOutcome>,
    pub(super) sessions: Vec<RetainedAtomicMessagingSessionOutcome>,
    pub(super) packets: Vec<Packet>,
    pub(super) close: Arc<OnceLock<Result<(), EngineError>>>,
    pub(super) attempts: usize,
}
impl<A> RetainedAtomicMessagingReport<A> {
    pub fn socket(&self) -> &RetainedConnectionJoinReport<()> {
        &self.socket
    }
    pub fn anchor(&self) -> &A {
        &self.anchor
    }
    pub fn admissions(&self) -> &[RetainedAtomicMessagingAdmissionOutcome] {
        &self.admissions
    }
    pub fn sessions(&self) -> &[RetainedAtomicMessagingSessionOutcome] {
        &self.sessions
    }
    pub fn workers(&self) -> impl Iterator<Item = &RetainedAtomicMessagingWorkerOutcome> {
        self.packets.iter().flat_map(|packet| packet.rows.iter())
    }
    /// None means no completed native Close result, not an invented success.
    pub fn native_close(&self) -> Option<&Result<(), EngineError>> {
        self.close.get()
    }
    pub fn session_attempts(&self) -> usize {
        self.attempts
    }
    pub fn session_joins(&self) -> usize {
        self.sessions.len()
    }
    pub fn worker_launches(&self) -> usize {
        self.packets
            .iter()
            .map(|packet| packet.launches.len())
            .sum()
    }
    pub fn worker_joins(&self) -> usize {
        self.workers().count()
    }
    /// Includes every raw cancellation error, even caller-requested cleanup.
    pub fn has_failures(&self) -> bool {
        let socket = &self.socket;
        [socket.wrapper(), socket.actor(), socket.reader()]
            .into_iter()
            .flatten()
            .any(Result::is_err)
            || socket.outcomes().primary.as_ref().is_some_and(|value| {
                matches!(
                    value,
                    crate::listener::RetainedConnectionOutcome::Finished(Err(_))
                )
            })
            || socket
                .outcomes()
                .websocket_close
                .as_ref()
                .is_some_and(Result::is_err)
            || self.native_close().is_some_and(Result::is_err)
            || self.admissions.iter().any(|row| row.error.is_some())
            || self.sessions.iter().any(|row| !row.is_completed())
            || self
                .workers()
                .any(|row| !matches!(row.original, Ok(Ok(()))))
    }
}
impl<A> fmt::Debug for RetainedAtomicMessagingReport<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedAtomicMessagingReport")
            .field("session_attempts", &self.attempts)
            .field("session_joins", &self.session_joins())
            .field("worker_launches", &self.worker_launches())
            .field("worker_joins", &self.worker_joins())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct Gate {
    state: Mutex<(bool, bool, Option<std::task::Waker>)>,
    changed: Notify,
}
#[cfg(test)]
impl Gate {
    pub(super) fn arm(&self) {
        *locked(&self.state) = (true, false, None);
    }
    pub(super) fn entered(&self) -> bool {
        locked(&self.state).1
    }
    pub(super) fn release(&self) {
        let wake = {
            let mut state = locked(&self.state);
            state.0 = false;
            state.2.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        self.changed.notify_waiters();
    }
    pub(super) fn poll(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        let mut state = locked(&self.state);
        if !state.0 {
            return std::task::Poll::Ready(());
        }
        state.1 = true;
        state.2 = Some(cx.waker().clone());
        self.changed.notify_waiters();
        std::task::Poll::Pending
    }
    pub(super) async fn hold(&self) {
        std::future::poll_fn(|cx| self.poll(cx)).await
    }
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct Hooks {
    pub(super) binding: Gate,
    pub(super) discovery_ready: Gate,
    pub(super) conversion: Gate,
    pub(super) admission_ready: Gate,
    pub(super) session_ready: Gate,
    pub(super) worker_ready: Gate,
    pub(super) worker_start: Gate,
    pub(super) wrapper_return: Gate,
    pub(super) worker_fault: Mutex<Option<super::super::IngressError>>,
    pub(super) session_fault: Mutex<Option<super::super::IngressError>>,
    pub(super) worker_panic: Mutex<Option<Box<dyn std::any::Any + Send>>>,
    pub(super) session_panic: std::sync::atomic::AtomicBool,
    pub(super) worker_ids: Mutex<Vec<Id>>,
    pub(super) native_address: std::sync::atomic::AtomicUsize,
}
#[cfg(test)]
impl Hooks {
    pub(super) fn release(&self) {
        for gate in [
            &self.binding,
            &self.discovery_ready,
            &self.conversion,
            &self.admission_ready,
            &self.session_ready,
            &self.worker_ready,
            &self.worker_start,
            &self.wrapper_return,
        ] {
            gate.release();
        }
    }
}
