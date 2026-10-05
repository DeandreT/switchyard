//! Opt-in typed diagnostic storage, not an attached protocol observer.

use std::{
    fmt,
    sync::{
        Arc, Mutex, TryLockError,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

mod capture;
mod schema;

pub use capture::{DiagnosticCapture, MAX_SERVER_DIAGNOSTIC_FORMAT_BYTES};
pub use schema::{
    DiagnosticConnectionPhase, DiagnosticEndClass, DiagnosticEvent, DiagnosticFixtureBoundary,
    DiagnosticFrameClass, DiagnosticRecord, DiagnosticRetirementPhase, DiagnosticScopeKind,
    DiagnosticTaskPhase, DiagnosticWriterPhase,
};

pub const MAX_SERVER_DIAGNOSTIC_EVENTS: usize = 4096;

/// Static recorder failures; no error contains caller or backend text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DiagnosticRefusal {
    #[error("diagnostic storage allocation was refused")]
    Allocation,
    #[error("the diagnostic recorder is occupied")]
    Contended,
    #[error("the diagnostic recorder is poisoned")]
    Poisoned,
    #[error("the diagnostic event buffer is full")]
    Full,
    #[error("fresh diagnostic correlation is exhausted")]
    OrdinalExhausted,
    #[error("the diagnostic scope belongs to a different recorder")]
    ForeignScope,
    #[error("the diagnostic capture exceeds its formatting limit")]
    FormatLimit,
}

/// Saturating independent loss observations, not lifecycle coverage or health.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DiagnosticLossSummary {
    pub buffer_full: u64,
    pub contention: u64,
    pub poisoned: u64,
    pub ordinal_exhausted: u64,
    pub foreign_scope: u64,
    pub capture_allocation: u64,
}

#[derive(Default)]
struct Losses {
    buffer_full: AtomicU64,
    contention: AtomicU64,
    poisoned: AtomicU64,
    ordinal_exhausted: AtomicU64,
    foreign_scope: AtomicU64,
    capture_allocation: AtomicU64,
}

impl Losses {
    fn read(&self) -> DiagnosticLossSummary {
        DiagnosticLossSummary {
            buffer_full: self.buffer_full.load(Ordering::Relaxed),
            contention: self.contention.load(Ordering::Relaxed),
            poisoned: self.poisoned.load(Ordering::Relaxed),
            ordinal_exhausted: self.ordinal_exhausted.load(Ordering::Relaxed),
            foreign_scope: self.foreign_scope.load(Ordering::Relaxed),
            capture_allocation: self.capture_allocation.load(Ordering::Relaxed),
        }
    }
}

fn lose(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
        Some(count.saturating_add(1))
    });
}

struct State {
    records: Mutex<Vec<DiagnosticRecord>>,
    last_scope: AtomicU64,
    losses: Losses,
}

/// Explicitly allocated finite recorder, with no network or task integration.
///
/// This owns only typed rows/counters. It has no native identity, connection,
/// socket, message, broker or store handle and spawns nothing. Its events are
/// caller-supplied observations, not evidence that a protocol boundary happened.
/// Zero loss or absence does not establish complete coverage or external health.
#[derive(Clone)]
pub struct ServerDiagnosticRecorder(Arc<State>);

/// Fresh recorder-local correlation, not a wire ID or protocol authority.
///
/// Cloning retains only diagnostic storage. The private provenance refuses use
/// with a different recorder. Ordinals never wrap or become externally chosen.
///
/// ```compile_fail
/// let _ = amqp::DiagnosticScope { ordinal: 42 };
/// ```
///
/// ```compile_fail
/// fn replace(scope: &mut amqp::DiagnosticScope) { scope.ordinal = 42; }
/// ```
#[derive(Clone)]
pub struct DiagnosticScope {
    state: Arc<State>,
    ordinal: u64,
    parent: Option<u64>,
    kind: DiagnosticScopeKind,
}

impl ServerDiagnosticRecorder {
    /// Reserve the entire 4096-row logical limit before any publication.
    ///
    /// The event vector's allocation is fallible. Arc/allocator bookkeeping and
    /// spare capacity are not universal OOM safety or an aggregate RSS limit.
    pub fn try_new() -> Result<Self, DiagnosticRefusal> {
        let mut records = Vec::new();
        reserve(&mut records, MAX_SERVER_DIAGNOSTIC_EVENTS)?;
        Ok(Self(Arc::new(State {
            records: Mutex::new(records),
            last_scope: AtomicU64::new(0),
            losses: Losses::default(),
        })))
    }

    /// Allocate fresh opaque correlation without a clock read or payload copy.
    pub fn scope(
        &self,
        kind: DiagnosticScopeKind,
        parent: Option<&DiagnosticScope>,
    ) -> Result<DiagnosticScope, DiagnosticRefusal> {
        if parent.is_some_and(|scope| !Arc::ptr_eq(&self.0, &scope.state)) {
            lose(&self.0.losses.foreign_scope);
            return Err(DiagnosticRefusal::ForeignScope);
        }
        let previous = self
            .0
            .last_scope
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
                last.checked_add(1)
            })
            .map_err(|_| {
                lose(&self.0.losses.ordinal_exhausted);
                DiagnosticRefusal::OrdinalExhausted
            })?;
        Ok(DiagnosticScope {
            state: Arc::clone(&self.0),
            ordinal: previous + 1,
            parent: parent.map(|scope| scope.ordinal),
            kind,
        })
    }

    /// Publish already typed Copy data; elapsed time is supplied, not clock-read.
    ///
    /// No allocation, formatting, callbacks, I/O or await runs under the lock.
    /// A concurrent producer may be refused rather than waited on. Publication
    /// order is not a causal clock; elapsed milliseconds saturate at u64::MAX.
    pub fn record(
        &self,
        scope: &DiagnosticScope,
        event: DiagnosticEvent,
        elapsed: Duration,
    ) -> Result<(), DiagnosticRefusal> {
        if !Arc::ptr_eq(&self.0, &scope.state) {
            lose(&self.0.losses.foreign_scope);
            return Err(DiagnosticRefusal::ForeignScope);
        }
        let elapsed_millis = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        let mut records = self.0.records.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => {
                lose(&self.0.losses.contention);
                DiagnosticRefusal::Contended
            }
            TryLockError::Poisoned(_) => {
                lose(&self.0.losses.poisoned);
                DiagnosticRefusal::Poisoned
            }
        })?;
        if records.len() == MAX_SERVER_DIAGNOSTIC_EVENTS {
            lose(&self.0.losses.buffer_full);
            return Err(DiagnosticRefusal::Full);
        }
        let sequence = records.len() as u64 + 1;
        records.push(DiagnosticRecord {
            sequence,
            scope: scope.ordinal,
            parent: scope.parent,
            kind: scope.kind,
            elapsed_millis,
            event,
        });
        Ok(())
    }

    /// Read independent loss counters without acquiring the event-buffer lock.
    /// Counters are individually observed, not one atomic cross-counter snapshot.
    pub fn loss_summary(&self) -> DiagnosticLossSummary {
        self.0.losses.read()
    }

    /// Copy immutable bounded rows after fallible output reservation outside locks.
    ///
    /// Rows represent one locked view. Concurrent loss counters are independent
    /// observations and may continue changing after this capture. A captured
    /// event family never certifies an attached producer or lifecycle coverage.
    pub fn capture(&self) -> Result<DiagnosticCapture, DiagnosticRefusal> {
        let mut output = Vec::new();
        reserve(&mut output, MAX_SERVER_DIAGNOSTIC_EVENTS).inspect_err(|_| {
            lose(&self.0.losses.capture_allocation);
        })?;
        let records = self.0.records.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => {
                lose(&self.0.losses.contention);
                DiagnosticRefusal::Contended
            }
            TryLockError::Poisoned(_) => {
                lose(&self.0.losses.poisoned);
                DiagnosticRefusal::Poisoned
            }
        })?;
        output.extend_from_slice(&records);
        let losses = self.loss_summary();
        drop(records);
        Ok(DiagnosticCapture::new(output, losses))
    }
}

fn reserve<T>(output: &mut Vec<T>, capacity: usize) -> Result<(), DiagnosticRefusal> {
    output
        .try_reserve_exact(capacity)
        .map_err(|_| DiagnosticRefusal::Allocation)
}

impl fmt::Debug for ServerDiagnosticRecorder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerDiagnosticRecorder")
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for DiagnosticScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiagnosticScope")
            .field("kind", &self.kind)
            .field("ordinal", &self.ordinal)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
