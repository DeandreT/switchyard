/// Fixed recorder-only correlation families, with no attached producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticScopeKind {
    Fixture,
    Listener,
    Connection,
    Actor,
    Reader,
    Session,
    Collector,
    Retirement,
    Write,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticFixtureBoundary {
    Started,
    ClientEnded,
    CleanupStarted,
    CleanupEnded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticConnectionPhase {
    Accepted,
    AdmissionRefused,
    HandshakeStarted,
    HandshakeSucceeded,
    HandshakeFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticFrameClass {
    Heartbeat,
    Open,
    Begin,
    Attach,
    Flow,
    Transfer,
    Disposition,
    TransactionalDisposition,
    Detach,
    End,
    Close,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticEndClass {
    Returned,
    Cancelled,
    Eof,
    ReadError,
    DecodeError,
    FrameError,
    CommandError,
    ReceiveIdle,
    PeerIdle,
    WriteFailed,
    CloseTimeout,
    Panicked,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticRetirementPhase {
    Installed,
    CollectorReceived,
    EventPublished,
    Admitted,
    Refused,
    StagingAccepted,
    StagingRefused,
    ProvisionalRequested,
    Prepared,
    PreparationRefused,
    ReplyPublished,
    ReplyLost,
    OwnerClosed,
    Obsolete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticWriterPhase {
    PreflightStarted,
    PreflightRefused,
    WriteStarted,
    WriteAllDone,
    FlushDone,
    WriteAccepted,
    TimeoutWrite,
    TimeoutFlush,
    TimeoutAfterFlush,
    ErrorWrite,
    ErrorFlush,
    DroppedWrite,
    DroppedFlush,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticTaskPhase {
    Reserved,
    Spawned,
    AbortRequested,
    BodyEnded,
    ActuallyJoined,
    SupervisorInterrupted,
    CapacityRefused,
}

/// Manually supplied bounded schema, not proof of actual wire/task observation.
/// No variant can carry strings, raw IDs, payloads, conditions or arbitrary errors.
///
/// ```compile_fail
/// let _ = amqp::DiagnosticEvent::Ended(String::from("private error"));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticEvent {
    Fixture(DiagnosticFixtureBoundary),
    Connection(DiagnosticConnectionPhase),
    DecodedFrame(DiagnosticFrameClass),
    ActorDispatch(DiagnosticFrameClass),
    Ended(DiagnosticEndClass),
    Retirement(DiagnosticRetirementPhase),
    Writer(DiagnosticWriterPhase),
    Task(DiagnosticTaskPhase),
}

/// An immutable row containing only fresh local correlation and fixed typed data.
///
/// ```compile_fail
/// fn replace(row: &mut amqp::DiagnosticRecord) { row.scope = 42; }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagnosticRecord {
    pub(super) sequence: u64,
    pub(super) scope: u64,
    pub(super) parent: Option<u64>,
    pub(super) kind: DiagnosticScopeKind,
    pub(super) elapsed_millis: u64,
    pub(super) event: DiagnosticEvent,
}

impl DiagnosticRecord {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn scope_ordinal(&self) -> u64 {
        self.scope
    }
    pub fn parent_ordinal(&self) -> Option<u64> {
        self.parent
    }
    pub fn scope_kind(&self) -> DiagnosticScopeKind {
        self.kind
    }
    pub fn elapsed_millis(&self) -> u64 {
        self.elapsed_millis
    }
    pub fn event(&self) -> DiagnosticEvent {
        self.event
    }
}

impl DiagnosticScopeKind {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Fixture => "fixture",
            Self::Listener => "listener",
            Self::Connection => "connection",
            Self::Actor => "actor",
            Self::Reader => "reader",
            Self::Session => "session",
            Self::Collector => "collector",
            Self::Retirement => "retirement",
            Self::Write => "write",
        }
    }
}

impl DiagnosticEvent {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Fixture(DiagnosticFixtureBoundary::Started) => "fixture-started",
            Self::Fixture(DiagnosticFixtureBoundary::ClientEnded) => "client-ended",
            Self::Fixture(DiagnosticFixtureBoundary::CleanupStarted) => "cleanup-started",
            Self::Fixture(DiagnosticFixtureBoundary::CleanupEnded) => "cleanup-ended",
            Self::Connection(DiagnosticConnectionPhase::Accepted) => "connection-accepted",
            Self::Connection(DiagnosticConnectionPhase::AdmissionRefused) => "admission-refused",
            Self::Connection(DiagnosticConnectionPhase::HandshakeStarted) => "handshake-started",
            Self::Connection(DiagnosticConnectionPhase::HandshakeSucceeded) => {
                "handshake-succeeded"
            }
            Self::Connection(DiagnosticConnectionPhase::HandshakeFailed) => "handshake-failed",
            Self::DecodedFrame(frame) => frame.label(),
            Self::ActorDispatch(frame) => frame.dispatch_label(),
            Self::Ended(reason) => reason.label(),
            Self::Retirement(phase) => phase.label(),
            Self::Writer(phase) => phase.label(),
            Self::Task(phase) => phase.label(),
        }
    }
}

macro_rules! labels {
    ($name:ident, $method:ident; $($variant:ident => $label:literal),+ $(,)?) => {
        impl $name {
            fn $method(self) -> &'static str {
                match self { $(Self::$variant => $label),+ }
            }
        }
    };
}

labels!(DiagnosticFrameClass, label;
    Heartbeat => "decoded-heartbeat", Open => "decoded-open", Begin => "decoded-begin",
    Attach => "decoded-attach", Flow => "decoded-flow", Transfer => "decoded-transfer",
    Disposition => "decoded-disposition", TransactionalDisposition => "decoded-transactional-disposition",
    Detach => "decoded-detach", End => "decoded-end", Close => "decoded-close", Other => "decoded-other",
);
labels!(DiagnosticFrameClass, dispatch_label;
    Heartbeat => "dispatch-heartbeat", Open => "dispatch-open", Begin => "dispatch-begin",
    Attach => "dispatch-attach", Flow => "dispatch-flow", Transfer => "dispatch-transfer",
    Disposition => "dispatch-disposition", TransactionalDisposition => "dispatch-transactional-disposition",
    Detach => "dispatch-detach", End => "dispatch-end", Close => "dispatch-close", Other => "dispatch-other",
);
labels!(DiagnosticEndClass, label;
    Returned => "ended-returned", Cancelled => "ended-cancelled", Eof => "ended-eof",
    ReadError => "ended-read-error", DecodeError => "ended-decode-error", FrameError => "ended-frame-error",
    CommandError => "ended-command-error", ReceiveIdle => "ended-receive-idle", PeerIdle => "ended-peer-idle",
    WriteFailed => "ended-write-failed", CloseTimeout => "ended-close-timeout", Panicked => "ended-panicked",
    Other => "ended-other",
);
labels!(DiagnosticRetirementPhase, label;
    Installed => "retirement-installed", CollectorReceived => "retirement-collector-received",
    EventPublished => "retirement-event-published", Admitted => "retirement-admitted",
    Refused => "retirement-refused", StagingAccepted => "retirement-staging-accepted",
    StagingRefused => "retirement-staging-refused", ProvisionalRequested => "retirement-provisional-requested",
    Prepared => "retirement-prepared", PreparationRefused => "retirement-preparation-refused",
    ReplyPublished => "retirement-reply-published", ReplyLost => "retirement-reply-lost",
    OwnerClosed => "retirement-owner-closed", Obsolete => "retirement-obsolete",
);
labels!(DiagnosticWriterPhase, label;
    PreflightStarted => "write-preflight-started", PreflightRefused => "write-preflight-refused",
    WriteStarted => "write-started", WriteAllDone => "write-all-done", FlushDone => "flush-done",
    WriteAccepted => "write-accepted", TimeoutWrite => "timeout-write", TimeoutFlush => "timeout-flush",
    TimeoutAfterFlush => "timeout-after-flush", ErrorWrite => "error-write", ErrorFlush => "error-flush",
    DroppedWrite => "dropped-write", DroppedFlush => "dropped-flush",
);
labels!(DiagnosticTaskPhase, label;
    Reserved => "task-reserved", Spawned => "task-spawned", AbortRequested => "task-abort-requested",
    BodyEnded => "task-body-ended", ActuallyJoined => "task-actually-joined",
    SupervisorInterrupted => "supervisor-interrupted", CapacityRefused => "task-capacity-refused",
);
