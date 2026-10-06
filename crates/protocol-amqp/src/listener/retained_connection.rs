//! One accepted socket's opt-in task ownership, not whole-listener custody.

use std::{
    fmt,
    future::Future,
    io,
    sync::{Arc, Mutex, MutexGuard},
};

use amqp::{
    ServerConnectionAcceptor, ServerConnectionJoinReport, ServerConnectionObservations,
    ServerConnectionOwner,
};
use tokio::{net::TcpStream, runtime::Handle, task::JoinError};

use super::AmqpListener;

#[cfg(test)]
pub(super) mod controls;
#[cfg(test)]
mod observation_tests;
mod outcomes;
pub(super) mod ready;
mod wrapper;

#[cfg(test)]
pub(super) use controls::Controls;
use outcomes::Outcomes;
pub(super) use outcomes::{PrimaryPublisher, Publisher};
use wrapper::{Installation, Wrapper};

pub(super) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// An original result returned by the existing handshake, driver or close boundary.
pub type RetainedConnectionResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// The wrapper's observed primary disposition, independent of WebSocket close.
pub enum RetainedConnectionOutcome {
    /// The existing first-poll short-circuit; no handshake error was constructed.
    SkippedExpiredDeadline,
    /// Negotiation succeeded but the engine launch was sealed; Io was advanced.
    LaunchRefused,
    Finished(RetainedConnectionResult),
}

/// Original results from exactly the wrapper and its optional Actor/Reader.
pub struct RetainedConnectionTaskJoins {
    pub wrapper: Option<Result<(), JoinError>>,
    pub actor: Option<Result<(), JoinError>>,
    pub reader: Option<Result<(), JoinError>>,
    pub native_observations: ServerConnectionObservations,
}

/// Observed values only; missing data is not an invented success or guessed cause.
pub struct RetainedConnectionOutcomes {
    pub primary: Option<RetainedConnectionOutcome>,
    pub websocket_close: Option<RetainedConnectionResult>,
}

/// Externally retain this root and its captured live runtime through finish.
///
/// Construct it BEFORE starting one accepted socket. All created Wrapper, Actor
/// and Reader tokens stay here until their actual joins, in that order. A Handle
/// does not keep a Runtime alive. Drive finish on the captured live runtime; a
/// current-thread runtime also needs its I/O and timers driven.
///
/// The anchor need not be Send or 'static and never enters a task. These three
/// joins do NOT cover socket acceptance, listener/session/link descendants,
/// Broker jobs, certificates, native stores or fixture resources. A report does
/// not certify safe reopen or explain arbitrary internally handled engine failures.
/// Native observations retain only original peer-Close/reply and abort-call data,
/// never a cancellation-cause or health proof.
///
/// Stop is cooperative and adds no wrapper/Actor hard abort or termination bound.
/// Borrowed finish loss restores its token/results. Dropping the unfinished root
/// cannot await and can detach them: root/runtime loss and uncooperative tasks or
/// destructors are unsupported. No automatic rescue or global custody is provided.
///
/// ```compile_fail
/// let (owner, _) = protocol_amqp::RetainedConnectionOwner::new(
///     tokio::runtime::Handle::current(), ());
/// let _private_controls = owner.controls;
/// ```
#[must_use = "retain and finish on the captured live runtime"]
pub struct RetainedConnectionOwner<A> {
    wrapper: Arc<Wrapper>,
    engine: ServerConnectionOwner<A>,
    outcomes: Arc<Outcomes>,
    #[cfg(test)]
    controls: Arc<Controls>,
    reported: bool,
}

/// Unique consuming launch capability, never its wrapper's own handle or anchor.
///
/// Dropping an unused starter creates no role. Original refused/setup requests
/// belong to the caller and are not covered by the owner's empty task report.
///
/// ```compile_fail
/// let (_, starter) = protocol_amqp::RetainedConnectionOwner::new(
///     tokio::runtime::Handle::current(), ());
/// let _copy = starter.clone();
/// ```
#[must_use = "start once while retaining the separate owner"]
pub struct RetainedConnectionStarter {
    wrapper: Arc<Wrapper>,
    acceptor: ServerConnectionAcceptor,
    publisher: Publisher,
    runtime: Handle,
}

/// Opaque report available only after every CREATED covered role actually joined.
///
/// Raw primary/close errors and each original JoinError/panic payload survive all
/// barriers separately. Post-report disposal can still panic. No arbitrary
/// unobserved temporary value or uncovered descendant custody is implied.
///
/// ```compile_fail
/// let _report = protocol_amqp::RetainedConnectionJoinReport {
///     wrapper: None, primary: None, websocket_close: None, anchor: (),
/// };
/// ```
#[must_use]
pub struct RetainedConnectionJoinReport<A> {
    wrapper: Option<Result<(), JoinError>>,
    engine: ServerConnectionJoinReport<A>,
    outcomes: RetainedConnectionOutcomes,
}

impl<A> RetainedConnectionOwner<A> {
    /// Creates a split root/capability, with both data cells before any spawn.
    pub fn new(runtime: Handle, anchor: A) -> (Self, RetainedConnectionStarter) {
        let (engine, acceptor) = ServerConnectionOwner::new(runtime.clone(), anchor);
        let wrapper = Arc::new(Wrapper::new());
        let outcomes = Arc::new(Outcomes::new());
        #[cfg(test)]
        let controls = Arc::new(Controls::default());
        let publisher = outcomes.publisher(
            #[cfg(test)]
            controls.clone(),
        );
        (
            Self {
                wrapper: wrapper.clone(),
                engine,
                outcomes,
                #[cfg(test)]
                controls,
                reported: false,
            },
            RetainedConnectionStarter {
                wrapper,
                acceptor,
                publisher,
                runtime,
            },
        )
    }

    /// Seals launch and forwards existing engine cancellation, without awaiting.
    pub fn stop(&self) {
        self.wrapper.seal();
        self.engine.stop();
    }

    #[cfg(test)]
    pub(super) fn controls(&self) -> Arc<Controls> {
        self.controls.clone()
    }

    #[cfg(test)]
    pub(super) fn abort_wrapper(&self) {
        if let Some(abort) = self.wrapper.abort_handle() {
            abort.abort();
        }
    }

    #[cfg(test)]
    pub(super) fn published(&self) -> (bool, bool) {
        self.outcomes.published()
    }

    /// Borrows/restores each original token and joins Wrapper -> Actor -> Reader.
    ///
    /// Cancellation/timeout leaves the root retained and stopped, not completed.
    /// Drive it again on captured live A. No new abort, deadline or retry policy
    /// is applied. Later cached calls return None, without a fresh health check.
    pub async fn finish(&mut self) -> Option<RetainedConnectionJoinReport<A>> {
        if self.reported {
            return None;
        }
        self.stop();
        self.wrapper.join().await;
        let engine = self.engine.finish().await.expect("unreported engine owner");
        let wrapper = self.wrapper.take_result();
        let outcomes = self.outcomes.take();
        self.reported = true;
        Some(RetainedConnectionJoinReport {
            wrapper,
            engine,
            outcomes,
        })
    }
}

impl<A> Drop for RetainedConnectionOwner<A> {
    fn drop(&mut self) {
        self.stop();
    }
}

impl RetainedConnectionStarter {
    pub(super) fn claim(self) -> Option<(Claim, ServerConnectionAcceptor, Publisher)> {
        if !self.wrapper.claim() {
            return None;
        }
        // This guard precedes every possible pulse/spawn/test callback.
        let claim = Claim {
            installation: Installation::new(self.wrapper),
            runtime: self.runtime,
        };
        claim.installation.pulse();
        Some((claim, self.acceptor, self.publisher))
    }
}

/// Lives only in the synchronous start frame, never in its installed future.
pub(super) struct Claim {
    installation: Installation,
    runtime: Handle,
}

impl Claim {
    pub(super) fn spawn<F>(mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.installation.handle = Some(self.runtime.spawn(future));
    }
}

impl<A> RetainedConnectionJoinReport<A> {
    pub fn wrapper(&self) -> Option<&Result<(), JoinError>> {
        self.wrapper.as_ref()
    }

    pub fn actor(&self) -> Option<&Result<(), JoinError>> {
        self.engine.actor()
    }

    pub fn reader(&self) -> Option<&Result<(), JoinError>> {
        self.engine.reader()
    }

    pub fn native_observations(&self) -> &ServerConnectionObservations {
        self.engine.observations()
    }

    pub fn outcomes(&self) -> &RetainedConnectionOutcomes {
        &self.outcomes
    }

    pub fn anchor(&self) -> &A {
        self.engine.anchor()
    }

    pub fn into_parts(self) -> (RetainedConnectionTaskJoins, RetainedConnectionOutcomes, A) {
        let (engine, anchor) = self.engine.into_parts();
        (
            RetainedConnectionTaskJoins {
                wrapper: self.wrapper,
                actor: engine.actor,
                reader: engine.reader,
                native_observations: engine.observations,
            },
            self.outcomes,
            anchor,
        )
    }
}

/// Pre-spawn failure; Setup preserves the original io::Error and typed cause.
pub enum RetainedConnectionStartCause {
    Setup(io::Error),
    Stopped,
}

/// Original configured listener/socket returned without any new task creation.
///
/// Setup follows original option validation -> TCP_NODELAY -> checked deadline
/// order before stopped refusal. TCP_NODELAY may already be set. No socket read
/// or TLS/HTTP/AMQP negotiation occurred; the caller retains/disposes this request.
pub struct RetainedConnectionRequest<B> {
    pub listener: AmqpListener<B>,
    pub stream: TcpStream,
}

/// Original request and failure, kept separate from created-role task reports.
#[must_use]
pub struct RetainedConnectionStartError<B> {
    cause: RetainedConnectionStartCause,
    request: Box<RetainedConnectionRequest<B>>,
}

impl<B> RetainedConnectionStartError<B> {
    pub fn cause(&self) -> &RetainedConnectionStartCause {
        &self.cause
    }

    pub fn into_parts(self) -> (RetainedConnectionStartCause, RetainedConnectionRequest<B>) {
        (self.cause, *self.request)
    }

    pub(super) fn new(
        cause: RetainedConnectionStartCause,
        listener: AmqpListener<B>,
        stream: TcpStream,
    ) -> Self {
        Self {
            cause,
            request: Box::new(RetainedConnectionRequest { listener, stream }),
        }
    }
}

macro_rules! opaque_debug {
    ($name:ident $(<$parameter:ident>)?) => {
        impl $(<$parameter>)? fmt::Debug for $name $(<$parameter>)? {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.debug_struct(stringify!($name)).finish_non_exhaustive()
            }
        }
    };
}

opaque_debug!(RetainedConnectionOwner<A>);
opaque_debug!(RetainedConnectionStarter);
opaque_debug!(RetainedConnectionJoinReport<A>);
opaque_debug!(RetainedConnectionTaskJoins);
opaque_debug!(RetainedConnectionOutcomes);
opaque_debug!(RetainedConnectionRequest<B>);
opaque_debug!(RetainedConnectionStartError<B>);

impl fmt::Debug for RetainedConnectionOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SkippedExpiredDeadline => "SkippedExpiredDeadline",
            Self::LaunchRefused => "LaunchRefused",
            Self::Finished(_) => "Finished(..)",
        })
    }
}

impl fmt::Debug for RetainedConnectionStartCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Setup(_) => "Setup(..)",
            Self::Stopped => "Stopped",
        })
    }
}
