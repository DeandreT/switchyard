//! Opt-in actual socket-task custody, conditional on a retained external root.
//!
//! The root and captured runtime must remain live until borrowed finish completes.
//! This does not own an acceptance parent, listener, session/link tasks or a fixture.

#[cfg(test)]
mod controls;
mod role;
#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::{runtime::Handle, sync::watch, task::JoinError};

#[cfg(test)]
use super::NativeConnectionIdentity;
use super::connection_launch::{ActorBirth, NegotiatedConnection};
use super::{
    ConnectionLifecycle, ConnectionOptions, EngineError, NativeIngressPolicy, SaslAuthenticator,
    ServerConnection,
};
#[cfg(test)]
use controls::Controls;
#[cfg(test)]
pub(super) use controls::FinalGuard;
use role::{Role, State};

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Default)]
struct Stop {
    sealed: bool,
    cancellation: Option<watch::Sender<bool>>,
}

#[cfg(test)]
#[derive(Default)]
struct Observations {
    identity: Option<NativeConnectionIdentity>,
    terminated: Option<watch::Receiver<bool>>,
}

/// The retained owner of one server Actor and optional Reader.
///
/// Construct this root before starting acceptance and retain both the root and
/// its runtime until finish completes. A Handle alone does not keep a runtime
/// alive. Finish must be driven on that live runtime; a current-thread runtime
/// must also be driven for its I/O and timers.
///
/// An anchor need not be Send or 'static and is never captured by either task.
/// Joining these two tasks does not certify that an anchor, provider or Broker
/// has no other users. Acceptance futures/parents and their transports are NOT
/// covered. Stop seals launch but does not cancel an outstanding negotiation.
///
/// Dropping an unfinished owner requests cancellation but cannot join: stored
/// tokens can detach. Root/runtime loss, uncooperative work/destructors and process
/// abort are unsupported. There is no automatic rescuer or termination deadline.
/// Test controls are never public or enabled in normal dependency builds.
///
/// ```compile_fail
/// let (owner, _) = amqp::ServerConnectionOwner::new(
///     tokio::runtime::Handle::current(), ());
/// let _private_controls = owner.controls;
/// ```
#[must_use = "retain and finish this owner on its captured live runtime"]
pub struct ServerConnectionOwner<A> {
    actor: Arc<Role>,
    reader: Arc<Role>,
    stop: Arc<Mutex<Stop>>,
    #[cfg(test)]
    observations: Arc<Mutex<Observations>>,
    runtime: Handle,
    #[cfg(test)]
    controls: Arc<Controls>,
    anchor: Option<A>,
    reported: bool,
}

/// A unique, consuming server-acceptance capability, separate from its root.
///
/// Acceptance performs the existing negotiation, then atomically claims the
/// Actor. A sealed root returns the original advanced transport, not an invented
/// handshake error. Negotiation errors retain their original typed causes.
///
/// This capability is deliberately not Clone.
///
/// ```compile_fail
/// let (_, acceptor) = amqp::ServerConnectionOwner::new(
///     tokio::runtime::Handle::current(), ());
/// let _duplicate = acceptor.clone();
/// ```
#[must_use = "accept once, while retaining the separate connection owner"]
pub struct ServerConnectionAcceptor {
    actor: Arc<Role>,
    reader: Arc<Role>,
    stop: Arc<Mutex<Stop>>,
    #[cfg(test)]
    observations: Arc<Mutex<Observations>>,
    runtime: Handle,
    #[cfg(test)]
    controls: Arc<Controls>,
}

/// The result of successful negotiation, independently of launch admission.
pub enum ScopedConnectionAcceptance<Io> {
    Accepted(ServerConnection),
    Refused(RefusedServerConnection<Io>),
}

/// A transport whose successful negotiation was followed by a sealed launch.
///
/// No Actor, Reader or connection identity was created. The transport has already
/// exchanged negotiation bytes; it is not fresh and cannot restart negotiation.
/// Its disposal belongs to the caller/acceptance parent, not the socket-task root.
pub struct RefusedServerConnection<Io>(NegotiatedConnection<Io>);

impl<Io> RefusedServerConnection<Io> {
    pub fn into_transport(self) -> Io {
        self.0.into_transport()
    }
}

/// Original results from the fixed two roles; None means that role was not created.
pub struct ServerConnectionTaskJoins {
    pub actor: Option<Result<(), JoinError>>,
    pub reader: Option<Result<(), JoinError>>,
}

/// An opaque report emitted only after all CREATED socket tasks actually joined.
///
/// Identity retirement, exit notifications, abort requests and driver return are
/// not this barrier. Internally handled Actor I/O errors remain internal: these
/// original unit-task results cannot newly explain them. Raw panic payloads and
/// anchor were retained until both joins; dropping them afterward can still panic.
///
/// ```compile_fail
/// let _report = amqp::ServerConnectionJoinReport {
///     actor: None, reader: None, anchor: (),
/// };
/// ```
#[must_use]
pub struct ServerConnectionJoinReport<A> {
    actor: Option<Result<(), JoinError>>,
    reader: Option<Result<(), JoinError>>,
    anchor: A,
}

impl<A> ServerConnectionJoinReport<A> {
    pub fn actor(&self) -> Option<&Result<(), JoinError>> {
        self.actor.as_ref()
    }

    pub fn reader(&self) -> Option<&Result<(), JoinError>> {
        self.reader.as_ref()
    }

    pub fn anchor(&self) -> &A {
        &self.anchor
    }

    pub fn into_parts(self) -> (ServerConnectionTaskJoins, A) {
        (
            ServerConnectionTaskJoins {
                actor: self.actor,
                reader: self.reader,
            },
            self.anchor,
        )
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

opaque_debug!(ServerConnectionOwner<A>);
opaque_debug!(ServerConnectionAcceptor);
opaque_debug!(ServerConnectionJoinReport<A>);
opaque_debug!(ServerConnectionTaskJoins);
opaque_debug!(RefusedServerConnection<Io>);

impl<Io> fmt::Debug for ScopedConnectionAcceptance<Io> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accepted(_) => formatter.write_str("ScopedConnectionAcceptance::Accepted(..)"),
            Self::Refused(_) => formatter.write_str("ScopedConnectionAcceptance::Refused(..)"),
        }
    }
}

impl<A> ServerConnectionOwner<A> {
    /// Creates the root and its one independent acceptance capability; spawns nothing.
    pub fn new(runtime: Handle, anchor: A) -> (Self, ServerConnectionAcceptor) {
        let owner = Self {
            actor: Arc::new(Role::new()),
            reader: Arc::new(Role::new()),
            stop: Arc::new(Mutex::new(Stop::default())),
            #[cfg(test)]
            observations: Arc::new(Mutex::new(Observations::default())),
            runtime,
            #[cfg(test)]
            controls: Arc::new(Controls::default()),
            anchor: Some(anchor),
            reported: false,
        };
        let acceptor = owner.acceptor();
        (owner, acceptor)
    }

    fn acceptor(&self) -> ServerConnectionAcceptor {
        ServerConnectionAcceptor {
            actor: self.actor.clone(),
            reader: self.reader.clone(),
            stop: self.stop.clone(),
            #[cfg(test)]
            observations: self.observations.clone(),
            runtime: self.runtime.clone(),
            #[cfg(test)]
            controls: self.controls.clone(),
        }
    }

    /// Stickily seals future launch/Reader claims and requests existing cancellation.
    ///
    /// This neither cancels negotiation nor observes actual task completion.
    pub fn stop(&self) {
        let cancellation = {
            let mut stop = locked(&self.stop);
            stop.sealed = true;
            stop.cancellation.clone()
        };
        if let Some(cancellation) = cancellation {
            let _ = cancellation.send(true);
        }
    }

    /// Joins each created ORIGINAL token, Actor before Reader, retaining all results.
    ///
    /// Dropping/cancelling this borrowed waiter restores pending tokens/results to
    /// the still-retained root. Drive it again on the captured live runtime.
    /// Cooperative Actor cancellation can remain incomplete indefinitely.
    /// A subsequent cached call returns None and does no fresh health check.
    pub async fn finish(&mut self) -> Option<ServerConnectionJoinReport<A>> {
        if self.reported {
            return None;
        }
        self.stop();
        // The actual actor barrier eliminates its only reader creator/borrower.
        self.actor.join(false, true).await;
        self.reader.join(true, true).await;
        let actor = self.actor.take_result();
        let reader = self.reader.take_result();
        self.reported = true;
        Some(ServerConnectionJoinReport {
            actor,
            reader,
            anchor: self.anchor.take().expect("live anchor"),
        })
    }

    #[cfg(test)]
    fn new_for_test(runtime: Handle, anchor: A) -> Self {
        Self::new(runtime, anchor).0
    }

    #[cfg(test)]
    fn launch<Io>(
        &self,
        negotiated: NegotiatedConnection<Io>,
    ) -> Result<ServerConnection, RefusedServerConnection<Io>>
    where
        Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        self.acceptor().launch(negotiated)
    }
}

impl<A> Drop for ServerConnectionOwner<A> {
    fn drop(&mut self) {
        self.stop();
    }
}

impl ServerConnectionAcceptor {
    /// Ordinary server acceptance; native transactions remain disabled.
    pub async fn accept_with_options<Io>(
        self,
        stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
        options: ConnectionOptions,
    ) -> Result<ScopedConnectionAcceptance<Io>, EngineError>
    where
        Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        self.accept(
            stream,
            container_id,
            sasl,
            options,
            NativeIngressPolicy::Disabled,
        )
        .await
    }

    /// Opt-in native posting policy, matching the existing ingress acceptance API.
    pub async fn accept_with_transactional_ingress<Io>(
        self,
        stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
        options: ConnectionOptions,
    ) -> Result<ScopedConnectionAcceptance<Io>, EngineError>
    where
        Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        self.accept(
            stream,
            container_id,
            sasl,
            options,
            NativeIngressPolicy::Posting,
        )
        .await
    }

    /// Opt-in native work with the same narrow coordinator default as the existing API.
    ///
    /// This does not enable SDK transaction scopes or change ordinary acceptance.
    pub async fn accept_with_transactional_work_defaults<Io>(
        self,
        stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
        options: ConnectionOptions,
    ) -> Result<ScopedConnectionAcceptance<Io>, EngineError>
    where
        Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        self.accept(
            stream,
            container_id,
            sasl,
            options,
            NativeIngressPolicy::WorkDefaults,
        )
        .await
    }

    async fn accept<Io>(
        self,
        stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
        options: ConnectionOptions,
        policy: NativeIngressPolicy,
    ) -> Result<ScopedConnectionAcceptance<Io>, EngineError>
    where
        Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        let negotiated =
            super::connection_launch::negotiate(stream, container_id, sasl, options, policy)
                .await?;
        Ok(match self.launch(negotiated) {
            Ok(connection) => ScopedConnectionAcceptance::Accepted(connection),
            Err(refused) => ScopedConnectionAcceptance::Refused(refused),
        })
    }

    fn launch<Io>(
        self,
        negotiated: NegotiatedConnection<Io>,
    ) -> Result<ServerConnection, RefusedServerConnection<Io>>
    where
        Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        let accepted = {
            let stop = locked(&self.stop);
            let mut actor = locked(&self.actor.state);
            if stop.sealed || !matches!(*actor, State::Dormant) {
                false
            } else {
                *actor = State::Claimed;
                true
            }
        };
        if !accepted {
            return Err(RefusedServerConnection(negotiated));
        }
        let claim = ActorClaim {
            installation: role::Installation::new(self.actor.clone()),
            reader: self.reader,
            stop: self.stop,
            #[cfg(test)]
            observations: self.observations,
            runtime: self.runtime,
            #[cfg(test)]
            controls: self.controls,
        };
        self.actor.pulse();
        Ok(negotiated.launch(ActorBirth::Scoped(claim)))
    }
}

/// Lives in the synchronous launcher, never in the actor future it installs.
pub(super) struct ActorClaim {
    installation: role::Installation,
    reader: Arc<Role>,
    stop: Arc<Mutex<Stop>>,
    #[cfg(test)]
    observations: Arc<Mutex<Observations>>,
    runtime: Handle,
    #[cfg(test)]
    controls: Arc<Controls>,
}

impl ActorClaim {
    pub(super) fn bind(&self, lifecycle: &ConnectionLifecycle) {
        #[cfg(test)]
        self.controls.before_bind();
        let stopped = {
            let mut stop = locked(&self.stop);
            stop.cancellation = Some(lifecycle.cancellation.clone());
            stop.sealed
        };
        #[cfg(test)]
        {
            let mut observations = locked(&self.observations);
            observations.identity = Some(lifecycle.identity.clone());
            observations.terminated = Some(lifecycle.terminated.clone());
        }
        if stopped {
            let _ = lifecycle.cancellation.send(true);
        }
    }

    pub(super) fn reader_birth(&self) -> ReaderBirth {
        ReaderBirth {
            role: self.reader.clone(),
            stop: self.stop.clone(),
            runtime: self.runtime.clone(),
            #[cfg(test)]
            controls: self.controls.clone(),
        }
    }

    #[cfg(test)]
    pub(super) fn final_guard(&self) -> FinalGuard {
        self.controls.actor_guard()
    }

    pub(super) fn spawn<F>(mut self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.installation.handle = Some(self.runtime.spawn(future));
        // Installation Drop roots the token before this no-await frame returns.
    }
}

/// The actor captures this reader-only capability, never its own handle cell.
pub(super) struct ReaderBirth {
    role: Arc<Role>,
    stop: Arc<Mutex<Stop>>,
    runtime: Handle,
    #[cfg(test)]
    controls: Arc<Controls>,
}

impl ReaderBirth {
    pub(super) fn spawn<F>(self, future: F) -> Option<ReaderView>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let accepted = {
            let stop = locked(&self.stop);
            let mut role = locked(&self.role.state);
            if stop.sealed || !matches!(*role, State::Dormant) {
                false
            } else {
                *role = State::Claimed;
                true
            }
        };
        if !accepted {
            drop(future);
            return None;
        }
        {
            let mut installation = role::Installation::new(self.role.clone());
            self.role.pulse();
            #[cfg(test)]
            self.controls.reader_claimed();
            #[cfg(test)]
            let guard = self.controls.reader_guard();
            #[cfg(test)]
            let controls = self.controls.clone();
            installation.handle = Some(self.runtime.spawn(async move {
                #[cfg(test)]
                let guard = guard;
                #[cfg(test)]
                controls.reader_start();
                future.await;
                #[cfg(test)]
                drop(guard);
            }));
        }
        #[cfg(test)]
        self.controls.reader_installed();
        Some(ReaderView { role: self.role })
    }
}

pub(super) struct ReaderView {
    role: Arc<Role>,
}

impl ReaderView {
    pub(super) async fn shutdown(&self) {
        self.role.join(true, false).await;
    }
}

#[cfg(test)]
type PairOwner<A> = ServerConnectionOwner<A>;
#[cfg(test)]
use self::RefusedServerConnection as LaunchRefused;
