//! Private actual socket-task custody, conditional on a retained external owner.
//!
//! The owner must remain live and drive `finish` on its captured runtime. Losing
//! that owner/runtime is deliberately unsupported; there is no detached rescuer.

mod controls;
mod role;
#[cfg(test)]
mod tests;

use std::sync::{Arc, Mutex, MutexGuard};

use tokio::{runtime::Handle, sync::watch, task::JoinError};

use super::connection_launch::{ActorBirth, NegotiatedConnection};
use super::{ConnectionLifecycle, NativeConnectionIdentity, ServerConnection};
use controls::Controls;
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

#[derive(Default)]
struct Observations {
    identity: Option<NativeConnectionIdentity>,
    terminated: Option<watch::Receiver<bool>>,
}

/// Two slots, no owning task/output registry, no mandatory Send bound on A.
struct PairOwner<A> {
    actor: Arc<Role>,
    reader: Arc<Role>,
    stop: Arc<Mutex<Stop>>,
    observations: Arc<Mutex<Observations>>,
    runtime: Handle,
    controls: Arc<Controls>,
    anchor: Option<A>,
    reported: bool,
}

struct Report<A> {
    /// None means this role was never created, not an invented successful join.
    actor: Option<Result<(), JoinError>>,
    reader: Option<Result<(), JoinError>>,
    anchor: A,
}

struct LaunchRefused<Io>(NegotiatedConnection<Io>);

impl<A> PairOwner<A> {
    fn new(runtime: Handle, anchor: A) -> Self {
        Self {
            actor: Arc::new(Role::new()),
            reader: Arc::new(Role::new()),
            stop: Arc::new(Mutex::new(Stop::default())),
            observations: Arc::new(Mutex::new(Observations::default())),
            runtime,
            controls: Arc::new(Controls::default()),
            anchor: Some(anchor),
            reported: false,
        }
    }

    fn launch<Io>(
        &self,
        negotiated: NegotiatedConnection<Io>,
    ) -> Result<ServerConnection, LaunchRefused<Io>>
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
            return Err(LaunchRefused(negotiated));
        }
        let claim = ActorClaim {
            installation: role::Installation::new(self.actor.clone()),
            reader: self.reader.clone(),
            stop: self.stop.clone(),
            observations: self.observations.clone(),
            runtime: self.runtime.clone(),
            controls: self.controls.clone(),
        };
        self.actor.pulse();
        Ok(negotiated.launch(ActorBirth::Scoped(claim)))
    }

    fn stop(&self) {
        let cancellation = {
            let mut stop = locked(&self.stop);
            stop.sealed = true;
            stop.cancellation.clone()
        };
        if let Some(cancellation) = cancellation {
            let _ = cancellation.send(true);
        }
    }

    async fn finish(&mut self) -> Option<Report<A>> {
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
        Some(Report {
            actor,
            reader,
            anchor: self.anchor.take().expect("live anchor"),
        })
    }
}

/// Lives in the synchronous launcher, never in the actor future it installs.
pub(super) struct ActorClaim {
    installation: role::Installation,
    reader: Arc<Role>,
    stop: Arc<Mutex<Stop>>,
    observations: Arc<Mutex<Observations>>,
    runtime: Handle,
    controls: Arc<Controls>,
}

impl ActorClaim {
    pub(super) fn bind(&self, lifecycle: &ConnectionLifecycle) {
        self.controls.before_bind();
        let stopped = {
            let mut stop = locked(&self.stop);
            stop.cancellation = Some(lifecycle.cancellation.clone());
            stop.sealed
        };
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
            controls: self.controls.clone(),
        }
    }

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
            self.controls.reader_claimed();
            let guard = self.controls.reader_guard();
            let controls = self.controls.clone();
            installation.handle = Some(self.runtime.spawn(async move {
                let guard = guard;
                controls.reader_start();
                future.await;
                drop(guard);
            }));
        }
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
