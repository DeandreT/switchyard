use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::watch;

/// An opaque observer of one negotiated native connection's actor lifetime.
///
/// Cloning an observer does not keep the actor active. An active observation
/// is neither authorization nor a reservation for a subsequent operation.
/// Connection provenance remains comparable after the actor has retired.
///
/// ```compile_fail
/// let _identity = amqp::NativeConnectionIdentity::default();
/// ```
///
/// ```compile_fail
/// let _identity = amqp::NativeConnectionIdentity::new();
/// ```
#[derive(Clone)]
pub struct NativeConnectionIdentity(Arc<AtomicBool>);

impl NativeConnectionIdentity {
    pub(super) fn new() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }

    pub fn is_active(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    pub fn same_connection(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn retire(&self) {
        self.0.store(false, Ordering::Release);
    }
}

impl fmt::Debug for NativeConnectionIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeConnectionIdentity")
            .field("active", &self.is_active())
            .finish()
    }
}

pub(super) struct ConnectionActorExit {
    identity: NativeConnectionIdentity,
    terminated: watch::Sender<bool>,
}

impl ConnectionActorExit {
    pub(super) fn new(identity: NativeConnectionIdentity, terminated: watch::Sender<bool>) -> Self {
        Self {
            identity,
            terminated,
        }
    }

    pub(super) fn identity(&self) -> &NativeConnectionIdentity {
        &self.identity
    }
}

impl Drop for ConnectionActorExit {
    fn drop(&mut self) {
        self.identity.retire();
        let _ = self.terminated.send(true);
    }
}
