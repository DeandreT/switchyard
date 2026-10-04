use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NodeStopCause {
    Shutdown,
    ClientOwnerLost,
    NetworkWorkerLost,
}

impl NodeStopCause {
    fn bit(self) -> u8 {
        match self {
            Self::Shutdown => 1,
            Self::ClientOwnerLost => 2,
            Self::NetworkWorkerLost => 4,
        }
    }
}

struct State {
    causes: AtomicU8,
    changed: Notify,
}

#[derive(Clone)]
pub(crate) struct StopSignal(Arc<State>);

impl StopSignal {
    pub(crate) fn new() -> Self {
        Self(Arc::new(State {
            causes: AtomicU8::new(0),
            changed: Notify::new(),
        }))
    }

    pub(crate) fn request(&self, cause: NodeStopCause) -> bool {
        let bit = cause.bit();
        let previous = self.0.causes.fetch_or(bit, Ordering::AcqRel);
        if previous & bit == 0 {
            self.0.changed.notify_waiters();
            true
        } else {
            false
        }
    }

    pub(crate) fn is_requested(&self) -> bool {
        self.0.causes.load(Ordering::Acquire) != 0
    }

    pub(crate) fn has_fatal(&self) -> bool {
        self.0.causes.load(Ordering::Acquire) & !NodeStopCause::Shutdown.bit() != 0
    }

    pub(crate) async fn requested(&self) {
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_requested() {
                return;
            }
            notified.await;
        }
    }
}

impl fmt::Debug for StopSignal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("StopSignal").finish_non_exhaustive()
    }
}
