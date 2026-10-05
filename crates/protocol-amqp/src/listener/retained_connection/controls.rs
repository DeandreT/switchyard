//! Protocol-local test checkpoints only; no engine-private controls or custody.

use std::{
    any::Any,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::{Notify, watch};

use super::locked;

#[derive(Clone, Copy, Default)]
pub(in crate::listener) struct Marks {
    pub(in crate::listener) first_poll: bool,
    pub(in crate::listener) opened: bool,
    pub(in crate::listener) primary_ready: bool,
    pub(in crate::listener) shutdown_pending: bool,
    pub(in crate::listener) close_pending: bool,
    pub(in crate::listener) close_ready: bool,
    pub(in crate::listener) launch_refused: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::listener) enum Site {
    PrimaryReady,
    ShutdownPending,
    ClosePending,
    CloseReady,
}

pub(in crate::listener) struct Gate {
    entered: AtomicBool,
    released: AtomicBool,
    changed: Notify,
}

impl Gate {
    pub(in crate::listener) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicBool::new(false),
            released: AtomicBool::new(false),
            changed: Notify::new(),
        })
    }

    async fn pause(&self) {
        self.entered.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.released.load(Ordering::SeqCst) {
                return;
            }
            changed.await;
        }
    }

    pub(in crate::listener) async fn entered(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.entered.load(Ordering::SeqCst) {
                return;
            }
            changed.await;
        }
    }

    pub(in crate::listener) fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

pub(in crate::listener) struct Controls {
    marks: watch::Sender<Marks>,
    first: Mutex<Option<Arc<Gate>>>,
    open: Mutex<Option<Arc<Gate>>>,
    panic: Mutex<Option<(Site, Box<dyn Any + Send>)>>,
}

impl Default for Controls {
    fn default() -> Self {
        let (marks, _) = watch::channel(Marks::default());
        Self {
            marks,
            first: Mutex::new(None),
            open: Mutex::new(None),
            panic: Mutex::new(None),
        }
    }
}

impl Controls {
    pub(in crate::listener) fn snapshot(&self) -> Marks {
        *self.marks.borrow()
    }

    pub(in crate::listener) fn release(&self) {
        let first = locked(&self.first).clone();
        let open = locked(&self.open).clone();
        if let Some(gate) = first {
            gate.release();
        }
        if let Some(gate) = open {
            gate.release();
        }
    }

    pub(in crate::listener) fn first_gate(&self, gate: Arc<Gate>) {
        *locked(&self.first) = Some(gate);
    }

    pub(in crate::listener) fn open_gate(&self, gate: Arc<Gate>) {
        *locked(&self.open) = Some(gate);
    }

    pub(in crate::listener) fn panic_at(&self, site: Site, payload: Box<dyn Any + Send>) {
        let previous = locked(&self.panic).replace((site, payload));
        assert!(previous.is_none(), "one test fault");
    }

    pub(in crate::listener) async fn wait_for(&self, predicate: fn(Marks) -> bool) {
        let mut marks = self.marks.subscribe();
        loop {
            if predicate(*marks.borrow_and_update()) {
                return;
            }
            if marks.changed().await.is_err() {
                return;
            }
        }
    }

    pub(in crate::listener) async fn first_poll(&self) {
        self.marks.send_modify(|marks| marks.first_poll = true);
        let gate = locked(&self.first).clone();
        if let Some(gate) = gate {
            gate.pause().await;
        }
    }

    pub(in crate::listener) async fn opened(&self, _deadline: tokio::time::Instant) {
        self.marks.send_modify(|marks| marks.opened = true);
        let gate = locked(&self.open).clone();
        if let Some(gate) = gate {
            gate.pause().await;
        }
    }

    pub(in crate::listener) fn primary_ready(&self) {
        self.marks.send_modify(|marks| marks.primary_ready = true);
        self.fault(Site::PrimaryReady);
    }

    pub(in crate::listener) fn launch_refused(&self) {
        self.marks.send_modify(|marks| marks.launch_refused = true);
    }

    /// Called only AFTER the ACTUAL shutdown future returned Poll::Pending.
    pub(in crate::listener) fn shutdown_pending(&self) {
        self.marks
            .send_modify(|marks| marks.shutdown_pending = true);
        self.fault(Site::ShutdownPending);
    }

    /// Called only AFTER the ACTUAL CloseHandle::finish returned Poll::Pending.
    pub(in crate::listener) fn close_pending(&self) {
        self.marks.send_modify(|marks| marks.close_pending = true);
        self.fault(Site::ClosePending);
    }

    pub(in crate::listener) fn close_ready(&self) {
        self.marks.send_modify(|marks| marks.close_ready = true);
        self.fault(Site::CloseReady);
    }

    pub(in crate::listener) fn take_unused_fault(&self) -> Option<Box<dyn Any + Send>> {
        let fault = { locked(&self.panic).take() };
        fault.map(|(_, payload)| payload)
    }

    fn fault(&self, site: Site) {
        let payload = {
            let mut fault = locked(&self.panic);
            if fault.as_ref().is_some_and(|(armed, _)| *armed == site) {
                fault.take().map(|(_, payload)| payload)
            } else {
                None
            }
        };
        if let Some(payload) = payload {
            std::panic::panic_any(payload);
        }
    }
}
