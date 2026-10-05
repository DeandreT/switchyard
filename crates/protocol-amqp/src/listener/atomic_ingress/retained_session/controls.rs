use std::{
    future::{Future, poll_fn},
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
};

use tokio::sync::Notify;

use super::super::{IngressError, routing::Branch};

#[derive(Default)]
pub(in crate::listener::atomic_ingress) struct Gate {
    armed: AtomicBool,
    entered: AtomicBool,
    released: AtomicBool,
    changed: Notify,
}

impl Gate {
    pub(in crate::listener::atomic_ingress) fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
    pub(in crate::listener::atomic_ingress) fn entered(&self) -> bool {
        self.entered.load(Ordering::SeqCst)
    }
    pub(in crate::listener::atomic_ingress) fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
    pub(in crate::listener::atomic_ingress) async fn hold(&self) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        self.entered.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
        loop {
            let changed = self.changed.notified();
            let mut changed = pin!(changed);
            changed.as_mut().enable();
            if self.released.load(Ordering::SeqCst) {
                return;
            }
            changed.await;
        }
    }
}

pub(in crate::listener::atomic_ingress) enum Fault {
    Error(IngressError),
    Panic(Box<dyn std::any::Any + Send>),
}

#[derive(Default)]
pub(in crate::listener::atomic_ingress) struct Controls {
    pub(in crate::listener::atomic_ingress) session_start: Gate,
    pub(in crate::listener::atomic_ingress) acceptance_pending: Gate,
    pub(in crate::listener::atomic_ingress) before_accept: Gate,
    pub(in crate::listener::atomic_ingress) before_claim: Gate,
    pub(in crate::listener::atomic_ingress) worker_start: Gate,
    pub(in crate::listener::atomic_ingress) worker_final: Gate,
    pub(in crate::listener::atomic_ingress) close_ack: Gate,
    pub(in crate::listener::atomic_ingress) finish_after_row: Gate,
    pub(in crate::listener::atomic_ingress) session_ready: Gate,
    pub(in crate::listener::atomic_ingress) worker_drops: AtomicUsize,
    pub(in crate::listener::atomic_ingress) joined_rows: AtomicUsize,
    pub(in crate::listener::atomic_ingress) worker_stopped_observed: AtomicUsize,
    pub(in crate::listener::atomic_ingress) authority_closed: AtomicBool,
    pub(in crate::listener::atomic_ingress) disposed_before_close: AtomicBool,
    pub(in crate::listener::atomic_ingress) receiver_torn_down: AtomicBool,
    pub(in crate::listener::atomic_ingress) owner_torn_down: AtomicBool,
    pub(in crate::listener::atomic_ingress) row_panic: AtomicBool,
    pub(in crate::listener::atomic_ingress) session_unwind_after_launch: AtomicBool,
    pub(in crate::listener::atomic_ingress) refused_future_dropped: AtomicBool,
    pub(in crate::listener::atomic_ingress) reply_registration_complete: AtomicBool,
    pub(in crate::listener::atomic_ingress) peer_end_entered: AtomicBool,
    pub(in crate::listener::atomic_ingress) fault: Mutex<Option<Fault>>,
    pub(in crate::listener::atomic_ingress) session_fault: Mutex<Option<Fault>>,
    pub(in crate::listener::atomic_ingress) branch: Mutex<Option<Branch>>,
    pub(in crate::listener::atomic_ingress) worker_branch: Mutex<Option<Branch>>,
}

impl Controls {
    pub(in crate::listener::atomic_ingress) async fn accept<F: Future>(
        &self,
        future: F,
    ) -> F::Output {
        self.before_accept.hold().await;
        let mut future = pin!(future);
        let first = poll_fn(|cx| match future.as_mut().poll(cx) {
            Poll::Ready(value) => Poll::Ready(Some(value)),
            Poll::Pending => Poll::Ready(None),
        })
        .await;
        if let Some(value) = first {
            return value;
        }
        // Entry is reached only after the REAL endpoint-accept future was Pending.
        self.acceptance_pending.hold().await;
        future.await
    }
    pub(in crate::listener::atomic_ingress) async fn before_claim(&self, branch: Branch) {
        if branch == Branch::CbsReplies {
            self.reply_registration_complete
                .store(true, Ordering::SeqCst);
        }
        let selected = *self.branch.lock().unwrap_or_else(|e| e.into_inner());
        if selected.is_none_or(|selected| selected == branch) {
            self.before_claim.hold().await;
        }
    }
    pub(in crate::listener::atomic_ingress) fn take_fault(&self) -> Option<Fault> {
        self.fault.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
    pub(in crate::listener::atomic_ingress) fn take_session_fault(&self) -> Option<Fault> {
        self.session_fault
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }
    pub(in crate::listener::atomic_ingress) fn blocks_worker(&self, branch: Branch) -> bool {
        self.worker_branch
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none_or(|selected| selected == branch)
    }
    pub(in crate::listener::atomic_ingress) fn refused_disposed(&self) {
        if !self.authority_closed.load(Ordering::SeqCst) {
            self.disposed_before_close.store(true, Ordering::SeqCst);
        }
        self.refused_future_dropped.store(true, Ordering::SeqCst);
    }
    pub(in crate::listener::atomic_ingress) fn release_all(&self) {
        for gate in [
            &self.session_start,
            &self.acceptance_pending,
            &self.before_accept,
            &self.before_claim,
            &self.worker_start,
            &self.worker_final,
            &self.close_ack,
            &self.finish_after_row,
            &self.session_ready,
        ] {
            gate.release();
        }
    }
}

pub(in crate::listener::atomic_ingress) struct WorkerDrop(
    pub(in crate::listener::atomic_ingress) Arc<Controls>,
);

impl Drop for WorkerDrop {
    fn drop(&mut self) {
        self.0.worker_drops.fetch_add(1, Ordering::SeqCst);
    }
}
