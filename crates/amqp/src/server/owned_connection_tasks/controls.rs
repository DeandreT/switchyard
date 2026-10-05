use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::Notify;

use super::locked;

#[derive(Default)]
pub(super) struct Gate {
    entered: AtomicBool,
    changed: Notify,
    released: Mutex<bool>,
    condition: Condvar,
}

impl Gate {
    pub(super) fn block(&self) {
        self.entered.store(true, Ordering::Release);
        self.changed.notify_waiters();
        let mut released = locked(&self.released);
        while !*released {
            released = self
                .condition
                .wait(released)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(super) async fn entered(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.entered.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    pub(super) fn release(&self) {
        *locked(&self.released) = true;
        self.condition.notify_all();
    }
}

#[derive(Default)]
pub(super) struct Controls {
    pub(super) before_bind: Option<Arc<Gate>>,
    pub(super) reader_claim: Option<Arc<Gate>>,
    pub(super) after_reader: Option<Arc<Gate>>,
    pub(super) actor_final: Option<Arc<Gate>>,
    pub(super) reader_final: Option<Arc<Gate>>,
    pub(super) before_reader_poll: Option<Arc<Gate>>,
    pub(super) actor_panic: bool,
    pub(super) reader_panic: bool,
    pub(super) actor_payload: Option<Arc<PayloadCounter>>,
    pub(super) reader_payload: Option<Arc<PayloadCounter>>,
}

#[derive(Default)]
pub(super) struct PayloadCounter(pub(super) std::sync::atomic::AtomicUsize);

struct PanicPayload(Arc<PayloadCounter>);

impl Drop for PanicPayload {
    fn drop(&mut self) {
        self.0.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Controls {
    pub(super) fn before_bind(&self) {
        if let Some(gate) = &self.before_bind {
            gate.block();
        }
    }
    pub(super) fn actor_guard(&self) -> FinalGuard {
        FinalGuard(self.actor_final.clone())
    }
    pub(super) fn reader_guard(&self) -> FinalGuard {
        FinalGuard(self.reader_final.clone())
    }
    pub(super) fn reader_claimed(&self) {
        if let Some(gate) = &self.reader_claim {
            gate.block();
        }
    }
    pub(super) fn reader_installed(&self) {
        if let Some(gate) = &self.after_reader {
            gate.block();
        }
        if self.actor_panic {
            match &self.actor_payload {
                Some(payload) => std::panic::panic_any(PanicPayload(payload.clone())),
                None => panic!("controlled actor failure after reader installation"),
            }
        }
    }
    pub(super) fn reader_start(&self) {
        if let Some(gate) = &self.before_reader_poll {
            gate.block();
        }
        if self.reader_panic {
            match &self.reader_payload {
                Some(payload) => std::panic::panic_any(PanicPayload(payload.clone())),
                None => panic!("controlled reader failure"),
            }
        }
    }
}

#[derive(Default)]
pub(in crate::server) struct FinalGuard(Option<Arc<Gate>>);

impl Drop for FinalGuard {
    fn drop(&mut self) {
        if let Some(gate) = &self.0 {
            gate.block();
        }
    }
}
