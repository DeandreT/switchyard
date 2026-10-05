use crate::listener::retained_connection::RetainedConnectionResult;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct Gate {
    armed: AtomicBool,
    entered: AtomicBool,
    released: AtomicBool,
    notify: Notify,
}
impl Gate {
    pub(super) fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
    pub(super) fn entered(&self) -> bool {
        self.entered.load(Ordering::SeqCst)
    }
    pub(super) fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
    pub(super) async fn hold(&self) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        self.entered.store(true, Ordering::SeqCst);
        loop {
            let notified = self.notify.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if self.released.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}
pub(super) enum Fault {
    Error(crate::listener::retained_connection::RetainedConnectionResult),
    Panic(Box<dyn std::any::Any + Send>),
}
#[derive(Default)]
pub(super) struct Hooks {
    pub(super) bind: Gate,
    pub(super) conversion: Gate,
    pub(super) before_claim: Gate,
    pub(super) wrapper_return: Gate,
    pub(super) socket_ready: Gate,
    pub(super) collector_ready: Gate,
    pub(super) discovery_drop: Mutex<Option<Box<dyn std::any::Any + Send>>>,
    pub(super) admission_drop: Mutex<Option<Box<dyn std::any::Any + Send>>>,
    admission_ready_seen: AtomicBool,
    pub(super) conversion_panic: Mutex<Option<Box<dyn std::any::Any + Send>>>,
    pub(super) terminal_fault: Mutex<Option<Fault>>,
}
impl Hooks {
    pub(super) fn release(&self) {
        for gate in [
            &self.bind,
            &self.conversion,
            &self.before_claim,
            &self.wrapper_return,
            &self.socket_ready,
            &self.collector_ready,
        ] {
            gate.release();
        }
    }
    pub(super) fn unused(&self) -> Vec<Fault> {
        let mut unused = Vec::new();
        for slot in [&self.discovery_drop, &self.conversion_panic] {
            let payload = slot.lock().unwrap_or_else(|e| e.into_inner()).take();
            if let Some(payload) = payload {
                unused.push(Fault::Panic(payload));
            }
        }
        if !self.admission_ready_seen.load(Ordering::SeqCst) {
            let payload = self
                .admission_drop
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            if let Some(payload) = payload {
                unused.push(Fault::Panic(payload));
            }
        }
        let fault = self
            .terminal_fault
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(fault) = fault {
            unused.push(fault);
        }
        unused
    }
    pub(super) fn terminal_result(&self) -> RetainedConnectionResult {
        let fault = self
            .terminal_fault
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        match fault {
            Some(Fault::Error(result)) => result,
            Some(Fault::Panic(payload)) => std::panic::resume_unwind(payload),
            None => Ok(()),
        }
    }
}
pub(super) struct Admission {
    inner: crate::listener::atomic_ingress::retained_collector::admissions::Original,
    hooks: Arc<Hooks>,
    ready: bool,
}
impl Admission {
    pub(super) fn new(
        inner: crate::listener::atomic_ingress::retained_collector::admissions::Original,
        hooks: Arc<Hooks>,
    ) -> Self {
        Self {
            inner,
            hooks,
            ready: false,
        }
    }
}
impl Future for Admission {
    type Output = Result<amqp::ServerSession, amqp::EngineError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self.inner.as_mut().poll(cx);
        if result.is_ready() {
            self.ready = true;
            self.hooks
                .admission_ready_seen
                .store(true, Ordering::SeqCst);
        }
        result
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        if self.ready {
            let payload = self
                .hooks
                .admission_drop
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            drop(payload);
        }
    }
}
pub(super) struct Discovery<F: Future> {
    inner: Pin<Box<F>>,
    hooks: Arc<Hooks>,
    ready: bool,
}
impl<F: Future> Discovery<F> {
    pub(super) fn new(inner: F, hooks: Arc<Hooks>) -> Self {
        Self {
            inner: Box::pin(inner),
            hooks,
            ready: false,
        }
    }
}
impl<F: Future> Future for Discovery<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self.inner.as_mut().poll(cx);
        self.ready |= result.is_ready();
        result
    }
}
impl<F: Future> Drop for Discovery<F> {
    fn drop(&mut self) {
        if self.ready {
            let payload = self
                .hooks
                .discovery_drop
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            if let Some(payload) = payload {
                std::panic::resume_unwind(payload);
            }
        }
    }
}
