//! Receiver-local originals survive an unwind of their borrowed pump.

use std::{
    any::Any,
    future::{Future, poll_fn},
    panic::AssertUnwindSafe,
    pin::Pin,
    task::Poll,
};

use amqp::EngineError;
use domain::{CommandKind, SessionHold};
use futures_util::FutureExt;
use tracing::{debug, warn};

use crate::{
    Broker,
    listener::ConnectionRetirementRequest,
    management::{DeliveryRegistration, SessionRegistration},
};

use super::{
    SettlementContext,
    intake::{RawReceiveResult, ReceiveIntake, ReceivePacket},
    pending_transfer::{PendingTransfer, TransferPacket},
    settle_started_delivery, unregister_deliveries,
    workers::SettlementWorkers,
};

pub(super) type PanicPayload = Box<dyn Any + Send>;

#[cfg(test)]
#[derive(Clone, Copy, PartialEq)]
pub(super) enum PanicFrontier {
    Waiting,
    ReceivePacket,
    TransferPacket,
}

#[cfg(test)]
pub(super) struct PumpPanic {
    frontier: PanicFrontier,
    requested: std::sync::atomic::AtomicBool,
    fired: std::sync::atomic::AtomicBool,
    waker: std::sync::Mutex<Option<std::task::Waker>>,
    changed: tokio::sync::Notify,
}

#[cfg(test)]
impl PumpPanic {
    pub(super) fn new(frontier: PanicFrontier) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            frontier,
            requested: std::sync::atomic::AtomicBool::new(false),
            fired: std::sync::atomic::AtomicBool::new(false),
            waker: std::sync::Mutex::new(None),
            changed: tokio::sync::Notify::new(),
        })
    }

    pub(super) fn request(&self) {
        self.requested
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().as_ref() {
            waker.wake_by_ref();
        }
    }

    fn checkpoint(&self, frontier: PanicFrontier) {
        if self.frontier == frontier && self.requested.load(std::sync::atomic::Ordering::SeqCst) {
            self.fired.store(true, std::sync::atomic::Ordering::SeqCst);
            self.changed.notify_waiters();
            panic!("receiving-primary-panic");
        }
    }

    pub(super) async fn fired(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.fired.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
tokio::task_local! {
    pub(super) static PUMP_PANIC: std::sync::Arc<PumpPanic>;
}

#[cfg(test)]
pub(super) fn pump_poll(context: &std::task::Context<'_>) {
    let _ = PUMP_PANIC.try_with(|control| {
        *control.waker.lock().unwrap() = Some(context.waker().clone());
        control.checkpoint(PanicFrontier::Waiting);
    });
}

#[cfg(test)]
pub(super) fn pump_checkpoint(frontier: PanicFrontier) {
    let _ = PUMP_PANIC.try_with(|control| control.checkpoint(frontier));
}

pub(super) struct OriginalCleanup<'a, T> {
    actual: Option<Pin<Box<dyn Future<Output = T> + Send + 'a>>>,
    result: Option<T>,
    panic: Option<PanicPayload>,
    poisoned: bool,
    completed: bool,
}

impl<'a, T> OriginalCleanup<'a, T> {
    pub(super) fn new(actual: impl Future<Output = T> + Send + 'a) -> Self {
        Self {
            actual: Some(Box::pin(actual)),
            result: None,
            panic: None,
            poisoned: false,
            completed: false,
        }
    }

    pub(super) async fn finish(&mut self) -> bool {
        if self.completed || self.poisoned {
            return false;
        }
        poll_fn(|context| {
            let polled = std::panic::catch_unwind(AssertUnwindSafe(|| {
                self.actual
                    .as_mut()
                    .expect("one original cleanup future")
                    .as_mut()
                    .poll(context)
            }));
            match polled {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(result)) => {
                    self.result = Some(result);
                    self.completed = true;
                    self.actual = None;
                    Poll::Ready(true)
                }
                Err(payload) => {
                    self.poisoned = true;
                    self.panic = Some(payload);
                    self.actual = None;
                    Poll::Ready(true)
                }
            }
        })
        .await
    }

    pub(super) fn result(&self) -> Option<&T> {
        self.result.as_ref()
    }
    pub(super) fn take_result(&mut self) -> Option<T> {
        self.result.take()
    }
    pub(super) fn take_panic(&mut self) -> Option<PanicPayload> {
        self.panic.take()
    }
}

pub(super) struct ReceivingCustody<'a, B> {
    pub(super) workers: SettlementWorkers,
    pub(super) intake: Option<ReceiveIntake<'a>>,
    pub(super) received: Option<ReceivePacket>,
    pub(super) retired_receive: Option<RawReceiveResult>,
    pub(super) receive_poisoned: bool,
    pub(super) transfer: Option<PendingTransfer<'a>>,
    pub(super) transferred: Option<TransferPacket>,
    pub(super) transfer_registration: Option<DeliveryRegistration>,
    pub(super) transfer_context: Option<SettlementContext<B>>,
    pub(super) registrations: Vec<DeliveryRegistration>,
    pub(super) credit_release: Option<OriginalCleanup<'a, Result<(), EngineError>>>,
    pub(super) session: Option<SessionHold>,
    pub(super) session_registration: Option<SessionRegistration>,
    release: Option<OriginalCleanup<'a, RawReceiveResult>>,
    intake_finished: bool,
    transfer_finished: bool,
    workers_finished: bool,
    primary_panic: Option<PanicPayload>,
    secondary_panics: Vec<PanicPayload>,
    retirement: Option<ConnectionRetirementRequest>,
    reported: bool,
}

impl<B> ReceivingCustody<'static, B> {
    pub(super) fn into_borrowed<'a>(self) -> ReceivingCustody<'a, B> {
        self
    }
}

impl<'a, B: Broker> ReceivingCustody<'a, B> {
    pub(super) fn new(
        context: &SettlementContext<B>,
        session: Option<SessionHold>,
        session_registration: Option<SessionRegistration>,
    ) -> Self {
        let release = session.clone().map(|hold| {
            let context = context.clone();
            // Calling submit is itself an invocation frontier; defer it until
            // the original intake, native start and workers have all drained.
            OriginalCleanup::new(async move {
                context
                    .broker
                    .submit(
                        context.namespace,
                        context.entity,
                        CommandKind::ReleaseSession { session: hold },
                    )
                    .await
            })
        });
        Self {
            workers: SettlementWorkers::new(),
            intake: None,
            received: None,
            retired_receive: None,
            receive_poisoned: false,
            transfer: None,
            transferred: None,
            transfer_registration: None,
            transfer_context: None,
            registrations: Vec::new(),
            credit_release: None,
            session,
            session_registration,
            release,
            intake_finished: false,
            transfer_finished: false,
            workers_finished: false,
            primary_panic: None,
            secondary_panics: Vec::new(),
            retirement: None,
            reported: false,
        }
    }

    pub(super) fn record_primary(&mut self, payload: PanicPayload) {
        self.primary_panic = Some(payload);
    }

    fn record_secondary(&mut self, payload: PanicPayload) {
        self.secondary_panics.push(payload);
    }

    pub(super) fn with_retirement(
        mut self,
        retirement: Option<&ConnectionRetirementRequest>,
    ) -> Self {
        self.retirement = retirement.cloned();
        self
    }

    fn notify_cleanup_fault(&self) {
        if let Some(retirement) = self.retirement.as_ref() {
            retirement.request();
        }
    }

    /// Cancellation drops only this borrower. Each completed phase and every
    /// exact future/result remains in this holder for the next finish attempt.
    pub(super) async fn finish(&mut self, context: &SettlementContext<B>) {
        if let Some(original) = self.intake.as_mut() {
            original.retire();
        }
        if let Some(original) = self.transfer.as_mut() {
            original.retire();
        }
        self.workers.retire();

        if let Some(original) = self.credit_release.as_mut() {
            let completed = original.finish().await;
            let fault = completed
                && (original.poisoned
                    || matches!(original.result(),
                Some(Err(error)) if !matches!(error,
                    EngineError::RemoteClosed | EngineError::RemoteDetached | EngineError::Stopped)));
            if let Some(payload) = original.take_panic() {
                self.record_secondary(payload);
            }
            if fault {
                self.notify_cleanup_fault();
            }
        }
        if !self.intake_finished {
            let mut fault = false;
            if let Some(original) = self.intake.as_mut().filter(|_| self.received.is_none()) {
                if let Err(payload) = AssertUnwindSafe(original.finish()).catch_unwind().await {
                    self.record_secondary(payload);
                    fault = true;
                }
                if self.received.is_none() {
                    self.received = self.intake.as_mut().and_then(ReceiveIntake::take_packet);
                }
            }
            self.intake_finished = true;
            if fault {
                self.notify_cleanup_fault();
            }
        }
        if !self.transfer_finished {
            let mut fault = false;
            if let Some(original) = self
                .transfer
                .as_mut()
                .filter(|_| self.transferred.is_none())
            {
                // A pump-cached terminal packet is not a new drain fault.
                self.transferred = original.take_packet();
                if self.transferred.is_none() {
                    if let Err(payload) = AssertUnwindSafe(original.finish()).catch_unwind().await {
                        self.record_secondary(payload);
                        fault = true;
                    }
                    self.transferred = self
                        .transfer
                        .as_mut()
                        .and_then(PendingTransfer::take_packet);
                    fault |= matches!(self.transferred.as_ref().and_then(|packet| packet.result.as_ref()),
                        Some(Err(error)) if !matches!(error,
                            EngineError::RemoteClosed | EngineError::RemoteDetached | EngineError::Stopped));
                }
            }
            if fault {
                self.transfer_finished = true;
                self.notify_cleanup_fault();
            }
            if self
                .transferred
                .as_ref()
                .is_some_and(|packet| matches!(packet.result.as_ref(), Some(Ok(_))))
            {
                let worker_context = self
                    .transfer_context
                    .take()
                    .expect("prepared native handoff context");
                let packet = self
                    .transferred
                    .take()
                    .expect("retained successful native packet");
                if let Some(Ok(pending)) = packet.result {
                    let registration = self.transfer_registration.take();
                    let retirement = self.workers.subscribe();
                    self.workers.adopt_retired(
                        registration.clone(),
                        settle_started_delivery(
                            pending,
                            packet.delivery,
                            registration,
                            worker_context,
                            retirement,
                        ),
                    );
                }
            }
            self.transfer_finished = true;
        }
        if !self.workers_finished {
            self.workers
                .finish_with_retirement(self.retirement.as_ref())
                .await;
            // Only joined completions can remove their own captured receipt.
            for joined in self.workers.finished() {
                if let Ok(completion) = &joined.result
                    && let Some(registration) = completion.registration.as_ref()
                {
                    self.registrations
                        .retain(|retained| retained != registration);
                }
            }
            self.workers_finished = true;
            // Drop credit through its original engine identity, only after all
            // started work is observed; this is not a second native operation.
            if let Some(packet) = self.received.take() {
                self.receive_poisoned = packet.panicked;
                self.retired_receive = packet.result;
                drop(packet.reservation);
            }
        }
        unregister_deliveries(&context.management, &mut self.registrations).await;
        if let Some(original) = self.release.as_mut() {
            let completed = original.finish().await;
            let fault = completed && original.poisoned;
            if let Some(payload) = original.take_panic() {
                self.record_secondary(payload);
            }
            if fault {
                self.notify_cleanup_fault();
            }
        }
        if let Some(registration) = self.session_registration.as_ref() {
            context.management.unregister_session(registration).await;
            self.session_registration = None;
        }
        if !self.reported {
            self.reported = true;
            // Subscriber callbacks can unwind too. All successor custody and
            // conditional cleanup is installed/completed before diagnostics.
            let reported = std::panic::catch_unwind(AssertUnwindSafe(|| {
                if let Some(original) = self.intake.as_ref() {
                    debug!(started = original.started(), panicked = self.receive_poisoned,
                        result = ?self.retired_receive, "retired Receive evidence retained");
                }
                if let Some(original) = self.transfer.as_ref() {
                    debug!(started = original.started(), "retired native start drained");
                }
                if let Some(packet) = self.transferred.as_ref() {
                    debug!(sequence = %packet.delivery.sequence, started = packet.started,
                        retired = packet.retired, panicked = packet.panicked,
                        "retired native terminal evidence retained");
                }
                for joined in self.workers.finished() {
                    if let Err(error) = &joined.result {
                        warn!(task = %joined.id, token = ?joined.lock_token, %error,
                            "settlement worker failed while draining");
                    }
                }
                for failed in self.workers.failures() {
                    debug_assert_eq!(
                        failed.lock_token,
                        failed
                            .registration
                            .as_ref()
                            .map(DeliveryRegistration::lock_token)
                    );
                    warn!(task = %failed.id, token = ?failed.lock_token, error = %failed.error,
                        "settlement worker failed during intake");
                }
                if let Some(Err(rejection)) =
                    self.release.as_ref().and_then(OriginalCleanup::result)
                {
                    debug!(session = ?self.session.as_ref().map(|hold| &hold.session_id), %rejection,
                        "session not released, leaving it to expire");
                }
            }));
            if let Err(payload) = reported {
                self.record_secondary(payload);
            }
        }
    }

    pub(super) fn take_panic(&mut self) -> Option<PanicPayload> {
        self.primary_panic.take().or_else(|| {
            if self.secondary_panics.is_empty() {
                None
            } else {
                Some(self.secondary_panics.remove(0))
            }
        })
    }

    #[cfg(test)]
    pub(super) fn release_result(&self) -> Option<&RawReceiveResult> {
        self.release.as_ref().and_then(OriginalCleanup::result)
    }

    #[cfg(test)]
    pub(super) fn secondary_panics(&self) -> &[PanicPayload] {
        &self.secondary_panics
    }

    #[cfg(test)]
    pub(super) fn record_cleanup_panic(&mut self, payload: PanicPayload) {
        self.record_secondary(payload);
    }
}
