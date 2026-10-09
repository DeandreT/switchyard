//! The attachment pump borrows its original work and ready handoff packet.

use std::{
    any::Any,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use amqp::{EngineError, LinkEndpoint, ServerSession};
use domain::{AcceptedSession, CommandKind, CommandOutcome, EntityPath, NamespaceName};
use futures_util::FutureExt;
use tracing::debug;

use super::{
    AttachmentHandoff, EntityLink, HandoffPacket, HandoffStep, RawAttachResult, RawGrantResult,
};
use crate::{Broker, management::ConnectionManagement};

pub(super) type PanicPayload = Box<dyn Any + Send>;

struct OriginalCleanup<'a, T> {
    actual: Option<Pin<Box<dyn Future<Output = T> + Send + 'a>>>,
    result: Option<T>,
    panic: Option<PanicPayload>,
    started: bool,
    retired: bool,
    panicked: bool,
}

impl<'a, T> OriginalCleanup<'a, T> {
    fn new(actual: impl Future<Output = T> + Send + 'a) -> Self {
        Self {
            actual: Some(Box::pin(actual)),
            result: None,
            panic: None,
            started: false,
            retired: false,
            panicked: false,
        }
    }

    fn retire(&mut self) {
        self.retired = true;
        if !self.started {
            self.actual = None;
        }
    }

    async fn finish(&mut self) {
        if self.actual.is_none() {
            return;
        }
        poll_fn(|context| {
            self.started = true;
            let polled = catch_unwind(AssertUnwindSafe(|| {
                self.actual
                    .as_mut()
                    .expect("original attachment cleanup is retained")
                    .as_mut()
                    .poll(context)
            }));
            match polled {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(result)) => {
                    self.result = Some(result);
                    self.actual = None;
                    Poll::Ready(())
                }
                Err(payload) => {
                    self.panic = Some(payload);
                    self.panicked = true;
                    self.actual = None;
                    Poll::Ready(())
                }
            }
        })
        .await;
    }
}

pub(in crate::listener) struct AttachmentCustody<'a> {
    pub(in crate::listener) handoff: AttachmentHandoff<'a>,
    pub(in crate::listener) grant: Option<RawGrantResult>,
    pub(in crate::listener) native: Option<RawAttachResult>,
    pub(in crate::listener) packet: Option<HandoffPacket>,
    pub(in crate::listener) ready: Option<EntityLink>,
    pub(in crate::listener) entity: Option<EntityPath>,
    pub(in crate::listener) registration: Option<crate::management::SessionRegistration>,
    refusal: Option<OriginalCleanup<'a, Result<(), EngineError>>>,
    unregister: Option<OriginalCleanup<'a, ()>>,
    release: Option<OriginalCleanup<'a, RawGrantResult>>,
    cleanup_panic: Option<PanicPayload>,
    prepared_cleanup: bool,
    adopted: bool,
    reported: bool,
}

impl<'a> AttachmentCustody<'a> {
    pub(in crate::listener) fn new(session: &'a ServerSession) -> Self {
        Self {
            handoff: AttachmentHandoff::new(session),
            grant: None,
            native: None,
            packet: None,
            ready: None,
            entity: None,
            registration: None,
            refusal: None,
            unregister: None,
            release: None,
            cleanup_panic: None,
            prepared_cleanup: false,
            adopted: false,
            reported: false,
        }
    }

    pub(in crate::listener) fn capture_grant(&mut self) {
        self.grant = match self.handoff.take_step() {
            Some(HandoffStep::Grant(result)) => Some(result),
            None => None,
            Some(HandoffStep::Native(_)) => unreachable!("grant phase returns a grant"),
        };
    }

    pub(in crate::listener) fn capture_native(&mut self) {
        self.native = match self.handoff.take_step() {
            Some(HandoffStep::Native(result)) => Some(result),
            None => None,
            Some(HandoffStep::Grant(_)) => unreachable!("native phase returns a native result"),
        };
    }

    pub(in crate::listener) fn capture_packet(&mut self) {
        self.packet = Some(
            self.handoff
                .take_packet()
                .expect("attachment packet follows original completion"),
        );
    }

    pub(super) fn begin_refusal(&mut self, endpoint: LinkEndpoint, error: amqp::Error) {
        self.refusal = Some(OriginalCleanup::new(async move {
            match endpoint {
                LinkEndpoint::Sender(sender) => sender.close_with_error(error).await,
                LinkEndpoint::Receiver(receiver) => receiver.close_with_error(error).await,
            }
        }));
    }

    pub(super) async fn observe_refusal(&mut self) {
        self.refusal
            .as_mut()
            .expect("refusal captured its original endpoint")
            .finish()
            .await;
        if let Some(payload) = self
            .refusal
            .as_mut()
            .and_then(|original| original.panic.take())
        {
            std::panic::resume_unwind(payload);
        }
    }

    pub(super) fn adopt(&mut self) -> EntityLink {
        let ready = self
            .ready
            .take()
            .expect("adoption consumes one ready entity packet");
        self.adopted = true;
        ready
    }

    fn accepted(&self) -> Option<&AcceptedSession> {
        self.ready
            .as_ref()
            .and_then(|ready| ready.accepted.as_ref())
            .or_else(|| {
                self.packet
                    .as_ref()
                    .and_then(|packet| packet.accepted.as_ref())
            })
            .or_else(
                || match self.packet.as_ref().and_then(|packet| packet.step.as_ref()) {
                    Some(HandoffStep::Grant(Ok(CommandOutcome::SessionAccepted(Some(
                        accepted,
                    ))))) => Some(accepted),
                    _ => None,
                },
            )
            .or(match self.grant.as_ref() {
                Some(Ok(CommandOutcome::SessionAccepted(Some(accepted)))) => Some(accepted),
                _ => None,
            })
    }

    pub(in crate::listener) async fn finish<B: Broker>(
        &mut self,
        broker: &'a B,
        namespace: &NamespaceName,
        management: &Arc<ConnectionManagement>,
    ) {
        if self.adopted {
            return;
        }
        if self.packet.is_none() {
            if let Err(payload) = AssertUnwindSafe(self.handoff.finish()).catch_unwind().await {
                self.cleanup_panic = Some(payload);
            }
            self.capture_packet();
        }
        if let Some(original) = self.refusal.as_mut() {
            original.retire();
            original.finish().await;
            if self.cleanup_panic.is_none() {
                self.cleanup_panic = original.panic.take();
            }
        }
        if !self.prepared_cleanup {
            if let Some(accepted) = self.accepted() {
                let hold = accepted.hold();
                let entity = self
                    .ready
                    .as_ref()
                    .map(|ready| &ready.entity)
                    .or(self.entity.as_ref())
                    .expect("an exact captured session grant has an entity")
                    .clone();
                let namespace = namespace.clone();
                // Capture before registry cleanup can await. Broker invocation
                // remains lazy and is never reconstructed on a cancelled finish.
                self.release = Some(OriginalCleanup::new(async move {
                    broker
                        .submit(
                            namespace,
                            entity,
                            CommandKind::ReleaseSession { session: hold },
                        )
                        .await
                }));
            }
            if let Some(registration) = self.registration.as_ref().or_else(|| {
                self.ready
                    .as_ref()
                    .and_then(|ready| ready.registration.as_ref())
            }) {
                let registration = registration.clone();
                let management = Arc::clone(management);
                self.unregister = Some(OriginalCleanup::new(async move {
                    management.unregister_session(&registration).await;
                }));
            }
            self.prepared_cleanup = true;
        }
        if let Some(original) = self.unregister.as_mut() {
            original.finish().await;
            if self.cleanup_panic.is_none() {
                self.cleanup_panic = original.panic.take();
            }
            self.registration = None;
            if let Some(ready) = self.ready.as_mut() {
                ready.registration = None;
            }
        }
        if let Some(original) = self.release.as_mut() {
            original.finish().await;
            if self.cleanup_panic.is_none() {
                self.cleanup_panic = original.panic.take();
            }
        }
        // No late Close is created for a retired handoff. Ready/native
        // endpoints are dropped only after their originals and hold cleanup.
        self.ready = None;
        if matches!(self.native, Some(Ok(_))) {
            self.native = None;
        }
        if let Some(packet) = self.packet.as_mut()
            && matches!(packet.step, Some(HandoffStep::Native(Ok(_))))
        {
            packet.step = None;
        }
    }

    pub(super) fn take_cleanup_panic(&mut self) -> Option<PanicPayload> {
        self.cleanup_panic.take()
    }

    pub(super) fn take_native_error(&mut self) -> Option<EngineError> {
        if matches!(self.native, Some(Err(_))) {
            let Some(Err(error)) = self.native.take() else {
                unreachable!("captured native error")
            };
            return Some(error);
        }
        let packet = self.packet.as_mut()?;
        if matches!(packet.step, Some(HandoffStep::Native(Err(_)))) {
            let Some(HandoffStep::Native(Err(error))) = packet.step.take() else {
                unreachable!("captured original native error")
            };
            return Some(error);
        }
        None
    }

    pub(super) fn report(&mut self) {
        if self.reported || self.adopted {
            return;
        }
        self.reported = true;
        if let Some(packet) = self.packet.as_ref() {
            debug!(phase = ?packet.phase, started = packet.started, retired = packet.retired,
                panicked = packet.panicked, "original attachment handoff retained");
        }
        if let Some(original) = self.release.as_ref() {
            debug!(started = original.started, panicked = original.panicked,
                result = ?original.result, "original exact attachment session release retained");
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::listener) enum PumpPoint {
    Grant,
    Native,
    Registry,
    Refusal,
    Prepared,
    GrantResult,
    NativeResult,
    Installed,
    Ready,
    EntryPrepared,
}

pub(super) async fn pump_fault(point: PumpPoint) {
    #[cfg(test)]
    if let Ok(fault) = PUMP_FAULT.try_with(Arc::clone)
        && fault.point == point
    {
        fault.reached.notify_one();
        fault.trigger.notified().await;
        std::panic::panic_any(Arc::clone(&fault.payload));
    }
    let _ = point;
    std::future::pending::<()>().await;
}

pub(super) async fn pump_checkpoint(point: PumpPoint) {
    #[cfg(test)]
    if PUMP_FAULT
        .try_with(|fault| fault.point == point)
        .unwrap_or(false)
    {
        pump_fault(point).await;
    }
    let _ = point;
}

#[cfg(test)]
pub(in crate::listener) struct PumpFault {
    pub(in crate::listener) point: PumpPoint,
    pub(in crate::listener) reached: tokio::sync::Notify,
    pub(in crate::listener) trigger: tokio::sync::Notify,
    pub(in crate::listener) payload: Arc<str>,
}

#[cfg(test)]
impl PumpFault {
    pub(in crate::listener) fn new(point: PumpPoint) -> Arc<Self> {
        Arc::new(Self {
            point,
            reached: tokio::sync::Notify::new(),
            trigger: tokio::sync::Notify::new(),
            payload: Arc::from("controlled outer attachment pump panic"),
        })
    }
}

#[cfg(test)]
tokio::task_local! { pub(in crate::listener) static PUMP_FAULT: Arc<PumpFault>; }
