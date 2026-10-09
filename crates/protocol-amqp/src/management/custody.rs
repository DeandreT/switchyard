//! Consumed original work, with broker invocation distinct from preparation.

use std::{
    any::Any,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};

use amqp::{EngineError, Outcome};
use domain::{CommandKind, CommandOutcome, EntityPath, NamespaceName};
use futures_util::FutureExt;
use tokio::sync::mpsc;
use tracing::debug;

use super::{ConnectionManagement, ManagementResponse};
use crate::{Broker, BrokerRejection, listener::connection_custody::ConnectionRetirementRequest};

pub(super) type PanicPayload = Box<dyn Any + Send>;

#[derive(Default)]
struct Progress {
    started: bool,
    retired: bool,
}

#[derive(Clone, Default)]
pub(super) struct OperationControl(Arc<Mutex<Progress>>);

impl OperationControl {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn begin(&self) -> bool {
        let mut progress = self
            .0
            .lock()
            .expect("management custody lock is not poisoned");
        if progress.retired {
            return false;
        }
        progress.started = true;
        true
    }

    pub(super) fn retire(&self) {
        self.0
            .lock()
            .expect("management custody lock is not poisoned")
            .retired = true;
    }

    pub(super) fn started(&self) -> bool {
        self.0
            .lock()
            .expect("management custody lock is not poisoned")
            .started
    }

    pub(super) fn is_retired(&self) -> bool {
        self.0
            .lock()
            .expect("management custody lock is not poisoned")
            .retired
    }
}

#[derive(Clone)]
pub(super) struct RequestBroker<B> {
    actual: B,
    control: OperationControl,
}

impl<B> RequestBroker<B> {
    pub(super) fn new(actual: B) -> Self {
        Self {
            actual,
            control: OperationControl::new(),
        }
    }

    pub(super) fn control(&self) -> OperationControl {
        self.control.clone()
    }
}

impl<B: Broker> Broker for RequestBroker<B> {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        // Even an eager underlying method is invoked only inside this
        // retained first poll. Started is not proof of broker admission.
        if !self.control.begin() {
            return Err(BrokerRejection::Unavailable(
                "management request retired before broker invocation".to_owned(),
            ));
        }
        self.actual.submit(namespace, entity, kind).await
    }

    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        self.actual.deliverable(namespace, entity)
    }
}

pub(super) struct OperationPacket<T> {
    pub(super) result: Option<T>,
    pub(super) started: bool,
    pub(super) retired: bool,
    pub(super) panicked: bool,
}

pub(super) struct PendingOperation<'a, T> {
    actual: Option<Pin<Box<dyn Future<Output = T> + Send + 'a>>>,
    control: OperationControl,
    result: Option<T>,
    available: bool,
    panicked: bool,
}

impl<'a, T> PendingOperation<'a, T> {
    pub(super) fn new(
        actual: impl Future<Output = T> + Send + 'a,
        control: OperationControl,
    ) -> Self {
        Self {
            actual: Some(Box::pin(actual)),
            control,
            result: None,
            available: true,
            panicked: false,
        }
    }

    pub(super) async fn observe(&mut self) -> Option<&T> {
        assert!(self.available, "cannot observe consumed management work");
        if self.actual.is_some() {
            poll_fn(|context| {
                let polled = catch_unwind(AssertUnwindSafe(|| {
                    self.actual
                        .as_mut()
                        .expect("original management work is retained")
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
                        self.panicked = true;
                        self.actual = None;
                        resume_unwind(payload)
                    }
                }
            })
            .await;
        }
        self.result.as_ref()
    }

    pub(super) fn retire(&mut self) {
        self.control.retire();
        if !self.control.started() {
            self.actual = None;
        }
    }

    pub(super) async fn finish(&mut self) -> Option<&T> {
        self.retire();
        self.observe().await
    }

    pub(super) fn take_packet(&mut self) -> Option<OperationPacket<T>> {
        if self.actual.is_some() || !self.available {
            return None;
        }
        self.available = false;
        Some(OperationPacket {
            result: self.result.take(),
            started: self.control.started(),
            retired: self.control.is_retired(),
            panicked: self.panicked,
        })
    }
}

#[derive(Default)]
pub(super) struct RequestCustody<'a> {
    pub(super) request: Option<PendingOperation<'a, ManagementResponse>>,
    pub(super) request_packet: Option<OperationPacket<ManagementResponse>>,
    pub(super) response: Option<ManagementResponse>,
    pub(super) native: Option<PendingOperation<'a, Result<(), EngineError>>>,
    pub(super) native_packet: Option<OperationPacket<Result<(), EngineError>>>,
    pub(super) close: Option<PendingOperation<'a, Result<(), EngineError>>>,
    pub(super) close_packet: Option<OperationPacket<Result<(), EngineError>>>,
    cleanup_panic: Option<PanicPayload>,
    native_panic: Option<PanicPayload>,
    close_panic: Option<PanicPayload>,
}

impl RequestCustody<'_> {
    pub(super) fn capture_request(&mut self) {
        self.request_packet = Some(
            self.request
                .as_mut()
                .expect("original request is retained")
                .take_packet()
                .expect("request capture follows completion"),
        );
        self.request = None;
    }

    pub(super) fn capture_native(&mut self) {
        self.native_packet = Some(
            self.native
                .as_mut()
                .expect("original acknowledgement is retained")
                .take_packet()
                .expect("acknowledgement capture follows completion"),
        );
        self.native = None;
    }

    #[cfg(test)]
    pub(super) async fn finish(&mut self, detached: impl Future<Output = ()> + Send) {
        self.finish_with_retirement(detached, None).await;
    }

    pub(super) async fn finish_with_retirement(
        &mut self,
        detached: impl Future<Output = ()> + Send,
        retirement: Option<&ConnectionRetirementRequest>,
    ) {
        if let Some(original) = self.request.as_mut() {
            original.retire();
        }
        if let Some(original) = self.native.as_mut() {
            original.retire();
        }
        if let Some(original) = self.request.as_mut() {
            let new_panic =
                if let Err(payload) = AssertUnwindSafe(original.finish()).catch_unwind().await {
                    self.cleanup_panic = Some(payload);
                    true
                } else {
                    false
                };
            self.capture_request();
            request_fault(retirement, new_panic);
        }
        if let Some(original) = self.native.as_mut() {
            let unfinished = original.actual.is_some();
            let new_panic =
                if let Err(payload) = AssertUnwindSafe(original.finish()).catch_unwind().await {
                    self.native_panic = Some(payload);
                    true
                } else {
                    false
                };
            self.capture_native();
            request_fault(
                retirement,
                new_panic || (unfinished && packet_error(self.native_packet.as_ref())),
            );
        }
        if let Some(original) = self.close.as_mut() {
            let unfinished = original.actual.is_some();
            let new_panic = if let Err(payload) = AssertUnwindSafe(finish_close(original, detached))
                .catch_unwind()
                .await
            {
                self.close_panic = Some(payload);
                true
            } else {
                false
            };
            self.close_packet = Some(
                original
                    .take_packet()
                    .expect("request Close capture follows completion"),
            );
            self.close = None;
            request_fault(
                retirement,
                new_panic || (unfinished && packet_error(self.close_packet.as_ref())),
            );
        }
    }

    pub(super) fn take_cleanup_panic(&mut self) -> Option<PanicPayload> {
        self.cleanup_panic
            .take()
            .or_else(|| self.native_panic.take())
            .or_else(|| self.close_panic.take())
    }

    pub(super) fn take_native_error(&mut self) -> Option<EngineError> {
        take_error(self.native_packet.as_mut()).or_else(|| take_error(self.close_packet.as_mut()))
    }

    pub(super) fn report(&self) {
        if let Some(packet) = self.request_packet.as_ref() {
            report_packet(packet, "request");
        }
        if let Some(packet) = self.native_packet.as_ref() {
            report_packet(packet, "acknowledgement");
        }
        if let Some(packet) = self.close_packet.as_ref() {
            report_packet(packet, "request close");
        }
    }
}

pub(super) struct ReplyCustody<'a> {
    pub(super) responses: mpsc::Receiver<ManagementResponse>,
    pub(super) original: Option<PendingOperation<'a, Result<Outcome, EngineError>>>,
    pub(super) packet: Option<OperationPacket<Result<Outcome, EngineError>>>,
    pub(super) close: Option<PendingOperation<'a, Result<(), EngineError>>>,
    pub(super) close_packet: Option<OperationPacket<Result<(), EngineError>>>,
    address: String,
    route: mpsc::Sender<ManagementResponse>,
    management: &'a ConnectionManagement,
    unregistered: bool,
    cleanup_panic: Option<PanicPayload>,
    close_panic: Option<PanicPayload>,
}

impl<'a> ReplyCustody<'a> {
    pub(super) fn new(
        responses: mpsc::Receiver<ManagementResponse>,
        address: String,
        route: mpsc::Sender<ManagementResponse>,
        management: &'a ConnectionManagement,
    ) -> Self {
        Self {
            responses,
            original: None,
            packet: None,
            close: None,
            close_packet: None,
            address,
            route,
            management,
            unregistered: false,
            cleanup_panic: None,
            close_panic: None,
        }
    }

    pub(super) fn capture_packet(&mut self) {
        self.packet = Some(
            self.original
                .as_mut()
                .expect("original reply is retained")
                .take_packet()
                .expect("reply capture follows completion"),
        );
        self.original = None;
    }

    #[cfg(test)]
    pub(super) async fn finish(&mut self, detached: impl Future<Output = ()> + Send) {
        self.finish_with_retirement(detached, None).await;
    }

    pub(super) async fn finish_with_retirement(
        &mut self,
        detached: impl Future<Output = ()> + Send,
        retirement: Option<&ConnectionRetirementRequest>,
    ) {
        if let Some(original) = self.original.as_mut() {
            original.retire();
        }
        self.responses.close();
        // An unauthorized Close can wake an already queued no-credit reply.
        // Both originals remain owned here if this borrowed finish is cancelled.
        // A retry must not promote the retained terminal result or panic cache.
        tokio::join!(
            async {
                if let Some(original) = self.original.as_mut() {
                    let unfinished = original.actual.is_some();
                    let new_panic = if let Err(payload) =
                        AssertUnwindSafe(original.finish()).catch_unwind().await
                    {
                        self.cleanup_panic = Some(payload);
                        true
                    } else {
                        false
                    };
                    request_fault(
                        retirement,
                        new_panic
                            || (unfinished && matches!(original.result.as_ref(), Some(Err(_)))),
                    );
                }
            },
            async {
                if let Some(original) = self.close.as_mut() {
                    let unfinished = original.actual.is_some();
                    let new_panic = if let Err(payload) =
                        AssertUnwindSafe(finish_close(original, detached))
                            .catch_unwind()
                            .await
                    {
                        self.close_panic = Some(payload);
                        true
                    } else {
                        false
                    };
                    request_fault(
                        retirement,
                        new_panic
                            || (unfinished && matches!(original.result.as_ref(), Some(Err(_)))),
                    );
                }
            },
        );
        if self.original.is_some() {
            self.capture_packet();
        }
        if let Some(original) = self.close.as_mut() {
            self.close_packet = Some(
                original
                    .take_packet()
                    .expect("reply Close capture follows completion"),
            );
            self.close = None;
        }
        if !self.unregistered {
            self.management
                .unregister_reply_route(&self.address, &self.route)
                .await;
            self.unregistered = true;
        }
    }

    pub(super) fn take_cleanup_panic(&mut self) -> Option<PanicPayload> {
        self.cleanup_panic
            .take()
            .or_else(|| self.close_panic.take())
    }

    pub(super) fn take_native_error(&mut self) -> Option<EngineError> {
        take_error(self.packet.as_mut()).or_else(|| take_error(self.close_packet.as_mut()))
    }

    pub(super) fn report(&self) {
        if let Some(packet) = self.packet.as_ref() {
            report_packet(packet, "reply");
        }
        if let Some(packet) = self.close_packet.as_ref() {
            report_packet(packet, "reply close");
        }
    }
}

fn request_fault(retirement: Option<&ConnectionRetirementRequest>, fault: bool) {
    if fault && let Some(retirement) = retirement {
        retirement.request();
    }
}

fn packet_error<T>(packet: Option<&OperationPacket<Result<T, EngineError>>>) -> bool {
    packet.is_some_and(|packet| matches!(packet.result.as_ref(), Some(Err(_))))
}

async fn finish_close(
    original: &mut PendingOperation<'_, Result<(), EngineError>>,
    detached: impl Future<Output = ()> + Send,
) {
    tokio::pin!(detached);
    tokio::select! {
        biased;
        () = &mut detached => { let _ = original.finish().await; }
        _ = original.observe() => {}
    }
}

fn take_error<T>(
    packet: Option<&mut OperationPacket<Result<T, EngineError>>>,
) -> Option<EngineError> {
    packet.and_then(|packet| match packet.result.take() {
        Some(Err(error)) => Some(error),
        _ => None,
    })
}

fn report_packet<T>(packet: &OperationPacket<T>, phase: &'static str) {
    debug!(
        started = packet.started,
        retired = packet.retired,
        panicked = packet.panicked,
        phase,
        "original management result retained"
    );
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum PumpPoint {
    RequestBroker,
    RequestNative,
    RequestRouting,
    ReplyNative,
    #[cfg(test)]
    RequestPrepared,
    #[cfg(test)]
    RequestResponse,
    #[cfg(test)]
    ReplyPrepared,
    #[cfg(test)]
    ReplyResult,
    #[cfg(test)]
    ReplyIdle,
}

pub(super) async fn pump_fault(point: PumpPoint) {
    #[cfg(test)]
    if let Ok(fault) = PUMP_FAULT.try_with(Arc::clone)
        && fault.point == point
    {
        fault.reached.notify_one();
        fault.trigger.notified().await;
        std::panic::panic_any("controlled outer management pump panic");
    }
    let _ = point;
    std::future::pending::<()>().await;
}

#[cfg(test)]
pub(super) async fn pump_checkpoint(point: PumpPoint) {
    if PUMP_FAULT
        .try_with(|fault| fault.point == point)
        .unwrap_or(false)
    {
        pump_fault(point).await;
    }
}

#[cfg(test)]
pub(super) struct PumpFault {
    pub(super) point: PumpPoint,
    pub(super) reached: tokio::sync::Notify,
    pub(super) trigger: tokio::sync::Notify,
}

#[cfg(test)]
impl PumpFault {
    pub(super) fn new(point: PumpPoint) -> Arc<Self> {
        Arc::new(Self {
            point,
            reached: tokio::sync::Notify::new(),
            trigger: tokio::sync::Notify::new(),
        })
    }
}

#[cfg(test)]
tokio::task_local! { pub(super) static PUMP_FAULT: Arc<PumpFault>; }
