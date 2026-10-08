//! Original CBS work, with token validation distinct from request preparation.

use std::{
    any::Any,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use amqp::{EngineError, Outcome};
use futures_util::FutureExt;
use tokio::sync::mpsc;
use tracing::debug;

use super::CbsResponse;
use crate::authorization::ConnectionAuthorization;

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
        let mut progress = self.0.lock().expect("CBS custody lock is not poisoned");
        if progress.retired {
            return false;
        }
        progress.started = true;
        true
    }

    pub(super) fn retire(&self) {
        self.0
            .lock()
            .expect("CBS custody lock is not poisoned")
            .retired = true;
    }

    pub(super) fn started(&self) -> bool {
        self.0
            .lock()
            .expect("CBS custody lock is not poisoned")
            .started
    }

    pub(super) fn is_retired(&self) -> bool {
        self.0
            .lock()
            .expect("CBS custody lock is not poisoned")
            .retired
    }
}

pub(super) struct TokenValidation<'a> {
    actual: Pin<Box<dyn Future<Output = Result<(), auth::SasError>> + Send + 'a>>,
    control: OperationControl,
    started: bool,
}

impl<'a> TokenValidation<'a> {
    pub(super) fn new(
        authorization: &'a ConnectionAuthorization,
        token: &'a str,
        audience: &'a str,
        control: OperationControl,
    ) -> Self {
        Self {
            actual: Box::pin(authorization.validate_and_add(token, audience)),
            control,
            started: false,
        }
    }
}

impl Future for TokenValidation<'_> {
    type Output = Option<Result<(), auth::SasError>>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.started {
            if !self.control.begin() {
                return Poll::Ready(None);
            }
            // Constructing the concrete async validate_and_add future is
            // inert. Its validation and grant-lock wait begin at this poll.
            self.started = true;
        }
        self.actual.as_mut().poll(context).map(Some)
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
        assert!(self.available, "cannot observe consumed CBS work");
        if self.actual.is_some() {
            poll_fn(|context| {
                let polled = catch_unwind(AssertUnwindSafe(|| {
                    self.actual
                        .as_mut()
                        .expect("original CBS work is retained")
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
    pub(super) token: Option<PendingOperation<'a, Option<CbsResponse>>>,
    pub(super) token_packet: Option<OperationPacket<Option<CbsResponse>>>,
    pub(super) response: Option<CbsResponse>,
    pub(super) native: Option<PendingOperation<'a, Result<(), EngineError>>>,
    pub(super) native_packet: Option<OperationPacket<Result<(), EngineError>>>,
    cleanup_panic: Option<PanicPayload>,
}

impl RequestCustody<'_> {
    pub(super) fn capture_token(&mut self) {
        let packet = self
            .token
            .as_mut()
            .expect("original CBS token is retained")
            .take_packet()
            .expect("CBS token capture follows completion");
        self.token_packet = Some(packet);
        self.token = None;
    }

    pub(super) fn capture_native(&mut self) {
        let packet = self
            .native
            .as_mut()
            .expect("original CBS acknowledgement is retained")
            .take_packet()
            .expect("CBS acknowledgement capture follows completion");
        self.native_packet = Some(packet);
        self.native = None;
    }

    pub(super) async fn finish(&mut self) {
        if let Some(original) = self.token.as_mut() {
            original.retire();
        }
        if let Some(original) = self.native.as_mut() {
            original.retire();
        }
        if let Some(original) = self.token.as_mut() {
            if let Err(payload) = AssertUnwindSafe(original.finish()).catch_unwind().await {
                self.cleanup_panic = Some(payload);
            }
            self.capture_token();
        }
        if let Some(original) = self.native.as_mut() {
            if let Err(payload) = AssertUnwindSafe(original.finish()).catch_unwind().await
                && self.cleanup_panic.is_none()
            {
                self.cleanup_panic = Some(payload);
            }
            self.capture_native();
        }
    }

    pub(super) fn take_cleanup_panic(&mut self) -> Option<PanicPayload> {
        self.cleanup_panic.take()
    }

    pub(super) fn take_native_error(&mut self) -> Option<EngineError> {
        self.native_packet
            .as_mut()
            .and_then(|packet| match packet.result.take() {
                Some(Err(error)) => Some(error),
                _ => None,
            })
    }

    pub(super) fn report(&self) {
        if let Some(packet) = self.token_packet.as_ref() {
            debug!(
                started = packet.started,
                retired = packet.retired,
                panicked = packet.panicked,
                "original CBS token result retained"
            );
        }
        if let Some(packet) = self.native_packet.as_ref() {
            debug!(
                started = packet.started,
                retired = packet.retired,
                panicked = packet.panicked,
                "original CBS acknowledgement result retained"
            );
        }
    }
}

pub(super) struct ReplyCustody<'a> {
    pub(super) responses: mpsc::Receiver<CbsResponse>,
    pub(super) original: Option<PendingOperation<'a, Result<Outcome, EngineError>>>,
    pub(super) packet: Option<OperationPacket<Result<Outcome, EngineError>>>,
    address: String,
    route: mpsc::Sender<CbsResponse>,
    authorization: &'a ConnectionAuthorization,
    unregistered: bool,
    cleanup_panic: Option<PanicPayload>,
}

impl<'a> ReplyCustody<'a> {
    pub(super) fn new(
        responses: mpsc::Receiver<CbsResponse>,
        address: String,
        route: mpsc::Sender<CbsResponse>,
        authorization: &'a ConnectionAuthorization,
    ) -> Self {
        Self {
            responses,
            original: None,
            packet: None,
            address,
            route,
            authorization,
            unregistered: false,
            cleanup_panic: None,
        }
    }

    pub(super) fn capture_packet(&mut self) {
        let packet = self
            .original
            .as_mut()
            .expect("original CBS reply is retained")
            .take_packet()
            .expect("CBS reply capture follows completion");
        self.packet = Some(packet);
        self.original = None;
    }

    pub(super) fn take_native_error(&mut self) -> Option<EngineError> {
        self.packet
            .as_mut()
            .and_then(|packet| match packet.result.take() {
                Some(Err(error)) => Some(error),
                _ => None,
            })
    }

    pub(super) async fn finish(&mut self) {
        if let Some(original) = self.original.as_mut() {
            original.retire();
        }
        self.responses.close();
        if let Some(original) = self.original.as_mut() {
            if let Err(payload) = AssertUnwindSafe(original.finish()).catch_unwind().await {
                self.cleanup_panic = Some(payload);
            }
            self.capture_packet();
        }
        if !self.unregistered {
            self.authorization
                .unregister_reply_route(&self.address, &self.route)
                .await;
            self.unregistered = true;
        }
    }

    pub(super) fn take_cleanup_panic(&mut self) -> Option<PanicPayload> {
        self.cleanup_panic.take()
    }

    pub(super) fn report(&self) {
        if let Some(packet) = self.packet.as_ref() {
            debug!(
                started = packet.started,
                retired = packet.retired,
                panicked = packet.panicked,
                "original CBS reply result retained"
            );
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum PumpPoint {
    RequestToken,
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
        std::panic::panic_any("controlled outer CBS pump panic");
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
tokio::task_local! {
    pub(super) static PUMP_FAULT: Arc<PumpFault>;
}
