//! One accepted connection retains originals outside its borrowed pump.

use std::{
    any::Any,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, resume_unwind},
    pin::Pin,
    task::Poll,
};

use amqp::{
    ConnectionShutdownError, ConnectionStop, EngineError, IncomingSession, ServerConnection,
    ServerSession,
};
use futures_util::FutureExt;
use tokio::sync::watch;
use tracing::{debug, warn};

pub(super) type ConnectionError = Box<dyn std::error::Error + Send + Sync>;
type PanicPayload = Box<dyn Any + Send>;
type Primary = std::thread::Result<Result<(), ConnectionError>>;

#[derive(Clone)]
pub(crate) struct ConnectionRetirementRequest {
    stop: ConnectionStop,
    requested: watch::Sender<bool>,
}

impl ConnectionRetirementRequest {
    pub(crate) fn request(&self) {
        self.requested.send_replace(true);
        self.stop.request();
    }
}

pub(super) async fn wait_for_retirement(mut requested: watch::Receiver<bool>) {
    while !*requested.borrow_and_update() {
        if requested.changed().await.is_err() {
            return;
        }
    }
}

enum NativeFuture {
    Accept(Pin<Box<dyn Future<Output = Result<ServerSession, EngineError>> + Send>>),
    Close(Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send>>),
}

pub(super) enum NativePacket {
    Accepted(Result<ServerSession, EngineError>),
    Closed(Result<(), EngineError>),
}

struct OriginalNative {
    actual: Option<NativeFuture>,
    packet: Option<NativePacket>,
    started: bool,
    retired: bool,
    poisoned: bool,
}

impl OriginalNative {
    fn new(actual: NativeFuture) -> Self {
        Self {
            actual: Some(actual),
            packet: None,
            started: false,
            retired: false,
            poisoned: false,
        }
    }

    fn retire(&mut self) {
        self.retired = true;
        if !self.started {
            self.actual = None;
        }
    }

    async fn observe(&mut self) -> std::thread::Result<()> {
        if self.actual.is_none() {
            return Ok(());
        }
        poll_fn(|context| {
            self.started = true;
            let polled = std::panic::catch_unwind(AssertUnwindSafe(|| {
                match self.actual.as_mut().expect("one original native operation") {
                    NativeFuture::Accept(actual) => {
                        actual.as_mut().poll(context).map(NativePacket::Accepted)
                    }
                    NativeFuture::Close(actual) => {
                        actual.as_mut().poll(context).map(NativePacket::Closed)
                    }
                }
            }));
            match polled {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(packet)) => {
                    self.packet = Some(packet);
                    self.actual = None;
                    Poll::Ready(Ok(()))
                }
                Err(payload) => {
                    self.poisoned = true;
                    self.actual = None;
                    Poll::Ready(Err(payload))
                }
            }
        })
        .await
    }
}

pub(super) struct ConnectionCustody {
    pub(super) connection: ServerConnection,
    request: ConnectionRetirementRequest,
    requested: watch::Receiver<bool>,
    original: Option<OriginalNative>,
    primary: Option<Primary>,
    native_error: Option<EngineError>,
    shutdown: Option<Result<(), ConnectionShutdownError>>,
    secondary_panic: Option<PanicPayload>,
    shutdown_poisoned: bool,
    finished: bool,
}

impl ConnectionCustody {
    pub(super) fn new(connection: ServerConnection) -> Self {
        let (requested, observer) = watch::channel(false);
        let request = ConnectionRetirementRequest {
            stop: connection.stop_owned(),
            requested,
        };
        Self {
            connection,
            request,
            requested: observer,
            original: None,
            primary: None,
            native_error: None,
            shutdown: None,
            secondary_panic: None,
            shutdown_poisoned: false,
            finished: false,
        }
    }

    pub(super) fn request_handle(&self) -> ConnectionRetirementRequest {
        self.request.clone()
    }

    pub(super) fn retirement_observer(&self) -> impl Future<Output = ()> + Send + 'static + use<> {
        wait_for_retirement(self.requested.clone())
    }

    pub(super) fn is_retired(&self) -> bool {
        *self.requested.borrow() || self.requested.has_changed().is_err()
    }

    pub(super) fn begin_accept(&mut self, incoming: IncomingSession) {
        assert!(
            self.original.is_none(),
            "one native connection operation at a time"
        );
        self.original = Some(OriginalNative::new(NativeFuture::Accept(Box::pin(
            self.connection.accept_session_owned(incoming),
        ))));
    }

    pub(super) fn begin_close(&mut self, error: Option<amqp::Error>) {
        assert!(
            self.original.is_none(),
            "one native connection operation at a time"
        );
        let actual = match error {
            Some(error) => Box::pin(self.connection.close_with_error_owned(error))
                as Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send>>,
            None => Box::pin(self.connection.close_owned()),
        };
        self.original = Some(OriginalNative::new(NativeFuture::Close(actual)));
    }

    pub(super) async fn observe_native(&mut self) {
        if self.is_retired() && !self.original.as_ref().expect("prepared original").started {
            self.original.as_mut().unwrap().retire();
        }
        let result = tokio::select! {
            biased;
            () = pump_fault(PumpPoint::Native) => unreachable!("connection fault checkpoint panics"),
            result = self.original.as_mut().unwrap().observe() => result,
        };
        if let Err(payload) = result {
            resume_unwind(payload);
        }
    }

    pub(super) fn native_error_ready(&self) -> bool {
        matches!(
            self.original
                .as_ref()
                .and_then(|original| original.packet.as_ref()),
            Some(NativePacket::Accepted(Err(_)) | NativePacket::Closed(Err(_)))
        )
    }

    pub(super) fn packet_ready(&self) -> bool {
        self.original
            .as_ref()
            .is_some_and(|original| original.packet.is_some())
    }

    pub(super) fn take_packet(&mut self) -> Option<NativePacket> {
        let original = self.original.take().expect("one completed original");
        assert!(
            original.actual.is_none(),
            "native packet follows original completion"
        );
        original.packet
    }

    pub(super) fn record_primary(&mut self, primary: Primary) {
        assert!(self.primary.is_none(), "one primary pump result");
        self.primary = Some(primary);
    }

    /// Cancellation drops only this borrower, not originals or retained results.
    pub(super) async fn finish(&mut self) {
        self.request_handle().request();
        if let Some(original) = self.original.as_mut() {
            original.retire();
            if let Err(payload) = original.observe().await
                && self.secondary_panic.is_none()
            {
                self.secondary_panic = Some(payload);
            }
            if self.native_error.is_none() {
                let error = match original.packet.as_mut() {
                    Some(NativePacket::Accepted(result)) => result.as_mut().err(),
                    Some(NativePacket::Closed(result)) => result.as_mut().err(),
                    None => None,
                };
                if error.is_some() {
                    self.native_error = match original.packet.take().unwrap() {
                        NativePacket::Accepted(Err(error)) | NativePacket::Closed(Err(error)) => {
                            Some(error)
                        }
                        _ => unreachable!("the retained packet is an original error"),
                    };
                }
            }
        }
        if self.shutdown.is_none() && !self.shutdown_poisoned {
            match AssertUnwindSafe(self.connection.shutdown())
                .catch_unwind()
                .await
            {
                Ok(result) => self.shutdown = Some(result),
                Err(payload) => {
                    self.shutdown_poisoned = true;
                    if self.secondary_panic.is_none() {
                        self.secondary_panic = Some(payload);
                    }
                }
            }
        }
        self.finished = true;
    }

    pub(super) fn finish_result(&mut self) -> Result<(), ConnectionError> {
        assert!(self.finished, "native cleanup finishes before reporting");
        let diagnostics = std::panic::catch_unwind(AssertUnwindSafe(|| {
            if let Some(Err(error)) = self.shutdown.as_ref() {
                warn!(%error, "native tasks failed during connection cleanup");
            }
            if let Some(original) = self.original.as_ref() {
                debug!(
                    started = original.started,
                    retired = original.retired,
                    poisoned = original.poisoned,
                    "original connection operation retained"
                );
            }
        }))
        .err();
        match self.primary.take().expect("retained primary pump result") {
            Err(payload) => resume_unwind(payload),
            Ok(Err(error)) => Err(error),
            Ok(Ok(())) => {
                if let Some(error) = self.native_error.take() {
                    return Err(error.into());
                }
                if let Some(Err(error)) = self.shutdown.take() {
                    return Err(error.into());
                }
                if let Some(payload) = self.secondary_panic.take().or(diagnostics) {
                    resume_unwind(payload);
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, PartialEq)]
pub(super) enum PumpPoint {
    Intake,
    Native,
    #[cfg(test)]
    Packet,
}

#[cfg(test)]
pub(super) struct PumpFault {
    point: PumpPoint,
    requested: std::sync::atomic::AtomicBool,
    reached: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
    waker: std::sync::Mutex<Option<std::task::Waker>>,
    pub(super) identity: std::sync::Arc<()>,
}

#[cfg(test)]
impl PumpFault {
    pub(super) fn new(point: PumpPoint) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            point,
            requested: false.into(),
            reached: false.into(),
            changed: tokio::sync::Notify::new(),
            waker: std::sync::Mutex::new(None),
            identity: std::sync::Arc::new(()),
        })
    }
    pub(super) fn request(&self) {
        self.requested
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().as_ref() {
            waker.wake_by_ref();
        }
    }
    pub(super) async fn reached(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.reached.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
tokio::task_local! { pub(super) static PUMP_FAULT: std::sync::Arc<PumpFault>; }

pub(super) async fn pump_fault(point: PumpPoint) {
    #[cfg(not(test))]
    {
        let _ = point;
        std::future::pending::<()>().await;
    }
    #[cfg(test)]
    poll_fn(|context| {
        let _ = PUMP_FAULT.try_with(|control| {
            if control.point == point {
                control
                    .reached
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                control.changed.notify_waiters();
                *control.waker.lock().unwrap() = Some(context.waker().clone());
                if control.requested.load(std::sync::atomic::Ordering::SeqCst) {
                    std::panic::panic_any(std::sync::Arc::clone(&control.identity));
                }
            }
        });
        Poll::<()>::Pending
    })
    .await;
}

#[cfg(test)]
pub(super) async fn packet_checkpoint() {
    if PUMP_FAULT
        .try_with(|control| control.point == PumpPoint::Packet)
        .unwrap_or(false)
    {
        pump_fault(PumpPoint::Packet).await;
    }
}
