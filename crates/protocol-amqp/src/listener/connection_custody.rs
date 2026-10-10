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

use super::connection_session_family::{
    AdmittedSessionTask, ConnectionSessionFamily, FamilyPoint, SessionFamilyFailures, checkpoint,
};

pub(super) type ConnectionError = Box<dyn std::error::Error + Send + Sync>;
type PanicPayload = Box<dyn Any + Send>;
type Primary = std::thread::Result<Result<(), ConnectionError>>;

pub(super) enum ConnectionTaskExit {
    Complete(Result<(), ConnectionError>),
    ReportOnly(PanicPayload),
}

impl ConnectionTaskExit {
    pub(super) fn into_result(self) -> Result<(), ConnectionError> {
        match self {
            Self::Complete(result) => result,
            Self::ReportOnly(payload) => resume_unwind(payload),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionRetirementRequest {
    stop: ConnectionStop,
    requested: watch::Sender<bool>,
}

impl ConnectionRetirementRequest {
    fn capture_with_observer(connection: &ServerConnection) -> (Self, watch::Receiver<bool>) {
        let (requested, observer) = watch::channel(false);
        (
            Self {
                stop: connection.stop_owned(),
                requested,
            },
            observer,
        )
    }

    #[cfg(test)]
    pub(crate) fn capture(connection: &ServerConnection) -> Self {
        Self::capture_with_observer(connection).0
    }

    pub(crate) fn request(&self) {
        self.requested.send_replace(true);
        self.stop.request();
    }

    pub(crate) fn observer(&self) -> impl Future<Output = ()> + Send + 'static + use<> {
        wait_for_retirement(self.requested.subscribe())
    }

    #[cfg(test)]
    pub(crate) fn is_requested(&self) -> bool {
        *self.requested.borrow()
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
    family: ConnectionSessionFamily,
    pending_session: Option<AdmittedSessionTask>,
    #[cfg(test)]
    acceptance_preparations: usize,
    finished: bool,
}

impl ConnectionCustody {
    pub(super) fn new(connection: ServerConnection) -> Self {
        let (request, observer) = ConnectionRetirementRequest::capture_with_observer(&connection);
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
            family: ConnectionSessionFamily::new(),
            pending_session: None,
            #[cfg(test)]
            acceptance_preparations: 0,
            finished: false,
        }
    }

    pub(super) fn request_handle(&self) -> ConnectionRetirementRequest {
        self.request.clone()
    }

    pub(super) fn retirement_observer(&self) -> impl Future<Output = ()> + Send + 'static + use<> {
        self.request_handle().observer()
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
        #[cfg(test)]
        {
            self.acceptance_preparations += 1;
        }
    }

    pub(super) async fn next_incoming_session(&mut self) -> Option<IncomingSession> {
        loop {
            let incoming = tokio::select! {
                biased;
                true = self.family.next(), if !self.family.is_empty() => {
                    #[cfg(test)]
                    let id = self.family.reaped_id();
                    #[cfg(not(test))]
                    let id = None;
                    checkpoint(self, FamilyPoint::LiveReaped, id).await;
                    continue;
                },
                incoming = self.connection.next_incoming_session() => incoming,
            };
            return incoming;
        }
    }

    pub(super) fn capture_session(&mut self, receipt: AdmittedSessionTask) {
        assert!(
            self.pending_session.is_none(),
            "one external session receipt at a time"
        );
        self.pending_session = Some(receipt);
        self.finished = false;
    }

    pub(super) fn adopt_pending(&mut self) -> Option<tokio::task::Id> {
        let receipt = self.pending_session.take()?;
        self.finished = false;
        Some(self.family.adopt(receipt))
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
        self.family.retire();
    }

    /// Cancellation drops only this borrower, not originals or retained results.
    pub(super) async fn finish(&mut self) {
        self.request_handle().request();
        self.family.retire();
        let _ = self.adopt_pending();
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
        checkpoint(self, FamilyPoint::NativeShutdownCached, None).await;
        self.family.finish().await;
        self.finished = true;
    }

    #[cfg(test)]
    pub(super) fn finish_result(&mut self) -> Result<(), ConnectionError> {
        self.finish_exit().into_result()
    }

    pub(super) fn finish_exit(&mut self) -> ConnectionTaskExit {
        assert!(
            self.finished,
            "native and session cleanup finish before reporting"
        );
        assert!(
            self.pending_session.is_none(),
            "all external session receipts must be adopted"
        );
        self.family.assert_drained();
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
        let primary = match self.primary.take() {
            Some(primary) => primary,
            None => unreachable!("retained primary pump result"),
        };
        resolve_terminal_exit(ConnectionTerminalParts {
            primary,
            native_error: self.native_error.take(),
            shutdown: self.shutdown.take(),
            secondary_panic: self.secondary_panic.take(),
            family: self.family.take_failures(),
            diagnostic: diagnostics,
        })
    }

    #[cfg(test)]
    pub(super) fn accepted_end_observer(
        &self,
    ) -> Option<impl Future<Output = ()> + Send + 'static + use<>> {
        match self
            .original
            .as_ref()
            .and_then(|original| original.packet.as_ref())
        {
            Some(NativePacket::Accepted(Ok(session))) => Some(session.on_end_owned()),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn acceptance_packet_address(&self) -> Option<usize> {
        self.original
            .as_ref()
            .and_then(|original| original.packet.as_ref())
            .map(|packet| std::ptr::from_ref(packet) as usize)
    }

    #[cfg(test)]
    pub(super) fn acceptance_started(&self) -> bool {
        self.original
            .as_ref()
            .is_some_and(|original| original.started)
    }

    #[cfg(test)]
    pub(super) fn acceptance_preparations(&self) -> usize {
        self.acceptance_preparations
    }

    #[cfg(test)]
    pub(super) fn pending_receipt_id(&self) -> Option<tokio::task::Id> {
        self.pending_session.as_ref().map(|receipt| receipt.id)
    }

    #[cfg(test)]
    pub(super) fn shutdown_cached(&self) -> bool {
        self.shutdown.is_some()
    }

    #[cfg(test)]
    pub(super) fn family(&self) -> &ConnectionSessionFamily {
        &self.family
    }

    #[cfg(test)]
    pub(super) fn family_facts(
        &self,
        point: FamilyPoint,
        id: Option<tokio::task::Id>,
    ) -> super::connection_session_family::FamilyFacts {
        super::connection_session_family::FamilyFacts {
            point,
            id,
            pending: self.family.len(),
            live_ready: self.family.live_ready_count(),
            finished: self.family.finished().len(),
            accepted_cached: matches!(
                self.original
                    .as_ref()
                    .and_then(|original| original.packet.as_ref()),
                Some(NativePacket::Accepted(Ok(_)))
            ),
            receipt_cached: self.pending_session.is_some(),
            shutdown_cached: self.shutdown_cached(),
            packet_address: self.acceptance_packet_address(),
        }
    }
}

pub(super) struct ConnectionTerminalParts {
    pub(super) primary: Primary,
    pub(super) native_error: Option<EngineError>,
    pub(super) shutdown: Option<Result<(), ConnectionShutdownError>>,
    pub(super) secondary_panic: Option<PanicPayload>,
    pub(super) family: SessionFamilyFailures,
    pub(super) diagnostic: Option<PanicPayload>,
}

#[cfg(test)]
pub(super) fn resolve_terminal(parts: ConnectionTerminalParts) -> Result<(), ConnectionError> {
    resolve_terminal_exit(parts).into_result()
}

pub(super) fn resolve_terminal_exit(parts: ConnectionTerminalParts) -> ConnectionTaskExit {
    match parts.primary {
        Err(payload) => resume_unwind(payload),
        Ok(Err(error)) => return ConnectionTaskExit::Complete(Err(error)),
        Ok(Ok(())) => {}
    }
    if let Some(error) = parts.native_error {
        return ConnectionTaskExit::Complete(Err(error.into()));
    }
    if let Some(Err(error)) = parts.shutdown {
        return ConnectionTaskExit::Complete(Err(error.into()));
    }
    if let Some(payload) = parts.secondary_panic {
        resume_unwind(payload);
    }
    if let Some(error) = parts.family.join_error {
        return ConnectionTaskExit::Complete(Err(error.into()));
    }
    if let Some(error) = parts.family.returned_error {
        return ConnectionTaskExit::Complete(Err(error));
    }
    if let Some(error) = parts.family.bridge_fault {
        return ConnectionTaskExit::Complete(Err(error.into()));
    }
    if let Some(payload) = parts
        .family
        .report_only
        .or(parts.diagnostic)
        .or(parts.family.diagnostic)
    {
        return ConnectionTaskExit::ReportOnly(payload);
    }
    ConnectionTaskExit::Complete(Ok(()))
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
