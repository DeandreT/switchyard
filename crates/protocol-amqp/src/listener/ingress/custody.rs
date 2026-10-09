//! Per-delivery originals stay outside the borrowed inbound pump.

use std::{
    any::Any,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    task::Poll,
};

#[cfg(test)]
use std::sync::Arc;

use amqp::EngineError;
use futures_util::FutureExt;
use tracing::{debug, warn};

use super::{SendIntake, SendPacket};
use crate::listener::SendOutcome;

pub(in crate::listener) type PanicPayload = Box<dyn Any + Send>;

pub(in crate::listener) struct NativePacket {
    pub(in crate::listener) result: Option<Result<(), EngineError>>,
    pub(in crate::listener) started: bool,
    pub(in crate::listener) retired: bool,
    pub(in crate::listener) panicked: bool,
}

type NativeOriginal<'a> = Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'a>>;

pub(in crate::listener) struct NativeSend<'a> {
    actual: Option<NativeOriginal<'a>>,
    result: Option<Result<(), EngineError>>,
    started: bool,
    retired: bool,
    panicked: bool,
    available: bool,
}

impl<'a> NativeSend<'a> {
    pub(in crate::listener) fn new(
        actual: impl Future<Output = Result<(), EngineError>> + Send + 'a,
    ) -> Self {
        Self {
            actual: Some(Box::pin(actual)),
            result: None,
            started: false,
            retired: false,
            panicked: false,
            available: true,
        }
    }

    pub(in crate::listener) async fn observe(&mut self) -> Option<&Result<(), EngineError>> {
        assert!(self.available, "native Send work is not consumed");
        if self.actual.is_some() {
            poll_fn(|context| {
                self.started = true;
                let polled = catch_unwind(AssertUnwindSafe(|| {
                    self.actual
                        .as_mut()
                        .expect("one original native Send operation")
                        .as_mut()
                        .poll(context)
                }));
                #[cfg(test)]
                let _ = NATIVE_POLLED.try_with(|witness| witness.notify_one());
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

    pub(in crate::listener) fn retire(&mut self) {
        self.retired = true;
        if !self.started {
            self.actual = None;
        }
    }

    pub(in crate::listener) async fn finish(&mut self) -> Option<&Result<(), EngineError>> {
        self.retire();
        self.observe().await
    }

    fn take_packet(&mut self) -> Option<NativePacket> {
        if self.actual.is_some() || !self.available {
            return None;
        }
        self.available = false;
        Some(NativePacket {
            result: self.result.take(),
            started: self.started,
            retired: self.retired,
            panicked: self.panicked,
        })
    }
}

#[derive(Default)]
pub(in crate::listener) struct SendCustody<'a> {
    pub(in crate::listener) original: Option<SendIntake<'a>>,
    pub(in crate::listener) packet: Option<SendPacket>,
    pub(in crate::listener) expected: Option<SendOutcome>,
    pub(in crate::listener) native: Option<NativeSend<'a>>,
    pub(in crate::listener) native_packet: Option<NativePacket>,
    pub(in crate::listener) close: Option<NativeSend<'a>>,
    pub(in crate::listener) close_packet: Option<NativePacket>,
    secondary: Option<PanicPayload>,
    reported: bool,
}

impl SendCustody<'_> {
    pub(in crate::listener) fn capture_send(&mut self) {
        self.packet = Some(
            self.original
                .as_mut()
                .expect("original Send retained")
                .take_packet()
                .expect("Send capture follows terminal original"),
        );
        self.original = None;
    }

    pub(in crate::listener) fn capture_native(&mut self) {
        self.native_packet = Some(
            self.native
                .as_mut()
                .expect("native Send retained")
                .take_packet()
                .expect("native capture follows terminal original"),
        );
        self.native = None;
    }

    pub(in crate::listener) fn capture_close(&mut self) {
        self.close_packet = Some(
            self.close
                .as_mut()
                .expect("Send Close retained")
                .take_packet()
                .expect("Close capture follows terminal original"),
        );
        self.close = None;
    }

    pub(in crate::listener) async fn finish(&mut self) {
        if let Some(original) = self.original.as_mut() {
            original.retire();
        }
        if let Some(original) = self.native.as_mut() {
            original.retire();
        }
        if let Some(original) = self.close.as_mut() {
            original.retire();
        }
        if let Some(original) = self.original.as_mut() {
            if let Err(payload) = AssertUnwindSafe(original.finish()).catch_unwind().await {
                self.secondary = Some(payload);
            }
            self.capture_send();
        }
        if let Some(original) = self.native.as_mut() {
            if let Err(payload) = AssertUnwindSafe(original.finish()).catch_unwind().await
                && self.secondary.is_none()
            {
                self.secondary = Some(payload);
            }
            self.capture_native();
        }
        if let Some(original) = self.close.as_mut() {
            if let Err(payload) = AssertUnwindSafe(original.finish()).catch_unwind().await
                && self.secondary.is_none()
            {
                self.secondary = Some(payload);
            }
            self.capture_close();
        }
    }

    pub(in crate::listener) fn take_secondary(&mut self) -> Option<PanicPayload> {
        self.secondary.take()
    }

    pub(in crate::listener) fn take_native_error(&mut self) -> Option<EngineError> {
        fn take(packet: Option<&mut NativePacket>) -> Option<EngineError> {
            let packet = packet?;
            if matches!(packet.result, Some(Err(_))) {
                let Some(Err(error)) = packet.result.take() else {
                    unreachable!("retained native error")
                };
                Some(error)
            } else {
                None
            }
        }
        take(self.native_packet.as_mut()).or_else(|| take(self.close_packet.as_mut()))
    }

    pub(in crate::listener) fn report(&mut self) {
        if self.reported {
            return;
        }
        self.reported = true;
        if let Some(packet) = self.packet.as_ref() {
            debug!(
                started = packet.started,
                retired = packet.retired,
                panicked = packet.panicked,
                "original Send result retained without a new acknowledgement"
            );
            match packet.result.as_ref() {
                Some(Ok(other))
                    if self
                        .expected
                        .is_some_and(|expected| !expected.matches(other)) =>
                {
                    warn!(?other, "original Send produced an unexpected outcome")
                }
                Some(Err(rejection)) => debug!(%rejection, "original Send was rejected"),
                _ => {}
            }
        }
        for (name, packet) in [
            ("acknowledgement", self.native_packet.as_ref()),
            ("close", self.close_packet.as_ref()),
        ] {
            if let Some(packet) = packet {
                debug!(name, started = packet.started, retired = packet.retired, panicked = packet.panicked,
                    result = ?packet.result, "original native Send result retained");
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::listener) enum PumpPoint {
    Prepared,
    Send,
    Packet,
    NativePrepared,
    Native,
    NativePacket,
    ClosePrepared,
    Close,
    ClosePacket,
}

pub(in crate::listener) async fn pump_fault(point: PumpPoint) {
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

pub(in crate::listener) async fn pump_checkpoint(point: PumpPoint) {
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
            payload: Arc::from("original outer Send pump panic"),
        })
    }
}
#[cfg(test)]
tokio::task_local! { pub(in crate::listener) static PUMP_FAULT: Arc<PumpFault>; }

#[cfg(test)]
tokio::task_local! { pub(in crate::listener) static NATIVE_POLLED: Arc<tokio::sync::Notify>; }
