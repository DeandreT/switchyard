//! Original CBS work, with token validation distinct from request preparation.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use crate::authorization::ConnectionAuthorization;

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
}

pub(super) struct PendingOperation<'a, T> {
    actual: Option<Pin<Box<dyn Future<Output = T> + Send + 'a>>>,
    control: OperationControl,
    result: Option<T>,
    available: bool,
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
        }
    }

    pub(super) async fn observe(&mut self) -> Option<&T> {
        assert!(self.available, "cannot observe consumed CBS work");
        if self.actual.is_some() {
            poll_fn(|context| {
                match self
                    .actual
                    .as_mut()
                    .expect("original CBS work is retained")
                    .as_mut()
                    .poll(context)
                {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(result) => {
                        self.result = Some(result);
                        self.actual = None;
                        Poll::Ready(())
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
        })
    }
}
