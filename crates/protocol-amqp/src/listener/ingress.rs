//! Original Send/SendBatch submission custody for one incoming delivery.

use std::{
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    task::Poll,
};

use domain::CommandOutcome;

use crate::BrokerRejection;

pub(super) mod custody;

pub(super) type RawSendResult = Result<CommandOutcome, BrokerRejection>;

pub(super) struct SendPacket {
    pub(super) started: bool,
    pub(super) retired: bool,
    pub(super) panicked: bool,
    pub(super) result: Option<RawSendResult>,
}

pub(super) struct SendIntake<'a> {
    actual: Option<Pin<Box<dyn Future<Output = RawSendResult> + Send + 'a>>>,
    started: bool,
    retired: bool,
    result: Option<RawSendResult>,
    consumed: bool,
    panicked: bool,
}

impl<'a> SendIntake<'a> {
    pub(super) fn new(actual: impl Future<Output = RawSendResult> + Send + 'a) -> Self {
        Self {
            actual: Some(Box::pin(actual)),
            started: false,
            retired: false,
            result: None,
            consumed: false,
            panicked: false,
        }
    }

    #[cfg(test)]
    pub(super) fn started(&self) -> bool {
        self.started
    }

    /// A dropped observer retains the original future and cached result. None
    /// means unstarted retirement or terminal poison, never an empty outcome.
    /// The terminal packet's panicked flag distinguishes a poisoned original.
    pub(super) async fn observe(&mut self) -> Option<&RawSendResult> {
        assert!(!self.consumed, "cannot observe a consumed send intake");
        if self.panicked || (self.retired && !self.started) {
            return None;
        }
        if self.result.is_none() {
            poll_fn(|context| {
                self.started = true;
                let polled = catch_unwind(AssertUnwindSafe(|| {
                    self.actual
                        .as_mut()
                        .expect("send intake must retain its original future")
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
        self.retired = true;
        if !self.started {
            self.actual = None;
        }
    }

    pub(super) async fn finish(&mut self) -> Option<&RawSendResult> {
        self.retire();
        self.observe().await
    }

    pub(super) fn take_packet(&mut self) -> Option<SendPacket> {
        if self.actual.is_some() || self.consumed {
            return None;
        }
        self.consumed = true;
        Some(SendPacket {
            started: self.started,
            retired: self.retired,
            panicked: self.panicked,
            result: self.result.take(),
        })
    }
}
