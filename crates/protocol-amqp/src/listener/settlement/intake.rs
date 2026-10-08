//! Original receive-future and credit custody for one receiving link.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

use amqp::CreditReservation;
use domain::CommandOutcome;

use crate::BrokerRejection;

#[cfg(test)]
#[path = "intake/tests.rs"]
mod tests;

pub(super) type RawReceiveResult = Result<CommandOutcome, BrokerRejection>;

pub(super) struct ReceivePacket {
    pub(super) reservation: CreditReservation,
    pub(super) result: Option<RawReceiveResult>,
    pub(super) panicked: bool,
}

pub(super) struct ReceiveIntake<'a> {
    reservation: Option<CreditReservation>,
    actual: Option<Pin<Box<dyn Future<Output = RawReceiveResult> + Send + 'a>>>,
    started: bool,
    retired: bool,
    panicked: bool,
    result: Option<RawReceiveResult>,
}

impl<'a> ReceiveIntake<'a> {
    pub(super) fn new(
        reservation: CreditReservation,
        actual: impl Future<Output = RawReceiveResult> + Send + 'a,
    ) -> Self {
        Self {
            reservation: Some(reservation),
            actual: Some(Box::pin(actual)),
            started: false,
            retired: false,
            panicked: false,
            result: None,
        }
    }

    pub(super) fn started(&self) -> bool {
        self.started
    }

    /// A dropped observer leaves the original future, credit, and any completed
    /// result in this owner. A panicked original is terminal, never re-polled.
    pub(super) async fn observe(&mut self) -> Option<&RawReceiveResult> {
        assert!(
            self.reservation.is_some(),
            "cannot observe a consumed receive intake"
        );
        if self.panicked || (self.retired && !self.started) {
            return None;
        }
        if self.result.is_none() {
            poll_fn(|context| {
                self.started = true;
                let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.actual
                        .as_mut()
                        .expect("receive intake must retain its original future")
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
                        std::panic::resume_unwind(payload)
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

    pub(super) async fn finish(&mut self) -> Option<&RawReceiveResult> {
        self.retire();
        self.observe().await
    }

    pub(super) fn take_packet(&mut self) -> Option<ReceivePacket> {
        if self.actual.is_some() {
            return None;
        }
        Some(ReceivePacket {
            reservation: self.reservation.take()?,
            result: self.result.take(),
            panicked: self.panicked,
        })
    }
}
