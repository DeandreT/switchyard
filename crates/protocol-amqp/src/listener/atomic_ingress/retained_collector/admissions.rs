use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use amqp::{EngineError, ServerSession};
use tokio::task::Id;

use super::super::retained_session::budget::Ticket;

pub(in crate::listener) type Original =
    Pin<Box<dyn Future<Output = Result<ServerSession, EngineError>> + Send>>;

pub(in crate::listener) struct Reservation(pub(super) Ticket);

pub(super) enum Outcome {
    Pending,
    Observed(Result<ServerSession, EngineError>),
    Launched { id: Id, ordinal: usize },
}

pub(in crate::listener) struct Record {
    pub(super) future: Original,
    pub(super) ticket: Option<Ticket>,
    pub(super) outcome: Outcome,
    pub(super) classified: bool,
    pub(super) observed_pending: bool,
}

impl Record {
    pub(in crate::listener) fn original_address(&self) -> usize {
        self.future.as_ref().get_ref() as *const _ as *const () as usize
    }
    pub(in crate::listener) fn refund(&mut self) {
        drop(self.ticket.take());
    }
    pub(in crate::listener) fn reserved(future: Original, reservation: Reservation) -> Self {
        Self::new(future, reservation.0)
    }
    pub(super) fn new(future: Original, ticket: Ticket) -> Self {
        Self {
            future,
            ticket: Some(ticket),
            outcome: Outcome::Pending,
            classified: false,
            observed_pending: false,
        }
    }
    pub(super) fn poll_original(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if !matches!(self.outcome, Outcome::Pending) || self.classified {
            return Poll::Pending;
        }
        match self.future.as_mut().poll(cx) {
            Poll::Pending => {
                self.observed_pending = true;
                Poll::Pending
            }
            Poll::Ready(original) => {
                // Keep BOTH the raw output and the completed original future rooted.
                self.outcome = Outcome::Observed(original);
                Poll::Ready(())
            }
        }
    }
}
