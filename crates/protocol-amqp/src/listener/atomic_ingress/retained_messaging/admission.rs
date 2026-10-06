use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use amqp::{EngineError, IncomingSession, ServerConnection, ServerSession};

use super::outcomes::{RetainedAtomicMessagingAdmissionOutcome as Outcome, locked};
use super::worker_history::{Budget, Closed, Ticket};

pub(super) type Original = Pin<Box<dyn Future<Output = Result<ServerSession, EngineError>> + Send>>;
pub(super) struct Record {
    pub(super) ordinal: usize,
    pub(super) original: Original,
    pub(super) result: Option<Result<ServerSession, EngineError>>,
    pub(super) ticket: Option<Ticket>,
}
pub(super) enum Payload {
    Empty,
    Incoming(IncomingSession),
    Admission(Record),
    Transferred,
}
pub(super) struct Packet {
    pub(super) payload: Payload,
}
pub(super) struct Cell(Mutex<Option<Packet>>);
pub(super) struct Loan {
    cell: Arc<Cell>,
    packet: Option<Packet>,
}

impl Cell {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new(Some(Packet {
            payload: Payload::Empty,
        }))))
    }
    pub(super) fn loan(self: &Arc<Self>) -> Option<Loan> {
        let packet = locked(&self.0).take()?;
        Some(Loan {
            cell: Arc::clone(self),
            packet: Some(packet),
        })
    }
    pub(super) fn take_record(self: &Arc<Self>) -> Option<Record> {
        let packet = {
            let mut slot = locked(&self.0);
            if !slot
                .as_ref()
                .is_some_and(|packet| matches!(packet.payload, Payload::Admission(_)))
            {
                return None;
            }
            slot.take().expect("checked original admission packet")
        };
        let mut loan = Loan {
            cell: Arc::clone(self),
            packet: Some(packet),
        };
        let Payload::Admission(record) =
            std::mem::replace(&mut loan.packet_mut().payload, Payload::Transferred)
        else {
            unreachable!("checked original admission payload")
        };
        Some(record)
    }
    pub(super) fn take_after_creator(self: &Arc<Self>) -> Packet {
        locked(&self.0)
            .take()
            .expect("actual original Wrapper creator join before receipt extraction")
    }
    #[cfg(test)]
    pub(super) fn original_address(&self) -> Option<usize> {
        match locked(&self.0).as_ref().map(|packet| &packet.payload) {
            Some(Payload::Admission(record)) => {
                Some(record.original.as_ref().get_ref() as *const _ as *const () as usize)
            }
            _ => None,
        }
    }
    #[cfg(test)]
    pub(super) fn has_incoming(&self) -> bool {
        locked(&self.0)
            .as_ref()
            .is_some_and(|packet| matches!(packet.payload, Payload::Incoming(_)))
    }
}
impl Loan {
    pub(super) fn packet(&self) -> &Packet {
        self.packet.as_ref().expect("armed whole admission packet")
    }
    pub(super) fn packet_mut(&mut self) -> &mut Packet {
        self.packet.as_mut().expect("armed whole admission packet")
    }
    pub(super) fn observed(&mut self, incoming: IncomingSession) {
        assert!(
            matches!(self.packet().payload, Payload::Empty),
            "one original receipt per lifetime attempt"
        );
        self.packet_mut().payload = Payload::Incoming(incoming);
    }
    pub(super) fn convert(
        &mut self,
        connection: &ServerConnection,
        ordinal: usize,
        budget: &Arc<Budget>,
    ) -> Result<(), Closed> {
        let ticket = budget.reserve()?;
        let Payload::Incoming(incoming) =
            std::mem::replace(&mut self.packet_mut().payload, Payload::Empty)
        else {
            unreachable!("actual observed IncomingSession before conversion")
        };
        // The exact owned native future is restored without an intervening await.
        let original = Box::pin(connection.accept_session(incoming));
        self.packet_mut().payload = Payload::Admission(Record {
            ordinal,
            original,
            result: None,
            ticket: Some(ticket),
        });
        Ok(())
    }
}
impl Drop for Loan {
    fn drop(&mut self) {
        if let Some(packet) = self.packet.take() {
            let previous = locked(&self.cell.0).replace(packet);
            assert!(
                previous.is_none(),
                "unique whole admission packet restoration"
            );
        }
    }
}
impl Record {
    pub(super) fn poll_original(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.result.is_some() {
            return Poll::Ready(());
        }
        match self.original.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(original) => {
                self.result = Some(original);
                Poll::Ready(())
            }
        }
    }
    pub(super) fn into_outcome(
        self,
        error: Option<EngineError>,
        launched: Option<(tokio::task::Id, usize)>,
        unlaunched: Option<ServerSession>,
    ) -> Outcome {
        assert!(
            self.result.is_none(),
            "raw acceptance already consumed into its retained disposition"
        );
        Outcome {
            ordinal: self.ordinal,
            error,
            launched,
            _original: Some(self.original),
            _incoming: None,
            _unlaunched: unlaunched,
        }
    }
}
