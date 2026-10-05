use std::{
    future::{Future, poll_fn},
    pin::pin,
    sync::{Arc, Mutex},
    task::Poll,
};

use super::control::{Control, StorageClaim};
use crate::listener::atomic_ingress::retained_collector::admissions::{
    Original, Record, Reservation,
};
use amqp::{IncomingSession, ServerConnection};

pub(super) enum Payload {
    Empty,
    Incoming(IncomingSession),
    Admission(Original),
    Transferred,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Disposition {
    Dormant,
    Issued,
    Observed,
    Accepted,
    Refused,
    Transferred,
}

pub(super) struct Packet {
    pub(super) payload: Payload,
    pub(super) ticket: Option<Reservation>,
    pub(super) disposition: Disposition,
}
impl Packet {
    fn install_claim(&mut self, _claim: StorageClaim) {
        self.disposition = Disposition::Accepted;
    }
}

pub(super) struct Cell(Mutex<Option<Packet>>);
pub(super) struct Port {
    cell: Arc<Cell>,
}

impl Cell {
    pub(super) fn original_address(&self) -> Option<usize> {
        let slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        match slot.as_ref().map(|packet| &packet.payload) {
            Some(Payload::Admission(original)) => {
                Some(original.as_ref().get_ref() as *const _ as *const () as usize)
            }
            _ => None,
        }
    }
    pub(super) fn install_claimed(&self, claim: StorageClaim) {
        let packet = self.0.lock().unwrap_or_else(|e| e.into_inner()).take();
        let mut packet = packet.expect("restored original before white-box storage claim");
        packet.install_claim(claim);
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = Some(packet);
    }
    pub(super) fn new() -> (Arc<Self>, Port) {
        let cell = Arc::new(Self(Mutex::new(Some(Packet {
            payload: Payload::Empty,
            ticket: None,
            disposition: Disposition::Dormant,
        }))));
        (cell.clone(), Port { cell })
    }
    pub(super) fn arm(&self, ticket: Reservation) {
        let packet = self.0.lock().unwrap_or_else(|e| e.into_inner()).take();
        // Factory binding is the only writer before its payload-free ack.
        let mut packet = packet.expect("unloaned dormant port");
        packet.ticket = Some(ticket);
        packet.disposition = Disposition::Issued;
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = Some(packet);
    }
    pub(super) fn take(&self) -> Packet {
        let packet = self.0.lock().unwrap_or_else(|e| e.into_inner()).take();
        packet.expect("actual Wrapper creator barrier before packet extraction")
    }
    pub(super) fn take_record(&self) -> Option<Record> {
        let packet = {
            let mut slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
            if slot
                .as_ref()
                .is_some_and(|p| p.disposition == Disposition::Accepted)
            {
                slot.take()
            } else {
                None
            }
        };
        let mut packet = packet?;
        let payload = std::mem::replace(&mut packet.payload, Payload::Transferred);
        let Payload::Admission(original) = payload else {
            unreachable!("accepted original admission")
        };
        let reservation = packet.ticket.take().expect("accepted storage reservation");
        packet.disposition = Disposition::Transferred;
        {
            let mut slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
            *slot = Some(packet);
        }
        Some(Record::reserved(original, reservation))
    }
}

struct Loan {
    cell: Arc<Cell>,
    packet: Option<Packet>,
}
impl Port {
    fn loan(&self) -> Loan {
        let packet = self.cell.0.lock().unwrap_or_else(|e| e.into_inner()).take();
        Loan {
            cell: self.cell.clone(),
            packet: Some(packet.expect("unique fixed-port loan")),
        }
    }
    pub(super) async fn discover<B: crate::NativeAtomicBroker>(
        self,
        connection: &mut ServerConnection,
        control: &Control<B>,
    ) -> bool {
        let found = {
            let mut original = pin!(super::hooks::Discovery::new(
                connection.next_incoming_session(),
                control.hooks.clone()
            ));
            poll_fn(|cx| {
                let mut loan = self.loan();
                if control.sealed() {
                    return Poll::Ready(false);
                }
                match original.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Some(incoming)) => {
                        let packet = loan.packet.as_mut().expect("armed discovery packet");
                        packet.payload = Payload::Incoming(incoming);
                        packet.disposition = Disposition::Observed;
                        // Restore BEFORE the completed borrowed future/captures drop.
                        drop(loan);
                        Poll::Ready(true)
                    }
                    Poll::Ready(None) => Poll::Ready(false),
                }
            })
            .await
        };
        if !found {
            return false;
        }
        let mut loan = self.loan();
        let packet = loan.packet.as_mut().expect("armed conversion packet");
        let payload = std::mem::replace(&mut packet.payload, Payload::Empty);
        let Payload::Incoming(incoming) = payload else {
            unreachable!("observed original incoming")
        };
        // No await/callback between original conversion and its restoring slot.
        packet.payload = Payload::Admission(Box::pin(connection.accept_session(incoming)));
        // The real cold future is armed before this closed test-only wrapper.
        let payload = std::mem::replace(&mut packet.payload, Payload::Empty);
        let Payload::Admission(original) = payload else {
            unreachable!("armed original conversion")
        };
        packet.payload = Payload::Admission(Box::pin(super::hooks::Admission::new(
            original,
            control.hooks.clone(),
        )));
        control.hooks.conversion.hold().await;
        let payload = control
            .hooks
            .conversion_panic
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(payload) = payload {
            std::panic::resume_unwind(payload);
        }
        control.hooks.before_claim.hold().await;
        if let Some(claim) = control.claim_storage() {
            packet.install_claim(claim);
        } else {
            packet.disposition = Disposition::Refused;
        }
        let refund = if packet.disposition == Disposition::Refused {
            packet.ticket.take()
        } else {
            None
        };
        drop(loan);
        drop(refund);
        control.pulse();
        true
    }
}
impl Drop for Loan {
    fn drop(&mut self) {
        if let Some(packet) = self.packet.take() {
            let previous = {
                let mut slot = self.cell.0.lock().unwrap_or_else(|e| e.into_inner());
                slot.replace(packet)
            };
            // Any impossible overlap is diagnosed only after lock release.
            assert!(previous.is_none(), "whole unique port restoration");
        }
    }
}
