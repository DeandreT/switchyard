use std::{
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use crate::{ReceiveClaimAbortGuard, ReceiveClaimState, ReceiveOwnerUnavailableCause};

use super::*;

struct Receiving {
    guard: Option<ReceiveClaimAbortGuard>,
    events: Arc<Mutex<Vec<&'static str>>>,
    polls: Arc<AtomicUsize>,
}

impl Future for Receiving {
    type Output = Result<Option<Delivery>, ReceiveExit>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::AcqRel);
        Poll::Pending
    }
}

impl Drop for Receiving {
    fn drop(&mut self) {
        drop(self.guard.take());
        self.events.lock().expect("events").push("owner cancelled");
    }
}

struct NativeCredit {
    permit: ReceiveClaimPermit,
    expected: ReceiveClaimState,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Drop for NativeCredit {
    fn drop(&mut self) {
        assert_eq!(self.permit.state(), self.expected);
        self.events.lock().expect("events").push("native refunded");
    }
}

type PacketFixture = (
    ReceivePacket<NativeCredit>,
    crate::ReceiveClaimTicket,
    Arc<Mutex<Vec<&'static str>>>,
    Arc<AtomicUsize>,
);

fn packet() -> PacketFixture {
    let (permit, ticket) = ReceiveClaimPermit::new(u64::MAX);
    let events = Arc::default();
    let polls = Arc::default();
    let receiving = Receiving {
        guard: Some(permit.abort_on_drop()),
        events: Arc::clone(&events),
        polls: Arc::clone(&polls),
    };
    let reservation = NativeCredit {
        permit,
        expected: ReceiveClaimState::Cancelled,
        events: Arc::clone(&events),
    };
    (
        ReceivePacket {
            receiving: Box::pin(receiving),
            reservation,
        },
        ticket,
        events,
        polls,
    )
}

#[test]
fn unpolled_intake_packet_cancels_owner_before_refunding_native_credit() {
    let (packet, ticket, events, polls) = packet();
    let operation = packet.run();
    drop(operation);
    assert_eq!(
        *events.lock().expect("events"),
        ["owner cancelled", "native refunded"]
    );
    assert_eq!(polls.load(Ordering::Acquire), 0);
    assert_eq!(ticket.try_claim(), Err(ReceiveClaimError::Cancelled));
}

#[test]
fn pending_intake_packet_has_the_same_drop_order_after_polling() {
    let (packet, ticket, events, polls) = packet();
    let mut operation = Box::pin(packet.run());
    let waker = futures_util::task::noop_waker();
    assert!(
        operation
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(operation);
    assert_eq!(
        *events.lock().expect("events"),
        ["owner cancelled", "native refunded"]
    );
    assert_eq!(polls.load(Ordering::Acquire), 1);
    assert_eq!(ticket.try_claim(), Err(ReceiveClaimError::Cancelled));
}

#[test]
fn cancellation_after_owner_start_only_returns_local_native_credit() {
    let (mut packet, ticket, events, _) = packet();
    packet.reservation.expected = ReceiveClaimState::Started;
    assert_eq!(ticket.try_claim(), Ok(()));
    let permit = packet.reservation.permit.clone();
    drop(packet.run());
    assert_eq!(
        *events.lock().expect("events"),
        ["owner cancelled", "native refunded"]
    );
    assert_eq!(permit.state(), ReceiveClaimState::Started);
}

#[test]
fn healthy_cancelled_receive_is_an_immediate_controlled_error_not_detach() {
    let error = receive_submit_error(ReceiveSubmitError::Claim(ReceiveClaimError::Cancelled));
    let ReceiveExit::Refused(error) = error else {
        panic!("cancelled does not await native detach");
    };
    assert_eq!(
        error.condition,
        ErrorCondition::Amqp(AmqpError::InternalError)
    );
    assert_eq!(
        error.description.as_deref(),
        Some("the receive owner could not produce a result")
    );
}

#[test]
fn guarded_failures_have_static_distinct_wire_classifications() {
    assert!(matches!(
        receive_submit_error(ReceiveSubmitError::Claim(
            ReceiveClaimError::AuthorizationExpired
        )),
        ReceiveExit::Unauthorized
    ));
    let ReceiveExit::Refused(error) = receive_submit_error(ReceiveSubmitError::Unsupported) else {
        panic!("unsupported");
    };
    assert_eq!(
        error.condition,
        ErrorCondition::Amqp(AmqpError::NotImplemented)
    );
    for error in [
        ReceiveSubmitError::Claim(ReceiveClaimError::ClaimClockUnavailable),
        ReceiveSubmitError::OwnerUnavailable(ReceiveOwnerUnavailableCause::Storage),
    ] {
        let ReceiveExit::Refused(error) = receive_submit_error(error) else {
            panic!("static internal error");
        };
        assert_eq!(
            error.condition,
            ErrorCondition::Amqp(AmqpError::InternalError)
        );
        assert_eq!(
            error.description.as_deref(),
            Some("the receive owner could not produce a result")
        );
    }
    assert!(matches!(
        receive_submit_error(ReceiveSubmitError::Refused(
            domain::BrokerError::EntityBindingStale
        )),
        ReceiveExit::Broker(BrokerRejection::Refused(
            domain::BrokerError::EntityBindingStale
        ))
    ));
}
