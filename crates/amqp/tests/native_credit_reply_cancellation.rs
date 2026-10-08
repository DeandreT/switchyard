//! Actual cached credit replies retain the same guard until observed or dropped.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
    time::Duration,
};

use amqp::{
    Accepted, Attach, Begin, Binary, Detach, Disposition, DrainRequest, EngineError, Flow, Frame,
    LinkEndpoint, Message, Open, Performative, ProtocolHeader, ReceiverSettleMode, Role, Sender,
    SenderSettleMode, ServerConnection, ServerSession, Source, read_frame, read_protocol_header,
    write_frame, write_protocol_header,
};
use tokio::{
    io::{AsyncReadExt, DuplexStream, duplex},
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(5);
const CHANNEL: u16 = 1;
const HANDLE: u32 = 1;

fn frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

async fn control(peer: &mut DuplexStream) -> Performative {
    let Frame::Amqp {
        channel,
        performative: Some(performative),
        payload,
    } = timeout(WAIT, read_frame(peer))
        .await
        .expect("actual wire reply")
        .unwrap()
    else {
        panic!("AMQP control frame");
    };
    assert!(channel == CHANNEL || matches!(&performative, Performative::Open(_)));
    assert!(payload.is_empty());
    performative
}

struct Wire {
    connection: ServerConnection,
    session: ServerSession,
    peer: DuplexStream,
}

impl Wire {
    async fn new() -> (Self, Sender) {
        let (stream, mut peer) = duplex(64 * 1024);
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept(stream, "credit-server", None),
                async {
                    write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                        .await
                        .unwrap();
                    assert_eq!(
                        read_protocol_header(&mut peer).await.unwrap(),
                        ProtocolHeader::AMQP
                    );
                    write_frame(
                        &mut peer,
                        &frame(0, Performative::Open(Open::new("credit-peer"))),
                    )
                    .await
                    .unwrap();
                    assert!(matches!(control(&mut peer).await, Performative::Open(_)));
                }
            )
        })
        .await
        .expect("actual native Open");
        let mut connection = connection.unwrap();
        write_frame(
            &mut peer,
            &frame(CHANNEL, Performative::Begin(Begin::default())),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let session = timeout(WAIT, connection.accept_session(incoming))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(control(&mut peer).await, Performative::Begin(_)));
        let mut wire = Self {
            connection,
            session,
            peer,
        };
        let sender = wire.attach().await;
        (wire, sender)
    }

    async fn attach(&mut self) -> Sender {
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Attach(Box::new(Attach {
                    name: "cached-credit".to_owned(),
                    handle: HANDLE,
                    role: Role::Receiver,
                    snd_settle_mode: SenderSettleMode::Unsettled,
                    rcv_settle_mode: ReceiverSettleMode::First,
                    source: Some(Source::new("orders")),
                    target: None,
                    unsettled: None,
                    incomplete_unsettled: false,
                    initial_delivery_count: None,
                    max_message_size: None,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                })),
            ),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, self.session.next_incoming_attach())
            .await
            .unwrap()
            .unwrap();
        let LinkEndpoint::Sender(sender) =
            timeout(WAIT, self.session.accept_attach(incoming, 1024 * 1024))
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("actual native Sender");
        };
        let Performative::Attach(response) = control(&mut self.peer).await else {
            panic!("Attach reply");
        };
        assert_eq!(response.handle, HANDLE);
        assert_eq!(response.name, "cached-credit");
        sender
    }

    async fn grant(&mut self, sender: &mut Sender) -> DrainRequest {
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Flow(Flow {
                    handle: Some(HANDLE),
                    delivery_count: Some(0),
                    link_credit: Some(1),
                    drain: true,
                    incoming_window: 2048,
                    outgoing_window: 2048,
                    ..Flow::default()
                }),
            ),
        )
        .await
        .unwrap();
        // The actual native drain notification proves the preceding Flow was
        // processed, including its one-slot credit grant.
        timeout(WAIT, sender.on_drain())
            .await
            .expect("original drain notification")
            .unwrap()
    }

    async fn drained(&mut self, next_outgoing_id: u32) {
        let Performative::Flow(response) = control(&mut self.peer).await else {
            panic!("actual drain reply");
        };
        assert_eq!(response.handle, Some(HANDLE));
        assert_eq!(response.delivery_count, Some(1));
        assert_eq!(response.link_credit, Some(0));
        assert_eq!(response.next_outgoing_id, next_outgoing_id);
        assert!(response.drain);
    }

    async fn detach(&mut self) {
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Detach(Detach {
                    handle: HANDLE,
                    closed: true,
                    error: None,
                }),
            ),
        )
        .await
        .unwrap();
        let Performative::Detach(response) = control(&mut self.peer).await else {
            panic!("actual Detach reply");
        };
        assert_eq!(response.handle, HANDLE);
        assert!(response.closed);
    }

    async fn stop(&mut self) {
        self.connection.stop();
        timeout(WAIT, self.connection.shutdown())
            .await
            .expect("original native tasks joined")
            .unwrap();
    }
}

async fn pending_once<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    let polled =
        tokio::task::unconstrained(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))).await;
    assert!(matches!(polled, Poll::Pending));
}

async fn held(sender: &Sender, request: DrainRequest) {
    assert!(matches!(
        timeout(WAIT, sender.drained(request)).await.unwrap(),
        Err(EngineError::InvalidState(_))
    ));
}

async fn released(sender: &Sender, request: DrainRequest) {
    timeout(WAIT, sender.drained(request))
        .await
        .expect("actual cleanup command barrier")
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_an_unobserved_accepted_reply_reclaims_credit() {
    let (mut wire, mut sender) = Wire::new().await;
    let request = wire.grant(&mut sender).await;
    let mut original = Box::pin(sender.on_credit());
    pending_once(original.as_mut()).await;
    // Commands are FIFO: this refusal proves ReserveCredit succeeded while
    // its actual guard remains cached in the original unpolled reply.
    held(&sender, request).await;
    drop(original);
    // Native cleanup is biased before commands; the actual successful reply
    // witnesses reclamation, not elapsed absence.
    released(&sender, request).await;
    wire.drained(0).await;
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn closing_the_reply_before_grant_rolls_back_without_a_stuck_slot() {
    let (mut wire, mut sender) = Wire::new().await;
    let request = wire.grant(&mut sender).await;
    let mut original = Box::pin(sender.on_credit());
    pending_once(original.as_mut()).await;
    // No yield occurs before the original reply receiver closes; the driver
    // processes the queued request afterward. Private coverage checks rollback.
    drop(original);
    released(&sender, request).await;
    wire.drained(0).await;
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_an_ordinary_returned_guard_reclaims_credit() {
    let (mut wire, mut sender) = Wire::new().await;
    let request = wire.grant(&mut sender).await;
    let guard = timeout(WAIT, sender.on_credit()).await.unwrap().unwrap();
    held(&sender, request).await;
    drop(guard);
    released(&sender, request).await;
    wire.drained(0).await;
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn consuming_an_ordinary_guard_writes_transfer_then_completes_drain_once() {
    let (mut wire, mut sender) = Wire::new().await;
    let request = wire.grant(&mut sender).await;
    let guard = timeout(WAIT, sender.on_credit()).await.unwrap().unwrap();
    let pending = timeout(
        WAIT,
        sender.send_pending_with_credit(guard, Message::data(vec![7]), Binary::from(vec![7])),
    )
    .await
    .unwrap()
    .unwrap();
    let Frame::Amqp {
        channel,
        performative: Some(Performative::Transfer(transfer)),
        payload,
    } = timeout(WAIT, read_frame(&mut wire.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("actual reserved Transfer");
    };
    assert_eq!(channel, CHANNEL);
    assert_eq!(transfer.delivery_tag, Some(Binary::from(vec![7])));
    assert!(!payload.is_empty());
    wire.drained(1).await;
    write_frame(
        &mut wire.peer,
        &frame(
            CHANNEL,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: transfer.delivery_id.unwrap(),
                last: None,
                settled: true,
                state: Some(amqp::DeliveryState::Accepted(Accepted)),
                batchable: false,
            }),
        ),
    )
    .await
    .unwrap();
    let (_, outcome, confirmation) = timeout(WAIT, pending).await.unwrap().unwrap().into_parts();
    assert!(matches!(outcome, amqp::Outcome::Accepted(_)));
    assert!(confirmation.is_none());
    released(&sender, request).await;
    // The successful native command above is a FIFO barrier for all preceding
    // guard consumption/cleanup, so no duplicate drain is already queued.
    let mut byte = [0];
    let mut read = Box::pin(wire.peer.read(&mut byte));
    pending_once(read.as_mut()).await;
    drop(read);
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_old_cached_credit_preserves_replacements_independent_reservation() {
    let (mut wire, mut old_sender) = Wire::new().await;
    let old_request = wire.grant(&mut old_sender).await;
    let mut original = Box::pin(old_sender.on_credit());
    pending_once(original.as_mut()).await;
    held(&old_sender, old_request).await;
    wire.detach().await;
    let mut replacement = wire.attach().await;
    let request = wire.grant(&mut replacement).await;
    let guard = timeout(WAIT, replacement.on_credit())
        .await
        .unwrap()
        .unwrap();
    held(&replacement, request).await;
    drop(original);
    // The old cleanup's incarnation must not affect the fresh link's slot,
    // even though its channel and handle are identical to the retired link.
    held(&replacement, request).await;
    drop(guard);
    released(&replacement, request).await;
    wire.drained(0).await;
    wire.stop().await;
}
