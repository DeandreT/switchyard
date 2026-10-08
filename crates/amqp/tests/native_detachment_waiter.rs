//! An owned Receiver watch remains usable beside original borrowed native work.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
    time::Duration,
};

use amqp::{
    Attach, Begin, Detach, End, EngineError, Frame, LinkEndpoint, Message, Open, Performative,
    ProtocolHeader, Receiver, ReceiverSettleMode, Role, SenderSettleMode, ServerConnection,
    ServerSession, Target, Transfer, encode_message, read_frame, read_protocol_header, write_frame,
    write_protocol_header,
};
use tokio::{
    io::{DuplexStream, duplex},
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(5);
const CHANNEL: u16 = 1;
const HANDLE: u32 = 1;

fn frame(channel: u16, performative: Performative, payload: Vec<u8>) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload,
    }
}

async fn control(peer: &mut DuplexStream, channel: u16) -> Performative {
    let Frame::Amqp {
        channel: actual,
        performative: Some(performative),
        payload,
    } = timeout(WAIT, read_frame(peer)).await.unwrap().unwrap()
    else {
        panic!("actual native response");
    };
    assert_eq!(actual, channel);
    assert!(payload.is_empty());
    performative
}

async fn pending_once<F: Future + ?Sized>(mut original: Pin<&mut F>) {
    let polled =
        tokio::task::unconstrained(poll_fn(|cx| Poll::Ready(original.as_mut().poll(cx)))).await;
    assert!(matches!(polled, Poll::Pending));
}

struct Wire {
    connection: ServerConnection,
    session: ServerSession,
    peer: DuplexStream,
    receiver: Option<Receiver>,
}

impl Wire {
    async fn new() -> Self {
        let (stream, mut peer) = duplex(64 * 1024);
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept(stream, "owned-watch", None),
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
                        &frame(0, Performative::Open(Open::new("watch-peer")), Vec::new()),
                    )
                    .await
                    .unwrap();
                    assert!(matches!(control(&mut peer, 0).await, Performative::Open(_)));
                }
            )
        })
        .await
        .unwrap();
        let mut connection = connection.unwrap();
        write_frame(
            &mut peer,
            &frame(CHANNEL, Performative::Begin(Begin::default()), Vec::new()),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let session = connection.accept_session(incoming).await.unwrap();
        assert!(matches!(
            control(&mut peer, CHANNEL).await,
            Performative::Begin(_)
        ));
        let mut wire = Self {
            connection,
            session,
            peer,
            receiver: None,
        };
        wire.receiver = Some(wire.attach().await);
        wire
    }

    async fn attach(&mut self) -> Receiver {
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Attach(Box::new(Attach {
                    name: "receiver-watch".to_owned(),
                    handle: HANDLE,
                    role: Role::Sender,
                    snd_settle_mode: SenderSettleMode::Unsettled,
                    rcv_settle_mode: ReceiverSettleMode::First,
                    source: None,
                    target: Some(Target::new("orders")),
                    unsettled: None,
                    incomplete_unsettled: false,
                    initial_delivery_count: Some(0),
                    max_message_size: None,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                })),
                Vec::new(),
            ),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, self.session.next_incoming_attach())
            .await
            .unwrap()
            .unwrap();
        let LinkEndpoint::Receiver(receiver) =
            self.session.accept_attach(incoming, 1024).await.unwrap()
        else {
            panic!("actual native Receiver");
        };
        assert!(matches!(
            control(&mut self.peer, CHANNEL).await,
            Performative::Attach(_)
        ));
        assert!(matches!(
            control(&mut self.peer, CHANNEL).await,
            Performative::Flow(_)
        ));
        receiver
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
                Vec::new(),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            control(&mut self.peer, CHANNEL).await,
            Performative::Detach(_)
        ));
    }

    async fn end(&mut self) {
        write_frame(
            &mut self.peer,
            &frame(CHANNEL, Performative::End(End::default()), Vec::new()),
        )
        .await
        .unwrap();
        assert!(matches!(
            control(&mut self.peer, CHANNEL).await,
            Performative::End(_)
        ));
    }

    async fn fifo(&mut self) {
        write_frame(
            &mut self.peer,
            &frame(2, Performative::Begin(Begin::default()), Vec::new()),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, self.connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let _session = timeout(WAIT, self.connection.accept_session(incoming))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            control(&mut self.peer, 2).await,
            Performative::Begin(_)
        ));
    }

    async fn send(&mut self) {
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Transfer(Transfer {
                    handle: HANDLE,
                    delivery_id: Some(0),
                    delivery_tag: Some(vec![1].into()),
                    message_format: Some(0),
                    settled: Some(false),
                    more: false,
                    rcv_settle_mode: None,
                    state: None,
                    resume: false,
                    aborted: false,
                    batchable: false,
                }),
                encode_message(&Message::data(vec![42])).unwrap(),
            ),
        )
        .await
        .unwrap();
    }

    async fn stop(&mut self) {
        self.connection.stop();
        timeout(WAIT, self.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn owned_receiver_waiter_survives_borrowed_observer_cancel_and_actual_terminals() {
    for terminal in 0..3 {
        let mut wire = Wire::new().await;
        let mut receiver = wire.receiver.take().unwrap();
        let mut detached = Box::pin(receiver.on_detach_owned());
        let mut observer = Box::pin(detached.as_mut());
        pending_once(observer.as_mut()).await;
        drop(observer);
        // The owned watch must not retain a Receiver borrow alongside recv.
        let mut received = Box::pin(receiver.recv());
        pending_once(received.as_mut()).await;
        match terminal {
            0 => wire.detach().await,
            1 => wire.end().await,
            _ => wire.stop().await,
        }
        timeout(WAIT, detached.as_mut()).await.unwrap();
        assert!(matches!(
            timeout(WAIT, received).await.unwrap(),
            Err(EngineError::RemoteDetached | EngineError::Stopped)
        ));
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn owned_receiver_waiter_and_stale_close_cannot_retire_same_handle_replacement() {
    let mut wire = Wire::new().await;
    let old = wire.receiver.take().unwrap();
    let old_detached = old.on_detach_owned();
    wire.detach().await;
    timeout(WAIT, old_detached).await.unwrap();
    let mut replacement = wire.attach().await;
    let mut replacement_detached = Box::pin(replacement.on_detach_owned());
    old.close().await.unwrap();
    drop(old);
    wire.fifo().await;
    pending_once(replacement_detached.as_mut()).await;
    wire.send().await;
    let delivery = timeout(WAIT, replacement.recv()).await.unwrap().unwrap();
    assert_eq!(delivery.message(), &Message::data(vec![42]));
    replacement.accept(&delivery).await.unwrap();
    assert!(matches!(
        control(&mut wire.peer, CHANNEL).await,
        Performative::Disposition(_)
    ));
    pending_once(replacement_detached.as_mut()).await;
    wire.detach().await;
    timeout(WAIT, replacement_detached.as_mut()).await.unwrap();
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn owned_receiver_waiter_outlives_endpoint_without_treating_drop_as_detach() {
    let mut wire = Wire::new().await;
    let receiver = wire.receiver.take().unwrap();
    let mut detached = Box::pin(receiver.on_detach_owned());
    drop(receiver);
    wire.fifo().await;
    pending_once(detached.as_mut()).await;
    wire.end().await;
    timeout(WAIT, detached.as_mut()).await.unwrap();
    wire.stop().await;
}

use amqp::{Accepted, DeliveryState, Disposition, Flow, Sender, Source};

impl Wire {
    async fn attach_sender(&mut self) -> Sender {
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Attach(Box::new(Attach {
                    name: "sender-watch".to_owned(),
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
                Vec::new(),
            ),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, self.session.next_incoming_attach())
            .await
            .unwrap()
            .unwrap();
        let LinkEndpoint::Sender(sender) =
            self.session.accept_attach(incoming, 1024).await.unwrap()
        else {
            panic!("actual native Sender");
        };
        assert!(matches!(
            control(&mut self.peer, CHANNEL).await,
            Performative::Attach(_)
        ));
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Flow(Flow {
                    handle: Some(HANDLE),
                    delivery_count: Some(0),
                    link_credit: Some(1),
                    incoming_window: 2048,
                    outgoing_window: 2048,
                    ..Flow::default()
                }),
                Vec::new(),
            ),
        )
        .await
        .unwrap();
        sender
    }

    async fn sender_wire() -> (Self, Sender) {
        let mut wire = Self::new().await;
        wire.detach().await;
        drop(wire.receiver.take());
        let sender = wire.attach_sender().await;
        (wire, sender)
    }

    async fn outbound_transfer(&mut self) -> u32 {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Transfer(transfer)),
            payload,
        } = timeout(WAIT, read_frame(&mut self.peer))
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("actual native Transfer");
        };
        assert_eq!(channel, CHANNEL);
        assert!(!payload.is_empty());
        transfer.delivery_id.unwrap()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn owned_sender_waiter_survives_cancelled_observer_beside_original_native_start() {
    for terminal in 0..3 {
        let (mut wire, sender) = Wire::sender_wire().await;
        let mut watch = Box::pin(sender.on_detach_owned());
        let mut watched = Box::pin(async { watch.as_mut().await });
        pending_once(watched.as_mut()).await;
        drop(watched);
        let reservation = timeout(WAIT, sender.on_credit()).await.unwrap().unwrap();
        let mut original = Box::pin(sender.send_pending_with_credit(
            reservation,
            Message::data(b"original".to_vec()),
            b"original".to_vec().into(),
        ));
        let mut observer = Box::pin(async { original.as_mut().await });
        pending_once(observer.as_mut()).await;
        drop(observer);
        let id = wire.outbound_transfer().await;
        wire.fifo().await;
        // The later native Begin response proves the original start command
        // completed before this terminal; its success reply is still unobserved.
        pending_once(watch.as_mut()).await;
        match terminal {
            0 => wire.detach().await,
            1 => wire.end().await,
            _ => wire.stop().await,
        }
        timeout(WAIT, watch.as_mut()).await.unwrap();
        let pending = timeout(WAIT, original.as_mut()).await.unwrap().unwrap();
        assert_eq!(pending.identity().delivery_id(), id);
        assert_eq!(pending.identity().channel(), CHANNEL);
        assert_eq!(pending.identity().handle(), HANDLE);
        assert!(matches!(
            timeout(WAIT, pending).await.unwrap(),
            Err(EngineError::RemoteDetached | EngineError::Stopped)
        ));
        drop(original);
        drop(watch);
        drop(sender);
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn owned_sender_waiter_and_stale_close_cannot_retire_same_handle_replacement() {
    let (mut wire, old) = Wire::sender_wire().await;
    let mut old_watch = Box::pin(old.on_detach_owned());
    pending_once(old_watch.as_mut()).await;
    wire.detach().await;
    timeout(WAIT, old_watch.as_mut()).await.unwrap();
    let replacement = wire.attach_sender().await;
    let mut replacement_watch = Box::pin(replacement.on_detach_owned());
    old.close().await.unwrap();
    drop(old);
    wire.fifo().await;
    pending_once(replacement_watch.as_mut()).await;
    let reservation = replacement.on_credit().await.unwrap();
    let mut original = Box::pin(replacement.send_pending_with_credit(
        reservation,
        Message::data(b"replacement".to_vec()),
        b"replacement".to_vec().into(),
    ));
    pending_once(original.as_mut()).await;
    let id = wire.outbound_transfer().await;
    let pending = timeout(WAIT, original.as_mut()).await.unwrap().unwrap();
    write_frame(
        &mut wire.peer,
        &frame(
            CHANNEL,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled: true,
                state: Some(DeliveryState::Accepted(Accepted)),
                batchable: false,
            }),
            Vec::new(),
        ),
    )
    .await
    .unwrap();
    let outcome = timeout(WAIT, pending).await.unwrap().unwrap();
    assert!(matches!(outcome.outcome(), amqp::Outcome::Accepted(_)));
    pending_once(replacement_watch.as_mut()).await;
    wire.detach().await;
    timeout(WAIT, replacement_watch.as_mut()).await.unwrap();
    drop(original);
    drop(replacement_watch);
    drop(replacement);
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn owned_sender_waiter_outlives_endpoint_without_treating_drop_as_detach() {
    let (mut wire, sender) = Wire::sender_wire().await;
    let mut watch = Box::pin(sender.on_detach_owned());
    pending_once(watch.as_mut()).await;
    drop(sender);
    wire.fifo().await;
    pending_once(watch.as_mut()).await;
    wire.end().await;
    timeout(WAIT, watch.as_mut()).await.unwrap();
    wire.stop().await;
}
