//! Session capacity counts frames, retains partial sends and never gates control.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

use amqp::{
    Attach, Begin, Binary, CreditReservation, Detach, End, EngineError, Flow, Frame, LinkEndpoint,
    Message, Open, Outcome, PendingDelivery, Performative, ProtocolHeader, ReceiverSettleMode,
    Role, Sender, SenderSettleMode, ServerConnection, ServerSession, Source, Transfer,
    decode_message, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    io::{AsyncRead, DuplexStream, ReadBuf, duplex},
    time::timeout,
};

#[path = "../src/test_deadline.rs"]
mod test_deadline;
use test_deadline::WAIT;

const CHANNEL: u16 = 1;
const HANDLE: u32 = 1;
const FRAME_SIZE: u32 = 1024;

async fn write(peer: &mut DuplexStream, performative: Performative) {
    timeout(
        WAIT,
        write_frame(
            peer,
            &Frame::Amqp {
                channel: CHANNEL,
                performative: Some(performative),
                payload: Vec::new(),
            },
        ),
    )
    .await
    .expect("peer control write deadline")
    .expect("peer control write");
}

async fn frame(peer: &mut DuplexStream) -> Frame {
    timeout(WAIT, read_frame(peer))
        .await
        .expect("server frame deadline")
        .expect("server frame")
}

async fn control(peer: &mut DuplexStream) -> Performative {
    let Frame::Amqp {
        performative: Some(value),
        payload,
        ..
    } = frame(peer).await
    else {
        panic!("expected a server control frame")
    };
    assert!(
        payload.is_empty(),
        "control does not contain message payload"
    );
    value
}

async fn transfer(peer: &mut DuplexStream) -> (Transfer, Vec<u8>) {
    let Frame::Amqp {
        channel: CHANNEL,
        performative: Some(Performative::Transfer(value)),
        payload,
    } = frame(peer).await
    else {
        panic!("expected a server Transfer on the original session")
    };
    assert!(payload.len() <= FRAME_SIZE as usize);
    (value, payload)
}

async fn grant(peer: &mut DuplexStream, handle: Option<u32>, received: u32, window: u32) {
    write(
        peer,
        Performative::Flow(Flow {
            next_incoming_id: Some(received),
            incoming_window: window,
            next_outgoing_id: 0,
            outgoing_window: 2048,
            handle,
            delivery_count: handle.map(|_| 0),
            link_credit: handle.map(|_| 8192),
            ..Flow::default()
        }),
    )
    .await;
}

async fn credit(sender: &Sender) -> CreditReservation {
    timeout(WAIT, sender.on_credit())
        .await
        .expect("link credit deadline")
        .expect("spare link credit")
}

async fn release(fence: CreditReservation) {
    timeout(WAIT, fence.release())
        .await
        .expect("native FIFO release barrier deadline")
        .expect("native FIFO release barrier");
}

async fn pending<F: Future>(mut future: Pin<&mut F>) {
    let state =
        tokio::task::unconstrained(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))).await;
    assert!(
        state.is_pending(),
        "the original send has not finished its Transfer frames"
    );
}

async fn quiet(peer: &mut DuplexStream) {
    let mut bytes = [0; 1];
    let mut buf = ReadBuf::new(&mut bytes);
    let state = tokio::task::unconstrained(poll_fn(|cx| {
        Poll::Ready(Pin::new(&mut *peer).poll_read(cx, &mut buf))
    }))
    .await;
    assert!(
        state.is_pending(),
        "no Transfer escapes the exhausted session window"
    );
}

async fn accepted(delivery: PendingDelivery) {
    let outcome = timeout(WAIT, delivery)
        .await
        .expect("settled send result deadline")
        .expect("settled send result");
    assert!(matches!(outcome.outcome(), Outcome::Accepted(_)));
}

async fn attach(
    session: &mut ServerSession,
    peer: &mut DuplexStream,
    handle: u32,
    window: u32,
) -> Sender {
    write(
        peer,
        Performative::Attach(Box::new(Attach {
            name: format!("window-{handle}"),
            handle,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Settled,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(Source::new("orders")),
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: Some(2 * 1024 * 1024),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        })),
    )
    .await;
    let incoming = timeout(WAIT, session.next_incoming_attach())
        .await
        .expect("incoming Attach deadline")
        .expect("incoming Attach");
    let endpoint = timeout(WAIT, session.accept_attach(incoming, 2 * 1024 * 1024))
        .await
        .expect("accept Attach deadline")
        .expect("accepted Attach");
    assert!(matches!(control(peer).await, Performative::Attach(_)));
    grant(peer, Some(handle), 0, window).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("native sender")
    };
    // Positive command result also proves the peer Flow was consumed.
    release(credit(&sender).await).await;
    sender
}

struct Fixture {
    connection: ServerConnection,
    session: ServerSession,
    sender: Sender,
    peer: DuplexStream,
}

impl Fixture {
    async fn new(window: u32) -> Self {
        let (stream, mut peer) = duplex(64 * 1024);
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept(stream, "window-server", None),
                async {
                    write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                        .await
                        .unwrap();
                    assert_eq!(
                        read_protocol_header(&mut peer).await.unwrap(),
                        ProtocolHeader::AMQP
                    );
                    let mut open = Open::new("window-peer");
                    open.max_frame_size = FRAME_SIZE;
                    write_frame(
                        &mut peer,
                        &Frame::Amqp {
                            channel: 0,
                            performative: Some(Performative::Open(open)),
                            payload: Vec::new(),
                        },
                    )
                    .await
                    .unwrap();
                    assert!(matches!(control(&mut peer).await, Performative::Open(_)));
                }
            )
        })
        .await
        .expect("native Open deadline");
        let mut connection = connection.expect("native connection");
        write(
            &mut peer,
            Performative::Begin(Begin {
                incoming_window: window,
                ..Begin::default()
            }),
        )
        .await;
        let incoming = timeout(WAIT, connection.next_incoming_session())
            .await
            .expect("incoming Begin deadline")
            .expect("incoming Begin");
        let mut session = timeout(WAIT, connection.accept_session(incoming))
            .await
            .expect("session acceptance deadline")
            .expect("session acceptance");
        assert!(matches!(control(&mut peer).await, Performative::Begin(_)));
        let sender = attach(&mut session, &mut peer, HANDLE, window).await;
        Self {
            connection,
            session,
            sender,
            peer,
        }
    }

    async fn shutdown(&mut self) {
        timeout(WAIT, self.connection.shutdown())
            .await
            .expect("original native joins deadline")
            .expect("original native tasks joined");
        assert!(
            timeout(WAIT, read_frame(&mut self.peer))
                .await
                .expect("closed socket deadline")
                .is_err()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn fragmented_send_exceeds_two_thousand_frames_without_consuming_delivery_ids_per_frame() {
    let mut f = Fixture::new(0).await;
    let queued_barrier = credit(&f.sender).await;
    let mut boundary_barrier = Some(credit(&f.sender).await);
    let expected = Message::data(vec![0x5a; 1100 * 1024]);
    let mut sending = Box::pin(
        f.sender
            .send_pending(expected.clone(), Binary::from(vec![1])),
    );
    pending(sending.as_mut()).await;
    release(queued_barrier).await;
    quiet(&mut f.peer).await;
    let mut received = 0;
    let mut bytes = Vec::new();
    let mut done = false;
    while !done {
        grant(&mut f.peer, None, received, 32).await;
        for _ in 0..32 {
            let (value, payload) = transfer(&mut f.peer).await;
            assert_eq!(value.handle, HANDLE);
            assert_eq!(value.delivery_id, (received == 0).then_some(0));
            if received != 0 {
                assert!(value.delivery_tag.is_none());
                assert!(value.message_format.is_none());
            }
            received += 1;
            bytes.extend_from_slice(&payload);
            if !value.more {
                done = true;
                break;
            }
        }
        release(boundary_barrier.take().unwrap()).await;
        quiet(&mut f.peer).await;
        if !done {
            pending(sending.as_mut()).await;
            boundary_barrier = Some(credit(&f.sender).await);
        }
    }
    assert!(received > 2048);
    assert_eq!(decode_message(&bytes).unwrap(), expected);
    let delivery = timeout(WAIT, sending.as_mut())
        .await
        .expect("original full send deadline")
        .expect("original full send");
    assert_eq!(delivery.identity().delivery_id(), 0);
    drop(sending);
    accepted(delivery).await;

    grant(&mut f.peer, None, received, 1).await;
    let (next, (wire, _)) = timeout(WAIT, async {
        tokio::join!(
            f.sender
                .send_pending(Message::data(vec![2]), Binary::from(vec![2])),
            transfer(&mut f.peer)
        )
    })
    .await
    .expect("second delivery deadline");
    let next = next.expect("second delivery starts");
    assert_eq!(wire.delivery_id, Some(1));
    assert_eq!(next.identity().delivery_id(), 1);
    accepted(next).await;
    received += 1;
    write(
        &mut f.peer,
        Performative::Flow(Flow {
            next_incoming_id: Some(received),
            incoming_window: 0,
            outgoing_window: 2048,
            handle: Some(HANDLE),
            delivery_count: Some(2),
            link_credit: Some(0),
            drain: true,
            ..Flow::default()
        }),
    )
    .await;
    let Performative::Flow(drained) = control(&mut f.peer).await else {
        panic!("drain Flow")
    };
    assert!(drained.drain);
    assert_eq!(drained.delivery_count, Some(2));
    assert_eq!(
        drained.next_outgoing_id, received,
        "Flow reports frames, not two delivery IDs"
    );
    f.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn session_only_and_other_link_flow_share_one_transfer_window() {
    let mut f = Fixture::new(0).await;
    let second = attach(&mut f.session, &mut f.peer, 2, 0).await;
    let fence = credit(&second).await;
    let mut first = Box::pin(
        f.sender
            .send_pending(Message::data(vec![1]), Binary::from(vec![1])),
    );
    let mut next = Box::pin(second.send_pending(Message::data(vec![2]), Binary::from(vec![2])));
    pending(first.as_mut()).await;
    pending(next.as_mut()).await;
    release(fence).await;
    quiet(&mut f.peer).await;
    grant(&mut f.peer, None, 0, 1).await;
    let (a, _) = transfer(&mut f.peer).await;
    assert_eq!(a.handle, HANDLE);
    let first_delivery = timeout(WAIT, first.as_mut()).await.unwrap().unwrap();
    accepted(first_delivery).await;
    drop(first);
    pending(next.as_mut()).await;
    // The next update names link 1, but its session slot must release link 2.
    grant(&mut f.peer, Some(HANDLE), 1, 1).await;
    let (b, _) = transfer(&mut f.peer).await;
    assert_eq!(b.handle, 2);
    assert_eq!(b.delivery_id, Some(1));
    let next_delivery = timeout(WAIT, next.as_mut()).await.unwrap().unwrap();
    accepted(next_delivery).await;
    drop(next);
    f.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn zero_window_queued_sends_end_on_detach_end_eof_and_joined_stop() {
    for initial_window in [0, 1] {
        for event in ["detach", "end", "eof", "stop"] {
            let mut f = Fixture::new(initial_window).await;
            let fence = credit(&f.sender).await;
            let mut sending = Box::pin(
                f.sender
                    .send_pending(Message::data(vec![1; 2048]), Binary::from(vec![1])),
            );
            pending(sending.as_mut()).await;
            release(fence).await;
            if initial_window == 1 {
                let (fragment, _) = transfer(&mut f.peer).await;
                assert!(fragment.more);
                pending(sending.as_mut()).await;
            }
            quiet(&mut f.peer).await;
            match event {
                "detach" => {
                    write(
                        &mut f.peer,
                        Performative::Detach(Detach {
                            handle: HANDLE,
                            closed: true,
                            error: None,
                        }),
                    )
                    .await;
                    assert!(matches!(
                        control(&mut f.peer).await,
                        Performative::Detach(_)
                    ));
                }
                "end" => {
                    write(&mut f.peer, Performative::End(End::default())).await;
                    assert!(matches!(control(&mut f.peer).await, Performative::End(_)));
                }
                "eof" => drop(f.peer),
                "stop" => f.connection.stop(),
                _ => unreachable!(),
            }
            let result = timeout(WAIT, sending.as_mut())
                .await
                .expect("original queued send released");
            assert!(
                matches!(result, Err(EngineError::RemoteDetached)),
                "{event}"
            );
            drop(sending);
            timeout(WAIT, f.connection.shutdown())
                .await
                .expect("original reader/driver joins")
                .expect("original reader/driver joined");
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reusing_a_channel_does_not_inherit_old_window_or_partial_send_state() {
    let mut f = Fixture::new(1).await;
    let fence = credit(&f.sender).await;
    let mut sending = Box::pin(
        f.sender
            .send_pending(Message::data(vec![1; 2048]), Binary::from(vec![1])),
    );
    pending(sending.as_mut()).await;
    release(fence).await;
    assert!(transfer(&mut f.peer).await.0.more);
    write(&mut f.peer, Performative::End(End::default())).await;
    assert!(matches!(control(&mut f.peer).await, Performative::End(_)));
    assert!(timeout(WAIT, sending.as_mut()).await.unwrap().is_err());
    drop(sending);
    write(
        &mut f.peer,
        Performative::Begin(Begin {
            incoming_window: 0,
            ..Begin::default()
        }),
    )
    .await;
    let incoming = timeout(WAIT, f.connection.next_incoming_session())
        .await
        .unwrap()
        .unwrap();
    let mut replacement = timeout(WAIT, f.connection.accept_session(incoming))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(control(&mut f.peer).await, Performative::Begin(_)));
    let sender = attach(&mut replacement, &mut f.peer, HANDLE, 0).await;
    let fence = credit(&sender).await;
    let mut next = Box::pin(sender.send_pending(Message::data(vec![2]), Binary::from(vec![2])));
    pending(next.as_mut()).await;
    release(fence).await;
    quiet(&mut f.peer).await;
    grant(&mut f.peer, None, 0, 1).await;
    let (transfer, _) = transfer(&mut f.peer).await;
    assert_eq!(transfer.delivery_id, Some(0));
    assert!(!transfer.more);
    accepted(timeout(WAIT, next.as_mut()).await.unwrap().unwrap()).await;
    drop(next);
    f.shutdown().await;
}
