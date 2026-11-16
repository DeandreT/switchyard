//! Real-wire sending progress; drain bootstraps a fresh zero-count sender.
use amqp::{
    Attach, Begin, Binary, CreditReservation, Detach, Flow, Frame, LinkEndpoint, Message, Open,
    Outcome, PendingDelivery, Performative, ProtocolHeader, ReceiverSettleMode, Role, Sender,
    SenderSettleMode, ServerConnection, ServerSession, Source, Transfer, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};
use tokio::{
    io::{DuplexStream, duplex},
    time::timeout,
};
#[path = "../src/test_deadline.rs"]
mod test_deadline;
use test_deadline::WAIT;

async fn write(peer: &mut DuplexStream, p: Performative) {
    timeout(
        WAIT,
        write_frame(
            peer,
            &Frame::Amqp {
                channel: if matches!(p, Performative::Open(_)) {
                    0
                } else {
                    1
                },
                performative: Some(p),
                payload: vec![],
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
}
async fn read(peer: &mut DuplexStream) -> Performative {
    let Frame::Amqp {
        performative: Some(p),
        ..
    } = timeout(WAIT, read_frame(peer)).await.unwrap().unwrap()
    else {
        panic!("performative")
    };
    p
}
async fn pending<F: Future>(mut f: Pin<&mut F>) {
    assert!(
        tokio::task::unconstrained(poll_fn(|cx| Poll::Ready(f.as_mut().poll(cx))))
            .await
            .is_pending()
    );
}
async fn credit(sender: &Sender) -> CreditReservation {
    timeout(WAIT, sender.on_credit()).await.unwrap().unwrap()
}
async fn accepted(p: PendingDelivery) {
    assert!(matches!(
        timeout(WAIT, p).await.unwrap().unwrap().outcome(),
        Outcome::Accepted(_)
    ));
}
async fn attach(
    session: &mut ServerSession,
    peer: &mut DuplexStream,
    handle: u32,
    settled: bool,
) -> Sender {
    write(
        peer,
        Performative::Attach(Box::new(Attach {
            name: format!("wrap-{handle}"),
            handle,
            role: Role::Receiver,
            snd_settle_mode: if settled {
                SenderSettleMode::Settled
            } else {
                SenderSettleMode::Unsettled
            },
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
    )
    .await;
    let incoming = timeout(WAIT, session.next_incoming_attach())
        .await
        .unwrap()
        .unwrap();
    let endpoint = timeout(WAIT, session.accept_attach(incoming, 2 * 1024 * 1024))
        .await
        .unwrap()
        .unwrap();
    let Performative::Attach(a) = read(peer).await else {
        panic!("Attach, not an extra Transfer/Detach")
    };
    assert_eq!(a.initial_delivery_count, Some(0));
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("sender")
    };
    sender
}
struct Fixture {
    connection: ServerConnection,
    session: ServerSession,
    sender: Sender,
    peer: DuplexStream,
    frames: u32,
}
impl Fixture {
    async fn new(settled: bool) -> Self {
        let (stream, mut peer) = duplex(64 * 1024);
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(ServerConnection::accept(stream, "wrap", None), async {
                write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                    .await
                    .unwrap();
                assert_eq!(
                    read_protocol_header(&mut peer).await.unwrap(),
                    ProtocolHeader::AMQP
                );
                let mut open = Open::new("wrap-peer");
                open.max_frame_size = 1024;
                write(&mut peer, Performative::Open(open)).await;
                assert!(matches!(read(&mut peer).await, Performative::Open(_)));
            })
        })
        .await
        .unwrap();
        let mut connection = connection.unwrap();
        write(
            &mut peer,
            Performative::Begin(Begin {
                incoming_window: 4096,
                ..Begin::default()
            }),
        )
        .await;
        let incoming = timeout(WAIT, connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let mut session = timeout(WAIT, connection.accept_session(incoming))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(read(&mut peer).await, Performative::Begin(_)));
        let sender = attach(&mut session, &mut peer, 1, settled).await;
        Self {
            connection,
            session,
            sender,
            peer,
            frames: 0,
        }
    }
    async fn flow(
        &mut self,
        count: Option<u32>,
        credit: Option<u32>,
        drain: bool,
        echo: bool,
        window: u32,
    ) {
        write(
            &mut self.peer,
            Performative::Flow(Flow {
                handle: Some(1),
                delivery_count: count,
                link_credit: credit,
                next_incoming_id: Some(self.frames),
                incoming_window: window,
                outgoing_window: 2048,
                drain,
                echo,
                ..Flow::default()
            }),
        )
        .await;
    }
    async fn bootstrap(&mut self) {
        let mut count = 0;
        for n in [2147483647, 2147483647, 1] {
            self.flow(Some(count), Some(n), true, false, 4096).await;
            let request = timeout(WAIT, self.sender.on_drain())
                .await
                .unwrap()
                .unwrap();
            timeout(WAIT, self.sender.drained(request))
                .await
                .unwrap()
                .unwrap();
            count = count.wrapping_add(n);
            let Performative::Flow(f) = read(&mut self.peer).await else {
                panic!("bootstrap Flow, never a Transfer")
            };
            assert_eq!((f.delivery_count, f.link_credit), (Some(count), Some(0)));
            assert!(f.drain && !f.echo);
            assert_eq!(f.next_outgoing_id, 0);
        }
        assert_eq!(count, u32::MAX);
    }
    async fn transfer(&mut self, id: u32, marker: u8) -> Transfer {
        let Frame::Amqp {
            performative: Some(Performative::Transfer(t)),
            payload,
            ..
        } = timeout(WAIT, read_frame(&mut self.peer))
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("original Transfer")
        };
        assert_eq!(t.delivery_id, Some(id));
        assert_eq!(t.delivery_tag, Some(Binary::from(vec![marker])));
        assert!(!payload.is_empty());
        self.frames += 1;
        t
    }
    // AcceptAttach is enqueued after the original send poll. Its wire response
    // must follow any illegal Transfer from that send; this is a positive FIFO
    // proof of pause, not a quiet-socket timeout or an unpolled replacement send.
    async fn barrier(&mut self, handle: u32) {
        let _ = attach(&mut self.session, &mut self.peer, handle, true).await;
    }
    async fn shutdown(&mut self) {
        timeout(WAIT, self.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(
            timeout(WAIT, read_frame(&mut self.peer))
                .await
                .unwrap()
                .is_err()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn one_wrap_credit_pauses_original_second_send_through_repeated_echo_and_count_only_flow() {
    let mut f = Fixture::new(true).await;
    f.bootstrap().await;
    f.flow(Some(u32::MAX), Some(1), false, false, 4096).await;
    timeout(WAIT, credit(&f.sender).await.release())
        .await
        .unwrap()
        .unwrap();
    let mut first = Box::pin(
        f.sender
            .send_pending(Message::data(vec![1]), Binary::from(vec![1])),
    );
    let mut second = Box::pin(
        f.sender
            .send_pending(Message::data(vec![2]), Binary::from(vec![2])),
    );
    pending(first.as_mut()).await;
    pending(second.as_mut()).await;
    // Split field borrows keep the same two original futures alive.
    let Frame::Amqp {
        performative: Some(Performative::Transfer(t)),
        ..
    } = timeout(WAIT, read_frame(&mut f.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("one Transfer")
    };
    assert_eq!(
        (t.delivery_id, t.delivery_tag),
        (Some(0), Some(Binary::from(vec![1])))
    );
    f.frames = 1;
    accepted(timeout(WAIT, first.as_mut()).await.unwrap().unwrap()).await;
    drop(first);
    // Cannot call a whole-fixture mutable method while second borrows sender.
    let _barrier = attach(&mut f.session, &mut f.peer, 2, true).await;
    for (count, amount, echo) in [
        (u32::MAX, Some(1), true),
        (0, None, false),
        (0, Some(0), false),
    ] {
        write(
            &mut f.peer,
            Performative::Flow(Flow {
                handle: Some(1),
                delivery_count: Some(count),
                link_credit: amount,
                echo,
                next_incoming_id: Some(1),
                incoming_window: 4096,
                ..Flow::default()
            }),
        )
        .await;
    }
    let _barrier = attach(&mut f.session, &mut f.peer, 3, true).await;
    pending(second.as_mut()).await;
    write(
        &mut f.peer,
        Performative::Flow(Flow {
            handle: Some(1),
            delivery_count: Some(0),
            link_credit: Some(1),
            next_incoming_id: Some(1),
            incoming_window: 4096,
            ..Flow::default()
        }),
    )
    .await;
    let Frame::Amqp {
        performative: Some(Performative::Transfer(t)),
        ..
    } = timeout(WAIT, read_frame(&mut f.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("same queued send resumes")
    };
    assert_eq!(
        (t.delivery_id, t.delivery_tag),
        (Some(1), Some(Binary::from(vec![2])))
    );
    let delivered = timeout(WAIT, second.as_mut()).await.unwrap().unwrap();
    assert_eq!(delivered.identity().delivery_id(), 1);
    accepted(delivered).await;
    drop(second);
    f.barrier(4).await;
    f.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_uint_drain_uses_three_flows_at_zero_session_capacity_then_accepts_known_half_range() {
    let mut f = Fixture::new(true).await;
    f.flow(Some(0), Some(u32::MAX), true, true, 0).await;
    let request = timeout(WAIT, f.sender.on_drain()).await.unwrap().unwrap();
    timeout(WAIT, f.sender.drained(request))
        .await
        .unwrap()
        .unwrap();
    for (count, left) in [(2147483647, 2147483648), (4294967294, 1), (u32::MAX, 0)] {
        let Performative::Flow(flow) = read(&mut f.peer).await else {
            panic!("Flow, no synthetic message")
        };
        assert_eq!(
            (flow.delivery_count, flow.link_credit),
            (Some(count), Some(left))
        );
        assert_eq!(flow.next_outgoing_id, 0);
        assert!(flow.drain && !flow.echo);
    }
    f.flow(Some(2147483648), Some(2147483648), false, false, 1)
        .await;
    let guard = credit(&f.sender).await;
    let mut send = Box::pin(f.sender.send_pending_with_credit(
        guard,
        Message::data(vec![7]),
        Binary::from(vec![7]),
    ));
    pending(send.as_mut()).await;
    let Frame::Amqp {
        performative: Some(Performative::Transfer(t)),
        ..
    } = timeout(WAIT, read_frame(&mut f.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("real delivery")
    };
    assert_eq!(t.delivery_id, Some(0));
    accepted(timeout(WAIT, send.as_mut()).await.unwrap().unwrap()).await;
    drop(send);
    f.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn held_consumed_released_and_dropped_guards_do_not_return_revoked_wrap_permission() {
    for action in ["consume", "release", "drop"] {
        let mut f = Fixture::new(true).await;
        f.bootstrap().await;
        f.flow(Some(u32::MAX), Some(1), false, false, 4096).await;
        let guard = credit(&f.sender).await;
        f.flow(Some(u32::MAX), Some(0), true, false, 4096).await;
        let request = timeout(WAIT, f.sender.on_drain()).await.unwrap().unwrap();
        assert!(
            timeout(WAIT, f.sender.drained(request))
                .await
                .unwrap()
                .is_err()
        );
        if action == "consume" {
            let mut send = Box::pin(f.sender.send_pending_with_credit(
                guard,
                Message::data(vec![8]),
                Binary::from(vec![8]),
            ));
            pending(send.as_mut()).await;
            assert!(matches!(read(&mut f.peer).await, Performative::Transfer(_)));
            accepted(timeout(WAIT, send.as_mut()).await.unwrap().unwrap()).await;
            drop(send);
            f.frames = 1;
        } else if action == "release" {
            timeout(WAIT, guard.release()).await.unwrap().unwrap();
        } else {
            drop(guard);
        }
        let Performative::Flow(drained) = read(&mut f.peer).await else {
            panic!("drain only after reservation ends")
        };
        let count = if action == "consume" { 0 } else { u32::MAX };
        assert_eq!(
            (drained.delivery_count, drained.link_credit),
            (Some(count), Some(0))
        );
        assert_eq!(drained.next_outgoing_id, f.frames);
        let mut waiting = Box::pin(
            f.sender
                .send_pending(Message::data(vec![9]), Binary::from(vec![9])),
        );
        pending(waiting.as_mut()).await;
        let _barrier = attach(&mut f.session, &mut f.peer, 2, true).await;
        write(
            &mut f.peer,
            Performative::Flow(Flow {
                handle: Some(1),
                delivery_count: Some(count),
                link_credit: Some(1),
                next_incoming_id: Some(f.frames),
                incoming_window: 4096,
                ..Flow::default()
            }),
        )
        .await;
        let Frame::Amqp {
            performative: Some(Performative::Transfer(t)),
            ..
        } = timeout(WAIT, read_frame(&mut f.peer))
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("new grant resumes original")
        };
        assert_eq!(t.delivery_id, Some(f.frames));
        accepted(timeout(WAIT, waiting.as_mut()).await.unwrap().unwrap()).await;
        drop(waiting);
        f.shutdown().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn early_omission_is_initial_and_invalid_progress_retires_only_its_link_until_answered() {
    for bad in [None, Some(2), Some(1_u32.wrapping_add(1 << 31)), Some(0)] {
        let mut f = Fixture::new(true).await;
        f.flow(None, Some(1), false, false, 4096).await;
        let mut send = Box::pin(
            f.sender
                .send_pending(Message::data(vec![1]), Binary::from(vec![1])),
        );
        pending(send.as_mut()).await;
        let Frame::Amqp {
            performative: Some(Performative::Transfer(t)),
            ..
        } = timeout(WAIT, read_frame(&mut f.peer))
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("first")
        };
        assert_eq!(t.delivery_id, Some(0));
        accepted(timeout(WAIT, send.as_mut()).await.unwrap().unwrap()).await;
        drop(send);
        f.frames = 1;
        f.flow(None, Some(1), false, false, 4096).await;
        f.barrier(2).await;
        // Establish peer baseline 1; count-only cannot grant another delivery.
        f.flow(Some(1), None, false, false, 4096).await;
        f.barrier(3).await;
        f.flow(bad, Some(u32::MAX), false, false, 4096).await;
        let Performative::Detach(d) = read(&mut f.peer).await else {
            panic!("invalid report closes only this link")
        };
        assert_eq!(d.handle, 1);
        assert!(d.closed);
        assert_eq!(
            d.error.unwrap().condition,
            amqp::AmqpError::InvalidField.into()
        );
        f.flow(bad, Some(u32::MAX), false, false, 4096).await;
        f.barrier(4).await; // no duplicate Detach while its answer is outstanding
        write(
            &mut f.peer,
            Performative::Detach(Detach {
                handle: 1,
                closed: true,
                error: None,
            }),
        )
        .await;
        let replacement = attach(&mut f.session, &mut f.peer, 1, true).await;
        f.flow(Some(0), Some(1), false, false, 4096).await;
        let mut send =
            Box::pin(replacement.send_pending(Message::data(vec![3]), Binary::from(vec![3])));
        pending(send.as_mut()).await;
        f.transfer(1, 3).await;
        accepted(timeout(WAIT, send.as_mut()).await.unwrap().unwrap()).await;
        drop(send);
        f.shutdown().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn fragmented_unsettled_wrap_keeps_original_identity_and_zero_drain_behind_final_write() {
    let mut f = Fixture::new(false).await;
    f.bootstrap().await;
    f.flow(Some(u32::MAX), Some(1), false, false, 1).await;
    let mut sending = Box::pin(
        f.sender
            .send_pending(Message::data(vec![42; 2048]), Binary::from(vec![42])),
    );
    pending(sending.as_mut()).await;
    let Frame::Amqp {
        performative: Some(Performative::Transfer(first)),
        payload,
        ..
    } = timeout(WAIT, read_frame(&mut f.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("first fragment")
    };
    assert_eq!(
        (first.delivery_id, first.delivery_tag),
        (Some(0), Some(vec![42].into()))
    );
    assert!(first.more && !payload.is_empty());
    write(
        &mut f.peer,
        Performative::Flow(Flow {
            handle: Some(1),
            delivery_count: Some(0),
            link_credit: Some(0),
            drain: true,
            next_incoming_id: Some(1),
            incoming_window: 0,
            ..Flow::default()
        }),
    )
    .await;
    let request = timeout(WAIT, f.sender.on_drain()).await.unwrap().unwrap();
    assert!(
        timeout(WAIT, f.sender.drained(request))
            .await
            .unwrap()
            .is_err()
    );
    let _barrier = attach(&mut f.session, &mut f.peer, 2, true).await;
    pending(sending.as_mut()).await;
    write(
        &mut f.peer,
        Performative::Flow(Flow {
            next_incoming_id: Some(1),
            incoming_window: 4096,
            ..Flow::default()
        }),
    )
    .await;
    let mut frames = 1;
    loop {
        let Frame::Amqp {
            performative: Some(Performative::Transfer(t)),
            ..
        } = timeout(WAIT, read_frame(&mut f.peer))
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("continuation before drain")
        };
        assert!(t.delivery_id.is_none() && t.delivery_tag.is_none());
        frames += 1;
        if !t.more {
            break;
        }
    }
    let Performative::Flow(drained) = read(&mut f.peer).await else {
        panic!("drain after final Transfer")
    };
    assert_eq!(
        (
            drained.delivery_count,
            drained.link_credit,
            drained.next_outgoing_id
        ),
        (Some(0), Some(0), frames)
    );
    let original = timeout(WAIT, sending.as_mut()).await.unwrap().unwrap();
    assert_eq!(original.identity().delivery_id(), 0);
    drop(sending);
    write(
        &mut f.peer,
        Performative::Disposition(amqp::Disposition {
            role: Role::Receiver,
            first: 0,
            last: None,
            settled: true,
            state: Some(amqp::DeliveryState::Accepted(amqp::Accepted)),
            batchable: false,
        }),
    )
    .await;
    accepted(original).await;
    f.frames = frames;
    f.flow(Some(0), Some(1), false, false, 4096).await;
    let mut next = Box::pin(
        f.sender
            .send_pending(Message::data(vec![43]), Binary::from(vec![43])),
    );
    pending(next.as_mut()).await;
    let Frame::Amqp {
        performative: Some(Performative::Transfer(t)),
        ..
    } = timeout(WAIT, read_frame(&mut f.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("next real delivery")
    };
    assert_eq!(
        t.delivery_id,
        Some(1),
        "control progress and fragment frames allocate no delivery IDs"
    );
    let original = timeout(WAIT, next.as_mut()).await.unwrap().unwrap();
    drop(next);
    write(
        &mut f.peer,
        Performative::Disposition(amqp::Disposition {
            role: Role::Receiver,
            first: 1,
            last: None,
            settled: true,
            state: Some(amqp::DeliveryState::Accepted(amqp::Accepted)),
            batchable: false,
        }),
    )
    .await;
    accepted(original).await;
    f.shutdown().await;
}
