//! Raw wire witnesses for native receive custody (not the feature test client).
use amqp::{
    Attach, Begin, End, Flow, Frame, LinkEndpoint, Message, Open, Performative, ProtocolHeader,
    Receiver, ReceiverSettleMode, Role, SenderSettleMode, ServerConnection, ServerSession,
    Transfer, encode_message, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    io::{DuplexStream, duplex},
    time::timeout,
};
#[path = "../src/test_deadline.rs"]
mod test_deadline;
use test_deadline::WAIT;

async fn write(
    peer: &mut DuplexStream,
    channel: u16,
    performative: Performative,
    payload: Vec<u8>,
) {
    timeout(
        WAIT,
        write_frame(
            peer,
            &Frame::Amqp {
                channel,
                performative: Some(performative),
                payload,
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
}
async fn read(peer: &mut DuplexStream) -> (u16, Performative) {
    let Frame::Amqp {
        channel,
        performative: Some(p),
        ..
    } = timeout(WAIT, read_frame(peer)).await.unwrap().unwrap()
    else {
        panic!("AMQP frame")
    };
    (channel, p)
}
fn attach(handle: u32, role: Role) -> Attach {
    Attach {
        name: format!("incoming-{handle}"),
        handle,
        role,
        snd_settle_mode: SenderSettleMode::Settled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: None,
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}
fn transfer(handle: u32, id: Option<u32>, more: bool) -> Transfer {
    Transfer {
        handle,
        delivery_id: id,
        delivery_tag: id.map(|id| amqp::Binary::from(id.to_be_bytes().to_vec())),
        message_format: id.map(|_| 0),
        settled: id.map(|_| true),
        more,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}
struct Fixture {
    connection: ServerConnection,
    session: ServerSession,
    peer: DuplexStream,
}
impl Fixture {
    async fn new(seed: u32) -> Self {
        let (stream, mut peer) = duplex(64 * 1024);
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept(stream, "incoming-server", None),
                async {
                    write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                        .await
                        .unwrap();
                    assert_eq!(
                        read_protocol_header(&mut peer).await.unwrap(),
                        ProtocolHeader::AMQP
                    );
                    write(
                        &mut peer,
                        0,
                        Performative::Open(Open::new("incoming-peer")),
                        vec![],
                    )
                    .await;
                    assert!(matches!(read(&mut peer).await.1, Performative::Open(_)));
                }
            )
        })
        .await
        .unwrap();
        let mut connection = connection.unwrap();
        let session = Self::begin(&mut connection, &mut peer, 1, seed).await;
        let mut f = Self {
            connection,
            session,
            peer,
        };
        f.accept(100, Role::Receiver, 1).await;
        f
    }
    async fn begin(
        connection: &mut ServerConnection,
        peer: &mut DuplexStream,
        channel: u16,
        seed: u32,
    ) -> ServerSession {
        write(
            peer,
            channel,
            Performative::Begin(Begin {
                next_outgoing_id: seed,
                ..Begin::default()
            }),
            vec![],
        )
        .await;
        let incoming = timeout(WAIT, connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let session = timeout(WAIT, connection.accept_session(incoming))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(read(peer).await.1, Performative::Begin(_)));
        session
    }
    async fn accept(&mut self, handle: u32, role: Role, maximum: u64) -> LinkEndpoint {
        self.accept_at(handle, role, maximum, 0).await
    }
    async fn accept_at(
        &mut self,
        handle: u32,
        role: Role,
        maximum: u64,
        count: u32,
    ) -> LinkEndpoint {
        let receiving = role == Role::Sender;
        let mut offer = attach(handle, role);
        offer.initial_delivery_count = receiving.then_some(count);
        write(
            &mut self.peer,
            1,
            Performative::Attach(Box::new(offer)),
            vec![],
        )
        .await;
        let incoming = timeout(WAIT, self.session.next_incoming_attach())
            .await
            .unwrap()
            .unwrap();
        let endpoint = timeout(WAIT, self.session.accept_attach(incoming, maximum))
            .await
            .unwrap()
            .unwrap();
        let Performative::Attach(response) = read(&mut self.peer).await.1 else {
            panic!("Attach response")
        };
        if receiving {
            assert_eq!(
                response.max_message_size,
                Some(maximum.min(16 * 1024 * 1024))
            );
            let Performative::Flow(flow) = read(&mut self.peer).await.1 else {
                panic!("initial credit")
            };
            assert_eq!(flow.delivery_count, Some(count));
            assert_eq!(flow.link_credit, Some(2048));
        }
        endpoint
    }
    async fn receiver(&mut self, handle: u32, maximum: u64) -> Receiver {
        let LinkEndpoint::Receiver(receiver) = self.accept(handle, Role::Sender, maximum).await
        else {
            panic!("receiver")
        };
        receiver
    }
    async fn barrier(&mut self) -> Flow {
        self.barrier_flows().await.0
    }
    async fn barrier_flows(&mut self) -> (Flow, Vec<Flow>) {
        let mut flows = Vec::new();
        write(
            &mut self.peer,
            1,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 2048,
                handle: Some(100),
                delivery_count: Some(0),
                link_credit: Some(0),
                drain: true,
                ..Flow::default()
            }),
            vec![],
        )
        .await;
        loop {
            let (channel, p) = read(&mut self.peer).await;
            assert_eq!(channel, 1);
            let Performative::Flow(flow) = p else {
                panic!("Flow barrier: {p:?}")
            };
            if flow.drain {
                return (flow, flows);
            }
            flows.push(flow);
        }
    }
    async fn replenished(&mut self, expected: u32) -> Flow {
        let Performative::Flow(flow) = read(&mut self.peer).await.1 else {
            panic!("replenishment Flow")
        };
        assert!(!flow.drain);
        assert_eq!(flow.next_incoming_id, Some(expected));
        flow
    }
    async fn shutdown(&mut self) {
        timeout(WAIT, self.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
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
async fn interleaved_partials_exceed_2048_frames_and_wrap_from_peer_begin() {
    let seed = u32::MAX - 1200;
    let mut f = Fixture::new(seed).await;
    let mut a = f.receiver(1, 4096).await;
    let mut b = f.receiver(2, 4096).await;
    let encoded = encode_message(&Message::data(vec![7; 2050])).unwrap();
    let mut total = 0_u32;
    for (index, byte) in encoded.iter().enumerate() {
        for handle in [1, 2] {
            write(
                &mut f.peer,
                1,
                Performative::Transfer(transfer(
                    handle,
                    (index == 0).then_some(handle),
                    index + 1 != encoded.len(),
                )),
                vec![*byte],
            )
            .await;
            total += 1;
            if total.is_multiple_of(1024) {
                assert_eq!(
                    f.replenished(seed.wrapping_add(total))
                        .await
                        .incoming_window,
                    2048
                );
            }
        }
    }
    assert!(total > 4096);
    let flow = f.barrier().await;
    assert_eq!(flow.next_incoming_id, Some(seed.wrapping_add(total)));
    assert_eq!(flow.incoming_window, 2048 - total % 1024);
    assert_eq!(
        timeout(WAIT, a.recv())
            .await
            .unwrap()
            .unwrap()
            .encoded_message(),
        encoded
    );
    assert_eq!(
        timeout(WAIT, b.recv())
            .await
            .unwrap()
            .unwrap()
            .encoded_message(),
        encoded
    );
    f.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn stalled_consumer_zero_window_keeps_flow_dispatch_and_consumption_replenishes() {
    let mut f = Fixture::new(17).await;
    let mut receiver = f.receiver(1, 4096).await;
    // Register a real pending dequeue, then cancel its original waiter. The
    // subsequently arriving messages must remain entirely in queue custody.
    let mut waiting = Box::pin(receiver.recv());
    let state = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(waiting.as_mut(), cx))
    })
    .await;
    assert!(state.is_pending());
    drop(waiting);
    assert_eq!(f.barrier().await.incoming_window, 2048);
    // Empty encoded messages must still occupy both delivery and final-frame slots.
    for id in 0..2048 {
        write(
            &mut f.peer,
            1,
            Performative::Transfer(transfer(1, Some(id), false)),
            vec![],
        )
        .await;
    }
    let flow = f.barrier().await;
    assert_eq!(flow.next_incoming_id, Some(2065));
    assert_eq!(flow.incoming_window, 0);
    // A cancelled receive before polling cannot refund anything.
    drop(receiver.recv());
    assert_eq!(f.barrier().await.incoming_window, 0);
    let first = timeout(WAIT, receiver.recv()).await.unwrap().unwrap();
    let clone = first.clone();
    let flow = f.replenished(2065).await;
    assert_eq!(flow.incoming_window, 1);
    drop(first);
    drop(clone);
    assert_eq!(f.barrier().await.incoming_window, 1);
    let mut spare = f.receiver(2, 4096).await;
    write(
        &mut f.peer,
        1,
        Performative::Transfer(transfer(2, Some(2048), false)),
        vec![],
    )
    .await;
    assert_eq!(f.barrier().await.incoming_window, 0);
    f.shutdown().await;
    // Accepted queue custody remains drainable after original tasks are joined.
    for _ in 0..2047 {
        timeout(WAIT, receiver.recv()).await.unwrap().unwrap();
    }
    timeout(WAIT, spare.recv()).await.unwrap().unwrap();
    assert!(receiver.recv().await.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn over_window_ends_only_affected_session_and_old_consumption_cannot_refund_reuse() {
    let mut f = Fixture::new(0).await;
    let mut old = f.receiver(1, 4096).await;
    let _spare = f.receiver(2, 4096).await;
    let mut independent = Fixture::begin(&mut f.connection, &mut f.peer, 2, 29).await;
    for id in 0..2048 {
        write(
            &mut f.peer,
            1,
            Performative::Transfer(transfer(1, Some(id), false)),
            vec![],
        )
        .await;
    }
    assert_eq!(f.barrier().await.incoming_window, 0);
    write(
        &mut f.peer,
        1,
        Performative::Transfer(transfer(2, Some(2048), false)),
        vec![],
    )
    .await;
    // These frames have already crossed the local error End on the wire. They
    // must be discarded, not dispatched against a removed session/old links.
    write(
        &mut f.peer,
        1,
        Performative::Attach(Box::new(attach(77, Role::Sender))),
        vec![],
    )
    .await;
    write(
        &mut f.peer,
        1,
        Performative::Transfer(transfer(77, Some(2049), false)),
        vec![0xff],
    )
    .await;
    write(
        &mut f.peer,
        1,
        Performative::Flow(Flow {
            next_incoming_id: Some(u32::MAX),
            ..Flow::default()
        }),
        vec![],
    )
    .await;
    let (channel, p) = read(&mut f.peer).await;
    assert_eq!(channel, 1);
    let Performative::End(end) = p else {
        panic!("session error End")
    };
    assert_eq!(
        end.error.unwrap().condition.as_symbol().to_string(),
        "amqp:session:window-violation"
    );
    timeout(WAIT, f.session.on_end_owned()).await.unwrap();
    // A channel cannot be reused before the answering End. This offer must not
    // enter native intake or consume the later replacement's incarnation.
    write(
        &mut f.peer,
        1,
        Performative::Begin(Begin {
            next_outgoing_id: 42,
            ..Begin::default()
        }),
        vec![],
    )
    .await;
    old.recv().await.unwrap();
    // An actual delivery on another session proves crossed traffic did not
    // produce connection EOF or stop unrelated session progress.
    write(
        &mut f.peer,
        2,
        Performative::Attach(Box::new(attach(3, Role::Sender))),
        vec![],
    )
    .await;
    let incoming = timeout(WAIT, independent.next_incoming_attach())
        .await
        .unwrap()
        .unwrap();
    let LinkEndpoint::Receiver(mut other) =
        independent.accept_attach(incoming, 4096).await.unwrap()
    else {
        panic!("independent receiver")
    };
    assert!(matches!(
        read(&mut f.peer).await,
        (2, Performative::Attach(_))
    ));
    assert!(matches!(
        read(&mut f.peer).await,
        (2, Performative::Flow(_))
    ));
    let encoded = encode_message(&Message::data(vec![11])).unwrap();
    write(
        &mut f.peer,
        2,
        Performative::Transfer(transfer(3, Some(0), false)),
        encoded.clone(),
    )
    .await;
    assert_eq!(
        timeout(WAIT, other.recv())
            .await
            .unwrap()
            .unwrap()
            .encoded_message(),
        encoded
    );
    // Consume the peer's acknowledgement without answering End a second time.
    // The independent Attach response is an ordered positive wire barrier.
    write(&mut f.peer, 1, Performative::End(End::default()), vec![]).await;
    write(
        &mut f.peer,
        2,
        Performative::Attach(Box::new(attach(4, Role::Receiver))),
        vec![],
    )
    .await;
    let incoming = timeout(WAIT, independent.next_incoming_attach())
        .await
        .unwrap()
        .unwrap();
    independent.accept_attach(incoming, 1).await.unwrap();
    assert!(matches!(
        read(&mut f.peer).await,
        (2, Performative::Attach(_))
    ));
    f.session = Fixture::begin(&mut f.connection, &mut f.peer, 1, u32::MAX).await;
    f.accept(100, Role::Receiver, 1).await;
    let _new = f.receiver(1, 4096).await;
    for _ in 0..2047 {
        old.recv().await.unwrap();
    }
    let flow = f.barrier().await;
    assert_eq!(flow.next_incoming_id, Some(u32::MAX));
    assert_eq!(flow.incoming_window, 2048);
    f.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn reserved_assembly_survives_resource_refusal_and_abort_releases_headroom() {
    let mut f = Fixture::new(0).await;
    let mut receivers = Vec::new();
    for handle in 1..=6 {
        receivers.push(f.receiver(handle, u64::MAX).await);
    }
    let encoded = encode_message(&Message::data(vec![4; 3000])).unwrap();
    for handle in 1..=4 {
        write(
            &mut f.peer,
            1,
            Performative::Transfer(transfer(handle, Some(handle), true)),
            encoded[..1].to_vec(),
        )
        .await;
    }
    write(
        &mut f.peer,
        1,
        Performative::Transfer(transfer(5, Some(5), true)),
        encoded[..1].to_vec(),
    )
    .await;
    let Performative::Detach(detach) = read(&mut f.peer).await.1 else {
        panic!("resource refusal")
    };
    assert_eq!(detach.handle, 5);
    assert_eq!(
        detach.error.unwrap().condition.as_symbol().to_string(),
        "amqp:resource-limit-exceeded"
    );
    let mut aborted = transfer(1, None, false);
    aborted.aborted = true;
    write(&mut f.peer, 1, Performative::Transfer(aborted), vec![]).await;
    write(
        &mut f.peer,
        1,
        Performative::Transfer(transfer(6, Some(6), true)),
        encoded[..1].to_vec(),
    )
    .await;
    for handle in [2, 3, 4, 6] {
        write(
            &mut f.peer,
            1,
            Performative::Transfer(transfer(handle, None, false)),
            encoded[1..].to_vec(),
        )
        .await;
    }
    let flow = f.barrier().await;
    assert_eq!(flow.next_incoming_id, Some(11));
    for handle in [2, 3, 4, 6] {
        assert_eq!(
            receivers[handle - 1]
                .recv()
                .await
                .unwrap()
                .encoded_message(),
            encoded
        );
    }
    f.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_handoff_and_partials_retire_on_end_eof_and_stop() {
    for event in ["end", "eof", "stop"] {
        for full in [false, true] {
            let mut f = Fixture::new(0).await;
            let mut receiver = f.receiver(1, 4096).await;
            let _partial = f.receiver(2, 4096).await;
            let completed = if full { 2048 } else { 40 };
            for id in 0..completed {
                write(
                    &mut f.peer,
                    1,
                    Performative::Transfer(transfer(1, Some(id), false)),
                    vec![],
                )
                .await;
            }
            if !full {
                write(
                    &mut f.peer,
                    1,
                    Performative::Transfer(transfer(2, Some(40), true)),
                    vec![0],
                )
                .await;
            }
            let flow = f.barrier().await;
            assert_eq!(
                flow.next_incoming_id,
                Some(if full { completed } else { completed + 1 })
            );
            if full {
                assert_eq!(flow.incoming_window, 0);
            }
            match event {
                "end" => {
                    write(&mut f.peer, 1, Performative::End(End::default()), vec![]).await;
                    assert!(matches!(read(&mut f.peer).await.1, Performative::End(_)));
                }
                "eof" => {
                    use tokio::io::AsyncWriteExt;
                    f.peer.shutdown().await.unwrap();
                }
                "stop" => f.connection.stop(),
                _ => unreachable!(),
            }
            timeout(WAIT, f.session.on_end_owned()).await.unwrap();
            f.shutdown().await;
            for _ in 0..completed {
                receiver.recv().await.unwrap();
            }
            assert!(receiver.recv().await.is_err());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn effective_native_message_ceiling_accepts_exact_bytes_and_refuses_one_over_only_on_its_link()
 {
    const MAXIMUM: usize = 16 * 1024 * 1024;
    let mut f = Fixture::new(0).await;
    let mut receiver = f.receiver(1, u64::MAX).await;
    let mut other = f.receiver(2, 4096).await;
    let mut frames = 0;
    for (id, size) in [(0, MAXIMUM), (1, MAXIMUM + 1)] {
        // One Data section's descriptor and binary32 constructor use eight bytes.
        let encoded = encode_message(&Message::data(vec![8; size - 8])).unwrap();
        assert_eq!(encoded.len(), size);
        let chunks = encoded.chunks(128 * 1024).collect::<Vec<_>>();
        for (index, payload) in chunks.iter().enumerate() {
            write(
                &mut f.peer,
                1,
                Performative::Transfer(transfer(
                    1,
                    (index == 0).then_some(id),
                    index + 1 != chunks.len(),
                )),
                payload.to_vec(),
            )
            .await;
            frames += 1;
        }
        if size == MAXIMUM {
            let delivery = timeout(WAIT, receiver.recv()).await.unwrap().unwrap();
            assert_eq!(delivery.encoded_message(), encoded);
        } else {
            let Performative::Detach(detach) = read(&mut f.peer).await.1 else {
                panic!("oversize Detach")
            };
            assert_eq!(detach.handle, 1);
            assert_eq!(
                detach.error.unwrap().condition.as_symbol().to_string(),
                "amqp:link:message-size-exceeded"
            );
        }
    }
    let encoded = encode_message(&Message::data(vec![9])).unwrap();
    write(
        &mut f.peer,
        1,
        Performative::Transfer(transfer(2, Some(2), false)),
        encoded.clone(),
    )
    .await;
    assert_eq!(f.barrier().await.next_incoming_id, Some(frames + 1));
    assert_eq!(other.recv().await.unwrap().encoded_message(), encoded);
    f.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn incoming_zero_window_still_dispatches_flow_that_resumes_the_original_outgoing_send() {
    let mut f = Fixture::new(0).await;
    let _stalled = f.receiver(1, 4096).await;
    let LinkEndpoint::Sender(sender) = f.accept(101, Role::Receiver, 1).await else {
        panic!("sender")
    };
    write(
        &mut f.peer,
        1,
        Performative::Flow(Flow {
            next_incoming_id: Some(0),
            incoming_window: 0,
            handle: Some(101),
            delivery_count: Some(0),
            link_credit: Some(2),
            ..Flow::default()
        }),
        vec![],
    )
    .await;
    let credit = timeout(WAIT, sender.on_credit()).await.unwrap().unwrap();
    let fence = timeout(WAIT, sender.on_credit()).await.unwrap().unwrap();
    let message = Message::data(vec![42]);
    let mut sending = Box::pin(sender.send_pending_with_credit(
        credit,
        message.clone(),
        amqp::Binary::from(vec![42]),
    ));
    let state = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(sending.as_mut(), cx))
    })
    .await;
    assert!(state.is_pending());
    // Original command FIFO proves this send is owned and paused, not unpolled.
    timeout(WAIT, fence.release()).await.unwrap().unwrap();
    for id in 0..2048 {
        write(
            &mut f.peer,
            1,
            Performative::Transfer(transfer(1, Some(id), false)),
            vec![],
        )
        .await;
    }
    write(
        &mut f.peer,
        1,
        Performative::Flow(Flow {
            next_incoming_id: Some(0),
            incoming_window: 0,
            handle: Some(100),
            delivery_count: Some(0),
            link_credit: Some(0),
            drain: true,
            ..Flow::default()
        }),
        vec![],
    )
    .await;
    let Performative::Flow(flow) = read(&mut f.peer).await.1 else {
        panic!("paused-send barrier")
    };
    assert!(flow.drain);
    assert_eq!(flow.next_incoming_id, Some(2048));
    assert_eq!(flow.incoming_window, 0);
    assert_eq!(flow.next_outgoing_id, 0);
    write(
        &mut f.peer,
        1,
        Performative::Flow(Flow {
            next_incoming_id: Some(0),
            incoming_window: 1,
            ..Flow::default()
        }),
        vec![],
    )
    .await;
    let Frame::Amqp {
        channel: 1,
        performative: Some(Performative::Transfer(value)),
        payload,
    } = timeout(WAIT, read_frame(&mut f.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("resumed original Transfer")
    };
    assert_eq!(value.delivery_id, Some(0));
    assert!(!value.more);
    assert_eq!(amqp::decode_message(&payload).unwrap(), message);
    let delivery = timeout(WAIT, sending.as_mut()).await.unwrap().unwrap();
    assert_eq!(delivery.identity().delivery_id(), 0);
    assert!(matches!(
        timeout(WAIT, delivery).await.unwrap().unwrap().outcome(),
        amqp::Outcome::Accepted(_)
    ));
    drop(sending);
    f.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn one_link_sustains_6144_deliveries_across_count_wrap_and_consumer_stalls() {
    let mut f = Fixture::new(53).await;
    let seed = u32::MAX - 1000;
    let LinkEndpoint::Receiver(mut receiver) = f.accept_at(1, Role::Sender, 4096, seed).await
    else {
        panic!("receiver")
    };
    let mut sent = 0_u32;
    let mut credit = 2048;
    while sent < 6144 {
        let batch = credit.min(6144 - sent);
        assert!(
            batch > 0,
            "actual consumption must restore sender admission"
        );
        for id in sent..sent + batch {
            write(
                &mut f.peer,
                1,
                Performative::Transfer(transfer(1, Some(id), false)),
                encode_message(&Message::data(id.to_be_bytes().to_vec())).unwrap(),
            )
            .await;
        }
        sent += batch;
        credit -= batch;
        let (stalled, flows) = f.barrier_flows().await;
        assert!(
            flows.iter().all(|flow| flow.handle != Some(1)),
            "no consumption, no link refill"
        );
        assert_eq!(stalled.next_incoming_id, Some(53 + sent));
        if sent == 2048 {
            assert_eq!(stalled.incoming_window, 0);
            drop(receiver.recv());
            assert!(
                f.barrier_flows().await.1.is_empty(),
                "cancelled recv returns no capacity"
            );
        }
        for id in sent - batch..sent {
            assert_eq!(
                receiver.recv().await.unwrap().message(),
                &Message::data(id.to_be_bytes().to_vec())
            );
        }
        let (_, flows) = f.barrier_flows().await;
        for flow in flows.into_iter().filter(|flow| flow.handle == Some(1)) {
            assert_eq!(flow.delivery_count, Some(seed.wrapping_add(sent)));
            credit = flow.link_credit.unwrap();
            assert!(credit <= 2048);
        }
    }
    assert!(seed.wrapping_add(sent) < seed);
    f.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn receiving_echo_and_drain_are_explicitly_refused_without_false_progress() {
    for echo in [false, true] {
        let mut f = Fixture::new(0).await;
        let mut old = f.receiver(1, 4096).await;
        write(
            &mut f.peer,
            1,
            Performative::Transfer(transfer(1, Some(0), true)),
            vec![0],
        )
        .await;
        write(
            &mut f.peer,
            1,
            Performative::Flow(Flow {
                handle: Some(1),
                delivery_count: Some(1000),
                drain: !echo,
                echo,
                incoming_window: 2048,
                ..Flow::default()
            }),
            vec![],
        )
        .await;
        let Performative::Detach(detach) = read(&mut f.peer).await.1 else {
            panic!("explicit refusal")
        };
        assert_eq!(
            detach.error.unwrap().condition.as_symbol().to_string(),
            "amqp:not-implemented"
        );
        timeout(WAIT, old.on_detach()).await.unwrap();
        // An already in-flight transfer can consume a frame but cannot recreate
        // the refused link or fabricate a delivery acknowledgement.
        write(
            &mut f.peer,
            1,
            Performative::Transfer(transfer(1, None, false)),
            vec![1],
        )
        .await;
        assert_eq!(f.barrier().await.next_incoming_id, Some(2));
        assert!(old.recv().await.is_err());
        f.shutdown().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn retired_queue_dequeue_never_refills_a_reused_handle() {
    let mut f = Fixture::new(0).await;
    let mut old = f.receiver(1, 4096).await;
    for id in 0..1024 {
        write(
            &mut f.peer,
            1,
            Performative::Transfer(transfer(1, Some(id), false)),
            vec![],
        )
        .await;
    }
    f.barrier().await;
    write(
        &mut f.peer,
        1,
        Performative::Detach(amqp::Detach {
            handle: 1,
            closed: true,
            error: None,
        }),
        vec![],
    )
    .await;
    assert!(matches!(read(&mut f.peer).await.1, Performative::Detach(_)));
    let _new = f.receiver(1, 4096).await;
    for _ in 0..1024 {
        old.recv().await.unwrap();
    }
    let (flow, refunds) = f.barrier_flows().await;
    assert_eq!(flow.incoming_window, 2048);
    assert!(
        refunds.iter().all(|flow| flow.handle != Some(1)),
        "old custody cannot grant new link credit"
    );
    f.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn fragmented_aborts_refill_once_per_delivery_not_per_repeated_continuation_id() {
    let mut f = Fixture::new(0).await;
    let seed = u32::MAX - 511;
    let _receiver = f.accept_at(1, Role::Sender, 4096, seed).await;
    for id in 0..1024 {
        write(
            &mut f.peer,
            1,
            Performative::Transfer(transfer(1, Some(id), true)),
            vec![0],
        )
        .await;
        let mut abort = transfer(1, Some(id), false);
        abort.aborted = true;
        write(&mut f.peer, 1, Performative::Transfer(abort), vec![]).await;
    }
    let (session, flows) = f.barrier_flows().await;
    assert_eq!(session.next_incoming_id, Some(2048));
    assert_eq!(session.incoming_window, 2048);
    let refunds = flows
        .iter()
        .filter(|flow| flow.handle == Some(1))
        .collect::<Vec<_>>();
    assert_eq!(refunds.len(), 1);
    assert_eq!(refunds[0].delivery_count, Some(seed.wrapping_add(1024)));
    assert_eq!(refunds[0].link_credit, Some(2048));
    f.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_a_stalled_consumer_returns_global_storage_but_no_more_link_credit() {
    let mut f = Fixture::new(0).await;
    let receiver = f.receiver(1, 4096).await;
    for id in 0..2048 {
        write(
            &mut f.peer,
            1,
            Performative::Transfer(transfer(1, Some(id), false)),
            vec![],
        )
        .await;
    }
    assert_eq!(f.barrier().await.incoming_window, 0);
    drop(receiver);
    let (session, flows) = f.barrier_flows().await;
    assert_eq!(session.incoming_window, 2048);
    assert!(flows.iter().all(|flow| flow.handle != Some(1)));
    f.shutdown().await;
}
