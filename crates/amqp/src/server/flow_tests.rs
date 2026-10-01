use tokio::{io::DuplexStream, time::timeout};

use super::*;
use crate::{Source, Target};

const DEADLINE: Duration = Duration::from_secs(5);

#[tokio::test]
async fn queued_consumption_does_not_publish_credit_after_local_detach() {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let mut writer = FrameWriter::new(wire, 512).expect("writer");
    let mut session = SessionState::new(&Begin::default());
    session.local_begin_sent = true;
    let mut delivery_queues = Vec::new();
    for handle in 0..2 {
        let consumption = Arc::new(Consumption::new(Arc::new(Notify::new())));
        let mut credit = ReceiveCredit::new(0, LINK_CREDIT, consumption.clone());
        credit.take_refill().expect("initial grant");
        credit.try_begin_delivery().expect("reserved delivery");
        consumption.consumed();
        let (deliveries, receiver) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        delivery_queues.push(receiver);
        let (detached, _) = watch::channel(false);
        session.links.insert(
            handle,
            LinkState::Receiving(ReceivingLink {
                max_message_size: u64::MAX,
                deliveries,
                partial: None,
                detached,
                credit,
                decoders: MessageFormatDecoders::default(),
                identity: LinkIdentity::new(),
                sender_settle_mode: SenderSettleMode::Mixed,
                receiver_settle_mode: ReceiverSettleMode::First,
            }),
        );
    }
    remember_closing_handle(&mut session, 0).expect("local detach already sent");
    let mut sessions = HashMap::from([(0, session)]);
    refresh_consumed(&mut writer, &mut sessions)
        .await
        .expect("queued consumption remains valid");
    refill_link(0, 0, sessions.get_mut(&0).expect("session"), &mut writer)
        .await
        .expect("closing-link transfer refill remains valid");
    writer
        .write_amqp(
            0,
            Performative::Flow(sessions[&0].flow.snapshot().flow(None, None)),
            Vec::new(),
        )
        .await
        .expect("ordered barrier");
    let Frame::Amqp {
        performative: Some(Performative::Flow(healthy)),
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("healthy link receives credit");
    };
    assert_eq!(healthy.handle, Some(1));
    assert_eq!(healthy.delivery_count, Some(1));
    assert_eq!(healthy.link_credit, Some(LINK_CREDIT));
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::Flow(Flow { handle: None, .. })),
            ..
        }
    ));
    assert!(!sessions[&0].ending);
}

async fn next_frame(peer: &mut DuplexStream) -> Frame {
    timeout(DEADLINE, read_frame(peer))
        .await
        .expect("flow processing must not wait for the application")
        .expect("valid AMQP frame")
}

async fn server_pair(peer_maximum: u32) -> (ServerConnection, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let opening = async {
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("server header");
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open {
                max_frame_size: peer_maximum,
                ..Open::new("flow-peer")
            }),
            Vec::new(),
        )
        .await
        .expect("peer open");
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        peer
    };
    let (connection, peer) = tokio::join!(ServerConnection::accept(wire, "server", None), opening);
    (connection.expect("server connection"), peer)
}

async fn begin(
    connection: &mut ServerConnection,
    peer: &mut DuplexStream,
    channel: u16,
    peer_begin: Begin,
) -> ServerSession {
    write_amqp(peer, channel, Performative::Begin(peer_begin), Vec::new())
        .await
        .expect("peer begin");
    let incoming = timeout(DEADLINE, connection.next_incoming_session())
        .await
        .expect("incoming begin")
        .expect("connection open");
    let session = timeout(DEADLINE, connection.accept_session(incoming))
        .await
        .expect("accept begin")
        .expect("valid session");
    assert!(matches!(
        next_frame(peer).await,
        Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Begin(_)),
            ..
        } if actual == channel
    ));
    session
}

fn attach(handle: u32, role: Role, second: bool, initial_count: u32) -> Attach {
    Attach {
        name: format!("flow-link-{handle}"),
        handle,
        role: role.clone(),
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: if second {
            ReceiverSettleMode::Second
        } else {
            ReceiverSettleMode::First
        },
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue")),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: (role == Role::Sender).then_some(initial_count),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

async fn accept_link(
    session: &mut ServerSession,
    peer: &mut DuplexStream,
    requested: Attach,
) -> (LinkEndpoint, Option<Flow>) {
    let role = requested.role.clone();
    let handle = requested.handle;
    write_amqp(
        peer,
        session.channel,
        Performative::Attach(Box::new(requested)),
        Vec::new(),
    )
    .await
    .expect("peer attach");
    let incoming = timeout(DEADLINE, session.next_incoming_attach())
        .await
        .expect("incoming attach")
        .expect("session open");
    let endpoint = timeout(DEADLINE, session.accept_attach(incoming, 4 * 1024 * 1024))
        .await
        .expect("accept attach")
        .expect("valid link");
    assert!(matches!(
        next_frame(peer).await,
        Frame::Amqp {
            performative: Some(Performative::Attach(attach)),
            ..
        } if attach.handle == handle
    ));
    let credit = if role == Role::Sender {
        let Frame::Amqp {
            performative: Some(Performative::Flow(flow)),
            ..
        } = next_frame(peer).await
        else {
            panic!("initial receiver credit");
        };
        Some(flow)
    } else {
        None
    };
    (endpoint, credit)
}

fn peer_flow(
    next_incoming_id: u32,
    incoming_window: u32,
    next_outgoing_id: u32,
    handle: Option<u32>,
    delivery_count: Option<u32>,
    link_credit: Option<u32>,
    echo: bool,
) -> Flow {
    Flow {
        next_incoming_id: Some(next_incoming_id),
        incoming_window,
        next_outgoing_id,
        outgoing_window: SESSION_WINDOW,
        handle,
        delivery_count,
        link_credit,
        echo,
        ..Flow::default()
    }
}

fn transfer() -> Transfer {
    Transfer {
        handle: 0,
        delivery_id: None,
        delivery_tag: Some(Vec::new().into()),
        message_format: Some(0),
        settled: None,
        more: false,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

async fn echo(peer: &mut DuplexStream, channel: u16, flow: Flow) -> Flow {
    write_amqp(peer, channel, Performative::Flow(flow), Vec::new())
        .await
        .expect("peer flow");
    let Frame::Amqp {
        channel: actual,
        performative: Some(Performative::Flow(flow)),
        ..
    } = next_frame(peer).await
    else {
        panic!("flow echo must remain responsive");
    };
    assert_eq!(actual, channel);
    assert!(!flow.echo);
    flow
}

async fn enqueue(
    sender: &Sender,
    message: Message,
) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
    let (reply, response) = oneshot::channel();
    sender
        .commands
        .send(Command::Send {
            channel: sender.channel,
            handle: sender.handle,
            identity: sender.identity.clone(),
            message: Box::new(message),
            delivery_tag: vec![1, 2, 3].into(),
            reply,
        })
        .await
        .expect("live command queue");
    response
}

#[tokio::test]
async fn one_message_credit_resumes_fragments_on_handleless_session_flow_and_latches_early_outcome()
{
    let (mut connection, mut peer) = server_pair(512).await;
    let mut session = begin(
        &mut connection,
        &mut peer,
        0,
        Begin {
            incoming_window: 1,
            ..Begin::default()
        },
    )
    .await;
    let (endpoint, _) =
        accept_link(&mut session, &mut peer, attach(0, Role::Receiver, true, 0)).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("local sender");
    };
    write_amqp(
        &mut peer,
        0,
        Performative::Flow(peer_flow(0, 1, 0, Some(0), Some(0), Some(1), false)),
        Vec::new(),
    )
    .await
    .expect("one delivery grant");
    let message = Message::data(vec![7; 1_600]);
    let encoded = encode_message(&message).expect("message encoding");
    let mut response = enqueue(&sender, message).await;
    let Frame::Amqp {
        performative: Some(Performative::Transfer(first)),
        payload,
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("first fragment");
    };
    assert_eq!(first.delivery_id, Some(0));
    assert!(first.more);
    let mut received = payload;
    let mut frames = 1;
    write_amqp(
        &mut peer,
        0,
        Performative::Disposition(Disposition {
            role: Role::Receiver,
            first: 0,
            last: None,
            settled: false,
            state: Some(DeliveryState::Accepted(Accepted)),
            batchable: false,
        }),
        Vec::new(),
    )
    .await
    .expect("early second-mode outcome");
    let barrier = echo(&mut peer, 0, peer_flow(1, 0, 0, None, None, None, true)).await;
    assert_eq!(barrier.next_outgoing_id, 1);
    assert!(matches!(
        response.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

    loop {
        write_amqp(
            &mut peer,
            0,
            Performative::Flow(peer_flow(frames, 1, 0, None, None, None, false)),
            Vec::new(),
        )
        .await
        .expect("one more session frame, no additional message credit");
        let frame = next_frame(&mut peer).await;
        assert!(crate::encode_frame(&frame).expect("frame encoding").len() <= 512);
        let Frame::Amqp {
            performative: Some(Performative::Transfer(transfer)),
            payload,
            ..
        } = frame
        else {
            panic!("continuation fragment");
        };
        assert_eq!(transfer.delivery_id, None);
        assert_eq!(transfer.delivery_tag, None);
        received.extend(payload);
        frames += 1;
        if !transfer.more {
            break;
        }
    }
    assert!(frames > 2);
    assert_eq!(received, encoded);
    let outcome = timeout(DEADLINE, response)
        .await
        .expect("latched outcome resolves after the final frame")
        .expect("send reply")
        .expect("accepted delivery");
    assert_eq!(
        outcome.acknowledgement.as_ref().map(AckIdentity::id),
        Some(0)
    );
    let (reply, acknowledged) = oneshot::channel();
    connection
        .commands
        .send(Command::SettleOutgoing {
            channel: 0,
            handle: 0,
            owner: sender.identity.clone(),
            identity: outcome.acknowledgement,
            state: DeliveryState::Accepted(Accepted),
            reply,
        })
        .await
        .expect("second-mode acknowledgment command");
    timeout(DEADLINE, acknowledged)
        .await
        .expect("pending acknowledgment was installed before replying")
        .expect("ack reply")
        .expect("ack succeeds");
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::Disposition(Disposition {
                role: Role::Sender,
                first: 0,
                settled: true,
                ..
            })),
            ..
        }
    ));

    write_amqp(
        &mut peer,
        0,
        Performative::Flow(peer_flow(frames, 1, 0, Some(0), Some(1), Some(1), false)),
        Vec::new(),
    )
    .await
    .expect("next delivery grant");
    let next = enqueue(&sender, Message::data(vec![9])).await;
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::Transfer(Transfer {
                delivery_id: Some(1),
                more: false,
                ..
            })),
            ..
        }
    ));
    drop(next);
    connection.shutdown().await;
}

#[tokio::test]
async fn full_delivery_queue_keeps_echo_responsive_and_consumption_refills_exactly_one_slot_at_wrap()
 {
    let (mut connection, mut peer) = server_pair(512).await;
    let mut session = begin(
        &mut connection,
        &mut peer,
        0,
        Begin {
            next_outgoing_id: u32::MAX,
            ..Begin::default()
        },
    )
    .await;
    let mut requested = attach(0, Role::Sender, false, u32::MAX);
    requested.snd_settle_mode = SenderSettleMode::Mixed;
    let (endpoint, initial) = accept_link(&mut session, &mut peer, requested).await;
    let LinkEndpoint::Receiver(mut receiver) = endpoint else {
        panic!("local receiver");
    };
    assert_eq!(
        initial.expect("initial grant").link_credit,
        Some(LINK_CREDIT)
    );
    assert!(
        timeout(Duration::from_millis(1), receiver.recv())
            .await
            .is_err()
    );
    let unchanged = echo(
        &mut peer,
        0,
        peer_flow(
            0,
            SESSION_WINDOW,
            u32::MAX,
            Some(0),
            Some(u32::MAX),
            None,
            true,
        ),
    )
    .await;
    assert_eq!(unchanged.delivery_count, Some(u32::MAX));
    assert_eq!(unchanged.link_credit, Some(LINK_CREDIT));
    let encoded = encode_message(&Message::data(vec![6])).expect("encoded message");
    for id in 0..LINK_CREDIT {
        write_amqp(
            &mut peer,
            0,
            Performative::Transfer(Transfer {
                handle: 0,
                delivery_id: Some(id),
                settled: Some(true),
                ..transfer()
            }),
            encoded.clone(),
        )
        .await
        .expect("published slot grant");
    }
    let count = u32::MAX.wrapping_add(LINK_CREDIT);
    let full = echo(
        &mut peer,
        0,
        peer_flow(0, SESSION_WINDOW, count, Some(0), Some(count), None, true),
    )
    .await;
    assert_eq!(full.delivery_count, Some(count));
    assert_eq!(full.link_credit, Some(0));
    assert_eq!(receiver.recv().await.expect("queued delivery").id, 0);
    let Frame::Amqp {
        performative: Some(Performative::Flow(refill)),
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("consumption notification refills one slot");
    };
    assert_eq!(refill.delivery_count, Some(count));
    assert_eq!(refill.link_credit, Some(1));
    write_amqp(
        &mut peer,
        0,
        Performative::Transfer(Transfer {
            handle: 0,
            delivery_id: Some(LINK_CREDIT),
            settled: Some(true),
            ..transfer()
        }),
        encoded,
    )
    .await
    .expect("only the freed slot is used");
    let refilled = echo(
        &mut peer,
        0,
        peer_flow(
            0,
            SESSION_WINDOW,
            count.wrapping_add(1),
            Some(0),
            Some(count.wrapping_add(1)),
            None,
            true,
        ),
    )
    .await;
    assert_eq!(refilled.link_credit, Some(0));
    connection.shutdown().await;
}

#[tokio::test]
async fn partial_and_first_frame_abort_release_only_their_reserved_slot() {
    let (mut connection, mut peer) = server_pair(512).await;
    let mut session = begin(&mut connection, &mut peer, 0, Begin::default()).await;
    let mut requested = attach(0, Role::Sender, false, 0);
    requested.snd_settle_mode = SenderSettleMode::Mixed;
    let (endpoint, _) = accept_link(&mut session, &mut peer, requested).await;
    let LinkEndpoint::Receiver(mut receiver) = endpoint else {
        panic!("local receiver");
    };
    write_amqp(
        &mut peer,
        0,
        Performative::Transfer(Transfer {
            handle: 0,
            delivery_id: Some(0),
            more: true,
            ..transfer()
        }),
        vec![0],
    )
    .await
    .expect("partial first frame");
    write_amqp(
        &mut peer,
        0,
        Performative::Transfer(Transfer {
            handle: 0,
            aborted: true,
            ..transfer()
        }),
        Vec::new(),
    )
    .await
    .expect("partial abort");
    let Frame::Amqp {
        performative: Some(Performative::Flow(refill)),
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("partial reservation released");
    };
    assert_eq!(refill.delivery_count, Some(1));
    assert_eq!(refill.link_credit, Some(LINK_CREDIT));
    write_amqp(
        &mut peer,
        0,
        Performative::Transfer(Transfer {
            handle: 0,
            delivery_id: Some(1),
            settled: Some(true),
            ..transfer()
        }),
        encode_message(&Message::data(vec![4])).expect("encoded message"),
    )
    .await
    .expect("queued delivery");
    write_amqp(
        &mut peer,
        0,
        Performative::Transfer(Transfer {
            handle: 0,
            delivery_id: Some(2),
            aborted: true,
            ..transfer()
        }),
        Vec::new(),
    )
    .await
    .expect("standalone abort");
    let Frame::Amqp {
        performative: Some(Performative::Flow(refill)),
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("standalone reservation released");
    };
    assert_eq!(refill.delivery_count, Some(3));
    assert_eq!(refill.link_credit, Some(LINK_CREDIT - 1));
    assert_eq!(
        receiver
            .recv()
            .await
            .expect("abort did not consume queued delivery")
            .id,
        1
    );
    connection.shutdown().await;
}

#[tokio::test]
async fn wrapping_disposition_resolves_existing_ids_without_reusing_an_outstanding_alias() {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let mut writer = FrameWriter::new(wire, 512).expect("frame writer");
    let mut session = SessionState::new(&Begin::default());
    session.local_begin_sent = true;
    session.flow = SessionWindow::new(u32::MAX, 0, SESSION_WINDOW, SESSION_WINDOW, SESSION_WINDOW);
    session.next_delivery_id = u32::MAX;
    let mut credit = LinkCredit::new(u32::MAX);
    credit
        .update_peer(Some(u32::MAX), 2, false)
        .expect("wrapped link grant");
    let (old_reply, mut old_outcome) = oneshot::channel();
    let (detached, _) = watch::channel(false);
    let identity = LinkIdentity::new();
    session.links.insert(
        0,
        LinkState::Sending(Box::new(SendingLink {
            identity: identity.clone(),
            auto_acknowledge: false,
            max_message_size: None,
            receiver_settle_mode: ReceiverSettleMode::First,
            settle_mode: SenderSettleMode::Unsettled,
            credit,
            queued: VecDeque::new(),
            active: None,
            unsettled: HashMap::from([(
                0,
                OutgoingDelivery {
                    reply: old_reply,
                    outcome: None,
                },
            )]),
            pending_acknowledgements: HashMap::new(),
            detached,
        })),
    );
    let mut sessions = HashMap::from([(0, session)]);
    let mut outcomes = Vec::new();
    for value in [1, 2] {
        let (reply, response) = oneshot::channel();
        handle_command(
            Command::Send {
                channel: 0,
                handle: 0,
                identity: identity.clone(),
                message: Box::new(Message::data(vec![value])),
                delivery_tag: vec![value].into(),
                reply,
            },
            &mut writer,
            &mut sessions,
            512,
        )
        .await
        .expect("queue send");
        outcomes.push(response);
    }
    let mut cursor = 0;
    assert!(
        !pump_connection(&mut writer, &mut sessions, &mut cursor)
            .await
            .expect("pump until collision")
    );
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::Transfer(Transfer {
                delivery_id: Some(u32::MAX),
                ..
            })),
            ..
        }
    ));
    assert_eq!(sessions[&0].next_delivery_id, 0);
    assert_eq!(sessions[&0].flow.snapshot().next_outgoing_id, 0);
    let LinkState::Sending(link) = &sessions[&0].links[&0] else {
        panic!("sending link");
    };
    assert_eq!(link.queued.len(), 1);
    assert_eq!(link.unsettled.len(), 2);
    assert!(matches!(
        old_outcome.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    apply_disposition(
        0,
        Disposition {
            role: Role::Receiver,
            first: u32::MAX,
            last: Some(0),
            settled: true,
            state: Some(DeliveryState::Accepted(Accepted)),
            batchable: false,
        },
        &mut writer,
        &mut sessions,
    )
    .await
    .expect("wrapped existing-key membership");
    old_outcome
        .await
        .expect("old alias reply")
        .expect("old alias accepted");
    outcomes
        .remove(0)
        .await
        .expect("MAX reply")
        .expect("MAX accepted");
    assert!(
        !pump_connection(&mut writer, &mut sessions, &mut cursor)
            .await
            .expect("resume after alias released")
    );
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::Transfer(Transfer {
                delivery_id: Some(0),
                ..
            })),
            ..
        }
    ));
    let LinkState::Sending(link) = &sessions[&0].links[&0] else {
        panic!("sending link");
    };
    assert_eq!(link.unsettled.len(), 1);
    assert_eq!(sessions[&0].flow.snapshot().next_outgoing_id, 1);
}

#[tokio::test]
async fn unknown_link_flow_ends_only_its_session_and_does_not_allocate_pending_state() {
    let (mut connection, mut peer) = server_pair(512).await;
    let _refused = begin(&mut connection, &mut peer, 0, Begin::default()).await;
    write_amqp(
        &mut peer,
        0,
        Performative::Flow(peer_flow(
            0,
            SESSION_WINDOW,
            0,
            Some(99),
            Some(0),
            Some(1),
            false,
        )),
        Vec::new(),
    )
    .await
    .expect("unknown handle flow");
    let Frame::Amqp {
        channel: 0,
        performative: Some(Performative::End(end)),
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("unknown Flow handle refuses its session");
    };
    assert_eq!(
        end.error.expect("session refusal").condition.as_symbol(),
        Symbol::from("amqp:session:unattached-handle")
    );
    write_amqp(
        &mut peer,
        0,
        Performative::Transfer(Transfer {
            handle: 99,
            delivery_id: Some(0),
            ..transfer()
        }),
        vec![0],
    )
    .await
    .expect("crossing transfer after End");
    write_amqp(&mut peer, 0, Performative::End(End::default()), Vec::new())
        .await
        .expect("End acknowledgment");
    let mut healthy = begin(&mut connection, &mut peer, 1, Begin::default()).await;
    let (endpoint, _) =
        accept_link(&mut healthy, &mut peer, attach(0, Role::Receiver, false, 0)).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("healthy sender");
    };
    write_amqp(
        &mut peer,
        1,
        Performative::Flow(peer_flow(
            0,
            SESSION_WINDOW,
            0,
            Some(0),
            Some(0),
            Some(1),
            false,
        )),
        Vec::new(),
    )
    .await
    .expect("healthy grant");
    let _outcome = enqueue(&sender, Message::data(vec![3])).await;
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            channel: 1,
            performative: Some(Performative::Transfer(_)),
            ..
        }
    ));
    connection.shutdown().await;
}

#[tokio::test]
async fn pending_attach_coalesces_omitted_credit_without_erasing_grants_echo_or_session_updates() {
    let (mut connection, mut peer) = server_pair(512).await;
    let mut session = begin(&mut connection, &mut peer, 0, Begin::default()).await;
    write_amqp(
        &mut peer,
        0,
        Performative::Attach(Box::new(attach(0, Role::Receiver, false, 0))),
        Vec::new(),
    )
    .await
    .expect("pending receiver attach");
    let incoming = session
        .next_incoming_attach()
        .await
        .expect("incoming attach");
    write_amqp(
        &mut peer,
        0,
        Performative::Flow(peer_flow(
            0,
            SESSION_WINDOW,
            0,
            Some(0),
            Some(0),
            Some(10),
            true,
        )),
        Vec::new(),
    )
    .await
    .expect("explicit pending grant and echo");
    write_amqp(
        &mut peer,
        0,
        Performative::Flow(peer_flow(0, 1, 0, Some(0), Some(0), None, false)),
        Vec::new(),
    )
    .await
    .expect("later omitted credit and changed session allowance");
    let barrier = echo(&mut peer, 0, peer_flow(0, 1, 0, None, None, None, true)).await;
    assert_eq!(barrier.next_outgoing_id, 0);
    let endpoint = session
        .accept_attach(incoming, 1024)
        .await
        .expect("accept after flows");
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("local sender");
    };
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::Attach(_)),
            ..
        }
    ));
    let Frame::Amqp {
        performative: Some(Performative::Flow(link_echo)),
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("the coalesced earlier echo is answered");
    };
    assert!(!link_echo.echo);
    assert_eq!(link_echo.delivery_count, Some(0));
    assert_eq!(link_echo.link_credit, Some(10));
    let _first = enqueue(&sender, Message::data(vec![1])).await;
    let _second = enqueue(&sender, Message::data(vec![2])).await;
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::Transfer(Transfer {
                delivery_id: Some(0),
                ..
            })),
            ..
        }
    ));
    let barrier = echo(&mut peer, 0, peer_flow(1, 0, 0, None, None, None, true)).await;
    assert_eq!(barrier.next_outgoing_id, 1);
    write_amqp(
        &mut peer,
        0,
        Performative::Flow(peer_flow(1, 1, 0, None, None, None, false)),
        Vec::new(),
    )
    .await
    .expect("handleless session grant wakes the queued send");
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::Transfer(Transfer {
                delivery_id: Some(1),
                ..
            })),
            ..
        }
    ));
    connection.shutdown().await;
}

#[tokio::test]
async fn pending_drain_preserves_an_earlier_grant_and_impossible_count_refuses_only_that_session() {
    let (mut connection, mut peer) = server_pair(512).await;
    let mut session = begin(&mut connection, &mut peer, 0, Begin::default()).await;
    write_amqp(
        &mut peer,
        0,
        Performative::Attach(Box::new(attach(0, Role::Receiver, false, 0))),
        Vec::new(),
    )
    .await
    .expect("pending receiver attach");
    let incoming = session
        .next_incoming_attach()
        .await
        .expect("incoming attach");
    write_amqp(
        &mut peer,
        0,
        Performative::Flow(peer_flow(
            0,
            SESSION_WINDOW,
            0,
            Some(0),
            Some(0),
            Some(10),
            false,
        )),
        Vec::new(),
    )
    .await
    .expect("pending grant");
    let mut drain = peer_flow(0, SESSION_WINDOW, 0, Some(0), Some(0), None, true);
    drain.drain = true;
    write_amqp(&mut peer, 0, Performative::Flow(drain), Vec::new())
        .await
        .expect("pending drain with omitted credit");
    echo(
        &mut peer,
        0,
        peer_flow(0, SESSION_WINDOW, 0, None, None, None, true),
    )
    .await;
    let _sender = session
        .accept_attach(incoming, 1024)
        .await
        .expect("accept dry draining link");
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::Attach(_)),
            ..
        }
    ));
    let Frame::Amqp {
        performative: Some(Performative::Flow(completed)),
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("one drain completion");
    };
    assert_eq!(completed.delivery_count, Some(10));
    assert_eq!(completed.link_credit, Some(0));
    assert!(completed.drain);
    assert!(!completed.echo);
    let echo = echo(
        &mut peer,
        0,
        peer_flow(0, SESSION_WINDOW, 0, Some(0), Some(10), None, true),
    )
    .await;
    assert_eq!(echo.delivery_count, Some(10));
    assert_eq!(echo.link_credit, Some(0));
    assert!(!echo.drain);

    write_amqp(
        &mut peer,
        0,
        Performative::Attach(Box::new(attach(1, Role::Receiver, false, 0))),
        Vec::new(),
    )
    .await
    .expect("another pending receiver");
    let _incoming = session
        .next_incoming_attach()
        .await
        .expect("pending receiver");
    write_amqp(
        &mut peer,
        0,
        Performative::Flow(peer_flow(
            0,
            SESSION_WINDOW,
            0,
            Some(1),
            Some(1),
            Some(1),
            false,
        )),
        Vec::new(),
    )
    .await
    .expect("impossible preaccept count");
    let Frame::Amqp {
        performative: Some(Performative::End(end)),
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("impossible pending count refuses the session");
    };
    assert_eq!(
        end.error.expect("invalid count").condition.as_symbol(),
        Symbol::from("amqp:invalid-field")
    );
    let _healthy = begin(&mut connection, &mut peer, 1, Begin::default()).await;
    connection.shutdown().await;
}

#[tokio::test]
async fn detached_pending_attach_rejects_late_approval_before_the_handle_can_be_reused() {
    let (mut connection, mut peer) = server_pair(512).await;
    let mut session = begin(&mut connection, &mut peer, 0, Begin::default()).await;
    write_amqp(
        &mut peer,
        0,
        Performative::Attach(Box::new(attach(0, Role::Receiver, false, 0))),
        Vec::new(),
    )
    .await
    .expect("pending attach");
    let incoming = session
        .next_incoming_attach()
        .await
        .expect("application approval pending");
    write_amqp(
        &mut peer,
        0,
        Performative::Detach(Detach {
            handle: 0,
            closed: true,
            error: None,
        }),
        Vec::new(),
    )
    .await
    .expect("peer cancels before approval");
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::Detach(Detach { handle: 0, .. })),
            ..
        }
    ));
    assert!(matches!(
        session.accept_attach(incoming, 1024).await,
        Err(EngineError::RemoteDetached)
    ));
    // The echoed Flow is a processing barrier and must be the next frame: a
    // stale application approval must never emit an Attach after cancellation.
    echo(
        &mut peer,
        0,
        peer_flow(0, SESSION_WINDOW, 0, None, None, None, true),
    )
    .await;
    let (endpoint, _) =
        accept_link(&mut session, &mut peer, attach(0, Role::Receiver, false, 0)).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("fresh approved sender");
    };
    write_amqp(
        &mut peer,
        0,
        Performative::Flow(peer_flow(
            0,
            SESSION_WINDOW,
            0,
            Some(0),
            Some(0),
            Some(1),
            false,
        )),
        Vec::new(),
    )
    .await
    .expect("fresh grant");
    let _outcome = enqueue(&sender, Message::data(vec![5])).await;
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::Transfer(Transfer {
                delivery_id: Some(0),
                ..
            })),
            ..
        }
    ));
    connection.shutdown().await;
}

#[tokio::test]
async fn pending_handle_reuse_before_stale_approval_is_refused_without_affecting_other_sessions() {
    let (mut connection, mut peer) = server_pair(512).await;
    let mut session = begin(&mut connection, &mut peer, 0, Begin::default()).await;
    write_amqp(
        &mut peer,
        0,
        Performative::Attach(Box::new(attach(0, Role::Receiver, false, 0))),
        Vec::new(),
    )
    .await
    .expect("pending attach");
    let incoming = session
        .next_incoming_attach()
        .await
        .expect("application approval pending");
    write_amqp(
        &mut peer,
        0,
        Performative::Detach(Detach {
            handle: 0,
            closed: true,
            error: None,
        }),
        Vec::new(),
    )
    .await
    .expect("peer cancellation");
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::Detach(_)),
            ..
        }
    ));
    write_amqp(
        &mut peer,
        0,
        Performative::Attach(Box::new(attach(0, Role::Receiver, false, 0))),
        Vec::new(),
    )
    .await
    .expect("premature same-name handle reuse");
    let Frame::Amqp {
        performative: Some(Performative::End(end)),
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("local canceled-approval policy refuses premature reuse");
    };
    assert_eq!(
        end.error.expect("handle reuse error").condition.as_symbol(),
        Symbol::from("amqp:session:handle-in-use")
    );
    assert!(session.accept_attach(incoming, 1024).await.is_err());
    let _healthy = begin(&mut connection, &mut peer, 1, Begin::default()).await;
    echo(
        &mut peer,
        1,
        peer_flow(0, SESSION_WINDOW, 0, None, None, None, true),
    )
    .await;
    connection.shutdown().await;
}

#[tokio::test]
async fn ended_session_rejects_stale_approval_send_and_settlement_without_closing_other_sessions() {
    let (mut connection, mut peer) = server_pair(512).await;
    let mut session = begin(&mut connection, &mut peer, 0, Begin::default()).await;
    let (endpoint, _) =
        accept_link(&mut session, &mut peer, attach(0, Role::Receiver, false, 0)).await;
    let LinkEndpoint::Sender(mut sender) = endpoint else {
        panic!("local sender");
    };
    write_amqp(
        &mut peer,
        0,
        Performative::Attach(Box::new(attach(1, Role::Receiver, false, 0))),
        Vec::new(),
    )
    .await
    .expect("delayed application approval");
    let incoming = session
        .next_incoming_attach()
        .await
        .expect("pending approval");
    let _healthy = begin(&mut connection, &mut peer, 1, Begin::default()).await;
    write_amqp(&mut peer, 0, Performative::End(End::default()), Vec::new())
        .await
        .expect("peer ends old session");
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::End(_)),
            ..
        }
    ));
    assert!(matches!(
        session.accept_attach(incoming, 1024).await,
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        sender.send(Message::data(vec![1]), vec![1].into()).await,
        Err(EngineError::RemoteDetached)
    ));
    let (reply, settled) = oneshot::channel();
    connection
        .commands
        .send(Command::Settle {
            channel: 0,
            handle: 0,
            identity: IncomingLedger::new()
                .reserve(&LinkIdentity::new(), 0, &[0])
                .expect("stale token"),
            state: DeliveryState::Accepted(Accepted),
            reply,
        })
        .await
        .expect("stale settlement command");
    assert!(matches!(
        settled.await.expect("stale settlement reply"),
        Err(EngineError::RemoteDetached)
    ));
    echo(
        &mut peer,
        1,
        peer_flow(0, SESSION_WINDOW, 0, None, None, None, true),
    )
    .await;
    connection.shutdown().await;
}

#[cfg(feature = "test-client")]
async fn client_pair() -> (ClientConnection, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let responding = async {
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("client header");
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open::new("peer")),
            Vec::new(),
        )
        .await
        .expect("peer Open");
        peer
    };
    let (connection, peer) = tokio::join!(ClientConnection::open(wire, "client", None), responding);
    (connection.expect("client connection"), peer)
}

#[cfg(feature = "test-client")]
async fn client_begin(connection: &mut ClientConnection, peer: &mut DuplexStream) -> ClientSession {
    let responding = async {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Begin(_)),
            ..
        } = next_frame(peer).await
        else {
            panic!("client Begin");
        };
        write_amqp(
            peer,
            channel,
            Performative::Begin(Begin {
                remote_channel: Some(channel),
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await
        .expect("peer Begin");
    };
    let (session, ()) = tokio::join!(connection.begin(), responding);
    session.expect("client session")
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_session_end_releases_pending_begin_and_attach_callers_without_closing_connection() {
    let (mut connection, mut peer) = client_pair().await;
    let refusing = async {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Begin(_)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("pending Begin");
        };
        write_amqp(
            &mut peer,
            channel,
            Performative::End(End::default()),
            Vec::new(),
        )
        .await
        .expect("peer rejects Begin");
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::End(_)),
                ..
            }
        ));
    };
    let (rejected, ()) = timeout(DEADLINE, async {
        tokio::join!(connection.begin(), refusing)
    })
    .await
    .expect("pending Begin fails promptly");
    assert!(matches!(rejected, Err(EngineError::RemoteDetached)));
    let mut session = client_begin(&mut connection, &mut peer).await;
    let refusing = async {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Attach(_)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("pending Attach");
        };
        write_amqp(
            &mut peer,
            channel,
            Performative::End(End::default()),
            Vec::new(),
        )
        .await
        .expect("peer rejects Attach session");
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::End(_)),
                ..
            }
        ));
    };
    let (rejected, ()) = timeout(DEADLINE, async {
        tokio::join!(session.attach_sender("pending", "queue"), refusing)
    })
    .await
    .expect("pending Attach fails promptly");
    assert!(matches!(rejected, Err(EngineError::RemoteDetached)));
    let _healthy = client_begin(&mut connection, &mut peer).await;
    connection.shutdown().await;
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn pending_sender_counts_must_agree_before_client_attach_and_peer_detach_cancels_pending_reply()
 {
    let (mut connection, mut peer) = client_pair().await;
    let mut session = client_begin(&mut connection, &mut peer).await;
    let refusing = async {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Attach(attach)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("pending receiver Attach");
        };
        for count in [7, 8] {
            write_amqp(
                &mut peer,
                channel,
                Performative::Flow(peer_flow(
                    0,
                    SESSION_WINDOW,
                    0,
                    Some(attach.handle),
                    Some(count),
                    None,
                    false,
                )),
                Vec::new(),
            )
            .await
            .expect("early sender Flow");
        }
        let Frame::Amqp {
            performative: Some(Performative::End(end)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("disagreeing pre-Attach sender counters refuse session");
        };
        assert_eq!(
            end.error.expect("count error").condition.as_symbol(),
            Symbol::from("amqp:invalid-field")
        );
        write_amqp(
            &mut peer,
            channel,
            Performative::End(End::default()),
            Vec::new(),
        )
        .await
        .expect("End acknowledgment");
    };
    let (rejected, ()) = timeout(DEADLINE, async {
        tokio::join!(session.attach_receiver("pending-count", "queue"), refusing)
    })
    .await
    .expect("pending count rejection unblocks caller");
    assert!(matches!(rejected, Err(EngineError::RemoteDetached)));
    let mut healthy = client_begin(&mut connection, &mut peer).await;
    let refusing = async {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Attach(attach)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("pending sender Attach");
        };
        write_amqp(
            &mut peer,
            channel,
            Performative::Detach(Detach {
                handle: attach.handle,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await
        .expect("peer cancels pending Attach");
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Detach(_)),
                ..
            }
        ));
    };
    let (rejected, ()) = timeout(DEADLINE, async {
        tokio::join!(healthy.attach_sender("pending-detach", "queue"), refusing)
    })
    .await
    .expect("peer Detach unblocks pending caller");
    assert!(matches!(rejected, Err(EngineError::RemoteDetached)));
    connection.shutdown().await;
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_sender_attach_must_match_cached_count_and_stale_send_does_not_close_other_sessions()
{
    let (mut connection, mut peer) = client_pair().await;
    let mut session = client_begin(&mut connection, &mut peer).await;
    let refusing = async {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Attach(attach)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("pending receiver Attach");
        };
        write_amqp(
            &mut peer,
            channel,
            Performative::Flow(peer_flow(
                0,
                SESSION_WINDOW,
                0,
                Some(attach.handle),
                Some(7),
                None,
                false,
            )),
            Vec::new(),
        )
        .await
        .expect("cached first sender count");
        let mut response = attach.response(attach.source.clone(), attach.target.clone());
        response.initial_delivery_count = Some(8);
        write_amqp(
            &mut peer,
            channel,
            Performative::Attach(Box::new(response)),
            Vec::new(),
        )
        .await
        .expect("mismatching actual sender count");
        let Frame::Amqp {
            performative: Some(Performative::End(end)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("mismatching Attach must refuse before granting receiver credit");
        };
        assert_eq!(
            end.error.expect("count mismatch").condition.as_symbol(),
            Symbol::from("amqp:invalid-field")
        );
        write_amqp(
            &mut peer,
            channel,
            Performative::End(End::default()),
            Vec::new(),
        )
        .await
        .expect("End acknowledgment");
    };
    let (rejected, ()) = timeout(DEADLINE, async {
        tokio::join!(session.attach_receiver("cached-count", "queue"), refusing)
    })
    .await
    .expect("mismatch unblocks attach caller");
    assert!(matches!(rejected, Err(EngineError::InvalidState(_))));
    let mut old_session = client_begin(&mut connection, &mut peer).await;
    let accepting = async {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Attach(attach)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("client sender Attach");
        };
        let response = attach.response(attach.source.clone(), attach.target.clone());
        write_amqp(
            &mut peer,
            channel,
            Performative::Attach(Box::new(response)),
            Vec::new(),
        )
        .await
        .expect("peer receiver response");
        channel
    };
    let (sender, old_channel) =
        tokio::join!(old_session.attach_sender("old-sender", "queue"), accepting);
    let mut sender = sender.expect("client sender");
    let _healthy = client_begin(&mut connection, &mut peer).await;
    write_amqp(
        &mut peer,
        old_channel,
        Performative::End(End::default()),
        Vec::new(),
    )
    .await
    .expect("peer ends sender session");
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::End(_)),
            ..
        }
    ));
    assert!(matches!(
        sender.send(Message::data(vec![1])).await,
        Err(EngineError::RemoteDetached)
    ));
    let _another = client_begin(&mut connection, &mut peer).await;
    connection.shutdown().await;
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn six_thousand_bidirectional_deliveries_replenish_session_windows_and_link_slots() {
    timeout(Duration::from_secs(20), async {
        let (server_wire, client_wire) = tokio::io::duplex(64 * 1024);
        let (server, client) = tokio::join!(
            ServerConnection::accept(server_wire, "server", None),
            ClientConnection::open(client_wire, "client", None)
        );
        let mut server = server.expect("server connection");
        let mut client = client.expect("client connection");
        let accepting = async {
            let incoming = server
                .next_incoming_session()
                .await
                .expect("incoming begin");
            server
                .accept_session(incoming)
                .await
                .expect("server session")
        };
        let (client_session, server_session) = tokio::join!(client.begin(), accepting);
        let mut client_session = client_session.expect("client session");
        let mut server_session = server_session;
        let accepting = async {
            let incoming = server_session
                .next_incoming_attach()
                .await
                .expect("incoming sender");
            server_session
                .accept_attach(incoming, 4 * 1024 * 1024)
                .await
                .expect("server receiver")
        };
        let (client_sender, server_receiver) =
            tokio::join!(client_session.attach_sender("request", "queue"), accepting);
        let mut client_sender = client_sender.expect("client sender");
        let LinkEndpoint::Receiver(mut server_receiver) = server_receiver else {
            panic!("local receiver");
        };
        let accepting = async {
            let incoming = server_session
                .next_incoming_attach()
                .await
                .expect("incoming receiver");
            server_session
                .accept_attach(incoming, 4 * 1024 * 1024)
                .await
                .expect("server sender")
        };
        let (client_receiver, server_sender) = tokio::join!(
            client_session.attach_receiver("response", "queue"),
            accepting
        );
        let mut client_receiver = client_receiver.expect("client receiver");
        let LinkEndpoint::Sender(mut server_sender) = server_sender else {
            panic!("local sender");
        };
        for id in 0..6_000_u32 {
            let incoming = async {
                let delivery = server_receiver.recv().await.expect("server delivery");
                assert_eq!(delivery.id, id);
                server_receiver
                    .accept(&delivery)
                    .await
                    .expect("server settlement");
            };
            let (outcome, ()) = tokio::join!(client_sender.send(Message::data(vec![1])), incoming);
            assert!(matches!(
                outcome.expect("client send"),
                Outcome::Accepted(_)
            ));
            let incoming = async {
                let delivery = client_receiver.recv().await.expect("client delivery");
                assert_eq!(delivery.id, id);
                client_receiver
                    .accept(&delivery)
                    .await
                    .expect("client settlement");
            };
            let (outcome, ()) = tokio::join!(
                server_sender.send(Message::data(vec![2]), id.to_be_bytes().to_vec().into()),
                incoming
            );
            assert!(matches!(
                outcome.expect("server send"),
                Outcome::Accepted(_)
            ));
        }
        let (server_close, client_close) = tokio::join!(server.close(), client.close());
        server_close.expect("server close");
        client_close.expect("client close");
    })
    .await
    .expect("no counter or dispatch stall after multiple window refills");
}
