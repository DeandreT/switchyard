use std::time::Duration;

use tokio::{io::DuplexStream, time::timeout};

use super::*;
use crate::{Source, Target};

const CHANNEL: u16 = 3;

fn transfer(handle: u32, id: Option<u32>, more: bool) -> Transfer {
    Transfer {
        handle,
        delivery_id: id,
        delivery_tag: id.map(|id| id.to_be_bytes().to_vec().into()),
        message_format: id.map(|_| 0),
        settled: Some(true),
        more,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn session_state() -> SessionState {
    let mut session = SessionState::new(&Begin::default());
    session.local_begin_sent = true;
    session
}

fn receiving_credit() -> ReceiveCredit {
    let mut credit = ReceiveCredit::new(0, 1, Arc::new(Consumption::new(Arc::new(Notify::new()))));
    credit.take_refill();
    credit
}

fn receiver_attach(handle: u32, maximum: Option<u64>, mode: SenderSettleMode) -> Attach {
    Attach {
        name: format!("receiver-{handle}"),
        handle,
        role: Role::Receiver,
        snd_settle_mode: mode,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("queue")),
        target: Some(Target::new("reply")),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: None,
        max_message_size: maximum,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

async fn next_frame(peer: &mut DuplexStream) -> Frame {
    timeout(Duration::from_secs(2), read_frame(peer))
        .await
        .expect("the engine responds promptly")
        .expect("valid raw AMQP frame")
}

async fn accept_sender(
    sessions: &mut HashMap<u16, SessionState>,
    wire: &mut FrameWriter<DuplexStream>,
    peer: &mut DuplexStream,
    handle: u32,
    maximum: Option<u64>,
    mode: SenderSettleMode,
) -> Sender {
    let (commands, mut command_rx) = mpsc::channel(1);
    let (_, incoming_attaches) = mpsc::channel(1);
    let session = ServerSession {
        channel: CHANNEL,
        commands,
        incoming_attaches,
        consumed: Arc::new(Notify::new()),
    };
    let requested = receiver_attach(handle, maximum, mode);
    sessions
        .get_mut(&CHANNEL)
        .expect("session fixture")
        .pending_attaches
        .insert(
            handle,
            PendingLinkFlow::new(requested.role.clone(), requested.initial_delivery_count),
        );
    let accepting = session.accept_attach(requested, 1_024);
    let applying = async {
        let command = command_rx.recv().await.expect("accept command");
        handle_command(command, wire, sessions, u32::MAX)
            .await
            .expect("accepting a link succeeds");
    };
    let (endpoint, ()) = tokio::join!(accepting, applying);
    let LinkEndpoint::Sender(sender) = endpoint.expect("local sending endpoint") else {
        panic!("the peer receiver creates a sender");
    };
    assert!(matches!(
        next_frame(peer).await,
        Frame::Amqp {
            performative: Some(Performative::Attach(_)),
            ..
        }
    ));
    sender
}

async fn send(
    sessions: &mut HashMap<u16, SessionState>,
    wire: &mut FrameWriter<DuplexStream>,
    handle: u32,
    message: Message,
) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
    send_with_frame_limit(sessions, wire, handle, message, u32::MAX).await
}

async fn send_with_frame_limit(
    sessions: &mut HashMap<u16, SessionState>,
    wire: &mut FrameWriter<DuplexStream>,
    handle: u32,
    message: Message,
    maximum_frame_size: u32,
) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
    let (reply, response) = oneshot::channel();
    handle_command(
        Command::Send {
            channel: CHANNEL,
            handle,
            message: Box::new(message),
            delivery_tag: vec![handle as u8].into(),
            reply,
        },
        wire,
        sessions,
        maximum_frame_size,
    )
    .await
    .expect("a rejected send does not stop the connection");
    let mut cursor = 0;
    while pump_connection(wire, sessions, &mut cursor)
        .await
        .expect("bounded send pump")
    {}
    response
}

#[tokio::test]
async fn peer_limit_applies_to_the_whole_encoded_message_across_smaller_transfer_frames() {
    let message = Message::data(vec![8; 1_024]);
    let encoded = encode_message(&message).expect("valid message");
    let length = encoded.len() as u64;
    let maximum_frame_size = 512;
    for maximum in [length, length - 1] {
        let (wire, mut peer) = tokio::io::duplex(64 * 1_024);
        let mut wire = FrameWriter::new(wire, maximum_frame_size).expect("frame writer");
        let mut sessions = HashMap::from([(CHANNEL, session_state())]);
        let _sender = accept_sender(
            &mut sessions,
            &mut wire,
            &mut peer,
            0,
            Some(maximum),
            SenderSettleMode::Settled,
        )
        .await;
        grant_credit(&mut sessions, &mut wire, 0).await;
        let response = send_with_frame_limit(
            &mut sessions,
            &mut wire,
            0,
            message.clone(),
            maximum_frame_size,
        )
        .await;
        if maximum < length {
            assert_size_detach(next_frame(&mut peer).await, 0);
            assert!(matches!(
                response.await.expect("oversize response"),
                Err(EngineError::MessageSizeExceeded { .. })
            ));
            assert_eq!(sessions[&CHANNEL].next_delivery_id, 0);
            continue;
        }
        let mut assembled = Vec::new();
        let mut frame_count = 0;
        loop {
            let Frame::Amqp {
                performative: Some(Performative::Transfer(transfer)),
                payload,
                ..
            } = next_frame(&mut peer).await
            else {
                panic!("a fitting message emits transfer fragments");
            };
            assert_eq!(transfer.delivery_id, (frame_count == 0).then_some(0));
            assert!(
                crate::encode_frame(&Frame::Amqp {
                    channel: CHANNEL,
                    performative: Some(Performative::Transfer(transfer.clone())),
                    payload: payload.clone()
                })
                .expect("frame encoding")
                .len()
                    <= maximum_frame_size as usize
            );
            assembled.extend_from_slice(&payload);
            frame_count += 1;
            if !transfer.more {
                break;
            }
        }
        assert!(frame_count > 1);
        assert_eq!(assembled, encoded);
        assert!(matches!(
            response
                .await
                .expect("send response")
                .expect("fitting send")
                .outcome,
            Outcome::Accepted(_)
        ));
    }
}

async fn grant_credit(
    sessions: &mut HashMap<u16, SessionState>,
    wire: &mut FrameWriter<DuplexStream>,
    handle: u32,
) {
    apply_flow(
        CHANNEL,
        Flow {
            next_incoming_id: Some(sessions[&CHANNEL].flow.snapshot().next_outgoing_id),
            incoming_window: SESSION_WINDOW,
            next_outgoing_id: sessions[&CHANNEL].flow.snapshot().next_incoming_id,
            outgoing_window: SESSION_WINDOW,
            handle: Some(handle),
            delivery_count: Some(0),
            link_credit: Some(1),
            ..Flow::default()
        },
        wire,
        sessions,
        u32::MAX,
    )
    .await
    .expect("the peer grants one delivery");
}

fn assert_size_detach(frame: Frame, expected_handle: u32) {
    let Frame::Amqp {
        channel: CHANNEL,
        performative: Some(Performative::Detach(detach)),
        payload,
    } = frame
    else {
        panic!("the oversized link emits a detach, not a transfer");
    };
    assert_eq!(detach.handle, expected_handle);
    assert!(detach.closed);
    assert!(payload.is_empty());
    assert_eq!(
        detach
            .error
            .expect("a size condition is supplied")
            .condition
            .as_symbol(),
        Symbol::from("amqp:link:message-size-exceeded")
    );
}

#[tokio::test]
async fn advertised_none_and_zero_are_unlimited_and_exact_encoded_size_is_accepted() {
    let message = Message::data(vec![3; 80]);
    let encoded = encode_message(&message).expect("valid message");
    let length = encoded.len() as u64;
    for maximum in [None, Some(0), Some(length)] {
        let (wire, mut peer) = tokio::io::duplex(64 * 1_024);
        let mut wire = FrameWriter::new(wire, u32::MAX).expect("frame writer");
        let mut sessions = HashMap::from([(CHANNEL, session_state())]);
        let sender = accept_sender(
            &mut sessions,
            &mut wire,
            &mut peer,
            0,
            maximum,
            SenderSettleMode::Settled,
        )
        .await;
        assert_eq!(
            sender.max_message_size(),
            maximum.filter(|value| *value != 0)
        );
        grant_credit(&mut sessions, &mut wire, 0).await;
        let response = send(&mut sessions, &mut wire, 0, message.clone()).await;
        let Frame::Amqp {
            performative: Some(Performative::Transfer(transfer)),
            payload,
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("a fitting message is transferred");
        };
        assert_eq!(transfer.delivery_id, Some(0));
        assert_eq!(transfer.settled, Some(true));
        assert_eq!(payload, encoded);
        assert!(matches!(
            response
                .await
                .expect("send response")
                .expect("fitting send")
                .outcome,
            Outcome::Accepted(_)
        ));
    }
}

#[tokio::test]
async fn oversized_sends_detach_only_the_link_before_credit_or_delivery_ids_are_consumed() {
    let message = Message::data(vec![4; 80]);
    let length = encode_message(&message).expect("valid message").len() as u64;
    for mode in [SenderSettleMode::Unsettled, SenderSettleMode::Settled] {
        for credit_available in [false, true] {
            let (wire, mut peer) = tokio::io::duplex(64 * 1_024);
            let mut wire = FrameWriter::new(wire, u32::MAX).expect("frame writer");
            let mut sessions = HashMap::from([(CHANNEL, session_state())]);
            let mut oversized = accept_sender(
                &mut sessions,
                &mut wire,
                &mut peer,
                0,
                Some(length - 1),
                mode.clone(),
            )
            .await;
            let _healthy = accept_sender(
                &mut sessions,
                &mut wire,
                &mut peer,
                1,
                None,
                SenderSettleMode::Settled,
            )
            .await;
            if credit_available {
                grant_credit(&mut sessions, &mut wire, 0).await;
            }
            let response = send(&mut sessions, &mut wire, 0, message.clone()).await;
            assert_size_detach(next_frame(&mut peer).await, 0);
            assert!(matches!(
                response.await.expect("send response"),
                Err(EngineError::MessageSizeExceeded {
                    message_bytes,
                    maximum_bytes,
                }) if message_bytes == length && maximum_bytes == length - 1
            ));
            timeout(Duration::from_secs(2), oversized.on_detach())
                .await
                .expect("the sender observes its link detach");
            assert_eq!(sessions[&CHANNEL].next_delivery_id, 0);
            assert!(!sessions[&CHANNEL].links.contains_key(&0));
            assert!(sessions[&CHANNEL].links.contains_key(&1));

            grant_credit(&mut sessions, &mut wire, 1).await;
            let response = send(
                &mut sessions,
                &mut wire,
                1,
                Message::data(b"healthy".to_vec()),
            )
            .await;
            let Frame::Amqp {
                performative: Some(Performative::Transfer(transfer)),
                ..
            } = next_frame(&mut peer).await
            else {
                panic!("the unrelated link remains usable");
            };
            assert_eq!(transfer.handle, 1);
            assert_eq!(transfer.delivery_id, Some(0));
            response
                .await
                .expect("healthy response")
                .expect("healthy send");
        }
    }
}

#[tokio::test]
async fn locally_enforced_receiving_limits_reject_fragment_growth_without_stopping_other_links() {
    let (wire, mut peer) = tokio::io::duplex(64 * 1_024);
    let mut wire = FrameWriter::new(wire, u32::MAX).expect("frame writer");
    let (deliveries, mut received) = mpsc::channel(1);
    let (detached, mut detached_rx) = watch::channel(false);
    let mut session = session_state();
    session.links.insert(
        0,
        LinkState::Receiving(ReceivingLink {
            max_message_size: 8,
            deliveries,
            partial: None,
            detached,
            credit: receiving_credit(),
            decoders: MessageFormatDecoders::default(),
            identity: LinkIdentity::new(),
            sender_settle_mode: SenderSettleMode::Mixed,
            receiver_settle_mode: ReceiverSettleMode::First,
        }),
    );
    let (healthy_tx, mut healthy_rx) = mpsc::channel(1);
    let (healthy_detached, _) = watch::channel(false);
    session.links.insert(
        1,
        LinkState::Receiving(ReceivingLink {
            max_message_size: u64::MAX,
            deliveries: healthy_tx,
            partial: None,
            detached: healthy_detached,
            credit: receiving_credit(),
            decoders: MessageFormatDecoders::default(),
            identity: LinkIdentity::new(),
            sender_settle_mode: SenderSettleMode::Mixed,
            receiver_settle_mode: ReceiverSettleMode::First,
        }),
    );
    let mut sessions = HashMap::from([(CHANNEL, session)]);
    receive_transfer(
        CHANNEL,
        transfer(0, Some(0), true),
        vec![0; 4],
        &mut sessions,
        &mut wire,
    )
    .await
    .expect("a bounded initial fragment is buffered");
    receive_transfer(
        CHANNEL,
        transfer(0, None, false),
        vec![0; 5],
        &mut sessions,
        &mut wire,
    )
    .await
    .expect("oversize is a link refusal");
    assert_size_detach(next_frame(&mut peer).await, 0);
    wait_for_detach(&mut detached_rx).await;
    assert!(received.recv().await.is_none());
    receive_transfer(
        CHANNEL,
        transfer(0, None, false),
        vec![0; 2],
        &mut sessions,
        &mut wire,
    )
    .await
    .expect("a continuation can cross the link detach");
    let (deliveries_tx, _) = mpsc::channel(1);
    let (detached_tx, _) = watch::channel(false);
    let (reply, response) = oneshot::channel();
    handle_command(
        Command::AcceptLink {
            channel: CHANNEL,
            attach: Box::new(receiver_attach(0, None, SenderSettleMode::Settled)),
            max_message_size: 1_024,
            properties: None,
            decoders: MessageFormatDecoders::default(),
            deliveries_tx,
            detached_tx,
            consumption: Arc::new(Consumption::new(Arc::new(Notify::new()))),
            identity: LinkIdentity::new(),
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("refusing premature handle reuse does not stop the connection");
    assert!(response.await.expect("reuse response").is_err());
    let (incoming, _) = mpsc::channel(1);
    handle_frame(
        Frame::Amqp {
            channel: CHANNEL,
            performative: Some(Performative::Detach(Detach {
                handle: 0,
                closed: true,
                error: None,
            })),
            payload: Vec::new(),
        },
        &mut wire,
        &incoming,
        &mut sessions,
        u32::MAX,
        false,
    )
    .await
    .expect("the peer acknowledges the link detach");
    assert!(!sessions[&CHANNEL].closing_handles.contains(&0));
    let _reused = accept_sender(
        &mut sessions,
        &mut wire,
        &mut peer,
        0,
        None,
        SenderSettleMode::Settled,
    )
    .await;
    let message = Message::data(b"healthy".to_vec());
    receive_transfer(
        CHANNEL,
        transfer(1, Some(1), false),
        encode_message(&message).expect("valid message"),
        &mut sessions,
        &mut wire,
    )
    .await
    .expect("the unrelated receiver remains usable");
    assert_eq!(
        healthy_rx.recv().await.expect("healthy delivery").message,
        message
    );
    receive_transfer(
        CHANNEL,
        transfer(77, Some(2), false),
        Vec::new(),
        &mut sessions,
        &mut wire,
    )
    .await
    .expect("unknown handle refuses its session");
    assert!(matches!(
        next_frame(&mut peer).await,
        Frame::Amqp {
            performative: Some(Performative::End(_)),
            ..
        }
    ));
    assert!(
        sessions[&CHANNEL].ending,
        "unknown handles are not treated as crossing transfers"
    );
}

#[test]
fn closing_handle_retention_is_bounded() {
    let mut session = session_state();
    session.closing_handles = (0..MAX_CLOSING_HANDLES as u32).collect();
    remember_closing_handle(&mut session, 0).expect("an existing tombstone spends no extra slot");
    assert!(remember_closing_handle(&mut session, MAX_CLOSING_HANDLES as u32).is_err());
    assert_eq!(session.closing_handles.len(), MAX_CLOSING_HANDLES);
}

#[cfg(feature = "test-client")]
mod client_tests {
    use super::*;

    async fn open_client() -> (ClientConnection, DuplexStream) {
        let (stream, mut peer) = tokio::io::duplex(64 * 1_024);
        let opening = ClientConnection::open(stream, "bounded-client", None);
        let accepting = async {
            assert_eq!(
                read_protocol_header(&mut peer)
                    .await
                    .expect("client header"),
                ProtocolHeader::AMQP
            );
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
                Performative::Open(Open::new("raw-peer")),
                Vec::new(),
            )
            .await
            .expect("peer open");
        };
        let (connection, ()) = tokio::join!(opening, accepting);
        (connection.expect("client connection"), peer)
    }

    async fn begin_client(
        connection: &mut ClientConnection,
        peer: &mut DuplexStream,
    ) -> ClientSession {
        let beginning = connection.begin();
        let accepting = async {
            let Frame::Amqp {
                channel,
                performative: Some(Performative::Begin(_)),
                ..
            } = next_frame(peer).await
            else {
                panic!("client begin");
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
            .expect("peer begin");
        };
        let (session, ()) = tokio::join!(beginning, accepting);
        session.expect("client session")
    }

    async fn attach_receiver(
        session: &mut ClientSession,
        peer: &mut DuplexStream,
        name: &str,
        maximum: Option<u64>,
    ) -> (ClientReceiver, u32) {
        let mut builder = ClientReceiver::builder().name(name).source("queue");
        if let Some(maximum) = maximum {
            builder = builder.max_message_size(maximum);
        }
        let attaching = builder.attach(session);
        let accepting = async {
            let Frame::Amqp {
                channel,
                performative: Some(Performative::Attach(attach)),
                ..
            } = next_frame(peer).await
            else {
                panic!("client attach");
            };
            assert_eq!(attach.max_message_size, maximum);
            let handle = attach.handle;
            let mut response = attach.response(attach.source.clone(), attach.target.clone());
            response.snd_settle_mode = SenderSettleMode::Mixed;
            write_amqp(
                peer,
                channel,
                Performative::Attach(Box::new(response)),
                Vec::new(),
            )
            .await
            .expect("peer attach");
            assert!(matches!(
                next_frame(peer).await,
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                }
            ));
            handle
        };
        let (receiver, handle) = tokio::join!(attaching, accepting);
        (receiver.expect("client receiver"), handle)
    }

    async fn raw_transfer(
        peer: &mut DuplexStream,
        handle: u32,
        id: u32,
        payload: Vec<u8>,
        more: bool,
    ) {
        write_amqp(
            peer,
            0,
            Performative::Transfer(transfer(handle, Some(id), more)),
            payload,
        )
        .await
        .expect("raw transfer");
    }

    async fn attach_sender(
        session: &mut ClientSession,
        peer: &mut DuplexStream,
        name: &str,
        maximum: Option<u64>,
    ) -> (ClientSender, u32) {
        let attaching = session.attach_sender(name, "queue");
        let accepting = async {
            let Frame::Amqp {
                channel,
                performative: Some(Performative::Attach(attach)),
                ..
            } = next_frame(peer).await
            else {
                panic!("client sending attach");
            };
            let handle = attach.handle;
            assert_eq!(attach.role, Role::Sender);
            let mut response = attach.response(attach.source.clone(), attach.target.clone());
            response.max_message_size = maximum;
            write_amqp(
                peer,
                channel,
                Performative::Attach(Box::new(response)),
                Vec::new(),
            )
            .await
            .expect("peer receiving attach");
            handle
        };
        let (sender, handle) = tokio::join!(attaching, accepting);
        (sender.expect("client sender"), handle)
    }

    async fn close_client(connection: &ClientConnection, peer: &mut DuplexStream) {
        let closing = connection.close();
        let responding = async {
            loop {
                match next_frame(peer).await {
                    Frame::Amqp {
                        performative: Some(Performative::Close(_)),
                        ..
                    } => break,
                    Frame::Amqp {
                        performative: Some(Performative::Flow(_)),
                        ..
                    } => {}
                    frame => panic!("expected close or consumption refill, got {frame:?}"),
                }
            }
            write_amqp(peer, 0, Performative::Close(Close::default()), Vec::new())
                .await
                .expect("peer close");
        };
        let (closed, ()) = tokio::join!(closing, responding);
        closed.expect("client close");
    }

    #[tokio::test]
    async fn receiver_builder_advertises_and_enforces_its_fragmented_message_limit() {
        let (mut connection, mut peer) = open_client().await;
        let mut session = begin_client(&mut connection, &mut peer).await;
        let fitting = Message::data(b"fits".to_vec());
        let encoded = encode_message(&fitting).expect("valid message");
        let maximum = encoded.len() as u64;
        let (mut bounded, handle) =
            attach_receiver(&mut session, &mut peer, "bounded", Some(maximum)).await;
        let (mut healthy, healthy_handle) =
            attach_receiver(&mut session, &mut peer, "healthy", None).await;
        raw_transfer(&mut peer, handle, 0, encoded.clone(), false).await;
        assert_eq!(
            bounded
                .recv()
                .await
                .expect("exact-boundary delivery")
                .message(),
            &fitting
        );
        raw_transfer(&mut peer, handle, 1, vec![0; 3], true).await;
        raw_transfer(&mut peer, handle, 1, vec![0; encoded.len() - 2], false).await;
        let detach = loop {
            match next_frame(&mut peer).await {
                Frame::Amqp {
                    performative: Some(Performative::Detach(detach)),
                    ..
                } => break detach,
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("expected bounded detach or consumption refill, got {frame:?}"),
            }
        };
        assert_eq!(detach.handle, handle);
        assert_eq!(
            detach
                .error
                .expect("link size condition")
                .condition
                .as_symbol(),
            Symbol::from("amqp:link:message-size-exceeded")
        );
        assert!(matches!(
            bounded.recv().await,
            Err(EngineError::RemoteDetached)
        ));
        raw_transfer(&mut peer, handle, 1, vec![0; 2], false).await;
        write_amqp(
            &mut peer,
            0,
            Performative::Detach(Detach {
                handle,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await
        .expect("peer acknowledges the detach after a crossing continuation");
        raw_transfer(&mut peer, healthy_handle, 2, encoded, false).await;
        assert_eq!(
            healthy.recv().await.expect("unrelated delivery").message(),
            &fitting
        );
        close_client(&connection, &mut peer).await;
    }

    #[tokio::test]
    async fn receiver_builder_zero_means_unlimited() {
        let (mut connection, mut peer) = open_client().await;
        let mut session = begin_client(&mut connection, &mut peer).await;
        let (mut receiver, handle) =
            attach_receiver(&mut session, &mut peer, "unlimited", Some(0)).await;
        let message = Message::data(vec![5; 256]);
        raw_transfer(
            &mut peer,
            handle,
            0,
            encode_message(&message).expect("valid message"),
            false,
        )
        .await;
        assert_eq!(
            receiver.recv().await.expect("unlimited delivery").message(),
            &message
        );
        close_client(&connection, &mut peer).await;
    }

    #[tokio::test]
    async fn client_sender_retains_peer_size_and_rejects_before_credit_or_delivery_id_allocation() {
        let (mut connection, mut peer) = open_client().await;
        let mut session = begin_client(&mut connection, &mut peer).await;
        let oversized_message = Message::data(vec![7; 80]);
        let encoded_size = encode_message(&oversized_message)
            .expect("valid message")
            .len() as u64;
        let (mut bounded, handle) =
            attach_sender(&mut session, &mut peer, "bounded", Some(encoded_size - 1)).await;
        let sending = bounded.send(oversized_message);
        let refusing = async {
            let Frame::Amqp {
                performative: Some(Performative::Detach(detach)),
                ..
            } = next_frame(&mut peer).await
            else {
                panic!("an oversized client send detaches without a transfer or credit");
            };
            assert_eq!(detach.handle, handle);
            assert_eq!(
                detach.error.expect("size condition").condition.as_symbol(),
                Symbol::from("amqp:link:message-size-exceeded")
            );
            write_amqp(
                &mut peer,
                0,
                Performative::Detach(Detach {
                    handle,
                    closed: true,
                    error: None,
                }),
                Vec::new(),
            )
            .await
            .expect("detach acknowledgement");
        };
        let (rejected, ()) = tokio::join!(sending, refusing);
        assert!(
            matches!(rejected, Err(EngineError::MessageSizeExceeded { message_bytes, maximum_bytes }) if message_bytes == encoded_size && maximum_bytes == encoded_size - 1)
        );
        let (mut healthy, handle) = attach_sender(&mut session, &mut peer, "healthy", None).await;
        write_amqp(
            &mut peer,
            0,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: SESSION_WINDOW,
                outgoing_window: SESSION_WINDOW,
                handle: Some(handle),
                delivery_count: Some(0),
                link_credit: Some(1),
                ..Flow::default()
            }),
            Vec::new(),
        )
        .await
        .expect("healthy credit");
        let sending = healthy.send(Message::data(b"healthy".to_vec()));
        let accepting = async {
            let Frame::Amqp {
                performative: Some(Performative::Transfer(transfer)),
                ..
            } = next_frame(&mut peer).await
            else {
                panic!("healthy transfer");
            };
            assert_eq!(transfer.handle, handle);
            assert_eq!(transfer.delivery_id, Some(0));
            write_amqp(
                &mut peer,
                0,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: 0,
                    last: None,
                    settled: true,
                    state: Some(DeliveryState::Accepted(Accepted)),
                    batchable: false,
                }),
                Vec::new(),
            )
            .await
            .expect("healthy disposition");
        };
        let (accepted, ()) = tokio::join!(sending, accepting);
        assert!(matches!(
            accepted.expect("healthy send"),
            Outcome::Accepted(_)
        ));
        close_client(&connection, &mut peer).await;
    }
}
