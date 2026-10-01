use tokio::{io::DuplexStream, time::timeout};

use super::*;

const DEADLINE: Duration = Duration::from_secs(2);

async fn frame(peer: &mut DuplexStream) -> (u16, Performative, Vec<u8>) {
    let Frame::Amqp {
        channel,
        performative: Some(performative),
        payload,
    } = timeout(DEADLINE, read_frame(peer))
        .await
        .expect("prompt peer frame")
        .expect("complete frame")
    else {
        panic!("AMQP performative");
    };
    (channel, performative, payload)
}

async fn silent(peer: &mut DuplexStream) {
    assert!(
        timeout(Duration::from_millis(20), read_frame(peer))
            .await
            .is_err()
    );
}

async fn begin(
    connection: &mut ClientConnection,
    peer: &mut DuplexStream,
    maximum: u32,
) -> ClientSession {
    let responding = async {
        let (local, performative, _) = frame(peer).await;
        assert!(matches!(performative, Performative::Begin(_)));
        write_amqp(
            peer,
            17 + local,
            Performative::Begin(Begin {
                remote_channel: Some(local),
                handle_max: maximum,
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await
        .expect("peer Begin");
    };
    let (session, ()) = timeout(DEADLINE, async {
        tokio::join!(connection.begin(), responding)
    })
    .await
    .expect("prompt Begin association");
    session.expect("client session")
}

async fn pair(maximum: u32) -> (ClientConnection, ClientSession, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let responding = async {
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("client header");
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        assert!(matches!(
            frame(&mut peer).await,
            (0, Performative::Open(_), _)
        ));
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open::new("handle-peer")),
            Vec::new(),
        )
        .await
        .expect("peer Open");
        peer
    };
    let (connection, mut peer) = timeout(DEADLINE, async {
        tokio::join!(
            ClientConnection::open(wire, "handle-client", None),
            responding
        )
    })
    .await
    .expect("prompt client handshake");
    let mut connection = connection.expect("client connection");
    let session = begin(&mut connection, &mut peer, maximum).await;
    (connection, session, peer)
}

struct Pending {
    request: Attach,
    identity: LinkIdentity,
    inbox: mpsc::Receiver<Delivery>,
    detached: watch::Receiver<bool>,
    reply: oneshot::Receiver<Result<(u32, Attach), EngineError>>,
}

async fn attach_command(session: &ClientSession, name: &str, role: Role) -> Pending {
    let (deliveries_tx, inbox) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
    let (detached_tx, detached) = watch::channel(false);
    let identity = LinkIdentity::new();
    let (reply, result) = oneshot::channel();
    session
        .commands
        .send(ClientCommand::Attach {
            channel: session.channel,
            session: session.identity.clone(),
            request: Box::new(AttachRequest {
                name: name.to_owned(),
                role: role.clone(),
                sender_settle_mode: SenderSettleMode::Mixed,
                receiver_settle_mode: ReceiverSettleMode::First,
                source: Some(Source::new("queue")),
                target: Some(Target::new("queue")),
                max_message_size: None,
            }),
            deliveries_tx,
            detached_tx,
            consumption: Arc::new(Consumption::new(session.consumed.clone())),
            identity: identity.clone(),
            reply,
        })
        .await
        .expect("Attach command");
    Pending {
        request: Attach {
            name: name.to_owned(),
            handle: 0,
            role,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: None,
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        },
        identity,
        inbox,
        detached,
        reply: result,
    }
}

async fn pending(
    session: &ClientSession,
    peer: &mut DuplexStream,
    name: &str,
    role: Role,
) -> Pending {
    let mut pending = attach_command(session, name, role).await;
    let (channel, performative, payload) = frame(peer).await;
    assert_eq!(channel, session.channel);
    assert!(payload.is_empty());
    let Performative::Attach(attach) = performative else {
        panic!("client Attach");
    };
    assert_eq!(attach.name, name);
    pending.request = *attach;
    pending
}

async fn answer(
    peer: &mut DuplexStream,
    session: &ClientSession,
    pending: &mut Pending,
    peer_handle: u32,
) {
    let mut response = pending.request.response(
        pending.request.source.clone(),
        pending.request.target.clone(),
    );
    response.handle = peer_handle;
    write_amqp(
        peer,
        17 + session.channel,
        Performative::Attach(Box::new(response)),
        Vec::new(),
    )
    .await
    .expect("peer Attach response");
    let (reply, placeholder) = oneshot::channel();
    drop(reply);
    let result = std::mem::replace(&mut pending.reply, placeholder);
    let (local, raw_response) = timeout(DEADLINE, result)
        .await
        .expect("prompt Attach result")
        .expect("Attach reply")
        .expect("accepted Attach");
    assert_eq!(local, pending.request.handle);
    assert_eq!(raw_response.handle, peer_handle);
    if pending.request.role == Role::Receiver {
        let (channel, performative, _) = frame(peer).await;
        assert_eq!(channel, session.channel);
        assert!(matches!(performative, Performative::Flow(flow) if flow.handle == Some(local)));
    }
}

async fn flow(peer: &mut DuplexStream, session: &ClientSession, peer_handle: u32, echo: bool) {
    write_amqp(
        peer,
        17 + session.channel,
        Performative::Flow(Flow {
            incoming_window: SESSION_WINDOW,
            outgoing_window: SESSION_WINDOW,
            handle: Some(peer_handle),
            delivery_count: Some(0),
            link_credit: Some(1),
            echo,
            ..Flow::default()
        }),
        Vec::new(),
    )
    .await
    .expect("peer receiver Flow");
}

async fn assert_end(peer: &mut DuplexStream, local: u16, condition: &str) {
    let (channel, performative, payload) = frame(peer).await;
    assert_eq!(channel, local);
    assert!(payload.is_empty());
    let Performative::End(end) = performative else {
        panic!("session End");
    };
    assert_eq!(
        end.error.expect("End error").condition.as_symbol(),
        Symbol::from(condition)
    );
}

async fn send(
    session: &ClientSession,
    local: u32,
    identity: &LinkIdentity,
) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
    let (reply, result) = oneshot::channel();
    session
        .commands
        .send(ClientCommand::Send {
            channel: session.channel,
            handle: local,
            identity: identity.clone(),
            message: Box::new(Message::data(vec![local as u8])),
            delivery_tag: vec![local as u8].into(),
            message_format: 0,
            reply,
        })
        .await
        .expect("Send command");
    result
}

async fn cleanup(connection: ClientConnection) {
    timeout(DEADLINE, connection.shutdown())
        .await
        .expect("owned client cleanup");
}

#[tokio::test]
async fn crossed_receiver_handles_route_incoming_messages_and_detach_to_local_authority() {
    let (connection, session, mut peer) = pair(1).await;
    let mut first = pending(&session, &mut peer, "first-receiver", Role::Receiver).await;
    let mut second = pending(&session, &mut peer, "second-receiver", Role::Receiver).await;
    assert_eq!((first.request.handle, second.request.handle), (0, 1));
    answer(&mut peer, &session, &mut second, 0).await;
    answer(&mut peer, &session, &mut first, 1).await;
    for (peer_handle, id, byte) in [(1, 0, 10), (0, 1, 20)] {
        write_amqp(
            &mut peer,
            17,
            Performative::Transfer(Transfer {
                handle: peer_handle,
                delivery_id: Some(id),
                delivery_tag: Some(vec![byte].into()),
                message_format: Some(0),
                settled: Some(true),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            encode_message(&Message::data(vec![byte])).expect("message"),
        )
        .await
        .expect("peer Transfer");
    }
    let first_delivery = timeout(DEADLINE, first.inbox.recv())
        .await
        .expect("prompt first delivery")
        .expect("first inbox");
    let second_delivery = timeout(DEADLINE, second.inbox.recv())
        .await
        .expect("prompt second delivery")
        .expect("second inbox");
    assert_eq!(first_delivery.identity.id(), 0);
    assert_eq!(second_delivery.identity.id(), 1);
    assert!(first_delivery.identity.belongs_to(&first.identity));
    assert!(!first_delivery.identity.belongs_to(&second.identity));
    assert!(second_delivery.identity.belongs_to(&second.identity));
    assert!(!second_delivery.identity.belongs_to(&first.identity));
    assert_eq!(first_delivery.message, Message::data(vec![10]));
    assert_eq!(second_delivery.message, Message::data(vec![20]));
    write_amqp(
        &mut peer,
        17,
        Performative::Detach(Detach {
            handle: 1,
            closed: true,
            error: None,
        }),
        Vec::new(),
    )
    .await
    .expect("first peer Detach");
    assert!(matches!(
        frame(&mut peer).await,
        (0, Performative::Detach(Detach { handle: 0, .. }), _)
    ));
    assert!(*first.detached.borrow());
    assert!(!*second.detached.borrow());
    cleanup(connection).await;
}

#[tokio::test]
async fn crossed_sender_handles_route_peer_credit_to_outgoing_local_transfers() {
    let (connection, session, mut peer) = pair(1).await;
    let mut first = pending(&session, &mut peer, "first-sender", Role::Sender).await;
    let mut second = pending(&session, &mut peer, "second-sender", Role::Sender).await;
    answer(&mut peer, &session, &mut second, 0).await;
    answer(&mut peer, &session, &mut first, 1).await;
    for (peer_handle, endpoint) in [(1, &first), (0, &second)] {
        flow(&mut peer, &session, peer_handle, true).await;
        let (channel, performative, _) = frame(&mut peer).await;
        assert_eq!(channel, session.channel);
        assert!(
            matches!(performative, Performative::Flow(flow) if flow.handle == Some(endpoint.request.handle))
        );
        let _outcome = send(&session, endpoint.request.handle, &endpoint.identity).await;
        let (channel, performative, payload) = frame(&mut peer).await;
        assert_eq!(channel, session.channel);
        assert!(
            matches!(performative, Performative::Transfer(transfer) if transfer.handle == endpoint.request.handle)
        );
        assert_eq!(
            decode_message(&payload).expect("outgoing message"),
            Message::data(vec![endpoint.request.handle as u8])
        );
    }
    cleanup(connection).await;
}

#[tokio::test]
async fn unpublished_numeric_peer_flow_or_detach_refuses_only_its_session() {
    for use_detach in [false, true] {
        let (mut connection, session, mut peer) = pair(0).await;
        let healthy = begin(&mut connection, &mut peer, 0).await;
        let pending = pending(&session, &mut peer, "unpublished", Role::Sender).await;
        if use_detach {
            write_amqp(
                &mut peer,
                17,
                Performative::Detach(Detach {
                    handle: pending.request.handle,
                    closed: true,
                    error: None,
                }),
                Vec::new(),
            )
            .await
            .expect("unpublished Detach");
        } else {
            flow(&mut peer, &session, pending.request.handle, false).await;
        }
        assert_end(&mut peer, 0, "amqp:session:unattached-handle").await;
        assert!(matches!(
            timeout(DEADLINE, pending.reply)
                .await
                .expect("prompt pending failure")
                .expect("Attach reply"),
            Err(EngineError::RemoteDetached)
        ));
        assert!(pending.identity.is_retired());
        assert!(!healthy.identity.is_retired());
        write_amqp(&mut peer, 17, Performative::End(End::default()), Vec::new())
            .await
            .expect("refused session End acknowledgement");
        write_amqp(
            &mut peer,
            17 + healthy.channel,
            Performative::Flow(Flow {
                incoming_window: SESSION_WINDOW,
                outgoing_window: SESSION_WINDOW,
                echo: true,
                ..Flow::default()
            }),
            Vec::new(),
        )
        .await
        .expect("healthy session barrier");
        assert!(
            matches!(frame(&mut peer).await, (channel, Performative::Flow(_), _) if channel == healthy.channel)
        );
        cleanup(connection).await;
    }
}

#[tokio::test]
async fn pending_and_closing_local_handles_respect_zero_peer_maximum_until_exact_detach_ack() {
    let (connection, session, mut peer) = pair(0).await;
    let mut first = pending(&session, &mut peer, "first", Role::Sender).await;
    let excess = attach_command(&session, "pending-excess", Role::Sender).await;
    assert!(matches!(
        timeout(DEADLINE, excess.reply)
            .await
            .expect("prompt pending-range refusal")
            .expect("Attach reply"),
        Err(EngineError::InvalidState(_))
    ));
    silent(&mut peer).await;
    answer(&mut peer, &session, &mut first, 7).await;
    let (reply, ended) = oneshot::channel();
    session
        .commands
        .send(ClientCommand::Detach {
            channel: session.channel,
            handle: first.request.handle,
            identity: first.identity.clone(),
            reply,
        })
        .await
        .expect("local Detach command");
    assert!(matches!(
        frame(&mut peer).await,
        (0, Performative::Detach(Detach { handle: 0, .. }), _)
    ));
    let excess = attach_command(&session, "closing-excess", Role::Sender).await;
    assert!(matches!(
        timeout(DEADLINE, excess.reply)
            .await
            .expect("prompt closing-range refusal")
            .expect("Attach reply"),
        Err(EngineError::InvalidState(_))
    ));
    flow(&mut peer, &session, 7, true).await;
    silent(&mut peer).await;
    write_amqp(
        &mut peer,
        17,
        Performative::Detach(Detach {
            handle: 7,
            closed: true,
            error: None,
        }),
        Vec::new(),
    )
    .await
    .expect("exact peer Detach acknowledgement");
    timeout(DEADLINE, ended)
        .await
        .expect("prompt Detach result")
        .expect("Detach reply")
        .expect("acknowledged Detach");
    let mut fresh = pending(&session, &mut peer, "fresh", Role::Sender).await;
    assert_eq!(fresh.request.handle, 0);
    answer(&mut peer, &session, &mut fresh, 7).await;
    let stale = send(&session, first.request.handle, &first.identity).await;
    assert!(matches!(
        timeout(DEADLINE, stale)
            .await
            .expect("prompt stale send failure")
            .expect("Send reply"),
        Err(EngineError::RemoteDetached)
    ));
    silent(&mut peer).await;
    flow(&mut peer, &session, 7, false).await;
    let _fresh = send(&session, fresh.request.handle, &fresh.identity).await;
    assert!(matches!(
        frame(&mut peer).await,
        (0, Performative::Transfer(Transfer { handle: 0, .. }), _)
    ));
    cleanup(connection).await;
}

#[tokio::test]
async fn duplicate_peer_handle_cannot_claim_another_pending_link_and_spares_a_sibling_session() {
    let (mut connection, session, mut peer) = pair(1).await;
    let healthy = begin(&mut connection, &mut peer, 0).await;
    let mut first = pending(&session, &mut peer, "first", Role::Sender).await;
    let second = pending(&session, &mut peer, "second", Role::Sender).await;
    answer(&mut peer, &session, &mut first, 7).await;
    let mut response = second
        .request
        .response(second.request.source.clone(), second.request.target.clone());
    response.handle = 7;
    write_amqp(
        &mut peer,
        17,
        Performative::Attach(Box::new(response)),
        Vec::new(),
    )
    .await
    .expect("duplicate peer handle response");
    assert_end(&mut peer, 0, "amqp:session:handle-in-use").await;
    assert!(matches!(
        timeout(DEADLINE, second.reply)
            .await
            .expect("prompt rejected alias")
            .expect("Attach reply"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(first.identity.is_retired());
    assert!(second.identity.is_retired());
    assert!(!healthy.identity.is_retired());
    cleanup(connection).await;
}
