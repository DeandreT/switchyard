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
        .expect("prompt frame")
        .expect("peer frame")
    else {
        panic!("AMQP performative")
    };
    (channel, performative, payload)
}

async fn begin(connection: &mut ClientConnection, peer: &mut DuplexStream) -> ClientSession {
    let responding = async {
        let (local, performative, _) = frame(peer).await;
        assert!(matches!(performative, Performative::Begin(_)));
        write_amqp(
            peer,
            17 + local,
            Performative::Begin(Begin {
                remote_channel: Some(local),
                handle_max: 7,
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
    .expect("prompt Begin");
    session.expect("client session")
}

async fn pair() -> (ClientConnection, ClientSession, DuplexStream) {
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
        let mut open = Open::new("live-name-peer");
        open.max_frame_size = 512;
        write_amqp(&mut peer, 0, Performative::Open(open), Vec::new())
            .await
            .expect("peer Open");
        peer
    };
    let (connection, mut peer) = timeout(DEADLINE, async {
        tokio::join!(
            ClientConnection::open(wire, "live-name-client", None),
            responding
        )
    })
    .await
    .expect("client handshake");
    let mut connection = connection.expect("connection");
    let session = begin(&mut connection, &mut peer).await;
    (connection, session, peer)
}

struct Pending {
    request: Attach,
    owner: LinkIdentity,
    detached: watch::Receiver<bool>,
    reply: oneshot::Receiver<Result<(u32, Attach), EngineError>>,
}

async fn command(session: &ClientSession, name: &str, role: Role) -> Pending {
    let (deliveries_tx, _) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
    let (detached_tx, detached) = watch::channel(false);
    let owner = LinkIdentity::new();
    let (reply, result) = oneshot::channel();
    session
        .commands
        .send(ClientCommand::Attach {
            channel: session.channel,
            session: session.identity.clone(),
            request: Box::new(AttachRequest {
                name: name.into(),
                role: role.clone(),
                sender_settle_mode: SenderSettleMode::Mixed,
                receiver_settle_mode: ReceiverSettleMode::First,
                source: Some(Source::new("queue")),
                target: Some(Target::new("queue").into()),
                max_message_size: None,
            }),
            deliveries_tx,
            detached_tx,
            consumption: Arc::new(Consumption::new(session.consumed.clone())),
            identity: owner.clone(),
            reply,
        })
        .await
        .expect("Attach command");
    Pending {
        request: Attach {
            name: name.into(),
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
        owner,
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
    let mut pending = command(session, name, role).await;
    let (channel, performative, payload) = frame(peer).await;
    let Performative::Attach(attach) = performative else {
        panic!("client Attach")
    };
    assert_eq!(channel, session.channel);
    assert!(payload.is_empty());
    assert_eq!(attach.name, name);
    pending.request = *attach;
    pending
}

async fn respond(peer: &mut DuplexStream, session: &ClientSession, pending: &Pending, handle: u32) {
    let mut attach = pending.request.response(
        pending.request.source.clone(),
        pending.request.target.clone(),
    );
    attach.handle = handle;
    write_amqp(
        peer,
        17 + session.channel,
        Performative::Attach(Box::new(attach)),
        Vec::new(),
    )
    .await
    .expect("peer Attach response");
}

async fn answer(
    peer: &mut DuplexStream,
    session: &ClientSession,
    pending: &mut Pending,
    handle: u32,
) {
    respond(peer, session, pending, handle).await;
    let (discard, placeholder) = oneshot::channel();
    drop(discard);
    let result = std::mem::replace(&mut pending.reply, placeholder);
    let (local, raw) = timeout(DEADLINE, result)
        .await
        .expect("prompt approval")
        .expect("approval result")
        .expect("approved endpoint");
    assert_eq!(local, pending.request.handle);
    assert_eq!(raw.handle, handle);
    if pending.request.role == Role::Receiver {
        assert!(
            matches!(frame(peer).await, (channel, Performative::Flow(flow), payload) if channel == session.channel && flow.handle == Some(local) && payload.is_empty())
        );
    }
}

async fn refused(session: &ClientSession, peer: &mut DuplexStream, name: &str, role: Role) {
    let refused = command(session, name, role).await;
    assert!(matches!(
        timeout(DEADLINE, refused.reply)
            .await
            .expect("prompt name refusal")
            .expect("refusal reply"),
        Err(EngineError::InvalidState(_))
    ));
    assert!(
        timeout(Duration::from_millis(20), read_frame(peer))
            .await
            .is_err(),
        "local admission refusal has no wire output"
    );
    assert!(!session.identity.is_retired());
}

async fn barrier(peer: &mut DuplexStream, session: &ClientSession) {
    write_amqp(
        peer,
        17 + session.channel,
        Performative::Flow(Flow {
            incoming_window: SESSION_WINDOW,
            outgoing_window: SESSION_WINDOW,
            echo: true,
            ..Flow::default()
        }),
        Vec::new(),
    )
    .await
    .expect("session Flow barrier");
    assert!(
        matches!(frame(peer).await, (channel, Performative::Flow(flow), payload) if channel == session.channel && flow.handle.is_none() && payload.is_empty())
    );
}

async fn echo(peer: &mut DuplexStream, session: &ClientSession, peer_handle: u32, local: u32) {
    write_amqp(
        peer,
        17 + session.channel,
        Performative::Flow(Flow {
            incoming_window: SESSION_WINDOW,
            outgoing_window: SESSION_WINDOW,
            handle: Some(peer_handle),
            delivery_count: Some(0),
            link_credit: Some(1),
            echo: true,
            ..Flow::default()
        }),
        Vec::new(),
    )
    .await
    .expect("link Flow echo");
    assert!(
        matches!(frame(peer).await, (channel, Performative::Flow(flow), payload) if channel == session.channel && flow.handle == Some(local) && payload.is_empty())
    );
}

async fn cleanup(connection: ClientConnection) {
    timeout(DEADLINE, connection.shutdown())
        .await
        .expect("owned driver cleanup");
}

#[tokio::test]
async fn pending_same_role_names_remain_reserved_after_the_caller_drops_without_advancing_other_cursors()
 {
    for role in [Role::Sender, Role::Receiver] {
        let (mut connection, first, mut peer) = pair().await;
        let other = begin(&mut connection, &mut peer).await;
        let original = pending(&first, &mut peer, "held", role.clone()).await;
        refused(&first, &mut peer, "held", role.clone()).await;
        refused(&other, &mut peer, "held", role.clone()).await;
        drop(original.reply);
        refused(&other, &mut peer, "held", role.clone()).await;
        assert!(!original.owner.is_retired() && !*original.detached.borrow());
        let mut healthy = pending(&other, &mut peer, "other", role.clone()).await;
        assert_eq!(
            healthy.request.handle, 0,
            "rejected names do not consume a handle"
        );
        answer(&mut peer, &other, &mut healthy, 77).await;
        let mut response = original.request.response(
            original.request.source.clone(),
            original.request.target.clone(),
        );
        response.handle = 42;
        write_amqp(
            &mut peer,
            17 + first.channel,
            Performative::Attach(Box::new(response)),
            Vec::new(),
        )
        .await
        .expect("dropped caller response");
        if role == Role::Receiver {
            assert!(
                matches!(frame(&mut peer).await, (channel, Performative::Flow(flow), _) if channel == first.channel && flow.handle == Some(original.request.handle))
            );
        }
        barrier(&mut peer, &first).await;
        echo(&mut peer, &first, 42, original.request.handle).await;
        assert!(!original.owner.is_retired() && !*original.detached.borrow());
        cleanup(connection).await;
    }
}

#[tokio::test]
async fn installed_and_normal_closing_names_release_only_after_the_mapped_detach_ack() {
    for role in [Role::Sender, Role::Receiver] {
        let (mut connection, first, mut peer) = pair().await;
        let other = begin(&mut connection, &mut peer).await;
        let mut original = pending(&first, &mut peer, "held", role.clone()).await;
        answer(&mut peer, &first, &mut original, 42).await;
        refused(&other, &mut peer, "held", role.clone()).await;
        let (reply, result) = oneshot::channel();
        first
            .commands
            .send(ClientCommand::Detach {
                channel: first.channel,
                handle: original.request.handle,
                identity: original.owner.clone(),
                reply,
            })
            .await
            .expect("normal local close");
        assert!(
            matches!(frame(&mut peer).await, (channel, Performative::Detach(detach), payload) if channel == first.channel && detach.handle == original.request.handle && detach.error.is_none() && payload.is_empty())
        );
        assert!(original.owner.is_retired());
        refused(&other, &mut peer, "held", role.clone()).await;
        write_amqp(
            &mut peer,
            17 + first.channel,
            Performative::Detach(Detach {
                handle: 42,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await
        .expect("normal close ACK");
        timeout(DEADLINE, result)
            .await
            .expect("prompt close completion")
            .expect("close result")
            .expect("normal close success");
        let mut replacement = pending(&other, &mut peer, "held", role).await;
        assert_eq!(replacement.request.handle, 0);
        answer(&mut peer, &other, &mut replacement, 77).await;
        echo(&mut peer, &other, 77, replacement.request.handle).await;
        assert!(!replacement.owner.is_retired());
        cleanup(connection).await;
    }
}

#[tokio::test]
async fn opposite_role_same_name_pending_replies_cross_without_wrong_channel_consumption() {
    for same_session in [false, true] {
        let (mut connection, sending_session, mut peer) = pair().await;
        let other_session = if same_session {
            None
        } else {
            Some(begin(&mut connection, &mut peer).await)
        };
        let receiving_session = other_session.as_ref().unwrap_or(&sending_session);
        let mut sending = pending(&sending_session, &mut peer, "duplex", Role::Sender).await;
        let mut receiving = pending(receiving_session, &mut peer, "duplex", Role::Receiver).await;
        assert_eq!(sending.request.handle, 0);
        assert_eq!(receiving.request.handle, if same_session { 1 } else { 0 });
        if !same_session {
            respond(&mut peer, &sending_session, &receiving, 77).await;
            barrier(&mut peer, &sending_session).await;
            assert!(matches!(
                sending.reply.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert!(matches!(
                receiving.reply.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        assert!(!sending.owner.is_retired() && !receiving.owner.is_retired());
        answer(&mut peer, receiving_session, &mut receiving, 77).await;
        assert!(
            matches!(
                sending.reply.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ),
            "the receiver reply does not consume the sender pending owner"
        );
        answer(&mut peer, &sending_session, &mut sending, 42).await;
        echo(&mut peer, receiving_session, 77, receiving.request.handle).await;
        echo(&mut peer, &sending_session, 42, sending.request.handle).await;
        assert!(!sending.owner.is_retired() && !receiving.owner.is_retired());
        assert!(!*sending.detached.borrow() && !*receiving.detached.borrow());
        cleanup(connection).await;
    }
}

#[tokio::test]
async fn absent_exact_direction_allows_wrong_role_refusal_only_for_the_current_session() {
    let (mut connection, target, mut peer) = pair().await;
    let sibling = begin(&mut connection, &mut peer).await;
    let request = pending(&target, &mut peer, "held", Role::Receiver).await;
    let mut healthy = pending(&sibling, &mut peer, "healthy", Role::Sender).await;
    let mut wrong = request.request.response(
        request.request.source.clone(),
        request.request.target.clone(),
    );
    wrong.handle = 42;
    wrong.role = Role::Receiver;
    wrong.initial_delivery_count = None;
    write_amqp(
        &mut peer,
        17 + sibling.channel,
        Performative::Attach(Box::new(wrong.clone())),
        Vec::new(),
    )
    .await
    .expect("foreign wrong-role response");
    barrier(&mut peer, &sibling).await;
    assert!(!request.owner.is_retired() && !healthy.owner.is_retired());
    write_amqp(
        &mut peer,
        17 + target.channel,
        Performative::Attach(Box::new(wrong)),
        Vec::new(),
    )
    .await
    .expect("current wrong-role response");
    assert!(
        matches!(frame(&mut peer).await, (channel, Performative::End(end), payload) if channel == target.channel && payload.is_empty() && end.error.as_ref().expect("wrong-role refusal").condition.as_symbol() == Symbol::from("amqp:invalid-field"))
    );
    assert!(matches!(
        timeout(DEADLINE, request.reply)
            .await
            .expect("prompt failed pending reply")
            .expect("pending result"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(request.owner.is_retired() && target.identity.is_retired());
    answer(&mut peer, &sibling, &mut healthy, 77).await;
    echo(&mut peer, &sibling, 77, healthy.request.handle).await;
    cleanup(connection).await;
}

#[tokio::test]
async fn installed_opposite_direction_and_case_distinct_names_are_independent() {
    let (mut connection, first, mut peer) = pair().await;
    let other = begin(&mut connection, &mut peer).await;
    let mut sending = pending(&first, &mut peer, "held", Role::Sender).await;
    answer(&mut peer, &first, &mut sending, 42).await;
    let mut receiving = pending(&other, &mut peer, "held", Role::Receiver).await;
    answer(&mut peer, &other, &mut receiving, 77).await;
    let mut distinct = pending(&other, &mut peer, "Held", Role::Sender).await;
    assert_eq!(distinct.request.handle, 1);
    answer(&mut peer, &other, &mut distinct, 78).await;
    echo(&mut peer, &first, 42, sending.request.handle).await;
    echo(&mut peer, &other, 77, receiving.request.handle).await;
    echo(&mut peer, &other, 78, distinct.request.handle).await;
    cleanup(connection).await;
}

#[tokio::test]
async fn session_end_releases_pending_names_and_leaves_other_session_admission_healthy() {
    let (mut connection, first, mut peer) = pair().await;
    let other = begin(&mut connection, &mut peer).await;
    let pending_owner = pending(&first, &mut peer, "held", Role::Receiver).await;
    write_amqp(
        &mut peer,
        17 + first.channel,
        Performative::End(End::default()),
        Vec::new(),
    )
    .await
    .expect("peer session End");
    assert!(
        matches!(frame(&mut peer).await, (channel, Performative::End(End { error: None }), payload) if channel == first.channel && payload.is_empty())
    );
    assert!(matches!(
        timeout(DEADLINE, pending_owner.reply)
            .await
            .expect("retired pending caller")
            .expect("pending result"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(pending_owner.owner.is_retired() && first.identity.is_retired());
    let mut replacement = pending(&other, &mut peer, "held", Role::Receiver).await;
    assert_eq!(replacement.request.handle, 0);
    answer(&mut peer, &other, &mut replacement, 77).await;
    echo(&mut peer, &other, 77, replacement.request.handle).await;
    cleanup(connection).await;
}

type WrapperEntry = (
    PendingAttach,
    oneshot::Receiver<Result<(u32, Attach), EngineError>>,
    LinkIdentity,
    watch::Receiver<bool>,
);

fn wrapper_entry(channel: u16, role: Role) -> WrapperEntry {
    let owner = LinkIdentity::new();
    let (reply, result) = oneshot::channel();
    let (detached, watch) = watch::channel(false);
    let consumption = Arc::new(Consumption::new(Arc::new(Notify::new())));
    let link = if role == Role::Sender {
        LinkState::Sending(Box::new(SendingLink {
            identity: owner.clone(),
            auto_acknowledge: true,
            max_message_size: None,
            receiver_settle_mode: ReceiverSettleMode::First,
            default_outcome: None,
            outstanding_tags: HashSet::new(),
            settle_mode: SenderSettleMode::Mixed,
            credit: LinkCredit::new(0),
            reservations: Default::default(),
            queued: VecDeque::new(),
            active: None,
            unsettled: HashMap::new(),
            pending_acknowledgements: HashMap::new(),
            detached,
        }))
    } else {
        let (deliveries, _) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        LinkState::Receiving(Box::new(ReceivingLink {
            identity: owner.clone(),
            max_message_size: 1024,
            deliveries: deliveries.into(),
            partial: None,
            detached,
            credit: ReceiveCredit::new(0, LINK_CREDIT, consumption.clone()),
            decoders: MessageFormatDecoders::default(),
            sender_settle_mode: SenderSettleMode::Mixed,
            receiver_settle_mode: ReceiverSettleMode::First,
        }))
    };
    (
        PendingAttach {
            channel,
            session: SessionIdentity::new(),
            handle: 0,
            reply,
            link,
            consumption,
            refused_response: None,
        },
        result,
        owner,
        watch,
    )
}

#[tokio::test]
async fn directional_pending_wrapper_and_cleanup_preserve_the_opposite_owner() {
    let mut pending = PendingAttaches::default();
    let (sending, sending_reply, sending_owner, sending_watch) = wrapper_entry(0, Role::Sender);
    let (receiving, mut receiving_reply, receiving_owner, receiving_watch) =
        wrapper_entry(1, Role::Receiver);
    assert!(pending.insert("same".into(), sending).is_none());
    assert!(pending.insert("same".into(), receiving).is_none());
    assert!(
        pending.contains_key("same", &Role::Sender)
            && pending.contains_key("same", &Role::Receiver)
    );
    assert_eq!(
        pending.get("same", &Role::Sender).expect("sender").channel,
        0
    );
    assert_eq!(
        pending
            .get("same", &Role::Receiver)
            .expect("receiver")
            .channel,
        1
    );
    assert_eq!(pending.iter().count(), 2);
    assert!(pending.iter().all(|(name, role, entry)| name == "same"
        && entry.channel == if role == Role::Receiver { 1 } else { 0 }));
    let sending = pending
        .remove("same", &Role::Sender)
        .expect("only sender removed");
    assert!(
        !pending.contains_key("same", &Role::Sender)
            && pending.contains_key("same", &Role::Receiver)
    );
    assert!(pending.insert("same".into(), sending).is_none());
    let mut begins = HashMap::new();
    let mut detaches = HashMap::new();
    let mut ends = HashMap::new();
    fail_pending_session(0, &mut begins, &mut pending, &mut detaches, &mut ends);
    assert!(matches!(
        sending_reply.await.expect("sender cleanup reply"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(sending_owner.is_retired() && *sending_watch.borrow());
    assert!(!receiving_owner.is_retired() && !*receiving_watch.borrow());
    assert!(matches!(
        receiving_reply.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(
        !pending.contains_key("same", &Role::Sender)
            && pending.contains_key("same", &Role::Receiver)
    );
    fail_pending_connection(&mut begins, &mut pending, &mut detaches, &mut ends);
    assert!(matches!(
        receiving_reply.await.expect("receiver cleanup reply"),
        Err(EngineError::RemoteClosed)
    ));
    assert!(receiving_owner.is_retired() && *receiving_watch.borrow());
    assert_eq!(pending.iter().count(), 0);
    for role in [Role::Sender, Role::Receiver] {
        let (entry, _, _, _) = wrapper_entry(2, role);
        assert!(pending.insert("drain".into(), entry).is_none());
    }
    let drained: Vec<_> = pending.drain().collect();
    assert_eq!(drained.len(), 2);
    assert!(drained.iter().all(|(name, _)| name == "drain"));
    assert_eq!(pending.iter().count(), 0);
}
