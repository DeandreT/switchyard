use super::*;

#[tokio::test]
async fn an_ended_pending_session_cannot_accept_across_channel_reuse() {
    let channel = 3;
    let (incoming_tx, mut incoming_rx) = mpsc::channel(4);
    let mut pending_sessions = HashMap::new();
    let mut sessions = HashMap::new();
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    receive_begin(
        channel,
        &incoming_tx,
        &mut pending_sessions,
        &mut sessions,
        &mut wire,
    )
    .await;
    let first = incoming_rx.recv().await.expect("the first Begin is queued");

    handle_frame(
        amqp_frame(channel, Performative::End(End::default())),
        &mut wire,
        &incoming_tx,
        &mut pending_sessions,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("End invalidates the pending session");
    let _ = read_frame(&mut peer)
        .await
        .expect("the answering End decodes");

    receive_begin(
        channel,
        &incoming_tx,
        &mut pending_sessions,
        &mut sessions,
        &mut wire,
    )
    .await;
    let second = incoming_rx
        .recv()
        .await
        .expect("the reused Begin is queued");
    assert_ne!(first.incarnation, second.incarnation);

    let (attach_tx, _attaches) = mpsc::channel(1);
    let (reply, response) = oneshot::channel();
    handle_command(
        Command::AcceptSession {
            channel,
            incarnation: first.incarnation,
            attach_tx,
            reply,
        },
        &mut wire,
        &mut pending_sessions,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("a stale session acceptance keeps the connection live");
    assert!(matches!(
        response.await.expect("the stale reply remains live"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(sessions.is_empty());
    assert_eq!(pending_sessions.get(&channel), Some(&second.incarnation));

    let (attach_tx, _attaches) = mpsc::channel(1);
    let (reply, response) = oneshot::channel();
    handle_command(
        Command::AcceptSession {
            channel,
            incarnation: second.incarnation,
            attach_tx,
            reply,
        },
        &mut wire,
        &mut pending_sessions,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("the current session is accepted");
    response
        .await
        .expect("the current reply remains live")
        .expect("the current session succeeds");
    assert_eq!(sessions[&channel].incarnation, second.incarnation);
    let _ = read_frame(&mut peer)
        .await
        .expect("the answering Begin decodes");
}

#[tokio::test]
async fn a_stale_session_cannot_install_a_link_into_its_replacement() {
    let channel = 3;
    let current_session = 22;
    let mut sessions = HashMap::from([(
        channel,
        SessionState {
            incarnation: current_session,
            attach_tx: None,
            pending_attaches: HashMap::from([(1, 44)]),
            links: HashMap::new(),
            pending_flows: HashMap::new(),
            next_outgoing_id: 0,
        },
    )]);
    let mut pending_sessions = HashMap::new();
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);
    let (deliveries_tx, _deliveries) = mpsc::channel(1);
    let (detached_tx, _detached) = watch::channel(false);
    let (drain_tx, _drains) = watch::channel(None);
    let (credit_tx, _credits) = watch::channel(false);
    let (reply, response) = oneshot::channel();

    handle_command(
        Command::AcceptLink {
            channel,
            session_incarnation: current_session - 1,
            incarnation: 44,
            attach: Box::new(Attach {
                name: String::from("stale"),
                handle: 1,
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
            }),
            max_message_size: 1024,
            properties: None,
            deliveries_tx,
            detached_tx,
            drain_tx,
            credit_tx,
            reply,
        },
        &mut wire,
        &mut pending_sessions,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("a stale link acceptance keeps the connection live");
    assert!(matches!(
        response.await.expect("the stale link reply remains live"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(sessions[&channel].links.is_empty());
    assert_no_frame(&mut peer).await;
}

#[tokio::test]
async fn a_detached_pending_attach_cannot_cross_handle_reuse() {
    let channel = 3;
    let handle = 1;
    let session_incarnation = 22;
    let (attach_tx, mut attaches) = mpsc::channel(4);
    let mut sessions = HashMap::from([(
        channel,
        SessionState {
            incarnation: session_incarnation,
            attach_tx: Some(attach_tx),
            pending_attaches: HashMap::new(),
            links: HashMap::new(),
            pending_flows: HashMap::new(),
            next_outgoing_id: 0,
        },
    )]);
    let mut pending_sessions = HashMap::new();
    let (incoming_session_tx, _incoming_sessions) = mpsc::channel(1);
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    receive_attach(
        channel,
        handle,
        "first",
        &incoming_session_tx,
        &mut pending_sessions,
        &mut sessions,
        &mut wire,
    )
    .await;
    let first = attaches.recv().await.expect("the first Attach is queued");
    apply_flow(
        channel,
        Flow {
            handle: Some(handle),
            delivery_count: Some(0),
            link_credit: Some(10),
            ..Flow::default()
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("credit is buffered for the pending Attach");
    assert!(sessions[&channel].pending_flows.contains_key(&handle));

    handle_frame(
        amqp_frame(
            channel,
            Performative::Detach(Detach {
                handle,
                closed: true,
                error: None,
            }),
        ),
        &mut wire,
        &incoming_session_tx,
        &mut pending_sessions,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("Detach cancels the pending Attach");
    let _ = read_frame(&mut peer)
        .await
        .expect("the pending Detach response decodes");
    assert!(!sessions[&channel].pending_attaches.contains_key(&handle));
    assert!(!sessions[&channel].pending_flows.contains_key(&handle));

    apply_flow(
        channel,
        Flow {
            handle: Some(handle),
            delivery_count: Some(0),
            link_credit: Some(99),
            ..Flow::default()
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("late credit for a detached handle is ignored");
    assert!(!sessions[&channel].pending_flows.contains_key(&handle));

    receive_attach(
        channel,
        handle,
        "second",
        &incoming_session_tx,
        &mut pending_sessions,
        &mut sessions,
        &mut wire,
    )
    .await;
    let second = attaches.recv().await.expect("the reused Attach is queued");
    assert_ne!(first.incarnation, second.incarnation);

    let stale = accept_link(
        channel,
        session_incarnation,
        first,
        &mut wire,
        &mut pending_sessions,
        &mut sessions,
    )
    .await;
    assert!(matches!(stale, Err(EngineError::RemoteDetached)));
    assert_eq!(
        sessions[&channel].pending_attaches.get(&handle),
        Some(&second.incarnation)
    );

    accept_link(
        channel,
        session_incarnation,
        second,
        &mut wire,
        &mut pending_sessions,
        &mut sessions,
    )
    .await
    .expect("the current Attach is accepted");
    let Some(LinkState::Sending(link)) = sessions[&channel].links.get(&handle) else {
        panic!("the current receiver creates a sending link");
    };
    assert_eq!(link.credit_limit, 0, "stale Flow credit crossed link reuse");
}

#[tokio::test]
async fn settlement_after_session_end_is_link_scoped() {
    let mut sessions = HashMap::new();
    let mut pending_sessions = HashMap::new();
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);
    let (reply, response) = oneshot::channel();

    handle_command(
        Command::Settle {
            channel: 3,
            handle: 1,
            incarnation: 9,
            delivery_id: 7,
            state: DeliveryState::Accepted(Accepted),
            reply,
        },
        &mut wire,
        &mut pending_sessions,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("late settlement must not stop the connection");
    assert!(matches!(
        response.await.expect("the settlement reply remains live"),
        Err(EngineError::RemoteDetached)
    ));
    assert_no_frame(&mut peer).await;
}

async fn receive_begin(
    channel: u16,
    incoming: &mpsc::Sender<IncomingSession>,
    pending_sessions: &mut HashMap<u16, u64>,
    sessions: &mut HashMap<u16, SessionState>,
    wire: &mut tokio::io::DuplexStream,
) {
    handle_frame(
        amqp_frame(channel, Performative::Begin(Begin::default())),
        wire,
        incoming,
        pending_sessions,
        sessions,
        u32::MAX,
    )
    .await
    .expect("Begin queues an incoming session");
}

async fn receive_attach(
    channel: u16,
    handle: u32,
    name: &str,
    incoming: &mpsc::Sender<IncomingSession>,
    pending_sessions: &mut HashMap<u16, u64>,
    sessions: &mut HashMap<u16, SessionState>,
    wire: &mut tokio::io::DuplexStream,
) {
    handle_frame(
        amqp_frame(
            channel,
            Performative::Attach(Box::new(Attach {
                name: name.to_owned(),
                handle,
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
        wire,
        incoming,
        pending_sessions,
        sessions,
        u32::MAX,
    )
    .await
    .expect("Attach queues a generation-bound incoming link");
}

async fn accept_link(
    channel: u16,
    session_incarnation: u64,
    incoming: IncomingAttach,
    wire: &mut tokio::io::DuplexStream,
    pending_sessions: &mut HashMap<u16, u64>,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<u64, EngineError> {
    let (deliveries_tx, _deliveries) = mpsc::channel(1);
    let (detached_tx, _detached) = watch::channel(false);
    let (drain_tx, _drains) = watch::channel(None);
    let (credit_tx, _credits) = watch::channel(false);
    let (reply, response) = oneshot::channel();
    handle_command(
        Command::AcceptLink {
            channel,
            session_incarnation,
            incarnation: incoming.incarnation,
            attach: Box::new(incoming.attach),
            max_message_size: 1024,
            properties: None,
            deliveries_tx,
            detached_tx,
            drain_tx,
            credit_tx,
            reply,
        },
        wire,
        pending_sessions,
        sessions,
        u32::MAX,
    )
    .await
    .expect("link acceptance handling keeps the connection live");
    response.await.expect("the link reply remains live")
}

fn amqp_frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

async fn assert_no_frame(peer: &mut tokio::io::DuplexStream) {
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), read_frame(peer))
            .await
            .is_err(),
        "a stale session command must write no frame"
    );
}
