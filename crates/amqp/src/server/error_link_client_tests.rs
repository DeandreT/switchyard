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
                handle_max: 3,
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
        let mut open = Open::new("error-name-peer");
        open.max_frame_size = 512;
        write_amqp(&mut peer, 0, Performative::Open(open), Vec::new())
            .await
            .expect("peer Open");
        peer
    };
    let (connection, mut peer) = timeout(DEADLINE, async {
        tokio::join!(
            ClientConnection::open(wire, "error-name-client", None),
            responding
        )
    })
    .await
    .expect("client handshake");
    let mut connection = connection.expect("client connection");
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
                target: Some(Target::new("queue")),
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
    assert_eq!(channel, session.channel);
    assert!(payload.is_empty());
    let Performative::Attach(attach) = performative else {
        panic!("client Attach")
    };
    assert_eq!(attach.name, name);
    pending.request = *attach;
    pending
}

async fn answer(
    peer: &mut DuplexStream,
    session: &ClientSession,
    pending: &mut Pending,
    handle: u32,
    empty: bool,
) {
    let mut response = pending.request.response(
        pending.request.source.clone(),
        pending.request.target.clone(),
    );
    response.handle = handle;
    if empty {
        response.unsettled = Some(Default::default());
    }
    write_amqp(
        peer,
        17 + session.channel,
        Performative::Attach(Box::new(response)),
        Vec::new(),
    )
    .await
    .expect("peer Attach");
    let (reply, placeholder) = oneshot::channel();
    drop(reply);
    let result = std::mem::replace(&mut pending.reply, placeholder);
    let (local, raw) = timeout(DEADLINE, result)
        .await
        .expect("prompt approval")
        .expect("Attach result")
        .expect("accepted fresh Attach");
    assert_eq!(local, pending.request.handle);
    assert_eq!(raw.handle, handle);
    if pending.request.role == Role::Receiver {
        assert!(
            matches!(frame(peer).await, (channel, Performative::Flow(flow), _) if channel == session.channel && flow.handle == Some(local))
        );
    }
}

async fn poison(peer: &mut DuplexStream, session: &ClientSession, role: Role) -> LinkIdentity {
    let pending = pending(session, peer, "dead", role).await;
    assert_eq!(pending.request.handle, 0);
    let mut response = pending.request.response(
        pending.request.source.clone(),
        pending.request.target.clone(),
    );
    response.handle = 42;
    response.incomplete_unsettled = true;
    write_amqp(
        peer,
        17 + session.channel,
        Performative::Attach(Box::new(response)),
        Vec::new(),
    )
    .await
    .expect("peer recovery response");
    let (channel, performative, payload) = frame(peer).await;
    assert_eq!(channel, session.channel);
    assert!(payload.is_empty());
    assert!(
        matches!(performative, Performative::Detach(detach) if detach.handle == 0 && detach.error.as_ref().expect("recovery error").condition.as_symbol() == Symbol::from("amqp:not-implemented"))
    );
    assert!(matches!(
        timeout(DEADLINE, pending.reply)
            .await
            .expect("prompt refusal")
            .expect("refusal result"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(pending.owner.is_retired() && *pending.detached.borrow());
    pending.owner
}

async fn barrier(peer: &mut DuplexStream, session: &ClientSession) {
    barrier_at(peer, session, 0).await;
}

async fn barrier_at(peer: &mut DuplexStream, session: &ClientSession, next_outgoing_id: u32) {
    write_amqp(
        peer,
        17 + session.channel,
        Performative::Flow(Flow {
            incoming_window: SESSION_WINDOW,
            outgoing_window: SESSION_WINDOW,
            next_outgoing_id,
            echo: true,
            ..Flow::default()
        }),
        Vec::new(),
    )
    .await
    .expect("session barrier");
    assert!(
        matches!(frame(peer).await, (channel, Performative::Flow(flow), _) if channel == session.channel && flow.handle.is_none())
    );
}

async fn ack(peer: &mut DuplexStream, session: &ClientSession) {
    write_amqp(
        peer,
        17 + session.channel,
        Performative::Detach(Detach {
            handle: 42,
            closed: true,
            error: None,
        }),
        Vec::new(),
    )
    .await
    .expect("error detach acknowledgement");
    barrier(peer, session).await;
}

async fn link_flow(peer: &mut DuplexStream, session: &ClientSession, handle: u32) {
    write_amqp(
        peer,
        17 + session.channel,
        Performative::Flow(Flow {
            incoming_window: SESSION_WINDOW,
            outgoing_window: SESSION_WINDOW,
            handle: Some(handle),
            delivery_count: Some(0),
            link_credit: Some(1),
            echo: true,
            ..Flow::default()
        }),
        Vec::new(),
    )
    .await
    .expect("peer link Flow");
}

async fn end(peer: &mut DuplexStream, session: &ClientSession, condition: &str) {
    let (channel, performative, payload) = frame(peer).await;
    assert_eq!(channel, session.channel);
    assert!(payload.is_empty());
    assert!(
        matches!(performative, Performative::End(end) if end.error.as_ref().expect("End condition").condition.as_symbol() == Symbol::from(condition))
    );
}

async fn cleanup(connection: ClientConnection) {
    timeout(DEADLINE, connection.shutdown())
        .await
        .expect("owned driver cleanup");
}

#[tokio::test]
async fn exact_local_error_name_refusal_preserves_the_cursor_and_opposite_role_and_case_remain_valid()
 {
    for role in [Role::Sender, Role::Receiver] {
        let (connection, session, mut peer) = pair().await;
        poison(&mut peer, &session, role.clone()).await;
        ack(&mut peer, &session).await;
        let refused = command(&session, "dead", role.clone()).await;
        assert!(matches!(
            timeout(DEADLINE, refused.reply)
                .await
                .expect("prompt local name refusal")
                .expect("local refusal result"),
            Err(EngineError::InvalidState(_))
        ));
        assert!(
            timeout(Duration::from_millis(20), read_frame(&mut peer))
                .await
                .is_err()
        );
        let opposite = if role == Role::Sender {
            Role::Receiver
        } else {
            Role::Sender
        };
        let mut allowed = pending(&session, &mut peer, "dead", opposite).await;
        assert_eq!(
            allowed.request.handle, 1,
            "a rejected name does not advance the local handle cursor"
        );
        answer(&mut peer, &session, &mut allowed, 42, true).await;
        link_flow(&mut peer, &session, 42).await;
        assert!(
            matches!(frame(&mut peer).await, (channel, Performative::Flow(flow), _) if channel == session.channel && flow.handle == Some(allowed.request.handle))
        );
        assert!(!allowed.owner.is_retired() && !*allowed.detached.borrow());
        let mut case_sensitive = pending(&session, &mut peer, "Dead", role).await;
        assert_eq!(case_sensitive.request.handle, 2);
        answer(&mut peer, &session, &mut case_sensitive, 43, false).await;
        assert!(!case_sensitive.owner.is_retired());
        cleanup(connection).await;
    }
}

#[tokio::test]
async fn names_survive_session_end_and_unsolicited_responses_are_classified_before_pending_lookup()
{
    for empty in [false, true] {
        let (mut connection, session, mut peer) = pair().await;
        poison(&mut peer, &session, Role::Sender).await;
        ack(&mut peer, &session).await;
        write_amqp(
            &mut peer,
            17 + session.channel,
            Performative::End(End::default()),
            Vec::new(),
        )
        .await
        .expect("peer session End");
        assert!(
            matches!(frame(&mut peer).await, (channel, Performative::End(End { error: None }), _) if channel == session.channel)
        );
        assert!(session.identity.is_retired());
        let target = begin(&mut connection, &mut peer).await;
        let sibling = begin(&mut connection, &mut peer).await;
        let local = command(&target, "dead", Role::Sender).await;
        assert!(matches!(
            timeout(DEADLINE, local.reply)
                .await
                .expect("cross-session local refusal")
                .expect("local refusal reply"),
            Err(EngineError::InvalidState(_))
        ));
        let mut healthy = pending(&sibling, &mut peer, "healthy", Role::Sender).await;
        let mut unsolicited = healthy.request.response(
            healthy.request.source.clone(),
            healthy.request.target.clone(),
        );
        unsolicited.name = "dead".into();
        unsolicited.handle = 43;
        if empty {
            unsolicited.unsettled = Some(Default::default());
        }
        write_amqp(
            &mut peer,
            17 + target.channel,
            Performative::Attach(Box::new(unsolicited)),
            Vec::new(),
        )
        .await
        .expect("unsolicited known-name Attach");
        end(
            &mut peer,
            &target,
            if empty {
                "amqp:not-implemented"
            } else {
                "amqp:session:errant-link"
            },
        )
        .await;
        assert!(target.identity.is_retired());
        assert!(!sibling.identity.is_retired() && !healthy.owner.is_retired());
        answer(&mut peer, &sibling, &mut healthy, 77, false).await;
        link_flow(&mut peer, &sibling, 77).await;
        assert!(
            matches!(frame(&mut peer).await, (channel, Performative::Flow(flow), _) if channel == sibling.channel && flow.handle == Some(healthy.request.handle))
        );
        cleanup(connection).await;
    }
}

#[tokio::test]
async fn historical_detach_after_ack_is_ignored_and_late_flow_or_transfer_refuses_only_its_session()
{
    for incoming_flow in [false, true] {
        let (mut connection, session, mut peer) = pair().await;
        let sibling = begin(&mut connection, &mut peer).await;
        poison(&mut peer, &session, Role::Receiver).await;
        ack(&mut peer, &session).await;
        write_amqp(
            &mut peer,
            17 + session.channel,
            Performative::Detach(Detach {
                handle: 42,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await
        .expect("historical Detach");
        barrier(&mut peer, &session).await;
        assert!(!session.identity.is_retired());
        let mut healthy = pending(&sibling, &mut peer, "healthy", Role::Sender).await;
        if incoming_flow {
            link_flow(&mut peer, &session, 42).await;
        } else {
            write_amqp(
                &mut peer,
                17 + session.channel,
                Performative::Transfer(Transfer {
                    handle: 42,
                    delivery_id: Some(7),
                    delivery_tag: Some(vec![7].into()),
                    message_format: Some(0),
                    settled: Some(false),
                    more: true,
                    rcv_settle_mode: None,
                    state: None,
                    resume: false,
                    aborted: false,
                    batchable: false,
                }),
                vec![1; 257],
            )
            .await
            .expect("late peer Transfer");
        }
        end(&mut peer, &session, "amqp:session:errant-link").await;
        assert!(session.identity.is_retired());
        assert!(!sibling.identity.is_retired() && !healthy.owner.is_retired());
        answer(&mut peer, &sibling, &mut healthy, 77, false).await;
        barrier(&mut peer, &sibling).await;
        cleanup(connection).await;
    }
}

#[tokio::test]
async fn occupied_client_error_alias_has_conservative_end_priority_for_null_unrelated_and_empty_attach()
 {
    for kind in 0..3 {
        let (mut connection, session, mut peer) = pair().await;
        let sibling = begin(&mut connection, &mut peer).await;
        poison(&mut peer, &session, Role::Sender).await;
        let mut response = Attach {
            name: if kind == 1 { "unrelated" } else { "dead" }.into(),
            handle: 42,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(Source::new("queue")),
            target: Some(Target::new("queue")),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        if kind == 2 {
            response.unsettled = Some(Default::default());
        }
        write_amqp(
            &mut peer,
            17 + session.channel,
            Performative::Attach(Box::new(response)),
            Vec::new(),
        )
        .await
        .expect("occupied error-alias Attach");
        end(
            &mut peer,
            &session,
            if kind == 2 {
                "amqp:not-implemented"
            } else {
                "amqp:session:errant-link"
            },
        )
        .await;
        assert!(!sibling.identity.is_retired());
        barrier(&mut peer, &sibling).await;
        cleanup(connection).await;
    }
}

#[tokio::test]
async fn ordinary_client_close_does_not_poison_the_name_for_a_fresh_same_role_link() {
    let (connection, session, mut peer) = pair().await;
    let mut first = pending(&session, &mut peer, "ordinary", Role::Sender).await;
    answer(&mut peer, &session, &mut first, 42, true).await;
    let (reply, result) = oneshot::channel();
    session
        .commands
        .send(ClientCommand::Detach {
            channel: session.channel,
            handle: first.request.handle,
            identity: first.owner.clone(),
            reply,
        })
        .await
        .expect("ordinary local Detach");
    assert!(
        matches!(frame(&mut peer).await, (channel, Performative::Detach(Detach { handle: 0, error: None, .. }), _) if channel == session.channel)
    );
    write_amqp(
        &mut peer,
        17 + session.channel,
        Performative::Detach(Detach {
            handle: 42,
            closed: true,
            error: None,
        }),
        Vec::new(),
    )
    .await
    .expect("normal Detach ACK");
    timeout(DEADLINE, result)
        .await
        .expect("prompt normal close")
        .expect("close reply")
        .expect("normal close success");
    let mut replacement = pending(&session, &mut peer, "ordinary", Role::Sender).await;
    assert_eq!(replacement.request.handle, 1);
    answer(&mut peer, &session, &mut replacement, 42, false).await;
    link_flow(&mut peer, &session, 42).await;
    assert!(
        matches!(frame(&mut peer).await, (channel, Performative::Flow(flow), _) if channel == session.channel && flow.handle == Some(replacement.request.handle))
    );
    assert!(!replacement.owner.is_retired());
    cleanup(connection).await;
}

#[tokio::test]
async fn historical_detach_cannot_reply_or_retire_a_fresh_different_peer_binding() {
    let (connection, session, mut peer) = pair().await;
    poison(&mut peer, &session, Role::Sender).await;
    ack(&mut peer, &session).await;
    let mut replacement = pending(&session, &mut peer, "replacement", Role::Sender).await;
    answer(&mut peer, &session, &mut replacement, 43, false).await;
    write_amqp(
        &mut peer,
        17 + session.channel,
        Performative::Detach(Detach {
            handle: 42,
            closed: true,
            error: None,
        }),
        Vec::new(),
    )
    .await
    .expect("historical peer Detach");
    barrier(&mut peer, &session).await;
    assert!(!replacement.owner.is_retired() && !*replacement.detached.borrow());
    link_flow(&mut peer, &session, 43).await;
    assert!(
        matches!(frame(&mut peer).await, (channel, Performative::Flow(flow), _) if channel == session.channel && flow.handle == Some(replacement.request.handle))
    );
    cleanup(connection).await;
}

#[tokio::test]
async fn pending_same_name_response_records_its_own_error_handle_after_another_session_fails() {
    let (mut connection, original_session, mut peer) = pair().await;
    let replacement_session = begin(&mut connection, &mut peer).await;
    let mut original = pending(&original_session, &mut peer, "dead", Role::Receiver).await;
    answer(&mut peer, &original_session, &mut original, 42, false).await;
    let replacement = pending(&replacement_session, &mut peer, "dead", Role::Receiver).await;
    assert!(!original.owner.is_retired() && !replacement.owner.is_retired());

    write_amqp(
        &mut peer,
        17 + original_session.channel,
        Performative::Transfer(Transfer {
            handle: 42,
            delivery_id: Some(7),
            delivery_tag: Some(vec![7].into()),
            message_format: Some(999),
            settled: Some(false),
            more: true,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        }),
        Vec::new(),
    )
    .await
    .expect("old receiver error");
    assert!(
        matches!(frame(&mut peer).await, (channel, Performative::Detach(detach), payload)
            if channel == original_session.channel && payload.is_empty()
                && detach.handle == original.request.handle
                && detach.error.as_ref().expect("old error").condition.as_symbol() == Symbol::from("amqp:not-implemented"))
    );
    assert!(original.owner.is_retired() && *original.detached.borrow());
    assert!(!replacement.owner.is_retired());

    let mut response = replacement.request.response(
        replacement.request.source.clone(),
        replacement.request.target.clone(),
    );
    response.handle = 77;
    response.unsettled = Some(Default::default());
    assert!(
        !has_recovery_state(&response),
        "empty unsettled alone is fresh state"
    );
    write_amqp(
        &mut peer,
        17 + replacement_session.channel,
        Performative::Attach(Box::new(response)),
        Vec::new(),
    )
    .await
    .expect("now-known pending response");
    assert!(
        matches!(frame(&mut peer).await, (channel, Performative::Detach(detach), payload)
            if channel == replacement_session.channel && payload.is_empty()
                && detach.handle == replacement.request.handle
                && detach.error.as_ref().expect("pending recovery error").condition.as_symbol() == Symbol::from("amqp:not-implemented"))
    );
    assert!(matches!(
        timeout(DEADLINE, replacement.reply)
            .await
            .expect("prompt pending refusal")
            .expect("pending result"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(replacement.owner.is_retired() && *replacement.detached.borrow());
    assert!(!replacement_session.identity.is_retired());
    write_amqp(
        &mut peer,
        17 + replacement_session.channel,
        Performative::Detach(Detach {
            handle: 77,
            closed: true,
            error: None,
        }),
        Vec::new(),
    )
    .await
    .expect("pending refusal ACK");
    barrier(&mut peer, &replacement_session).await;
    link_flow(&mut peer, &replacement_session, 77).await;
    end(&mut peer, &replacement_session, "amqp:session:errant-link").await;
    assert!(replacement_session.identity.is_retired());
    assert!(!original_session.identity.is_retired());
    barrier_at(&mut peer, &original_session, 1).await;
    cleanup(connection).await;
}
