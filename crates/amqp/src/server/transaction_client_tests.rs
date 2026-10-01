use tokio::{io::DuplexStream, time::timeout};

use super::*;
use crate::{Coordinator, Declared, TransactionId, TransactionalState};

const DEADLINE: Duration = Duration::from_secs(2);

fn transaction_state(declared: bool) -> DeliveryState {
    let txn_id = TransactionId::new([3]).expect("bounded transaction id");
    if declared {
        DeliveryState::Declared(Declared { txn_id })
    } else {
        DeliveryState::Transactional(TransactionalState {
            txn_id,
            outcome: Some(Outcome::Accepted(Accepted)),
        })
    }
}

async fn frame(peer: &mut DuplexStream) -> (u16, Performative) {
    let Frame::Amqp {
        channel,
        performative: Some(performative),
        payload,
    } = timeout(DEADLINE, read_frame(peer))
        .await
        .expect("prompt frame")
        .expect("frame")
    else {
        panic!("AMQP performative")
    };
    assert!(payload.is_empty() || matches!(&performative, Performative::Transfer(_)));
    (channel, performative)
}

async fn begin(connection: &mut ClientConnection, peer: &mut DuplexStream) -> ClientSession {
    let respond = async {
        let (channel, performative) = frame(peer).await;
        assert!(matches!(performative, Performative::Begin(_)));
        write_amqp(
            peer,
            17 + channel,
            Performative::Begin(Begin {
                remote_channel: Some(channel),
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await
        .expect("Begin response");
    };
    let (session, ()) = timeout(DEADLINE, async {
        tokio::join!(connection.begin(), respond)
    })
    .await
    .expect("prompt session");
    session.expect("session")
}

async fn pair() -> (ClientConnection, ClientSession, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let respond = async {
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("AMQP header");
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        assert!(matches!(frame(&mut peer).await, (0, Performative::Open(_))));
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open::new("transaction-peer")),
            Vec::new(),
        )
        .await
        .expect("Open response");
        peer
    };
    let (connection, mut peer) = timeout(DEADLINE, async {
        tokio::join!(
            ClientConnection::open(wire, "transaction-client", None),
            respond
        )
    })
    .await
    .expect("prompt connection");
    let mut connection = connection.expect("connection");
    let session = begin(&mut connection, &mut peer).await;
    (connection, session, peer)
}

struct Pending {
    request: Attach,
    identity: LinkIdentity,
    inbox: mpsc::Receiver<Delivery>,
    reply: oneshot::Receiver<Result<(u32, Attach), EngineError>>,
}

async fn pending(
    session: &ClientSession,
    peer: &mut DuplexStream,
    role: Role,
    name: &str,
) -> Pending {
    let (deliveries_tx, inbox) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
    let (detached_tx, _) = watch::channel(false);
    let identity = LinkIdentity::new();
    let (reply, result) = oneshot::channel();
    session
        .commands
        .send(ClientCommand::Attach {
            channel: session.channel,
            session: session.identity.clone(),
            request: Box::new(AttachRequest {
                name: name.into(),
                role,
                sender_settle_mode: SenderSettleMode::Mixed,
                receiver_settle_mode: ReceiverSettleMode::First,
                source: Some(Source::new("queue")),
                target: Some(Target::new("queue").into()),
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
    let (channel, Performative::Attach(request)) = frame(peer).await else {
        panic!("Attach request")
    };
    assert_eq!(channel, session.channel);
    Pending {
        request: *request,
        identity,
        inbox,
        reply: result,
    }
}

async fn respond(
    peer: &mut DuplexStream,
    session: &ClientSession,
    request: &Attach,
    peer_handle: u32,
) {
    let mut response = request.response(request.source.clone(), request.target.clone());
    response.handle = peer_handle;
    write_amqp(
        peer,
        17 + session.channel,
        Performative::Attach(Box::new(response)),
        Vec::new(),
    )
    .await
    .expect("Attach response");
}

async fn install(
    peer: &mut DuplexStream,
    session: &ClientSession,
    role: Role,
    peer_handle: u32,
    name: &str,
) -> (Attach, LinkIdentity, mpsc::Receiver<Delivery>) {
    let pending = pending(session, peer, role.clone(), name).await;
    respond(peer, session, &pending.request, peer_handle).await;
    timeout(DEADLINE, pending.reply)
        .await
        .expect("prompt attach reply")
        .expect("reply")
        .expect("ordinary attach");
    if role == Role::Receiver {
        assert!(
            matches!(frame(peer).await, (channel, Performative::Flow(flow)) if channel == session.channel && flow.handle == Some(pending.request.handle))
        );
    }
    (pending.request, pending.identity, pending.inbox)
}

async fn healthy_receipt(
    peer: &mut DuplexStream,
    session: &ClientSession,
    inbox: &mut mpsc::Receiver<Delivery>,
) {
    let payload = encode_message(&Message::default()).expect("message");
    write_amqp(
        peer,
        17 + session.channel,
        Performative::Transfer(Transfer {
            handle: 77,
            delivery_id: Some(0),
            delivery_tag: Some(vec![9].into()),
            message_format: Some(0),
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        }),
        payload,
    )
    .await
    .expect("healthy transfer");
    let delivery = timeout(DEADLINE, inbox.recv())
        .await
        .expect("healthy session remains responsive")
        .expect("ordinary delivery");
    assert_eq!(delivery.message(), &Message::default());
    assert!(!session.identity.is_retired());
}

async fn close(connection: &mut ClientConnection, peer: &mut DuplexStream) {
    let respond = async {
        assert!(matches!(frame(peer).await, (0, Performative::Close(_))));
        write_amqp(peer, 0, Performative::Close(Close::default()), Vec::new())
            .await
            .expect("Close response");
    };
    let (result, ()) = timeout(DEADLINE, async {
        tokio::join!(connection.close(), respond)
    })
    .await
    .expect("prompt close");
    result.expect("close");
}

#[tokio::test]
async fn matched_coordinator_and_transaction_source_responses_refuse_without_installing_pending_link()
 {
    for marker in 0..4 {
        let (mut connection, session, mut peer) = pair().await;
        let sibling = begin(&mut connection, &mut peer).await;
        let (_, healthy, mut healthy_inbox) =
            install(&mut peer, &sibling, Role::Receiver, 77, "healthy").await;
        let mut pending = pending(&session, &mut peer, Role::Sender, "pending").await;
        let mut response = pending.request.response(
            pending.request.source.clone(),
            pending.request.target.clone(),
        );
        response.handle = 55;
        if marker == 0 {
            response.target = Some(Coordinator::default().into());
        } else if marker >= 2 {
            let outcome = if marker == 2 {
                "amqp:transactional-state:list"
            } else {
                "amqp:declared:list"
            };
            response.source.as_mut().expect("source").outcomes =
                Some(vec![Symbol::from(outcome)].into());
        } else {
            response.source.as_mut().expect("source").default_outcome =
                Some(transaction_state(true));
        }
        write_amqp(
            &mut peer,
            17 + session.channel,
            Performative::Attach(Box::new(response)),
            Vec::new(),
        )
        .await
        .expect("unsupported response");
        assert!(
            matches!(frame(&mut peer).await, (channel, Performative::End(end)) if channel == session.channel && end.error.as_ref().expect("error").condition.as_symbol().as_str() == "amqp:not-implemented")
        );
        assert!(matches!(
            timeout(DEADLINE, pending.reply)
                .await
                .expect("prompt refusal")
                .expect("reply"),
            Err(EngineError::RemoteDetached)
        ));
        assert!(pending.identity.is_retired() && pending.inbox.try_recv().is_err());
        assert!(!healthy.is_retired());
        healthy_receipt(&mut peer, &sibling, &mut healthy_inbox).await;
        close(&mut connection, &mut peer).await;
    }
}

#[tokio::test]
async fn transactional_transfer_disposition_and_flow_cannot_become_ordinary_client_operations() {
    for case in 0..5 {
        let (mut connection, session, mut peer) = pair().await;
        let sibling = begin(&mut connection, &mut peer).await;
        let (_, healthy, mut healthy_inbox) =
            install(&mut peer, &sibling, Role::Receiver, 77, "healthy").await;
        let local_role = if case < 2 {
            Role::Receiver
        } else {
            Role::Sender
        };
        let (request, owner, mut inbox) =
            install(&mut peer, &session, local_role, 55, "fault").await;
        let mut send_result = None;
        let performative = if case < 2 {
            Performative::Transfer(Transfer {
                handle: 55,
                delivery_id: Some(0),
                delivery_tag: Some(vec![1].into()),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: Some(transaction_state(case == 0)),
                resume: false,
                aborted: false,
                batchable: false,
            })
        } else if case < 4 {
            write_amqp(
                &mut peer,
                17 + session.channel,
                Performative::Flow(Flow {
                    handle: Some(55),
                    next_incoming_id: Some(0),
                    incoming_window: SESSION_WINDOW,
                    outgoing_window: SESSION_WINDOW,
                    delivery_count: Some(0),
                    link_credit: Some(1),
                    ..Flow::default()
                }),
                Vec::new(),
            )
            .await
            .expect("ordinary credit");
            let (reply, result) = oneshot::channel();
            session
                .commands
                .send(ClientCommand::Send {
                    channel: session.channel,
                    handle: request.handle,
                    identity: owner.clone(),
                    message: Box::new(Message::default()),
                    delivery_tag: vec![1].into(),
                    message_format: 0,
                    reply,
                })
                .await
                .expect("ordinary send");
            assert!(
                matches!(frame(&mut peer).await, (channel, Performative::Transfer(_)) if channel == session.channel)
            );
            send_result = Some(result);
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: u32::MAX,
                last: Some(0),
                settled: true,
                state: Some(transaction_state(case == 2)),
                batchable: false,
            })
        } else {
            Performative::Flow(Flow {
                handle: Some(55),
                next_incoming_id: Some(0),
                incoming_window: 0,
                outgoing_window: 0,
                delivery_count: Some(0),
                link_credit: Some(100),
                properties: Some(
                    [(Symbol::from("txn-id"), crate::Value::Null)]
                        .into_iter()
                        .collect(),
                ),
                ..Flow::default()
            })
        };
        let payload = if case < 2 {
            encode_message(&Message::default()).expect("message")
        } else {
            Vec::new()
        };
        write_amqp(&mut peer, 17 + session.channel, performative, payload)
            .await
            .expect("transaction marker");
        if case == 4 {
            assert!(
                matches!(frame(&mut peer).await, (channel, Performative::Detach(detach)) if channel == session.channel && detach.handle == request.handle && detach.error.as_ref().expect("error").condition.as_symbol().as_str() == "amqp:not-implemented")
            );
            assert!(!session.identity.is_retired());
        } else {
            assert!(
                matches!(frame(&mut peer).await, (channel, Performative::End(end)) if channel == session.channel && end.error.as_ref().expect("error").condition.as_symbol().as_str() == "amqp:not-implemented")
            );
        }
        if let Some(result) = send_result {
            assert!(matches!(
                timeout(DEADLINE, result)
                    .await
                    .expect("prompt send refusal")
                    .expect("reply"),
                Err(EngineError::RemoteDetached)
            ));
        }
        assert!(owner.is_retired() && inbox.try_recv().is_err());
        assert!(!healthy.is_retired());
        healthy_receipt(&mut peer, &sibling, &mut healthy_inbox).await;
        close(&mut connection, &mut peer).await;
    }
}

#[tokio::test]
async fn public_receiver_source_rejects_transaction_default_before_writing_attach() {
    let (mut connection, mut session, mut peer) = pair().await;
    let source = Source {
        default_outcome: Some(transaction_state(true)),
        ..Source::new("queue")
    };
    assert!(matches!(
        session
            .attach_receiver_with("invalid-source", source, None, SenderSettleMode::Mixed)
            .await,
        Err(EngineError::InvalidState(_))
    ));
    let (_, owner, _) = install(&mut peer, &session, Role::Sender, 55, "valid-source").await;
    assert!(!owner.is_retired());
    close(&mut connection, &mut peer).await;
}
