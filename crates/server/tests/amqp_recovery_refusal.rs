//! Switchyard's explicit, link-local refusal of unsupported AMQP recovery.

use std::{collections::HashMap, error::Error, time::Duration};

use amqp::{
    Accepted, Attach, Begin, ClientConnection, ClientReceiver, ClientSession, ConnectionOptions,
    DeliveryState, Detach, EngineError, Flow, Frame, IncomingAttach, LinkEndpoint, Message, Open,
    OrderedMap, Performative, ProtocolHeader, Receiver, ReceiverSettleMode, Role, SenderSettleMode,
    ServerConnection, ServerSession, Source, Symbol, Target, Transfer, encode_message, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

struct Peer {
    stream: TcpStream,
    transfers: HashMap<u16, u32>,
}

impl Peer {
    async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> TestResult {
        if matches!(performative, Performative::Transfer(_)) {
            let count = self.transfers.entry(channel).or_default();
            *count = count.wrapping_add(1);
        }
        timeout(
            IO_TIMEOUT,
            write_frame(
                &mut self.stream,
                &Frame::Amqp {
                    channel,
                    performative: Some(performative),
                    payload,
                },
            ),
        )
        .await??;
        Ok(())
    }

    async fn read(&mut self) -> TestResult<Frame> {
        Ok(timeout(IO_TIMEOUT, read_frame(&mut self.stream)).await??)
    }

    async fn barrier(&mut self, channel: u16) -> TestResult<Vec<Frame>> {
        let count = self.transfers.get(&channel).copied().unwrap_or(0);
        self.send(
            channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 2_048,
                next_outgoing_id: count,
                outgoing_window: 2_048,
                echo: true,
                ..Flow::default()
            }),
            Vec::new(),
        )
        .await?;
        let mut frames = Vec::new();
        loop {
            let frame = self.read().await?;
            let echoed = matches!(&frame, Frame::Amqp {
                channel: actual, performative: Some(Performative::Flow(flow)), ..
            } if *actual == channel && flow.handle.is_none() && flow.next_incoming_id == Some(count));
            assert!(
                !matches!(
                    &frame,
                    Frame::Amqp {
                        performative: Some(Performative::End(_) | Performative::Close(_)),
                        ..
                    }
                ),
                "recovery refusal must not end a healthy session: {frame:?}"
            );
            frames.push(frame);
            if echoed {
                return Ok(frames);
            }
        }
    }

    async fn refused_attach(&mut self, channel: u16, handle: u32, role: Role) -> TestResult {
        let response = self.read().await?;
        let Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Attach(response)),
            payload,
        } = response
        else {
            panic!("unsupported recovery must answer Attach before Detach");
        };
        assert_eq!(actual, channel);
        assert_eq!(response.handle, handle);
        assert_eq!(response.role, role.opposite());
        assert!(payload.is_empty());
        assert!(response.source.is_none());
        assert!(response.target.is_none());
        assert!(response.unsettled.is_none());
        assert!(!response.incomplete_unsettled);
        self.refused_detach(channel, handle).await
    }

    async fn refused_detach(&mut self, channel: u16, handle: u32) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Detach(detach)),
                    ..
                } => {
                    assert_eq!(actual, channel);
                    assert_eq!(detach.handle, handle);
                    assert!(detach.closed);
                    assert_eq!(
                        detach
                            .error
                            .expect("explicit unsupported condition")
                            .condition
                            .as_symbol(),
                        Symbol::from("amqp:not-implemented")
                    );
                    self.send(
                        channel,
                        Performative::Detach(Detach {
                            handle,
                            closed: true,
                            error: None,
                        }),
                        Vec::new(),
                    )
                    .await?;
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("refusal must stay link-local: {other:?}"),
            }
        }
    }

    async fn accepted(&mut self, channel: u16, id: u32) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } => {
                    assert_eq!(actual, channel);
                    assert_eq!(disposition.role, Role::Receiver);
                    assert_eq!(disposition.first, id);
                    assert_eq!(disposition.last, None);
                    assert!(disposition.settled);
                    assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("healthy sibling must produce an outcome: {other:?}"),
            }
        }
    }
}

struct ServerPeer {
    connection: ServerConnection,
    peer: Peer,
}

impl ServerPeer {
    async fn new() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut stream = TcpStream::connect(listener.local_addr()?).await?;
        stream.set_nodelay(true)?;
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let accepting = tokio::spawn(ServerConnection::accept_with_options(
            socket,
            "recovery-server",
            None,
            ConnectionOptions::default().idle_timeout_millis(0),
        ));
        write_protocol_header(&mut stream, ProtocolHeader::AMQP).await?;
        assert_eq!(
            timeout(IO_TIMEOUT, read_protocol_header(&mut stream)).await??,
            ProtocolHeader::AMQP
        );
        write_frame(
            &mut stream,
            &Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(Open::new("raw-recovery-peer"))),
                payload: Vec::new(),
            },
        )
        .await?;
        assert!(matches!(
            timeout(IO_TIMEOUT, read_frame(&mut stream)).await??,
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        Ok(Self {
            connection: timeout(IO_TIMEOUT, accepting).await???,
            peer: Peer {
                stream,
                transfers: HashMap::new(),
            },
        })
    }

    async fn begin(&mut self) -> TestResult<ServerSession> {
        self.peer
            .send(0, Performative::Begin(Begin::default()), Vec::new())
            .await?;
        let incoming = timeout(IO_TIMEOUT, self.connection.next_incoming_session())
            .await?
            .expect("incoming session");
        let session = timeout(IO_TIMEOUT, self.connection.accept_session(incoming)).await??;
        assert!(matches!(self.peer.read().await?, Frame::Amqp {
            channel: 0, performative: Some(Performative::Begin(begin)), ..
        } if begin.remote_channel == Some(0)));
        Ok(session)
    }

    async fn request_attach(
        &mut self,
        session: &mut ServerSession,
        attach: Attach,
    ) -> TestResult<IncomingAttach> {
        self.peer
            .send(
                0,
                Performative::Attach(Box::new(attach.clone())),
                Vec::new(),
            )
            .await?;
        let received = timeout(IO_TIMEOUT, session.next_incoming_attach())
            .await?
            .expect("only supported Attach is offered to the application");
        assert_eq!(received.name, attach.name);
        assert_eq!(received.handle, attach.handle);
        Ok(received)
    }

    async fn approve(
        &mut self,
        session: &ServerSession,
        attach: IncomingAttach,
    ) -> TestResult<Receiver> {
        let handle = attach.handle;
        let LinkEndpoint::Receiver(receiver) =
            timeout(IO_TIMEOUT, session.accept_attach(attach, 4 * 1024 * 1024)).await??
        else {
            panic!("peer sender creates the server receiver");
        };
        let mut attached = false;
        let mut credited = false;
        while !attached || !credited {
            match self.peer.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Attach(response)),
                    ..
                } => {
                    assert_eq!(response.handle, handle);
                    assert!(response.source.is_some());
                    assert!(response.target.is_some());
                    assert!(!response.incomplete_unsettled);
                    assert!(response.unsettled.as_ref().is_none_or(|map| map.is_empty()));
                    attached = true;
                }
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Flow(flow)),
                    ..
                } => {
                    if flow.handle == Some(handle) {
                        assert_eq!(flow.delivery_count, Some(0));
                        assert_eq!(flow.link_credit, Some(32));
                        credited = true;
                    }
                }
                other => panic!("unexpected supported attach response: {other:?}"),
            }
        }
        Ok(receiver)
    }

    async fn receiver(&mut self, session: &mut ServerSession, handle: u32) -> TestResult<Receiver> {
        let attach = self
            .request_attach(session, attach(handle, Role::Sender))
            .await?;
        self.approve(session, attach).await
    }

    async fn healthy(&mut self, receiver: &mut Receiver, handle: u32, id: u32) -> TestResult {
        let message = Message::data(id.to_be_bytes().to_vec());
        self.peer
            .send(
                0,
                Performative::Transfer(first(handle, id)),
                encode_message(&message)?,
            )
            .await?;
        let delivery = timeout(IO_TIMEOUT, receiver.recv()).await??;
        assert_eq!(delivery.message(), &message);
        receiver.accept(&delivery).await?;
        self.peer.accepted(0, id).await?;
        let frames = self.peer.barrier(0).await?;
        assert_only_flows(&frames);
        Ok(())
    }

    async fn finish(self) {
        self.connection.shutdown().await;
    }
}

fn attach(handle: u32, role: Role) -> Attach {
    Attach {
        name: format!("recovery-{handle}"),
        handle,
        initial_delivery_count: (role == Role::Sender).then_some(0),
        role,
        snd_settle_mode: SenderSettleMode::Mixed,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue")),
        unsettled: None,
        incomplete_unsettled: false,
        max_message_size: Some(4 * 1024 * 1024),
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

fn retained(attach: &mut Attach, nonempty: bool, incomplete: bool) {
    let mut unsettled = OrderedMap::new();
    if nonempty {
        unsettled.insert(vec![5].into(), Some(DeliveryState::Accepted(Accepted)));
    }
    attach.unsettled = Some(unsettled);
    attach.incomplete_unsettled = incomplete;
}

fn first(handle: u32, id: u32) -> Transfer {
    Transfer {
        delivery_id: Some(id),
        delivery_tag: Some(id.to_be_bytes().to_vec().into()),
        message_format: Some(0),
        ..continuation(handle)
    }
}

fn continuation(handle: u32) -> Transfer {
    Transfer {
        handle,
        delivery_id: None,
        delivery_tag: None,
        message_format: None,
        settled: Some(false),
        more: false,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn assert_only_flows(frames: &[Frame]) {
    assert!(
        frames.iter().all(|frame| matches!(
            frame,
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                ..
            }
        )),
        "barrier must not observe an unintended attach, disposition, or refusal: {frames:?}"
    );
}

#[tokio::test]
async fn retained_or_incomplete_server_attach_is_refused_before_application_approval_for_both_roles()
-> TestResult {
    for role in [Role::Sender, Role::Receiver] {
        for (nonempty, incomplete) in [(true, false), (false, true), (true, true)] {
            let mut node = ServerPeer::new().await?;
            let mut session = node.begin().await?;
            let mut recovery = attach(0, role.clone());
            retained(&mut recovery, nonempty, incomplete);
            node.peer
                .send(0, Performative::Attach(Box::new(recovery)), Vec::new())
                .await?;
            // No approval is offered or granted before this peer response.
            node.peer.refused_attach(0, 0, role.clone()).await?;
            let mut sibling = node.receiver(&mut session, 1).await?;
            node.healthy(&mut sibling, 1, 5).await?;
            node.finish().await;
        }
    }
    Ok(())
}

#[tokio::test]
async fn complete_empty_unsettled_map_is_accepted_and_receives_a_normal_delivery() -> TestResult {
    let mut node = ServerPeer::new().await?;
    let mut session = node.begin().await?;
    let mut request = attach(0, Role::Sender);
    retained(&mut request, false, false);
    let request = node.request_attach(&mut session, request).await?;
    assert!(
        request
            .unsettled
            .as_ref()
            .expect("explicit complete empty map")
            .is_empty()
    );
    let mut receiver = node.approve(&session, request).await?;
    node.healthy(&mut receiver, 0, 7).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn pipelined_recovery_waits_for_local_begin_and_never_becomes_public_link_approval()
-> TestResult {
    for (nonempty, incomplete) in [(true, false), (false, true)] {
        let mut node = ServerPeer::new().await?;
        let mut healthy_session = node.begin().await?;
        let mut healthy = node.receiver(&mut healthy_session, 0).await?;
        node.peer
            .send(1, Performative::Begin(Begin::default()), Vec::new())
            .await?;
        let mut recovery = attach(0, Role::Sender);
        retained(&mut recovery, nonempty, incomplete);
        node.peer
            .send(1, Performative::Attach(Box::new(recovery)), Vec::new())
            .await?;
        // Session 0's echo is ordered after the pipelined session 1 input. It
        // proves no session 1 response escaped while local Begin was pending.
        assert_only_flows(&node.peer.barrier(0).await?);
        let incoming = timeout(IO_TIMEOUT, node.connection.next_incoming_session())
            .await?
            .expect("pending pipelined session");
        let mut session = timeout(IO_TIMEOUT, node.connection.accept_session(incoming)).await??;
        assert!(matches!(node.peer.read().await?, Frame::Amqp {
            channel: 1, performative: Some(Performative::Begin(begin)), ..
        } if begin.remote_channel == Some(1)));
        node.peer.refused_attach(1, 0, Role::Sender).await?;

        let supported = attach(1, Role::Sender);
        node.peer
            .send(
                1,
                Performative::Attach(Box::new(supported.clone())),
                Vec::new(),
            )
            .await?;
        let request = timeout(IO_TIMEOUT, session.next_incoming_attach())
            .await?
            .expect("only the supported sibling reaches application approval");
        assert_eq!(request.name, supported.name);
        assert_eq!(request.handle, 1);
        let LinkEndpoint::Receiver(mut receiver) =
            timeout(IO_TIMEOUT, session.accept_attach(request, 4 * 1024 * 1024)).await??
        else {
            panic!("supported pipelined session receiver");
        };
        assert!(matches!(node.peer.read().await?, Frame::Amqp {
            channel: 1, performative: Some(Performative::Attach(response)), ..
        } if response.handle == 1 && response.target.is_some()));
        assert!(matches!(node.peer.read().await?, Frame::Amqp {
            channel: 1, performative: Some(Performative::Flow(flow)), ..
        } if flow.handle == Some(1) && flow.link_credit == Some(32)));
        let message = Message::data(b"supported after pipelined refusal".to_vec());
        node.peer
            .send(
                1,
                Performative::Transfer(first(1, 31)),
                encode_message(&message)?,
            )
            .await?;
        let delivery = timeout(IO_TIMEOUT, receiver.recv()).await??;
        assert_eq!(delivery.message(), &message);
        receiver.accept(&delivery).await?;
        node.peer.accepted(1, 31).await?;
        node.healthy(&mut healthy, 0, 17).await?;
        node.finish().await;
    }
    Ok(())
}

#[tokio::test]
async fn caller_mutated_recovery_state_is_refused_locally_without_consuming_pending_approval()
-> TestResult {
    let mut node = ServerPeer::new().await?;
    let mut session = node.begin().await?;
    let request = node
        .request_attach(&mut session, attach(0, Role::Sender))
        .await?;
    for (nonempty, incomplete) in [(true, false), (false, true)] {
        let mut changed = request.clone();
        retained(&mut changed, nonempty, incomplete);
        assert!(matches!(
            timeout(IO_TIMEOUT, session.accept_attach(changed, 4 * 1024 * 1024)).await?,
            Err(EngineError::InvalidState(_))
        ));
        assert_only_flows(&node.peer.barrier(0).await?);
    }
    let mut receiver = node.approve(&session, request).await?;
    node.healthy(&mut receiver, 0, 8).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn fresh_resumed_transfers_refuse_before_identity_payload_or_abort_handling() -> TestResult {
    for case in 0..3 {
        let mut node = ServerPeer::new().await?;
        let mut session = node.begin().await?;
        let mut affected = node.receiver(&mut session, 0).await?;
        let mut sibling = node.receiver(&mut session, 1).await?;
        let mut transfer = if case == 0 {
            continuation(0)
        } else {
            first(0, 10)
        };
        transfer.resume = true;
        transfer.aborted = case == 2;
        transfer.more = case == 2;
        node.peer
            .send(0, Performative::Transfer(transfer), vec![255; 20])
            .await?;
        node.peer.refused_detach(0, 0).await?;
        assert!(matches!(
            timeout(IO_TIMEOUT, affected.recv()).await?,
            Err(EngineError::RemoteDetached)
        ));
        // Also checks the session's next-incoming frame count advanced normally.
        assert_only_flows(&node.peer.barrier(0).await?);
        node.healthy(&mut sibling, 1, 10).await?;
        node.finish().await;
    }
    Ok(())
}

#[tokio::test]
async fn resumed_continuation_cleans_its_partial_aliases_and_keeps_sibling_messages_settleable()
-> TestResult {
    let mut node = ServerPeer::new().await?;
    let mut session = node.begin().await?;
    let mut affected = node.receiver(&mut session, 0).await?;
    let mut sibling = node.receiver(&mut session, 1).await?;
    let bytes = encode_message(&Message::data(vec![1, 2, 3]))?;
    let mut initial = first(0, 42);
    initial.more = true;
    node.peer
        .send(0, Performative::Transfer(initial), bytes[..2].to_vec())
        .await?;
    node.peer
        .send(0, Performative::Transfer(first(1, 43)), bytes.clone())
        .await?;
    let held = timeout(IO_TIMEOUT, sibling.recv()).await??;
    let mut resumed = first(0, 99);
    resumed.delivery_tag = Some(vec![99].into());
    resumed.message_format = Some(u32::MAX);
    resumed.resume = true;
    resumed.aborted = true;
    resumed.more = true;
    node.peer
        .send(0, Performative::Transfer(resumed), vec![255; 20])
        .await?;
    node.peer.refused_detach(0, 0).await?;
    assert!(matches!(
        timeout(IO_TIMEOUT, affected.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    sibling.accept(&held).await?;
    node.peer.accepted(0, 43).await?;
    node.healthy(&mut sibling, 1, 42).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn resumed_transfer_takes_precedence_over_exhausted_link_slots_and_retires_queued_receipts()
-> TestResult {
    let mut node = ServerPeer::new().await?;
    let mut session = node.begin().await?;
    let mut affected = node.receiver(&mut session, 0).await?;
    let mut sibling = node.receiver(&mut session, 1).await?;
    for id in 0..32 {
        node.peer
            .send(
                0,
                Performative::Transfer(first(0, id)),
                encode_message(&Message::data(vec![id as u8]))?,
            )
            .await?;
    }
    assert_only_flows(&node.peer.barrier(0).await?);
    let mut resumed = continuation(0);
    resumed.resume = true;
    node.peer
        .send(0, Performative::Transfer(resumed), vec![255; 20])
        .await?;
    node.peer.refused_detach(0, 0).await?;
    let stale = timeout(IO_TIMEOUT, affected.recv()).await??;
    assert!(matches!(
        affected.accept(&stale).await,
        Err(EngineError::InvalidState(_))
    ));
    assert_only_flows(&node.peer.barrier(0).await?);
    node.healthy(&mut sibling, 1, 0).await?;
    node.finish().await;
    Ok(())
}

async fn client_peer() -> TestResult<(ClientConnection, Peer)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let stream = TcpStream::connect(listener.local_addr()?).await?;
    stream.set_nodelay(true)?;
    let (mut socket, _) = listener.accept().await?;
    socket.set_nodelay(true)?;
    let opening = tokio::spawn(
        ClientConnection::builder()
            .container_id("recovery-client")
            .idle_timeout_millis(0)
            .open_with_stream(stream),
    );
    assert_eq!(
        timeout(IO_TIMEOUT, read_protocol_header(&mut socket)).await??,
        ProtocolHeader::AMQP
    );
    write_protocol_header(&mut socket, ProtocolHeader::AMQP).await?;
    assert!(matches!(
        timeout(IO_TIMEOUT, read_frame(&mut socket)).await??,
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(_)),
            ..
        }
    ));
    write_frame(
        &mut socket,
        &Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(Open::new("raw-recovery-server"))),
            payload: Vec::new(),
        },
    )
    .await?;
    Ok((
        timeout(IO_TIMEOUT, opening).await???,
        Peer {
            stream: socket,
            transfers: HashMap::new(),
        },
    ))
}

async fn client_session(
    connection: &mut ClientConnection,
    peer: &mut Peer,
) -> TestResult<ClientSession> {
    let (session, ()) = timeout(IO_TIMEOUT, async {
        tokio::try_join!(
            async { Ok::<_, Box<dyn Error>>(connection.begin().await?) },
            async {
                let Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Begin(_)),
                    ..
                } = peer.read().await?
                else {
                    panic!("client must begin its session");
                };
                peer.send(
                    0,
                    Performative::Begin(Begin {
                        remote_channel: Some(0),
                        ..Begin::default()
                    }),
                    Vec::new(),
                )
                .await
            }
        )
    })
    .await??;
    Ok(session)
}

async fn client_attach_reply(peer: &mut Peer, nonempty: bool, incomplete: bool) -> TestResult<u32> {
    let Frame::Amqp {
        channel: 0,
        performative: Some(Performative::Attach(request)),
        ..
    } = peer.read().await?
    else {
        panic!("client must request receiver attachment");
    };
    assert_eq!(request.role, Role::Receiver);
    let handle = request.handle;
    let mut reply = request.response(request.source.clone(), request.target.clone());
    retained(&mut reply, nonempty, incomplete);
    peer.send(0, Performative::Attach(Box::new(reply)), Vec::new())
        .await?;
    Ok(handle)
}

#[tokio::test]
async fn public_client_refuses_retained_or_incomplete_echo_before_endpoint_credit_then_retries()
-> TestResult {
    for (nonempty, incomplete) in [(true, false), (false, true), (true, true)] {
        let (mut connection, mut peer) = client_peer().await?;
        let mut session = client_session(&mut connection, &mut peer).await?;
        let (result, peer_result) = timeout(IO_TIMEOUT, async {
            tokio::join!(
                ClientReceiver::builder()
                    .name("unsupported")
                    .source(Source::new("queue"))
                    .attach(&mut session),
                async {
                    let handle = client_attach_reply(&mut peer, nonempty, incomplete).await?;
                    // Any initial link credit here would mean the endpoint was installed.
                    let Frame::Amqp {
                        channel: 0,
                        performative: Some(Performative::Detach(detach)),
                        ..
                    } = peer.read().await?
                    else {
                        panic!("unsupported echo must detach without first granting credit");
                    };
                    assert_eq!(detach.handle, handle);
                    assert!(detach.closed);
                    assert_eq!(
                        detach
                            .error
                            .expect("explicit unsupported error")
                            .condition
                            .as_symbol(),
                        Symbol::from("amqp:not-implemented")
                    );
                    peer.send(
                        0,
                        Performative::Detach(Detach {
                            handle,
                            closed: true,
                            error: None,
                        }),
                        Vec::new(),
                    )
                    .await
                }
            )
        })
        .await?;
        assert!(matches!(result, Err(EngineError::RemoteDetached)));
        // The peer branch must independently finish even when attach returns an error.
        peer_result?;
        let (receiver, handle) = timeout(IO_TIMEOUT, async {
            tokio::try_join!(
                async {
                    Ok::<_, Box<dyn Error>>(
                        ClientReceiver::builder()
                            .name("complete-empty")
                            .source(Source::new("queue"))
                            .attach(&mut session)
                            .await?,
                    )
                },
                async {
                    let handle = client_attach_reply(&mut peer, false, false).await?;
                    let Frame::Amqp {
                        channel: 0,
                        performative: Some(Performative::Flow(flow)),
                        ..
                    } = peer.read().await?
                    else {
                        panic!("supported echo installs receiver credit");
                    };
                    assert_eq!(flow.handle, Some(handle));
                    assert_eq!(flow.link_credit, Some(32));
                    Ok::<_, Box<dyn Error>>(handle)
                }
            )
        })
        .await??;
        let mut receiver = receiver;
        let message = Message::data(b"healthy after recovery refusal".to_vec());
        peer.send(
            0,
            Performative::Transfer(first(handle, 91)),
            encode_message(&message)?,
        )
        .await?;
        let delivery = timeout(IO_TIMEOUT, receiver.recv()).await??;
        assert_eq!(delivery.message(), &message);
        receiver.accept(&delivery).await?;
        peer.accepted(0, 91).await?;
        connection.shutdown().await;
    }
    Ok(())
}
