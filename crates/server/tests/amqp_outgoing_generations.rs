//! Outgoing ownership survives numeric link, channel, and delivery-ID reuse.

use std::{error::Error, time::Duration};

use amqp::{
    Accepted, Attach, Begin, Close, ConnectionOptions, DeliveryState, Detach, Disposition, End,
    EngineError, Flow, Frame, LinkEndpoint, Message, Open, PendingSettlement, Performative,
    ProtocolHeader, Receiver, ReceiverSettleMode, Role, Sender, SenderSettleMode, ServerConnection,
    ServerSession, Source, Target, Transfer, decode_message, encode_message, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(10);
const HANDLE: u32 = 7;

struct Peer {
    stream: TcpStream,
    sent_transfers: u32,
    received_transfers: u32,
}

impl Peer {
    async fn send(&mut self, performative: Performative, payload: Vec<u8>) -> TestResult {
        if matches!(&performative, Performative::Begin(_)) {
            self.sent_transfers = 0;
            self.received_transfers = 0;
        } else if matches!(&performative, Performative::Transfer(_)) {
            self.sent_transfers = self.sent_transfers.wrapping_add(1);
        }
        timeout(
            DEADLINE,
            write_frame(
                &mut self.stream,
                &Frame::Amqp {
                    channel: 0,
                    performative: Some(performative),
                    payload,
                },
            ),
        )
        .await??;
        Ok(())
    }

    async fn read(&mut self) -> TestResult<Frame> {
        let frame = timeout(DEADLINE, read_frame(&mut self.stream)).await??;
        if matches!(
            &frame,
            Frame::Amqp {
                performative: Some(Performative::Transfer(_)),
                ..
            }
        ) {
            self.received_transfers = self.received_transfers.wrapping_add(1);
        }
        Ok(frame)
    }

    fn flow(&self, handle: Option<u32>, echo: bool) -> Flow {
        Flow {
            next_incoming_id: Some(self.received_transfers),
            incoming_window: 2_048,
            next_outgoing_id: self.sent_transfers,
            outgoing_window: 2_048,
            handle,
            delivery_count: handle.map(|_| 0),
            link_credit: handle.map(|_| 4),
            echo,
            ..Flow::default()
        }
    }

    async fn barrier(&mut self) -> TestResult<Vec<Frame>> {
        self.send(Performative::Flow(self.flow(None, true)), Vec::new())
            .await?;
        let mut frames = Vec::new();
        loop {
            let frame = self.read().await?;
            let echoed = matches!(&frame, Frame::Amqp {
                channel: 0, performative: Some(Performative::Flow(flow)), ..
            } if flow.handle.is_none() && flow.next_incoming_id == Some(self.sent_transfers));
            assert!(
                !matches!(
                    &frame,
                    Frame::Amqp {
                        performative: Some(Performative::End(_) | Performative::Close(_)),
                        ..
                    }
                ),
                "stale local calls must preserve the connection: {frame:?}"
            );
            frames.push(frame);
            if echoed {
                return Ok(frames);
            }
        }
    }

    async fn disposition(&mut self, role: Role, id: u32) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Disposition(value)),
                    payload,
                } => {
                    assert!(payload.is_empty());
                    assert_eq!(value.role, role);
                    assert_eq!(value.first, id);
                    assert_eq!(value.last, None);
                    assert!(value.settled);
                    assert_eq!(value.state, Some(DeliveryState::Accepted(Accepted)));
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("owned disposition expected: {other:?}"),
            }
        }
    }

    async fn receive_and_accept(&mut self, expected: &Message) -> TestResult<u32> {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Transfer(value)),
                    payload,
                } => {
                    assert_eq!(value.handle, HANDLE);
                    assert!(!value.more);
                    assert!(!value.aborted);
                    assert_eq!(value.settled, Some(false));
                    assert_eq!(decode_message(&payload)?, *expected);
                    let id = value.delivery_id.expect("first outgoing delivery ID");
                    self.send(
                        Performative::Disposition(Disposition {
                            role: Role::Receiver,
                            first: id,
                            last: None,
                            settled: false,
                            state: Some(DeliveryState::Accepted(Accepted)),
                            batchable: false,
                        }),
                        Vec::new(),
                    )
                    .await?;
                    return Ok(id);
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("fresh outgoing transfer expected: {other:?}"),
            }
        }
    }
}

struct Node {
    connection: ServerConnection,
    peer: Peer,
}

impl Node {
    async fn new() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut stream = timeout(DEADLINE, TcpStream::connect(listener.local_addr()?)).await??;
        stream.set_nodelay(true)?;
        let (socket, _) = timeout(DEADLINE, listener.accept()).await??;
        socket.set_nodelay(true)?;
        let accepting = tokio::spawn(ServerConnection::accept_with_options(
            socket,
            "outgoing-generation-server",
            None,
            ConnectionOptions::default().idle_timeout_millis(0),
        ));
        timeout(
            DEADLINE,
            write_protocol_header(&mut stream, ProtocolHeader::AMQP),
        )
        .await??;
        assert_eq!(
            timeout(DEADLINE, read_protocol_header(&mut stream)).await??,
            ProtocolHeader::AMQP
        );
        let mut peer = Peer {
            stream,
            sent_transfers: 0,
            received_transfers: 0,
        };
        peer.send(
            Performative::Open(Open {
                max_frame_size: 512,
                ..Open::new("outgoing-generation-peer")
            }),
            Vec::new(),
        )
        .await?;
        assert!(matches!(
            peer.read().await?,
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        Ok(Self {
            connection: timeout(DEADLINE, accepting).await???,
            peer,
        })
    }

    async fn begin(&mut self) -> TestResult<ServerSession> {
        self.peer
            .send(Performative::Begin(Begin::default()), Vec::new())
            .await?;
        let incoming = timeout(DEADLINE, self.connection.next_incoming_session())
            .await?
            .expect("incoming Begin");
        let session = timeout(DEADLINE, self.connection.accept_session(incoming)).await??;
        assert!(matches!(self.peer.read().await?, Frame::Amqp {
            channel: 0, performative: Some(Performative::Begin(begin)), ..
        } if begin.remote_channel == Some(0)));
        Ok(session)
    }

    async fn attach(
        &mut self,
        session: &mut ServerSession,
        role: Role,
        name: &str,
    ) -> TestResult<LinkEndpoint> {
        self.peer
            .send(
                Performative::Attach(Box::new(Attach {
                    name: name.to_owned(),
                    handle: HANDLE,
                    role: role.clone(),
                    snd_settle_mode: SenderSettleMode::Unsettled,
                    rcv_settle_mode: if role == Role::Receiver {
                        ReceiverSettleMode::Second
                    } else {
                        ReceiverSettleMode::First
                    },
                    source: Some(Source::new("orders")),
                    target: Some(Target::new("orders")),
                    unsettled: None,
                    incomplete_unsettled: false,
                    initial_delivery_count: (role == Role::Sender).then_some(0),
                    max_message_size: None,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                })),
                Vec::new(),
            )
            .await?;
        let incoming = timeout(DEADLINE, session.next_incoming_attach())
            .await?
            .expect("fresh incoming Attach");
        assert_eq!(incoming.name, name);
        let endpoint = timeout(DEADLINE, session.accept_attach(incoming, 256 * 1024)).await??;
        assert!(matches!(self.peer.read().await?, Frame::Amqp {
            channel: 0, performative: Some(Performative::Attach(value)), ..
        } if value.handle == HANDLE && value.role == role.opposite()));
        if role == Role::Sender {
            assert!(matches!(self.peer.read().await?, Frame::Amqp {
                channel: 0, performative: Some(Performative::Flow(flow)), ..
            } if flow.handle == Some(HANDLE) && flow.link_credit == Some(32)));
        } else {
            self.peer
                .send(
                    Performative::Flow(self.peer.flow(Some(HANDLE), false)),
                    Vec::new(),
                )
                .await?;
            assert_flow_only(&self.peer.barrier().await?);
        }
        Ok(endpoint)
    }

    async fn detach(&mut self) -> TestResult {
        self.peer
            .send(
                Performative::Detach(Detach {
                    handle: HANDLE,
                    closed: true,
                    error: None,
                }),
                Vec::new(),
            )
            .await?;
        loop {
            match self.peer.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Detach(value)),
                    ..
                } => {
                    assert_eq!(value.handle, HANDLE);
                    assert!(value.closed);
                    assert!(value.error.is_none());
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("peer Detach must receive one acknowledgement: {other:?}"),
            }
        }
    }

    async fn end(&mut self) -> TestResult {
        self.peer
            .send(Performative::End(End::default()), Vec::new())
            .await?;
        loop {
            match self.peer.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::End(value)),
                    ..
                } => {
                    assert!(value.error.is_none());
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("peer End must receive one acknowledgement: {other:?}"),
            }
        }
    }

    async fn send(&mut self, sender: &mut Sender, tag: u8) -> TestResult<(PendingSettlement, u32)> {
        let message = Message::data(vec![tag]);
        let (settlement, id) = timeout(DEADLINE, async {
            tokio::join!(
                sender.send_with_settlement(message.clone(), vec![tag].into()),
                self.peer.receive_and_accept(&message)
            )
        })
        .await?;
        let settlement = settlement?;
        assert!(matches!(settlement.outcome(), amqp::Outcome::Accepted(_)));
        Ok((settlement, id?))
    }

    async fn receive(&mut self, receiver: &mut Receiver) -> TestResult {
        let message = Message::data(b"replacement remains healthy".to_vec());
        self.peer
            .send(
                Performative::Transfer(Transfer {
                    handle: HANDLE,
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
                encode_message(&message)?,
            )
            .await?;
        let delivery = timeout(DEADLINE, receiver.recv()).await??;
        assert_eq!(delivery.message(), &message);
        timeout(DEADLINE, receiver.accept(&delivery)).await??;
        self.peer.disposition(Role::Receiver, 0).await
    }

    async fn finish(mut self) -> TestResult {
        let (closed, peer) = timeout(DEADLINE, async {
            tokio::join!(self.connection.close(), async {
                loop {
                    match self.peer.read().await? {
                        Frame::Amqp {
                            performative: Some(Performative::Close(_)),
                            ..
                        } => {
                            self.peer
                                .send(Performative::Close(Close::default()), Vec::new())
                                .await?;
                            return Ok::<(), Box<dyn Error>>(());
                        }
                        Frame::Amqp {
                            performative: Some(Performative::Flow(_)),
                            ..
                        } => {}
                        other => panic!("clean connection Close expected: {other:?}"),
                    }
                }
            })
        })
        .await?;
        closed?;
        peer?;
        Ok(())
    }
}

fn assert_flow_only(frames: &[Frame]) {
    assert!(!frames.is_empty());
    assert!(
        frames.iter().all(|frame| matches!(
            frame,
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                ..
            }
        )),
        "stale calls must not emit Transfer, Detach, Attach, or Disposition: {frames:?}"
    );
}

#[tokio::test]
async fn detached_server_sender_cannot_send_or_close_either_replacement_role() -> TestResult {
    for role in [Role::Receiver, Role::Sender] {
        let mut node = Node::new().await?;
        let mut session = node.begin().await?;
        let LinkEndpoint::Sender(mut old) = node
            .attach(&mut session, Role::Receiver, "old-sender")
            .await?
        else {
            panic!("server Sender");
        };
        node.detach().await?;
        timeout(DEADLINE, old.on_detach()).await?;
        let replacement = node
            .attach(&mut session, role.clone(), "replacement")
            .await?;
        assert!(matches!(
            timeout(DEADLINE, old.send(Message::data(vec![1]), vec![1].into())).await?,
            Err(EngineError::RemoteDetached)
        ));
        timeout(DEADLINE, old.close()).await??;
        assert_flow_only(&node.peer.barrier().await?);
        match replacement {
            LinkEndpoint::Sender(mut sender) => {
                let (settlement, id) = node.send(&mut sender, 2).await?;
                assert_eq!(id, 0);
                timeout(DEADLINE, settlement.accept()).await??;
                node.peer.disposition(Role::Sender, id).await?;
            }
            LinkEndpoint::Receiver(mut receiver) => node.receive(&mut receiver).await?,
        }
        node.detach().await?;
        assert_flow_only(&node.peer.barrier().await?);
        node.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn retained_pending_receipt_cannot_acknowledge_reused_channel_handle_and_id() -> TestResult {
    let mut node = Node::new().await?;
    let mut old_session = node.begin().await?;
    let LinkEndpoint::Sender(mut old_sender) = node
        .attach(&mut old_session, Role::Receiver, "old-session-sender")
        .await?
    else {
        panic!("original server Sender");
    };
    let (old_receipt, old_id) = node.send(&mut old_sender, 1).await?;
    assert_eq!(old_id, 0);
    assert_flow_only(&node.peer.barrier().await?);
    node.end().await?;
    timeout(DEADLINE, old_sender.on_detach()).await?;
    assert!(
        timeout(DEADLINE, old_session.next_incoming_attach())
            .await?
            .is_none()
    );

    let mut replacement_session = node.begin().await?;
    let LinkEndpoint::Sender(mut replacement) = node
        .attach(
            &mut replacement_session,
            Role::Receiver,
            "replacement-session-sender",
        )
        .await?
    else {
        panic!("replacement server Sender");
    };
    let (current_receipt, current_id) = node.send(&mut replacement, 2).await?;
    assert_eq!(
        current_id, old_id,
        "fresh session deliberately reuses delivery ID zero"
    );
    assert!(matches!(
        timeout(DEADLINE, old_receipt.accept()).await?,
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        timeout(
            DEADLINE,
            old_receipt.reject(amqp::Error::new(
                amqp::AmqpError::InternalError,
                "stale explicit settlement",
                None,
            ))
        )
        .await?,
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        timeout(
            DEADLINE,
            old_sender.send(Message::data(vec![3]), vec![3].into())
        )
        .await?,
        Err(EngineError::RemoteDetached)
    ));
    timeout(DEADLINE, old_sender.close()).await??;
    assert_flow_only(&node.peer.barrier().await?);

    timeout(DEADLINE, current_receipt.accept()).await??;
    node.peer.disposition(Role::Sender, current_id).await?;
    timeout(DEADLINE, current_receipt.accept()).await??;
    assert_flow_only(&node.peer.barrier().await?);
    let (next_receipt, next_id) = node.send(&mut replacement, 4).await?;
    assert_eq!(next_id, 1);
    timeout(DEADLINE, next_receipt.accept()).await??;
    node.peer.disposition(Role::Sender, next_id).await?;
    node.end().await?;
    node.finish().await?;
    Ok(())
}

#[tokio::test]
async fn oversized_borrowed_rejection_preserves_the_receipt_for_acceptance_and_terminal_repeat()
-> TestResult {
    let mut node = Node::new().await?;
    let mut session = node.begin().await?;
    let LinkEndpoint::Sender(mut sender) = node
        .attach(&mut session, Role::Receiver, "retry-sender")
        .await?
    else {
        panic!("server Sender");
    };
    let (receipt, id) = node.send(&mut sender, 1).await?;
    assert_eq!(id, 0);
    let oversized = || amqp::Error::new(amqp::AmqpError::InternalError, "x".repeat(1024), None);
    assert!(matches!(
        timeout(DEADLINE, receipt.reject(oversized())).await?,
        Err(EngineError::Io(_))
    ));
    assert!(matches!(receipt.outcome(), amqp::Outcome::Accepted(_)));
    assert_flow_only(&node.peer.barrier().await?);

    timeout(DEADLINE, receipt.accept()).await??;
    node.peer.disposition(Role::Sender, id).await?;
    timeout(DEADLINE, receipt.accept()).await??;
    timeout(DEADLINE, receipt.reject(oversized())).await??;
    assert_flow_only(&node.peer.barrier().await?);

    let (next, next_id) = node.send(&mut sender, 2).await?;
    assert_eq!(next_id, 1);
    timeout(DEADLINE, receipt.accept()).await??;
    assert_flow_only(&node.peer.barrier().await?);
    timeout(DEADLINE, next.accept()).await??;
    node.peer.disposition(Role::Sender, next_id).await?;
    node.detach().await?;
    node.finish().await?;
    Ok(())
}
