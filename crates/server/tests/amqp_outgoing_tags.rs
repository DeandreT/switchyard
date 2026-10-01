//! Outgoing tag ownership and settlement through actual TCP connections.

use std::{error::Error as StdError, time::Duration};

use amqp::{
    Accepted, Attach, Begin, ConnectionOptions, DeliveryState, Disposition, EngineError, Flow,
    Frame, LinkEndpoint, Message, Open, Outcome, PendingSettlement, Performative, ProtocolHeader,
    ReceiverSettleMode, Role, Sender, SenderSettleMode, ServerConnection, ServerSession, Source,
    Target, decode_message, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn StdError>>;
const DEADLINE: Duration = Duration::from_secs(10);
const WINDOW: u32 = 2_048;

struct Node {
    connection: ServerConnection,
    peer: TcpStream,
    received_transfers: u32,
}

impl Node {
    async fn new() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let mut peer = timeout(DEADLINE, TcpStream::connect(address)).await??;
        peer.set_nodelay(true)?;
        let (socket, _) = timeout(DEADLINE, listener.accept()).await??;
        socket.set_nodelay(true)?;
        let accepting = tokio::spawn(ServerConnection::accept_with_options(
            socket,
            format!("tag-owner-{}", address.port()),
            None,
            ConnectionOptions::default().idle_timeout_millis(0),
        ));
        timeout(
            DEADLINE,
            write_protocol_header(&mut peer, ProtocolHeader::AMQP),
        )
        .await??;
        assert_eq!(
            timeout(DEADLINE, read_protocol_header(&mut peer)).await??,
            ProtocolHeader::AMQP
        );
        timeout(
            DEADLINE,
            write_frame(
                &mut peer,
                &Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Open(Open {
                        max_frame_size: 512,
                        idle_time_out: Some(0),
                        ..Open::new(format!("tag-peer-{}", address.port()))
                    })),
                    payload: Vec::new(),
                },
            ),
        )
        .await??;
        assert!(matches!(
            timeout(DEADLINE, read_frame(&mut peer)).await??,
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        Ok(Self {
            connection: timeout(DEADLINE, accepting).await???,
            peer,
            received_transfers: 0,
        })
    }

    async fn send(&mut self, performative: Performative) -> TestResult {
        timeout(
            DEADLINE,
            write_frame(
                &mut self.peer,
                &Frame::Amqp {
                    channel: 0,
                    performative: Some(performative),
                    payload: Vec::new(),
                },
            ),
        )
        .await??;
        Ok(())
    }

    async fn read(&mut self) -> TestResult<Frame> {
        let frame = timeout(DEADLINE, read_frame(&mut self.peer)).await??;
        if matches!(
            &frame,
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Transfer(_)),
                ..
            }
        ) {
            self.received_transfers = self.received_transfers.wrapping_add(1);
        }
        Ok(frame)
    }

    async fn control(&mut self) -> TestResult<Performative> {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                Frame::Amqp {
                    channel: 0,
                    performative: Some(performative),
                    payload,
                } => {
                    assert!(payload.is_empty());
                    return Ok(performative);
                }
                other => panic!("control frame expected: {other:?}"),
            }
        }
    }

    fn flow(&self) -> Flow {
        Flow {
            next_incoming_id: Some(self.received_transfers),
            incoming_window: WINDOW,
            next_outgoing_id: 0,
            outgoing_window: WINDOW,
            ..Flow::default()
        }
    }

    async fn barrier(&mut self) -> TestResult {
        let count = self.received_transfers;
        let mut flow = self.flow();
        flow.echo = true;
        self.send(Performative::Flow(flow)).await?;
        loop {
            let frame = self.read().await?;
            let Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Flow(flow)),
                payload,
            } = frame
            else {
                panic!("tag probe must not emit Transfer, Disposition, or refusal: {frame:?}");
            };
            assert!(payload.is_empty());
            assert!(flow.handle.is_none());
            if flow.next_incoming_id == Some(0) && flow.next_outgoing_id == count {
                return Ok(());
            }
        }
    }

    async fn begin(&mut self) -> TestResult<ServerSession> {
        self.send(Performative::Begin(Begin::default())).await?;
        let incoming = timeout(DEADLINE, self.connection.next_incoming_session())
            .await?
            .expect("incoming session");
        let session = timeout(DEADLINE, self.connection.accept_session(incoming)).await??;
        let Performative::Begin(begin) = self.control().await? else {
            panic!("Begin response");
        };
        assert_eq!(begin.remote_channel, Some(0));
        Ok(session)
    }

    async fn sender(&mut self, session: &mut ServerSession, handle: u32) -> TestResult<Sender> {
        self.send(Performative::Attach(Box::new(Attach {
            name: format!("sender-{handle}"),
            handle,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::Second,
            source: Some(Source::new("queue")),
            target: Some(Target::new("queue")),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: Some(4 * 1024 * 1024),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        })))
        .await?;
        let incoming = timeout(DEADLINE, session.next_incoming_attach())
            .await?
            .expect("incoming receiver");
        let endpoint = timeout(DEADLINE, session.accept_attach(incoming, 256 * 1024)).await??;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("sending endpoint");
        };
        let Performative::Attach(response) = self.control().await? else {
            panic!("Attach response");
        };
        assert_eq!(response.handle, handle);
        assert_eq!(response.role, Role::Sender);
        let mut flow = self.flow();
        flow.handle = Some(handle);
        flow.delivery_count = Some(0);
        flow.link_credit = Some(16);
        self.send(Performative::Flow(flow)).await?;
        self.barrier().await?;
        Ok(sender)
    }

    async fn disposition(&mut self, id: u32, settled: bool) -> TestResult {
        self.send(Performative::Disposition(Disposition {
            role: Role::Receiver,
            first: id,
            last: None,
            settled,
            state: (!settled).then_some(DeliveryState::Accepted(Accepted)),
            batchable: false,
        }))
        .await
    }

    async fn pending(
        &mut self,
        sender: &mut Sender,
        handle: u32,
        tag: &[u8],
    ) -> TestResult<(PendingSettlement, u32)> {
        let message = Message::data(b"tagged delivery".to_vec());
        let (result, id) = timeout(DEADLINE, async {
            tokio::join!(
                sender.send_with_settlement(message.clone(), tag.to_vec().into()),
                async {
                    let mut bytes = Vec::new();
                    let mut first_id = None;
                    loop {
                        let frame = self.read().await?;
                        let Frame::Amqp {
                            channel: 0,
                            performative: Some(Performative::Transfer(transfer)),
                            payload,
                        } = frame
                        else {
                            panic!("tagged Transfer expected: {frame:?}");
                        };
                        assert_eq!(transfer.handle, handle);
                        if first_id.is_none() {
                            first_id = transfer.delivery_id;
                            assert_eq!(
                                transfer.delivery_tag.as_ref().map(|value| value.as_ref()),
                                Some(tag)
                            );
                            assert_eq!(transfer.message_format, Some(0));
                            assert_ne!(transfer.settled, Some(true));
                        } else {
                            assert!(transfer.delivery_id.is_none());
                            assert!(transfer.delivery_tag.is_none());
                        }
                        bytes.extend(payload);
                        if !transfer.more {
                            break;
                        }
                    }
                    assert_eq!(decode_message(&bytes)?, message);
                    let id = first_id.expect("first delivery ID");
                    self.disposition(id, false).await?;
                    Ok::<_, Box<dyn StdError>>(id)
                }
            )
        })
        .await?;
        let receipt = result?;
        assert_eq!(receipt.outcome(), &Outcome::Accepted(Accepted));
        Ok((receipt, id?))
    }

    async fn duplicate(&mut self, sender: &mut Sender, tag: &[u8]) -> TestResult {
        assert!(matches!(
            timeout(
                DEADLINE,
                sender.send_with_settlement(
                    Message::data(b"duplicate".to_vec()),
                    tag.to_vec().into(),
                )
            )
            .await?,
            Err(EngineError::InvalidState(_))
        ));
        self.barrier().await
    }

    async fn accept(&mut self, receipt: &PendingSettlement, id: u32) -> TestResult {
        timeout(DEADLINE, receipt.accept()).await??;
        let Performative::Disposition(disposition) = self.control().await? else {
            panic!("one Sender ACK");
        };
        assert_eq!(disposition.role, Role::Sender);
        assert_eq!(disposition.first, id);
        assert_eq!(disposition.last, None);
        assert!(disposition.settled);
        assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
        self.barrier().await
    }

    async fn finish(self) -> TestResult {
        timeout(DEADLINE, self.connection.shutdown()).await?;
        Ok(())
    }
}

#[tokio::test]
async fn duplicate_tag_waits_for_local_ack_and_old_terminal_receipt_cannot_release_a_reuse()
-> TestResult {
    let mut node = Node::new().await?;
    let mut session = node.begin().await?;
    let mut sender = node.sender(&mut session, 0).await?;
    let tag = b"same-tag";
    let (old, old_id) = node.pending(&mut sender, 0, tag).await?;
    node.duplicate(&mut sender, tag).await?;
    node.accept(&old, old_id).await?;
    let (fresh, fresh_id) = node.pending(&mut sender, 0, tag).await?;
    assert_eq!(fresh_id, old_id.wrapping_add(1));
    timeout(DEADLINE, old.accept()).await??;
    node.barrier().await?;
    node.duplicate(&mut sender, tag).await?;
    node.accept(&fresh, fresh_id).await?;
    let (last, last_id) = node.pending(&mut sender, 0, tag).await?;
    node.accept(&last, last_id).await?;
    node.finish().await
}

#[tokio::test]
async fn remote_settlement_releases_empty_and_maximum_tags_without_obsolete_receipt_side_effects()
-> TestResult {
    for tag in [Vec::new(), vec![7; 32]] {
        let mut node = Node::new().await?;
        let mut session = node.begin().await?;
        let mut sender = node.sender(&mut session, 0).await?;
        let (old, old_id) = node.pending(&mut sender, 0, &tag).await?;
        node.duplicate(&mut sender, &tag).await?;
        node.disposition(old_id, true).await?;
        node.barrier().await?;
        let (fresh, fresh_id) = node.pending(&mut sender, 0, &tag).await?;
        timeout(DEADLINE, old.accept()).await??;
        node.barrier().await?;
        node.duplicate(&mut sender, &tag).await?;
        node.accept(&fresh, fresh_id).await?;
        node.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn identical_outstanding_tags_on_distinct_links_are_independent() -> TestResult {
    let mut node = Node::new().await?;
    let mut session = node.begin().await?;
    let mut first = node.sender(&mut session, 0).await?;
    let mut second = node.sender(&mut session, 1).await?;
    let tag = b"shared-tag";
    let (a, a_id) = node.pending(&mut first, 0, tag).await?;
    let (b, b_id) = node.pending(&mut second, 1, tag).await?;
    assert_ne!(a_id, b_id);
    node.duplicate(&mut first, tag).await?;
    node.duplicate(&mut second, tag).await?;
    node.accept(&a, a_id).await?;
    let (a_next, a_next_id) = node.pending(&mut first, 0, tag).await?;
    node.duplicate(&mut second, tag).await?;
    node.accept(&b, b_id).await?;
    node.duplicate(&mut first, tag).await?;
    node.accept(&a_next, a_next_id).await?;
    node.finish().await
}
