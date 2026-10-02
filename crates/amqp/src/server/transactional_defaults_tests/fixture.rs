use std::future::Future;

use tokio::io::DuplexStream;

use super::*;

pub(super) const CHANNEL: u16 = 19;
pub(super) const HANDLE: u32 = 17;

pub(super) async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("bounded native default-profile operation")
}

#[derive(Clone, Copy)]
pub(super) enum Policy {
    Disabled,
    Posting,
    Work,
    Defaults,
}

pub(super) fn request(role: Role) -> Attach {
    Attach {
        name: "default-profile-link".to_owned(),
        handle: HANDLE,
        role: role.clone(),
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::Second,
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue").into()),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: (role == Role::Sender).then_some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

pub(super) fn coordinator_request(count: Option<u32>) -> Attach {
    let mut attach = request(Role::Sender);
    attach.initial_delivery_count = count;
    attach.source.as_mut().expect("source").distribution_mode = Some(Symbol::from("move"));
    attach.target = Some(Coordinator::default().into());
    attach
}

pub(super) struct Fixture {
    pub(super) connection: ServerConnection,
    io: DuplexStream,
    channels: HashMap<u16, u16>,
    handles: HashMap<u16, HashSet<u32>>,
    incoming: HashMap<u16, u32>,
    outgoing: HashMap<u16, u32>,
}

impl Fixture {
    pub(super) async fn new(policy: Policy) -> Self {
        bounded(async {
            let (wire, mut io) = tokio::io::duplex(65_536);
            let opening = async {
                write_protocol_header(&mut io, ProtocolHeader::AMQP)
                    .await
                    .expect("peer header");
                expect_header(&mut io, ProtocolHeader::AMQP)
                    .await
                    .expect("native header");
                let mut open = Open::new("native-default-profile-tests");
                open.channel_max = 7;
                write_amqp(&mut io, 0, Performative::Open(open), Vec::new())
                    .await
                    .expect("peer Open");
                assert!(matches!(
                    read_frame(&mut io).await.expect("native Open"),
                    Frame::Amqp {
                        performative: Some(Performative::Open(_)),
                        ..
                    }
                ));
                io
            };
            let accepting = async {
                let options = ConnectionOptions::default().idle_timeout_millis(0);
                match policy {
                    Policy::Disabled => {
                        ServerConnection::accept_with_options(wire, "defaults", None, options).await
                    }
                    Policy::Posting => {
                        ServerConnection::accept_with_transactional_ingress(
                            wire, "defaults", None, options,
                        )
                        .await
                    }
                    Policy::Work => {
                        ServerConnection::accept_with_transactional_work(
                            wire, "defaults", None, options,
                        )
                        .await
                    }
                    Policy::Defaults => {
                        ServerConnection::accept_with_transactional_work_defaults(
                            wire, "defaults", None, options,
                        )
                        .await
                    }
                }
            };
            let (connection, io) = tokio::join!(accepting, opening);
            Self {
                connection: connection.expect("native connection"),
                io,
                channels: HashMap::new(),
                handles: HashMap::new(),
                incoming: HashMap::new(),
                outgoing: HashMap::new(),
            }
        })
        .await
    }

    pub(super) fn local(&self) -> u16 {
        self.channels[&CHANNEL]
    }

    pub(super) async fn send(&mut self, performative: Performative, payload: Vec<u8>) {
        bounded(write_amqp(&mut self.io, CHANNEL, performative, payload))
            .await
            .expect("peer frame");
    }

    async fn frame(&mut self) -> Frame {
        let frame = bounded(read_frame(&mut self.io))
            .await
            .expect("native frame");
        if let Frame::Amqp {
            channel,
            performative: Some(Performative::Transfer(_)),
            ..
        } = &frame
        {
            *self.incoming.entry(*channel).or_default() += 1;
        }
        frame
    }

    fn valid_flow(&self, frame: &Frame) -> bool {
        matches!(frame, Frame::Amqp { channel, performative: Some(Performative::Flow(flow)), payload }
            if payload.is_empty() && self.channels.values().any(|known| known == channel)
                && flow.handle.is_none_or(|handle| self.handles.get(channel).is_some_and(|known| known.contains(&handle))))
    }

    pub(super) async fn control(&mut self) -> (u16, Performative) {
        for _ in 0..16 {
            let frame = self.frame().await;
            if self.valid_flow(&frame) {
                continue;
            }
            let Frame::Amqp {
                channel,
                performative: Some(performative),
                payload,
            } = frame
            else {
                panic!("native control frame");
            };
            assert!(payload.is_empty());
            return (channel, performative);
        }
        panic!("bounded interleaved Flow count");
    }

    pub(super) async fn session(&mut self) -> ServerSession {
        self.send(
            Performative::Begin(Begin {
                handle_max: 7,
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await;
        let incoming = bounded(self.connection.next_incoming_session())
            .await
            .expect("incoming Begin");
        let (session, (channel, frame)) = bounded(async {
            tokio::join!(self.connection.accept_session(incoming), self.control())
        })
        .await;
        let session = session.expect("accepted session");
        let Performative::Begin(begin) = frame else {
            panic!("native Begin");
        };
        assert_eq!(begin.remote_channel, Some(CHANNEL));
        assert_eq!(channel, session.channel);
        assert_ne!(channel, CHANNEL);
        self.channels.insert(CHANNEL, channel);
        session
    }

    pub(super) async fn incoming(
        &mut self,
        session: &mut ServerSession,
        attach: Attach,
    ) -> IncomingAttach {
        self.send(Performative::Attach(Box::new(attach)), Vec::new())
            .await;
        bounded(session.next_incoming_attach())
            .await
            .expect("actor-approved Attach")
    }

    pub(super) async fn attached(&mut self) -> Attach {
        let (channel, frame) = self.control().await;
        assert_eq!(channel, self.local());
        let Performative::Attach(attach) = frame else {
            panic!("native Attach");
        };
        self.handles
            .entry(channel)
            .or_default()
            .insert(attach.handle);
        *attach
    }

    pub(super) async fn credit(&mut self, handle: u32) -> Flow {
        for _ in 0..16 {
            let frame = self.frame().await;
            match &frame {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } if *channel == self.local()
                    && flow.handle == Some(handle)
                    && payload.is_empty() =>
                {
                    assert!(flow.link_credit.is_some_and(|credit| credit > 0));
                    return flow.clone();
                }
                _ if self.valid_flow(&frame) => {}
                _ => panic!("native receive credit"),
            }
        }
        panic!("bounded receive credit count");
    }

    pub(super) async fn flow(&mut self, handle: Option<u32>, echo: bool) {
        self.send(
            Performative::Flow(Flow {
                next_incoming_id: Some(self.incoming.get(&self.local()).copied().unwrap_or(0)),
                incoming_window: 2048,
                next_outgoing_id: self.outgoing.get(&CHANNEL).copied().unwrap_or(0),
                outgoing_window: 2048,
                handle,
                delivery_count: handle.map(|_| 0),
                link_credit: handle.map(|_| 4),
                available: None,
                drain: false,
                echo,
                properties: None,
            }),
            Vec::new(),
        )
        .await;
    }

    pub(super) async fn barrier(&mut self) {
        self.flow(None, true).await;
        for _ in 0..16 {
            let frame = self.frame().await;
            if matches!(&frame, Frame::Amqp { channel, performative: Some(Performative::Flow(Flow { handle: None, .. })), payload }
                if *channel == self.local() && payload.is_empty())
            {
                return;
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected frame before session barrier"
            );
        }
        panic!("bounded barrier Flow count");
    }

    pub(super) async fn outgoing(&mut self, handle: u32, expected: &Message) -> u32 {
        for _ in 0..16 {
            let frame = self.frame().await;
            if self.valid_flow(&frame) {
                continue;
            }
            let Frame::Amqp {
                channel,
                performative: Some(Performative::Transfer(transfer)),
                payload,
            } = frame
            else {
                panic!("actual original Transfer");
            };
            assert_eq!(channel, self.local());
            assert_eq!(transfer.handle, handle);
            assert_eq!(transfer.settled, Some(false));
            assert_eq!(
                transfer.delivery_tag,
                Some(b"negotiated-original".to_vec().into())
            );
            assert!(transfer.state.is_none());
            assert!(!transfer.more);
            assert_eq!(decode_message(&payload).expect("native body"), *expected);
            return transfer.delivery_id.expect("original ID");
        }
        panic!("bounded original Transfer count");
    }

    pub(super) async fn disposition(&mut self, id: u32, state: DeliveryState, settled: bool) {
        self.send(
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled,
                state: Some(state),
                batchable: false,
            }),
            Vec::new(),
        )
        .await;
    }

    pub(super) async fn declare(&mut self, coordinator: &mut CoordinatorEndpoint) {
        let message = Message {
            body: Body::Value(TransactionCommand::Declare(Declare { global_id: None }).into()),
            ..Message::default()
        };
        self.send(
            Performative::Transfer(Transfer {
                handle: HANDLE,
                delivery_id: Some(0),
                delivery_tag: Some(b"declare".to_vec().into()),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            encode_message(&message).expect("Declare body"),
        )
        .await;
        *self.outgoing.entry(CHANNEL).or_default() += 1;
        let CoordinatorRequest::Declare(receipt) =
            bounded(coordinator.recv()).await.expect("Declare receipt")
        else {
            panic!("actual Declare receipt");
        };
        let id = TransactionId::new([42]).expect("bounded transaction ID");
        let (identity, (channel, frame)) =
            bounded(async { tokio::join!(receipt.declared(id.clone()), self.control()) }).await;
        assert_eq!(
            identity.expect("declared identity").state(),
            NativeTransactionState::Pending
        );
        assert_eq!(channel, self.local());
        let Performative::Disposition(disposition) = frame else {
            panic!("Declared response");
        };
        assert_eq!(disposition.role, Role::Receiver);
        assert_eq!(disposition.first, 0);
        assert!(
            matches!(disposition.state, Some(DeliveryState::Declared(declared)) if declared.txn_id == id)
        );
    }

    pub(super) async fn shutdown(&self) {
        bounded(self.connection.shutdown()).await;
    }
}
