use std::future::Future;

use tokio::io::DuplexStream;

use super::flush_gate::{FlushGate, GatedIo};
use super::*;

pub(super) const DEADLINE: Duration = Duration::from_secs(3);
pub(super) const CONTROL: u16 = 19;
pub(super) const SEND: u16 = 23;
pub(super) const POST: u16 = 29;
pub(super) const HEALTHY: u16 = 37;
pub(super) const HANDLE: u32 = 17;
pub(super) const TAG: &[u8] = b"retirement-tag";

pub(super) async fn bounded<T>(name: &'static str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, future).await.expect(name)
}

pub(super) fn message() -> Message {
    Message {
        body: Body::Data(vec![b"retirement-body".to_vec().into()]),
        ..Message::default()
    }
}

pub(super) fn txn(byte: u8) -> TransactionId {
    TransactionId::new([byte]).expect("bounded transaction ID")
}

pub(super) fn attach(handle: u32, name: &str, role: Role) -> Attach {
    Attach {
        name: name.to_owned(),
        handle,
        role: role.clone(),
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: if role == Role::Receiver {
            ReceiverSettleMode::Second
        } else {
            ReceiverSettleMode::First
        },
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

pub(super) struct Peer {
    pub(super) io: DuplexStream,
    channels: HashMap<u16, u16>,
    handles: HashMap<u16, HashSet<u32>>,
    incoming: HashMap<u16, u32>,
    outgoing: HashMap<u16, u32>,
}

impl Peer {
    pub(super) fn local(&self, channel: u16) -> u16 {
        self.channels[&channel]
    }

    pub(super) async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) {
        bounded(
            "peer native frame write",
            write_amqp(&mut self.io, channel, performative, payload),
        )
        .await
        .expect("valid peer frame");
    }

    pub(super) async fn frame(&mut self) -> Frame {
        let frame = bounded("native socket frame response", read_frame(&mut self.io))
            .await
            .expect("valid native frame");
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

    pub(super) fn valid_flow(&self, frame: &Frame) -> bool {
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
            match frame {
                Frame::Amqp {
                    channel,
                    performative: Some(performative),
                    payload,
                } if payload.is_empty() => return (channel, performative),
                frame => panic!("unexpected native control frame: {frame:?}"),
            }
        }
        panic!("bounded control response count");
    }

    pub(super) async fn attached(&mut self, channel: u16) -> Attach {
        let (local, frame) = self.control().await;
        assert_eq!(local, self.local(channel));
        let Performative::Attach(attach) = frame else {
            panic!("exact Attach response");
        };
        self.handles.entry(local).or_default().insert(attach.handle);
        *attach
    }

    pub(super) async fn credit_response(&mut self, channel: u16, handle: u32) {
        for _ in 0..16 {
            let frame = self.frame().await;
            match &frame {
                Frame::Amqp {
                    channel: local,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } if *local == self.local(channel)
                    && flow.handle == Some(handle)
                    && payload.is_empty() =>
                {
                    assert!(flow.link_credit.is_some_and(|credit| credit > 0));
                    return;
                }
                _ if self.valid_flow(&frame) => {}
                _ => panic!("unexpected credit response: {frame:?}"),
            }
        }
        panic!("bounded credit response count");
    }

    pub(super) async fn grant(&mut self, channel: u16, handle: u32) {
        self.flow(channel, Some(handle), false).await;
        self.barrier(channel).await;
    }

    async fn flow(&mut self, channel: u16, handle: Option<u32>, echo: bool) {
        self.send(
            channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(
                    self.incoming
                        .get(&self.local(channel))
                        .copied()
                        .unwrap_or(0),
                ),
                incoming_window: 2_048,
                next_outgoing_id: self.outgoing.get(&channel).copied().unwrap_or(0),
                outgoing_window: 2_048,
                handle,
                delivery_count: handle.map(|_| 0),
                link_credit: handle.map(|_| 256),
                available: None,
                drain: false,
                echo,
                properties: None,
            }),
            Vec::new(),
        )
        .await;
    }

    pub(super) async fn barrier(&mut self, channel: u16) {
        self.flow(channel, None, true).await;
        for _ in 0..16 {
            let frame = self.frame().await;
            match &frame {
                Frame::Amqp {
                    channel: local,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } if *local == self.local(channel)
                    && flow.handle.is_none()
                    && payload.is_empty() =>
                {
                    return;
                }
                _ if self.valid_flow(&frame) => {}
                _ => panic!("unexpected frame before session barrier: {frame:?}"),
            }
        }
        panic!("bounded session barrier count");
    }

    pub(super) async fn one_frame_window(&mut self, channel: u16) {
        self.send(
            channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(
                    self.incoming
                        .get(&self.local(channel))
                        .copied()
                        .unwrap_or(0),
                ),
                incoming_window: 1,
                next_outgoing_id: self.outgoing.get(&channel).copied().unwrap_or(0),
                outgoing_window: 2_048,
                handle: None,
                delivery_count: None,
                link_credit: None,
                available: None,
                drain: false,
                echo: true,
                properties: None,
            }),
            Vec::new(),
        )
        .await;
        for _ in 0..16 {
            let frame = self.frame().await;
            if matches!(&frame, Frame::Amqp { channel: local, performative: Some(Performative::Flow(Flow { handle: None, .. })), payload } if *local == self.local(channel) && payload.is_empty())
            {
                return;
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected window barrier frame: {frame:?}"
            );
        }
        panic!("bounded window barrier count");
    }

    pub(super) async fn disposition(&mut self, channel: u16, role: Role, id: u32) -> Disposition {
        let (actual, frame) = self.control().await;
        assert_eq!(actual, self.local(channel));
        let Performative::Disposition(disposition) = frame else {
            panic!("exact Disposition response");
        };
        assert_eq!(disposition.role, role);
        assert_eq!(disposition.first, id);
        assert!(disposition.last.is_none());
        disposition
    }

    pub(super) async fn outcome(
        &mut self,
        channel: u16,
        first: u32,
        last: Option<u32>,
        state: DeliveryState,
        settled: bool,
    ) {
        self.send(
            channel,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first,
                last,
                settled,
                state: Some(state),
                batchable: false,
            }),
            Vec::new(),
        )
        .await;
    }

    pub(super) async fn retirement(&mut self, channel: u16, id: u32, transaction: &TransactionId) {
        self.outcome(
            channel,
            id,
            None,
            DeliveryState::Transactional(TransactionalState {
                txn_id: transaction.clone(),
                outcome: Some(Outcome::Accepted(Accepted)),
            }),
            false,
        )
        .await;
    }

    pub(super) async fn outgoing(
        &mut self,
        channel: u16,
        handle: u32,
        expected: &Message,
        tag: &[u8],
    ) -> u32 {
        let mut bytes = Vec::new();
        let mut id = None;
        for _ in 0..1_024 {
            let frame = self.frame().await;
            if self.valid_flow(&frame) {
                continue;
            }
            let Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Transfer(transfer)),
                payload,
            } = frame
            else {
                panic!("actual outgoing Transfer: {frame:?}");
            };
            assert_eq!(actual, self.local(channel));
            assert_eq!(transfer.handle, handle);
            if id.is_none() {
                id = transfer.delivery_id;
                assert_eq!(transfer.delivery_tag, Some(tag.to_vec().into()));
                assert_eq!(transfer.message_format, Some(0));
                assert_eq!(transfer.settled, Some(false));
            } else {
                assert!(transfer.delivery_id.is_none());
            }
            assert!(transfer.state.is_none());
            bytes.extend(payload);
            if !transfer.more {
                assert_eq!(
                    decode_message(&bytes).expect("actual outgoing payload"),
                    *expected
                );
                return id.expect("original delivery ID");
            }
        }
        panic!("bounded outgoing fragment count");
    }

    pub(super) async fn transfer(
        &mut self,
        channel: u16,
        handle: u32,
        id: u32,
        transaction: Option<&TransactionId>,
        message: &Message,
    ) {
        self.send(
            channel,
            Performative::Transfer(Transfer {
                handle,
                delivery_id: Some(id),
                delivery_tag: Some(vec![id as u8].into()),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: transaction.map(|txn_id| {
                    DeliveryState::Transactional(TransactionalState {
                        txn_id: txn_id.clone(),
                        outcome: None,
                    })
                }),
                resume: false,
                aborted: false,
                batchable: false,
            }),
            encode_message(message).expect("native peer payload"),
        )
        .await;
        *self.outgoing.entry(channel).or_default() += 1;
    }

    pub(super) async fn partial_post(&mut self, transaction: &TransactionId) {
        self.send(
            POST,
            Performative::Transfer(Transfer {
                handle: HANDLE,
                delivery_id: Some(0),
                delivery_tag: Some(vec![0].into()),
                message_format: Some(0),
                settled: Some(false),
                more: true,
                rcv_settle_mode: None,
                state: Some(DeliveryState::Transactional(TransactionalState {
                    txn_id: transaction.clone(),
                    outcome: None,
                })),
                resume: false,
                aborted: false,
                batchable: false,
            }),
            vec![0],
        )
        .await;
        *self.outgoing.entry(POST).or_default() += 1;
    }

    pub(super) async fn detach(&mut self, channel: u16, handle: u32, local_handle: u32) {
        self.send(
            channel,
            Performative::Detach(Detach {
                handle,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await;
        let (local, frame) = self.control().await;
        assert_eq!(local, self.local(channel));
        let Performative::Detach(detach) = frame else {
            panic!("exact Detach response");
        };
        assert_eq!(detach.handle, local_handle);
        assert!(detach.closed);
        self.barrier(channel).await;
    }
}

#[derive(Clone, Copy)]
pub(super) enum Policy {
    Disabled,
    Posting,
    Work,
}

pub(super) struct Fixture {
    pub(super) connection: ServerConnection,
    pub(super) peer: Peer,
    pub(super) gate: Arc<FlushGate>,
    controls: HashMap<u16, u32>,
    posts: u32,
}

pub(super) struct Sent {
    pub(super) delivery: SentDelivery,
    pub(super) id: u32,
}

pub(super) struct Discharged {
    pub(super) receipt: SealedDischargeReceipt,
    pub(super) id: u32,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        Self::with_policy(Policy::Work, 262_144).await
    }

    pub(super) async fn with_policy(policy: Policy, maximum_frame: u32) -> Self {
        bounded("native retirement connection opening", async {
            let (wire, mut io) = tokio::io::duplex(65_536);
            let gate = Arc::new(FlushGate::default());
            let opening = async {
                write_protocol_header(&mut io, ProtocolHeader::AMQP)
                    .await
                    .expect("peer header");
                expect_header(&mut io, ProtocolHeader::AMQP)
                    .await
                    .expect("native header");
                let mut open = Open::new("retirement-tests");
                open.channel_max = 3;
                open.max_frame_size = maximum_frame;
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
                let wire = GatedIo::new(wire, Arc::clone(&gate));
                let options = ConnectionOptions::default().idle_timeout_millis(0);
                match policy {
                    Policy::Disabled => {
                        ServerConnection::accept_with_options(
                            wire,
                            "retirement-tests",
                            None,
                            options,
                        )
                        .await
                    }
                    Policy::Posting => {
                        ServerConnection::accept_with_transactional_ingress(
                            wire,
                            "retirement-tests",
                            None,
                            options,
                        )
                        .await
                    }
                    Policy::Work => {
                        ServerConnection::accept_with_transactional_work(
                            wire,
                            "retirement-tests",
                            None,
                            options,
                        )
                        .await
                    }
                }
            };
            let (connection, io) = tokio::join!(accepting, opening);
            Self {
                connection: connection.expect("native connection"),
                peer: Peer {
                    io,
                    channels: HashMap::new(),
                    handles: HashMap::new(),
                    incoming: HashMap::new(),
                    outgoing: HashMap::new(),
                },
                gate,
                controls: HashMap::new(),
                posts: 0,
            }
        })
        .await
    }

    pub(super) async fn session(&mut self, channel: u16) -> ServerSession {
        self.peer
            .send(
                channel,
                Performative::Begin(Begin {
                    handle_max: 3,
                    ..Begin::default()
                }),
                Vec::new(),
            )
            .await;
        let incoming = bounded(
            "incoming native session",
            self.connection.next_incoming_session(),
        )
        .await
        .expect("peer Begin");
        let (session, (local, frame)) = bounded("accepted native session", async {
            tokio::join!(
                self.connection.accept_session(incoming),
                self.peer.control()
            )
        })
        .await;
        let session = session.expect("accepted session");
        let Performative::Begin(begin) = frame else {
            panic!("Begin response");
        };
        assert_eq!(begin.remote_channel, Some(channel));
        assert_eq!(local, session.channel);
        assert_ne!(local, channel);
        self.peer.channels.insert(channel, local);
        session
    }

    pub(super) async fn incoming(
        &mut self,
        session: &mut ServerSession,
        channel: u16,
        attach: Attach,
    ) -> IncomingAttach {
        self.peer
            .send(channel, Performative::Attach(Box::new(attach)), Vec::new())
            .await;
        bounded(
            "actor-approved retirement Attach",
            session.next_incoming_attach(),
        )
        .await
        .expect("incoming Attach")
    }

    pub(super) async fn coordinator(
        &mut self,
        channel: u16,
    ) -> (ServerSession, CoordinatorEndpoint) {
        let mut session = self.session(channel).await;
        let mut request = attach(HANDLE, &format!("controller-{channel}"), Role::Sender);
        request.target = Some(Coordinator::default().into());
        request.source.as_mut().expect("control source").outcomes = Some(Array::from(vec![
            Symbol::from("amqp:accepted:list"),
            Symbol::from("amqp:rejected:list"),
            Symbol::from("amqp:declared:list"),
        ]));
        let incoming = self.incoming(&mut session, channel, request).await;
        let responses = async {
            let response = self.peer.attached(channel).await;
            assert!(
                response
                    .target
                    .as_ref()
                    .and_then(|target| target.as_coordinator())
                    .is_some()
            );
            assert_eq!(response.handle, 0);
            self.peer.credit_response(channel, response.handle).await;
        };
        let (endpoint, ()) = bounded("coordinator admission", async {
            tokio::join!(session.accept_coordinator(incoming, 0), responses)
        })
        .await;
        (session, endpoint.expect("coordinator endpoint"))
    }

    pub(super) async fn sender(
        &mut self,
        session: &mut ServerSession,
        channel: u16,
        handle: u32,
        name: &str,
    ) -> (TransactionalSender, u32) {
        let incoming = self
            .incoming(session, channel, attach(handle, name, Role::Receiver))
            .await;
        let (sender, response) = bounded("dedicated retirement sender admission", async {
            tokio::join!(
                session.accept_transactional_sender(incoming, 0),
                self.peer.attached(channel)
            )
        })
        .await;
        let sender = sender.expect("dedicated sender");
        assert_eq!(response.role, Role::Sender);
        assert_eq!(response.rcv_settle_mode, ReceiverSettleMode::Second);
        self.peer.grant(channel, handle).await;
        (sender, response.handle)
    }

    pub(super) async fn receiver(&mut self) -> (ServerSession, TransactionalReceiver) {
        let mut session = self.session(POST).await;
        let incoming = self
            .incoming(
                &mut session,
                POST,
                attach(HANDLE, "posting-link", Role::Sender),
            )
            .await;
        let responses = async {
            let response = self.peer.attached(POST).await;
            self.peer.credit_response(POST, response.handle).await;
        };
        let (endpoint, ()) = bounded("posting receiver admission", async {
            tokio::join!(
                session.accept_transactional_receiver(incoming, 0),
                responses
            )
        })
        .await;
        (session, endpoint.expect("posting endpoint"))
    }

    pub(super) async fn send(
        &mut self,
        sender: &mut TransactionalSender,
        channel: u16,
        local_handle: u32,
    ) -> Sent {
        self.send_tag(sender, channel, local_handle, TAG).await
    }

    pub(super) async fn send_tag(
        &mut self,
        sender: &mut TransactionalSender,
        channel: u16,
        local_handle: u32,
        tag: &[u8],
    ) -> Sent {
        let message = message();
        let (sent, id) = bounded("fully flushed sent handle", async {
            tokio::join!(
                sender.send_with_dispositions(message.clone(), tag.to_vec().into()),
                self.peer.outgoing(channel, local_handle, &message, tag)
            )
        })
        .await;
        Sent {
            delivery: sent.expect("actor-minted sent handle"),
            id,
        }
    }

    pub(super) async fn command(&mut self, channel: u16, command: TransactionCommand) -> u32 {
        let id = *self.controls.entry(channel).or_default();
        *self.controls.get_mut(&channel).expect("control counter") += 1;
        let message = Message {
            body: Body::Value(Value::from(command)),
            ..Message::default()
        };
        self.peer
            .transfer(channel, HANDLE, id, None, &message)
            .await;
        id
    }

    pub(super) async fn declare(
        &mut self,
        coordinator: &mut CoordinatorEndpoint,
        channel: u16,
        transaction: &TransactionId,
    ) -> NativeTransactionIdentity {
        let id = self
            .command(channel, TransactionCommand::Declare(Declare::default()))
            .await;
        let CoordinatorRequest::Declare(receipt) = bounded("Declare request", coordinator.recv())
            .await
            .expect("Declare receipt")
        else {
            panic!("Declare request");
        };
        let (identity, disposition) = bounded("Declared flush", async {
            tokio::join!(
                receipt.declared(transaction.clone()),
                self.peer.disposition(channel, Role::Receiver, id)
            )
        })
        .await;
        assert!(disposition.settled);
        assert!(
            matches!(disposition.state, Some(DeliveryState::Declared(crate::Declared { txn_id })) if txn_id == *transaction)
        );
        identity.expect("declared native group")
    }

    pub(super) async fn retirement(
        &mut self,
        sent: &mut Sent,
        transaction: &TransactionId,
    ) -> TransactionRetirementReceipt {
        self.peer.retirement(SEND, sent.id, transaction).await;
        let TransactionalDisposition::Retirement(receipt) = bounded(
            "native retirement receipt",
            sent.delivery.next_disposition(),
        )
        .await
        .expect("retirement event") else {
            panic!("retirement disposition");
        };
        assert_eq!(receipt.transaction_id(), transaction);
        assert_eq!(receipt.outcome(), &Outcome::Accepted(Accepted));
        assert!(
            receipt
                .delivery_identity()
                .same_delivery(sent.delivery.delivery_identity())
        );
        receipt
    }

    pub(super) async fn provisional(
        &mut self,
        receipt: TransactionRetirementReceipt,
        transaction: &TransactionId,
        id: u32,
    ) -> PreparedRetirement {
        let (prepared, disposition) = bounded("retirement provisional flush", async {
            tokio::join!(
                receipt.provisional_accept(),
                self.peer.disposition(SEND, Role::Sender, id)
            )
        })
        .await;
        assert!(!disposition.settled);
        assert!(
            matches!(disposition.state, Some(DeliveryState::Transactional(TransactionalState { txn_id, outcome: Some(Outcome::Accepted(_)) })) if txn_id == *transaction)
        );
        prepared.expect("prepared retirement")
    }

    pub(super) async fn post(
        &mut self,
        receiver: &mut TransactionalReceiver,
        transaction: &TransactionId,
    ) -> PreparedPosting {
        let id = self.posts;
        self.posts += 1;
        self.peer
            .transfer(POST, HANDLE, id, Some(transaction), &message())
            .await;
        let TransactionalIngress::Posting(receipt) = bounded("posting receipt", receiver.recv())
            .await
            .expect("post ingress")
        else {
            panic!("transaction posting");
        };
        let (prepared, disposition) = bounded("posting provisional flush", async {
            tokio::join!(
                receipt.provisional_accept(),
                self.peer.disposition(POST, Role::Receiver, id)
            )
        })
        .await;
        assert!(!disposition.settled);
        assert!(matches!(
            disposition.state,
            Some(DeliveryState::Transactional(_))
        ));
        prepared.expect("prepared posting")
    }

    pub(super) async fn discharge(
        &mut self,
        coordinator: &mut CoordinatorEndpoint,
        channel: u16,
        transaction: &TransactionId,
        fail: bool,
    ) -> Discharged {
        let id = self
            .command(
                channel,
                TransactionCommand::Discharge(Discharge {
                    txn_id: transaction.clone(),
                    fail: Some(fail),
                }),
            )
            .await;
        let CoordinatorRequest::Discharge(receipt) =
            bounded("Discharge request", coordinator.recv())
                .await
                .expect("sealed discharge")
        else {
            panic!("Discharge request");
        };
        assert_eq!(receipt.transaction_id(), transaction);
        Discharged { receipt, id }
    }

    pub(super) async fn control_accepted(&mut self, channel: u16, id: u32) {
        let disposition = self.peer.disposition(channel, Role::Receiver, id).await;
        assert!(disposition.settled);
        assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
    }

    pub(super) async fn shutdown(&self) {
        bounded(
            "native retirement actor shutdown",
            self.connection.shutdown(),
        )
        .await;
    }
}
