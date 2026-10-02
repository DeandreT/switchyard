use super::*;

#[path = "peer/decisions.rs"]
mod decisions;

pub(super) trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

pub(super) struct Peer {
    stream: Box<dyn Stream>,
    channels: HashMap<u16, u16>,
    handles: HashMap<u16, HashSet<u32>>,
    sent: HashMap<u16, u32>,
    received: HashMap<u16, u32>,
    deliveries: HashMap<u16, u32>,
}

pub(super) struct WireDelivery {
    pub(super) id: u32,
    pub(super) tag: Vec<u8>,
    pub(super) message: Message,
}

impl Peer {
    pub(super) async fn connect(address: std::net::SocketAddr) -> TestResult<Self> {
        let stream = timeout(DEADLINE, TcpStream::connect(address)).await??;
        stream.set_nodelay(true)?;
        Self::open(Box::new(stream)).await
    }

    pub(super) async fn open(mut stream: Box<dyn Stream>) -> TestResult<Self> {
        timeout(
            DEADLINE,
            write_protocol_header(&mut stream, ProtocolHeader::AMQP),
        )
        .await??;
        assert_eq!(
            timeout(DEADLINE, read_protocol_header(&mut stream)).await??,
            ProtocolHeader::AMQP
        );
        let mut peer = Self {
            stream,
            channels: HashMap::new(),
            handles: HashMap::new(),
            sent: HashMap::new(),
            received: HashMap::new(),
            deliveries: HashMap::new(),
        };
        let mut open = Open::new("native-messaging-peer");
        open.channel_max = 3;
        open.hostname = Some("tenant.servicebus.windows.net".into());
        peer.send(0, Performative::Open(open), vec![]).await?;
        assert!(
            matches!(peer.read().await?, Frame::Amqp { performative: Some(Performative::Open(_)), payload, .. } if payload.is_empty())
        );
        Ok(peer)
    }

    pub(super) async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> TestResult {
        if matches!(&performative, Performative::Transfer(_)) {
            *self.sent.entry(channel).or_default() += 1;
        }
        timeout(
            DEADLINE,
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
        let frame = timeout(DEADLINE, read_frame(&mut self.stream)).await??;
        if let Frame::Amqp {
            channel,
            performative: Some(Performative::Transfer(_)),
            ..
        } = &frame
        {
            *self.received.entry(*channel).or_default() += 1;
        }
        Ok(frame)
    }

    pub(super) fn local(&self, channel: u16) -> u16 {
        self.channels[&channel]
    }

    pub(super) async fn acknowledge_end(&mut self, channel: u16) -> TestResult {
        self.send(channel, Performative::End(amqp::End::default()), vec![])
            .await?;
        let local = self.channels.remove(&channel).expect("ended peer session");
        self.handles.remove(&local);
        self.received.remove(&local);
        self.sent.remove(&channel);
        self.deliveries.remove(&channel);
        Ok(())
    }

    fn valid_flow(&self, frame: &Frame) -> bool {
        matches!(frame, Frame::Amqp { channel, performative: Some(Performative::Flow(flow)), payload }
            if payload.is_empty() && self.channels.values().any(|known| known == channel)
            && flow.handle.is_none_or(|handle| self.handles.get(channel).is_some_and(|known| known.contains(&handle))))
    }

    pub(super) async fn control(&mut self) -> TestResult<(u16, Performative)> {
        for _ in 0..32 {
            let frame = self.read().await?;
            if self.valid_flow(&frame) {
                continue;
            }
            if let Frame::Amqp {
                channel,
                performative: Some(performative),
                payload,
            } = frame
            {
                assert!(payload.is_empty(), "no unanticipated outgoing payload");
                return Ok((channel, performative));
            }
            return Err("unexpected native control frame".into());
        }
        Err("bounded native control response missing".into())
    }

    pub(super) async fn begin(&mut self, channel: u16) -> TestResult {
        let expected = (0..=3)
            .find(|candidate| !self.channels.values().any(|known| known == candidate))
            .expect("test native channel available");
        self.sent.insert(channel, 0);
        self.deliveries.insert(channel, 0);
        self.send(
            channel,
            Performative::Begin(Begin {
                handle_max: 0,
                ..Begin::default()
            }),
            vec![],
        )
        .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel: local,
                performative: Some(Performative::Begin(begin)),
                payload,
            } = &frame
            {
                assert_eq!(*local, expected);
                assert_eq!(begin.remote_channel, Some(channel));
                assert!(payload.is_empty());
                self.channels.insert(channel, *local);
                self.handles.insert(*local, HashSet::new());
                return Ok(());
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected Begin response: {frame:?}"
            );
        }
        Err("bounded Begin response missing".into())
    }

    pub(super) fn attach_request(channel: u16, handle: u32, entity: &str, role: Role) -> Attach {
        Attach {
            name: format!("messaging-{channel}-{handle}"),
            handle,
            role: role.clone(),
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: if role == Role::Receiver {
                ReceiverSettleMode::Second
            } else {
                ReceiverSettleMode::First
            },
            source: (role == Role::Receiver).then(|| Source::new(entity)),
            target: Some(
                Target::new(if role == Role::Sender {
                    entity
                } else {
                    "native-reply"
                })
                .into(),
            ),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: (role == Role::Sender).then_some(0),
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    pub(super) async fn attach(&mut self, channel: u16, request: Attach) -> TestResult {
        self.send(channel, Performative::Attach(Box::new(request)), vec![])
            .await
    }

    pub(super) async fn admitted(&mut self, channel: u16, client_role: Role) -> TestResult {
        self.admitted_with_initial_count(channel, client_role, None)
            .await
    }

    async fn admitted_with_initial_count(
        &mut self,
        channel: u16,
        client_role: Role,
        initial_count: Option<u32>,
    ) -> TestResult {
        let mut attached = false;
        for _ in 0..32 {
            let frame = self.read().await?;
            match &frame {
                Frame::Amqp {
                    channel: local,
                    performative: Some(Performative::Attach(attach)),
                    payload,
                } if *local == self.local(channel) => {
                    assert!(payload.is_empty());
                    assert_eq!(attach.handle, 0);
                    assert_ne!(attach.role, client_role);
                    self.handles.entry(*local).or_default().insert(0);
                    attached = true;
                    if client_role == Role::Receiver {
                        assert_eq!(attach.snd_settle_mode, SenderSettleMode::Unsettled);
                        assert_eq!(attach.rcv_settle_mode, ReceiverSettleMode::Second);
                        return Ok(());
                    }
                }
                Frame::Amqp {
                    channel: local,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } if *local == self.local(channel) && flow.handle == Some(0) => {
                    assert!(attached && client_role == Role::Sender);
                    assert!(payload.is_empty());
                    assert!(flow.link_credit.is_some_and(|credit| credit > 0));
                    if let Some(expected) = initial_count {
                        assert_eq!(flow.delivery_count, Some(expected));
                    }
                    return Ok(());
                }
                _ => assert!(
                    self.valid_flow(&frame),
                    "unexpected link admission: {frame:?}"
                ),
            }
        }
        Err("bounded link admission missing".into())
    }

    pub(super) async fn coordinator(&mut self) -> TestResult {
        self.coordinator_with_initial_count(Some(0)).await
    }

    pub(super) async fn coordinator_with_initial_count(
        &mut self,
        initial_count: Option<u32>,
    ) -> TestResult {
        self.coordinator_with_profile(
            initial_count,
            SenderSettleMode::Unsettled,
            Source {
                outcomes: Some(
                    vec![
                        Symbol::from("amqp:declared:list"),
                        Symbol::from("amqp:accepted:list"),
                        Symbol::from("amqp:rejected:list"),
                    ]
                    .into(),
                ),
                ..Source::default()
            },
        )
        .await
    }

    pub(super) async fn coordinator_with_sdk_defaults(&mut self) -> TestResult {
        self.coordinator_with_profile(
            None,
            SenderSettleMode::Mixed,
            Source {
                distribution_mode: Some(Symbol::from("move")),
                ..Source::default()
            },
        )
        .await
    }

    async fn coordinator_with_profile(
        &mut self,
        initial_count: Option<u32>,
        sender_mode: SenderSettleMode,
        source: Source,
    ) -> TestResult {
        self.begin(CONTROL).await?;
        let mut request = Self::attach_request(CONTROL, CONTROL_HANDLE, "", Role::Sender);
        request.initial_delivery_count = initial_count;
        request.snd_settle_mode = sender_mode;
        request.target = Some(Coordinator::default().into());
        request.source = Some(source);
        self.attach(CONTROL, request).await?;
        self.admitted_with_initial_count(CONTROL, Role::Sender, Some(initial_count.unwrap_or(0)))
            .await
    }

    pub(super) async fn producer(&mut self, channel: u16, entity: &str) -> TestResult {
        self.begin(channel).await?;
        self.attach(
            channel,
            Self::attach_request(channel, POST_HANDLE, entity, Role::Sender),
        )
        .await?;
        self.admitted(channel, Role::Sender).await
    }

    pub(super) async fn consumer(&mut self) -> TestResult<WireDelivery> {
        self.consumer_with_distribution(None).await
    }

    pub(super) async fn consumer_with_distribution(
        &mut self,
        distribution: Option<&str>,
    ) -> TestResult<WireDelivery> {
        self.consumer_with_profile(SenderSettleMode::Unsettled, distribution)
            .await
    }

    pub(super) async fn consumer_with_sender_mode(
        &mut self,
        sender_mode: SenderSettleMode,
    ) -> TestResult<WireDelivery> {
        self.consumer_with_profile(sender_mode, None).await
    }

    async fn consumer_with_profile(
        &mut self,
        sender_mode: SenderSettleMode,
        distribution: Option<&str>,
    ) -> TestResult<WireDelivery> {
        self.begin(RECEIVE).await?;
        let mut request = Self::attach_request(RECEIVE, RECEIVE_HANDLE, "orders", Role::Receiver);
        request.snd_settle_mode = sender_mode;
        request
            .source
            .as_mut()
            .expect("receiver Source")
            .distribution_mode = distribution.map(Symbol::from);
        self.attach(RECEIVE, request).await?;
        self.admitted(RECEIVE, Role::Receiver).await?;
        self.send(
            RECEIVE,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 2_048,
                next_outgoing_id: 0,
                outgoing_window: 2_048,
                handle: Some(RECEIVE_HANDLE),
                delivery_count: Some(0),
                link_credit: Some(1),
                ..Flow::default()
            }),
            vec![],
        )
        .await?;
        let mut bytes = Vec::new();
        let mut original = None;
        for _ in 0..1_024 {
            let frame = self.read().await?;
            if self.valid_flow(&frame) {
                continue;
            }
            let Frame::Amqp {
                channel,
                performative: Some(Performative::Transfer(transfer)),
                payload,
            } = frame
            else {
                return Err("actual consumer Transfer missing".into());
            };
            assert_eq!(channel, self.local(RECEIVE));
            assert_eq!(transfer.handle, 0);
            assert!(transfer.state.is_none());
            if original.is_none() {
                assert_eq!(transfer.message_format, Some(0));
                assert_eq!(transfer.settled, Some(false));
                original = Some((
                    transfer.delivery_id.expect("original wire alias"),
                    transfer
                        .delivery_tag
                        .expect("original wire tag")
                        .as_ref()
                        .to_vec(),
                ));
            } else {
                assert!(transfer.delivery_id.is_none());
            }
            bytes.extend(payload);
            if !transfer.more {
                let (id, tag) = original.expect("original Transfer provenance");
                return Ok(WireDelivery {
                    id,
                    tag,
                    message: decode_message(&bytes)?,
                });
            }
        }
        Err("bounded consumer fragment count".into())
    }

    pub(super) async fn setup(&mut self) -> TestResult<WireDelivery> {
        self.coordinator().await?;
        self.producer(POST, "orders").await?;
        self.consumer().await
    }

    pub(super) async fn transfer(
        &mut self,
        channel: u16,
        handle: u32,
        transaction: Option<&TransactionId>,
        format: u32,
        message: &Message,
    ) -> TestResult<u32> {
        let id = self.deliveries[&channel];
        self.deliveries.insert(channel, id + 1);
        self.send(
            channel,
            Performative::Transfer(Transfer {
                handle,
                delivery_id: Some(id),
                delivery_tag: Some(vec![id as u8].into()),
                message_format: Some(format),
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
            encode_message(message)?,
        )
        .await?;
        Ok(id)
    }

    pub(super) async fn disposition(
        &mut self,
        channel: u16,
        role: Role,
        id: u32,
    ) -> TestResult<Disposition> {
        let (actual, performative) = self.control().await?;
        assert_eq!(actual, self.local(channel));
        let Performative::Disposition(disposition) = performative else {
            return Err("exact Disposition missing".into());
        };
        assert_eq!(disposition.role, role);
        assert_eq!(disposition.first, id);
        assert!(disposition.last.is_none());
        Ok(disposition)
    }

    pub(super) async fn declare(&mut self) -> TestResult<TransactionId> {
        let id = self
            .transfer(
                CONTROL,
                CONTROL_HANDLE,
                None,
                0,
                &control(TransactionCommand::Declare(Declare::default())),
            )
            .await?;
        let response = self.disposition(CONTROL, Role::Receiver, id).await?;
        assert!(response.settled);
        let Some(DeliveryState::Declared(declared)) = response.state else {
            return Err("Declared response missing".into());
        };
        Ok(declared.txn_id)
    }

    pub(super) async fn retire(
        &mut self,
        original: &WireDelivery,
        transaction: &TransactionId,
    ) -> TestResult {
        self.outcome(
            original.id,
            DeliveryState::Transactional(TransactionalState {
                txn_id: transaction.clone(),
                outcome: Some(Outcome::Accepted(amqp::Accepted)),
            }),
        )
        .await
    }

    pub(super) async fn outcome(&mut self, id: u32, state: DeliveryState) -> TestResult {
        self.send(
            RECEIVE,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled: false,
                state: Some(state),
                batchable: false,
            }),
            vec![],
        )
        .await
    }

    pub(super) async fn provisional(
        &mut self,
        channel: u16,
        role: Role,
        id: u32,
        transaction: &TransactionId,
    ) -> TestResult {
        let disposition = self.disposition(channel, role, id).await?;
        assert!(!disposition.settled);
        assert!(
            matches!(disposition.state, Some(DeliveryState::Transactional(TransactionalState { txn_id, outcome: Some(Outcome::Accepted(_)) })) if txn_id == *transaction)
        );
        Ok(())
    }

    pub(super) async fn discharge(
        &mut self,
        transaction: &TransactionId,
        fail: bool,
    ) -> TestResult<u32> {
        self.transfer(
            CONTROL,
            CONTROL_HANDLE,
            None,
            0,
            &control(TransactionCommand::Discharge(Discharge {
                txn_id: transaction.clone(),
                fail: Some(fail),
            })),
        )
        .await
    }

    pub(super) async fn barrier(&mut self, channel: u16) -> TestResult {
        let sent = self.sent[&channel];
        self.send(
            channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(
                    self.received
                        .get(&self.local(channel))
                        .copied()
                        .unwrap_or(0),
                ),
                incoming_window: 2_048,
                next_outgoing_id: sent,
                outgoing_window: 2_048,
                echo: true,
                ..Flow::default()
            }),
            vec![],
        )
        .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if matches!(&frame, Frame::Amqp { channel: actual, performative: Some(Performative::Flow(flow)), payload }
                if *actual == self.local(channel) && flow.handle.is_none() && flow.next_incoming_id == Some(sent) && payload.is_empty())
            {
                return Ok(());
            }
            assert!(
                self.valid_flow(&frame),
                "barrier rejects early outcome, replacement ACK or resend: {frame:?}"
            );
        }
        Err("bounded session barrier missing".into())
    }

    pub(super) async fn detached(
        &mut self,
        channel: u16,
        handle: u32,
        condition: Option<&str>,
        acknowledge: bool,
    ) -> TestResult {
        for _ in 0..32 {
            let (actual, frame) = self.control().await?;
            if let Performative::Attach(attach) = &frame {
                assert_eq!(actual, self.local(channel));
                assert_eq!(attach.handle, 0);
                self.handles.entry(actual).or_default().insert(0);
                continue;
            }
            assert_eq!(actual, self.local(channel));
            let Performative::Detach(detach) = frame else {
                return Err("scoped Detach missing; no successful settlement allowed".into());
            };
            assert_eq!(detach.handle, 0);
            assert!(detach.closed);
            match condition {
                Some(condition) => {
                    let error = detach.error.expect("explicit static refusal");
                    assert_eq!(error.condition.as_symbol().as_str(), condition);
                    assert!(
                        !error
                            .description
                            .as_deref()
                            .unwrap_or("")
                            .contains("DO-NOT-EXPOSE")
                    );
                }
                None => assert!(detach.error.is_none()),
            }
            if acknowledge {
                self.request_detach(channel, handle).await?;
            }
            return Ok(());
        }
        Err("bounded scoped Detach missing".into())
    }

    pub(super) async fn request_detach(&mut self, channel: u16, handle: u32) -> TestResult {
        self.send(
            channel,
            Performative::Detach(amqp::Detach {
                handle,
                closed: true,
                error: None,
            }),
            vec![],
        )
        .await
    }

    pub(super) async fn close_after_scoped_cleanup(mut self) -> TestResult {
        self.send(0, Performative::Close(Close { error: None }), vec![])
            .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if matches!(&frame, Frame::Amqp { performative: Some(Performative::Close(close)), payload, .. } if close.error.is_none() && payload.is_empty())
            {
                return Ok(());
            }
            if let Frame::Amqp {
                channel,
                performative: Some(Performative::Detach(detach)),
                payload,
            } = &frame
            {
                assert!(self.channels.values().any(|known| known == channel));
                assert_eq!(detach.handle, 0);
                assert!(detach.closed && payload.is_empty());
                if let Some(error) = &detach.error {
                    assert_eq!(
                        error.condition.as_symbol().as_str(),
                        "amqp:resource-limit-exceeded",
                        "cleanup route {channel}: {error:?}"
                    );
                    assert!(
                        !error
                            .description
                            .as_deref()
                            .unwrap_or("")
                            .contains("DO-NOT-EXPOSE")
                    );
                }
                continue;
            }
            assert!(
                self.valid_flow(&frame),
                "cleanup permits no applied transaction outcomes: {frame:?}"
            );
        }
        Err("bounded scoped cleanup Close missing".into())
    }

    pub(super) async fn close(mut self) -> TestResult {
        self.send(0, Performative::Close(Close { error: None }), vec![])
            .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if matches!(&frame, Frame::Amqp { performative: Some(Performative::Close(close)), payload, .. } if close.error.is_none() && payload.is_empty())
            {
                return Ok(());
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected connection cleanup: {frame:?}"
            );
        }
        Err("bounded Close response missing".into())
    }
}

pub(super) fn control(command: TransactionCommand) -> Message {
    Message {
        body: Body::Value(Value::from(command)),
        ..Message::default()
    }
}

pub(super) fn message(id: &str, bytes: &[u8]) -> Message {
    Message::builder()
        .properties(Properties {
            message_id: Some(id.into()),
            ..Properties::default()
        })
        .body(Body::Data(vec![bytes.to_vec().into()]))
        .build()
}

pub(super) fn batch(messages: &[Message]) -> TestResult<Message> {
    let encoded = messages
        .iter()
        .map(encode_message)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Message {
        body: Body::Data(encoded.into_iter().map(Into::into).collect()),
        ..Message::default()
    })
}
