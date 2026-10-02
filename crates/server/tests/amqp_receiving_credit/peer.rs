use super::*;

pub(super) trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

pub(super) struct Peer {
    stream: Box<dyn Stream>,
    channels: HashMap<u16, u16>,
    handles: HashMap<u16, HashSet<u32>>,
    sent: HashMap<u16, u32>,
    received: HashMap<u16, u32>,
}

pub(super) struct WireDelivery {
    pub(super) id: u32,
    pub(super) message: Message,
    pub(super) settled: bool,
}

impl Peer {
    pub(super) async fn connect(address: std::net::SocketAddr) -> TestResult<Self> {
        let tcp = timeout(DEADLINE, TcpStream::connect(address)).await??;
        tcp.set_nodelay(true)?;
        Self::open(Box::new(tcp)).await
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
        };
        let mut open = Open::new("ordinary-receive-credit-peer");
        open.channel_max = 1;
        open.hostname = Some("tenant.servicebus.windows.net".into());
        peer.send(0, Performative::Open(open), vec![]).await?;
        assert!(
            matches!(peer.read().await?, Frame::Amqp { channel: 0, performative: Some(Performative::Open(_)), payload } if payload.is_empty())
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

    pub(super) async fn read(&mut self) -> TestResult<Frame> {
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

    pub(super) fn valid_flow(&self, frame: &Frame) -> bool {
        matches!(frame, Frame::Amqp { channel, performative: Some(Performative::Flow(flow)), payload }
            if payload.is_empty() && self.channels.values().any(|known| known == channel)
            && flow.handle.is_none_or(|handle| self.handles.get(channel).is_some_and(|known| known.contains(&handle))))
    }

    pub(super) async fn begin(&mut self, channel: u16, local: u16, handle_max: u32) -> TestResult {
        self.sent.insert(channel, 0);
        self.send(
            channel,
            Performative::Begin(Begin {
                handle_max,
                ..Begin::default()
            }),
            vec![],
        )
        .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Begin(begin)),
                payload,
            } = &frame
            {
                assert_eq!(*actual, local);
                assert_eq!(begin.remote_channel, Some(channel));
                assert!(payload.is_empty());
                self.channels.insert(channel, local);
                self.handles.insert(local, HashSet::new());
                self.received.insert(local, 0);
                return Ok(());
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected Begin response: {frame:?}"
            );
        }
        Err("bounded Begin response missing".into())
    }

    pub(super) async fn attach_receiver(
        &mut self,
        name: &str,
        address: &str,
        mode: ReceiveMode,
    ) -> TestResult {
        let (sender_mode, receiver_mode) = match mode {
            ReceiveMode::PeekLock => (SenderSettleMode::Unsettled, ReceiverSettleMode::Second),
            ReceiveMode::ReceiveAndDelete => (SenderSettleMode::Settled, ReceiverSettleMode::First),
        };
        self.attach(
            CHANNEL,
            Attach {
                name: name.into(),
                handle: HANDLE,
                role: Role::Receiver,
                snd_settle_mode: sender_mode,
                rcv_settle_mode: receiver_mode,
                source: Some(Source::new(address)),
                target: Some(Target::new("generated-target").into()),
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: None,
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            },
            0,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn attach(
        &mut self,
        channel: u16,
        request: Attach,
        expected_handle: u32,
    ) -> TestResult {
        let role = request.role.clone();
        let sender_mode = request.snd_settle_mode.clone();
        let receiver_mode = request.rcv_settle_mode.clone();
        let name = request.name.clone();
        self.send(channel, Performative::Attach(Box::new(request)), vec![])
            .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Attach(attach)),
                payload,
            } = &frame
            {
                assert_eq!(*actual, self.local(channel));
                assert_eq!(attach.handle, expected_handle);
                assert_eq!(attach.role, role.opposite());
                assert_eq!(attach.name, name);
                assert_eq!(attach.snd_settle_mode, sender_mode);
                assert_eq!(attach.rcv_settle_mode, receiver_mode);
                assert!(payload.is_empty());
                self.handles
                    .entry(*actual)
                    .or_default()
                    .insert(expected_handle);
                return Ok(());
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected Attach response: {frame:?}"
            );
        }
        Err("bounded Attach response missing".into())
    }

    pub(super) fn flow(&self, channel: u16) -> Flow {
        Flow {
            next_incoming_id: Some(self.received[&self.local(channel)]),
            incoming_window: WINDOW,
            next_outgoing_id: self.sent[&channel],
            outgoing_window: WINDOW,
            ..Flow::default()
        }
    }

    pub(super) async fn grant(&mut self, count: u32, credit: u32, drain: bool) -> TestResult {
        let mut flow = self.flow(CHANNEL);
        flow.handle = Some(HANDLE);
        flow.delivery_count = Some(count);
        flow.link_credit = Some(credit);
        flow.drain = drain;
        self.send(CHANNEL, Performative::Flow(flow), vec![]).await
    }

    pub(super) async fn barrier(&mut self, channel: u16) -> TestResult {
        let mut flow = self.flow(channel);
        flow.echo = true;
        let incoming = flow.next_outgoing_id;
        self.send(channel, Performative::Flow(flow), vec![]).await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if matches!(&frame, Frame::Amqp { channel: actual, performative: Some(Performative::Flow(flow)), payload }
                if *actual == self.local(channel) && flow.handle.is_none()
                && flow.next_incoming_id == Some(incoming) && payload.is_empty())
            {
                return Ok(());
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected work before credit barrier: {frame:?}"
            );
        }
        Err("bounded Flow barrier missing".into())
    }

    pub(super) async fn drained(&mut self, count: u32) -> TestResult {
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel,
                performative: Some(Performative::Flow(flow)),
                payload,
            } = &frame
                && *channel == self.local(CHANNEL)
                && flow.handle == Some(0)
                && flow.drain
            {
                assert_eq!(flow.delivery_count, Some(count));
                assert_eq!(flow.link_credit, Some(0));
                assert!(payload.is_empty());
                return Ok(());
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected drain response: {frame:?}"
            );
        }
        Err("bounded drain response missing".into())
    }

    pub(super) async fn delivery(&mut self) -> TestResult<WireDelivery> {
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel,
                performative: Some(Performative::Transfer(transfer)),
                payload,
            } = &frame
            {
                assert_eq!(*channel, self.local(CHANNEL));
                assert_eq!(transfer.handle, 0);
                assert_eq!(transfer.message_format, Some(0));
                assert!(!transfer.more && transfer.state.is_none());
                assert!(transfer.delivery_tag.is_some());
                return Ok(WireDelivery {
                    id: transfer.delivery_id.ok_or("original delivery id missing")?,
                    message: decode_message(payload)?,
                    settled: transfer.settled == Some(true),
                });
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected delivery response: {frame:?}"
            );
        }
        Err("bounded Transfer response missing".into())
    }

    pub(super) async fn complete(&mut self, delivery: &WireDelivery) -> TestResult {
        if delivery.settled {
            return Ok(());
        }
        self.send(
            CHANNEL,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: delivery.id,
                last: None,
                settled: false,
                state: Some(DeliveryState::Accepted(Accepted)),
                batchable: false,
            }),
            vec![],
        )
        .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel,
                performative: Some(Performative::Disposition(disposition)),
                payload,
            } = &frame
            {
                assert_eq!(*channel, self.local(CHANNEL));
                assert_eq!(disposition.role, Role::Sender);
                assert_eq!(disposition.first, delivery.id);
                assert!(disposition.last.is_none() && disposition.settled && payload.is_empty());
                assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
                return Ok(());
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected settlement response: {frame:?}"
            );
        }
        Err("bounded sender settlement missing".into())
    }

    pub(super) async fn detach(&mut self) -> TestResult {
        self.send(
            CHANNEL,
            Performative::Detach(Detach {
                handle: HANDLE,
                closed: true,
                error: None,
            }),
            vec![],
        )
        .await?;
        self.detached(None).await
    }

    pub(super) async fn detached(&mut self, condition: Option<&str>) -> TestResult {
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel,
                performative: Some(Performative::Detach(detach)),
                payload,
            } = &frame
            {
                assert_eq!(*channel, self.local(CHANNEL));
                assert_eq!(detach.handle, 0);
                assert!(detach.closed && payload.is_empty());
                assert_eq!(
                    detach
                        .error
                        .as_ref()
                        .map(|error| error.condition.as_symbol()),
                    condition.map(Symbol::from)
                );
                if condition.is_some() {
                    self.send(
                        CHANNEL,
                        Performative::Detach(Detach {
                            handle: HANDLE,
                            closed: true,
                            error: None,
                        }),
                        vec![],
                    )
                    .await?;
                }
                self.handles
                    .get_mut(channel)
                    .expect("live session handles")
                    .remove(&0);
                return Ok(());
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected scoped Detach response: {frame:?}"
            );
        }
        Err("bounded Detach response missing".into())
    }

    pub(super) async fn close(mut self) -> TestResult {
        self.send(0, Performative::Close(Close::default()), vec![])
            .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Close(close)),
                payload,
            } = &frame
            {
                assert!(close.error.is_none() && payload.is_empty());
                return Ok(());
            }
            assert!(
                self.valid_flow(&frame),
                "unexpected Close response: {frame:?}"
            );
        }
        Err("bounded Close response missing".into())
    }
}

pub(super) fn assert_body(delivery: &WireDelivery, id: &str) {
    assert_eq!(
        delivery
            .message
            .properties
            .as_ref()
            .and_then(|properties| properties.message_id.as_ref()),
        Some(&amqp::MessageId::from(id))
    );
    assert_eq!(
        delivery.message.body,
        Body::Data(vec![id.as_bytes().to_vec().into()])
    );
}
