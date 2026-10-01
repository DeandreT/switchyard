use super::*;

#[derive(Clone, Debug)]
pub(super) struct ObservedStore<S> {
    pub(super) inner: S,
    pub(super) reads: Arc<AtomicUsize>,
    pub(super) writes: Arc<AtomicUsize>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StorageError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key)
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, StoredValue)>, StorageError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.scan_from(prefix, start, limit)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.apply(batch)
    }
}

pub(super) struct Node<P: StoreProvider> {
    pub(super) broker: Broker,
    pub(super) store: ObservedStore<P::Store>,
    pub(super) namespace: NamespaceName,
    pub(super) entity: EntityPath,
    pub(super) address: std::net::SocketAddr,
    listener: JoinHandle<()>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    pub(super) async fn start(provider: P) -> TestResult<Self> {
        let store = ObservedStore {
            inner: provider.open()?,
            reads: Arc::new(AtomicUsize::new(0)),
            writes: Arc::new(AtomicUsize::new(0)),
        };
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            ManualClock::at(1_000),
        ));
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        broker
            .handle()
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )
            .await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let handle = broker.handle();
        let scope = namespace.clone();
        let listener = tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(handle, scope)
                .with_idle_timeout_millis(0)
                .serve(listener)
                .await;
        });
        Ok(Self {
            broker,
            store,
            namespace,
            entity,
            address,
            listener,
            _provider: provider,
        })
    }

    pub(super) fn reset(&self) {
        self.store.reads.store(0, Ordering::Relaxed);
        self.store.writes.store(0, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.inner.snapshot()?)
    }

    pub(super) fn unchanged_without_owner_io(&self, before: &StoreSnapshot) -> TestResult {
        assert_eq!(
            self.store.reads.load(Ordering::Relaxed),
            0,
            "unsupported transaction reached broker state"
        );
        assert_eq!(self.store.writes.load(Ordering::Relaxed), 0);
        assert_eq!(&self.snapshot()?, before);
        Ok(())
    }

    pub(super) async fn stop(self) {
        self.listener.abort();
        let _ = self.listener.await;
        drop(self.broker);
    }
}

pub(super) struct Peer {
    stream: TcpStream,
    channels: HashMap<u16, u16>,
    handles: HashMap<u16, HashSet<u32>>,
    sent: HashMap<u16, u32>,
    received: HashMap<u16, u32>,
    published: HashMap<u16, u32>,
}

impl Peer {
    pub(super) async fn connect(address: std::net::SocketAddr) -> TestResult<Self> {
        let mut stream = timeout(DEADLINE, TcpStream::connect(address)).await??;
        stream.set_nodelay(true)?;
        timeout(
            DEADLINE,
            write_protocol_header(&mut stream, ProtocolHeader::AMQP),
        )
        .await??;
        assert_eq!(
            timeout(DEADLINE, read_protocol_header(&mut stream)).await??,
            ProtocolHeader::AMQP
        );
        let mut open = Open::new("unsupported-transaction-peer");
        // Input channels may exceed our outgoing range: mappings must be exact.
        open.channel_max = 1;
        timeout(
            DEADLINE,
            write_frame(
                &mut stream,
                &Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Open(open)),
                    payload: vec![],
                },
            ),
        )
        .await??;
        assert!(matches!(
            timeout(DEADLINE, read_frame(&mut stream)).await??,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        Ok(Self {
            stream,
            channels: HashMap::new(),
            handles: HashMap::new(),
            sent: HashMap::new(),
            received: HashMap::new(),
            published: HashMap::new(),
        })
    }

    pub(super) async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> TestResult {
        if matches!(&performative, Performative::Transfer(_)) {
            let count = self.sent.entry(channel).or_default();
            *count = count.wrapping_add(1);
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
            let count = self.received.entry(*channel).or_default();
            *count = count.wrapping_add(1);
        }
        Ok(frame)
    }

    pub(super) fn local(&self, peer: u16) -> u16 {
        self.channels[&peer]
    }

    fn harmless_flow(&self, frame: &Frame) -> bool {
        matches!(frame, Frame::Amqp { channel, performative: Some(Performative::Flow(flow)), payload }
            if self.channels.values().any(|known| known == channel) && payload.is_empty()
                && flow.handle.is_none_or(|handle| self.handles.get(channel).is_some_and(|known| known.contains(&handle))))
    }

    pub(super) async fn begin(&mut self, channel: u16, expected_local: u16) -> TestResult {
        self.begin_with_handle_max(channel, expected_local, 0).await
    }

    pub(super) async fn begin_with_handle_max(
        &mut self,
        channel: u16,
        expected_local: u16,
        handle_max: u32,
    ) -> TestResult {
        self.sent.insert(channel, 0);
        self.received.insert(expected_local, 0);
        self.published.insert(channel, 0);
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
                channel: local,
                performative: Some(Performative::Begin(begin)),
                payload,
            } = &frame
            {
                assert_eq!(*local, expected_local);
                assert_eq!(begin.remote_channel, Some(channel));
                assert!(payload.is_empty());
                self.channels.insert(channel, *local);
                self.handles.insert(*local, HashSet::new());
                return Ok(());
            }
            assert!(
                self.harmless_flow(&frame),
                "unexpected Begin response: {frame:?}"
            );
        }
        panic!("Begin exceeded bounded frame count")
    }

    pub(super) async fn attach(
        &mut self,
        channel: u16,
        handle: u32,
        role: Role,
        target: Option<TargetTerminus>,
    ) -> TestResult {
        self.send(
            channel,
            Performative::Attach(Box::new(Attach {
                name: format!("transaction-{channel}-{handle}"),
                handle,
                role: role.clone(),
                snd_settle_mode: SenderSettleMode::Unsettled,
                rcv_settle_mode: ReceiverSettleMode::First,
                source: (role == Role::Receiver).then(|| Source::new("orders")),
                target,
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: (role == Role::Sender).then_some(0),
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            })),
            vec![],
        )
        .await
    }

    pub(super) async fn ordinary(&mut self, channel: u16, handle: u32, role: Role) -> TestResult {
        self.ordinary_at(channel, handle, role, 0).await
    }

    pub(super) async fn ordinary_at(
        &mut self,
        channel: u16,
        handle: u32,
        role: Role,
        local_handle: u32,
    ) -> TestResult {
        self.attach(
            channel,
            handle,
            role.clone(),
            Some(Target::new("orders").into()),
        )
        .await?;
        let local = self.local(channel);
        let mut attached = false;
        let mut credited = role == Role::Receiver;
        for _ in 0..32 {
            let frame = self.read().await?;
            match &frame {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Attach(attach)),
                    ..
                } if *actual == local => {
                    assert_eq!(attach.handle, local_handle);
                    assert_eq!(attach.role, role.opposite());
                    assert!(
                        attach
                            .target
                            .as_ref()
                            .and_then(TargetTerminus::as_target)
                            .is_some()
                    );
                    self.handles.entry(local).or_default().insert(attach.handle);
                    attached = true;
                }
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if *actual == local && flow.handle == Some(local_handle) => {
                    assert!(
                        role == Role::Sender && flow.link_credit.is_some_and(|credit| credit > 0)
                    );
                    credited = true;
                }
                _ => assert!(
                    self.harmless_flow(&frame),
                    "ordinary Attach response: {frame:?}"
                ),
            }
            if attached && credited {
                return Ok(());
            }
        }
        panic!("Attach exceeded bounded frame count")
    }

    pub(super) async fn healthy_session(&mut self) -> TestResult {
        self.begin(HEALTHY, 1).await?;
        self.ordinary(HEALTHY, HEALTHY_HANDLE, Role::Sender).await
    }

    pub(super) async fn refused(&mut self, channel: u16) -> TestResult {
        let local = self.local(channel);
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel: actual,
                performative: Some(Performative::End(end)),
                payload,
            } = &frame
            {
                assert_eq!(*actual, local, "refusal reached the wrong mapped session");
                assert!(payload.is_empty());
                assert_eq!(
                    end.error
                        .as_ref()
                        .expect("unsupported error")
                        .condition
                        .as_symbol(),
                    Symbol::from("amqp:not-implemented")
                );
                self.send(channel, Performative::End(End::default()), vec![])
                    .await?;
                return Ok(());
            }
            assert!(
                self.harmless_flow(&frame),
                "unsupported transaction must not produce ordinary acknowledgment or admission: {frame:?}"
            );
        }
        panic!("unsupported refusal exceeded bounded frame count")
    }

    pub(super) async fn detached(
        &mut self,
        channel: u16,
        peer_handle: u32,
        local_handle: u32,
    ) -> TestResult {
        let local = self.local(channel);
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Detach(detach)),
                payload,
            } = &frame
            {
                assert_eq!(*actual, local);
                assert_eq!(detach.handle, local_handle);
                assert!(detach.closed);
                assert!(payload.is_empty());
                assert_eq!(
                    detach
                        .error
                        .as_ref()
                        .expect("transactional acquisition refused")
                        .condition
                        .as_symbol(),
                    Symbol::from("amqp:not-implemented")
                );
                self.send(
                    channel,
                    Performative::Detach(amqp::Detach {
                        handle: peer_handle,
                        closed: true,
                        error: None,
                    }),
                    vec![],
                )
                .await?;
                return Ok(());
            }
            assert!(
                self.harmless_flow(&frame),
                "txn-id Flow must refuse only the installed link: {frame:?}"
            );
        }
        panic!("Detach exceeded bounded frame count")
    }

    pub(super) async fn barrier(&mut self, channel: u16) -> TestResult {
        let local = self.local(channel);
        let sent = self.sent[&channel];
        let received = self.received[&local];
        self.send(
            channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(received),
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
                if *actual == local && flow.handle.is_none() && flow.next_incoming_id == Some(sent) && payload.is_empty())
            {
                return Ok(());
            }
            assert!(
                self.harmless_flow(&frame),
                "session failed after link-scoped refusal: {frame:?}"
            );
        }
        panic!("Flow barrier exceeded bounded frame count")
    }

    pub(super) async fn publish(&mut self, channel: u16, handle: u32, id: &str) -> TestResult {
        let delivery_id = self.published[&channel];
        self.published.insert(channel, delivery_id.wrapping_add(1));
        self.send(
            channel,
            Performative::Transfer(transfer(handle, Some(delivery_id), false, None)),
            encode_message(&message(id))?,
        )
        .await?;
        let local = self.local(channel);
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Disposition(disposition)),
                ..
            } = &frame
            {
                assert_eq!(*actual, local);
                assert_eq!(disposition.role, Role::Receiver);
                assert_eq!(disposition.first, delivery_id);
                assert!(disposition.last.is_none_or(|last| last == delivery_id));
                assert!(disposition.settled);
                assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
                return Ok(());
            }
            assert!(
                self.harmless_flow(&frame),
                "healthy publication refused: {frame:?}"
            );
        }
        panic!("healthy publication exceeded bounded frame count")
    }

    pub(super) async fn delivery(&mut self, channel: u16, handle: u32) -> TestResult<u32> {
        let local = self.local(channel);
        self.send(
            channel,
            Performative::Flow(Flow {
                handle: Some(handle),
                delivery_count: Some(0),
                link_credit: Some(1),
                incoming_window: 2_048,
                outgoing_window: 2_048,
                ..Flow::default()
            }),
            vec![],
        )
        .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Transfer(transfer)),
                payload,
            } = &frame
            {
                assert_eq!(*actual, local);
                assert_eq!(transfer.handle, 0);
                assert!(!transfer.more);
                assert!(!payload.is_empty());
                assert_ne!(transfer.settled, Some(true));
                return Ok(transfer.delivery_id.expect("server delivery id"));
            }
            assert!(
                self.harmless_flow(&frame),
                "expected held delivery: {frame:?}"
            );
        }
        panic!("delivery exceeded bounded frame count")
    }

    pub(super) async fn close(mut self) -> TestResult {
        self.send(0, Performative::Close(Close::default()), vec![])
            .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if matches!(
                frame,
                Frame::Amqp {
                    performative: Some(Performative::Close(_)),
                    ..
                }
            ) {
                return Ok(());
            }
            assert!(self.harmless_flow(&frame), "closing connection: {frame:?}");
        }
        panic!("Close exceeded bounded frame count")
    }
}

pub(super) fn message(id: &str) -> Message {
    Message::builder()
        .properties(Properties {
            message_id: Some(id.into()),
            ..Properties::default()
        })
        .body(Body::Value(Value::String(id.into())))
        .build()
}

pub(super) fn transfer(
    handle: u32,
    delivery_id: Option<u32>,
    more: bool,
    state: Option<DeliveryState>,
) -> Transfer {
    Transfer {
        handle,
        delivery_id,
        delivery_tag: delivery_id.map(|id| id.to_be_bytes().to_vec().into()),
        message_format: delivery_id.map(|_| 0),
        settled: delivery_id.map(|_| false),
        more,
        rcv_settle_mode: None,
        state,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

pub(super) fn transactional(outcome: Option<Outcome>) -> TestResult<DeliveryState> {
    Ok(DeliveryState::Transactional(TransactionalState {
        txn_id: TransactionId::new([1, 2, 3])?,
        outcome,
    }))
}
