//! Second-phase settlement confirms the broker result, not the client request.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use amqp::{
    Attach, Begin, DeliveryState, Error as AmqpError, ErrorCondition, Fields, Flow, Frame, Open,
    Performative, ProtocolHeader, ReceiverSettleMode, Rejected, Role, SenderSettleMode, Source,
    Symbol, Value, decode_message, read_frame, read_protocol_header, write_frame,
    write_protocol_header,
};
use domain::{
    BrokerError, CommandKind, CommandOutcome, Delivery, DeliveryLock, EntityBinding,
    EntityIncarnationKind, EntityPath, LockToken, MessageStatus, MessageValue, NamespaceName,
    SequenceNumber, SettlementDisposition, Timestamp,
};
use protocol_amqp::{
    AmqpListener, Attachment, Broker, BrokerRejection, EntityAdmission, EntityMetadata,
};
use tokio::{
    net::{TcpListener, TcpStream, tcp::OwnedWriteHalf},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Decision = Result<CommandOutcome, BrokerRejection>;

#[derive(Clone)]
struct ControlledBroker(Arc<BrokerState>);

struct BrokerState {
    delivered: AtomicBool,
    started: mpsc::Sender<CommandKind>,
    decision: Mutex<Option<oneshot::Receiver<Decision>>>,
}

impl Broker for ControlledBroker {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        let entity = target.canonical_entity().expect("fixture target");
        Ok(self
            .entity_metadata(namespace.clone(), target)
            .await?
            .map(|metadata| EntityAdmission {
                metadata,
                binding: EntityBinding::new(
                    namespace,
                    entity.clone(),
                    entity,
                    EntityIncarnationKind::Queue,
                    1,
                )
                .expect("fixture binding"),
            }))
    }

    async fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Decision {
        self.submit(binding.namespace().clone(), entity, kind).await
    }

    async fn rules_fenced(
        &self,
        binding: EntityBinding,
        topic: EntityPath,
        subscription: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        self.rules(binding.namespace().clone(), topic, subscription)
            .await
    }

    async fn rules(
        &self,
        _namespace: NamespaceName,
        _topic: EntityPath,
        _subscription: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        Err(BrokerRejection::Unavailable("unexpected rule read".into()))
    }

    async fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        Ok((namespace.as_str() == "tenant"
            && matches!(target, Attachment::Queue(entity) if entity.as_str() == "orders"))
        .then_some(EntityMetadata::Queue(domain::QueueConfig::default())))
    }

    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Decision {
        assert_eq!(namespace.as_str(), "tenant");
        assert_eq!(entity.as_str(), "orders");
        match kind {
            CommandKind::Receive { .. } => {
                if self.0.delivered.swap(true, Ordering::SeqCst) {
                    return Ok(CommandOutcome::Received(None));
                }
                Ok(CommandOutcome::Received(Some(Delivery {
                    sequence: SequenceNumber::new(1),
                    message_id: "held-message".to_owned(),
                    body: b"original payload".to_vec(),
                    enqueued_at: Timestamp::from_millis(1_000),
                    expires_at: None,
                    time_to_live_millis: None,
                    envelope: None,
                    delivery_count: 1,
                    status: MessageStatus::Active,
                    scheduled_enqueue_time: None,
                    lock: Some(DeliveryLock {
                        token: LockToken::new(7),
                        locked_until: Timestamp::from_millis(61_000),
                    }),
                    session_id: None,
                    dead_letter: None,
                })))
            }
            kind @ CommandKind::Settle { .. } => {
                self.0
                    .started
                    .send(kind)
                    .await
                    .expect("the test observes the command");
                let decision = self
                    .0
                    .decision
                    .lock()
                    .expect("decision mutex")
                    .take()
                    .expect("only one settlement is submitted");
                decision
                    .await
                    .expect("the test releases the broker decision")
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    async fn deliverable(&self, _namespace: &NamespaceName, _entity: &EntityPath) {
        std::future::pending::<()>().await;
    }
}

struct Fixture {
    peer: Peer,
    started: mpsc::Receiver<CommandKind>,
    decision: Option<oneshot::Sender<Decision>>,
    listener: JoinHandle<()>,
    delivery_id: u32,
}

impl Fixture {
    async fn start() -> TestResult<Self> {
        let (started_tx, started) = mpsc::channel(1);
        let (decision_tx, decision_rx) = oneshot::channel();
        let broker = ControlledBroker(Arc::new(BrokerState {
            delivered: AtomicBool::new(false),
            started: started_tx,
            decision: Mutex::new(Some(decision_rx)),
        }));
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?;
        let listener = tokio::spawn(async move {
            let _ = AmqpListener::new(broker, NamespaceName::new("tenant").expect("namespace"))
                .serve(socket)
                .await;
        });
        let mut peer = Peer::connect(address).await?;
        let delivery_id = peer.receive_one().await?;
        Ok(Self {
            peer,
            started,
            decision: Some(decision_tx),
            listener,
            delivery_id,
        })
    }

    async fn reject_with(&mut self, info: Fields) -> TestResult {
        self.peer
            .send(Performative::Disposition(amqp::Disposition {
                role: Role::Receiver,
                first: self.delivery_id,
                last: None,
                settled: false,
                state: Some(DeliveryState::Rejected(Rejected {
                    error: Some(AmqpError {
                        condition: ErrorCondition::Custom(Symbol::from(
                            "com.microsoft:dead-letter",
                        )),
                        description: None,
                        info: Some(info),
                    }),
                })),
                batchable: false,
            }))
            .await
    }

    async fn command(&mut self) -> TestResult<CommandKind> {
        Ok(
            tokio::time::timeout(Duration::from_secs(2), self.started.recv())
                .await?
                .expect("the listener submitted a settlement"),
        )
    }

    async fn assert_not_confirmed(&mut self) {
        assert!(
            tokio::time::timeout(Duration::from_millis(100), self.peer.disposition())
                .await
                .is_err(),
            "settlement must not be acknowledged before the broker decides"
        );
    }

    fn release(&mut self, decision: Decision) {
        self.decision
            .take()
            .expect("one broker decision")
            .send(decision)
            .expect("the listener still awaits the broker");
    }

    async fn acknowledgement(&mut self) -> TestResult<amqp::Disposition> {
        let disposition =
            tokio::time::timeout(Duration::from_secs(2), self.peer.disposition()).await??;
        assert_eq!(disposition.role, Role::Sender);
        assert_eq!(disposition.first, self.delivery_id);
        assert!(disposition.last.is_none() || disposition.last == Some(self.delivery_id));
        assert!(disposition.settled);
        Ok(disposition)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

struct Peer {
    writer: OwnedWriteHalf,
    frames: mpsc::Receiver<std::io::Result<Frame>>,
    reader: JoinHandle<()>,
}

impl Peer {
    async fn connect(address: std::net::SocketAddr) -> TestResult<Self> {
        let mut socket = TcpStream::connect(address).await?;
        write_protocol_header(&mut socket, ProtocolHeader::AMQP).await?;
        assert_eq!(
            read_protocol_header(&mut socket).await?,
            ProtocolHeader::AMQP
        );
        let (mut reader, writer) = socket.into_split();
        let (frames_tx, frames) = mpsc::channel(16);
        let reader = tokio::spawn(async move {
            loop {
                let frame = read_frame(&mut reader).await;
                let failed = frame.is_err();
                if frames_tx.send(frame).await.is_err() || failed {
                    break;
                }
            }
        });
        let mut peer = Self {
            writer,
            frames,
            reader,
        };
        peer.send(Performative::Open(Open::new("raw-settlement-client")))
            .await?;
        assert!(matches!(
            peer.next().await?,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        peer.send(Performative::Begin(Begin::default())).await?;
        assert!(matches!(
            peer.next().await?,
            Frame::Amqp {
                performative: Some(Performative::Begin(_)),
                ..
            }
        ));
        peer.send(Performative::Attach(Box::new(Attach {
            name: "raw-settlement-receiver".to_owned(),
            handle: 0,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::Second,
            source: Some(Source::new("orders")),
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        })))
        .await?;
        loop {
            if let Frame::Amqp {
                performative: Some(Performative::Attach(attach)),
                ..
            } = peer.next().await?
            {
                assert_eq!(attach.role, Role::Sender);
                assert_eq!(attach.rcv_settle_mode, ReceiverSettleMode::Second);
                break;
            }
        }
        peer.send(Performative::Flow(Flow {
            incoming_window: 2_048,
            outgoing_window: 2_048,
            handle: Some(0),
            delivery_count: Some(0),
            link_credit: Some(1),
            ..Flow::default()
        }))
        .await?;
        Ok(peer)
    }

    async fn send(&mut self, performative: Performative) -> TestResult {
        write_frame(
            &mut self.writer,
            &Frame::Amqp {
                channel: 0,
                performative: Some(performative),
                payload: Vec::new(),
            },
        )
        .await?;
        Ok(())
    }

    async fn next(&mut self) -> TestResult<Frame> {
        // Keep frame reads in their own task, so a cancelled no-ack assertion
        // cannot consume half a frame and corrupt the following read.
        Ok(
            tokio::time::timeout(Duration::from_secs(2), self.frames.recv())
                .await?
                .expect("the frame reader stays alive")?,
        )
    }

    async fn receive_one(&mut self) -> TestResult<u32> {
        loop {
            match self.next().await? {
                Frame::Amqp {
                    performative: Some(Performative::Transfer(transfer)),
                    payload,
                    ..
                } => {
                    assert!(!transfer.more);
                    assert_eq!(transfer.settled, Some(false));
                    assert_eq!(
                        decode_message(&payload)?.body,
                        amqp::Body::Data(vec![b"original payload".to_vec().into()])
                    );
                    return Ok(transfer.delivery_id.expect("first transfer carries its ID"));
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)) | None,
                    ..
                } => {}
                other => panic!("unexpected frame before delivery: {other:?}"),
            }
        }
    }

    async fn disposition(&mut self) -> TestResult<amqp::Disposition> {
        loop {
            match self.next().await? {
                Frame::Amqp {
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } => return Ok(disposition),
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)) | None,
                    ..
                } => {}
                other => panic!("unexpected frame before settlement: {other:?}"),
            }
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

fn dead_letter_info() -> Fields {
    let mut info = Fields::new();
    info.insert(
        Symbol::from("DeadLetterReason"),
        Value::String("invalid-order".to_owned()),
    );
    info.insert(
        Symbol::from("DeadLetterErrorDescription"),
        Value::String("the order is incomplete".to_owned()),
    );
    info.insert(Symbol::from("attempts"), Value::Uint(3));
    info
}

fn assert_dead_letter_command(kind: CommandKind) {
    let CommandKind::Settle {
        sequence,
        lock_token,
        disposition,
        properties_to_modify,
    } = kind
    else {
        panic!("the receiver requested a settlement");
    };
    assert_eq!(sequence, SequenceNumber::new(1));
    assert_eq!(lock_token, LockToken::new(7));
    assert_eq!(
        disposition,
        SettlementDisposition::DeadLetter {
            reason: "invalid-order".to_owned(),
            description: "the order is incomplete".to_owned(),
        }
    );
    assert_eq!(
        properties_to_modify,
        std::collections::BTreeMap::from([("attempts".to_owned(), MessageValue::Uint(3))])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_letter_request_is_accepted_only_after_the_broker_approves_it() -> TestResult {
    let mut fixture = Fixture::start().await?;
    fixture.reject_with(dead_letter_info()).await?;
    assert_dead_letter_command(fixture.command().await?);
    fixture.assert_not_confirmed().await;
    fixture.release(Ok(CommandOutcome::DeadLettered));
    assert!(matches!(
        fixture.acknowledgement().await?.state,
        Some(DeliveryState::Accepted(_))
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_dead_letter_request_reports_the_actual_broker_error() -> TestResult {
    let mut fixture = Fixture::start().await?;
    fixture.reject_with(dead_letter_info()).await?;
    assert_dead_letter_command(fixture.command().await?);
    fixture.assert_not_confirmed().await;
    fixture.release(Err(BrokerRejection::Refused(BrokerError::LockExpired {
        sequence: SequenceNumber::new(1),
        locked_until: Timestamp::from_millis(61_000),
    })));
    let Some(DeliveryState::Rejected(rejected)) = fixture.acknowledgement().await?.state else {
        panic!("the failed operation receives a rejected acknowledgement");
    };
    let error = rejected
        .error
        .expect("the acknowledgement carries a failure");
    assert_eq!(
        error.condition.as_symbol().as_str(),
        protocol_amqp::MESSAGE_LOCK_LOST
    );
    assert_ne!(
        error.condition.as_symbol().as_str(),
        "com.microsoft:dead-letter"
    );
    assert!(
        error
            .description
            .expect("broker failure description")
            .contains("expired")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_reserved_update_is_rejected_without_submitting_a_command() -> TestResult {
    let mut fixture = Fixture::start().await?;
    let mut info = dead_letter_info();
    info.insert(Symbol::from("DeadLetterReason"), Value::Uint(3));
    fixture.reject_with(info).await?;
    let Some(DeliveryState::Rejected(rejected)) = fixture.acknowledgement().await?.state else {
        panic!("an invalid update cannot be acknowledged as successful");
    };
    let error = rejected.error.expect("invalid-field error");
    assert_eq!(
        error.condition.as_symbol().as_str(),
        protocol_amqp::INVALID_FIELD
    );
    assert!(matches!(
        fixture.started.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    Ok(())
}
