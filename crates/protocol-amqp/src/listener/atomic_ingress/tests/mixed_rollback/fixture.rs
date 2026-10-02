use std::{collections::HashMap, time::Duration};

use amqp::{
    Accepted, Attach, Begin, ConnectionOptions, Coordinator, CoordinatorEndpoint, Declare,
    DeliveryState, Discharge, Disposition, Flow, Frame, Message, NativeTransactionIdentity, Open,
    Outcome, Performative, ProtocolHeader, ReceiverSettleMode, Role, SenderSettleMode,
    SentDelivery, ServerConnection, ServerSession, Source, Symbol, Target, TransactionCommand,
    TransactionId, TransactionPostingReceipt, TransactionRetirementReceipt,
    TransactionalDisposition, TransactionalIngress, TransactionalReceiver, TransactionalSender,
    TransactionalState, Transfer, Value, encode_message, read_frame, read_protocol_header,
    write_frame, write_protocol_header,
};
use domain::{
    EntityBinding, EntityIncarnationKind, EntityPath, LockToken, NamespaceName, QueueConfig,
    SequenceNumber,
};
use tokio::{
    io::DuplexStream,
    sync::{mpsc, oneshot},
    time::timeout,
};

use super::super::super::{Event, QueueAdmission, WorkerClose, groups::HeldDelivery, owner::Owner};
use super::super::recorder::Recorder;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(super) const DEADLINE: Duration = Duration::from_secs(5);
pub(super) const CONTROL: u16 = 19;
pub(super) const POST: u16 = 23;
pub(super) const SEND: u16 = 27;
const HANDLE: u32 = 17;

pub(super) struct Fixture {
    pub(super) owner: Owner<Recorder>,
    pub(super) recorder: Recorder,
    pub(super) connection: ServerConnection,
    pub(super) coordinator: CoordinatorEndpoint,
    pub(super) receiver: TransactionalReceiver,
    pub(super) sender: TransactionalSender,
    pub(super) peer: Peer,
    pub(super) observer: Option<NativeTransactionIdentity>,
    _close: Vec<mpsc::Receiver<WorkerClose>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.owner.close();
    }
}

pub(super) struct Peer {
    io: DuplexStream,
    sent: HashMap<u16, u32>,
}

impl Peer {
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
                &mut self.io,
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

    pub(super) async fn frame(&mut self) -> TestResult<Frame> {
        Ok(timeout(DEADLINE, read_frame(&mut self.io)).await??)
    }

    pub(super) async fn non_flow(&mut self) -> TestResult<Frame> {
        for _ in 0..32 {
            let frame = self.frame().await?;
            if matches!(&frame, Frame::Amqp {
                channel: CONTROL | POST | SEND,
                performative: Some(Performative::Flow(_)), payload,
            } if payload.is_empty())
            {
                continue;
            }
            return Ok(frame);
        }
        Err("bounded native response missing".into())
    }

    pub(super) async fn disposition(
        &mut self,
        channel: u16,
        role: Role,
        id: u32,
    ) -> TestResult<Disposition> {
        let Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Disposition(disposition)),
            payload,
        } = self.non_flow().await?
        else {
            return Err("native disposition missing".into());
        };
        assert_eq!(actual, channel);
        assert_eq!(disposition.role, role);
        assert_eq!(disposition.first, id);
        assert!(disposition.last.is_none());
        assert!(payload.is_empty());
        Ok(disposition)
    }

    async fn control(&mut self, id: u32, command: TransactionCommand) -> TestResult {
        let message = Message {
            body: amqp::Body::Value(Value::from(command)),
            ..Message::default()
        };
        self.send(
            CONTROL,
            Performative::Transfer(transfer(id, None)),
            encode_message(&message)?,
        )
        .await
    }

    pub(super) async fn barrier(&mut self) -> TestResult {
        self.send(
            CONTROL,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 1_000,
                next_outgoing_id: self.sent.get(&CONTROL).copied().unwrap_or(0),
                outgoing_window: 1_000,
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
        .await?;
        for _ in 0..32 {
            match self.frame().await? {
                Frame::Amqp {
                    channel: CONTROL,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } if flow.handle.is_none() && payload.is_empty() => return Ok(()),
                Frame::Amqp {
                    channel: CONTROL | POST | SEND,
                    performative: Some(Performative::Flow(_)),
                    payload,
                } if payload.is_empty() => {}
                frame => return Err(format!("unexpected barrier frame: {frame:?}").into()),
            }
        }
        Err("native barrier missing".into())
    }
}

fn transfer(id: u32, transaction: Option<TransactionId>) -> Transfer {
    Transfer {
        handle: HANDLE,
        delivery_id: Some(id),
        delivery_tag: Some(id.to_be_bytes().to_vec().into()),
        message_format: Some(0),
        settled: Some(false),
        more: false,
        rcv_settle_mode: None,
        state: transaction.map(|txn_id| {
            DeliveryState::Transactional(TransactionalState {
                txn_id,
                outcome: None,
            })
        }),
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn attach(name: &str, role: Role) -> Attach {
    let sender = role == Role::Sender;
    Attach {
        name: name.to_owned(),
        handle: HANDLE,
        role,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: if sender {
            ReceiverSettleMode::First
        } else {
            ReceiverSettleMode::Second
        },
        source: Some(Source::new("orders")),
        target: Some(Target::new("orders").into()),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: sender.then_some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

async fn session(
    connection: &mut ServerConnection,
    peer: &mut Peer,
    channel: u16,
) -> TestResult<ServerSession> {
    peer.send(channel, Performative::Begin(Begin::default()), Vec::new())
        .await?;
    let incoming = timeout(DEADLINE, connection.next_incoming_session())
        .await?
        .ok_or("session missing")?;
    let (session, response) = tokio::join!(connection.accept_session(incoming), peer.non_flow());
    assert!(
        matches!(response?, Frame::Amqp { channel: actual, performative: Some(Performative::Begin(begin)), payload }
        if actual == channel && begin.remote_channel == Some(channel) && payload.is_empty())
    );
    Ok(session?)
}

async fn attached(peer: &mut Peer, channel: u16, receiving: bool) -> TestResult {
    assert!(matches!(peer.non_flow().await?, Frame::Amqp {
        channel: actual, performative: Some(Performative::Attach(attach)), payload,
    } if actual == channel && attach.handle == HANDLE && payload.is_empty()));
    if receiving {
        assert!(matches!(peer.frame().await?, Frame::Amqp {
            channel: actual, performative: Some(Performative::Flow(flow)), payload,
        } if actual == channel && flow.handle == Some(HANDLE)
            && flow.link_credit.is_some_and(|credit| credit > 0) && payload.is_empty()));
    }
    Ok(())
}

impl Fixture {
    pub(super) async fn new() -> TestResult<Self> {
        timeout(DEADLINE, async {
            let (server, io) = tokio::io::duplex(16_384);
            let mut peer = Peer {
                io,
                sent: HashMap::new(),
            };
            let opening = async {
                write_protocol_header(&mut peer.io, ProtocolHeader::AMQP).await?;
                assert_eq!(
                    read_protocol_header(&mut peer.io).await?,
                    ProtocolHeader::AMQP
                );
                peer.send(
                    0,
                    Performative::Open(Open::new("mixed-owner-peer")),
                    Vec::new(),
                )
                .await?;
                assert!(matches!(
                    peer.frame().await?,
                    Frame::Amqp {
                        performative: Some(Performative::Open(_)),
                        ..
                    }
                ));
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(peer)
            };
            let (connection, peer) = tokio::join!(
                ServerConnection::accept_with_transactional_work(
                    server,
                    "mixed-owner-server",
                    None,
                    ConnectionOptions::default()
                ),
                opening,
            );
            let mut connection = connection?;
            let mut peer = peer?;
            let mut control = session(&mut connection, &mut peer, CONTROL).await?;
            let mut request = attach("control", Role::Sender);
            request.target = Some(Coordinator::default().into());
            request.source = Some(Source {
                outcomes: Some(
                    vec![
                        Symbol::from("amqp:declared:list"),
                        Symbol::from("amqp:accepted:list"),
                        Symbol::from("amqp:rejected:list"),
                    ]
                    .into(),
                ),
                ..Source::default()
            });
            peer.send(CONTROL, Performative::Attach(Box::new(request)), Vec::new())
                .await?;
            let incoming = control
                .next_incoming_attach()
                .await
                .ok_or("coordinator attach missing")?;
            let (coordinator, response) = tokio::join!(
                control.accept_coordinator(incoming, 0),
                attached(&mut peer, CONTROL, true)
            );
            response?;
            let coordinator = coordinator?;
            let mut posting = session(&mut connection, &mut peer, POST).await?;
            peer.send(
                POST,
                Performative::Attach(Box::new(attach("posting", Role::Sender))),
                Vec::new(),
            )
            .await?;
            let incoming = posting
                .next_incoming_attach()
                .await
                .ok_or("posting attach missing")?;
            let (receiver, response) = tokio::join!(
                posting.accept_transactional_receiver(incoming, 0),
                attached(&mut peer, POST, true)
            );
            response?;
            let receiver = receiver?;
            let mut retiring = session(&mut connection, &mut peer, SEND).await?;
            peer.send(
                SEND,
                Performative::Attach(Box::new(attach("retirement", Role::Receiver))),
                Vec::new(),
            )
            .await?;
            let incoming = retiring
                .next_incoming_attach()
                .await
                .ok_or("retirement attach missing")?;
            let (sender, response) = tokio::join!(
                retiring.accept_transactional_sender(incoming, 0),
                attached(&mut peer, SEND, false)
            );
            response?;
            let sender = sender?;
            peer.send(
                SEND,
                Performative::Flow(Flow {
                    next_incoming_id: Some(0),
                    incoming_window: 1_000,
                    next_outgoing_id: 0,
                    outgoing_window: 1_000,
                    handle: Some(HANDLE),
                    delivery_count: Some(0),
                    link_credit: Some(10),
                    available: None,
                    drain: false,
                    echo: false,
                    properties: None,
                }),
                Vec::new(),
            )
            .await?;
            peer.barrier().await?;
            let recorder = Recorder::default();
            let mut owner = Owner::new(connection.connection_identity().clone(), recorder.clone());
            let binding = EntityBinding::new(
                NamespaceName::new("tenant")?,
                EntityPath::new("orders")?,
                EntityPath::new("orders")?,
                EntityIncarnationKind::Queue,
                7,
            )?;
            let mut close_commands = Vec::new();
            let (close, commands) = mpsc::channel(1);
            close_commands.push(commands);
            let (reply, registered) = oneshot::channel();
            owner.process(Event::RegisterController {
                identity: coordinator.controller_identity().clone(),
                authorization: None,
                close,
                reply,
            });
            registered.await?.expect("actual controller registration");
            let (close, commands) = mpsc::channel(1);
            close_commands.push(commands);
            let (reply, registered) = oneshot::channel();
            owner.process(Event::RegisterProducer {
                identity: receiver.receiver_identity(),
                admission: QueueAdmission {
                    binding: binding.clone(),
                    config: QueueConfig::default(),
                },
                authorization: None,
                close,
                reply,
            });
            registered.await?.expect("actual producer registration");
            let (close, commands) = mpsc::channel(1);
            close_commands.push(commands);
            let (reply, registered) = oneshot::channel();
            owner.process(Event::RegisterConsumer {
                identity: sender.sender_identity(),
                admission: QueueAdmission {
                    binding,
                    config: QueueConfig::default(),
                },
                authorization: None,
                close,
                reply,
            });
            registered.await?.expect("actual consumer registration");
            Ok(Self {
                owner,
                recorder,
                connection,
                coordinator,
                receiver,
                sender,
                peer,
                observer: None,
                _close: close_commands,
            })
        })
        .await?
    }

    pub(super) async fn declare(&mut self, id: u32) -> TestResult<TransactionId> {
        self.peer
            .control(id, TransactionCommand::Declare(Declare::default()))
            .await?;
        let request = timeout(DEADLINE, self.coordinator.recv()).await??;
        self.owner.process(Event::Control {
            source: self.coordinator.controller_identity().clone(),
            request,
        });
        let (operation, response) = tokio::join!(
            timeout(DEADLINE, self.owner.next_operation()),
            self.peer.disposition(CONTROL, Role::Receiver, id)
        );
        let operation = operation?.ok_or("declaration completion missing")?;
        if let super::super::super::operations::Operation::Declared {
            result: Ok(identity),
            ..
        } = &operation
        {
            self.observer = Some(identity.clone());
        }
        self.owner.accept_completion(operation);
        let Some(DeliveryState::Declared(declared)) = response?.state else {
            return Err("Declared outcome missing".into());
        };
        Ok(declared.txn_id)
    }

    pub(super) async fn held(&mut self) -> TestResult<SentDelivery> {
        let message = Message::data(b"held native generation".to_vec());
        let (sent, frame) = timeout(DEADLINE, async {
            tokio::join!(
                self.sender
                    .send_with_dispositions(message, b"held-original".to_vec().into()),
                self.peer.non_flow()
            )
        })
        .await?;
        assert!(
            matches!(frame?, Frame::Amqp { channel: SEND, performative: Some(Performative::Transfer(transfer)), payload }
            if transfer.delivery_id == Some(0) && !transfer.more && !payload.is_empty())
        );
        let sent = sent?;
        let (reply, registered) = oneshot::channel();
        self.owner.process(Event::RegisterHeld {
            source: self.sender.sender_identity(),
            delivery: HeldDelivery {
                sequence: SequenceNumber::new(2),
                token: LockToken::new(3),
                original: sent.delivery_identity().clone(),
            },
            reply,
        });
        registered.await?.expect("actual held registration");
        Ok(sent)
    }

    pub(super) async fn retirement(
        &mut self,
        sent: &mut SentDelivery,
        transaction: &TransactionId,
    ) -> TestResult<TransactionRetirementReceipt> {
        self.peer
            .send(
                SEND,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: 0,
                    last: None,
                    settled: false,
                    state: Some(DeliveryState::Transactional(TransactionalState {
                        txn_id: transaction.clone(),
                        outcome: Some(Outcome::Accepted(Accepted)),
                    })),
                    batchable: false,
                }),
                Vec::new(),
            )
            .await?;
        let TransactionalDisposition::Retirement(receipt) =
            timeout(DEADLINE, sent.next_disposition()).await??
        else {
            return Err("actual retirement receipt missing".into());
        };
        Ok(receipt)
    }

    pub(super) async fn posting(
        &mut self,
        transaction: &TransactionId,
        id: u32,
    ) -> TestResult<TransactionPostingReceipt> {
        self.peer
            .send(
                POST,
                Performative::Transfer(transfer(id, Some(transaction.clone()))),
                encode_message(&Message::data(b"delayed posting".to_vec()))?,
            )
            .await?;
        let TransactionalIngress::Posting(receipt) =
            timeout(DEADLINE, self.receiver.recv()).await??
        else {
            return Err("actual posting receipt missing".into());
        };
        Ok(receipt)
    }

    pub(super) async fn discharge(
        &mut self,
        transaction: &TransactionId,
        id: u32,
        fail: bool,
    ) -> TestResult {
        self.peer
            .control(
                id,
                TransactionCommand::Discharge(Discharge {
                    txn_id: transaction.clone(),
                    fail: Some(fail),
                }),
            )
            .await?;
        let request = timeout(DEADLINE, self.coordinator.recv()).await??;
        self.owner.process(Event::Control {
            source: self.coordinator.controller_identity().clone(),
            request,
        });
        Ok(())
    }

    pub(super) async fn shutdown(&mut self) -> TestResult {
        self.owner.close();
        timeout(DEADLINE, self.connection.shutdown()).await?;
        Ok(())
    }
}
