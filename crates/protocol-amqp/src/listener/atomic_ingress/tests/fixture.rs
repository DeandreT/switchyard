use std::{collections::HashMap, sync::Arc, time::Duration};

use amqp::{
    Attach, Begin, ConnectionOptions, Coordinator, CoordinatorEndpoint, CoordinatorRequest,
    Declare, DeliveryState, Discharge, Disposition, Flow, Frame, Message, Open, Performative,
    ProtocolHeader, ReceiverSettleMode, Role, SenderSettleMode, ServerConnection, ServerSession,
    Source, Symbol, Target, TransactionCommand, TransactionId, TransactionPostingReceipt,
    TransactionalIngress, TransactionalReceiver, TransactionalState, Transfer, Value,
    encode_message, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use domain::{EntityBinding, EntityIncarnationKind, EntityPath, NamespaceName, QueueConfig};
use tokio::{
    io::DuplexStream,
    sync::{mpsc, oneshot},
    time::timeout,
};

use super::super::{Event, QueueAdmission, WorkerClose, owner::Owner};
use super::begin_gate::{BeginGate, GatedIo};
use super::recorder::Recorder;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(super) const DEADLINE: Duration = Duration::from_secs(5);
pub(super) const CONTROL: u16 = 19;
pub(super) const POST: u16 = 23;
const CONTROL_HANDLE: u32 = 17;
const POST_HANDLE: u32 = 31;

pub(super) struct Fixture {
    pub(super) owner: Owner<Recorder>,
    pub(super) recorder: Recorder,
    pub(super) connection: ServerConnection,
    pub(super) peer: Peer,
    pub(super) coordinator: CoordinatorEndpoint,
    pub(super) receiver: TransactionalReceiver,
    pub(super) observer: Option<amqp::NativeTransactionIdentity>,
    pub(super) begin_gate: Arc<BeginGate>,
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
                channel: CONTROL | POST,
                performative: Some(Performative::Flow(_)), payload
            } if payload.is_empty())
            {
                continue;
            }
            return Ok(frame);
        }
        Err("native owner response exceeded bounded Flow allowance".into())
    }

    pub(super) async fn disposition(&mut self, expected: u16, id: u32) -> TestResult<Disposition> {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Disposition(disposition)),
            payload,
        } = self.non_flow().await?
        else {
            return Err("native owner disposition missing".into());
        };
        assert_eq!(channel, expected);
        assert!(payload.is_empty());
        assert_eq!(disposition.role, Role::Receiver);
        assert_eq!(disposition.first, id);
        assert!(disposition.last.is_none());
        Ok(disposition)
    }

    async fn command(&mut self, id: u32, command: TransactionCommand) -> TestResult {
        let message = Message {
            body: amqp::Body::Value(Value::from(command)),
            ..Message::default()
        };
        self.send(
            CONTROL,
            Performative::Transfer(transfer(CONTROL_HANDLE, id, None)),
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
                    channel: CONTROL | POST,
                    performative: Some(Performative::Flow(_)),
                    payload,
                } if payload.is_empty() => {}
                frame => {
                    return Err(format!("unexpected native owner barrier frame: {frame:?}").into());
                }
            }
        }
        Err("native owner barrier missing".into())
    }
}

fn transfer(handle: u32, id: u32, transaction: Option<TransactionId>) -> Transfer {
    Transfer {
        handle,
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

fn sender_attach(name: &str, handle: u32) -> Attach {
    Attach {
        name: name.to_owned(),
        handle,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::default()),
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
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
        .ok_or("native owner session missing")?;
    let (session, frame) = tokio::join!(connection.accept_session(incoming), peer.non_flow());
    let Frame::Amqp {
        channel: actual,
        performative: Some(Performative::Begin(begin)),
        payload,
    } = frame?
    else {
        return Err("native owner Begin missing".into());
    };
    assert_eq!(actual, channel);
    assert_eq!(begin.remote_channel, Some(channel));
    assert!(payload.is_empty());
    Ok(session?)
}

async fn attached(peer: &mut Peer, channel: u16, handle: u32) -> TestResult {
    let Frame::Amqp {
        channel: actual,
        performative: Some(Performative::Attach(attach)),
        payload,
    } = peer.non_flow().await?
    else {
        return Err("native owner Attach missing".into());
    };
    assert_eq!(actual, channel);
    assert_eq!(attach.handle, handle);
    assert!(payload.is_empty());
    let Frame::Amqp {
        channel: actual,
        performative: Some(Performative::Flow(flow)),
        payload,
    } = peer.frame().await?
    else {
        return Err("native owner link credit missing".into());
    };
    assert_eq!(actual, channel);
    assert_eq!(flow.handle, Some(handle));
    assert!(flow.link_credit.is_some_and(|credit| credit > 0));
    assert!(payload.is_empty());
    Ok(())
}

impl Fixture {
    pub(super) async fn new() -> TestResult<Self> {
        timeout(DEADLINE, async {
            let (server, io) = tokio::io::duplex(16_384);
            let begin_gate = Arc::new(BeginGate::default());
            let server = GatedIo::new(server, Arc::clone(&begin_gate));
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
                    Performative::Open(Open::new("native-owner-order-peer")),
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
                ServerConnection::accept_with_transactional_ingress(
                    server,
                    "native-owner-order-server",
                    None,
                    ConnectionOptions::default()
                ),
                opening
            );
            let mut connection = connection?;
            let mut peer = peer?;
            let mut control = session(&mut connection, &mut peer, CONTROL).await?;
            let mut attach = sender_attach("control", CONTROL_HANDLE);
            attach.target = Some(Coordinator::default().into());
            attach.source = Some(Source {
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
            peer.send(CONTROL, Performative::Attach(Box::new(attach)), Vec::new())
                .await?;
            let incoming = control
                .next_incoming_attach()
                .await
                .ok_or("native owner coordinator missing")?;
            let (coordinator, response) = tokio::join!(
                control.accept_coordinator(incoming, 0),
                attached(&mut peer, CONTROL, CONTROL_HANDLE)
            );
            response?;
            let coordinator = coordinator?;
            let mut data = session(&mut connection, &mut peer, POST).await?;
            let mut attach = sender_attach("data", POST_HANDLE);
            attach.target = Some(Target::new("orders").into());
            peer.send(POST, Performative::Attach(Box::new(attach)), Vec::new())
                .await?;
            let incoming = data
                .next_incoming_attach()
                .await
                .ok_or("native owner receiver missing")?;
            let (receiver, response) = tokio::join!(
                data.accept_transactional_receiver(incoming, 0),
                attached(&mut peer, POST, POST_HANDLE)
            );
            response?;
            let receiver = receiver?;
            let recorder = Recorder::default();
            let mut owner = Owner::new(connection.connection_identity().clone(), recorder.clone());
            let (close_controller, controller_commands) = mpsc::channel(1);
            let (reply, registered) = oneshot::channel();
            owner.process(Event::RegisterController {
                identity: coordinator.controller_identity().clone(),
                close: close_controller,
                reply,
            });
            registered.await?.expect("actual controller registration");
            let namespace = NamespaceName::new("tenant")?;
            let entity = EntityPath::new("orders")?;
            let binding = EntityBinding::new(
                namespace,
                entity.clone(),
                entity,
                EntityIncarnationKind::Queue,
                7,
            )?;
            let (close_producer, producer_commands) = mpsc::channel(1);
            let (reply, registered) = oneshot::channel();
            owner.process(Event::RegisterProducer {
                identity: receiver.receiver_identity(),
                admission: QueueAdmission {
                    binding,
                    config: QueueConfig::default(),
                },
                authorization: None,
                close: close_producer,
                reply,
            });
            registered.await?.expect("actual receiver registration");
            Ok(Self {
                owner,
                recorder,
                connection,
                peer,
                coordinator,
                receiver,
                observer: None,
                begin_gate,
                _close: vec![controller_commands, producer_commands],
            })
        })
        .await?
    }

    pub(super) async fn declare(&mut self) -> TestResult<TransactionId> {
        self.peer
            .command(0, TransactionCommand::Declare(Declare::default()))
            .await?;
        let request = timeout(DEADLINE, self.coordinator.recv()).await??;
        self.owner.process(Event::Control {
            source: self.coordinator.controller_identity().clone(),
            request,
        });
        let (operation, response) = tokio::join!(
            timeout(DEADLINE, self.owner.next_operation()),
            self.peer.disposition(CONTROL, 0)
        );
        let operation = operation?.ok_or("native owner declaration operation missing")?;
        if let super::super::operations::Operation::Declared {
            result: Ok(identity),
            ..
        } = &operation
        {
            self.observer = Some(identity.clone());
        }
        self.owner.accept_completion(operation);
        let response = response?;
        assert!(response.settled);
        let Some(DeliveryState::Declared(declared)) = response.state else {
            return Err("native owner Declared missing".into());
        };
        Ok(declared.txn_id)
    }

    pub(super) async fn posting(
        &mut self,
        transaction: &TransactionId,
        body: &[u8],
    ) -> TestResult<TransactionPostingReceipt> {
        self.peer
            .send(
                POST,
                Performative::Transfer(transfer(POST_HANDLE, 0, Some(transaction.clone()))),
                encode_message(&Message::data(body.to_vec()))?,
            )
            .await?;
        let TransactionalIngress::Posting(posting) =
            timeout(DEADLINE, self.receiver.recv()).await??
        else {
            return Err("native owner posting receipt missing".into());
        };
        Ok(posting)
    }

    pub(super) async fn sealed(
        &mut self,
        transaction: &TransactionId,
    ) -> TestResult<amqp::SealedDischargeReceipt> {
        self.peer
            .command(
                1,
                TransactionCommand::Discharge(Discharge {
                    txn_id: transaction.clone(),
                    fail: Some(false),
                }),
            )
            .await?;
        let CoordinatorRequest::Discharge(sealed) =
            timeout(DEADLINE, self.coordinator.recv()).await??
        else {
            return Err("native owner sealed receipt missing".into());
        };
        Ok(sealed)
    }

    pub(super) fn stage(&mut self, receipt: TransactionPostingReceipt) {
        self.owner.process(Event::Posting {
            source: self.receiver.receiver_identity(),
            receipt,
        });
    }

    pub(super) fn discharge(&mut self, sealed: amqp::SealedDischargeReceipt) {
        self.owner.process(Event::Control {
            source: self.coordinator.controller_identity().clone(),
            request: CoordinatorRequest::Discharge(sealed),
        });
    }

    pub(super) async fn shutdown(&mut self) -> TestResult {
        self.owner.close();
        timeout(DEADLINE, self.connection.shutdown()).await?;
        Ok(())
    }

    pub(super) async fn close_expired_controller(&mut self) -> TestResult {
        let command = timeout(DEADLINE, self._close[0].recv())
            .await?
            .ok_or("controller close command missing")?;
        let WorkerClose::Close(Some(error)) = command else {
            return Err("expired controller error missing".into());
        };
        let expected = error.condition.as_symbol().as_str().to_owned();
        assert_eq!(expected, "amqp:transaction:timeout");
        let (closed, frame) = tokio::join!(
            self.coordinator.close_with_error(error),
            self.peer.non_flow()
        );
        closed?;
        let Frame::Amqp {
            channel: CONTROL,
            performative: Some(Performative::Detach(detach)),
            payload,
        } = frame?
        else {
            return Err("expired controller Detach missing".into());
        };
        assert_eq!(detach.handle, CONTROL_HANDLE);
        assert!(detach.closed);
        assert_eq!(
            detach
                .error
                .ok_or("controller detach error missing")?
                .condition
                .as_symbol()
                .as_str(),
            expected
        );
        assert!(payload.is_empty());
        Ok(())
    }
}
