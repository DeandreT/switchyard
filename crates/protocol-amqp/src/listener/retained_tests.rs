use std::{error::Error as StdError, time::Duration};

use amqp::{
    Accepted, ApplicationProperties, Attach, Begin, Body, Close, ConnectionOptions, DeliveryState,
    Disposition, Flow, Frame, LinkEndpoint, Message, Open, Performative, Properties,
    ProtocolHeader, ReceiverSettleMode, Role, SenderSettleMode, Target, Transfer, encode_message,
    read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use domain::{
    BrokerError, CommandKind, CommandOutcome, EntityBinding, EntityIncarnationKind, EntityPath,
    MessageValue, NamespaceName, SequenceNumber,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

use super::{ServerConnection, serve_open_connection, serve_sending_client};
use crate::{
    Attachment, Broker, BrokerRejection, EntityAdmission, EntityMetadata, broker::BoundBroker,
};

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;
type BrokerReply = Result<CommandOutcome, BrokerRejection>;
const DEADLINE: Duration = Duration::from_secs(5);

struct Submission {
    binding: EntityBinding,
    entity: EntityPath,
    kind: CommandKind,
    reply: oneshot::Sender<BrokerReply>,
}

#[derive(Clone)]
struct GatedBroker {
    submissions: mpsc::Sender<Submission>,
    finished: mpsc::UnboundedSender<()>,
}

struct CallbackDrop(mpsc::UnboundedSender<()>);

impl Drop for CallbackDrop {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

struct Gates {
    submissions: mpsc::Receiver<Submission>,
    finished: mpsc::UnboundedReceiver<()>,
}

fn broker() -> (GatedBroker, Gates) {
    let (submissions, received) = mpsc::channel(4);
    let (finished, completed) = mpsc::unbounded_channel();
    (
        GatedBroker {
            submissions,
            finished,
        },
        Gates {
            submissions: received,
            finished: completed,
        },
    )
}

fn namespace() -> NamespaceName {
    NamespaceName::new("tenant").expect("namespace")
}

fn entity() -> EntityPath {
    EntityPath::new("orders").expect("entity")
}

fn binding() -> EntityBinding {
    EntityBinding::new(
        namespace(),
        entity(),
        entity(),
        EntityIncarnationKind::Queue,
        7,
    )
    .expect("binding")
}

impl Broker for GatedBroker {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        assert_eq!(namespace, self::namespace());
        assert_eq!(target, Attachment::Queue(entity()));
        Ok(Some(EntityAdmission {
            metadata: EntityMetadata::Queue(domain::QueueConfig::default()),
            binding: binding(),
        }))
    }

    async fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> BrokerReply {
        let _callback = CallbackDrop(self.finished.clone());
        let (reply, result) = oneshot::channel();
        self.submissions
            .send(Submission {
                binding,
                entity,
                kind,
                reply,
            })
            .await
            .map_err(|_| BrokerRejection::Unavailable("test observer closed".into()))?;
        result
            .await
            .map_err(|_| BrokerRejection::Unavailable("test reply closed".into()))?
    }

    async fn submit(&self, _: NamespaceName, _: EntityPath, _: CommandKind) -> BrokerReply {
        panic!("an admitted producer must retain its exact binding")
    }

    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        panic!("producer admission must read metadata and identity together")
    }

    async fn rules(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        panic!("ordinary producer does not read rules")
    }

    async fn rules_fenced(
        &self,
        _: EntityBinding,
        _: EntityPath,
        _: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        panic!("ordinary producer does not read rules")
    }

    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        panic!("ordinary producer does not wait for deliverable messages")
    }
}

impl Gates {
    async fn submission(&mut self) -> TestResult<Submission> {
        Ok(timeout(DEADLINE, self.submissions.recv())
            .await?
            .expect("listener submitted a command"))
    }
}

impl Submission {
    fn assert_input(&self, body: &[u8]) {
        assert_eq!(self.binding, binding());
        assert_eq!(self.entity, entity());
        let CommandKind::SendEnvelope {
            message_id,
            body: actual,
            session_id,
            envelope,
            ..
        } = &self.kind
        else {
            panic!("ordinary transfer must produce SendEnvelope");
        };
        assert_eq!(message_id, "item-1");
        assert_eq!(actual, body);
        assert!(session_id.is_none());
        assert_eq!(envelope.properties.subject.as_deref(), Some("label"));
        assert_eq!(
            envelope.application_properties.get("value"),
            Some(&MessageValue::Int(42))
        );
    }

    fn respond(self, result: BrokerReply) {
        assert!(self.reply.send(result).is_ok(), "callback remains pending");
    }
}

fn message(body: &[u8]) -> Message {
    Message::builder()
        .properties(Properties {
            message_id: Some("item-1".into()),
            subject: Some("label".into()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("value", 42_i32)
                .build(),
        )
        .body(Body::Data(vec![body.to_vec().into()]))
        .build()
}

fn request(mode: &ReceiverSettleMode) -> Attach {
    Attach {
        name: "retained-producer".into(),
        handle: 0,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: mode.clone(),
        source: None,
        target: Some(Target::new("orders").into()),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

struct Peer {
    stream: TcpStream,
    transfers: u32,
}

impl Peer {
    async fn send(&mut self, performative: Performative, payload: Vec<u8>) -> TestResult {
        if matches!(&performative, Performative::Transfer(_)) {
            self.transfers += 1;
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
        Ok(timeout(DEADLINE, read_frame(&mut self.stream)).await??)
    }

    async fn attach(&mut self, mode: &ReceiverSettleMode) -> TestResult {
        self.send(Performative::Begin(Begin::default()), Vec::new())
            .await?;
        assert!(matches!(self.read().await?, Frame::Amqp {
            channel: 0, performative: Some(Performative::Begin(begin)), ..
        } if begin.remote_channel == Some(0)));
        self.send(Performative::Attach(Box::new(request(mode))), Vec::new())
            .await?;
        self.attached(mode).await
    }

    async fn attached(&mut self, mode: &ReceiverSettleMode) -> TestResult {
        let mut attached = false;
        let mut credited = false;
        while !attached || !credited {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Attach(attach)),
                    ..
                } => {
                    assert_eq!(attach.name, "retained-producer");
                    assert_eq!(attach.role, Role::Receiver);
                    assert_eq!(attach.handle, 0);
                    assert_eq!(&attach.rcv_settle_mode, mode);
                    attached = true;
                }
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Flow(flow)),
                    ..
                } => {
                    assert_eq!(flow.handle, Some(0));
                    assert_eq!(flow.link_credit, Some(32));
                    credited = true;
                }
                other => panic!("producer must attach and receive credit: {other:?}"),
            }
        }
        Ok(())
    }

    async fn delivery(&mut self, id: u32, message: &Message) -> TestResult {
        self.send(
            Performative::Transfer(Transfer {
                handle: 0,
                delivery_id: Some(id),
                delivery_tag: Some(id.to_be_bytes().to_vec().into()),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            encode_message(message)?,
        )
        .await
    }

    // This native session echo fences preceding peer frames, not broker commit.
    async fn barrier_without_disposition(&mut self) -> TestResult {
        let expected = self.transfers;
        self.send(
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 2_048,
                next_outgoing_id: expected,
                outgoing_window: 2_048,
                echo: true,
                ..Flow::default()
            }),
            Vec::new(),
        )
        .await?;
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Flow(flow)),
                    ..
                } => {
                    if flow.handle.is_none() && flow.next_incoming_id == Some(expected) {
                        return Ok(());
                    }
                }
                other => panic!("no disposition may precede the broker reply: {other:?}"),
            }
        }
    }

    async fn disposition(
        &mut self,
        id: u32,
        mode: &ReceiverSettleMode,
    ) -> TestResult<DeliveryState> {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Disposition(disposition)),
                    payload,
                } => {
                    assert!(payload.is_empty());
                    assert_eq!(disposition.role, Role::Receiver);
                    assert_eq!(disposition.first, id);
                    assert_eq!(disposition.last, None);
                    assert_eq!(disposition.settled, mode == &ReceiverSettleMode::First);
                    return Ok(disposition.state.expect("terminal disposition"));
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("expected a terminal disposition: {other:?}"),
            }
        }
    }

    async fn acknowledge(&mut self, id: u32, mode: &ReceiverSettleMode) -> TestResult {
        if mode == &ReceiverSettleMode::Second {
            self.send(
                Performative::Disposition(Disposition {
                    role: Role::Sender,
                    first: id,
                    last: None,
                    settled: true,
                    state: None,
                    batchable: false,
                }),
                Vec::new(),
            )
            .await?;
        }
        self.barrier_without_disposition().await
    }
}

async fn open() -> TestResult<(ServerConnection, Peer)> {
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let mut stream = timeout(DEADLINE, TcpStream::connect(socket.local_addr()?)).await??;
    stream.set_nodelay(true)?;
    let (incoming, _) = timeout(DEADLINE, socket.accept()).await??;
    incoming.set_nodelay(true)?;
    let accepting = tokio::spawn(ServerConnection::accept_with_options(
        incoming,
        "retained-listener-server",
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
        transfers: 0,
    };
    peer.send(
        Performative::Open(Open::new("retained-listener-peer")),
        Vec::new(),
    )
    .await?;
    assert!(matches!(
        peer.read().await?,
        Frame::Amqp {
            performative: Some(Performative::Open(_)),
            ..
        }
    ));
    Ok((timeout(DEADLINE, accepting).await???, peer))
}

struct Wire {
    peer: Peer,
    listener: JoinHandle<TestResult>,
}

impl Wire {
    async fn start(broker: GatedBroker, mode: &ReceiverSettleMode) -> TestResult<Self> {
        let (mut connection, mut peer) = open().await?;
        let listener = tokio::spawn(async move {
            let result = serve_open_connection(&mut connection, namespace(), broker, None).await;
            connection.shutdown().await;
            result
        });
        peer.attach(mode).await?;
        Ok(Self { peer, listener })
    }

    async fn close(mut self) -> TestResult {
        self.peer
            .send(Performative::Close(Close::default()), Vec::new())
            .await?;
        loop {
            match self.peer.read().await? {
                Frame::Amqp {
                    performative: Some(Performative::Close(close)),
                    ..
                } => {
                    assert!(close.error.is_none());
                    break;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("connection must close cleanly: {other:?}"),
            }
        }
        timeout(DEADLINE, self.listener).await???;
        Ok(())
    }
}

fn accepted() -> BrokerReply {
    Ok(CommandOutcome::Sent {
        sequence: SequenceNumber::new(11),
    })
}

#[tokio::test]
async fn retained_listener_waits_for_the_exact_fenced_callback_in_both_settlement_modes()
-> TestResult {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let (broker, mut gates) = broker();
        let mut wire = Wire::start(broker, &mode).await?;
        wire.peer.delivery(0, &message(b"held\0payload")).await?;
        let submitted = gates.submission().await?;
        submitted.assert_input(b"held\0payload");
        wire.peer.barrier_without_disposition().await?;
        assert!(gates.finished.try_recv().is_err());
        submitted.respond(accepted());
        assert_eq!(
            wire.peer.disposition(0, &mode).await?,
            DeliveryState::Accepted(Accepted)
        );
        wire.peer.acknowledge(0, &mode).await?;
        wire.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn retained_listener_maps_refusal_and_accepts_the_next_publication() -> TestResult {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let (broker, mut gates) = broker();
        let mut wire = Wire::start(broker, &mode).await?;
        wire.peer.delivery(0, &message(b"refused")).await?;
        let submitted = gates.submission().await?;
        submitted.assert_input(b"refused");
        wire.peer.barrier_without_disposition().await?;
        submitted.respond(Err(BrokerRejection::Refused(
            BrokerError::MessageTooLarge {
                body_bytes: 7,
                maximum_bytes: 6,
            },
        )));
        let DeliveryState::Rejected(rejected) = wire.peer.disposition(0, &mode).await? else {
            panic!("typed broker refusal must reject the transfer");
        };
        assert_eq!(
            rejected
                .error
                .expect("mapped refusal")
                .condition
                .as_symbol(),
            amqp::Symbol::from(crate::MESSAGE_SIZE_EXCEEDED)
        );
        wire.peer.acknowledge(0, &mode).await?;
        wire.peer.delivery(1, &message(b"healthy")).await?;
        let submitted = gates.submission().await?;
        submitted.assert_input(b"healthy");
        submitted.respond(accepted());
        assert_eq!(
            wire.peer.disposition(1, &mode).await?,
            DeliveryState::Accepted(Accepted)
        );
        wire.peer.acknowledge(1, &mode).await?;
        wire.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn retained_listener_rejects_malformed_ingress_without_submitting_then_keeps_the_link()
-> TestResult {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let (broker, mut gates) = broker();
        let mut wire = Wire::start(broker, &mode).await?;
        let mut malformed = message(b"bad session identifier");
        malformed.properties.as_mut().expect("properties").group_id = Some(String::new());
        wire.peer.delivery(0, &malformed).await?;
        let DeliveryState::Rejected(rejected) = wire.peer.disposition(0, &mode).await? else {
            panic!("malformed ingress must be rejected");
        };
        assert_eq!(
            rejected.error.expect("parse refusal").condition.as_symbol(),
            amqp::Symbol::from(crate::INVALID_FIELD)
        );
        assert!(gates.submissions.try_recv().is_err());
        wire.peer.acknowledge(0, &mode).await?;
        wire.peer
            .delivery(1, &message(b"healthy after parse refusal"))
            .await?;
        let submitted = gates.submission().await?;
        submitted.assert_input(b"healthy after parse refusal");
        submitted.respond(accepted());
        assert_eq!(
            wire.peer.disposition(1, &mode).await?,
            DeliveryState::Accepted(Accepted)
        );
        wire.peer.acknowledge(1, &mode).await?;
        wire.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn explicitly_aborting_a_blocked_sending_task_drops_its_callback_without_acknowledging()
-> TestResult {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let (mut connection, mut peer) = open().await?;
        peer.send(Performative::Begin(Begin::default()), Vec::new())
            .await?;
        let incoming = timeout(DEADLINE, connection.next_incoming_session())
            .await?
            .expect("incoming session");
        let mut session = timeout(DEADLINE, connection.accept_session(incoming)).await??;
        assert!(matches!(
            peer.read().await?,
            Frame::Amqp {
                performative: Some(Performative::Begin(_)),
                ..
            }
        ));
        peer.send(Performative::Attach(Box::new(request(&mode))), Vec::new())
            .await?;
        let incoming = timeout(DEADLINE, session.next_incoming_attach())
            .await?
            .expect("incoming attach");
        let endpoint = timeout(DEADLINE, session.accept_attach(incoming, 1024 * 1024)).await??;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("producer must give a receiving endpoint");
        };
        let (broker, mut gates) = broker();
        let task = tokio::spawn(serve_sending_client(
            receiver,
            namespace(),
            entity(),
            BoundBroker::new(broker, binding()),
            None,
        ));
        peer.attached(&mode).await?;
        peer.delivery(0, &message(b"explicit cancellation")).await?;
        let submitted = gates.submission().await?;
        submitted.assert_input(b"explicit cancellation");
        peer.barrier_without_disposition().await?;
        task.abort();
        let joined = timeout(DEADLINE, task).await?;
        assert!(
            joined
                .expect_err("task was explicitly aborted")
                .is_cancelled()
        );
        assert_eq!(timeout(DEADLINE, gates.finished.recv()).await?, Some(()));
        assert!(submitted.reply.send(accepted()).is_err());
        peer.barrier_without_disposition().await?;
        timeout(DEADLINE, connection.shutdown()).await?;
    }
    Ok(())
}
