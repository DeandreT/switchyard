//! A session granted during a closing attach must be released immediately.

use std::{
    error::Error,
    future::Future,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use amqp::{
    Attach, ClientConnection, ClientReceiver, Close, Frame, Open, Performative, ProtocolHeader,
    ReceiverSettleMode, Role, SenderSettleMode, Source, read_frame, read_protocol_header,
    write_frame, write_protocol_header,
};
use domain::{
    CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueConfig, SessionId, StateMachine,
};
use protocol_amqp::{AmqpListener, BrokerRejection};
use server::{Broker, BrokerHandle, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const TEST_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Default)]
struct AttachGate {
    accepted: Notify,
    return_outcome: Notify,
    released: Notify,
    blocked: AtomicBool,
}

#[derive(Clone)]
struct ControlledBroker {
    inner: BrokerHandle,
    gate: Arc<AttachGate>,
}

impl protocol_amqp::Broker for ControlledBroker {
    fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: protocol_amqp::Attachment,
    ) -> impl Future<Output = Result<Option<protocol_amqp::EntityMetadata>, BrokerRejection>> + Send
    {
        protocol_amqp::Broker::entity_metadata(&self.inner, namespace, target)
    }

    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let accepting = matches!(&kind, CommandKind::AcceptSession { .. });
        let releasing = matches!(&kind, CommandKind::ReleaseSession { .. });
        let outcome = protocol_amqp::Broker::submit(&self.inner, namespace, entity, kind).await;
        if accepting && !self.gate.blocked.swap(true, Ordering::SeqCst) {
            assert!(matches!(
                &outcome,
                Ok(CommandOutcome::SessionAccepted(Some(_)))
            ));
            self.gate.accepted.notify_one();
            self.gate.return_outcome.notified().await;
        }
        if releasing && matches!(&outcome, Ok(CommandOutcome::SessionReleased)) {
            self.gate.released.notify_one();
        }
        outcome
    }

    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        protocol_amqp::Broker::deliverable(&self.inner, namespace, entity)
    }
}

async fn write_performative(stream: &mut TcpStream, performative: Performative) -> TestResult {
    write_frame(
        stream,
        &Frame::Amqp {
            channel: 0,
            performative: Some(performative),
            payload: Vec::new(),
        },
    )
    .await?;
    Ok(())
}

fn session_source() -> TestResult<Source> {
    let mut source = Source::new("orders");
    protocol_amqp::stamp_session_filter(&mut source, &SessionId::new("cart-1")?);
    Ok(source)
}

#[tokio::test]
async fn closing_before_attach_acceptance_releases_the_committed_session_hold() -> TestResult {
    timeout(TEST_TIMEOUT, exercise()).await??;
    Ok(())
}

async fn exercise() -> TestResult {
    let clock = ManualClock::at(1_000);
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(MemoryStore::default()),
        clock,
    ));
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("orders")?;
    broker.handle().submit_blocking(
        namespace.clone(),
        entity.clone(),
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            },
        },
    )?;
    broker.handle().submit_blocking(
        namespace.clone(),
        entity,
        CommandKind::Send {
            message_id: String::from("seed"),
            body: b"seed".to_vec(),
            time_to_live_millis: None,
            session_id: Some(SessionId::new("cart-1")?),
        },
    )?;
    let gate = Arc::new(AttachGate::default());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let served = tokio::spawn(
        AmqpListener::new(
            ControlledBroker {
                inner: broker.handle(),
                gate: Arc::clone(&gate),
            },
            namespace,
        )
        .with_max_connections(NonZeroUsize::new(2).expect("allow a replacement during cleanup"))
        .serve(listener),
    );

    let mut peer = TcpStream::connect(address).await?;
    write_protocol_header(&mut peer, ProtocolHeader::AMQP).await?;
    assert_eq!(read_protocol_header(&mut peer).await?, ProtocolHeader::AMQP);
    write_performative(&mut peer, Performative::Open(Open::new("closing-peer"))).await?;
    assert!(matches!(
        read_frame(&mut peer).await?,
        Frame::Amqp {
            performative: Some(Performative::Open(_)),
            ..
        }
    ));
    write_performative(&mut peer, Performative::Begin(amqp::Begin::default())).await?;
    assert!(matches!(
        read_frame(&mut peer).await?,
        Frame::Amqp {
            performative: Some(Performative::Begin(_)),
            ..
        }
    ));
    write_performative(
        &mut peer,
        Performative::Attach(Box::new(Attach {
            name: String::from("blocked-session-attach"),
            handle: 0,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(session_source()?),
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        })),
    )
    .await?;
    gate.accepted.notified().await;
    write_performative(&mut peer, Performative::Close(Close::default())).await?;
    assert!(matches!(
        read_frame(&mut peer).await?,
        Frame::Amqp {
            performative: Some(Performative::Close(_)),
            ..
        }
    ));

    gate.return_outcome.notify_one();
    gate.released.notified().await;
    // The clock never advanced, so expiry cannot account for this reuse.
    let mut connection =
        ClientConnection::open(TcpStream::connect(address).await?, "next", None).await?;
    let mut session = connection.begin().await?;
    let mut receiver = ClientReceiver::builder()
        .name("replacement-session-attach")
        .source(session_source()?)
        .attach(&mut session)
        .await?;
    let delivery = receiver.recv().await?;
    assert_eq!(
        delivery.message().body,
        amqp::Message::data(b"seed".to_vec()).body
    );
    receiver.accept(&delivery).await?;
    connection.close().await?;
    served.abort();
    Ok(())
}
