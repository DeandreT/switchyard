use std::{error::Error as StdError, time::Duration};

use amqp::{
    Accepted, Attach, Begin, Close, ConnectionOptions, DeliveryState, End, Flow, Frame, Message,
    Open, Performative, ProtocolHeader, ReceiverSettleMode, Role, SenderSettleMode, Symbol, Target,
    Transfer, encode_message, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use domain::{CommandKind, CommandOutcome, EntityPath, NamespaceName, SequenceNumber};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::timeout,
};

use super::{ServerConnection, serve_open_connection};
use crate::{Attachment, Broker, BrokerRejection, EntityMetadata};

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct ObservedBroker(mpsc::Sender<()>);

impl Broker for ObservedBroker {
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
    ) -> Result<CommandOutcome, BrokerRejection> {
        assert_eq!(namespace.as_str(), "tenant");
        assert_eq!(entity.as_str(), "orders");
        let CommandKind::SendEnvelope { body, .. } = kind else {
            panic!("unexpected broker command after startup: {kind:?}");
        };
        assert_eq!(body, b"healthy after stale session");
        self.0.send(()).await.expect("test observes the submission");
        Ok(CommandOutcome::Sent {
            sequence: SequenceNumber::new(7),
        })
    }

    async fn deliverable(&self, _namespace: &NamespaceName, _entity: &EntityPath) {
        std::future::pending::<()>().await;
    }
}

struct Peer(TcpStream);

impl Peer {
    async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> TestResult {
        timeout(
            IO_TIMEOUT,
            write_frame(
                &mut self.0,
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
        Ok(timeout(IO_TIMEOUT, read_frame(&mut self.0)).await??)
    }

    async fn begin(&mut self, channel: u16) -> TestResult {
        assert!(matches!(self.read().await?, Frame::Amqp {
            channel: actual, performative: Some(Performative::Begin(begin)), ..
        } if actual == channel && begin.remote_channel == Some(channel)));
        Ok(())
    }
}

async fn open() -> TestResult<(ServerConnection, Peer)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut stream = TcpStream::connect(listener.local_addr()?).await?;
    stream.set_nodelay(true)?;
    let (socket, _) = listener.accept().await?;
    socket.set_nodelay(true)?;
    let accepting = tokio::spawn(ServerConnection::accept_with_options(
        socket,
        "stale-session-server",
        None,
        ConnectionOptions::default().idle_timeout_millis(0),
    ));
    timeout(
        IO_TIMEOUT,
        write_protocol_header(&mut stream, ProtocolHeader::AMQP),
    )
    .await??;
    assert_eq!(
        timeout(IO_TIMEOUT, read_protocol_header(&mut stream)).await??,
        ProtocolHeader::AMQP
    );
    timeout(
        IO_TIMEOUT,
        write_frame(
            &mut stream,
            &Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(Open::new("raw-stale-session-peer"))),
                payload: Vec::new(),
            },
        ),
    )
    .await??;
    assert!(matches!(
        timeout(IO_TIMEOUT, read_frame(&mut stream)).await??,
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(_)),
            ..
        }
    ));
    Ok((timeout(IO_TIMEOUT, accepting).await???, Peer(stream)))
}

async fn stale_session_does_not_close_the_listener(refused: bool) -> TestResult {
    let (mut connection, mut peer) = open().await?;
    peer.send(0, Performative::Begin(Begin::default()), Vec::new())
        .await?;
    if refused {
        peer.send(
            0,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 2_048,
                next_outgoing_id: 0,
                outgoing_window: 2_048,
                handle: Some(99),
                delivery_count: Some(0),
                ..Flow::default()
            }),
            Vec::new(),
        )
        .await?;
    } else {
        peer.send(0, Performative::End(End::default()), Vec::new())
            .await?;
    }
    peer.begin(0).await?;
    let Frame::Amqp {
        channel: 0,
        performative: Some(Performative::End(end)),
        ..
    } = peer.read().await?
    else {
        panic!("stale session must already be ended/refused");
    };
    assert_eq!(
        end.error.as_ref().map(|error| error.condition.as_symbol()),
        refused.then(|| Symbol::from("amqp:session:unattached-handle"))
    );

    // The real TCP engine has already processed End before the application loop
    // starts. Its untouched IncomingSession event is deterministically stale.
    let (submitted, mut observed) = mpsc::channel(1);
    let listener = tokio::spawn(async move {
        let result = serve_open_connection(
            &mut connection,
            NamespaceName::new("tenant").expect("namespace"),
            ObservedBroker(submitted),
            None,
        )
        .await;
        connection.shutdown().await;
        result
    });
    peer.send(1, Performative::Begin(Begin::default()), Vec::new())
        .await?;
    peer.begin(1).await?;
    peer.send(
        1,
        Performative::Attach(Box::new(Attach {
            name: String::from("healthy-producer"),
            handle: 0,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: None,
            target: Some(Target::new("orders")),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: Some(0),
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        })),
        Vec::new(),
    )
    .await?;
    let mut attached = false;
    let mut credited = false;
    while !attached || !credited {
        match peer.read().await? {
            Frame::Amqp {
                channel: 1,
                performative: Some(Performative::Attach(attach)),
                ..
            } => {
                assert_eq!(attach.handle, 0);
                assert_eq!(attach.role, Role::Receiver);
                assert!(attach.target.is_some());
                attached = true;
            }
            Frame::Amqp {
                channel: 1,
                performative: Some(Performative::Flow(flow)),
                ..
            } => {
                assert_eq!(flow.handle, Some(0));
                assert_eq!(flow.link_credit, Some(32));
                credited = true;
            }
            other => panic!("stale event must not close the connection: {other:?}"),
        }
    }
    peer.send(
        1,
        Performative::Transfer(Transfer {
            handle: 0,
            delivery_id: Some(7),
            delivery_tag: Some(vec![7].into()),
            message_format: Some(0),
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        }),
        encode_message(&Message::data(b"healthy after stale session".to_vec()))?,
    )
    .await?;
    timeout(IO_TIMEOUT, observed.recv())
        .await?
        .expect("later healthy broker submission");
    loop {
        match peer.read().await? {
            Frame::Amqp {
                channel: 1,
                performative: Some(Performative::Disposition(disposition)),
                ..
            } => {
                assert_eq!(disposition.role, Role::Receiver);
                assert_eq!(disposition.first, 7);
                assert!(disposition.settled);
                assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
                break;
            }
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                ..
            } => {}
            other => panic!("healthy submission must settle: {other:?}"),
        }
    }
    if refused {
        peer.send(0, Performative::End(End::default()), Vec::new())
            .await?;
    }
    peer.send(0, Performative::Close(Close::default()), Vec::new())
        .await?;
    loop {
        match peer.read().await? {
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
            other => panic!("clean listener shutdown: {other:?}"),
        }
    }
    timeout(IO_TIMEOUT, listener).await???;
    Ok(())
}

#[tokio::test]
async fn queued_remote_end_does_not_close_the_real_listener_before_later_submit() -> TestResult {
    stale_session_does_not_close_the_listener(false).await
}

#[tokio::test]
async fn queued_session_refusal_does_not_close_the_real_listener_before_later_submit() -> TestResult
{
    stale_session_does_not_close_the_listener(true).await
}
