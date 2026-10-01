use std::{error::Error as StdError, sync::Arc, time::Duration};

use amqp::{
    Accepted, Attach, Begin, Close, ConnectionOptions, DeliveryState, Detach, Frame, Message, Open,
    Performative, ProtocolHeader, ReceiverSettleMode, Role, SenderSettleMode, Source, Target,
    Transfer, encode_message, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use domain::{
    AcceptedSession, CommandKind, CommandOutcome, EntityPath, LockToken, NamespaceName,
    SequenceNumber, SessionId, SessionLock, Timestamp,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    time::timeout,
};

use super::{ServerConnection, serve_open_connection};
use crate::{Attachment, Broker, BrokerRejection, EntityMetadata, stamp_session_filter};

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Eq, PartialEq)]
enum Observation {
    Planning,
    Released,
    Sent,
}

#[derive(Clone)]
struct PausedBroker {
    grant_session: bool,
    proceed: Arc<Semaphore>,
    observed: mpsc::Sender<Observation>,
}

impl Broker for PausedBroker {
    async fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        Ok((namespace.as_str() == "tenant"
            && matches!(target, Attachment::Queue(entity) if entity.as_str() == "orders"))
        .then_some(EntityMetadata::Queue(domain::QueueConfig {
            requires_session: true,
            ..domain::QueueConfig::default()
        })))
    }

    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        assert_eq!(namespace.as_str(), "tenant");
        assert_eq!(entity.as_str(), "orders");
        match kind {
            CommandKind::AcceptSession { session_id, .. } => {
                assert_eq!(session_id, Some(SessionId::new("cart-1").unwrap()));
                self.observed.send(Observation::Planning).await.unwrap();
                self.proceed.acquire().await.unwrap().forget();
                Ok(CommandOutcome::SessionAccepted(self.grant_session.then(
                    || AcceptedSession {
                        session_id: SessionId::new("cart-1").unwrap(),
                        lock: SessionLock {
                            token: LockToken::new(77),
                            locked_until: Timestamp::from_millis(60_000),
                        },
                        state: Vec::new(),
                    },
                )))
            }
            CommandKind::ReleaseSession { session } => {
                assert!(self.grant_session);
                assert_eq!(session.session_id.as_str(), "cart-1");
                assert_eq!(session.token, LockToken::new(77));
                self.observed.send(Observation::Released).await.unwrap();
                Ok(CommandOutcome::SessionReleased)
            }
            CommandKind::SendEnvelope { body, .. } => {
                assert_eq!(body, b"healthy after cancelled approval");
                self.observed.send(Observation::Sent).await.unwrap();
                Ok(CommandOutcome::Sent {
                    sequence: SequenceNumber::new(7),
                })
            }
            other => panic!("cancelled approval must not start a receiver task: {other:?}"),
        }
    }

    async fn deliverable(&self, _namespace: &NamespaceName, _entity: &EntityPath) {
        std::future::pending::<()>().await;
    }
}

struct Peer(TcpStream);

impl Peer {
    async fn send(&mut self, performative: Performative, payload: Vec<u8>) -> TestResult {
        timeout(
            IO_TIMEOUT,
            write_frame(
                &mut self.0,
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
        Ok(timeout(IO_TIMEOUT, read_frame(&mut self.0)).await??)
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
        "cancelled-approval-server",
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
    let mut peer = Peer(stream);
    peer.send(
        Performative::Open(Open::new("raw-cancelled-approval-peer")),
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
    Ok((timeout(IO_TIMEOUT, accepting).await???, peer))
}

fn request(role: Role) -> Attach {
    let source = if role == Role::Receiver {
        let mut source = Source::new("orders");
        stamp_session_filter(&mut source, &SessionId::new("cart-1").unwrap());
        Some(source)
    } else {
        None
    };
    Attach {
        name: String::from("reused-link"),
        handle: 0,
        target: (role == Role::Sender).then(|| Target::new("orders")),
        initial_delivery_count: (role == Role::Sender).then_some(0),
        role,
        snd_settle_mode: SenderSettleMode::Mixed,
        rcv_settle_mode: ReceiverSettleMode::First,
        source,
        unsettled: None,
        incomplete_unsettled: false,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

async fn cancelled_planning_preserves_session(grant_session: bool) -> TestResult {
    let (mut connection, mut peer) = open().await?;
    let (observed, mut observations) = mpsc::channel(4);
    let proceed = Arc::new(Semaphore::new(0));
    let broker = PausedBroker {
        grant_session,
        proceed: Arc::clone(&proceed),
        observed,
    };
    let listener = tokio::spawn(async move {
        let result = serve_open_connection(
            &mut connection,
            NamespaceName::new("tenant").unwrap(),
            broker,
            None,
        )
        .await;
        connection.shutdown().await;
        result
    });
    peer.send(Performative::Begin(Begin::default()), Vec::new())
        .await?;
    assert!(matches!(peer.read().await?, Frame::Amqp {
        channel: 0, performative: Some(Performative::Begin(begin)), ..
    } if begin.remote_channel == Some(0)));
    peer.send(
        Performative::Attach(Box::new(request(Role::Receiver))),
        Vec::new(),
    )
    .await?;
    assert_eq!(
        timeout(IO_TIMEOUT, observations.recv()).await?,
        Some(Observation::Planning)
    );
    peer.send(
        Performative::Detach(Detach {
            handle: 0,
            closed: true,
            error: None,
        }),
        Vec::new(),
    )
    .await?;
    assert!(matches!(peer.read().await?, Frame::Amqp {
        channel: 0, performative: Some(Performative::Attach(attach)), payload,
    } if attach.name == "reused-link" && attach.handle == 0 && attach.role == Role::Sender
        && attach.initial_delivery_count == Some(0) && attach.snd_settle_mode == SenderSettleMode::Mixed
        && attach.rcv_settle_mode == ReceiverSettleMode::First && attach.source.is_none() && attach.target.is_none()
        && attach.unsettled.is_none() && !attach.incomplete_unsettled && payload.is_empty()));
    assert!(matches!(peer.read().await?, Frame::Amqp {
        channel: 0, performative: Some(Performative::Detach(detach)), ..
    } if detach.handle == 0 && detach.closed && detach.error.is_none()));

    // The Detach acknowledgement fences engine cancellation while the domain
    // decision remains paused. Its replacement can already reuse the handle.
    peer.send(
        Performative::Attach(Box::new(request(Role::Sender))),
        Vec::new(),
    )
    .await?;
    proceed.add_permits(1);
    if grant_session {
        assert_eq!(
            timeout(IO_TIMEOUT, observations.recv()).await?,
            Some(Observation::Released)
        );
    }
    let mut attached = false;
    let mut credited = false;
    while !attached || !credited {
        match peer.read().await? {
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Attach(attach)),
                ..
            } => {
                assert_eq!(attach.name, "reused-link");
                assert_eq!(attach.handle, 0);
                assert_eq!(attach.role, Role::Receiver);
                assert_eq!(
                    attach.target.as_ref().and_then(|t| t.address.as_deref()),
                    Some("orders")
                );
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
            other => panic!("cancelled approval must not terminate the session: {other:?}"),
        }
    }
    peer.send(
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
        encode_message(&Message::data(b"healthy after cancelled approval".to_vec()))?,
    )
    .await?;
    assert_eq!(
        timeout(IO_TIMEOUT, observations.recv()).await?,
        Some(Observation::Sent)
    );
    loop {
        match peer.read().await? {
            Frame::Amqp {
                channel: 0,
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
            other => panic!("replacement producer must settle: {other:?}"),
        }
    }
    peer.send(Performative::Close(Close::default()), Vec::new())
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
            other => panic!("replacement session must close cleanly: {other:?}"),
        }
    }
    timeout(IO_TIMEOUT, listener).await???;
    Ok(())
}

#[tokio::test]
async fn cancelled_receiver_approval_releases_its_granted_hold_and_keeps_session_live() -> TestResult
{
    cancelled_planning_preserves_session(true).await
}

#[tokio::test]
async fn cancelled_refused_approval_keeps_the_replacement_link_live() -> TestResult {
    cancelled_planning_preserves_session(false).await
}
