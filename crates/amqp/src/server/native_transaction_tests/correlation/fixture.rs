use std::{
    any::Any,
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};

use crate::{
    ScopedConnectionAcceptance, ServerConnectionAbortSource, ServerConnectionAcceptor,
    ServerConnectionOwner, ServerPeerCloseReplyState,
};

use super::*;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy)]
pub(super) struct Route {
    pub(super) peer: u16,
    pub(super) server: u16,
}

struct LinkProgress {
    sent: u32,
    last_grant: u32,
}

pub(super) struct CorrelationFixture {
    connection: ServerConnection,
    peer: tokio::io::DuplexStream,
    frames_sent: HashMap<u16, u32>,
    links: HashMap<(u16, u32), LinkProgress>,
}

impl CorrelationFixture {
    pub(super) async fn open(acceptor: ServerConnectionAcceptor) -> TestResult<Self> {
        let (wire, mut peer) = tokio::io::duplex(64 * 1024);
        let greeting = async {
            write_protocol_header(&mut peer, ProtocolHeader::AMQP).await?;
            expect_header(&mut peer, ProtocolHeader::AMQP).await?;
            write_amqp(
                &mut peer,
                0,
                Performative::Open(Open::new("outcome-correlation-peer")),
                Vec::new(),
            )
            .await?;
            let frame = tokio::time::timeout(DEADLINE, read_frame(&mut peer)).await??;
            assert!(matches!(frame, Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(_)),
                payload,
            } if payload.is_empty()));
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        };
        let accepting = acceptor.accept_with_transactional_ingress(
            wire,
            "outcome-correlation-server",
            None,
            ConnectionOptions::default(),
        );
        let (accepted, greeted) =
            tokio::time::timeout(DEADLINE, async { tokio::join!(accepting, greeting) }).await?;
        greeted?;
        let ScopedConnectionAcceptance::Accepted(connection) = accepted? else {
            panic!("fresh retained owner refused native launch")
        };
        Ok(Self {
            connection,
            peer,
            frames_sent: HashMap::new(),
            links: HashMap::new(),
        })
    }

    async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> TestResult {
        let transfer = matches!(performative, Performative::Transfer(_));
        tokio::time::timeout(
            DEADLINE,
            write_amqp(&mut self.peer, channel, performative, payload),
        )
        .await??;
        if transfer {
            *self.frames_sent.entry(channel).or_default() += 1;
        }
        Ok(())
    }

    async fn read(&mut self) -> TestResult<Frame> {
        Ok(tokio::time::timeout(DEADLINE, read_frame(&mut self.peer)).await??)
    }

    pub(super) async fn session(&mut self, channel: u16) -> TestResult<(ServerSession, Route)> {
        self.send(channel, Performative::Begin(Begin::default()), Vec::new())
            .await?;
        let incoming = tokio::time::timeout(DEADLINE, self.connection.next_incoming_session())
            .await?
            .expect("original incoming session");
        let session =
            tokio::time::timeout(DEADLINE, self.connection.accept_session(incoming)).await??;
        let Frame::Amqp {
            channel: server,
            performative: Some(Performative::Begin(begin)),
            payload,
        } = self.read().await?
        else {
            panic!("expected native Begin")
        };
        assert_eq!(begin.remote_channel, Some(channel));
        assert!(payload.is_empty());
        assert_eq!(session.channel, server);
        assert!(self.frames_sent.insert(channel, 0).is_none());
        Ok((
            session,
            Route {
                peer: channel,
                server,
            },
        ))
    }

    async fn approval(
        &mut self,
        session: &mut ServerSession,
        route: Route,
        attach: Attach,
    ) -> TestResult<IncomingAttach> {
        self.send(
            route.peer,
            Performative::Attach(Box::new(attach)),
            Vec::new(),
        )
        .await?;
        Ok(
            tokio::time::timeout(DEADLINE, session.next_incoming_attach())
                .await?
                .expect("original actor-approved attach"),
        )
    }

    async fn attached(
        &mut self,
        route: Route,
        handle: u32,
        mode: ReceiverSettleMode,
        coordinator: bool,
    ) -> TestResult {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Attach(attach)),
            payload,
        } = self.read().await?
        else {
            panic!("expected native Attach")
        };
        assert_eq!(channel, route.server);
        assert_eq!(attach.handle, handle);
        assert_eq!(attach.role, Role::Receiver);
        assert_eq!(attach.rcv_settle_mode, mode);
        assert!(payload.is_empty());
        if coordinator {
            assert!(
                attach
                    .target
                    .as_ref()
                    .and_then(|target| target.as_coordinator())
                    .is_some()
            );
        } else {
            assert_eq!(
                attach
                    .target
                    .as_ref()
                    .and_then(|target| target.as_target())
                    .and_then(|target| target.address.as_deref()),
                Some("queue")
            );
        }
        assert!(
            self.links
                .insert(
                    (channel, handle),
                    LinkProgress {
                        sent: 0,
                        last_grant: 0,
                    }
                )
                .is_none()
        );
        let Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Flow(flow)),
            payload,
        } = self.read().await?
        else {
            panic!("expected original link credit")
        };
        assert_eq!(actual, channel);
        assert_eq!(flow.handle, Some(handle));
        assert_eq!(flow.delivery_count, Some(0));
        assert_eq!(flow.link_credit, Some(32));
        assert!(!flow.drain && !flow.echo);
        assert!(payload.is_empty());
        Ok(())
    }

    pub(super) async fn coordinator(
        &mut self,
        session: &mut ServerSession,
        route: Route,
        mode: ReceiverSettleMode,
    ) -> TestResult<CoordinatorEndpoint> {
        let mut attach = coordinator_attach(CONTROL_HANDLE);
        attach.rcv_settle_mode = mode.clone();
        let incoming = self.approval(session, route, attach).await?;
        let (endpoint, attached) = tokio::join!(
            session.accept_coordinator(incoming, 0),
            self.attached(route, CONTROL_HANDLE, mode, true),
        );
        attached?;
        Ok(endpoint?)
    }

    pub(super) async fn receiver(
        &mut self,
        session: &mut ServerSession,
        route: Route,
        mode: ReceiverSettleMode,
    ) -> TestResult<TransactionalReceiver> {
        let mut attach = ordinary_attach(POST_HANDLE);
        attach.rcv_settle_mode = mode.clone();
        let incoming = self.approval(session, route, attach).await?;
        let (endpoint, attached) = tokio::join!(
            session.accept_transactional_receiver(incoming, 0),
            self.attached(route, POST_HANDLE, mode, false),
        );
        attached?;
        Ok(endpoint?)
    }

    async fn transfer(
        &mut self,
        route: Route,
        handle: u32,
        id: u32,
        message: Message,
    ) -> TestResult {
        self.send(
            route.peer,
            Performative::Transfer(first(handle, id, None, false)),
            encode_message(&message)?,
        )
        .await?;
        self.links
            .get_mut(&(route.server, handle))
            .expect("original admitted link")
            .sent += 1;
        Ok(())
    }

    pub(super) async fn ordinary(&mut self, route: Route, id: u32) -> TestResult {
        self.transfer(route, POST_HANDLE, id, Message::data(vec![0x31]))
            .await
    }

    pub(super) async fn declare(&mut self, route: Route, id: u32) -> TestResult {
        self.transfer(
            route,
            CONTROL_HANDLE,
            id,
            Message {
                body: crate::Body::Value(Value::from(TransactionCommand::Declare(
                    Declare::default(),
                ))),
                ..Message::default()
            },
        )
        .await
    }

    fn refill(&mut self, channel: u16, flow: Flow, payload: Vec<u8>) {
        assert!(payload.is_empty());
        let handle = flow
            .handle
            .expect("only a known link refill may precede an outcome");
        let progress = self
            .links
            .get_mut(&(channel, handle))
            .expect("only original admitted routes");
        let count = flow.delivery_count.expect("refill delivery count");
        assert!(count > progress.last_grant && count <= progress.sent);
        assert_eq!(flow.link_credit, Some(32));
        assert!(!flow.drain && !flow.echo);
        progress.last_grant = count;
    }

    pub(super) async fn outcome(
        &mut self,
        route: Route,
        id: u32,
        mode: ReceiverSettleMode,
        declared: Option<&TransactionId>,
    ) -> TestResult {
        for _ in 0..8 {
            match self.read().await? {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Disposition(disposition)),
                    payload,
                } => {
                    assert_eq!(channel, route.server);
                    assert_eq!(disposition.role, Role::Receiver);
                    assert_eq!(disposition.first, id);
                    assert_eq!(disposition.last, None);
                    assert_eq!(disposition.settled, mode == ReceiverSettleMode::First);
                    assert!(!disposition.batchable);
                    assert!(payload.is_empty());
                    match (declared, disposition.state) {
                        (None, Some(DeliveryState::Accepted(_))) => {}
                        (
                            Some(expected),
                            Some(DeliveryState::Declared(crate::Declared { txn_id })),
                        ) => {
                            assert!(&txn_id == expected);
                        }
                        _ => panic!("outcome kind does not match its original receipt"),
                    }
                    return Ok(());
                }
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } => self.refill(channel, flow, payload),
                _ => panic!("unexpected outcome frame kind"),
            }
        }
        panic!("outcome frame count exceeded");
    }

    pub(super) async fn acknowledge(&mut self, route: Route, id: u32) -> TestResult {
        self.send(
            route.peer,
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
        self.barrier(route).await
    }

    pub(super) async fn barrier(&mut self, route: Route) -> TestResult {
        let next = self.frames_sent[&route.peer];
        self.send(
            route.peer,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 1_000,
                next_outgoing_id: next,
                outgoing_window: 1_000,
                echo: true,
                ..Flow::default()
            }),
            Vec::new(),
        )
        .await?;
        for _ in 0..8 {
            match self.read().await? {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } if channel == route.server && flow.handle.is_none() => {
                    assert_eq!(flow.next_incoming_id, Some(next));
                    assert!(flow.delivery_count.is_none() && flow.link_credit.is_none());
                    assert!(flow.available.is_none() && flow.properties.is_none());
                    assert!(!flow.drain && !flow.echo);
                    assert!(payload.is_empty());
                    return Ok(());
                }
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } => self.refill(channel, flow, payload),
                _ => panic!("extra outcome or terminal frame before the original echo barrier"),
            }
        }
        panic!("echo barrier frame count exceeded");
    }

    pub(super) async fn close(&mut self) -> TestResult {
        self.send(0, Performative::Close(Close::default()), Vec::new())
            .await?;
        for _ in 0..8 {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Close(close)),
                    payload,
                } => {
                    assert!(close.error.is_none());
                    assert!(payload.is_empty());
                    return Ok(());
                }
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } => self.refill(channel, flow, payload),
                _ => panic!("unexpected cleanup frame kind"),
            }
        }
        panic!("Close frame count exceeded");
    }
}

pub(super) async fn caught<F: Future>(future: F) -> Result<F::Output, Box<dyn Any + Send>> {
    let mut future = pin!(future);
    poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await
}

pub(super) async fn finish(
    owner: &mut ServerConnectionOwner<()>,
    observed: Result<TestResult, Box<dyn Any + Send>>,
) -> TestResult {
    let report = owner.finish().await;
    let observed = match observed {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    };
    observed?;
    let report = report.expect("actual original task join report");
    assert!(report.actor().is_some_and(Result::is_ok));
    let observations = report.observations();
    let close = observations
        .peer_close()
        .expect("actual original peer Close");
    assert_eq!(close.channel(), 0);
    assert!(close.payload().is_empty());
    assert!(close.close().error.is_none());
    assert!(!close.locally_closing());
    assert_eq!(close.reply_state(), ServerPeerCloseReplyState::Ready);
    assert!(close.reply_result().is_some_and(Result::is_ok));
    match report.reader().expect("original Reader result") {
        Ok(()) => {}
        Err(error) => {
            let original = observations.reader().expect("original Reader observation");
            assert!(error.is_cancelled());
            assert_eq!(error.id(), original.id());
            assert!(original.requested_by(ServerConnectionAbortSource::ActorReaderShutdown));
        }
    }
    Ok(())
}
