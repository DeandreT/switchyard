use std::{
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use tokio::{io::DuplexStream, time::timeout};

use super::*;
use crate::{Source, Target};

const DEADLINE: Duration = Duration::from_secs(2);

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    identities: HashMap<u16, SessionIdentity>,
    writer: FrameWriter<DuplexStream>,
    peer: DuplexStream,
    incoming_tx: mpsc::Sender<IncomingSession>,
    incoming: mpsc::Receiver<IncomingSession>,
}

impl Fixture {
    fn new(capacity: usize) -> Self {
        let (wire, peer) = tokio::io::duplex(64 * 1024);
        let (incoming_tx, incoming) = mpsc::channel(capacity);
        Self {
            sessions: HashMap::new(),
            identities: HashMap::new(),
            writer: FrameWriter::new(wire, 512).expect("frame writer"),
            peer,
            incoming_tx,
            incoming,
        }
    }

    async fn input(&mut self, channel: u16, performative: Performative) {
        let action = self
            .input_result(channel, performative)
            .await
            .expect("session handling does not close the connection");
        assert!(matches!(action, FrameAction::Continue));
    }

    async fn input_result(
        &mut self,
        channel: u16,
        performative: Performative,
    ) -> Result<FrameAction, EngineError> {
        let begins = matches!(&performative, Performative::Begin(_));
        let result = timeout(
            DEADLINE,
            handle_frame(
                Frame::Amqp {
                    channel,
                    performative: Some(performative),
                    payload: Vec::new(),
                },
                &mut self.writer,
                &self.incoming_tx,
                &mut self.sessions,
                512,
                u16::MAX,
                false,
            ),
        )
        .await
        .expect("session response is prompt");
        if begins && let Some(session) = self.sessions.get(&channel) {
            self.identities.insert(channel, session.identity.clone());
        }
        result
    }

    async fn begin(&mut self, channel: u16) {
        self.input(channel, Performative::Begin(Begin::default()))
            .await;
        let incoming = self.incoming.try_recv().expect("pending peer Begin");
        assert_eq!(incoming.channel, channel);
        assert!(!self.sessions[&channel].local_begin_sent);
        assert!(self.sessions[&channel].attach_tx.is_none());
    }

    async fn approve(
        &mut self,
        channel: u16,
    ) -> (Result<(), EngineError>, mpsc::Receiver<IncomingAttach>) {
        let (attach_tx, attaches) = mpsc::channel(MAX_PENDING_ATTACHES);
        let (reply, result) = oneshot::channel();
        let action = timeout(
            DEADLINE,
            handle_command(
                Command::AcceptSession {
                    channel,
                    identity: self.identities[&channel].clone(),
                    attach_tx,
                    reply,
                },
                &mut self.writer,
                &mut self.sessions,
                512,
            ),
        )
        .await
        .expect("session approval is prompt")
        .expect("approval failure is local");
        assert!(matches!(action, CommandAction::Continue));
        (result.await.expect("approval result"), attaches)
    }

    async fn frame(&mut self) -> (u16, Performative) {
        next_frame(&mut self.peer).await
    }

    async fn assert_begin(&mut self, channel: u16) {
        let (actual, performative) = self.frame().await;
        assert_eq!(actual, channel);
        let Performative::Begin(begin) = performative else {
            panic!("a local Begin must precede any other session response");
        };
        assert_eq!(begin.remote_channel, Some(channel));
    }

    async fn assert_end(&mut self, channel: u16, condition: Option<&str>) {
        let (actual, performative) = self.frame().await;
        assert_eq!(actual, channel);
        let Performative::End(end) = performative else {
            panic!("session End response");
        };
        assert_eq!(
            end.error.map(|error| error.condition.as_symbol()),
            condition.map(Symbol::from)
        );
    }

    async fn assert_silent(&mut self) {
        assert_silent(&mut self.peer).await;
    }
}

async fn next_frame(peer: &mut DuplexStream) -> (u16, Performative) {
    let frame = timeout(DEADLINE, read_frame(peer))
        .await
        .expect("prompt session frame")
        .expect("encoded session frame");
    let Frame::Amqp {
        channel,
        performative: Some(performative),
        payload,
    } = frame
    else {
        panic!("AMQP session performative");
    };
    assert!(payload.is_empty());
    (channel, performative)
}

async fn assert_silent(peer: &mut DuplexStream) {
    assert!(
        timeout(Duration::from_millis(20), read_frame(peer))
            .await
            .is_err(),
        "no duplicate Begin or unsolicited session response"
    );
}

fn flow() -> Flow {
    Flow {
        incoming_window: 2048,
        outgoing_window: 2048,
        ..Flow::default()
    }
}

fn attach(handle: u32, role: Role) -> Attach {
    Attach {
        name: format!("startup-{handle}"),
        handle,
        role: role.clone(),
        snd_settle_mode: SenderSettleMode::Mixed,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue")),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: (role == Role::Sender).then_some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

fn unattached_transfer() -> Transfer {
    Transfer {
        handle: 99,
        delivery_id: Some(0),
        delivery_tag: Some(vec![0].into()),
        message_format: Some(0),
        settled: None,
        more: false,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

#[tokio::test]
async fn preapproval_flow_echo_starts_wire_session_once_without_approving_links() {
    let mut fixture = Fixture::new(32);
    fixture.begin(3).await;
    fixture
        .input(3, Performative::Attach(Box::new(attach(1, Role::Sender))))
        .await;
    fixture.assert_silent().await;
    assert_eq!(fixture.sessions[&3].pending_attach_events.len(), 1);
    fixture
        .input(
            3,
            Performative::Flow(Flow {
                echo: true,
                ..flow()
            }),
        )
        .await;
    fixture.assert_begin(3).await;
    assert!(matches!(fixture.frame().await, (3, Performative::Flow(_))));
    assert!(fixture.sessions[&3].local_begin_sent);
    assert!(fixture.sessions[&3].attach_tx.is_none());
    assert!(fixture.sessions[&3].links.is_empty());
    fixture.assert_silent().await;

    let (result, mut approvals) = fixture.approve(3).await;
    result.expect("application session approval still succeeds");
    assert_eq!(approvals.try_recv().expect("one link approval").handle, 1);
    assert!(approvals.try_recv().is_err());
    fixture.assert_silent().await;
    let (duplicate, _) = fixture.approve(3).await;
    assert!(matches!(duplicate, Err(EngineError::InvalidState(_))));
    assert!(approvals.try_recv().is_err());
    fixture.assert_silent().await;

    fixture
        .input(
            3,
            Performative::Flow(Flow {
                echo: true,
                ..flow()
            }),
        )
        .await;
    assert!(matches!(fixture.frame().await, (3, Performative::Flow(_))));
    fixture.assert_silent().await;
}

#[tokio::test]
async fn pending_detach_starts_wire_session_then_cancels_only_that_approval() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new(32);
        fixture.begin(5).await;
        fixture
            .input(5, Performative::Attach(Box::new(attach(1, role.clone()))))
            .await;
        fixture
            .input(
                5,
                Performative::Detach(Detach {
                    handle: 1,
                    closed: true,
                    error: None,
                }),
            )
            .await;
        fixture.assert_begin(5).await;
        let (channel, performative) = fixture.frame().await;
        assert_eq!(channel, 5);
        let Performative::Detach(detach) = performative else {
            panic!("pending link Detach acknowledgement");
        };
        assert_eq!(detach.handle, 1);
        assert!(detach.closed);
        assert!(detach.error.is_none());
        assert!(fixture.sessions[&5].local_begin_sent);
        assert!(fixture.sessions[&5].pending_attaches.is_empty());
        assert!(fixture.sessions[&5].pending_attach_events.is_empty());
        assert!(!fixture.sessions[&5].ending);
        fixture.assert_silent().await;

        let (result, mut approvals) = fixture.approve(5).await;
        result.expect("pending Detach does not cancel session approval");
        assert!(approvals.try_recv().is_err());
        fixture.assert_silent().await;
        fixture
            .input(5, Performative::Attach(Box::new(attach(1, role))))
            .await;
        assert_eq!(approvals.try_recv().expect("fresh reused handle").handle, 1);
        fixture.assert_silent().await;
    }
}

#[tokio::test]
async fn immediate_peer_end_starts_wire_session_before_ack_and_never_starts_unknown_sessions() {
    let mut fixture = Fixture::new(32);
    fixture.begin(7).await;
    fixture.input(7, Performative::End(End::default())).await;
    fixture.assert_begin(7).await;
    fixture.assert_end(7, None).await;
    assert!(!fixture.sessions.contains_key(&7));
    let (result, _) = fixture.approve(7).await;
    assert!(matches!(result, Err(EngineError::RemoteDetached)));
    fixture.assert_silent().await;
    fixture.begin(7).await;
    fixture
        .input(
            7,
            Performative::Flow(Flow {
                echo: true,
                ..flow()
            }),
        )
        .await;
    fixture.assert_begin(7).await;
    assert!(matches!(fixture.frame().await, (7, Performative::Flow(_))));
    fixture.assert_silent().await;

    for performative in [
        Performative::Flow(flow()),
        Performative::Transfer(unattached_transfer()),
        Performative::End(End::default()),
    ] {
        let mut unknown = Fixture::new(32);
        unknown.input(99, performative).await;
        let (channel, performative) = unknown.frame().await;
        assert_eq!(channel, 0);
        let Performative::Close(close) = performative else {
            panic!("unknown peer channel gets a connection Close, not a Begin");
        };
        assert_eq!(
            close.error.expect("framing error").condition.as_symbol(),
            Symbol::from("amqp:connection:framing-error")
        );
        assert!(unknown.sessions.is_empty());
        assert!(unknown.incoming.try_recv().is_err());
        unknown.assert_silent().await;
    }
}

#[tokio::test]
async fn preapproval_refusals_start_wire_session_then_end_without_publishing_pending_links() {
    for (case, condition) in [
        (0, "amqp:session:handle-in-use"),
        (1, "amqp:resource-limit-exceeded"),
        (2, "amqp:invalid-field"),
        (3, "amqp:session:unattached-handle"),
    ] {
        let mut fixture = Fixture::new(32);
        fixture.begin(9).await;
        match case {
            0 => {
                for _ in 0..2 {
                    fixture
                        .input(9, Performative::Attach(Box::new(attach(0, Role::Sender))))
                        .await;
                }
            }
            1 => {
                for handle in 0..MAX_PENDING_ATTACHES as u32 {
                    fixture
                        .input(
                            9,
                            Performative::Attach(Box::new(attach(handle, Role::Sender))),
                        )
                        .await;
                }
                assert_eq!(
                    fixture.sessions[&9].pending_attach_events.len(),
                    MAX_PENDING_ATTACHES
                );
                fixture.assert_silent().await;
                fixture
                    .input(
                        9,
                        Performative::Attach(Box::new(attach(
                            MAX_PENDING_ATTACHES as u32,
                            Role::Sender,
                        ))),
                    )
                    .await;
            }
            2 => {
                fixture
                    .input(
                        9,
                        Performative::Flow(Flow {
                            link_credit: Some(1),
                            ..flow()
                        }),
                    )
                    .await;
            }
            3 => {
                fixture
                    .input(9, Performative::Transfer(unattached_transfer()))
                    .await;
            }
            _ => unreachable!("four refusal cases"),
        }
        fixture.assert_begin(9).await;
        fixture.assert_end(9, Some(condition)).await;
        let state = &fixture.sessions[&9];
        assert!(state.local_begin_sent);
        assert!(state.ending);
        assert!(state.attach_tx.is_none());
        assert!(state.pending_attach_events.is_empty());
        assert!(state.pending_attaches.is_empty());
        assert!(state.identity.is_retired());
        assert!(state.links.is_empty());
        let (result, mut approvals) = fixture.approve(9).await;
        assert!(matches!(result, Err(EngineError::RemoteDetached)));
        assert!(approvals.try_recv().is_err());
        fixture
            .input(9, Performative::Attach(Box::new(attach(0, Role::Sender))))
            .await;
        fixture.input(9, Performative::End(End::default())).await;
        assert!(!fixture.sessions.contains_key(&9));
        fixture.assert_silent().await;

        fixture.begin(10).await;
        let (result, _) = fixture.approve(10).await;
        result.expect("another session remains usable after refusal");
        fixture.assert_begin(10).await;
        fixture.assert_silent().await;
    }
}

#[tokio::test]
async fn incoming_session_queue_overflow_starts_only_refused_channel_and_preserves_pending_sibling()
{
    let mut fixture = Fixture::new(1);
    fixture
        .input(11, Performative::Begin(Begin::default()))
        .await;
    assert!(!fixture.sessions[&11].local_begin_sent);
    fixture
        .input(12, Performative::Begin(Begin::default()))
        .await;
    fixture.assert_begin(12).await;
    fixture
        .assert_end(12, Some("amqp:resource-limit-exceeded"))
        .await;
    assert!(fixture.sessions[&12].local_begin_sent);
    assert!(fixture.sessions[&12].ending);
    assert!(!fixture.sessions[&11].local_begin_sent);
    assert!(!fixture.sessions[&11].ending);
    assert_eq!(
        fixture
            .incoming
            .try_recv()
            .expect("pending sibling")
            .channel,
        11
    );
    assert!(fixture.incoming.try_recv().is_err());
    fixture.assert_silent().await;

    let (result, _) = fixture.approve(11).await;
    result.expect("queued sibling can still be approved");
    fixture.assert_begin(11).await;
    fixture.input(12, Performative::End(End::default())).await;
    assert!(!fixture.sessions.contains_key(&12));
    fixture.assert_silent().await;
}

struct FailedBeginWriter {
    fail_write: bool,
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl AsyncWrite for FailedBeginWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.fail_write {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected Begin write failure",
            )));
        }
        self.bytes
            .lock()
            .expect("captured bytes")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "injected Begin flush failure",
        )))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn local_begin_flag_is_not_published_on_failed_write_or_flush() {
    for fail_write in [true, false] {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let mut writer = FrameWriter::new(
            FailedBeginWriter {
                fail_write,
                bytes: bytes.clone(),
            },
            512,
        )
        .expect("frame writer");
        let mut session = SessionState::new(&Begin::default());
        session.peer_channel = Some(0);
        assert!(!session.local_begin_sent);
        let result = timeout(DEADLINE, ensure_local_begin(0, &mut session, &mut writer))
            .await
            .expect("failed IO is prompt");
        assert!(matches!(result, Err(EngineError::Io(_))));
        assert!(!session.local_begin_sent);
        assert!(session.attach_tx.is_none());
        assert_eq!(bytes.lock().expect("captured bytes").is_empty(), fail_write);
    }
}

#[cfg(feature = "test-client")]
async fn client_pair() -> (ClientConnection, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let responding = async {
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("client header");
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        assert!(matches!(
            next_frame(&mut peer).await,
            (0, Performative::Open(_))
        ));
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open::new("startup-peer")),
            Vec::new(),
        )
        .await
        .expect("peer Open");
        peer
    };
    let (connection, peer) = timeout(DEADLINE, async {
        tokio::join!(
            ClientConnection::open(wire, "startup-client", None),
            responding
        )
    })
    .await
    .expect("client handshake is prompt");
    (connection.expect("client connection"), peer)
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_does_not_repeat_its_completed_begin_for_echo_or_early_refusal() {
    for peer_begins in [false, true] {
        let (mut connection, mut peer) = client_pair().await;
        let responding = async {
            let (channel, performative) = next_frame(&mut peer).await;
            assert!(matches!(performative, Performative::Begin(_)));
            write_amqp(
                &mut peer,
                channel,
                Performative::Begin(Begin {
                    remote_channel: Some(channel),
                    ..Begin::default()
                }),
                Vec::new(),
            )
            .await
            .expect("peer session response");
            if !peer_begins {
                write_amqp(
                    &mut peer,
                    channel,
                    Performative::End(End::default()),
                    Vec::new(),
                )
                .await
                .expect("peer early End after its Begin association");
            }
            channel
        };
        let (session, channel) = timeout(DEADLINE, async {
            tokio::join!(connection.begin(), responding)
        })
        .await
        .expect("client session response is prompt");
        if peer_begins {
            session.expect("client session accepted");
            write_amqp(
                &mut peer,
                channel,
                Performative::Flow(Flow {
                    echo: true,
                    ..flow()
                }),
                Vec::new(),
            )
            .await
            .expect("peer echo request");
            assert!(
                matches!(next_frame(&mut peer).await, (actual, Performative::Flow(_)) if actual == channel)
            );
            write_amqp(
                &mut peer,
                channel,
                Performative::Flow(Flow {
                    handle: Some(99),
                    delivery_count: Some(0),
                    link_credit: Some(1),
                    ..flow()
                }),
                Vec::new(),
            )
            .await
            .expect("unattached link Flow");
            let (actual, performative) = next_frame(&mut peer).await;
            assert_eq!(actual, channel);
            let Performative::End(end) = performative else {
                panic!("client refusal must not repeat Begin");
            };
            assert_eq!(
                end.error.expect("session refusal").condition.as_symbol(),
                Symbol::from("amqp:session:unattached-handle")
            );
            write_amqp(
                &mut peer,
                channel,
                Performative::End(End::default()),
                Vec::new(),
            )
            .await
            .expect("peer End acknowledgement");
        } else {
            session.expect("the valid Begin response resolves before the early End");
            assert!(
                matches!(next_frame(&mut peer).await, (actual, Performative::End(_)) if actual == channel)
            );
        }
        assert_silent(&mut peer).await;

        let responding = async {
            let (fresh_channel, performative) = next_frame(&mut peer).await;
            assert_ne!(fresh_channel, channel);
            assert!(matches!(performative, Performative::Begin(_)));
            write_amqp(
                &mut peer,
                fresh_channel,
                Performative::Begin(Begin {
                    remote_channel: Some(fresh_channel),
                    ..Begin::default()
                }),
                Vec::new(),
            )
            .await
            .expect("healthy sibling Begin");
        };
        let (fresh, ()) = timeout(DEADLINE, async {
            tokio::join!(connection.begin(), responding)
        })
        .await
        .expect("connection survives session refusal");
        fresh.expect("fresh session remains usable");
        assert_silent(&mut peer).await;
        timeout(DEADLINE, connection.shutdown())
            .await
            .expect("owned client task cleanup");
    }
}
