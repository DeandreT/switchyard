use std::{
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use super::session_channels::{local_channel_for_peer, preferred_vacant_channel, vacant_channel};
use super::*;

#[derive(Clone, Default)]
struct Writer(Arc<Mutex<Vec<u8>>>);

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0
            .lock()
            .expect("captured bytes")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    writer: FrameWriter<Writer>,
    output: Writer,
    incoming_tx: mpsc::Sender<IncomingSession>,
    incoming: mpsc::Receiver<IncomingSession>,
    maximum: u16,
}

impl Fixture {
    fn new(maximum: u16) -> Self {
        let output = Writer::default();
        let (incoming_tx, incoming) = mpsc::channel(MAX_SESSIONS_PER_CONNECTION + 1);
        Self {
            sessions: HashMap::new(),
            writer: FrameWriter::new(output.clone(), 512).expect("frame writer"),
            output,
            incoming_tx,
            incoming,
            maximum,
        }
    }

    fn seed(&mut self, local: u16, peer: u16) {
        let mut session = SessionState::new(&Begin::default());
        session.peer_channel = Some(peer);
        session.local_begin_sent = true;
        assert!(self.sessions.insert(local, session).is_none());
    }

    async fn input(&mut self, peer: u16, performative: Performative) {
        let action = handle_frame(
            Frame::Amqp {
                channel: peer,
                performative: Some(performative),
                payload: Vec::new(),
            },
            &mut self.writer,
            &self.incoming_tx,
            &mut self.sessions,
            512,
            self.maximum,
            false,
        )
        .await
        .expect("channel handling is a protocol response");
        assert!(matches!(action, FrameAction::Continue));
    }

    async fn begin(&mut self, peer: u16) -> IncomingSession {
        self.input(peer, Performative::Begin(Begin::default()))
            .await;
        self.incoming.try_recv().expect("incoming session")
    }

    async fn approve(&mut self, receipt: &IncomingSession) -> Result<(), EngineError> {
        let (attach_tx, _) = mpsc::channel(MAX_PENDING_ATTACHES);
        let (reply, result) = oneshot::channel();
        let action = handle_command(
            Command::AcceptSession {
                channel: receipt.channel,
                identity: receipt.identity.clone(),
                attach_tx,
                reply,
            },
            &mut self.writer,
            &mut self.sessions,
            512,
        )
        .await
        .expect("approval command is local");
        assert!(matches!(action, CommandAction::Continue));
        result.await.expect("approval reply")
    }

    async fn frames(&self) -> Vec<Frame> {
        let bytes = std::mem::take(&mut *self.output.0.lock().expect("captured bytes"));
        let mut remaining = bytes.as_slice();
        let mut frames = Vec::new();
        while !remaining.is_empty() {
            let frame = read_frame(&mut remaining).await.expect("complete frame");
            assert!(crate::encode_frame(&frame).expect("frame encoding").len() <= 512);
            frames.push(frame);
        }
        frames
    }

    async fn assert_close(&self, condition: &str) {
        let frames = self.frames().await;
        let [
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Close(close)),
                payload,
            },
        ] = frames.as_slice()
        else {
            panic!("one bounded connection Close: {frames:?}");
        };
        assert!(payload.is_empty());
        assert_eq!(
            close
                .error
                .as_ref()
                .expect("Close error")
                .condition
                .as_symbol(),
            Symbol::from(condition)
        );
    }
}

fn echo() -> Performative {
    Performative::Flow(Flow {
        incoming_window: SESSION_WINDOW,
        outgoing_window: SESSION_WINDOW,
        echo: true,
        ..Flow::default()
    })
}

fn assert_begin(frame: &Frame, local: u16, peer: u16) {
    let Frame::Amqp {
        channel,
        performative: Some(Performative::Begin(begin)),
        payload,
    } = frame
    else {
        panic!("session Begin response");
    };
    assert_eq!(*channel, local);
    assert_eq!(begin.remote_channel, Some(peer));
    assert!(payload.is_empty());
}

#[test]
fn peer_lookup_does_not_use_local_numeric_equality_and_keeps_ending_bindings() {
    let mut fixture = Fixture::new(u16::MAX);
    fixture.seed(0, 1);
    fixture.seed(1, 0);
    fixture.seed(7, 42);
    fixture.sessions.get_mut(&1).expect("session").ending = true;
    fixture.sessions[&1].identity.retire();
    assert_eq!(local_channel_for_peer(0, &fixture.sessions), Some(1));
    assert_eq!(local_channel_for_peer(1, &fixture.sessions), Some(0));
    assert_eq!(local_channel_for_peer(42, &fixture.sessions), Some(7));
    assert_eq!(local_channel_for_peer(7, &fixture.sessions), None);
    assert_eq!(local_channel_for_peer(2, &fixture.sessions), None);
    fixture
        .sessions
        .insert(2, SessionState::new(&Begin::default()));
    assert_eq!(local_channel_for_peer(2, &fixture.sessions), None);
}

#[test]
fn local_allocation_is_inclusive_bounded_wrap_safe_and_prefers_only_vacant_channels() {
    let mut sessions = HashMap::new();
    assert_eq!(vacant_channel(0, 0, &sessions), Some(0));
    assert_eq!(preferred_vacant_channel(42, 1, &sessions), Some(0));
    assert_eq!(preferred_vacant_channel(1, 1, &sessions), Some(1));
    sessions.insert(1, SessionState::new(&Begin::default()));
    assert_eq!(preferred_vacant_channel(1, 1, &sessions), Some(0));
    sessions.insert(0, SessionState::new(&Begin::default()));
    assert_eq!(vacant_channel(1, 1, &sessions), None);
    assert_eq!(preferred_vacant_channel(42, 1, &sessions), None);
    sessions.clear();
    sessions.insert(u16::MAX, SessionState::new(&Begin::default()));
    assert_eq!(vacant_channel(u16::MAX, u16::MAX, &sessions), Some(0));
    sessions.insert(0, SessionState::new(&Begin::default()));
    assert_eq!(vacant_channel(u16::MAX, u16::MAX, &sessions), Some(1));
    sessions.clear();
    for channel in 0..MAX_SESSIONS_PER_CONNECTION as u16 {
        let mut session = SessionState::new(&Begin::default());
        session.ending = true;
        sessions.insert(channel, session);
    }
    assert_eq!(
        vacant_channel(0, u16::MAX, &sessions),
        Some(MAX_SESSIONS_PER_CONNECTION as u16)
    );
}

#[tokio::test]
async fn preapproval_echo_uses_allocated_local_channel_and_echoes_the_peer_association() {
    let mut fixture = Fixture::new(0);
    let receipt = fixture.begin(17).await;
    assert_eq!(receipt.channel, 0);
    assert_eq!(fixture.sessions[&0].peer_channel, Some(17));
    assert!(!fixture.sessions[&0].local_begin_sent);
    assert!(fixture.frames().await.is_empty());
    fixture.input(17, echo()).await;
    let frames = fixture.frames().await;
    assert_eq!(frames.len(), 2);
    assert_begin(&frames[0], 0, 17);
    assert!(matches!(&frames[1], Frame::Amqp {
        channel: 0, performative: Some(Performative::Flow(_)), payload,
    } if payload.is_empty()));
    assert!(fixture.sessions[&0].attach_tx.is_none());
    fixture.approve(&receipt).await.expect("session approved");
    assert!(fixture.frames().await.is_empty());
}

#[tokio::test]
async fn crossed_peer_channels_route_flow_and_end_to_their_exact_local_sessions() {
    let mut fixture = Fixture::new(1);
    fixture.seed(0, 1);
    fixture.seed(1, 0);
    let first = fixture.sessions[&0].identity.clone();
    let second = fixture.sessions[&1].identity.clone();
    fixture.input(0, echo()).await;
    fixture.input(1, echo()).await;
    let frames = fixture.frames().await;
    assert!(matches!(
        frames.as_slice(),
        [
            Frame::Amqp {
                channel: 1,
                performative: Some(Performative::Flow(_)),
                ..
            },
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Flow(_)),
                ..
            },
        ]
    ));
    fixture.input(0, Performative::End(End::default())).await;
    assert!(first.same_session(&fixture.sessions[&0].identity));
    assert!(!first.is_retired());
    assert!(second.is_retired());
    assert!(!fixture.sessions.contains_key(&1));
    assert!(matches!(
        fixture.frames().await.as_slice(),
        [Frame::Amqp {
            channel: 1,
            performative: Some(Performative::End(_)),
            ..
        },]
    ));
}

#[tokio::test]
async fn unbound_incoming_end_cannot_remove_a_numerically_equal_local_session() {
    let mut fixture = Fixture::new(0);
    fixture.seed(0, 17);
    fixture.input(0, Performative::End(End::default())).await;
    fixture.assert_close("amqp:connection:framing-error").await;
    assert_eq!(fixture.sessions[&0].peer_channel, Some(17));
    assert_eq!(local_channel_for_peer(17, &fixture.sessions), Some(0));
}

#[tokio::test]
async fn duplicate_incoming_begin_and_unexpected_response_publish_no_new_session() {
    for response in [false, true] {
        let mut fixture = Fixture::new(1);
        let receipt = fixture.begin(17).await;
        assert_eq!(receipt.channel, 0);
        fixture
            .input(
                17,
                Performative::Begin(Begin {
                    remote_channel: response.then_some(0),
                    ..Begin::default()
                }),
            )
            .await;
        fixture.assert_close("amqp:connection:framing-error").await;
        assert_eq!(fixture.sessions.len(), 1);
        assert!(fixture.incoming.try_recv().is_err());
    }
    let mut fixture = Fixture::new(1);
    fixture
        .input(
            17,
            Performative::Begin(Begin {
                remote_channel: Some(0),
                ..Begin::default()
            }),
        )
        .await;
    fixture.assert_close("amqp:connection:framing-error").await;
    assert!(fixture.sessions.is_empty());
    assert!(fixture.incoming.try_recv().is_err());
}

#[tokio::test]
async fn exhausted_peer_output_range_keeps_pending_and_ending_channels_reserved() {
    for ending in [false, true] {
        let mut fixture = Fixture::new(0);
        let receipt = fixture.begin(17).await;
        assert_eq!(receipt.channel, 0);
        if ending {
            refuse_session(
                0,
                "amqp:invalid-field",
                "test session refusal",
                &mut fixture.writer,
                &mut fixture.sessions,
            )
            .await
            .expect("session refusal");
            let frames = fixture.frames().await;
            assert_eq!(frames.len(), 2);
            assert_begin(&frames[0], 0, 17);
            assert!(matches!(
                &frames[1],
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::End(_)),
                    ..
                }
            ));
        }
        fixture
            .input(18, Performative::Begin(Begin::default()))
            .await;
        fixture.assert_close("amqp:resource-limit-exceeded").await;
        assert_eq!(fixture.sessions.len(), 1);
        assert_eq!(fixture.sessions[&0].peer_channel, Some(17));
        assert!(fixture.incoming.try_recv().is_err());
    }
}

#[tokio::test]
async fn ending_session_discards_only_its_mapped_peer_traffic_until_end() {
    let mut fixture = Fixture::new(1);
    fixture.seed(0, 17);
    fixture.seed(1, 0);
    refuse_session(
        0,
        "amqp:invalid-field",
        "test session refusal",
        &mut fixture.writer,
        &mut fixture.sessions,
    )
    .await
    .expect("session refusal");
    assert!(matches!(
        fixture.frames().await.as_slice(),
        [Frame::Amqp {
            channel: 0,
            performative: Some(Performative::End(_)),
            ..
        },]
    ));
    fixture
        .input(17, Performative::Begin(Begin::default()))
        .await;
    fixture.input(17, echo()).await;
    assert!(fixture.frames().await.is_empty());
    fixture.input(0, echo()).await;
    assert!(matches!(
        fixture.frames().await.as_slice(),
        [Frame::Amqp {
            channel: 1,
            performative: Some(Performative::Flow(_)),
            ..
        },]
    ));
    fixture.input(17, Performative::End(End::default())).await;
    assert!(fixture.frames().await.is_empty());
    assert!(!fixture.sessions.contains_key(&0));
    assert_eq!(fixture.sessions[&1].peer_channel, Some(0));
}

#[tokio::test]
async fn peer_acknowledged_slot_reuse_does_not_accept_an_old_session_receipt() {
    let mut fixture = Fixture::new(0);
    let old = fixture.begin(17).await;
    fixture.input(17, Performative::End(End::default())).await;
    let frames = fixture.frames().await;
    assert_eq!(frames.len(), 2);
    assert_begin(&frames[0], 0, 17);
    assert!(matches!(
        &frames[1],
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::End(_)),
            ..
        }
    ));
    assert!(old.identity.is_retired());
    let fresh = fixture.begin(19).await;
    assert_eq!(fresh.channel, old.channel);
    assert!(!fresh.identity.same_session(&old.identity));
    assert_eq!(fixture.sessions[&0].peer_channel, Some(19));
    assert!(matches!(
        fixture.approve(&old).await,
        Err(EngineError::RemoteDetached)
    ));
    assert!(fixture.frames().await.is_empty());
    assert!(!fixture.sessions[&0].local_begin_sent);
    assert!(fixture.sessions[&0].attach_tx.is_none());
    fixture
        .approve(&fresh)
        .await
        .expect("fresh session approved");
    let frames = fixture.frames().await;
    assert_eq!(frames.len(), 1);
    assert_begin(&frames[0], 0, 19);
}

#[tokio::test]
async fn duplicate_open_on_an_ending_peer_binding_is_not_discarded() {
    let mut fixture = Fixture::new(0);
    fixture.seed(0, 17);
    let session = fixture.sessions.get_mut(&0).expect("bound session");
    session.ending = true;
    stop_session(session);
    let result = handle_frame(
        Frame::Amqp {
            channel: 17,
            performative: Some(Performative::Open(Open::new("duplicate-peer"))),
            payload: Vec::new(),
        },
        &mut fixture.writer,
        &fixture.incoming_tx,
        &mut fixture.sessions,
        512,
        fixture.maximum,
        false,
    )
    .await;
    assert!(
        matches!(result, Err(EngineError::InvalidState(ref message)) if message == "duplicate AMQP open")
    );
    assert!(fixture.frames().await.is_empty());
    assert_eq!(fixture.sessions[&0].peer_channel, Some(17));
    assert!(fixture.sessions[&0].ending);
}
