use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use amqp::{
    Accepted, Attach, Begin, Body, ConnectionOptions, DeliveryState, Detach, Disposition, End,
    Flow, Frame, LinkEndpoint, Message, Open, Performative, ProtocolHeader, ReceiverSettleMode,
    Role, Sender, SenderSettleMode, ServerConnection, ServerSession, Source, Target, Transfer,
    decode_message, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{io::DuplexStream, sync::Notify};

mod flush_gate;

pub(super) use flush_gate::FlushGate;
use flush_gate::GatedIo;

pub(super) const CHANNEL: u16 = 19;
pub(super) const OTHER_CHANNEL: u16 = 23;
pub(super) const HANDLE: u32 = 7;
const LIMIT: Duration = Duration::from_secs(5);

pub(super) async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, future)
        .await
        .expect("bounded native wire operation")
}

pub(super) fn message() -> Message {
    Message {
        body: Body::Data(vec![b"reserved-original".to_vec().into()]),
        ..Message::default()
    }
}

pub(super) async fn pending<F: Future>(mut future: Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

pub(super) struct ReplyWake {
    woke: AtomicBool,
    changed: Notify,
}

impl ReplyWake {
    pub(super) fn poll_pending<F: Future>(future: Pin<&mut F>) -> Arc<Self> {
        let observer = Arc::new(Self {
            woke: AtomicBool::new(false),
            changed: Notify::new(),
        });
        let waker = Waker::from(observer.clone());
        assert!(future.poll(&mut Context::from_waker(&waker)).is_pending());
        observer
    }

    pub(super) async fn wait(&self) {
        bounded(async {
            while !self.woke.load(Ordering::Acquire) {
                self.changed.notified().await;
            }
        })
        .await;
    }
}

impl Wake for ReplyWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woke.store(true, Ordering::Release);
        self.changed.notify_one();
    }
}

pub(super) struct Peer {
    io: DuplexStream,
    channels: HashMap<u16, u16>,
    handles: HashMap<(u16, u32), u32>,
    transfers: HashMap<u16, u32>,
}

impl Peer {
    pub(super) fn local_channel(&self, peer_channel: u16) -> u16 {
        self.channels[&peer_channel]
    }

    pub(super) async fn send(&mut self, channel: u16, performative: Performative) {
        bounded(write_frame(
            &mut self.io,
            &Frame::Amqp {
                channel,
                performative: Some(performative),
                payload: Vec::new(),
            },
        ))
        .await
        .expect("valid peer frame");
    }

    async fn frame(&mut self) -> Frame {
        let frame = bounded(read_frame(&mut self.io))
            .await
            .expect("native frame");
        if let Frame::Amqp {
            channel,
            performative: Some(Performative::Transfer(_)),
            ..
        } = &frame
        {
            *self.transfers.entry(*channel).or_default() += 1;
        }
        frame
    }

    pub(super) async fn control(&mut self) -> (u16, Performative) {
        for _ in 0..16 {
            match self.frame().await {
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    payload,
                    ..
                } if payload.is_empty() => {}
                Frame::Amqp {
                    channel,
                    performative: Some(performative),
                    payload,
                } if payload.is_empty() => return (channel, performative),
                frame => panic!("unexpected native control frame: {frame:?}"),
            }
        }
        panic!("native control frame count exceeded");
    }

    pub(super) async fn grant(
        &mut self,
        channel: u16,
        handle: u32,
        count: u32,
        credit: u32,
        drain: bool,
        echo: bool,
    ) {
        self.grant_with_window(channel, handle, count, credit, drain, echo, 1_000)
            .await;
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn grant_with_window(
        &mut self,
        channel: u16,
        handle: u32,
        count: u32,
        credit: u32,
        drain: bool,
        echo: bool,
        incoming_window: u32,
    ) {
        self.send(
            channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(
                    self.transfers
                        .get(&self.channels[&channel])
                        .copied()
                        .unwrap_or(0),
                ),
                incoming_window,
                next_outgoing_id: 0,
                outgoing_window: 1_000,
                handle: Some(handle),
                delivery_count: Some(count),
                link_credit: Some(credit),
                drain,
                echo,
                ..Flow::default()
            }),
        )
        .await;
    }

    pub(super) async fn flow(
        &mut self,
        channel: u16,
        handle: u32,
        count: u32,
        credit: u32,
        drain: bool,
    ) -> Flow {
        self.grant(channel, handle, count, credit, drain, true)
            .await;
        let response = self.link_flow(channel).await;
        assert_eq!(response.handle, Some(self.handles[&(channel, handle)]));
        response
    }

    pub(super) async fn link_flow(&mut self, peer_channel: u16) -> Flow {
        for _ in 0..16 {
            match self.frame().await {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } if channel == self.channels[&peer_channel] && payload.is_empty() => {
                    if flow.handle.is_some() {
                        return flow;
                    }
                }
                frame => panic!("unexpected frame before exact link Flow: {frame:?}"),
            }
        }
        panic!("native link Flow frame count exceeded");
    }

    pub(super) async fn barrier(&mut self, peer_channel: u16) {
        self.window(peer_channel, 1_000).await;
    }

    pub(super) async fn window(&mut self, peer_channel: u16, incoming_window: u32) {
        self.send(
            peer_channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(
                    self.transfers
                        .get(&self.channels[&peer_channel])
                        .copied()
                        .unwrap_or(0),
                ),
                incoming_window,
                next_outgoing_id: 0,
                outgoing_window: 1_000,
                echo: true,
                ..Flow::default()
            }),
        )
        .await;
        for _ in 0..16 {
            match self.frame().await {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } if channel == self.channels[&peer_channel] && payload.is_empty() => {
                    if flow.handle.is_none() {
                        return;
                    }
                }
                frame => panic!("unexpected frame before exact session barrier: {frame:?}"),
            }
        }
        panic!("native session barrier frame count exceeded");
    }

    pub(super) async fn transfer(
        &mut self,
        peer_channel: u16,
        handle: u32,
        tag: &[u8],
    ) -> Transfer {
        self.transfer_message(peer_channel, handle, tag, &message())
            .await
    }

    pub(super) async fn transfer_message(
        &mut self,
        peer_channel: u16,
        handle: u32,
        tag: &[u8],
        expected: &Message,
    ) -> Transfer {
        for _ in 0..16 {
            match self.frame().await {
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    payload,
                    ..
                } if payload.is_empty() => {}
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Transfer(transfer)),
                    payload,
                } => {
                    assert_eq!(channel, self.channels[&peer_channel]);
                    assert_eq!(transfer.handle, handle);
                    assert_eq!(transfer.delivery_tag, Some(tag.to_vec().into()));
                    assert_eq!(transfer.settled, Some(false));
                    assert!(!transfer.more);
                    assert_eq!(
                        decode_message(&payload).expect("outgoing message"),
                        *expected
                    );
                    return transfer;
                }
                frame => panic!("unexpected native outgoing frame: {frame:?}"),
            }
        }
        panic!("native Transfer frame count exceeded");
    }

    pub(super) async fn accepted(&mut self, channel: u16, id: u32, settled: bool) {
        self.outcome(channel, id, settled, DeliveryState::Accepted(Accepted))
            .await;
    }

    pub(super) async fn outcome(
        &mut self,
        channel: u16,
        id: u32,
        settled: bool,
        state: DeliveryState,
    ) {
        self.send(
            channel,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled,
                state: Some(state),
                batchable: false,
            }),
        )
        .await;
    }

    pub(super) async fn detach(&mut self, channel: u16, peer_handle: u32, local_handle: u32) {
        self.send(
            channel,
            Performative::Detach(Detach {
                handle: peer_handle,
                closed: true,
                error: None,
            }),
        )
        .await;
        let (actual, Performative::Detach(response)) = self.control().await else {
            panic!("exact native Detach response");
        };
        assert_eq!(actual, self.channels[&channel]);
        assert_eq!(response.handle, local_handle);
        assert!(response.closed);
        self.handles.remove(&(channel, peer_handle));
    }

    pub(super) async fn oversized_detach(
        &mut self,
        channel: u16,
        peer_handle: u32,
        local_handle: u32,
    ) {
        let (actual, Performative::Detach(response)) = self.control().await else {
            panic!("scoped message-size Detach");
        };
        assert_eq!(actual, self.channels[&channel]);
        assert_eq!(response.handle, local_handle);
        assert!(response.closed);
        let error = response.error.expect("message-size error");
        assert_eq!(
            error.condition.as_symbol().as_str(),
            "amqp:link:message-size-exceeded"
        );
        self.send(
            channel,
            Performative::Detach(Detach {
                handle: peer_handle,
                closed: true,
                error: None,
            }),
        )
        .await;
        self.handles.remove(&(channel, peer_handle));
        self.barrier(channel).await;
    }

    pub(super) async fn end(&mut self, channel: u16) {
        self.send(channel, Performative::End(End::default())).await;
        let (actual, Performative::End(response)) = self.control().await else {
            panic!("exact native End response");
        };
        assert_eq!(actual, self.channels[&channel]);
        assert!(response.error.is_none());
        let local = self
            .channels
            .remove(&channel)
            .expect("live session channel");
        self.handles
            .retain(|(peer_channel, _), _| *peer_channel != channel);
        self.transfers.remove(&local);
    }
}

pub(super) struct Fixture {
    pub(super) connection: ServerConnection,
    pub(super) peer: Peer,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        Self::open(None).await
    }

    pub(super) async fn gated() -> (Self, Arc<FlushGate>) {
        let gate = Arc::new(FlushGate::new());
        (Self::open(Some(Arc::clone(&gate))).await, gate)
    }

    async fn open(gate: Option<Arc<FlushGate>>) -> Self {
        bounded(async {
            let (wire, mut io) = tokio::io::duplex(16_384);
            let wire = GatedIo::new(wire, gate);
            let opening = async {
                write_protocol_header(&mut io, ProtocolHeader::AMQP)
                    .await
                    .expect("peer protocol header");
                assert_eq!(
                    read_protocol_header(&mut io).await.expect("native header"),
                    ProtocolHeader::AMQP
                );
                let mut open = Open::new("credit-reservation-peer");
                open.channel_max = 3;
                write_frame(
                    &mut io,
                    &Frame::Amqp {
                        channel: 0,
                        performative: Some(Performative::Open(open)),
                        payload: Vec::new(),
                    },
                )
                .await
                .expect("peer Open");
                assert!(matches!(
                    read_frame(&mut io).await.expect("native Open"),
                    Frame::Amqp {
                        performative: Some(Performative::Open(_)),
                        ..
                    }
                ));
                io
            };
            let (connection, io) = tokio::join!(
                ServerConnection::accept_with_options(
                    wire,
                    "credit-reservation-native",
                    None,
                    ConnectionOptions::default().idle_timeout_millis(0),
                ),
                opening
            );
            Self {
                connection: connection.expect("native connection"),
                peer: Peer {
                    io,
                    channels: HashMap::new(),
                    handles: HashMap::new(),
                    transfers: HashMap::new(),
                },
            }
        })
        .await
    }

    pub(super) async fn session(&mut self, channel: u16) -> ServerSession {
        self.peer
            .send(channel, Performative::Begin(Begin::default()))
            .await;
        let incoming = bounded(self.connection.next_incoming_session())
            .await
            .expect("actual incoming session");
        let (session, (local, response)) = bounded(async {
            tokio::join!(
                self.connection.accept_session(incoming),
                self.peer.control()
            )
        })
        .await;
        let Performative::Begin(begin) = response else {
            panic!("native Begin response");
        };
        assert_eq!(begin.remote_channel, Some(channel));
        assert_ne!(local, channel);
        self.peer.channels.insert(channel, local);
        session.expect("accepted native session")
    }

    pub(super) async fn sender(
        &mut self,
        session: &mut ServerSession,
        channel: u16,
        peer_handle: u32,
        name: &str,
        mode: ReceiverSettleMode,
        maximum: Option<u64>,
    ) -> (Sender, u32) {
        self.peer
            .send(
                channel,
                Performative::Attach(Box::new(Attach {
                    name: name.to_owned(),
                    handle: peer_handle,
                    role: Role::Receiver,
                    snd_settle_mode: SenderSettleMode::Unsettled,
                    rcv_settle_mode: mode,
                    source: Some(Source::new("orders")),
                    target: Some(Target::new("orders").into()),
                    unsettled: None,
                    incomplete_unsettled: false,
                    initial_delivery_count: None,
                    max_message_size: maximum,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                })),
            )
            .await;
        let incoming = bounded(session.next_incoming_attach())
            .await
            .expect("actual incoming receiver");
        let (endpoint, (local, response)) = bounded(async {
            tokio::join!(session.accept_attach(incoming, 0), self.peer.control())
        })
        .await;
        assert_eq!(local, self.peer.channels[&channel]);
        let Performative::Attach(response) = response else {
            panic!("native sender Attach response");
        };
        assert_eq!(response.role, Role::Sender);
        assert_eq!(response.initial_delivery_count, Some(0));
        let LinkEndpoint::Sender(sender) = endpoint.expect("accepted sender") else {
            panic!("native Sender endpoint");
        };
        self.peer
            .handles
            .insert((channel, peer_handle), response.handle);
        (sender, response.handle)
    }

    pub(super) async fn shutdown(self) {
        bounded(self.connection.shutdown()).await;
        assert!(!self.connection.connection_identity().is_active());
    }
}
