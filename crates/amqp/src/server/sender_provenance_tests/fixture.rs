use std::future::Future;

use tokio::io::DuplexStream;

use super::*;
use crate::{Body, Source, Target};

pub(super) const DEADLINE: Duration = Duration::from_secs(3);
pub(super) const CHANNEL: u16 = 19;
pub(super) const OTHER_CHANNEL: u16 = 23;
pub(super) const HANDLE: u32 = 7;
pub(super) const OTHER_HANDLE: u32 = 8;
pub(super) const TAG: &[u8] = b"private-delivery-tag";

pub(super) async fn bounded<T>(name: &'static str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, future).await.expect(name)
}

pub(super) fn message() -> Message {
    Message {
        body: Body::Data(vec![b"private-message-body".to_vec().into()]),
        ..Message::default()
    }
}

pub(super) struct Peer {
    pub(super) io: DuplexStream,
    channels: HashMap<u16, u16>,
    handles: HashMap<u16, HashSet<u32>>,
    received: HashMap<u16, u32>,
}

impl Peer {
    pub(super) async fn send(&mut self, channel: u16, performative: Performative) {
        bounded(
            "bounded peer frame write",
            write_amqp(&mut self.io, channel, performative, Vec::new()),
        )
        .await
        .expect("valid peer frame");
    }

    pub(super) async fn frame(&mut self) -> Frame {
        let frame = bounded("bounded native frame response", read_frame(&mut self.io))
            .await
            .expect("valid native frame");
        if let Frame::Amqp {
            channel,
            performative: Some(Performative::Transfer(_)),
            ..
        } = &frame
        {
            *self.received.entry(*channel).or_default() += 1;
        }
        frame
    }

    fn allowed_flow(&self, frame: &Frame) -> bool {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Flow(flow)),
            payload,
        } = frame
        else {
            return false;
        };
        payload.is_empty()
            && self.channels.values().any(|known| known == channel)
            && flow.handle.is_none_or(|handle| {
                self.handles
                    .get(channel)
                    .is_some_and(|known| known.contains(&handle))
            })
    }

    pub(super) async fn control(&mut self) -> (u16, Performative) {
        for _ in 0..16 {
            let frame = self.frame().await;
            if self.allowed_flow(&frame) {
                continue;
            }
            match frame {
                Frame::Amqp {
                    channel,
                    performative: Some(performative),
                    payload,
                } if payload.is_empty() => {
                    return (channel, performative);
                }
                frame => panic!("unexpected native control response: {frame:?}"),
            }
        }
        panic!("no control response within bounded frame count");
    }

    pub(super) async fn transfer(&mut self, peer_channel: u16, local_handle: u32) -> Transfer {
        self.transfer_with_settlement(peer_channel, local_handle, false)
            .await
    }

    pub(super) async fn transfer_with_settlement(
        &mut self,
        peer_channel: u16,
        local_handle: u32,
        settled: bool,
    ) -> Transfer {
        for _ in 0..16 {
            let frame = self.frame().await;
            if self.allowed_flow(&frame) {
                continue;
            }
            let Frame::Amqp {
                channel,
                performative: Some(Performative::Transfer(transfer)),
                payload,
            } = frame
            else {
                panic!("expected actual outgoing delivery: {frame:?}");
            };
            assert_eq!(channel, self.channels[&peer_channel]);
            assert_eq!(transfer.handle, local_handle);
            assert_eq!(transfer.delivery_tag, Some(TAG.to_vec().into()));
            assert_eq!(transfer.message_format, Some(0));
            assert_eq!(transfer.settled, Some(settled));
            assert!(!transfer.more);
            assert!(transfer.state.is_none());
            assert_eq!(
                decode_message(&payload).expect("decoded outgoing payload"),
                message()
            );
            return transfer;
        }
        panic!("no delivery within bounded frame count");
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

    pub(super) async fn barrier(&mut self, peer_channel: u16) {
        let channel = self.channels[&peer_channel];
        self.send(
            peer_channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(self.received.get(&channel).copied().unwrap_or(0)),
                incoming_window: 1_000,
                next_outgoing_id: 0,
                outgoing_window: 1_000,
                handle: None,
                delivery_count: None,
                link_credit: None,
                available: None,
                drain: false,
                echo: true,
                properties: None,
            }),
        )
        .await;
        for _ in 0..16 {
            let frame = self.frame().await;
            match &frame {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } if *actual == channel && flow.handle.is_none() && payload.is_empty() => return,
                _ if self.allowed_flow(&frame) => {}
                _ => panic!("unexpected frame before exact session barrier: {frame:?}"),
            }
        }
        panic!("no exact session barrier within bounded frame count");
    }

    pub(super) async fn end(&mut self, peer_channel: u16) {
        self.send(peer_channel, Performative::End(End { error: None }))
            .await;
        let (channel, response) = self.control().await;
        assert_eq!(channel, self.channels[&peer_channel]);
        assert!(matches!(response, Performative::End(End { error: None })));
        self.forget_session(peer_channel);
    }

    pub(super) fn forget_session(&mut self, peer_channel: u16) {
        let local = self.channels.remove(&peer_channel).expect("live session");
        self.handles.remove(&local);
        self.received.remove(&local);
    }
}

pub(super) struct Fixture {
    pub(super) connection: ServerConnection,
    pub(super) peer: Peer,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        Self::with_native_transactions(false).await
    }

    pub(super) async fn with_native_transactions(enabled: bool) -> Self {
        bounded("bounded native connection opening", async {
            let (wire, mut io) = tokio::io::duplex(16_384);
            let opening = async {
                write_protocol_header(&mut io, ProtocolHeader::AMQP)
                    .await
                    .expect("peer header");
                expect_header(&mut io, ProtocolHeader::AMQP)
                    .await
                    .expect("native header");
                let mut open = Open::new("same-container");
                open.channel_max = 3;
                write_amqp(&mut io, 0, Performative::Open(open), Vec::new())
                    .await
                    .expect("peer Open");
                assert!(matches!(
                    read_frame(&mut io).await.expect("native Open"),
                    Frame::Amqp {
                        channel: 0,
                        performative: Some(Performative::Open(_)),
                        ..
                    }
                ));
                io
            };
            let accepting = async {
                let options = ConnectionOptions::default().idle_timeout_millis(0);
                if enabled {
                    ServerConnection::accept_with_transactional_ingress(
                        wire,
                        "same-container",
                        None,
                        options,
                    )
                    .await
                } else {
                    ServerConnection::accept_with_options(wire, "same-container", None, options)
                        .await
                }
            };
            let (connection, io) = tokio::join!(accepting, opening);
            Self {
                connection: connection.expect("native connection accepted"),
                peer: Peer {
                    io,
                    channels: HashMap::new(),
                    handles: HashMap::new(),
                    received: HashMap::new(),
                },
            }
        })
        .await
    }

    pub(super) async fn session(&mut self, peer_channel: u16) -> ServerSession {
        bounded("bounded native session admission", async {
            self.peer
                .send(
                    peer_channel,
                    Performative::Begin(Begin {
                        handle_max: 1,
                        ..Begin::default()
                    }),
                )
                .await;
            let incoming = self
                .connection
                .next_incoming_session()
                .await
                .expect("incoming session");
            let (session, response) = tokio::join!(
                self.connection.accept_session(incoming),
                self.peer.control()
            );
            let session = session.expect("accepted session");
            let (channel, Performative::Begin(begin)) = response else {
                panic!("session Begin response");
            };
            assert_eq!(begin.remote_channel, Some(peer_channel));
            assert_eq!(channel, session.channel);
            assert_ne!(channel, peer_channel);
            self.peer.channels.insert(peer_channel, channel);
            session
        })
        .await
    }

    pub(super) async fn incoming(
        &mut self,
        session: &mut ServerSession,
        channel: u16,
        handle: u32,
        name: &str,
        mode: ReceiverSettleMode,
    ) -> IncomingAttach {
        self.incoming_with_mode(
            session,
            channel,
            handle,
            name,
            mode,
            SenderSettleMode::Unsettled,
        )
        .await
    }

    pub(super) async fn incoming_with_mode(
        &mut self,
        session: &mut ServerSession,
        channel: u16,
        handle: u32,
        name: &str,
        mode: ReceiverSettleMode,
        sender_mode: SenderSettleMode,
    ) -> IncomingAttach {
        bounded("bounded peer receiver Attach publication", async {
            self.peer
                .send(
                    channel,
                    Performative::Attach(Box::new(Attach {
                        name: name.to_owned(),
                        handle,
                        role: Role::Receiver,
                        snd_settle_mode: sender_mode,
                        rcv_settle_mode: mode,
                        source: Some(Source::new("same-queue")),
                        target: Some(Target::new("same-queue").into()),
                        unsettled: None,
                        incomplete_unsettled: false,
                        initial_delivery_count: None,
                        max_message_size: None,
                        offered_capabilities: None,
                        desired_capabilities: None,
                        properties: None,
                    })),
                )
                .await;
            session
                .next_incoming_attach()
                .await
                .expect("incoming peer receiver")
        })
        .await
    }

    pub(super) async fn accept(
        &mut self,
        session: &mut ServerSession,
        incoming: IncomingAttach,
        peer_channel: u16,
        peer_handle: u32,
    ) -> Sender {
        bounded("bounded native sender approval", async {
            let (endpoint, (channel, response)) =
                tokio::join!(session.accept_attach(incoming, 0), self.peer.control());
            let LinkEndpoint::Sender(sender) = endpoint.expect("accepted native sender") else {
                panic!("sender endpoint");
            };
            let Performative::Attach(response) = response else {
                panic!("sender Attach response");
            };
            assert_eq!(channel, session.channel);
            assert_eq!(response.role, Role::Sender);
            assert_eq!(response.handle, sender.handle);
            assert_eq!(response.name, sender.name());
            assert_ne!(sender.handle, peer_handle);
            self.peer
                .handles
                .entry(channel)
                .or_default()
                .insert(sender.handle);
            self.peer
                .send(
                    peer_channel,
                    Performative::Flow(Flow {
                        next_incoming_id: Some(
                            self.peer.received.get(&channel).copied().unwrap_or(0),
                        ),
                        incoming_window: 1_000,
                        next_outgoing_id: 0,
                        outgoing_window: 1_000,
                        handle: Some(peer_handle),
                        delivery_count: Some(0),
                        link_credit: Some(64),
                        available: None,
                        drain: false,
                        echo: false,
                        properties: None,
                    }),
                )
                .await;
            self.peer.barrier(peer_channel).await;
            sender
        })
        .await
    }

    pub(super) async fn sender(
        &mut self,
        session: &mut ServerSession,
        channel: u16,
        handle: u32,
        name: &str,
        mode: ReceiverSettleMode,
    ) -> Sender {
        let incoming = self.incoming(session, channel, handle, name, mode).await;
        self.accept(session, incoming, channel, handle).await
    }

    pub(super) async fn receipt(
        &mut self,
        sender: &mut Sender,
        channel: u16,
        settled: bool,
    ) -> (PendingSettlement, u32) {
        bounded("bounded outgoing delivery and receiver outcome", async {
            let handle = sender.handle;
            let responding = async {
                let transfer = self.peer.transfer(channel, handle).await;
                let id = transfer.delivery_id.expect("first transfer id");
                self.peer
                    .outcome(channel, id, settled, DeliveryState::Accepted(Accepted))
                    .await;
                id
            };
            let (receipt, id) = tokio::join!(
                sender.send_with_settlement(message(), TAG.to_vec().into()),
                responding
            );
            let receipt = receipt.expect("ordinary receiver outcome");
            assert_eq!(receipt.outcome(), &Outcome::Accepted(Accepted));
            (receipt, id)
        })
        .await
    }

    pub(super) async fn acknowledge(&mut self, receipt: &PendingSettlement, channel: u16, id: u32) {
        bounded("bounded second-mode sender acknowledgement", async {
            let (result, (actual, response)) = tokio::join!(receipt.accept(), self.peer.control());
            result.expect("sender acknowledgement flushed");
            assert_eq!(actual, self.peer.channels[&channel]);
            let Performative::Disposition(response) = response else {
                panic!("sender acknowledgement");
            };
            assert_eq!(response.role, Role::Sender);
            assert_eq!(response.first, id);
            assert!(response.last.is_none());
            assert!(response.settled);
            assert_eq!(response.state, Some(DeliveryState::Accepted(Accepted)));
        })
        .await;
    }

    pub(super) async fn close_link(&mut self, sender: &Sender, channel: u16, handle: u32) {
        bounded("bounded exact sender link close", async {
            let responding = async {
                let (actual, response) = self.peer.control().await;
                assert_eq!(actual, self.peer.channels[&channel]);
                let Performative::Detach(response) = response else {
                    panic!("link Detach");
                };
                assert_eq!(response.handle, sender.handle);
                assert!(response.closed);
                assert!(response.error.is_none());
                self.peer
                    .send(
                        channel,
                        Performative::Detach(Detach {
                            handle,
                            closed: true,
                            error: None,
                        }),
                    )
                    .await;
                self.peer.barrier(channel).await;
            };
            let (result, ()) = tokio::join!(sender.close(), responding);
            result.expect("sender link closed");
            self.peer
                .handles
                .get_mut(&sender.channel)
                .expect("session handles")
                .remove(&sender.handle);
        })
        .await;
    }

    pub(super) async fn shutdown(&self) {
        bounded("bounded native actor shutdown", self.connection.shutdown()).await;
    }

    pub(super) async fn close(&mut self) {
        bounded("bounded graceful native connection close", async {
            let responding = async {
                let (channel, response) = self.peer.control().await;
                assert_eq!(channel, 0);
                assert!(matches!(
                    response,
                    Performative::Close(Close { error: None })
                ));
                self.peer
                    .send(0, Performative::Close(Close { error: None }))
                    .await;
            };
            let (result, ()) = tokio::join!(self.connection.close(), responding);
            result.expect("native Close completed");
        })
        .await;
    }
}
