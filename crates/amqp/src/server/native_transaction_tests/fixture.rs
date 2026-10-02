use std::{
    future::Future,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use tokio::io::{DuplexStream, ReadBuf};

use super::*;

pub(super) const DEADLINE: Duration = Duration::from_secs(3);
pub(super) const CONTROL_CHANNEL: u16 = 19;
pub(super) const POST_CHANNEL: u16 = 23;
pub(super) const HEALTHY_CHANNEL: u16 = 29;
pub(super) const CONTROL_HANDLE: u32 = 17;
pub(super) const POST_HANDLE: u32 = 31;

pub(super) async fn bounded<T>(name: &'static str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, future).await.expect(name)
}

#[derive(Default)]
pub(super) struct FlushGate {
    capture: AtomicBool,
    target: Mutex<Option<FlushTarget>>,
    blocked: AtomicBool,
    failed: AtomicBool,
    entered: AtomicBool,
    waker: Mutex<Option<Waker>>,
    changed: Notify,
}

#[derive(Clone, Copy)]
enum FlushTarget {
    Attach(u16),
    Declared {
        channel: u16,
        id: u32,
    },
    Provisional {
        channel: u16,
        id: u32,
    },
    Refusal {
        channel: u16,
        id: u32,
        handle: u32,
        rejected: bool,
    },
    ErrorDetach {
        channel: u16,
        handle: u32,
        condition: &'static str,
    },
}

impl FlushTarget {
    fn matches(self, frame: &Frame) -> bool {
        match (self, frame) {
            (
                Self::Attach(expected),
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Attach(_)),
                    ..
                },
            ) => *channel == expected,
            (
                Self::Declared {
                    channel: expected,
                    id,
                },
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                },
            ) => {
                *channel == expected
                    && disposition.role == Role::Receiver
                    && disposition.first == id
                    && disposition.last.is_none()
                    && matches!(disposition.state, Some(DeliveryState::Declared(_)))
            }
            (
                Self::Provisional {
                    channel: expected,
                    id,
                },
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                },
            ) => {
                *channel == expected
                    && disposition.role == Role::Receiver
                    && disposition.first == id
                    && disposition.last.is_none()
                    && !disposition.settled
                    && matches!(
                        disposition.state,
                        Some(DeliveryState::Transactional(TransactionalState {
                            outcome: Some(Outcome::Accepted(_)),
                            ..
                        }))
                    )
            }
            (
                Self::Refusal {
                    channel: expected,
                    id,
                    rejected: true,
                    ..
                },
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                },
            ) => {
                *channel == expected
                    && disposition.role == Role::Receiver
                    && disposition.first == id
                    && disposition.last.is_none()
                    && matches!(&disposition.state, Some(DeliveryState::Rejected(crate::Rejected { error: Some(error) })) if error.condition.as_symbol().as_str() == "amqp:transaction:rollback")
            }
            (
                Self::Refusal {
                    channel: expected,
                    handle,
                    rejected: false,
                    ..
                },
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Detach(detach)),
                    ..
                },
            ) => {
                *channel == expected
                    && detach.handle == handle
                    && detach.closed
                    && detach.error.as_ref().is_some_and(|error| {
                        error.condition.as_symbol().as_str() == "amqp:transaction:rollback"
                    })
            }
            (
                Self::ErrorDetach {
                    channel: expected,
                    handle,
                    condition,
                },
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Detach(detach)),
                    payload,
                },
            ) => {
                *channel == expected
                    && detach.handle == handle
                    && detach.closed
                    && payload.is_empty()
                    && detach
                        .error
                        .as_ref()
                        .is_some_and(|error| error.condition.as_symbol().as_str() == condition)
            }
            _ => false,
        }
    }
}

impl FlushGate {
    fn arm(&self, target: FlushTarget, failed: bool) {
        assert!(self.capture.load(Ordering::Acquire), "arm after handshake");
        let mut slot = self.target.lock().expect("flush target");
        self.entered.store(false, Ordering::Release);
        self.failed.store(failed, Ordering::Release);
        self.blocked.store(!failed, Ordering::Release);
        *slot = Some(target);
    }

    pub(super) fn block_attach(&self, channel: u16) {
        self.arm(FlushTarget::Attach(channel), false);
    }

    pub(super) fn block_declared(&self, channel: u16, id: u32) {
        self.arm(FlushTarget::Declared { channel, id }, false);
    }

    pub(super) fn block_provisional(&self, channel: u16, id: u32) {
        self.arm(FlushTarget::Provisional { channel, id }, false);
    }

    pub(super) fn fail_provisional(&self, channel: u16, id: u32) {
        self.arm(FlushTarget::Provisional { channel, id }, true);
    }

    pub(super) fn block_refusal(&self, channel: u16, id: u32, handle: u32, rejected: bool) {
        self.arm(
            FlushTarget::Refusal {
                channel,
                id,
                handle,
                rejected,
            },
            false,
        );
    }

    pub(super) fn fail_refusal(&self, channel: u16, id: u32, handle: u32, rejected: bool) {
        self.arm(
            FlushTarget::Refusal {
                channel,
                id,
                handle,
                rejected,
            },
            true,
        );
    }

    pub(super) fn block_error_detach(&self, channel: u16, handle: u32, condition: &'static str) {
        self.arm(
            FlushTarget::ErrorDetach {
                channel,
                handle,
                condition,
            },
            false,
        );
    }

    pub(super) fn fail_error_detach(&self, channel: u16, handle: u32, condition: &'static str) {
        self.arm(
            FlushTarget::ErrorDetach {
                channel,
                handle,
                condition,
            },
            true,
        );
    }

    fn matches(&self, bytes: &[u8]) -> bool {
        let target = *self.target.lock().expect("flush target");
        target.is_some_and(|target| {
            target.matches(
                &crate::codec::decode_frame_for_test(bytes).expect("complete captured actor frame"),
            )
        })
    }

    pub(super) fn unblock(&self) {
        self.blocked.store(false, Ordering::Release);
        if let Some(waker) = self.waker.lock().expect("flush waiter").take() {
            waker.wake();
        }
    }

    pub(super) async fn wait(&self) {
        bounded("actor reaches gated flush", async {
            loop {
                let mut changed = Box::pin(self.changed.notified());
                changed.as_mut().enable();
                if self.entered.load(Ordering::Acquire) {
                    return;
                }
                changed.await;
            }
        })
        .await;
    }
}

struct GatedIo {
    inner: DuplexStream,
    gate: Arc<FlushGate>,
    written: Vec<u8>,
    matching_flush: Option<bool>,
}

impl AsyncRead for GatedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, bytes)
    }
}

impl AsyncWrite for GatedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, bytes);
        if let Poll::Ready(Ok(accepted)) = &result
            && self.gate.capture.load(Ordering::Acquire)
        {
            let total = self
                .written
                .len()
                .checked_add(*accepted)
                .expect("captured frame length");
            assert!(
                total <= DEFAULT_MAX_FRAME_SIZE as usize,
                "fixture captures at most its advertised frame cap"
            );
            self.written.extend_from_slice(&bytes[..*accepted]);
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let matches = if let Some(matches) = self.matching_flush {
            matches
        } else {
            let matches = !self.written.is_empty() && self.gate.matches(&self.written);
            self.matching_flush = Some(matches);
            matches
        };
        if matches && self.gate.failed.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "controlled target flush failure",
            )));
        }
        if matches && self.gate.blocked.load(Ordering::Acquire) {
            *self.gate.waker.lock().expect("flush waiter") = Some(cx.waker().clone());
            self.gate.entered.store(true, Ordering::Release);
            self.gate.changed.notify_waiters();
            if self.gate.blocked.load(Ordering::Acquire) {
                return Poll::Pending;
            }
        }
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            self.written.clear();
            self.matching_flush = None;
        }
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub(super) struct RawPeer {
    pub(super) io: DuplexStream,
    pub(super) transfers: HashMap<u16, u32>,
}

impl RawPeer {
    pub(super) async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) {
        bounded(
            "peer frame write",
            write_amqp(&mut self.io, channel, performative, payload),
        )
        .await
        .expect("peer writes valid AMQP");
    }

    pub(super) async fn frame(&mut self) -> Frame {
        bounded("actor frame response", read_frame(&mut self.io))
            .await
            .expect("valid native response")
    }

    pub(super) async fn attach(&mut self, channel: u16, attach: Attach) {
        self.send(channel, Performative::Attach(Box::new(attach)), Vec::new())
            .await;
    }

    pub(super) async fn transfer(&mut self, channel: u16, transfer: Transfer, payload: Vec<u8>) {
        self.send(channel, Performative::Transfer(transfer), payload)
            .await;
        *self.transfers.entry(channel).or_default() += 1;
    }

    pub(super) async fn command(
        &mut self,
        channel: u16,
        handle: u32,
        id: u32,
        command: TransactionCommand,
    ) {
        let payload = encode_message(&Message {
            body: crate::Body::Value(Value::from(command)),
            ..Message::default()
        })
        .expect("control command encoding");
        self.transfer(channel, first(handle, id, None, false), payload)
            .await;
    }

    pub(super) async fn end(&mut self, expected_channel: u16) -> End {
        for _ in 0..8 {
            match self.frame().await {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::End(end)),
                    ..
                } if channel == expected_channel => return end,
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("unexpected session refusal: {frame:?}"),
            }
        }
        panic!("no End within bounded response count");
    }

    pub(super) async fn attached(&mut self, channel: u16) -> Attach {
        for _ in 0..8 {
            match self.frame().await {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Attach(attach)),
                    payload,
                } => {
                    assert_eq!(actual, channel);
                    assert!(payload.is_empty());
                    return *attach;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    payload,
                    ..
                } if payload.is_empty() => {}
                frame => panic!("unexpected Attach response: {frame:?}"),
            }
        }
        panic!("no Attach within bounded response count");
    }

    async fn begin(&mut self, channel: u16) -> Begin {
        for _ in 0..8 {
            match self.frame().await {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Begin(begin)),
                    payload,
                } => {
                    assert_eq!(actual, channel);
                    assert_eq!(begin.remote_channel, Some(channel));
                    assert!(payload.is_empty());
                    return begin;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    payload,
                    ..
                } if payload.is_empty() => {}
                frame => panic!("unexpected Begin response: {frame:?}"),
            }
        }
        panic!("no Begin within bounded response count");
    }

    pub(super) async fn credit(&mut self, channel: u16, handle: u32) {
        let Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Flow(flow)),
            payload,
        } = self.frame().await
        else {
            panic!("link credit response")
        };
        assert_eq!(actual, channel);
        assert_eq!(flow.handle, Some(handle));
        assert!(flow.link_credit.is_some_and(|credit| credit > 0));
        assert!(payload.is_empty());
    }

    pub(super) async fn disposition(&mut self, channel: u16, id: u32) -> Disposition {
        for _ in 0..8 {
            match self.frame().await {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Disposition(disposition)),
                    payload,
                } => {
                    assert_eq!(actual, channel);
                    assert_eq!(disposition.role, Role::Receiver);
                    assert_eq!(disposition.first, id);
                    assert!(disposition.last.is_none());
                    assert!(payload.is_empty());
                    return disposition;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("unexpected disposition response: {frame:?}"),
            }
        }
        panic!("no Disposition within bounded response count");
    }

    pub(super) async fn detach(&mut self, channel: u16, handle: u32) {
        self.send(
            channel,
            Performative::Detach(Detach {
                handle,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await;
        for _ in 0..8 {
            match self.frame().await {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Detach(detach)),
                    payload,
                } if actual == channel => {
                    assert_eq!(detach.handle, handle);
                    assert!(detach.closed);
                    assert!(detach.error.is_none());
                    assert!(payload.is_empty());
                    return;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("unexpected Detach acknowledgement: {frame:?}"),
            }
        }
        panic!("no Detach within bounded response count");
    }

    pub(super) async fn refusal(&mut self, channel: u16, handle: u32, condition: &str) {
        for _ in 0..8 {
            match self.frame().await {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Detach(detach)),
                    payload,
                } if actual == channel => {
                    assert_eq!(detach.handle, handle);
                    assert!(detach.closed);
                    assert_eq!(
                        detach
                            .error
                            .expect("explicit link error")
                            .condition
                            .as_symbol()
                            .as_str(),
                        condition
                    );
                    assert!(payload.is_empty());
                    return;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("unexpected scoped native refusal: {frame:?}"),
            }
        }
        panic!("no scoped refusal within bounded response count");
    }

    pub(super) async fn barrier(&mut self, channel: u16) {
        let next = self.transfers.get(&channel).copied().unwrap_or(0);
        self.send(
            channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 1_000,
                next_outgoing_id: next,
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
        .await;
        for _ in 0..8 {
            match self.frame().await {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if actual == channel && flow.handle.is_none() => return,
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("unexpected barrier response: {frame:?}"),
            }
        }
        panic!("no echo barrier within bounded response count");
    }
}

pub(super) struct Fixture {
    pub(super) connection: ServerConnection,
    pub(super) peer: RawPeer,
    pub(super) gate: Arc<FlushGate>,
}

impl Fixture {
    pub(super) async fn new(enabled: bool) -> Self {
        bounded("native fixture negotiation", async {
            let (stream, peer) = tokio::io::duplex(16_384);
            let gate = Arc::new(FlushGate::default());
            let mut peer = RawPeer {
                io: peer,
                transfers: HashMap::new(),
            };
            let opening = async {
                write_protocol_header(&mut peer.io, ProtocolHeader::AMQP)
                    .await
                    .expect("peer protocol header");
                expect_header(&mut peer.io, ProtocolHeader::AMQP)
                    .await
                    .expect("server protocol header");
                peer.send(
                    0,
                    Performative::Open(Open::new("raw-native-transaction-peer")),
                    Vec::new(),
                )
                .await;
                assert!(matches!(
                    peer.frame().await,
                    Frame::Amqp {
                        performative: Some(Performative::Open(_)),
                        ..
                    }
                ));
                peer
            };
            let stream = GatedIo {
                inner: stream,
                gate: Arc::clone(&gate),
                written: Vec::new(),
                matching_flush: None,
            };
            let accepting = async {
                if enabled {
                    ServerConnection::accept_with_transactional_ingress(
                        stream,
                        "native-transaction-server",
                        None,
                        ConnectionOptions::default(),
                    )
                    .await
                } else {
                    ServerConnection::accept_with_options(
                        stream,
                        "native-transaction-server",
                        None,
                        ConnectionOptions::default(),
                    )
                    .await
                }
            };
            let (connection, peer) = tokio::join!(accepting, opening);
            gate.capture.store(true, Ordering::Release);
            Self {
                connection: connection.expect("server connection"),
                peer,
                gate,
            }
        })
        .await
    }

    pub(super) async fn session(&mut self, channel: u16) -> ServerSession {
        bounded("native session approval", async {
            self.peer
                .send(channel, Performative::Begin(Begin::default()), Vec::new())
                .await;
            let incoming = self
                .connection
                .next_incoming_session()
                .await
                .expect("incoming session");
            let (session, _) = tokio::join!(
                self.connection.accept_session(incoming),
                self.peer.begin(channel)
            );
            session.expect("server session")
        })
        .await
    }

    pub(super) async fn coordinator(&mut self) -> (ServerSession, CoordinatorEndpoint) {
        self.coordinator_with(coordinator_attach(CONTROL_HANDLE))
            .await
    }

    pub(super) async fn coordinator_with(
        &mut self,
        attach: Attach,
    ) -> (ServerSession, CoordinatorEndpoint) {
        let mut session = self.session(CONTROL_CHANNEL).await;
        self.peer.attach(CONTROL_CHANNEL, attach).await;
        let incoming = bounded("coordinator approval", session.next_incoming_attach())
            .await
            .expect("actor-approved coordinator");
        let responses = async {
            let attach = self.peer.attached(CONTROL_CHANNEL).await;
            assert_eq!(attach.handle, CONTROL_HANDLE);
            let coordinator = attach
                .target
                .as_ref()
                .and_then(|target| target.as_coordinator())
                .expect("coordinator target");
            let capabilities = coordinator
                .capabilities
                .as_ref()
                .expect("supported coordinator capabilities");
            assert_eq!(
                capabilities
                    .iter()
                    .map(|symbol| symbol.as_str())
                    .collect::<Vec<_>>(),
                vec![
                    "amqp:local-transactions",
                    "amqp:multi-txns-per-ssn",
                    "amqp:multi-ssns-per-txn",
                ]
            );
            assert_eq!(
                attach.max_message_size,
                Some(MAX_NATIVE_TRANSACTION_CONTROL_BYTES)
            );
            self.peer.credit(CONTROL_CHANNEL, CONTROL_HANDLE).await;
        };
        let (endpoint, ()) = tokio::join!(session.accept_coordinator(incoming, 0), responses);
        (session, endpoint.expect("accepted coordinator"))
    }

    pub(super) async fn receiver(&mut self) -> (ServerSession, TransactionalReceiver) {
        self.receiver_with_mode(ReceiverSettleMode::First).await
    }

    pub(super) async fn receiver_with_mode(
        &mut self,
        mode: ReceiverSettleMode,
    ) -> (ServerSession, TransactionalReceiver) {
        self.receiver_approved(mode, None).await
    }

    pub(super) async fn receiver_with_decoders(
        &mut self,
        mode: ReceiverSettleMode,
        decoders: MessageFormatDecoders,
    ) -> (ServerSession, TransactionalReceiver) {
        self.receiver_approved(mode, Some(decoders)).await
    }

    async fn receiver_approved(
        &mut self,
        mode: ReceiverSettleMode,
        decoders: Option<MessageFormatDecoders>,
    ) -> (ServerSession, TransactionalReceiver) {
        let mut session = self.session(POST_CHANNEL).await;
        let mut attach = ordinary_attach(POST_HANDLE);
        attach.rcv_settle_mode = mode;
        self.peer.attach(POST_CHANNEL, attach).await;
        let incoming = bounded(
            "transactional receiver approval",
            session.next_incoming_attach(),
        )
        .await
        .expect("actor-approved data receiver");
        let responses = async {
            let attach = self.peer.attached(POST_CHANNEL).await;
            assert_eq!(attach.handle, POST_HANDLE);
            assert_eq!(
                attach
                    .target
                    .as_ref()
                    .and_then(|target| target.as_target())
                    .and_then(|target| target.address.as_deref()),
                Some("queue")
            );
            self.peer.credit(POST_CHANNEL, POST_HANDLE).await;
        };
        let accepting = async {
            match decoders {
                Some(decoders) => {
                    session
                        .accept_transactional_receiver_with_decoders(incoming, 0, decoders)
                        .await
                }
                None => session.accept_transactional_receiver(incoming, 0).await,
            }
        };
        let (endpoint, ()) = tokio::join!(accepting, responses);
        (session, endpoint.expect("accepted transactional receiver"))
    }

    pub(super) async fn declare(
        &mut self,
        coordinator: &mut CoordinatorEndpoint,
        id: &TransactionId,
    ) -> NativeTransactionIdentity {
        self.peer
            .command(
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                0,
                TransactionCommand::Declare(Declare::default()),
            )
            .await;
        let CoordinatorRequest::Declare(declare) = bounded("Declare receipt", coordinator.recv())
            .await
            .expect("valid control request")
        else {
            panic!("Declare request")
        };
        let (result, disposition) = tokio::join!(
            declare.declared(id.clone()),
            self.peer.disposition(CONTROL_CHANNEL, 0)
        );
        let identity = result.expect("registered ID after Declared flush");
        assert!(disposition.settled);
        assert!(
            matches!(disposition.state, Some(DeliveryState::Declared(crate::Declared { txn_id })) if txn_id == *id)
        );
        assert_eq!(identity.transaction_id(), id);
        assert!(
            identity
                .controller_identity()
                .same_controller(coordinator.controller_identity())
        );
        identity
    }

    pub(super) async fn discharge(
        &mut self,
        coordinator: &mut CoordinatorEndpoint,
        id: &TransactionId,
        fail: bool,
    ) -> SealedDischargeReceipt {
        self.peer
            .command(
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                1,
                TransactionCommand::Discharge(Discharge {
                    txn_id: id.clone(),
                    fail: Some(fail),
                }),
            )
            .await;
        let CoordinatorRequest::Discharge(sealed) =
            bounded("Discharge receipt", coordinator.recv())
                .await
                .expect("sealed control request")
        else {
            panic!("Discharge request")
        };
        assert_eq!(sealed.transaction_id(), id);
        assert_eq!(sealed.fail(), fail);
        sealed
    }

    pub(super) async fn post(&mut self, id: &TransactionId, message: &Message) {
        self.peer
            .transfer(
                POST_CHANNEL,
                first(POST_HANDLE, 0, Some(id.clone()), false),
                encode_message(message).expect("post encoding"),
            )
            .await;
    }

    pub(super) async fn provisional(
        &mut self,
        posting: TransactionPostingReceipt,
        id: &TransactionId,
    ) -> PreparedPosting {
        assert_eq!(posting.transaction_id(), id);
        let (prepared, disposition) = tokio::join!(
            posting.provisional_accept(),
            self.peer.disposition(POST_CHANNEL, 0)
        );
        assert!(
            !disposition.settled,
            "provisional acceptance is not terminal settlement"
        );
        assert!(
            matches!(disposition.state, Some(DeliveryState::Transactional(TransactionalState { txn_id, outcome: Some(Outcome::Accepted(_)) })) if txn_id == *id)
        );
        prepared.expect("prepared posting after provisional ACK flush")
    }

    pub(super) async fn committed(&mut self, ready: NativeReadySubmission) -> usize {
        let (ticket, resources) = ready.into_owner_parts();
        let claim = ticket
            .try_claim()
            .expect("one native owner claims exact prepared bundle");
        claim.finish(NativeTransactionDecision::Committed);
        self.finish_committed(resources).await
    }

    pub(super) async fn finish_committed(
        &mut self,
        resources: NativeTransactionResources,
    ) -> usize {
        let responses = async {
            let mut posting_acknowledgements = 0;
            for _ in 0..8 {
                match self.peer.frame().await {
                    Frame::Amqp {
                        channel,
                        performative: Some(Performative::Disposition(disposition)),
                        payload,
                    } => {
                        assert_eq!(disposition.role, Role::Receiver);
                        assert!(disposition.last.is_none());
                        assert!(disposition.settled);
                        assert!(payload.is_empty());
                        if channel == CONTROL_CHANNEL {
                            assert_eq!(disposition.first, 1);
                            assert!(matches!(
                                disposition.state,
                                Some(DeliveryState::Accepted(_))
                            ));
                            return posting_acknowledgements;
                        }
                        assert_eq!(channel, POST_CHANNEL);
                        assert_eq!(disposition.first, 0);
                        assert!(
                            matches!(disposition.state, Some(DeliveryState::Accepted(_))),
                            "final applied posting outcome is not provisional TxState"
                        );
                        posting_acknowledgements += 1;
                    }
                    Frame::Amqp {
                        performative: Some(Performative::Flow(_)),
                        ..
                    } => {}
                    frame => panic!("unexpected final native disposition: {frame:?}"),
                }
            }
            panic!("no discharge decision within bounded response count");
        };
        let (result, posting_acknowledgements) = tokio::join!(resources.finish(), responses);
        result.expect("committed native discharge flush");
        posting_acknowledgements
    }
}

pub(super) fn ordinary_attach(handle: u32) -> Attach {
    Attach {
        name: format!("ordinary-{handle}"),
        handle,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue").into()),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

pub(super) fn coordinator_attach(handle: u32) -> Attach {
    Attach {
        name: format!("controller-{handle}"),
        source: None,
        target: Some(Coordinator::default().into()),
        ..ordinary_attach(handle)
    }
}

pub(super) fn first(handle: u32, id: u32, txn: Option<TransactionId>, more: bool) -> Transfer {
    Transfer {
        handle,
        delivery_id: Some(id),
        delivery_tag: Some(vec![id as u8].into()),
        message_format: Some(0),
        settled: Some(false),
        more,
        rcv_settle_mode: None,
        state: txn.map(|txn_id| {
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

pub(super) fn continuation(handle: u32, state: Option<DeliveryState>, more: bool) -> Transfer {
    Transfer {
        handle,
        delivery_id: None,
        delivery_tag: None,
        message_format: None,
        settled: None,
        more,
        rcv_settle_mode: None,
        state,
        resume: false,
        aborted: false,
        batchable: false,
    }
}
