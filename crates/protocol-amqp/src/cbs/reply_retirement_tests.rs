//! Actual native CBS reply lifetimes and identity-checked channel cleanup.

#[path = "leaf_fault_tests.rs"]
mod leaf_fault_tests;

use std::{
    future::{Future, poll_fn},
    io,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use amqp::{
    Accepted, Attach, Begin, Disposition, Flow, Frame, LinkEndpoint, Open, Performative,
    ProtocolHeader, ReceiverSettleMode, Role, SenderSettleMode, ServerConnection, ServerSession,
    Source, Target, Transfer, decode_message, encode_message, read_frame, read_protocol_header,
    write_frame, write_protocol_header,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, duplex},
    sync::Notify,
    time::timeout,
};

use super::custody::{PUMP_FAULT, PumpFault};
use super::request_retirement_tests::{assert_outer_panic, authorization};
use super::*;

pub(super) const WAIT: Duration = Duration::from_secs(8);
pub(super) const ADDRESS: &str = "cbs-replies";
const CHANNEL: u16 = 1;
const HANDLE: u32 = 1;

#[derive(Default)]
struct WriteState {
    held: bool,
    reached: bool,
    fail: bool,
    waker: Option<Waker>,
}

#[derive(Default)]
struct WriteGate {
    state: StdMutex<WriteState>,
    changed: Notify,
}

impl WriteGate {
    fn arm(&self, fail: bool) {
        let mut state = self.state.lock().unwrap();
        state.held = true;
        state.reached = false;
        state.fail = fail;
    }

    async fn reached(&self) {
        loop {
            let changed = self.changed.notified();
            if self.state.lock().unwrap().reached {
                return;
            }
            changed.await;
        }
    }

    fn fail_held_write(&self) {
        let mut state = self.state.lock().unwrap();
        assert!(state.held && state.reached);
        state.fail = true;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.held = false;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
}

struct GateIo {
    inner: DuplexStream,
    gate: Arc<WriteGate>,
}

impl AsyncRead for GateIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, bytes)
    }
}

impl AsyncWrite for GateIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        {
            let mut state = self.gate.state.lock().unwrap();
            if state.held {
                state.reached = true;
                self.gate.changed.notify_one();
                if state.fail {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "controlled CBS native write failure",
                    )));
                }
                state.waker = Some(context.waker().clone());
                return Poll::Pending;
            }
        }
        Pin::new(&mut self.inner).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

fn frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

pub(super) struct Wire {
    connection: ServerConnection,
    peer: DuplexStream,
    writes: Arc<WriteGate>,
    sessions: Vec<ServerSession>,
    next_channel: u16,
}

impl Wire {
    pub(super) async fn new(
        role: Role,
        credit: u32,
        settle: ReceiverSettleMode,
    ) -> (Self, LinkEndpoint) {
        let (stream, mut peer) = duplex(128 * 1024);
        let writes = Arc::new(WriteGate::default());
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept(
                    GateIo {
                        inner: stream,
                        gate: Arc::clone(&writes)
                    },
                    "cbs-server",
                    None,
                ),
                async {
                    write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                        .await
                        .unwrap();
                    assert_eq!(
                        read_protocol_header(&mut peer).await.unwrap(),
                        ProtocolHeader::AMQP
                    );
                    write_frame(
                        &mut peer,
                        &frame(0, Performative::Open(Open::new("cbs-peer"))),
                    )
                    .await
                    .unwrap();
                    assert!(matches!(
                        read_frame(&mut peer).await.unwrap(),
                        Frame::Amqp {
                            performative: Some(Performative::Open(_)),
                            ..
                        }
                    ));
                }
            )
        })
        .await
        .unwrap();
        let mut wire = Self {
            connection: connection.unwrap(),
            peer,
            writes,
            sessions: Vec::new(),
            next_channel: 2,
        };
        wire.begin(CHANNEL).await;
        write_frame(
            &mut wire.peer,
            &frame(
                CHANNEL,
                Performative::Attach(Box::new(Attach {
                    name: "cbs-transport".to_owned(),
                    handle: HANDLE,
                    role: role.clone(),
                    snd_settle_mode: SenderSettleMode::Unsettled,
                    rcv_settle_mode: settle,
                    source: (role == Role::Receiver).then(|| Source::new(crate::CBS_NODE)),
                    target: Some(Target::new(if role == Role::Receiver {
                        ADDRESS
                    } else {
                        crate::CBS_NODE
                    })),
                    unsettled: None,
                    incomplete_unsettled: false,
                    initial_delivery_count: (role == Role::Sender).then_some(0),
                    max_message_size: None,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                })),
            ),
        )
        .await
        .unwrap();
        let session = wire.sessions.last_mut().unwrap();
        let attach = timeout(WAIT, session.next_incoming_attach())
            .await
            .unwrap()
            .unwrap();
        let endpoint = timeout(WAIT, session.accept_attach(attach, 128 * 1024))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            wire.control(CHANNEL).await,
            Performative::Attach(_)
        ));
        if role == Role::Sender {
            assert!(matches!(wire.control(CHANNEL).await, Performative::Flow(_)));
        } else if credit > 0 {
            write_frame(
                &mut wire.peer,
                &frame(
                    CHANNEL,
                    Performative::Flow(Flow {
                        handle: Some(HANDLE),
                        delivery_count: Some(0),
                        link_credit: Some(credit),
                        incoming_window: u32::MAX,
                        outgoing_window: u32::MAX,
                        ..Flow::default()
                    }),
                ),
            )
            .await
            .unwrap();
        }
        (wire, endpoint)
    }

    pub(super) async fn control(&mut self, channel: u16) -> Performative {
        let Frame::Amqp {
            channel: actual,
            performative: Some(performative),
            payload,
        } = timeout(WAIT, read_frame(&mut self.peer))
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("actual native control frame")
        };
        assert_eq!(actual, channel);
        assert!(payload.is_empty());
        performative
    }

    async fn begin(&mut self, channel: u16) {
        write_frame(
            &mut self.peer,
            &frame(channel, Performative::Begin(Begin::default())),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, self.connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        self.sessions.push(
            timeout(WAIT, self.connection.accept_session(incoming))
                .await
                .unwrap()
                .unwrap(),
        );
        assert!(matches!(
            self.control(channel).await,
            Performative::Begin(_)
        ));
    }

    pub(super) async fn barrier(&mut self) {
        let channel = self.next_channel;
        self.next_channel += 1;
        self.begin(channel).await;
    }

    pub(super) async fn detach(&mut self) {
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Detach(amqp::Detach {
                    handle: HANDLE,
                    closed: true,
                    error: None,
                }),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            self.control(CHANNEL).await,
            Performative::Detach(_)
        ));
    }

    pub(super) async fn request_message(&mut self, message: &Message, settled: bool) {
        write_frame(
            &mut self.peer,
            &Frame::Amqp {
                channel: CHANNEL,
                performative: Some(Performative::Transfer(Transfer {
                    handle: HANDLE,
                    delivery_id: Some(1),
                    delivery_tag: Some(Binary::from(vec![1])),
                    message_format: Some(0),
                    settled: Some(settled),
                    more: false,
                    rcv_settle_mode: None,
                    state: None,
                    resume: false,
                    aborted: false,
                    batchable: false,
                })),
                payload: encode_message(message).unwrap(),
            },
        )
        .await
        .unwrap();
    }

    async fn response_transfer(&mut self) -> (Transfer, Message) {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Transfer(transfer)),
            payload,
        } = timeout(WAIT, read_frame(&mut self.peer))
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("actual CBS response Transfer")
        };
        assert_eq!(channel, CHANNEL);
        (transfer, decode_message(&payload).unwrap())
    }

    async fn accept_response(&mut self, id: u32) {
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: id,
                    last: None,
                    settled: false,
                    state: Some(DeliveryState::Accepted(Accepted)),
                    batchable: false,
                }),
            ),
        )
        .await
        .unwrap();
    }

    pub(super) async fn no_frame_yet(&mut self) {
        pending_once(Box::pin(read_frame(&mut self.peer)).as_mut()).await;
    }

    pub(super) async fn stop(&mut self) {
        self.connection.stop();
        let first = timeout(WAIT, self.connection.shutdown())
            .await
            .expect("original native tasks joined");
        assert_eq!(first, self.connection.shutdown().await);
    }
}

impl Drop for Wire {
    fn drop(&mut self) {
        self.writes.release();
        self.connection.stop();
    }
}

pub(super) async fn pending_once<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    poll_fn(|context| {
        assert!(future.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
}

async fn assert_unregistered(authorization: &ConnectionAuthorization) {
    let mut probe = Box::pin(
        authorization.route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999))),
    );
    pending_once(probe.as_mut()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn outer_reply_panic_before_native_start_closes_only_its_captured_route() {
    for settle in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        for replace in [false, true] {
            for point in [PumpPoint::ReplyPrepared, PumpPoint::ReplyIdle] {
                let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, settle.clone()).await;
                let LinkEndpoint::Sender(sender) = endpoint else {
                    panic!("CBS reply sender")
                };
                let authorization = authorization();
                let (route, responses) =
                    authorization.register_reply_route(ADDRESS.to_owned()).await;
                if point == PumpPoint::ReplyPrepared {
                    route
                        .send(CbsResponse::accepted(MessageId::Ulong(42)))
                        .await
                        .unwrap();
                }
                let fault = PumpFault::new(point);
                let mut serving = Box::pin(
                    PUMP_FAULT.scope(
                        Arc::clone(&fault),
                        AssertUnwindSafe(serve_cbs_replies(
                            sender,
                            ADDRESS.to_owned(),
                            route.clone(),
                            responses,
                            Arc::clone(&authorization),
                        ))
                        .catch_unwind(),
                    ),
                );
                pending_once(serving.as_mut()).await;
                timeout(WAIT, fault.reached.notified()).await.unwrap();
                let replacement = if replace {
                    Some(authorization.register_reply_route(ADDRESS.to_owned()).await)
                } else {
                    None
                };
                fault.trigger.notify_one();
                assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
                assert!(route.is_closed());
                if let Some((_new_route, mut responses)) = replacement {
                    assert!(matches!(
                        responses.try_recv(),
                        Err(mpsc::error::TryRecvError::Empty)
                    ));
                    authorization
                        .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
                        .await
                        .unwrap();
                    assert_eq!(
                        responses.recv().await.unwrap().correlation_id,
                        MessageId::Ulong(999)
                    );
                } else {
                    assert_unregistered(&authorization).await;
                }
                wire.barrier().await;
                wire.no_frame_yet().await;
                wire.stop().await;
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outer_reply_panic_retains_begun_write_or_confirmation_and_preserves_primary_panic() {
    for (settle, confirmation) in [
        (ReceiverSettleMode::First, false),
        (ReceiverSettleMode::Second, false),
        (ReceiverSettleMode::Second, true),
    ] {
        for fail in [false, true] {
            let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, settle.clone()).await;
            let LinkEndpoint::Sender(sender) = endpoint else {
                panic!("CBS reply sender")
            };
            let authorization = authorization();
            let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
            route
                .send(CbsResponse::accepted(MessageId::Ulong(42)))
                .await
                .unwrap();
            if !confirmation {
                wire.writes.arm(false);
            }
            let fault = PumpFault::new(PumpPoint::ReplyNative);
            let mut serving = Box::pin(
                PUMP_FAULT.scope(
                    Arc::clone(&fault),
                    AssertUnwindSafe(serve_cbs_replies(
                        sender,
                        ADDRESS.to_owned(),
                        route.clone(),
                        responses,
                        Arc::clone(&authorization),
                    ))
                    .catch_unwind(),
                ),
            );
            pending_once(serving.as_mut()).await;
            timeout(WAIT, fault.reached.notified()).await.unwrap();
            if confirmation {
                let (transfer, _) = wire.response_transfer().await;
                wire.accept_response(transfer.delivery_id.unwrap()).await;
                wire.barrier().await;
                wire.writes.arm(false);
                pending_once(serving.as_mut()).await;
            }
            timeout(WAIT, wire.writes.reached()).await.unwrap();
            let (_replacement, mut replacement_responses) =
                authorization.register_reply_route(ADDRESS.to_owned()).await;
            fault.trigger.notify_one();
            for _ in 0..2 {
                pending_once(serving.as_mut()).await;
                assert!(
                    route.is_closed(),
                    "the original channel closes before the held native operation completes"
                );
                assert!(wire.writes.state.lock().unwrap().held);
            }
            wire.no_frame_yet().await;
            if fail {
                wire.writes.fail_held_write();
            } else {
                wire.stop().await;
                assert!(
                    wire.writes.state.lock().unwrap().held,
                    "native Stop, not dropping an observer, interrupted the write"
                );
            }
            assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
            assert!(matches!(
                replacement_responses.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            authorization
                .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
                .await
                .unwrap();
            assert_eq!(
                replacement_responses.recv().await.unwrap().correlation_id,
                MessageId::Ulong(999)
            );
            wire.stop().await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outer_reply_panic_keeps_ready_or_cached_outcome_and_starts_no_new_confirmation() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::Second).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("CBS reply sender")
    };
    let authorization = authorization();
    let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    route
        .send(CbsResponse::accepted(MessageId::Ulong(42)))
        .await
        .unwrap();
    let fault = PumpFault::new(PumpPoint::ReplyNative);
    let mut serving = Box::pin(
        PUMP_FAULT.scope(
            Arc::clone(&fault),
            AssertUnwindSafe(serve_cbs_replies(
                sender,
                ADDRESS.to_owned(),
                route.clone(),
                responses,
                Arc::clone(&authorization),
            ))
            .catch_unwind(),
        ),
    );
    pending_once(serving.as_mut()).await;
    timeout(WAIT, fault.reached.notified()).await.unwrap();
    let (transfer, _) = wire.response_transfer().await;
    wire.accept_response(transfer.delivery_id.unwrap()).await;
    wire.barrier().await;
    // The outer fault wins before the same retained future observes its ready outcome.
    fault.trigger.notify_one();
    assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
    assert!(route.is_closed());
    assert_unregistered(&authorization).await;
    wire.barrier().await;
    wire.no_frame_yet().await;
    wire.stop().await;
    for (settle, second) in [
        (ReceiverSettleMode::First, false),
        (ReceiverSettleMode::Second, true),
    ] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, settle).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("CBS reply sender")
        };
        let authorization = super::request_retirement_tests::authorization();
        let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        route
            .send(CbsResponse::accepted(MessageId::Ulong(42)))
            .await
            .unwrap();
        let fault = PumpFault::new(PumpPoint::ReplyResult);
        let mut serving = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(serve_cbs_replies(
                    sender,
                    ADDRESS.to_owned(),
                    route.clone(),
                    responses,
                    Arc::clone(&authorization),
                ))
                .catch_unwind(),
            ),
        );
        pending_once(serving.as_mut()).await;
        let (transfer, _) = wire.response_transfer().await;
        wire.accept_response(transfer.delivery_id.unwrap()).await;
        wire.barrier().await;
        pending_once(serving.as_mut()).await;
        if second {
            let Performative::Disposition(confirmation) = wire.control(CHANNEL).await else {
                panic!("original second confirmation")
            };
            assert_eq!(confirmation.first, transfer.delivery_id.unwrap());
            assert_eq!(confirmation.state, Some(DeliveryState::Accepted(Accepted)));
            assert!(confirmation.settled);
            pending_once(serving.as_mut()).await;
        }
        timeout(WAIT, fault.reached.notified()).await.unwrap();
        let (_replacement, mut replacement_responses) =
            authorization.register_reply_route(ADDRESS.to_owned()).await;
        let held = authorization.reply_route_lock().await;
        fault.trigger.notify_one();
        for _ in 0..2 {
            pending_once(serving.as_mut()).await;
            assert!(
                route.is_closed(),
                "cached native completion precedes the held conditional cleanup"
            );
        }
        drop(held);
        assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
        assert!(matches!(
            replacement_responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        authorization
            .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
            .await
            .unwrap();
        assert_eq!(
            replacement_responses.recv().await.unwrap().correlation_id,
            MessageId::Ulong(999)
        );
        wire.barrier().await;
        wire.no_frame_yet().await;
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reply_owner_retains_cached_native_packet_and_route_across_cancelled_unregister() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::First).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("CBS reply sender")
    };
    let authorization = authorization();
    let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    let mut custody =
        ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &authorization);
    let control = OperationControl::new();
    custody.original = Some(PendingOperation::new(
        send_cbs_response(
            &sender,
            CbsResponse::accepted(MessageId::Ulong(42)),
            control.clone(),
        ),
        control.clone(),
    ));
    pending_once(Box::pin(custody.original.as_mut().unwrap().observe()).as_mut()).await;
    let (transfer, _) = wire.response_transfer().await;
    wire.accept_response(transfer.delivery_id.unwrap()).await;
    wire.barrier().await;
    assert!(matches!(
        timeout(WAIT, custody.original.as_mut().unwrap().observe())
            .await
            .unwrap(),
        Some(Ok(Outcome::Accepted(_)))
    ));
    custody.capture_packet();
    let (_replacement, mut replacement_responses) =
        authorization.register_reply_route(ADDRESS.to_owned()).await;
    let held = authorization.reply_route_lock().await;
    for _ in 0..2 {
        pending_once(Box::pin(custody.finish()).as_mut()).await;
        assert!(custody.original.is_none() && route.is_closed());
        let packet = custody.packet.as_ref().unwrap();
        assert!(packet.started && !packet.panicked);
        assert!(matches!(
            packet.result.as_ref(),
            Some(Ok(Outcome::Accepted(_)))
        ));
    }
    drop(held);
    timeout(WAIT, custody.finish()).await.unwrap();
    timeout(WAIT, custody.finish()).await.unwrap();
    assert!(matches!(
        custody.packet.as_ref().unwrap().result.as_ref(),
        Some(Ok(Outcome::Accepted(_)))
    ));
    assert!(matches!(
        replacement_responses.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    authorization
        .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
        .await
        .unwrap();
    assert_eq!(
        replacement_responses.recv().await.unwrap().correlation_id,
        MessageId::Ulong(999)
    );
    wire.barrier().await;
    wire.no_frame_yet().await;
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn outer_request_panic_retains_begun_accept_or_reject_until_native_stop_or_error() {
    for reject in [false, true] {
        for fail in [false, true] {
            let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
            let LinkEndpoint::Receiver(receiver) = endpoint else {
                panic!("CBS request receiver")
            };
            let authorization = authorization();
            let (_route, mut responses) =
                authorization.register_reply_route(ADDRESS.to_owned()).await;
            let message = if reject {
                Message::default()
            } else {
                super::request_retirement_tests::request("invalid-token", 42)
            };
            wire.request_message(&message, false).await;
            wire.barrier().await;
            wire.writes.arm(false);
            let fault = PumpFault::new(PumpPoint::RequestNative);
            let mut serving = Box::pin(
                PUMP_FAULT.scope(
                    Arc::clone(&fault),
                    AssertUnwindSafe(serve_cbs_requests(receiver, Arc::clone(&authorization)))
                        .catch_unwind(),
                ),
            );
            pending_once(serving.as_mut()).await;
            timeout(WAIT, fault.reached.notified()).await.unwrap();
            timeout(WAIT, wire.writes.reached()).await.unwrap();
            fault.trigger.notify_one();
            for _ in 0..2 {
                pending_once(serving.as_mut()).await;
                assert!(wire.writes.state.lock().unwrap().held);
                assert!(matches!(
                    responses.try_recv(),
                    Err(mpsc::error::TryRecvError::Empty)
                ));
            }
            wire.no_frame_yet().await;
            if fail {
                wire.writes.fail_held_write();
            } else {
                wire.stop().await;
            }
            assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
            assert!(matches!(
                responses.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert!(authorization.grant_snapshot().await.is_empty());
            wire.stop().await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_no_credit_reply_retires_queued_original_without_replacement_retry() {
    for stop in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual CBS reply sender")
        };
        let authorization = authorization();
        let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        route
            .send(CbsResponse::accepted(MessageId::Ulong(42)))
            .await
            .unwrap();
        let mut serving = Box::pin(serve_cbs_replies(
            sender,
            ADDRESS.to_owned(),
            route.clone(),
            responses,
            Arc::clone(&authorization),
        ));
        pending_once(serving.as_mut()).await;
        wire.barrier().await;
        let (_replacement, mut replacement_responses) =
            authorization.register_reply_route(ADDRESS.to_owned()).await;
        if stop {
            wire.stop().await;
        } else {
            wire.detach().await;
        }
        let error = timeout(WAIT, serving.as_mut()).await.unwrap().unwrap_err();
        assert!(matches!(
            error.downcast_ref::<EngineError>(),
            Some(EngineError::RemoteDetached)
        ));
        assert!(route.is_closed());
        assert!(matches!(
            replacement_responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        authorization
            .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
            .await
            .unwrap();
        assert_eq!(
            replacement_responses.recv().await.unwrap().correlation_id,
            MessageId::Ulong(999)
        );
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_no_credit_native_error_precedes_secondary_diagnostic_panic_after_retirement() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct PanicDiagnostics(Arc<AtomicUsize>);

    impl tracing::Subscriber for PanicDiagnostics {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target().ends_with("::cbs::custody")
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, _: &tracing::Event<'_>) {
            self.0.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any("controlled secondary CBS diagnostic panic");
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    // Keep tracing's single-dispatcher fast path from using another test
    // thread's empty default when it first registers or rebuilds a callsite.
    let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    for settle in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        for stop in [false, true] {
            let (mut wire, endpoint) = Wire::new(Role::Receiver, 0, settle.clone()).await;
            let LinkEndpoint::Sender(sender) = endpoint else {
                panic!("actual CBS reply sender")
            };
            let authorization = authorization();
            let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
            route
                .send(CbsResponse::accepted(MessageId::Ulong(42)))
                .await
                .unwrap();
            let diagnostics = Arc::new(AtomicUsize::new(0));
            let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
            std::thread::spawn(tracing::callsite::rebuild_interest_cache)
                .join()
                .unwrap();
            assert_eq!(diagnostics.load(Ordering::SeqCst), 0);
            let mut serving = Box::pin(
                AssertUnwindSafe(serve_cbs_replies(
                    sender,
                    ADDRESS.to_owned(),
                    route.clone(),
                    responses,
                    Arc::clone(&authorization),
                ))
                .catch_unwind(),
            );
            // Scope the subscriber to polls of this same original wrapper only.
            let mut observed = Box::pin(poll_fn(|context| {
                tracing::dispatcher::with_default(&dispatch, || serving.as_mut().poll(context))
            }));
            pending_once(observed.as_mut()).await;
            wire.barrier().await;
            let (_replacement, mut replacement_responses) =
                authorization.register_reply_route(ADDRESS.to_owned()).await;
            if stop {
                wire.stop().await;
            } else {
                wire.detach().await;
            }
            let error = timeout(WAIT, observed.as_mut())
                .await
                .unwrap()
                .expect("secondary diagnostic panic must not mask the original native error")
                .unwrap_err();
            assert!(matches!(
                error.downcast_ref::<EngineError>(),
                Some(EngineError::RemoteDetached)
            ));
            assert_eq!(
                diagnostics.load(Ordering::SeqCst),
                1,
                "caught diagnostic event was actually reached"
            );
            assert!(
                route.is_closed(),
                "captured route cleanup precedes diagnostics"
            );
            assert!(matches!(
                replacement_responses.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            authorization
                .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
                .await
                .unwrap();
            assert_eq!(
                replacement_responses.recv().await.unwrap().correlation_id,
                MessageId::Ulong(999)
            );
            wire.stop().await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_idle_reply_detach_unregisters_only_its_captured_channel() {
    for replace in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual CBS reply sender")
        };
        let authorization = authorization();
        let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        let mut serving = Box::pin(serve_cbs_replies(
            sender,
            ADDRESS.to_owned(),
            route.clone(),
            responses,
            Arc::clone(&authorization),
        ));
        pending_once(serving.as_mut()).await;
        let replacement = if replace {
            Some(authorization.register_reply_route(ADDRESS.to_owned()).await)
        } else {
            None
        };
        wire.detach().await;
        timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
        assert!(route.is_closed());
        if let Some((_replacement, mut responses)) = replacement {
            assert!(matches!(
                responses.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            authorization
                .route_response(ADDRESS, CbsResponse::accepted(MessageId::Ulong(999)))
                .await
                .unwrap();
            assert_eq!(
                responses.recv().await.unwrap().correlation_id,
                MessageId::Ulong(999)
            );
        } else {
            assert_unregistered(&authorization).await;
        }
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_reply_writer_stays_original_until_explicit_stop_and_retained_native_join() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::First).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("actual CBS reply sender")
    };
    let authorization = authorization();
    let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    route
        .send(CbsResponse::accepted(MessageId::Ulong(42)))
        .await
        .unwrap();
    wire.writes.arm(false);
    let mut serving = Box::pin(serve_cbs_replies(
        sender,
        ADDRESS.to_owned(),
        route.clone(),
        responses,
        Arc::clone(&authorization),
    ));
    pending_once(serving.as_mut()).await;
    timeout(WAIT, wire.writes.reached()).await.unwrap();
    pending_once(serving.as_mut()).await;
    assert!(!route.is_closed());
    wire.stop().await;
    assert!(
        wire.writes.state.lock().unwrap().held,
        "joined stop, not releasing the writer, interrupted the original"
    );
    let error = timeout(WAIT, serving.as_mut()).await.unwrap().unwrap_err();
    assert!(matches!(
        error.downcast_ref::<EngineError>(),
        Some(EngineError::Stopped)
    ));
    assert!(route.is_closed());
    assert_unregistered(&authorization).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_reply_write_error_and_closed_response_channel_unregister_original_route() {
    for write_error in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::First).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual CBS reply sender")
        };
        let authorization = authorization();
        let (route, mut responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        if write_error {
            route
                .send(CbsResponse::accepted(MessageId::Ulong(42)))
                .await
                .unwrap();
            wire.writes.arm(true);
        } else {
            responses.close();
        }
        let mut serving = Box::pin(serve_cbs_replies(
            sender,
            ADDRESS.to_owned(),
            route.clone(),
            responses,
            Arc::clone(&authorization),
        ));
        if write_error {
            pending_once(serving.as_mut()).await;
            timeout(WAIT, wire.writes.reached()).await.unwrap();
        }
        let result = timeout(WAIT, serving.as_mut()).await.unwrap();
        if write_error {
            assert!(matches!(
                result.unwrap_err().downcast_ref::<EngineError>(),
                Some(EngineError::Stopped)
            ));
        } else {
            result.unwrap();
        }
        assert!(route.is_closed());
        assert_unregistered(&authorization).await;
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_begun_second_confirmation_retains_original_error_through_retirement() {
    for fail in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::Second).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual CBS reply sender")
        };
        let authorization = authorization();
        let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        route
            .send(CbsResponse::accepted(MessageId::Ulong(42)))
            .await
            .unwrap();
        let mut serving = Box::pin(serve_cbs_replies(
            sender,
            ADDRESS.to_owned(),
            route.clone(),
            responses,
            Arc::clone(&authorization),
        ));
        pending_once(serving.as_mut()).await;
        let (transfer, _) = wire.response_transfer().await;
        wire.accept_response(transfer.delivery_id.unwrap()).await;
        wire.barrier().await;
        wire.writes.arm(false);
        pending_once(serving.as_mut()).await;
        timeout(WAIT, wire.writes.reached()).await.unwrap();
        pending_once(serving.as_mut()).await;
        assert!(!route.is_closed());
        if fail {
            wire.writes.fail_held_write();
        } else {
            wire.stop().await;
        }
        let error = timeout(WAIT, serving.as_mut()).await.unwrap().unwrap_err();
        if fail {
            assert!(
                matches!(error.downcast_ref::<EngineError>(), Some(EngineError::InvalidState(description)) if description == "controlled CBS native write failure")
            );
        } else {
            assert!(matches!(
                error.downcast_ref::<EngineError>(),
                Some(EngineError::Stopped)
            ));
        }
        assert!(route.is_closed());
        assert_unregistered(&authorization).await;
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_ready_reply_outcome_then_retirement_starts_no_new_second_confirmation() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::Second).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("actual CBS reply sender")
    };
    let authorization = authorization();
    let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    route
        .send(CbsResponse::accepted(MessageId::Ulong(42)))
        .await
        .unwrap();
    let mut serving = Box::pin(serve_cbs_replies(
        sender,
        ADDRESS.to_owned(),
        route.clone(),
        responses,
        Arc::clone(&authorization),
    ));
    pending_once(serving.as_mut()).await;
    let (transfer, _) = wire.response_transfer().await;
    wire.accept_response(transfer.delivery_id.unwrap()).await;
    wire.barrier().await;
    wire.detach().await;
    timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
    wire.barrier().await;
    wire.no_frame_yet().await;
    assert!(route.is_closed());
    assert_unregistered(&authorization).await;
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn healthy_correlated_reply_keeps_identity_bound_second_confirmation_and_wire_shape() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::Second).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("actual CBS reply sender")
    };
    let authorization = authorization();
    let (route, responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
    route
        .send(CbsResponse::accepted(MessageId::Ulong(42)))
        .await
        .unwrap();
    let mut serving = Box::pin(serve_cbs_replies(
        sender,
        ADDRESS.to_owned(),
        route.clone(),
        responses,
        Arc::clone(&authorization),
    ));
    pending_once(serving.as_mut()).await;
    let (transfer, message) = wire.response_transfer().await;
    assert_eq!(
        message.properties.unwrap().correlation_id,
        Some(MessageId::Ulong(42))
    );
    let properties = message.application_properties.unwrap();
    assert_eq!(properties.get(STATUS_CODE_PROPERTY), Some(&Value::Int(202)));
    assert_eq!(
        properties.get(STATUS_DESCRIPTION_PROPERTY),
        Some(&Value::String("Accepted".to_owned()))
    );
    assert!(matches!(message.body, Body::Value(Value::Null)));
    let id = transfer.delivery_id.unwrap();
    wire.accept_response(id).await;
    wire.barrier().await;
    pending_once(serving.as_mut()).await;
    let Performative::Disposition(confirmation) = wire.control(CHANNEL).await else {
        panic!("actual identity-bound confirmation")
    };
    assert_eq!(confirmation.role, Role::Sender);
    assert_eq!(confirmation.first, id);
    assert!(confirmation.settled);
    assert_eq!(confirmation.state, Some(DeliveryState::Accepted(Accepted)));
    wire.barrier().await;
    pending_once(serving.as_mut()).await;
    wire.detach().await;
    timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
    assert!(route.is_closed());
    assert_unregistered(&authorization).await;
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_request_accept_and_reject_preserve_begun_native_errors() {
    for reject in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("actual CBS request receiver")
        };
        let authorization = authorization();
        let (_route, mut responses) = authorization.register_reply_route(ADDRESS.to_owned()).await;
        let message = if reject {
            Message::default()
        } else {
            super::request_retirement_tests::request("invalid-token", 42)
        };
        wire.request_message(&message, false).await;
        wire.barrier().await;
        wire.writes.arm(false);
        let mut serving = Box::pin(serve_cbs_requests(receiver, Arc::clone(&authorization)));
        pending_once(serving.as_mut()).await;
        timeout(WAIT, wire.writes.reached()).await.unwrap();
        pending_once(serving.as_mut()).await;
        wire.stop().await;
        assert!(wire.writes.state.lock().unwrap().held);
        let error = timeout(WAIT, serving.as_mut()).await.unwrap().unwrap_err();
        assert!(matches!(
            error.downcast_ref::<EngineError>(),
            Some(EngineError::Stopped)
        ));
        assert!(matches!(
            responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(authorization.grant_snapshot().await.is_empty());
    }
}
