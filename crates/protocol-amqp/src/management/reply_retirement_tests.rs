//! Original native replies and local route waits under observed retirement.

use std::{
    future::{Future, poll_fn},
    io,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    task::{Context, Poll, Waker},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use amqp::{
    Accepted, Attach, Begin, Disposition, Flow, Frame, LinkEndpoint, Open, Performative,
    Properties, ProtocolHeader, ReceiverSettleMode, Role, SenderSettleMode, ServerConnection,
    ServerSession, Source, Target, Transfer, decode_message, encode_message, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use auth::{PermissionSet, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use serde_amqp::primitives::Binary;
use sha2::Sha256;
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, duplex},
    time::timeout,
};

use super::*;
use crate::SharedAccessAuthentication;

const WAIT: Duration = Duration::from_secs(8);
const ADDRESS: &str = "management-replies";
const CHANNEL: u16 = 1;
const HANDLE: u32 = 1;

#[path = "reply_panic_tests.rs"]
mod panic_tests;

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

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.held = false;
        if let Some(waker) = state.waker.take() {
            waker.wake();
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
                        "controlled native reply write failure",
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
                    "management-server",
                    None
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
                        &frame(0, Performative::Open(Open::new("management-peer"))),
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
                    name: "management-transport".to_owned(),
                    handle: HANDLE,
                    role: role.clone(),
                    snd_settle_mode: SenderSettleMode::Unsettled,
                    rcv_settle_mode: settle,
                    source: (role == Role::Receiver).then(|| Source::new("orders/$management")),
                    target: Some(Target::new(if role == Role::Receiver {
                        ADDRESS
                    } else {
                        "orders/$management"
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
            panic!("actual native control frame");
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

    async fn request(&mut self) {
        let mut properties = ApplicationProperties::default();
        properties.insert(OPERATION_PROPERTY, "unsupported-control");
        let message = Message {
            properties: Some(Properties {
                message_id: Some(MessageId::Ulong(42)),
                reply_to: Some(ADDRESS.to_owned()),
                ..Properties::default()
            }),
            application_properties: Some(properties),
            ..Message::default()
        };
        self.request_message(&message).await;
    }

    pub(super) async fn request_message(&mut self, message: &Message) {
        write_frame(
            &mut self.peer,
            &Frame::Amqp {
                channel: CHANNEL,
                performative: Some(Performative::Transfer(Transfer {
                    handle: HANDLE,
                    delivery_id: Some(1),
                    delivery_tag: Some(Binary::from(vec![1])),
                    message_format: Some(0),
                    settled: Some(false),
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

    pub(super) async fn no_frame_yet(&mut self) {
        let mut next = Box::pin(read_frame(&mut self.peer));
        pending_once(next.as_mut()).await;
    }

    pub(super) async fn stop(&mut self) {
        self.connection.stop();
        let _ = timeout(WAIT, self.connection.shutdown())
            .await
            .expect("original native tasks joined");
    }
}

impl Drop for Wire {
    fn drop(&mut self) {
        self.writes.release();
        self.connection.stop();
    }
}

async fn pending_once<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    poll_fn(|context| {
        assert!(future.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
}

fn response(id: u64) -> ManagementResponse {
    ManagementResponse::accepted(
        MessageId::Ulong(id),
        Some("tracking-control".to_owned()),
        Value::Null,
    )
}

fn authorization() -> ManagementAuthorization {
    authorization_with(PermissionSet::LISTEN)
}

fn authorization_with(permissions: PermissionSet) -> ManagementAuthorization {
    let rule = SharedAccessRule::new(
        "listen",
        ResourceScope::namespace("tenant.servicebus.windows.net").unwrap(),
        SharedAccessKey::new("secret").unwrap(),
        None,
        permissions,
    )
    .unwrap();
    let policy = SharedAccessPolicy::new([rule]).unwrap();
    let grant = policy.authenticate_plain("listen", "secret").unwrap();
    ManagementAuthorization::new(
        ConnectionAuthorization::new(
            SharedAccessAuthentication::new(policy, "tenant.servicebus.windows.net").unwrap(),
            Some(grant),
        ),
        ResourceScope::entity("tenant.servicebus.windows.net", "orders").unwrap(),
    )
}

async fn expire(authorization: &ManagementAuthorization) {
    let encoded = "amqps%3A%2F%2Ftenant.servicebus.windows.net";
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 2;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    mac.update(format!("{encoded}\n{expiry}").as_bytes());
    let signature = STANDARD
        .encode(mac.finalize().into_bytes())
        .replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D");
    authorization
        .connection
        .validate_and_add(
            &format!("SharedAccessSignature sr={encoded}&sig={signature}&se={expiry}&skn=listen"),
            "amqps://tenant.servicebus.windows.net",
        )
        .await
        .unwrap();
    assert!(authorization.ensure_any().await.is_ok());
    timeout(WAIT, authorization.wait_until_unauthorized())
        .await
        .unwrap();
}

#[derive(Clone)]
struct NoBroker;

impl Broker for NoBroker {
    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        panic!("unsupported operation must not invoke the broker");
    }
    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        std::future::pending().await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn operation_authorization_precedes_invalid_session_fields_and_locked_lookup() {
    let management = ConnectionManagement::new();
    let held = management.sessions.write().await;
    let authorization = authorization_with(PermissionSet::SEND);
    let mut properties = ApplicationProperties::default();
    properties.insert(OPERATION_PROPERTY, GET_SESSION_STATE_OPERATION);
    properties.insert(TRACKING_ID_PROPERTY, "authorization-control");
    let message = Message {
        application_properties: Some(properties),
        body: Body::Value(Value::Null),
        ..Message::default()
    };
    let broker = RequestBroker::new(NoBroker);
    let namespace = NamespaceName::new("tenant").unwrap();
    let entity = EntityPath::new("orders").unwrap();
    let mut original = PendingOperation::new(
        process_request(
            &message,
            MessageId::Ulong(42),
            &namespace,
            &entity,
            &broker,
            &management,
            Some(&authorization),
        ),
        broker.control(),
    );
    timeout(WAIT, original.observe()).await.unwrap().unwrap();
    let packet = original.take_packet().unwrap();
    assert!(!packet.started && !packet.retired);
    let response = packet.result.unwrap();
    assert_eq!(response.status_code, 401);
    let wire = response.into_message();
    assert_eq!(
        wire.properties.unwrap().correlation_id,
        Some(MessageId::Ulong(42))
    );
    assert_eq!(
        wire.application_properties
            .unwrap()
            .get(TRACKING_ID_PROPERTY),
        Some(&Value::String("authorization-control".to_owned()))
    );
    drop(held);
}

#[tokio::test(flavor = "current_thread")]
async fn consumed_native_errors_precede_cleanup_and_begun_close_errors_are_not_normalized() {
    for error in [
        EngineError::RemoteClosed,
        EngineError::RemoteDetached,
        EngineError::Stopped,
        EngineError::InvalidState("original-close".to_owned()),
    ] {
        let expected = error.to_string();
        let returned = guarded_close(async { Err(error) }, std::future::pending())
            .await
            .unwrap_err();
        assert_eq!(returned.to_string(), expected);
    }

    let mut original = native_operation(async {
        Err::<(), _>(EngineError::InvalidState("original-native".to_owned()))
    });
    assert!(original.observe().await.unwrap().is_err());
    let _ = original.finish().await;
    let returned = consume_native_result(
        &mut original,
        Err(EngineError::InvalidState("cleanup-native".to_owned())),
    )
    .unwrap_err();
    assert!(matches!(
        returned,
        EngineError::InvalidState(description) if description == "original-native"
    ));
    assert!(original.take_packet().is_none());

    let mut preparation = native_operation(std::future::pending::<Result<(), EngineError>>());
    assert!(preparation.finish().await.is_none());
    consume_native_result(&mut preparation, Ok(())).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn actual_request_accept_and_reject_keep_begun_native_errors_after_joined_stop() {
    for reject in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("actual management request receiver");
        };
        let management = ConnectionManagement::new();
        let (_route, mut responses) = management.register_reply_route(ADDRESS.to_owned()).await;
        if reject {
            wire.request_message(&Message::default()).await;
        } else {
            wire.request().await;
        }
        wire.barrier().await;
        wire.writes.arm(false);
        let mut serving = Box::pin(serve_management_requests(
            receiver,
            NamespaceName::new("tenant").unwrap(),
            EntityPath::new("orders").unwrap(),
            NoBroker,
            management,
            None,
        ));
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
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_authorization_close_write_errors_remain_original_for_request_and_reply() {
    for role in [Role::Sender, Role::Receiver] {
        let (mut wire, endpoint) = Wire::new(role, 0, ReceiverSettleMode::First).await;
        let management = ConnectionManagement::new();
        let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
        let authorization = authorization();
        let serving_authorization = authorization.clone();
        let mut serving = Box::pin(async move {
            match endpoint {
                LinkEndpoint::Receiver(receiver) => {
                    serve_management_requests(
                        receiver,
                        NamespaceName::new("tenant").unwrap(),
                        EntityPath::new("orders").unwrap(),
                        NoBroker,
                        Arc::clone(&management),
                        Some(serving_authorization),
                    )
                    .await
                }
                LinkEndpoint::Sender(sender) => {
                    serve_management_replies(
                        sender,
                        ADDRESS.to_owned(),
                        route,
                        responses,
                        Arc::clone(&management),
                        Some(serving_authorization),
                    )
                    .await
                }
            }
        });
        pending_once(serving.as_mut()).await;
        expire(&authorization).await;
        wire.writes.arm(true);
        pending_once(serving.as_mut()).await;
        timeout(WAIT, wire.writes.reached()).await.unwrap();
        let error = timeout(WAIT, serving.as_mut()).await.unwrap().unwrap_err();
        assert!(matches!(
            error.downcast_ref::<EngineError>(),
            Some(EngineError::Stopped)
        ));
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_request_full_seventeenth_route_and_missing_route_retire_without_relookup() {
    for full in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("actual management request receiver");
        };
        let management = ConnectionManagement::new();
        let old = if full {
            let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
            for id in 0..16 {
                route.send(response(id)).await.unwrap();
            }
            assert_eq!(route.capacity(), 0);
            Some((route, responses))
        } else {
            None
        };
        wire.request().await;
        wire.barrier().await;
        let mut serving = Box::pin(serve_management_requests(
            receiver,
            NamespaceName::new("tenant").unwrap(),
            EntityPath::new("orders").unwrap(),
            NoBroker,
            Arc::clone(&management),
            None,
        ));
        pending_once(serving.as_mut()).await;
        let Performative::Disposition(disposition) = wire.control(CHANNEL).await else {
            panic!("original request acknowledgement");
        };
        assert_eq!(disposition.first, 1);
        assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
        wire.barrier().await;
        pending_once(serving.as_mut()).await;
        let (replacement, mut replacement_responses) =
            management.register_reply_route(ADDRESS.to_owned()).await;
        wire.detach().await;
        timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
        assert!(matches!(
            replacement_responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(
            management
                .routes
                .lock()
                .await
                .senders
                .get(ADDRESS)
                .unwrap()
                .same_channel(&replacement)
        );
        if let Some((route, mut responses)) = old {
            assert_eq!(route.capacity(), 0);
            for id in 0..16 {
                assert_eq!(
                    responses.try_recv().unwrap().correlation_id,
                    MessageId::Ulong(id)
                );
            }
            assert!(matches!(
                responses.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_no_credit_reply_retires_on_detach_or_authorization_without_replacement_retry() {
    for unauthorized in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual management reply sender");
        };
        let management = ConnectionManagement::new();
        let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
        route.send(response(42)).await.unwrap();
        let authorization = unauthorized.then(authorization);
        let mut serving = Box::pin(serve_management_replies(
            sender,
            ADDRESS.to_owned(),
            route.clone(),
            responses,
            Arc::clone(&management),
            authorization.clone(),
        ));
        pending_once(serving.as_mut()).await;
        wire.barrier().await;
        let (replacement, mut replacement_responses) =
            management.register_reply_route(ADDRESS.to_owned()).await;
        if let Some(authorization) = authorization.as_ref() {
            expire(authorization).await;
            let (result, detach) = timeout(WAIT, async {
                tokio::join!(serving.as_mut(), wire.control(CHANNEL))
            })
            .await
            .unwrap();
            assert!(matches!(
                result.unwrap_err().downcast_ref::<EngineError>(),
                Some(EngineError::RemoteDetached)
            ));
            let Performative::Detach(detach) = detach else {
                panic!("actual unauthorized close");
            };
            assert_eq!(
                detach.error.unwrap().condition,
                AmqpError::UnauthorizedAccess.into()
            );
        } else {
            wire.detach().await;
            let error = timeout(WAIT, serving.as_mut()).await.unwrap().unwrap_err();
            assert!(matches!(
                error.downcast_ref::<EngineError>(),
                Some(EngineError::RemoteDetached)
            ));
        }
        assert!(route.is_closed());
        assert!(matches!(
            replacement_responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(
            management
                .routes
                .lock()
                .await
                .senders
                .get(ADDRESS)
                .unwrap()
                .same_channel(&replacement)
        );
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_started_reply_writer_is_retained_until_joined_stop_then_route_cleanup() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::First).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("actual management reply sender");
    };
    let management = ConnectionManagement::new();
    let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
    route.send(response(42)).await.unwrap();
    wire.writes.arm(false);
    let mut serving = Box::pin(serve_management_replies(
        sender,
        ADDRESS.to_owned(),
        route.clone(),
        responses,
        Arc::clone(&management),
        None,
    ));
    pending_once(serving.as_mut()).await;
    timeout(WAIT, wire.writes.reached()).await.unwrap();
    pending_once(serving.as_mut()).await;
    assert!(
        management
            .routes
            .lock()
            .await
            .senders
            .get(ADDRESS)
            .unwrap()
            .same_channel(&route)
    );
    wire.stop().await;
    assert!(
        wire.writes.state.lock().unwrap().held,
        "only joined stop interrupted the held original write"
    );
    let error = timeout(WAIT, serving.as_mut()).await.unwrap().unwrap_err();
    assert!(matches!(
        error.downcast_ref::<EngineError>(),
        Some(EngineError::Stopped)
    ));
    assert!(route.is_closed());
    assert!(!management.routes.lock().await.senders.contains_key(ADDRESS));
}

#[tokio::test(flavor = "current_thread")]
async fn actual_native_reply_write_error_and_closed_response_channel_clean_original_route() {
    for write_error in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::First).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual management reply sender");
        };
        let management = ConnectionManagement::new();
        let (route, mut responses) = management.register_reply_route(ADDRESS.to_owned()).await;
        if write_error {
            route.send(response(42)).await.unwrap();
            wire.writes.arm(true);
        } else {
            responses.close();
        }
        let mut serving = Box::pin(serve_management_replies(
            sender,
            ADDRESS.to_owned(),
            route.clone(),
            responses,
            Arc::clone(&management),
            None,
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
        assert!(!management.routes.lock().await.senders.contains_key(ADDRESS));
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_begun_reply_confirmation_error_precedes_authorization_cleanup_close() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::Second).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("actual management reply sender");
    };
    let management = ConnectionManagement::new();
    let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
    route.send(response(42)).await.unwrap();
    let authorization = authorization();
    let mut serving = Box::pin(serve_management_replies(
        sender,
        ADDRESS.to_owned(),
        route.clone(),
        responses,
        Arc::clone(&management),
        Some(authorization.clone()),
    ));
    pending_once(serving.as_mut()).await;
    let Frame::Amqp {
        performative: Some(Performative::Transfer(transfer)),
        ..
    } = timeout(WAIT, read_frame(&mut wire.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("actual response Transfer");
    };
    write_frame(
        &mut wire.peer,
        &frame(
            CHANNEL,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: transfer.delivery_id.unwrap(),
                last: None,
                settled: false,
                state: Some(DeliveryState::Accepted(Accepted)),
                batchable: false,
            }),
        ),
    )
    .await
    .unwrap();
    wire.barrier().await;
    wire.writes.arm(false);
    pending_once(serving.as_mut()).await;
    timeout(WAIT, wire.writes.reached()).await.unwrap();
    expire(&authorization).await;
    timeout(
        WAIT,
        poll_fn(|context| {
            assert!(
                serving.as_mut().poll(context).is_pending(),
                "the original confirmation write remains held"
            );
            if route.is_closed() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .expect("the original reply pump observed authorization retirement");
    wire.writes.fail_held_write();
    let error = timeout(WAIT, serving.as_mut()).await.unwrap().unwrap_err();
    assert!(matches!(
        error.downcast_ref::<EngineError>(),
        Some(EngineError::InvalidState(description))
            if description == "controlled native reply write failure"
    ));
    assert!(route.is_closed());
    assert!(!management.routes.lock().await.senders.contains_key(ADDRESS));
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_retirement_after_reply_outcome_starts_no_new_second_confirmation() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::Second).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("actual management reply sender");
    };
    let management = ConnectionManagement::new();
    let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
    route.send(response(42)).await.unwrap();
    let mut serving = Box::pin(serve_management_replies(
        sender,
        ADDRESS.to_owned(),
        route.clone(),
        responses,
        Arc::clone(&management),
        None,
    ));
    pending_once(serving.as_mut()).await;
    let Frame::Amqp {
        performative: Some(Performative::Transfer(transfer)),
        ..
    } = timeout(WAIT, read_frame(&mut wire.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("actual response Transfer");
    };
    write_frame(
        &mut wire.peer,
        &frame(
            CHANNEL,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: transfer.delivery_id.unwrap(),
                last: None,
                settled: false,
                state: Some(DeliveryState::Accepted(Accepted)),
                batchable: false,
            }),
        ),
    )
    .await
    .unwrap();
    // The original native outcome is available, but serving has not polled
    // it again or admitted its identity-bound confirmation.
    wire.barrier().await;
    wire.detach().await;
    timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
    wire.barrier().await;
    wire.no_frame_yet().await;
    assert!(route.is_closed());
    assert!(!management.routes.lock().await.senders.contains_key(ADDRESS));
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn healthy_reply_preserves_correlation_tracking_and_identity_bound_second_confirmation() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::Second).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("actual management reply sender");
    };
    let management = ConnectionManagement::new();
    let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
    route.send(response(42)).await.unwrap();
    let mut serving = Box::pin(serve_management_replies(
        sender,
        ADDRESS.to_owned(),
        route.clone(),
        responses,
        Arc::clone(&management),
        None,
    ));
    pending_once(serving.as_mut()).await;
    let Frame::Amqp {
        performative: Some(Performative::Transfer(transfer)),
        payload,
        ..
    } = timeout(WAIT, read_frame(&mut wire.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("actual response Transfer");
    };
    let message = decode_message(&payload).unwrap();
    assert_eq!(
        message.properties.unwrap().correlation_id,
        Some(MessageId::Ulong(42))
    );
    assert_eq!(
        message
            .application_properties
            .unwrap()
            .get(TRACKING_ID_PROPERTY),
        Some(&Value::String("tracking-control".to_owned()))
    );
    let id = transfer.delivery_id.unwrap();
    write_frame(
        &mut wire.peer,
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
    wire.barrier().await;
    pending_once(serving.as_mut()).await;
    let Performative::Disposition(confirmed) = wire.control(CHANNEL).await else {
        panic!("identity-bound native confirmation");
    };
    assert_eq!(confirmed.role, Role::Sender);
    assert_eq!(confirmed.first, id);
    assert!(confirmed.settled);
    assert_eq!(confirmed.state, Some(DeliveryState::Accepted(Accepted)));
    wire.barrier().await;
    pending_once(serving.as_mut()).await;
    wire.detach().await;
    timeout(WAIT, serving.as_mut()).await.unwrap().unwrap();
    assert!(route.is_closed());
    assert!(!management.routes.lock().await.senders.contains_key(ADDRESS));
    wire.stop().await;
}
