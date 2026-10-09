//! Real additional admission offers; injected originals are qualified separately.

#[path = "retirement_tests.rs"]
mod retirement_tests;

use super::*;
use crate::listener::connection_custody::ConnectionRetirementRequest;
use crate::listener::control_attachment_custody::{
    ControlAttachmentCustody, ControlContext, PUMP_FAULT as ADMISSION_FAULT,
    PumpFault as AdmissionFault, PumpPoint as AdmissionPoint, RoutePacket,
    serve_control_attachment,
};
use amqp::{Detach, End, EngineError};
use std::panic::{catch_unwind, panic_any};

const ADMISSION_CHANNEL: u16 = 200;
const ADMISSION_HANDLE: u32 = 7;
const REPLY_ADDRESS: &str = "control-admission-replies";

#[derive(Clone, Copy, Debug)]
enum ControlRole {
    CbsRequest,
    CbsReply,
    ManagementRequest,
    ManagementReply,
}

const ROLES: [ControlRole; 4] = [
    ControlRole::CbsRequest,
    ControlRole::CbsReply,
    ControlRole::ManagementRequest,
    ControlRole::ManagementReply,
];

impl ControlRole {
    fn peer_role(self) -> Role {
        match self {
            Self::CbsRequest | Self::ManagementRequest => Role::Sender,
            Self::CbsReply | Self::ManagementReply => Role::Receiver,
        }
    }

    fn reply(self) -> bool {
        matches!(self, Self::CbsReply | Self::ManagementReply)
    }

    fn cbs(self) -> bool {
        matches!(self, Self::CbsRequest | Self::CbsReply)
    }

    fn attach(self, settle: ReceiverSettleMode, missing_count: bool) -> Attach {
        let service = if self.cbs() {
            crate::CBS_NODE
        } else {
            "orders/$management"
        };
        Attach {
            name: "original-control-admission".to_owned(),
            handle: ADMISSION_HANDLE,
            role: self.peer_role(),
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: settle,
            source: self.reply().then(|| Source::new(service)),
            target: Some(Target::new(if self.reply() {
                REPLY_ADDRESS
            } else {
                service
            })),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: (!self.reply() && !missing_count).then_some(0),
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }
}

async fn offered(
    role: ControlRole,
    settle: ReceiverSettleMode,
    missing_count: bool,
) -> (Wire, ServerSession, Attach, LinkEndpoint) {
    let (mut wire, baseline) = Wire::new(
        if role.reply() {
            Role::Sender
        } else {
            Role::Receiver
        },
        if role.reply() { 0 } else { 1 },
        settle.clone(),
    )
    .await;
    wire.begin(ADMISSION_CHANNEL).await;
    let mut session = wire.sessions.pop().unwrap();
    let attach = offer(&mut wire, &mut session, role.attach(settle, missing_count)).await;
    (wire, session, attach, baseline)
}

async fn offer(wire: &mut Wire, session: &mut ServerSession, attach: Attach) -> Attach {
    write_frame(
        &mut wire.peer,
        &frame(ADMISSION_CHANNEL, Performative::Attach(Box::new(attach))),
    )
    .await
    .unwrap();
    timeout(WAIT, session.next_incoming_attach())
        .await
        .unwrap()
        .unwrap()
}

async fn acceptance_frames(wire: &mut Wire, role: ControlRole) -> Attach {
    let Performative::Attach(attach) = wire.control(ADMISSION_CHANNEL).await else {
        panic!("actual offered admission Attach answer");
    };
    assert_eq!(attach.handle, ADMISSION_HANDLE);
    assert_eq!(attach.name, "original-control-admission");
    assert_eq!(
        attach.role,
        if role.reply() {
            Role::Sender
        } else {
            Role::Receiver
        }
    );
    assert_eq!(
        attach.max_message_size,
        (!role.reply()).then_some(crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64)
    );
    if !role.reply() {
        let Performative::Flow(flow) = wire.control(ADMISSION_CHANNEL).await else {
            panic!("actual original receiver credit");
        };
        assert_eq!(flow.handle, Some(ADMISSION_HANDLE));
        assert!(flow.link_credit.unwrap() > 0);
    }
    *attach
}

async fn credit(wire: &mut Wire, delivery_count: u32) {
    write_frame(
        &mut wire.peer,
        &frame(
            ADMISSION_CHANNEL,
            Performative::Flow(Flow {
                handle: Some(ADMISSION_HANDLE),
                delivery_count: Some(delivery_count),
                link_credit: Some(1),
                incoming_window: u32::MAX,
                outgoing_window: u32::MAX,
                ..Flow::default()
            }),
        ),
    )
    .await
    .unwrap();
}

async fn request(wire: &mut Wire, channel: u16, handle: u32, id: u64, reply_to: &str) {
    let message = Message {
        properties: Some(Properties {
            message_id: Some(MessageId::Ulong(id)),
            reply_to: Some(reply_to.to_owned()),
            ..Properties::default()
        }),
        ..Message::default()
    };
    write_frame(
        &mut wire.peer,
        &Frame::Amqp {
            channel,
            performative: Some(Performative::Transfer(Transfer {
                handle,
                delivery_id: Some(id as u32),
                delivery_tag: Some(Binary::from(id.to_be_bytes().to_vec())),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            })),
            payload: encode_message(&message).unwrap(),
        },
    )
    .await
    .unwrap();
}

async fn peer_end(wire: &mut Wire) {
    write_frame(
        &mut wire.peer,
        &frame(ADMISSION_CHANNEL, Performative::End(End::default())),
    )
    .await
    .unwrap();
}

async fn peer_detach(wire: &mut Wire) {
    write_frame(
        &mut wire.peer,
        &frame(
            ADMISSION_CHANNEL,
            Performative::Detach(Detach {
                handle: ADMISSION_HANDLE,
                closed: true,
                error: None,
            }),
        ),
    )
    .await
    .unwrap();
}

fn context<'a, B: Broker>(
    session: &'a ServerSession,
    broker: &'a B,
    namespace: &'a NamespaceName,
    authorization: &'a Arc<ConnectionAuthorization>,
    management: &'a Arc<ConnectionManagement>,
    retirement: Option<&'a ConnectionRetirementRequest>,
) -> ControlContext<'a, B> {
    ControlContext {
        session,
        broker,
        namespace,
        authorization: Some(authorization),
        management,
        retirement,
    }
}

async fn decoded_reply(wire: &mut Wire, channel: u16, id: u64, cbs: bool) -> Transfer {
    let Frame::Amqp {
        channel: actual,
        performative: Some(Performative::Transfer(transfer)),
        payload,
    } = timeout(WAIT, read_frame(&mut wire.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("actual native service response");
    };
    assert_eq!(actual, channel);
    let message = decode_message(&payload).unwrap();
    assert_eq!(
        message.properties.unwrap().correlation_id,
        Some(MessageId::Ulong(id))
    );
    let status_key = if cbs {
        "status-code"
    } else {
        crate::management::STATUS_CODE_PROPERTY
    };
    assert_eq!(
        message.application_properties.unwrap().get(status_key),
        Some(&Value::Int(400))
    );
    transfer
}

async fn accepted_reply(wire: &mut Wire, channel: u16, transfer: &Transfer) {
    write_frame(
        &mut wire.peer,
        &frame(
            channel,
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
}

struct Environment {
    session: ServerSession,
    broker: NoBroker,
    namespace: NamespaceName,
    authorization: Arc<ConnectionAuthorization>,
    management: Arc<ConnectionManagement>,
    notice: ConnectionRetirementRequest,
}

impl Environment {
    fn context(&self) -> ControlContext<'_, NoBroker> {
        self.with_broker(&self.broker)
    }

    fn with_broker<'a, B: Broker>(&'a self, broker: &'a B) -> ControlContext<'a, B> {
        context(
            &self.session,
            broker,
            &self.namespace,
            &self.authorization,
            &self.management,
            Some(&self.notice),
        )
    }
}

async fn fixture(role: ControlRole) -> (Wire, Environment, Attach, LinkEndpoint) {
    let (wire, session, attach, baseline) = offered(role, ReceiverSettleMode::First, false).await;
    let environment = Environment {
        session,
        broker: NoBroker,
        namespace: NamespaceName::new("tenant").unwrap(),
        authorization: Arc::clone(&authorization().connection),
        management: ConnectionManagement::new(),
        notice: ConnectionRetirementRequest::capture(&wire.connection),
    };
    (wire, environment, attach, baseline)
}

fn exact_panic<T>(result: std::thread::Result<T>, expected: &Arc<str>) {
    let payload = match result {
        Err(payload) => payload,
        Ok(_) => panic!("original raw panic resumes"),
    };
    assert!(Arc::ptr_eq(
        payload.downcast_ref::<Arc<str>>().unwrap(),
        expected
    ));
}

async fn route_lock<'a>(role: ControlRole, environment: &'a Environment) -> Box<dyn Send + 'a> {
    if role.cbs() {
        Box::new(environment.authorization.reply_route_lock().await)
    } else {
        Box::new(environment.management.routes.lock().await)
    }
}

fn begin_route(
    custody: &mut ControlAttachmentCustody<'_>,
    role: ControlRole,
    environment: &Environment,
) -> bool {
    if role.cbs() {
        custody.begin_cbs_route(
            Arc::clone(&environment.authorization),
            REPLY_ADDRESS.to_owned(),
        )
    } else {
        custody.begin_management_route(
            Arc::clone(&environment.management),
            REPLY_ADDRESS.to_owned(),
        )
    }
}

async fn replacement(role: ControlRole, environment: &Environment) -> RoutePacket {
    if role.cbs() {
        let registry = Arc::clone(&environment.authorization);
        let (route, responses) = registry
            .register_reply_route(REPLY_ADDRESS.to_owned())
            .await;
        RoutePacket::Cbs {
            registry,
            address: REPLY_ADDRESS.to_owned(),
            route,
            responses,
        }
    } else {
        let registry = Arc::clone(&environment.management);
        let (route, responses) = registry
            .register_reply_route(REPLY_ADDRESS.to_owned())
            .await;
        RoutePacket::Management {
            registry,
            address: REPLY_ADDRESS.to_owned(),
            route,
            responses,
        }
    }
}

enum RouteProbe {
    Cbs(mpsc::Sender<crate::cbs::CbsResponse>),
    Management(mpsc::Sender<ManagementResponse>),
}

impl RouteProbe {
    fn captured(packet: &RoutePacket) -> Self {
        match packet {
            RoutePacket::Cbs { address, route, .. } => {
                assert_eq!(address, REPLY_ADDRESS);
                Self::Cbs(route.clone())
            }
            RoutePacket::Management { address, route, .. } => {
                assert_eq!(address, REPLY_ADDRESS);
                Self::Management(route.clone())
            }
        }
    }

    fn check(&self, packet: &RoutePacket, closed: bool) {
        match (self, packet) {
            (Self::Cbs(original), RoutePacket::Cbs { route, .. }) => {
                assert!(original.same_channel(route));
                assert_eq!(original.is_closed(), closed);
            }
            (Self::Management(original), RoutePacket::Management { route, .. }) => {
                assert!(original.same_channel(route));
                assert_eq!(original.is_closed(), closed);
            }
            _ => panic!("original typed route stays exact"),
        }
    }
}

async fn replacement_usable(packet: RoutePacket) {
    let cbs = matches!(&packet, RoutePacket::Cbs { .. });
    // These already accepted endpoints prove actual routing, not admission of another leaf.
    let (mut requests, request_endpoint) =
        Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
    let (mut replies, reply_endpoint) =
        Wire::new(Role::Receiver, 1, ReceiverSettleMode::First).await;
    let LinkEndpoint::Receiver(receiver) = request_endpoint else {
        panic!("real request receiver")
    };
    let LinkEndpoint::Sender(sender) = reply_endpoint else {
        panic!("real reply sender")
    };
    let (request_task, reply_task) = match packet {
        RoutePacket::Cbs {
            registry,
            address,
            route,
            mut responses,
        } => {
            assert!(matches!(
                responses.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            let request_registry = Arc::clone(&registry);
            (
                tokio::spawn(async move {
                    let _ = crate::cbs::serve_cbs_requests(receiver, request_registry).await;
                }),
                tokio::spawn(async move {
                    let _ =
                        crate::cbs::serve_cbs_replies(sender, address, route, responses, registry)
                            .await;
                }),
            )
        }
        RoutePacket::Management {
            registry,
            address,
            route,
            mut responses,
        } => {
            assert!(matches!(
                responses.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            let request_registry = Arc::clone(&registry);
            (
                tokio::spawn(async move {
                    let _ = serve_management_requests(
                        receiver,
                        NamespaceName::new("tenant").unwrap(),
                        EntityPath::new("orders").unwrap(),
                        NoBroker,
                        request_registry,
                        None,
                    )
                    .await;
                }),
                tokio::spawn(async move {
                    let _ =
                        serve_management_replies(sender, address, route, responses, registry, None)
                            .await;
                }),
            )
        }
    };
    request(&mut requests, CHANNEL, HANDLE, 999, REPLY_ADDRESS).await;
    let Performative::Disposition(ack) = requests.control(CHANNEL).await else {
        panic!("real replacement request ack")
    };
    assert_eq!(ack.first, 999);
    let transfer = decoded_reply(&mut replies, CHANNEL, 999, cbs).await;
    accepted_reply(&mut replies, CHANNEL, &transfer).await;
    replies.barrier().await;
    requests.stop().await;
    replies.stop().await;
    timeout(WAIT, request_task).await.unwrap().unwrap();
    timeout(WAIT, reply_task).await.unwrap().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn actual_control_roles_adopt_original_endpoint_once_and_preserve_settlement() {
    // Twelve actual added admissions; the complementary old endpoint is only transport.
    for role in ROLES {
        for settle in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            for missing_count in [false, true] {
                if role.reply() && missing_count {
                    continue;
                }
                let (mut wire, session, attach, baseline) =
                    offered(role, settle.clone(), missing_count).await;
                let link_authorization = authorization();
                let authorization = Arc::clone(&link_authorization.connection);
                let management = ConnectionManagement::new();
                let namespace = NamespaceName::new("tenant").unwrap();
                let broker = NoBroker;
                let admitted = timeout(
                    WAIT,
                    serve_control_attachment(
                        context(
                            &session,
                            &broker,
                            &namespace,
                            &authorization,
                            &management,
                            None,
                        ),
                        attach,
                    ),
                )
                .await
                .unwrap()
                .unwrap()
                .unwrap();
                let admitted_id = admitted.task.id();
                let answer = acceptance_frames(&mut wire, role).await;
                let expected = role.attach(settle.clone(), missing_count);
                assert_eq!(answer.source, expected.source);
                assert_eq!(answer.target, expected.target);
                assert_eq!(answer.initial_delivery_count, role.reply().then_some(0));
                let companion = match baseline {
                    LinkEndpoint::Receiver(receiver) => {
                        assert!(role.reply());
                        credit(&mut wire, 0).await;
                        let authorization = Arc::clone(&authorization);
                        let management = Arc::clone(&management);
                        tokio::spawn(async move {
                            if role.cbs() {
                                let _ =
                                    crate::cbs::serve_cbs_requests(receiver, authorization).await;
                            } else {
                                let _ = serve_management_requests(
                                    receiver,
                                    namespace,
                                    EntityPath::new("orders").unwrap(),
                                    NoBroker,
                                    management,
                                    Some(link_authorization),
                                )
                                .await;
                            }
                        })
                    }
                    LinkEndpoint::Sender(sender) => {
                        assert!(!role.reply());
                        let authorization = Arc::clone(&authorization);
                        let management = Arc::clone(&management);
                        if role.cbs() {
                            let (route, responses) = authorization
                                .register_reply_route(REPLY_ADDRESS.to_owned())
                                .await;
                            tokio::spawn(async move {
                                let _ = crate::cbs::serve_cbs_replies(
                                    sender,
                                    REPLY_ADDRESS.to_owned(),
                                    route,
                                    responses,
                                    authorization,
                                )
                                .await;
                            })
                        } else {
                            let (route, responses) = management
                                .register_reply_route(REPLY_ADDRESS.to_owned())
                                .await;
                            tokio::spawn(async move {
                                let _ = serve_management_replies(
                                    sender,
                                    REPLY_ADDRESS.to_owned(),
                                    route,
                                    responses,
                                    management,
                                    Some(link_authorization),
                                )
                                .await;
                            })
                        }
                    }
                };
                let (request_channel, request_handle, reply_channel) = if role.reply() {
                    (CHANNEL, HANDLE, ADMISSION_CHANNEL)
                } else {
                    (ADMISSION_CHANNEL, ADMISSION_HANDLE, CHANNEL)
                };
                request(
                    &mut wire,
                    request_channel,
                    request_handle,
                    42,
                    REPLY_ADDRESS,
                )
                .await;
                let Performative::Disposition(ack) = wire.control(request_channel).await else {
                    panic!("original request acknowledgement");
                };
                assert_eq!(ack.role, Role::Receiver);
                assert_eq!(ack.first, 42);
                assert!(matches!(ack.state, Some(DeliveryState::Accepted(_))));
                let transfer = decoded_reply(&mut wire, reply_channel, 42, role.cbs()).await;
                accepted_reply(&mut wire, reply_channel, &transfer).await;
                if settle == ReceiverSettleMode::Second {
                    let Performative::Disposition(confirmation) = wire.control(reply_channel).await
                    else {
                        panic!("same original second-settlement confirmation");
                    };
                    assert_eq!(confirmation.role, Role::Sender);
                    assert_eq!(confirmation.first, transfer.delivery_id.unwrap());
                    assert!(confirmation.settled);
                    assert!(matches!(
                        confirmation.state,
                        Some(DeliveryState::Accepted(_))
                    ));
                    wire.barrier().await;
                } else {
                    wire.barrier().await;
                    wire.no_frame_yet().await;
                }
                assert_eq!(admitted.task.id(), admitted_id);
                wire.stop().await;
                timeout(WAIT, admitted.task).await.unwrap().unwrap();
                timeout(WAIT, companion).await.unwrap().unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_control_acceptance_writer_survives_parent_fault_until_captured_stop() {
    for role in ROLES {
        let (mut wire, environment, attach, _baseline) = fixture(role).await;
        let (mut independent, _endpoint) =
            Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        wire.writes.arm(false);
        let fault = AdmissionFault::new(AdmissionPoint::AcceptanceBegun);
        let mut serving = Box::pin(
            ADMISSION_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(serve_control_attachment(environment.context(), attach))
                    .catch_unwind(),
            ),
        );
        pending_once(serving.as_mut()).await;
        timeout(WAIT, wire.writes.reached()).await.unwrap();
        timeout(WAIT, fault.reached.notified()).await.unwrap();
        assert!(!environment.notice.is_requested());
        fault.trigger.notify_one();
        // A borrowed observation can be cancelled; the original native write stays held.
        for _ in 0..2 {
            pending_once(serving.as_mut()).await;
            assert!(
                environment.notice.is_requested(),
                "fault requests original Stop before fixture teardown"
            );
        }
        exact_panic(
            timeout(WAIT, serving.as_mut()).await.unwrap(),
            &fault.payload,
        );
        assert!(wire.writes.state.lock().unwrap().held);
        assert!(environment.session.is_ended());
        independent.barrier().await;
        environment.notice.request();
        wire.stop().await;
        independent.stop().await;
    }
}

struct ReportPanic {
    reached: Arc<AtomicUsize>,
    payload: Arc<str>,
}

impl tracing::Subscriber for ReportPanic {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata
            .target()
            .ends_with("::listener::control_attachment_custody")
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Message(bool);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message"
                    && format!("{value:?}").contains("original control-link admission retained")
                {
                    self.0 = true;
                }
            }
        }
        let mut message = Message(false);
        event.record(&mut message);
        if message.0 {
            self.reached.fetch_add(1, Ordering::SeqCst);
            panic_any(Arc::clone(&self.payload));
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test(flavor = "current_thread")]
async fn actual_control_acceptance_error_precedes_secondary_diagnostics() {
    let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    for role in ROLES {
        for stop in [false, true] {
            let (mut wire, environment, attach, _baseline) = fixture(role).await;
            wire.writes.arm(false);
            let reports = Arc::new(AtomicUsize::new(0));
            let dispatch = tracing::Dispatch::new(ReportPanic {
                reached: Arc::clone(&reports),
                payload: Arc::from("secondary admission report panic"),
            });
            std::thread::spawn(tracing::callsite::rebuild_interest_cache)
                .join()
                .unwrap();
            let mut serving = Box::pin(
                AssertUnwindSafe(serve_control_attachment(environment.context(), attach))
                    .catch_unwind(),
            );
            let mut observed = Box::pin(poll_fn(|context| {
                tracing::dispatcher::with_default(&dispatch, || serving.as_mut().poll(context))
            }));
            pending_once(observed.as_mut()).await;
            timeout(WAIT, wire.writes.reached()).await.unwrap();
            if stop {
                environment.notice.request();
            } else {
                wire.writes.fail_held_write();
            }
            // Terminal native observation is positive, unlike one speculative Pending poll.
            timeout(WAIT, environment.session.on_end_owned())
                .await
                .unwrap();
            let result = timeout(WAIT, observed.as_mut())
                .await
                .unwrap()
                .expect("native error outranks reached report panic");
            assert!(matches!(result, Err(EngineError::Stopped)));
            assert_eq!(reports.load(Ordering::SeqCst), 1);
            assert_eq!(
                environment.notice.is_requested(),
                stop,
                "a reached report is not a new primary fault"
            );
            assert!(wire.writes.state.lock().unwrap().held);
            wire.stop().await;
        }
    }
}
