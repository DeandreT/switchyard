//! Original inbound broker results across positively observed link retirement.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use amqp::{
    AmqpError, Attach, Begin, Body, DeliveryState, Detach, Disposition, Frame, LinkEndpoint,
    Message, Open, Performative, Properties, ProtocolHeader, Receiver, ReceiverSettleMode, Role,
    SenderSettleMode, ServerConnection, ServerSession, Target, Transfer, encode_message,
    read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use auth::{
    Permission, PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use domain::{
    BrokerError, CommandKind, CommandOutcome, MessageState, QueueCounters, SequenceNumber,
    StateMachine,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use storage::StateStore;
use tokio::{
    io::{DuplexStream, duplex},
    sync::Notify,
    time::timeout,
};

use super::test_support::{Actor, ActualBroker, WAIT, pending_once};
use crate::authorization::ConnectionAuthorization;
use crate::listener::{
    LinkAuthorization, SEND_INTAKE_RETIREMENT, ingress::SendIntake, serve_sending_client,
};
use crate::{
    Broker, BrokerRejection, SERVICE_BUS_BATCH_MESSAGE_FORMAT, SharedAccessAuthentication,
};

const CHANNEL: u16 = 1;
const HANDLE: u32 = 1;

fn frame(channel: u16, performative: Performative, payload: Vec<u8>) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload,
    }
}

async fn control(peer: &mut DuplexStream, expected_channel: u16) -> Performative {
    let Frame::Amqp {
        channel,
        performative: Some(performative),
        payload,
    } = timeout(WAIT, read_frame(peer)).await.unwrap().unwrap()
    else {
        panic!("actual AMQP response");
    };
    assert_eq!(channel, expected_channel);
    assert!(payload.is_empty());
    performative
}

struct IngressWire {
    connection: ServerConnection,
    _session: ServerSession,
    peer: DuplexStream,
    receiver: Option<Receiver>,
    next_channel: u16,
}

impl IngressWire {
    async fn new() -> Self {
        let (stream, mut peer) = duplex(64 * 1024);
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept(stream, "ingress-server", None),
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
                        &frame(0, Performative::Open(Open::new("ingress-peer")), Vec::new()),
                    )
                    .await
                    .unwrap();
                    assert!(matches!(control(&mut peer, 0).await, Performative::Open(_)));
                }
            )
        })
        .await
        .unwrap();
        let mut connection = connection.unwrap();
        write_frame(
            &mut peer,
            &frame(CHANNEL, Performative::Begin(Begin::default()), Vec::new()),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let mut session = connection.accept_session(incoming).await.unwrap();
        assert!(matches!(
            control(&mut peer, CHANNEL).await,
            Performative::Begin(_)
        ));
        write_frame(
            &mut peer,
            &frame(
                CHANNEL,
                Performative::Attach(Box::new(Attach {
                    name: "owned-ingress".to_owned(),
                    handle: HANDLE,
                    role: Role::Sender,
                    snd_settle_mode: SenderSettleMode::Unsettled,
                    rcv_settle_mode: ReceiverSettleMode::First,
                    source: None,
                    target: Some(Target::new("orders")),
                    unsettled: None,
                    incomplete_unsettled: false,
                    initial_delivery_count: Some(0),
                    max_message_size: None,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                })),
                Vec::new(),
            ),
        )
        .await
        .unwrap();
        let attach = timeout(WAIT, session.next_incoming_attach())
            .await
            .unwrap()
            .unwrap();
        let LinkEndpoint::Receiver(receiver) =
            session.accept_attach(attach, 1024 * 1024).await.unwrap()
        else {
            panic!("actual incoming native Receiver");
        };
        assert!(matches!(
            control(&mut peer, CHANNEL).await,
            Performative::Attach(_)
        ));
        assert!(matches!(
            control(&mut peer, CHANNEL).await,
            Performative::Flow(_)
        ));
        Self {
            connection,
            _session: session,
            peer,
            receiver: Some(receiver),
            next_channel: 2,
        }
    }

    async fn send(&mut self, id: u32, message: Message, message_format: u32) {
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Transfer(Transfer {
                    handle: HANDLE,
                    delivery_id: Some(id),
                    delivery_tag: Some(vec![id as u8].into()),
                    message_format: Some(message_format),
                    settled: Some(false),
                    more: false,
                    rcv_settle_mode: None,
                    state: None,
                    resume: false,
                    aborted: false,
                    batchable: false,
                }),
                encode_message(&message).unwrap(),
            ),
        )
        .await
        .unwrap();
    }

    async fn disposition(&mut self, id: u32) -> Disposition {
        let Performative::Disposition(disposition) = control(&mut self.peer, CHANNEL).await else {
            panic!("original delivery disposition");
        };
        assert_eq!(disposition.role, Role::Receiver);
        assert_eq!(disposition.first, id);
        assert!(disposition.settled);
        disposition
    }

    async fn detach(&mut self) -> Detach {
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Detach(Detach {
                    handle: HANDLE,
                    closed: true,
                    error: None,
                }),
                Vec::new(),
            ),
        )
        .await
        .unwrap();
        let Performative::Detach(detach) = control(&mut self.peer, CHANNEL).await else {
            panic!("processed Detach without a preceding late disposition");
        };
        detach
    }

    async fn no_ack_barrier(&mut self) {
        let channel = self.next_channel;
        self.next_channel += 1;
        write_frame(
            &mut self.peer,
            &frame(channel, Performative::Begin(Begin::default()), Vec::new()),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, self.connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let _session = timeout(WAIT, self.connection.accept_session(incoming))
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                control(&mut self.peer, channel).await,
                Performative::Begin(_)
            ),
            "actual command FIFO/wire response, with no earlier disposition"
        );
    }

    async fn stop(&mut self) {
        self.connection.stop();
        timeout(WAIT, self.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}

fn messages(batch: bool) -> (Message, u32) {
    let children: Vec<_> = (0..if batch { 2 } else { 1 })
        .map(|index| {
            let marker = format!("ingress-{index}");
            Message::builder()
                .properties(Properties {
                    message_id: Some(marker.clone().into()),
                    ..Properties::default()
                })
                .body(Body::Data(vec![marker.into_bytes().into()]))
                .build()
        })
        .collect();
    if batch {
        (
            Message::builder()
                .body(Body::Data(
                    children
                        .iter()
                        .map(|child| encode_message(child).unwrap().into())
                        .collect(),
                ))
                .build(),
            SERVICE_BUS_BATCH_MESSAGE_FORMAT,
        )
    } else {
        (children.into_iter().next().unwrap(), 0)
    }
}

fn counters(actor: &Actor) -> QueueCounters {
    actor
        .store()
        .get(&domain::keys::queue_counters(
            &actor.namespace,
            &actor.entity,
        ))
        .unwrap()
        .map(|bytes| domain::codec::decode(&bytes).unwrap())
        .unwrap_or_default()
}

fn assert_stored(actor: &Actor, count: u64) {
    let machine = StateMachine::new(actor.store().clone());
    for index in 0..count {
        let record = machine
            .message(
                &actor.namespace,
                &actor.entity,
                SequenceNumber::new(index + 1),
            )
            .unwrap()
            .unwrap();
        assert_eq!(record.message_id, format!("ingress-{index}"));
        assert_eq!(record.body, format!("ingress-{index}").as_bytes());
        assert_eq!(record.state, MessageState::Ready);
        assert_eq!(record.enqueued_at, machine.last_applied_time().unwrap());
        assert_eq!(record.delivery_count, 0);
    }
    assert!(
        machine
            .message(
                &actor.namespace,
                &actor.entity,
                SequenceNumber::new(count + 1)
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(counters(actor).next_sequence, count + 1);
    assert_eq!(counters(actor).next_lock_token, 1);
}

fn assert_one_submit(actor: &Actor, batch: bool) {
    let log = actor.log.lock().unwrap();
    assert_eq!(log.len(), 2, "one entry and one returned original result");
    assert!(!log[0].returned && log[1].returned);
    assert_eq!(log[0].kind, log[1].kind);
    assert_eq!(matches!(log[0].kind, CommandKind::SendBatch { .. }), batch);
    assert!(matches!(
        log[0].kind,
        CommandKind::Send { .. } | CommandKind::SendBatch { .. }
    ));
}

fn authorization() -> LinkAuthorization {
    let rule = SharedAccessRule::new(
        "send",
        ResourceScope::namespace("tenant.servicebus.windows.net").unwrap(),
        SharedAccessKey::new("secret").unwrap(),
        None,
        PermissionSet::SEND,
    )
    .unwrap();
    let policy = SharedAccessPolicy::new([rule]).unwrap();
    let grant = policy.authenticate_plain("send", "secret").unwrap();
    LinkAuthorization {
        connection: ConnectionAuthorization::new(
            SharedAccessAuthentication::new(policy, "tenant.servicebus.windows.net").unwrap(),
            Some(grant),
        ),
        resource: ResourceScope::entity("tenant.servicebus.windows.net", "orders").unwrap(),
        permission: Permission::Send,
    }
}

async fn expire(authorization: &LinkAuthorization) {
    let resource = "amqps%3A%2F%2Ftenant.servicebus.windows.net";
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 2;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD
        .encode(mac.finalize().into_bytes())
        .replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D");
    authorization
        .connection
        .validate_and_add(
            &format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn=send"),
            "amqps://tenant.servicebus.windows.net",
        )
        .await
        .unwrap();
    assert!(authorization.ensure().await.is_ok());
    timeout(WAIT, authorization.wait_until_unauthorized())
        .await
        .expect("positive actual authorization expiry");
    assert!(authorization.ensure().await.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_send_and_batch_retirement_drains_one_hidden_result_and_reopens() {
    tokio::spawn(async move {
        for durable in [false, true] {
            for batch in [false, true] {
                for after in [false, true] {
                    for auth in [false, true] {
                        let mut actor = Actor::new(durable, false);
                        let before = actor.store().snapshot().unwrap();
                        actor.clock.set(2_000);
                        actor.gate.arm_put(actor.key(SequenceNumber::new(1)));
                        let authorization = auth.then(authorization);
                        let mut wire = IngressWire::new().await;
                        let receiver = wire.receiver.take().unwrap();
                        let retirement = Arc::new(Notify::new());
                        let mut pump = tokio::spawn(SEND_INTAKE_RETIREMENT.scope(
                            Arc::clone(&retirement),
                            serve_sending_client(
                                receiver,
                                actor.namespace.clone(),
                                actor.entity.clone(),
                                actor.broker.as_ref().unwrap().clone(),
                                authorization.clone(),
                            ),
                        ));
                        let (message, format) = messages(batch);
                        wire.send(0, message, format).await;
                        actor.gate.reached(false).await;
                        if after {
                            actor.gate.release(false);
                            actor.gate.reached(true).await;
                        }
                        assert_eq!(actor.store().snapshot().unwrap() == before, !after);
                        assert_eq!(
                            StateMachine::new(actor.store().clone())
                                .last_applied_time()
                                .unwrap()
                                .as_millis(),
                            if after { 2_000 } else { 1_000 }
                        );
                        wire.no_ack_barrier().await;
                        if let Some(authorization) = authorization.as_ref() {
                            expire(authorization).await;
                        } else {
                            assert!(wire.detach().await.error.is_none());
                        }
                        timeout(WAIT, retirement.notified())
                            .await
                            .expect("actual pump selected retirement before result release");
                        if let Some(authorization) = authorization.as_ref() {
                            assert!(authorization.ensure().await.is_err());
                        }
                        pending_once(std::pin::Pin::new(&mut pump)).await;
                        {
                            let log = actor.log.lock().unwrap();
                            assert_eq!(log.len(), 1);
                            assert!(!log[0].returned);
                        }
                        wire.no_ack_barrier().await;
                        actor.gate.release_all();
                        timeout(WAIT, &mut pump).await.unwrap().unwrap().unwrap();
                        assert_one_submit(&actor, batch);
                        assert_eq!(
                            StateMachine::new(actor.store().clone())
                                .last_applied_time()
                                .unwrap()
                                .as_millis(),
                            2_000
                        );
                        if auth {
                            let detach = match control(&mut wire.peer, CHANNEL).await {
                                Performative::Detach(detach) => detach,
                                other => panic!(
                                    "auth retirement without late acknowledgement: {other:?}"
                                ),
                            };
                            assert_eq!(
                                detach.error.unwrap().condition,
                                AmqpError::UnauthorizedAccess.into()
                            );
                        }
                        wire.no_ack_barrier().await;
                        assert_stored(&actor, if batch { 2 } else { 1 });
                        let committed = actor.store().snapshot().unwrap();
                        wire.stop().await;
                        actor.reopen();
                        assert_eq!(actor.store().snapshot().unwrap(), committed);
                        assert_eq!(
                            StateMachine::new(actor.store().clone())
                                .last_applied_time()
                                .unwrap()
                                .as_millis(),
                            2_000
                        );
                        assert_stored(&actor, if batch { 2 } else { 1 });
                    }
                }
            }
        }
    })
    .await
    .expect("test observer joined");
}

fn submission(
    actor: &Actor,
    batch: bool,
) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send + 'static {
    let broker = actor.broker.as_ref().unwrap().clone();
    let namespace = actor.namespace.clone();
    let entity = actor.entity.clone();
    let command = if batch {
        CommandKind::SendBatch {
            messages: vec![domain::MessageInput {
                message_id: "owner".to_owned(),
                body: vec![1],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            }],
        }
    } else {
        CommandKind::Send {
            message_id: "owner".to_owned(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        }
    };
    async move { broker.submit(namespace, entity, command).await }
}

#[tokio::test(flavor = "current_thread")]
async fn original_send_cache_survives_cancelled_borrowed_finish_without_resubmission() {
    tokio::spawn(async move {
        for durable in [false, true] {
            for batch in [false, true] {
                let mut actor = Actor::new(durable, false);
                actor.gate.arm_put(actor.key(SequenceNumber::new(1)));
                let mut original = SendIntake::new(submission(&actor, batch));
                assert!(!original.started());
                let mut observed = Box::pin(original.observe());
                pending_once(observed.as_mut()).await;
                drop(observed);
                actor.gate.reached(false).await;
                assert!(original.started());
                let mut finished = Box::pin(original.finish());
                pending_once(finished.as_mut()).await;
                drop(finished);
                assert!(original.take_packet().is_none());
                actor.gate.release_all();
                let result = timeout(WAIT, original.finish())
                    .await
                    .unwrap()
                    .unwrap()
                    .clone();
                let expected = if batch {
                    CommandOutcome::BatchSent {
                        sequences: vec![SequenceNumber::new(1)],
                        stored: 1,
                    }
                } else {
                    CommandOutcome::Sent {
                        sequence: SequenceNumber::new(1),
                    }
                };
                assert_eq!(result, Ok(expected));
                assert_eq!(original.observe().await, Some(&result));
                assert_eq!(original.finish().await, Some(&result));
                let packet = original.take_packet().unwrap();
                assert!(packet.started && packet.retired);
                assert_eq!(packet.result, Some(result));
                assert!(original.take_packet().is_none());
                assert_one_submit(&actor, batch);
                let committed = actor.store().snapshot().unwrap();
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), committed);
            }
        }
    })
    .await
    .expect("test observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn retired_unpolled_send_and_original_refusal_remain_distinct() {
    tokio::spawn(async move {
        for batch in [false, true] {
            let actor = Actor::new(false, true);
            let before = actor.store().snapshot().unwrap();
            let mut unpolled = SendIntake::new(submission(&actor, batch));
            unpolled.retire();
            assert!(!unpolled.started());
            assert!(unpolled.observe().await.is_none());
            assert!(unpolled.finish().await.is_none());
            let packet = unpolled.take_packet().unwrap();
            assert!(!packet.started && packet.retired && packet.result.is_none());
            assert!(actor.log.lock().unwrap().is_empty());
            assert_eq!(actor.store().snapshot().unwrap(), before);
            let mut original = SendIntake::new(submission(&actor, batch));
            let refusal = Err(BrokerRejection::Refused(BrokerError::SessionRequired));
            assert_eq!(original.observe().await, Some(&refusal));
            assert_eq!(original.finish().await, Some(&refusal));
            let packet = original.take_packet().unwrap();
            assert!(packet.started && packet.retired);
            assert_eq!(packet.result, Some(refusal));
            assert_one_submit(&actor, batch);
            assert_eq!(actor.store().snapshot().unwrap(), before);
        }
    })
    .await
    .expect("test observer joined");
}

#[derive(Clone)]
enum TestBroker {
    Actual(ActualBroker),
    Unexpected(Arc<AtomicUsize>),
}

impl Broker for TestBroker {
    async fn submit(
        &self,
        namespace: domain::NamespaceName,
        entity: domain::EntityPath,
        command: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        match self {
            Self::Actual(broker) => broker.submit(namespace, entity, command).await,
            Self::Unexpected(calls) => {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(CommandOutcome::QueueCreated) // Qualified raw-shape control, not an actual broker outcome.
            }
        }
    }
    fn deliverable(
        &self,
        _: &domain::NamespaceName,
        _: &domain::EntityPath,
    ) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn healthy_refusal_and_conversion_paths_keep_wire_priority_and_link_usable() {
    tokio::spawn(async move {
        // 0/1: healthy; 2/3: actual refusal; 4/5: conversion; 6: synthetic raw shape.
        for case in 0..7 {
            let mut actor = Actor::new(false, matches!(case, 2 | 3));
            let before = actor.store().snapshot().unwrap();
            let synthetic = Arc::new(AtomicUsize::new(0));
            let broker = if case == 6 {
                TestBroker::Unexpected(Arc::clone(&synthetic))
            } else {
                TestBroker::Actual(actor.broker.as_ref().unwrap().clone())
            };
            let mut wire = IngressWire::new().await;
            let receiver = wire.receiver.take().unwrap();
            let pump = tokio::spawn(serve_sending_client(
                receiver,
                actor.namespace.clone(),
                actor.entity.clone(),
                broker,
                None,
            ));
            let batch = matches!(case, 1 | 3 | 4);
            let (mut message, mut format) = messages(batch);
            if case == 4 {
                message = Message::builder()
                    .body(Body::Data(vec![vec![255].into()]))
                    .build();
            }
            if case == 5 {
                format = 7;
            }
            wire.send(0, message, format).await;
            let disposition = wire.disposition(0).await;
            if case <= 1 {
                assert!(matches!(
                    disposition.state,
                    Some(DeliveryState::Accepted(_))
                ));
                assert_one_submit(&actor, batch);
                assert_stored(&actor, if batch { 2 } else { 1 });
            } else {
                let Some(DeliveryState::Rejected(rejected)) = disposition.state else {
                    panic!("precise refusal");
                };
                let expected = match case {
                    2 | 3 => AmqpError::NotAllowed.into(),
                    4 | 5 => AmqpError::InvalidField.into(),
                    _ => AmqpError::InternalError.into(),
                };
                assert_eq!(rejected.error.unwrap().condition, expected);
                assert_stored(&actor, 0);
                assert_eq!(actor.store().snapshot().unwrap(), before);
                if case <= 3 {
                    assert_one_submit(&actor, batch);
                } else {
                    assert!(actor.log.lock().unwrap().is_empty());
                }
                if matches!(case, 4 | 5) {
                    let (message, format) = messages(false);
                    wire.send(1, message, format).await;
                    assert!(matches!(
                        wire.disposition(1).await.state,
                        Some(DeliveryState::Accepted(_))
                    ));
                    assert_one_submit(&actor, false);
                    assert_stored(&actor, 1);
                }
                assert_eq!(synthetic.load(Ordering::SeqCst), usize::from(case == 6));
            }
            wire.no_ack_barrier().await;
            wire.detach().await;
            timeout(WAIT, pump).await.unwrap().unwrap().unwrap();
            let committed = actor.store().snapshot().unwrap();
            wire.stop().await;
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), committed);
        }
    })
    .await
    .expect("test observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn observed_retirement_before_helper_poll_precedes_conversion_and_submission() {
    tokio::spawn(async move {
        for auth in [false, true] {
            for invalid in [false, true] {
                let actor = Actor::new(false, false);
                let before = actor.store().snapshot().unwrap();
                let mut wire = IngressWire::new().await;
                let authorization = auth.then(authorization);
                let receiver = wire.receiver.take().unwrap();
                let (message, format) = messages(false);
                wire.send(0, message, if invalid { 7 } else { format })
                    .await;
                wire.no_ack_barrier().await;
                if let Some(authorization) = authorization.as_ref() {
                    expire(authorization).await;
                } else {
                    wire.detach().await;
                }
                let pump = tokio::spawn(serve_sending_client(
                    receiver,
                    actor.namespace.clone(),
                    actor.entity.clone(),
                    actor.broker.as_ref().unwrap().clone(),
                    authorization,
                ));
                timeout(WAIT, pump).await.unwrap().unwrap().unwrap();
                if auth {
                    let Performative::Detach(detach) = control(&mut wire.peer, CHANNEL).await
                    else {
                        panic!("authorization outranks queued conversion");
                    };
                    assert_eq!(
                        detach.error.unwrap().condition,
                        AmqpError::UnauthorizedAccess.into()
                    );
                }
                wire.no_ack_barrier().await;
                assert!(actor.log.lock().unwrap().is_empty());
                assert_eq!(actor.store().snapshot().unwrap(), before);
                wire.stop().await;
            }
        }
    })
    .await
    .expect("test observer joined");
}

#[derive(Clone)]
struct EagerBroker {
    actual: ActualBroker,
    calls: Arc<AtomicUsize>,
    bodies: Arc<AtomicUsize>,
}

impl Broker for EagerBroker {
    fn submit(
        &self,
        namespace: domain::NamespaceName,
        entity: domain::EntityPath,
        command: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let actual = self.actual.clone();
        let bodies = Arc::clone(&self.bodies);
        async move {
            bodies.fetch_add(1, Ordering::SeqCst);
            actual.submit(namespace, entity, command).await
        }
    }
    fn deliverable(
        &self,
        _: &domain::NamespaceName,
        _: &domain::EntityPath,
    ) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn eager_submit_invocation_is_distinct_from_poll_and_retired_work_invokes_neither() {
    tokio::spawn(async move {
        for retired in [false, true] {
            let actor = Actor::new(false, false);
            let calls = Arc::new(AtomicUsize::new(0));
            let bodies = Arc::new(AtomicUsize::new(0));
            let broker = EagerBroker {
                actual: actor.broker.as_ref().unwrap().clone(),
                calls: Arc::clone(&calls),
                bodies: Arc::clone(&bodies),
            };
            let mut wire = IngressWire::new().await;
            let receiver = wire.receiver.take().unwrap();
            let (message, format) = messages(false);
            wire.send(0, message, format).await;
            wire.no_ack_barrier().await;
            if retired {
                wire.detach().await;
            }
            let pump = tokio::spawn(serve_sending_client(
                receiver,
                actor.namespace.clone(),
                actor.entity.clone(),
                broker.clone(),
                None,
            ));
            if !retired {
                assert!(matches!(
                    wire.disposition(0).await.state,
                    Some(DeliveryState::Accepted(_))
                ));
                wire.detach().await;
            }
            timeout(WAIT, pump).await.unwrap().unwrap().unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(!retired));
            assert_eq!(bodies.load(Ordering::SeqCst), usize::from(!retired));
            wire.no_ack_barrier().await;
            if retired {
                assert!(actor.log.lock().unwrap().is_empty());
                assert_stored(&actor, 0);
            } else {
                assert_one_submit(&actor, false);
                assert_stored(&actor, 1);
            }
            wire.stop().await;

            // Qualified owner-wrapper control: no production phase hook is implied.
            let before_calls = calls.load(Ordering::SeqCst);
            let before_bodies = bodies.load(Ordering::SeqCst);
            let mut unpolled = SendIntake::new(async {
                broker
                    .submit(
                        actor.namespace.clone(),
                        actor.entity.clone(),
                        CommandKind::Send {
                            message_id: "unpolled".to_owned(),
                            body: vec![1],
                            time_to_live_millis: None,
                            session_id: None,
                            scheduled_enqueue_at: None,
                            envelope: None,
                        },
                    )
                    .await
            });
            unpolled.retire();
            assert!(unpolled.finish().await.is_none());
            assert!(unpolled.take_packet().unwrap().result.is_none());
            assert_eq!(calls.load(Ordering::SeqCst), before_calls);
            assert_eq!(bodies.load(Ordering::SeqCst), before_bodies);
        }
    })
    .await
    .expect("test observer joined");
}
