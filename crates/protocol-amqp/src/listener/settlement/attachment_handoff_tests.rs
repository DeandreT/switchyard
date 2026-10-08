//! Actual broker grants and original native acceptance across session retirement.
//! Submission logs use one test-observer task, not production grant tasks.

use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use amqp::{
    Attach, Begin, Detach, End, EngineError, Frame, LinkEndpoint, Open, Performative,
    ProtocolHeader, ReceiverSettleMode, Role, SenderSettleMode, ServerConnection, ServerSession,
    Source, Target, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{
    AcceptedSession, BrokerError, CommandKind, CommandOutcome, EntityPath, NamespaceName,
    QueueCounters, SessionHold, SessionId, StateMachine,
};
use serde_amqp::{Value, primitives::Symbol};
use storage::StateStore;
use tokio::{
    io::{DuplexStream, duplex},
    time::timeout,
};

use super::test_support::{Actor, ActualBroker, WAIT, pending_once};
use crate::authorization::ConnectionAuthorization;
use crate::listener::attachments::{
    AttachmentHandoff, HandoffPhase, HandoffStep, accept_entity_link,
};
use crate::management::ConnectionManagement;
use crate::{
    Broker, BrokerRejection, SESSION_FILTER, SessionRequest, SharedAccessAuthentication,
    read_session_filter,
};

const CHANNEL: u16 = 1;
const HANDLE: u32 = 1;
const LINK: &str = "exact-handoff";

fn frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

async fn control(peer: &mut DuplexStream, channel: u16) -> Performative {
    let Frame::Amqp {
        channel: actual,
        performative: Some(performative),
        payload,
    } = timeout(WAIT, read_frame(peer)).await.unwrap().unwrap()
    else {
        panic!("actual native control frame");
    };
    assert_eq!(actual, channel);
    assert!(payload.is_empty());
    performative
}

struct PendingWire {
    connection: ServerConnection,
    peer: DuplexStream,
}

impl PendingWire {
    async fn new() -> (Self, ServerSession) {
        let (stream, mut peer) = duplex(64 * 1024);
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept(stream, "handoff-server", None),
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
                        &frame(0, Performative::Open(Open::new("handoff-peer"))),
                    )
                    .await
                    .unwrap();
                    assert!(matches!(control(&mut peer, 0).await, Performative::Open(_)));
                }
            )
        })
        .await
        .unwrap();
        let mut wire = Self {
            connection: connection.unwrap(),
            peer,
        };
        let session = wire.begin(CHANNEL).await;
        (wire, session)
    }

    async fn begin(&mut self, channel: u16) -> ServerSession {
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
        let session = timeout(WAIT, self.connection.accept_session(incoming))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            control(&mut self.peer, channel).await,
            Performative::Begin(_)
        ));
        session
    }

    async fn offer(&mut self, session: &mut ServerSession, filter: Option<Value>) -> Attach {
        self.offer_role(session, filter, Role::Receiver).await
    }

    async fn offer_role(
        &mut self,
        session: &mut ServerSession,
        filter: Option<Value>,
        role: Role,
    ) -> Attach {
        let mut source = Source::new("orders");
        if let Some(filter) = filter {
            source
                .filter
                .get_or_insert_with(Default::default)
                .insert(Symbol::from(SESSION_FILTER), filter);
        }
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Attach(Box::new(Attach {
                    name: LINK.to_owned(),
                    handle: HANDLE,
                    role: role.clone(),
                    snd_settle_mode: SenderSettleMode::Unsettled,
                    rcv_settle_mode: ReceiverSettleMode::First,
                    source: (role == Role::Receiver).then_some(source),
                    target: (role == Role::Sender).then(|| Target::new("orders")),
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
        timeout(WAIT, session.next_incoming_attach())
            .await
            .unwrap()
            .unwrap()
    }

    async fn end(&mut self) {
        write_frame(
            &mut self.peer,
            &frame(CHANNEL, Performative::End(End::default())),
        )
        .await
        .unwrap();
        assert!(matches!(
            control(&mut self.peer, CHANNEL).await,
            Performative::End(_)
        ));
    }

    async fn detach(&mut self) {
        write_frame(
            &mut self.peer,
            &frame(
                CHANNEL,
                Performative::Detach(Detach {
                    handle: HANDLE,
                    closed: true,
                    error: None,
                }),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            control(&mut self.peer, CHANNEL).await,
            Performative::Detach(_)
        ));
    }

    async fn native_fifo_after_accept(&mut self, expects_attach: bool) -> ServerSession {
        // The original acceptance was already queued. Completing this original
        // second-session command proves its reply preceded this FIFO barrier.
        write_frame(
            &mut self.peer,
            &frame(2, Performative::Begin(Begin::default())),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, self.connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let independent = timeout(WAIT, self.connection.accept_session(incoming))
            .await
            .unwrap()
            .unwrap();
        if expects_attach {
            assert!(matches!(
                control(&mut self.peer, CHANNEL).await,
                Performative::Attach(_)
            ));
        }
        assert!(matches!(
            control(&mut self.peer, 2).await,
            Performative::Begin(_)
        ));
        independent
    }

    async fn stop(&mut self) {
        self.connection.stop();
        timeout(WAIT, self.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}

fn session_id() -> SessionId {
    SessionId::new("cart-80").unwrap()
}

fn filter(next: bool) -> Value {
    if next {
        Value::Null
    } else {
        Value::String(session_id().as_str().to_owned())
    }
}

fn accepted(outcome: &CommandOutcome) -> &AcceptedSession {
    let CommandOutcome::SessionAccepted(Some(accepted)) = outcome else {
        panic!("actual session grant");
    };
    accepted
}

fn grant_count(actor: &Actor) -> usize {
    actor
        .log
        .lock()
        .unwrap()
        .iter()
        .filter(|entry| !entry.returned && matches!(entry.kind, CommandKind::AcceptSession { .. }))
        .count()
}

fn release_holds(actor: &Actor) -> Vec<SessionHold> {
    actor
        .log
        .lock()
        .unwrap()
        .iter()
        .filter_map(|entry| match &entry.kind {
            CommandKind::ReleaseSession { session } if !entry.returned => Some(session.clone()),
            _ => None,
        })
        .collect()
}

fn assert_reopened_release(actor: &mut Actor, hold: &SessionHold) {
    let before = actor.store().snapshot().unwrap();
    actor.reopen();
    assert_eq!(actor.store().snapshot().unwrap(), before);
    let machine = StateMachine::new(actor.store().clone());
    let record = machine
        .session(&actor.namespace, &actor.entity, &hold.session_id)
        .unwrap()
        .unwrap();
    assert!(record.lock.is_none());
    assert!(record.state.is_empty());
}

fn wrong_permission() -> Arc<ConnectionAuthorization> {
    let rule = SharedAccessRule::new(
        "send-only",
        ResourceScope::namespace("tenant.servicebus.windows.net").unwrap(),
        SharedAccessKey::new("secret").unwrap(),
        None,
        PermissionSet::SEND,
    )
    .unwrap();
    let policy = SharedAccessPolicy::new([rule]).unwrap();
    let grant = policy.authenticate_plain("send-only", "secret").unwrap();
    ConnectionAuthorization::new(
        SharedAccessAuthentication::new(policy, "tenant.servicebus.windows.net").unwrap(),
        Some(grant),
    )
}

fn counters(actor: &Actor) -> QueueCounters {
    domain::codec::decode(
        &actor
            .store()
            .get(&domain::keys::queue_counters(
                &actor.namespace,
                &actor.entity,
            ))
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

fn stored_grant(actor: &Actor) -> AcceptedSession {
    let record = StateMachine::new(actor.store().clone())
        .session(&actor.namespace, &actor.entity, &session_id())
        .unwrap()
        .unwrap();
    AcceptedSession {
        session_id: session_id(),
        lock: record.lock.unwrap(),
        state: record.state,
    }
}

fn grant_started_and_returned(actor: &Actor) {
    let log = actor.log.lock().unwrap();
    assert_eq!(
        log.iter()
            .filter(
                |entry| !entry.returned && matches!(entry.kind, CommandKind::AcceptSession { .. })
            )
            .count(),
        1
    );
    assert_eq!(
        log.iter()
            .filter(
                |entry| entry.returned && matches!(entry.kind, CommandKind::AcceptSession { .. })
            )
            .count(),
        1
    );
    assert!(!log.iter().any(|entry| matches!(
        entry.kind,
        CommandKind::Receive { .. }
            | CommandKind::Complete { .. }
            | CommandKind::Abandon { .. }
            | CommandKind::Defer { .. }
            | CommandKind::DeadLetter { .. }
    )));
}

type ReleaseResults = Vec<(SessionHold, Result<(), BrokerRejection>)>;

#[derive(Clone)]
struct ReleaseWitness {
    actual: ActualBroker,
    results: Arc<Mutex<ReleaseResults>>,
}

impl ReleaseWitness {
    fn new(actor: &Actor) -> Self {
        Self {
            actual: actor.broker.as_ref().unwrap().clone(),
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Broker for ReleaseWitness {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let hold = match &kind {
            CommandKind::ReleaseSession { session } => Some(session.clone()),
            _ => None,
        };
        let result = self.actual.submit(namespace, entity, kind).await;
        if let Some(hold) = hold {
            self.results
                .lock()
                .unwrap()
                .push((hold, result.as_ref().map(|_| ()).map_err(Clone::clone)));
        }
        result
    }

    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        self.actual.deliverable(namespace, entity)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ended_before_first_helper_poll_never_submits_a_grant() {
    tokio::spawn(async move {
        for durable in [false, true] {
            for next in [false, true] {
                let mut actor = Actor::new(durable, true);
                actor.send("unpolled-handoff", Some(session_id()));
                let before = actor.store().snapshot().unwrap();
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire.offer(&mut session, Some(filter(next))).await;
                let management = ConnectionManagement::new();
                let original = accept_entity_link(
                    &session,
                    actor.broker.as_ref().unwrap(),
                    &actor.namespace,
                    "orders",
                    attach,
                    None,
                    &management,
                );
                wire.end().await;
                assert!(session.is_ended());
                assert!(timeout(WAIT, original).await.unwrap().unwrap().is_none());
                let mut owner = AttachmentHandoff::new(&session);
                owner.begin_grant(actor.broker.as_ref().unwrap().submit(
                    actor.namespace.clone(),
                    actor.entity.clone(),
                    CommandKind::AcceptSession {
                        session_id: (!next).then(session_id),
                        lock_duration_millis: None,
                    },
                ));
                assert!(owner.observe().await.is_none());
                assert!(owner.finish().await.is_none());
                let packet = owner.take_packet().unwrap();
                assert_eq!(packet.phase, HandoffPhase::Grant);
                assert!(!packet.started && packet.retired);
                assert!(packet.accepted.is_none() && packet.step.is_none());
                drop(owner);
                let mut idle = AttachmentHandoff::new(&session);
                assert!(idle.finish().await.is_none());
                let idle_packet = idle.take_packet().unwrap();
                assert_eq!(idle_packet.phase, HandoffPhase::Idle);
                assert!(!idle_packet.started && idle_packet.retired);
                assert!(idle_packet.accepted.is_none() && idle_packet.step.is_none());
                drop(idle);
                assert_eq!(grant_count(&actor), 0);
                assert!(release_holds(&actor).is_empty());
                assert_eq!(actor.store().snapshot().unwrap(), before);
                assert!(management.registered_session(LINK).await.is_none());
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), before);
                wire.stop().await;
            }
        }
    })
    .await
    .expect("original test observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn actual_helper_drains_one_hidden_grant_then_releases_its_exact_hold() {
    tokio::spawn(async move {
        for durable in [false, true] {
            for next in [false, true] {
                for after in [false, true] {
                    let mut actor = Actor::new(durable, true);
                    let sequence = actor.send("hidden-grant", Some(session_id()));
                    let before = actor.store().snapshot().unwrap();
                    let before_counters = counters(&actor);
                    actor.gate.arm_put(domain::keys::session(
                        &actor.namespace,
                        &actor.entity,
                        &session_id(),
                    ));
                    let (mut wire, mut session) = PendingWire::new().await;
                    let attach = wire.offer(&mut session, Some(filter(next))).await;
                    let management = ConnectionManagement::new();
                    let broker = ReleaseWitness::new(&actor);
                    let mut original = Box::pin(accept_entity_link(
                        &session,
                        &broker,
                        &actor.namespace,
                        "orders",
                        attach,
                        None,
                        &management,
                    ));
                    pending_once(original.as_mut()).await;
                    actor.gate.reached(false).await;
                    if after {
                        actor.gate.release(false);
                        actor.gate.reached(true).await;
                        assert_ne!(actor.store().snapshot().unwrap(), before);
                    } else {
                        assert_eq!(actor.store().snapshot().unwrap(), before);
                    }
                    wire.end().await;
                    assert!(session.is_ended());
                    // Cancel only a borrowed observer; the consumed helper and
                    // its original raw broker future remain in this same slot.
                    let mut observer = Box::pin(original.as_mut());
                    pending_once(observer.as_mut()).await;
                    drop(observer);
                    assert_eq!(grant_count(&actor), 1);
                    assert!(release_holds(&actor).is_empty());
                    if !after {
                        actor.gate.release(false);
                        actor.gate.reached(true).await;
                    }
                    let hold = stored_grant(&actor).hold();
                    assert_eq!(
                        counters(&actor).next_lock_token,
                        before_counters.next_lock_token + 1
                    );
                    actor.gate.release(true);
                    assert!(
                        timeout(WAIT, original.as_mut())
                            .await
                            .unwrap()
                            .unwrap()
                            .is_none()
                    );
                    drop(original);
                    grant_started_and_returned(&actor);
                    assert_eq!(release_holds(&actor), vec![hold.clone()]);
                    assert_eq!(
                        *broker.results.lock().unwrap(),
                        vec![(hold.clone(), Ok(()))]
                    );
                    assert!(management.registered_session(LINK).await.is_none());
                    assert_eq!(
                        counters(&actor).next_lock_token,
                        before_counters.next_lock_token + 1
                    );
                    drop(broker);
                    assert_reopened_release(&mut actor, &hold);
                    assert_eq!(
                        StateMachine::new(actor.store().clone())
                            .session_ready_sequences(
                                &actor.namespace,
                                &actor.entity,
                                &hold.session_id,
                                10
                            )
                            .unwrap(),
                        vec![sequence]
                    );
                    wire.stop().await;
                }
            }
        }
    })
    .await
    .expect("original test observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn cached_actual_grant_retires_without_starting_native_acceptance() {
    tokio::spawn(async move {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            let (mut wire, mut session) = PendingWire::new().await;
            let _attach = wire.offer(&mut session, Some(filter(false))).await;
            let mut owner = AttachmentHandoff::new(&session);
            owner.begin_grant(actor.broker.as_ref().unwrap().submit(
                actor.namespace.clone(),
                actor.entity.clone(),
                CommandKind::AcceptSession {
                    session_id: Some(session_id()),
                    lock_duration_millis: None,
                },
            ));
            let Some(HandoffStep::Grant(Ok(outcome))) =
                timeout(WAIT, owner.observe()).await.unwrap()
            else {
                panic!("actual cached grant");
            };
            let expected = accepted(outcome).clone();
            let cached = owner.observe().await.unwrap() as *const HandoffStep as usize;
            assert_eq!(
                owner.observe().await.unwrap() as *const HandoffStep as usize,
                cached
            );
            let Some(HandoffStep::Grant(Ok(outcome))) = owner.take_step() else {
                panic!("same raw grant");
            };
            assert_eq!(accepted(&outcome), &expected);
            owner.remember_session(expected.clone());
            wire.end().await;
            assert!(owner.finish().await.is_none());
            let packet = owner.take_packet().unwrap();
            assert_eq!(packet.phase, HandoffPhase::Grant);
            assert!(packet.started && packet.retired);
            assert!(packet.step.is_none());
            assert_eq!(packet.accepted.as_ref(), Some(&expected));
            assert!(owner.take_packet().is_none());
            drop(owner);
            // This direct owner control explicitly disposes its captured grant;
            // actual consumed-helper cleanup is the separate gated case above.
            super::release_session(
                actor.broker.as_ref().unwrap(),
                &actor.namespace,
                &actor.entity,
                Some(&expected.hold()),
            )
            .await;
            assert_eq!(release_holds(&actor), vec![expected.hold()]);
            assert_reopened_release(&mut actor, &expected.hold());
            wire.stop().await;
        }
    })
    .await
    .expect("original test observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn original_native_success_is_cached_across_cancel_end_and_retry() {
    tokio::spawn(async move {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            let expected = accepted(&actor.intent(CommandKind::AcceptSession {
                session_id: Some(session_id()),
                lock_duration_millis: None,
            }))
            .clone();
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let mut owner = AttachmentHandoff::new(&session);
            owner.remember_session(expected.clone());
            owner.begin_accept(session.accept_attach_with_properties(attach, 1024 * 1024, None));
            let mut observer = Box::pin(owner.observe());
            pending_once(observer.as_mut()).await;
            drop(observer);
            let independent = wire.native_fifo_after_accept(true).await;
            wire.end().await;
            assert!(session.is_ended());
            assert!(!independent.is_ended());
            let first = owner.finish().await.unwrap() as *const HandoffStep as usize;
            assert!(matches!(
                owner.finish().await,
                Some(HandoffStep::Native(Ok(LinkEndpoint::Sender(_))))
            ));
            assert_eq!(
                owner.finish().await.unwrap() as *const HandoffStep as usize,
                first
            );
            let packet = owner.take_packet().unwrap();
            assert_eq!(packet.phase, HandoffPhase::Native);
            assert!(packet.started && packet.retired);
            assert_eq!(packet.accepted.as_ref(), Some(&expected));
            let Some(HandoffStep::Native(Ok(LinkEndpoint::Sender(mut sender)))) = packet.step
            else {
                panic!("same original endpoint");
            };
            timeout(WAIT, sender.on_detach()).await.unwrap();
            assert!(owner.take_packet().is_none());
            drop(owner);
            super::release_session(
                actor.broker.as_ref().unwrap(),
                &actor.namespace,
                &actor.entity,
                Some(&expected.hold()),
            )
            .await;
            assert_eq!(release_holds(&actor), vec![expected.hold()]);
            assert_reopened_release(&mut actor, &expected.hold());
            wire.stop().await;
        }
    })
    .await
    .expect("original test observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn original_native_error_is_cached_without_recreating_acceptance() {
    tokio::spawn(async move {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            let expected = accepted(&actor.intent(CommandKind::AcceptSession {
                session_id: Some(session_id()),
                lock_duration_millis: None,
            }))
            .clone();
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            wire.detach().await;
            assert!(!session.is_ended());
            let mut owner = AttachmentHandoff::new(&session);
            owner.remember_session(expected.clone());
            owner.begin_accept(session.accept_attach_with_properties(attach, 1024 * 1024, None));
            let mut observer = Box::pin(owner.observe());
            pending_once(observer.as_mut()).await;
            drop(observer);
            let independent = wire.native_fifo_after_accept(false).await;
            assert!(!independent.is_ended());
            let first = owner.finish().await.unwrap() as *const HandoffStep as usize;
            assert!(matches!(
                owner.finish().await,
                Some(HandoffStep::Native(Err(EngineError::RemoteDetached)))
            ));
            assert_eq!(
                owner.finish().await.unwrap() as *const HandoffStep as usize,
                first
            );
            let packet = owner.take_packet().unwrap();
            assert_eq!(packet.phase, HandoffPhase::Native);
            assert!(packet.started && packet.retired);
            assert_eq!(packet.accepted.as_ref(), Some(&expected));
            assert!(matches!(
                packet.step,
                Some(HandoffStep::Native(Err(EngineError::RemoteDetached)))
            ));
            assert!(owner.take_packet().is_none());
            drop(owner);
            super::release_session(
                actor.broker.as_ref().unwrap(),
                &actor.namespace,
                &actor.entity,
                Some(&expected.hold()),
            )
            .await;
            assert_eq!(release_holds(&actor), vec![expected.hold()]);
            assert_reopened_release(&mut actor, &expected.hold());
            wire.stop().await;
        }
    })
    .await
    .expect("original test observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn late_helper_release_refusal_preserves_same_entity_replacement_and_registry() {
    tokio::spawn(async move {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.gate.arm_put(domain::keys::session(&actor.namespace, &actor.entity, &session_id()));
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let mut original = Box::pin(accept_entity_link(&session, &broker, &actor.namespace, "orders", attach, None, &management));
            pending_once(original.as_mut()).await;
            actor.gate.reached(false).await;
            actor.gate.release(false);
            actor.gate.reached(true).await;
            let old = stored_grant(&actor);
            actor.gate.release(true);
            // An actual queued command on the same broker is a positive apply/
            // reply barrier while the original protocol observer stays unpolled.
            actor.intent(CommandKind::GetSessionState { session: old.hold() });
            wire.end().await;
            actor.clock.set(old.lock.locked_until.as_millis());
            let replacement = accepted(&actor.intent(CommandKind::AcceptSession { session_id: Some(session_id()), lock_duration_millis: None })).clone();
            assert_ne!(old.hold().token, replacement.hold().token);
            management.register_session(LINK, actor.entity.clone(), replacement.hold()).await;
            assert!(timeout(WAIT, original.as_mut()).await.unwrap().unwrap().is_none());
            drop(original);
            assert_eq!(release_holds(&actor), vec![old.hold()]);
            {
                let results = broker.results.lock().unwrap();
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].0, old.hold());
                assert!(matches!(&results[0].1, Err(BrokerRejection::Refused(BrokerError::SessionLockNotHeld { session_id: actual })) if actual == &session_id()));
            }
            let registered = Some((actor.entity.clone(), replacement.hold()));
            assert_eq!(management.registered_session(LINK).await, registered);
            // Exercise the existing same-entity full-hold comparison explicitly;
            // this is not a late-register or cross-entity aliasing guarantee.
            management.unregister_session(LINK, &old.hold()).await;
            assert_eq!(management.registered_session(LINK).await, registered);
            assert_eq!(stored_grant(&actor), replacement);
            drop(broker);
            let before_reopen = actor.store().snapshot().unwrap();
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before_reopen);
            assert_eq!(stored_grant(&actor), replacement);
            assert_eq!(management.registered_session(LINK).await, registered);
            wire.stop().await;
        }
    }).await.expect("original test observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn healthy_named_and_next_available_handoffs_echo_and_register_the_exact_grant() {
    tokio::spawn(async move {
        for durable in [false, true] {
            for next in [false, true] {
                let mut actor = Actor::new(durable, true);
                actor.send("healthy-handoff", Some(session_id()));
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire.offer(&mut session, Some(filter(next))).await;
                let management = ConnectionManagement::new();
                let link = timeout(
                    WAIT,
                    accept_entity_link(
                        &session,
                        actor.broker.as_ref().unwrap(),
                        &actor.namespace,
                        "orders",
                        attach,
                        None,
                        &management,
                    ),
                )
                .await
                .unwrap()
                .unwrap()
                .unwrap();
                let expected = link.accepted.as_ref().unwrap().clone();
                assert_eq!(expected.session_id, session_id());
                assert_eq!(link.entity, actor.entity);
                assert!(link.authorization.is_none());
                assert_eq!(link.mode, domain::ReceiveMode::PeekLock);
                assert_eq!(
                    management.registered_session(LINK).await,
                    Some((actor.entity.clone(), expected.hold()))
                );
                let Performative::Attach(response) = control(&mut wire.peer, CHANNEL).await else {
                    panic!("actual echoed Attach");
                };
                assert_eq!(
                    read_session_filter(response.source.as_ref()).unwrap(),
                    SessionRequest::Named(session_id())
                );
                let ticks = 621_355_968_000_000_000_i64
                    + i64::try_from(expected.lock.locked_until.as_millis()).unwrap() * 10_000;
                assert_eq!(
                    response
                        .properties
                        .as_ref()
                        .unwrap()
                        .get(&Symbol::from("com.microsoft:locked-until-utc")),
                    Some(&Value::Long(ticks))
                );
                grant_started_and_returned(&actor);
                wire.end().await;
                let LinkEndpoint::Sender(mut sender) = link.endpoint else {
                    panic!("actual receiving-client sender");
                };
                timeout(WAIT, sender.on_detach()).await.unwrap();
                management.unregister_session(LINK, &expected.hold()).await;
                super::release_session(
                    actor.broker.as_ref().unwrap(),
                    &actor.namespace,
                    &actor.entity,
                    Some(&expected.hold()),
                )
                .await;
                assert_reopened_release(&mut actor, &expected.hold());
                wire.stop().await;
            }
        }
    })
    .await
    .expect("original test observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_and_refused_links_keep_existing_plan_precedence_and_wire_conditions() {
    tokio::spawn(async move {
        for durable in [false, true] {
            for role in [Role::Sender, Role::Receiver] {
                let actor = Actor::new(durable, false);
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire.offer_role(&mut session, None, role.clone()).await;
                let management = ConnectionManagement::new();
                let link = timeout(
                    WAIT,
                    accept_entity_link(
                        &session,
                        actor.broker.as_ref().unwrap(),
                        &actor.namespace,
                        "orders",
                        attach,
                        None,
                        &management,
                    ),
                )
                .await
                .unwrap()
                .unwrap()
                .unwrap();
                assert!(link.accepted.is_none());
                assert_eq!(grant_count(&actor), 0);
                assert!(release_holds(&actor).is_empty());
                assert!(matches!(
                    control(&mut wire.peer, CHANNEL).await,
                    Performative::Attach(_)
                ));
                if role == Role::Sender {
                    assert!(matches!(
                        control(&mut wire.peer, CHANNEL).await,
                        Performative::Flow(_)
                    ));
                }
                wire.end().await;
                drop(link);
                wire.stop().await;
            }
            for unauthorized in [false, true] {
                let actor = Actor::new(durable, true);
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire.offer(&mut session, Some(Value::Long(7))).await;
                let authorization = unauthorized.then(wrong_permission);
                let management = ConnectionManagement::new();
                assert!(
                    timeout(
                        WAIT,
                        accept_entity_link(
                            &session,
                            actor.broker.as_ref().unwrap(),
                            &actor.namespace,
                            "orders",
                            attach,
                            authorization.as_ref(),
                            &management
                        )
                    )
                    .await
                    .unwrap()
                    .unwrap()
                    .is_none()
                );
                assert!(matches!(
                    control(&mut wire.peer, CHANNEL).await,
                    Performative::Attach(_)
                ));
                let Performative::Detach(detach) = control(&mut wire.peer, CHANNEL).await else {
                    panic!("actual link-scoped refusal");
                };
                let expected = if unauthorized {
                    "amqp:unauthorized-access"
                } else {
                    "amqp:invalid-field"
                };
                assert_eq!(
                    detach.error.unwrap().condition.as_symbol(),
                    Symbol::from(expected)
                );
                assert_eq!(grant_count(&actor), 0);
                assert!(release_holds(&actor).is_empty());
                assert!(!session.is_ended());
                wire.stop().await;
            }
            for held in [false, true] {
                let actor = Actor::new(durable, true);
                let existing = held.then(|| {
                    accepted(&actor.intent(CommandKind::AcceptSession {
                        session_id: Some(session_id()),
                        lock_duration_millis: None,
                    }))
                    .clone()
                });
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire.offer(&mut session, Some(filter(!held))).await;
                let management = ConnectionManagement::new();
                assert!(
                    timeout(
                        WAIT,
                        accept_entity_link(
                            &session,
                            actor.broker.as_ref().unwrap(),
                            &actor.namespace,
                            "orders",
                            attach,
                            None,
                            &management
                        )
                    )
                    .await
                    .unwrap()
                    .unwrap()
                    .is_none()
                );
                assert!(matches!(
                    control(&mut wire.peer, CHANNEL).await,
                    Performative::Attach(_)
                ));
                let Performative::Detach(detach) = control(&mut wire.peer, CHANNEL).await else {
                    panic!("actual broker refusal");
                };
                assert_eq!(
                    detach.error.unwrap().condition.as_symbol(),
                    Symbol::from(if held {
                        crate::SESSION_CANNOT_BE_LOCKED
                    } else {
                        crate::TIMEOUT
                    })
                );
                assert_eq!(grant_count(&actor), 1);
                assert!(release_holds(&actor).is_empty());
                if let Some(existing) = existing {
                    assert_eq!(stored_grant(&actor), existing);
                }
                assert!(!session.is_ended());
                wire.stop().await;
            }
        }
    })
    .await
    .expect("original test observer joined");
}

#[derive(Clone)]
struct UnexpectedGrantShape;

impl Broker for UnexpectedGrantShape {
    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        assert!(matches!(kind, CommandKind::AcceptSession { .. }));
        // Explicit shape control only: the real domain broker does not return
        // QueueCreated for AcceptSession. Native acceptance remains real.
        Ok(CommandOutcome::QueueCreated)
    }

    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unexpected_grant_shape_is_a_qualified_internal_error_not_no_session() {
    let (mut wire, mut session) = PendingWire::new().await;
    let attach = wire.offer(&mut session, Some(filter(false))).await;
    let management = ConnectionManagement::new();
    assert!(
        timeout(
            WAIT,
            accept_entity_link(
                &session,
                &UnexpectedGrantShape,
                &NamespaceName::new("tenant").unwrap(),
                "orders",
                attach,
                None,
                &management
            )
        )
        .await
        .unwrap()
        .unwrap()
        .is_none()
    );
    assert!(matches!(
        control(&mut wire.peer, CHANNEL).await,
        Performative::Attach(_)
    ));
    let Performative::Detach(detach) = control(&mut wire.peer, CHANNEL).await else {
        panic!("actual shape-refusal Detach");
    };
    let error = detach.error.unwrap();
    assert_eq!(
        error.condition.as_symbol(),
        Symbol::from("amqp:internal-error")
    );
    assert!(error.description.unwrap().contains("unexpected outcome"));
    assert!(management.registered_session(LINK).await.is_none());
    assert!(!session.is_ended());
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_helper_native_result_retirement() {
    tokio::spawn(async move {
        for durable in [false, true] {
            for native_error in [false, true] {
                let mut actor = Actor::new(durable, true);
                actor.send("native-phase", Some(session_id()));
                let before_counters = counters(&actor);
                actor.gate.arm_put(domain::keys::session(
                    &actor.namespace,
                    &actor.entity,
                    &session_id(),
                ));
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire.offer(&mut session, Some(filter(false))).await;
                let management = ConnectionManagement::new();
                let broker = ReleaseWitness::new(&actor);
                let mut original = Box::pin(accept_entity_link(
                    &session,
                    &broker,
                    &actor.namespace,
                    "orders",
                    attach,
                    None,
                    &management,
                ));
                pending_once(original.as_mut()).await;
                actor.gate.reached(false).await;
                actor.gate.release(false);
                actor.gate.reached(true).await;
                let expected = stored_grant(&actor);
                actor.gate.release(true);
                // The actual broker owner completes this later command only
                // after its original grant reply was delivered to the cache.
                actor.intent(CommandKind::GetSessionState {
                    session: expected.hold(),
                });
                assert_eq!(grant_count(&actor), 1);
                assert!(release_holds(&actor).is_empty());
                if native_error {
                    wire.detach().await;
                }
                assert!(!session.is_ended());
                // This single unconstrained poll consumes the ready broker
                // result and queues original native acceptance. No engine yield
                // occurs before this borrowed observer is dropped.
                let mut observer = Box::pin(original.as_mut());
                pending_once(observer.as_mut()).await;
                drop(observer);
                grant_started_and_returned(&actor);
                let independent = wire.native_fifo_after_accept(!native_error).await;
                // The native FIFO reply proves success/error is cached before
                // End, not merely that answering Attach bytes were written.
                wire.end().await;
                assert!(session.is_ended());
                assert!(!independent.is_ended());
                assert!(
                    timeout(WAIT, original.as_mut())
                        .await
                        .unwrap()
                        .unwrap()
                        .is_none()
                );
                drop(original);
                grant_started_and_returned(&actor);
                assert_eq!(release_holds(&actor), vec![expected.hold()]);
                assert_eq!(
                    *broker.results.lock().unwrap(),
                    vec![(expected.hold(), Ok(()))]
                );
                assert!(management.registered_session(LINK).await.is_none());
                assert!(!independent.is_ended());
                assert_eq!(
                    counters(&actor).next_lock_token,
                    before_counters.next_lock_token + 1
                );
                drop(broker);
                assert_reopened_release(&mut actor, &expected.hold());
                wire.stop().await;
            }
        }
    })
    .await
    .expect("original test observer joined");
}
