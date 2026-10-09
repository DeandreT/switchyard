//! Controlled internal faults in real receiving leaves, not ancestor survival.

use super::*;
use crate::listener::settlement::{
    custody::{PUMP_PANIC, PanicFrontier, PumpPanic},
    prepare_receiving_entry, serve_receiving_entry,
};
use crate::listener::{ConnectionRetirementRequest, ReceivingLinkProtocol};
use domain::{MessageEnvelope, MessageState, SessionHold};
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;

fn prepared<B: Broker>(
    wire: &mut Wire,
    actor: &Actor,
    broker: B,
    hold: Option<SessionHold>,
    management: Arc<ConnectionManagement>,
    registration: Option<crate::management::SessionRegistration>,
) -> (
    Sender,
    crate::listener::settlement::PreparedReceivingEntry<B>,
    ConnectionRetirementRequest,
) {
    let notice = ConnectionRetirementRequest::capture(&wire.connection);
    let sender = wire.sender.take().unwrap();
    let entry = prepare_receiving_entry(
        &sender,
        actor.namespace.clone(),
        actor.entity.clone(),
        broker,
        ReceiveMode::PeekLock,
        hold,
        ReceivingLinkProtocol {
            authorization: None,
            management,
            session_registration: registration,
        },
    )
    .with_retirement(Some(notice.clone()));
    (sender, entry, notice)
}

fn receiving_panic(
    result: std::thread::Result<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
) {
    assert_eq!(
        result
            .expect_err("original receiving pump panic resumes")
            .downcast_ref::<&str>(),
        Some(&"receiving-primary-panic")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn receiving_fault_notice_precedes_original_receive_commit_drain() {
    tokio::spawn(async {
        for durable in [false, true] {
            for after in [false, true] {
                let mut actor = Actor::new(durable, false);
                let sequence = actor.send("held-intake", None);
                actor.gate.arm_put(actor.key(sequence));
                let mut wire = Wire::new_with_credit(ReceiverSettleMode::Second, 1).await;
                let control = PumpPanic::new(PanicFrontier::Waiting);
                let (sender, entry, notice) = prepared(&mut wire, &actor, actor.broker.as_ref().unwrap().clone(),
                    None, ConnectionManagement::new(), None);
                let mut original = Box::pin(PUMP_PANIC.scope(Arc::clone(&control),
                    AssertUnwindSafe(serve_receiving_entry(sender, entry)).catch_unwind()));
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("original Receive is held") },
                    () = actor.gate.reached(false) => {},
                }}).await.unwrap();
                if after { actor.gate.release(false); actor.gate.reached(true).await; }
                assert!(!notice.is_requested());
                control.request();
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("notice precedes original Receive drain") },
                    () = notice.observer() => {},
                }}).await.unwrap();
                assert!(notice.is_requested());
                pending_once(original.as_mut()).await;
                timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
                actor.gate.release_all();
                receiving_panic(timeout(WAIT, original.as_mut()).await.unwrap());
                drop(original);
                assert_eq!(actor.receive_count(), 1);
                assert!(matches!(StateMachine::new(actor.store().clone())
                    .message(&actor.namespace, &actor.entity, sequence).unwrap().unwrap().state,
                    MessageState::Locked { .. }));
                let snapshot = actor.store().snapshot().unwrap();
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            }
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn receiving_fault_notice_interrupts_original_native_transfer_without_gate_release() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        let sequence = actor.send("held-native", None);
        let mut wire = Wire::new_with_credit(ReceiverSettleMode::Second, 1).await;
        let _barrier = wire.session_barrier(2).await;
        let _release = wire.writes.release_on_drop();
        let control = PumpPanic::new(PanicFrontier::Waiting);
        let management = ConnectionManagement::new();
        let (sender, entry, notice) = prepared(
            &mut wire,
            &actor,
            actor.broker.as_ref().unwrap().clone(),
            None,
            Arc::clone(&management),
            None,
        );
        wire.writes.block();
        let original = tokio::spawn(PUMP_PANIC.scope(
            Arc::clone(&control),
            AssertUnwindSafe(serve_receiving_entry(sender, entry)).catch_unwind(),
        ));
        wire.writes.reached().await;
        assert_eq!(actor.receive_count(), 1);
        let locked = StateMachine::new(actor.store().clone())
            .message(&actor.namespace, &actor.entity, sequence)
            .unwrap()
            .unwrap();
        let MessageState::Locked { token, .. } = locked.state else {
            panic!("the held original Transfer owns the committed lock");
        };
        assert!(management.delivery(LINK, token).await.is_some());
        let committed = actor.store().snapshot().unwrap();
        assert!(!notice.is_requested());
        control.request();
        timeout(WAIT, notice.observer()).await.unwrap();
        receiving_panic(timeout(WAIT, original).await.unwrap().unwrap());
        assert!(wire.writes.state.lock().unwrap().blocked);
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actor.receive_count(), 1);
        assert!(management.delivery(LINK, token).await.is_none());
        {
            let log = actor.log.lock().unwrap();
            assert_eq!(log.len(), 2, "only the original Receive enters and returns");
            assert!(!log[0].returned && log[1].returned);
            assert!(
                log.iter()
                    .all(|submission| matches!(submission.kind, CommandKind::Receive { .. }))
            );
        }
        assert_eq!(
            StateMachine::new(actor.store().clone())
                .message(&actor.namespace, &actor.entity, sequence)
                .unwrap()
                .unwrap(),
            locked
        );
        assert_eq!(actor.store().snapshot().unwrap(), committed);
        actor.reopen();
        assert_eq!(actor.store().snapshot().unwrap(), committed);
    }
}

#[derive(Clone)]
struct CompleteFaultBroker {
    actual: ActualBroker,
    first: SequenceNumber,
    reached: Arc<Notify>,
    release: Arc<Notify>,
    payload: Arc<str>,
}

impl Broker for CompleteFaultBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        if matches!(kind, CommandKind::Complete { sequence, .. } if sequence == self.first) {
            self.reached.notify_one();
            self.release.notified().await;
            std::panic::panic_any(Arc::clone(&self.payload));
        }
        self.actual.submit(namespace, entity, kind).await
    }
    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

async fn transferred(wire: &mut Wire) -> u32 {
    let Performative::Transfer(transfer) = performative(&mut wire.peer).await else {
        panic!("actual Transfer")
    };
    transfer.delivery_id.unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn retained_receiving_worker_join_error_notifies_before_opposite_confirmation_drain() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        let first = actor.send("first-worker", None);
        let second = actor.send("second-worker", None);
        let mut wire = Wire::new_with_credit(ReceiverSettleMode::Second, 2).await;
        let _release = wire.writes.release_on_drop();
        let payload: Arc<str> = Arc::from("original receiving worker fault");
        let broker = CompleteFaultBroker {
            actual: actor.broker.as_ref().unwrap().clone(),
            first,
            reached: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            payload: Arc::clone(&payload),
        };
        let management = ConnectionManagement::new();
        let (sender, entry, notice) = prepared(
            &mut wire,
            &actor,
            broker.clone(),
            None,
            Arc::clone(&management),
            None,
        );
        let original = tokio::spawn(serve_receiving_entry(sender, entry));
        let first_id = transferred(&mut wire).await;
        let second_id = transferred(&mut wire).await;
        wire.accepted(first_id).await;
        timeout(WAIT, broker.reached.notified()).await.unwrap();
        wire.writes.block();
        wire.accepted(second_id).await;
        wire.writes.reached().await;
        assert!(!notice.is_requested());
        broker.release.notify_one();
        timeout(WAIT, notice.observer()).await.unwrap();
        let error = timeout(WAIT, original)
            .await
            .unwrap()
            .unwrap()
            .err()
            .unwrap();
        let join = error.downcast::<tokio::task::JoinError>().unwrap();
        assert!(join.is_panic());
        let caught = join.into_panic();
        assert!(Arc::ptr_eq(
            caught.downcast_ref::<Arc<str>>().unwrap(),
            &payload
        ));
        assert!(wire.writes.state.lock().unwrap().blocked);
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actor.complete_count(), 1);
        assert!(
            StateMachine::new(actor.store().clone())
                .message(&actor.namespace, &actor.entity, second)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            StateMachine::new(actor.store().clone())
                .message(&actor.namespace, &actor.entity, first)
                .unwrap()
                .unwrap()
                .state,
            MessageState::Locked { .. }
        ));
        drop(broker);
        let snapshot = actor.store().snapshot().unwrap();
        actor.reopen();
        assert_eq!(actor.store().snapshot().unwrap(), snapshot);
    }
}

#[derive(Clone)]
struct ProtocolReleaseBroker {
    actual: ActualBroker,
    gate: Arc<CommitGate>,
    key: Vec<u8>,
    cause: Arc<Mutex<Option<crate::ProtocolError>>>,
}

impl Broker for ProtocolReleaseBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let receiving = matches!(kind, CommandKind::Receive { .. });
        let result = self.actual.submit(namespace, entity, kind).await;
        if receiving && let Ok(CommandOutcome::Received(Some(delivery))) = &result {
            let cause = crate::message::write_delivery_from(delivery, None)
                .err()
                .unwrap();
            *self.cause.lock().unwrap() = Some(cause);
            self.gate.arm_put(self.key.clone());
        }
        result
    }
    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn receiving_protocol_cause_survives_notice_and_held_original_session_release() {
    // Controlled internal malformed envelope, not ordinary network admission.
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            let id = SessionId::new("malformed-envelope-session").unwrap();
            let CommandOutcome::Sent { sequence } = actor.intent(CommandKind::Send {
                message_id: "malformed-envelope".to_owned(), body: Vec::new(), time_to_live_millis: None,
                session_id: Some(id.clone()), scheduled_enqueue_at: None,
                envelope: Some(MessageEnvelope::new(vec![0xff])),
            }) else { panic!("actual malformed envelope stored") };
            let CommandOutcome::SessionAccepted(Some(accepted)) = actor.intent(CommandKind::AcceptSession {
                session_id: Some(id), lock_duration_millis: None,
            }) else { panic!("actual session grant") };
            let hold = accepted.hold();
            let management = ConnectionManagement::new();
            let claim = management.claim_session(LINK, actor.entity.clone());
            let registration = management.install_session(&claim, hold.clone(), || true).await.unwrap();
            let cause = Arc::new(Mutex::new(None));
            let broker = ProtocolReleaseBroker { actual: actor.broker.as_ref().unwrap().clone(),
                gate: Arc::clone(&actor.gate), key: domain::keys::session(&actor.namespace, &actor.entity, &hold.session_id),
                cause: Arc::clone(&cause) };
            let mut wire = Wire::new_with_credit(ReceiverSettleMode::Second, 1).await;
            let (sender, entry, notice) = prepared(&mut wire, &actor, broker, Some(hold.clone()),
                Arc::clone(&management), Some(registration.clone()));
            let mut original = Box::pin(serve_receiving_entry(sender, entry));
            timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("original session Release is held") },
                () = actor.gate.reached(false) => {},
            }}).await.unwrap();
            assert!(notice.is_requested());
            timeout(WAIT, notice.observer()).await.unwrap();
            assert_eq!(management.registered_session_owner(LINK).await, Some(registration));
            timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
            pending_once(original.as_mut()).await;
            assert_eq!(actor.log.lock().unwrap().iter().filter(|submission|
                !submission.returned && matches!(&submission.kind, CommandKind::ReleaseSession { session } if session == &hold)).count(), 1);
            actor.gate.release_all();
            let error = timeout(WAIT, original.as_mut()).await.unwrap().err().unwrap();
            let error = error.downcast::<crate::ProtocolError>().unwrap();
            assert_eq!(*error, cause.lock().unwrap().take().unwrap());
            assert!(matches!(*error, crate::ProtocolError::InvalidEnvelope { .. }));
            drop(original);
            assert_eq!(actor.receive_count(), 1);
            assert_eq!(actor.log.lock().unwrap().iter().filter(|submission|
                !submission.returned && matches!(submission.kind, CommandKind::ReleaseSession { .. })).count(), 1);
            assert!(management.registered_session_owner(LINK).await.is_none());
            assert!(matches!(StateMachine::new(actor.store().clone()).message(&actor.namespace, &actor.entity, sequence)
                .unwrap().unwrap().state, MessageState::Locked { .. }));
            let snapshot = actor.store().snapshot().unwrap();
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            assert!(matches!(StateMachine::new(actor.store().clone()).message(&actor.namespace, &actor.entity, sequence)
                .unwrap().unwrap().state, MessageState::Locked { .. }));
        }
    }).await.expect("original observer joined");
}

#[derive(Clone)]
struct UnexpectedReceiveBroker;

impl Broker for UnexpectedReceiveBroker {
    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        assert!(matches!(kind, CommandKind::Receive { .. }));
        Ok(CommandOutcome::QueueCreated)
    }
    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn receiving_unexpected_broker_outcome_is_benign_and_keeps_native_connection_usable() {
    let actor = Actor::new(false, false);
    let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
    let (sender, entry, notice) = prepared(
        &mut wire,
        &actor,
        UnexpectedReceiveBroker,
        None,
        ConnectionManagement::new(),
        None,
    );
    timeout(WAIT, serve_receiving_entry(sender, entry))
        .await
        .unwrap()
        .unwrap();
    let Performative::Detach(detach) = performative(&mut wire.peer).await else {
        panic!("broker refusal Detach")
    };
    assert_eq!(
        detach.error.unwrap().condition,
        amqp::AmqpError::ResourceLocked.into()
    );
    assert!(!notice.is_requested());
    let _new_session = wire.session_barrier(2).await;
    wire.stop().await;
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        let sequence = actor.send("healthy-receive", None);
        actor.gate.arm(actor.key(sequence));
        let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
        let (sender, entry, notice) = prepared(
            &mut wire,
            &actor,
            actor.broker.as_ref().unwrap().clone(),
            None,
            ConnectionManagement::new(),
            None,
        );
        let original = tokio::spawn(serve_receiving_entry(sender, entry));
        let id = transferred(&mut wire).await;
        wire.accepted(id).await;
        timeout(WAIT, actor.gate.reached(false)).await.unwrap();
        wire.detach().await;
        assert!(!notice.is_requested());
        actor.gate.release_all();
        timeout(WAIT, original).await.unwrap().unwrap().unwrap();
        assert!(!notice.is_requested());
        assert_eq!(actor.complete_count(), 1);
        assert!(
            StateMachine::new(actor.store().clone())
                .message(&actor.namespace, &actor.entity, sequence)
                .unwrap()
                .is_none()
        );
        let _new_session = wire.session_barrier(2).await;
        wire.stop().await;
        let snapshot = actor.store().snapshot().unwrap();
        actor.reopen();
        assert_eq!(actor.store().snapshot().unwrap(), snapshot);
    }
}
