//! Actual session admissions/effects; primitive result bridges live separately.
//! Fixture deadlines are reachability guards, not finite product IO promises.

use super::*;
use crate::listener::control_attachment_custody::RoutePacket;
use crate::listener::{ConnectionRetirementRequest, serve_session_with_retirement};
use crate::listener::{
    serve_session_pump,
    session_custody::{
        ADOPTIONS, AdoptionObserver, JoinedLeafTask, LeafKind, PUMP_FAULT, PumpFault, PumpPoint,
        SessionCustody,
    },
};
use amqp::{Body, encode_message};
use domain::{MessageState, QueueCounters};
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;

#[path = "session_family_fixture.rs"]
mod fixture;
use fixture::*;

// Two mode rows: all six offers go through the actual serve_session entrypoint.
#[tokio::test(flavor = "current_thread")]
async fn actual_session_adopts_all_six_leaf_kinds_and_services_survive_until_processed_end() {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let actor = Actor::new(false, false);
        let (mut wire, session) = AdmissionWire::new().await;
        let mut ended = Box::pin(session.on_end_owned());
        let mut old_incarnation = Box::pin(session.on_end_owned());
        let authorization = authorization();
        let management = ConnectionManagement::new();
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let observer = AdoptionObserver::new(false);
        let original = tokio::spawn(ADOPTIONS.scope(
            Arc::clone(&observer),
            serve_session_with_retirement(
                session,
                actor.namespace.clone(),
                actor.broker.as_ref().unwrap().clone(),
                Some(authorization),
                management,
                Some(notice.clone()),
            ),
        ));
        for (offset, role) in ADMISSIONS.into_iter().enumerate() {
            let handle = offset as u32 + 1;
            wire.offer(role, handle, mode.clone(), None).await;
            wire.accepted(role, handle).await;
            adopted(&observer, offset + 1).await;
            let records = observer.records.lock().unwrap();
            assert_eq!(records[offset].0, role.kind());
            assert!(
                records[..offset]
                    .iter()
                    .all(|(_, id)| *id != records[offset].1)
            );
        }
        wire.credit(4).await;
        wire.credit(6).await;
        wire.request(3, 42, request_message(42), 0).await;
        accepted_request(&mut wire, 42).await;
        reply(&mut wire, 4, 42, true, mode.clone()).await;
        wire.request(5, 43, request_message(43), 0).await;
        accepted_request(&mut wire, 43).await;
        reply(&mut wire, 6, 43, false, mode).await;
        assert!(!notice.is_requested());
        wire.end().await;
        timeout(WAIT, ended.as_mut()).await.unwrap();
        assert!(matches!(
            wire.control(FAMILY_CHANNEL).await,
            Performative::End(_)
        ));
        timeout(WAIT, original).await.unwrap().unwrap().unwrap();
        assert!(!notice.is_requested());
        assert_eq!(observer.records.lock().unwrap().len(), 6);
        let replacement = wire.begin(FAMILY_CHANNEL).await;
        assert!(!replacement.is_ended());
        assert!(old_incarnation.as_mut().now_or_never().is_some());
        wire.stop().await;
    }
}

// Eight actual store rows: Memory/Fjall x Send/Batch x before/after apply.
#[tokio::test(flavor = "current_thread")]
async fn actual_session_processed_end_keeps_send_and_batch_originals_through_cancelled_finish() {
    for durable in [false, true] {
        for batch in [false, true] {
            for after in [false, true] {
                let mut actor = Actor::new(durable, false);
                let (mut wire, session) = AdmissionWire::new().await;
                let mut ended = Box::pin(session.on_end_owned());
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let mut custody = SessionCustody::new(session, Some(notice.clone()));
                let observer = AdoptionObserver::new(false);
                let mut original = Box::pin(
                    ADOPTIONS.scope(
                        Arc::clone(&observer),
                        AssertUnwindSafe(serve_session_pump(
                            &mut custody,
                            actor.namespace.clone(),
                            actor.broker.as_ref().unwrap().clone(),
                            Some(authorization()),
                            ConnectionManagement::new(),
                        ))
                        .catch_unwind(),
                    ),
                );
                wire.offer(Admission::DataSend, 1, ReceiverSettleMode::Second, None)
                    .await;
                drive(original.as_mut(), wire.accepted(Admission::DataSend, 1)).await;
                drive(original.as_mut(), adopted(&observer, 1)).await;
                // This real idle sibling supplies the completed-first packet.
                wire.offer(
                    Admission::ManagementRequest,
                    2,
                    ReceiverSettleMode::First,
                    None,
                )
                .await;
                drive(
                    original.as_mut(),
                    wire.accepted(Admission::ManagementRequest, 2),
                )
                .await;
                drive(original.as_mut(), adopted(&observer, 2)).await;
                actor.clock.set(2_000);
                actor.gate.arm_put(actor.key(SequenceNumber::new(1)));
                let first = data_message(10, b"first");
                let message = if batch {
                    Message {
                        body: Body::Data(vec![
                            encode_message(&first).unwrap().into(),
                            encode_message(&data_message(11, b"second")).unwrap().into(),
                        ]),
                        ..Message::default()
                    }
                } else {
                    first
                };
                wire.request(
                    1,
                    0,
                    message,
                    if batch {
                        crate::SERVICE_BUS_BATCH_MESSAGE_FORMAT
                    } else {
                        0
                    },
                )
                .await;
                drive(original.as_mut(), actor.gate.reached(false)).await;
                if after {
                    actor.gate.release(false);
                    drive(original.as_mut(), actor.gate.reached(true)).await;
                }
                actor.clock.set(3_000);
                wire.end().await;
                drive(original.as_mut(), ended.as_mut()).await;
                assert!(matches!(
                    wire.control(FAMILY_CHANNEL).await,
                    Performative::End(_)
                ));
                let primary = timeout(WAIT, original.as_mut()).await.unwrap();
                drop(original);
                custody.record_primary(primary);
                assert!(!notice.is_requested());
                partial_finish(&mut custody).await;
                assert_eq!(custody.family().finished().len(), 1);
                let packet = &custody.family().finished()[0];
                assert_eq!(packet.kind, LeafKind::ManagementRequest);
                let address = (&**packet as *const JoinedLeafTask) as usize;
                for _ in 0..2 {
                    let mut finish = Box::pin(custody.finish());
                    pending_once(finish.as_mut()).await;
                    drop(finish);
                    assert_eq!(
                        (&*custody.family().finished()[0] as *const JoinedLeafTask) as usize,
                        address
                    );
                }
                assert_eq!(
                    invocations(&actor, |kind| matches!(
                        kind,
                        CommandKind::Send { .. } | CommandKind::SendBatch { .. }
                    )),
                    1
                );
                actor.gate.release_all();
                timeout(WAIT, custody.finish()).await.unwrap();
                assert_eq!(custody.family().finished().len(), 2);
                assert_eq!(
                    (&*custody.family().finished()[0] as *const JoinedLeafTask) as usize,
                    address
                );
                assert_eq!(custody.family().finished()[1].kind, LeafKind::DataSend);
                custody.into_result().unwrap();
                let machine = StateMachine::new(actor.store().clone());
                assert_eq!(machine.last_applied_time().unwrap().as_millis(), 2_000);
                for sequence in 1..=if batch { 2 } else { 1 } {
                    assert!(matches!(
                        machine
                            .message(
                                &actor.namespace,
                                &actor.entity,
                                SequenceNumber::new(sequence)
                            )
                            .unwrap()
                            .unwrap()
                            .state,
                        MessageState::Ready
                    ));
                }
                let counters: QueueCounters = domain::codec::decode(
                    &actor
                        .store()
                        .get(&domain::keys::queue_counters(
                            &actor.namespace,
                            &actor.entity,
                        ))
                        .unwrap()
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(counters.next_sequence, if batch { 3 } else { 2 });
                drop(machine);
                assert!(!notice.is_requested());
                let committed = actor.store().snapshot().unwrap();
                wire.stop().await;
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), committed);
            }
        }
    }
}

// Eight actual store rows: both stores/modes and before/after Complete+Release.
#[tokio::test(flavor = "current_thread")]
async fn actual_session_processed_end_joins_complete_before_one_captured_release_and_conditional_rows()
 {
    tokio::spawn(async {
        for durable in [false, true] {
            for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
                for after in [false, true] {
                    let mut actor = Actor::new(durable, true);
                    let id = SessionId::new("family-session").unwrap();
                    let sequence = actor.send("family-complete", Some(id.clone()));
                    let (mut wire, session) = AdmissionWire::new().await;
                    let mut ended = Box::pin(session.on_end_owned());
                    let notice = ConnectionRetirementRequest::capture(&wire.connection);
                    let management = ConnectionManagement::new();
                    let mut custody = SessionCustody::new(session, Some(notice.clone()));
                    let observer = AdoptionObserver::new(false);
                    let release_broker =
                        ReleaseGateBroker::new(actor.broker.as_ref().unwrap().clone());
                    let mut original = Box::pin(
                        ADOPTIONS.scope(
                            Arc::clone(&observer),
                            AssertUnwindSafe(serve_session_pump(
                                &mut custody,
                                actor.namespace.clone(),
                                release_broker.clone(),
                                Some(authorization()),
                                Arc::clone(&management),
                            ))
                            .catch_unwind(),
                        ),
                    );
                    wire.offer(Admission::DataReceive, 1, mode.clone(), Some(id.as_str()))
                        .await;
                    drive(original.as_mut(), wire.accepted(Admission::DataReceive, 1)).await;
                    drive(original.as_mut(), adopted(&observer, 1)).await;
                    let old_session = management
                        .registered_session_owner(Admission::DataReceive.name())
                        .await
                        .unwrap();
                    let (_, hold) = management
                        .registered_session(Admission::DataReceive.name())
                        .await
                        .unwrap();
                    wire.credit(1).await;
                    let (transfer, _) = drive(original.as_mut(), wire.transfer(1)).await;
                    let tag: &[u8] = transfer.delivery_tag.as_ref().unwrap().as_ref();
                    let token =
                        domain::LockToken::new(u64::from_be_bytes(tag[8..].try_into().unwrap()));
                    actor.gate.arm(actor.key(sequence));
                    wire.outcome(transfer.delivery_id.unwrap(), mode.clone())
                        .await;
                    drive(original.as_mut(), actor.gate.reached(false)).await;
                    if after {
                        actor.gate.release(false);
                        drive(original.as_mut(), actor.gate.reached(true)).await;
                    }
                    let replacement_delivery = management
                        .register_delivery(
                            Admission::DataReceive.name(),
                            actor.entity.clone(),
                            sequence,
                            token,
                        )
                        .await;
                    let claim = management
                        .claim_session(Admission::DataReceive.name(), actor.entity.clone());
                    let replacement_session = management
                        .install_session(&claim, hold, || true)
                        .await
                        .unwrap();
                    assert_ne!(old_session, replacement_session);
                    let delivery_row = management.delivery_write_lock().await;
                    wire.end().await;
                    drive(original.as_mut(), ended.as_mut()).await;
                    assert!(matches!(
                        wire.control(FAMILY_CHANNEL).await,
                        Performative::End(_)
                    ));
                    let primary = timeout(WAIT, original.as_mut()).await.unwrap();
                    drop(original);
                    custody.record_primary(primary);
                    for _ in 0..2 {
                        let mut finish = Box::pin(custody.finish());
                        pending_once(finish.as_mut()).await;
                    }
                    assert_eq!(actor.complete_count(), 1);
                    assert_eq!(
                        invocations(&actor, |kind| matches!(
                            kind,
                            CommandKind::ReleaseSession { .. }
                        )),
                        0
                    );
                    actor.gate.release_all();
                    returned(&actor, |kind| matches!(kind, CommandKind::Complete { .. })).await;
                    // The same retained ReleaseSession proxy waits before its
                    // unchanged ActualBroker delegate can reach a store apply.
                    release_broker.barrier.reached().await;
                    let release_worker = release_broker.barrier.calls()[0];
                    assert_eq!(release_broker.barrier.calls().len(), 1);
                    assert_eq!(
                        invocations(&actor, |kind| matches!(
                            kind,
                            CommandKind::ReleaseSession { .. }
                        )),
                        0
                    );
                    for _ in 0..2 {
                        let mut finish = Box::pin(custody.finish());
                        pending_once(finish.as_mut()).await;
                    }
                    actor
                        .gate
                        .arm_put(domain::keys::session(&actor.namespace, &actor.entity, &id));
                    let session_row = management.session_write_lock().await;
                    drop(delivery_row);
                    release_broker.barrier.release();
                    actor.gate.reached(false).await;
                    if after {
                        actor.gate.release(false);
                        actor.gate.reached(true).await;
                    }
                    for _ in 0..2 {
                        let mut finish = Box::pin(custody.finish());
                        pending_once(finish.as_mut()).await;
                    }
                    assert_eq!(
                        invocations(&actor, |kind| matches!(
                            kind,
                            CommandKind::ReleaseSession { .. }
                        )),
                        1
                    );
                    assert_eq!(release_broker.barrier.calls(), vec![release_worker]);
                    actor.gate.release_all();
                    returned(&actor, |kind| {
                        matches!(kind, CommandKind::ReleaseSession { .. })
                    })
                    .await;
                    for _ in 0..2 {
                        let mut finish = Box::pin(custody.finish());
                        pending_once(finish.as_mut()).await;
                    }
                    assert!(custody.family().finished().is_empty());
                    drop(session_row);
                    timeout(WAIT, custody.finish()).await.unwrap();
                    {
                        let records = observer.records.lock().unwrap();
                        assert_eq!(custody.family().finished()[0].id, records[0].1);
                    }
                    custody.into_result().unwrap();
                    assert!(!notice.is_requested());
                    assert_eq!(
                        management
                            .registered_session_owner(Admission::DataReceive.name())
                            .await,
                        Some(replacement_session.clone())
                    );
                    assert!(
                        management
                            .delivery(Admission::DataReceive.name(), token)
                            .await
                            .is_some()
                    );
                    management.unregister_delivery(&replacement_delivery).await;
                    management.unregister_session(&replacement_session).await;
                    {
                        let log = actor.log.lock().unwrap();
                        let complete = log
                            .iter()
                            .position(|entry| {
                                entry.returned && matches!(entry.kind, CommandKind::Complete { .. })
                            })
                            .unwrap();
                        let release = log
                            .iter()
                            .position(|entry| {
                                !entry.returned
                                    && matches!(entry.kind, CommandKind::ReleaseSession { .. })
                            })
                            .unwrap();
                        assert!(complete < release);
                        assert_eq!(log[release].worker, release_worker);
                    }
                    assert_eq!(actor.complete_count(), 1);
                    assert_eq!(
                        invocations(&actor, |kind| matches!(
                            kind,
                            CommandKind::ReleaseSession { .. }
                        )),
                        1
                    );
                    let machine = StateMachine::new(actor.store().clone());
                    assert!(
                        machine
                            .message(&actor.namespace, &actor.entity, sequence)
                            .unwrap()
                            .is_none()
                    );
                    assert!(
                        machine
                            .session(&actor.namespace, &actor.entity, &id)
                            .unwrap()
                            .unwrap()
                            .lock
                            .is_none()
                    );
                    drop(machine);
                    let committed = actor.store().snapshot().unwrap();
                    wire.stop().await;
                    actor.reopen();
                    assert_eq!(actor.store().snapshot().unwrap(), committed);
                }
            }
        }
    })
    .await
    .unwrap();
}

// Six native rows on Memory only: backend repetition is not native evidence.
#[tokio::test(flavor = "current_thread")]
async fn actual_session_parent_fault_interrupts_begun_native_children_without_writer_release() {
    for role in ADMISSIONS {
        let actor = Actor::new(false, false);
        if matches!(role, Admission::DataReceive) {
            actor.send("held-original-transfer", None);
        }
        let (mut wire, session) = AdmissionWire::new().await;
        let _write_release = wire.writes.release_on_drop();
        let authorization = authorization();
        let management = ConnectionManagement::new();
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let observer = AdoptionObserver::new(false);
        let fault = PumpFault::new(PumpPoint::Intake);
        let mut original = tokio::spawn(
            ADOPTIONS.scope(
                Arc::clone(&observer),
                PUMP_FAULT.scope(
                    Arc::clone(&fault),
                    AssertUnwindSafe(serve_session_with_retirement(
                        session,
                        actor.namespace.clone(),
                        actor.broker.as_ref().unwrap().clone(),
                        Some(Arc::clone(&authorization)),
                        Arc::clone(&management),
                        Some(notice.clone()),
                    ))
                    .catch_unwind(),
                ),
            ),
        );
        let mut cbs_external = None;
        let mut management_external = None;
        if matches!(role, Admission::CbsRequest) {
            cbs_external = Some(authorization.register_reply_route(REPLIES.to_owned()).await);
        }
        if matches!(role, Admission::ManagementRequest) {
            management_external = Some(management.register_reply_route(REPLIES.to_owned()).await);
        }
        wire.offer(role, 1, ReceiverSettleMode::Second, None).await;
        wire.accepted(role, 1).await;
        adopted(&observer, 1).await;
        if matches!(role, Admission::CbsReply | Admission::ManagementReply) {
            let request = if matches!(role, Admission::CbsReply) {
                Admission::CbsRequest
            } else {
                Admission::ManagementRequest
            };
            wire.offer(request, 2, ReceiverSettleMode::First, None)
                .await;
            wire.accepted(request, 2).await;
            adopted(&observer, 2).await;
            wire.request(2, 42, request_message(42), 0).await;
            accepted_request(&mut wire, 42).await;
        }
        wire.writes.block();
        match role {
            Admission::DataReceive | Admission::CbsReply | Admission::ManagementReply => {
                wire.credit(1).await
            }
            Admission::DataSend => wire.request(1, 0, data_message(100, b"held-ack"), 0).await,
            Admission::CbsRequest | Admission::ManagementRequest => {
                wire.request(1, 42, request_message(42), 0).await
            }
        }
        wire.writes.reached().await;
        assert!(!notice.is_requested());
        fault.trigger.notify_one();
        timeout(WAIT, notice.observer()).await.unwrap();
        assert!(wire.writes.state.lock().unwrap().blocked);
        let raw = timeout(WAIT, &mut original).await.unwrap().unwrap();
        let payload = raw.expect_err("actual session parent panic is retained through child joins");
        assert!(Arc::ptr_eq(
            payload.downcast_ref::<Arc<str>>().unwrap(),
            &fault.payload
        ));
        assert!(wire.writes.state.lock().unwrap().blocked);
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        if let Some((route, mut responses)) = cbs_external {
            responses.close();
            authorization.unregister_reply_route(REPLIES, &route).await;
        }
        if let Some((route, mut responses)) = management_external {
            responses.close();
            management.unregister_reply_route(REPLIES, &route).await;
        }
        if matches!(role, Admission::DataSend) {
            assert_eq!(
                invocations(&actor, |kind| matches!(kind, CommandKind::Send { .. })),
                1
            );
        }
        if matches!(role, Admission::DataReceive) {
            assert_eq!(actor.receive_count(), 1);
        }
    }
}

// Two actual reply admissions. Only CBS exposes an existing row-lock fixture;
// management proves unlocked conditional cleanup, not a held private mutex.
#[tokio::test(flavor = "current_thread")]
async fn actual_session_reply_cleanup_preserves_usable_same_address_replacements() {
    for cbs in [true, false] {
        let actor = Actor::new(false, false);
        let (mut wire, session) = AdmissionWire::new().await;
        let mut ended = Box::pin(session.on_end_owned());
        let authorization = authorization();
        let management = ConnectionManagement::new();
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let observer = AdoptionObserver::new(false);
        let role = if cbs {
            Admission::CbsReply
        } else {
            Admission::ManagementReply
        };
        let mut custody = SessionCustody::new(session, Some(notice.clone()));
        let mut original = Box::pin(
            ADOPTIONS.scope(
                Arc::clone(&observer),
                AssertUnwindSafe(serve_session_pump(
                    &mut custody,
                    actor.namespace.clone(),
                    actor.broker.as_ref().unwrap().clone(),
                    Some(Arc::clone(&authorization)),
                    Arc::clone(&management),
                ))
                .catch_unwind(),
            ),
        );
        wire.offer(role, 1, ReceiverSettleMode::First, None).await;
        drive(original.as_mut(), wire.accepted(role, 1)).await;
        drive(original.as_mut(), adopted(&observer, 1)).await;
        let id = observer.records.lock().unwrap()[0].1;
        let mut replacement = replacement_route(cbs, &authorization, &management).await;
        match &mut replacement {
            RoutePacket::Cbs { responses, .. } => assert!(matches!(
                responses.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            )),
            RoutePacket::Management { responses, .. } => assert!(matches!(
                responses.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            )),
        }
        let held = if cbs {
            Some(authorization.reply_route_lock().await)
        } else {
            None
        };
        wire.end().await;
        drive(original.as_mut(), ended.as_mut()).await;
        assert!(matches!(
            wire.control(FAMILY_CHANNEL).await,
            Performative::End(_)
        ));
        let primary = timeout(WAIT, original.as_mut()).await.unwrap();
        drop(original);
        custody.record_primary(primary);
        if cbs {
            for _ in 0..2 {
                let mut finish = Box::pin(custody.finish());
                pending_once(finish.as_mut()).await;
            }
            assert!(custody.family().finished().is_empty());
        }
        drop(held);
        timeout(WAIT, custody.finish()).await.unwrap();
        assert_eq!(custody.family().finished()[0].id, id);
        custody.into_result().unwrap();
        assert!(!notice.is_requested());
        // Reuse the native channel only after its End is positively processed.
        let session = wire.begin(FAMILY_CHANNEL).await;
        let mut ended = Box::pin(session.on_end_owned());
        let request = if cbs {
            Admission::CbsRequest
        } else {
            Admission::ManagementRequest
        };
        let next_observer = AdoptionObserver::new(false);
        let next = tokio::spawn(ADOPTIONS.scope(
            Arc::clone(&next_observer),
            serve_session_with_retirement(
                session,
                actor.namespace.clone(),
                actor.broker.as_ref().unwrap().clone(),
                Some(Arc::clone(&authorization)),
                Arc::clone(&management),
                Some(notice.clone()),
            ),
        ));
        wire.offer(request, 1, ReceiverSettleMode::First, None)
            .await;
        wire.accepted(request, 1).await;
        adopted(&next_observer, 1).await;
        wire.request(1, 44, request_message(44), 0).await;
        accepted_request(&mut wire, 44).await;
        received_replacement(&mut replacement).await;
        wire.end().await;
        timeout(WAIT, ended.as_mut()).await.unwrap();
        assert!(matches!(
            wire.control(FAMILY_CHANNEL).await,
            Performative::End(_)
        ));
        timeout(WAIT, next).await.unwrap().unwrap().unwrap();
        assert!(!notice.is_requested());
        remove_replacement(replacement).await;
        wire.stop().await;
    }
}

// Two refusals, two live remote-detach companions and six buffered-but-ended
// offers. End readiness is witnessed before starting the retired parent pump.
#[tokio::test(flavor = "current_thread")]
async fn actual_session_refusal_remote_detach_and_retired_intake_create_no_fault_notice() {
    for role in [Admission::DataSend, Admission::DataReceive] {
        let actor = Actor::new(false, false);
        let (mut wire, session) = AdmissionWire::new().await;
        let mut ended = Box::pin(session.on_end_owned());
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let observer = AdoptionObserver::new(false);
        let original = tokio::spawn(ADOPTIONS.scope(
            Arc::clone(&observer),
            serve_session_with_retirement(
                session,
                actor.namespace.clone(),
                actor.broker.as_ref().unwrap().clone(),
                Some(authorization()),
                ConnectionManagement::new(),
                Some(notice.clone()),
            ),
        ));
        let mut invalid = role.attach(1, ReceiverSettleMode::First, None);
        if role.sending() {
            invalid.target.as_mut().unwrap().address = Some(String::new());
        } else {
            invalid.source.as_mut().unwrap().address = Some(String::new());
        }
        write_frame(
            &mut wire.peer,
            &family_frame(FAMILY_CHANNEL, Performative::Attach(Box::new(invalid))),
        )
        .await
        .unwrap();
        wire.accepted(role, 1).await;
        let Performative::Detach(detach) = wire.control(FAMILY_CHANNEL).await else {
            panic!("actual refusal Detach")
        };
        assert_eq!(detach.handle, 1);
        assert!(detach.error.is_some());
        assert!(observer.records.lock().unwrap().is_empty());
        assert!(!notice.is_requested());
        // A valid sibling is admitted after the refusal; actual remote Detach
        // is benign and does not become a session-primary fault.
        wire.offer(role, 2, ReceiverSettleMode::First, None).await;
        wire.accepted(role, 2).await;
        adopted(&observer, 1).await;
        wire.detach(2).await;
        wire.end().await;
        timeout(WAIT, ended.as_mut()).await.unwrap();
        assert!(matches!(
            wire.control(FAMILY_CHANNEL).await,
            Performative::End(_)
        ));
        timeout(WAIT, original).await.unwrap().unwrap().unwrap();
        assert!(!notice.is_requested());
        assert_eq!(observer.records.lock().unwrap().len(), 1);
        wire.stop().await;
    }
    for role in ADMISSIONS {
        let actor = Actor::new(false, false);
        let (mut wire, session) = AdmissionWire::new().await;
        let mut ended = Box::pin(session.on_end_owned());
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let observer = AdoptionObserver::new(false);
        wire.offer(role, 1, ReceiverSettleMode::First, None).await;
        wire.end().await;
        timeout(WAIT, ended.as_mut()).await.unwrap();
        assert!(matches!(
            wire.control(FAMILY_CHANNEL).await,
            Performative::End(_)
        ));
        ADOPTIONS
            .scope(
                Arc::clone(&observer),
                serve_session_with_retirement(
                    session,
                    actor.namespace.clone(),
                    actor.broker.as_ref().unwrap().clone(),
                    Some(authorization()),
                    ConnectionManagement::new(),
                    Some(notice.clone()),
                ),
            )
            .await
            .unwrap();
        assert!(observer.records.lock().unwrap().is_empty());
        assert!(actor.log.lock().unwrap().is_empty());
        assert!(!notice.is_requested());
        // Native FIFO companion: a replacement Begin answers without a new
        // Attach/Flow/Detach from the never-started retired admission.
        let replacement = wire.begin(FAMILY_CHANNEL).await;
        assert!(!replacement.is_ended());
        wire.stop().await;
    }
}

// One actual reporting-origin row, distinct from genuine parent/native faults.
#[tokio::test(flavor = "current_thread")]
async fn actual_session_refused_attachment_report_preserves_raw_diagnostic_and_live_independent_session()
 {
    use std::sync::atomic::AtomicUsize;
    use tracing::instrument::WithSubscriber;
    let actor = Actor::new(false, false);
    let (mut wire, session) = AdmissionWire::new().await;
    let independent = wire.begin(FAMILY_CHANNEL + 1).await;
    let notice = ConnectionRetirementRequest::capture(&wire.connection);
    let observer = AdoptionObserver::new(false);
    let reached = Arc::new(AtomicUsize::new(0));
    let payload: Arc<str> = Arc::from("reached actual refused-attachment report");
    let other = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let dispatch = tracing::Dispatch::new(AttachmentReportFault {
        reached: Arc::clone(&reached),
        payload: Arc::clone(&payload),
    });
    std::thread::spawn(tracing::callsite::rebuild_interest_cache)
        .join()
        .unwrap();
    let original = tokio::spawn(
        ADOPTIONS.scope(
            Arc::clone(&observer),
            AssertUnwindSafe(serve_session_with_retirement(
                session,
                actor.namespace.clone(),
                actor.broker.as_ref().unwrap().clone(),
                Some(authorization()),
                ConnectionManagement::new(),
                Some(notice.clone()),
            ))
            .catch_unwind()
            .with_subscriber(dispatch),
        ),
    );
    let mut invalid = Admission::DataSend.attach(1, ReceiverSettleMode::First, None);
    invalid.target.as_mut().unwrap().address = Some(String::new());
    write_frame(
        &mut wire.peer,
        &family_frame(FAMILY_CHANNEL, Performative::Attach(Box::new(invalid))),
    )
    .await
    .unwrap();
    wire.accepted(Admission::DataSend, 1).await;
    let Performative::Detach(detach) = wire.control(FAMILY_CHANNEL).await else {
        panic!("successful actual refusal Detach")
    };
    assert_eq!(detach.handle, 1);
    assert!(detach.error.is_some());
    let raw = timeout(WAIT, original)
        .await
        .unwrap()
        .unwrap()
        .expect_err("raw report-only diagnostic resumes after local drain");
    assert!(Arc::ptr_eq(
        raw.downcast_ref::<Arc<str>>().unwrap(),
        &payload
    ));
    assert_eq!(reached.load(Ordering::SeqCst), 1);
    assert!(observer.records.lock().unwrap().is_empty());
    assert!(actor.log.lock().unwrap().is_empty());
    assert!(!notice.is_requested());
    // This independently created native session proves connection usability;
    // direct endpoint acceptance is not an additional serve_session admission.
    independent_session_usable(&mut wire, independent, FAMILY_CHANNEL + 1).await;
    assert!(!notice.is_requested());
    drop(other);
    wire.stop().await;
}
