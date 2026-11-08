//! Session acceptance, receiving and retirement share one pre-grant authority.
//! Metadata drift below is owned fault injection, not a live Delete operation.

use super::*;
use crate::listener::attachments::{
    custody::{PUMP_FAULT, PumpFault, PumpPoint},
    serve_entity_attachment,
};
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;

fn assert_frontier(actor: &Actor, broker: &Witness) {
    let frontier = broker.staged_frontier.lock().unwrap();
    let frontier = frontier.as_ref().unwrap();
    assert_eq!(observations(actor), frontier.counts);
    assert_eq!(actor.store().snapshot().unwrap(), frontier.snapshot);
    broker.assert_original();
}

#[tokio::test(flavor = "current_thread")]
async fn named_and_next_available_keep_echo_fifo_isolation_and_release_in_both_modes() {
    tokio::spawn(async {
        for durable in [false, true] {
            for next in [false, true] {
                for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
                    let mut actor = Actor::new(durable, true);
                    let id = super::super::session_id();
                    let other = domain::SessionId::new("other-session").unwrap();
                    let first = actor.send("first", Some(id.clone()));
                    let isolated = actor.send("isolated", Some(other.clone()));
                    let second = actor.send("second", Some(id.clone()));
                    let management = ConnectionManagement::new();
                    let broker = Witness::new(&actor);
                    let (mut wire, task) = attached_with_session(
                        &actor,
                        broker.clone(),
                        management.clone(),
                        &actor.entity,
                        mode,
                        None,
                        Some(next),
                    )
                    .await;
                    assert_eq!(
                        management
                            .registered_session(LINK)
                            .await
                            .unwrap()
                            .1
                            .session_id,
                        id
                    );
                    for (index, sequence) in [first, second].into_iter().enumerate() {
                        credit(&mut wire, index as u32 + 1).await;
                        let Frame::Amqp {
                            performative: Some(Performative::Transfer(transfer)),
                            payload,
                            ..
                        } = timeout(WAIT, read_frame(&mut wire.peer))
                            .await
                            .unwrap()
                            .unwrap()
                        else {
                            panic!("real session Transfer")
                        };
                        let message = amqp::decode_message(&payload).unwrap();
                        assert_eq!(
                            message
                                .message_annotations
                                .as_ref()
                                .unwrap()
                                .get(&amqp::AnnotationKey::from("x-opt-sequence-number")),
                            Some(&Value::Long(sequence.as_u64() as i64))
                        );
                        if mode == ReceiveMode::PeekLock {
                            write_frame(
                                &mut wire.peer,
                                &frame(
                                    CHANNEL,
                                    Performative::Disposition(Disposition {
                                        role: Role::Receiver,
                                        first: transfer.delivery_id.unwrap(),
                                        last: None,
                                        settled: false,
                                        state: Some(DeliveryState::Accepted(amqp::Accepted)),
                                        batchable: false,
                                    }),
                                ),
                            )
                            .await
                            .unwrap();
                            assert!(matches!(
                                control(&mut wire.peer, CHANNEL).await,
                                Performative::Disposition(_)
                            ));
                        }
                    }
                    credit(&mut wire, 3).await;
                    timeout(WAIT, broker.empty.notified()).await.unwrap();
                    stop(wire, task).await;
                    assert!(management.registered_session(LINK).await.is_none());
                    broker.assert_original();
                    assert_eq!(
                        broker
                            .commands
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|kind| matches!(kind, CommandKind::ReleaseSession { .. }))
                            .count(),
                        1
                    );
                    let machine = StateMachine::new(actor.store().clone());
                    assert!(
                        machine
                            .session(&actor.namespace, &actor.entity, &id)
                            .unwrap()
                            .unwrap()
                            .lock
                            .is_none()
                    );
                    assert_eq!(
                        machine
                            .session_ready_sequences(&actor.namespace, &actor.entity, &other, 10)
                            .unwrap(),
                        vec![isolated]
                    );
                    drop(machine);
                    drop(broker);
                    actor.reopen();
                }
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn generation_drift_between_capture_and_accept_refuses_without_clock_or_apply() {
    tokio::spawn(async {
        for durable in [false, true] {
            for next in [false, true] {
                let mut actor = Actor::new(durable, true);
                actor.send("before-grant", Some(super::super::session_id()));
                let broker = Witness::new(&actor);
                broker.drift_after_bind.store(true, Ordering::SeqCst);
                let management = ConnectionManagement::new();
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire
                    .offer(&mut session, Some(super::super::filter(next)))
                    .await;
                assert!(
                    accept_entity_link(
                        &session,
                        &broker,
                        &actor.namespace,
                        "orders",
                        attach,
                        None,
                        &management
                    )
                    .await
                    .unwrap()
                    .is_none()
                );
                assert!(matches!(
                    control(&mut wire.peer, CHANNEL).await,
                    Performative::Attach(_)
                ));
                let Performative::Detach(detach) = control(&mut wire.peer, CHANNEL).await else {
                    panic!("refused accept")
                };
                assert_eq!(
                    detach.error.unwrap().condition.as_symbol().as_str(),
                    crate::NOT_FOUND
                );
                assert!(management.registered_session(LINK).await.is_none());
                assert_frontier(&actor, &broker);
                assert!(matches!(
                    &broker.commands.lock().unwrap()[..],
                    [CommandKind::AcceptSession { .. }]
                ));
                wire.stop().await;
                drop(broker);
                actor.reopen();
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn stale_session_receive_and_final_release_use_only_the_pre_grant_capture() {
    tokio::spawn(async {
        for durable in [false, true] {
            for next in [false, true] {
                for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
                    let mut actor = Actor::new(durable, true);
                    actor.send("stale-receive", Some(super::super::session_id()));
                    let broker = Witness::new(&actor);
                    let management = ConnectionManagement::new();
                    let (mut wire, task) = attached_with_session(
                        &actor,
                        broker.clone(),
                        management.clone(),
                        &actor.entity,
                        mode,
                        None,
                        Some(next),
                    )
                    .await;
                    broker.drift();
                    broker.record_frontier();
                    credit(&mut wire, 1).await;
                    detached_not_found(&mut wire).await;
                    timeout(WAIT, task).await.unwrap().unwrap().unwrap();
                    assert!(management.registered_session(LINK).await.is_none());
                    assert_frontier(&actor, &broker);
                    assert!(matches!(
                        &broker.commands.lock().unwrap()[..],
                        [
                            CommandKind::AcceptSession { .. },
                            CommandKind::Receive { .. },
                            CommandKind::ReleaseSession { .. }
                        ]
                    ));
                    wire.stop().await;
                    drop(broker);
                    actor.reopen();
                }
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn failed_attach_and_late_admission_cleanup_never_recapture_a_drifted_grant() {
    tokio::spawn(async {
    for durable in [false, true] {
        for next in [false, true] {
            for point in [
                PumpPoint::GrantResult,
                PumpPoint::NativeResult,
                PumpPoint::Installed,
                PumpPoint::EntryPrepared,
            ] {
                let mut actor = Actor::new(durable, true);
                actor.send("late-grant", Some(super::super::session_id()));
                let broker = Witness::new(&actor);
                broker.drift_after_grant.store(true, Ordering::SeqCst);
                let management = ConnectionManagement::new();
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire
                    .offer(&mut session, Some(super::super::filter(next)))
                    .await;
                let fault = PumpFault::new(point);
                let mut original = Box::pin(
                    PUMP_FAULT.scope(
                        fault.clone(),
                        AssertUnwindSafe(serve_entity_attachment(
                            &session,
                            &broker,
                            &actor.namespace,
                            "orders",
                            attach,
                            None,
                            &management,
                        ))
                        .catch_unwind(),
                    ),
                );
                tokio::select! {
                    _ = original.as_mut() => panic!("fault must retain the original attachment"),
                    () = fault.reached.notified() => {},
                }
                fault.trigger.notify_one();
                assert!(timeout(WAIT, original.as_mut()).await.unwrap().is_err());
                drop(original);
                assert!(management.registered_session(LINK).await.is_none());
                assert_frontier(&actor, &broker);
                assert!(matches!(
                    &broker.commands.lock().unwrap()[..],
                    [
                        CommandKind::AcceptSession { .. },
                        CommandKind::ReleaseSession { .. }
                    ]
                ));
                // A stale release must not clear the surviving old-generation
                // lock through a freshly captured name. Timer expiry is separate.
                assert!(
                    StateMachine::new(actor.store().clone())
                        .session(&actor.namespace, &actor.entity, &super::super::session_id(),)
                        .unwrap()
                        .unwrap()
                        .lock
                        .is_some()
                );
                wire.stop().await;
                drop(broker);
                actor.reopen();
            }
        }
    }
    }).await.unwrap();
}
