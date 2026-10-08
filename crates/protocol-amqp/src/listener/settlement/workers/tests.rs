//! Original settlement workers on real native links and the actual broker owner.

use amqp::{Detach, Flow, Frame, Performative, ReceiverSettleMode, read_frame, write_frame};
use domain::{CommandKind, CommandOutcome, Delivery, ReceiveMode, SessionHold, SessionId};
use std::{collections::HashSet, pin::Pin, sync::Arc};
use storage::StateStore;
use tokio::{task::Id, time::timeout};

use super::*;
use crate::listener::{
    ReceivingLinkProtocol,
    settlement::{serve_receiving_client, settle_started_delivery, test_support::*},
};
use crate::management::ConnectionManagement;

async fn start_worker(
    workers: &mut SettlementWorkers,
    wire: &mut Wire,
    actor: &Actor,
    delivery: Delivery,
    management: Arc<ConnectionManagement>,
) -> (Id, u32) {
    let token = delivery.lock.unwrap().token;
    management
        .register_delivery(LINK, actor.entity.clone(), delivery.sequence, token)
        .await;
    let (pending, wire_id) = wire.start(&delivery).await;
    let retired = workers.subscribe();
    let id = workers.spawn(
        Some(token),
        settle_started_delivery(pending, delivery, actor.context(management), retired),
    );
    (id, wire_id)
}

fn assert_original_finished(workers: &SettlementWorkers, id: Id, token: domain::LockToken) {
    assert!(workers.is_empty());
    assert_eq!(workers.finished().len(), 1);
    let joined = &workers.finished()[0];
    assert_eq!(joined.id, id);
    assert_eq!(joined.lock_token, Some(token));
    let completion = joined
        .result
        .as_ref()
        .expect("original worker joined normally");
    assert_eq!(completion.lock_token, Some(token));
    assert!(completion.result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn unanswered_actual_outcomes_retire_without_new_settlement_commands() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut actor = Actor::new(durable, false);
            actor.send("unanswered", None);
            let delivery = actor.receive();
            let token = delivery.lock.unwrap().token;
            let before = actor.store().snapshot().unwrap();
            let mut wire = Wire::new(mode).await;
            let mut workers = SettlementWorkers::new();
            let (id, _) = start_worker(
                &mut workers,
                &mut wire,
                &actor,
                delivery,
                ConnectionManagement::new(),
            )
            .await;
            let mut observer = Box::pin(workers.finish());
            pending_once(observer.as_mut()).await;
            drop(observer);
            assert_eq!(workers.len(), 1);
            assert_eq!(workers.pending.iter().next().unwrap().id, id);
            timeout(WAIT, workers.finish()).await.unwrap();
            assert_original_finished(&workers, id, token);
            assert_eq!(actor.complete_count(), 0);
            assert_eq!(actor.store().snapshot().unwrap(), before);
            timeout(WAIT, workers.finish()).await.unwrap();
            assert_original_finished(&workers, id, token);
            wire.stop().await;
            drop(workers);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_broker_submission_drains_across_cancelled_finish_and_reopen() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            for after_commit in [false, true] {
                let mut actor = Actor::new(durable, false);
                actor.send("commit", None);
                let delivery = actor.receive();
                let sequence = delivery.sequence;
                let token = delivery.lock.unwrap().token;
                let key = actor.key(sequence);
                let lock_key = domain::keys::lock(
                    &actor.namespace,
                    &actor.entity,
                    delivery.lock.unwrap().locked_until,
                    sequence,
                );
                let before = actor.store().snapshot().unwrap();
                let expected: Vec<_> = before
                    .entries()
                    .iter()
                    .filter(|(stored, _)| stored != &key && stored != &lock_key)
                    .cloned()
                    .collect();
                actor.gate.arm(key.clone());
                let mut wire = Wire::new(mode.clone()).await;
                let mut workers = SettlementWorkers::new();
                let (id, wire_id) = start_worker(
                    &mut workers,
                    &mut wire,
                    &actor,
                    delivery,
                    ConnectionManagement::new(),
                )
                .await;
                wire.accepted(wire_id).await;
                actor.gate.reached(false).await;
                if after_commit {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                    assert!(actor.store().get(&key).unwrap().is_none());
                } else {
                    assert_eq!(actor.store().snapshot().unwrap(), before);
                }
                assert_eq!(actor.complete_count(), 1);
                {
                    let log = actor.log.lock().unwrap();
                    assert!(
                        log.iter()
                            .any(|entry| entry.worker == id && !entry.returned)
                    );
                    assert!(!log.iter().any(|entry| entry.returned));
                }
                wire.no_queued_frame().await;
                let mut observer = Box::pin(workers.finish());
                pending_once(observer.as_mut()).await;
                drop(observer);
                assert_eq!(workers.len(), 1);
                assert_eq!(workers.pending.iter().next().unwrap().id, id);
                assert!(workers.finished().is_empty());
                if !after_commit {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                }
                actor.gate.release(true);
                timeout(WAIT, workers.finish()).await.unwrap();
                assert_original_finished(&workers, id, token);
                wire.no_queued_frame().await;
                assert_eq!(actor.complete_count(), 1);
                assert_eq!(actor.gate.state.lock().unwrap().commits, 1);
                assert!(
                    actor
                        .log
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|entry| entry.worker == id && entry.returned)
                );
                let committed = actor.store().snapshot().unwrap();
                assert_eq!(committed.entries(), expected.as_slice());
                assert!(actor.store().get(&key).unwrap().is_none());
                timeout(WAIT, workers.finish()).await.unwrap();
                assert_original_finished(&workers, id, token);
                wire.stop().await;
                drop(workers);
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), committed);
                assert!(actor.store().get(&key).unwrap().is_none());
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_second_confirmation_write_retires_without_resubmission() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        actor.send("confirmation", None);
        let delivery = actor.receive();
        let token = delivery.lock.unwrap().token;
        let key = actor.key(delivery.sequence);
        actor.gate.arm(key.clone());
        let mut wire = Wire::new(ReceiverSettleMode::Second).await;
        let mut workers = SettlementWorkers::new();
        let (id, wire_id) = start_worker(
            &mut workers,
            &mut wire,
            &actor,
            delivery,
            ConnectionManagement::new(),
        )
        .await;
        wire.accepted(wire_id).await;
        actor.gate.reached(false).await;
        actor.gate.release(false);
        actor.gate.reached(true).await;
        wire.writes.block();
        actor.gate.release(true);
        wire.writes.reached().await;
        assert_eq!(actor.complete_count(), 1);
        assert!(actor.store().get(&key).unwrap().is_none());
        timeout(WAIT, workers.finish()).await.unwrap();
        assert_original_finished(&workers, id, token);
        assert_eq!(actor.complete_count(), 1);
        let committed = actor.store().snapshot().unwrap();
        wire.stop().await;
        drop(workers);
        actor.reopen();
        assert_eq!(actor.store().snapshot().unwrap(), committed);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_ready_remote_outcome_wins_sticky_retirement() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut actor = Actor::new(durable, false);
            actor.send("ready-frontier", None);
            let delivery = actor.receive();
            let token = delivery.lock.unwrap().token;
            let key = actor.key(delivery.sequence);
            let mut wire = Wire::new(mode).await;
            let management = ConnectionManagement::new();
            management
                .register_delivery(LINK, actor.entity.clone(), delivery.sequence, token)
                .await;
            // Retain the genuine PendingDelivery without polling its outcome.
            let (pending, wire_id) = wire.start(&delivery).await;
            wire.accepted(wire_id).await;
            write_frame(
                &mut wire.peer,
                &frame(Performative::Flow(Flow {
                    handle: Some(HANDLE),
                    delivery_count: Some(1),
                    link_credit: Some(0),
                    drain: true,
                    incoming_window: 2048,
                    outgoing_window: 2048,
                    ..Flow::default()
                })),
            )
            .await
            .unwrap();
            let Performative::Flow(drained) = performative(&mut wire.peer).await else {
                panic!("positive native drain response");
            };
            assert_eq!(drained.handle, Some(HANDLE));
            assert_eq!(drained.delivery_count, Some(1));
            assert_eq!(drained.link_credit, Some(0));
            assert!(drained.drain);
            // The engine processed Accepted before this actual Flow response.
            let mut workers = SettlementWorkers::new();
            let retirement = workers.subscribe();
            let id = workers.spawn(
                Some(token),
                settle_started_delivery(pending, delivery, actor.context(management), retirement),
            );
            workers.retire();
            timeout(WAIT, workers.finish()).await.unwrap();
            assert_original_finished(&workers, id, token);
            assert_eq!(actor.complete_count(), 1);
            assert!(actor.store().get(&key).unwrap().is_none());
            let committed = actor.store().snapshot().unwrap();
            wire.stop().await;
            drop(workers);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), committed);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn later_delivery_settles_while_an_earlier_remote_outcome_is_unanswered() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut actor = Actor::new(durable, false);
            actor.send("earlier", None);
            actor.send("later", None);
            let earlier = actor.receive();
            let later = actor.receive();
            let earlier_key = actor.key(earlier.sequence);
            let later_key = actor.key(later.sequence);
            let mut wire = Wire::new(mode).await;
            let mut workers = SettlementWorkers::new();
            start_worker(
                &mut workers,
                &mut wire,
                &actor,
                earlier,
                ConnectionManagement::new(),
            )
            .await;
            let (_, wire_id) = start_worker(
                &mut workers,
                &mut wire,
                &actor,
                later,
                ConnectionManagement::new(),
            )
            .await;
            wire.accepted(wire_id).await;
            let completion = timeout(WAIT, workers.next()).await.unwrap().unwrap();
            assert!(completion.result.is_ok());
            if wire.mode == ReceiverSettleMode::Second {
                wire.confirmed(wire_id).await;
            }
            assert_eq!(workers.len(), 1);
            assert!(actor.store().get(&earlier_key).unwrap().is_some());
            assert!(actor.store().get(&later_key).unwrap().is_none());
            assert_eq!(actor.complete_count(), 1);
            timeout(WAIT, workers.finish()).await.unwrap();
            assert_eq!(actor.complete_count(), 1);
            wire.stop().await;
            drop(workers);
            actor.reopen();
            assert!(actor.store().get(&earlier_key).unwrap().is_some());
            assert!(actor.store().get(&later_key).unwrap().is_none());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn original_real_worker_panic_retains_token_id_and_payload_through_next() {
    let actor = Actor::new(false, false);
    actor.send("aborted-worker", None);
    let delivery = actor.receive();
    let token = delivery.lock.unwrap().token;
    let mut wire = Wire::new(ReceiverSettleMode::Second).await;
    let mut workers = SettlementWorkers::new();
    let (pending, wire_id) = wire.start(&delivery).await;
    let retirement = workers.subscribe();
    let context = actor.context(ConnectionManagement::new());
    let id = workers.spawn(Some(token), async move {
        settle_started_delivery(pending, delivery, context, retirement).await?;
        panic!("original-settlement-panic");
    });
    wire.accepted(wire_id).await;
    wire.confirmed(wire_id).await;
    let completion = timeout(WAIT, workers.next()).await.unwrap().unwrap();
    assert!(completion.lock_token.is_none());
    assert!(matches!(
        &completion.result,
        Err(super::super::SettlementFailure::Engine(
            amqp::EngineError::Stopped
        ))
    ));
    let mut registered_locks = HashSet::from([token]);
    assert!(matches!(
        super::super::handle_completion(completion, &mut registered_locks),
        Some(super::super::PumpExit::Clean)
    ));
    assert!(registered_locks.contains(&token));
    assert_eq!(workers.failures().len(), 1);
    let failure = &workers.failures()[0];
    assert_eq!(failure.id, id);
    assert_eq!(failure.lock_token, Some(token));
    assert_eq!(failure.error.id(), id);
    assert!(failure.error.is_panic());
    timeout(WAIT, workers.finish()).await.unwrap();
    assert_eq!(workers.failures()[0].id, id);
    let error = workers.into_join_error().expect("original raw panic");
    assert_eq!(error.id(), id);
    let payload = error.into_panic();
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"original-settlement-panic")
    );
    assert_eq!(actor.complete_count(), 1);
    wire.stop().await;
}

async fn originals_completed(workers: &SettlementWorkers) {
    timeout(WAIT, async {
        while workers
            .pending
            .iter()
            .any(|task| !task.handle.is_finished())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual original tasks reached completion");
}

#[tokio::test(flavor = "current_thread")]
async fn finish_retains_a_completed_original_before_a_cancelled_pending_join() {
    let actor = Actor::new(false, false);
    actor.send("already-completed", None);
    actor.send("still-submitted", None);
    let completed = actor.receive();
    let submitted = actor.receive();
    actor.gate.arm(actor.key(submitted.sequence));
    let mut wire = Wire::new(ReceiverSettleMode::First).await;
    let mut workers = SettlementWorkers::new();
    let (completed_id, wire_id) = start_worker(
        &mut workers,
        &mut wire,
        &actor,
        completed,
        ConnectionManagement::new(),
    )
    .await;
    wire.accepted(wire_id).await;
    originals_completed(&workers).await;
    let (submitted_id, wire_id) = start_worker(
        &mut workers,
        &mut wire,
        &actor,
        submitted,
        ConnectionManagement::new(),
    )
    .await;
    wire.accepted(wire_id).await;
    actor.gate.reached(false).await;
    let mut observer = Box::pin(workers.finish());
    pending_once(observer.as_mut()).await;
    drop(observer);
    assert_eq!(workers.len(), 1);
    assert_eq!(workers.pending.iter().next().unwrap().id, submitted_id);
    assert_eq!(workers.finished().len(), 1);
    assert_eq!(workers.finished()[0].id, completed_id);
    assert!(
        workers.finished()[0]
            .result
            .as_ref()
            .unwrap()
            .result
            .is_ok()
    );
    actor.gate.release_all();
    timeout(WAIT, workers.finish()).await.unwrap();
    assert_eq!(workers.finished().len(), 2);
    assert_eq!(workers.finished()[0].id, completed_id);
    assert_eq!(workers.finished()[1].id, submitted_id);
    assert!(
        workers
            .finished()
            .iter()
            .all(|joined| joined.result.as_ref().unwrap().result.is_ok())
    );
    timeout(WAIT, workers.finish()).await.unwrap();
    assert_eq!(workers.finished().len(), 2);
    assert_eq!(actor.complete_count(), 2);
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn thirty_two_completed_unreaped_real_workers_still_consume_the_bound() {
    let actor = Actor::new(false, false);
    for marker in 0..33 {
        actor.send(&format!("bounded-{marker}"), None);
    }
    let mut wire = Wire::new(ReceiverSettleMode::First).await;
    let mut workers = SettlementWorkers::new();
    let mut ids = Vec::new();
    for _ in 0..32 {
        let (id, wire_id) = start_worker(
            &mut workers,
            &mut wire,
            &actor,
            actor.receive(),
            ConnectionManagement::new(),
        )
        .await;
        ids.push(id);
        wire.accepted(wire_id).await;
    }
    originals_completed(&workers).await;
    assert_eq!(workers.len(), 32);
    let extra = actor.receive();
    let key = actor.key(extra.sequence);
    let (pending, _) = wire.start(&extra).await;
    let token = extra.lock.unwrap().token;
    let retirement = workers.subscribe();
    let context = actor.context(ConnectionManagement::new());
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        workers.spawn(
            Some(token),
            settle_started_delivery(pending, extra, context, retirement),
        );
    }));
    assert!(
        refused.is_err(),
        "completed originals must be reaped before slot reuse"
    );
    assert_eq!(workers.len(), 32);
    timeout(WAIT, workers.finish()).await.unwrap();
    assert!(workers.is_empty());
    assert_eq!(workers.finished().len(), 32);
    assert!(
        workers
            .finished()
            .iter()
            .all(|joined| ids.contains(&joined.id))
    );
    assert_eq!(actor.complete_count(), 32);
    assert!(actor.store().get(&key).unwrap().is_some());
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn natural_pump_joins_commit_before_releasing_its_held_session() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut actor = Actor::new(durable, true);
            let session_id = SessionId::new("session").unwrap();
            let sequence = actor.send("pump-commit", Some(session_id.clone()));
            actor.send("pump-unanswered", Some(session_id.clone()));
            let CommandOutcome::SessionAccepted(Some(accepted)) =
                actor.intent(CommandKind::AcceptSession {
                    session_id: Some(session_id),
                    lock_duration_millis: None,
                })
            else {
                panic!("session accepted by actual broker");
            };
            let hold: SessionHold = accepted.hold();
            actor.gate.arm(actor.key(sequence));
            let mut wire = Wire::new(mode).await;
            let management = ConnectionManagement::new();
            let sender = wire.sender.take().unwrap();
            let broker = actor.broker.as_ref().unwrap().clone();
            let namespace = actor.namespace.clone();
            let entity = actor.entity.clone();
            let protocol = ReceivingLinkProtocol {
                authorization: None,
                management: Arc::clone(&management),
            };
            let mut pump = tokio::spawn(serve_receiving_client(
                sender,
                namespace,
                entity,
                broker,
                ReceiveMode::PeekLock,
                Some(hold),
                protocol,
            ));
            let mut ids = Vec::new();
            let mut unanswered_token = None;
            for _ in 0..2 {
                let Frame::Amqp {
                    performative: Some(Performative::Transfer(transfer)),
                    ..
                } = timeout(WAIT, read_frame(&mut wire.peer))
                    .await
                    .unwrap()
                    .unwrap()
                else {
                    panic!("actual pump Transfer");
                };
                ids.push(transfer.delivery_id.unwrap());
                let tag: &[u8] = transfer.delivery_tag.as_ref().unwrap().as_ref();
                unanswered_token = Some(domain::LockToken::new(u64::from_be_bytes(
                    tag[8..].try_into().unwrap(),
                )));
            }
            let unanswered_token = unanswered_token.unwrap();
            assert!(management.delivery(LINK, unanswered_token).await.is_some());
            wire.accepted(ids[0]).await;
            actor.gate.reached(false).await;
            write_frame(
                &mut wire.peer,
                &frame(Performative::Detach(Detach {
                    handle: HANDLE,
                    closed: true,
                    error: None,
                })),
            )
            .await
            .unwrap();
            assert!(matches!(
                performative(&mut wire.peer).await,
                Performative::Detach(_)
            ));
            pending_once(Pin::new(&mut pump)).await;
            assert!(
                !actor
                    .log
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|entry| matches!(entry.kind, CommandKind::ReleaseSession { .. }))
            );
            actor.gate.release(false);
            actor.gate.reached(true).await;
            pending_once(Pin::new(&mut pump)).await;
            assert!(
                !actor
                    .log
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|entry| matches!(entry.kind, CommandKind::ReleaseSession { .. }))
            );
            actor.gate.release(true);
            timeout(WAIT, &mut pump)
                .await
                .expect("natural original pump finished")
                .unwrap()
                .unwrap();
            assert!(management.delivery(LINK, unanswered_token).await.is_none());
            {
                let log = actor.log.lock().unwrap();
                let committed = log
                    .iter()
                    .position(|entry| {
                        entry.returned && matches!(entry.kind, CommandKind::Complete { .. })
                    })
                    .unwrap();
                let release = log
                    .iter()
                    .position(|entry| {
                        !entry.returned && matches!(entry.kind, CommandKind::ReleaseSession { .. })
                    })
                    .unwrap();
                assert!(committed < release);
            }
            assert_eq!(actor.complete_count(), 1);
            let committed = actor.store().snapshot().unwrap();
            wire.stop().await;
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), committed);
        }
    }
}

// These controls exercise owner limits, not fabricated native settlement outcomes.
#[tokio::test(flavor = "current_thread")]
async fn late_adoption_preserves_retained_raw_failure_and_allows_only_one_original() {
    let mut workers = SettlementWorkers::new();
    let token = domain::LockToken::new(900);
    let failed_id = workers.spawn(Some(token), async { panic!("retained-before-adoption") });
    let completion = timeout(WAIT, workers.next()).await.unwrap().unwrap();
    assert!(completion.lock_token.is_none());
    assert_eq!(workers.failures()[0].id, failed_id);
    assert_eq!(workers.failures()[0].lock_token, Some(token));
    assert!(workers.failures()[0].error.is_panic());
    assert!(workers.finished().is_empty());
    let ordinary = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        workers.spawn(None, async { Ok(()) });
    }));
    assert!(ordinary.is_err());
    let adopted_id = workers.adopt_retired(None, async { Ok(()) });
    let duplicate = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        workers.adopt_retired(None, async { Ok(()) });
    }));
    assert!(duplicate.is_err());
    timeout(WAIT, workers.finish()).await.unwrap();
    assert_eq!(workers.finished().len(), 1);
    assert_eq!(workers.finished()[0].id, adopted_id);
    assert!(
        workers.finished()[0]
            .result
            .as_ref()
            .unwrap()
            .result
            .is_ok()
    );
    assert_eq!(workers.failures()[0].id, failed_id);
    let failure = workers.into_join_error().unwrap();
    assert_eq!(failure.id(), failed_id);
    assert_eq!(
        failure.into_panic().downcast_ref::<&str>(),
        Some(&"retained-before-adoption")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn first_finish_poll_permanently_closes_late_adoption_even_without_cached_joins() {
    let mut empty = SettlementWorkers::new();
    assert!(empty.finish().await.is_empty());
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        empty.adopt_retired(None, async { Ok(()) });
    }));
    assert!(
        refused.is_err(),
        "an empty completed drain still closes admission"
    );

    let mut workers = SettlementWorkers::new();
    let (release, paused) = tokio::sync::oneshot::channel();
    let original_id = workers.spawn(None, async {
        paused.await.unwrap();
        Ok(())
    });
    let mut borrowed = Box::pin(workers.finish());
    pending_once(borrowed.as_mut()).await;
    drop(borrowed);
    assert!(workers.finished().is_empty());
    assert_eq!(workers.len(), 1);
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        workers.adopt_retired(None, async { Ok(()) });
    }));
    assert!(
        refused.is_err(),
        "cancelling the observer cannot reopen admission"
    );
    release.send(()).unwrap();
    timeout(WAIT, workers.finish()).await.unwrap();
    assert_eq!(workers.finished()[0].id, original_id);
}

#[tokio::test(flavor = "current_thread")]
async fn late_adoption_counts_unreaped_originals_and_retained_failures_toward_thirty_two() {
    let mut workers = SettlementWorkers::new();
    let ids: Vec<_> = (0..31)
        .map(|_| workers.spawn(None, async { Ok(()) }))
        .collect();
    originals_completed(&workers).await;
    workers.retire();
    let last = workers.adopt_retired(None, async { Ok(()) });
    assert_eq!(workers.len(), 32);
    timeout(WAIT, workers.finish()).await.unwrap();
    assert_eq!(workers.finished().len(), 32);
    assert!(
        workers
            .finished()
            .iter()
            .all(|joined| joined.id == last || ids.contains(&joined.id))
    );

    let mut workers = SettlementWorkers::new();
    let failed = workers.spawn(None, async { panic!("failure-consumes-capacity") });
    let mut releases = Vec::new();
    let mut original_ids = Vec::new();
    for _ in 0..31 {
        let (release, paused) = tokio::sync::oneshot::channel();
        releases.push(release);
        original_ids.push(workers.spawn(None, async {
            paused.await.unwrap();
            Ok(())
        }));
    }
    timeout(WAIT, workers.next()).await.unwrap().unwrap();
    assert_eq!(workers.failures()[0].id, failed);
    assert_eq!(workers.len() + workers.failures().len(), 32);
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        workers.adopt_retired(None, async { Ok(()) });
    }));
    assert!(refused.is_err());
    for release in releases {
        release.send(()).unwrap();
    }
    timeout(WAIT, workers.finish()).await.unwrap();
    assert!(
        workers
            .finished()
            .iter()
            .all(|joined| original_ids.contains(&joined.id))
    );
    assert_eq!(workers.failures()[0].id, failed);
    assert_eq!(workers.into_join_error().unwrap().id(), failed);
}
