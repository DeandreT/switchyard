//! Real OPEN/Begin admissions; qualified terminal-part controls are separate.
//! Timeouts bound positive fixture reachability, not product IO latency.

use super::connection_family_fixture::pending_once;
use super::connection_family_fixture::*;
use super::*;
use crate::listener::{
    connection_custody::{ConnectionCustody, PUMP_FAULT, PumpFault, PumpPoint},
    connection_session_family::{
        FAMILY_OBSERVER, FamilyObserver, FamilyPoint, PreparedSessionTask,
    },
    serve_open_connection,
    session_custody::SessionTaskExit,
};
use futures_util::FutureExt;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::AtomicUsize,
};
use tracing::instrument::WithSubscriber;

fn same_panic(payload: &(dyn std::any::Any + Send), expected: &Arc<str>) {
    assert!(Arc::ptr_eq(
        payload.downcast_ref::<Arc<str>>().unwrap(),
        expected
    ));
}
fn parent_panic(custody: &mut ConnectionCustody, expected: &Arc<str>) {
    let payload = catch_unwind(AssertUnwindSafe(|| custody.finish_result()))
        .expect_err("same original parent panic after all joins");
    same_panic(&*payload, expected);
}
async fn partial_finish(custody: &mut ConnectionCustody, first: Id) {
    timeout(WAIT, async {
        loop {
            let mut finish = Box::pin(custody.finish());
            pending_once(finish.as_mut()).await;
            drop(finish);
            if custody
                .family()
                .finished()
                .iter()
                .any(|packet| packet.id == first)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("completed first original packet cached while second is held");
}

#[tokio::test(flavor = "current_thread")]
async fn actual_connection_family_begun_acceptance_is_retained_through_native_stop() {
    let actor = Actor::new(false, false);
    let (mut wire, connection) = ConnectionWire::open().await;
    wire.buffered_begin(1).await;
    wire.writes.block();
    let _release = wire.writes.release_on_drop();
    let mut custody = ConnectionCustody::new(connection);
    let notice = custody.request_handle();
    let fault = PumpFault::new(PumpPoint::Native);
    let mut original = Box::pin(
        PUMP_FAULT.scope(
            Arc::clone(&fault),
            AssertUnwindSafe(serve_open_connection(
                &mut custody,
                actor.namespace.clone(),
                actor.broker.as_ref().unwrap().clone(),
                None,
            ))
            .catch_unwind(),
        ),
    );
    drive(original.as_mut(), wire.writes.reached()).await;
    fault.request();
    let primary = timeout(WAIT, original.as_mut()).await.unwrap();
    drop(original);
    assert!(Arc::ptr_eq(
        primary
            .as_ref()
            .unwrap_err()
            .downcast_ref::<Arc<()>>()
            .unwrap(),
        &fault.identity
    ));
    assert!(custody.acceptance_started());
    assert_eq!(custody.acceptance_preparations(), 1);
    assert!(custody.pending_receipt_id().is_none() && custody.family().pending_ids().is_empty());
    custody.record_primary(primary);
    assert!(!notice.is_requested());
    // Stop, not a fabricated Begin packet or a released writer, closes native work.
    timeout(WAIT, custody.finish()).await.unwrap();
    assert!(notice.is_requested() && custody.shutdown_cached());
    assert_eq!(custody.acceptance_preparations(), 1);
    assert!(custody.family().finished().is_empty());
    let payload = catch_unwind(AssertUnwindSafe(|| custody.finish_result()))
        .expect_err("same raw native-checkpoint parent panic");
    assert!(Arc::ptr_eq(
        payload.downcast_ref::<Arc<()>>().unwrap(),
        &fault.identity
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn actual_connection_family_ready_acceptance_survives_context_and_parent_panics() {
    // Context Clone and post-cache parent hook are injected faults around real acceptance.
    for checkpoint_fault in [false, true] {
        let actor = Actor::new(false, false);
        let (mut wire, connection) = ConnectionWire::open().await;
        let mut custody = ConnectionCustody::new(connection);
        let notice = custody.request_handle();
        let calls = Arc::new(AtomicUsize::new(0));
        let context = Arc::<str>::from("same pre-take Broker clone panic");
        let broker = CloneFaultBroker {
            actual: actor.broker.as_ref().unwrap().clone(),
            armed: Arc::new(AtomicBool::new(!checkpoint_fault)),
            calls: Arc::clone(&calls),
            payload: Arc::clone(&context),
        };
        let observer = FamilyObserver::new(
            checkpoint_fault.then_some(FamilyPoint::AcceptedPacket),
            checkpoint_fault,
        );
        let expected = if checkpoint_fault {
            Arc::clone(&observer.payload)
        } else {
            context
        };
        let mut original = Box::pin(
            FAMILY_OBSERVER.scope(
                Arc::clone(&observer),
                AssertUnwindSafe(serve_open_connection(
                    &mut custody,
                    actor.namespace.clone(),
                    broker,
                    None,
                ))
                .catch_unwind(),
            ),
        );
        wire.begin(1).await;
        if checkpoint_fault {
            drive(
                original.as_mut(),
                observed(&observer, |record| {
                    record.point == FamilyPoint::AcceptedPacket
                }),
            )
            .await;
            observer.release.notify_one();
        }
        let primary = timeout(WAIT, original.as_mut()).await.unwrap();
        drop(original);
        same_panic(&**primary.as_ref().unwrap_err(), &expected);
        let cached = facts(&observer, FamilyPoint::AcceptedPacket, 0);
        assert!(cached.accepted_cached && !cached.receipt_cached);
        assert_eq!(cached.pending, 0);
        let address = custody.acceptance_packet_address().unwrap();
        assert_eq!(cached.packet_address, Some(address));
        assert_eq!(custody.acceptance_preparations(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(!checkpoint_fault));
        assert!(custody.pending_receipt_id().is_none());
        let mut ended = Box::pin(custody.accepted_end_observer().unwrap());
        custody.record_primary(primary);
        assert!(!notice.is_requested());
        wire.begin_answer(1).await;
        wire.end(1).await;
        timeout(WAIT, ended.as_mut()).await.unwrap();
        wire.end_answer(1).await;
        timeout(WAIT, custody.finish()).await.unwrap();
        assert_eq!(custody.acceptance_packet_address(), Some(address));
        assert!(custody.family().finished().is_empty());
        parent_panic(&mut custody, &expected);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_connection_family_cached_spawn_receipt_is_adopted_after_parent_panic() {
    let actor = Actor::new(false, false);
    let (mut wire, connection) = ConnectionWire::open().await;
    let mut custody = ConnectionCustody::new(connection);
    let notice = custody.request_handle();
    let observer = FamilyObserver::new(Some(FamilyPoint::ReceiptCached), true);
    let mut original = Box::pin(
        FAMILY_OBSERVER.scope(
            Arc::clone(&observer),
            AssertUnwindSafe(serve_open_connection(
                &mut custody,
                actor.namespace.clone(),
                actor.broker.as_ref().unwrap().clone(),
                None,
            ))
            .catch_unwind(),
        ),
    );
    wire.begin(1).await;
    drive(
        original.as_mut(),
        observed(&observer, |record| {
            record.point == FamilyPoint::ReceiptCached
        }),
    )
    .await;
    let cached = facts(&observer, FamilyPoint::ReceiptCached, 0);
    let id = cached.id.unwrap();
    assert!(cached.receipt_cached && !cached.accepted_cached);
    assert_eq!(cached.pending, 0);
    observer.release.notify_one();
    let primary = timeout(WAIT, original.as_mut()).await.unwrap();
    drop(original);
    assert_eq!(custody.pending_receipt_id(), Some(id));
    assert!(custody.family().pending_ids().is_empty());
    assert_eq!(custody.acceptance_preparations(), 1);
    custody.record_primary(primary);
    assert!(!notice.is_requested());
    timeout(WAIT, custody.finish()).await.unwrap();
    assert!(custody.pending_receipt_id().is_none());
    assert_eq!(custody.family().finished().len(), 1);
    let packet = &custody.family().finished()[0];
    assert_eq!(packet.id, id);
    assert!(packet.joined.is_ok());
    assert!(matches!(
        packet.exit,
        Some(SessionTaskExit::Complete(Ok(())))
    ));
    parent_panic(&mut custody, &observer.payload);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_connection_family_native_shutdown_precedes_original_held_session_joins() {
    let actor = Actor::new(false, false);
    let (mut wire, connection) = ConnectionWire::open().await;
    let mut custody = ConnectionCustody::new(connection);
    let notice = custody.request_handle();
    let observer = FamilyObserver::new(Some(FamilyPoint::NativeShutdownCached), false);
    let fault = PumpFault::new(PumpPoint::Intake);
    let mut original = Box::pin(
        PUMP_FAULT.scope(
            Arc::clone(&fault),
            FAMILY_OBSERVER.scope(
                Arc::clone(&observer),
                AssertUnwindSafe(serve_open_connection(
                    &mut custody,
                    actor.namespace.clone(),
                    actor.broker.as_ref().unwrap().clone(),
                    None,
                ))
                .catch_unwind(),
            ),
        ),
    );
    wire.begin(1).await;
    drive(original.as_mut(), wire.begin_answer(1)).await;
    drive(original.as_mut(), adopted(&observer, 1)).await;
    let id = facts(&observer, FamilyPoint::Adopted, 0).id.unwrap();
    wire.offer(1, 1, false).await;
    drive(original.as_mut(), wire.accepted(1, 1)).await;
    wire.writes.block();
    let _release = wire.writes.release_on_drop();
    wire.send(1, 1, 9).await;
    drive(original.as_mut(), wire.writes.reached()).await;
    drive(original.as_mut(), returned(&actor)).await;
    assert!(!notice.is_requested());
    fault.request();
    let primary = timeout(WAIT, original.as_mut()).await.unwrap();
    drop(original);
    custody.record_primary(primary);
    let mut finish = Box::pin(FAMILY_OBSERVER.scope(Arc::clone(&observer), custody.finish()));
    drive(
        finish.as_mut(),
        observed(&observer, |record| {
            record.point == FamilyPoint::NativeShutdownCached
        }),
    )
    .await;
    let frontier = facts(&observer, FamilyPoint::NativeShutdownCached, 0);
    assert!(frontier.shutdown_cached);
    assert_eq!((frontier.pending, frontier.finished), (1, 0));
    assert!(notice.is_requested());
    observer.release.notify_one();
    timeout(WAIT, finish.as_mut()).await.unwrap();
    drop(finish);
    assert_eq!(custody.family().finished()[0].id, id);
    assert_eq!(invocations(&actor), 1);
    let payload = catch_unwind(AssertUnwindSafe(|| custody.finish_result()))
        .expect_err("original stronger parent panic after native-first cleanup");
    assert!(Arc::ptr_eq(
        payload.downcast_ref::<Arc<()>>().unwrap(),
        &fault.identity
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn actual_connection_family_processed_end_and_reuse_keep_original_admission_identity() {
    // Four actual owner rows: Memory/Fjall x before/after one Send apply.
    for durable in [false, true] {
        for after in [false, true] {
            let mut actor = Actor::new(durable, false);
            let (mut wire, connection) = ConnectionWire::open().await;
            let mut custody = ConnectionCustody::new(connection);
            let notice = custody.request_handle();
            let observer = FamilyObserver::new(Some(FamilyPoint::AcceptedPacket), false);
            let mut original = Box::pin(
                FAMILY_OBSERVER.scope(
                    Arc::clone(&observer),
                    AssertUnwindSafe(serve_open_connection(
                        &mut custody,
                        actor.namespace.clone(),
                        actor.broker.as_ref().unwrap().clone(),
                        None,
                    ))
                    .catch_unwind(),
                ),
            );
            wire.begin(1).await;
            drive(
                original.as_mut(),
                observed(&observer, |record| {
                    record.point == FamilyPoint::AcceptedPacket
                }),
            )
            .await;
            let mut ended = observer.accepted_end.lock().unwrap().take().unwrap();
            observer.release.notify_one();
            drive(original.as_mut(), wire.begin_answer(1)).await;
            drive(original.as_mut(), adopted(&observer, 1)).await;
            let old_id = facts(&observer, FamilyPoint::Adopted, 0).id.unwrap();
            wire.offer(1, 1, false).await;
            drive(original.as_mut(), wire.accepted(1, 1)).await;
            actor.clock.set(2_000);
            actor.gate.arm_put(actor.key(SequenceNumber::new(1)));
            if !after {
                actor.gate.release(true);
            }
            wire.send(1, 1, 10).await;
            drive(original.as_mut(), actor.gate.reached(false)).await;
            if after {
                actor.gate.release(false);
                drive(original.as_mut(), actor.gate.reached(true)).await;
            }
            assert_eq!(invocations(&actor), 1);
            assert_eq!(
                actor
                    .store()
                    .get(&actor.key(SequenceNumber::new(1)))
                    .unwrap()
                    .is_some(),
                after
            );
            actor.clock.set(3_000);
            wire.writes.block();
            let _release = wire.writes.release_on_drop();
            wire.end(1).await;
            drive(original.as_mut(), ended.as_mut()).await;
            drive(original.as_mut(), wire.writes.reached()).await;
            let mut answer = Box::pin(wire.end_answer(1));
            pending_once(answer.as_mut()).await;
            drop(answer);
            // SAME cached acceptance observer is consumed once; held answer is a later frontier.
            wire.writes.release();
            drive(original.as_mut(), wire.end_answer(1)).await;
            wire.begin(1).await;
            drive(
                original.as_mut(),
                checkpoint_count(&observer, FamilyPoint::AcceptedPacket, 2),
            )
            .await;
            observer.release.notify_one();
            drive(original.as_mut(), wire.begin_answer(1)).await;
            drive(original.as_mut(), adopted(&observer, 2)).await;
            let new_id = facts(&observer, FamilyPoint::Adopted, 1).id.unwrap();
            assert_ne!(new_id, old_id);
            assert_eq!(facts(&observer, FamilyPoint::Adopted, 1).pending, 2);
            assert!(!notice.is_requested());
            wire.offer(1, 2, false).await;
            drive(original.as_mut(), wire.accepted(1, 2)).await;
            actor.gate.release_all();
            drive(original.as_mut(), reaped(&observer, old_id)).await;
            assert_eq!(invocations(&actor), 1);
            assert_eq!(actor.gate.state.lock().unwrap().commits, 1);
            assert_eq!(
                StateMachine::new(actor.store().clone())
                    .last_applied_time()
                    .unwrap()
                    .as_millis(),
                2_000
            );
            wire.send(1, 2, 11).await;
            drive(original.as_mut(), wire.accepted_send(1)).await;
            assert_eq!(invocations(&actor), 2);
            wire.end(1).await;
            drive(original.as_mut(), wire.end_answer(1)).await;
            drive(original.as_mut(), reaped(&observer, new_id)).await;
            assert!(!notice.is_requested());
            notice.request();
            let primary = timeout(WAIT, original.as_mut()).await.unwrap();
            drop(original);
            custody.record_primary(primary);
            timeout(WAIT, custody.finish()).await.unwrap();
            custody.finish_result().unwrap();
            drop(custody);
            let before = actor.store().snapshot().unwrap();
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_connection_family_twice_cancelled_finish_keeps_completed_first_and_store_held_second()
 {
    for after in [false, true] {
        let actor = Actor::new(false, false);
        let (mut wire, connection) = ConnectionWire::open().await;
        let mut custody = ConnectionCustody::new(connection);
        let notice = custody.request_handle();
        let observer = FamilyObserver::new(None, false);
        let mut original = Box::pin(
            FAMILY_OBSERVER.scope(
                Arc::clone(&observer),
                AssertUnwindSafe(serve_open_connection(
                    &mut custody,
                    actor.namespace.clone(),
                    actor.broker.as_ref().unwrap().clone(),
                    None,
                ))
                .catch_unwind(),
            ),
        );
        for channel in 1..=2 {
            wire.begin(channel).await;
            drive(original.as_mut(), wire.begin_answer(channel)).await;
            drive(original.as_mut(), adopted(&observer, channel as usize)).await;
        }
        let first = facts(&observer, FamilyPoint::Adopted, 0).id.unwrap();
        let second = facts(&observer, FamilyPoint::Adopted, 1).id.unwrap();
        wire.offer(2, 1, false).await;
        drive(original.as_mut(), wire.accepted(2, 1)).await;
        actor.gate.arm_put(actor.key(SequenceNumber::new(1)));
        if !after {
            actor.gate.release(true);
        }
        wire.send(2, 1, 12).await;
        drive(original.as_mut(), actor.gate.reached(false)).await;
        if after {
            actor.gate.release(false);
            drive(original.as_mut(), actor.gate.reached(true)).await;
        }
        // Keep the SAME parent borrower unpolled so it cannot live-reap the idle first.
        wire.end(1).await;
        wire.end_answer(1).await;
        notice.request();
        let primary = timeout(WAIT, original.as_mut()).await.unwrap();
        drop(original);
        custody.record_primary(primary);
        partial_finish(&mut custody, first).await;
        assert_eq!(custody.family().finished().len(), 1);
        assert_eq!(custody.family().pending_ids(), vec![second]);
        let address = packet_address(&custody.family().finished()[0]);
        for _ in 0..2 {
            let mut finish = Box::pin(custody.finish());
            pending_once(finish.as_mut()).await;
            drop(finish);
            assert_eq!(custody.family().finished()[0].id, first);
            assert_eq!(packet_address(&custody.family().finished()[0]), address);
            assert_eq!(custody.family().pending_ids(), vec![second]);
            assert_eq!(invocations(&actor), 1);
        }
        actor.gate.release_all();
        timeout(WAIT, custody.finish()).await.unwrap();
        assert_eq!(custody.family().finished().len(), 2);
        assert_eq!(custody.family().finished()[1].id, second);
        assert_eq!(packet_address(&custody.family().finished()[0]), address);
        assert_eq!(actor.gate.state.lock().unwrap().commits, 1);
        custody.finish_result().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_connection_family_typed_refusal_report_keeps_raw_payload_without_new_stop() {
    let actor = Actor::new(false, false);
    let (mut wire, connection) = ConnectionWire::open().await;
    let mut custody = ConnectionCustody::new(connection);
    let notice = custody.request_handle();
    let observer = FamilyObserver::new(None, false);
    let reached = Arc::new(AtomicUsize::new(0));
    let family_reached = Arc::new(AtomicUsize::new(0));
    let report = Arc::<str>::from("same actual successful-refusal reporting panic");
    let dispatch = tracing::Dispatch::new(ReportFault {
        attachment_reached: Arc::clone(&reached),
        family_reached: Arc::clone(&family_reached),
        attachment: Arc::clone(&report),
        family: Arc::from("unexpected diagnostic-only family reporter"),
    });
    let other = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    std::thread::spawn(tracing::callsite::rebuild_interest_cache)
        .join()
        .unwrap();
    let mut original = Box::pin(
        FAMILY_OBSERVER
            .scope(
                Arc::clone(&observer),
                AssertUnwindSafe(serve_open_connection(
                    &mut custody,
                    actor.namespace.clone(),
                    actor.broker.as_ref().unwrap().clone(),
                    None,
                ))
                .catch_unwind(),
            )
            .with_subscriber(dispatch),
    );
    wire.begin(1).await;
    drive(original.as_mut(), wire.begin_answer(1)).await;
    drive(original.as_mut(), adopted(&observer, 1)).await;
    let report_id = facts(&observer, FamilyPoint::Adopted, 0).id.unwrap();
    wire.offer(1, 1, true).await;
    drive(original.as_mut(), wire.refused(1, 1)).await;
    drive(original.as_mut(), reaped(&observer, report_id)).await;
    assert_eq!(reached.load(Ordering::SeqCst), 1);
    assert_eq!(family_reached.load(Ordering::SeqCst), 0);
    assert!(!notice.is_requested());
    // A second session on this SAME live connection is admitted and services actual data.
    wire.begin(2).await;
    drive(original.as_mut(), wire.begin_answer(2)).await;
    drive(original.as_mut(), adopted(&observer, 2)).await;
    let independent = facts(&observer, FamilyPoint::Adopted, 1).id.unwrap();
    assert_ne!(independent, report_id);
    wire.offer(2, 1, false).await;
    drive(original.as_mut(), wire.accepted(2, 1)).await;
    wire.send(2, 1, 13).await;
    drive(original.as_mut(), wire.accepted_send(2)).await;
    assert_eq!(invocations(&actor), 1);
    wire.end(2).await;
    drive(original.as_mut(), wire.end_answer(2)).await;
    drive(original.as_mut(), reaped(&observer, independent)).await;
    assert!(!notice.is_requested());
    notice.request();
    let primary = timeout(WAIT, original.as_mut()).await.unwrap();
    drop(original);
    custody.record_primary(primary);
    timeout(WAIT, custody.finish()).await.unwrap();
    let payload = custody.family().failures().report_only.as_ref().unwrap();
    same_panic(&**payload, &report);
    let address = &**payload as *const _ as *const () as usize;
    let payload = catch_unwind(AssertUnwindSafe(|| custody.finish_result()))
        .expect_err("raw report-only packet, not outer JoinError");
    assert_eq!(&*payload as *const _ as *const () as usize, address);
    same_panic(&*payload, &report);
    drop(other);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_connection_family_live_reaping_discards_healthy_join_history() {
    let actor = Actor::new(false, false);
    let (mut wire, connection) = ConnectionWire::open().await;
    let mut custody = ConnectionCustody::new(connection);
    let notice = custody.request_handle();
    let observer = FamilyObserver::new(None, false);
    let mut original = Box::pin(
        FAMILY_OBSERVER.scope(
            Arc::clone(&observer),
            AssertUnwindSafe(serve_open_connection(
                &mut custody,
                actor.namespace.clone(),
                actor.broker.as_ref().unwrap().clone(),
                None,
            ))
            .catch_unwind(),
        ),
    );
    for turn in 0..12 {
        wire.begin(1).await;
        drive(original.as_mut(), wire.begin_answer(1)).await;
        drive(original.as_mut(), adopted(&observer, turn + 1)).await;
        let id = facts(&observer, FamilyPoint::Adopted, turn).id.unwrap();
        wire.offer(1, 1, false).await;
        drive(original.as_mut(), wire.accepted(1, 1)).await;
        wire.end(1).await;
        drive(original.as_mut(), wire.end_answer(1)).await;
        drive(original.as_mut(), reaped(&observer, id)).await;
        let retained = facts(&observer, FamilyPoint::LiveReaped, turn);
        assert_eq!(
            (retained.pending, retained.live_ready, retained.finished),
            (0, 0, 0)
        );
        assert!(!notice.is_requested());
    }
    notice.request();
    let primary = timeout(WAIT, original.as_mut()).await.unwrap();
    drop(original);
    custody.record_primary(primary);
    timeout(WAIT, custody.finish()).await.unwrap();
    assert!(custody.family().finished().is_empty() && custody.family().pending_ids().is_empty());
    custody.finish_result().unwrap();
    assert_eq!(
        observer
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|record| record.point == FamilyPoint::Adopted)
            .count(),
        12
    );
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_connection_custody_late_original_after_drain_keeps_cached_primary() {
    // Real OPEN/native shutdown, but manually composed session receipts, not wire admission.
    for adopt_first in [false, true] {
        let (_wire, connection) = ConnectionWire::open().await;
        let mut custody = ConnectionCustody::new(connection);
        let primary = Arc::<str>::from("same cached primary before late receipt");
        let payload = Box::new(Arc::clone(&primary));
        let primary_address = &*payload as *const _ as usize;
        custody.record_primary(Err(payload));
        timeout(WAIT, custody.finish()).await.unwrap();
        assert!(custody.shutdown_cached() && custody.family().finished().is_empty());
        let (release, held) = tokio::sync::oneshot::channel();
        let (entered, reached) = tokio::sync::oneshot::channel();
        let runs = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&runs);
        let receipt = PreparedSessionTask::new().spawn(async move {
            count.fetch_add(1, Ordering::SeqCst);
            let _ = entered.send(());
            held.await.unwrap()
        });
        let id = receipt.id;
        custody.capture_session(receipt);
        if adopt_first {
            assert_eq!(custody.adopt_pending(), Some(id));
        }
        // This assertion must precede raw primary/category extraction.
        for _ in 0..2 {
            assert!(catch_unwind(AssertUnwindSafe(|| custody.finish_result())).is_err());
        }
        timeout(WAIT, reached).await.unwrap().unwrap();
        for _ in 0..2 {
            let mut finish = Box::pin(custody.finish());
            pending_once(finish.as_mut()).await;
            drop(finish);
            assert_eq!(custody.family().pending_ids(), vec![id]);
        }
        let report = Arc::<str>::from("lower late original report-only payload");
        assert!(
            release
                .send(SessionTaskExit::ReportOnly(Box::new(report)))
                .is_ok()
        );
        timeout(WAIT, custody.finish()).await.unwrap();
        assert_eq!(custody.family().finished()[0].id, id);
        let payload = catch_unwind(AssertUnwindSafe(|| custody.finish_result()))
            .expect_err("original primary survives rejected early resolutions");
        assert_eq!(&*payload as *const _ as *const () as usize, primary_address);
        same_panic(&*payload, &primary);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}
