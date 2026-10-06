use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::sync::Semaphore;

use super::super::{RetainedAtomicMessagingDrain as Drain, outcomes::locked};
use super::fixture::{
    Anchor, Fixture, TestResult, caught, controller, facts, payload, producer, rethrow, worker_ids,
};

#[tokio::test]
async fn wrapper_actor_reader_results_precede_report_publication() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.begin(1).await?;
        fixture.wait_sessions(2).await?;
        fixture.attach(0, producer(0)).await?;
        fixture.attach(1, controller(0)).await?;
        fixture.wait_workers(2).await?;
        assert!(fixture.report.is_none());
        assert!(!fixture.owner.control().progress().reported());
        Ok(())
    })
    .await;
    let cleanup = fixture.complete().await;
    rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (2, 2, 2, true));
    assert!(report.socket().wrapper().is_some());
    assert!(report.socket().actor().is_some());
    assert!(report.socket().reader().is_some());
    assert_eq!(report.worker_launches(), report.worker_joins());
    // Presence means actual joins, not invented success or native-buffer cleanliness.
    Ok(())
}

#[tokio::test]
async fn original_session_and_worker_ids_match_complete_report() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let hooks = fixture.hooks.clone();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.begin(1).await?;
        fixture.wait_sessions(2).await?;
        fixture.attach(0, producer(0)).await?;
        fixture.attach(1, controller(0)).await?;
        fixture.wait_workers(2).await?;
        let sessions = fixture.owner.original_session_ids();
        let workers = worker_ids(&hooks);
        hooks.session_ready.arm();
        fixture.owner.abort_session_without_request(0);
        fixture.gate(&hooks.session_ready).await?;
        assert_eq!(fixture.owner.control().progress().session_joins(), 1);
        Ok((sessions, workers))
    })
    .await;
    let cleanup = fixture.complete().await;
    let (sessions, workers) = rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (2, 2, 2, true));
    assert!(
        report
            .sessions()
            .iter()
            .all(|row| sessions.contains(&row.id()))
    );
    assert!(report.workers().all(|row| workers.contains(&row.id())));
    let without_request = &report.sessions()[0];
    assert!(
        without_request
            .join_error()
            .is_some_and(|error| error.is_cancelled())
    );
    assert!(!without_request.abort_requested());
    assert_eq!(without_request.drain(), Drain::Live);
    let requested = &report.sessions()[1];
    assert!(
        requested
            .join_error()
            .is_some_and(|error| error.is_cancelled())
    );
    assert!(requested.abort_requested());
    assert_eq!(requested.drain(), Drain::External);
    assert!(
        report.has_failures(),
        "both raw cancellations remain errors regardless of test cleanup policy"
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_finish_during_wrapper_join_keeps_root() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let hooks = fixture.hooks.clone();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.wait_sessions(1).await?;
        let original_ids = fixture.owner.original_session_ids();
        hooks.wrapper_return.arm();
        fixture.cancel_finish_at(&hooks.wrapper_return).await?;
        assert!(fixture.report.is_none());
        assert_eq!(fixture.owner.original_session_ids(), original_ids);
        assert!(fixture.owner.control().progress().authority_closed());
        assert!(!fixture.owner.control().progress().reported());
        Ok(original_ids)
    })
    .await;
    let cleanup = fixture.complete().await;
    let original_ids = rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 1, 0, true));
    assert_eq!(report.sessions()[0].id(), original_ids[0]);
    Ok(())
}

#[tokio::test]
async fn cancelled_finish_after_session_ready_resumes_classification() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let hooks = fixture.hooks.clone();
    let (original, identity, drops) = payload();
    *locked(&hooks.session_fault) = Some(Box::new(original));
    hooks.session_ready.arm();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.gate(&hooks.session_ready).await?;
        assert_eq!(fixture.owner.control().progress().session_joins(), 1);
        fixture.cancel_finish_at(&hooks.session_ready).await?;
        assert!(fixture.report.is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        Ok(())
    })
    .await;
    let cleanup = fixture.complete().await;
    rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 1, 0, true));
    let row = &report.sessions()[0];
    assert!(row.join_error().is_none());
    assert!(
        !row.abort_requested(),
        "cached Ready is never retroactively tagged by stop"
    );
    let raw = row
        .routing_error()
        .and_then(|error| error.downcast_ref::<super::fixture::Payload>())
        .expect("original routing error");
    assert!(Arc::ptr_eq(&raw.identity, &identity));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(report.has_failures());
    drop(report);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn cancelled_finish_after_worker_ready_keeps_original_error() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let hooks = fixture.hooks.clone();
    let (original, identity, drops) = payload();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.wait_sessions(1).await?;
        fixture.attach(0, producer(0)).await?;
        fixture.wait_workers(1).await?;
        *locked(&hooks.worker_fault) = Some(Box::new(original));
        hooks.worker_ready.arm();
        fixture.end(0).await?;
        fixture.gate(&hooks.worker_ready).await?;
        fixture.cancel_finish_at(&hooks.worker_ready).await?;
        assert!(fixture.report.is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        Ok(())
    })
    .await;
    let cleanup = fixture.complete().await;
    rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 1, 1, true));
    let row = report.workers().next().expect("original worker row");
    assert!(
        !row.abort_requested(),
        "actual Ready preceded external stop"
    );
    assert_ne!(row.drain(), Drain::External);
    let raw = row
        .worker_error()
        .and_then(|error| error.downcast_ref::<super::fixture::Payload>())
        .expect("same original worker error");
    assert!(Arc::ptr_eq(&raw.identity, &identity));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(report.has_failures());
    drop(report);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn worker_error_does_not_skip_other_session_joins() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let hooks = fixture.hooks.clone();
    let (original, identity, drops) = payload();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.begin(1).await?;
        fixture.wait_sessions(2).await?;
        fixture.attach(0, producer(0)).await?;
        fixture.attach(1, controller(0)).await?;
        fixture.wait_workers(2).await?;
        let sessions = fixture.owner.original_session_ids();
        *locked(&hooks.worker_fault) = Some(Box::new(original));
        fixture.end(0).await?;
        let control = fixture.owner.control();
        fixture
            .drive(async {
                while !control.progress().authority_closed() {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await?;
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        Ok(sessions)
    })
    .await;
    let cleanup = fixture.complete().await;
    let sessions = rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (2, 2, 2, true));
    assert!(
        report
            .sessions()
            .iter()
            .all(|row| sessions.contains(&row.id()))
    );
    let row = report
        .workers()
        .find(|row| row.worker_error().is_some())
        .expect("one original worker error");
    let raw = row
        .worker_error()
        .and_then(|error| error.downcast_ref::<super::fixture::Payload>())
        .expect("original boxed payload");
    assert!(Arc::ptr_eq(&raw.identity, &identity));
    assert!(report.has_failures());
    drop(report);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn session_unwind_restores_whole_worker_packet() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let hooks = fixture.hooks.clone();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.wait_sessions(1).await?;
        hooks.worker_start.arm();
        hooks.session_ready.arm();
        hooks.session_panic.store(true, Ordering::SeqCst);
        fixture.attach(0, producer(0)).await?;
        fixture.gate(&hooks.session_ready).await?;
        let original = worker_ids(&hooks);
        assert_eq!(original.len(), 1);
        assert_eq!(fixture.owner.restored_worker_ids(0), original);
        Ok(original)
    })
    .await;
    let cleanup = fixture.complete().await;
    let original = rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 1, 1, true));
    let session = &report.sessions()[0];
    assert!(session.join_error().is_some_and(|error| error.is_panic()));
    assert!(
        !session.abort_requested(),
        "observed panic was not rewritten as requested cancellation"
    );
    let worker = report
        .workers()
        .next()
        .expect("original unwound Session packet worker");
    assert_eq!(worker.id(), original[0]);
    assert!(
        worker
            .join_error()
            .is_some_and(|error| error.is_cancelled())
    );
    assert!(worker.abort_requested());
    assert_eq!(worker.drain(), Drain::External);
    assert!(report.has_failures());
    Ok(())
}

#[tokio::test]
async fn admission_permit_stays_anchored_until_every_original_join() -> TestResult {
    let semaphore = Arc::new(Semaphore::new(1));
    let permit = semaphore.clone().acquire_owned().await?;
    let anchor = Anchor::new(Some(permit));
    let drops = anchor.drops.clone();
    let mut fixture = Fixture::with_anchor(2, 4, anchor, None, false).await?;
    let hooks = fixture.hooks.clone();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.wait_sessions(1).await?;
        hooks.worker_start.arm();
        fixture.attach(0, producer(0)).await?;
        fixture.wait_workers(1).await?;
        hooks.wrapper_return.arm();
        fixture.cancel_finish_at(&hooks.wrapper_return).await?;
        assert!(fixture.report.is_none());
        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        Ok(())
    })
    .await;
    let cleanup = fixture.complete().await;
    rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 1, 1, true));
    assert!(report.anchor().permit.is_some());
    assert_eq!(semaphore.available_permits(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(report);
    assert_eq!(semaphore.available_permits(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn abandoned_unreported_root_permanently_retains_original_custody_and_permit() -> TestResult {
    let semaphore = Arc::new(Semaphore::new(1));
    let anchor = Anchor::new(Some(semaphore.clone().acquire_owned().await?));
    let drops = anchor.drops.clone();
    let mut fixture = Fixture::with_anchor(2, 4, anchor, None, false).await?;
    let hooks = fixture.hooks.clone();
    let control = fixture.owner.control();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.wait_sessions(1).await?;
        hooks.worker_start.arm();
        fixture.attach(0, producer(0)).await?;
        fixture.wait_workers(1).await?;
        assert_eq!(fixture.owner.original_session_ids().len(), 1);
        assert_eq!(worker_ids(&hooks).len(), 1);
        Ok(())
    })
    .await;
    if !matches!(&observed, Ok(Ok(()))) {
        let cleanup = fixture.complete().await;
        rethrow(observed)?;
        drop(cleanup?);
        return Err("abandonment setup did not complete".into());
    }
    let Fixture { owner, peer, .. } = fixture;
    drop(owner);
    drop(peer);
    hooks.release();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !control.progress().bridge_done {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(control.progress().authority_closed());
    assert!(
        !control.progress().reported(),
        "task exit observation is not an actual join report"
    );
    assert_eq!(semaphore.available_permits(), 0);
    assert!(semaphore.clone().try_acquire_owned().is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let cold_semaphore = Arc::new(Semaphore::new(1));
    let cold_anchor = Anchor::new(Some(cold_semaphore.clone().acquire_owned().await?));
    let cold_drops = cold_anchor.drops.clone();
    let (cold_owner, cold_starter) =
        super::super::RetainedAtomicMessagingOwner::<_, super::fixture::Recorder>::new(
            tokio::runtime::Handle::current(),
            super::super::RetainedAtomicMessagingLimits::new(2, 4)?,
            cold_anchor,
        )
        .map_err(|_| "bounded cold abandonment setup refused")?;
    let cold_control = cold_owner.control();
    drop(cold_owner);
    assert!(cold_control.progress().authority_closed());
    assert!(!cold_control.progress().reported());
    assert_eq!(cold_control.progress().session_attempts(), 0);
    assert_eq!(cold_control.progress().worker_launches(), 0);
    drop(cold_starter);
    assert_eq!(cold_semaphore.available_permits(), 0);
    assert!(cold_semaphore.clone().try_acquire_owned().is_err());
    assert_eq!(cold_drops.load(Ordering::SeqCst), 0);
    // Two deliberate finite holders (actual-work and cold). No library-global abandonment bound.
    Ok(())
}
