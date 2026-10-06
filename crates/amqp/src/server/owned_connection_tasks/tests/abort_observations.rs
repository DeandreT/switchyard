use super::observation_fixture as obs;
use super::*;
use crate::{Close, ServerConnectionAbortSource as Source};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_shutdown_records_same_reader_abort_call() -> TestResult {
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    let (connection, mut peer, _) = obs::open(&mut owner).await?;
    let observed: Result<obs::TestResult<_>, _> = obs::caught(async {
        fixture::observe_reader(&owner).await?;
        obs::send_close(&mut peer, Close::default(), 0, Vec::new()).await?;
        obs::read_close(&mut peer).await?;
        fixture::observe_actor_ready(&owner).await?;
        Ok(owner.reader.observation().map(|row| row.id()))
    })
    .await;
    let report = owner.finish().await.expect("original report");
    drop(connection);
    drop(peer);
    let before = obs::resume(observed)?;
    let row = report.observations().reader().expect("original Reader ID");
    assert_eq!(Some(row.id()), before);
    assert!(row.requested_by(Source::ActorReaderShutdown));
    assert!(!row.requested_by(Source::OwnerFinish));
    assert!(
        report
            .reader()
            .is_some_and(|result| result.as_ref().is_err_and(|error| error.is_cancelled()))
    );
    assert!(report.actor().is_some_and(Result::is_ok));
    assert!(
        !report
            .observations()
            .actor()
            .expect("original Actor")
            .abort_requested()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_finish_records_same_reader_abort_call() -> TestResult {
    let (negotiated, peer, _) = negotiated().await?;
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    owner.controls = Arc::new(Controls {
        actor_panic: true,
        ..Controls::default()
    });
    let connection = obs::launch(&mut owner, negotiated).await;
    let observed = obs::caught(async {
        fixture::observe_actor_ready(&owner).await?;
        Ok::<_, tokio::time::error::Elapsed>(owner.reader.observation().map(|row| row.id()))
    })
    .await;
    let report = owner.finish().await.expect("original report");
    drop(connection);
    drop(peer);
    let id = obs::resume(observed)?;
    let row = report.observations().reader().expect("original Reader");
    assert_eq!(Some(row.id()), id);
    assert!(row.requested_by(Source::OwnerFinish));
    assert!(!row.requested_by(Source::ActorReaderShutdown));
    assert!(
        report
            .actor()
            .is_some_and(|result| result.as_ref().is_err_and(|error| error.is_panic()))
    );
    assert!(
        report
            .reader()
            .is_some_and(|result| result.as_ref().is_err_and(|error| error.is_cancelled()))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_cached_reader_cancel_has_no_private_abort_call() -> TestResult {
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    let (connection, mut peer, control) = obs::open(&mut owner).await?;
    control.arm(1);
    let observed: Result<obs::TestResult<_>, _> = obs::caught(async {
        fixture::observe_reader(&owner).await?;
        obs::send_close(&mut peer, Close::default(), 0, Vec::new()).await?;
        control.entered().await?;
        obs::original_reader_abort(&owner).abort();
        owner.reader.join(None, false).await;
        Ok(owner.reader.observation().map(|row| row.id()))
    })
    .await;
    control.release();
    let report = owner.finish().await.expect("original report");
    drop(connection);
    drop(peer);
    let id = obs::resume(observed)?;
    let row = report
        .observations()
        .reader()
        .expect("cached original Reader");
    assert_eq!(Some(row.id()), id);
    assert!(!row.abort_requested());
    assert!(
        report
            .reader()
            .is_some_and(|result| result.as_ref().is_err_and(|error| error.is_cancelled()))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_actor_reader_loan_restores_token_fact_and_result() -> TestResult {
    let (negotiated, peer, _) = negotiated().await?;
    let gate = Arc::new(Gate::default());
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    owner.controls = Arc::new(Controls {
        before_reader_poll: Some(gate.clone()),
        ..Controls::default()
    });
    let connection = obs::launch(&mut owner, negotiated).await;
    let observed: Result<obs::TestResult<_>, _> = obs::caught(async {
        let abort = fixture::actor_abort(&owner);
        fixture::observe_reader(&owner).await?;
        observe_gate(&gate).await?;
        let _ = connection.lifecycle.cancellation.send(true);
        tokio::time::timeout(obs::DEADLINE, async {
            while !matches!(*locked(&owner.reader.state), State::Leased) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        abort.abort();
        fixture::observe_actor_ready(&owner).await?;
        let restored = matches!(*locked(&owner.reader.state), State::Pending(_));
        let before = owner
            .reader
            .observation()
            .map(|row| (row.id(), row.requested_by(Source::ActorReaderShutdown)));
        Ok((restored, before))
    })
    .await;
    gate.release();
    let report = owner.finish().await.expect("original report");
    drop(connection);
    drop(peer);
    let (restored, before) = obs::resume(observed)?;
    let row = report.observations().reader().expect("restored Reader");
    assert!(restored);
    assert_eq!(before, Some((row.id(), true)));
    assert!(row.requested_by(Source::ActorReaderShutdown));
    assert!(
        report
            .actor()
            .is_some_and(|result| result.as_ref().is_err_and(|error| error.is_cancelled()))
    );
    assert!(
        report
            .reader()
            .is_some_and(|result| result.as_ref().is_err_and(|error| error.is_cancelled()))
    );
    assert!(
        !report
            .observations()
            .actor()
            .expect("original Actor")
            .abort_requested()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_owner_finish_restores_token_fact_and_result() -> TestResult {
    let (negotiated, peer, _) = negotiated().await?;
    let gate = Arc::new(Gate::default());
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    owner.controls = Arc::new(Controls {
        before_reader_poll: Some(gate.clone()),
        actor_panic: true,
        ..Controls::default()
    });
    let connection = obs::launch(&mut owner, negotiated).await;
    let mut first_finish = None;
    let observed: Result<obs::TestResult<_>, _> = obs::caught(async {
        observe_gate(&gate).await?;
        fixture::observe_actor_ready(&owner).await?;
        let original = owner.reader.observation().map(|row| row.id());
        let mut waiter = Box::pin(owner.finish());
        first_finish = Some(poll_once(waiter.as_mut()));
        drop(waiter);
        let restored = matches!(*locked(&owner.reader.state), State::Pending(_));
        let issued = owner
            .reader
            .observation()
            .map(|row| (row.id(), row.requested_by(Source::OwnerFinish)));
        Ok((original, restored, issued))
    })
    .await;
    gate.release();
    let report = owner.finish().await;
    drop(connection);
    drop(peer);
    let (original, restored, issued) = obs::resume(observed)?;
    assert!(first_finish.as_ref().is_some_and(Poll::is_pending) && restored);
    let report = report.expect("same original finish");
    let row = report.observations().reader().expect("original Reader");
    assert_eq!(original, Some(row.id()));
    assert_eq!(issued, Some((row.id(), true)));
    assert!(row.requested_by(Source::OwnerFinish));
    assert!(
        report
            .reader()
            .is_some_and(|result| result.as_ref().is_err_and(|error| error.is_cancelled()))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_and_reader_panics_remain_raw_despite_abort_facts() -> TestResult {
    for actor_panic in [false, true] {
        let (negotiated, peer, _) = negotiated().await?;
        let payload = Arc::new(PayloadCounter::default());
        let mut owner = PairOwner::new_for_test(Handle::current(), ());
        owner.controls = Arc::new(Controls {
            actor_panic,
            reader_panic: !actor_panic,
            actor_payload: actor_panic.then(|| payload.clone()),
            reader_payload: (!actor_panic).then(|| payload.clone()),
            ..Controls::default()
        });
        let connection = obs::launch(&mut owner, negotiated).await;
        let observed = obs::caught(fixture::observe_actor_ready(&owner)).await;
        let report = owner.finish().await.expect("all original joins");
        drop(connection);
        drop(peer);
        obs::resume(observed)?;
        let original = if actor_panic {
            report.actor()
        } else {
            report.reader()
        };
        assert!(
            original.is_some_and(|result| result.as_ref().is_err_and(|error| error.is_panic()))
        );
        assert!(report.actor().is_some() && report.reader().is_some());
        assert_eq!(payload.0.load(Ordering::SeqCst), 0);
        assert!(
            report
                .observations()
                .reader()
                .expect("original Reader")
                .abort_requested()
        );
        drop(report);
        assert_eq!(payload.0.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn uncreated_roles_have_no_invented_abort_receipts() -> TestResult {
    let mut cold = PairOwner::new_for_test(Handle::current(), ());
    let cold_report = cold.finish().await.expect("cold report");
    assert!(cold_report.actor().is_none() && cold_report.reader().is_none());
    assert!(
        cold_report.observations().actor().is_none()
            && cold_report.observations().reader().is_none()
    );
    assert!(cold_report.observations().peer_close().is_none());
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    let (connection, peer, _) = obs::open(&mut owner).await?;
    let observed = obs::caught(async {
        fixture::actor_abort(&owner).abort();
    })
    .await;
    let report = owner.finish().await.expect("original unpolled Actor");
    drop(connection);
    drop(peer);
    obs::resume(observed);
    assert!(
        report
            .actor()
            .is_some_and(|result| result.as_ref().is_err_and(|error| error.is_cancelled()))
    );
    assert!(report.reader().is_none());
    assert!(
        !report
            .observations()
            .actor()
            .expect("installed Actor ID")
            .abort_requested()
    );
    assert!(
        report.observations().reader().is_none() && report.observations().peer_close().is_none()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_report_parts_retain_original_ids_and_abort_facts() -> TestResult {
    let anchor = std::rc::Rc::new(());
    let mut owner = PairOwner::new_for_test(Handle::current(), anchor.clone());
    let (connection, mut peer, _) = obs::open(&mut owner).await?;
    let observed: Result<TestResult, _> = obs::caught(async {
        obs::send_close(&mut peer, Close::default(), 0, Vec::new()).await?;
        obs::read_close(&mut peer).await?;
        fixture::observe_actor_ready(&owner).await?;
        Ok(())
    })
    .await;
    let report = owner.finish().await.expect("original report");
    let reader = report.observations().reader().expect("Reader ID").id();
    let actor = report.observations().actor().expect("Actor ID").id();
    let cached = owner.finish().await;
    drop(connection);
    drop(peer);
    obs::resume(observed)?;
    let (parts, retained) = report.into_parts();
    assert!(cached.is_none());
    assert_eq!(
        parts.observations.reader().expect("original Reader").id(),
        reader
    );
    assert_eq!(
        parts.observations.actor().expect("original Actor").id(),
        actor
    );
    assert!(
        parts
            .observations
            .reader()
            .expect("Reader")
            .requested_by(Source::ActorReaderShutdown)
    );
    assert!(
        !parts
            .observations
            .reader()
            .expect("Reader")
            .requested_by(Source::OwnerFinish)
    );
    assert!(parts.actor.is_some() && parts.reader.is_some());
    assert!(std::rc::Rc::ptr_eq(&retained, &anchor));
    assert!(parts.observations.peer_close().is_some());
    Ok(())
}
