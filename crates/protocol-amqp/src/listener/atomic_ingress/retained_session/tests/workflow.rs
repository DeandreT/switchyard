use std::sync::{Arc, atomic::Ordering};

use amqp::{Detach, Performative, Source, Target};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use tokio::{runtime::Builder, sync::oneshot};

use super::super::super::{EVENT_CAPACITY, Event, IngressMode, routing::Branch};
use super::super::{SessionExit, controls::Controls};
use super::fixture::{Fixture, TestResult, assert_barriers, consumer, controller, producer};
use crate::{SharedAccessAuthentication, authorization::ConnectionAuthorization};

fn controls() -> Arc<Controls> {
    Arc::new(Controls::default())
}

#[tokio::test]
async fn invalid_total_launch_budget_refuses_before_any_task_spawn() -> TestResult {
    for limit in [0, 129, usize::MAX] {
        let h = Fixture::new(IngressMode::Posting, limit, None, controls()).await?;
        let drops = Arc::clone(&h.anchor_drops);
        let cleanup = h.complete().await;
        assert!(cleanup.report.is_none());
        assert!(cleanup.counts.is_none());
        let refused = cleanup
            .refused
            .as_ref()
            .expect("original Session and anchor refused");
        let _original_session = &refused.session;
        assert_eq!(refused.anchor.drops.load(Ordering::SeqCst), 0);
        drop(cleanup);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn non_static_non_send_anchor_remains_external_to_actual_session() -> TestResult {
    let controls = controls();
    let mut h = Fixture::new(IngressMode::Posting, 0, None, Arc::clone(&controls)).await?;
    let Some(refused) = h.take_refused() else {
        let cleanup = h.complete().await;
        drop(cleanup);
        return Err("missing original invalid-budget Session".into());
    };
    let Some(identity) = h.connection_identity() else {
        let cleanup = h.complete().await;
        drop(refused);
        drop(cleanup);
        return Err("missing retained connection identity".into());
    };
    let super::super::Refused {
        session,
        anchor: original_anchor,
    } = refused;
    let namespace = match domain::NamespaceName::new("tenant") {
        Ok(namespace) => namespace,
        Err(error) => {
            let cleanup = h.complete().await;
            drop(session);
            drop(original_anchor);
            drop(cleanup);
            return Err(error.into());
        }
    };
    let mut anchor = std::rc::Rc::new(());
    let launched = super::super::launch(
        session,
        identity,
        namespace,
        h.recorder.clone(),
        None,
        IngressMode::Posting,
        1,
        tokio::runtime::Handle::current(),
        &mut anchor,
        controls,
    );
    let (report, refused) = match launched {
        Ok(mut root) => {
            root.stop();
            (Some(root.finish().await), None)
        }
        Err(refused) => (None, Some(refused)),
    };
    let cleanup = h.complete().await;
    let joined = report.as_ref().is_some_and(|report| {
        report.rows.is_empty()
            && report
                .session
                .as_ref()
                .is_err_and(|error| error.is_cancelled())
    });
    drop(report);
    drop(refused);
    drop(original_anchor);
    drop(cleanup);
    assert!(joined);
    assert_eq!(std::rc::Rc::strong_count(&anchor), 1);
    Ok(())
}

#[test]
fn queued_session_abort_restores_empty_original_worker_packet() -> TestResult {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let controls = controls();
    controls.session_start.arm();
    let mut h = runtime.block_on(Fixture::new(
        IngressMode::Posting,
        1,
        None,
        Arc::clone(&controls),
    ))?;
    let entered_before_abort = controls.session_start.entered();
    h.root.as_mut().expect("created root").stop();
    let cleanup = runtime.block_on(h.complete());
    assert_barriers(&cleanup, 0);
    assert!(!entered_before_abort);
    assert!(!controls.session_start.entered());
    assert!(
        cleanup
            .report
            .as_ref()
            .expect("report")
            .session
            .as_ref()
            .is_err_and(|e| e.is_cancelled())
    );
    assert_eq!(cleanup.counts, Some((0, 0, true)));
    Ok(())
}

#[tokio::test]
async fn first_polled_session_cancel_restores_actual_worker_tokens() -> TestResult {
    let controls = controls();
    controls.worker_start.arm();
    let mut h = Fixture::new(IngressMode::Posting, 1, None, Arc::clone(&controls)).await?;
    let observed = async {
        h.send_attach(producer(1)).await?;
        h.wait_gate(&controls.worker_start).await
    }
    .await;
    let cleanup = h.complete().await;
    observed?;
    assert_barriers(&cleanup, 1);
    assert_eq!(cleanup.counts, Some((0, 1, true)));
    assert!(
        cleanup
            .report
            .as_ref()
            .expect("report")
            .session
            .as_ref()
            .is_err_and(|e| e.is_cancelled())
    );
    Ok(())
}

#[tokio::test]
async fn session_unwind_restores_set_without_aborting_blocked_sibling() -> TestResult {
    let controls = controls();
    controls.worker_start.arm();
    controls.session_ready.arm();
    controls
        .session_unwind_after_launch
        .store(true, Ordering::SeqCst);
    let mut h = Fixture::new(IngressMode::Posting, 1, None, Arc::clone(&controls)).await?;
    let observed = async {
        h.send_attach(producer(1)).await?;
        h.wait_gate(&controls.session_ready).await
    }
    .await;
    let rows_before_root_drain = controls.joined_rows.load(Ordering::SeqCst);
    let installed_before_root_drain = h.counts().1;
    let cleanup = h.complete().await;
    observed?;
    assert_barriers(&cleanup, 1);
    assert_eq!(installed_before_root_drain, 1);
    assert_eq!(rows_before_root_drain, 0);
    assert!(
        cleanup
            .report
            .as_ref()
            .expect("report")
            .session
            .as_ref()
            .is_err_and(|e| e.is_panic())
    );
    Ok(())
}

async fn peer_end(mode: IngressMode, receiving: bool) -> TestResult {
    let mut h = Fixture::new(mode, 3, None, controls()).await?;
    let observed = async {
        for attach in [controller(1), producer(2)] {
            let handle = attach.handle;
            h.send_attach(attach).await?;
            h.attached(handle).await?;
        }
        if receiving {
            h.send_attach(consumer(3)).await?;
            h.attached(3).await?;
        }
        h.wait_committed(if receiving { 3 } else { 2 }).await?;
        h.peer_end().await?;
        h.observe_natural_finish().await
    }
    .await;
    let cleanup = h.complete().await;
    observed?;
    assert_barriers(&cleanup, if receiving { 3 } else { 2 });
    assert!(matches!(
        cleanup.report.as_ref().expect("report").session,
        Ok(SessionExit::Completed)
    ));
    assert!(
        cleanup
            .report
            .as_ref()
            .expect("report")
            .rows
            .iter()
            .all(|row| matches!(row.original, Ok(Ok(()))))
    );
    Ok(())
}

#[tokio::test]
async fn posting_peer_end_joins_actual_controller_and_producer() -> TestResult {
    peer_end(IngressMode::Posting, false).await
}

#[tokio::test]
async fn messaging_peer_end_joins_actual_controller_producer_and_consumer() -> TestResult {
    peer_end(IngressMode::Messaging, true).await
}

#[tokio::test]
async fn completed_worker_replacement_does_not_replenish_lifetime_budget() -> TestResult {
    let controls = controls();
    let mut h = Fixture::new(IngressMode::Posting, 1, None, Arc::clone(&controls)).await?;
    let observed = async {
        h.send_attach(producer(1)).await?;
        h.attached(1).await?;
        h.send_performative(Performative::Detach(Detach {
            handle: 1,
            closed: true,
            error: None,
        }))
        .await?;
        h.drive(async {
            while controls.joined_rows.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await?;
        h.send_attach(producer(2)).await?;
        h.observe_natural_finish().await
    }
    .await;
    let cleanup = h.complete().await;
    observed?;
    assert_barriers(&cleanup, 1);
    assert!(matches!(
        cleanup.report.as_ref().expect("report").session,
        Ok(SessionExit::Closed(super::super::budget::Closed::Budget))
    ));
    assert_eq!(cleanup.counts, Some((0, 1, true)));
    Ok(())
}

fn authorized() -> TestResult<Arc<ConnectionAuthorization>> {
    const HOST: &str = "tenant.servicebus.windows.net";
    let policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "manage",
        ResourceScope::namespace(HOST)?,
        SharedAccessKey::new("secret")?,
        None,
        PermissionSet::MANAGE,
    )?])?;
    let grant = policy.authenticate_plain("manage", "secret")?;
    Ok(ConnectionAuthorization::new(
        SharedAccessAuthentication::new(policy, HOST)?,
        Some(grant),
    ))
}

#[tokio::test]
async fn all_actual_worker_branches_share_one_total_launch_budget() -> TestResult {
    let controls = controls();
    controls.worker_start.arm();
    let mut h = Fixture::new(
        IngressMode::Messaging,
        5,
        Some(authorized()?),
        Arc::clone(&controls),
    )
    .await?;
    let observed = async {
        let mut request = producer(4);
        request.target = Some(Target::new(crate::CBS_NODE).into());
        let mut reply = consumer(5);
        reply.source = Some(Source::new(crate::CBS_NODE));
        reply.target = Some(Target::new("replies").into());
        for attach in [controller(1), producer(2), consumer(3), request, reply] {
            let handle = attach.handle;
            h.send_attach(attach).await?;
            h.attached(handle).await?;
        }
        h.wait_committed(5).await?;
        h.send_attach(producer(6)).await?;
        h.observe_natural_finish().await
    }
    .await;
    let cleanup = h.complete().await;
    observed?;
    assert_barriers(&cleanup, 5);
    let report = cleanup.report.as_ref().expect("report");
    for branch in [
        Branch::Controller,
        Branch::Producer,
        Branch::Consumer,
        Branch::CbsRequests,
        Branch::CbsReplies,
    ] {
        assert_eq!(
            report
                .launches
                .iter()
                .filter(|launch| launch.branch == branch)
                .count(),
            1
        );
    }
    assert!(matches!(
        report.session,
        Ok(SessionExit::Closed(super::super::budget::Closed::Budget))
    ));
    Ok(())
}

#[tokio::test]
async fn accepted_before_seal_worker_claim_retains_installation_obligation() -> TestResult {
    let controls = controls();
    controls.worker_start.arm();
    let mut h = Fixture::new(IngressMode::Posting, 1, None, Arc::clone(&controls)).await?;
    let observed = async {
        h.send_attach(producer(1)).await?;
        h.wait_committed(1).await
    }
    .await;
    h.root.as_ref().expect("root").seal_launches();
    let cleanup = h.complete().await;
    observed?;
    assert_barriers(&cleanup, 1);
    assert_eq!(cleanup.counts, Some((0, 1, true)));
    Ok(())
}

#[tokio::test]
async fn closed_owner_event_pump_unblocks_actual_worker_stopped_acknowledgments() -> TestResult {
    let controls = controls();
    let mut h = Fixture::new(IngressMode::Posting, 1, None, Arc::clone(&controls)).await?;
    let observed = async {
        h.send_attach(producer(1)).await?;
        h.attached(1).await?;
        h.root.as_mut().expect("root").close_authority();
        h.peer_end().await?;
        h.observe_natural_finish().await
    }
    .await;
    let cleanup = h.complete().await;
    observed?;
    assert_barriers(&cleanup, 1);
    assert!(controls.worker_stopped_observed.load(Ordering::SeqCst) > 0);
    assert!(controls.authority_closed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn full_event_pipe_drains_stop_reports_before_all_worker_joins() -> TestResult {
    let controls = controls();
    controls.worker_start.arm();
    let mut h = Fixture::new(IngressMode::Posting, 1, None, Arc::clone(&controls)).await?;
    let observed = async {
        h.send_attach(producer(1)).await?;
        h.wait_committed(1).await
    }
    .await;
    let mut acknowledgments = Vec::with_capacity(EVENT_CAPACITY);
    let mut sends = Vec::with_capacity(EVENT_CAPACITY);
    for _ in 0..EVENT_CAPACITY {
        let (reply, acknowledged) = oneshot::channel();
        acknowledgments.push(acknowledged);
        sends.push(
            h.root
                .as_ref()
                .expect("root")
                .sender
                .as_ref()
                .expect("sender")
                .try_send(Event::StopConnection { reply }),
        );
    }
    controls.worker_start.release();
    let ended = async {
        h.peer_end().await?;
        h.observe_natural_finish().await
    }
    .await;
    let cleanup = h.complete().await;
    observed?;
    ended?;
    assert_barriers(&cleanup, 1);
    assert!(sends.iter().all(Result::is_ok));
    assert!(controls.worker_stopped_observed.load(Ordering::SeqCst) > 0);
    drop(acknowledgments);
    Ok(())
}

#[tokio::test]
async fn root_stop_collects_original_cancelled_worker_join_errors() -> TestResult {
    let controls = controls();
    controls.worker_start.arm();
    let mut h = Fixture::new(IngressMode::Posting, 2, None, Arc::clone(&controls)).await?;
    let observed = async {
        h.send_attach(controller(1)).await?;
        h.attached(1).await?;
        h.send_attach(producer(2)).await?;
        h.wait_committed(2).await
    }
    .await;
    let cleanup = h.complete().await;
    observed?;
    assert_barriers(&cleanup, 2);
    assert!(
        cleanup
            .report
            .as_ref()
            .expect("report")
            .rows
            .iter()
            .all(|row| row
                .original
                .as_ref()
                .is_err_and(|error| error.is_cancelled()))
    );
    Ok(())
}

#[tokio::test]
async fn no_report_or_anchor_drop_before_all_actual_task_barriers() -> TestResult {
    let controls = controls();
    controls.worker_start.arm();
    let mut h = Fixture::new(IngressMode::Posting, 1, None, Arc::clone(&controls)).await?;
    let drops = Arc::clone(&h.anchor_drops);
    let observed = async {
        h.send_attach(producer(1)).await?;
        h.wait_gate(&controls.worker_start).await
    }
    .await;
    let before = drops.load(Ordering::SeqCst);
    let had_report = h.report.is_some();
    let cleanup = h.complete().await;
    observed?;
    assert_barriers(&cleanup, 1);
    assert_eq!(before, 0);
    assert!(!had_report);
    assert_eq!(
        cleanup
            .report
            .as_ref()
            .expect("report")
            .anchor
            .drops
            .load(Ordering::SeqCst),
        0
    );
    drop(cleanup);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn postbarrier_event_and_operation_teardown_follows_owner_close() -> TestResult {
    let controls = controls();
    let mut h = Fixture::new(IngressMode::Posting, 1, None, Arc::clone(&controls)).await?;
    let observed = async {
        h.send_attach(producer(1)).await?;
        h.attached(1).await?;
        h.peer_end().await?;
        h.observe_natural_finish().await
    }
    .await;
    let submitted = Arc::clone(&h.recorder.submitted);
    let cleanup = h.complete().await;
    observed?;
    assert_barriers(&cleanup, 1);
    assert!(controls.authority_closed.load(Ordering::SeqCst));
    assert!(controls.receiver_torn_down.load(Ordering::SeqCst));
    assert!(controls.owner_torn_down.load(Ordering::SeqCst));
    assert_eq!(submitted.load(Ordering::SeqCst), 0);
    Ok(())
}
