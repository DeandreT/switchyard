use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use amqp::{
    AmqpError, Attach, Detach, ErrorCondition, Frame, Performative, ReceiverSettleMode,
    SenderSettleMode, Source, Target,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use tokio::time::timeout;

use super::super::super::{IngressMode, routing::Branch};
use super::super::{SessionExit, budget::Closed, controls::Controls};
use super::fixture::{Cleanup, DEADLINE, Fixture, TestResult, consumer, controller, producer};
use crate::{SharedAccessAuthentication, authorization::ConnectionAuthorization};

struct Facts {
    barriers: bool,
    cancelled: bool,
    sealed: bool,
    counts: Option<(usize, usize, bool)>,
    faults_unused: bool,
}

fn facts(cleanup: &Cleanup, expected: usize) -> Facts {
    let session = cleanup.report.as_ref();
    let workers = session.is_some_and(|report| {
        report.launches.len() == expected
            && report.rows.len() == expected
            && report.launches.iter().enumerate().all(|(ordinal, launch)| {
                launch.ordinal == ordinal
                    && report.rows.iter().filter(|row| row.id == launch.id).count() == 1
            })
    });
    let socket = cleanup.socket.as_ref().is_some_and(|report| {
        report.actor().is_some_and(Result::is_ok)
            && report.reader().is_some_and(|row| {
                row.is_ok() || row.as_ref().is_err_and(|error| error.is_cancelled())
            })
    });
    Facts {
        barriers: workers && socket && cleanup.refused.is_none(),
        cancelled: session.is_some_and(|report| {
            report
                .session
                .as_ref()
                .is_err_and(|error| error.is_cancelled())
        }),
        sealed: session.is_some_and(|report| {
            matches!(&report.session, Ok(SessionExit::Closed(Closed::Sealed)))
        }),
        counts: cleanup.counts,
        faults_unused: cleanup.unused_fault.is_none() && cleanup.unused_session_fault.is_none(),
    }
}

fn dispose(cleanup: Cleanup) -> bool {
    std::panic::catch_unwind(AssertUnwindSafe(|| drop(cleanup))).is_ok()
}

fn assert_cleanup(facts: &Facts, disposed: bool, anchor_drops: &AtomicUsize) {
    assert!(
        facts.barriers,
        "every original Session/worker/socket token joined"
    );
    assert!(facts.faults_unused, "no unobserved fault payload");
    assert!(disposed, "post-barrier raw result disposal");
    assert_eq!(anchor_drops.load(Ordering::SeqCst), 1);
}

#[derive(Default)]
struct DetachObservation {
    frames: Vec<TestResult<Frame>>,
}

impl DetachObservation {
    fn into_detach(self, handle: u32) -> TestResult<Detach> {
        for frame in self.frames {
            if let Frame::Amqp {
                performative: Some(Performative::Detach(detach)),
                ..
            } = frame?
                && detach.handle == handle
            {
                return Ok(detach);
            }
        }
        Err("Detach exceeds bounded frame allowance".into())
    }
}

async fn observe_detach(fixture: &mut Fixture, handle: u32) -> DetachObservation {
    let mut observed = DetachObservation::default();
    for _ in 0..64 {
        let frame = fixture.frame().await;
        let finished = frame.is_err()
            || matches!(&frame,
            Ok(Frame::Amqp { performative: Some(Performative::Detach(detach)), .. })
                if detach.handle == handle);
        // Retain every original frame/error observation until actual completion.
        observed.frames.push(frame);
        if finished {
            break;
        }
    }
    observed
}

async fn wait_refunded(fixture: &mut Fixture) -> TestResult {
    let budget = Arc::clone(&fixture.root.as_ref().expect("created root").budget);
    fixture
        .drive(async {
            while budget.counts().0 != 0 {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
}

fn unauthorized() -> TestResult<Arc<ConnectionAuthorization>> {
    const HOST: &str = "tenant.servicebus.windows.net";
    let policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "test-rule",
        ResourceScope::namespace(HOST)?,
        SharedAccessKey::new("test-secret")?,
        None,
        PermissionSet::SEND,
    )?])?;
    Ok(ConnectionAuthorization::new(
        SharedAccessAuthentication::new(policy, HOST)?,
        None,
    ))
}

async fn rejected_before_reservation(
    mode: IngressMode,
    attach: Attach,
    authorization: Option<Arc<ConnectionAuthorization>>,
    missing: bool,
    requires_session: bool,
    expected_binds: usize,
    error: AmqpError,
) -> TestResult {
    let controls = Arc::new(Controls::default());
    controls.before_accept.arm();
    let mut fixture = Fixture::new(mode, 1, authorization, Arc::clone(&controls)).await?;
    fixture.recorder.missing.store(missing, Ordering::SeqCst);
    fixture
        .recorder
        .requires_session
        .store(requires_session, Ordering::SeqCst);
    let anchor_drops = Arc::clone(&fixture.anchor_drops);
    let handle = attach.handle;
    let sent = fixture.send_attach(attach).await;
    let detached = if sent.is_ok() {
        observe_detach(&mut fixture, handle).await
    } else {
        DetachObservation::default()
    };
    let counts = fixture.counts();
    let binds = fixture.recorder.binds.load(Ordering::SeqCst);
    let submitted = fixture.recorder.submitted.load(Ordering::SeqCst);
    let accepted = controls.before_accept.entered();
    let anchor_alive = anchor_drops.load(Ordering::SeqCst) == 0;
    let cleanup = fixture.complete().await;
    let facts = facts(&cleanup, 0);
    let disposed = dispose(cleanup);
    sent?;
    let detached = detached.into_detach(handle)?;
    assert_cleanup(&facts, disposed, &anchor_drops);
    assert!(anchor_alive);
    assert!(facts.cancelled);
    assert_eq!(facts.counts, Some((0, 0, true)));
    assert_eq!(counts, (0, 0, false));
    assert_eq!(binds, expected_binds);
    assert_eq!(submitted, 0);
    assert!(!accepted, "prevalidation did not enter worker acceptance");
    assert_eq!(
        detached.error.as_ref().map(|error| &error.condition),
        Some(&ErrorCondition::Amqp(error))
    );
    Ok(())
}

#[tokio::test]
async fn posting_role_refusal_does_not_reserve_worker() -> TestResult {
    rejected_before_reservation(
        IngressMode::Posting,
        consumer(0),
        None,
        false,
        false,
        0,
        AmqpError::NotImplemented,
    )
    .await
}

#[tokio::test]
async fn settled_producer_refusal_does_not_reserve_worker() -> TestResult {
    let mut attach = producer(0);
    attach.snd_settle_mode = SenderSettleMode::Settled;
    rejected_before_reservation(
        IngressMode::Posting,
        attach,
        None,
        false,
        false,
        0,
        AmqpError::NotAllowed,
    )
    .await
}

#[tokio::test]
async fn consumer_settlement_refusal_does_not_reserve_worker() -> TestResult {
    let mut attach = consumer(0);
    attach.rcv_settle_mode = ReceiverSettleMode::First;
    rejected_before_reservation(
        IngressMode::Messaging,
        attach,
        None,
        false,
        false,
        0,
        AmqpError::NotAllowed,
    )
    .await
}

#[tokio::test]
async fn missing_queue_refusal_does_not_reserve_worker() -> TestResult {
    rejected_before_reservation(
        IngressMode::Posting,
        producer(0),
        None,
        true,
        false,
        1,
        AmqpError::NotFound,
    )
    .await
}

#[tokio::test]
async fn session_queue_refusal_does_not_reserve_worker() -> TestResult {
    rejected_before_reservation(
        IngressMode::Messaging,
        consumer(0),
        None,
        false,
        true,
        1,
        AmqpError::NotAllowed,
    )
    .await
}

#[tokio::test]
async fn unauthorized_queue_refusal_does_not_reserve_worker() -> TestResult {
    rejected_before_reservation(
        IngressMode::Posting,
        producer(0),
        Some(unauthorized()?),
        false,
        false,
        0,
        AmqpError::UnauthorizedAccess,
    )
    .await
}

#[tokio::test]
async fn unauthorized_coordinator_refusal_does_not_reserve_worker() -> TestResult {
    rejected_before_reservation(
        IngressMode::Posting,
        controller(0),
        Some(unauthorized()?),
        false,
        false,
        0,
        AmqpError::UnauthorizedAccess,
    )
    .await
}

#[tokio::test]
async fn actual_pending_acceptance_cancellation_refunds_ticket() -> TestResult {
    let controls = Arc::new(Controls::default());
    controls.acceptance_pending.arm();
    let mut fixture = Fixture::new(IngressMode::Posting, 1, None, Arc::clone(&controls)).await?;
    let anchor_drops = Arc::clone(&fixture.anchor_drops);
    let sent = fixture.send_attach(producer(0)).await;
    let pending = fixture.wait_gate(&controls.acceptance_pending).await;
    let reserved = fixture.counts();
    fixture.root.as_mut().expect("created root").stop();
    let observed = fixture.observe_natural_finish().await;
    let cleanup = fixture.complete().await;
    let facts = facts(&cleanup, 0);
    let disposed = dispose(cleanup);
    sent?;
    pending?;
    observed?;
    assert_cleanup(&facts, disposed, &anchor_drops);
    assert!(
        controls.acceptance_pending.entered(),
        "actual endpoint poll was Pending"
    );
    assert_eq!(reserved, (1, 0, false));
    assert_eq!(facts.counts, Some((0, 0, true)));
    assert!(facts.cancelled);
    assert_eq!(controls.worker_drops.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn remote_detach_before_accept_refunds_ticket_for_replacement() -> TestResult {
    let controls = Arc::new(Controls::default());
    controls.before_accept.arm();
    let mut fixture = Fixture::new(IngressMode::Posting, 1, None, Arc::clone(&controls)).await?;
    let anchor_drops = Arc::clone(&fixture.anchor_drops);
    let sent = fixture.send_attach(producer(0)).await;
    let before_accept = fixture.wait_gate(&controls.before_accept).await;
    let reserved = fixture.counts();
    let detach_sent = fixture
        .send_performative(Performative::Detach(Detach {
            handle: 0,
            closed: true,
            error: None,
        }))
        .await;
    let detached = if detach_sent.is_ok() {
        observe_detach(&mut fixture, 0).await
    } else {
        DetachObservation::default()
    };
    // The wire acknowledgment retires the actual pending attach before acceptance.
    controls.before_accept.release();
    let refunded = wait_refunded(&mut fixture).await;
    let replacement = fixture.send_attach(producer(1)).await;
    let committed = fixture.wait_committed(1).await;
    let attached = fixture.attached(1).await;
    let counts = fixture.counts();
    let binds = fixture.recorder.binds.load(Ordering::SeqCst);
    let cleanup = fixture.complete().await;
    let facts = facts(&cleanup, 1);
    let disposed = dispose(cleanup);
    sent?;
    before_accept?;
    detach_sent?;
    refunded?;
    replacement?;
    committed?;
    attached?;
    let detached = detached.into_detach(0)?;
    assert_cleanup(&facts, disposed, &anchor_drops);
    assert_eq!(reserved, (1, 0, false));
    assert_eq!(counts, (0, 1, false));
    assert_eq!(facts.counts, Some((0, 1, true)));
    assert_eq!(binds, 2);
    assert!(detached.error.is_none());
    assert_eq!(controls.worker_drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn invalid_cbs_acceptance_refunds_ticket_for_replacement() -> TestResult {
    let controls = Arc::new(Controls::default());
    let authorization = unauthorized()?;
    let mut fixture = Fixture::new(
        IngressMode::Posting,
        1,
        Some(authorization),
        Arc::clone(&controls),
    )
    .await?;
    let anchor_drops = Arc::clone(&fixture.anchor_drops);
    let mut invalid = consumer(0);
    invalid.source = Some(Source::new(crate::CBS_NODE));
    // Missing reply target is accepted as an endpoint but cannot launch CBS work.
    invalid.target = Some(Target::default().into());
    let sent = fixture.send_attach(invalid).await;
    let detached = if sent.is_ok() {
        observe_detach(&mut fixture, 0).await
    } else {
        DetachObservation::default()
    };
    let refunded = wait_refunded(&mut fixture).await;
    let before_replacement = fixture.counts();
    let mut replacement_attach = producer(1);
    replacement_attach.target = Some(Target::new(crate::CBS_NODE).into());
    let replacement = fixture.send_attach(replacement_attach).await;
    let committed = fixture.wait_committed(1).await;
    let attached = fixture.attached(1).await;
    let counts = fixture.counts();
    let binds = fixture.recorder.binds.load(Ordering::SeqCst);
    let branch = fixture
        .root
        .as_ref()
        .map(|_| controls.reply_registration_complete.load(Ordering::SeqCst));
    let cleanup = fixture.complete().await;
    let launched_requests = cleanup.report.as_ref().is_some_and(|report| {
        report.launches.len() == 1 && report.launches[0].branch == Branch::CbsRequests
    });
    let facts = facts(&cleanup, 1);
    let disposed = dispose(cleanup);
    sent?;
    refunded?;
    replacement?;
    committed?;
    attached?;
    let detached = detached.into_detach(0)?;
    assert_cleanup(&facts, disposed, &anchor_drops);
    assert_eq!(
        detached.error.as_ref().map(|error| &error.condition),
        Some(&ErrorCondition::Amqp(AmqpError::InvalidField))
    );
    assert_eq!(before_replacement, (0, 0, false));
    assert_eq!(counts, (0, 1, false));
    assert_eq!(facts.counts, Some((0, 1, true)));
    assert_eq!(binds, 0);
    assert_eq!(branch, Some(false));
    assert!(launched_requests);
    assert_eq!(controls.worker_drops.load(Ordering::SeqCst), 1);
    Ok(())
}

async fn seal_accepted(
    branch: Branch,
    attach: Attach,
    authorization: Option<Arc<ConnectionAuthorization>>,
) -> TestResult {
    let controls = Arc::new(Controls::default());
    controls.before_claim.arm();
    controls.close_ack.arm();
    *controls
        .branch
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(branch);
    let mut fixture = Fixture::new(
        IngressMode::Posting,
        1,
        authorization,
        Arc::clone(&controls),
    )
    .await?;
    let anchor_drops = Arc::clone(&fixture.anchor_drops);
    let handle = attach.handle;
    let sent = fixture.send_attach(attach).await;
    let accepted = fixture.wait_gate(&controls.before_claim).await;
    let attached = fixture.attached(handle).await;
    let counts = fixture.counts();
    let registered = controls.reply_registration_complete.load(Ordering::SeqCst);
    fixture.root.as_ref().expect("created root").seal_launches();
    controls.before_claim.release();
    // Keep finish driven while its close-ack reply is borrowed by process().
    let mut finished = None;
    let mut held = None;
    let waited = timeout(DEADLINE, async {
        tokio::join!(
            async {
                // Root every Ready original outside the timed combinator.
                finished = Some(fixture.observe_natural_finish().await);
            },
            async {
                while !controls.close_ack.entered() {
                    tokio::task::yield_now().await;
                }
                held = Some((
                    controls.authority_closed.load(Ordering::SeqCst),
                    controls.refused_future_dropped.load(Ordering::SeqCst),
                    controls.disposed_before_close.load(Ordering::SeqCst),
                    anchor_drops.load(Ordering::SeqCst),
                ));
                controls.close_ack.release();
            }
        );
    })
    .await;
    let cleanup = fixture.complete().await;
    let facts = facts(&cleanup, 0);
    let disposed = dispose(cleanup);
    sent?;
    accepted?;
    attached?;
    waited?;
    finished.ok_or("missing finish observation")??;
    let held = held.ok_or("missing close-ack observation")?;
    assert_cleanup(&facts, disposed, &anchor_drops);
    assert_eq!(counts, (1, 0, false));
    assert_eq!(held, (true, false, false, 0));
    assert_eq!(registered, branch == Branch::CbsReplies);
    assert!(facts.sealed);
    assert_eq!(facts.counts, Some((0, 0, true)));
    assert!(controls.refused_future_dropped.load(Ordering::SeqCst));
    assert!(!controls.disposed_before_close.load(Ordering::SeqCst));
    assert_eq!(controls.worker_drops.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn accepted_producer_seal_retains_future_until_close_ack() -> TestResult {
    seal_accepted(Branch::Producer, producer(0), None).await
}

#[tokio::test]
async fn registered_cbs_reply_seal_retains_future_until_close_ack() -> TestResult {
    let mut attach = consumer(0);
    attach.source = Some(Source::new(crate::CBS_NODE));
    attach.target = Some(Target::new("replies").into());
    seal_accepted(Branch::CbsReplies, attach, Some(unauthorized()?)).await
}
