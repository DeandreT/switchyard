use super::{super::*, fixture::*};
use amqp::Performative;
use std::{
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

async fn blocked_worker(panic: bool) -> TestResult {
    let (mut fixture, controls) = opened::<false>(2).await?;
    controls.arm_worker(0);
    let setup = async {
        fixture.begin(0).await?;
        fixture.begin(1).await?;
        fixture.attached(0, producer(0)).await?;
        fixture
            .drive(async {
                while !controls.worker_entered(0) {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await
    }
    .await;
    let mut drops = None;
    if setup.is_ok() {
        let (payload, witness) = payload();
        drops = Some(witness);
        let fault = if panic {
            hooks::Fault::Panic(Box::new(payload))
        } else {
            hooks::Fault::Error(Err(Box::new(payload)))
        };
        *fixture
            .hooks
            .terminal_fault
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(fault);
        fixture.hooks.socket_ready.arm();
    }
    let hooks = fixture.hooks.clone();
    let observation = if setup.is_ok() {
        fixture.finish_at(&hooks.socket_ready).await
    } else {
        Err("setup not ready".into())
    };
    let before = drops
        .as_ref()
        .map_or(0, |drops| drops.load(Ordering::SeqCst));
    let rooted = fixture.root.socket_report.as_ref().is_some_and(|report| {
        if panic { report.wrapper().is_some_and(|result|result.as_ref().is_err_and(tokio::task::JoinError::is_panic)) }
        else { matches!(&report.outcomes().primary, Some(crate::listener::retained_connection::RetainedConnectionOutcome::Finished(Err(error))) if error.downcast_ref::<Payload>().is_some()) }
    });
    let cleanup = fixture.complete().await;
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    let after = drops
        .as_ref()
        .map_or(0, |drops| drops.load(Ordering::SeqCst));
    setup?;
    observation?;
    assert_eq!(facts, (2, 2, 1, true));
    assert!(rooted && disposed);
    assert_eq!((before, after), (0, 1));
    Ok(())
}

#[tokio::test]
async fn wrapper_late_injected_error_stays_rooted_through_blocked_worker_cleanup() -> TestResult {
    blocked_worker(false).await
}
#[tokio::test]
async fn wrapper_panic_original_joinerror_stays_rooted_through_worker_cleanup() -> TestResult {
    blocked_worker(true).await
}

#[tokio::test]
async fn final_session_error_is_unique_while_sibling_and_wrapper_wait() -> TestResult {
    let (mut fixture, controls) = opened::<false>(2).await?;
    controls.arm_session(0);
    controls.arm_session(1);
    controls.arm_session_ready(0);
    let setup = async {
        fixture.begin(0).await?;
        fixture.begin(1).await?;
        fixture
            .drive(async {
                while !controls.session_entered(0) || !controls.session_entered(1) {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await
    }
    .await;
    let mut drops = None;
    let mut previous = None;
    if setup.is_ok() {
        let (payload, witness) = payload();
        drops = Some(witness);
        previous = controls.inject_session_error(0, Box::new(payload));
        controls.release_session(0);
    }
    let observation = if setup.is_ok() {
        fixture
            .drive(async {
                while !controls.session_ready_entered(0) {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await
    } else {
        Err("setup not ready".into())
    };
    let before = drops
        .as_ref()
        .map_or(0, |drops| drops.load(Ordering::SeqCst));
    let cleanup = fixture.complete().await;
    let facts = evidence(&cleanup);
    let original = cleanup
        .report
        .as_ref()
        .and_then(|report| report.collector.as_ref())
        .and_then(|report| report.session_error(0))
        .and_then(|error| error.downcast_ref::<Payload>())
        .is_some_and(|payload| {
            drops
                .as_ref()
                .is_some_and(|drops| Arc::ptr_eq(drops, &payload.drops))
        });
    let previous_present = previous.is_some();
    if let Some(previous) = previous {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| previous.dispose()));
    }
    let disposed = dispose(cleanup);
    let after = drops
        .as_ref()
        .map_or(0, |drops| drops.load(Ordering::SeqCst));
    setup?;
    observation?;
    assert_eq!(facts, (2, 2, 0, true));
    assert!(original && disposed && !previous_present);
    assert_eq!((before, after), (0, 1));
    Ok(())
}

async fn gated_open() -> TestResult<(Fixture, atomic::ScopedControls)> {
    let mut fixture = Fixture::gated().await?;
    let opening = fixture.hello().await;
    let binding = if opening.is_ok() {
        fixture.bound().await
    } else {
        Err("opening incomplete".into())
    };
    let (opening, binding) = match (opening, binding) {
        (Ok(()), Ok(binding)) => return Ok((fixture, binding)),
        results => results,
    };
    let cleanup = fixture.complete().await;
    let _ = dispose(cleanup);
    opening?;
    Err(binding.err().unwrap_or_else(|| "incomplete binding".into()))
}

#[tokio::test]
async fn final_worker_error_is_unique_during_actual_pending_socket_shutdown() -> TestResult {
    let (mut fixture, controls) = gated_open().await?;
    controls.arm_worker_final(0);
    let setup = async {
        fixture.begin(0).await?;
        fixture.attached(0, producer(0)).await?;
        fixture
            .send(CHANNELS[0], Performative::End(amqp::End::default()))
            .await?;
        fixture
            .drive(async {
                while !controls.worker_final_entered(0) {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await?;
        let io = fixture.io.as_ref().expect("real flush gate").clone();
        io.block();
        fixture
            .send(CHANNELS[1], Performative::Begin(amqp::Begin::default()))
            .await?;
        fixture
            .drive(async move {
                while !io.entered() {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await
    }
    .await;
    let mut drops = None;
    let mut previous = None;
    if setup.is_ok() {
        let (payload, witness) = payload();
        drops = Some(witness);
        previous = controls.inject_worker_error(0, Box::new(payload));
        controls.release_worker_final(0);
    }
    let socket_controls = fixture.root.socket.controls();
    let observation = if setup.is_ok() {
        fixture
            .drive(async {
                socket_controls
                    .wait_for(|marks| marks.shutdown_pending)
                    .await;
                Ok(())
            })
            .await
    } else {
        Err("setup not ready".into())
    };
    let pending = socket_controls.snapshot().shutdown_pending;
    let before = drops
        .as_ref()
        .map_or(0, |drops| drops.load(Ordering::SeqCst));
    let cleanup = fixture.complete().await;
    let original = cleanup
        .report
        .as_ref()
        .and_then(|report| report.collector.as_ref())
        .and_then(|report| report.worker_error(0))
        .and_then(|error| error.downcast_ref::<Payload>())
        .is_some_and(|payload| {
            drops
                .as_ref()
                .is_some_and(|drops| Arc::ptr_eq(drops, &payload.drops))
        });
    let previous_present = previous.is_some();
    if let Some(previous) = previous {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| previous.dispose()));
    }
    let disposed = dispose(cleanup);
    let after = drops
        .as_ref()
        .map_or(0, |drops| drops.load(Ordering::SeqCst));
    setup?;
    observation?;
    assert!(pending && original && disposed && !previous_present);
    assert_eq!((before, after), (0, 1));
    Ok(())
}

async fn after_row(socket_first: bool) -> TestResult {
    let (mut fixture, controls) = opened::<false>(2).await?;
    controls.arm_worker(0);
    let setup = async {
        fixture.begin(0).await?;
        fixture.begin(1).await?;
        fixture.attached(0, producer(0)).await
    }
    .await;
    let mut first = Ok(());
    if setup.is_ok() && socket_first {
        fixture.hooks.socket_ready.arm();
        let hooks = fixture.hooks.clone();
        first = fixture.finish_at(&hooks.socket_ready).await;
        fixture.hooks.socket_ready.release();
    }
    controls.arm_row();
    let observation = if setup.is_ok() && first.is_ok() {
        fixture.finish_when(|| controls.row_entered()).await
    } else {
        Err("setup not ready".into())
    };
    let socket_rooted = fixture.root.socket_report.is_some();
    let cleanup = fixture.complete().await;
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    setup?;
    first?;
    observation?;
    assert!(socket_rooted && !disposed);
    assert_eq!(facts, (2, 2, 1, true));
    Ok(())
}

#[tokio::test]
async fn socket_report_ready_survives_later_collector_drain_observer_loss() -> TestResult {
    after_row(true).await
}

#[tokio::test]
async fn cancelled_finish_during_actual_wrapper_wait_restores_tokens_and_pump() -> TestResult {
    let (mut fixture, _controls) = opened::<false>(2).await?;
    fixture.hooks.wrapper_return.arm();
    let setup = async {
        fixture.begin(0).await?;
        fixture.begin(1).await
    }
    .await;
    let hooks = fixture.hooks.clone();
    let observation = if setup.is_ok() {
        fixture.finish_at(&hooks.wrapper_return).await
    } else {
        Err("setup not ready".into())
    };
    let pending = hooks.wrapper_return.entered();
    let unfinished = fixture.root.socket_report.is_none();
    let cleanup = fixture.complete().await;
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    setup?;
    observation?;
    assert!(pending && unfinished && !disposed);
    assert_eq!(facts, (2, 2, 0, true));
    Ok(())
}

#[tokio::test]
async fn cancelled_finish_after_session_ready_keeps_original_result_before_packet_drain()
-> TestResult {
    let (mut fixture, controls) = opened::<false>(2).await?;
    controls.arm_session_ready(0);
    let setup = async {
        fixture.begin(0).await?;
        fixture.begin(1).await
    }
    .await;
    let observation = if setup.is_ok() {
        fixture
            .finish_when(|| controls.session_ready_entered(0))
            .await
    } else {
        Err("setup not ready".into())
    };
    let cleanup = fixture.complete().await;
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    setup?;
    observation?;
    assert_eq!(facts, (2, 2, 0, true));
    assert!(!disposed);
    Ok(())
}

#[tokio::test]
async fn cancelled_finish_after_worker_ready_restores_whole_packet_and_original_row() -> TestResult
{
    after_row(false).await
}

#[tokio::test]
async fn two_session_worker_stopped_acknowledgments_progress_while_wrapper_waits() -> TestResult {
    let (mut fixture, controls) = opened::<false>(2).await?;
    let observation = async {
        fixture.begin(0).await?;
        fixture.begin(1).await?;
        fixture.attached(0, producer(0)).await?;
        fixture.attached(1, controller(0)).await?;
        fixture
            .send(CHANNELS[0], Performative::End(amqp::End::default()))
            .await?;
        fixture
            .send(CHANNELS[1], Performative::End(amqp::End::default()))
            .await?;
        fixture
            .drive(async {
                while controls.worker_stopped() < 2 {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await
    }
    .await;
    let count = controls.worker_stopped();
    let first_absent = fixture.root.socket_report.is_none();
    let cleanup = fixture.complete().await;
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    observation?;
    assert!(count >= 2 && first_absent && !disposed);
    assert_eq!(facts, (2, 2, 2, true));
    Ok(())
}

struct BorrowedAnchor<'a> {
    _local: &'a Rc<()>,
    drops: Arc<AtomicUsize>,
}
impl Drop for BorrowedAnchor<'_> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn nonsend_nonstatic_anchor_released_only_after_both_report_barriers() -> TestResult {
    let local = Rc::new(());
    let drops = Arc::new(AtomicUsize::new(0));
    let anchor = BorrowedAnchor {
        _local: &local,
        drops: drops.clone(),
    };
    let mut fixture = Fixture::with_anchor::<false>(2, anchor, false).await?;
    let observation = async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.begin(1).await
    }
    .await;
    let before = drops.load(Ordering::SeqCst);
    let cleanup = fixture.complete().await;
    let facts = evidence(&cleanup);
    let held = drops.load(Ordering::SeqCst);
    let disposed = dispose(cleanup);
    let after = drops.load(Ordering::SeqCst);
    observation?;
    assert_eq!(facts, (2, 2, 0, true));
    assert_eq!((before, held, after), (0, 0, 1));
    assert!(!disposed);
    Ok(())
}
