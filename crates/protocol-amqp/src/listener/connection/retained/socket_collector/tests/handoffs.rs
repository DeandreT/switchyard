use super::{super::*, fixture::*, recorder::Recorder};
use std::{
    future::Future,
    sync::atomic::Ordering,
    task::{Context, Poll, Waker},
};

#[tokio::test]
async fn invalid_k_returns_original_anchor_before_socket_custody() -> TestResult {
    for limit in [0, 129] {
        let anchor = Anchor::new();
        let drops = anchor.drops.clone();
        let refused = Root::<_, Recorder>::new::<false>(Handle::current(), limit, anchor);
        let facts = match &refused {
            Err(refused) => (
                refused.limit,
                drops.load(Ordering::SeqCst),
                std::sync::Arc::ptr_eq(&refused.anchor.drops, &drops),
            ),
            Ok(_) => (usize::MAX, usize::MAX, false),
        };
        drop(refused);
        assert_eq!(facts, (limit, 0, true));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn existing_setup_error_precedes_sealed_launch_and_returns_original_request() -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (client, server) = tokio::join!(TcpStream::connect(address), listener.accept());
    let peer = client?;
    let (server, _) = server?;
    let (mut root, launch) = match Root::<(), Recorder>::new::<false>(Handle::current(), 2, ()) {
        Ok(pair) => pair,
        Err(_) => return Err("valid factory refused".into()),
    };
    root.stop();
    let config = AmqpListener::new(Recorder::default(), domain::NamespaceName::new("tenant")?)
        .with_idle_timeout_millis(500);
    let result = launch.start(config, server);
    let report = root.finish().await;
    let original = result.as_ref().is_err_and(|error| matches!(error.cause(),crate::listener::retained_connection::RetainedConnectionStartCause::Setup(error) if error.kind()==std::io::ErrorKind::InvalidInput));
    let absent = report.socket.as_ref().is_some_and(|socket| {
        socket.wrapper().is_none() && socket.actor().is_none() && socket.reader().is_none()
    });
    drop(peer);
    drop(result);
    drop(report);
    assert!(original && absent);
    Ok(())
}

#[tokio::test]
async fn stop_before_open_context_prevents_collector_binding() -> TestResult {
    let mut fixture = Fixture::tcp::<false>(2).await?;
    fixture.root.stop();
    let opening = fixture.hello().await;
    let cleanup = fixture.complete().await;
    let absent = cleanup
        .report
        .as_ref()
        .is_some_and(|report| report.collector.is_none());
    let panicked = dispose(cleanup);
    drop(opening);
    assert!(absent && !panicked);
    Ok(())
}

async fn held_binding() -> TestResult {
    let mut fixture = Fixture::tcp::<false>(2).await?;
    fixture.hooks.bind.arm();
    let opening = fixture.hello().await;
    let hooks = fixture.hooks.clone();
    let entered = fixture.gate(&hooks.bind).await;
    let rooted = fixture.root.context.is_some();
    let before = fixture.root.collector.is_none();
    let cleanup = fixture.complete().await;
    let after = cleanup
        .report
        .as_ref()
        .is_some_and(|r| r.context.is_some() && r.collector.is_none());
    let panicked = dispose(cleanup);
    opening?;
    entered?;
    assert!(rooted && before && after && !panicked);
    Ok(())
}
#[tokio::test]
async fn stop_after_rooted_open_before_bind_ack_keeps_context_and_no_session() -> TestResult {
    held_binding().await
}
#[tokio::test]
async fn cancelled_bind_ack_observation_keeps_dormant_ports_and_external_root() -> TestResult {
    held_binding().await
}

#[tokio::test]
async fn incoming_ready_is_rooted_before_completed_discovery_capture_drop_panics() -> TestResult {
    let (mut fixture, _controls) = opened::<false>(2).await?;
    let (fault, drops) = payload();
    *fixture
        .hooks
        .discovery_drop
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(Box::new(fault));
    let sent = fixture
        .send(
            CHANNELS[0],
            amqp::Performative::Begin(amqp::Begin::default()),
        )
        .await;
    let hooks = fixture.hooks.clone();
    let observed = tokio::time::timeout(DEADLINE, async {
        while hooks
            .discovery_drop
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let before = drops.load(Ordering::SeqCst);
    let cleanup = fixture.complete().await;
    let rooted = cleanup.report.as_ref().is_some_and(|r| {
        matches!(r.histories[0].payload, handoff::Payload::Incoming(_))
            && r.socket.as_ref().is_some_and(|s| {
                s.wrapper()
                    .is_some_and(|r| r.as_ref().is_err_and(|e| e.is_panic()))
            })
    });
    let panicked = dispose(cleanup);
    sent?;
    observed?;
    assert_eq!(before, 0);
    assert!(rooted && panicked);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn admission_ready_is_rooted_before_completed_admission_capture_drop_panics() -> TestResult {
    let (mut fixture, controls) = opened::<false>(2).await?;
    controls.arm_admission(0);
    let (fault, drops) = payload();
    *fixture
        .hooks
        .admission_drop
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(Box::new(fault));
    let accepted = fixture.begin_echo(0).await;
    let observer = controls.clone();
    let entered = fixture
        .drive(async move {
            while !observer.admission_entered(0) {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await;
    let before = drops.load(Ordering::SeqCst);
    let cleanup = fixture.complete().await;
    let counts = evidence(&cleanup);
    let panicked = dispose(cleanup);
    accepted?;
    entered?;
    assert_eq!(before, 0);
    assert_eq!(counts.1, 0);
    assert!(panicked);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn seal_before_final_handoff_claim_retains_exact_cold_original_unpolled() -> TestResult {
    let (mut fixture, _controls) = opened::<false>(2).await?;
    fixture.hooks.before_claim.arm();
    let sent = fixture
        .send(
            CHANNELS[0],
            amqp::Performative::Begin(amqp::Begin::default()),
        )
        .await;
    let hooks = fixture.hooks.clone();
    let entered = fixture.gate(&hooks.before_claim).await;
    fixture.root.stop();
    hooks.before_claim.release();
    let cleanup = fixture.complete().await;
    let original = cleanup.report.as_ref().is_some_and(|r| {
        matches!(r.histories[0].payload, handoff::Payload::Admission(_))
            && r.histories[0].disposition == handoff::Disposition::Refused
            && r.histories[0].ticket.is_none()
    });
    let counts = evidence(&cleanup);
    let panicked = dispose(cleanup);
    sent?;
    entered?;
    assert!(original && !panicked);
    assert_eq!(counts.1, 0);
    Ok(())
}

#[tokio::test]
async fn accepted_handoff_claim_then_seal_finishes_storage_not_admission_activation() -> TestResult
{
    let (mut fixture, _controls) = opened::<false>(2).await?;
    *fixture
        .hooks
        .conversion_panic
        .lock()
        .unwrap_or_else(|e| e.into_inner()) =
        Some(Box::new("controlled restoring conversion unwind"));
    let sent = fixture
        .send(
            CHANNELS[0],
            amqp::Performative::Begin(amqp::Begin::default()),
        )
        .await;
    let cell = fixture.root.cells[0].clone();
    let restored = tokio::time::timeout(DEADLINE, async {
        while cell.original_address().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let address = cell.original_address();
    // Controlled scalar interleaving on an actual restored cold original. No
    // await is added between production claim and storage installation.
    let claim = fixture.root.control.claim_storage();
    let accepted = claim.is_some();
    fixture.root.stop();
    if let Some(claim) = claim {
        cell.install_claimed(claim);
    }
    let cleanup = fixture.complete().await;
    let original = cleanup.report.as_ref().is_some_and(|r| {
        r.stopped[0]
            .as_ref()
            .is_some_and(|record| Some(record.original_address()) == address)
            && r.histories[0].disposition == handoff::Disposition::Transferred
    });
    let counts = evidence(&cleanup);
    let panicked = dispose(cleanup);
    sent?;
    restored?;
    assert!(address.is_some() && accepted && original && !panicked);
    assert_eq!(counts.1, 0);
    Ok(())
}

#[tokio::test]
async fn wrapper_unwind_during_conversion_restores_original_packet_and_ticket() -> TestResult {
    let (mut fixture, _controls) = opened::<false>(2).await?;
    let (fault, drops) = payload();
    *fixture
        .hooks
        .conversion_panic
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(Box::new(fault));
    let sent = fixture
        .send(
            CHANNELS[0],
            amqp::Performative::Begin(amqp::Begin::default()),
        )
        .await;
    let hooks = fixture.hooks.clone();
    let observed = tokio::time::timeout(DEADLINE, async {
        while hooks
            .conversion_panic
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let before = drops.load(Ordering::SeqCst);
    let cleanup = fixture.complete().await;
    let restored = cleanup.report.as_ref().is_some_and(|r| {
        matches!(r.histories[0].payload, handoff::Payload::Admission(_))
            && r.histories[0].ticket.is_none()
    });
    let panicked = dispose(cleanup);
    sent?;
    observed?;
    assert!(restored && panicked);
    assert_eq!(before, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn unused_ports_and_discovery_none_refund_s_without_reissuing_p_history() -> TestResult {
    let (mut fixture, _controls) = opened::<false>(2).await?;
    let close = fixture
        .send(0, amqp::Performative::Close(amqp::Close::default()))
        .await;
    let control = fixture.root.control.clone();
    let ended = fixture
        .drive(async move {
            while !control.requested() {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await;
    let cleanup = fixture.complete().await;
    let refunded = cleanup.report.as_ref().is_some_and(|r| {
        r.histories
            .iter()
            .all(|p| p.ticket.is_none() && matches!(p.payload, handoff::Payload::Empty))
    });
    let counts = evidence(&cleanup);
    let panicked = dispose(cleanup);
    close?;
    ended?;
    assert!(refunded && !panicked);
    assert_eq!(counts.0, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_active_drive_after_actual_admission_pending_keeps_original() -> TestResult {
    let (mut fixture, _controls) = opened::<false>(2).await?;
    let sent = fixture
        .send(
            CHANNELS[0],
            amqp::Performative::Begin(amqp::Begin::default()),
        )
        .await;
    let control = fixture.root.control.clone();
    let published = fixture
        .drive(async move {
            while control.claims() == 0 {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await;
    let polled = {
        let mut borrowed = Box::pin(fixture.root.drive());
        let mut cx = Context::from_waker(Waker::noop());
        let pending = matches!(borrowed.as_mut().poll(&mut cx), Poll::Pending);
        drop(borrowed);
        pending
    };
    let actual = fixture
        .root
        .collector
        .as_ref()
        .is_some_and(atomic::Root::admission_observed_pending);
    let cleanup = fixture.complete().await;
    let panicked = dispose(cleanup);
    sent?;
    published?;
    assert!(polled && actual && !panicked);
    Ok(())
}
