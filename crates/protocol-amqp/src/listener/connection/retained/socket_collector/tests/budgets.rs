use super::{super::*, fixture::*, recorder::Recorder};
use amqp::Performative;
use std::{sync::atomic::Ordering, time::Duration};

fn caught<T>(value: T) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value))).is_err()
}

#[tokio::test]
async fn global_worker_k_history_and_unique_ordinals_span_two_real_sessions() -> TestResult {
    let (mut fixture, controls) = opened::<false>(1).await?;
    let observation = async {
        fixture.begin(0).await?;
        fixture.begin(1).await?;
        fixture.attached(0, producer(0)).await?;
        fixture
            .send(CHANNELS[1], Performative::Attach(Box::new(controller(0))))
            .await?;
        let control = fixture.root.control.clone();
        fixture
            .drive(async move {
                control.terminal().await;
                Ok(())
            })
            .await
    }
    .await;
    let committed = controls.worker_commits();
    let cleanup = fixture.complete().await;
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    observation?;
    // The existing collector validates unique original IDs/ordinals at finish;
    // this actual socket case supplies two distinct Session packets and K=1.
    assert_eq!(committed, 1);
    assert_eq!(facts, (2, 2, 1, true));
    assert!(!disposed);
    Ok(())
}

#[tokio::test]
async fn two_original_histories_never_discover_or_retain_third_begin() -> TestResult {
    let (mut fixture, _controls) = opened::<false>(2).await?;
    let first = fixture.begin(0).await;
    let second = if first.is_ok() {
        fixture.begin(1).await
    } else {
        Err("first Begin incomplete".into())
    };
    let third = if second.is_ok() {
        fixture
            .send(11, Performative::Begin(amqp::Begin::default()))
            .await
    } else {
        Err("second Begin incomplete".into())
    };
    let mut original = Box::pin(fixture.frame());
    let third_echo = tokio::time::timeout(Duration::from_millis(50), original.as_mut()).await;
    drop(original);
    let no_echo = third_echo.is_err();
    let claims = fixture.root.control.claims();
    let cleanup = fixture.complete().await;
    let facts = evidence(&cleanup);
    let histories = cleanup.report.as_ref().is_some_and(|report| {
        report.histories.len() == 2
            && report
                .histories
                .iter()
                .all(|packet| packet.disposition == handoff::Disposition::Transferred)
    });
    let disposed = dispose(cleanup);
    first?;
    second?;
    third?;
    drop(third_echo);
    assert!(no_echo && histories && !disposed);
    assert_eq!(claims, 2);
    assert_eq!(facts, (2, 2, 0, true));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn captured_a_drives_all_roles_after_distinct_observer_b_runtime_loss() -> TestResult {
    let (mut fixture, _controls) = opened::<false>(2).await?;
    let control = fixture.root.control.clone();
    let host = std::thread::spawn(move || -> std::io::Result<tokio::task::JoinHandle<()>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let observer = runtime.spawn(async move {
            control.terminal().await;
        });
        // B ends before this data-only observer polls. A/root remain live.
        drop(runtime);
        Ok(observer)
    });
    let mut host_result = host.join();
    let observation = async {
        fixture.begin(0).await?;
        fixture.begin(1).await?;
        fixture.attached(0, producer(0)).await
    }
    .await;
    let cleanup = fixture.complete().await;
    let facts = evidence(&cleanup);
    let observer_result = match &mut host_result {
        Ok(Ok(observer)) => Some((&mut *observer).await),
        _ => None,
    };
    let cancelled = observer_result.as_ref().is_some_and(|result| {
        result
            .as_ref()
            .is_err_and(tokio::task::JoinError::is_cancelled)
    });
    let observer_disposed = caught(observer_result);
    let host_disposed = caught(host_result);
    let disposed = dispose(cleanup);
    observation?;
    assert!(cancelled && !observer_disposed && !host_disposed && !disposed);
    assert_eq!(facts, (2, 2, 1, true));
    Ok(())
}

#[tokio::test]
async fn unstarted_unique_socket_capability_yields_absent_roles_and_collector() -> TestResult {
    let anchor = Anchor::new();
    let drops = anchor.drops.clone();
    let (mut root, launch) = match Root::<_, Recorder>::new::<false>(Handle::current(), 2, anchor) {
        Ok(pair) => pair,
        Err(_) => return Err("valid unstarted factory refused".into()),
    };
    let report = root.finish().await;
    let absent = report.socket.as_ref().is_some_and(|socket| {
        socket.wrapper().is_none() && socket.actor().is_none() && socket.reader().is_none()
    }) && report.collector.is_none()
        && report.context.is_none()
        && report.histories.iter().all(|packet| {
            matches!(packet.payload, handoff::Payload::Empty) && packet.ticket.is_none()
        });
    let held = drops.load(Ordering::SeqCst);
    drop(launch);
    let cleanup = Cleanup::report_only(report);
    let disposed = dispose(cleanup);
    assert!(absent && !disposed);
    assert_eq!((held, drops.load(Ordering::SeqCst)), (0, 1));
    Ok(())
}
