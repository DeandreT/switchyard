use super::*;
use fixture::tcp_pair;

#[test]
fn captured_live_runtime_owns_wrapper_after_distinct_start_runtime_dies_before_first_poll()
-> TestResult {
    let a = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (stream, mut peer) = a.block_on(tcp_pair())?;
    let namespace = NamespaceName::new("tenant")?;
    let anchor = std::rc::Rc::new(String::from("private-runtime-anchor"));
    let (mut owner, starter) = RetainedConnectionOwner::new(a.handle().clone(), &anchor);
    let controls = owner.controls();
    let broker = NoBroker::default();
    broker.plan.wait_begin.store(true, Ordering::SeqCst);
    let configured = AmqpListener::new(broker.clone(), namespace);
    let host = std::thread::Builder::new().spawn(move || -> io::Result<_> {
        let b = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let original = b
            .block_on(async { configured.start_retained_with_driver(stream, starter, TestDriver) });
        drop(b);
        Ok(original)
    });
    let original_host = match host {
        Ok(host) => Ok(host.join()),
        Err(error) => Err(error),
    };
    let queued = !controls.snapshot().first_poll;
    let (setup, observed, report) = a.block_on(async {
        let setup = bounded(async {
            hello(&mut peer).await?;
            send_begin(&mut peer).await
        })
        .await;
        let observed = bounded(controls.wait_for(|m| m.primary_ready)).await;
        drop(peer);
        let report = owner.finish().await;
        (setup, observed, report)
    });
    let report = report.expect("retained captured-runtime report");
    assert!(matches!(&original_host, Ok(Ok(Ok(Ok(()))))));
    setup??;
    observed?;
    assert!(queued);
    successful_socket_joins(&report);
    assert!(std::ptr::eq(*report.anchor(), &anchor));
    assert_eq!(broker.plan.calls.load(Ordering::SeqCst), 0);
    drop(original_host);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_host_panic_payload_disposal_occurs_only_after_all_socket_barriers() -> TestResult
{
    let mut h = Harness::new((), TestDriver, false);
    h.wait_begin();
    h.io.drop_gate.arm();
    let primary = Arc::new(PayloadWitness::default());
    h.error(primary.clone());
    let setup = bounded(async {
        let peer = h
            .peer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing peer"))?;
        hello(peer).await?;
        send_begin(peer).await
    })
    .await;
    let observed = bounded(h.controls.wait_for(|m| m.shutdown_pending)).await;
    let stalled = bounded(async {
        while !h.io.drop_gate.entered() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let payload = Arc::new(PayloadWitness::default());
    payload.panic_on_drop.store(true, Ordering::SeqCst);
    let host_payload = payload.clone();
    let host = std::thread::Builder::new().spawn(move || {
        std::panic::panic_any(PayloadError {
            witness: host_payload,
            name: "private-original-host-panic",
        });
    });
    let original_host = match host {
        Ok(host) => Ok(host.join()),
        Err(error) => Err(error),
    };
    let before = (
        primary.drops.load(Ordering::SeqCst),
        payload.drops.load(Ordering::SeqCst),
    );
    let final_report = h.finish().await;
    let original_typed_payload = matches!(&original_host, Ok(Err(error))
        if error.downcast_ref::<PayloadError>().is_some_and(|e| Arc::ptr_eq(&e.witness, &payload)));
    let disposal = catch_unwind(AssertUnwindSafe(|| drop(original_host)));
    let report = final_report.expect("all three actual socket barriers");
    let (joins, outcomes, _anchor) = report.into_parts();
    setup??;
    observed?;
    stalled?;
    assert_eq!(before, (0, 0));
    assert!(original_typed_payload);
    assert!(matches!(joins.wrapper, Some(Ok(()))));
    assert!(matches!(joins.actor, Some(Ok(()))));
    assert!(normal_reader_join(joins.reader.as_ref()));
    drop((joins, outcomes));
    assert!(disposal.is_err());
    assert_eq!(payload.drops.load(Ordering::SeqCst), 1);
    assert_eq!(primary.drops.load(Ordering::SeqCst), 1);
    Ok(())
}
