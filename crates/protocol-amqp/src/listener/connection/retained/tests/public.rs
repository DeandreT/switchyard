use super::*;
use fixture::{authentication, sasl_hello, tcp_pair};

fn listener(broker: NoBroker) -> AmqpListener<NoBroker> {
    AmqpListener::new(
        broker,
        NamespaceName::new("tenant").expect("fixture namespace"),
    )
}

#[tokio::test]
async fn unused_starter_creates_no_role_or_observed_outcome() {
    let (mut owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    drop(starter);
    let report = owner.finish().await.expect("empty report");
    assert!(report.wrapper().is_none() && report.actor().is_none() && report.reader().is_none());
    assert!(report.outcomes().primary.is_none() && report.outcomes().websocket_close.is_none());
    assert!(owner.finish().await.is_none());
}

#[tokio::test]
async fn stopped_start_returns_original_configuration_and_socket_without_roles() -> TestResult {
    let (stream, peer) = tcp_pair().await?;
    let address = stream.local_addr()?;
    let (mut owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    owner.stop();
    let configured = listener(NoBroker::default()).with_handshake_timeout(Duration::from_secs(7));
    let original = configured.start_retained_connection(stream, starter);
    let report = owner.finish().await.expect("empty report");
    let error = original.expect_err("stopped refusal");
    assert_eq!(format!("{error:?}"), "RetainedConnectionStartError { .. }");
    let (cause, request) = error.into_parts();
    assert!(matches!(cause, StartCause::Stopped));
    assert_eq!(request.listener.handshake_timeout, Duration::from_secs(7));
    assert_eq!(request.stream.local_addr()?, address);
    assert!(request.stream.nodelay()?);
    assert!(report.wrapper().is_none() && report.actor().is_none() && report.reader().is_none());
    drop((cause, request, peer));
    Ok(())
}

#[tokio::test]
async fn original_option_validation_precedes_stopped_refusal() -> TestResult {
    let (stream, peer) = tcp_pair().await?;
    let (mut owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    owner.stop();
    let original = listener(NoBroker::default())
        .with_idle_timeout_millis(1)
        .start_retained_connection(stream, starter);
    let report = owner.finish().await.expect("empty report");
    let (cause, request) = original.expect_err("original setup error").into_parts();
    let StartCause::Setup(error) = &cause else {
        panic!("setup must precede stop");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.get_ref().is_some_and(|e| e.is::<amqp::EngineError>()));
    assert!(report.wrapper().is_none() && report.actor().is_none() && report.reader().is_none());
    drop((cause, request, peer));
    Ok(())
}

#[tokio::test]
async fn unrepresentable_absolute_deadline_precedes_stopped_refusal() -> TestResult {
    let (stream, peer) = tcp_pair().await?;
    let (mut owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    owner.stop();
    let original = listener(NoBroker::default())
        .with_handshake_timeout(Duration::MAX)
        .start_retained_connection(stream, starter);
    let report = owner.finish().await.expect("empty report");
    let (cause, request) = original
        .expect_err("checked deadline setup error")
        .into_parts();
    assert!(
        matches!(&cause, StartCause::Setup(error) if error.kind() == io::ErrorKind::InvalidInput)
    );
    assert!(request.stream.nodelay()?);
    assert!(report.wrapper().is_none());
    drop((cause, request, peer));
    Ok(())
}

#[tokio::test]
async fn accepted_wrapper_claim_keeps_installation_obligation_after_seal() {
    let (mut owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    let (claim, acceptor, publisher) = starter.claim().expect("claim before seal");
    owner.stop();
    drop(acceptor);
    claim.spawn(async move {
        publisher.primary.publish(Outcome::SkippedExpiredDeadline);
    });
    let report = owner.finish().await.expect("installed original wrapper");
    assert!(matches!(report.wrapper(), Some(Ok(()))));
    assert!(report.actor().is_none() && report.reader().is_none());
    assert!(matches!(
        report.outcomes().primary,
        Some(Outcome::SkippedExpiredDeadline)
    ));
}

#[test]
fn queued_unpolled_wrapper_abort_retains_and_actually_joins_original_token() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (stream, peer) = runtime.block_on(tcp_pair())?;
    let (mut owner, starter) = RetainedConnectionOwner::new(runtime.handle().clone(), ());
    let original = listener(NoBroker::default()).start_retained_connection(stream, starter);
    owner.abort_wrapper();
    let report = runtime
        .block_on(owner.finish())
        .expect("original queued wrapper");
    original.map_err(|_| io::Error::other("unexpected start failure"))?;
    assert!(
        report
            .wrapper()
            .is_some_and(|r| r.as_ref().is_err_and(|e| e.is_cancelled()))
    );
    assert!(report.actor().is_none() && report.reader().is_none());
    assert!(report.outcomes().primary.is_none());
    drop(peer);
    Ok(())
}

#[tokio::test]
async fn first_polled_wrapper_abort_preserves_empty_engine_barriers() -> TestResult {
    let (stream, peer) = tcp_pair().await?;
    let (mut owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    let controls = owner.controls();
    let gate = Gate::new();
    controls.first_gate(gate.clone());
    let original = listener(NoBroker::default()).start_retained_connection(stream, starter);
    let observed = bounded(gate.entered()).await;
    owner.abort_wrapper();
    gate.release();
    let report = owner.finish().await.expect("original wrapper");
    original.map_err(|_| io::Error::other("unexpected start failure"))?;
    observed?;
    assert!(
        report
            .wrapper()
            .is_some_and(|r| r.as_ref().is_err_and(|e| e.is_cancelled()))
    );
    assert!(report.actor().is_none() && report.reader().is_none());
    assert!(report.outcomes().primary.is_none());
    drop(peer);
    Ok(())
}

#[tokio::test]
async fn expired_first_poll_is_distinct_skipped_unit_not_invented_timeout() -> TestResult {
    let (stream, peer) = tcp_pair().await?;
    let (mut owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    let original = listener(NoBroker::default())
        .with_handshake_timeout(Duration::ZERO)
        .start_retained_connection(stream, starter);
    let report = owner.finish().await.expect("expired wrapper report");
    original.map_err(|_| io::Error::other("unexpected start failure"))?;
    assert!(matches!(report.wrapper(), Some(Ok(()))));
    assert!(matches!(
        report.outcomes().primary,
        Some(Outcome::SkippedExpiredDeadline)
    ));
    assert!(report.actor().is_none() && report.reader().is_none());
    drop(peer);
    Ok(())
}

#[tokio::test]
async fn returned_negotiation_engine_error_keeps_original_typed_io_cause() -> TestResult {
    let mut h = Harness::new((), TestDriver, false);
    let original = Arc::new(PayloadWitness::default());
    *crate::listener::retained_connection::locked(&h.io.read_error) = Some(original.clone());
    let observed = bounded(async {
        while !h.owner.published().0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let before = original.drops.load(Ordering::SeqCst);
    let report = h.finish().await.expect("negotiation error report");
    observed?;
    assert_eq!(before, 0);
    let engine = primary_error(&report).and_then(|e| e.downcast_ref::<amqp::EngineError>());
    let cause = match engine {
        Some(amqp::EngineError::Io(error)) => error.get_ref(),
        _ => None,
    };
    assert!(
        cause
            .and_then(|e| e.downcast_ref::<PayloadError>())
            .is_some_and(|e| Arc::ptr_eq(&e.witness, &original))
    );
    assert!(report.actor().is_none() && report.reader().is_none());
    drop(report);
    assert_eq!(original.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn authenticated_nested_open_acceptance_restores_leaf_capability_for_driver() -> TestResult {
    let authentication = authentication()?;
    let mut h = Harness::configured(
        (),
        TestDriver,
        false,
        Some(authentication),
        Duration::from_secs(2),
    );
    h.wait_begin();
    let original = Arc::new(PayloadWitness::default());
    h.error(original.clone());
    let setup = bounded(async {
        let peer = h
            .peer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing peer"))?;
        let code = sasl_hello(peer, "ANONYMOUS").await?;
        hello(peer).await?;
        send_begin(peer).await?;
        Ok::<_, io::Error>(code)
    })
    .await;
    let observed = bounded(h.controls.wait_for(|m| m.primary_ready)).await;
    let report = h.finish().await.expect("authenticated report");
    let code = setup??;
    observed?;
    assert_eq!(code, amqp::SaslCode::Ok);
    successful_socket_joins(&report);
    assert!(h.started);
    assert!(
        primary_error(&report)
            .and_then(|e| e.downcast_ref::<PayloadError>())
            .is_some_and(|e| Arc::ptr_eq(&e.witness, &original))
    );
    assert_eq!(h.broker.plan.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn nested_open_timeout_roots_one_original_timeout_after_pending_leaf_restoration()
-> TestResult {
    let authentication = authentication()?;
    let mut h = Harness::configured(
        (),
        TestDriver,
        false,
        Some(authentication),
        Duration::from_secs(1),
    );
    let setup = bounded(async {
        let peer = h
            .peer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing peer"))?;
        sasl_hello(peer, "ANONYMOUS").await
    })
    .await;
    let observed = bounded(async {
        while !h.owner.published().0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let report = h.finish().await.expect("whole Open timeout report");
    assert_eq!(setup??, amqp::SaslCode::Ok);
    observed?;
    assert!(
        primary_error(&report)
            .and_then(|e| e.downcast_ref::<io::Error>())
            .is_some_and(|e| e.kind() == io::ErrorKind::TimedOut)
    );
    assert!(report.actor().is_none() && report.reader().is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn advanced_refusal_is_published_before_original_io_destructor_panics() -> TestResult {
    let mut h = Harness::new((), TestDriver, false);
    let payload = Arc::new(PayloadWitness::default());
    *crate::listener::retained_connection::locked(&h.io.drop_panic) = Some(payload.clone());
    h.owner.stop();
    let setup = bounded(async {
        let peer = h
            .peer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing peer"))?;
        hello(peer).await
    })
    .await;
    let observed = bounded(h.controls.wait_for(|m| m.launch_refused)).await;
    let before = payload.drops.load(Ordering::SeqCst);
    let report = h.finish().await.expect("refusal report");
    setup??;
    observed?;
    assert_eq!(before, 0);
    assert!(h.io.refused_seen_before_drop.load(Ordering::SeqCst));
    assert!(matches!(
        report.outcomes().primary,
        Some(Outcome::LaunchRefused)
    ));
    assert!(
        report
            .wrapper()
            .is_some_and(|r| r.as_ref().is_err_and(|e| e.is_panic()))
    );
    assert!(report.actor().is_none() && report.reader().is_none());
    drop(report);
    assert_eq!(payload.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn opaque_report_preserves_borrowed_non_send_anchor_without_raw_formatting() -> TestResult {
    let secret = std::rc::Rc::new(String::from("private-anchor-token"));
    let mut h = Harness::new(&secret, TestDriver, false);
    h.wait_begin();
    let original = Arc::new(PayloadWitness::default());
    h.error(original.clone());
    let setup = bounded(async {
        let peer = h
            .peer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing peer"))?;
        hello(peer).await?;
        send_begin(peer).await
    })
    .await;
    let observed = bounded(h.controls.wait_for(|m| m.primary_ready)).await;
    let report = h.finish().await.expect("non-Send borrowed anchor report");
    setup??;
    observed?;
    successful_socket_joins(&report);
    assert_eq!(format!("{report:?}"), "RetainedConnectionJoinReport { .. }");
    assert_eq!(
        format!("{:?}", report.outcomes()),
        "RetainedConnectionOutcomes { .. }"
    );
    assert!(std::ptr::eq(*report.anchor(), &secret));
    let io_drops = h.io.dropped.load(Ordering::SeqCst);
    let cached = h.owner.finish().await;
    assert!(cached.is_none());
    assert_eq!(h.io.dropped.load(Ordering::SeqCst), io_drops);
    let (joins, outcomes, anchor) = report.into_parts();
    assert!(std::ptr::eq(anchor, &secret));
    assert_eq!(format!("{joins:?}"), "RetainedConnectionTaskJoins { .. }");
    drop((joins, outcomes));
    assert_eq!(original.drops.load(Ordering::SeqCst), 1);
    Ok(())
}
