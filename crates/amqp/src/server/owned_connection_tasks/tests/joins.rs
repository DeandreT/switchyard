use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_close_preserves_exact_wire_and_both_actual_joins() -> TestResult {
    let (negotiated, mut peer, witness) = negotiated().await?;
    let mut owner = PairOwner::new(Handle::current(), ());
    let connection = launch(&owner, negotiated);
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(connection.close(), async {
            let frame = crate::read_frame(&mut peer).await?;
            match frame {
                crate::Frame::Amqp {
                    channel: 0,
                    performative: Some(crate::Performative::Close(close)),
                    ..
                } if close.error.is_none() => {}
                _ => return Err(io::Error::other("expected graceful Close")),
            }
            crate::write_frame(
                &mut peer,
                &crate::Frame::Amqp {
                    channel: 0,
                    performative: Some(crate::Performative::Close(crate::Close { error: None })),
                    payload: Vec::new(),
                },
            )
            .await
        })
    })
    .await;
    let report = owner.finish().await.expect("actual report");
    drop(connection);
    drop(peer);
    let (local, remote) = closed?;
    local?;
    remote?;
    let mut expected = b"AMQP\0\x01\0\0".to_vec();
    expected.extend(crate::encode_frame(&crate::server::checked_open_frame(
        crate::Open {
            max_frame_size: crate::server::DEFAULT_MAX_FRAME_SIZE,
            idle_time_out: Some(
                crate::server::ConnectionOptions::default().advertised_idle_timeout(),
            ),
            ..crate::Open::new("server")
        },
    )?)?);
    expected.extend(crate::encode_frame(&crate::Frame::Amqp {
        channel: 0,
        performative: Some(crate::Performative::Close(crate::Close { error: None })),
        payload: Vec::new(),
    })?);
    assert_eq!(*locked(&witness.wire), expected);
    assert!(report.actor.is_some_and(|result| result.is_ok()));
    assert!(report.reader.is_some());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parent_drop_after_reader_birth_joins_both_original_tokens() -> TestResult {
    let (negotiated, peer, witness) = negotiated().await?;
    let mut owner = PairOwner::new(Handle::current(), ());
    let connection = launch(&owner, negotiated);
    let observed = fixture::observe_reader(&owner).await;
    drop(connection);
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    observed?;
    assert!(report.actor.is_some_and(|result| result.is_ok()));
    assert!(report.reader.is_some());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    assert!(owner.finish().await.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_unwind_after_reader_installation_keeps_reader_rooted() -> TestResult {
    let (negotiated, peer, witness) = negotiated().await?;
    let mut owner = PairOwner::new(Handle::current(), ());
    owner.controls = Arc::new(Controls {
        actor_panic: true,
        ..Controls::default()
    });
    let connection = launch(&owner, negotiated);
    let observed = fixture::observe_actor_ready(&owner).await;
    let reader_rooted = matches!(
        *locked(&owner.reader.state),
        State::Pending(_) | State::Joined(_)
    );
    drop(connection);
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    observed?;
    assert!(reader_rooted);
    assert!(
        report
            .actor
            .is_some_and(|result| result.is_err_and(|error| error.is_panic()))
    );
    assert!(report.reader.is_some());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exit_signal_is_not_actor_join_and_lost_waiter_restores_token() -> TestResult {
    let (negotiated, peer, witness) = negotiated().await?;
    let gate = Arc::new(Gate::default());
    let anchor = std::rc::Rc::new(());
    let mut owner = PairOwner::new(Handle::current(), anchor.clone());
    owner.controls = Arc::new(Controls {
        actor_final: Some(gate.clone()),
        ..Controls::default()
    });
    let connection = launch(&owner, negotiated);
    let reader_observed = fixture::observe_reader(&owner).await;
    drop(connection);
    let observed = observe_gate(&gate).await;
    let signal = locked(&owner.observations)
        .terminated
        .as_ref()
        .is_some_and(|receiver| *receiver.borrow());
    let retired = locked(&owner.observations)
        .identity
        .as_ref()
        .is_some_and(|identity| !identity.is_active());
    let mut waiter = Box::pin(owner.finish());
    let pending = poll_once(waiter.as_mut()).is_pending();
    drop(waiter);
    let retained = matches!(*locked(&owner.actor.state), State::Pending(_));
    let anchor_retained = std::rc::Rc::strong_count(&anchor);
    gate.release();
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    reader_observed?;
    observed?;
    assert!(signal && retired && pending && retained);
    assert!(report.actor.as_ref().is_some_and(|result| result.is_ok()));
    assert!(report.reader.is_some());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(anchor_retained, 2);
    assert!(std::rc::Rc::ptr_eq(&report.anchor, &anchor));
    drop(report);
    assert_eq!(std::rc::Rc::strong_count(&anchor), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_cancel_during_reader_loan_restores_same_pending_token() -> TestResult {
    let (negotiated, peer, witness) = negotiated().await?;
    let gate = Arc::new(Gate::default());
    let mut owner = PairOwner::new(Handle::current(), ());
    owner.controls = Arc::new(Controls {
        before_reader_poll: Some(gate.clone()),
        ..Controls::default()
    });
    let connection = launch(&owner, negotiated);
    let abort = fixture::actor_abort(&owner);
    let reader_observed = fixture::observe_reader(&owner).await;
    let observed = observe_gate(&gate).await;
    let _ = connection.lifecycle.cancellation.send(true);
    let loan_observed = tokio::time::timeout(Duration::from_secs(2), async {
        while !matches!(*locked(&owner.reader.state), State::Leased) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    abort.abort();
    let actor_observed = fixture::observe_actor_ready(&owner).await;
    let restored = matches!(*locked(&owner.reader.state), State::Pending(_));
    let mut waiter = Box::pin(owner.finish());
    let pending = poll_once(waiter.as_mut()).is_pending();
    drop(waiter);
    let root_loan_restored = matches!(*locked(&owner.reader.state), State::Pending(_));
    gate.release();
    drop(connection);
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    reader_observed.map_err(|_| io::Error::other("reader installation observation timed out"))?;
    observed.map_err(|_| io::Error::other("reader first-poll gate observation timed out"))?;
    loan_observed.map_err(|_| io::Error::other("actor reader-loan observation timed out"))?;
    actor_observed.map_err(|_| io::Error::other("cancelled actor observation timed out"))?;
    assert!(restored && pending && root_loan_restored);
    assert!(
        report
            .actor
            .is_some_and(|result| result.is_err_and(|error| error.is_cancelled()))
    );
    assert!(
        report
            .reader
            .is_some_and(|result| result.is_err_and(|error| error.is_cancelled()))
    );
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_panic_payload_waits_for_actual_actor_barrier() -> TestResult {
    let (negotiated, peer, _) = negotiated().await?;
    let gate = Arc::new(Gate::default());
    let payload = Arc::new(PayloadCounter::default());
    let mut owner = PairOwner::new(Handle::current(), ());
    owner.controls = Arc::new(Controls {
        actor_final: Some(gate.clone()),
        reader_panic: true,
        reader_payload: Some(payload.clone()),
        ..Controls::default()
    });
    let connection = launch(&owner, negotiated);
    let observed = observe_gate(&gate).await;
    let retained_result = matches!(*locked(&owner.reader.state), State::Joined(Err(_)));
    let mut waiter = Box::pin(owner.finish());
    let pending = poll_once(waiter.as_mut()).is_pending();
    drop(waiter);
    let before = payload.0.load(Ordering::SeqCst);
    gate.release();
    drop(connection);
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    let after_join = payload.0.load(Ordering::SeqCst);
    let panic_result = report
        .reader
        .as_ref()
        .is_some_and(|result| result.as_ref().is_err_and(|error| error.is_panic()));
    drop(report);
    observed?;
    assert!(retained_result && pending && panic_result);
    assert_eq!(
        (before, after_join, payload.0.load(Ordering::SeqCst)),
        (0, 0, 1)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_panic_payload_waits_for_actual_reader_barrier() -> TestResult {
    let (negotiated, peer, _) = negotiated().await?;
    let gate = Arc::new(Gate::default());
    let payload = Arc::new(PayloadCounter::default());
    let mut owner = PairOwner::new(Handle::current(), ());
    owner.controls = Arc::new(Controls {
        reader_final: Some(gate.clone()),
        actor_panic: true,
        actor_payload: Some(payload.clone()),
        ..Controls::default()
    });
    let connection = launch(&owner, negotiated);
    let actor_observed = fixture::observe_actor_ready(&owner).await;
    let mut waiter = Box::pin(owner.finish());
    let pending = poll_once(waiter.as_mut()).is_pending();
    let observed = observe_gate(&gate).await;
    drop(waiter);
    let actor_retained = matches!(*locked(&owner.actor.state), State::Joined(Err(_)));
    let reader_retained = matches!(*locked(&owner.reader.state), State::Pending(_));
    let before = payload.0.load(Ordering::SeqCst);
    gate.release();
    drop(connection);
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    let after_join = payload.0.load(Ordering::SeqCst);
    let panic_result = report
        .actor
        .as_ref()
        .is_some_and(|result| result.as_ref().is_err_and(|error| error.is_panic()));
    drop(report);
    actor_observed?;
    observed?;
    assert!(pending && actor_retained && reader_retained && panic_result);
    assert_eq!(
        (before, after_join, payload.0.load(Ordering::SeqCst)),
        (0, 0, 1)
    );
    Ok(())
}
