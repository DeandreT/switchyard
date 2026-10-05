use super::*;
use tokio::runtime::{Builder, Runtime};

fn fallback() -> io::Result<Runtime> {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
}

#[test]
fn claimed_actor_obligation_survives_stop_before_lifecycle_binding() -> TestResult {
    let runtime = fallback()?;
    let (negotiated, peer, witness) = runtime.block_on(negotiated())?;
    let gate = Arc::new(Gate::default());
    let mut owner = PairOwner::new_for_test(runtime.handle().clone(), ());
    owner.controls = Arc::new(Controls {
        before_bind: Some(gate.clone()),
        ..Controls::default()
    });
    let (host, observed, claimed) = std::thread::scope(|scope| {
        let host = scope.spawn(|| owner.launch(negotiated));
        let observed = runtime.block_on(observe_gate(&gate));
        owner.stop();
        let claimed = matches!(*locked(&owner.actor.state), State::Claimed);
        gate.release();
        (host.join(), observed, claimed)
    });
    let report = runtime.block_on(owner.finish()).expect("actual report");
    let (host_ok, signalled) = match host {
        Ok(Ok(connection)) => {
            let signalled = *connection.lifecycle.terminated.borrow();
            drop(connection);
            (true, signalled)
        }
        _ => (false, false),
    };
    drop(peer);
    observed?;
    assert!(claimed && host_ok && signalled);
    assert!(report.actor.is_some_and(|result| result.is_ok()));
    assert!(report.reader.is_none());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn claimed_reader_installation_survives_seal_before_spawn() -> TestResult {
    let runtime = fallback()?;
    let (negotiated, peer, witness) = runtime.block_on(negotiated())?;
    let gate = Arc::new(Gate::default());
    let mut owner = PairOwner::new_for_test(runtime.handle().clone(), ());
    owner.controls = Arc::new(Controls {
        reader_claim: Some(gate.clone()),
        ..Controls::default()
    });
    let connection = launch(&owner, negotiated);
    let observed = runtime.block_on(observe_gate(&gate));
    let claimed = matches!(*locked(&owner.reader.state), State::Claimed);
    let pending = runtime.block_on(async {
        let mut waiter = Box::pin(owner.finish());
        poll_once(waiter.as_mut()).is_pending()
    });
    gate.release();
    drop(connection);
    let report = runtime.block_on(owner.finish()).expect("actual report");
    drop(peer);
    observed?;
    assert!(claimed && pending);
    assert!(report.actor.is_some_and(|result| result.is_ok()));
    assert!(report.reader.is_some());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicked_connection_parent_is_separately_joined_from_pair() -> TestResult {
    let (negotiated, peer, witness) = negotiated().await?;
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    let connection = launch(&owner, negotiated);
    let observed = fixture::observe_reader(&owner).await;
    let parent: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        let _connection = connection;
        panic!("controlled caller failure, not an actor failure");
    });
    let parent_result = parent.await;
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    observed?;
    assert!(parent_result.is_err_and(|error| error.is_panic()));
    assert!(report.actor.is_some_and(|result| result.is_ok()));
    assert!(report.reader.is_some());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn distinct_current_runtime_death_before_actor_poll_keeps_fallback_handles() -> TestResult {
    // A does not poll queued work until B and its separately retained OS host exit.
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let (negotiated, peer, witness) = runtime.block_on(negotiated())?;
    let mut owner = PairOwner::new_for_test(runtime.handle().clone(), ());
    let host = std::thread::scope(|scope| {
        scope
            .spawn(
                || -> io::Result<Result<ServerConnection, LaunchRefused<fixture::MarkedIo>>> {
                    let current = Builder::new_current_thread().enable_all().build()?;
                    let connection = current.block_on(async { owner.launch(negotiated) });
                    drop(current);
                    Ok(connection)
                },
            )
            .join()
    });
    let actor_unpolled = matches!(*locked(&owner.reader.state), State::Dormant);
    let report = runtime.block_on(owner.finish()).expect("actual report");
    let host_ok = match host {
        Ok(Ok(Ok(connection))) => {
            drop(connection);
            true
        }
        _ => false,
    };
    drop(peer);
    assert!(host_ok && actor_unpolled);
    assert!(report.actor.is_some_and(|result| result.is_ok()));
    assert!(report.reader.is_none());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

struct DeadObserver {
    task: tokio::task::JoinHandle<()>,
    first_pending: bool,
}

struct HostOutcome {
    host_ok: bool,
    first_pending: bool,
    observer_result: Option<Result<(), JoinError>>,
    failure: Option<std::thread::Result<io::Result<DeadObserver>>>,
}

fn collect_host(
    runtime: &Runtime,
    host: std::thread::Result<io::Result<DeadObserver>>,
) -> HostOutcome {
    match host {
        Ok(Ok(observer)) => HostOutcome {
            host_ok: true,
            first_pending: observer.first_pending,
            observer_result: Some(runtime.block_on(observer.task)),
            failure: None,
        },
        failure => HostOutcome {
            host_ok: false,
            first_pending: false,
            observer_result: None,
            failure: Some(failure),
        },
    }
}

fn dying_observer(connection: ServerConnection) -> io::Result<DeadObserver> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let (send, receive) = tokio::sync::oneshot::channel();
    let task = runtime.spawn(async move {
        let mut observing = Box::pin(connection.shutdown());
        let first_pending = poll_once(observing.as_mut()).is_pending();
        let _ = send.send(first_pending);
        observing.await;
    });
    let first_pending = runtime.block_on(receive).unwrap_or(false);
    drop(runtime);
    Ok(DeadObserver {
        task,
        first_pending,
    })
}

#[test]
fn distinct_observer_runtime_death_during_cleanup_does_not_own_pair() -> TestResult {
    let runtime = fallback()?;
    let (negotiated, peer, witness) = runtime.block_on(negotiated())?;
    let installed = Arc::new(Gate::default());
    let reader_drop = Arc::new(Gate::default());
    let mut owner = PairOwner::new_for_test(runtime.handle().clone(), ());
    owner.controls = Arc::new(Controls {
        after_reader: Some(installed.clone()),
        reader_final: Some(reader_drop.clone()),
        ..Controls::default()
    });
    let connection = launch(&owner, negotiated);
    let installed_observed = runtime.block_on(observe_gate(&installed));
    let host = std::thread::spawn(move || dying_observer(connection)).join();
    let host = collect_host(&runtime, host);
    installed.release();
    owner.stop();
    let reader_observed = runtime.block_on(observe_gate(&reader_drop));
    let pending = runtime.block_on(async {
        let mut waiter = Box::pin(owner.finish());
        poll_once(waiter.as_mut()).is_pending()
    });
    reader_drop.release();
    let report = runtime.block_on(owner.finish()).expect("actual report");
    drop(peer);
    installed_observed?;
    reader_observed?;
    assert!(host.host_ok && host.first_pending && pending);
    assert!(
        host.observer_result
            .is_some_and(|result| result.is_err_and(|error| error.is_cancelled()))
    );
    assert!(report.actor.is_some_and(|result| result.is_ok()));
    assert!(report.reader.is_some());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

struct HostPanicPayload(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for HostPanicPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("controlled host-payload disposal failure after both joins");
    }
}

#[test]
fn real_host_panic_payload_disposal_waits_for_both_pair_barriers() -> TestResult {
    let runtime = fallback()?;
    let (negotiated, peer, witness) = runtime.block_on(negotiated())?;
    let installed = Arc::new(Gate::default());
    let reader_drop = Arc::new(Gate::default());
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut owner = PairOwner::new_for_test(runtime.handle().clone(), ());
    owner.controls = Arc::new(Controls {
        after_reader: Some(installed.clone()),
        reader_final: Some(reader_drop.clone()),
        ..Controls::default()
    });
    let connection = launch(&owner, negotiated);
    let installed_observed = runtime.block_on(observe_gate(&installed));
    let host_payload = HostPanicPayload(drops.clone());
    let host = std::thread::spawn(move || -> io::Result<DeadObserver> {
        let _connection = connection;
        std::panic::panic_any(host_payload);
    })
    .join();
    let mut host = collect_host(&runtime, host);
    let before_release = drops.load(Ordering::SeqCst);
    let retained_failure = host.failure.is_some() && !host.host_ok;
    installed.release();
    owner.stop();
    let reader_observed = runtime.block_on(observe_gate(&reader_drop));
    let pending = runtime.block_on(async {
        let mut waiter = Box::pin(owner.finish());
        poll_once(waiter.as_mut()).is_pending()
    });
    let before_reader_release = drops.load(Ordering::SeqCst);
    reader_drop.release();
    let report = runtime.block_on(owner.finish()).expect("actual report");
    drop(peer);
    let before_disposal = drops.load(Ordering::SeqCst);
    let original_type = host
        .failure
        .as_ref()
        .and_then(|failure| failure.as_ref().err())
        .is_some_and(|payload| payload.is::<HostPanicPayload>());
    let disposal = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(host.failure.take());
    }));
    installed_observed?;
    reader_observed?;
    assert!(retained_failure && original_type && pending && disposal.is_err());
    assert_eq!(
        (before_release, before_reader_release, before_disposal),
        (0, 0, 0)
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(report.actor.is_some_and(|result| result.is_ok()));
    assert!(report.reader.is_some());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}
