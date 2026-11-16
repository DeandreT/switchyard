//! Actual h2 socket proofs and original RPC/permit custody across certificate expiry.
use super::*;
#[path = "../tests/workload/mod.rs"]
mod workload;
use storage::{FjallStore, MemoryStore, StateStore};
use tokio::io::AsyncWriteExt;
use workload::Fixture;

fn config(f: &Fixture, optional: bool, cap: u64) -> AdminTlsConfig {
    AdminTlsConfig::workload(
        f.endpoint(f.policy(&[&f.old], optional, cap), b"h2"),
        optional.then(|| JwtPolicy::from_json(&policy().to_string()).unwrap()),
        ResourceScope::parse(ROOT).unwrap(),
    )
    .unwrap()
}
async fn start(
    f: &Fixture,
    broker: &Broker,
    optional: bool,
    cap: u64,
) -> (
    SocketAddr,
    AdminRetirement,
    tokio::task::JoinHandle<AdminListenerService>,
) {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let mut service = AdminListenerService::new(
        socket,
        config(f, optional, cap),
        broker.handle(),
        NamespaceName::new("tenant").unwrap(),
        WAIT,
    );
    let retirement = service.retirement_handle();
    let serving = tokio::spawn(async move {
        service.serve().await;
        service
    });
    (address, retirement, serving)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_h2_admission_rechecks_after_the_listener_mutex_and_never_spawns_or_reads_clock()
-> workload::Result {
    let f = Fixture::new();
    let clock = GatedClock::default();
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(MemoryStore::default()),
        clock.clone(),
    ));
    let (address, retirement, serving) = start(&f, &broker, false, 2).await;
    let mut entered = retirement.admission_trace.entered.subscribe();
    let mut decided = retirement.admission_trace.decided.subscribe();
    let mut rpc = f.rpc(address, Some(&f.manager)).await?;
    rpc.list_entities(Request::new(list())).await?;
    // Positively consume the actual readiness spawn, not just the pre-lock
    // waiter. The persistent count cannot be erased by later original reaping.
    let admitted = retirement.admission.lock().unwrap().admitted.clone();
    tokio::time::timeout(WAIT, admitted.notified()).await?;
    let initial_admissions = retirement
        .admission
        .lock()
        .unwrap()
        .admitted_count
        .load(Ordering::SeqCst);
    assert_eq!(initial_admissions, 1);
    let readiness = AdmissionAttemptId(1);
    tokio::time::timeout(WAIT, entered.wait_for(|id| *id == Some(readiness))).await??;
    let readiness_decision = (*tokio::time::timeout(
        WAIT,
        decided.wait_for(|decision| decision.is_some_and(|decision| decision.attempt == readiness)),
    )
    .await??)
        .unwrap();
    assert!(!readiness_decision.retired);
    assert_eq!(readiness_decision.admitted_count, initial_admissions);
    assert!(
        retirement
            .admission
            .lock()
            .unwrap()
            .permits
            .available_permits()
            > 0
    );
    let admission = retirement.admission.clone();
    let (held, held_rx) = oneshot::channel();
    let (release, released) = mpsc::channel();
    let locker = std::thread::spawn(move || {
        let _lock = admission.lock().unwrap();
        held.send(()).unwrap();
        released.recv_timeout(WAIT).unwrap();
    });
    held_rx.await?;
    let target = AdmissionAttemptId(2);
    let call = tokio::spawn(async move { rpc.create_entity(Request::new(create("late"))).await });
    tokio::time::timeout(WAIT, entered.wait_for(|id| *id == Some(target))).await??;
    // The same original RPC is now blocked on the actual global admission lock.
    // Its certificate lease expires while the lock is held.
    tokio::time::sleep(Duration::from_secs(3)).await;
    release.send(())?;
    locker.join().unwrap();
    // Client IO expiry is NOT an admission receipt. Wait for this SAME blocked
    // attempt to acquire the real mutex and exit its decision before allowing
    // global retirement to compete for that mutex.
    let decision = (*tokio::time::timeout(
        WAIT,
        decided.wait_for(|decision| decision.is_some_and(|decision| decision.attempt == target)),
    )
    .await??)
        .unwrap();
    assert_eq!(decision.attempt, target);
    assert!(
        !decision.retired,
        "global retirement must not decide the blocked attempt"
    );
    assert_eq!(
        decision.admitted_count, initial_admissions,
        "the target must refuse before spawn, even if later authorization would reject/reap it"
    );
    {
        let admission = retirement.admission.lock().unwrap();
        assert!(!admission.retired);
        assert_eq!(
            admission.admitted_count.load(Ordering::SeqCst),
            initial_admissions
        );
    }
    assert!(!*retirement.signal.borrow());
    assert!(call.await?.is_err());
    assert_eq!(clock.calls.load(Ordering::SeqCst), 0);
    assert!(retirement.admission.lock().unwrap().calls.is_empty());
    retirement.request();
    let mut service = tokio::time::timeout(WAIT, serving).await??;
    service.finish().await;
    assert!(service.take_failure().is_none());
    // Every original stream has now joined, so a regressed lease check cannot
    // hide a late spawn by rejecting/reaping it before this assertion.
    assert_eq!(
        retirement
            .admission
            .lock()
            .unwrap()
            .admitted_count
            .load(Ordering::SeqCst),
        initial_admissions,
        "expired RPC must never spawn, even if later authorization rejects it"
    );
    let mut broker = Some(broker);
    let mut timer = None;
    crate::stop_timer_and_broker(
        &ShutdownBudget::new(Instant::now(), WAIT),
        &mut timer,
        &mut broker,
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn begun_h2_original_keeps_global_permit_through_expiry_cancel_and_actual_join_on_both_backends()
-> workload::Result {
    for durable in [false, true] {
        for cancel in [false, true] {
            for mixed in [false, true] {
                let dir = TempDir::new()?;
                let memory = MemoryStore::default();
                let clock = GatedClock::default();
                let broker = if durable {
                    Broker::spawn(LocalProposer::new(
                        StateMachine::new(FjallStore::open(dir.path())?),
                        clock.clone(),
                    ))
                } else {
                    Broker::spawn(LocalProposer::new(
                        StateMachine::new(memory.clone()),
                        clock.clone(),
                    ))
                };
                let f = Fixture::new();
                let (address, retirement, serving) = start(&f, &broker, mixed, 2).await;
                // Exclude the client's ordinary WAIT=120s request timeout from
                // the IO-expiry proof: two-second lease plus five-second margin,
                // measured before dialing/readiness on this exact h2 connection.
                let io_deadline = Instant::now() + Duration::from_secs(7);
                let mut rpc = f
                    .rpc(address, Some(if mixed { &f.sender } else { &f.manager }))
                    .await?;
                let token = sign(claims("operator"));
                rpc.list_entities(if mixed {
                    request(list(), &token)
                } else {
                    Request::new(list())
                })
                .await?;
                let calls = retirement.admission.lock().unwrap().calls.clone();
                let permits = retirement.admission.lock().unwrap().permits.clone();
                let (release, reached) = clock.gate();
                let call = tokio::spawn(async move {
                    rpc.create_entity(if mixed {
                        request(create("once"), &token)
                    } else {
                        Request::new(create("once"))
                    })
                    .await
                });
                tokio::time::timeout(WAIT, reached).await??;
                assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
                if cancel {
                    call.abort();
                    assert!(call.await.unwrap_err().is_cancelled());
                } else {
                    assert!(
                        tokio::time::timeout_at(io_deadline, call).await??.is_err(),
                        "expiry closes wire without cancelling retained RPC"
                    );
                    assert!(
                        Instant::now() < io_deadline,
                        "a ready client timeout after the bound is not lease-driven IO retirement"
                    );
                }
                if cancel {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                assert_eq!(permits.available_permits(), DEFAULT_MAX_IN_FLIGHT_RPCS - 1);
                assert_eq!(calls.len(), 1);
                retirement.request();
                let mut service = tokio::time::timeout(WAIT, serving).await??;
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), service.finish())
                        .await
                        .is_err()
                );
                assert_eq!(permits.available_permits(), DEFAULT_MAX_IN_FLIGHT_RPCS - 1);
                release.release().unwrap();
                tokio::time::timeout(WAIT, service.finish()).await?;
                assert!(service.take_failure().is_none());
                assert!(calls.is_empty());
                assert_eq!(permits.available_permits(), DEFAULT_MAX_IN_FLIGHT_RPCS);
                assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
                let found = admin_api::EntityBackend::get(
                    &broker.handle(),
                    NamespaceName::new("tenant")?,
                    admin_api::EntityTarget::Entity(domain::EntityPath::new("once")?),
                )
                .await?;
                assert!(found.is_some());
                let mut owner = Some(broker);
                let mut timer = None;
                crate::stop_timer_and_broker(
                    &ShutdownBudget::new(Instant::now(), WAIT),
                    &mut timer,
                    &mut owner,
                )
                .await?;
                assert!(owner.is_none());
                // Reopen ONLY after successful original joins and the real owner stop.
                let store: Box<dyn Snapshot> = if durable {
                    Box::new(FjallStore::open(dir.path())?)
                } else {
                    Box::new(memory)
                };
                assert!(store.queue_exists());
            }
        }
    }
    Ok(())
}
trait Snapshot {
    fn queue_exists(&self) -> bool;
}
impl<S: StateStore> Snapshot for S {
    fn queue_exists(&self) -> bool {
        self.get(&domain::keys::queue_config(
            &NamespaceName::new("tenant").unwrap(),
            &domain::EntityPath::new("once").unwrap(),
        ))
        .unwrap()
        .is_some()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn short_peer_and_partial_h2_io_retire_without_stopping_existing_healthy_peer()
-> workload::Result {
    let f = Fixture::new();
    let short = workload::client_identity(&f.old, "manager", Some(workload::now() + 5));
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(MemoryStore::default()),
        SystemClock,
    ));
    let (address, retirement, serving) = start(&f, &broker, false, 60).await;
    let mut healthy = f.rpc(address, Some(&f.manager)).await?;
    healthy.list_entities(Request::new(list())).await?;
    let io_deadline = Instant::now() + Duration::from_secs(10); // five-second leaf plus five-second margin
    let mut partial = f.tls12(address, Some(&short), b"h2").await?;
    partial.write_all(b"PRI * HTTP/2.0\r\n").await?;
    let mut bytes = Vec::new();
    use tokio::io::AsyncReadExt;
    let _ = tokio::time::timeout_at(io_deadline, partial.read_to_end(&mut bytes)).await?;
    assert!(
        Instant::now() < io_deadline,
        "the partial h2 socket must close within its certificate-specific bound"
    );
    healthy.list_entities(Request::new(list())).await?;
    retirement.request();
    let mut service = serving.await?;
    service.finish().await;
    assert!(service.take_failure().is_none());
    let mut owner = Some(broker);
    let mut timer = None;
    crate::stop_timer_and_broker(
        &ShutdownBudget::new(Instant::now(), WAIT),
        &mut timer,
        &mut owner,
    )
    .await?;
    Ok(())
}
