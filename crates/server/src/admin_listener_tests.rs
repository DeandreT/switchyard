//! Actual trusted TLS saturation, using private admission witnesses rather than
//! timing guesses about whether a cancelled client reached the server.

use std::{
    collections::VecDeque,
    error::Error,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
};

use admin_api::wire::{EntityKind, entity_service_client::EntityServiceClient};
use domain::{AdminCatalogEntry, AdminCatalogKind, StateMachine, Timestamp};
use protocol_amqp::AmqpListener;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use tempfile::TempDir;
use tokio::{sync::oneshot, task::JoinHandle, time::Instant};
use tonic::{Code, transport::Channel};

use super::*;
use crate::{
    Broker, Clock, LocalProposer, ShutdownBudget, ShutdownExpired, ShutdownStage, ShutdownTrigger,
    SystemClock, serve_transports, shut_down_with_admin,
};

#[path = "../tests/admin_tls/mod.rs"]
mod admin_tls;
use admin_tls::{ROOT, claims, client, list, policy, request, sign};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type ClientCall = JoinHandle<Result<Response<wire::Entity>, Status>>;
const WAIT: Duration = Duration::from_secs(120);
const LIMIT: usize = 3;

fn configuration() -> TestResult<(AdminTlsConfig, String)> {
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])?;
    let tls = protocol_amqp::tls_server_config(
        cert.pem().as_bytes(),
        key_pair.serialize_pem().as_bytes(),
    )?;
    Ok((
        AdminTlsConfig::new(
            tls,
            JwtPolicy::from_json(&policy().to_string())?,
            ResourceScope::parse(ROOT)?,
        )?,
        cert.pem(),
    ))
}

#[test]
fn rpc_limit_is_finite_listener_configuration_validated_before_serving() -> TestResult {
    assert_eq!(configuration()?.0.max_in_flight_rpcs, 1_024);
    assert!(configuration()?.0.with_max_in_flight_rpcs(0).is_err());
    assert!(
        configuration()?
            .0
            .with_max_in_flight_rpcs(Semaphore::MAX_PERMITS + 1)
            .is_err()
    );
    assert_eq!(
        configuration()?
            .0
            .with_max_in_flight_rpcs(LIMIT)?
            .max_in_flight_rpcs,
        LIMIT
    );
    Ok(())
}

struct Gate {
    entered: oneshot::Sender<()>,
    release: mpsc::Receiver<()>,
}

/// Releases on a failed assertion too, so test failure cannot strand the owner.
struct Release(Option<mpsc::Sender<()>>);
impl Release {
    fn release(mut self) -> TestResult {
        self.0.take().expect("held gate").send(())?;
        Ok(())
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

#[derive(Clone, Default)]
struct GatedClock {
    gates: Arc<Mutex<VecDeque<Gate>>>,
    calls: Arc<AtomicUsize>,
}
impl GatedClock {
    fn gate(&self) -> (Release, oneshot::Receiver<()>) {
        let (entered, observed) = oneshot::channel();
        let (release, released) = mpsc::channel();
        self.gates.lock().unwrap().push_back(Gate {
            entered,
            release: released,
        });
        (Release(Some(release)), observed)
    }
}
impl Clock for GatedClock {
    fn now(&self) -> Timestamp {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let gate = self.gates.lock().unwrap().pop_front();
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            gate.release.recv_timeout(WAIT).unwrap();
        }
        SystemClock.now()
    }
}

fn create(path: &str) -> wire::CreateEntityRequest {
    wire::CreateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        kind: EntityKind::Queue as i32,
        ..Default::default()
    }
}

async fn admit_create(
    mut rpc: EntityServiceClient<Channel>,
    path: &str,
    token: &str,
    admitted: &Notify,
) -> TestResult<ClientCall> {
    let entered = admitted.notified();
    tokio::pin!(entered);
    entered.as_mut().enable();
    let body = create(path);
    let token = token.to_owned();
    let call = tokio::spawn(async move { rpc.create_entity(request(body, &token)).await });
    tokio::time::timeout(WAIT, entered).await?;
    Ok(call)
}

async fn cancel_client(call: ClientCall) {
    call.abort();
    assert!(
        call.await.unwrap_err().is_cancelled(),
        "the original backend call was still held when its client was cancelled"
    );
}

async fn excess_calls(
    address: SocketAddr,
    cert: &str,
    token: &str,
    permits: &Semaphore,
) -> TestResult {
    // Every iteration replaces the client channel, establishing a fresh actual
    // TLS/h2 connection. Reconnects cannot create fresh RPC capacity.
    for index in 0..16 {
        let mut rpc = client(address, cert, "localhost").await?;
        let status = tokio::time::timeout(
            WAIT,
            rpc.create_entity(request(create(&format!("excess-{index}")), token)),
        )
        .await?
        .unwrap_err();
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(
            status.message(),
            "native administration RPC capacity reached"
        );
        assert_eq!(permits.available_permits(), 0);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn listener_wide_rpc_bound_survives_repeated_client_cancellation_and_reconnects() -> TestResult
{
    let directory = TempDir::new()?;
    let clock = GatedClock::default();
    let mut broker = Some(Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::FjallStore::open(directory.path())?),
        clock.clone(),
    )));
    let handle = broker.as_ref().unwrap().handle();
    let namespace = NamespaceName::new("tenant")?;
    let (config, cert) = configuration()?;
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let mut admin = AdminListenerService::new(
        socket,
        config.with_max_in_flight_rpcs(LIMIT)?,
        handle.clone(),
        namespace.clone(),
        WAIT,
    );
    let retirement = admin.retirement_handle();
    let (permits, admitted) = {
        let admission = retirement.admission.lock().unwrap();
        (admission.permits.clone(), admission.admitted.clone())
    };
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let amqp_address = socket.local_addr()?;
    let mut amqp = AmqpListener::new(handle.clone(), namespace.clone()).into_tcp_service(socket);
    let amqp_retirement = amqp.retirement_handle();
    let serving = tokio::spawn(async move {
        let trigger = ShutdownTrigger::default();
        serve_transports(&mut amqp, Some(&mut admin), &trigger).await;
        (amqp, admin, trigger)
    });
    let mut rpc = client(address, &cert, "localhost").await?;
    let token = sign(claims("operator"));
    rpc.list_entities(request(list(), &token)).await?;
    // Consume the readiness admission witness, and wait for its original task
    // to be joined rather than assuming receiving a reply already released it.
    tokio::time::timeout(WAIT, admitted.notified()).await?;
    drop(tokio::time::timeout(WAIT, permits.clone().acquire_many_owned(LIMIT as u32)).await??);
    let (first_release, first_entered) = clock.gate();
    let (second_release, second_entered) = clock.gate();

    let first = admit_create(rpc, "held", &token, &admitted).await?;
    tokio::time::timeout(WAIT, first_entered).await??;
    cancel_client(first).await;
    assert_eq!(
        permits.available_permits(),
        LIMIT - 1,
        "cancelling the client must not release an original's permit"
    );
    for path in ["queued-a", "queued-b"] {
        let rpc = client(address, &cert, "localhost").await?;
        let call = admit_create(rpc, path, &token, &admitted).await?;
        cancel_client(call).await;
    }
    assert_eq!(permits.available_permits(), 0);
    assert_eq!(
        clock.calls.load(Ordering::SeqCst),
        1,
        "the broker owner is genuinely held, not answering the cancelled clients"
    );
    excess_calls(address, &cert, &token, &permits).await?;

    first_release.release()?;
    tokio::time::timeout(WAIT, second_entered).await??;
    // Only completion AND join of the first ORIGINAL, not client cancellation or
    // stream replacement, returns this slot. The second original is still held.
    let released = tokio::time::timeout(WAIT, permits.clone().acquire_owned()).await??;
    assert_eq!(clock.calls.load(Ordering::SeqCst), 2);
    assert_eq!(permits.available_permits(), 0);
    drop(released);
    let rpc = client(address, &cert, "localhost").await?;
    let replacement = admit_create(rpc, "replacement", &token, &admitted).await?;
    cancel_client(replacement).await;
    assert_eq!(permits.available_permits(), 0);
    excess_calls(address, &cert, &token, &permits).await?;

    let mut retiring_peer = client(address, &cert, "localhost").await?;
    amqp_retirement.request();
    retirement.request();
    let (mut amqp, mut admin, trigger) = tokio::time::timeout(WAIT, serving).await??;
    let retired = retiring_peer
        .list_entities(request(list(), &token))
        .await
        .unwrap_err();
    assert_eq!(
        retired.code(),
        Code::Unavailable,
        "retirement takes precedence even while the listener-wide permit pool is full"
    );
    assert_eq!(retired.message(), "administration is retiring");
    let started = trigger.started().unwrap();
    assert!(!trigger.request());
    assert_eq!(trigger.started(), Some(started));
    let mut timer = None;
    let short = ShutdownBudget::new(Instant::now(), Duration::from_millis(100));
    assert!(matches!(
        shut_down_with_admin(&short, &mut amqp, Some(&mut admin), &mut timer, &mut broker).await,
        Err(ShutdownExpired {
            stage: ShutdownStage::ProtocolCleanup
        })
    ));
    assert!(
        broker.is_some(),
        "both protocol drains precede broker shutdown"
    );
    assert_eq!(
        permits.available_permits(),
        0,
        "a cancelled cleanup borrower must not release the accepted originals' permits"
    );
    second_release.release()?;
    let budget = ShutdownBudget::new(Instant::now(), WAIT);
    shut_down_with_admin(
        &budget,
        &mut amqp,
        Some(&mut admin),
        &mut timer,
        &mut broker,
    )
    .await?;
    assert!(admin.take_failure().is_none());
    assert!(broker.is_none());
    assert_eq!(permits.available_permits(), LIMIT);
    assert!(admin.calls.is_empty());
    assert_eq!(
        clock.calls.load(Ordering::SeqCst),
        4,
        "only the bounded accepted originals reached the broker; no rejected reconnect did"
    );
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    assert!(tokio::net::TcpStream::connect(amqp_address).await.is_err());
    drop((amqp, admin, handle));

    let reopened = Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::FjallStore::open(directory.path())?),
        SystemClock,
    ));
    let page = admin_api::EntityBackend::list(
        &reopened.handle(),
        namespace,
        AdminCatalogKind::Queues,
        None,
        100,
    )
    .await?;
    let paths = page
        .entries
        .into_iter()
        .map(|entry| match entry {
            AdminCatalogEntry::Queue(queue) => queue.path.as_str().to_owned(),
            _ => panic!("queue selection returned another kind"),
        })
        .collect::<Vec<_>>();
    assert_eq!(paths, ["held", "queued-a", "queued-b", "replacement"]);
    assert!(page.continuation.is_none());
    Ok(())
}

/// A single-thread executor makes the completion distinction deterministic:
/// before the client receives each TLS response, its original task has returned
/// (there is no await after sending the result). The accept pump is paused, so
/// no original can be reaped accidentally by another serving borrower.
#[tokio::test]
async fn completed_backend_unjoined_originals_keep_listener_capacity() -> TestResult {
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::MemoryStore::default()),
        SystemClock,
    ));
    let handle = broker.handle();
    let namespace = NamespaceName::new("tenant")?;
    let (config, cert) = configuration()?;
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let mut admin = AdminListenerService::new(
        socket,
        config.with_max_in_flight_rpcs(LIMIT)?,
        handle.clone(),
        namespace.clone(),
        WAIT,
    );
    let (pause, paused) = oneshot::channel();
    let serving = tokio::spawn(async move {
        tokio::select! {
            () = admin.serve() => panic!("the accept pump must remain live until explicitly paused"),
            _ = paused => {},
        }
        admin
    });
    let token = sign(claims("operator"));
    let mut rpc = client(address, &cert, "localhost").await?;
    rpc.list_entities(request(list(), &token)).await?;
    pause.send(()).unwrap();
    let mut admin = tokio::time::timeout(WAIT, serving).await??;
    // Discard no handles: explicitly join the optional readiness original first.
    while let Some(joined) = admin.calls.join_next().await {
        joined?;
    }
    let permits = admin.retirement.admission.lock().unwrap().permits.clone();
    assert_eq!(permits.available_permits(), LIMIT);

    for path in ["completed-a", "completed-b", "completed-c"] {
        assert_eq!(
            rpc.create_entity(request(create(path), &token))
                .await?
                .into_inner()
                .path,
            path
        );
    }
    let page = admin_api::EntityBackend::list(
        &handle,
        namespace.clone(),
        AdminCatalogKind::Queues,
        None,
        100,
    )
    .await?;
    assert_eq!(
        page.entries.len(),
        LIMIT,
        "all backend operations completed successfully"
    );
    assert_eq!(
        permits.available_permits(),
        0,
        "completed-but-unjoined originals still consume every slot"
    );
    assert_eq!(admin.calls.len(), LIMIT);
    assert_eq!(admin.calls.permit_count(), LIMIT);
    for _ in 0..8 {
        let status = rpc
            .create_entity(request(create("unjoined-overflow"), &token))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(admin.calls.len(), LIMIT);
    }
    // Actual join, not backend completion or dropping a caller, returns one slot.
    admin
        .calls
        .join_next()
        .await
        .expect("a retained completed original")?;
    assert_eq!(permits.available_permits(), 1);
    rpc.create_entity(request(create("after-join"), &token))
        .await?;
    assert_eq!(permits.available_permits(), 0);
    assert_eq!(admin.calls.len(), LIMIT);
    let page =
        admin_api::EntityBackend::list(&handle, namespace, AdminCatalogKind::Queues, None, 100)
            .await?;
    assert_eq!(
        page.entries.len(),
        LIMIT + 1,
        "no saturated call reached the real backend"
    );
    tokio::time::timeout(WAIT, admin.finish()).await?;
    assert!(admin.take_failure().is_none());
    assert_eq!(permits.available_permits(), LIMIT);
    assert!(admin.calls.is_empty());
    drop((admin, handle, broker));
    Ok(())
}

/// The signal is sent at the end of a current-thread original, with no further
/// await, so when observed the original is complete but has not been joined.
async fn ready_original(tasks: &Tasks, permit: Option<OwnedSemaphorePermit>) -> TestResult {
    let (finished, observed) = oneshot::channel();
    tasks.spawn_retained(
        async move {
            let _ = finished.send(());
        },
        permit,
    );
    tokio::time::timeout(WAIT, observed).await??;
    Ok(())
}

async fn ready_connection(connections: &mut JoinSet<()>) -> TestResult {
    let (finished, observed) = oneshot::channel();
    connections.spawn(async move {
        let _ = finished.send(());
    });
    tokio::time::timeout(WAIT, observed).await??;
    Ok(())
}

#[tokio::test]
async fn continuously_ready_families_reap_in_bounded_rotating_turns() -> TestResult {
    let permits = Arc::new(Semaphore::new(1));
    let calls = Tasks::default();
    let streams = Tasks::default();
    let mut connections = JoinSet::new();
    ready_original(&calls, Some(permits.clone().try_acquire_owned()?)).await?;
    ready_original(&streams, None).await?;
    ready_connection(&mut connections).await?;
    let mut turn = ReapKind::Rpc;
    for expected in [
        ReapKind::Rpc,
        ReapKind::Stream,
        ReapKind::Connection,
        ReapKind::Rpc,
        ReapKind::Stream,
        ReapKind::Connection,
    ] {
        let (kind, joined) =
            tokio::time::timeout(WAIT, reap_tasks(turn, &calls, &streams, &mut connections))
                .await?;
        joined?;
        assert_eq!(
            kind, expected,
            "a continuously ready family cannot monopolize reaping"
        );
        turn = kind.next();
        // Replenish the selected family before the next turn. All three stay
        // ready throughout; RPC progress cannot rely on exhausting stream exits.
        match kind {
            ReapKind::Rpc => {
                ready_original(&calls, Some(permits.clone().try_acquire_owned()?)).await?
            }
            ReapKind::Stream => ready_original(&streams, None).await?,
            ReapKind::Connection => ready_connection(&mut connections).await?,
        }
    }
    calls.join_next().await.unwrap()?;
    streams.join_next().await.unwrap()?;
    connections.join_next().await.unwrap()?;
    assert!(calls.is_empty());
    assert!(streams.is_empty());
    assert!(connections.is_empty());
    assert_eq!(permits.available_permits(), 1);
    Ok(())
}

#[tokio::test]
async fn original_panic_and_cancelled_join_waiter_return_capacity_only_on_join() -> TestResult {
    let permits = Arc::new(Semaphore::new(1));
    let calls = Tasks::default();
    let (entered, observed) = oneshot::channel();
    calls.spawn_retained(
        async move {
            let _ = entered.send(());
            panic!("deliberate original RPC panic");
        },
        Some(permits.clone().try_acquire_owned()?),
    );
    tokio::time::timeout(WAIT, observed).await??;
    assert_eq!(
        permits.available_permits(),
        0,
        "panic alone does not discard the retained original"
    );
    let error = calls.join_next().await.unwrap().unwrap_err();
    assert!(error.is_panic());
    assert_eq!(permits.available_permits(), 1);

    let (release, held) = oneshot::channel();
    calls.spawn_retained(
        async move {
            held.await.unwrap();
        },
        Some(permits.clone().try_acquire_owned()?),
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(1), calls.join_next())
            .await
            .is_err()
    );
    assert_eq!(
        permits.available_permits(),
        0,
        "cancelling a pending join must retain its original and permit"
    );
    assert_eq!(calls.len(), 1);
    release.send(()).unwrap();
    tokio::time::timeout(WAIT, calls.join_next())
        .await?
        .unwrap()?;
    assert_eq!(permits.available_permits(), 1);
    assert!(calls.is_empty());
    Ok(())
}
