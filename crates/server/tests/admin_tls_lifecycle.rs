//! Real TLS accepted-work custody with a real broker held inside its clock.
mod admin_tls;

use admin_api::wire;
use admin_tls::{ROOT, claims, client, list, policy, request, sign};
use auth::{JwtPolicy, ResourceScope};
use domain::{EntityPath, NamespaceName, StateMachine, Timestamp};
use protocol_amqp::AmqpListener;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{
    AdminListenerService, AdminTlsConfig, Broker, Clock, LocalProposer, Shutdown, ShutdownBudget,
    ShutdownExpired, ShutdownStage, ShutdownTrigger, SystemClock, TimerThread, serve_transports,
    shut_down_with_admin,
};
use std::{
    error::Error,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Duration,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, time::Instant};

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;
const WAIT: Duration = Duration::from_secs(120);

struct Gate {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}
#[derive(Clone, Default)]
struct GatedClock(Arc<Mutex<Option<Gate>>>);
impl Clock for GatedClock {
    fn now(&self) -> Timestamp {
        if let Some(gate) = self.0.lock().unwrap().take() {
            gate.entered.send(()).unwrap();
            gate.release.recv_timeout(WAIT).unwrap();
        }
        SystemClock.now()
    }
}

async fn custody(cancel_client: bool) -> Result {
    let directory = TempDir::new()?;
    let clock = GatedClock::default();
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::FjallStore::open(directory.path())?),
        clock.clone(),
    ));
    let handle = broker.handle();
    let namespace = NamespaceName::new("tenant")?;
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])?;
    let tls = protocol_amqp::tls_server_config(
        cert.pem().as_bytes(),
        key_pair.serialize_pem().as_bytes(),
    )?;
    let config = AdminTlsConfig::new(
        tls,
        JwtPolicy::from_json(&policy().to_string())?,
        ResourceScope::parse(ROOT)?,
    )?;
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let mut admin =
        AdminListenerService::new(socket, config, handle.clone(), namespace.clone(), WAIT);
    let admin_retirement = admin.retirement_handle();
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let amqp_address = socket.local_addr()?;
    let mut amqp = AmqpListener::new(handle.clone(), namespace.clone()).into_tcp_service(socket);
    let amqp_retirement = amqp.retirement_handle();
    let trigger = ShutdownTrigger::default();
    let serving = tokio::spawn(async move {
        serve_transports(&mut amqp, Some(&mut admin), &trigger).await;
        (amqp, admin, trigger)
    });
    let mut rpc = client(address, &cert.pem(), "localhost").await?;
    let token = sign(claims("operator"));
    // Authenticated RPC, not a TCP/log-only readiness assertion.
    rpc.list_entities(request(list(), &token)).await?;
    let (entered, observed) = mpsc::channel();
    let (release, released) = mpsc::channel();
    *clock.0.lock().unwrap() = Some(Gate {
        entered,
        release: released,
    });
    let accepted = tokio::spawn({
        let mut rpc = rpc.clone();
        let token = token.clone();
        async move {
            rpc.create_entity(request(
                wire::CreateEntityRequest {
                    namespace: "tenant".into(),
                    path: "accepted".into(),
                    kind: wire::EntityKind::Queue as i32,
                    ..Default::default()
                },
                &token,
            ))
            .await
        }
    });
    tokio::task::spawn_blocking(move || observed.recv_timeout(WAIT)).await??;
    if cancel_client {
        accepted.abort();
    }
    // Both retirement requests linearize before either protocol drain.
    amqp_retirement.request();
    admin_retirement.request();
    let (mut amqp, mut admin, trigger) = tokio::time::timeout(WAIT, serving).await??;
    let denied = rpc
        .list_entities(request(list(), &token))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::Unavailable);
    assert_eq!(
        denied.message(),
        "administration is retiring",
        "the original h2 connection still exists: admission retires before protocol cleanup"
    );
    let (witness, witnessed) = mpsc::channel();
    let latch = Arc::new(Shutdown::default());
    let timer_thread = thread::spawn({
        let latch = latch.clone();
        let handle = handle.clone();
        move || {
            let deadline = std::time::Instant::now() + WAIT;
            while !latch.is_signalled() {
                assert!(std::time::Instant::now() < deadline);
                thread::sleep(Duration::from_millis(5));
            }
            let entity = admin_api::EntityBackend::get(
                &handle,
                namespace,
                admin_api::EntityTarget::Entity(EntityPath::new("accepted").unwrap()),
            );
            let found = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(entity)
                .unwrap()
                .is_some();
            witness.send(found).unwrap();
        }
    });
    let mut timer = Some(TimerThread::new(latch, timer_thread));
    let mut broker = Some(broker);
    // Expiring a borrower does not drop or abort the original accepted work.
    let short = ShutdownBudget::new(Instant::now(), Duration::from_millis(100));
    assert!(matches!(
        shut_down_with_admin(&short, &mut amqp, Some(&mut admin), &mut timer, &mut broker).await,
        Err(ShutdownExpired {
            stage: ShutdownStage::ProtocolCleanup
        })
    ));
    assert!(timer.is_some() && broker.is_some());
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    assert!(tokio::net::TcpStream::connect(amqp_address).await.is_err());
    assert!(
        witnessed.try_recv().is_err(),
        "timer must not start while a backend call is held"
    );
    let started = trigger.started().unwrap();
    assert!(!trigger.request());
    assert_eq!(trigger.started(), Some(started));
    release.send(())?;
    if !cancel_client {
        assert_eq!(accepted.await??.into_inner().path, "accepted");
    } else {
        assert!(accepted.await.unwrap_err().is_cancelled());
    }
    let budget = ShutdownBudget::new(Instant::now(), WAIT);
    shut_down_with_admin(
        &budget,
        &mut amqp,
        Some(&mut admin),
        &mut timer,
        &mut broker,
    )
    .await?;
    assert!(
        witnessed.recv_timeout(WAIT)?,
        "timer observes accepted commit while broker still lives"
    );
    assert!(timer.is_none() && broker.is_none());
    assert!(admin.take_failure().is_none());
    drop((admin, amqp, handle));
    let reopened = Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::FjallStore::open(directory.path())?),
        SystemClock,
    ));
    assert!(
        admin_api::EntityBackend::get(
            &reopened.handle(),
            NamespaceName::new("tenant")?,
            admin_api::EntityTarget::Entity(EntityPath::new("accepted")?)
        )
        .await?
        .is_some()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn accepted_rpc_retains_commit_and_response_through_retirement_and_cancelled_drain() -> Result
{
    custody(false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn client_cancellation_does_not_cancel_accepted_backend_work() -> Result {
    custody(true).await
}

/// Linux shutdown(2) on a duplicate of the listening descriptor makes the real
/// accept fail. This is confined to this test's own ephemeral listener, without
/// injected production failure modes or changes to the AMQP owner.
#[cfg(target_os = "linux")]
async fn accept_failure(admin_fails: bool) -> Result {
    use protocol_amqp::{AmqpListenerExit, AmqpListenerFailure};
    use rustix::{
        io::dup,
        net::{Shutdown as SocketShutdown, shutdown},
    };
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::MemoryStore::default()),
        SystemClock,
    ));
    let namespace = NamespaceName::new("tenant")?;
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])?;
    let tls = protocol_amqp::tls_server_config(
        cert.pem().as_bytes(),
        key_pair.serialize_pem().as_bytes(),
    )?;
    let config = AdminTlsConfig::new(
        tls,
        JwtPolicy::from_json(&policy().to_string())?,
        ResourceScope::parse(ROOT)?,
    )?;
    let admin_socket = TcpListener::bind("127.0.0.1:0").await?;
    let admin_address = admin_socket.local_addr()?;
    let amqp_socket = TcpListener::bind("127.0.0.1:0").await?;
    let amqp_address = amqp_socket.local_addr()?;
    let fault = dup(if admin_fails {
        &admin_socket
    } else {
        &amqp_socket
    })?;
    let mut admin = AdminListenerService::new(
        admin_socket,
        config,
        broker.handle(),
        namespace.clone(),
        WAIT,
    );
    let mut amqp = AmqpListener::new(broker.handle(), namespace).into_tcp_service(amqp_socket);
    let serving = tokio::spawn(async move {
        let trigger = ShutdownTrigger::default();
        serve_transports(&mut amqp, Some(&mut admin), &trigger).await;
        (amqp, admin, trigger)
    });
    let mut rpc = client(admin_address, &cert.pem(), "localhost").await?;
    let token = sign(claims("operator"));
    rpc.list_entities(request(list(), &token)).await?;
    let connection = amqp::ClientConnection::builder()
        .container_id("admin-failure-witness")
        .open(format!("amqp://{amqp_address}").as_str())
        .await?;
    // Also leave an original TLS handshake incomplete. Retirement must join it,
    // not wait for its normal admission timeout or silently detach it.
    let pending_tls = tokio::net::TcpStream::connect(admin_address).await?;
    shutdown(&fault, SocketShutdown::Both)?;
    let (mut amqp, mut admin, trigger) = tokio::time::timeout(WAIT, serving).await??;
    let started = trigger
        .started()
        .expect("a transport failure starts the shared budget");
    assert!(!trigger.request());
    assert_eq!(trigger.started(), Some(started));
    let denied = rpc
        .list_entities(request(list(), &token))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::Unavailable);
    assert_eq!(
        denied.message(),
        "administration is retiring",
        "both admissions retire before either transport cleanup starts"
    );
    let mut timer = None;
    let mut broker = Some(broker);
    let budget = ShutdownBudget::new(started, WAIT);
    let exit = shut_down_with_admin(
        &budget,
        &mut amqp,
        Some(&mut admin),
        &mut timer,
        &mut broker,
    )
    .await?;
    if admin_fails {
        assert!(
            admin.take_failure().is_some(),
            "the real admin accept failure is retained through cleanup"
        );
        assert!(matches!(exit, Some(AmqpListenerExit::Complete(Ok(())))));
    } else {
        assert!(
            matches!(
                exit,
                Some(AmqpListenerExit::Complete(Err(
                    AmqpListenerFailure::Accept(_)
                )))
            ),
            "the original AMQP accept failure retains precedence through dual cleanup"
        );
        assert!(admin.take_failure().is_none());
    }
    assert!(broker.is_none());
    assert!(tokio::net::TcpStream::connect(admin_address).await.is_err());
    assert!(tokio::net::TcpStream::connect(amqp_address).await.is_err());
    drop((pending_tls, connection));
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn real_admin_accept_failure_retires_amqp_and_joins_both_originals() -> Result {
    accept_failure(true).await
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn real_amqp_accept_failure_retires_admin_and_preserves_original_failure() -> Result {
    accept_failure(false).await
}
