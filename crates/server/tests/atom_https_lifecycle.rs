//! Test-only mutation witness: this does not install an Atom CRUD/XML adapter.
#[allow(dead_code)]
mod admin_tls;
#[allow(dead_code)]
mod atom_http;
use admin_api::atom::https::{
    Address, AuthorizedHead, AuthorizedRequest, BodyMode, Handler, HandlerFuture, HttpsListener,
    Refusal, Reply,
};
use atom_http::{Identity, TARGET, TestResult, WAIT};
use auth::{JwtPolicy, ResourceScope};
use domain::{
    CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueConfig, SequenceNumber,
    StateMachine, Timestamp,
};
use hyper::{Method, StatusCode};
use protocol_amqp::{AmqpListener, AmqpListenerExit, AmqpListenerFailure};
use server::{
    AdminListenerService, AdminTlsConfig, Broker, BrokerHandle, Clock, LocalProposer, Shutdown,
    ShutdownBudget, ShutdownExpired, ShutdownStage, ShutdownTrigger, SystemClock, TimerThread,
    serve_all_transports, shut_down_all_transports,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};
use tempfile::TempDir;
use tokio::{io::AsyncWriteExt, net::TcpListener, sync::oneshot, time::Instant};

struct MutationWitness(BrokerHandle);
impl Handler for MutationWitness {
    fn select(&self, head: &AuthorizedHead) -> Result<BodyMode, Refusal> {
        if head.method() != Method::PUT {
            return Err(Refusal::Invalid);
        }
        Ok(BodyMode::Empty)
    }
    fn handle(&self, request: AuthorizedRequest) -> HandlerFuture<'_> {
        Box::pin(async move {
            let Address::Entity(entity) = request.head().address() else {
                return Err(Refusal::Invalid);
            };
            self.0
                .submit(
                    request.head().namespace().clone(),
                    entity.clone(),
                    CommandKind::CreateQueue {
                        config: QueueConfig::default(),
                    },
                )
                .await
                .map_err(|_| Refusal::Internal)?;
            Ok(Reply::new(
                StatusCode::OK,
                b"test-only mutation committed".to_vec(),
            ))
        })
    }
}
struct Gate {
    entered: oneshot::Sender<()>,
    release: mpsc::Receiver<()>,
}
#[derive(Clone, Default)]
struct GatedClock(Arc<Mutex<Option<Gate>>>);
impl Clock for GatedClock {
    fn now(&self) -> Timestamp {
        let gate = self.0.lock().unwrap().take();
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            gate.release.recv_timeout(WAIT).unwrap();
        }
        SystemClock.now()
    }
}
struct Release(Option<mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn three_original_families_drain_begun_work_after_disconnect_and_cancelled_borrower()
-> TestResult {
    let directory = TempDir::new()?;
    let clock = GatedClock::default();
    let mut broker = Some(Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::FjallStore::open(directory.path())?),
        clock.clone(),
    )));
    let handle = broker.as_ref().unwrap().handle();
    let identity = Identity::new()?;
    let namespace = NamespaceName::new("tenant")?;
    let amqp_socket = TcpListener::bind("127.0.0.1:0").await?;
    let amqp_address = amqp_socket.local_addr()?;
    let native_socket = TcpListener::bind("127.0.0.1:0").await?;
    let native_address = native_socket.local_addr()?;
    let atom_socket = TcpListener::bind("127.0.0.1:0").await?;
    let atom_address = atom_socket.local_addr()?;
    let mut amqp =
        AmqpListener::new(handle.clone(), namespace.clone()).into_tcp_service(amqp_socket);
    let mut native = AdminListenerService::new(
        native_socket,
        AdminTlsConfig::new(
            identity.native()?,
            JwtPolicy::from_json(&admin_tls::policy().to_string())?,
            ResourceScope::parse(admin_tls::ROOT)?,
        )?,
        handle.clone(),
        namespace.clone(),
        WAIT,
    );
    let mut atom = HttpsListener::with_handler(
        atom_socket,
        identity.atom()?,
        Arc::new(MutationWitness(handle.clone())),
    );
    let amqp_retire = amqp.retirement_handle();
    let native_retire = native.retirement_handle();
    let atom_retire = atom.retirement_handle();
    let trigger = ShutdownTrigger::default();
    let request_trigger = trigger.clone();
    let serving = tokio::spawn(async move {
        serve_all_transports(&mut amqp, Some(&mut native), Some(&mut atom), &trigger).await;
        (amqp, native, atom, trigger)
    });
    let token = admin_tls::sign(admin_tls::claims("operator"));
    let mut rpc = admin_tls::client(native_address, &identity.ca_pem, "localhost").await?;
    rpc.list_entities(admin_tls::request(admin_tls::list(), &token))
        .await?;
    let connection = amqp::ClientConnection::builder()
        .container_id("atom-custody-witness")
        .open(format!("amqp://{amqp_address}").as_str())
        .await?;
    let mut retiring_peer = atom_http::connect(atom_address, &identity).await?;
    let (entered, observed) = oneshot::channel();
    let (release, released) = mpsc::channel();
    let mut release = Release(Some(release));
    *clock.0.lock().unwrap() = Some(Gate {
        entered,
        release: released,
    });
    let mut caller = atom_http::connect(atom_address, &identity).await?;
    caller.write_all(format!("PUT {TARGET} HTTP/1.1\r\nHost: unrelated.example\r\nAuthorization: Bearer {token}\r\nContent-Length: 0\r\n\r\n").as_bytes()).await?;
    tokio::time::timeout(WAIT, observed).await??;
    drop(caller); // The begun backend original, not this stream, owns custody.
    request_trigger.request();
    amqp_retire.request();
    native_retire.request();
    atom_retire.request();
    let (mut amqp, mut native, mut atom, trigger) = tokio::time::timeout(WAIT, serving).await??;
    assert_eq!(
        rpc.list_entities(admin_tls::request(admin_tls::list(), &token))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    retiring_peer
        .write_all(atom_http::wire(&token, TARGET).as_bytes())
        .await?;
    assert!(
        atom_http::read(&mut retiring_peer)
            .await?
            .starts_with("HTTP/1.1 503")
    );
    let started = trigger.started().unwrap();
    assert!(!trigger.request());
    assert_eq!(trigger.started(), Some(started));
    let timer_started = Arc::new(AtomicBool::new(false));
    let latch = Arc::new(Shutdown::default());
    let (witness, observed) = mpsc::channel();
    let timer_thread = thread::spawn({
        let latch = latch.clone();
        let started = timer_started.clone();
        let handle = handle.clone();
        let namespace = namespace.clone();
        move || {
            let deadline = std::time::Instant::now() + WAIT;
            while !latch.is_signalled() {
                assert!(std::time::Instant::now() < deadline);
                thread::sleep(Duration::from_millis(5));
            }
            started.store(true, Ordering::SeqCst);
            let outcome = handle.submit_blocking(
                namespace,
                EntityPath::new("orders").unwrap(),
                CommandKind::Peek {
                    from_sequence: SequenceNumber::new(0),
                    max_messages: 1,
                    session: None,
                },
            );
            let _ = witness.send(matches!(outcome, Ok(CommandOutcome::Peeked(_))));
        }
    });
    let mut timer = Some(TimerThread::new(latch, timer_thread));
    let short = ShutdownBudget::new(Instant::now(), Duration::from_millis(100));
    assert!(matches!(
        shut_down_all_transports(
            &short,
            &mut amqp,
            Some(&mut native),
            Some(&mut atom),
            &mut timer,
            &mut broker
        )
        .await,
        Err(ShutdownExpired {
            stage: ShutdownStage::ProtocolCleanup
        })
    ));
    assert!(broker.is_some() && timer.is_some());
    assert!(!timer_started.load(Ordering::SeqCst));
    for address in [amqp_address, native_address, atom_address] {
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
    }
    release.0.take().unwrap().send(())?;
    // Retry a cancelled borrower; this is not a reset of a running process budget.
    let budget = ShutdownBudget::new(Instant::now(), WAIT);
    shut_down_all_transports(
        &budget,
        &mut amqp,
        Some(&mut native),
        Some(&mut atom),
        &mut timer,
        &mut broker,
    )
    .await?;
    assert!(observed.recv_timeout(WAIT)?);
    assert!(broker.is_none() && timer.is_none());
    assert!(native.take_failure().is_none() && atom.take_failure().is_none());
    drop((amqp, native, atom, handle, connection));
    let reopened = Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::FjallStore::open(directory.path())?),
        SystemClock,
    ));
    assert!(
        admin_api::EntityBackend::get(
            &reopened.handle(),
            namespace,
            admin_api::EntityTarget::Entity(EntityPath::new("orders")?)
        )
        .await?
        .is_some()
    );
    Ok(())
}

#[cfg(target_os = "linux")]
async fn accept_failure(source: u8) -> TestResult {
    use rustix::{
        io::dup,
        net::{Shutdown as SocketShutdown, shutdown},
    };
    let mut broker = Some(Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::MemoryStore::default()),
        SystemClock,
    )));
    let handle = broker.as_ref().unwrap().handle();
    let namespace = NamespaceName::new("tenant")?;
    let identity = Identity::new()?;
    let amqp_socket = TcpListener::bind("127.0.0.1:0").await?;
    let amqp_address = amqp_socket.local_addr()?;
    let native_socket = TcpListener::bind("127.0.0.1:0").await?;
    let native_address = native_socket.local_addr()?;
    let atom_socket = TcpListener::bind("127.0.0.1:0").await?;
    let atom_address = atom_socket.local_addr()?;
    let fault = dup(match source {
        0 => &amqp_socket,
        1 => &native_socket,
        _ => &atom_socket,
    })?;
    let mut amqp =
        AmqpListener::new(handle.clone(), namespace.clone()).into_tcp_service(amqp_socket);
    let mut native = AdminListenerService::new(
        native_socket,
        AdminTlsConfig::new(
            identity.native()?,
            JwtPolicy::from_json(&admin_tls::policy().to_string())?,
            ResourceScope::parse(admin_tls::ROOT)?,
        )?,
        handle,
        namespace,
        WAIT,
    );
    let mut atom = HttpsListener::new(atom_socket, identity.atom()?);
    let serving = tokio::spawn(async move {
        let trigger = ShutdownTrigger::default();
        serve_all_transports(&mut amqp, Some(&mut native), Some(&mut atom), &trigger).await;
        (amqp, native, atom, trigger)
    });
    let token = admin_tls::sign(admin_tls::claims("operator"));
    let mut rpc = admin_tls::client(native_address, &identity.ca_pem, "localhost").await?;
    rpc.list_entities(admin_tls::request(admin_tls::list(), &token))
        .await?;
    assert!(
        atom_http::exchange(atom_address, &identity, &token, TARGET)
            .await?
            .starts_with("HTTP/1.1 501")
    );
    let connection = amqp::ClientConnection::builder()
        .container_id("atom-accept-failure")
        .open(format!("amqp://{amqp_address}").as_str())
        .await?;
    let mut peer = atom_http::connect(atom_address, &identity).await?;
    shutdown(&fault, SocketShutdown::Both)?;
    let (mut amqp, mut native, mut atom, trigger) = tokio::time::timeout(WAIT, serving).await??;
    let started = trigger.started().unwrap();
    assert!(!trigger.request());
    assert_eq!(trigger.started(), Some(started));
    peer.write_all(atom_http::wire(&token, TARGET).as_bytes())
        .await?;
    assert!(
        atom_http::read(&mut peer)
            .await?
            .starts_with("HTTP/1.1 503")
    );
    assert_eq!(
        rpc.list_entities(admin_tls::request(admin_tls::list(), &token))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    let budget = ShutdownBudget::new(started, WAIT);
    let mut timer = None;
    let exit = shut_down_all_transports(
        &budget,
        &mut amqp,
        Some(&mut native),
        Some(&mut atom),
        &mut timer,
        &mut broker,
    )
    .await?;
    assert!(broker.is_none());
    if source == 0 {
        assert!(matches!(
            exit,
            Some(AmqpListenerExit::Complete(Err(
                AmqpListenerFailure::Accept(_)
            )))
        ));
    } else {
        assert!(matches!(exit, Some(AmqpListenerExit::Complete(Ok(())))));
    }
    assert_eq!(native.take_failure().is_some(), source == 1);
    assert_eq!(atom.take_failure().is_some(), source == 2);
    for address in [amqp_address, native_address, atom_address] {
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
    }
    drop(connection);
    Ok(())
}
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn atom_failure_retires_native_and_amqp() -> TestResult {
    accept_failure(2).await
}
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn native_failure_retires_atom_and_amqp() -> TestResult {
    accept_failure(1).await
}
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn amqp_failure_retires_both_https_listeners_with_original_error() -> TestResult {
    accept_failure(0).await
}
