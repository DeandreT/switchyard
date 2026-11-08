//! The shutdown sequence run in process, with stages stalled on purpose.
//!
//! The binary tests cover real signals; these cover what a signal cannot
//! arrange on demand: a stage that never finishes, and witnesses that observe
//! one stage from inside the next.

use std::{
    error::Error,
    future::Future,
    net::{SocketAddr, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use amqp::{
    ClientConnection as Connection, ClientReceiver as Receiver, ClientSender as Sender,
    ClientSession as Session, Message, Outcome, Properties,
};
use domain::{
    CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueConfig, SequenceNumber,
    StateMachine, Timestamp,
};
use protocol_amqp::{AmqpListener, AmqpListenerService, BrokerRejection};
use server::{
    Broker, BrokerHandle, Clock, LocalProposer, Shutdown, ShutdownBudget, ShutdownExpired,
    ShutdownStage, SubmitError, SystemClock, TimerThread, shut_down,
};
use storage::FjallStore;
use tempfile::TempDir;
use tokio::{net::TcpListener, time::Instant};

#[path = "../src/test_deadline.rs"]
mod test_deadline;

use test_deadline::WAIT;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const QUEUE: &str = "orders";
/// Short enough to keep a deliberately stalled stage quick, long enough that
/// an unstalled stage on a loaded host is not mistaken for one.
const STALL_BUDGET: Duration = Duration::from_secs(2);

fn names() -> (NamespaceName, EntityPath) {
    (
        NamespaceName::new("tenant").expect("a valid namespace"),
        EntityPath::new(QUEUE).expect("a valid entity"),
    )
}

fn create_queue(handle: &BrokerHandle) -> TestResult {
    let (namespace, entity) = names();
    handle.submit_blocking(
        namespace,
        entity,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    Ok(())
}

fn send_command(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.to_owned(),
        body: Vec::new(),
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_at: None,
        envelope: None,
    }
}

fn message(id: &str) -> Message {
    let mut message = Message::builder()
        .body(amqp::Body::Data(vec![id.as_bytes().to_vec().into()]))
        .build();
    message.properties = Some(Properties {
        message_id: Some(id.to_owned().into()),
        ..Properties::default()
    });
    message
}

fn stored_ids(directory: &std::path::Path) -> TestResult<Vec<String>> {
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(FjallStore::open(directory)?),
        SystemClock,
    ));
    let (namespace, entity) = names();
    let outcome = broker.handle().submit_blocking(
        namespace,
        entity,
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 100,
            session: None,
        },
    )?;
    let CommandOutcome::Peeked(deliveries) = outcome else {
        return Err(format!("a peek answered {outcome:?}").into());
    };
    Ok(deliveries
        .into_iter()
        .map(|delivery| delivery.message_id)
        .collect())
}

/// A listener on an ephemeral port, serving until its retirement is requested.
async fn listen<B: protocol_amqp::Broker>(
    broker: B,
) -> TestResult<(SocketAddr, AmqpListenerService<B>)> {
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let service = AmqpListener::new(broker, names().0).into_tcp_service(socket);
    Ok((address, service))
}

async fn connect(address: SocketAddr) -> TestResult<Connection> {
    Ok(Connection::builder()
        .container_id("process-shutdown-test-client")
        .open(format!("amqp://{address}").as_str())
        .await?)
}

/// Serves `service` on a task until `request` retires it, then hands it back.
fn serve<B: protocol_amqp::Broker>(
    mut service: AmqpListenerService<B>,
) -> (
    protocol_amqp::AmqpListenerRetirement,
    impl Future<Output = AmqpListenerService<B>>,
) {
    let retirement = service.retirement_handle();
    let serving = tokio::spawn(async move {
        service.serve().await;
        service
    });
    (retirement, async move {
        tokio::time::timeout(WAIT, serving)
            .await
            .expect("the listener pump observes retirement")
            .expect("the listener pump does not panic")
    })
}

/// A timer stand-in that waits for its latch, then runs `witness` while the
/// broker is still meant to be up.
fn witness_timer(witness: impl FnOnce() + Send + 'static) -> TimerThread {
    let latch = Arc::new(Shutdown::default());
    let thread = thread::spawn({
        let latch = Arc::clone(&latch);
        move || {
            let deadline = std::time::Instant::now() + WAIT;
            while !latch.is_signalled() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the timer stage never began"
                );
                thread::sleep(Duration::from_millis(5));
            }
            witness();
        }
    });
    TimerThread::new(latch, thread)
}

/// Stages run in order: by the time the timer stage runs, the listener socket
/// is closed and connections are joined, yet the broker still commits; after
/// the broker stage it admits nothing, and the durable store reopens with
/// every committed send.
#[tokio::test(flavor = "multi_thread")]
async fn the_stages_run_in_order_and_keep_committed_work() -> TestResult {
    let directory = TempDir::new()?;
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(FjallStore::open(directory.path())?),
        SystemClock,
    ));
    let handle = broker.handle();
    create_queue(&handle)?;
    let (address, service) = listen(handle.clone()).await?;
    let (retirement, retired) = serve(service);

    let mut connection = connect(address).await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "sender", QUEUE).await?;
    assert!(matches!(
        tokio::time::timeout(WAIT, sender.send(message("client-committed"))).await??,
        Outcome::Accepted(_)
    ));
    let mut receiver = Receiver::attach(&mut session, "receiver", QUEUE).await?;
    let held = tokio::time::timeout(WAIT, receiver.recv()).await??;
    let pending_receive = tokio::spawn(async move { receiver.recv().await.map(|_| ()) });

    let (observed, observations) = mpsc::channel();
    let timer = witness_timer({
        let handle = handle.clone();
        move || {
            let listener_closed = TcpStream::connect(address).is_err();
            let (namespace, entity) = names();
            let committed = handle.submit_blocking(namespace, entity, send_command("from-timer"));
            observed
                .send((listener_closed, committed))
                .expect("the test waits for the witness");
        }
    });

    retirement.request();
    let mut service = retired.await;
    let mut timer = Some(timer);
    let mut broker = Some(broker);
    let budget = ShutdownBudget::new(Instant::now(), WAIT);
    let exit = shut_down(&budget, &mut service, &mut timer, &mut broker).await?;
    assert!(exit.is_some(), "the listener outcome is extracted once");
    assert!(timer.is_none() && broker.is_none(), "every stage ran");

    let (listener_closed, committed) = observations.recv_timeout(WAIT)?;
    assert!(
        listener_closed,
        "protocol cleanup closed the listener first"
    );
    assert!(
        matches!(committed, Ok(CommandOutcome::Sent { .. })),
        "the broker outlives the timer stage: {committed:?}"
    );
    assert!(
        tokio::time::timeout(WAIT, pending_receive).await??.is_err(),
        "the live receiving link ended with its connection"
    );
    assert!(
        tokio::time::timeout(WAIT, sender.send(message("after")))
            .await?
            .is_err(),
        "nothing is accepted after shutdown"
    );
    let (namespace, entity) = names();
    assert_eq!(
        handle.submit_blocking(namespace, entity, send_command("late")),
        Err(SubmitError::BrokerStopped),
        "a stopped broker admits nothing new"
    );
    drop((held, session, connection, service, handle));

    let mut stored = stored_ids(directory.path())?;
    stored.sort();
    assert_eq!(stored, ["client-committed", "from-timer"]);
    Ok(())
}

/// A broker whose sends never return, as a stalled commit would look from
/// the protocol edge.
#[derive(Clone)]
struct StalledSends {
    inner: BrokerHandle,
    entered: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl protocol_amqp::Broker for StalledSends {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        if matches!(
            kind,
            CommandKind::SendContent { .. } | CommandKind::SendContentBatch { .. }
        ) {
            if let Some(entered) = self.entered.lock().expect("not poisoned").take() {
                let _ = entered.send(());
            }
            std::future::pending::<()>().await;
        }
        protocol_amqp::Broker::submit(&self.inner, namespace, entity, kind).await
    }

    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        protocol_amqp::Broker::deliverable(&self.inner, namespace, entity)
    }
}

/// Protocol cleanup waits for broker work a link already began, so a commit
/// that never returns holds it until the budget runs out. The stage is
/// reported, the timer and broker are left untouched, and the stalled send is
/// never acknowledged.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_protocol_cleanup_expires_before_the_timer_or_broker() -> TestResult {
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::MemoryStore::default()),
        SystemClock,
    ));
    let handle = broker.handle();
    create_queue(&handle)?;
    let (entered, entered_send) = tokio::sync::oneshot::channel();
    let (address, service) = listen(StalledSends {
        inner: handle.clone(),
        entered: Arc::new(Mutex::new(Some(entered))),
    })
    .await?;
    let (retirement, retired) = serve(service);

    let mut connection = connect(address).await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "sender", QUEUE).await?;
    let send = tokio::spawn(async move { sender.send(message("stalled")).await });
    tokio::time::timeout(WAIT, entered_send).await??;

    let timer_stopped = Arc::new(AtomicBool::new(false));
    let timer = witness_timer({
        let timer_stopped = Arc::clone(&timer_stopped);
        move || timer_stopped.store(true, Ordering::SeqCst)
    });
    retirement.request();
    let mut service = retired.await;
    let mut timer = Some(timer);
    let mut broker = Some(broker);
    let started = Instant::now();
    let budget = ShutdownBudget::new(started, STALL_BUDGET);
    assert_eq!(
        shut_down(&budget, &mut service, &mut timer, &mut broker)
            .await
            .err(),
        Some(ShutdownExpired {
            stage: ShutdownStage::ProtocolCleanup
        })
    );
    let elapsed = started.elapsed();
    assert!(elapsed >= STALL_BUDGET, "expired early: {elapsed:?}");
    assert!(elapsed < WAIT, "expiry is bounded by the budget");
    assert!(timer.is_some(), "the timer stage never started");
    assert!(broker.is_some(), "the broker stage never started");
    assert!(!timer_stopped.load(Ordering::SeqCst));
    assert!(
        !send.is_finished() || !matches!(send.await?, Ok(Outcome::Accepted(_))),
        "an uncommitted send is never acknowledged"
    );
    let (namespace, entity) = names();
    assert!(
        matches!(
            handle.submit_blocking(namespace, entity, send_command("still-up")),
            Ok(CommandOutcome::Sent { .. })
        ),
        "the broker is still serving after an earlier stage expired"
    );

    if let Some(timer) = timer.take() {
        timer.stop().await;
    }
    assert!(timer_stopped.load(Ordering::SeqCst));
    drop((session, connection, service));
    tokio::task::spawn_blocking(move || drop(broker)).await?;
    Ok(())
}

/// A timer thread that ignores its latch until released.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_timer_expires_before_the_broker_stops() -> TestResult {
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::MemoryStore::default()),
        SystemClock,
    ));
    let handle = broker.handle();
    create_queue(&handle)?;
    let (_address, service) = listen(handle.clone()).await?;
    let (retirement, retired) = serve(service);

    let (release, released) = mpsc::channel::<()>();
    let latch = Arc::new(Shutdown::default());
    let stalled = thread::spawn(move || {
        released
            .recv_timeout(WAIT)
            .expect("the test releases the stalled timer");
    });
    retirement.request();
    let mut service = retired.await;
    let mut timer = Some(TimerThread::new(latch, stalled));
    let mut broker = Some(broker);
    let started = Instant::now();
    let budget = ShutdownBudget::new(started, STALL_BUDGET);
    assert_eq!(
        shut_down(&budget, &mut service, &mut timer, &mut broker)
            .await
            .err(),
        Some(ShutdownExpired {
            stage: ShutdownStage::Timer
        })
    );
    assert!(started.elapsed() >= STALL_BUDGET);
    assert!(timer.is_none(), "the timer stage started");
    assert!(broker.is_some(), "the broker stage never started");
    let (namespace, entity) = names();
    assert!(matches!(
        handle.submit_blocking(namespace, entity, send_command("still-up")),
        Ok(CommandOutcome::Sent { .. })
    ));
    release.send(())?;
    drop(service);
    tokio::task::spawn_blocking(move || drop(broker)).await?;
    Ok(())
}

/// A clock that blocks the owner thread inside its first stamp once armed,
/// as a slow commit would.
struct StallingClock {
    armed: Arc<AtomicBool>,
    entered: Mutex<mpsc::Sender<()>>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl Clock for StallingClock {
    fn now(&self) -> Timestamp {
        if self.armed.swap(false, Ordering::SeqCst) {
            let _ = self.entered.lock().expect("not poisoned").send(());
            self.release
                .lock()
                .expect("not poisoned")
                .recv_timeout(WAIT)
                .expect("the test releases the stalled commit");
        }
        SystemClock.now()
    }
}

/// The broker stage joins the owner, which first finishes the command it is
/// applying. A commit that outlasts the budget expires the broker stage; the
/// command still completes and answers its caller once released.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_commit_expires_the_broker_stage_and_still_answers() -> TestResult {
    let armed = Arc::new(AtomicBool::new(false));
    let (entered, entered_commit) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::MemoryStore::default()),
        StallingClock {
            armed: Arc::clone(&armed),
            entered: Mutex::new(entered),
            release: Mutex::new(released),
        },
    ));
    let handle = broker.handle();
    create_queue(&handle)?;
    let (_address, service) = listen(handle.clone()).await?;
    let (retirement, retired) = serve(service);

    armed.store(true, Ordering::SeqCst);
    let admitted = thread::spawn({
        let handle = handle.clone();
        move || {
            let (namespace, entity) = names();
            handle.submit_blocking(namespace, entity, send_command("admitted"))
        }
    });
    entered_commit.recv_timeout(WAIT)?;

    retirement.request();
    let mut service = retired.await;
    let mut timer = None;
    let mut broker = Some(broker);
    let started = Instant::now();
    let budget = ShutdownBudget::new(started, STALL_BUDGET);
    assert_eq!(
        shut_down(&budget, &mut service, &mut timer, &mut broker)
            .await
            .err(),
        Some(ShutdownExpired {
            stage: ShutdownStage::Broker
        })
    );
    assert!(started.elapsed() >= STALL_BUDGET);
    assert!(broker.is_none(), "the broker stage started");
    assert!(
        !admitted.is_finished(),
        "the admitted command is still running"
    );

    release.send(())?;
    let outcome = tokio::task::spawn_blocking(move || admitted.join())
        .await?
        .expect("the submitting thread does not panic");
    assert!(
        matches!(outcome, Ok(CommandOutcome::Sent { .. })),
        "work admitted before the stop completes: {outcome:?}"
    );
    let deadline = Instant::now() + WAIT;
    loop {
        let (namespace, entity) = names();
        match handle.submit(namespace, entity, send_command("late")).await {
            Err(SubmitError::BrokerStopped) => break,
            other => {
                assert!(
                    Instant::now() < deadline,
                    "the abandoned broker stage never stopped the owner: {other:?}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    drop(service);
    Ok(())
}
