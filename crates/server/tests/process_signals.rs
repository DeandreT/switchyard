//! The real binary stopped by real signals.
//!
//! Each test starts `switchyard`, waits for it to accept connections, delivers
//! SIGTERM or SIGINT and reads the ordered shutdown stages back from its log.
//! Durable tests then reopen the Fjall directory the process closed and check
//! that what a client was told was committed is there, and nothing else is.

#![cfg(target_os = "linux")]

use std::{
    collections::BTreeMap,
    error::Error,
    io::{BufRead, BufReader, Read},
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use amqp::{
    ClientConnection as Connection, ClientReceiver as Receiver, ClientSender as Sender,
    ClientSession as Session, Message, Outcome, Properties,
};
use domain::{
    CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueConfig, SequenceNumber,
    StateMachine,
};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustix::process::{Pid, Signal, kill_process};
use server::{Broker, LocalProposer, SHUTDOWN_EXPIRED_EXIT_CODE, SystemClock};
use storage::FjallStore;
use tempfile::TempDir;

#[allow(dead_code)]
mod durable_node;

use durable_node::{Management, WAIT, lock_token_of};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const NAMESPACE: &str = "tenant";
const QUEUE: &str = "orders";
const STAGES: [&str; 4] = [
    "stage=listener-admission",
    "stage=protocol-cleanup",
    "stage=timer",
    "stage=broker",
];

/// One running `switchyard` process and everything it has logged so far.
struct Process {
    child: Option<Child>,
    lines: mpsc::Receiver<(Stream, String)>,
    log: Vec<String>,
    errors: Vec<String>,
    address: String,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Stream {
    Out,
    Err,
}

impl Process {
    /// Starts a process on an ephemeral port and waits until it accepts.
    fn start(arguments: &[&str]) -> TestResult<Self> {
        Self::start_with(arguments, &[])
    }

    fn start_with(arguments: &[&str], environment: &[(&str, &str)]) -> TestResult<Self> {
        let mut listen = vec!["--listen", "127.0.0.1:0"];
        listen.extend_from_slice(arguments);
        let mut process = Self::spawn(&listen, environment)?;
        let ready = process.wait_for("accepting AMQP connections")?;
        process.address = field(&ready, "address")
            .ok_or_else(|| format!("the ready line names no address: {ready}"))?;
        Ok(process)
    }

    /// Starts a process without waiting for it to accept.
    fn spawn(arguments: &[&str], environment: &[(&str, &str)]) -> TestResult<Self> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_switchyard"))
            .args(["--mode", "development"])
            .args(["--namespace", NAMESPACE])
            .args(arguments)
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .envs(environment.iter().copied())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let (sender, lines) = mpsc::channel();
        for (kind, stream) in [
            (
                Stream::Out,
                Box::new(child.stdout.take().expect("piped stdout")) as Box<dyn Read + Send>,
            ),
            (
                Stream::Err,
                Box::new(child.stderr.take().expect("piped stderr")),
            ),
        ] {
            let sender = sender.clone();
            thread::spawn(move || {
                for line in BufReader::new(stream).lines().map_while(Result::ok) {
                    if sender.send((kind, without_escapes(&line))).is_err() {
                        break;
                    }
                }
            });
        }
        Ok(Self {
            child: Some(child),
            lines,
            log: Vec::new(),
            errors: Vec::new(),
            address: String::new(),
        })
    }

    fn record(&mut self, (kind, line): (Stream, String)) -> String {
        if kind == Stream::Err {
            self.errors.push(line.clone());
        }
        self.log.push(line.clone());
        line
    }

    fn pid(&self) -> Pid {
        Pid::from_child(self.child.as_ref().expect("the process is owned"))
    }

    fn signal(&self, signal: Signal) -> TestResult {
        Ok(kill_process(self.pid(), signal)?)
    }

    /// Reads log lines until one contains `needle`.
    fn wait_for(&mut self, needle: &str) -> TestResult<String> {
        let deadline = Instant::now() + WAIT;
        if let Some(line) = self.log.iter().find(|line| line.contains(needle)) {
            return Ok(line.clone());
        }
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(remaining) {
                Ok(line) => {
                    let line = self.record(line);
                    if line.contains(needle) {
                        return Ok(line);
                    }
                }
                Err(_) => {
                    return Err(format!(
                        "no log line contained {needle:?}; log so far:\n{}",
                        self.log.join("\n")
                    )
                    .into());
                }
            }
        }
    }

    /// Waits for the exit, then for every line the process wrote.
    fn wait_exit(&mut self) -> TestResult<ExitStatus> {
        let deadline = Instant::now() + WAIT;
        let child = self.child.as_mut().expect("the process is owned");
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if Instant::now() >= deadline {
                return Err("the process did not exit before the deadline".into());
            }
            thread::sleep(Duration::from_millis(10));
        };
        self.child = None;
        // The readers end at end of stream, which follows the exit.
        while let Ok(line) = self.lines.recv_timeout(WAIT) {
            self.record(line);
        }
        Ok(status)
    }

    /// Reads log lines until `count` of them contain `needle`.
    fn wait_for_count(&mut self, needle: &str, count: usize) -> TestResult {
        let deadline = Instant::now() + WAIT;
        while self.count(needle) < count {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(remaining) {
                Ok(line) => {
                    self.record(line);
                }
                Err(_) => {
                    return Err(format!(
                        "fewer than {count} lines contained {needle:?}:\n{}",
                        self.log.join("\n")
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    async fn wait_exit_blocking(&mut self) -> TestResult<ExitStatus> {
        tokio::task::block_in_place(|| self.wait_exit())
    }

    fn count(&self, needle: &str) -> usize {
        self.log.iter().filter(|line| line.contains(needle)).count()
    }

    fn position(&self, needle: &str) -> Option<usize> {
        self.log.iter().position(|line| line.contains(needle))
    }

    /// Every stage finished exactly once, in order, then shutdown completed.
    fn assert_ordered_shutdown(&self) {
        let mut previous = None;
        for needle in STAGES.iter().copied().chain(["shutdown complete"]) {
            assert_eq!(
                self.count(needle),
                1,
                "{needle} appears once in:\n{}",
                self.log.join("\n")
            );
            let position = self.position(needle);
            assert!(
                position > previous,
                "{needle} follows the previous stage in:\n{}",
                self.log.join("\n")
            );
            previous = position;
        }
    }

    async fn connect(&self) -> TestResult<Connection> {
        Ok(Connection::builder()
            .container_id("process-signal-test-client")
            .open(format!("amqp://{}", self.address).as_str())
            .await?)
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Removes ANSI escape sequences in case the log is colored anyway.
fn without_escapes(line: &str) -> String {
    let mut plain = String::with_capacity(line.len());
    let mut characters = line.chars();
    while let Some(character) = characters.next() {
        if character == '\u{1b}' {
            for terminator in characters.by_ref() {
                if terminator.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            plain.push(character);
        }
    }
    plain
}

fn field(line: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    line.split_whitespace()
        .find_map(|token| token.strip_prefix(prefix.as_str()))
        .map(str::to_owned)
}

fn assert_success(status: ExitStatus, process: &Process) {
    assert_eq!(
        status.code(),
        Some(0),
        "a shutdown within budget exits successfully; log:\n{}",
        process.log.join("\n")
    );
}

fn names() -> TestResult<(NamespaceName, EntityPath)> {
    Ok((NamespaceName::new(NAMESPACE)?, EntityPath::new(QUEUE)?))
}

/// Creates the queue in `directory` and closes the store again, so the process
/// under test starts on a store that already has it.
fn create_queue(directory: &Path, config: QueueConfig) -> TestResult {
    let (namespace, entity) = names()?;
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(FjallStore::open(directory)?),
        SystemClock,
    ));
    broker
        .handle()
        .submit_blocking(namespace, entity, CommandKind::CreateQueue { config })?;
    drop(broker);
    Ok(())
}

/// Message IDs on the queue, counted, read from the directory after the
/// process that wrote it has exited.
fn stored_message_ids(directory: &Path) -> TestResult<BTreeMap<String, usize>> {
    let (namespace, entity) = names()?;
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(FjallStore::open(directory)?),
        SystemClock,
    ));
    let mut counts = BTreeMap::new();
    let mut from = SequenceNumber::new(0);
    loop {
        let outcome = broker.handle().submit_blocking(
            namespace.clone(),
            entity.clone(),
            CommandKind::Peek {
                from_sequence: from,
                max_messages: 100,
                session: None,
            },
        )?;
        let CommandOutcome::Peeked(deliveries) = outcome else {
            return Err(format!("a peek answered {outcome:?}").into());
        };
        let Some(last) = deliveries.last().map(|delivery| delivery.sequence) else {
            break;
        };
        for delivery in deliveries {
            *counts.entry(delivery.message_id).or_insert(0) += 1;
        }
        from = SequenceNumber::new(last.as_u64() + 1);
    }
    drop(broker);
    Ok(counts)
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

#[test]
fn an_idle_process_stops_in_order_on_either_signal() -> TestResult {
    for (signal, name) in [(Signal::TERM, "SIGTERM"), (Signal::INT, "SIGINT")] {
        let mut process = Process::start(&["--storage", "memory"])?;
        process.signal(signal)?;
        let status = process.wait_exit()?;
        assert_success(status, &process);
        assert_eq!(process.count("shutting down"), 1);
        assert!(
            process
                .log
                .iter()
                .any(|line| line.contains("shutting down") && line.contains(name)),
            "the log names {name}:\n{}",
            process.log.join("\n")
        );
        process.assert_ordered_shutdown();
    }
    Ok(())
}

/// Holds the timer stage open in a debug build of the binary, so shutdown is
/// observably in progress while later signals arrive.
const STALL_TIMER: &str = "SWITCHYARD_TEST_STALL_TIMER_MILLIS";

#[test]
fn repeated_signals_join_the_shutdown_in_progress() -> TestResult {
    let mut process = Process::start_with(&["--storage", "memory"], &[(STALL_TIMER, "8000")])?;
    process.signal(Signal::TERM)?;
    // Protocol cleanup is done and the stalled timer stage holds shutdown open.
    process.wait_for("stage=protocol-cleanup")?;
    for (signal, name, joined) in [
        (Signal::INT, "SIGINT", 1),
        (Signal::TERM, "SIGTERM", 2),
        (Signal::INT, "SIGINT", 3),
    ] {
        process.signal(signal)?;
        process.wait_for_count("shutdown already in progress", joined)?;
        assert!(
            process
                .log
                .iter()
                .rev()
                .find(|line| line.contains("shutdown already in progress"))
                .is_some_and(|line| line.contains(name)),
            "the joined signal is named {name}:\n{}",
            process.log.join("\n")
        );
    }
    assert!(
        process.count("stage=timer") == 0,
        "the signals arrived while the timer stage was still running"
    );
    let status = process.wait_exit()?;
    assert_success(status, &process);
    assert_eq!(
        process.count("shutting down"),
        1,
        "only the first signal starts shutdown:\n{}",
        process.log.join("\n")
    );
    assert_eq!(process.count("shutdown already in progress"), 3);
    process.assert_ordered_shutdown();
    Ok(())
}

/// A startup that fails after the timer and broker are running still stops
/// them within the budget, keeping its own error and status.
#[test]
fn a_startup_failure_after_signal_registration_exits_promptly() -> TestResult {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = occupied.local_addr()?.to_string();
    let refused = format!("switchyard: could not listen on {address}: ");

    let mut process = Process::spawn(&["--storage", "memory", "--listen", &address], &[])?;
    let status = process.wait_exit()?;
    assert_eq!(status.code(), Some(1), "{}", process.log.join("\n"));
    assert_eq!(process.errors.len(), 1, "{:?}", process.errors);
    assert!(
        process.errors[0].starts_with(&refused),
        "the startup error is unchanged: {:?}",
        process.errors
    );
    assert!(
        !process
            .log
            .iter()
            .any(|line| line.contains("accepting AMQP"))
    );

    // A teardown that overruns is bounded too: status 3, after the startup
    // error, naming the stage that did not finish.
    let mut process = Process::spawn(
        &[
            "--storage",
            "memory",
            "--listen",
            &address,
            "--shutdown-timeout-secs",
            "1",
        ],
        &[(STALL_TIMER, "600000")],
    )?;
    let status = process.wait_exit()?;
    assert_eq!(
        status.code(),
        Some(i32::from(SHUTDOWN_EXPIRED_EXIT_CODE)),
        "{}",
        process.log.join("\n")
    );
    assert_eq!(process.errors.len(), 2, "{:?}", process.errors);
    assert!(
        process.errors[0].starts_with(&refused),
        "{:?}",
        process.errors
    );
    assert_eq!(
        process.errors[1],
        "switchyard: the shutdown budget expired during the timer stage"
    );
    drop(occupied);
    Ok(())
}

/// An overrun shutdown exits 3 without finishing, and leaves a store a new
/// process reopens with every committed message.
#[tokio::test(flavor = "multi_thread")]
async fn an_expired_shutdown_exits_three_and_keeps_committed_state() -> TestResult {
    let directory = TempDir::new()?;
    create_queue(directory.path(), QueueConfig::default())?;
    let data_dir = directory.path().to_str().ok_or("a UTF-8 path")?.to_owned();
    let arguments = ["--storage", "fjall", "--data-dir", data_dir.as_str()];
    let mut stalled = arguments.to_vec();
    stalled.extend(["--shutdown-timeout-secs", "1"]);
    let mut process = Process::start_with(&stalled, &[(STALL_TIMER, "600000")])?;

    let mut connection = process.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "sender", QUEUE).await?;
    assert!(matches!(
        tokio::time::timeout(WAIT, sender.send(message("committed-1"))).await??,
        Outcome::Accepted(_)
    ));
    let signalled = Instant::now();
    process.signal(Signal::TERM)?;
    let status = tokio::task::block_in_place(|| process.wait_exit())?;
    assert_eq!(
        status.code(),
        Some(i32::from(SHUTDOWN_EXPIRED_EXIT_CODE)),
        "{}",
        process.log.join("\n")
    );
    assert!(signalled.elapsed() >= Duration::from_secs(1));
    assert_eq!(
        process.errors,
        ["switchyard: the shutdown budget expired during the timer stage"]
    );
    assert!(
        process.log.iter().any(|line| {
            line.contains("shutdown budget expired") && line.contains("stage=timer")
        })
    );
    for finished in ["stage=listener-admission", "stage=protocol-cleanup"] {
        assert_eq!(process.count(finished), 1, "{finished} finished");
    }
    for unfinished in ["stage=timer", "stage=broker"] {
        assert!(
            !process
                .log
                .iter()
                .any(|line| line.contains("shutdown stage finished") && line.contains(unfinished)),
            "{unfinished} never finished"
        );
    }
    assert_eq!(process.count("shutdown complete"), 0);
    drop((sender, session, connection));

    let stored = stored_message_ids(directory.path())?;
    assert_eq!(stored.get("committed-1"), Some(&1), "{stored:?}");
    let mut reopened = Process::start(&arguments)?;
    reopened.signal(Signal::TERM)?;
    let status = reopened.wait_exit_blocking().await?;
    assert_success(status, &reopened);
    reopened.assert_ordered_shutdown();
    assert_eq!(stored_message_ids(directory.path())?, stored);
    Ok(())
}

#[test]
fn a_tls_websocket_listener_retires_on_sigterm() -> TestResult {
    let directory = TempDir::new()?;
    let certificate = directory.path().join("certificate.pem");
    let private_key = directory.path().join("private-key.pem");
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec![String::from("localhost")])?;
    std::fs::write(&certificate, cert.pem())?;
    std::fs::write(&private_key, key_pair.serialize_pem())?;
    for transport in ["amqp-tcp", "amqp-websockets"] {
        let mut process = Process::start(&[
            "--storage",
            "memory",
            "--transport",
            transport,
            "--tls-certificate",
            certificate.to_str().ok_or("a UTF-8 path")?,
            "--tls-private-key",
            private_key.to_str().ok_or("a UTF-8 path")?,
        ])?;
        process.signal(Signal::TERM)?;
        let status = process.wait_exit()?;
        assert_success(status, &process);
        process.assert_ordered_shutdown();
    }
    Ok(())
}

/// A connected client with a sender, a receiver holding an unsettled lock and
/// sends still in flight when SIGTERM arrives. Every send the client saw
/// accepted is on disk exactly once after the process exits; a send that is
/// not on disk was never reported accepted.
#[tokio::test(flavor = "multi_thread")]
async fn live_links_drain_and_only_committed_sends_survive_reopen() -> TestResult {
    const IN_FLIGHT: usize = 16;
    let directory = TempDir::new()?;
    create_queue(directory.path(), QueueConfig::default())?;
    let data_dir = directory.path().to_str().ok_or("a UTF-8 path")?.to_owned();
    let arguments = ["--storage", "fjall", "--data-dir", data_dir.as_str()];
    let mut process = Process::start(&arguments)?;

    let mut connection = process.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "committed-sender", QUEUE).await?;
    for id in ["committed-1", "committed-2"] {
        assert!(matches!(
            tokio::time::timeout(WAIT, sender.send(message(id))).await??,
            Outcome::Accepted(_)
        ));
    }
    let mut receiver = Receiver::attach(&mut session, "locked-receiver", QUEUE).await?;
    let locked = tokio::time::timeout(WAIT, receiver.recv()).await??;
    let locked_id = match locked
        .message()
        .properties
        .as_ref()
        .and_then(|properties| properties.message_id.clone())
    {
        Some(amqp::MessageId::String(id)) => id,
        other => return Err(format!("the delivery carried {other:?}").into()),
    };
    assert_eq!(locked_id, "committed-1");
    // Left unsettled: the receiving link holds this lock at shutdown, and a
    // second receive keeps a request pending on the link.
    let pending_receive = tokio::spawn(async move {
        let next = receiver.recv().await;
        (receiver, next)
    });

    let mut in_flight = Vec::with_capacity(IN_FLIGHT);
    for index in 0..IN_FLIGHT {
        let mut sender = Sender::attach(&mut session, format!("in-flight-{index}"), QUEUE).await?;
        let id = format!("in-flight-{index}");
        in_flight.push(tokio::spawn(async move {
            let outcome = sender.send(message(&id)).await;
            (id, matches!(outcome, Ok(Outcome::Accepted(_))))
        }));
    }
    process.signal(Signal::TERM)?;

    let status = tokio::task::block_in_place(|| process.wait_exit())?;
    assert_success(status, &process);
    process.assert_ordered_shutdown();

    let mut accepted = Vec::new();
    for send in in_flight {
        let (id, was_accepted) = tokio::time::timeout(WAIT, send).await??;
        if was_accepted {
            accepted.push(id);
        }
    }
    let (_receiver, next) = tokio::time::timeout(WAIT, pending_receive).await??;
    if let Ok(delivery) = next {
        // A delivery that raced the signal is still a committed message; its
        // lock was never settled, so it remains on disk.
        let id = delivery
            .message()
            .properties
            .as_ref()
            .and_then(|properties| properties.message_id.clone());
        assert!(id.is_some());
    }
    drop(session);
    drop(connection);

    let stored = stored_message_ids(directory.path())?;
    for (id, copies) in &stored {
        assert_eq!(*copies, 1, "{id} is stored once: {stored:?}");
    }
    for id in ["committed-1", "committed-2"] {
        assert!(stored.contains_key(id), "{id} survives reopen: {stored:?}");
    }
    for id in &accepted {
        assert!(
            stored.contains_key(id),
            "{id} was reported accepted, so it was committed: {stored:?}"
        );
    }
    for id in stored.keys() {
        assert!(
            id.starts_with("committed-") || id.starts_with("in-flight-"),
            "nothing unsent is stored: {stored:?}"
        );
    }

    // The process closed the store cleanly: a new one opens it, serves and
    // stops in order again.
    let mut reopened = Process::start(&arguments)?;
    let mut connection = reopened.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "after-reopen", QUEUE).await?;
    assert!(matches!(
        tokio::time::timeout(WAIT, sender.send(message("committed-3"))).await??,
        Outcome::Accepted(_)
    ));
    reopened.signal(Signal::INT)?;
    let status = tokio::task::block_in_place(|| reopened.wait_exit())?;
    assert_success(status, &reopened);
    reopened.assert_ordered_shutdown();
    drop(session);
    drop(connection);
    let after = stored_message_ids(directory.path())?;
    assert_eq!(after.get("committed-3"), Some(&1));
    for (id, copies) in &stored {
        assert_eq!(
            after.get(id),
            Some(copies),
            "a second shutdown changed nothing already committed"
        );
    }
    Ok(())
}

#[test]
fn the_expired_status_is_distinct_from_a_refused_startup() {
    // The binary reports a refused startup as 1; see startup_guards.
    assert_ne!(i32::from(SHUTDOWN_EXPIRED_EXIT_CODE), 1);
    assert_ne!(SHUTDOWN_EXPIRED_EXIT_CODE, 0);
}

fn message_id(delivery: &amqp::ClientDelivery) -> TestResult<String> {
    match delivery
        .message()
        .properties
        .as_ref()
        .and_then(|properties| properties.message_id.clone())
    {
        Some(amqp::MessageId::String(id)) => Ok(id),
        other => Err(format!("the delivery carried {other:?}").into()),
    }
}

/// Dispositions around shutdown on Fjall. A complete or abandon the broker
/// answered before the signal stays settled after reopen; a lock nobody
/// settled before the process stopped returns the message to ready once it
/// expires; a complete racing the signal is acknowledged only if it
/// committed, though a committed one may lose its answer to the shutdown. No message is settled twice or redelivered more than once.
#[tokio::test(flavor = "multi_thread")]
async fn settlements_committed_before_stop_persist_and_cut_off_locks_return() -> TestResult {
    const LOCK_MILLIS: u64 = 2_000;
    let directory = TempDir::new()?;
    create_queue(
        directory.path(),
        QueueConfig {
            lock_duration_millis: LOCK_MILLIS,
            ..QueueConfig::default()
        },
    )?;
    let data_dir = directory.path().to_str().ok_or("a UTF-8 path")?.to_owned();
    let arguments = ["--storage", "fjall", "--data-dir", data_dir.as_str()];
    let mut process = Process::start(&arguments)?;

    let mut connection = process.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "sender", QUEUE).await?;
    let ids = [
        "completed-by-management",
        "completed-by-link",
        "abandoned-by-management",
        "racing-complete",
        "held-lock",
    ];
    for id in ids {
        assert!(matches!(
            tokio::time::timeout(WAIT, sender.send(message(id))).await??,
            Outcome::Accepted(_)
        ));
    }
    let mut receiver = Receiver::attach(&mut session, "receiver", QUEUE).await?;
    let mut deliveries = BTreeMap::new();
    for _ in ids {
        let delivery = tokio::time::timeout(WAIT, receiver.recv()).await??;
        deliveries.insert(message_id(&delivery)?, delivery);
    }
    let mut management = Management::attach(&mut session, QUEUE).await?;
    for (id, status) in [
        ("completed-by-management", "completed"),
        ("abandoned-by-management", "abandoned"),
    ] {
        let (code, response) = management
            .update_disposition("receiver", lock_token_of(&deliveries[id]), status, &[])
            .await?;
        assert_eq!(code, 200, "{id} {status} committed: {response:?}");
    }
    receiver.accept(&deliveries["completed-by-link"]).await?;
    management
        .wait_until("the link completion commits", |entries| {
            !entries.iter().any(|entry| {
                matches!(
                    entry.message.properties.as_ref().and_then(|p| p.message_id.clone()),
                    Some(amqp::MessageId::String(id)) if id == "completed-by-link"
                )
            })
        })
        .await?;

    // The racing complete and the signal go out together; whichever wins,
    // the client hears 200 only for a committed completion.
    let racing_lock = lock_token_of(&deliveries["racing-complete"]);
    let (racing, signalled) = tokio::join!(
        management.update_disposition("receiver", racing_lock, "completed", &[]),
        async {
            process
                .signal(Signal::TERM)
                .map_err(|error| error.to_string())
        }
    );
    signalled?;
    let racing_acknowledged = matches!(racing, Ok((200, _)));
    let status = process.wait_exit_blocking().await?;
    assert_success(status, &process);
    process.assert_ordered_shutdown();
    drop((management, receiver, sender, session, connection));

    let stored = stored_message_ids(directory.path())?;
    let mut expected = BTreeMap::from([
        (String::from("abandoned-by-management"), 1),
        (String::from("held-lock"), 1),
    ]);
    // An acknowledged completion committed. One whose answer the shutdown
    // cut off may have committed or not, but is never stored twice.
    match stored.get("racing-complete") {
        None => {}
        Some(1) if !racing_acknowledged => {
            expected.insert(String::from("racing-complete"), 1);
        }
        other => panic!(
            "racing-complete stored {other:?} after an acknowledgement of {racing_acknowledged}"
        ),
    }
    assert_eq!(
        stored, expected,
        "committed settlements persist, nothing else is settled"
    );

    // Every remaining message is delivered once more: the abandoned one at
    // once, the unsettled locks after they expire.
    let mut reopened = Process::start(&arguments)?;
    let mut connection = reopened.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut receiver = Receiver::attach(&mut session, "after-reopen", QUEUE).await?;
    let mut redelivered = BTreeMap::new();
    while redelivered.len() < expected.len() {
        let delivery = tokio::time::timeout(WAIT, receiver.recv()).await??;
        *redelivered.entry(message_id(&delivery)?).or_insert(0) += 1;
        receiver.accept(&delivery).await?;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(LOCK_MILLIS * 2), receiver.recv())
            .await
            .is_err(),
        "nothing is delivered a second time"
    );
    assert_eq!(redelivered, expected);
    reopened.signal(Signal::TERM)?;
    let status = reopened.wait_exit_blocking().await?;
    assert_success(status, &reopened);
    drop((receiver, session, connection));
    assert!(
        stored_message_ids(directory.path())?.is_empty(),
        "the redelivered copies completed once"
    );
    Ok(())
}
