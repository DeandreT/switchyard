//! A single-node broker on a real Fjall directory that a test can stop and
//! restart in place, plus the AMQP management client those tests share.
//!
//! A restart drops every handle on the directory: the listener, the broker
//! owner thread and the store it owns. The reopened broker reads back only
//! what reached disk, so whatever a client sees afterwards came off the store.

use std::{error::Error, fmt::Display, time::Duration};

use amqp::{
    AnnotationKey, ApplicationProperties, Array, Body, ClientConnection as Connection,
    ClientDelivery, ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session,
    Message, OrderedMap, Outcome, Properties, Value,
};
use domain::{
    CommandKind, CommandOutcome, EntityPath, NamespaceName, SequenceNumber, StateMachine,
};
use protocol_amqp::{AmqpListenerExit, AmqpListenerRetirement};
use server::{Broker, LocalProposer, ManualClock};
use storage::{FjallStore, StateStore, StoreSnapshot};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};

pub type TestResult<T = ()> = Result<T, Box<dyn Error>>;

pub const NAMESPACE: &str = "tenant";
/// The broker clock when a node is created.
pub const START_MILLIS: u64 = 1_000;

#[path = "../../src/test_deadline.rs"]
mod test_deadline;

pub(crate) use test_deadline::WAIT;

pub fn phase(label: impl Display) {
    eprintln!(
        "[durable-content:{}] {label}",
        std::thread::current().name().unwrap_or("unnamed")
    );
}

struct Running {
    broker: Broker,
    clock: ManualClock,
    listener: Option<JoinHandle<Result<(), String>>>,
    listener_exit: Option<Result<(), String>>,
    retirement: AmqpListenerRetirement,
    address: String,
}

pub struct DurableNode {
    directory: TempDir,
    now: u64,
    running: Option<Running>,
}

impl DurableNode {
    /// Creates the entities on a fresh directory, then starts serving them.
    pub async fn create(setup: Vec<(&str, CommandKind)>) -> TestResult<Self> {
        let mut node = Self {
            directory: TempDir::new()?,
            now: START_MILLIS,
            running: None,
        };
        node.start().await?;
        for (entity, kind) in setup {
            node.submit(entity, kind)?;
        }
        Ok(node)
    }

    async fn start(&mut self) -> TestResult {
        phase("node start: store open");
        let clock = ManualClock::at(self.now);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(FjallStore::open(self.directory.path())?),
            clock.clone(),
        ));
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = format!("amqp://{}", socket.local_addr()?);
        let mut service =
            protocol_amqp::AmqpListener::new(broker.handle(), NamespaceName::new(NAMESPACE)?)
                .into_tcp_service(socket);
        let retirement = service.retirement_handle();
        let listener = tokio::spawn(async move {
            service.serve().await;
            service.finish().await;
            match service.take_exit() {
                Some(AmqpListenerExit::Complete(Ok(()))) => Ok(()),
                Some(AmqpListenerExit::Complete(Err(error))) => Err(error.to_string()),
                Some(AmqpListenerExit::ReportOnly(_)) => Err("listener owner panicked".into()),
                None => Err("listener originals were not drained".into()),
            }
        });
        self.running = Some(Running {
            broker,
            clock,
            listener: Some(listener),
            listener_exit: None,
            retirement,
            address,
        });
        phase("node start: ready");
        Ok(())
    }

    async fn stop(&mut self) -> TestResult {
        if let Some(running) = self.running.as_mut() {
            phase("node stop: retire and join original listener family");
            running.retirement.request();
            // Borrow the original handle. Cancellation must not remove custody
            // or let reopen run while accepted operations can still use the store.
            if let Some(listener) = running.listener.as_mut() {
                let exit = listener
                    .await
                    .map_err(|error| error.to_string())
                    .and_then(|exit| exit);
                running.listener_exit = Some(exit);
                running.listener = None;
            }
            running
                .listener_exit
                .as_ref()
                .expect("original listener exit")
                .as_ref()
                .map_err(Clone::clone)?;
            phase("node stop: listener family joined; broker join");
            let running = self.running.take().expect("the joined original node");
            drop(running.broker);
            phase("node stop: broker joined and store released");
        }
        Ok(())
    }

    /// Stops the broker, opens the directory on its own to run `inspect`
    /// against what is on disk, closes it again and starts a new broker.
    pub async fn reopen<R>(
        &mut self,
        inspect: impl FnOnce(&StateMachine<FjallStore>) -> TestResult<R>,
    ) -> TestResult<R> {
        self.stop().await?;
        phase("node reopen: isolated store open");
        let inspected = {
            let machine = StateMachine::new(FjallStore::open(self.directory.path())?);
            phase("node reopen: inspection");
            inspect(&machine)
        };
        phase("node reopen: isolated store dropped; restart");
        self.start().await?;
        inspected
    }

    /// Restarts on the same directory without looking at it.
    pub async fn restart(&mut self) -> TestResult {
        self.reopen(|_| Ok(())).await
    }

    /// Every stored row, read from disk across a restart.
    pub async fn snapshot(&mut self) -> TestResult<StoreSnapshot> {
        self.reopen(|machine| Ok(machine.store().snapshot()?)).await
    }

    /// Moves the broker clock forward; later restarts keep the new time.
    pub fn advance_to(&mut self, millis: u64) {
        assert!(millis >= self.now, "the test clock only moves forward");
        self.now = millis;
        self.running().clock.set(millis);
    }

    /// Returns every lock on `entity` to ready by letting it expire.
    ///
    /// A closed connection releases the locks its links held, but only once
    /// that release reaches the broker; a restart right after the close can
    /// win the race. Lock expiry settles the copy either way, without
    /// counting a delivery.
    pub fn expire_locks(&mut self, entity: &str) -> TestResult {
        self.advance_to(self.now + domain::DEFAULT_LOCK_DURATION_MILLIS + 1);
        self.submit(entity, CommandKind::ExpireLocks)?;
        Ok(())
    }

    fn running(&self) -> &Running {
        self.running.as_ref().expect("the node is running")
    }

    pub async fn connect(&self) -> TestResult<Connection> {
        phase("client connect: open");
        let connection = Connection::builder()
            .container_id("durable-content-test-client")
            .open(self.running().address.as_str())
            .await?;
        phase("client connect: opened");
        Ok(connection)
    }

    /// Normal completion joins originals before the temporary directory drops.
    pub async fn shutdown(&mut self) -> TestResult {
        self.stop().await
    }

    pub fn submit(&self, entity: &str, kind: CommandKind) -> TestResult<CommandOutcome> {
        Ok(self.running().broker.handle().submit_blocking(
            NamespaceName::new(NAMESPACE)?,
            EntityPath::new(entity)?,
            kind,
        )?)
    }
}

impl Drop for DurableNode {
    fn drop(&mut self) {
        phase("node Drop: fallback cleanup (not drain proof)");
        if let Some(running) = self.running.take() {
            running.retirement.request();
            if let Some(listener) = running.listener {
                listener.abort();
            }
            drop(running.broker);
        }
        phase("node Drop: fields/temporary directory follow");
    }
}

#[cfg(test)]
mod custody_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canceled_reopen_keeps_the_original_listener_join_before_store_open() -> TestResult {
        let mut node = DurableNode::create(Vec::new()).await?;
        let (release, gate) = tokio::sync::oneshot::channel();
        let running = node.running.as_mut().expect("running");
        let original = running.listener.take().expect("original listener");
        // This retained wrapper cannot publish completion before its original
        // listener has actually joined, even after the reopen waiter is canceled.
        running.listener = Some(tokio::spawn(async move {
            let result = original.await.map_err(|error| error.to_string())?;
            let _ = gate.await;
            result
        }));
        let inspected = Arc::new(AtomicBool::new(false));
        {
            let inspected = inspected.clone();
            let reopen = node.reopen(move |_| {
                inspected.store(true, Ordering::SeqCst);
                Ok(())
            });
            tokio::pin!(reopen);
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut reopen)
                    .await
                    .is_err()
            );
        }
        assert!(
            !inspected.load(Ordering::SeqCst),
            "unjoined owners cannot authorize reopen"
        );
        release
            .send(())
            .expect("original waiter retained after cancellation");
        node.reopen(|_| {
            inspected.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await?;
        assert!(inspected.load(Ordering::SeqCst));
        node.shutdown().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_protocol_header_is_retired_before_reopen() -> TestResult {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut node = DurableNode::create(Vec::new()).await?;
        let address = node.running().address.trim_start_matches("amqp://");
        let mut peer = tokio::net::TcpStream::connect(address).await?;
        peer.write_all(b"AMQP\0\x01\0\0").await?;
        let mut response = [0; 8];
        tokio::time::timeout(WAIT, peer.read_exact(&mut response)).await??;
        assert_eq!(&response, b"AMQP\0\x01\0\0");
        // The response proves the original was accepted. No Open follows, so
        // the peer cannot complete admission on its own.
        node.snapshot().await?;
        drop(peer);
        node.shutdown().await
    }
}

pub fn namespace() -> NamespaceName {
    NamespaceName::new(NAMESPACE).expect("a valid namespace")
}

/// An entity path, with `<queue>/$deadletterqueue` naming that queue's DLQ.
pub fn entity(path: &str) -> EntityPath {
    match path.strip_suffix("/$deadletterqueue") {
        Some(queue) => EntityPath::new(queue)
            .and_then(|queue| queue.dead_letter_queue())
            .expect("a valid dead-letter queue path"),
        None => EntityPath::new(path).expect("a valid entity path"),
    }
}

pub fn sequence(value: i64) -> SequenceNumber {
    SequenceNumber::new(u64::try_from(value).expect("a positive sequence"))
}

/// A request/reply pair on one entity's management node.
pub struct Management {
    requests: Sender,
    responses: Receiver,
    reply_to: String,
    next_id: u64,
}

impl Management {
    pub async fn attach(session: &mut Session, entity: &str) -> TestResult<Self> {
        phase("management attach: response receiver");
        let node = format!("{entity}/$management");
        let reply_to = format!("{entity}-replies");
        let responses = Receiver::builder()
            .name(format!("{entity}-management-response"))
            .source(node.as_str())
            .target(reply_to.as_str())
            .attach(session)
            .await?;
        let requests = Sender::attach(
            session,
            format!("{entity}-management-request"),
            node.as_str(),
        )
        .await?;
        phase("management attach: complete");
        Ok(Self {
            requests,
            responses,
            reply_to,
            next_id: 0,
        })
    }

    /// Sends one request and returns its status code and response.
    pub async fn request(
        &mut self,
        operation: &str,
        link_name: Option<&str>,
        body: OrderedMap<Value, Value>,
    ) -> TestResult<(i32, Message)> {
        self.next_id += 1;
        let mut properties = ApplicationProperties::builder()
            .insert(protocol_amqp::OPERATION_PROPERTY, operation)
            .build();
        if let Some(link_name) = link_name {
            properties.insert(protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY, link_name);
        }
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(format!("request-{}", self.next_id).into()),
                reply_to: Some(self.reply_to.clone()),
                ..Properties::default()
            })
            .application_properties(properties)
            .body(Body::Value(Value::Map(body)))
            .build();
        phase(format_args!("management {operation}: send"));
        assert!(matches!(
            self.requests.send(request).await?,
            Outcome::Accepted(_)
        ));
        phase(format_args!("management {operation}: response wait"));
        let response = tokio::time::timeout(WAIT, self.responses.recv()).await??;
        let message = response.message().clone();
        phase(format_args!("management {operation}: response accept"));
        self.responses.accept(&response).await?;
        phase(format_args!("management {operation}: complete"));
        let status = match message
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY))
        {
            Some(Value::Int(status)) => *status,
            other => return Err(format!("the response carried no status: {other:?}").into()),
        };
        Ok((status, message))
    }

    pub async fn peek(&mut self, from: i64, count: i32) -> TestResult<Vec<Entry>> {
        let (status, response) = self
            .request(
                protocol_amqp::PEEK_MESSAGE_OPERATION,
                None,
                map([
                    ("from-sequence-number", Value::Long(from)),
                    ("message-count", Value::Int(count)),
                ]),
            )
            .await?;
        assert_eq!(status, 200, "peek found nothing: {response:?}");
        response_entries(&response)
    }

    /// Peeks up to `count` copies from `from`; an empty entity answers 204.
    pub async fn peek_any(&mut self, from: i64, count: i32) -> TestResult<Vec<Entry>> {
        let (status, response) = self
            .request(
                protocol_amqp::PEEK_MESSAGE_OPERATION,
                None,
                map([
                    ("from-sequence-number", Value::Long(from)),
                    ("message-count", Value::Int(count)),
                ]),
            )
            .await?;
        match status {
            200 => response_entries(&response),
            204 => Ok(Vec::new()),
            _ => Err(format!("peek failed: {response:?}").into()),
        }
    }

    /// Waits until a peek shows the effect of a link settlement.
    ///
    /// A link disposition reaches the broker asynchronously, so a restart
    /// straight after one could stop the broker before it applies.
    pub async fn wait_until(&mut self, what: &str, done: impl Fn(&[Entry]) -> bool) -> TestResult {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            if done(&self.peek_any(1, 100).await?) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("timed out waiting until {what}").into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Schedules encoded messages and returns their placeholder sequences.
    pub async fn schedule(&mut self, messages: &[(&str, Vec<u8>)]) -> TestResult<(i32, Message)> {
        let entries = messages
            .iter()
            .map(|(message_id, encoded)| {
                Value::Map(map([
                    ("message", Value::Binary(encoded.clone().into())),
                    ("message-id", Value::String((*message_id).to_owned())),
                ]))
            })
            .collect();
        self.request(
            protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
            None,
            map([("messages", Value::List(entries))]),
        )
        .await
    }

    /// Receives deferred messages by sequence in peek-lock mode.
    pub async fn receive_deferred(&mut self, sequence: i64) -> TestResult<(i32, Vec<Entry>)> {
        let (status, response) = self
            .request(
                protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
                None,
                map([
                    (
                        "sequence-numbers",
                        Value::Array(Array::from(vec![Value::Long(sequence)])),
                    ),
                    ("receiver-settle-mode", Value::Uint(1)),
                ]),
            )
            .await?;
        if status != 200 {
            return Ok((status, Vec::new()));
        }
        Ok((status, response_entries(&response)?))
    }

    pub async fn update_disposition(
        &mut self,
        link_name: &str,
        lock_token: amqp::Uuid,
        status: &str,
        properties: &[(&str, Value)],
    ) -> TestResult<(i32, Message)> {
        let mut body = map([
            (
                protocol_amqp::LOCK_TOKENS,
                Value::Array(Array::from(vec![Value::Uuid(lock_token)])),
            ),
            ("disposition-status", Value::String(status.to_owned())),
        ]);
        if !properties.is_empty() {
            let mut changes = OrderedMap::new();
            for (name, value) in properties {
                changes.insert(Value::String((*name).to_owned()), value.clone());
            }
            body.insert(
                Value::String(String::from("properties-to-modify")),
                Value::Map(changes),
            );
        }
        self.request(
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            Some(link_name),
            body,
        )
        .await
    }
}

pub fn map<const N: usize>(entries: [(&str, Value); N]) -> OrderedMap<Value, Value> {
    let mut map = OrderedMap::new();
    for (name, value) in entries {
        map.insert(Value::String(name.to_owned()), value);
    }
    map
}

/// One message embedded in a peek or receive-by-sequence response.
pub struct Entry {
    pub message: Message,
    pub encoded: Vec<u8>,
    /// Present on an entry received by sequence in peek-lock mode.
    pub lock: Option<amqp::Uuid>,
}

fn response_entries(response: &Message) -> TestResult<Vec<Entry>> {
    let Body::Value(Value::Map(body)) = &response.body else {
        return Err("the response must carry an AMQP value map".into());
    };
    let Some(Value::List(entries)) = body.get(&Value::String(String::from("messages"))) else {
        return Err("the response did not contain messages".into());
    };
    entries
        .iter()
        .map(|entry| {
            let Value::Map(entry) = entry else {
                return Err("a response entry must be an AMQP map".into());
            };
            let Some(Value::Binary(encoded)) = entry.get(&Value::String(String::from("message")))
            else {
                return Err("a response entry did not contain an encoded message".into());
            };
            let lock = match entry.get(&Value::String(String::from("lock-token"))) {
                Some(Value::Uuid(token)) => Some(token.clone()),
                _ => None,
            };
            Ok(Entry {
                message: amqp::decode_message(encoded)?,
                encoded: encoded.to_vec(),
                lock,
            })
        })
        .collect()
}

/// The error condition a refused management response names.
pub fn error_condition(response: &Message) -> Option<Value> {
    response
        .application_properties
        .as_ref()
        .and_then(|properties| properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY))
        .cloned()
}

pub fn annotation<'a>(message: &'a Message, name: &str) -> Option<&'a Value> {
    message
        .message_annotations
        .as_ref()
        .and_then(|annotations| annotations.get(&AnnotationKey::from(name)))
}

pub fn sequence_of(message: &Message) -> i64 {
    match annotation(message, "x-opt-sequence-number") {
        Some(Value::Long(sequence)) => *sequence,
        other => panic!("expected a sequence annotation, got {other:?}"),
    }
}

pub fn lock_token_of(delivery: &ClientDelivery) -> amqp::Uuid {
    match delivery
        .message()
        .delivery_annotations
        .as_ref()
        .and_then(|annotations| annotations.get(&AnnotationKey::from("x-opt-lock-token")))
    {
        Some(Value::Uuid(token)) => token.clone(),
        other => panic!("expected a lock token, got {other:?}"),
    }
}

/// The broker-rendered state of a peeked copy: 1 deferred, 2 scheduled.
pub fn state_of(entry: &Entry) -> Option<&Value> {
    annotation(&entry.message, "x-opt-message-state")
}

/// Whether a peek holds the copy at `sequence` in `state`.
pub fn holds(entries: &[Entry], sequence: i64, state: Option<i32>) -> bool {
    entries.iter().any(|entry| {
        sequence_of(&entry.message) == sequence && state_of(entry) == state.map(Value::Int).as_ref()
    })
}

pub async fn next(receiver: &mut Receiver) -> TestResult<ClientDelivery> {
    Ok(tokio::time::timeout(WAIT, receiver.recv()).await??)
}

pub async fn assert_nothing_more(receiver: &mut Receiver) {
    assert!(
        tokio::time::timeout(Duration::from_millis(250), receiver.recv())
            .await
            .is_err(),
        "an unexpected message was delivered"
    );
}

/// The broker owns these names; a patch never stores them.
fn is_reserved_dead_letter_name(name: &str) -> bool {
    name == protocol_amqp::DEAD_LETTER_REASON_PROPERTY
        || name == protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY
}

/// Annotation keys the broker owns and renders from its own state.
pub const BROKER_ANNOTATIONS: [&str; 7] = [
    "x-opt-sequence-number",
    "x-opt-enqueue-sequence-number",
    "x-opt-enqueued-time",
    "x-opt-scheduled-enqueue-time",
    "x-opt-locked-until",
    "x-opt-deadletter-source",
    "x-opt-message-state",
];
const LOCK_TOKEN: &str = "x-opt-lock-token";
/// A lock token a sender forges; the broker must never render it.
pub const FORGED_LOCK: [u8; 16] = [0xee; 16];

/// What a copy should look like beyond its original content.
#[derive(Clone, Debug, Default)]
pub struct Expect<'a> {
    pub delivery_count: u32,
    pub ttl: Option<u32>,
    /// A receive with a lock carries the broker's lock token.
    pub locked: bool,
    /// The copy-local overlay, in the order it was first written.
    pub overlay: &'a [(&'a str, Value)],
    /// The broker-appended dead-letter reason and description.
    pub dead_letter: Option<(&'a str, &'a str)>,
    /// The broker-owned message state, absent for an active copy.
    pub state: Option<i32>,
}

/// Canonical bytes of one value: `OrderedFloat` equality treats every NaN as
/// equal and `-0.0` as `0.0`, so content fidelity is compared encoded.
pub fn encoded(value: &Value) -> Vec<u8> {
    serde_amqp::to_vec(value).expect("a test value encodes")
}

fn annotations_without(
    annotations: Option<&OrderedMap<AnnotationKey, Value>>,
    owned: &[&str],
) -> Value {
    let mut kept = OrderedMap::new();
    for (key, value) in annotations.into_iter().flat_map(|map| map.iter()) {
        let broker_owned =
            matches!(key, AnnotationKey::Symbol(name) if owned.contains(&name.as_str()));
        if !broker_owned {
            let key = match key {
                AnnotationKey::Symbol(name) => Value::Symbol(name.clone()),
                AnnotationKey::Ulong(code) => Value::Ulong(*code),
            };
            kept.insert(key, value.clone());
        }
    }
    Value::Map(kept)
}

fn application_map(properties: Option<&ApplicationProperties>) -> Value {
    let mut map = OrderedMap::new();
    for (name, value) in properties
        .into_iter()
        .flat_map(|properties| properties.0.iter())
    {
        map.insert(Value::String(name.clone()), value.clone());
    }
    Value::Map(map)
}

/// The application properties a copy renders: the original's, then the
/// overlay (an existing name keeps its place), then any dead-letter fields.
pub fn rendered_properties(original: &Message, expect: &Expect<'_>) -> Value {
    let mut properties = original.application_properties.clone().unwrap_or_default();
    for (name, value) in expect.overlay {
        if !is_reserved_dead_letter_name(name) {
            properties.insert(*name, value.clone());
        }
    }
    properties
        .0
        .shift_remove(protocol_amqp::DEAD_LETTER_REASON_PROPERTY);
    properties
        .0
        .shift_remove(protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY);
    if let Some((reason, description)) = expect.dead_letter {
        properties.insert(protocol_amqp::DEAD_LETTER_REASON_PROPERTY, reason);
        properties.insert(protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY, description);
    }
    application_map(Some(&properties))
}

/// Checks every section of a rendered copy against the message originally
/// sent, and that its encoded body and footer are the original's bytes.
pub fn assert_renders(
    received: &Message,
    received_bytes: &[u8],
    original: &Message,
    expect: Expect<'_>,
) {
    let label = format!(
        "{:?}",
        original
            .properties
            .as_ref()
            .and_then(|p| p.message_id.clone())
    );

    let mut header = original.header.clone().unwrap_or_default();
    header.ttl = expect.ttl;
    header.delivery_count = expect.delivery_count;
    assert_eq!(received.header.as_ref(), Some(&header), "{label}: header");

    let mut properties = received.properties.clone();
    if let Some(properties) = properties.as_mut() {
        assert_eq!(
            properties.absolute_expiry_time.is_some(),
            expect.ttl.is_some(),
            "{label}: absolute expiry follows the broker's TTL"
        );
        properties.absolute_expiry_time = original
            .properties
            .as_ref()
            .and_then(|original| original.absolute_expiry_time);
    }
    assert_eq!(properties, original.properties, "{label}: properties");

    assert_eq!(
        encoded(&application_map(received.application_properties.as_ref())),
        encoded(&rendered_properties(original, &expect)),
        "{label}: application properties\n got {:?}",
        received.application_properties
    );

    assert_eq!(
        encoded(&annotations_without(
            received.message_annotations.as_ref().map(|a| &a.0),
            &BROKER_ANNOTATIONS
        )),
        encoded(&annotations_without(
            original.message_annotations.as_ref().map(|a| &a.0),
            &BROKER_ANNOTATIONS
        )),
        "{label}: custom message annotations"
    );
    assert!(
        matches!(annotation(received, "x-opt-sequence-number"), Some(Value::Long(sequence)) if *sequence > 0)
            && annotation(received, "x-opt-enqueued-time").is_some(),
        "{label}: broker annotations are rendered"
    );
    assert_eq!(
        annotation(received, "x-opt-deadletter-source"),
        None,
        "{label}: a direct copy names no dead-letter source"
    );
    assert_eq!(
        annotation(received, "x-opt-message-state"),
        expect.state.map(Value::Int).as_ref(),
        "{label}: message state"
    );
    let enqueued = match annotation(received, "x-opt-enqueued-time") {
        Some(Value::Timestamp(enqueued)) => enqueued.milliseconds(),
        other => panic!("{label}: enqueued time is {other:?}"),
    };
    assert!(
        enqueued >= START_MILLIS as i64,
        "{label}: the broker renders its own enqueue time"
    );
    match annotation(received, "x-opt-locked-until") {
        // The broker's lock runs past the enqueue, unlike the forged value.
        Some(Value::Timestamp(locked_until)) if expect.locked => assert!(
            locked_until.milliseconds() > enqueued,
            "{label}: locked-until {} is not the broker's",
            locked_until.milliseconds()
        ),
        None if !expect.locked => {}
        other => panic!("{label}: locked-until is {other:?}"),
    }
    assert_eq!(
        encoded(&annotations_without(
            received.delivery_annotations.as_ref().map(|a| &a.0),
            &[LOCK_TOKEN]
        )),
        encoded(&annotations_without(
            original.delivery_annotations.as_ref().map(|a| &a.0),
            &[LOCK_TOKEN]
        )),
        "{label}: custom delivery annotations"
    );
    assert_eq!(
        received
            .delivery_annotations
            .as_ref()
            .and_then(|a| a.get(&AnnotationKey::from(LOCK_TOKEN)))
            .map(|token| token != &Value::Uuid(amqp::Uuid::from(FORGED_LOCK))),
        expect.locked.then_some(true),
        "{label}: only the broker's lock token is rendered"
    );

    assert_eq!(received.body, original.body, "{label}: body");
    assert_eq!(received.footer, original.footer, "{label}: footer");
    let tail = amqp::encode_message(&Message {
        body: original.body.clone(),
        footer: original.footer.clone(),
        ..Message::default()
    })
    .expect("the original tail encodes");
    assert!(
        received_bytes.ends_with(&tail),
        "{label}: the body and footer are not the original bytes"
    );
}

/// The stored copy's exact original bytes and its overlay, read off disk.
pub fn assert_stored(
    machine: &StateMachine<FjallStore>,
    entity_path: &str,
    sequence_number: i64,
    original: &[u8],
    overlay: &[(&str, Value)],
) -> TestResult<domain::MessageRecord> {
    let record = machine
        .message(
            &namespace(),
            &entity(entity_path),
            sequence(sequence_number),
        )?
        .ok_or_else(|| format!("{entity_path} holds no message {sequence_number}"))?;
    let envelope = record
        .envelope
        .as_ref()
        .ok_or("the stored copy has no AMQP content")?;
    assert_eq!(
        envelope.as_bytes(),
        original,
        "{entity_path}/{sequence_number}: the stored original changed"
    );
    let stored: Vec<_> = envelope
        .overlay()
        .iter()
        .map(|property| (property.name.clone(), property.value.clone()))
        .collect();
    let expected: Vec<_> = overlay
        .iter()
        .filter(|(name, _)| !is_reserved_dead_letter_name(name))
        .map(|(name, value)| ((*name).to_owned(), encoded(value)))
        .collect();
    assert_eq!(stored, expected, "{entity_path}/{sequence_number}: overlay");
    Ok(record)
}
