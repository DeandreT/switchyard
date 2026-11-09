//! Actual authenticated HTTPS over the existing real, serialized broker read.
#[allow(dead_code)]
mod admin_tls;
#[allow(dead_code)]
mod atom_http;
#[path = "../../admin-api/tests/support/subscription_xml.rs"]
mod validate;

use admin_api::{
    EntityService,
    atom::{
        https::{Configuration, HttpsListener, Limits},
        subscription_get::SubscriptionGet,
    },
    rpc::BackendErrorKind,
};
use atom_http::{Identity, TestResult, WAIT};
use auth::{JwtPolicy, ResourceScope};
use domain::{
    Command, CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use server::{Broker, Clock, LocalProposer};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};
use tokio::{io::AsyncWriteExt, net::TcpListener, sync::oneshot, task::JoinHandle};

fn ns(value: &str) -> NamespaceName {
    NamespaceName::new(value).unwrap()
}
fn path(value: &str) -> EntityPath {
    EntityPath::new(value).unwrap()
}
fn name(value: &str) -> SubscriptionName {
    SubscriptionName::new(value).unwrap()
}
const TARGET: &str = "/topic/Subscriptions/normal?api-version=2024-05&enrich=False";
const MAX_TTL: u64 = (i64::MAX as u64) / 10000;
fn configs() -> [(&'static str, SubscriptionConfig); 4] {
    [
        (
            "normal",
            SubscriptionConfig {
                lock_duration_millis: 60001,
                default_time_to_live_millis: Some(3600001),
                max_delivery_count: 17,
                dead_lettering_on_message_expiration: false,
                dead_lettering_on_filter_evaluation_exceptions: false,
            },
        ),
        (
            "min",
            SubscriptionConfig {
                lock_duration_millis: 5000,
                default_time_to_live_millis: Some(1000),
                max_delivery_count: 1,
                ..SubscriptionConfig::default()
            },
        ),
        (
            "max",
            SubscriptionConfig {
                lock_duration_millis: 300000,
                default_time_to_live_millis: Some(MAX_TTL),
                max_delivery_count: i32::MAX as u32,
                dead_lettering_on_message_expiration: false,
                ..SubscriptionConfig::default()
            },
        ),
        ("unlimited", SubscriptionConfig::default()),
    ]
}
fn seed<S: StateStore>(raw: S) {
    let machine = StateMachine::new(raw);
    let apply = |namespace: &str, entity: &str, kind| {
        machine
            .apply(&Command::new(
                ns(namespace),
                path(entity),
                Timestamp::from_millis(47),
                kind,
            ))
            .unwrap();
    };
    for (namespace, topic) in [
        ("tenant", "topic"),
        ("other", "foreign"),
        ("tenant", "rules"),
        ("tenant", "subscriptions"),
    ] {
        apply(
            namespace,
            topic,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        );
        let children: Vec<_> = if topic == "topic" {
            configs().to_vec()
        } else {
            vec![(
                if topic == "rules" {
                    "subscriptions"
                } else {
                    "rules"
                },
                SubscriptionConfig::default(),
            )]
        };
        for (child, config) in children {
            apply(
                namespace,
                topic,
                CommandKind::CreateSubscription {
                    name: name(child),
                    config,
                },
            );
        }
    }
    apply(
        "tenant",
        "topic",
        CommandKind::CreateSubscription {
            name: name("unsupported"),
            config: SubscriptionConfig {
                lock_duration_millis: 4999,
                ..SubscriptionConfig::default()
            },
        },
    );
}

struct Gate {
    key: Vec<u8>,
    entered: oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}
#[derive(Default)]
struct Control {
    gets: Mutex<Vec<Vec<u8>>>,
    applies: AtomicUsize,
    snapshots: AtomicUsize,
    gate: Mutex<Option<Gate>>,
    fail: Mutex<Option<Vec<u8>>>,
}
impl Control {
    fn arm(&self, key: Vec<u8>) -> (oneshot::Receiver<()>, Release) {
        let (entered, observed) = oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        *self.gate.lock().unwrap() = Some(Gate {
            key,
            entered,
            release: held,
        });
        (observed, Release(Some(release)))
    }
}
struct Release(Option<std::sync::mpsc::Sender<()>>);
impl Release {
    fn release(&mut self) {
        if let Some(send) = self.0.take() {
            let _ = send.send(());
        }
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        self.release();
    }
}
#[derive(Clone)]
struct Observed<S> {
    raw: S,
    control: Arc<Control>,
}
impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.control.gets.lock().unwrap().push(key.to_vec());
        let gate = {
            let mut gate = self.control.gate.lock().unwrap();
            if gate.as_ref().is_some_and(|g| g.key == key) {
                gate.take()
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            gate.release
                .recv_timeout(WAIT)
                .expect("test releases real owner read");
        }
        if self.control.fail.lock().unwrap().as_deref() == Some(key) {
            return Err(StorageError::LockPoisoned);
        }
        self.raw.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.control.applies.fetch_add(1, Ordering::SeqCst);
        self.raw.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.control.snapshots.fetch_add(1, Ordering::SeqCst);
        self.raw.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.raw.scan_from(prefix, start, limit)
    }
}
#[derive(Clone)]
struct NoClock;
impl Clock for NoClock {
    fn now(&self) -> Timestamp {
        panic!("subscription Get must not capture Clock")
    }
}

fn policy() -> serde_json::Value {
    let mut p = admin_tls::policy();
    for (subject, resource) in [
        ("topic", "topic"),
        ("sub", "topic/subscriptions/normal"),
        ("keyword", "rules/subscriptions/subscriptions"),
    ] {
        p["bindings"].as_array_mut().unwrap().push(serde_json::json!({"subject":subject,"scope":format!("{}/{resource}",admin_tls::ROOT),"permissions":["manage"]}));
    }
    p
}
fn configuration(identity: &Identity, limits: Limits) -> TestResult<Configuration> {
    Ok(Configuration::new(
        identity.chain.as_bytes(),
        identity.key.as_bytes(),
        JwtPolicy::from_json(&policy().to_string())?,
        ns("tenant"),
        ResourceScope::parse(admin_tls::ROOT)?,
    )?
    .with_limits(limits)?)
}
fn classify(error: &server::SubmitError) -> BackendErrorKind {
    if matches!(error, server::SubmitError::BrokerStopped) {
        BackendErrorKind::Unavailable
    } else {
        BackendErrorKind::Internal
    }
}
struct Node {
    broker: Option<Broker>,
    control: Arc<Control>,
    identity: Identity,
    address: SocketAddr,
    retirement: admin_api::atom::https::Retirement,
    serving: JoinHandle<HttpsListener>,
}
impl Node {
    async fn start<S: StateStore>(raw: S, limits: Limits) -> TestResult<Self> {
        let control = Arc::new(Control::default());
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(Observed {
                raw,
                control: control.clone(),
            }),
            NoClock,
        ));
        let handle = broker.handle();
        let identity = Identity::new()?;
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?;
        let mut service = HttpsListener::with_handler(
            socket,
            configuration(&identity, limits)?,
            Arc::new(SubscriptionGet::new(
                EntityService::new(ns("tenant"), handle.clone()),
                classify,
            )),
        );
        let retirement = service.retirement_handle();
        let serving = tokio::spawn(async move {
            service.serve().await;
            service
        });
        Ok(Self {
            broker: Some(broker),
            control,
            identity,
            address,
            retirement,
            serving,
        })
    }
    async fn request(&self, bytes: &[u8]) -> TestResult<Reply> {
        let mut stream = atom_http::connect(self.address, &self.identity).await?;
        stream.write_all(bytes).await?;
        Reply::parse(&atom_http::read(&mut stream).await?)
    }
    async fn get(&self, subject: &str, target: &str) -> TestResult<Reply> {
        self.request(
            wire(
                Some(&admin_tls::sign(admin_tls::claims(subject))),
                "GET",
                target,
                "",
                "",
            )
            .as_bytes(),
        )
        .await
    }
    async fn stop(self) -> TestResult {
        self.retirement.request();
        let mut listener = tokio::time::timeout(WAIT, self.serving).await??;
        tokio::time::timeout(WAIT, listener.finish()).await?;
        assert!(listener.take_failure().is_none());
        drop(self.broker); // Actual listener originals joined before owner stops.
        assert!(tokio::net::TcpStream::connect(self.address).await.is_err());
        Ok(())
    }
}
#[derive(serde::Serialize)]
struct Reply {
    status: u16,
    content_type: String,
    body: String,
}
impl Reply {
    fn parse(wire: &str) -> TestResult<Self> {
        let (head, body) = wire.split_once("\r\n\r\n").ok_or("HTTP head")?;
        assert_eq!(
            wire.matches("HTTP/1.1 ").count(),
            1,
            "one request per connection"
        );
        let status = head.split_whitespace().nth(1).ok_or("status")?.parse()?;
        let content_type = head
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(key, _)| key.eq_ignore_ascii_case("content-type"))
            .map(|(_, value)| value.trim().to_owned())
            .unwrap_or_default();
        Ok(Self {
            status,
            content_type,
            body: body.into(),
        })
    }
    fn xml_error(&self, status: u16) {
        assert_eq!(self.status, status);
        assert_eq!(self.content_type, "application/xml; charset=utf-8");
        assert_eq!(validate::error(self.body.as_bytes()).unwrap().0, status);
    }
}
fn wire(token: Option<&str>, method: &str, target: &str, extra: &str, body: &str) -> String {
    let auth = token
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    format!(
        "{method} {target} HTTP/1.1\r\nHost: untrusted-other.servicebus.windows.net\r\n{auth}{extra}\r\n{body}"
    )
}
fn capture(backend: &str, case: &str, reply: &Reply) -> TestResult {
    if let Some(root) = std::env::var_os("SWITCHYARD_ATOM31_CAPTURE_ROOT") {
        let root = PathBuf::from(root);
        assert!(root.is_absolute());
        let directory = root.join(backend);
        std::fs::create_dir_all(&directory)?;
        std::fs::write(
            directory.join(format!("{case}.json")),
            serde_json::to_vec(reply)?,
        )?;
    }
    Ok(())
}

async fn definitions<P: StoreProvider>(provider: P, backend: &str) -> TestResult {
    let raw = provider.open()?;
    seed(raw.clone());
    // A corrupt unrelated namespace and malformed Clock must not be consulted.
    raw.apply(
        WriteBatch::default()
            .put(
                keys::topic_config(&ns("other"), &path("unrelated")),
                vec![0xff],
            )
            .put(keys::clock(), vec![0xff]),
    )?;
    let snapshot = raw.snapshot()?;
    let node = Node::start(raw.clone(), Limits::default()).await?;
    for (child, _) in configs() {
        let target = format!("/ToPiC/Subscriptions/{child}?api-version=2024-05&enrich=False");
        let reply = node.get("operator", &target).await?;
        assert_eq!(reply.status, 200);
        assert_eq!(reply.content_type, "application/atom+xml; charset=utf-8");
        assert_eq!(validate::entry(reply.body.as_bytes()).unwrap()[0], child);
        let fixture: &[u8] = match child {
            "normal" => include_bytes!(
                "../../conformance/atom-fixtures/responses/subscription-get/normal.xml"
            ),
            "min" => {
                include_bytes!("../../conformance/atom-fixtures/responses/subscription-get/min.xml")
            }
            "max" => {
                include_bytes!("../../conformance/atom-fixtures/responses/subscription-get/max.xml")
            }
            "unlimited" => include_bytes!(
                "../../conformance/atom-fixtures/responses/subscription-get/unlimited.xml"
            ),
            _ => unreachable!(),
        };
        assert_eq!(
            reply.body.as_bytes(),
            fixture.strip_suffix(b"\n").unwrap_or(fixture)
        );
        capture(backend, child, &reply)?;
    }
    for subject in ["topic", "sub"] {
        assert_eq!(node.get(subject, TARGET).await?.status, 200);
    }
    for target in [
        "/rules/Subscriptions/subscriptions?api-version=2021-05&enrich=False",
        "/subscriptions/Subscriptions/rules?api-version=2024-05&enrich=False",
    ] {
        assert_eq!(node.get("operator", target).await?.status, 200);
    }
    assert_eq!(
        node.get(
            "keyword",
            "/rules/Subscriptions/subscriptions?api-version=2021-05&enrich=False"
        )
        .await?
        .status,
        200
    );
    let missing = node
        .get(
            "operator",
            "/foreign/Subscriptions/rules?api-version=2024-05&enrich=False",
        )
        .await?;
    missing.xml_error(404);
    capture(backend, "missing", &missing)?;
    assert_eq!(
        node.get(
            "operator",
            "/absent/Subscriptions/missing?api-version=2024-05&enrich=False"
        )
        .await?
        .status,
        404
    );
    let unsupported = node
        .get(
            "operator",
            "/topic/Subscriptions/unsupported?api-version=2024-05&enrich=False",
        )
        .await?;
    node.get(
        "operator",
        "/topic/Subscriptions/missing?api-version=2024-05&enrich=False",
    )
    .await?
    .xml_error(404);
    unsupported.xml_error(501);
    capture(backend, "unsupported", &unsupported)?;
    let before = node.control.gets.lock().unwrap().len();
    for (subject, target) in [
        ("sender", TARGET),
        ("foreign", TARGET),
        ("entity", TARGET),
        (
            "sub",
            "/topic/Subscriptions/sibling?api-version=2024-05&enrich=False",
        ),
        ("sub", "/$Resources/queues?api-version=2024-05&enrich=False"),
    ] {
        assert_eq!(node.get(subject, target).await?.status, 403);
    }
    assert_eq!(
        node.request(wire(None, "GET", TARGET, "Content-Length: 100\r\n", "").as_bytes())
            .await?
            .status,
        401
    );
    let token = admin_tls::sign(admin_tls::claims("operator"));
    let mut expired = admin_tls::claims("operator");
    expired["iat"] = 1.into();
    expired["exp"] = 2.into();
    assert_eq!(
        node.request(wire(Some(&admin_tls::sign(expired)), "GET", TARGET, "", "").as_bytes())
            .await?
            .status,
        401
    );
    assert_eq!(
        node.request(
            wire(
                Some(&token),
                "GET",
                TARGET,
                &format!("Authorization: Bearer {token}\r\n"),
                ""
            )
            .as_bytes()
        )
        .await?
        .status,
        401
    );
    for (extra, body, expected) in [
        ("Content-Length: 1\r\n", "", 415),
        ("Content-Length: 999999\r\n", "", 413),
        ("Transfer-Encoding: chunked\r\n", "0\r\n\r\n", 415),
        (
            "Content-Length: 1\r\nTransfer-Encoding: chunked\r\n",
            "0\r\n\r\n",
            415,
        ),
        (
            "Transfer-Encoding: chunked\r\nContent-Length: 1\r\n",
            "0\r\n\r\n",
            415,
        ),
        ("Content-Encoding: gzip\r\n", "", 415),
        (
            "Content-Length: 100\r\n",
            "<!DOCTYPE external SYSTEM 'file:///not-read'>",
            415,
        ),
        (
            "Transfer-Encoding: chunked\r\nTrailer: X-Test\r\n",
            "0\r\nX-Test: no\r\n\r\n",
            415,
        ),
        ("Expect: 100-continue\r\n", "", 400),
        ("Upgrade: h2c\r\n", "", 400),
        ("Content-Length: 1\r\nContent-Length: 2\r\n", "x", 400),
    ] {
        assert_eq!(
            node.request(wire(Some(&token), "GET", TARGET, extra, body).as_bytes())
                .await?
                .status,
            expected
        );
    }
    let mut invalid = wire(Some(&token), "GET", TARGET, "Content-Length: 1\r\n", "").into_bytes();
    invalid.push(0xff);
    assert_eq!(node.request(&invalid).await?.status, 415);
    for (target, expected) in [
        ("/topic?api-version=2024-05&enrich=False", 501),
        ("/rules?api-version=2024-05&enrich=False", 501),
        ("/$Resources/queues?api-version=2024-05&enrich=False", 400),
        ("/topic/Subscriptions?api-version=2024-05&enrich=False", 400),
        ("/topic/Subscriptions/normal?api-version=2024-05", 400),
        (
            "/topic/Subscriptions/normal?api-version=2024-05&enrich=True",
            400,
        ),
        (
            "/topic/Subscriptions/normal?api-version=2024-05&enrich=False&extension=x",
            400,
        ),
        (
            "/topic/Subscriptions/normal?api-version=2099-01&enrich=False",
            400,
        ),
        (
            "/topic/Subscriptions/normal?api-version=2024-05&enrich=False&enrich=True",
            400,
        ),
        (
            "/topic/Subscriptions/normal/Rules?api-version=2024-05&enrich=False",
            501,
        ),
        ("/$namespaceinfo?api-version=2024-05&enrich=False", 501),
    ] {
        assert_eq!(node.get("operator", target).await?.status, expected);
    }
    for method in ["PUT", "POST", "PATCH", "DELETE", "HEAD"] {
        assert_eq!(
            node.request(
                wire(Some(&token), method, TARGET, "Content-Length: 100\r\n", "").as_bytes()
            )
            .await?
            .status,
            400
        );
    }
    assert_eq!(
        node.control.gets.lock().unwrap().len(),
        before,
        "all denied/closed/body cases avoid the owner"
    );
    let invalid = node
        .get("operator", "/topic/Subscriptions/normal?api-version=bad")
        .await?;
    capture(backend, "invalid", &invalid)?;
    let pipelined = format!(
        "{}{}",
        wire(Some(&token), "GET", TARGET, "Content-Length: 0\r\n", ""),
        wire(
            Some(&token),
            "GET",
            "/topic/Subscriptions/min?api-version=2024-05&enrich=False",
            "",
            ""
        )
    );
    assert_eq!(node.request(pipelined.as_bytes()).await?.status, 200);
    let read_keys = node.control.gets.lock().unwrap().clone();
    assert!(!read_keys.contains(&keys::clock()));
    assert_eq!(
        read_keys
            .iter()
            .filter(|key| **key
                == keys::topic_subscription(&ns("tenant"), &path("topic"), &name("min")))
            .count(),
        1
    );
    assert_eq!(node.control.applies.load(Ordering::SeqCst), 0);
    assert_eq!(node.control.snapshots.load(Ordering::SeqCst), 0);
    assert_eq!(raw.snapshot()?, snapshot);
    let control = node.control.clone();
    node.stop().await?;
    // Joining all callback originals while the broker was still alive rules out
    // a delayed second pipelined dispatch, not merely a second wire response.
    assert_eq!(
        control
            .gets
            .lock()
            .unwrap()
            .iter()
            .filter(|key| **key
                == keys::topic_subscription(&ns("tenant"), &path("topic"), &name("min")))
            .count(),
        1
    );
    // Separately stop an idle real owner: the listener must return fixed 503.
    let mut stopped_node = Node::start(raw.clone(), Limits::default()).await?;
    drop(stopped_node.broker.take());
    let stopped = stopped_node.get("operator", TARGET).await?;
    stopped.xml_error(503);
    capture(backend, "stopped", &stopped)?;
    stopped_node.stop().await?;
    drop(raw);
    let reopened = provider.open()?;
    assert_eq!(reopened.snapshot()?, snapshot);
    Ok(())
}

async fn corruptions<P: StoreProvider>(provider: P, backend: &str) -> TestResult {
    let raw = provider.open()?;
    seed(raw.clone());
    let child = path("topic").subscription(&name("normal"))?;
    let shadow = child.dead_letter_queue()?;
    for (index, (key, value)) in [
        (
            keys::topic_subscription(&ns("tenant"), &path("topic"), &name("normal")),
            None,
        ),
        (
            keys::topic_subscription(&ns("tenant"), &path("topic"), &name("normal")),
            Some(vec![0xff]),
        ),
        (keys::topic_config(&ns("tenant"), &path("topic")), None),
        (
            keys::topic_config(&ns("tenant"), &path("topic")),
            Some(vec![0xff]),
        ),
        (keys::queue_config(&ns("tenant"), &child), None),
        (keys::queue_config(&ns("tenant"), &child), Some(vec![0xff])),
        (
            keys::queue_config(&ns("tenant"), &child),
            Some(codec::queue::encode(&QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            })?),
        ),
        (keys::queue_config(&ns("tenant"), &shadow), None),
        (keys::queue_config(&ns("tenant"), &shadow), Some(vec![0xff])),
        (keys::entity_metadata(&ns("tenant"), &child), None),
        (
            keys::entity_metadata(&ns("tenant"), &child),
            Some(vec![0xff]),
        ),
        (
            keys::entity_metadata(&ns("tenant"), &child),
            Some(codec::encode(&(1_u64, 2_u8, true))?),
        ),
        (
            keys::entity_metadata(&ns("tenant"), &child),
            raw.get(&keys::entity_metadata(&ns("tenant"), &path("topic")))?,
        ),
        (
            keys::entity_metadata(&ns("tenant"), &path("topic")),
            Some(vec![0xff]),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let original = raw.get(&key)?.ok_or("seed key")?;
        raw.apply(match value {
            Some(value) => WriteBatch::default().put(key.clone(), value),
            None => WriteBatch::default().delete(key.clone()),
        })?;
        let snapshot = raw.snapshot()?;
        let node = Node::start(raw.clone(), Limits::default()).await?;
        let reply = node.get("operator", TARGET).await?;
        reply.xml_error(500);
        assert!(!reply.body.contains("topic"));
        if index == 0 {
            capture(backend, "corrupt", &reply)?;
        }
        assert_eq!(node.control.applies.load(Ordering::SeqCst), 0);
        assert_eq!(raw.snapshot()?, snapshot);
        node.stop().await?;
        raw.apply(WriteBatch::default().put(key, original))?;
    }
    let node = Node::start(raw.clone(), Limits::default()).await?;
    *node.control.fail.lock().unwrap() = Some(keys::topic_subscription(
        &ns("tenant"),
        &path("topic"),
        &name("normal"),
    ));
    node.get("operator", TARGET).await?.xml_error(500);
    node.stop().await?;
    let node = Node::start(
        raw.clone(),
        Limits {
            reply_bytes: 128,
            ..Limits::default()
        },
    )
    .await?;
    let reply = node.get("operator", TARGET).await?;
    assert_eq!(reply.status, 500);
    assert_eq!(reply.content_type, "text/plain; charset=utf-8");
    assert!(!reply.body.contains("<entry"));
    node.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_definitions_auth_input_and_captures() -> TestResult {
    definitions(MemoryProvider::new(), "memory").await
}
#[tokio::test(flavor = "multi_thread")]
async fn fjall_definitions_auth_input_and_captures() -> TestResult {
    definitions(DurableProvider::temporary()?, "fjall").await
}
#[tokio::test(flavor = "multi_thread")]
async fn memory_selected_corruption_and_bounded_replies() -> TestResult {
    corruptions(MemoryProvider::new(), "memory").await
}
#[tokio::test(flavor = "multi_thread")]
async fn fjall_selected_corruption_and_bounded_replies() -> TestResult {
    corruptions(DurableProvider::temporary()?, "fjall").await
}

#[derive(Clone)]
struct CountingClock(Arc<AtomicUsize>);
impl Clock for CountingClock {
    fn now(&self) -> Timestamp {
        self.0.fetch_add(1, Ordering::SeqCst);
        Timestamp::from_millis(1000)
    }
}

async fn serialized<P: StoreProvider>(provider: P) -> TestResult {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };
    let raw = provider.open()?;
    seed(raw.clone());
    let control = Arc::new(Control::default());
    let clock = CountingClock(Arc::new(AtomicUsize::new(0)));
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(Observed {
            raw: raw.clone(),
            control: control.clone(),
        }),
        clock.clone(),
    ));
    let handle = broker.handle();
    let service = server::AdminService::new(ns("tenant"), handle.clone());
    let (entered, mut release) = control.arm(keys::topic_subscription(
        &ns("tenant"),
        &path("topic"),
        &name("normal"),
    ));
    let get = tokio::spawn(async move {
        service
            .get(admin_api::GetEntityRequest {
                namespace: "tenant".into(),
                address: admin_api::EntityAddress::Subscription {
                    topic: "topic".into(),
                    name: "normal".into(),
                },
            })
            .await
    });
    tokio::time::timeout(WAIT, entered).await??;
    let write = handle.submit(
        ns("tenant"),
        path("unrelated-write"),
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    tokio::pin!(write);
    assert!(matches!(
        write.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    assert_eq!(clock.0.load(Ordering::SeqCst), 0);
    assert_eq!(control.applies.load(Ordering::SeqCst), 0);
    release.release();
    tokio::time::timeout(WAIT, get).await???;
    tokio::time::timeout(WAIT, write).await??;
    assert_eq!(clock.0.load(Ordering::SeqCst), 1);
    assert_eq!(control.applies.load(Ordering::SeqCst), 1);
    drop(broker);
    Ok(())
}

async fn held_read_shutdown<P: StoreProvider>(provider: P, deadline: bool) -> TestResult {
    use server::{
        AdminListenerService, AdminTlsConfig, Shutdown, ShutdownBudget, ShutdownExpired,
        ShutdownStage, ShutdownTrigger, TimerThread, serve_all_transports,
        shut_down_all_transports,
    };
    use std::sync::atomic::AtomicBool;
    let raw = provider.open()?;
    seed(raw.clone());
    let snapshot = raw.snapshot()?;
    let control = Arc::new(Control::default());
    let mut broker = Some(Broker::spawn(LocalProposer::new(
        StateMachine::new(Observed {
            raw: raw.clone(),
            control: control.clone(),
        }),
        NoClock,
    )));
    let handle = broker.as_ref().unwrap().handle();
    let identity = Identity::new()?;
    let amqp_socket = TcpListener::bind("127.0.0.1:0").await?;
    let native_socket = TcpListener::bind("127.0.0.1:0").await?;
    let atom_socket = TcpListener::bind("127.0.0.1:0").await?;
    let addresses = [
        amqp_socket.local_addr()?,
        native_socket.local_addr()?,
        atom_socket.local_addr()?,
    ];
    let mut amqp = protocol_amqp::AmqpListener::new(handle.clone(), ns("tenant"))
        .into_tcp_service(amqp_socket);
    let mut native = AdminListenerService::new(
        native_socket,
        AdminTlsConfig::new(
            identity.native()?,
            JwtPolicy::from_json(&admin_tls::policy().to_string())?,
            ResourceScope::parse(admin_tls::ROOT)?,
        )?,
        handle.clone(),
        ns("tenant"),
        WAIT,
    );
    let mut atom = HttpsListener::with_handler(
        atom_socket,
        configuration(
            &identity,
            Limits {
                requests: 1,
                body_timeout: Duration::from_millis(100),
                request_timeout: Duration::from_secs(1),
                connection_timeout: Duration::from_secs(10),
                ..Limits::default()
            },
        )?,
        Arc::new(SubscriptionGet::new(
            EntityService::new(ns("tenant"), handle.clone()),
            classify,
        )),
    );
    let retire = (
        amqp.retirement_handle(),
        native.retirement_handle(),
        atom.retirement_handle(),
    );
    let trigger = ShutdownTrigger::default();
    let requested = trigger.clone();
    let serving = tokio::spawn(async move {
        serve_all_transports(&mut amqp, Some(&mut native), Some(&mut atom), &trigger).await;
        (amqp, native, atom, trigger)
    });
    let token = admin_tls::sign(admin_tls::claims("operator"));
    let mut rpc = admin_tls::client(addresses[1], &identity.ca_pem, "localhost").await?;
    let (entered, mut release) = control.arm(keys::topic_subscription(
        &ns("tenant"),
        &path("topic"),
        &name("normal"),
    ));
    let mut caller = atom_http::connect(addresses[2], &identity).await?;
    caller
        .write_all(atom_http::wire(&token, TARGET).as_bytes())
        .await?;
    tokio::time::timeout(WAIT, entered).await??;
    if deadline {
        let reply = Reply::parse(&atom_http::read(&mut caller).await?)?;
        assert_eq!(reply.status, 504);
        assert!(reply.body.contains("outcome unknown"));
    }
    drop(caller);
    assert!(
        atom_http::exchange(addresses[2], &identity, &token, TARGET)
            .await?
            .starts_with("HTTP/1.1 503")
    );
    assert_eq!(
        control.gets.lock().unwrap().len(),
        1,
        "begun real read retains the only slot"
    );
    requested.request();
    retire.0.request();
    retire.1.request();
    retire.2.request();
    let (mut amqp, mut native, mut atom, trigger) = tokio::time::timeout(WAIT, serving).await??;
    let started = trigger.started().unwrap();
    assert!(!trigger.request());
    assert_eq!(trigger.started(), Some(started));
    assert_eq!(
        rpc.list_entities(admin_tls::request(admin_tls::list(), &token))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    let latch = Arc::new(Shutdown::default());
    let timer_started = Arc::new(AtomicBool::new(false));
    let (timer_send, timer_read) = std::sync::mpsc::channel();
    let timer_thread = std::thread::spawn({
        let latch = latch.clone();
        let timer_started = timer_started.clone();
        let handle = handle.clone();
        move || {
            let until = std::time::Instant::now() + WAIT;
            while !latch.is_signalled() {
                assert!(std::time::Instant::now() < until);
                std::thread::sleep(Duration::from_millis(5));
            }
            timer_started.store(true, Ordering::SeqCst);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let read = rt.block_on(EntityService::new(ns("tenant"), handle).get(
                admin_api::GetEntityRequest {
                    namespace: "tenant".into(),
                    address: admin_api::EntityAddress::Subscription {
                        topic: "topic".into(),
                        name: "normal".into(),
                    },
                },
            ));
            let _ = timer_send.send(read.is_ok());
        }
    });
    let mut timer = Some(TimerThread::new(latch, timer_thread));
    let short = ShutdownBudget::new(tokio::time::Instant::now(), Duration::from_millis(100));
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
    assert!(timer.is_some() && broker.is_some());
    assert!(!timer_started.load(Ordering::SeqCst));
    for address in addresses {
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
    }
    release.release();
    // Retry only the canceled test borrower. Production uses its original budget.
    let budget = ShutdownBudget::new(tokio::time::Instant::now(), WAIT);
    let exit = shut_down_all_transports(
        &budget,
        &mut amqp,
        Some(&mut native),
        Some(&mut atom),
        &mut timer,
        &mut broker,
    )
    .await?;
    assert!(
        matches!(
            exit,
            Some(protocol_amqp::AmqpListenerExit::Complete(Ok(())))
        ),
        "only joined successful AMQP cleanup permits normal completion and reopen"
    );
    assert!(timer_read.recv_timeout(WAIT)?);
    assert!(broker.is_none() && timer.is_none());
    assert!(atom.take_failure().is_none() && native.take_failure().is_none());
    assert_eq!(control.applies.load(Ordering::SeqCst), 0);
    assert_eq!(control.snapshots.load(Ordering::SeqCst), 0);
    assert_eq!(raw.snapshot()?, snapshot);
    drop((amqp, native, atom, handle, rpc, raw));
    assert_eq!(provider.open()?.snapshot()?, snapshot);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_complete_get_serializes_writers_without_clock() -> TestResult {
    serialized(MemoryProvider::new()).await
}
#[tokio::test(flavor = "multi_thread")]
async fn fjall_complete_get_serializes_writers_without_clock() -> TestResult {
    serialized(DurableProvider::temporary()?).await
}
#[tokio::test(flavor = "multi_thread")]
async fn memory_disconnect_retains_real_get_through_shared_shutdown() -> TestResult {
    held_read_shutdown(MemoryProvider::new(), false).await
}
#[tokio::test(flavor = "multi_thread")]
async fn fjall_deadline_retains_real_get_through_shared_shutdown() -> TestResult {
    held_read_shutdown(DurableProvider::temporary()?, true).await
}

#[tokio::test(flavor = "multi_thread")]
async fn untrusted_tls_wrong_hostname_and_plaintext_never_reach_real_get() -> TestResult {
    use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
    let raw = storage::MemoryStore::default();
    seed(raw.clone());
    let node = Node::start(raw, Limits::default()).await?;
    let wrong = Identity::new()?;
    for (ca, host) in [
        (node.identity.ca.clone(), "wrong.example"),
        (wrong.ca, "localhost"),
    ] {
        let mut roots = RootCertStore::empty();
        roots.add(ca)?;
        let config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()?
                .with_root_certificates(roots)
                .with_no_client_auth();
        let tcp = tokio::net::TcpStream::connect(node.address).await?;
        assert!(
            tokio::time::timeout(
                WAIT,
                tokio_rustls::TlsConnector::from(Arc::new(config))
                    .connect(ServerName::try_from(host)?, tcp)
            )
            .await?
            .is_err()
        );
    }
    let mut tcp = tokio::net::TcpStream::connect(node.address).await?;
    tcp.write_all(wire(None, "GET", TARGET, "", "").as_bytes())
        .await?;
    drop(tcp);
    let control = node.control.clone();
    node.stop().await?;
    assert!(control.gets.lock().unwrap().is_empty());
    assert_eq!(control.applies.load(Ordering::SeqCst), 0);
    Ok(())
}
