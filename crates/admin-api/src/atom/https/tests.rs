use super::*;
use http_body::{Frame, SizeHint};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use std::{
    error::Error,
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinHandle,
};
use tokio_rustls::{TlsConnector, client::TlsStream};

// Reuse the exact existing public RSA fixture, not a new signing setup/key.
#[allow(dead_code)]
#[path = "../../../../server/tests/admin_tls/mod.rs"]
mod fixture;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const WAIT: Duration = Duration::from_secs(120);
const TARGET: &str = "/orders?api-version=2024-05&enrich=False";
const KEYWORD_PATHS: &[&str] = &[
    "rules",
    "subscriptions",
    "nested/rules",
    "subscriptions/child",
    "rules/Subscriptions/rules",
    "subscriptions/Subscriptions/subscriptions",
];

fn keyword_policy() -> serde_json::Value {
    let mut policy = fixture::policy();
    let bindings = policy["bindings"].as_array_mut().unwrap();
    for (index, path) in KEYWORD_PATHS.iter().enumerate() {
        bindings.push(serde_json::json!({
            "subject": format!("keyword-{index}"),
            "scope": format!("{}/{path}", fixture::ROOT),
            "permissions": ["manage"],
        }));
    }
    policy
}

struct Identity {
    chain: String,
    key: String,
    ca: CertificateDer<'static>,
}
fn identity() -> TestResult<Identity> {
    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::new(vec!["private-test-ca".into()])?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key)?;
    let key = KeyPair::generate()?;
    let cert = CertificateParams::new(vec!["localhost".into()])?.signed_by(&key, &ca, &ca_key)?;
    Ok(Identity {
        chain: format!("{}{}", cert.pem(), ca.pem()),
        key: key.serialize_pem(),
        ca: ca.der().clone(),
    })
}
fn config(identity: &Identity, limits: Limits) -> Configuration {
    Configuration::new(
        identity.chain.as_bytes(),
        identity.key.as_bytes(),
        JwtPolicy::from_json(&fixture::policy().to_string()).unwrap(),
        NamespaceName::new("tenant").unwrap(),
        ResourceScope::parse(fixture::ROOT).unwrap(),
    )
    .unwrap()
    .with_limits(limits)
    .unwrap()
}

#[derive(Default)]
struct Recording {
    selections: AtomicUsize,
    backend: AtomicUsize,
    entered: Notify,
    completed: Notify,
    hold: Mutex<Option<oneshot::Receiver<()>>>,
    reply_size: AtomicUsize,
    bodies: Mutex<Vec<Vec<u8>>>,
}
impl Handler for Recording {
    fn select(&self, head: &AuthorizedHead) -> Result<BodyMode, Refusal> {
        self.selections.fetch_add(1, Ordering::SeqCst);
        assert_eq!(head.namespace().as_str(), "tenant");
        if head.profile() != Profile::DefinitionSingleton {
            return Err(Refusal::RuntimeUnavailable);
        }
        Ok(if head.method() == Method::PUT {
            BodyMode::Bytes
        } else {
            BodyMode::Empty
        })
    }
    fn handle(&self, request: AuthorizedRequest) -> HandlerFuture<'_> {
        Box::pin(async move {
            self.bodies.lock().unwrap().push(request.body().to_vec());
            self.backend.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            let hold = self.hold.lock().unwrap().take();
            if let Some(hold) = hold {
                let _ = hold.await;
            }
            assert_eq!(
                request.head().scope().host(),
                "tenant.servicebus.windows.net"
            );
            self.completed.notify_one();
            Ok(Reply::new(
                StatusCode::OK,
                vec![b'x'; self.reply_size.load(Ordering::SeqCst).max(4)],
            ))
        })
    }
}

struct CountBody {
    polls: Arc<AtomicUsize>,
    bytes: Option<Bytes>,
}
impl Body for CountBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(self.bytes.take().map(|bytes| Ok(Frame::data(bytes))))
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}
fn boundary(identity: &Identity, handler: Arc<dyn Handler>, limits: Limits) -> Boundary {
    let config = Arc::new(config(identity, limits));
    Boundary {
        config,
        handler,
        admission: Arc::new(Mutex::new(Admission {
            retired: false,
            permits: Arc::new(Semaphore::new(limits.requests)),
            requests: TaskFamily::default(),
        })),
    }
}
fn counted(
    token: Option<&str>,
    target: &str,
    method: Method,
    bytes: &'static [u8],
) -> (Request<CountBody>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let mut builder = Request::builder()
        .method(method)
        .uri(target)
        .version(Version::HTTP_11)
        .header(header::HOST, "untrusted-peer.example");
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    (
        builder
            .body(CountBody {
                polls: polls.clone(),
                bytes: Some(Bytes::from_static(bytes)),
            })
            .unwrap(),
        polls,
    )
}

#[tokio::test]
async fn authorization_precedes_every_callback_and_body_poll() -> TestResult {
    let identity = identity()?;
    let handler = Arc::new(Recording::default());
    let boundary = boundary(&identity, handler.clone(), Limits::default());
    let mut expired = fixture::claims("operator");
    expired["iat"] = 1.into();
    expired["exp"] = 2.into();
    for (token, expected) in [
        (None, Refusal::Unauthenticated),
        (Some("malformed".into()), Refusal::Unauthenticated),
        (Some(fixture::sign(expired)), Refusal::Unauthenticated),
        (
            Some(fixture::sign(fixture::claims("sender"))),
            Refusal::Forbidden,
        ),
        (
            Some(fixture::sign(fixture::claims("foreign"))),
            Refusal::Forbidden,
        ),
    ] {
        let (mut request, polls) = counted(token.as_deref(), TARGET, Method::PUT, b"<not-read/>");
        request.headers_mut().insert(
            header::CONTENT_LENGTH,
            header::HeaderValue::from_static("999999999"),
        );
        let error = boundary
            .dispatch(request, Instant::now() + WAIT)
            .await
            .err()
            .unwrap();
        assert_eq!(error, expected);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(handler.selections.load(Ordering::SeqCst), 0);
        assert_eq!(handler.backend.load(Ordering::SeqCst), 0);
    }
    let token = fixture::sign(fixture::claims("entity"));
    let (request, polls) = counted(
        Some(&token),
        "/$Resources/queues?api-version=2024-05",
        Method::GET,
        b"never-read",
    );
    assert_eq!(
        boundary
            .dispatch(request, Instant::now() + WAIT)
            .await
            .err(),
        Some(Refusal::Forbidden)
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    // Entity Manage may authorize its canonical address without namespace Manage.
    let (request, polls) = counted(
        Some(&token),
        TARGET,
        Method::GET,
        b"not-a-body-without-framing",
    );
    assert!(
        boundary
            .dispatch(request, Instant::now() + WAIT)
            .await
            .is_ok()
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(handler.backend.load(Ordering::SeqCst), 1);
    let (mut request, polls) = counted(Some(&token), TARGET, Method::GET, b"not-read");
    request.headers_mut().append(
        header::AUTHORIZATION,
        header::HeaderValue::from_str(&format!("Bearer {token}"))?,
    );
    assert_eq!(
        boundary
            .dispatch(request, Instant::now() + WAIT)
            .await
            .err(),
        Some(Refusal::Unauthenticated)
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(handler.backend.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn canonical_resources_not_host_or_query_choose_authority() -> TestResult {
    let identity = identity()?;
    let handler = Arc::new(Recording::default());
    let boundary = boundary(&identity, handler.clone(), Limits::default());
    let token = fixture::sign(fixture::claims("entity"));
    let (request, _) = counted(
        Some(&token),
        "/%6FrDeRs?api-version=2024-05&enrich=False",
        Method::GET,
        b"",
    );
    assert!(
        boundary
            .dispatch(request, Instant::now() + WAIT)
            .await
            .is_ok()
    );
    for target in [
        "/orders2?api-version=2024-05&enrich=False",
        "/$Resources/topics?api-version=2024-05&enrich=False",
    ] {
        let (request, polls) = counted(Some(&token), target, Method::GET, b"not-read");
        assert_eq!(
            boundary
                .dispatch(request, Instant::now() + WAIT)
                .await
                .err(),
            Some(Refusal::Forbidden)
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    let (request, _) = counted(
        None,
        "/orders?api-version=invalid&unknown=true",
        Method::GET,
        b"",
    );
    assert_eq!(
        boundary
            .dispatch(request, Instant::now() + WAIT)
            .await
            .err(),
        Some(Refusal::Unauthenticated)
    );
    for target in [
        "/orders/%2fhidden",
        "/orders/../hidden",
        "/orders/%2E%2E/hidden",
        "/orders//hidden",
        "/orders/%FF",
        "/orders/%GG",
        "https://foreign.example/orders",
    ] {
        let uri = target.parse()?;
        assert!(route::address(&uri).is_err());
    }
    let address = route::address(&"/TOPIC/Subscriptions/EAST".parse()?).unwrap();
    let scope = address.scope(&boundary.config.root).unwrap();
    assert_eq!(scope.host(), "tenant.servicebus.windows.net");
    assert_eq!(
        scope.path().collect::<Vec<_>>(),
        ["topic", "subscriptions", "east"]
    );
    assert_eq!(handler.backend.load(Ordering::SeqCst), 1);
    assert_eq!(handler.selections.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn closed_profiles_and_unsupported_bodies_never_dispatch() -> TestResult {
    let identity = identity()?;
    let boundary = boundary(&identity, Arc::new(ClosedRoutes), Limits::default());
    let token = fixture::sign(fixture::claims("operator"));
    for (target, method, expected) in [
        (TARGET, Method::GET, Refusal::Unsupported),
        (
            "/$Resources/queues?api-version=2024-05&enrich=False&$skip=100&$top=100",
            Method::GET,
            Refusal::RuntimeUnavailable,
        ),
        (
            "/topic/Subscriptions?api-version=2021-05",
            Method::GET,
            Refusal::RuntimeUnavailable,
        ),
        (
            "/orders?api-version=2024-05&enrich=True",
            Method::GET,
            Refusal::RuntimeUnavailable,
        ),
        (TARGET, Method::PUT, Refusal::Invalid),
        ("/orders?api-version=2099-01", Method::GET, Refusal::Invalid),
        (
            "/orders?api-version=2024-05&enrich=False&enrich=True",
            Method::GET,
            Refusal::Invalid,
        ),
        (
            "/$Resources/queues?api-version=2024-05&$skip=0&%24skip=1&$top=100",
            Method::GET,
            Refusal::Invalid,
        ),
    ] {
        let (request, polls) = counted(
            Some(&token),
            target,
            method,
            b"<!DOCTYPE entity SYSTEM 'file:///not-read'>",
        );
        assert_eq!(
            boundary
                .dispatch(request, Instant::now() + WAIT)
                .await
                .err(),
            Some(expected)
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn keyword_names_keep_entity_authorization_before_profiles_and_body_polls() -> TestResult {
    let identity = identity()?;
    let handler = Arc::new(Recording::default());
    let mut boundary = boundary(&identity, handler.clone(), Limits::default());
    Arc::get_mut(&mut boundary.config).unwrap().policy =
        JwtPolicy::from_json(&keyword_policy().to_string())?;
    let wrong = fixture::sign(fixture::claims("entity"));
    for (index, path) in KEYWORD_PATHS.iter().enumerate() {
        let target = format!("/{path}?api-version=2024-05&enrich=False");
        let address = route::address(&target.parse()?).unwrap();
        assert!(matches!(
            address,
            Address::Entity(_) | Address::Subscription { .. }
        ));
        assert_eq!(
            address
                .scope(&boundary.config.root)
                .unwrap()
                .path()
                .collect::<Vec<_>>()
                .join("/"),
            path.to_ascii_lowercase()
        );
        // Even an invalid profile cannot be inspected before the scope check.
        for query in [
            "api-version=2024-05&enrich=False",
            "api-version=invalid&unknown=true",
        ] {
            let (request, polls) = counted(
                Some(&wrong),
                &format!("/{path}?{query}"),
                Method::PUT,
                b"not-read",
            );
            assert_eq!(
                boundary
                    .dispatch(request, Instant::now() + WAIT)
                    .await
                    .err(),
                Some(Refusal::Forbidden)
            );
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert_eq!(handler.selections.load(Ordering::SeqCst), index);
            assert_eq!(handler.backend.load(Ordering::SeqCst), index);
        }
        let token = fixture::sign(fixture::claims(&format!("keyword-{index}")));
        let (request, polls) = counted(Some(&token), &target, Method::GET, b"not-read");
        assert!(
            boundary
                .dispatch(request, Instant::now() + WAIT)
                .await
                .is_ok()
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    for target in [
        "/rules/../child",
        "/subscriptions//child",
        "/rules/%2fchild",
        "/subscriptions/$management/Subscriptions/sub",
    ] {
        let (request, polls) = counted(None, target, Method::GET, b"not-read");
        assert_eq!(
            boundary
                .dispatch(request, Instant::now() + WAIT)
                .await
                .err(),
            Some(Refusal::Invalid)
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    assert_eq!(
        handler.selections.load(Ordering::SeqCst),
        KEYWORD_PATHS.len()
    );
    assert_eq!(handler.backend.load(Ordering::SeqCst), KEYWORD_PATHS.len());
    Ok(())
}

#[tokio::test]
async fn authorized_input_reply_and_encoding_bounds_fail_closed() -> TestResult {
    let identity = identity()?;
    let handler = Arc::new(Recording::default());
    let limits = Limits {
        body_bytes: 4,
        reply_bytes: 128,
        ..Limits::default()
    };
    let boundary = boundary(&identity, handler.clone(), limits);
    let token = fixture::sign(fixture::claims("operator"));
    for (data, length, encoding, expected) in [
        (b"large".as_slice(), "5", false, Refusal::TooLarge),
        (b"large".as_slice(), "4", false, Refusal::TooLarge),
        (b"body".as_slice(), "4", true, Refusal::Encoding),
    ] {
        let bytes: &'static [u8] = if data.len() == 5 { b"large" } else { b"body" };
        let (mut request, polls) = counted(Some(&token), TARGET, Method::PUT, bytes);
        request.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/atom+xml"),
        );
        request.headers_mut().insert(
            header::CONTENT_LENGTH,
            header::HeaderValue::from_str(length)?,
        );
        if encoding {
            request.headers_mut().insert(
                header::CONTENT_ENCODING,
                header::HeaderValue::from_static("gzip"),
            );
        }
        assert_eq!(
            boundary
                .dispatch(request, Instant::now() + WAIT)
                .await
                .err(),
            Some(expected)
        );
        if length == "5" || encoding {
            assert_eq!(polls.load(Ordering::SeqCst), 0);
        }
        assert_eq!(handler.backend.load(Ordering::SeqCst), 0);
    }
    handler.reply_size.store(129, Ordering::SeqCst);
    let (request, _) = counted(Some(&token), TARGET, Method::GET, b"");
    assert_eq!(
        boundary
            .dispatch(request, Instant::now() + WAIT)
            .await
            .err(),
        Some(Refusal::Internal)
    );
    Ok(())
}

#[tokio::test]
async fn expired_unbegun_request_cannot_read_body_or_begin_backend() -> TestResult {
    let identity = identity()?;
    let handler = Arc::new(Recording::default());
    let boundary = boundary(&identity, handler.clone(), Limits::default());
    let token = fixture::sign(fixture::claims("operator"));
    let (request, polls) = counted(Some(&token), TARGET, Method::PUT, b"not-read");
    assert_eq!(
        boundary
            .dispatch(request, Instant::now() - Duration::from_secs(1))
            .await
            .err(),
        Some(Refusal::Deadline)
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(handler.selections.load(Ordering::SeqCst), 0);
    assert_eq!(handler.backend.load(Ordering::SeqCst), 0);
    Ok(())
}

struct Node {
    address: SocketAddr,
    identity: Identity,
    retirement: Retirement,
    serving: JoinHandle<HttpsListener>,
    connections: Arc<Semaphore>,
    requests: Arc<Semaphore>,
}
impl Node {
    async fn start(handler: Arc<dyn Handler>, limits: Limits) -> TestResult<Self> {
        Self::start_with_policy(handler, limits, fixture::policy()).await
    }

    async fn start_with_policy(
        handler: Arc<dyn Handler>,
        limits: Limits,
        policy: serde_json::Value,
    ) -> TestResult<Self> {
        let identity = identity()?;
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?;
        let mut config = config(&identity, limits);
        config.policy = JwtPolicy::from_json(&policy.to_string())?;
        let mut service = HttpsListener::with_handler(socket, config, handler);
        let retirement = service.retirement_handle();
        let (connections, requests) = service.permit_witnesses();
        let serving = tokio::spawn(async move {
            service.serve().await;
            service
        });
        Ok(Self {
            address,
            identity,
            retirement,
            serving,
            connections,
            requests,
        })
    }
    async fn connect(&self) -> TestResult<TlsStream<TcpStream>> {
        connect(self.address, self.identity.ca.clone(), "localhost").await
    }
    async fn stop(self) -> TestResult {
        self.retirement.request();
        let mut service = tokio::time::timeout(WAIT, self.serving).await??;
        tokio::time::timeout(WAIT, service.finish()).await?;
        assert!(service.take_failure().is_none());
        assert!(TcpStream::connect(self.address).await.is_err());
        Ok(())
    }
}
async fn connect(
    address: SocketAddr,
    ca: CertificateDer<'static>,
    host: &str,
) -> TestResult<TlsStream<TcpStream>> {
    let mut roots = RootCertStore::empty();
    roots.add(ca)?;
    let mut tls =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(tokio::time::timeout(
        WAIT,
        TlsConnector::from(Arc::new(tls)).connect(
            ServerName::try_from(host.to_owned())?,
            TcpStream::connect(address).await?,
        ),
    )
    .await??)
}
fn wire(token: Option<&str>, method: &str, target: &str, extra: &str, body: &str) -> String {
    let auth = token
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    format!("{method} {target} HTTP/1.1\r\nHost: foreign-peer.example\r\n{auth}{extra}\r\n{body}")
}
async fn exchange(node: &Node, wire: &str) -> TestResult<String> {
    exchange_bytes(node, wire.as_bytes()).await
}
async fn exchange_bytes(node: &Node, wire: &[u8]) -> TestResult<String> {
    let mut stream = node.connect().await?;
    stream.write_all(wire).await?;
    read(&mut stream).await
}
async fn read(stream: &mut TlsStream<TcpStream>) -> TestResult<String> {
    let mut bytes = Vec::new();
    tokio::time::timeout(WAIT, stream.take(1024 * 1024).read_to_end(&mut bytes)).await??;
    Ok(String::from_utf8(bytes)?)
}

#[tokio::test(flavor = "multi_thread")]
async fn actual_private_ca_https_authorizes_before_body_and_keeps_routes_closed() -> TestResult {
    let node = Node::start(Arc::new(ClosedRoutes), Limits::default()).await?;
    let token = fixture::sign(fixture::claims("operator"));
    assert!(
        exchange(&node, &wire(Some(&token), "GET", TARGET, "", ""))
            .await?
            .starts_with("HTTP/1.1 501")
    );
    // No body bytes are sent. Missing credentials must get an immediate refusal,
    // not wait to read the declared body or touch a handler/backend.
    assert!(
        exchange(
            &node,
            &wire(None, "PUT", TARGET, "Content-Length: 100\r\n", "")
        )
        .await?
        .starts_with("HTTP/1.1 401")
    );
    let sender = fixture::sign(fixture::claims("sender"));
    assert!(
        exchange(&node, &wire(Some(&sender), "GET", TARGET, "", ""))
            .await?
            .starts_with("HTTP/1.1 403")
    );
    assert!(
        exchange(
            &node,
            &wire(
                Some(&token),
                "GET",
                "/$Resources/queues?api-version=2024-05&$skip=0&$top=100",
                "",
                ""
            )
        )
        .await?
        .starts_with("HTTP/1.1 400")
    );
    assert!(
        connect(node.address, node.identity.ca.clone(), "wrong.example")
            .await
            .is_err()
    );
    assert!(
        connect(node.address, identity()?.ca, "localhost")
            .await
            .is_err()
    );
    let mut plain = TcpStream::connect(node.address).await?;
    plain
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;
    let mut bytes = [0; 64];
    let n = tokio::time::timeout(WAIT, plain.read(&mut bytes))
        .await?
        .unwrap_or(0);
    assert!(!bytes[..n].starts_with(b"HTTP/1.1 200"));
    node.stop().await
}

#[tokio::test(flavor = "multi_thread")]
async fn actual_https_keyword_entity_names_reach_only_the_closed_singleton_route() -> TestResult {
    let node = Node::start_with_policy(Arc::new(ClosedRoutes), Limits::default(), keyword_policy())
        .await?;
    let operator = fixture::sign(fixture::claims("operator"));
    let wrong = fixture::sign(fixture::claims("entity"));
    let sender = fixture::sign(fixture::claims("sender"));
    let foreign = fixture::sign(fixture::claims("foreign"));
    for (index, path) in KEYWORD_PATHS.iter().enumerate() {
        let token = fixture::sign(fixture::claims(&format!("keyword-{index}")));
        let target = format!("/{path}?api-version=2024-05&enrich=False");
        for allowed in [&token, &operator] {
            assert!(
                exchange(&node, &wire(Some(allowed), "GET", &target, "", ""))
                    .await?
                    .starts_with("HTTP/1.1 501"),
                "keyword singleton {path}"
            );
        }
        for denied in [&wrong, &sender, &foreign] {
            assert!(
                exchange(
                    &node,
                    &wire(Some(denied), "GET", &target, "Content-Length: 100\r\n", "")
                )
                .await?
                .starts_with("HTTP/1.1 403")
            );
        }
        assert!(
            exchange(&node, &wire(None, "GET", &target, "", ""))
                .await?
                .starts_with("HTTP/1.1 401")
        );
        let sibling = format!("/{path}-sibling?api-version=2024-05&enrich=False");
        assert!(
            exchange(&node, &wire(Some(&token), "GET", &sibling, "", ""))
                .await?
                .starts_with("HTTP/1.1 403")
        );
        assert!(
            exchange(
                &node,
                &wire(
                    Some(&token),
                    "GET",
                    "/$Resources/queues?api-version=2024-05&enrich=False",
                    "",
                    ""
                )
            )
            .await?
            .starts_with("HTTP/1.1 403")
        );
    }
    // /Subscriptions is structural only below a topic. This real collection
    // still refuses runtime materialization, while its same-word singleton does not.
    assert!(
        exchange(
            &node,
            &wire(
                Some(&operator),
                "GET",
                "/rules/Subscriptions?api-version=2024-05&enrich=False",
                "",
                ""
            )
        )
        .await?
        .starts_with("HTTP/1.1 400")
    );
    // Extra rule-route suffixes remain unsupported and retain their existing
    // namespace-root authorization requirement; no rule adapter is installed.
    let subscription = fixture::sign(fixture::claims("keyword-4"));
    for suffix in ["Rules", "Rules/rules"] {
        let target =
            format!("/rules/Subscriptions/rules/{suffix}?api-version=2024-05&enrich=False");
        assert_eq!(
            route::address(&target.parse()?).unwrap(),
            Address::Unsupported
        );
        assert!(
            exchange(&node, &wire(Some(&operator), "GET", &target, "", ""))
                .await?
                .starts_with("HTTP/1.1 501")
        );
        for denied in [&subscription, &wrong, &sender, &foreign] {
            assert!(
                exchange(&node, &wire(Some(denied), "GET", &target, "", ""))
                    .await?
                    .starts_with("HTTP/1.1 403")
            );
        }
        assert!(
            exchange(&node, &wire(None, "GET", &target, "", ""))
                .await?
                .starts_with("HTTP/1.1 401")
        );
        assert!(
            exchange(
                &node,
                &wire(
                    Some(&operator),
                    "PUT",
                    &target,
                    "Content-Length: 100\r\n",
                    ""
                )
            )
            .await?
            .starts_with("HTTP/1.1 400")
        );
    }
    node.stop().await
}

#[tokio::test(flavor = "multi_thread")]
async fn actual_https_framing_trailers_utf8_and_pipelining_never_dispatch_a_hidden_request()
-> TestResult {
    let handler = Arc::new(Recording::default());
    let node = Node::start(handler.clone(), Limits::default()).await?;
    let token = fixture::sign(fixture::claims("operator"));
    let second = wire(Some(&token), "GET", TARGET, "", "");
    let conflicting = wire(
        Some(&token),
        "PUT",
        TARGET,
        "Content-Type: application/atom+xml\r\nContent-Length: 4\r\nContent-Length: 5\r\n",
        "body",
    );
    let reply = exchange(&node, &format!("{conflicting}{second}")).await?;
    assert!(reply.starts_with("HTTP/1.1 400"));
    assert_eq!(reply.matches("HTTP/1.1 ").count(), 1);
    assert_eq!(handler.selections.load(Ordering::SeqCst), 0);
    assert_eq!(handler.backend.load(Ordering::SeqCst), 0);

    let mut invalid_utf8 = wire(
        Some(&token),
        "PUT",
        TARGET,
        "Content-Type: application/atom+xml\r\nContent-Length: 1\r\n",
        "",
    )
    .into_bytes();
    invalid_utf8.push(0xff);
    invalid_utf8.extend_from_slice(second.as_bytes());
    let reply = exchange_bytes(&node, &invalid_utf8).await?;
    assert!(reply.starts_with("HTTP/1.1 415"));
    assert_eq!(reply.matches("HTTP/1.1 ").count(), 1);
    assert_eq!(handler.backend.load(Ordering::SeqCst), 0);

    let trailers = wire(
        Some(&token),
        "PUT",
        TARGET,
        "Content-Type: application/atom+xml\r\nTransfer-Encoding: chunked\r\nTrailer: X-Probe\r\n",
        "4\r\nbody\r\n0\r\nX-Probe: synthetic\r\n\r\n",
    );
    let reply = exchange(&node, &format!("{trailers}{second}")).await?;
    assert!(reply.starts_with("HTTP/1.1 415"));
    assert_eq!(reply.matches("HTTP/1.1 ").count(), 1);
    assert_eq!(handler.backend.load(Ordering::SeqCst), 0);

    // Hyper normalizes CL+TE to chunked (in either order) and closes the
    // connection. Assert that only that one bounded body can reach the handler,
    // never the pipelined request; do not claim a raw-header rejection here.
    for framing in [
        "Content-Length: 1\r\nTransfer-Encoding: chunked\r\n",
        "Transfer-Encoding: chunked\r\nContent-Length: 1\r\n",
    ] {
        let before = handler.backend.load(Ordering::SeqCst);
        let primary = wire(
            Some(&token),
            "PUT",
            TARGET,
            &format!("Content-Type: application/atom+xml\r\n{framing}"),
            "4\r\nbody\r\n0\r\n\r\n",
        );
        let reply = exchange(&node, &format!("{primary}{second}")).await?;
        assert!(reply.starts_with("HTTP/1.1 200"));
        assert_eq!(reply.matches("HTTP/1.1 ").count(), 1);
        assert_eq!(handler.backend.load(Ordering::SeqCst), before + 1);
        assert_eq!(handler.bodies.lock().unwrap().last().unwrap(), b"body");
    }
    let before = handler.backend.load(Ordering::SeqCst);
    let reply = exchange(&node, &format!("{second}{second}")).await?;
    assert!(reply.starts_with("HTTP/1.1 200"));
    assert_eq!(reply.matches("HTTP/1.1 ").count(), 1);
    assert_eq!(handler.backend.load(Ordering::SeqCst), before + 1);
    assert_eq!(handler.selections.load(Ordering::SeqCst), 5);
    node.stop().await?;
    // Actual joins also rule out a hidden accepted original starting later.
    assert_eq!(handler.backend.load(Ordering::SeqCst), 3);
    assert_eq!(handler.selections.load(Ordering::SeqCst), 5);
    assert_eq!(handler.bodies.lock().unwrap().len(), 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn actual_https_bounds_body_headers_reply_and_slow_input_without_backend_reads() -> TestResult
{
    let handler = Arc::new(Recording::default());
    let limits = Limits {
        body_bytes: 4,
        reply_bytes: 128,
        header_bytes: 8192,
        header_timeout: Duration::from_secs(1),
        body_timeout: Duration::from_millis(200),
        request_timeout: Duration::from_secs(5),
        connection_timeout: Duration::from_secs(10),
        ..Limits::default()
    };
    let node = Node::start(handler.clone(), limits).await?;
    let token = fixture::sign(fixture::claims("operator"));
    assert!(
        exchange(&node, &wire(Some(&token), "GET", TARGET, "", ""))
            .await?
            .starts_with("HTTP/1.1 200")
    );
    let before = handler.backend.load(Ordering::SeqCst);
    for (extra, body, expected) in [
        (
            "Content-Type: application/atom+xml\r\nContent-Length: 5\r\n",
            "",
            "413",
        ),
        (
            "Content-Type: application/atom+xml\r\nTransfer-Encoding: chunked\r\n",
            "5\r\nlarge\r\n0\r\n\r\n",
            "413",
        ),
        (
            "Content-Type: application/atom+xml\r\nContent-Length: 4\r\n",
            "",
            "408",
        ),
        ("Content-Encoding: gzip\r\nContent-Length: 4\r\n", "", "415"),
    ] {
        let reply = exchange(&node, &wire(Some(&token), "PUT", TARGET, extra, body)).await?;
        assert!(reply.starts_with(&format!("HTTP/1.1 {expected}")));
        assert_eq!(handler.backend.load(Ordering::SeqCst), before);
    }
    let extra = format!("X-Oversized: {}\r\n", "x".repeat(9000));
    let reply = exchange(&node, &wire(Some(&token), "GET", TARGET, &extra, "")).await?;
    assert!(reply.starts_with("HTTP/1.1 431"));
    let mut slow = node.connect().await?;
    slow.write_all(b"GET /orders HTTP/1.1\r\nHost: incomplete")
        .await?;
    let reply = read(&mut slow).await?;
    assert!(reply.is_empty() || reply.starts_with("HTTP/1.1 408"));
    assert_eq!(handler.backend.load(Ordering::SeqCst), before);
    handler.reply_size.store(129, Ordering::SeqCst);
    assert!(
        exchange(&node, &wire(Some(&token), "GET", TARGET, "", ""))
            .await?
            .starts_with("HTTP/1.1 500")
    );
    node.stop().await
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_and_request_originals_are_globally_bounded_through_cancel_and_join()
-> TestResult {
    let handler = Arc::new(Recording::default());
    let (release, held) = oneshot::channel();
    *handler.hold.lock().unwrap() = Some(held);
    let limits = Limits {
        connections: 4,
        requests: 1,
        ..Limits::default()
    };
    let node = Node::start(handler.clone(), limits).await?;
    let token = fixture::sign(fixture::claims("operator"));
    let mut first = node.connect().await?;
    first
        .write_all(wire(Some(&token), "GET", TARGET, "", "").as_bytes())
        .await?;
    tokio::time::timeout(WAIT, handler.entered.notified()).await?;
    eprintln!("atom-bound: first request entered");
    drop(first);
    for _ in 0..8 {
        assert!(
            exchange(&node, &wire(Some(&token), "GET", TARGET, "", ""))
                .await?
                .starts_with("HTTP/1.1 503")
        );
        assert_eq!(handler.backend.load(Ordering::SeqCst), 1);
        assert_eq!(node.requests.available_permits(), 0);
    }
    release.send(()).unwrap();
    eprintln!("atom-bound: first backend released");
    tokio::time::timeout(WAIT, handler.completed.notified()).await?;
    drop(tokio::time::timeout(WAIT, node.requests.clone().acquire_owned()).await??);
    eprintln!("atom-bound: first original joined");
    assert!(
        exchange(&node, &wire(Some(&token), "GET", TARGET, "", ""))
            .await?
            .starts_with("HTTP/1.1 200")
    );
    node.stop().await?;
    eprintln!("atom-bound: request-bound listener stopped");

    let handler = Arc::new(Recording::default());
    let (release, held) = oneshot::channel();
    *handler.hold.lock().unwrap() = Some(held);
    let node = Node::start(
        handler.clone(),
        Limits {
            connections: 1,
            ..Limits::default()
        },
    )
    .await?;
    let mut first = node.connect().await?;
    first
        .write_all(wire(Some(&token), "GET", TARGET, "", "").as_bytes())
        .await?;
    tokio::time::timeout(WAIT, handler.entered.notified()).await?;
    eprintln!("atom-bound: connection-bound request entered");
    assert_eq!(node.connections.available_permits(), 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), node.connect())
            .await
            .is_err()
    );
    eprintln!("atom-bound: second TLS admission stayed bounded");
    release.send(()).unwrap();
    read(&mut first).await?;
    eprintln!("atom-bound: first connection finished response");
    // Do not temporarily take the sole connection permit in this witness: the
    // accept pump deliberately does not poll the socket while capacity is zero.
    // A new TLS exchange proves its original was joined and capacity returned.
    assert!(
        exchange(&node, &wire(Some(&token), "GET", TARGET, "", ""))
            .await?
            .starts_with("HTTP/1.1 200")
    );
    node.stop().await
}

#[tokio::test(flavor = "multi_thread")]
async fn wire_deadline_does_not_cancel_begun_backend_or_return_its_slot() -> TestResult {
    let handler = Arc::new(Recording::default());
    let (release, held) = oneshot::channel();
    *handler.hold.lock().unwrap() = Some(held);
    let node = Node::start(
        handler.clone(),
        Limits {
            requests: 1,
            body_timeout: Duration::from_millis(50),
            request_timeout: Duration::from_secs(1),
            connection_timeout: Duration::from_secs(5),
            ..Limits::default()
        },
    )
    .await?;
    let token = fixture::sign(fixture::claims("operator"));
    let reply = exchange(&node, &wire(Some(&token), "GET", TARGET, "", "")).await?;
    assert!(reply.starts_with("HTTP/1.1 504"));
    assert!(reply.contains("outcome unknown"));
    assert_eq!(handler.backend.load(Ordering::SeqCst), 1);
    assert_eq!(node.requests.available_permits(), 0);
    assert!(
        exchange(&node, &wire(Some(&token), "GET", TARGET, "", ""))
            .await?
            .starts_with("HTTP/1.1 503")
    );
    release.send(()).unwrap();
    tokio::time::timeout(WAIT, handler.completed.notified()).await?;
    drop(tokio::time::timeout(WAIT, node.requests.clone().acquire_owned()).await??);
    node.stop().await
}

struct Panicking;
impl Handler for Panicking {
    fn select(&self, _: &AuthorizedHead) -> Result<BodyMode, Refusal> {
        Ok(BodyMode::Empty)
    }
    fn handle(&self, _: AuthorizedRequest) -> HandlerFuture<'_> {
        Box::pin(async { panic!("deliberate Atom callback panic") })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn callback_panic_retires_the_pump_and_joins_both_families() -> TestResult {
    let node = Node::start(
        Arc::new(Panicking),
        Limits {
            connections: 1,
            requests: 1,
            ..Limits::default()
        },
    )
    .await?;
    let token = fixture::sign(fixture::claims("operator"));
    assert!(
        exchange(&node, &wire(Some(&token), "GET", TARGET, "", ""))
            .await?
            .starts_with("HTTP/1.1 500")
    );
    let mut service = tokio::time::timeout(WAIT, node.serving).await??;
    tokio::time::timeout(WAIT, service.finish()).await?;
    assert!(service.take_failure().is_some());
    assert_eq!(node.requests.available_permits(), 1);
    assert_eq!(node.connections.available_permits(), 1);
    assert!(TcpStream::connect(node.address).await.is_err());
    Ok(())
}

#[test]
fn limits_fail_closed() {
    assert!(Limits::default().validate().is_ok());
    for limits in [
        Limits {
            requests: 0,
            ..Limits::default()
        },
        Limits {
            header_bytes: 1024,
            ..Limits::default()
        },
        Limits {
            reply_bytes: 1,
            ..Limits::default()
        },
        Limits {
            body_timeout: Duration::ZERO,
            ..Limits::default()
        },
    ] {
        assert!(limits.validate().is_err());
    }
}
