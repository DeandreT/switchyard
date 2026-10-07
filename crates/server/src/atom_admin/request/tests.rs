use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use auth::{PermissionSet, SharedAccessKey, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{StateMachine, Timestamp};
use hmac::{Hmac, Mac};
use hyper::{
    HeaderMap, Method, Uri, Version,
    body::{Frame, SizeHint},
    header::{CONTENT_TYPE, HeaderValue},
};
use sha2::Sha256;
use storage::{Key, MemoryStore, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};

use super::*;
use crate::{Broker, Clock, LocalProposer};

const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "atom-unit-secret";
const DEFINITION: &[u8] = br#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><MaxSizeInMegabytes>1</MaxSizeInMegabytes></QueueDescription></content></entry>"#;

fn token(audience: &str, expiry: u64) -> String {
    let resource = url::form_urlencoded::byte_serialize(audience.as_bytes()).collect::<String>();
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).unwrap();
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let signature = url::form_urlencoded::byte_serialize(signature.as_bytes()).collect::<String>();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn=rule")
}

#[derive(Clone, Default)]
struct CountStore {
    inner: MemoryStore,
    calls: Arc<AtomicUsize>,
}
impl StateStore for CountStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }
}

#[derive(Clone)]
struct CountClock(Arc<AtomicUsize>);
impl Clock for CountClock {
    fn now(&self) -> Timestamp {
        self.0.fetch_add(1, Ordering::SeqCst);
        Timestamp::from_millis(1_000)
    }
}

struct Fixture {
    _broker: Broker,
    context: RequestContext,
    store: CountStore,
    clock: CountClock,
}
impl Fixture {
    fn new(permissions: PermissionSet) -> Self {
        let store = CountStore::default();
        let clock = CountClock(Arc::default());
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let context = RequestContext {
            broker: broker.handle(),
            namespace: NamespaceName::new("tenant").unwrap(),
            policy: SharedAccessPolicy::new([SharedAccessRule::new(
                "rule",
                ResourceScope::namespace(HOST).unwrap(),
                SharedAccessKey::new(KEY).unwrap(),
                None,
                permissions,
            )
            .unwrap()])
            .unwrap(),
            audience: ResourceScope::namespace(HOST).unwrap(),
        };
        Self {
            _broker: broker,
            context,
            store,
            clock,
        }
    }
    fn assert_untouched(&self) {
        assert_eq!(self.store.calls.load(Ordering::SeqCst), 0);
        assert_eq!(self.clock.0.load(Ordering::SeqCst), 0);
        assert_eq!(
            self.store.inner.snapshot().unwrap(),
            StoreSnapshot::default()
        );
    }
}

struct Frames {
    frames: VecDeque<Result<Frame<Bytes>, &'static str>>,
    polls: Arc<AtomicUsize>,
    advance: Option<Arc<AtomicU64>>,
}
impl Frames {
    fn data(data: &'static [u8]) -> Self {
        Self {
            frames: VecDeque::from([Ok(Frame::data(Bytes::from_static(data)))]),
            polls: Arc::default(),
            advance: None,
        }
    }
}
impl Body for Frames {
    type Data = Bytes;
    type Error = &'static str;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(epoch) = self.advance.take() {
            epoch.store(101, Ordering::SeqCst);
        }
        Poll::Ready(self.frames.pop_front())
    }
    fn is_end_stream(&self) -> bool {
        false
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::new()
    }
}

fn request<B>(method: Method, path: &str, body: B, authorization: Option<String>) -> Request<B> {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "attacker.example:4567")
        .header(CONTENT_TYPE, "application/atom+xml");
    if let Some(token) = authorization {
        builder = builder.header(AUTHORIZATION, token);
    }
    builder.body(body).unwrap()
}

async fn bytes(response: Response<Full<Bytes>>) -> Bytes {
    response.into_body().collect().await.unwrap().to_bytes()
}

#[tokio::test]
async fn missing_authentication_precedes_query_method_xml_and_body_polling() {
    let fixture = Fixture::new(PermissionSet::MANAGE);
    let body = Frames::data(b"<not-xml");
    let polls = body.polls.clone();
    let response = handle_with_epoch(
        request(Method::POST, "/orders?unknown=invalid", body, None),
        &fixture.context,
        || Ok(100),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert!(
        !String::from_utf8(bytes(response).await.to_vec())
            .unwrap()
            .contains("InvalidXml")
    );
    fixture.assert_untouched();
}

#[tokio::test]
async fn send_permission_never_polls_a_definition_or_enters_the_owner() {
    let fixture = Fixture::new(PermissionSet::SEND);
    let body = Frames::data(DEFINITION);
    let polls = body.polls.clone();
    let response = handle_with_epoch(
        request(
            Method::PUT,
            "/orders?api-version=2024-05",
            body,
            Some(token(&format!("https://{HOST}"), 200)),
        ),
        &fixture.context,
        || Ok(100),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    fixture.assert_untouched();
}

#[tokio::test]
async fn expiry_during_body_collection_refuses_the_retained_grant_before_admission() {
    let fixture = Fixture::new(PermissionSet::MANAGE);
    let epoch = Arc::new(AtomicU64::new(100));
    let mut body = Frames::data(DEFINITION);
    body.advance = Some(epoch.clone());
    let response = handle_with_epoch(
        request(
            Method::PUT,
            "/orders?api-version=2024-05",
            body,
            Some(token(&format!("https://{HOST}"), 101)),
        ),
        &fixture.context,
        || Ok(epoch.load(Ordering::SeqCst)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    fixture.assert_untouched();
}

#[tokio::test]
async fn epoch_acquisition_failure_is_fail_closed_at_both_authorization_boundaries() {
    let fixture = Fixture::new(PermissionSet::MANAGE);
    for failing_call in [0, 1] {
        let calls = AtomicUsize::new(0);
        let response = handle_with_epoch(
            request(
                Method::PUT,
                "/orders?api-version=2024-05",
                Frames::data(DEFINITION),
                Some(token(&format!("https://{HOST}"), 200)),
            ),
            &fixture.context,
            || {
                if calls.fetch_add(1, Ordering::SeqCst) == failing_call {
                    Err(RequestFailure::Authentication)
                } else {
                    Ok(100)
                }
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        fixture.assert_untouched();
    }
}

#[tokio::test]
async fn host_and_forwarded_headers_cannot_change_the_fixed_authorization_scope() {
    let fixture = Fixture::new(PermissionSet::MANAGE);
    let mut input = request(
        Method::PUT,
        "/orders?api-version=2024-05",
        Frames::data(DEFINITION),
        Some(token(&format!("https://{HOST}/orders"), 200)),
    );
    input
        .headers_mut()
        .insert("forwarded", HeaderValue::from_static("host=other.example"));
    let response = handle_with_epoch(input, &fixture.context, || Ok(100)).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let view = fixture
        .context
        .broker
        .get_atom_finite_queue(
            fixture.context.namespace.clone(),
            domain::EntityPath::new("orders").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.binding.namespace().as_str(), "tenant");
}

#[tokio::test]
async fn entity_scope_cannot_authorize_a_sibling_or_namespace_collection() {
    let fixture = Fixture::new(PermissionSet::MANAGE);
    for path in [
        "/orders-sibling?api-version=2024-05",
        "/$Resources/queues?api-version=2024-05",
    ] {
        let response = handle_with_epoch(
            request(
                Method::GET,
                path,
                Frames::data(b""),
                Some(token(&format!("https://{HOST}/orders"), 200)),
            ),
            &fixture.context,
            || Ok(100),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        fixture.assert_untouched();
    }
}

#[tokio::test]
async fn amqp_or_nondefault_port_tokens_are_not_http_authorization() {
    let fixture = Fixture::new(PermissionSet::MANAGE);
    for audience in [
        format!("amqps://{HOST}"),
        format!("https://{HOST}:444"),
        "https://other.example".into(),
    ] {
        let response = handle_with_epoch(
            request(
                Method::PUT,
                "/orders?api-version=2024-05",
                Frames::data(DEFINITION),
                Some(token(&audience, 200)),
            ),
            &fixture.context,
            || Ok(100),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        fixture.assert_untouched();
    }
}

#[tokio::test]
async fn duplicate_and_overlong_authorization_are_owner_free() {
    let fixture = Fixture::new(PermissionSet::MANAGE);
    for duplicate in [false, true] {
        let mut input = request(
            Method::PUT,
            "/orders?api-version=2024-05",
            Frames::data(DEFINITION),
            Some(if duplicate {
                token(&format!("https://{HOST}"), 200)
            } else {
                "x".repeat(MAX_TOKEN_BYTES + 1)
            }),
        );
        if duplicate {
            input
                .headers_mut()
                .append(AUTHORIZATION, HeaderValue::from_static("not-a-token"));
        }
        let response = handle_with_epoch(input, &fixture.context, || Ok(100)).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        fixture.assert_untouched();
    }
}

#[test]
fn route_once_decodes_literal_case_percent_spaces_and_unicode() {
    let headers = HeaderMap::new();
    for (raw, expected) in [
        ("/Orders/$Management", "Orders/$Management"),
        ("/orders/%252F", "orders/%2F"),
        ("/orders/a%20b", "orders/a b"),
        ("/orders/%CE%B1", "orders/\u{03b1}"),
        ("/orders/%23retained", "orders/#retained"),
        ("/$resources/queues", "$resources/queues"),
    ] {
        let Target::Entity(actual) = route::target(&raw.parse().unwrap(), &headers).unwrap() else {
            panic!("literal entity route");
        };
        assert_eq!(actual, expected);
    }
    assert!(matches!(
        route::target(&"/$Resources/queues".parse().unwrap(), &headers).unwrap(),
        Target::Collection
    ));
}

#[test]
fn observed_uri_fragment_normalization_is_shared_by_route_and_authorization() {
    let uri: Uri = "/orders?api-version=2024-05#discarded".parse().unwrap();
    assert_eq!(
        uri.path_and_query().unwrap().as_str(),
        "/orders?api-version=2024-05"
    );
    let target = route::target(&uri, &HeaderMap::new()).unwrap();
    let scope = target
        .scope(&ResourceScope::namespace(HOST).unwrap())
        .unwrap();
    assert_eq!(scope.path().collect::<Vec<_>>(), ["orders"]);
    assert_eq!(target.entity().unwrap().as_str(), "orders");
}

#[test]
fn ambiguous_paths_and_absolute_targets_are_refused() {
    for path in [
        "/",
        "/orders/",
        "/orders//next",
        "/orders/.",
        "/orders/%2e%2e",
        "/orders/%2Fnext",
        "/orders/%5Cnext",
        "/orders/%00",
        "/orders/%FF",
        "/orders/%",
        "/orders/%GG",
        "https://other.example/orders",
        "*",
    ] {
        let uri: Uri = path.parse().unwrap();
        assert!(route::target(&uri, &HeaderMap::new()).is_err(), "{path}");
    }
}

#[test]
fn query_validation_is_strict_before_the_forgiving_library_parser() {
    let mut headers = HeaderMap::new();
    headers.insert("host", HeaderValue::from_static("localhost"));
    for query in [
        "api-version=2024-05&enrich=True",
        "api-version=2024-05&unknown=x",
        "api-version=2024-05&api%2Dversion=2024-05",
        "api-version=2024-05&enrich=%FF",
        "api-version=2024-05&enrich=%",
        "api-version=2024-05&&enrich=False",
        "api-version=2024-05&$top=+1",
        "api-version=2024-05&$top=0",
        "api-version=2024-05&$top=101",
        "api-version=2024-05&$skip=1001",
        "api-version=2024-05&$skip=184467440737095516160",
        "api-version=unsupported",
    ] {
        let uri: Uri = format!("/$Resources/queues?{query}").parse().unwrap();
        assert!(
            route::operation(
                &Method::GET,
                Version::HTTP_11,
                &uri,
                &headers,
                &Target::Collection
            )
            .is_err(),
            "{query}"
        );
    }
    for version in ["2024-05", "2021-05"] {
        let uri: Uri =
            format!("/$Resources/queues?api-version={version}&enrich=False&$skip=0&$top=100")
                .parse()
                .unwrap();
        assert_eq!(
            route::operation(
                &Method::GET,
                Version::HTTP_11,
                &uri,
                &headers,
                &Target::Collection
            ),
            Ok(Operation::List { skip: 0, top: 100 })
        );
    }
}

#[test]
fn full_put_uses_bare_atom_content_type_and_only_star_update_condition() {
    let uri: Uri = "/orders?api-version=2024-05".parse().unwrap();
    let target = Target::Entity("orders".into());
    let mut headers = HeaderMap::new();
    headers.insert("host", HeaderValue::from_static("localhost"));
    for invalid in [
        "application/atom+xml; charset=utf-8",
        "application/xml",
        "text/xml",
    ] {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(invalid));
        assert!(route::operation(&Method::PUT, Version::HTTP_11, &uri, &headers, &target).is_err());
    }
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("APPLICATION/ATOM+XML"),
    );
    assert_eq!(
        route::operation(&Method::PUT, Version::HTTP_11, &uri, &headers, &target),
        Ok(Operation::Create)
    );
    headers.insert("if-match", HeaderValue::from_static("*"));
    assert_eq!(
        route::operation(&Method::PUT, Version::HTTP_11, &uri, &headers, &target),
        Ok(Operation::Update)
    );
    headers.append("if-match", HeaderValue::from_static("*"));
    assert!(route::operation(&Method::PUT, Version::HTTP_11, &uri, &headers, &target).is_err());
}

#[test]
fn header_and_request_target_caps_include_duplicate_values() {
    let uri: Uri = "/orders".parse().unwrap();
    let mut headers = HeaderMap::new();
    for _ in 0..MAX_HEADER_COUNT + 1 {
        headers.append("x-repeat", HeaderValue::from_static("x"));
    }
    assert!(matches!(
        route::target(&uri, &headers),
        Err(RequestFailure::HeaderTooLarge)
    ));
    headers.clear();
    headers.insert(
        "x-long",
        HeaderValue::from_str(&"x".repeat(MAX_HEADER_BYTES)).unwrap(),
    );
    assert!(matches!(
        route::target(&uri, &headers),
        Err(RequestFailure::HeaderTooLarge)
    ));
    let too_long: Uri = format!("/{}", "x".repeat(MAX_TARGET_BYTES))
        .parse()
        .unwrap();
    assert!(route::target(&too_long, &HeaderMap::new()).is_err());
}

#[tokio::test]
async fn body_cap_is_applied_across_data_frames_before_buffer_growth() {
    let mut body = Frames::data(b"");
    body.frames = VecDeque::from([
        Ok(Frame::data(Bytes::from(vec![1; MAX_BODY_BYTES]))),
        Ok(Frame::data(Bytes::from_static(b"x"))),
    ]);
    assert_eq!(collect_body(body).await, Err(RequestFailure::BadRequest));
    let mut exact = Frames::data(b"");
    exact.frames = VecDeque::from([Ok(Frame::data(Bytes::from(vec![1; MAX_BODY_BYTES])))]);
    assert_eq!(collect_body(exact).await.unwrap().len(), MAX_BODY_BYTES);
}

#[tokio::test]
async fn empty_frames_and_trailers_have_independent_bounded_work() {
    for count in [MAX_BODY_FRAMES, MAX_BODY_FRAMES + 1] {
        let mut body = Frames::data(b"");
        body.frames = (0..count).map(|_| Ok(Frame::data(Bytes::new()))).collect();
        assert_eq!(collect_body(body).await.is_ok(), count == MAX_BODY_FRAMES);
    }
    let mut body = Frames::data(b"");
    body.frames = VecDeque::from([Ok(Frame::trailers(HeaderMap::new()))]);
    assert_eq!(collect_body(body).await, Err(RequestFailure::BadRequest));
}

#[tokio::test]
async fn body_failure_after_valid_xml_cannot_admit_a_partial_definition() {
    let fixture = Fixture::new(PermissionSet::MANAGE);
    let mut body = Frames::data(DEFINITION);
    body.frames.push_back(Err("secret backend framing detail"));
    let response = handle_with_epoch(
        request(
            Method::PUT,
            "/orders?api-version=2024-05",
            body,
            Some(token(&format!("https://{HOST}"), 200)),
        ),
        &fixture.context,
        || Ok(100),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    fixture.assert_untouched();
    assert!(
        !String::from_utf8(bytes(response).await.to_vec())
            .unwrap()
            .contains("secret")
    );
}

#[tokio::test]
async fn codec_and_noncodec_failures_keep_distinct_static_redacted_xml() {
    let codec = RequestFailure::Xml(xml::AtomXmlError::Malformed).into_response();
    assert_eq!(codec.status(), StatusCode::BAD_REQUEST);
    assert!(
        String::from_utf8(bytes(codec).await.to_vec())
            .unwrap()
            .contains("InvalidXml")
    );
    let failure = RequestFailure::from(crate::SubmitError::Propose(crate::ProposeError::Broker(
        domain::BrokerError::Storage(StorageError::Backend {
            operation: "secret operation",
            detail: "secret body/token/key".into(),
        }),
    )))
    .into_response();
    assert_eq!(failure.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let rendered = String::from_utf8(bytes(failure).await.to_vec()).unwrap();
    assert!(!rendered.contains("secret"));
    assert!(!rendered.contains("InvalidXml"));
    assert!(rendered.contains("InternalError"));
}
