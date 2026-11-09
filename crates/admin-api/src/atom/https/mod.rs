//! Server-owned TLS/HTTP/1 boundary. Production routes remain closed: no XML,
//! owner/generation reads, CRUD, rules or runtime materialization are activated.
mod listener;
mod route;
pub use listener::{HttpsListener, Retirement};
pub use route::{Address, Profile};

use crate::transport::TaskFamily;
use auth::{JwtError, JwtPolicy, Permission, ResourceScope};
use bytes::Bytes;
use domain::NamespaceName;
use http_body::Body;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Response, StatusCode, Version, header};
use rustls::{ServerConfig, pki_types::PrivateKeyDer};
use std::{
    convert::Infallible,
    fmt,
    future::Future,
    io::Cursor,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Semaphore, oneshot},
    time::Instant,
};

pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<Reply, Refusal>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Refusal {
    Unauthenticated,
    Forbidden,
    Invalid,
    Unsupported,
    RuntimeUnavailable,
    Encoding,
    TooLarge,
    Headers,
    BodyTimeout,
    Deadline,
    Busy,
    Retiring,
    Internal,
}
impl Refusal {
    pub fn status(self) -> StatusCode {
        match self {
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::Invalid | Self::RuntimeUnavailable => StatusCode::BAD_REQUEST,
            Self::Unsupported => StatusCode::NOT_IMPLEMENTED,
            Self::Encoding => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Headers => StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            Self::BodyTimeout => StatusCode::REQUEST_TIMEOUT,
            Self::Deadline => StatusCode::GATEWAY_TIMEOUT,
            Self::Busy | Self::Retiring => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
    fn message(self) -> &'static [u8] {
        match self {
            Self::Unauthenticated => b"valid bearer credential required",
            Self::Forbidden => b"management permission required",
            Self::Invalid => b"unsupported Atom input",
            Self::Unsupported => b"Atom adapters unavailable",
            Self::RuntimeUnavailable => b"runtime profile unavailable",
            Self::Encoding => b"unsupported body encoding",
            Self::TooLarge => b"request body too large",
            Self::Headers => b"request headers too large",
            Self::BodyTimeout => b"request body deadline expired",
            Self::Deadline => b"request deadline expired; outcome unknown",
            Self::Busy => b"Atom request capacity reached",
            Self::Retiring => b"Atom listener retiring",
            Self::Internal => b"Atom operation failed",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub header_bytes: usize,
    pub headers: usize,
    pub body_bytes: usize,
    pub reply_bytes: usize,
    pub connections: usize,
    pub requests: usize,
    pub handshake_timeout: Duration,
    pub header_timeout: Duration,
    pub body_timeout: Duration,
    pub request_timeout: Duration,
    pub connection_timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            header_bytes: 16 * 1024,
            headers: 64,
            body_bytes: 256 * 1024,
            reply_bytes: 256 * 1024,
            connections: 128,
            requests: 128,
            handshake_timeout: Duration::from_secs(10),
            header_timeout: Duration::from_secs(10),
            body_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(60),
        }
    }
}
impl Limits {
    fn validate(self) -> Result<(), ConfigurationError> {
        if !(8192..=65536).contains(&self.header_bytes)
            || !(1..=128).contains(&self.headers)
            || !(1..=1024 * 1024).contains(&self.body_bytes)
            || !(128..=1024 * 1024).contains(&self.reply_bytes)
            || !(1..=1024).contains(&self.connections)
            || !(1..=1024).contains(&self.requests)
            || [
                self.handshake_timeout,
                self.header_timeout,
                self.body_timeout,
                self.request_timeout,
                self.connection_timeout,
            ]
            .iter()
            .any(|duration| duration.is_zero() || *duration > Duration::from_secs(3600))
            || self.request_timeout <= self.body_timeout
            || self.connection_timeout < self.request_timeout
        {
            return Err(ConfigurationError);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigurationError;
impl fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid Atom HTTPS configuration")
    }
}
impl std::error::Error for ConfigurationError {}

/// Prepared identity/policy only; never holds an opened durable backend.
pub struct Configuration {
    tls: Arc<ServerConfig>,
    policy: JwtPolicy,
    namespace: NamespaceName,
    root: ResourceScope,
    limits: Limits,
}
impl Configuration {
    pub fn new(
        certificate: &[u8],
        key: &[u8],
        policy: JwtPolicy,
        namespace: NamespaceName,
        root: ResourceScope,
    ) -> Result<Self, ConfigurationError> {
        if certificate.len() > 64 * 1024
            || key.len() > 16 * 1024
            || root.path().next().is_some()
            || root.host().split('.').next() != Some(namespace.as_str())
        {
            return Err(ConfigurationError);
        }
        let items = rustls_pemfile::read_all(&mut Cursor::new(certificate))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ConfigurationError)?;
        let mut certs = Vec::new();
        for item in items {
            match item {
                rustls_pemfile::Item::X509Certificate(cert) => certs.push(cert),
                _ => return Err(ConfigurationError),
            }
        }
        if certs.is_empty() || certs.len() > 8 {
            return Err(ConfigurationError);
        }
        let mut keys = rustls_pemfile::read_all(&mut Cursor::new(key))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ConfigurationError)?;
        if keys.len() != 1 {
            return Err(ConfigurationError);
        }
        let key = match keys.pop().unwrap() {
            rustls_pemfile::Item::Pkcs1Key(key) => PrivateKeyDer::Pkcs1(key),
            rustls_pemfile::Item::Pkcs8Key(key) => PrivateKeyDer::Pkcs8(key),
            rustls_pemfile::Item::Sec1Key(key) => PrivateKeyDer::Sec1(key),
            _ => return Err(ConfigurationError),
        };
        let mut tls =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|_| ConfigurationError)?
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|_| ConfigurationError)?;
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            tls: Arc::new(tls),
            policy,
            namespace,
            root,
            limits: Limits::default(),
        })
    }
    pub fn with_limits(mut self, limits: Limits) -> Result<Self, ConfigurationError> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
    }
}

/// Credential-free, canonical, authorized metadata. Construction is private.
pub struct AuthorizedHead {
    namespace: NamespaceName,
    address: Address,
    scope: ResourceScope,
    method: Method,
    profile: Profile,
}
impl AuthorizedHead {
    pub fn namespace(&self) -> &NamespaceName {
        &self.namespace
    }
    pub fn address(&self) -> &Address {
        &self.address
    }
    pub fn scope(&self) -> &ResourceScope {
        &self.scope
    }
    pub fn method(&self) -> &Method {
        &self.method
    }
    pub fn profile(&self) -> Profile {
        self.profile
    }
}
pub struct AuthorizedRequest {
    head: AuthorizedHead,
    body: Vec<u8>,
}
impl AuthorizedRequest {
    pub fn head(&self) -> &AuthorizedHead {
        &self.head
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}
#[derive(Clone, Copy)]
pub enum BodyMode {
    Empty,
    Bytes,
}
pub struct Reply {
    status: StatusCode,
    bytes: Vec<u8>,
}
impl Reply {
    pub fn new(status: StatusCode, bytes: Vec<u8>) -> Self {
        Self { status, bytes }
    }
}

/// Trusted extension boundary for future adapters, called ONLY after authorization.
/// Selection does not poll the body. Once handle begins its original is retained
/// through completion/join even if the wire deadline or client connection ends.
pub trait Handler: Send + Sync + 'static {
    fn select(&self, head: &AuthorizedHead) -> Result<BodyMode, Refusal>;
    fn handle(&self, request: AuthorizedRequest) -> HandlerFuture<'_>;
}
pub struct ClosedRoutes;
impl Handler for ClosedRoutes {
    fn select(&self, head: &AuthorizedHead) -> Result<BodyMode, Refusal> {
        if head.method != Method::GET {
            return Err(Refusal::Invalid);
        }
        Err(match head.profile {
            Profile::RuntimeUnavailable => Refusal::RuntimeUnavailable,
            _ => Refusal::Unsupported,
        })
    }
    fn handle(&self, _: AuthorizedRequest) -> HandlerFuture<'_> {
        Box::pin(async { Err(Refusal::Unsupported) })
    }
}

struct Admission {
    retired: bool,
    permits: Arc<Semaphore>,
    requests: TaskFamily,
}
#[derive(Clone)]
struct Boundary {
    config: Arc<Configuration>,
    handler: Arc<dyn Handler>,
    admission: Arc<Mutex<Admission>>,
}
impl Boundary {
    fn authorize(&self, parts: &hyper::http::request::Parts) -> Result<AuthorizedHead, Refusal> {
        if parts.version != Version::HTTP_11 {
            return Err(Refusal::Invalid);
        }
        let address = route::address(&parts.uri)?;
        let scope = address.scope(&self.config.root)?;
        let mut auth = parts.headers.get_all(header::AUTHORIZATION).iter();
        let value = auth
            .next()
            .ok_or(Refusal::Unauthenticated)?
            .to_str()
            .map_err(|_| Refusal::Unauthenticated)?;
        if auth.next().is_some() {
            return Err(Refusal::Unauthenticated);
        }
        let (scheme, token) = value.split_once(' ').ok_or(Refusal::Unauthenticated)?;
        if !scheme.eq_ignore_ascii_case("bearer")
            || token.is_empty()
            || token.bytes().any(|byte| byte.is_ascii_whitespace())
        {
            return Err(Refusal::Unauthenticated);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Refusal::Internal)?
            .as_secs();
        let grant = self
            .config
            .policy
            .validate(token, &scope, now)
            .map_err(|error| {
                if error == JwtError::ScopeMismatch {
                    Refusal::Forbidden
                } else {
                    Refusal::Unauthenticated
                }
            })?;
        if !grant.allows(&scope, Permission::Manage, now) {
            return Err(Refusal::Forbidden);
        }
        // Profile decisions (and every trusted handler callback) follow auth.
        let profile = route::profile(&parts.uri, &address)?;
        Ok(AuthorizedHead {
            namespace: self.config.namespace.clone(),
            address,
            scope,
            method: parts.method.clone(),
            profile,
        })
    }

    async fn dispatch<B>(&self, request: Request<B>, deadline: Instant) -> Result<Reply, Refusal>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: Send,
    {
        if Instant::now() >= deadline {
            return Err(Refusal::Deadline);
        }
        let (parts, mut body) = request.into_parts();
        let head = self.authorize(&parts)?;
        let limits = self.config.limits;
        let header_bytes: usize = parts
            .headers
            .iter()
            .map(|(name, value)| name.as_str().len() + value.len() + 4)
            .sum();
        if parts.headers.len() > limits.headers || header_bytes > limits.header_bytes {
            return Err(Refusal::Headers);
        }
        if parts.headers.get_all(header::HOST).iter().count() != 1
            || parts.headers.contains_key(header::UPGRADE)
            || parts.headers.contains_key(header::EXPECT)
        {
            return Err(Refusal::Invalid);
        }
        if [
            header::CONTENT_TYPE,
            header::CONTENT_LENGTH,
            header::TRANSFER_ENCODING,
        ]
        .iter()
        .any(|name| parts.headers.get_all(name).iter().count() > 1)
            || (parts.headers.contains_key(header::CONTENT_LENGTH)
                && parts.headers.contains_key(header::TRANSFER_ENCODING))
        {
            return Err(Refusal::Invalid);
        }
        if parts.headers.contains_key(header::CONTENT_ENCODING) {
            return Err(Refusal::Encoding);
        }
        if Instant::now() >= deadline {
            return Err(Refusal::Deadline);
        }
        let mode = self.handler.select(&head)?;
        let content_length = match parts.headers.get(header::CONTENT_LENGTH) {
            Some(value) => Some(
                value
                    .to_str()
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or(Refusal::Invalid)?,
            ),
            None => None,
        };
        if content_length.is_some_and(|length| length > limits.body_bytes as u64) {
            return Err(Refusal::TooLarge);
        }
        let transfer = parts.headers.get(header::TRANSFER_ENCODING);
        if transfer.is_some_and(|value| value.as_bytes() != b"chunked") {
            return Err(Refusal::Encoding);
        }
        if matches!(mode, BodyMode::Empty)
            && (content_length.unwrap_or(0) != 0 || transfer.is_some())
        {
            return Err(Refusal::Encoding);
        }
        if matches!(mode, BodyMode::Bytes) {
            let content_type = parts
                .headers
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .ok_or(Refusal::Encoding)?;
            if ![
                "application/atom+xml",
                "application/atom+xml; charset=utf-8",
                "application/atom+xml;charset=utf-8",
            ]
            .iter()
            .any(|allowed| content_type.eq_ignore_ascii_case(allowed))
            {
                return Err(Refusal::Encoding);
            }
        }
        let body_deadline = deadline.min(Instant::now() + limits.body_timeout);
        if Instant::now() >= body_deadline {
            return Err(Refusal::BodyTimeout);
        }
        let bytes = tokio::time::timeout_at(body_deadline, async {
            let mut bytes = Vec::new();
            if matches!(mode, BodyMode::Bytes) {
                while let Some(frame) = body.frame().await {
                    if Instant::now() >= body_deadline {
                        return Err(Refusal::BodyTimeout);
                    }
                    let frame = frame.map_err(|_| Refusal::Invalid)?;
                    let data = frame.into_data().map_err(|_| Refusal::Encoding)?;
                    if data.len() > limits.body_bytes.saturating_sub(bytes.len()) {
                        return Err(Refusal::TooLarge);
                    }
                    bytes.extend_from_slice(&data);
                }
                std::str::from_utf8(&bytes).map_err(|_| Refusal::Encoding)?;
            }
            Ok(bytes)
        })
        .await
        .map_err(|_| Refusal::BodyTimeout)??;
        // A scheduling delay cannot start a fresh mutation after expiry. Once
        // handle begins, however, its original must run through actual completion.
        if Instant::now() >= deadline {
            return Err(Refusal::Deadline);
        }
        let reply = self
            .handler
            .handle(AuthorizedRequest { head, body: bytes })
            .await?;
        if reply.bytes.len() > limits.reply_bytes {
            return Err(Refusal::Internal);
        }
        Ok(reply)
    }

    async fn serve(
        &self,
        request: Request<hyper::body::Incoming>,
        deadline: Instant,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let (send, receive) = oneshot::channel();
        {
            let admission = self.admission.lock().expect("Atom admission");
            if admission.retired {
                return Ok(response(Err(Refusal::Retiring)));
            }
            let permit = match admission.permits.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => return Ok(response(Err(Refusal::Busy))),
            };
            let boundary = self.clone();
            admission.requests.spawn_retained(
                async move {
                    let result = boundary.dispatch(request, deadline).await;
                    let _ = send.send(result);
                },
                Some(permit),
            );
        }
        let result = match tokio::time::timeout_at(deadline, receive).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Refusal::Internal),
            Err(_) => Err(Refusal::Deadline),
        };
        Ok(response(if Instant::now() >= deadline {
            Err(Refusal::Deadline)
        } else {
            result
        }))
    }
}

fn response(result: Result<Reply, Refusal>) -> Response<Full<Bytes>> {
    let (status, bytes) = match result {
        Ok(reply) => (reply.status, Bytes::from(reply.bytes)),
        Err(error) => (error.status(), Bytes::from_static(error.message())),
    };
    let mut response = Response::new(Full::new(bytes.clone()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONNECTION,
        header::HeaderValue::from_static("close"),
    );
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        header::HeaderValue::from(bytes.len()),
    );
    if status == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            header::HeaderValue::from_static("Bearer"),
        );
    }
    response
}

#[cfg(test)]
mod tests;
