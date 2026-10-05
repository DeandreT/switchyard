//! Explicit opt-in route tests; listener joins do not cover tonic descendants.
use admin_api::v1::{
    ClockReadinessResponse, GetClockReadinessRequest, GetEntityRequest, ListRulesRequest,
    entity_service_client::EntityServiceClient,
    maintenance_service_client::MaintenanceServiceClient,
    maintenance_service_server::MaintenanceService, rule_service_client::RuleServiceClient,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{
    Command, CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine, Timestamp,
};
use hmac::{Hmac, Mac};
use prost::Message;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{
    Broker, Clock, LocalProposer, ManualClock, NativeAdminError, NativeAdminListener,
    NativeAdminService,
};
use sha2::Sha256;
use std::{
    error::Error,
    future::Future,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Waker},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use storage::{MemoryStore, StateStore, StorageError, StoreSnapshot, WriteBatch};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};
use tonic::{
    Code, Request,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use url::form_urlencoded::byte_serialize;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type ListenerJoin = Result<Result<(), NativeAdminError>, tokio::task::JoinError>;
const WAIT: Duration = Duration::from_secs(5);
const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "maintenance-transport-test-secret";

#[derive(Clone, Default)]
struct ObservedStore {
    memory: MemoryStore,
    gets: Arc<AtomicUsize>,
    applies: Arc<AtomicUsize>,
    failed: Arc<AtomicBool>,
}
impl StateStore for ObservedStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        if self.failed.load(Ordering::SeqCst) && key == [0] {
            return Err(StorageError::Backend {
                operation: "test read",
                detail: "private transport backend detail".into(),
            });
        }
        self.memory.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.memory.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.memory.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        self.memory.scan_from(prefix, start, limit)
    }
}

#[derive(Clone)]
struct CountedClock {
    manual: ManualClock,
    calls: Arc<AtomicUsize>,
    gate: Arc<(Mutex<(bool, bool)>, Condvar)>,
    entered: flume::Sender<()>,
    observed: flume::Receiver<()>,
}
impl CountedClock {
    fn new() -> Self {
        let (entered, observed) = flume::bounded(1);
        Self {
            manual: ManualClock::at(1_000),
            calls: Arc::new(AtomicUsize::new(0)),
            gate: Arc::new((Mutex::new((false, false)), Condvar::new())),
            entered,
            observed,
        }
    }
    fn arm(&self) {
        *self.gate.0.lock().expect("clock gate") = (true, false);
    }
    fn release(&self) {
        self.gate.0.lock().expect("clock gate").1 = true;
        self.gate.1.notify_all();
    }
}
impl Clock for CountedClock {
    fn now(&self) -> Timestamp {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut gate = self.gate.0.lock().expect("clock gate");
        if gate.0 {
            gate.0 = false;
            let _ = self.entered.try_send(());
            gate = self
                .gate
                .1
                .wait_timeout_while(gate, WAIT * 2, |value| !value.1)
                .expect("clock wait")
                .0;
        }
        drop(gate);
        self.manual.now()
    }
}

fn epoch() -> TestResult<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}
fn policy() -> TestResult<SharedAccessPolicy> {
    let namespace = ResourceScope::namespace(HOST)?;
    Ok(SharedAccessPolicy::new([
        SharedAccessRule::new(
            "manage",
            namespace.clone(),
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::MANAGE,
        )?,
        SharedAccessRule::new(
            "send",
            namespace.clone(),
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::SEND,
        )?,
        SharedAccessRule::new(
            "listen",
            namespace,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::LISTEN,
        )?,
        SharedAccessRule::new(
            "entity",
            ResourceScope::entity(HOST, "orders")?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::MANAGE,
        )?,
    ])?)
}
fn token(rule: &str, resource: &str, expiry: u64) -> TestResult<String> {
    let resource = byte_serialize(resource.as_bytes()).collect::<String>();
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes())?;
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let signature = byte_serialize(signature.as_bytes()).collect::<String>();
    Ok(format!(
        "SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={rule}"
    ))
}
fn request(
    namespace: &str,
    authorization: Option<&str>,
) -> TestResult<Request<GetClockReadinessRequest>> {
    let mut request = Request::new(GetClockReadinessRequest {
        namespace: namespace.into(),
    });
    if let Some(token) = authorization {
        request
            .metadata_mut()
            .insert("authorization", token.parse()?);
    }
    Ok(request)
}

struct Node {
    broker: Option<Broker>,
    service: NativeAdminService,
    listener: Option<JoinHandle<Result<(), NativeAdminError>>>,
    endpoint: String,
    certificate: Option<String>,
    clock: CountedClock,
    store: ObservedStore,
}
impl Node {
    async fn start(enabled: bool, tls: bool, authenticated: bool) -> TestResult<Self> {
        let store = ObservedStore::default();
        StateMachine::new(store.clone()).apply(&Command::new(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(1_000),
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ))?;
        store.gets.store(0, Ordering::SeqCst);
        store.applies.store(0, Ordering::SeqCst);
        let clock = CountedClock::new();
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let mut service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?);
        if enabled {
            service = service.with_development_maintenance_readiness();
        }
        if authenticated {
            service = service.with_shared_access_policy(policy()?, HOST)?;
        }
        let mut admin = NativeAdminListener::new(service.clone());
        let certificate = if tls {
            let CertifiedKey { cert, key_pair } =
                generate_simple_self_signed(vec!["localhost".into()])?;
            let pem = cert.pem();
            admin = admin.with_tls(pem.as_bytes(), key_pair.serialize_pem().as_bytes())?;
            Some(pem)
        } else {
            None
        };
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!(
            "{}://{}",
            if tls { "https" } else { "http" },
            socket.local_addr()?
        );
        let listener = tokio::spawn(admin.serve(socket));
        Ok(Self {
            broker: Some(broker),
            service,
            listener: Some(listener),
            endpoint,
            certificate,
            clock,
            store,
        })
    }
    async fn channel(&self) -> TestResult<Channel> {
        let mut endpoint = Endpoint::from_shared(self.endpoint.clone())?
            .connect_timeout(WAIT)
            .timeout(WAIT);
        if let Some(pem) = &self.certificate {
            endpoint = endpoint.tls_config(
                ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(Certificate::from_pem(pem)),
            )?;
        }
        Ok(timeout(WAIT, endpoint.connect()).await??)
    }
    async fn finish(mut self) -> ListenerJoin {
        self.clock.release();
        let listener = self.listener.take().expect("original listener token");
        listener.abort();
        let joined = listener.await;
        drop(self.broker.take());
        joined
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        self.clock.release();
        if let Some(listener) = &self.listener {
            listener.abort();
        }
        // Tested observation paths use finish(). Early setup Drop makes no join claim.
    }
}

fn listener_cancelled(joined: &ListenerJoin) -> bool {
    matches!(joined, Err(error) if error.is_cancelled())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_service_is_absent_without_explicit_enablement() -> TestResult {
    let node = Node::start(false, false, false).await?;
    let observed = async {
        let channel = node.channel().await?;
        let mut maintenance = MaintenanceServiceClient::new(channel.clone());
        let refused = maintenance
            .get_clock_readiness(request("tenant", None)?)
            .await
            .err()
            .ok_or("expected absent route")?;
        let mut entity = EntityServiceClient::new(channel.clone());
        let found = entity
            .get_entity(GetEntityRequest {
                namespace: "tenant".into(),
                path: "orders".into(),
            })
            .await?
            .into_inner();
        let mut rules = RuleServiceClient::new(channel);
        let rule_error = rules
            .list_rules(ListRulesRequest {
                namespace: "tenant".into(),
                subscription_path: "missing/Subscriptions/sub".into(),
                include_actions: false,
            })
            .await
            .err()
            .ok_or("missing subscription")?;
        Ok::<_, Box<dyn Error>>((
            refused.code(),
            found.path,
            rule_error.code(),
            node.clock.calls.load(Ordering::SeqCst),
        ))
    }
    .await;
    let cleanup = node.finish().await;
    let (code, path, rule_code, calls) = observed?;
    assert!(listener_cancelled(&cleanup));
    assert_eq!(code, Code::Unimplemented);
    assert_eq!(path, "orders");
    assert_ne!(rule_code, Code::Unimplemented);
    assert_eq!(calls, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_development_http_inherits_policy_free_namespace_scope() -> TestResult {
    let node = Node::start(true, false, false).await?;
    let observed = async {
        let mut client = MaintenanceServiceClient::new(node.channel().await?);
        let wrong = client
            .get_clock_readiness(request("other", None)?)
            .await
            .err()
            .ok_or("foreign namespace")?;
        let before = node.clock.calls.load(Ordering::SeqCst);
        let ready = client
            .get_clock_readiness(request("tenant", None)?)
            .await?
            .into_inner();
        Ok::<_, Box<dyn Error>>((
            wrong.code(),
            before,
            ready.state,
            node.clock.calls.load(Ordering::SeqCst),
            node.store.applies.load(Ordering::SeqCst),
        ))
    }
    .await;
    let cleanup = node.finish().await;
    let (code, before, state, calls, applies) = observed?;
    assert!(listener_cancelled(&cleanup));
    assert_eq!(code, Code::PermissionDenied);
    assert_eq!(before, 0);
    assert_eq!(state, 1);
    assert_eq!((calls, applies), (1, 0));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namespace_manage_checks_precede_the_owner_probe() -> TestResult {
    let node = Node::start(true, true, true).await?;
    let observed = async {
        let now = epoch()?;
        let namespace = format!("amqps://{HOST}");
        let manage = token("manage", &namespace, now + 300)?;
        let mut client = MaintenanceServiceClient::new(node.channel().await?);
        let attempts = [
            None,
            Some("invalid-private-token".into()),
            Some(token("manage", &namespace, now - 1)?),
            Some(token("send", &namespace, now + 300)?),
            Some(token("listen", &namespace, now + 300)?),
            Some(token("entity", &format!("{namespace}/orders"), now + 300)?),
        ];
        let mut denied = Vec::new();
        for authorization in &attempts {
            let status = client
                .get_clock_readiness(request("tenant", authorization.as_deref())?)
                .await
                .err()
                .ok_or("expected auth denial")?;
            denied.push((
                status.code(),
                node.clock.calls.load(Ordering::SeqCst),
                node.store.gets.load(Ordering::SeqCst),
            ));
        }
        let foreign = client
            .get_clock_readiness(request("other", Some(&manage))?)
            .await
            .err()
            .ok_or("foreign namespace")?;
        let before = (
            node.clock.calls.load(Ordering::SeqCst),
            node.store.gets.load(Ordering::SeqCst),
        );
        let ready = client
            .get_clock_readiness(request("tenant", Some(&manage))?)
            .await?
            .into_inner();
        Ok::<_, Box<dyn Error>>((
            denied,
            foreign.code(),
            before,
            ready.state,
            node.store.applies.load(Ordering::SeqCst),
        ))
    }
    .await;
    let cleanup = node.finish().await;
    let (denied, foreign, before, ready, applies) = observed?;
    assert!(listener_cancelled(&cleanup));
    assert_eq!(denied.len(), 6);
    for (index, (code, calls, gets)) in denied.into_iter().enumerate() {
        assert_eq!(
            code,
            if index < 3 {
                Code::Unauthenticated
            } else {
                Code::PermissionDenied
            }
        );
        assert_eq!((calls, gets), (0, 0));
    }
    assert_eq!(foreign, Code::PermissionDenied);
    assert_eq!(before, (0, 0));
    assert_eq!(ready, 1);
    assert_eq!(applies, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_tls_rpc_returns_only_fixed_bounded_states() -> TestResult {
    let mut node = Node::start(true, true, true).await?;
    let observed = async {
        let authorization = token("manage", &format!("amqps://{HOST}"), epoch()? + 300)?;
        let mut client = MaintenanceServiceClient::new(node.channel().await?);
        let mut responses = vec![
            client
                .get_clock_readiness(request("tenant", Some(&authorization))?)
                .await?
                .into_inner(),
        ];
        node.clock.manual.set(0);
        responses.push(
            client
                .get_clock_readiness(request("tenant", Some(&authorization))?)
                .await?
                .into_inner(),
        );
        node.store.failed.store(true, Ordering::SeqCst);
        responses.push(
            client
                .get_clock_readiness(request("tenant", Some(&authorization))?)
                .await?
                .into_inner(),
        );
        node.clock.release();
        drop(node.broker.take());
        responses.push(
            client
                .get_clock_readiness(request("tenant", Some(&authorization))?)
                .await?
                .into_inner(),
        );
        Ok::<_, Box<dyn Error>>((responses, node.store.applies.load(Ordering::SeqCst)))
    }
    .await;
    let cleanup = node.finish().await;
    let (responses, applies) = observed?;
    assert!(listener_cancelled(&cleanup));
    assert_eq!(
        responses
            .iter()
            .map(|response| response.state)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    for response in responses {
        assert!(response.encoded_len() <= 2);
        assert_eq!(response.encode_to_vec(), [8, response.state as u8]);
        assert_eq!(
            ClockReadinessResponse::decode(response.encode_to_vec().as_slice())?,
            response
        );
    }
    assert_eq!(applies, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_handler_cancellation_refunds_admission_without_mutation() -> TestResult {
    let node = Node::start(true, true, true).await?;
    let observed = async {
        let authorization = token("manage", &format!("amqps://{HOST}"), epoch()? + 300)?;
        let before = node.store.memory.snapshot()?;
        node.clock.arm();
        let mut handlers = Vec::new();
        let mut pending = Vec::new();
        for _ in 0..128 {
            let mut handler = Box::pin(
                node.service
                    .get_clock_readiness(request("tenant", Some(&authorization))?),
            );
            pending.push(
                handler
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending(),
            );
            handlers.push(handler);
        }
        node.clock.observed.recv_timeout(WAIT)?;
        let saturated = node
            .service
            .get_clock_readiness(request("tenant", Some(&authorization))?)
            .await
            .err()
            .ok_or("full handler admission")?;
        drop(handlers.pop());
        let mut replacement = Box::pin(
            node.service
                .get_clock_readiness(request("tenant", Some(&authorization))?),
        );
        let refunded = replacement
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();
        drop(replacement);
        drop(handlers);
        node.clock.release();
        node.service
            .get_clock_readiness(request("tenant", Some(&authorization))?)
            .await?;
        Ok::<_, Box<dyn Error>>((
            pending,
            saturated.code(),
            refunded,
            before,
            node.store.memory.snapshot()?,
            node.store.applies.load(Ordering::SeqCst),
            node.clock.calls.load(Ordering::SeqCst),
        ))
    }
    .await;
    let cleanup = node.finish().await;
    let (pending, code, refunded, before, after, applies, calls) = observed?;
    assert!(listener_cancelled(&cleanup));
    assert!(pending.into_iter().all(|value| value));
    assert_eq!(code, Code::ResourceExhausted);
    assert!(refunded);
    assert_eq!(before, after);
    assert_eq!(applies, 0);
    assert_eq!(calls, 130);
    Ok(())
}
