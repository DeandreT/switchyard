//! Positive witnesses for the concrete owner, wire, and broker commit tests.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use domain::{CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueConfig, StateMachine};
use storage::{
    FjallStore, Key, MemoryStore, Mutation, StateStore, StorageError, StoreSnapshot, Value,
    WriteBatch,
};
use tokio::{sync::Notify, task::Id, time::timeout};

use super::{AmqpListener, AmqpListenerService, Stage};
use crate::{Broker, BrokerRejection};

pub(super) const WAIT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Point {
    Accepted,
    Prepared,
    ReceiptCached,
    Adopted,
    Reaped,
}

#[derive(Clone, Debug)]
pub(super) struct Facts {
    pub(super) point: Point,
    pub(super) accepted: bool,
    pub(super) receipt: Option<Id>,
    pub(super) pending: Vec<Id>,
    pub(super) reaped: Option<Id>,
    pub(super) finished: Vec<Id>,
}

#[derive(Default)]
struct Observations {
    facts: Vec<Facts>,
    pending: Vec<(Id, Stage)>,
    native: Vec<Id>,
    joined: Vec<Id>,
    hold: Option<Point>,
    released: bool,
    panic: Option<(Point, Arc<String>)>,
    native_panic: Option<Arc<String>>,
}

#[derive(Default)]
pub(super) struct Observer {
    state: Mutex<Observations>,
    changed: Notify,
}

tokio::task_local! { pub(super) static OBSERVER: Arc<Observer>; }

impl Observer {
    pub(super) fn hold(&self, point: Point) {
        let mut state = self.state.lock().unwrap();
        state.hold = Some(point);
        state.released = false;
    }

    pub(super) fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_waiters();
    }

    pub(super) fn panic_at(&self, point: Point, payload: Arc<String>) {
        self.state.lock().unwrap().panic = Some((point, payload));
    }

    pub(super) fn panic_after_native(&self, payload: Arc<String>) {
        self.state.lock().unwrap().native_panic = Some(payload);
    }

    async fn wait(&self, predicate: impl Fn(&Observations) -> bool) {
        timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if predicate(&self.state.lock().unwrap()) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("positive owner witness");
    }

    pub(super) async fn reached(&self, point: Point) {
        self.wait(|state| state.facts.iter().any(|facts| facts.point == point))
            .await;
    }

    pub(super) async fn stalled(&self, stage: Stage) {
        self.wait(|state| state.pending.iter().any(|(_, reached)| *reached == stage))
            .await;
    }

    pub(super) async fn native(&self) {
        self.wait(|state| !state.native.is_empty()).await;
    }

    pub(super) async fn joined(&self, id: Id) {
        self.wait(|state| state.joined.contains(&id)).await;
    }

    pub(super) fn facts(&self, point: Point) -> Facts {
        self.state
            .lock()
            .unwrap()
            .facts
            .iter()
            .rev()
            .find(|facts| facts.point == point)
            .unwrap()
            .clone()
    }

    pub(super) fn stalled_id(&self, stage: Stage) -> Id {
        self.state
            .lock()
            .unwrap()
            .pending
            .iter()
            .find(|(_, reached)| *reached == stage)
            .unwrap()
            .0
    }

    pub(super) fn native_ids(&self) -> Vec<Id> {
        self.state.lock().unwrap().native.clone()
    }
}

pub(super) async fn checkpoint(facts: Facts) {
    let Ok(observer) = OBSERVER.try_with(Arc::clone) else {
        return;
    };
    let point = facts.point;
    let payload = {
        let mut state = observer.state.lock().unwrap();
        state.facts.push(facts);
        state
            .panic
            .as_ref()
            .filter(|(at, _)| *at == point)
            .map(|(_, payload)| Arc::clone(payload))
    };
    observer.changed.notify_waiters();
    if let Some(payload) = payload {
        std::panic::panic_any(payload);
    }
    observer
        .wait(|state| state.hold != Some(point) || state.released)
        .await;
}

pub(super) struct ObservedPending<F> {
    future: Pin<Box<F>>,
    stage: Stage,
    reported: bool,
}

impl<F> ObservedPending<F> {
    pub(super) fn new(future: F, stage: Stage) -> Self {
        Self {
            future: Box::pin(future),
            stage,
            reported: false,
        }
    }
}

impl<F: Future> Future for ObservedPending<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self.future.as_mut().poll(context);
        if result.is_pending() && !self.reported {
            self.reported = true;
            let stage = self.stage;
            let _ = OBSERVER.try_with(|observer| {
                observer
                    .state
                    .lock()
                    .unwrap()
                    .pending
                    .push((tokio::task::id(), stage));
                observer.changed.notify_waiters();
            });
        }
        result
    }
}

pub(super) fn native_accepted() {
    let payload = OBSERVER
        .try_with(|observer| {
            let mut state = observer.state.lock().unwrap();
            state.native.push(tokio::task::id());
            let payload = state.native_panic.clone();
            observer.changed.notify_waiters();
            payload
        })
        .ok()
        .flatten();
    if let Some(payload) = payload {
        std::panic::panic_any(payload);
    }
}

pub(super) fn joined(id: Id) {
    let _ = OBSERVER.try_with(|observer| {
        observer.state.lock().unwrap().joined.push(id);
        observer.changed.notify_waiters();
    });
}

pub(super) async fn drive<B: Broker, F: Future>(
    service: &mut AmqpListenerService<B>,
    observer: Arc<Observer>,
    scenario: F,
) -> F::Output {
    OBSERVER
        .scope(observer, async {
            tokio::select! {
                biased;
                result = scenario => result,
                () = service.serve() => panic!("admission ended before scenario witness"),
            }
        })
        .await
}

#[derive(Clone, Default)]
pub(super) struct QuietBroker {
    pub(super) calls: Arc<Mutex<Vec<CommandKind>>>,
}

impl Broker for QuietBroker {
    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.calls.lock().unwrap().push(kind);
        Err(BrokerRejection::Unavailable(
            "test broker has no entities".to_owned(),
        ))
    }
    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

pub(super) async fn owner<B: Broker>(broker: B) -> (AmqpListenerService<B>, std::net::SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    (
        AmqpListener::new(broker, NamespaceName::new("tenant").unwrap()).into_tcp_service(listener),
        address,
    )
}

#[derive(Default)]
struct CommitState {
    key: Vec<u8>,
    before: bool,
    after: bool,
    allow_before: bool,
    allow_after: bool,
    commits: usize,
}

#[derive(Default)]
pub(super) struct CommitGate {
    state: Mutex<CommitState>,
    released: Condvar,
    changed: Notify,
}

impl CommitGate {
    pub(super) fn arm(&self, key: Vec<u8>) {
        *self.state.lock().unwrap() = CommitState {
            key,
            ..CommitState::default()
        };
    }

    fn matches(&self, batch: &WriteBatch) -> bool {
        let state = self.state.lock().unwrap();
        !state.key.is_empty()
            && batch
                .mutations()
                .iter()
                .any(|mutation| matches!(mutation, Mutation::Put { key, .. } if *key == state.key))
    }

    fn pause(&self, after: bool) {
        let mut state = self.state.lock().unwrap();
        if after {
            state.after = true;
            state.commits += 1;
        } else {
            state.before = true;
        }
        self.changed.notify_waiters();
        while !(if after {
            state.allow_after
        } else {
            state.allow_before
        }) {
            state = self.released.wait(state).unwrap();
        }
    }

    pub(super) async fn reached(&self, after: bool) {
        timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let reached = {
                    let state = self.state.lock().unwrap();
                    if after { state.after } else { state.before }
                };
                if reached {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("real Send store apply reached commit boundary");
    }

    pub(super) fn release(&self, after: bool) {
        let mut state = self.state.lock().unwrap();
        if after {
            state.allow_after = true;
        } else {
            state.allow_before = true;
        }
        self.released.notify_all();
    }

    fn release_all(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.allow_before = true;
        state.allow_after = true;
        self.released.notify_all();
    }

    pub(super) fn commits(&self) -> usize {
        self.state.lock().unwrap().commits
    }
}

#[derive(Clone)]
enum Backend {
    Memory(MemoryStore),
    Durable(FjallStore),
}

#[derive(Clone)]
pub(super) struct GatedStore {
    inner: Backend,
    gate: Arc<CommitGate>,
}

impl StateStore for GatedStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        match &self.inner {
            Backend::Memory(store) => store.get(key),
            Backend::Durable(store) => store.get(key),
        }
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let gated = self.gate.matches(&batch);
        if gated {
            self.gate.pause(false);
        }
        match &self.inner {
            Backend::Memory(store) => store.apply(batch)?,
            Backend::Durable(store) => store.apply(batch)?,
        }
        if gated {
            self.gate.pause(true);
        }
        Ok(())
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        match &self.inner {
            Backend::Memory(store) => store.snapshot(),
            Backend::Durable(store) => store.snapshot(),
        }
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        match &self.inner {
            Backend::Memory(store) => store.scan_from(prefix, start, limit),
            Backend::Durable(store) => store.scan_from(prefix, start, limit),
        }
    }
}

#[derive(Clone)]
pub(super) struct Submission {
    pub(super) worker: Id,
    pub(super) kind: CommandKind,
    pub(super) returned: bool,
}

#[derive(Clone)]
pub(super) struct ActualBroker {
    handle: server::BrokerHandle,
    log: Arc<Mutex<Vec<Submission>>>,
}

impl Broker for ActualBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let worker = tokio::task::id();
        self.log.lock().unwrap().push(Submission {
            worker,
            kind: kind.clone(),
            returned: false,
        });
        let result = self.handle.submit(namespace, entity, kind.clone()).await;
        self.log.lock().unwrap().push(Submission {
            worker,
            kind,
            returned: true,
        });
        result.map_err(|error| match error {
            server::SubmitError::Propose(server::ProposeError::Broker(error)) => {
                BrokerRejection::Refused(error)
            }
            other => BrokerRejection::Unavailable(other.to_string()),
        })
    }
    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

pub(super) struct Actor {
    owner: Option<server::Broker>,
    pub(super) broker: Option<ActualBroker>,
    store: Option<GatedStore>,
    memory: Option<MemoryStore>,
    directory: Option<tempfile::TempDir>,
    pub(super) gate: Arc<CommitGate>,
    pub(super) log: Arc<Mutex<Vec<Submission>>>,
    pub(super) namespace: NamespaceName,
    pub(super) entity: EntityPath,
}

impl Actor {
    pub(super) fn new(durable: bool) -> Self {
        let gate = Arc::new(CommitGate::default());
        let directory = durable.then(|| tempfile::tempdir().unwrap());
        let memory = (!durable).then(MemoryStore::default);
        let inner = match &directory {
            Some(directory) => Backend::Durable(FjallStore::open(directory.path()).unwrap()),
            None => Backend::Memory(memory.as_ref().unwrap().clone()),
        };
        let store = GatedStore {
            inner,
            gate: Arc::clone(&gate),
        };
        let owner = server::Broker::spawn(server::LocalProposer::new(
            StateMachine::new(store.clone()),
            server::ManualClock::at(1_000),
        ));
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = ActualBroker {
            handle: owner.handle(),
            log: Arc::clone(&log),
        };
        let namespace = NamespaceName::new("tenant").unwrap();
        let entity = EntityPath::new("orders").unwrap();
        assert_eq!(
            broker
                .handle
                .submit_blocking(
                    namespace.clone(),
                    entity.clone(),
                    CommandKind::CreateQueue {
                        config: QueueConfig::default()
                    }
                )
                .unwrap(),
            CommandOutcome::QueueCreated
        );
        Self {
            owner: Some(owner),
            broker: Some(broker),
            store: Some(store),
            memory,
            directory,
            gate,
            log,
            namespace,
            entity,
        }
    }

    pub(super) fn key(&self) -> Vec<u8> {
        domain::keys::message(
            &self.namespace,
            &self.entity,
            domain::SequenceNumber::new(1),
        )
    }
    pub(super) fn store(&self) -> &GatedStore {
        self.store.as_ref().unwrap()
    }

    pub(super) fn reopen(&mut self) {
        self.gate.release_all();
        drop(self.broker.take());
        drop(self.owner.take());
        drop(self.store.take());
        let inner = match &self.directory {
            Some(directory) => Backend::Durable(FjallStore::open(directory.path()).unwrap()),
            None => Backend::Memory(self.memory.as_ref().unwrap().clone()),
        };
        self.store = Some(GatedStore {
            inner,
            gate: Arc::clone(&self.gate),
        });
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        self.gate.release_all();
        drop(self.broker.take());
        drop(self.owner.take());
    }
}

pub(super) async fn wss_owner<B: Broker>(
    broker: B,
) -> (AmqpListenerService<B>, std::net::SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    (
        AmqpListener::new(broker, NamespaceName::new("tenant").unwrap()).into_wss_service(listener),
        address,
    )
}

pub(super) fn wss_test_server_tls() -> rustls::ServerConfig {
    crate::tls_server_config(
        WSS_TEST_CERTIFICATE.as_bytes(),
        WSS_TEST_PRIVATE_KEY.as_bytes(),
    )
    .unwrap()
}

pub(super) fn wss_test_client_tls() -> tokio_rustls::TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in rustls_pemfile::certs(&mut std::io::Cursor::new(WSS_TEST_CERTIFICATE)) {
        roots.add(certificate.unwrap()).unwrap();
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

// Synthetic localhost-only identity. It is never used outside these controls.
const WSS_TEST_CERTIFICATE: &str = "-----BEGIN CERTIFICATE-----
MIIDSzCCAjOgAwIBAgIUIUfbpECmAHezITJMrbcNW6jsoKwwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MTAxMDA5NDEzMloYDzIxMjYw
OTE2MDk0MTMyWjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQCeRwezsxOZ7MrV85NqW5Q2FWiB5JHLzTPLpAMkEJ7g
u6obCOhBWuZCnuiZmLaH/pDrJndJu9vePYUSjJdtcWSg1NZHWzmLEvFUODYup9bP
EsGQHszp3o2337oLow0qhPztBEJBR5kzeeYeYV3GFvpPghyR9axk9v7YxikPOl3A
cRzmMF7KOKHu4doyn+a2DhqMbx0SvAdEYMimI9KhfcBTlogrLR3U9te4RnVONtLN
O5dNNzMIfXp1ZrWsuY6vW9xjM+lknCNiD/5zZ5+O++iWJdoGKQxg+DULFEOOKPdk
tQNvl4ft1RSq8EDvca5fr4jybtex+Es5RkyFj3ItJje7AgMBAAGjgZIwgY8wHQYD
VR0OBBYEFJC/fJxWYrV1jQjb13Q3WDG8SoMsMB8GA1UdIwQYMBaAFJC/fJxWYrV1
jQjb13Q3WDG8SoMsMBoGA1UdEQQTMBGCCWxvY2FsaG9zdIcEfwAAATAMBgNVHRMB
Af8EAjAAMBMGA1UdJQQMMAoGCCsGAQUFBwMBMA4GA1UdDwEB/wQEAwIFoDANBgkq
hkiG9w0BAQsFAAOCAQEAOqgIn0krYmtGzHxplSLUQF8Rkwjt2R1g4DqTSJaiduSF
+HRScpPRpJKdCpuzlMl4MFimCbsCBfhMdj555I2nUU3aZbi6HPFu3KKWrdUsdhzn
82CCsI9LP4V2kmvQW//0/SvH8XN7Z+XGquO1b8WFiRRaD7reVHk7mJCM4Te1KWgW
90f/86j2VxJlivNs1fyc169HCIYkD8ePjOcuplEc0b2fj31PTq6rO1hz8HaT4LAp
FMiMeIgTBPL0ZVTPdWNolRP4qbQ9TsAvmyF0N45iPZvObGXzZ4BLaRdzciG8PDVf
L+0LkM09zmRvlcm6WcZhiyuLBrPA5nQctTT75l8mcw==
-----END CERTIFICATE-----
";

const WSS_TEST_PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCeRwezsxOZ7MrV
85NqW5Q2FWiB5JHLzTPLpAMkEJ7gu6obCOhBWuZCnuiZmLaH/pDrJndJu9vePYUS
jJdtcWSg1NZHWzmLEvFUODYup9bPEsGQHszp3o2337oLow0qhPztBEJBR5kzeeYe
YV3GFvpPghyR9axk9v7YxikPOl3AcRzmMF7KOKHu4doyn+a2DhqMbx0SvAdEYMim
I9KhfcBTlogrLR3U9te4RnVONtLNO5dNNzMIfXp1ZrWsuY6vW9xjM+lknCNiD/5z
Z5+O++iWJdoGKQxg+DULFEOOKPdktQNvl4ft1RSq8EDvca5fr4jybtex+Es5RkyF
j3ItJje7AgMBAAECggEASetzeeX/GArCemStKuWFVTLvYYat3shB0fAR9XR7twp+
Oe0Rh4Bb/K45hI6RZPcrmF8ZJAsD9wm1TgHyMM9eB00PnDjLKZwZsFMmXujNBl3E
n7n6ypjBiCgJPCkU25BLjahJBzLYpnVeVz7y7DJMCfkCvdpGfsrqH7SAdZe1NEwD
g8svbthN8FiZPKaR6BTtU2Ybbq//tcqUZt1Ach/jjvyAOg38UtAqn83rdTY1Z1ir
/ZwNGKqiuAEcqPg/ozVfRVAMjF/iJ7VK/743gMjyqNlb7Wle86IUQYMOgwdjRM/s
CuLpBjwDngpA//XUkn21hFIlsK87cgZ1o/toSW/egQKBgQDK1Uk75zofDW1wqcxj
NVmV0eY9F9m+y0GT5gOR8U4XdIvOyOPp43Jf6e5wagCUk+RNVHUEJMtpfJxyfs9S
2Dlnc2kmROP2nHpern6tcEjwP8OEwUAaNmMRRgCYtdi2p77wLYWko41rbxaB0S1w
/OYTAhrwCGz2nOxe1Q89qDefgQKBgQDHw+xoaXFiPCDOLQu/kTmxx0YHAPk0lMmX
RjYp5tIqbhpRJcd9zIUA+as1hcoqyjRFOQoXMlo+4nfUe0uBeIEv7JCnnH1K1ukt
qMna2OCMyXSSlDXajxvZw8T5jadF844u+DD+gmw2e+Ps15v19HfWMwqh4QPELO1n
mD6yQ671OwKBgGkIc4jqnwVPfbmWo/lHuiFD9Vbzj9UmCcpdlMGxVD//IRqump8W
IbcbO1bI/Y3y24lTz9iaeR8h4R3FHvUGdQtmDzA7AlVFCHUZJQ4JFCwcyNFpBRnN
yhU6y7pIiHVlfhF/cTIlY2kbuGr29/IHc9iqAWsE4e/+qMQezmqePleBAoGAXUN+
atzXn7pcwoVLV9uwaOyVKA3vSvowB90yymtGMaMHzFzr/zJt4h+uLCXTVhjjQHI5
4MyUT4mh0mThwoeFIN+8RVSNNlSRtsx3uSHJ7FDdiNWDMw/fO9Lncm8M6l6kQg+D
qD8x0FoU9m1PFgxVqn5X9227OWqUR+pPUyDAXSMCgYAQ+VI1kqmvPBSaX1AUsGd0
8g+3BkoSVSZlSsEMAk/vqqG/o9bV2eff090sCefYNczDM9fDRShboDtrqO6UGg8S
rmqYg3OR5VjRUAMbIz5q/g1dhUn753hNEsydLU0/NiD1/twDo6jH0cmuQ/oJbR+X
T4pfboaIZXmuEC7SglvKqw==
-----END PRIVATE KEY-----
";
