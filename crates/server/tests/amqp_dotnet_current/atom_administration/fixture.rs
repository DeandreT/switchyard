use std::{
    fs,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{NamespaceName, StateMachine, Timestamp};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use rustls::{ServerConfig, pki_types::PrivatePkcs8KeyDer};
use server::{AtomAdminListener, Broker, BrokerHandle, Clock, LocalProposer};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle, time::timeout};

use super::{KEY, TestResult};

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Effects {
    pub(super) reads: usize,
    pub(super) applies: usize,
    pub(super) clock: usize,
}

#[derive(Clone)]
struct ProbeStore<S> {
    inner: S,
    reads: Arc<AtomicUsize>,
    applies: Arc<AtomicUsize>,
    batches: Arc<Mutex<Vec<WriteBatch>>>,
}

impl<S: StateStore> StateStore for ProbeStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.batches
            .lock()
            .expect("SDK fixture batch observations")
            .push(batch.clone());
        self.inner.apply(batch)
    }
}

#[derive(Clone)]
struct ProbeClock(Arc<AtomicUsize>);
impl Clock for ProbeClock {
    fn now(&self) -> Timestamp {
        self.0.fetch_add(1, Ordering::SeqCst);
        Timestamp::from_millis(1_000)
    }
}

fn certificate() -> TestResult<(ServerConfig, String)> {
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "Switchyard Atom test CA");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate()?;
    let ca = ca_params.self_signed(&ca_key)?;
    let mut leaf_params = CertificateParams::new(vec!["localhost".into()])?;
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    leaf_params.use_authority_key_identifier_extension = true;
    leaf_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let leaf_key = KeyPair::generate()?;
    let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key)?;
    let tls =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
            .with_no_client_auth()
            .with_single_cert(
                vec![leaf.der().clone(), ca.der().clone()],
                PrivatePkcs8KeyDer::from(leaf_key.serialize_der()).into(),
            )?;
    Ok((tls, ca.pem()))
}

pub(super) struct Fixture<P: StoreProvider> {
    pub(super) endpoint: String,
    pub(super) wrong_name_endpoint: String,
    pub(super) ca_file: std::path::PathBuf,
    pub(super) wrong_ca_file: std::path::PathBuf,
    pub(super) namespace: NamespaceName,
    broker: Option<Broker>,
    listener: Option<JoinHandle<Result<(), server::AtomAdminError>>>,
    shutdown: Option<oneshot::Sender<()>>,
    store: Option<ProbeStore<P::Store>>,
    clock: ProbeClock,
    provider: Option<P>,
    _certificates: tempfile::TempDir,
}

impl<P: StoreProvider> Fixture<P> {
    pub(super) async fn start(provider: P, namespace: NamespaceName) -> TestResult<Self> {
        let (tls, ca_pem) = certificate()?;
        let (_, wrong_ca_pem) = certificate()?;
        let certificates = tempfile::TempDir::new()?;
        let ca_file = certificates.path().join("trusted-ca.pem");
        let wrong_ca_file = certificates.path().join("untrusted-ca.pem");
        fs::write(&ca_file, ca_pem)?;
        fs::write(&wrong_ca_file, wrong_ca_pem)?;
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let port = socket.local_addr()?.port();
        let scope = ResourceScope::namespace("localhost")?;
        let policy = SharedAccessPolicy::new([
            SharedAccessRule::new(
                "manage",
                scope.clone(),
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::MANAGE,
            )?,
            SharedAccessRule::new(
                "send",
                scope.clone(),
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::SEND,
            )?,
        ])?;
        let store = ProbeStore {
            inner: provider.open()?,
            reads: Arc::default(),
            applies: Arc::default(),
            batches: Arc::default(),
        };
        let clock = ProbeClock(Arc::default());
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let admin = AtomAdminListener::new(broker.handle(), namespace.clone(), policy, scope, tls)?;
        let (shutdown, stopped) = oneshot::channel();
        let listener = tokio::spawn(admin.serve_until(socket, async move {
            let _ = stopped.await;
        }));
        Ok(Self {
            endpoint: format!("https://localhost:{port}/"),
            wrong_name_endpoint: format!("https://127.0.0.1:{port}/"),
            ca_file,
            wrong_ca_file,
            namespace,
            broker: Some(broker),
            listener: Some(listener),
            shutdown: Some(shutdown),
            store: Some(store),
            clock,
            provider: Some(provider),
            _certificates: certificates,
        })
    }

    pub(super) fn handle(&self) -> BrokerHandle {
        self.broker.as_ref().expect("live SDK broker").handle()
    }
    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.as_ref().expect("SDK store").inner.snapshot()?)
    }
    pub(super) fn machine(&self) -> StateMachine<P::Store> {
        StateMachine::new(self.store.as_ref().expect("SDK store").inner.clone())
    }
    pub(super) fn effects(&self) -> Effects {
        let store = self.store.as_ref().expect("SDK store");
        Effects {
            reads: store.reads.load(Ordering::SeqCst),
            applies: store.applies.load(Ordering::SeqCst),
            clock: self.clock.0.load(Ordering::SeqCst),
        }
    }
    pub(super) fn batches_since(&self, effects: Effects) -> Vec<WriteBatch> {
        self.store
            .as_ref()
            .expect("SDK store")
            .batches
            .lock()
            .expect("SDK fixture batch observations")[effects.applies..]
            .to_vec()
    }

    pub(super) async fn stop(&mut self) -> TestResult {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let mut failure: Option<Box<dyn std::error::Error>> = None;
        if let Some(mut listener) = self.listener.take() {
            match timeout(DEADLINE, &mut listener).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => failure = Some(Box::new(error)),
                Ok(Err(error)) => failure = Some(Box::new(error)),
                Err(_) => {
                    listener.abort();
                    let joined = timeout(DEADLINE, &mut listener).await;
                    failure = Some(if joined.is_err() {
                        "original SDK listener failed to join after abort".into()
                    } else {
                        "SDK listener shutdown deadline expired".into()
                    });
                }
            }
        }
        // Broker Drop joins the original owner after the listener releases its handles.
        drop(self.broker.take());
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(super) fn into_stopped_parts(mut self) -> (P, P::Store, NamespaceName) {
        assert!(self.broker.is_none() && self.listener.is_none());
        (
            self.provider.take().expect("SDK provider"),
            self.store.take().expect("SDK store").inner,
            self.namespace.clone(),
        )
    }
}

impl<P: StoreProvider> Drop for Fixture<P> {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(listener) = &self.listener {
            listener.abort();
        }
        drop(self.broker.take());
    }
}
