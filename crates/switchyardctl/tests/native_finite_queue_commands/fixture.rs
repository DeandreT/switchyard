use std::{
    any::Any,
    error::Error,
    fmt, fs,
    future::Future,
    panic::{AssertUnwindSafe, resume_unwind},
    pin::Pin,
    process::Output,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use admin_api::v1::entity_service_server::EntityServiceServer;
use domain::{CommandKind, CommandOutcome, EntityPath, NamespaceName, StateMachine, Timestamp};
use futures_util::{FutureExt, stream};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use serde_json::Value as Json;
use server::{
    Broker, Clock, LocalProposer, ManualClock, NativeAdminError, NativeAdminListener,
    NativeAdminService,
};
use storage::{Key, MemoryStore, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    process::Command,
    task::{JoinError, JoinHandle},
    time::timeout,
};
use tonic::transport::{Identity, Server, ServerTlsConfig};

use super::{process, tokens};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Observations {
    reads: AtomicUsize,
    scans: AtomicUsize,
    attempts: AtomicUsize,
    commits: AtomicUsize,
}

#[derive(Clone)]
pub(super) struct ObservedStore {
    pub inner: Arc<MemoryStore>,
    observations: Arc<Observations>,
}

impl StateStore for ObservedStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.observations.scans.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observations.attempts.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)?;
        self.observations.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct CountingClock {
    pub manual: ManualClock,
    calls: Arc<AtomicUsize>,
}

impl Clock for CountingClock {
    fn now(&self) -> Timestamp {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.manual.now()
    }
}

pub(super) struct Checkpoint {
    pub image: StoreSnapshot,
    reads: usize,
    scans: usize,
    pub attempts: usize,
    pub commits: usize,
    pub clocks: usize,
}

struct Certificates {
    ca: String,
    chain: String,
    key: String,
}

fn certificates(ca_name: &str) -> TestResult<Certificates> {
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, ca_name);
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
    Ok(Certificates {
        ca: ca.pem(),
        chain: format!("{}{}", leaf.pem(), ca.pem()),
        key: leaf_key.serialize_pem(),
    })
}

pub(super) struct Node {
    broker: Option<Broker>,
    listener: Option<JoinHandle<Result<(), NativeAdminError>>>,
    pub store: ObservedStore,
    pub clock: CountingClock,
    endpoint: String,
    credentials: TempDir,
    administrator: String,
    sender: String,
    sas: String,
    private_key: String,
}

impl Node {
    async fn start(old_registration: bool) -> TestResult<Self> {
        let credentials = process::at("finite-credential-directory", tempfile::tempdir())?;
        let administrator = tokens::jwt("administrator")?;
        let sender = tokens::jwt("sender")?;
        let sas = tokens::sas_token()?;
        let trusted = certificates("Switchyard finite CLI trusted CA")?;
        let wrong = certificates("Switchyard finite CLI untrusted CA")?;
        fs::write(credentials.path().join("ca.pem"), &trusted.ca)?;
        fs::write(credentials.path().join("wrong-ca.pem"), &wrong.ca)?;
        for (name, credential) in [
            ("administrator", format!("Bearer {administrator}")),
            ("sender", format!("Bearer {sender}")),
            ("sas", sas.clone()),
            ("invalid", "Bearer finite-cli-opaque-invalid".into()),
        ] {
            fs::write(credentials.path().join(name), format!("{credential}\n"))?;
        }
        let namespace = namespace()?;
        let sas_policy = tokens::sas_policy()?;
        let jwt_policy = tokens::policy()?;
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://{}", socket.local_addr()?);
        let store = ObservedStore {
            inner: Arc::new(MemoryStore::default()),
            observations: Arc::default(),
        };
        let clock = CountingClock {
            manual: ManualClock::at(1_000),
            calls: Arc::default(),
        };
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let service = NativeAdminService::new(broker.handle(), namespace)
            .with_shared_access_policy(sas_policy, tokens::HOST)?
            .with_offline_jwt_policy(jwt_policy, tokens::HOST)?;
        let listener = if old_registration {
            let mut server = Server::builder().tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(
                        trusted.chain.as_bytes(),
                        trusted.key.as_bytes(),
                    ))
                    .timeout(DEADLINE),
            )?;
            tokio::spawn(async move {
                let incoming = stream::poll_fn(move |context| {
                    socket
                        .poll_accept(context)
                        .map(|result| Some(result.map(|(stream, _)| stream)))
                });
                server
                    .add_service(EntityServiceServer::new(service))
                    .serve_with_incoming(incoming)
                    .await?;
                Ok(())
            })
        } else {
            let listener = NativeAdminListener::new(service)
                .with_tls(trusted.chain.as_bytes(), trusted.key.as_bytes())?
                .with_handshake_timeout(DEADLINE);
            tokio::spawn(listener.serve(socket))
        };
        Ok(Self {
            broker: Some(broker),
            listener: Some(listener),
            store,
            clock,
            endpoint,
            credentials,
            administrator,
            sender,
            sas,
            private_key: trusted.key,
        })
    }

    pub async fn run(&self, credential: &str, operation: &[String]) -> TestResult<Output> {
        self.run_tls(credential, operation, "ca.pem", "localhost")
            .await
    }

    pub async fn run_tls(
        &self,
        credential: &str,
        operation: &[String],
        ca_file: &str,
        name: &str,
    ) -> TestResult<Output> {
        let token_path = self.credentials.path().join(credential);
        let mut command = Command::new(env!("CARGO_BIN_EXE_switchyardctl"));
        command
            .args(["--namespace", "tenant", "--endpoint", &self.endpoint])
            .arg("--ca-certificate")
            .arg(self.credentials.path().join(ca_file))
            .args(["--tls-server-name", name, "--token-file"])
            .arg(&token_path)
            .args(operation)
            .env("TOKIO_WORKER_THREADS", "2");
        let path = token_path.display().to_string();
        process::run(
            command,
            &[
                &self.administrator,
                &self.sender,
                &self.sas,
                tokens::SAS_KEY,
                &self.private_key,
                "finite-cli-opaque-invalid",
                &path,
            ],
        )
        .await
    }

    pub async fn json(&self, credential: &str, operation: &[String]) -> TestResult<Json> {
        let output = self.run(credential, operation).await?;
        assert!(
            output.status.success(),
            "the original finite CLI child must succeed"
        );
        assert!(output.stderr.is_empty());
        process::at("finite-cli-json", serde_json::from_slice(&output.stdout))
    }

    pub async fn submit(&self, path: &str, kind: CommandKind) -> TestResult<CommandOutcome> {
        Ok(timeout(
            DEADLINE,
            self.broker
                .as_ref()
                .expect("original finite CLI owner")
                .handle()
                .submit(namespace()?, EntityPath::new(path)?, kind),
        )
        .await??)
    }

    pub fn checkpoint(&self) -> TestResult<Checkpoint> {
        let observations = &self.store.observations;
        Ok(Checkpoint {
            image: self.store.inner.snapshot()?,
            reads: observations.reads.load(Ordering::SeqCst),
            scans: observations.scans.load(Ordering::SeqCst),
            attempts: observations.attempts.load(Ordering::SeqCst),
            commits: observations.commits.load(Ordering::SeqCst),
            clocks: self.clock.calls.load(Ordering::SeqCst),
        })
    }

    pub fn unchanged(&self, before: &Checkpoint, clock_delta: usize) -> TestResult {
        let after = self.checkpoint()?;
        assert_eq!(after.image, before.image);
        assert_eq!(after.attempts, before.attempts);
        assert_eq!(after.commits, before.commits);
        assert_eq!(after.clocks, before.clocks + clock_delta);
        Ok(())
    }

    pub fn untouched(&self, before: &Checkpoint) -> TestResult {
        self.unchanged(before, 0)?;
        let after = self.checkpoint()?;
        assert_eq!(after.reads, before.reads);
        assert_eq!(after.scans, before.scans);
        Ok(())
    }

    async fn finish(mut self, observed: Result<TestResult, Box<dyn Any + Send>>) -> TestResult {
        let listener = if let Some(mut original) = self.listener.take() {
            let id = original.id();
            original.abort();
            let first = timeout(DEADLINE, &mut original).await;
            let expired = first.is_err();
            let joined = match first {
                Ok(joined) => Some(joined),
                Err(_) => timeout(DEADLINE, &mut original).await.ok(),
            };
            match joined {
                Some(Err(error)) if !expired && error.is_cancelled() && error.id() == id => Ok(()),
                Some(Ok(Ok(()))) if !expired => Ok(()),
                joined => Err(Box::new(ListenerFailure {
                    joined,
                    original: if expired { Some(original) } else { None },
                }) as Box<dyn Error>),
            }
        } else {
            Err("original finite CLI listener root is missing".into())
        };
        // Broker Drop requests Stop and joins the original owner synchronously, without an exposed deadline/result.
        drop(self.broker.take());
        // The actual discharge consumes the fixture's anchor after its original owner has joined.
        let store = std::mem::replace(&mut self.store.inner, Arc::new(MemoryStore::default()));
        let backend = Arc::try_unwrap(store)
            .map(drop)
            .map_err(|_| Box::<dyn Error>::from("original counted CLI backend is still held"));
        let cleanup = combine(listener, backend);
        settle(observed, cleanup)
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(listener) = &self.listener {
            listener.abort();
        }
    }
}

struct ListenerFailure {
    joined: Option<Result<Result<(), NativeAdminError>, JoinError>>,
    original: Option<JoinHandle<Result<(), NativeAdminError>>>,
}

impl fmt::Display for ListenerFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("original finite CLI listener join failed or exceeded its deadline")
    }
}

impl fmt::Debug for ListenerFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("ListenerFailure")
            .field("joined", &self.joined.is_some())
            .field("retained_original", &self.original.is_some())
            .finish()
    }
}

impl Error for ListenerFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.joined.as_ref()? {
            Err(error) => Some(error),
            Ok(Err(error)) => Some(error),
            Ok(Ok(())) => None,
        }
    }
}

struct FixtureFailure {
    primary: Option<Box<dyn Error>>,
    cleanup: Option<Box<dyn Error>>,
}

impl fmt::Display for FixtureFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(
            "finite CLI fixture failed; original primary and cleanup failures are retained",
        )
    }
}

impl fmt::Debug for FixtureFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("FixtureFailure")
            .field("primary", &self.primary.is_some())
            .field("cleanup", &self.cleanup.is_some())
            .finish()
    }
}

impl Error for FixtureFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.primary.as_deref().or(self.cleanup.as_deref())
    }
}

fn combine(primary: TestResult, cleanup: TestResult) -> TestResult {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (primary, cleanup) => Err(Box::new(FixtureFailure {
            primary: primary.err(),
            cleanup: cleanup.err(),
        })),
    }
}

fn settle(observed: Result<TestResult, Box<dyn Any + Send>>, cleanup: TestResult) -> TestResult {
    match observed {
        Err(payload) => {
            if cleanup.is_err() {
                eprintln!("finite CLI cleanup failed while preserving the original panic");
            }
            resume_unwind(payload)
        }
        Ok(primary) => combine(primary, cleanup),
    }
}

pub(super) async fn run<F>(old_registration: bool, case: F) -> TestResult
where
    F: for<'a> FnOnce(&'a Node) -> Pin<Box<dyn Future<Output = TestResult> + 'a>>,
{
    let node = Node::start(old_registration).await?;
    let observed = AssertUnwindSafe(case(&node)).catch_unwind().await;
    node.finish(observed).await
}

pub(super) fn namespace() -> TestResult<NamespaceName> {
    Ok(NamespaceName::new("tenant")?)
}

pub(super) fn assert_failure(output: Output, message: &str) {
    assert!(
        !output.status.success(),
        "the original finite CLI child must refuse"
    );
    assert!(output.stdout.is_empty());
    assert_eq!(
        output.stderr,
        format!("switchyardctl: {message}\n").as_bytes()
    );
}

pub(super) fn assert_code(output: Output, code: tonic::Code) {
    assert_failure(output, &format!("administration request failed ({code:?})"));
}
