//! The built CLI sends opaque offline JWT headers over actual trusted TLS.
#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

#[path = "native_offline_jwt/fixtures.rs"]
mod fixtures;
#[path = "native_offline_jwt/process.rs"]
mod process;

use std::{
    error::Error,
    fs,
    panic::{AssertUnwindSafe, resume_unwind},
    path::{Path, PathBuf},
    process::Output,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use domain::{
    Command as DomainCommand, CommandKind, EntityPath, NamespaceName, QueueConfig,
    QueueConfigUpdate, StateMachine, Timestamp,
};
use futures_util::FutureExt;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use serde_json::Value as Json;
use server::{
    Broker, Clock, LocalProposer, ManualClock, NativeAdminError, NativeAdminListener,
    NativeAdminService,
};
use storage::{Key, MemoryStore, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use tempfile::TempDir;
use tokio::{net::TcpListener, process::Command, task::JoinHandle, time::timeout};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Default)]
struct Observations {
    reads: AtomicUsize,
    writes: AtomicUsize,
}

#[derive(Clone)]
struct ObservedStore {
    inner: MemoryStore,
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
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observations.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }
}

struct Node {
    broker: Option<Broker>,
    listener: Option<JoinHandle<Result<(), NativeAdminError>>>,
    store: ObservedStore,
    clock: ManualClock,
    arguments: Vec<String>,
    credentials: TempDir,
    administrator: String,
    sender: String,
    sas: String,
    private_key: String,
}

impl Node {
    async fn start() -> TestResult<Self> {
        let credentials = process::at("credential-directory", tempfile::tempdir())?;
        let administrator = fixtures::jwt("administrator")
            .map_err(|error| process::retained("administrator-fixture", error))?;
        let sender =
            fixtures::jwt("sender").map_err(|error| process::retained("sender-fixture", error))?;
        let sas = fixtures::sas_token().map_err(|error| process::retained("sas-fixture", error))?;
        let CertifiedKey { cert, key_pair } = process::at(
            "tls-fixture",
            generate_simple_self_signed(vec!["localhost".into()]),
        )?;
        let certificate = cert.pem();
        let private_key = key_pair.serialize_pem();
        let ca_path = credentials.path().join("ca.pem");
        process::at("write-ca", fs::write(&ca_path, &certificate))?;
        for (name, credential) in [
            ("administrator", format!("Bearer {administrator}")),
            ("sender", format!("Bearer {sender}")),
            ("sas", sas.clone()),
            ("invalid", "Bearer opaque-sentinel-invalid".into()),
        ] {
            process::at(
                "write-token",
                fs::write(credentials.path().join(name), format!("{credential}\n")),
            )?;
        }
        let store = ObservedStore {
            inner: MemoryStore::default(),
            observations: Arc::default(),
        };
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let service = NativeAdminService::new(
            broker.handle(),
            process::at("namespace", NamespaceName::new("tenant"))?,
        )
        .with_shared_access_policy(
            fixtures::sas_policy().map_err(|error| process::retained("sas-policy", error))?,
            fixtures::HOST,
        )
        .map_err(|error| process::retained("sas-resource", Box::new(error)))?
        .with_offline_jwt_policy(
            fixtures::policy().map_err(|error| process::retained("jwt-policy", error))?,
            fixtures::HOST,
        )
        .map_err(|error| process::retained("jwt-resource", Box::new(error)))?;
        let listener = process::at(
            "listener-tls",
            NativeAdminListener::new(service)
                .with_tls(certificate.as_bytes(), private_key.as_bytes()),
        )?;
        let socket = process::at("listener-bind", TcpListener::bind("127.0.0.1:0").await)?;
        let endpoint = format!(
            "https://{}",
            process::at("listener-address", socket.local_addr())?
        );
        let arguments = vec![
            "--namespace".into(),
            "tenant".into(),
            "--endpoint".into(),
            endpoint,
            "--ca-certificate".into(),
            ca_path.display().to_string(),
            "--tls-server-name".into(),
            "localhost".into(),
        ];
        let listener = tokio::spawn(async move { listener.serve(socket).await });
        Ok(Self {
            broker: Some(broker),
            listener: Some(listener),
            store,
            clock,
            arguments,
            credentials,
            administrator,
            sender,
            sas,
            private_key,
        })
    }

    fn credential(&self, name: &str) -> PathBuf {
        self.credentials.path().join(name)
    }

    async fn run(&self, credential: &Path, operation: &[&str]) -> TestResult<Output> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_switchyardctl"));
        command
            .args(&self.arguments)
            .arg("--token-file")
            .arg(credential)
            .args(operation)
            .env("TOKIO_WORKER_THREADS", "2");
        let credential_path = credential.display().to_string();
        process::run(
            command,
            &[
                &self.administrator,
                &self.sender,
                &self.sas,
                fixtures::SAS_KEY,
                &self.private_key,
                "opaque-sentinel-invalid",
                &credential_path,
            ],
        )
        .await
    }

    async fn json(&self, credential: &Path, operation: &[&str]) -> TestResult<Json> {
        let output = self.run(credential, operation).await?;
        assert!(
            output.status.success(),
            "the original CLI child must succeed"
        );
        assert!(output.stderr.is_empty());
        process::at("decode-cli-json", serde_json::from_slice(&output.stdout))
    }

    fn baseline(&self) -> TestResult<StoreSnapshot> {
        let snapshot = process::at("snapshot-before-denial", self.store.snapshot())?;
        self.store.observations.reads.store(0, Ordering::SeqCst);
        self.store.observations.writes.store(0, Ordering::SeqCst);
        Ok(snapshot)
    }

    fn unchanged(&self, baseline: &StoreSnapshot) -> TestResult {
        assert_eq!(self.store.observations.reads.load(Ordering::SeqCst), 0);
        assert_eq!(self.store.observations.writes.load(Ordering::SeqCst), 0);
        assert_eq!(
            &process::at("snapshot-after-denial", self.store.snapshot())?,
            baseline
        );
        assert_eq!(self.clock.now(), Timestamp::from_millis(1_000));
        Ok(())
    }

    async fn stop(&mut self) -> TestResult {
        let mut listener = self.listener.take().expect("the original listener handle");
        listener.abort();
        let joined = timeout(Duration::from_secs(5), &mut listener).await;
        if joined.is_err() {
            self.listener = Some(listener);
        }
        // Broker Drop requests Stop and joins the original owner; its result is not exposed.
        drop(self.broker.take());
        match joined {
            Ok(Err(error)) if error.is_cancelled() => Ok(()),
            Ok(Err(error)) => Err(process::retained("listener-join", Box::new(error))),
            Ok(Ok(result)) => {
                result.map_err(|error| process::retained("listener-serve", Box::new(error)))
            }
            Err(error) => Err(process::retained(
                "listener-cleanup-deadline",
                Box::new(error),
            )),
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(listener) = &self.listener {
            listener.abort();
        }
    }
}

async fn exercise(node: &Node) -> TestResult {
    let administrator = node.credential("administrator");
    let sender = node.credential("sender");
    let created = node
        .json(
            &administrator,
            &[
                "queue",
                "create",
                "orders",
                "--lock-duration-millis",
                "30000",
            ],
        )
        .await?;
    assert_eq!(created["path"], "orders");
    assert_eq!(created["queue_config"]["lock_duration_millis"], 30_000);
    assert_eq!(
        node.json(&administrator, &["queue", "get", "orders"])
            .await?,
        created
    );
    let updated = node
        .json(
            &administrator,
            &[
                "queue",
                "update",
                "orders",
                "--lock-duration-millis",
                "40000",
            ],
        )
        .await?;
    assert_eq!(updated["queue_config"]["lock_duration_millis"], 40_000);
    let listed = node.json(&administrator, &["queue", "list"]).await?;
    assert_eq!(listed["entities"], serde_json::json!([updated.clone()]));
    assert_eq!(listed["next_page_token"], "");
    let baseline = node.baseline()?;
    let expected = StateMachine::new(MemoryStore::default());
    let namespace = process::at("canonical-namespace", NamespaceName::new("tenant"))?;
    let entity = process::at("canonical-entity", EntityPath::new("orders"))?;
    for kind in [
        CommandKind::CreateQueue {
            config: QueueConfig {
                lock_duration_millis: 30_000,
                ..QueueConfig::default()
            },
        },
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                lock_duration_millis: Some(40_000),
                ..QueueConfigUpdate::default()
            },
        },
    ] {
        process::at(
            "canonical-command",
            expected.apply(&DomainCommand::new(
                namespace.clone(),
                entity.clone(),
                Timestamp::from_millis(1_000),
                kind,
            )),
        )?;
    }
    assert_eq!(
        baseline,
        process::at("canonical-snapshot", expected.store().snapshot())?
    );
    for operation in [
        vec!["queue", "create", "forbidden"],
        vec!["queue", "get", "orders"],
        vec!["queue", "list"],
        vec![
            "queue",
            "update",
            "orders",
            "--lock-duration-millis",
            "70000",
        ],
        vec!["queue", "delete", "orders"],
    ] {
        let denied = node.run(&sender, &operation).await?;
        assert!(!denied.status.success());
        assert!(denied.stdout.is_empty());
        assert_eq!(
            denied.stderr,
            b"switchyardctl: administration request failed (PermissionDenied)\n"
        );
        node.unchanged(&baseline)?;
    }
    let invalid = node
        .run(&node.credential("invalid"), &["queue", "get", "orders"])
        .await?;
    assert!(!invalid.status.success());
    assert!(invalid.stdout.is_empty());
    assert_eq!(
        invalid.stderr,
        b"switchyardctl: administration request failed (Unauthenticated)\n"
    );
    node.unchanged(&baseline)?;
    assert_eq!(
        node.json(&node.credential("sas"), &["queue", "get", "orders"])
            .await?,
        updated
    );
    assert_eq!(
        node.json(&administrator, &["queue", "get", "orders"])
            .await?,
        updated
    );
    node.json(&administrator, &["queue", "delete", "orders"])
        .await?;
    let empty = node.json(&administrator, &["queue", "list"]).await?;
    assert_eq!(empty["entities"], serde_json::json!([]));
    assert_eq!(empty["next_page_token"], "");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_jwt_manage_crud_send_denials_and_sas_coexist_over_trusted_tls() -> TestResult {
    let mut node = Node::start().await?;
    // Each child has its own bounded run and cleanup; assertions run only after reaping.
    let observed = AssertUnwindSafe(exercise(&node)).catch_unwind().await;
    let cleanup = node.stop().await;
    match observed {
        Err(payload) => resume_unwind(payload),
        Ok(result) => result?,
    }
    cleanup
}
