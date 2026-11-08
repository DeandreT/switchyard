//! Spawn the actual switchyardctl binary against the actual authenticated TLS
//! AdminListenerService/Broker. Contract-only paging edge cases use a separate
//! scripted TLS RPC service, not a fake CLI client or replacement broker rules.
#[allow(dead_code)]
#[path = "../../server/tests/admin_tls/mod.rs"]
mod jwt_fixture;

use std::{
    error::Error,
    path::PathBuf,
    process::Output,
    sync::{Arc, Mutex},
    time::Duration,
};

use admin_api::wire::{
    self,
    entity_service_server::{EntityService, EntityServiceServer},
};
use auth::{JwtPolicy, ResourceScope};
use domain::{NamespaceName, StateMachine};
use jwt_fixture::{ROOT, claims, policy, sign};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use serde_json::Value;
use server::{
    AdminListenerService, AdminRetirement, AdminTlsConfig, Broker, LocalProposer, SystemClock,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};
use tonic::{
    Request, Response, Status,
    transport::{Identity, Server, ServerTlsConfig},
};

type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(60);

#[test]
fn compatibility_and_argument_boundaries_need_no_connection_setup() -> Result {
    let binary = env!("CARGO_BIN_EXE_switchyardctl");
    let output = std::process::Command::new(binary)
        .arg("compatibility")
        .output()?;
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("HTTPS Create/Get/List"));
    for args in [
        vec!["queue", "list"],
        vec!["queue", "list", "--page-size", "0"],
        vec!["queue", "list", "--max-pages", "10001"],
        vec!["queue", "list", "--timeout-secs", "0"],
    ] {
        let output = std::process::Command::new(binary).args(args).output()?;
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
    Ok(())
}

struct Files {
    _directory: TempDir,
    ca: PathBuf,
    token: PathBuf,
    credential: String,
    endpoint: String,
}
impl Files {
    fn new(certificate: &str, port: u16) -> Result<Self> {
        let directory = TempDir::new()?;
        let ca = directory.path().join("ca.pem");
        let token = directory.path().join("token");
        let credential = sign(claims("operator"));
        std::fs::write(&ca, certificate)?;
        std::fs::write(&token, format!("\r\n{credential}\r\n"))?;
        Ok(Self {
            _directory: directory,
            ca,
            token,
            credential,
            endpoint: format!("https://localhost:{port}"),
        })
    }
    async fn invoke(&self, args: &[&str]) -> Result<Output> {
        let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_switchyardctl"))
            .args([
                "--endpoint",
                &self.endpoint,
                "--namespace",
                "tenant",
                "--ca-file",
            ])
            .arg(&self.ca)
            .arg("--token-file")
            .arg(&self.token)
            .args(args)
            .kill_on_drop(true)
            .output();
        let output = tokio::time::timeout(WAIT, child).await??;
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains(&self.credential),
            "stderr exposed a credential"
        );
        assert!(
            !String::from_utf8_lossy(&output.stdout).contains(&self.credential),
            "stdout exposed a credential"
        );
        Ok(output)
    }
    async fn ok(&self, args: &[&str]) -> Result<Value> {
        let output = self.invoke(args).await?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        Ok(serde_json::from_slice(&output.stdout)?)
    }
    async fn fail(&self, args: &[&str], expected: &str) -> Result {
        let output = self.invoke(args).await?;
        assert!(!output.status.success());
        assert!(
            output.stdout.is_empty(),
            "failure emitted a partial catalog"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected),
            "expected {expected}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }
}

struct Real {
    files: Files,
    retirement: AdminRetirement,
    task: JoinHandle<AdminListenerService>,
    broker: Broker,
}
impl Real {
    async fn start() -> Result<Self> {
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec!["localhost".into()])?;
        let config = AdminTlsConfig::new(
            protocol_amqp::tls_server_config(
                cert.pem().as_bytes(),
                key_pair.serialize_pem().as_bytes(),
            )?,
            JwtPolicy::from_json(&policy().to_string())?,
            ResourceScope::parse(ROOT)?,
        )?;
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(storage::MemoryStore::default()),
            SystemClock,
        ));
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let files = Files::new(&cert.pem(), socket.local_addr()?.port())?;
        let mut admin = AdminListenerService::new(
            socket,
            config,
            broker.handle(),
            NamespaceName::new("tenant")?,
            WAIT,
        );
        let retirement = admin.retirement_handle();
        let task = tokio::spawn(async move {
            admin.serve().await;
            admin
        });
        Ok(Self {
            files,
            retirement,
            task,
            broker,
        })
    }
    async fn finish(self) -> Result {
        self.retirement.request();
        let mut admin = tokio::time::timeout(WAIT, self.task).await??;
        tokio::time::timeout(WAIT, admin.finish()).await?;
        assert!(admin.take_failure().is_none());
        drop((admin, self.broker));
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawned_create_get_list_all_kinds_options_and_real_paging() -> Result {
    let real = Real::start().await?;
    let f = &real.files;
    let queue = f
        .ok(&[
            "queue",
            "create",
            "orders",
            "--default-ttl-millis",
            "90000",
            "--lock-duration-millis",
            "1000",
            "--max-delivery-count",
            "7",
            "--max-message-bytes",
            "4096",
            "--requires-session",
            "false",
            "--requires-duplicate-detection",
            "false",
            "--duplicate-detection-history-millis",
            "0",
        ])
        .await?;
    assert_eq!(queue["requires_session"], false);
    assert_eq!(queue["requires_duplicate_detection"], false);
    assert_eq!(queue["duplicate_detection_history_millis"], 600000);
    assert_eq!(queue["max_delivery_count"], 7);
    assert_eq!(queue["default_ttl_millis"], 90000);
    assert_eq!(queue["lock_duration_millis"], 1000);
    assert_eq!(queue["max_message_bytes"], 4096);
    assert!(queue["max_size_bytes"].is_null());
    assert_eq!(f.ok(&["queue", "get", "orders"]).await?, queue);
    f.ok(&["queue", "create", "second"]).await?;
    let queues = f.ok(&["queue", "list", "--page-size", "1"]).await?;
    assert_eq!(queues["entities"].as_array().unwrap().len(), 2);
    assert!(queues["continuation"].is_null());
    f.fail(
        &["queue", "list", "--page-size", "1", "--max-pages", "1"],
        "page budget exhausted",
    )
    .await?;

    let topic = f
        .ok(&[
            "topic",
            "create",
            "news",
            "--default-ttl-millis",
            "12345",
            "--max-message-bytes",
            "8192",
        ])
        .await?;
    assert_eq!(topic["default_ttl_millis"], 12345);
    assert!(topic["lock_duration_millis"].is_null());
    assert_eq!(f.ok(&["topic", "get", "news"]).await?, topic);
    f.fail(&["queue", "get", "news"], "different kind").await?;
    f.fail(&["topic", "get", "orders"], "different kind")
        .await?;
    assert_eq!(f.ok(&["topic", "list"]).await?["entities"][0], topic);
    let subscription = f
        .ok(&[
            "subscription",
            "create",
            "news",
            "reader",
            "--default-ttl-millis",
            "54321",
            "--lock-duration-millis",
            "2000",
            "--max-delivery-count",
            "3",
        ])
        .await?;
    assert_eq!(subscription["subscription_topic"], "news");
    assert_eq!(subscription["subscription_name"], "reader");
    assert_eq!(subscription["default_ttl_millis"], 54321);
    assert_eq!(subscription["max_delivery_count"], 3);
    assert!(subscription["max_message_bytes"].is_null());
    assert_eq!(
        f.ok(&["subscription", "get", "news", "reader"]).await?,
        subscription
    );
    assert_eq!(
        f.ok(&["subscription", "list", "--page-size", "1"]).await?["entities"][0],
        subscription
    );
    for (kind, args) in [
        ("queue", vec!["orders"]),
        ("topic", vec!["news"]),
        ("subscription", vec!["news", "reader"]),
    ] {
        let mut duplicate = vec![kind, "create"];
        duplicate.extend(args);
        f.fail(&duplicate, "already exists").await?;
    }
    f.fail(&["queue", "get", "missing"], "not found").await?;
    f.fail(&["topic", "get", "missing"], "not found").await?;
    f.fail(&["subscription", "get", "news", "missing"], "not found")
        .await?;
    f.fail(
        &["subscription", "create", "missing", "reader"],
        "not found",
    )
    .await?;
    // Explicit zero reaches server validation, rather than becoming omission.
    for option in [
        "--default-ttl-millis",
        "--lock-duration-millis",
        "--max-delivery-count",
        "--max-message-bytes",
    ] {
        f.fail(&["queue", "create", "zero", option, "0"], "invalid entity")
            .await?;
    }
    // Even false is present: the server refuses queue-only options on topics.
    f.fail(
        &[
            "topic",
            "create",
            "unsupported",
            "--requires-session",
            "false",
        ],
        "unsupported",
    )
    .await?;
    f.fail(
        &[
            "subscription",
            "create",
            "news",
            "unsupported",
            "--requires-duplicate-detection",
            "false",
        ],
        "unsupported",
    )
    .await?;
    real.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawned_auth_tls_and_file_failures_are_useful_nonzero_and_redacted() -> Result {
    let mut real = Real::start().await?;
    real.files.ok(&["queue", "list"]).await?;
    std::fs::write(&real.files.token, sign(claims("sender")))?;
    real.files
        .fail(
            &["queue", "create", "q", "--max-message-bytes", "0"],
            "Manage permission",
        )
        .await?;
    let mut expired = claims("operator");
    expired["exp"] = 1.into();
    std::fs::write(&real.files.token, sign(expired))?;
    real.files
        .fail(&["queue", "list"], "credential rejected")
        .await?;
    std::fs::write(&real.files.token, "not-a-valid-credential")?;
    real.files
        .fail(&["queue", "list"], "credential rejected")
        .await?;
    for bad in [
        format!("{}\n{}", real.files.credential, real.files.credential),
        format!(" {}", real.files.credential),
        format!("{}\t", real.files.credential),
        format!("{}\n\n", real.files.credential),
    ] {
        std::fs::write(&real.files.token, bad)?;
        real.files
            .fail(&["queue", "list"], "one credential")
            .await?;
    }
    std::fs::write(&real.files.token, &real.files.credential)?;
    real.files.endpoint = real.files.endpoint.replace("localhost", "127.0.0.1");
    real.files
        .fail(&["queue", "list"], "certificate hostname")
        .await?;
    real.files.endpoint = real.files.endpoint.replace("127.0.0.1", "localhost");
    let alien = generate_simple_self_signed(vec!["localhost".into()])?;
    std::fs::write(&real.files.ca, alien.cert.pem())?;
    real.files.fail(&["queue", "list"], "trusted CA").await?;
    std::fs::write(&real.files.ca, vec![b'x'; 1024 * 1024 + 1])?;
    real.files.fail(&["queue", "list"], "at most").await?;
    std::fs::write(&real.files.token, vec![b'x'; 16 * 1024 + 1])?;
    real.files.fail(&["queue", "list"], "at most").await?;
    std::fs::remove_file(&real.files.token)?;
    real.files
        .fail(&["queue", "list"], "cannot open token")
        .await?;
    real.finish().await
}

#[derive(Clone)]
struct Script {
    requests: Arc<Mutex<Vec<wire::ListEntitiesRequest>>>,
    pages: Arc<Mutex<std::collections::VecDeque<wire::ListEntitiesResponse>>>,
    delay: bool,
}
#[tonic::async_trait]
impl EntityService for Script {
    async fn create_entity(
        &self,
        _: Request<wire::CreateEntityRequest>,
    ) -> std::result::Result<Response<wire::Entity>, Status> {
        Err(Status::unimplemented("test paging service"))
    }
    async fn get_entity(
        &self,
        _: Request<wire::GetEntityRequest>,
    ) -> std::result::Result<Response<wire::Entity>, Status> {
        Err(Status::unimplemented("test paging service"))
    }
    async fn delete_entity(
        &self,
        _: Request<wire::DeleteEntityRequest>,
    ) -> std::result::Result<Response<wire::Operation>, Status> {
        Err(Status::unimplemented("test paging service"))
    }
    async fn list_entities(
        &self,
        request: Request<wire::ListEntitiesRequest>,
    ) -> std::result::Result<Response<wire::ListEntitiesResponse>, Status> {
        self.requests.lock().unwrap().push(request.into_inner());
        if self.delay {
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        self.pages
            .lock()
            .unwrap()
            .pop_front()
            .map(Response::new)
            .ok_or_else(|| Status::internal("script exhausted"))
    }
}
fn page(cursor: Option<&[u8]>, entities: Vec<wire::Entity>) -> wire::ListEntitiesResponse {
    wire::ListEntitiesResponse {
        continuation: cursor.map(<[u8]>::to_vec),
        entities,
        ..Default::default()
    }
}
async fn scripted(
    pages: Vec<wire::ListEntitiesResponse>,
    args: &[&str],
    expected: Option<&str>,
    delay: bool,
) -> Result<Vec<wire::ListEntitiesRequest>> {
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])?;
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let files = Files::new(&cert.pem(), socket.local_addr()?.port())?;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let service = Script {
        requests: requests.clone(),
        pages: Arc::new(Mutex::new(pages.into())),
        delay,
    };
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = Server::builder()
        .tls_config(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(cert.pem(), key_pair.serialize_pem())),
        )?
        .add_service(EntityServiceServer::new(service));
    let task = tokio::spawn(server.serve_with_incoming_shutdown(
        tokio_stream::wrappers::TcpListenerStream::new(socket),
        async {
            let _ = stopped.await;
        },
    ));
    if let Some(expected) = expected {
        files.fail(args, expected).await?;
    } else {
        assert_eq!(
            files.ok(args).await?["entities"].as_array().unwrap().len(),
            1
        );
    }
    let _ = stop.send(());
    tokio::time::timeout(WAIT, task).await???;
    let result = requests.lock().unwrap().clone();
    Ok(result)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawned_opaque_paging_empty_pages_cycles_budgets_and_deadline() -> Result {
    let entity = wire::Entity {
        namespace: "tenant".into(),
        path: "last".into(),
        kind: wire::EntityKind::Queue as i32,
        ..Default::default()
    };
    // Deliberately arbitrary bytes: only echo/equality, never cursor internals.
    let requests = scripted(
        vec![
            page(Some(b"\0\xfffirst"), vec![]),
            page(Some(b"\x80next"), vec![]),
            page(None, vec![entity.clone()]),
        ],
        &["queue", "list", "--page-size", "1"],
        None,
        false,
    )
    .await?;
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].continuation, None);
    assert_eq!(
        requests[1].continuation.as_deref(),
        Some(&b"\0\xfffirst"[..])
    );
    assert_eq!(requests[2].continuation.as_deref(), Some(&b"\x80next"[..]));
    assert!(requests.iter().all(|r| r.page_size == 1
        && r.namespace == "tenant"
        && r.selection == wire::EntitySelection::Queues as i32));
    scripted(
        vec![
            page(Some(b"same"), vec![entity.clone()]),
            page(Some(b"same"), vec![]),
        ],
        &["queue", "list"],
        Some("cycle detected"),
        false,
    )
    .await?;
    scripted(
        vec![page(Some(b"next"), vec![])],
        &["queue", "list", "--max-pages", "1"],
        Some("page budget exhausted"),
        false,
    )
    .await?;
    let requests = scripted(
        vec![page(None, vec![entity.clone()])],
        &["queue", "list", "--continuation", "aW5pdA=="],
        None,
        false,
    )
    .await?;
    assert_eq!(requests[0].continuation.as_deref(), Some(&b"init"[..]));
    scripted(
        vec![page(Some(&vec![0; 65537]), vec![])],
        &["queue", "list"],
        Some("continuation exceeds byte limit"),
        false,
    )
    .await?;
    scripted(
        vec![page(None, vec![entity.clone(), entity.clone()])],
        &["queue", "list", "--page-size", "1"],
        Some("exceeded requested page size"),
        false,
    )
    .await?;
    #[allow(deprecated)]
    let textual = wire::ListEntitiesResponse {
        next_page_token: "unsupported-textual-cursor".into(),
        ..Default::default()
    };
    scripted(
        vec![textual],
        &["queue", "list"],
        Some("unsupported textual continuation"),
        false,
    )
    .await?;
    let mut large = entity;
    large.path = "x".repeat(2048);
    scripted(
        vec![page(None, vec![large])],
        &["queue", "list", "--max-output-bytes", "1024"],
        Some("output byte budget"),
        false,
    )
    .await?;
    scripted(
        // Every page fits a one-second RPC timeout, but the whole traversal
        // does not. The deadline must not restart for each page.
        vec![
            page(Some(b"one"), vec![]),
            page(Some(b"two"), vec![]),
            page(None, vec![]),
        ],
        &["queue", "list", "--timeout-secs", "1"],
        Some("deadline"),
        true,
    )
    .await?;
    Ok(())
}
