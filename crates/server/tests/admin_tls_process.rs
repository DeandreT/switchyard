//! Real binary, trusted TLS RPC readiness, offline JWT and durable owner tests.
#![cfg(target_os = "linux")]

mod admin_tls;
mod jwt_process;

use admin_api::wire::{self, EntityKind, entity_service_client::EntityServiceClient};
use admin_tls::{claims, client, list, policy, request, sign};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustix::process::{Pid, Signal, kill_process};
use std::{
    error::Error,
    fs::{self, File},
    net::{SocketAddr, TcpListener},
    path::Path,
    process::{Child, Command, Output, Stdio},
    time::Duration,
};
use tempfile::TempDir;
use tokio::time::Instant;
use tonic::{Code, transport::Channel};

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;
const WAIT: Duration = Duration::from_secs(120);

struct Process {
    child: Child,
    address: SocketAddr,
    amqp: SocketAddr,
}

fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

impl Process {
    async fn start(
        root: &Path,
        certificate: &str,
        extra: &[&str],
        env: &[(&str, &str)],
    ) -> Result<(Self, EntityServiceClient<Channel>)> {
        let address = address();
        let amqp = self::address();
        let log = File::create(root.join("process.log"))?;
        let child = Command::new(env!("CARGO_BIN_EXE_switchyard"))
            .args([
                "--namespace",
                "tenant",
                "--listen",
                &amqp.to_string(),
                "--admin-listen",
                &address.to_string(),
                "--admin-tls-certificate",
            ])
            .arg(root.join("cert.pem"))
            .arg("--admin-tls-private-key")
            .arg(root.join("key.pem"))
            .arg("--admin-jwt-policy-file")
            .arg(root.join("policy.json"))
            .args(["--storage", "fjall", "--data-dir"])
            .arg(root.join("state"))
            .args(extra)
            .envs(env.iter().copied())
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        let mut process = Self {
            child,
            address,
            amqp,
        };
        let deadline = Instant::now() + WAIT;
        let token = sign(claims("operator"));
        loop {
            if let Some(status) = process.child.try_wait()? {
                return Err(format!(
                    "startup exited {status}: {}",
                    fs::read_to_string(root.join("process.log"))?
                )
                .into());
            }
            if let Ok(mut rpc) = client(address, certificate, "localhost").await
                && rpc.list_entities(request(list(), &token)).await.is_ok()
            {
                return Ok((process, rpc));
            }
            if Instant::now() > deadline {
                return Err("authenticated TLS readiness deadline expired".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn signal(&self, signal: Signal) -> Result {
        kill_process(Pid::from_child(&self.child), signal)?;
        Ok(())
    }

    async fn wait(&mut self) -> Result<i32> {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status.code().unwrap_or(-1));
            }
            if Instant::now() > deadline {
                return Err("child did not finish".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn fixture() -> Result<(TempDir, String)> {
    let root = TempDir::new()?;
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])?;
    fs::write(root.path().join("cert.pem"), cert.pem())?;
    fs::write(root.path().join("key.pem"), key_pair.serialize_pem())?;
    fs::write(root.path().join("policy.json"), policy().to_string())?;
    Ok((root, cert.pem()))
}

fn create(path: &str, kind: EntityKind) -> wire::CreateEntityRequest {
    wire::CreateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        kind: kind as i32,
        ..Default::default()
    }
}

fn get(path: &str) -> wire::GetEntityRequest {
    wire::GetEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        ..Default::default()
    }
}

/// A startup-regression must fail within a bound, not leave a serving process
/// behind and hang the whole qualification run.
fn bounded_output(command: &mut Command) -> Result<Output> {
    struct StartupChild(Option<Child>);
    impl Drop for StartupChild {
        fn drop(&mut self) {
            if let Some(mut child) = self.0.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
    let mut child = StartupChild(Some(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    ));
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        if child
            .0
            .as_mut()
            .expect("owned startup child")
            .try_wait()?
            .is_some()
        {
            return Ok(child
                .0
                .take()
                .expect("exited startup child")
                .wait_with_output()?);
        }
        if std::time::Instant::now() >= deadline {
            return Err("startup did not refuse before its deadline".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_tls_crud_statuses_paging_and_durable_reopen() -> Result {
    let (root, cert) = fixture()?;
    let (mut process, mut rpc) = Process::start(root.path(), &cert, &[], &[]).await?;
    let token = sign(claims("operator"));
    let mut expected = Vec::new();
    for (path, kind) in [
        ("orders", EntityKind::Queue),
        ("other", EntityKind::Queue),
        ("events", EntityKind::Topic),
    ] {
        let entity = rpc
            .create_entity(request(create(path, kind), &token))
            .await?
            .into_inner();
        assert_eq!(
            rpc.get_entity(request(get(path), &token))
                .await?
                .into_inner(),
            entity
        );
        expected.push(entity);
    }
    let mut sub = create("", EntityKind::Subscription);
    sub.subscription_topic = Some("events".into());
    sub.subscription_name = Some("consumer".into());
    let subscription = rpc.create_entity(request(sub, &token)).await?.into_inner();
    let sub_get = wire::GetEntityRequest {
        namespace: "tenant".into(),
        subscription_topic: Some("events".into()),
        subscription_name: Some("consumer".into()),
        ..Default::default()
    };
    assert_eq!(
        rpc.get_entity(request(sub_get.clone(), &token))
            .await?
            .into_inner(),
        subscription
    );
    expected.push(subscription);
    assert_eq!(
        rpc.create_entity(request(create("orders", EntityKind::Queue), &token))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    assert_eq!(
        rpc.get_entity(request(get("missing"), &token))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let mut invalid = create("invalid", EntityKind::Queue);
    invalid.max_delivery_count = Some(0);
    assert_eq!(
        rpc.create_entity(request(invalid, &token))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut unsupported = create("unsupported", EntityKind::Queue);
    unsupported.max_size_bytes = Some(0);
    assert_eq!(
        rpc.create_entity(request(unsupported, &token))
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    assert_eq!(
        rpc.delete_entity(request(
            wire::DeleteEntityRequest {
                namespace: "tenant".into(),
                path: "orders".into()
            },
            &token
        ))
        .await
        .unwrap_err()
        .code(),
        Code::Unimplemented
    );
    let mut foreign = get("orders");
    foreign.namespace = "other".into();
    assert_eq!(
        rpc.get_entity(request(foreign, &token))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut seen = Vec::new();
    let mut page = list();
    page.page_size = 1;
    loop {
        let response = rpc
            .list_entities(request(page.clone(), &token))
            .await?
            .into_inner();
        seen.extend(response.entities);
        page.continuation = response.continuation;
        if page.continuation.is_none() {
            break;
        }
        assert!(seen.len() <= 4);
    }
    assert_eq!(seen.len(), expected.len());
    for entity in &expected {
        assert!(seen.contains(entity));
    }
    for (selection, count) in [
        (wire::EntitySelection::Queues, 2),
        (wire::EntitySelection::Topics, 1),
        (wire::EntitySelection::Subscriptions, 1),
    ] {
        let mut page = list();
        page.selection = selection as i32;
        assert_eq!(
            rpc.list_entities(request(page, &token))
                .await?
                .into_inner()
                .entities
                .len(),
            count
        );
    }
    process.signal(Signal::TERM)?;
    assert_eq!(process.wait().await?, 0);
    assert!(
        tokio::net::TcpStream::connect(process.address)
            .await
            .is_err()
    );
    assert!(tokio::net::TcpStream::connect(process.amqp).await.is_err());
    drop(rpc);
    let (mut reopened, mut rpc) = Process::start(root.path(), &cert, &[], &[]).await?;
    let all = rpc
        .list_entities(request(list(), &token))
        .await?
        .into_inner()
        .entities;
    assert_eq!(all.len(), 4);
    for entity in expected {
        assert!(all.contains(&entity));
    }
    assert!(rpc.get_entity(request(sub_get, &token)).await.is_ok());
    reopened.signal(Signal::INT)?;
    assert_eq!(reopened.wait().await?, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn credentials_are_per_rpc_and_bad_credentials_cannot_dispatch() -> Result {
    let (root, cert) = fixture()?;
    let (mut process, mut rpc) = Process::start(root.path(), &cert, &[], &[]).await?;
    assert_eq!(
        rpc.create_entity(create("unauthorized", EntityKind::Queue))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    let mut expired = claims("operator");
    expired["iat"] = serde_json::json!(1);
    expired["exp"] = serde_json::json!(2);
    let mut wrong_issuer = claims("operator");
    wrong_issuer["iss"] = serde_json::json!("https://foreign.example/");
    let mut wrong_aud = claims("operator");
    wrong_aud["aud"] = serde_json::json!("foreign");
    let mut future = claims("operator");
    future["nbf"] = future["exp"].clone();
    let mut bad_signature = sign(claims("operator")).into_bytes();
    let i = bad_signature.len() - 5;
    bad_signature[i] = if bad_signature[i] == b'A' { b'B' } else { b'A' };
    let bad_signature = String::from_utf8(bad_signature)?;
    for (token, code) in [
        ("malformed".into(), Code::Unauthenticated),
        (sign(expired), Code::Unauthenticated),
        (sign(wrong_issuer), Code::Unauthenticated),
        (sign(wrong_aud), Code::Unauthenticated),
        (sign(future), Code::Unauthenticated),
        (bad_signature, Code::Unauthenticated),
        (sign(claims("unknown")), Code::Unauthenticated),
        (sign(claims("sender")), Code::PermissionDenied),
        (sign(claims("entity")), Code::PermissionDenied),
        (sign(claims("foreign")), Code::PermissionDenied),
    ] {
        assert_eq!(
            rpc.create_entity(request(create("unauthorized", EntityKind::Queue), &token))
                .await
                .unwrap_err()
                .code(),
            code
        );
        assert_eq!(
            rpc.get_entity(request(get("unauthorized"), &token))
                .await
                .unwrap_err()
                .code(),
            code
        );
        assert_eq!(
            rpc.list_entities(request(list(), &token))
                .await
                .unwrap_err()
                .code(),
            code
        );
        assert_eq!(
            rpc.delete_entity(request(
                wire::DeleteEntityRequest {
                    namespace: "tenant".into(),
                    path: "unauthorized".into()
                },
                &token
            ))
            .await
            .unwrap_err()
            .code(),
            code
        );
    }
    let valid = sign(claims("operator"));
    let mut duplicate = request(create("unauthorized", EntityKind::Queue), &valid);
    duplicate
        .metadata_mut()
        .append("authorization", format!("Bearer {valid}").parse()?);
    assert_eq!(
        rpc.create_entity(duplicate).await.unwrap_err().code(),
        Code::Unauthenticated
    );
    let mut foreign_create = create("unauthorized", EntityKind::Queue);
    foreign_create.namespace = "foreign".into();
    assert_eq!(
        rpc.create_entity(request(foreign_create, &valid))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut foreign_list = list();
    foreign_list.namespace = "foreign".into();
    assert_eq!(
        rpc.list_entities(request(foreign_list, &valid))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert!(
        rpc.list_entities(request(list(), &valid))
            .await?
            .into_inner()
            .entities
            .is_empty()
    );
    // The same TLS connection is not an authorization cache.
    let mut short = claims("operator");
    let expires = short["iat"].as_u64().unwrap() + 3;
    short["exp"] = serde_json::json!(expires);
    let short = sign(short);
    rpc.list_entities(request(list(), &short)).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        rpc.list_entities(request(list(), &short))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    process.signal(Signal::TERM)?;
    assert_eq!(process.wait().await?, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_preserves_empty_continuing_catalog_pages() -> Result {
    let (root, cert) = fixture()?;
    let (mut process, mut rpc) = Process::start(root.path(), &cert, &[], &[]).await?;
    let token = sign(claims("operator"));
    rpc.create_entity(request(create("events", EntityKind::Topic), &token))
        .await?;
    for index in 0..130 {
        let mut sub = create("", EntityKind::Subscription);
        sub.subscription_topic = Some("events".into());
        sub.subscription_name = Some(format!("s{index:03}"));
        rpc.create_entity(request(sub, &token)).await?;
    }
    let mut page = list();
    page.selection = wire::EntitySelection::Queues as i32;
    let first = rpc
        .list_entities(request(page.clone(), &token))
        .await?
        .into_inner();
    assert!(first.entities.is_empty());
    assert!(first.continuation.is_some());
    page.continuation = first.continuation;
    let second = rpc.list_entities(request(page, &token)).await?.into_inner();
    assert!(second.entities.is_empty());
    assert!(second.continuation.is_none());
    process.signal(Signal::TERM)?;
    assert_eq!(process.wait().await?, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn plaintext_untrusted_ca_and_wrong_hostname_never_reach_entity_service() -> Result {
    let (root, cert) = fixture()?;
    let (mut process, _) = Process::start(root.path(), &cert, &[], &[]).await?;
    let other = generate_simple_self_signed(vec!["localhost".into()])?
        .cert
        .pem();
    assert!(client(process.address, &other, "localhost").await.is_err());
    assert!(
        client(process.address, &cert, "wrong.example")
            .await
            .is_err()
    );
    // A trusted TLS peer still must negotiate h2; HTTP/1.1 is not an admin mode.
    use base64::Engine as _;
    use rustls::{
        ClientConfig, RootCertStore,
        pki_types::{CertificateDer, ServerName},
    };
    use std::sync::Arc;
    let der = base64::engine::general_purpose::STANDARD.decode(
        cert.lines()
            .filter(|line| !line.starts_with("---"))
            .collect::<String>(),
    )?;
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(der))?;
    let mut tls =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tcp = tokio::net::TcpStream::connect(process.address).await?;
    assert!(
        tokio_rustls::TlsConnector::from(Arc::new(tls))
            .connect(ServerName::try_from("localhost")?, tcp)
            .await
            .is_err()
    );
    let plain = tonic::transport::Endpoint::from_shared(format!("http://{}", process.address))?
        .connect()
        .await;
    if let Ok(channel) = plain {
        assert!(
            EntityServiceClient::new(channel)
                .list_entities(request(list(), &sign(claims("operator"))))
                .await
                .is_err()
        );
    }
    // Bad peers do not retire the healthy TLS listener.
    let mut good = client(process.address, &cert, "localhost").await?;
    good.list_entities(request(list(), &sign(claims("operator"))))
        .await?;
    process.signal(Signal::TERM)?;
    assert_eq!(process.wait().await?, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn both_transports_share_one_nonresetting_process_shutdown_budget() -> Result {
    let (root, cert) = fixture()?;
    let (mut process, mut rpc) = Process::start(
        root.path(),
        &cert,
        &["--shutdown-timeout-secs", "2"],
        &[("SWITCHYARD_TEST_STALL_TIMER_MILLIS", "10000")],
    )
    .await?;
    let token = sign(claims("operator"));
    process.signal(Signal::TERM)?;
    let began = Instant::now();
    let deadline = began + WAIT;
    loop {
        if fs::read_to_string(root.path().join("process.log"))?.contains("stage=protocol-cleanup") {
            break;
        }
        if Instant::now() > deadline {
            return Err("protocol cleanup did not finish".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(rpc.list_entities(request(list(), &token)).await.is_err());
    assert!(
        tokio::net::TcpStream::connect(process.address)
            .await
            .is_err()
    );
    assert!(tokio::net::TcpStream::connect(process.amqp).await.is_err());
    process.signal(Signal::INT)?;
    assert_eq!(
        process.wait().await?,
        i32::from(server::SHUTDOWN_EXPIRED_EXIT_CODE)
    );
    // Log evidence proves a single budget/stage sequence, independent of host latency.
    let log = fs::read_to_string(root.path().join("process.log"))?;
    assert!(log.contains("shutdown already in progress"), "{log}");
    assert!(
        log.contains("budget expired during the timer stage"),
        "{log}"
    );
    assert_eq!(log.matches("stage=listener-admission").count(), 1);
    assert_eq!(log.matches("stage=protocol-cleanup").count(), 1);
    assert!(!log.contains("stage=broker"));
    Ok(())
}

#[test]
fn admin_security_configuration_refuses_before_durable_open_and_bind_failure_leaks_no_port()
-> Result {
    let (root, _) = fixture()?;
    let run = |extra: &[&str]| {
        bounded_output(
            Command::new(env!("CARGO_BIN_EXE_switchyard"))
                .args(["--storage", "fjall", "--data-dir"])
                .arg(root.path().join("state"))
                .args(extra),
        )
    };
    for args in [
        vec!["--admin-listen", "127.0.0.1:0"],
        vec!["--admin-jwt-policy-file", "missing"],
    ] {
        assert!(!run(&args)?.status.success());
        assert!(!root.path().join("state").exists());
    }
    let cert = root.path().join("cert.pem");
    let key = root.path().join("key.pem");
    let policy = root.path().join("policy.json");
    let mut args = vec![
        "--admin-listen",
        "127.0.0.1:0",
        "--admin-tls-certificate",
        cert.to_str().unwrap(),
        "--admin-tls-private-key",
        key.to_str().unwrap(),
        "--admin-jwt-policy-file",
        policy.to_str().unwrap(),
    ];
    fs::write(&policy, "not JSON")?;
    assert!(!run(&args)?.status.success());
    assert!(!root.path().join("state").exists());
    fs::write(&policy, vec![b' '; 64 * 1024 + 2])?;
    assert!(!run(&args)?.status.success());
    assert!(!root.path().join("state").exists());
    fs::write(&policy, admin_tls::policy().to_string())?;
    fs::write(&key, "not a TLS key")?;
    assert!(!run(&args)?.status.success());
    assert!(!root.path().join("state").exists());
    let CertifiedKey {
        cert: newcert,
        key_pair,
    } = generate_simple_self_signed(vec!["localhost".into()])?;
    fs::write(&key, key_pair.serialize_pem())?;
    // Mismatched key/cert is also refused before state opens.
    assert!(!run(&args)?.status.success());
    assert!(!root.path().join("state").exists());
    fs::write(&cert, newcert.pem())?;
    // Native opt-in never bypasses any production quorum/storage guard.
    let sas_key = root.path().join("sas-key.txt");
    fs::write(&sas_key, "test-only-shared-access-key")?;
    let refused = bounded_output(
        Command::new(env!("CARGO_BIN_EXE_switchyard"))
            .args([
                "--mode",
                "production",
                "--voters",
                "3",
                "--storage",
                "fjall",
                "--namespace",
                "tenant",
                "--data-dir",
            ])
            .arg(root.path().join("state"))
            .args(&args)
            .arg("--tls-certificate")
            .arg(&cert)
            .arg("--tls-private-key")
            .arg(&key)
            .args([
                "--shared-access-key-name",
                "test-rule",
                "--shared-access-key-file",
            ])
            .arg(&sas_key),
    )?;
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("requires quorum replication"));
    assert!(!root.path().join("state").exists());
    let occupied = TcpListener::bind("127.0.0.1:0")?;
    let occupied_addr = occupied.local_addr()?.to_string();
    args[1] = &occupied_addr;
    let amqp_addr = address().to_string();
    args.extend(["--listen", &amqp_addr]);
    assert!(!run(&args)?.status.success());
    assert!(
        TcpListener::bind(&amqp_addr).is_ok(),
        "the first bound socket was leaked"
    );
    // Startup teardown joined the broker and closed its store.
    drop(storage::FjallStore::open(root.path().join("state"))?);
    Ok(())
}
