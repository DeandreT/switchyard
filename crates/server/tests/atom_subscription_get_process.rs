//! The real startup installation opens only the explicit subscription Get profile.
#![cfg(target_os = "linux")]
#[allow(dead_code)]
mod admin_tls;
#[allow(dead_code)]
mod atom_http;
#[path = "../../admin-api/tests/support/subscription_xml.rs"]
mod validate;
use admin_api::wire;
use atom_http::{Identity, TestResult, WAIT};
use std::{
    fs,
    net::{SocketAddr, TcpListener},
    process::{Child, Command, Stdio},
    time::Duration,
};
use tempfile::TempDir;

struct ChildOwner(Child);
impl Drop for ChildOwner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn spawned_opt_in_installs_only_subscription_definition_get_and_drains() -> TestResult {
    let root = TempDir::new()?;
    let identity = Identity::new()?;
    fs::write(root.path().join("cert.pem"), &identity.chain)?;
    fs::write(root.path().join("key.pem"), &identity.key)?;
    let mut policy = admin_tls::policy();
    policy["bindings"].as_array_mut().unwrap().push(serde_json::json!({"subject":"sub","scope":format!("{}/topic/subscriptions/sub",admin_tls::ROOT),"permissions":["manage"]}));
    fs::write(root.path().join("policy.json"), policy.to_string())?;
    let (amqp, native, atom) = (address(), address(), address());
    let log = fs::File::create(root.path().join("process.log"))?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_switchyard"));
    command
        .args([
            "--namespace",
            "tenant",
            "--listen",
            &amqp.to_string(),
            "--admin-listen",
            &native.to_string(),
            "--atom-listen",
            &atom.to_string(),
            "--storage",
            "fjall",
            "--data-dir",
        ])
        .arg(root.path().join("state"));
    for flag in ["--admin-tls-certificate", "--atom-tls-certificate"] {
        command.arg(flag).arg(root.path().join("cert.pem"));
    }
    for flag in ["--admin-tls-private-key", "--atom-tls-private-key"] {
        command.arg(flag).arg(root.path().join("key.pem"));
    }
    for flag in ["--admin-jwt-policy-file", "--atom-jwt-policy-file"] {
        command.arg(flag).arg(root.path().join("policy.json"));
    }
    let mut child = ChildOwner(
        command
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?,
    );
    let token = admin_tls::sign(admin_tls::claims("operator"));
    let until = tokio::time::Instant::now() + WAIT;
    let mut rpc = loop {
        if let Some(status) = child.0.try_wait()? {
            return Err(format!(
                "startup {status}: {}",
                fs::read_to_string(root.path().join("process.log"))?
            )
            .into());
        }
        if let Ok(mut rpc) = admin_tls::client(native, &identity.ca_pem, "localhost").await
            && rpc
                .list_entities(admin_tls::request(admin_tls::list(), &token))
                .await
                .is_ok()
        {
            break rpc;
        }
        if tokio::time::Instant::now() >= until {
            return Err("startup deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    rpc.create_entity(admin_tls::request(
        wire::CreateEntityRequest {
            namespace: "tenant".into(),
            path: "topic".into(),
            kind: wire::EntityKind::Topic as i32,
            ..Default::default()
        },
        &token,
    ))
    .await?;
    rpc.create_entity(admin_tls::request(
        wire::CreateEntityRequest {
            namespace: "tenant".into(),
            subscription_topic: Some("topic".into()),
            subscription_name: Some("sub".into()),
            kind: wire::EntityKind::Subscription as i32,
            ..Default::default()
        },
        &token,
    ))
    .await?;
    let target = "/ToPiC/Subscriptions/SUB?api-version=2021-05&enrich=False";
    let sub = admin_tls::sign(admin_tls::claims("sub"));
    let missing = atom_http::exchange(
        atom,
        &identity,
        &token,
        "/topic/Subscriptions/missing?api-version=2024-05&enrich=False",
    )
    .await?;
    assert!(missing.starts_with("HTTP/1.1 404"));
    assert_eq!(
        validate::error(missing.split_once("\r\n\r\n").unwrap().1.as_bytes())
            .unwrap()
            .0,
        404
    );
    let reply = atom_http::exchange(atom, &identity, &sub, target).await?;
    assert!(reply.starts_with("HTTP/1.1 200"));
    assert!(
        reply
            .to_ascii_lowercase()
            .contains("content-type: application/atom+xml; charset=utf-8")
    );
    assert_eq!(
        validate::entry(reply.split_once("\r\n\r\n").unwrap().1.as_bytes()).unwrap()[0],
        "sub"
    );
    assert!(
        atom_http::exchange(
            atom,
            &identity,
            &admin_tls::sign(admin_tls::claims("sender")),
            target
        )
        .await?
        .starts_with("HTTP/1.1 403")
    );
    for (target, status) in [
        ("/topic?api-version=2024-05&enrich=False", 501),
        ("/orders?api-version=2024-05&enrich=False", 501),
        ("/topic/Subscriptions?api-version=2024-05&enrich=False", 400),
        (
            "/topic/Subscriptions/sub?api-version=2024-05&enrich=True",
            400,
        ),
        (
            "/topic/Subscriptions/sub/Rules?api-version=2024-05&enrich=False",
            501,
        ),
    ] {
        assert!(
            atom_http::exchange(atom, &identity, &token, target)
                .await?
                .starts_with(&format!("HTTP/1.1 {status}"))
        );
    }
    rustix::process::kill_process(
        rustix::process::Pid::from_child(&child.0),
        rustix::process::Signal::TERM,
    )?;
    let until = tokio::time::Instant::now() + WAIT;
    loop {
        if let Some(status) = child.0.try_wait()? {
            assert!(status.success());
            break;
        }
        if tokio::time::Instant::now() >= until {
            return Err("shutdown deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for address in [amqp, native, atom] {
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
    }
    let log = fs::read_to_string(root.path().join("process.log"))?;
    for stage in ["listener-admission", "protocol-cleanup", "timer", "broker"] {
        assert_eq!(log.matches(&format!("stage={stage}")).count(), 1);
    }
    drop(storage::FjallStore::open(root.path().join("state"))?);
    Ok(())
}
