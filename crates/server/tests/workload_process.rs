//! Real process opt-ins, independent socket proofs and immutable restart snapshots.
#![cfg(all(feature = "workload-tls", target_os = "linux"))]
#[allow(dead_code)]
mod admin_tls;
mod workload;
use admin_api::wire::{self, EntityKind};
use sha2::{Digest, Sha256};
use std::{
    fs,
    net::{SocketAddr, TcpListener},
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::{Code, Request};
use workload::*;

fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
struct Process {
    child: Child,
    amqp: SocketAddr,
    native: SocketAddr,
    atom: SocketAddr,
}
impl Drop for Process {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}
impl Process {
    async fn start(root: &Path, wss: bool, mixed: bool) -> Result<Self> {
        let (amqp, native, atom) = (address(), address(), address());
        let log = fs::File::create(root.join("process.log"))?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_switchyard"));
        command
            .args([
                "--namespace",
                "tenant.servicebus.windows.net",
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
            .arg(root.join("state"));
        for prefix in ["", "admin-", "atom-"] {
            command
                .arg(format!("--{prefix}tls-certificate"))
                .arg(root.join("server.pem"));
            command
                .arg(format!("--{prefix}tls-private-key"))
                .arg(root.join("server.key"));
        }
        for (flag, file) in [
            ("--amqp-workload-policy-file", "amqp.json"),
            ("--admin-workload-policy-file", "native.json"),
            ("--atom-workload-policy-file", "atom.json"),
        ] {
            command.arg(flag).arg(root.join(file));
        }
        if wss {
            command.args(["--transport", "amqp-websockets"]);
        }
        if mixed {
            for flag in [
                "--jwt-policy-file",
                "--admin-jwt-policy-file",
                "--atom-jwt-policy-file",
            ] {
                command.arg(flag).arg(root.join("jwt.json"));
            }
        }
        let child = command
            .env("RUST_LOG", "info")
            .env("TOKIO_WORKER_THREADS", "2")
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        let mut this = Self {
            child,
            amqp,
            native,
            atom,
        };
        let until = tokio::time::Instant::now() + WAIT;
        loop {
            if let Some(status) = this.child.try_wait()? {
                return Err(format!(
                    "startup {status}: {}",
                    fs::read_to_string(root.join("process.log"))?
                )
                .into());
            }
            if fs::read_to_string(root.join("process.log"))?.contains("accepting AMQP connections")
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "process failed to bind"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(this)
    }
    async fn stop(mut self) -> Result {
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(self.child.id() as i32).unwrap(),
            rustix::process::Signal::TERM,
        )?;
        let status = tokio::task::block_in_place(|| self.child.wait())?;
        assert!(
            status.success(),
            "only an actual successful owner join is a drain: {status}"
        );
        Ok(())
    }
}
fn files(root: &Path, f: &Fixture, policy: &serde_json::Value) -> Result {
    fs::write(root.join("server.pem"), f.server.cert.pem())?;
    fs::write(root.join("server.key"), f.server.key.serialize_pem())?;
    fs::write(root.join("jwt.json"), admin_tls::policy().to_string())?;
    policies(root, policy)
}
fn policies(root: &Path, policy: &serde_json::Value) -> Result {
    for name in ["amqp.json", "native.json", "atom.json"] {
        fs::write(root.join(name), policy.to_string())?;
    }
    Ok(())
}
fn create(name: &str, kind: EntityKind) -> wire::CreateEntityRequest {
    wire::CreateEntityRequest {
        namespace: "tenant".into(),
        path: name.into(),
        kind: kind as i32,
        ..Default::default()
    }
}
async fn seed(f: &Fixture, p: &Process) -> Result {
    let mut rpc = f.rpc(p.native, Some(&f.manager)).await?;
    rpc.list_entities(Request::new(admin_tls::list())).await?; // authenticated readiness
    rpc.create_entity(Request::new(create("orders", EntityKind::Queue)))
        .await?;
    rpc.create_entity(Request::new(create("events", EntityKind::Topic)))
        .await?;
    let mut sub = create("", EntityKind::Subscription);
    sub.subscription_topic = Some("events".into());
    sub.subscription_name = Some("consumer".into());
    rpc.create_entity(Request::new(sub)).await?;
    Ok(())
}
async fn amqp_send(f: &Fixture, p: &Process, wss: bool, id: &Identity) -> Result {
    let mut connection = f.amqp(p.amqp, Some(id), wss).await?;
    let mut session = amqp::ClientSession::begin(&mut connection).await?;
    let mut sender = amqp::ClientSender::attach(&mut session, "proof", "orders").await?;
    assert!(matches!(
        sender
            .send(
                amqp::Message::builder()
                    .body(amqp::Body::Data(vec![b"actual".to_vec().into()]))
                    .build()
            )
            .await?,
        amqp::Outcome::Accepted(_)
    ));
    sender.close().await?;
    connection.close().await?;
    connection.on_close().await;
    Ok(())
}
async fn refused(f: &Fixture, p: &Process, id: &Identity) -> Result {
    if let Ok(mut rpc) = f.rpc(p.native, Some(id)).await {
        assert!(
            rpc.list_entities(Request::new(admin_tls::list()))
                .await
                .is_err()
        );
    }
    assert!(f.atom(p.atom, Some(id), "", SUB).await.is_err());
    assert!(f.amqp(p.amqp, Some(id), false).await.is_err());
    Ok(())
}

async fn cbs(session: &mut amqp::ClientSession, token: String) -> Result {
    use amqp::{
        ApplicationProperties, Body, ClientReceiver, ClientSender, Message, Properties, Value,
    };
    let mut requests =
        ClientSender::attach(session, "optional-cbs", protocol_amqp::CBS_NODE).await?;
    let mut replies = ClientReceiver::builder()
        .name("optional-cbs-reply")
        .source(protocol_amqp::CBS_NODE)
        .target("reply")
        .attach(session)
        .await?;
    requests
        .send(
            Message::builder()
                .properties(Properties {
                    message_id: Some("credential".into()),
                    reply_to: Some("reply".into()),
                    ..Default::default()
                })
                .application_properties(
                    ApplicationProperties::builder()
                        .insert("operation", "put-token")
                        .insert("type", "jwt")
                        .insert("name", ROOT)
                        .build(),
                )
                .body(Body::Value(Value::String(token)))
                .build(),
        )
        .await?;
    let reply = replies.recv().await?;
    assert_eq!(
        reply
            .message()
            .application_properties
            .as_ref()
            .unwrap()
            .get("status-code"),
        Some(&Value::Int(202))
    );
    replies.accept(&reply).await?;
    requests.close().await?;
    replies.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_process_tcp_wss_native_and_supported_atom_use_independent_proofs_and_bearer_precedence()
-> Result {
    for wss in [false, true] {
        let dir = tempfile::tempdir()?;
        let f = Fixture::new();
        files(dir.path(), &f, &f.policy(&[&f.old], false, 60))?;
        let p = Process::start(dir.path(), wss, true).await?;
        seed(&f, &p).await?;
        amqp_send(&f, &p, wss, &f.manager).await?;
        amqp_send(&f, &p, wss, &f.sender).await?;
        let mut scoped = f.amqp(p.amqp, Some(&f.entity), wss).await?;
        let mut session = amqp::ClientSession::begin(&mut scoped).await?;
        let mut denied = amqp::ClientSender::attach(&mut session, "no-sibling", "orders").await?;
        assert!(
            denied
                .send(
                    amqp::Message::builder()
                        .body(amqp::Body::Data(vec![b"denied".to_vec().into()]))
                        .build()
                )
                .await
                .is_err()
        );
        scoped.close().await?;
        scoped.on_close().await;
        let mut entity = f.rpc(p.native, Some(&f.entity)).await?;
        assert_eq!(
            entity
                .list_entities(Request::new(admin_tls::list()))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let body = f.atom(p.atom, Some(&f.entity), "", SUB).await?;
        assert!(body.starts_with("HTTP/1.1 200"), "{body}");
        assert!(body.contains("SubscriptionDescription"));
        assert!(body.contains("schemas.microsoft.com/netservices/2010/10/servicebus/connect"));
        assert!(
            f.atom(
                p.atom,
                Some(&f.entity),
                "",
                "/events/Subscriptions/sibling?api-version=2024-05&enrich=False"
            )
            .await?
            .starts_with("HTTP/1.1 403")
        );
        let mut sender = f.rpc(p.native, Some(&f.sender)).await?;
        assert_eq!(
            sender
                .list_entities(Request::new(admin_tls::list()))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let token = admin_tls::sign(admin_tls::claims("operator"));
        sender
            .list_entities(admin_tls::request(admin_tls::list(), &token))
            .await?;
        assert!(
            f.atom(
                p.atom,
                Some(&f.sender),
                &format!("Authorization: Bearer {token}\r\n"),
                SUB
            )
            .await?
            .starts_with("HTTP/1.1 200")
        );
        let mut manager = f.rpc(p.native, Some(&f.manager)).await?;
        for subject in ["sender", "entity"] {
            let limited = admin_tls::sign(admin_tls::claims(subject));
            assert_eq!(
                manager
                    .list_entities(admin_tls::request(admin_tls::list(), &limited))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied
            );
            assert!(
                f.atom(
                    p.atom,
                    Some(&f.manager),
                    &format!("Authorization: Bearer {limited}\r\n"),
                    SUB
                )
                .await?
                .starts_with("HTTP/1.1 403")
            );
        }
        for value in ["Bearer invalid", "Basic ignored", "Bearer ", "Bearer a b"] {
            let mut request = Request::new(create("must-not-exist", EntityKind::Queue));
            request
                .metadata_mut()
                .insert("authorization", value.parse()?);
            assert_eq!(
                manager.create_entity(request).await.unwrap_err().code(),
                Code::Unauthenticated
            );
            assert!(
                f.atom(
                    p.atom,
                    Some(&f.manager),
                    &format!("Authorization: {value}\r\n"),
                    SUB
                )
                .await?
                .starts_with("HTTP/1.1 401")
            );
        }
        let mut request = admin_tls::request(admin_tls::list(), &token);
        request
            .metadata_mut()
            .append("authorization", "Bearer invalid".parse()?);
        assert_eq!(
            manager.list_entities(request).await.unwrap_err().code(),
            Code::Unauthenticated
        );
        assert!(
            f.atom(
                p.atom,
                Some(&f.manager),
                &format!("Authorization: Bearer {token}\r\nAuthorization: Bearer invalid\r\n"),
                SUB
            )
            .await?
            .starts_with("HTTP/1.1 401")
        );
        let forged = "X-Workload-Principal: spiffe://fixture.test/manager\r\n";
        assert!(
            f.atom(p.atom, Some(&f.sender), forged, SUB)
                .await?
                .starts_with("HTTP/1.1 403")
        );
        assert!(
            f.atom(
                p.atom,
                Some(&f.manager),
                "",
                "/events/Subscriptions/consumer?api-version=2024-05&enrich=True"
            )
            .await?
            .starts_with("HTTP/1.1 400")
        );
        if let Ok(mut absent) = f.rpc(p.native, None).await {
            assert!(
                absent
                    .list_entities(admin_tls::request(admin_tls::list(), &token))
                    .await
                    .is_err()
            );
        }
        assert!(
            f.atom(
                p.atom,
                None,
                &format!("Authorization: Bearer {token}\r\n"),
                SUB
            )
            .await
            .is_err()
        );
        p.stop().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn optional_absence_needs_real_bearer_and_present_certificate_still_caps_mixed_connections()
-> Result {
    let dir = tempfile::tempdir()?;
    let f = Fixture::new();
    files(dir.path(), &f, &f.policy(&[&f.old], true, IO_LEASE_SECONDS))?;
    let p = Process::start(dir.path(), false, true).await?;
    seed(&f, &p).await?;
    let token = admin_tls::sign(admin_tls::claims("operator"));
    let mut anonymous = f.amqp(p.amqp, None, false).await?;
    let mut session = amqp::ClientSession::begin(&mut anonymous).await?;
    let mut no_token =
        amqp::ClientSender::attach(&mut session, "no-optional-token", "orders").await?;
    let message = || {
        amqp::Message::builder()
            .body(amqp::Body::Data(vec![b"independent-token".to_vec().into()]))
            .build()
    };
    assert!(no_token.send(message()).await.is_err());
    cbs(&mut session, token.clone()).await?;
    let mut token_sender =
        amqp::ClientSender::attach(&mut session, "real-optional-token", "orders").await?;
    assert!(matches!(
        token_sender.send(message()).await?,
        amqp::Outcome::Accepted(_)
    ));
    let mut absent = f.rpc(p.native, None).await?;
    assert_eq!(
        absent
            .list_entities(Request::new(admin_tls::list()))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    assert!(
        f.atom(p.atom, None, "", SUB)
            .await?
            .starts_with("HTTP/1.1 401")
    );
    assert!(
        f.atom(
            p.atom,
            None,
            &format!("Authorization: Bearer {token}\r\n"),
            SUB
        )
        .await?
        .starts_with("HTTP/1.1 200")
    );
    let ordinary = admin_api::atom::https::Limits::default();
    for deadline in [
        ordinary.handshake_timeout,
        ordinary.header_timeout,
        ordinary.body_timeout,
        ordinary.request_timeout,
        ordinary.connection_timeout,
    ] {
        assert!(LEASE_IO_BOUND < deadline);
    }
    let prefix = format!(
        "GET {SUB} HTTP/1.1\r\nHost: endpoint.fixture.test\r\nAuthorization: Bearer {token}\r\nX-Held: "
    );
    // TLS 1.2 completion observes the server's post-client-verification Finished.
    // Absolute bounds start BEFORE dialing, so delayed observation cannot let
    // the process's ordinary 10-second header timeout impersonate a lease close.
    let control_deadline = tokio::time::Instant::now() + LEASE_IO_BOUND;
    let mut control =
        tokio::time::timeout_at(control_deadline, f.tls12(p.atom, None, b"http/1.1")).await??;
    tokio::time::timeout_at(control_deadline, control.write_all(prefix.as_bytes())).await??;
    let native_deadline = tokio::time::Instant::now() + LEASE_IO_BOUND;
    let mut certified =
        tokio::time::timeout_at(native_deadline, f.tls12(p.native, Some(&f.manager), b"h2"))
            .await??;
    let atom_deadline = tokio::time::Instant::now() + LEASE_IO_BOUND;
    let mut partial = tokio::time::timeout_at(
        atom_deadline,
        f.tls12(p.atom, Some(&f.manager), b"http/1.1"),
    )
    .await??;
    tokio::time::timeout_at(atom_deadline, partial.write_all(prefix.as_bytes())).await??;
    // Read from the exact sockets concurrently; a prior wait cannot silently
    // enlarge either bound. The HTTP prefix is incomplete but not malformed.
    let mut native_bytes = Vec::new();
    let mut atom_bytes = Vec::new();
    let (native_closed, atom_closed) = tokio::join!(
        async {
            let result =
                tokio::time::timeout_at(native_deadline, certified.read_to_end(&mut native_bytes))
                    .await;
            (result, tokio::time::Instant::now())
        },
        async {
            let result =
                tokio::time::timeout_at(atom_deadline, partial.read_to_end(&mut atom_bytes)).await;
            (result, tokio::time::Instant::now())
        },
    );
    let _ = native_closed.0?;
    let _ = atom_closed.0?;
    // Timeout polls can observe an already-ready read after their deadline when
    // the runtime was delayed. Such an overdue success is NOT an expiry proof.
    assert!(native_closed.1 < native_deadline);
    assert!(atom_closed.1 < atom_deadline);
    assert!(
        atom_bytes.is_empty(),
        "no ordinary HTTP error may impersonate certificate retirement"
    );
    // The identical prefix on an absent-certificate socket must stay open beyond
    // the lease-specific bound, then complete as an authenticated supported Get.
    let mut byte = [0];
    assert!(
        tokio::time::timeout_at(control_deadline, control.read(&mut byte))
            .await
            .is_err(),
        "the control must neither self-close nor trigger an early HTTP error"
    );
    control.write_all(b"value\r\n\r\n").await?;
    let mut reply = Vec::new();
    tokio::time::timeout(WAIT, control.read_to_end(&mut reply)).await??;
    assert!(String::from_utf8(reply)?.starts_with("HTTP/1.1 200"));
    absent
        .list_entities(admin_tls::request(admin_tls::list(), &token))
        .await?;
    assert!(matches!(
        token_sender.send(message()).await?,
        amqp::Outcome::Accepted(_)
    ));
    token_sender.close().await?;
    anonymous.close().await?;
    anonymous.on_close().await;
    let mut healthy = f.rpc(p.native, Some(&f.manager)).await?;
    healthy
        .list_entities(Request::new(admin_tls::list()))
        .await?;
    let untrusted = client_identity(&root(), "manager", None);
    if let Ok(mut bad) = f.rpc(p.native, Some(&untrusted)).await {
        assert!(
            bad.list_entities(admin_tls::request(admin_tls::list(), &token))
                .await
                .is_err()
        );
    }
    assert!(
        f.atom(
            p.atom,
            Some(&untrusted),
            &format!("Authorization: Bearer {token}\r\n"),
            SUB
        )
        .await
        .is_err()
    );
    p.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_snapshots_overlap_remove_roots_revoke_leaf_and_principal_and_replace_mappings()
-> Result {
    let dir = tempfile::tempdir()?;
    let f = Fixture::new();
    let old = f.policy(&[&f.old], false, 60);
    files(dir.path(), &f, &old)?;
    let p = Process::start(dir.path(), false, false).await?;
    seed(&f, &p).await?;
    let mut retained = f.rpc(p.native, Some(&f.manager)).await?;
    retained
        .list_entities(Request::new(admin_tls::list()))
        .await?;
    let overlap = f.policy(&[&f.old, &f.new], false, 60);
    policies(dir.path(), &overlap)?;
    refused(&f, &p, &f.next).await?; // files alone do not add a root
    retained
        .list_entities(Request::new(admin_tls::list()))
        .await?;
    p.stop().await?;
    let p = Process::start(dir.path(), false, false).await?;
    for id in [&f.manager, &f.next] {
        let mut rpc = f.rpc(p.native, Some(id)).await?;
        rpc.list_entities(Request::new(admin_tls::list())).await?;
        amqp_send(&f, &p, false, id).await?;
        assert!(
            f.atom(p.atom, Some(id), "", SUB)
                .await?
                .starts_with("HTTP/1.1 200")
        );
    }
    let only_new = f.policy(&[&f.new], false, 60);
    policies(dir.path(), &only_new)?;
    p.stop().await?;
    let p = Process::start(dir.path(), false, false).await?;
    assert!(
        retained
            .list_entities(Request::new(admin_tls::list()))
            .await
            .is_err()
    ); // no old connection/grant transferred
    refused(&f, &p, &f.manager).await?;
    let mut live = f.rpc(p.native, Some(&f.next)).await?;
    live.list_entities(Request::new(admin_tls::list())).await?;
    let mut revoked = only_new.clone();
    revoked["revoked_leaf_sha256"] =
        serde_json::json!([format!("{:x}", Sha256::digest(f.next.cert.der()))]);
    policies(dir.path(), &revoked)?;
    live.list_entities(Request::new(admin_tls::list())).await?; // files alone do not revoke
    p.stop().await?;
    let p = Process::start(dir.path(), false, false).await?;
    refused(&f, &p, &f.next).await?;
    let reissued = client_identity(&f.new, "manager", None);
    let mut replacement = f.rpc(p.native, Some(&reissued)).await?;
    replacement
        .list_entities(Request::new(admin_tls::list()))
        .await?;
    amqp_send(&f, &p, false, &reissued).await?;
    assert!(
        f.atom(p.atom, Some(&reissued), "", SUB)
            .await?
            .starts_with("HTTP/1.1 200")
    );
    p.stop().await?;
    let mut removed = only_new.clone();
    removed["bindings"]
        .as_array_mut()
        .unwrap()
        .retain(|b| b["principal"] != "spiffe://fixture.test/manager");
    policies(dir.path(), &removed)?;
    let p = Process::start(dir.path(), false, false).await?;
    refused(&f, &p, &reissued).await?;
    p.stop().await?;
    let mut mapped = only_new;
    mapped["bindings"][0]["permissions"] = serde_json::json!(["send"]);
    policies(dir.path(), &mapped)?;
    let p = Process::start(dir.path(), false, false).await?;
    let mut rpc = f.rpc(p.native, Some(&f.next)).await?;
    assert_eq!(
        rpc.list_entities(Request::new(admin_tls::list()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert!(
        f.atom(p.atom, Some(&f.next), "", SUB)
            .await?
            .starts_with("HTTP/1.1 403")
    );
    amqp_send(&f, &p, false, &f.next).await?;
    p.stop().await?;
    Ok(())
}

#[test]
fn invalid_candidates_refuse_before_store_creation_and_do_not_modify_existing_state() -> Result {
    let dir = tempfile::tempdir()?;
    let f = Fixture::new();
    let mut policy = f.policy(&[&f.old], false, 60);
    files(dir.path(), &f, &policy)?;
    let state = dir.path().join("not-created");
    let run = |extra: &[&str]| -> Result {
        let output = Command::new(env!("CARGO_BIN_EXE_switchyard"))
            .args([
                "--namespace",
                "tenant",
                "--listen",
                "127.0.0.1:0",
                "--storage",
                "fjall",
                "--data-dir",
            ])
            .arg(&state)
            .args(extra)
            .output()?;
        assert!(!output.status.success());
        if state.is_file() {
            assert_eq!(fs::read(&state)?, b"preserved-not-a-store");
            assert!(String::from_utf8_lossy(&output.stderr).contains("workload"));
        } else {
            assert!(
                !state.exists(),
                "invalid configuration opened durable state"
            );
        }
        Ok(())
    };
    let text = |name: &str| dir.path().join(name).to_str().unwrap().to_owned();
    let cert = text("server.pem");
    let key = text("server.key");
    let file = text("amqp.json");
    for flag in [
        "--amqp-workload-policy-file",
        "--admin-workload-policy-file",
        "--atom-workload-policy-file",
    ] {
        run(&[flag, &file])?;
    }
    policy["client_auth"] = serde_json::json!("optional");
    policies(dir.path(), &policy)?;
    run(&[
        "--amqp-workload-policy-file",
        &file,
        "--tls-certificate",
        &cert,
        "--tls-private-key",
        &key,
    ])?;
    for (kind, flag) in [
        ("native", "--admin-workload-policy-file"),
        ("atom", "--atom-workload-policy-file"),
    ] {
        let policy = text(&format!("{kind}.json"));
        let prefix = if kind == "native" { "admin" } else { "atom" };
        run(&[
            flag,
            &policy,
            &format!("--{prefix}-listen"),
            "127.0.0.1:0",
            &format!("--{prefix}-tls-certificate"),
            &cert,
            &format!("--{prefix}-tls-private-key"),
            &key,
        ])?;
    }
    for bytes in [
        vec![b'x'; 256 * 1024 + 1],
        b"{not valid}".to_vec(),
        vec![0xff],
    ] {
        fs::write(&file, bytes)?;
        run(&[
            "--amqp-workload-policy-file",
            &file,
            "--tls-certificate",
            &cert,
            "--tls-private-key",
            &key,
        ])?;
    }
    let valid = f.policy(&[&f.old], false, 60);
    policies(dir.path(), &valid)?;
    fs::write(&key, vec![b'x'; 32 * 1024 + 1])?;
    run(&[
        "--amqp-workload-policy-file",
        &file,
        "--tls-certificate",
        &cert,
        "--tls-private-key",
        &key,
    ])?;
    fs::write(&state, b"preserved-not-a-store")?;
    run(&[
        "--amqp-workload-policy-file",
        &file,
        "--tls-certificate",
        &cert,
        "--tls-private-key",
        &key,
    ])?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_endpoint_files_never_share_socket_proof_or_trust_and_production_guards_still_refuse()
-> Result {
    let dir = tempfile::tempdir()?;
    let f = Fixture::new();
    let old = f.policy(&[&f.old], false, 60);
    files(dir.path(), &f, &old)?;
    fs::write(
        dir.path().join("native.json"),
        f.policy(&[&f.new], false, 60).to_string(),
    )?;
    let p = Process::start(dir.path(), false, false).await?;
    let mut native = f.rpc(p.native, Some(&f.next)).await?;
    native
        .list_entities(Request::new(admin_tls::list()))
        .await?;
    native
        .create_entity(Request::new(create("orders", EntityKind::Queue)))
        .await?;
    native
        .create_entity(Request::new(create("events", EntityKind::Topic)))
        .await?;
    let mut sub = create("", EntityKind::Subscription);
    sub.subscription_topic = Some("events".into());
    sub.subscription_name = Some("consumer".into());
    native.create_entity(Request::new(sub)).await?;
    if let Ok(mut wrong) = f.rpc(p.native, Some(&f.manager)).await {
        assert!(
            wrong
                .list_entities(Request::new(admin_tls::list()))
                .await
                .is_err()
        );
    }
    assert!(f.amqp(p.amqp, Some(&f.next), false).await.is_err());
    assert!(f.atom(p.atom, Some(&f.next), "", SUB).await.is_err());
    amqp_send(&f, &p, false, &f.manager).await?;
    assert!(
        f.atom(p.atom, Some(&f.manager), "", SUB)
            .await?
            .starts_with("HTTP/1.1 200")
    );
    p.stop().await?;
    let state = dir.path().join("production-not-created");
    let output = Command::new(env!("CARGO_BIN_EXE_switchyard"))
        .args([
            "--namespace",
            "tenant",
            "--mode",
            "production",
            "--voters",
            "3",
            "--storage",
            "fjall",
            "--data-dir",
        ])
        .arg(&state)
        .arg("--tls-certificate")
        .arg(dir.path().join("server.pem"))
        .arg("--tls-private-key")
        .arg(dir.path().join("server.key"))
        .arg("--amqp-workload-policy-file")
        .arg(dir.path().join("amqp.json"))
        .output()?;
    assert!(!output.status.success());
    assert!(!state.exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("quorum replication"));
    Ok(())
}
