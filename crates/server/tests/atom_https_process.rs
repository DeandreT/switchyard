//! Three actual transports in the real process. Atom routes deliberately stay closed.
#![cfg(target_os = "linux")]
#[allow(dead_code)]
mod admin_tls;
#[allow(dead_code)]
mod atom_http;
use admin_api::wire::{self, entity_service_client::EntityServiceClient};
use atom_http::{Identity, TARGET, TestResult, WAIT};
use rustix::process::{Pid, Signal, kill_process};
use std::{
    fs::{self, File},
    net::{SocketAddr, TcpListener},
    path::Path,
    process::{Child, Command, Output, Stdio},
    time::Duration,
};
use tempfile::TempDir;
use tokio::time::Instant;
use tonic::transport::Channel;

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
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Process {
    fn signal(&self, signal: Signal) -> TestResult {
        kill_process(Pid::from_child(&self.child), signal)?;
        Ok(())
    }
    async fn wait(&mut self) -> TestResult<i32> {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status.code().unwrap_or(-1));
            }
            if Instant::now() >= deadline {
                return Err("child exit deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    async fn start(
        root: &Path,
        identity: &Identity,
        extra: &[&str],
        environment: &[(&str, &str)],
    ) -> TestResult<(Self, EntityServiceClient<Channel>)> {
        write_credentials(root, identity)?;
        let (amqp, native, atom) = (address(), address(), address());
        let log = File::create(root.join("process.log"))?;
        let mut command = command(root, amqp, native, atom);
        let child = command
            .args(extra)
            .envs(environment.iter().copied())
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        let mut process = Self {
            child,
            amqp,
            native,
            atom,
        };
        let token = admin_tls::sign(admin_tls::claims("operator"));
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(status) = process.child.try_wait()? {
                return Err(format!(
                    "startup exited {status}: {}",
                    fs::read_to_string(root.join("process.log"))?
                )
                .into());
            }
            if let Ok(mut rpc) = admin_tls::client(native, &identity.ca_pem, "localhost").await
                && rpc
                    .list_entities(admin_tls::request(admin_tls::list(), &token))
                    .await
                    .is_ok()
                && atom_http::exchange(atom, identity, &token, TARGET)
                    .await
                    .is_ok_and(|response| response.starts_with("HTTP/1.1 501"))
            {
                return Ok((process, rpc));
            }
            if Instant::now() >= deadline {
                return Err("actual authenticated boundary observation expired".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
fn write_credentials(root: &Path, identity: &Identity) -> TestResult {
    fs::write(root.join("cert.pem"), &identity.chain)?;
    fs::write(root.join("key.pem"), &identity.key)?;
    fs::write(root.join("policy.json"), admin_tls::policy().to_string())?;
    fs::write(root.join("native-cert.pem"), &identity.chain)?;
    fs::write(root.join("native-key.pem"), &identity.key)?;
    fs::write(
        root.join("native-policy.json"),
        admin_tls::policy().to_string(),
    )?;
    Ok(())
}
fn command(root: &Path, amqp: SocketAddr, native: SocketAddr, atom: SocketAddr) -> Command {
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
        .arg(root.join("state"));
    for (flag, file) in [
        ("--admin-tls-certificate", "native-cert.pem"),
        ("--atom-tls-certificate", "cert.pem"),
        ("--admin-tls-private-key", "native-key.pem"),
        ("--atom-tls-private-key", "key.pem"),
        ("--admin-jwt-policy-file", "native-policy.json"),
        ("--atom-jwt-policy-file", "policy.json"),
    ] {
        command.arg(flag).arg(root.join(file));
    }
    command
}
fn bounded_output(command: &mut Command) -> TestResult<Output> {
    let mut process = Process {
        child: command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
        amqp: address(),
        native: address(),
        atom: address(),
    };
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        if process.child.try_wait()?.is_some() {
            // Keep the guard intact while draining the small startup output.
            use std::io::Read;
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            process
                .child
                .stdout
                .take()
                .unwrap()
                .read_to_end(&mut stdout)?;
            process
                .child
                .stderr
                .take()
                .unwrap()
                .read_to_end(&mut stderr)?;
            return Ok(Output {
                status: process.child.wait()?,
                stdout,
                stderr,
            });
        }
        if std::time::Instant::now() >= deadline {
            return Err("startup refusal deadline".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn three_transports_keep_atom_closed_and_stop_under_one_budget() -> TestResult {
    let root = TempDir::new()?;
    let identity = Identity::new()?;
    let (mut process, mut native) = Process::start(root.path(), &identity, &[], &[]).await?;
    let token = admin_tls::sign(admin_tls::claims("operator"));
    let amqp = amqp::ClientConnection::builder()
        .container_id("atom-process-witness")
        .open(format!("amqp://{}", process.amqp).as_str())
        .await?;
    native
        .create_entity(admin_tls::request(
            wire::CreateEntityRequest {
                namespace: "tenant".into(),
                path: "orders".into(),
                kind: wire::EntityKind::Queue as i32,
                ..Default::default()
            },
            &token,
        ))
        .await?;
    assert!(
        atom_http::exchange(process.atom, &identity, &token, TARGET)
            .await?
            .starts_with("HTTP/1.1 501")
    );
    assert!(
        atom_http::exchange(
            process.atom,
            &identity,
            &token,
            "/$Resources/queues?api-version=2024-05&$skip=100&$top=100"
        )
        .await?
        .starts_with("HTTP/1.1 400")
    );
    assert_eq!(
        native
            .list_entities(admin_tls::request(admin_tls::list(), &token))
            .await?
            .into_inner()
            .entities
            .len(),
        1
    );
    amqp.close().await?;
    process.signal(Signal::TERM)?;
    assert_eq!(process.wait().await?, 0);
    for address in [process.amqp, process.native, process.atom] {
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
    }
    let log = fs::read_to_string(root.path().join("process.log"))?;
    for stage in ["listener-admission", "protocol-cleanup", "timer", "broker"] {
        assert_eq!(log.matches(&format!("stage={stage}")).count(), 1);
    }
    drop(storage::FjallStore::open(root.path().join("state"))?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn three_transport_repeated_signals_do_not_reset_timer_deadline() -> TestResult {
    let root = TempDir::new()?;
    let identity = Identity::new()?;
    let (mut process, _) = Process::start(
        root.path(),
        &identity,
        &["--shutdown-timeout-secs", "4"],
        &[("SWITCHYARD_TEST_STALL_TIMER_MILLIS", "10000")],
    )
    .await?;
    process.signal(Signal::TERM)?;
    let deadline = Instant::now() + WAIT;
    loop {
        if fs::read_to_string(root.path().join("process.log"))?.contains("stage=protocol-cleanup") {
            break;
        }
        if Instant::now() >= deadline {
            return Err("protocol cleanup deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    process.signal(Signal::INT)?;
    assert_eq!(
        process.wait().await?,
        i32::from(server::SHUTDOWN_EXPIRED_EXIT_CODE)
    );
    let log = fs::read_to_string(root.path().join("process.log"))?;
    assert!(log.contains("shutdown already in progress"));
    assert!(log.contains("budget expired during the timer stage"));
    assert_eq!(log.matches("stage=listener-admission").count(), 1);
    assert!(!log.contains("stage=broker"));
    // An overrun is not a successful drain and does not authorize a reopen test.
    Ok(())
}

#[test]
fn atom_configuration_refuses_before_store_and_third_bind_failure_closes_every_prior_socket()
-> TestResult {
    let root = TempDir::new()?;
    let identity = Identity::new()?;
    write_credentials(root.path(), &identity)?;
    let state = root.path().join("state");
    let mut partial = Command::new(env!("CARGO_BIN_EXE_switchyard"));
    partial
        .args(["--storage", "fjall", "--data-dir"])
        .arg(&state)
        .args(["--atom-listen", "127.0.0.1:0"]);
    assert!(!bounded_output(&mut partial)?.status.success());
    assert!(!state.exists());
    for (name, bytes) in [
        ("policy.json", b"invalid".as_slice()),
        ("policy.json", &vec![b' '; 65537]),
        ("key.pem", b"invalid".as_slice()),
    ] {
        fs::write(root.path().join(name), bytes)?;
        assert!(
            !bounded_output(&mut command(root.path(), address(), address(), address()))?
                .status
                .success()
        );
        assert!(!state.exists());
        write_credentials(root.path(), &identity)?;
    }
    let mut production = command(root.path(), address(), address(), address());
    production.args(["--mode", "production", "--voters", "3"]);
    let refused = bounded_output(&mut production)?;
    assert!(String::from_utf8_lossy(&refused.stderr).contains("production mode requires a TLS"));
    assert!(!state.exists());
    production
        .arg("--tls-certificate")
        .arg(root.path().join("cert.pem"))
        .arg("--tls-private-key")
        .arg(root.path().join("key.pem"))
        .arg("--jwt-policy-file")
        .arg(root.path().join("policy.json"));
    let refused = bounded_output(&mut production)?;
    assert!(String::from_utf8_lossy(&refused.stderr).contains("requires quorum replication"));
    assert!(!state.exists());
    let occupied = TcpListener::bind("127.0.0.1:0")?;
    let (amqp, native) = (address(), address());
    assert!(
        !bounded_output(&mut command(
            root.path(),
            amqp,
            native,
            occupied.local_addr()?
        ))?
        .status
        .success()
    );
    assert!(TcpListener::bind(amqp).is_ok());
    assert!(TcpListener::bind(native).is_ok());
    drop(storage::FjallStore::open(&state)?);
    Ok(())
}
