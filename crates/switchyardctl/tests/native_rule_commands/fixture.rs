use std::path::Path;

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{NamespaceName, StateMachine};
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{Broker, LocalProposer, ManualClock, NativeAdminListener, NativeAdminService};
use sha2::Sha256;
use storage::MemoryStore;
use tokio::task::JoinHandle;
use url::form_urlencoded::byte_serialize;

use super::*;

pub(super) struct Node {
    pub(super) broker: Broker,
    pub(super) store: MemoryStore,
    pub(super) clock: ManualClock,
    arguments: Vec<String>,
    listener: JoinHandle<()>,
    pub(super) files: TempDir,
}

impl Node {
    pub(super) async fn start(tls: bool) -> TestResult<Self> {
        let store = MemoryStore::default();
        let clock = ManualClock::at(9_007_199_254_740_993);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let mut service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?);
        let files = TempDir::new()?;
        let mut arguments = vec!["--namespace".into(), "tenant".into()];
        let identity = if tls {
            let policy = SharedAccessPolicy::new([
                SharedAccessRule::new(
                    "manage",
                    ResourceScope::namespace(HOST)?,
                    SharedAccessKey::new(KEY)?,
                    None,
                    PermissionSet::MANAGE,
                )?,
                SharedAccessRule::new(
                    "send",
                    ResourceScope::namespace(HOST)?,
                    SharedAccessKey::new(KEY)?,
                    None,
                    PermissionSet::SEND,
                )?,
            ])?;
            service = service.with_shared_access_policy(policy, HOST)?;
            let CertifiedKey { cert, key_pair } =
                generate_simple_self_signed(vec!["localhost".into()])?;
            let pem = cert.pem();
            let ca = files.path().join("ca.pem");
            let token = files.path().join("token");
            std::fs::write(&ca, &pem)?;
            std::fs::write(&token, format!("{}\n", sas("", "manage")))?;
            arguments.extend([
                "--ca-certificate".into(),
                ca.display().to_string(),
                "--tls-server-name".into(),
                "localhost".into(),
                "--token-file".into(),
                token.display().to_string(),
            ]);
            Some((pem, key_pair.serialize_pem()))
        } else {
            arguments.push("--allow-insecure".into());
            None
        };
        let mut admin = NativeAdminListener::new(service);
        if let Some((certificate, key)) = identity {
            admin = admin.with_tls(certificate.as_bytes(), key.as_bytes())?;
        }
        let socket = timeout(DEADLINE, TcpListener::bind("127.0.0.1:0")).await??;
        arguments.extend([
            "--endpoint".into(),
            format!(
                "{}://{}",
                if tls { "https" } else { "http" },
                socket.local_addr()?
            ),
        ]);
        let listener = tokio::spawn(async move {
            let _ = admin.serve(socket).await;
        });
        Ok(Self {
            broker,
            store,
            clock,
            arguments,
            listener,
            files,
        })
    }

    pub(super) async fn run(&self, command: &[&str]) -> TestResult<Output> {
        let mut arguments = self.arguments.clone();
        arguments.extend(command.iter().map(|value| (*value).into()));
        run(arguments).await
    }

    pub(super) async fn json(&self, command: &[&str]) -> TestResult<Value> {
        let output = self.run(command).await?;
        assert!(
            output.status.success(),
            "CLI failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    pub(super) fn filter(&self, name: &str, value: &Value) -> TestResult<String> {
        let path = self.files.path().join(name);
        std::fs::write(&path, serde_json::to_vec(value)?)?;
        Ok(path.display().to_string())
    }

    pub(super) fn token(&self, path: &str, rule: &str) -> TestResult {
        std::fs::write(
            self.files.path().join("token"),
            format!("{}\n", sas(path, rule)),
        )?;
        Ok(())
    }

    pub(super) async fn topology(&self) -> TestResult {
        self.json(&["topic", "create", "Orders"]).await?;
        for name in ["Alpha", "Beta"] {
            self.json(&["subscription", "create", "Orders", name])
                .await?;
        }
        Ok(())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

pub(super) async fn run(arguments: Vec<String>) -> TestResult<Output> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_switchyardctl"))
        .args(arguments)
        .env("TOKIO_WORKER_THREADS", "2")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child.stdout.take().expect("stdout");
    let stderr = child.stderr.take().expect("stderr");
    let collected = timeout(DEADLINE, async {
        let (status, stdout, stderr) =
            tokio::try_join!(child.wait(), read_output(stdout), read_output(stderr))?;
        Ok::<_, io::Error>(Output {
            status,
            stdout,
            stderr,
        })
    })
    .await;
    match collected {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => {
            let _ = child.kill().await;
            Err(error.into())
        }
        Err(_) => {
            child.kill().await?;
            Err(io::Error::new(io::ErrorKind::TimedOut, "CLI rule process timed out").into())
        }
    }
}

async fn read_output(reader: impl AsyncRead + Unpin) -> io::Result<Vec<u8>> {
    const MAXIMUM: usize = 4 * 1024 * 1024;
    let mut output = Vec::new();
    reader
        .take(MAXIMUM as u64 + 1)
        .read_to_end(&mut output)
        .await?;
    if output.len() > MAXIMUM {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "CLI output exceeds its test bound",
        ));
    }
    Ok(output)
}

pub(super) fn sas(path: &str, rule: &str) -> String {
    let audience = if path.is_empty() {
        format!("amqps://{HOST}")
    } else {
        format!("amqps://{HOST}/{path}")
    };
    let resource: String = byte_serialize(audience.as_bytes()).collect();
    let expiry = 4_102_444_800_u64;
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("HMAC key");
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature: String =
        byte_serialize(STANDARD.encode(mac.finalize().into_bytes()).as_bytes()).collect();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={rule}")
}

pub(super) fn local_arguments(socket: &TcpListener, file: &Path) -> TestResult<Vec<String>> {
    Ok(vec![
        "--endpoint".into(),
        format!("http://{}", socket.local_addr()?),
        "--allow-insecure".into(),
        "--namespace".into(),
        "tenant".into(),
        "rule".into(),
        "create".into(),
        "Orders".into(),
        "Alpha".into(),
        "Local".into(),
        "--filter-file".into(),
        file.display().to_string(),
    ])
}
