//! The CLI preserves opaque progress when hidden topology consumes a queue page.

use std::{
    error::Error,
    io,
    process::{Output, Stdio},
    time::Duration,
};

use domain::{EntityPath, NamespaceName, QueueConfig, StateMachine, codec, keys};
use serde_json::Value;
use server::{Broker, LocalProposer, ManualClock, NativeAdminListener, NativeAdminService};
use storage::{MemoryStore, StateStore, WriteBatch};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::TcpListener,
    process::Command,
    task::JoinHandle,
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct Node {
    broker: Broker,
    store: MemoryStore,
    clock: ManualClock,
    arguments: Vec<String>,
    listener: JoinHandle<()>,
}

impl Node {
    async fn start() -> TestResult<Self> {
        let store = MemoryStore::default();
        let namespace = NamespaceName::new("tenant")?;
        let mut batch = WriteBatch::default();
        for index in 0_usize..80 {
            let base = format!("a/subscriptions/Member{:04}", index / 2);
            let path = if index.is_multiple_of(2) {
                base
            } else {
                format!("{base}/$deadletterqueue")
            };
            batch.push_put(
                keys::queue_config(&namespace, &EntityPath::new(path)?),
                codec::encode(&QueueConfig::default())?,
            );
        }
        store.apply(batch)?;
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let admin = NativeAdminListener::new(NativeAdminService::new(broker.handle(), namespace));
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let arguments = vec![
            "--namespace".into(),
            "tenant".into(),
            "--allow-insecure".into(),
            "--endpoint".into(),
            format!("http://{}", socket.local_addr()?),
        ];
        let listener = tokio::spawn(async move {
            let _ = admin.serve(socket).await;
        });
        Ok(Self {
            broker,
            store,
            clock,
            arguments,
            listener,
        })
    }

    async fn json(&self, command: &[&str]) -> TestResult<Value> {
        let mut arguments = self.arguments.clone();
        arguments.extend(command.iter().map(|value| (*value).to_owned()));
        let output = run(arguments).await?;
        assert!(
            output.status.success(),
            "CLI failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        Ok(serde_json::from_slice(&output.stdout)?)
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

async fn run(arguments: Vec<String>) -> TestResult<Output> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_switchyardctl"))
        .args(arguments)
        .env("TOKIO_WORKER_THREADS", "2")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child.stdout.take().expect("stdout");
    let stderr = child.stderr.take().expect("stderr");
    let collected = timeout(Duration::from_secs(30), async {
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
            Err(io::Error::new(io::ErrorKind::TimedOut, "CLI paging timed out").into())
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
            "CLI output exceeds the test limit",
        ));
    }
    Ok(output)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_queue_pages_keep_changing_tokens_until_a_visible_queue_is_reached() -> TestResult {
    timeout(Duration::from_secs(60), async {
        let node = Node::start().await?;
        let created = node.json(&["queue", "create", "z-visible"]).await?;
        assert_eq!(created["path"], "z-visible");
        let before = node.store.snapshot()?;
        let applied = node.broker.handle().last_applied_blocking()?;
        node.clock.set(0);
        let first = node.json(&["queue", "list", "--page-size", "1"]).await?;
        assert_eq!(first["entities"].as_array().expect("entities").len(), 0);
        let first_token = first["next_page_token"].as_str().expect("token");
        assert!(first_token.starts_with("queue.scan.v1."));
        let second = node
            .json(&[
                "queue",
                "list",
                "--page-size",
                "1",
                "--page-token",
                first_token,
            ])
            .await?;
        assert_eq!(second["entities"].as_array().expect("entities").len(), 0);
        let second_token = second["next_page_token"].as_str().expect("token");
        assert!(second_token.starts_with("queue.scan.v1."));
        assert_ne!(first_token, second_token);
        let last = node
            .json(&[
                "queue",
                "list",
                "--page-size",
                "1",
                "--page-token",
                second_token,
            ])
            .await?;
        assert_eq!(last["entities"].as_array().expect("entities").len(), 1);
        assert_eq!(last["entities"][0], created);
        assert_eq!(last["next_page_token"], "");
        assert_eq!(
            node.json(&["queue", "list", "--page-size", "1"]).await?,
            first
        );
        assert_eq!(node.store.snapshot()?, before);
        assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
        Ok(())
    })
    .await?
}
