use std::{
    io,
    net::SocketAddr,
    process::{ExitStatus, Stdio},
    sync::Mutex,
};

use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    sync::Notify,
    task::JoinHandle,
};

use super::*;

const MAX_OUTPUT: usize = 4 * 1024 * 1024;
const ORDINARY: &str = "accepting AMQP connections";
const ADMIN: &str = "accepting native administration connections";
const EXPERIMENTAL: &str = "accepting experimental atomic messaging connections";

#[derive(Default)]
struct Captured {
    bytes: Vec<u8>,
    ended: bool,
    error: Option<&'static str>,
}

#[derive(Default)]
struct Capture {
    data: Mutex<Captured>,
    changed: Notify,
}

impl Capture {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.data.lock().expect("child output").bytes).into_owned()
    }
}

fn capture(
    mut reader: impl AsyncRead + Unpin + Send + 'static,
    shared: Arc<Capture>,
) -> JoinHandle<io::Result<()>> {
    tokio::spawn(async move {
        let mut buffer = [0; 4_096];
        loop {
            let result = reader.read(&mut buffer).await;
            let mut data = shared.data.lock().expect("child output");
            match result {
                Ok(0) => {
                    data.ended = true;
                    shared.changed.notify_waiters();
                    return Ok(());
                }
                Ok(count) => {
                    if data.bytes.len() + count > MAX_OUTPUT {
                        data.ended = true;
                        data.error = Some("child output exceeds the test limit");
                        shared.changed.notify_waiters();
                        return Err(io::Error::other("child output exceeds the test limit"));
                    }
                    data.bytes.extend_from_slice(&buffer[..count]);
                }
                Err(error) => {
                    data.ended = true;
                    data.error = Some("child output could not be read");
                    shared.changed.notify_waiters();
                    return Err(error);
                }
            }
            shared.changed.notify_waiters();
        }
    })
}

#[derive(Debug)]
pub(super) struct Addresses {
    pub(super) ordinary: SocketAddr,
    pub(super) admin: SocketAddr,
    pub(super) experimental: Option<SocketAddr>,
}

pub(super) struct Output {
    pub(super) status: ExitStatus,
    pub(super) stdout: String,
    pub(super) stderr: String,
}

pub(super) struct Process {
    child: Child,
    stdout: Arc<Capture>,
    stderr: Arc<Capture>,
    readers: Vec<JoinHandle<io::Result<()>>>,
}

impl Process {
    pub(super) fn spawn(arguments: &[String]) -> TestResult<Self> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_switchyard"))
            .args(arguments)
            .env("TOKIO_WORKER_THREADS", "2")
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdout = Arc::new(Capture::default());
        let stderr = Arc::new(Capture::default());
        let readers = vec![
            capture(child.stdout.take().expect("piped stdout"), stdout.clone()),
            capture(child.stderr.take().expect("piped stderr"), stderr.clone()),
        ];
        Ok(Self {
            child,
            stdout,
            stderr,
            readers,
        })
    }

    pub(super) async fn ready(&self, experimental: bool) -> TestResult<Addresses> {
        timeout(PROCESS_DEADLINE, async {
            loop {
                let changed = self.stdout.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let data = self.stdout.data.lock().expect("child output");
                    if let Some(error) = data.error {
                        return Err(io::Error::other(error));
                    }
                    let text = String::from_utf8_lossy(&data.bytes);
                    let ordinary = address(&text, ORDINARY)?;
                    let admin = address(&text, ADMIN)?;
                    let atomic = address(&text, EXPERIMENTAL)?;
                    if let (Some(ordinary), Some(admin)) = (ordinary, admin)
                        && (!experimental || atomic.is_some())
                    {
                        return Ok(Addresses {
                            ordinary,
                            admin,
                            experimental: atomic,
                        });
                    }
                    if data.ended {
                        return Err(io::Error::other(format!(
                            "server exited before readiness: stdout={text}; stderr={}",
                            self.stderr.text()
                        )));
                    }
                }
                changed.await;
            }
        })
        .await
        .map_err(|error| {
            io::Error::other(format!(
                "server readiness timed out: {error}; stdout={}; stderr={}",
                self.stdout.text(),
                self.stderr.text()
            ))
        })?
        .map_err(Into::into)
    }

    pub(super) async fn finish(mut self, kill: bool) -> TestResult<Output> {
        if kill && self.child.try_wait()?.is_none() {
            self.child.start_kill()?;
        }
        let status = timeout(PROCESS_DEADLINE, self.child.wait()).await??;
        for reader in self.readers.drain(..) {
            timeout(PROCESS_DEADLINE, reader).await???;
        }
        Ok(Output {
            status,
            stdout: self.stdout.text(),
            stderr: self.stderr.text(),
        })
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        for reader in &self.readers {
            reader.abort();
        }
    }
}

fn address(text: &str, marker: &str) -> io::Result<Option<SocketAddr>> {
    let mut result = None;
    // NO_COLOR gives a stable field token; use only complete bounded log lines.
    let Some(complete) = text.rfind('\n').map(|last| &text[..last]) else {
        return Ok(None);
    };
    for line in complete.lines().filter(|line| line.contains(marker)) {
        let address = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("address="))
            .ok_or_else(|| io::Error::other("listener readiness lacks its address"))?
            .parse()
            .map_err(|_| io::Error::other("listener readiness has an invalid address"))?;
        if result.replace(address).is_some() {
            return Err(io::Error::other("duplicate listener readiness"));
        }
    }
    Ok(result)
}

#[test]
fn readiness_waits_for_the_complete_fragmented_record() -> TestResult {
    let incomplete = format!("INFO {EXPERIMENTAL} address=127.0.0.1:");
    assert!(address(&incomplete, EXPERIMENTAL)?.is_none());
    let complete = format!("{incomplete}12345 namespace=tenant tls=false\n");
    assert_eq!(
        address(&complete, EXPERIMENTAL)?,
        Some("127.0.0.1:12345".parse()?)
    );
    assert!(address(&format!("{complete}INFO {ORDINARY} address="), ORDINARY)?.is_none());
    Ok(())
}
