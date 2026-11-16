#![cfg(feature = "consensus-driver-testkit")]
//! Actual maintained-core elections/commit, original TCP, three process/store
//! owners and actual indexed outcomes. Control lines are bounded fixture IO.
#[path = "consensus_application_process/gate.rs"]
mod gate;
use cluster::consensus::{
    ConsensusIdentity, Voter,
    driver::{
        DriverOptions,
        application::{
            ApplicationError, ApplicationHandle, ApplicationLimits, ApplicationOwner,
            ApplicationReceipt, ApplicationResult,
        },
    },
};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(120);
#[derive(Clone, Serialize, Deserialize)]
struct Config {
    root: PathBuf,
    nonce: String,
    endpoints: [String; 3],
}
impl Config {
    fn identity(&self) -> ConsensusIdentity {
        ConsensusIdentity::new(
            "local-apply".into(),
            self.nonce.clone(),
            domain::NamespaceName::new("tenant").unwrap(),
            std::array::from_fn(|i| Voter {
                id: i as u64 + 1,
                endpoint: self.endpoints[i].parse().unwrap(),
            }),
        )
        .unwrap()
    }
    fn store(&self, id: u64) -> PathBuf {
        self.root.join(format!("store-{id}"))
    }
}
struct Lines<R> {
    input: R,
    partial: Vec<u8>,
}
impl<R: AsyncRead + Unpin> Lines<R> {
    fn new(input: R) -> Self {
        Self {
            input,
            partial: Vec::new(),
        }
    }
    async fn next(&mut self) -> Result<Option<String>> {
        loop {
            let byte = match self.input.read_u8().await {
                Ok(b) => b,
                Err(e)
                    if e.kind() == std::io::ErrorKind::UnexpectedEof && self.partial.is_empty() =>
                {
                    return Ok(None);
                }
                Err(e) => return Err(e.into()),
            };
            if byte == b'\n' {
                return Ok(Some(String::from_utf8(std::mem::take(&mut self.partial))?));
            }
            if self.partial.len() == 8192 {
                return Err("control line limit".into());
            }
            self.partial.push(byte);
        }
    }
}
async fn emit(c: &Config, id: u64, value: Value) -> Result<()> {
    let mut b = serde_json::to_vec(
        &json!({"nonce":c.nonce,"node":id,"pid":std::process::id(),"value":value}),
    )?;
    if b.len() > 8191 {
        return Err("control output bound".into());
    }
    b.insert(0, b'@');
    b.push(b'\n');
    let mut out = tokio::io::stdout();
    out.write_all(&b).await?;
    out.flush().await?;
    Ok(())
}
fn data(kind: domain::CommandKind, t: u64) -> Vec<u8> {
    domain::DurableProposal::unbound(domain::Command::new(
        domain::NamespaceName::new("tenant").unwrap(),
        domain::EntityPath::new("q").unwrap(),
        domain::Timestamp::from_millis(t),
        kind,
    ))
    .encode()
    .unwrap()
}
fn full_data() -> Vec<u8> {
    let make = |n| {
        domain::DurableProposal::unbound(domain::Command::new(
            domain::NamespaceName::new("tenant").unwrap(),
            domain::EntityPath::new("q").unwrap(),
            domain::Timestamp::from_millis(3),
            domain::CommandKind::Send {
                message_id: "large".into(),
                body: vec![7; n],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        ))
        .encode()
    };
    let (mut low, mut high) = (0, cluster::consensus::MAX_CONSENSUS_PROPOSAL_BYTES);
    while low < high {
        let m = (low + high).div_ceil(2);
        if make(m).is_ok() {
            low = m;
        } else {
            high = m - 1;
        }
    }
    let b = make(low).unwrap();
    assert_eq!(b.len(), cluster::consensus::MAX_CONSENSUS_PROPOSAL_BYTES);
    b
}
fn status(owner: &ApplicationOwner, handle: &ApplicationHandle) -> Result<Value> {
    let a = handle.status();
    let d = handle.driver_status();
    let observation=owner.committed_observation()?.map(|o|json!({"index":o.index,"data_bytes":o.data_bytes,"context_bytes":o.context_bytes,"data_digest":o.data_digest,"context_digest":o.context_digest}));
    Ok(
        json!({"term":d.term,"role":format!("{:?}",d.role),"last":d.observed_last,"commit":d.observed_commit,"actual":d.applied,"a":a.applied_ordinal,"r":a.applied_raft,"l":a.projected,"j":a.projected_commit,"originals":a.originals,"bytes":a.original_bytes,"retired":d.retired,"outbound":d.pending_outbound,"observation":observation}),
    )
}
fn result(value: std::result::Result<ApplicationResult, ApplicationError>) -> Value {
    match value {
        Err(e) => json!({"error":format!("{e:?}")}),
        Ok(r) => {
            let (kind, body) = match r.outcome {
                domain::IndexedApplyOutcome::Applied(domain::CommandOutcome::Peeked(rows)) => {
                    ("Peeked", rows.iter().map(|d| d.body.len()).sum::<usize>())
                }
                domain::IndexedApplyOutcome::Applied(domain::CommandOutcome::QueueCreated) => {
                    ("QueueCreated", 0)
                }
                domain::IndexedApplyOutcome::Applied(domain::CommandOutcome::Sent { .. }) => {
                    ("Sent", 0)
                }
                domain::IndexedApplyOutcome::Applied(_) => ("Applied", 0),
                domain::IndexedApplyOutcome::Refused(_) => ("Refused", 0),
                domain::IndexedApplyOutcome::AlreadyApplied => ("ILLEGAL historical outcome", 0),
            };
            json!({"kind":kind,"body_bytes":body,"index":r.raft_index,"ordinal":r.application_ordinal,"term":r.term})
        }
    }
}
async fn control(
    owner: &mut ApplicationOwner,
    mut receipt: cluster::consensus::driver::Receipt<()>,
) -> Result<()> {
    tokio::select! {r=receipt.wait()=>r?,_=owner.run()=>return Err("retired during control".into())};
    Ok(())
}
async fn child(c: Config, id: u64, fresh: bool) -> Result<()> {
    let (gate_server, physical_gate) = gate::Server::bind();
    let options = DriverOptions {
        tick_interval: None,
        ..Default::default()
    };
    let (mut owner, handle) = ApplicationOwner::bind(
        c.identity(),
        id,
        options,
        ApplicationLimits::default(),
        fresh,
        || {
            Ok(gate::Backend {
                inner: storage::ConsensusFjallStore::open(c.store(id))?,
                gate: physical_gate,
            })
        },
    )
    .await?;
    let controls = owner.network_control();
    let mut lines = Lines::new(tokio::io::stdin());
    let mut receipt: Option<ApplicationReceipt> = None;
    emit(
        &c,
        id,
        json!({"ready":status(&owner,&handle)?,"gate":gate_server.endpoint.to_string()}),
    )
    .await?;
    loop {
        let line = tokio::select! {l=lines.next()=>l?,_=owner.run()=>return Err("unexpected application retirement".into())};
        let cmd: Value = serde_json::from_str(&line.ok_or("control EOF")?)?;
        let reply = match cmd["op"].as_str().ok_or("missing command")? {
            "status" => status(&owner, &handle)?,
            "campaign" => {
                control(&mut owner, handle.campaign()?).await?;
                json!({"ok":true})
            }
            "tick" => {
                let n = cmd["n"].as_u64().unwrap();
                if n > 100 {
                    return Err("tick bound".into());
                }
                for _ in 0..n {
                    control(&mut owner, handle.tick()?).await?;
                }
                json!({"ok":true})
            }
            "pause" => {
                controls.pause_application(cmd["value"].as_bool().unwrap());
                json!({"ok":true})
            }
            "block" => {
                let peers: Vec<u64> = serde_json::from_value(cmd["peers"].clone())?;
                if peers.len() > 3 {
                    return Err("block bound".into());
                }
                let mut p = [0; 3];
                for (i, id) in peers.into_iter().enumerate() {
                    p[i] = id;
                }
                controls.block(p);
                json!({"ok":true})
            }
            "submit" => {
                if receipt.is_some() {
                    return Err("fixture has unobserved original".into());
                }
                let (kind, t) = (cmd["kind"].as_str().unwrap(), cmd["time"].as_u64().unwrap());
                let (bytes, context) = match kind {
                    "create" => (
                        data(
                            domain::CommandKind::CreateQueue {
                                config: domain::QueueConfig {
                                    max_message_bytes:
                                        cluster::consensus::MAX_CONSENSUS_PROPOSAL_BYTES,
                                    ..Default::default()
                                },
                            },
                            t,
                        ),
                        vec![],
                    ),
                    "large" => (
                        full_data(),
                        vec![8; cluster::consensus::MAX_CONSENSUS_CONTEXT_BYTES],
                    ),
                    "peek" => (
                        data(
                            domain::CommandKind::Peek {
                                from_sequence: domain::SequenceNumber::new(1),
                                max_messages: 250,
                                session: None,
                            },
                            t,
                        ),
                        vec![],
                    ),
                    "noop" => (data(domain::CommandKind::ExpireSessionLocks, t), vec![]),
                    _ => return Err("unknown original".into()),
                };
                let bytes_len = bytes.len();
                receipt = Some(handle.submit(bytes, context, handle.driver_status().term)?);
                json!({"queued_only":true,"proposal_bytes":bytes_len})
            }
            "observe" => match receipt.as_mut().map(|r| r.wait().now_or_never()) {
                Some(Some(r)) => {
                    receipt = None;
                    result(r)
                }
                Some(None) => json!({"pending":true}),
                None => json!({"no_original":true}),
            },
            "busy" => {
                json!({"busy":matches!(handle.submit(data(domain::CommandKind::ExpireSessionLocks,100),vec![],handle.driver_status().term),Err(ApplicationError::Busy))})
            }
            "invalid" => {
                json!({"invalid":matches!(handle.submit(vec![0,1],vec![],handle.driver_status().term),Err(ApplicationError::InvalidProposal))})
            }
            "stop" => {
                let report = owner.finish().await;
                ensure(
                    report.panics == 0
                        && report.cancelled == 0
                        && owner.transport_status().global.operations == 0,
                    "unclean original application finish",
                )?;
                let closed = owner.into_closed().map_err(|_| "unfinished extraction")?;
                drop(closed);
                gate_server.join();
                emit(&c, id, json!({"stopped":true,"joined":report.joined})).await?;
                return Ok(());
            }
            _ => return Err("unknown fixture command".into()),
        };
        emit(&c, id, reply).await?;
    }
}
#[test]
fn application_child() {
    let Ok(path) = std::env::var("SWITCHYARD_APPLICATION_CONFIG") else {
        return;
    };
    let c: Config = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let id = std::env::var("SWITCHYARD_APPLICATION_ID")
        .unwrap()
        .parse()
        .unwrap();
    let fresh = std::env::var("SWITCHYARD_APPLICATION_FRESH").unwrap() == "1";
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(child(c, id, fresh))
        .unwrap();
}
struct Original {
    child: Child,
    input: ChildStdin,
    output: Lines<ChildStdout>,
    id: u64,
    pid: u32,
    nonce: String,
    joined: bool,
    gate: Option<Lines<tokio::net::TcpStream>>,
}
impl Original {
    fn spawn(c: &Config, path: &Path, id: u64, fresh: bool) -> Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .args(["--exact", "application_child", "--nocapture"])
            .env("SWITCHYARD_APPLICATION_CONFIG", path)
            .env("SWITCHYARD_APPLICATION_ID", id.to_string())
            .env(
                "SWITCHYARD_APPLICATION_FRESH",
                if fresh { "1" } else { "0" },
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        Ok(Self {
            pid: child.id().unwrap(),
            input: child.stdin.take().unwrap(),
            output: Lines::new(child.stdout.take().unwrap()),
            child,
            id,
            nonce: c.nonce.clone(),
            joined: false,
            gate: None,
        })
    }
    async fn event(&mut self) -> Result<Value> {
        timeout(DEADLINE, async {
            for _ in 0..64 {
                let l = self.output.next().await?.ok_or("child EOF")?;
                let Some(l) = l.strip_prefix('@') else {
                    continue;
                };
                let v: Value = serde_json::from_str(l)?;
                ensure(
                    v["nonce"] == self.nonce && v["node"] == self.id && v["pid"] == self.pid,
                    "foreign original control",
                )?;
                return Ok(v["value"].clone());
            }
            Err("event work bound".into())
        })
        .await?
    }
    async fn command(&mut self, v: Value) -> Result<Value> {
        let mut b = serde_json::to_vec(&v)?;
        ensure(b.len() < 8192, "control limit")?;
        b.push(b'\n');
        timeout(DEADLINE, self.input.write_all(&b)).await??;
        self.event().await
    }
    async fn gate(&mut self, command: &str) -> Result<String> {
        let gate = self
            .gate
            .as_mut()
            .ok_or("missing physical gate connection")?;
        timeout(DEADLINE, async {
            gate.input
                .write_all(format!("{command}\n").as_bytes())
                .await?;
            gate.next().await?.ok_or_else(|| "physical gate EOF".into())
        })
        .await?
    }
    async fn wait(&mut self, name: &str, p: impl Fn(&Value) -> bool) -> Result<Value> {
        timeout(DEADLINE, async {
            for _ in 0..10000 {
                let v = self.command(json!({"op":"status"})).await?;
                ensure(v["retired"] == false, "unexpected retired application")?;
                if p(&v) {
                    println!("application barrier node={} {name}: {v}", self.id);
                    return Ok(v);
                }
                tokio::task::yield_now().await;
            }
            Err(format!("unmet original frontier {name}").into())
        })
        .await?
    }
    async fn observed(&mut self, name: &str) -> Result<Value> {
        timeout(DEADLINE, async {
            for _ in 0..10000 {
                let v = self.command(json!({"op":"observe"})).await?;
                if v["pending"] != true {
                    println!("application original node={} {name}: {v}", self.id);
                    return Ok(v);
                }
                tokio::task::yield_now().await;
            }
            Err("original did not complete".into())
        })
        .await?
    }
    async fn stop(&mut self) -> Result<()> {
        ensure(
            self.gate("stop").await? == "stopped",
            "gate thread not stopped",
        )?;
        ensure(
            self.command(json!({"op":"stop"})).await?["stopped"] == true,
            "missing finished owner",
        )?;
        let exit = timeout(DEADLINE, self.child.wait()).await??;
        self.joined = true;
        ensure(exit.success(), "original process failed")?;
        println!(
            "application original pid={} node={} joined=true forced_cleanup=false",
            self.pid, self.id
        );
        Ok(())
    }
    async fn cleanup(&mut self) -> Result<()> {
        if !self.joined {
            if self.child.try_wait()?.is_none() {
                self.child.start_kill()?;
            }
            timeout(DEADLINE, self.child.wait()).await??;
            self.joined = true;
        }
        Ok(())
    }
}
fn ensure(v: bool, message: &str) -> Result<()> {
    if v {
        Ok(())
    } else {
        Err(message.to_owned().into())
    }
}
async fn block(nodes: &mut [Original], id: usize, peers: Vec<u64>) -> Result<()> {
    nodes[id - 1]
        .command(json!({"op":"block","peers":peers}))
        .await?;
    Ok(())
}
async fn caught_up(nodes: &mut [Original], r: u64, a: u64) -> Result<()> {
    for n in nodes {
        n.wait("actual applied prefix", |s| {
            s["actual"] == r && s["r"] == r && s["a"] == a
        })
        .await?;
    }
    Ok(())
}
async fn campaign_after_isolated_lease_expiry(
    nodes: &mut [Original],
    candidate: usize,
    peer: usize,
    old: usize,
) -> Result<()> {
    // Expire leader leases without allowing two randomized automatic campaigns
    // to race for this majority. These are dropped real core packets, not votes
    // supplied by the fixture. Drain staged packets before reconnecting peers.
    for id in [candidate, peer] {
        block(
            nodes,
            id,
            (1..=3)
                .filter(|other| *other != id)
                .map(|other| other as u64)
                .collect(),
        )
        .await?;
    }
    for id in [candidate, peer] {
        nodes[id - 1].command(json!({"op":"tick","n":10})).await?;
        nodes[id - 1]
            .wait("isolated lease expiry packets flushed", |s| {
                s["outbound"] == 0
            })
            .await?;
    }
    for id in [candidate, peer] {
        block(nodes, id, vec![old as u64]).await?;
    }
    nodes[candidate - 1]
        .command(json!({"op":"campaign"}))
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_three_process_commit_apply_outcomes_term_fences_context_and_restart() {
    let d = tempfile::tempdir().unwrap();
    let sockets: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let c = Config {
        root: d.path().to_path_buf(),
        nonce: format!("apply-{}", std::process::id()),
        endpoints: std::array::from_fn(|i| sockets[i].local_addr().unwrap().to_string()),
    };
    drop(sockets);
    let path = d.path().join("config.json");
    std::fs::write(&path, serde_json::to_vec(&c).unwrap()).unwrap();
    let mut nodes = Vec::new();
    let outcome = std::panic::AssertUnwindSafe(async {
        for id in 1..=3 {
            nodes.push(Original::spawn(&c, &path, id, true)?);
        }
        for n in nodes.iter_mut() {
            let ready = n.event().await?;
            ensure(ready["ready"]["a"] == 0, "unexpected initial domain state")?;
            n.gate = Some(Lines::new(
                tokio::net::TcpStream::connect(ready["gate"].as_str().ok_or("gate address")?)
                    .await?,
            ));
        }
        nodes[0].command(json!({"op":"campaign"})).await?;
        nodes[0]
            .wait("core elected and validated blank applied", |s| {
                s["role"] == "Leader" && s["actual"] == 1 && s["a"] == 0
            })
            .await?;
        nodes[0].command(json!({"op":"tick","n":1})).await?;
        caught_up(&mut nodes, 1, 0).await?;
        // Only follower 2 can supply the missing durable majority. Its actual
        // original backend call is held BEFORE applying the index-2 batch.
        block(&mut nodes, 1, vec![3]).await?;
        block(&mut nodes, 3, vec![1]).await?;
        ensure(
            nodes[1].gate("arm 2").await? == "armed",
            "physical gate not armed",
        )?;
        nodes[0].command(json!({"op":"pause","value":true})).await?;
        nodes[0]
            .command(json!({"op":"submit","kind":"create","time":2}))
            .await?;
        ensure(
            nodes[1].gate("wait").await? == "held",
            "original F2 call did not begin",
        )?;
        nodes[0]
            .wait("actual follower F2 write held, no durable majority", |s| {
                s["last"] == 2 && s["commit"] == 1 && s["actual"] == 1
            })
            .await?;
        ensure(
            nodes[0].command(json!({"op":"observe"})).await?["pending"] == true,
            "begun follower persistence was counted as a durable reply",
        )?;
        ensure(
            nodes[1].gate("release").await? == "released",
            "original F2 call not released",
        )?;
        nodes[0]
            .wait(
                "majority committed, leader application genuinely held",
                |s| s["commit"] == 2 && s["actual"] == 1 && s["a"] == 0 && s["originals"] == 1,
            )
            .await?;
        ensure(
            nodes[0].command(json!({"op":"observe"})).await?["pending"] == true,
            "commit/admission leaked a business result before actual apply",
        )?;
        ensure(
            nodes[0].command(json!({"op":"busy"})).await?["busy"] == true,
            "original count not retained",
        )?;
        ensure(
            nodes[0].command(json!({"op":"invalid"})).await?["invalid"] == true,
            "malformed local intent not isolated",
        )?;
        nodes[0]
            .command(json!({"op":"pause","value":false}))
            .await?;
        for id in 1..=3 {
            block(&mut nodes, id, vec![]).await?;
        }
        ensure(
            nodes[0].observed("actual leader create result").await?["kind"] == "QueueCreated",
            "missing real leader result",
        )?;
        nodes[0].command(json!({"op":"tick","n":1})).await?;
        caught_up(&mut nodes, 2, 1).await?;
        nodes[0]
            .command(json!({"op":"submit","kind":"large","time":3}))
            .await?;
        ensure(
            nodes[0].observed("full original send result").await?["kind"] == "Sent",
            "full legal proposal refused",
        )?;
        nodes[0].command(json!({"op":"tick","n":1})).await?;
        caught_up(&mut nodes, 3, 2).await?;
        let request = nodes[0]
            .command(json!({"op":"submit","kind":"peek","time":4}))
            .await?;
        let completed = nodes[0]
            .wait(
                "completed but unobserved result retains byte/count reservation",
                |s| {
                    s["a"] == 3
                        && s["actual"] == 4
                        && s["originals"] == 1
                        && s["bytes"].as_u64().unwrap() > 1024 * 1024
                },
            )
            .await?;
        ensure(completed["originals"] == 1, "unobserved result refunded")?;
        ensure(
            nodes[0].command(json!({"op":"busy"})).await?["busy"] == true,
            "completed result not counted",
        )?;
        let peek = nodes[0].observed("reply larger than proposal").await?;
        ensure(
            peek["kind"] == "Peeked"
                && peek["body_bytes"].as_u64().unwrap()
                    > request["proposal_bytes"].as_u64().unwrap(),
            "reply bound proof was vacuous",
        )?;
        nodes[0].command(json!({"op":"tick","n":1})).await?;
        caught_up(&mut nodes, 4, 3).await?;
        block(&mut nodes, 1, vec![2, 3]).await?;
        block(&mut nodes, 2, vec![1]).await?;
        block(&mut nodes, 3, vec![1]).await?;
        nodes[0].command(json!({"op":"pause","value":true})).await?;
        nodes[0]
            .command(json!({"op":"submit","kind":"noop","time":5}))
            .await?;
        nodes[0]
            .wait(
                "minority admission remains uncommitted and unapplied",
                |s| s["last"] == 5 && s["commit"] == 4 && s["actual"] == 4,
            )
            .await?;
        ensure(
            nodes[0].command(json!({"op":"observe"})).await?["pending"] == true,
            "minority produced business result",
        )?;
        campaign_after_isolated_lease_expiry(&mut nodes, 2, 3, 1).await?;
        nodes[1]
            .wait("new core leader", |s| {
                s["role"] == "Leader" && s["actual"] == 5 && s["commit"] == 5
            })
            .await?;
        for id in 1..=3 {
            block(&mut nodes, id, vec![]).await?;
        }
        nodes[1].command(json!({"op":"tick","n":1})).await?;
        nodes[0]
            .wait("higher term replaced uncommitted original", |s| {
                s["role"] == "Follower" && s["commit"] == 5
            })
            .await?;
        ensure(
            nodes[0].observed("old uncommitted original fenced").await?["error"] == "Unknown",
            "stale original received result",
        )?;
        nodes[0]
            .command(json!({"op":"pause","value":false}))
            .await?;
        nodes[1].command(json!({"op":"tick","n":1})).await?;
        caught_up(&mut nodes, 5, 3).await?;
        nodes[1].command(json!({"op":"pause","value":true})).await?;
        nodes[1]
            .command(json!({"op":"submit","kind":"noop","time":6}))
            .await?;
        nodes[1]
            .wait("committed original withheld before leader effects", |s| {
                s["commit"] == 6 && s["actual"] == 5
            })
            .await?;
        ensure(
            nodes[1].command(json!({"op":"observe"})).await?["pending"] == true,
            "no-op outcome before actual F1 checkpoint",
        )?;
        nodes[1].command(json!({"op":"tick","n":1})).await?;
        nodes[2]
            .wait(
                "future leader actually applied old committed command",
                |s| s["commit"] == 6 && s["actual"] == 6 && s["a"] == 4,
            )
            .await?;
        block(&mut nodes, 2, vec![1, 3]).await?;
        block(&mut nodes, 1, vec![2]).await?;
        block(&mut nodes, 3, vec![2]).await?;
        nodes[2].command(json!({"op":"pause","value":true})).await?;
        campaign_after_isolated_lease_expiry(&mut nodes, 3, 1, 2).await?;
        nodes[2]
            .wait(
                "new leader still catching up through its committed blank",
                |s| s["role"] == "Leader" && s["commit"] == 7 && s["actual"] == 6,
            )
            .await?;
        nodes[2]
            .command(json!({"op":"submit","kind":"noop","time":7}))
            .await?;
        ensure(
            nodes[2]
                .observed("catch-up fence refuses before RawNode proposal")
                .await?["error"]
                == "Busy",
            "new leader proposed business work before actual catch-up",
        )?;
        let denied = nodes[2].command(json!({"op":"status"})).await?;
        ensure(
            denied["last"] == 7 && denied["actual"] == 6,
            "definite catch-up refusal mutated the log",
        )?;
        nodes[2]
            .command(json!({"op":"pause","value":false}))
            .await?;
        nodes[2]
            .wait("third actual leader", |s| {
                s["role"] == "Leader" && s["actual"] == 7
            })
            .await?;
        for id in 1..=3 {
            block(&mut nodes, id, vec![]).await?;
        }
        nodes[2].command(json!({"op":"tick","n":1})).await?;
        nodes[1]
            .wait(
                "committed pre-apply original loses publication authority",
                |s| s["role"] == "Follower" && s["commit"] == 7 && s["actual"] == 5,
            )
            .await?;
        ensure(
            nodes[1]
                .observed("committed stale original remains unknown")
                .await?["error"]
                == "Unknown",
            "recovered prior result was invented",
        )?;
        nodes[1]
            .command(json!({"op":"pause","value":false}))
            .await?;
        nodes[2].command(json!({"op":"tick","n":1})).await?;
        caught_up(&mut nodes, 7, 4).await?;
        nodes[2]
            .command(json!({"op":"submit","kind":"noop","time":8}))
            .await?;
        let fresh = nodes[2]
            .observed("fresh original after leader change")
            .await?;
        ensure(
            fresh["index"] == 8 && fresh["ordinal"] == 5 && fresh["kind"] == "Applied",
            "new leader did not progress",
        )?;
        nodes[2].command(json!({"op":"tick","n":1})).await?;
        caught_up(&mut nodes, 8, 5).await?;
        // A result already legally published is a completed fact, unlike the
        // pre-apply stale originals above. Delay observation across stepdown.
        nodes[2]
            .command(json!({"op":"submit","kind":"peek","time":9}))
            .await?;
        nodes[2]
            .wait("legally published result remains unobserved", |s| {
                s["actual"] == 9 && s["a"] == 6 && s["originals"] == 1
            })
            .await?;
        nodes[2].command(json!({"op":"tick","n":1})).await?;
        caught_up(&mut nodes, 9, 6).await?;
        block(&mut nodes, 3, vec![1, 2]).await?;
        block(&mut nodes, 1, vec![3]).await?;
        block(&mut nodes, 2, vec![3]).await?;
        campaign_after_isolated_lease_expiry(&mut nodes, 1, 2, 3).await?;
        nodes[0]
            .wait("leader change after legal publication", |s| {
                s["role"] == "Leader" && s["actual"] == 10
            })
            .await?;
        for id in 1..=3 {
            block(&mut nodes, id, vec![]).await?;
        }
        nodes[0].command(json!({"op":"tick","n":1})).await?;
        nodes[2]
            .wait("published result original now follower", |s| {
                s["role"] == "Follower" && s["commit"] == 10
            })
            .await?;
        let delayed = nodes[2]
            .observed("legal result survives later stepdown and delayed observation")
            .await?;
        ensure(
            delayed["index"] == 9
                && delayed["ordinal"] == 6
                && delayed["term"] == 3
                && delayed["kind"] == "Peeked",
            "stepdown revoked an already published real result",
        )?;
        nodes[0].command(json!({"op":"tick","n":1})).await?;
        caught_up(&mut nodes, 10, 6).await?;
        let expected_data: Vec<u8> = Sha256::digest(full_data()).to_vec();
        let expected_context: Vec<u8> = Sha256::digest(vec![8; 1024 * 1024]).to_vec();
        for n in nodes.iter_mut() {
            let s = n.command(json!({"op":"status"})).await?;
            ensure(
                s["observation"]["index"] == 3
                    && s["observation"]["context_bytes"] == 1024 * 1024
                    && s["observation"]["data_digest"] == json!(expected_data)
                    && s["observation"]["context_digest"] == json!(expected_context),
                "original full data/context changed in replication",
            )?;
            n.stop().await?;
        }
        let start = nodes.len();
        for id in 1..=3 {
            nodes.push(Original::spawn(&c, &path, id, false)?);
        }
        for n in &mut nodes[start..] {
            let ready = n.event().await?;
            let s = ready["ready"].clone();
            n.gate = Some(Lines::new(
                tokio::net::TcpStream::connect(ready["gate"].as_str().ok_or("gate address")?)
                    .await?,
            ));
            ensure(
                s["a"] == 6
                    && s["r"] == 10
                    && s["actual"] == 10
                    && s["observation"]["data_digest"] == json!(expected_data)
                    && s["observation"]["context_digest"] == json!(expected_context)
                    && s["observation"]["context_bytes"] == 1024 * 1024,
                "actual physical restart lost exact prefix/context",
            )?;
            ensure(
                n.command(json!({"op":"observe"})).await?["no_original"] == true,
                "restart invented prior outcome",
            )?;
            n.stop().await?;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    let mut forced = false;
    for n in &mut nodes {
        if !n.joined {
            forced = true;
            n.cleanup().await.unwrap();
        }
    }
    match outcome {
        Ok(r) => r.unwrap(),
        Err(p) => std::panic::resume_unwind(p),
    }
    assert!(!forced, "kill/timeout is not graceful drain");
}
