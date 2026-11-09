#![cfg(feature = "consensus-driver-testkit")]
//! Real three-process maintained-core elections and durable replication. Link
//! faults only delay/drop/replay real packets; all deliveries use #359 TCP.
use cluster::{
    consensus::{
        ConsensusIdentity, ConsensusStore, RetainedLogLimits, Voter,
        driver::{AdmissionError, ConsensusDriver, DriverOptions, Receipt, testing::Rule},
    },
    transport::{FrameCodec, PeerTransport, TransportLimits},
};
use futures_util::FutureExt;
use raft::eraftpb::{Message, MessageType};
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
const LINE_LIMIT: usize = 8192;

#[derive(Clone, Serialize, Deserialize)]
struct Config {
    nonce: String,
    root: PathBuf,
    endpoints: [String; 3],
}
impl Config {
    fn identity(&self) -> ConsensusIdentity {
        ConsensusIdentity::new(
            "local-driver".into(),
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
            if self.partial.len() >= LINE_LIMIT {
                return Err("control line bound".into());
            }
            self.partial.push(byte);
        }
    }
}
async fn emit(config: &Config, id: u64, value: Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(
        &json!({"nonce":config.nonce,"id":id,"pid":std::process::id(),"value":value}),
    )?;
    if bytes.len() > LINE_LIMIT {
        return Err("control output bound".into());
    }
    bytes.insert(0, b'@');
    bytes.push(b'\n');
    let mut output = tokio::io::stdout();
    output.write_all(&bytes).await?;
    output.flush().await?;
    Ok(())
}
fn proposal(n: u64) -> Vec<u8> {
    domain::DurableProposal::unbound(domain::Command::new(
        domain::NamespaceName::new("tenant").unwrap(),
        domain::EntityPath::new(format!("q{n}")).unwrap(),
        domain::Timestamp::from_millis(n),
        domain::CommandKind::CreateQueue {
            config: domain::QueueConfig::default(),
        },
    ))
    .encode()
    .unwrap()
}
fn full_proposal() -> Vec<u8> {
    let make = |size| {
        domain::DurableProposal::unbound(domain::Command::new(
            domain::NamespaceName::new("tenant").unwrap(),
            domain::EntityPath::new("absent-queue").unwrap(),
            domain::Timestamp::from_millis(99),
            domain::CommandKind::Send {
                message_id: "durable-intent".into(),
                body: vec![7; size],
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
        let mid = (low + high).div_ceil(2);
        if make(mid).is_ok() {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    let data = make(low).unwrap();
    assert_eq!(data.len(), cluster::consensus::MAX_CONSENSUS_PROPOSAL_BYTES);
    data
}
fn full_context() -> Vec<u8> {
    vec![8; cluster::consensus::MAX_CONSENSUS_CONTEXT_BYTES]
}
async fn join_admission<T>(
    driver: &mut ConsensusDriver<storage::ConsensusFjallStore>,
    mut receipt: Receipt<T>,
) -> std::result::Result<T, AdmissionError> {
    tokio::select! {value=receipt.wait()=>value,_=driver.run()=>Err(AdmissionError::Retired)}
}
fn status(driver: &ConsensusDriver<storage::ConsensusFjallStore>) -> Value {
    let s = driver.status();
    let t = driver.transport_status();
    let c = driver.network_control();
    let processed=c.processed_append().map(|p|json!({"from":p.from,"term":p.term,"index":p.index,"reject":p.reject,"core_term":p.core_term,"matched":p.matched,"voters":p.voters,"commit":p.commit,"rejected_before_success":p.rejected_before_success}));
    json!({"role":format!("{:?}",s.role),"term":s.term,"vote":s.observed_vote,"commit":s.observed_commit,"last":s.observed_last,"applied":s.applied,"events":s.peer_events,"last_from":s.last_peer_from,"last_term":s.last_peer_term,"last_index":s.last_peer_index,"advanced":s.advanced,"retired":s.retired,"outbound":s.pending_outbound,"inputs":s.pending_events,"held":c.held(),"processed_append":processed,"captured":c.captured().map(|m|json!({"term":m.term,"index":m.index,"from":m.from,"to":m.to})),"io_active":t.global.operations,"refused":t.report.refused,"io_joined":t.report.joined})
}
fn inventory(store: &ConsensusStore<storage::ConsensusFjallStore>) -> Result<Value> {
    let entries = store.read_entries(1, 64)?;
    Ok(
        json!({"term":store.hard_state()?.term,"vote":store.hard_state()?.vote,"commit":store.hard_state()?.commit,"last":store.last_index()?,"entries":entries.iter().map(|e|json!({"index":e.index,"term":e.term,"digest":format!("{:x}",Sha256::digest(&e.data)),"context_len":e.context.len(),"context_digest":format!("{:x}",Sha256::digest(&e.context)),"blank":e.data.is_empty(),"ordinal":if e.index<=store.hard_state().unwrap().commit {store.application_ordinal(e.index).unwrap()}else{None}})).collect::<Vec<_>>()}),
    )
}
async fn child(config: Config, id: u64, fresh: bool) -> Result<()> {
    let path = config.store(id);
    let options = DriverOptions {
        tick_interval: None,
        ..Default::default()
    };
    let (mut driver, handle) = ConsensusDriver::bind(config.identity(), id, options, fresh, || {
        storage::ConsensusFjallStore::open(&path)
    })
    .await?;
    let controls = driver.network_control();
    let mut lines = Lines::new(tokio::io::stdin());
    emit(&config, id, json!({"kind":"ready","store":path})).await?;
    loop {
        let line = tokio::select! {line=lines.next()=>line?,_=driver.run()=>return Err("unexpected driver retirement".into())};
        let command: Value = serde_json::from_str(&line.ok_or("control EOF without stop")?)?;
        let reply = match command["op"].as_str().ok_or("missing op")? {
            "status" => status(&driver),
            "campaign" => {
                join_admission(&mut driver, handle.campaign()?).await?;
                json!({"ok":true})
            }
            "tick" => {
                let n = command["n"].as_u64().ok_or("missing ticks")?;
                if n > 100 {
                    return Err("tick bound".into());
                }
                for _ in 0..n {
                    join_admission(&mut driver, handle.tick()?).await?;
                }
                json!({"ok":true})
            }
            "propose" => {
                let term = handle.status().term;
                let receipt =
                    handle.propose(proposal(command["n"].as_u64().unwrap()), vec![], term)?;
                let a = join_admission(&mut driver, receipt).await?;
                json!({"index":a.raft_index,"term":a.term})
            }
            "large" => {
                let term = handle.status().term;
                let receipt = handle.propose(full_proposal(), full_context(), term)?;
                let a = join_admission(&mut driver, receipt).await?;
                json!({"index":a.raft_index,"term":a.term})
            }
            "invalid" => {
                json!({"rejected":matches!(handle.propose(vec![0,1,2],vec![],handle.status().term),Err(AdmissionError::InvalidProposal))})
            }
            "block" => {
                let peers: Vec<u64> = serde_json::from_value(command["peers"].clone())?;
                if peers.len() > 3 {
                    return Err("peer block bound".into());
                }
                let mut blocked = [0; 3];
                for (i, p) in peers.into_iter().enumerate() {
                    blocked[i] = p;
                }
                controls.block(blocked);
                json!({"ok":true})
            }
            "hold" | "capture" => {
                let rule = Rule {
                    peer: command["to"].as_u64().unwrap(),
                    kind: match command["kind"].as_str().unwrap() {
                        "append" => MessageType::MsgAppend,
                        "response" => MessageType::MsgAppendResponse,
                        _ => return Err("unknown rule kind".into()),
                    },
                    min_index: command["index"].as_u64().unwrap(),
                };
                if command["op"] == "hold" {
                    controls.hold(Some(rule));
                } else {
                    controls.capture(Some(rule));
                }
                json!({"ok":true})
            }
            "release" => {
                controls.release();
                json!({"ok":true})
            }
            "observe_append" => {
                controls.observe_append(
                    command["from"].as_u64().unwrap(),
                    command["index"].as_u64().unwrap(),
                );
                json!({"ok":true})
            }
            "replay" => {
                let message = controls.captured().ok_or("no actual captured packet")?;
                ensure(controls.replay(message), "replay bound")?;
                json!({"ok":true})
            }
            "stop" => {
                handle.retire();
                let report = driver.finish().await;
                ensure(
                    report.panics == 0 && report.cancelled == 0,
                    "transport did not join cleanly",
                )?;
                ensure(
                    driver.status().finished && driver.transport_status().global.operations == 0,
                    "driver originals still active",
                )?;
                let store = driver
                    .into_finished_store()
                    .map_err(|_| "not finished")?
                    .reopen()?;
                let mut inv = inventory(&store)?;
                inv["joined"] = json!(report.joined);
                drop(store);
                emit(&config, id, json!({"kind":"stopped","inventory":inv})).await?;
                return Ok(());
            }
            _ => return Err("unknown control".into()),
        };
        emit(&config, id, reply).await?;
    }
}

#[test]
fn driver_child() {
    let Ok(path) = std::env::var("SWITCHYARD_DRIVER_CONFIG") else {
        return;
    };
    let config: Config = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let id = std::env::var("SWITCHYARD_DRIVER_ID")
        .unwrap()
        .parse()
        .unwrap();
    let fresh = std::env::var("SWITCHYARD_DRIVER_FRESH").unwrap() == "1";
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(child(config, id, fresh))
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
}
impl Original {
    fn spawn(config: &Config, path: &Path, id: u64, fresh: bool) -> Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .args(["--exact", "driver_child", "--nocapture"])
            .env("SWITCHYARD_DRIVER_CONFIG", path)
            .env("SWITCHYARD_DRIVER_ID", id.to_string())
            .env("SWITCHYARD_DRIVER_FRESH", if fresh { "1" } else { "0" })
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let pid = child.id().ok_or("missing original pid")?;
        let input = child.stdin.take().unwrap();
        let output = Lines::new(child.stdout.take().unwrap());
        Ok(Self {
            child,
            input,
            output,
            id,
            pid,
            nonce: config.nonce.clone(),
            joined: false,
        })
    }
    async fn event(&mut self) -> Result<Value> {
        timeout(DEADLINE, async {
            for _ in 0..64 {
                let line = self.output.next().await?.ok_or("child EOF")?;
                let Some(line) = line.strip_prefix('@') else {
                    continue;
                };
                let value: Value = serde_json::from_str(line)?;
                ensure(
                    value["pid"] == self.pid
                        && value["id"] == self.id
                        && value["nonce"] == self.nonce,
                    "foreign control record",
                )?;
                return Ok(value["value"].clone());
            }
            Err("control work bound".into())
        })
        .await?
    }
    async fn command(&mut self, value: Value) -> Result<Value> {
        let mut bytes = serde_json::to_vec(&value)?;
        ensure(bytes.len() < LINE_LIMIT, "command bound")?;
        bytes.push(b'\n');
        timeout(DEADLINE, self.input.write_all(&bytes)).await??;
        self.event().await
    }
    async fn wait(
        &mut self,
        description: &str,
        predicate: impl Fn(&Value) -> bool,
    ) -> Result<Value> {
        timeout(DEADLINE, async {
            for _ in 0..10000 {
                let status = self.command(json!({"op":"status"})).await?;
                ensure(status["retired"] == false, "unexpected retired driver")?;
                if predicate(&status) {
                    println!("driver barrier node={} {description}: {status}", self.id);
                    return Ok(status);
                }
                tokio::task::yield_now().await;
            }
            Err(format!("unmet bounded barrier: {description}").into())
        })
        .await?
    }
    async fn stop(&mut self) -> Result<Value> {
        let value = self.command(json!({"op":"stop"})).await?;
        ensure(value["kind"] == "stopped", "missing original finish")?;
        let exit = timeout(DEADLINE, self.child.wait()).await??;
        self.joined = true;
        ensure(exit.success(), "original child failed")?;
        println!(
            "driver original pid={} node={} joined=true forced_cleanup=false",
            self.pid, self.id
        );
        Ok(value["inventory"].clone())
    }
    async fn force_join(&mut self) -> Result<()> {
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
fn ensure(condition: bool, detail: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(detail.to_owned().into())
    }
}
async fn block(children: &mut [Original], node: usize, peers: Vec<u64>) -> Result<()> {
    children[node - 1]
        .command(json!({"op":"block","peers":peers}))
        .await?;
    Ok(())
}
async fn committed(children: &mut [Original], index: u64) -> Result<()> {
    for child in children {
        child
            .wait("persisted commit", |s| {
                s["commit"].as_u64().unwrap() >= index && s["applied"] == 0
            })
            .await?;
    }
    Ok(())
}

async fn election_replication(children: &mut [Original], config: &Config) -> Result<Vec<Value>> {
    for child in children.iter_mut() {
        let ready = child.event().await?;
        ensure(
            ready["kind"] == "ready" && ready["store"] == json!(config.store(child.id)),
            "wrong process/store",
        )?;
    }
    children[0].command(json!({"op":"campaign"})).await?;
    let leader = children[0]
        .wait("fresh real core election", |s| {
            s["role"] == "Leader" && s["commit"] == 1
        })
        .await?;
    let old_term = leader["term"].as_u64().unwrap();
    children[0].command(json!({"op":"tick","n":1})).await?;
    committed(children, 1).await?;
    // Two active voters, one isolated. Capture an actual earlier core response
    // and hold all acknowledgements for the next index at a named barrier.
    block(children, 1, vec![3]).await?;
    block(children, 2, vec![3]).await?;
    block(children, 3, vec![1, 2]).await?;
    children[1]
        .command(json!({"op":"capture","to":1,"kind":"response","index":2}))
        .await?;
    let a = children[0].command(json!({"op":"propose","n":10})).await?;
    ensure(a["index"] == 2, "unexpected admission index")?;
    children[0]
        .wait("distinct voter majority", |s| s["commit"] == 2)
        .await?;
    children[1]
        .wait("captured earlier response", |s| s["captured"]["index"] == 2)
        .await?;
    // Establish a genuinely SHORTER voter at tip 2. It will later have to
    // reject the new leader's prev=3 probe and receive old-term index 3 over TCP.
    block(children, 1, vec![]).await?;
    block(children, 3, vec![2]).await?;
    children[0].command(json!({"op":"tick","n":1})).await?;
    children[2]
        .wait("shorter follower has committed tip two", |s| {
            s["last"] == 2 && s["commit"] == 2
        })
        .await?;
    block(children, 1, vec![3]).await?;
    block(children, 3, vec![1, 2]).await?;
    children[1]
        .command(json!({"op":"hold","to":1,"kind":"response","index":3}))
        .await?;
    let b = children[0].command(json!({"op":"propose","n":11})).await?;
    ensure(b["index"] == 3, "unexpected held index")?;
    children[1]
        .wait("old-term entry durable but reply held", |s| {
            s["last"] == 3 && s["held"].as_u64().unwrap() > 0
        })
        .await?;
    // The bounded capture retains the FIRST matching actual response (index 2).
    let events = children[0].command(json!({"op":"status"})).await?["events"]
        .as_u64()
        .unwrap();
    for _ in 0..3 {
        children[1].command(json!({"op":"replay"})).await?;
    }
    children[0]
        .wait(
            "duplicate earlier configured-voter responses delivered",
            |s| s["events"].as_u64().unwrap() >= events + 3,
        )
        .await?;
    let minority = children[0].command(json!({"op":"status"})).await?;
    ensure(
        minority["commit"] == 2 && minority["applied"] == 0,
        "held reply advanced a minority",
    )?;
    children[2]
        .wait("third voter still lacks uncommitted old-term three", |s| {
            s["last"] == 2 && s["commit"] == 2
        })
        .await?;
    // Cut off the old leader entirely. Its later locally durable suffix cannot
    // commit. No ticks are supplied to it until the new majority is established.
    block(children, 1, vec![2, 3]).await?;
    block(children, 2, vec![1]).await?;
    block(children, 3, vec![1]).await?;
    ensure(
        children[0].command(json!({"op":"invalid"})).await?["rejected"] == true,
        "malformed admission poisoned group",
    )?;
    let c = children[0].command(json!({"op":"propose","n":90})).await?;
    let d = children[0].command(json!({"op":"propose","n":91})).await?;
    ensure(c["index"] == 4 && d["index"] == 5, "divergent tail absent")?;
    let minority = children[0].command(json!({"op":"status"})).await?;
    ensure(
        minority["commit"] == 2 && minority["last"] == 5,
        "minority promoted local admissions",
    )?;
    // Permit a real reject(prev=3), backfill(3), successful response(3) sequence.
    // Only index >=4 ACKs are withheld. Node 1 is isolated, so no other voter
    // can supply the new term's blank ACK while we inspect actual core progress.
    children[2]
        .command(json!({"op":"hold","to":2,"kind":"response","index":4}))
        .await?;
    children[1]
        .command(json!({"op":"observe_append","from":3,"index":3}))
        .await?;
    children[1].command(json!({"op":"tick","n":10})).await?;
    children[2].command(json!({"op":"tick","n":10})).await?;
    children[1].command(json!({"op":"campaign"})).await?;
    children[1]
        .wait("new real-core leader", |s| {
            s["role"] == "Leader" && s["term"].as_u64().unwrap() > old_term && s["last"] == 4
        })
        .await?;
    let processed = children[1]
        .wait(
            "current-term successful old-entry ACK processed by new leader",
            |s| {
                let p = &s["processed_append"];
                p["from"] == 3
                    && p["index"] == 3
                    && p["reject"] == false
                    && p["term"] == s["term"]
                    && p["core_term"] == s["term"]
                    && p["rejected_before_success"] == true
            },
        )
        .await?;
    let proof = &processed["processed_append"];
    ensure(
        proof["voters"] == json!([1, 2, 3])
            && proof["matched"][0].as_u64().unwrap() < 4
            && proof["matched"][1] == 4
            && proof["matched"][2] == 3,
        "old-term ACK did not establish actual core majority at three only",
    )?;
    ensure(
        proof["commit"].as_u64().unwrap() < 3,
        "old-term majority committed at the processed-response frontier",
    )?;
    children[2]
        .wait("current-term index-four ACK held in actual follower", |s| {
            s["last"] == 4 && s["held"].as_u64().unwrap() > 0
        })
        .await?;
    let before = children[1].command(json!({"op":"status"})).await?;
    ensure(
        before["commit"].as_u64().unwrap() < 3,
        "old-term majority committed without current-term entry",
    )?;
    children[2].command(json!({"op":"release"})).await?;
    children[1]
        .wait("current-term majority commits earlier intent", |s| {
            s["commit"] == 4
        })
        .await?;
    // Heal via the actual core's retry/reconnect path; only the old uncommitted
    // suffix may be replaced. The committed prefix is checked after PID joins.
    for id in 1..=3 {
        block(children, id, vec![]).await?;
    }
    children[1].command(json!({"op":"tick","n":1})).await?;
    children[0]
        .wait("higher-term stepdown and divergent-tail repair", |s| {
            s["role"] == "Follower"
                && s["term"].as_u64().unwrap() > old_term
                && s["last"] == 4
                && s["commit"] == 4
        })
        .await?;
    children[1].command(json!({"op":"tick","n":1})).await?;
    committed(children, 4).await?;
    // A stale captured old-term response, delivered again over TCP, cannot
    // restore the former leader or change its term/commit authority.
    children[1].command(json!({"op":"replay"})).await?;
    children[0]
        .wait(
            "delayed old-term response cannot restore former leader",
            |s| {
                s["last_from"] == 2
                    && s["last_term"] == old_term
                    && s["last_index"] == 2
                    && s["role"] == "Follower"
                    && s["term"].as_u64().unwrap() > old_term
                    && s["commit"] == 4
            },
        )
        .await?;
    let final_admission = children[1].command(json!({"op":"propose","n":12})).await?;
    ensure(
        final_admission["index"] == 5,
        "healed group failed to progress",
    )?;
    children[1]
        .wait("healed current-term progress", |s| s["commit"] == 5)
        .await?;
    children[1].command(json!({"op":"tick","n":1})).await?;
    committed(children, 5).await?;
    let mut inventories = Vec::new();
    for child in children.iter_mut() {
        inventories.push(child.stop().await?);
    }
    for inv in &inventories {
        ensure(
            inv["entries"] == inventories[0]["entries"]
                && inv["commit"] == 5
                && inv["entries"][0]["blank"] == true
                && inv["entries"][0]["ordinal"].is_null()
                && inv["entries"][1]["ordinal"] == 1
                && inv["entries"][2]["ordinal"] == 2
                && inv["entries"][3]["blank"] == true
                && inv["entries"][4]["ordinal"] == 3,
            "history, committed prefix or application ordinal changed",
        )?;
    }
    Ok(inventories)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_process_elections_current_term_commit_partition_heal_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let sockets: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let config = Config {
        nonce: format!("driver-{}", std::process::id()),
        root: directory.path().to_path_buf(),
        endpoints: std::array::from_fn(|i| sockets[i].local_addr().unwrap().to_string()),
    };
    drop(sockets);
    let path = directory.path().join("config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let mut children = Vec::new();
    let outcome = std::panic::AssertUnwindSafe(async {
        for id in 1..=3 {
            children.push(Original::spawn(&config, &path, id, true)?);
        }
        let inventories = election_replication(&mut children, &config).await?;
        // Reopen only AFTER each original PID exited, never after a deadline or
        // a mere local-write receipt. Distinct real directories are preserved.
        for id in 1..=3 {
            let store = ConsensusStore::open(
                storage::ConsensusFjallStore::open(config.store(id))?,
                config.identity(),
                id,
                RetainedLogLimits::default(),
            )?;
            ensure(
                inventory(&store)?["entries"] == inventories[0]["entries"],
                "post-exit disk recovery changed history",
            )?;
        }
        restart_vote_checks(&mut children, &config, &path, &inventories[2]).await?;
        let start = children.len();
        for id in 1..=3 {
            children.push(Original::spawn(&config, &path, id, false)?);
        }
        let restarted = &mut children[start..];
        for child in restarted.iter_mut() {
            ensure(child.event().await?["kind"] == "ready", "restart not ready")?;
        }
        restarted[0].command(json!({"op":"campaign"})).await?;
        restarted[0]
            .wait("all three durable restarts elect a fresh leader", |s| {
                s["role"] == "Leader" && s["commit"] == 6
            })
            .await?;
        let a = restarted[0].command(json!({"op":"large"})).await?;
        ensure(a["index"] == 7, "restart admission index lost")?;
        restarted[0]
            .wait("restart real-core replication progress", |s| {
                s["commit"] == 7
            })
            .await?;
        restarted[0].command(json!({"op":"tick","n":1})).await?;
        committed(restarted, 7).await?;
        let expected = format!("{:x}", Sha256::digest(full_proposal()));
        let expected_context = format!("{:x}", Sha256::digest(full_context()));
        for child in restarted.iter_mut() {
            let inv = child.stop().await?;
            ensure(
                inv["entries"].as_array().unwrap()[..5]
                    == inventories[0]["entries"].as_array().unwrap()[..],
                "restarted committed prefix changed",
            )?;
            ensure(
                inv["entries"][6]["digest"] == expected,
                "full legal proposal changed across actual TCP",
            )?;
            ensure(
                inv["entries"][6]["context_len"] == cluster::consensus::MAX_CONSENSUS_CONTEXT_BYTES
                    && inv["entries"][6]["context_digest"] == expected_context,
                "full original entry context changed across actual TCP",
            )?;
            println!(
                "driver context replicated node={} length={} digest={}",
                child.id, inv["entries"][6]["context_len"], inv["entries"][6]["context_digest"]
            );
        }
        // All original PIDs above are joined and their stores released. Verify
        // each physical store again, against ORIGINAL expected bytes, not just
        // agreement between replicas (which could all have lost the context).
        for id in 1..=3 {
            let store = ConsensusStore::open(
                storage::ConsensusFjallStore::open(config.store(id))?,
                config.identity(),
                id,
                RetainedLogLimits::default(),
            )?;
            let recovered = inventory(&store)?;
            ensure(
                recovered["entries"][6]["digest"] == expected
                    && recovered["entries"][6]["context_len"]
                        == cluster::consensus::MAX_CONSENSUS_CONTEXT_BYTES
                    && recovered["entries"][6]["context_digest"] == expected_context,
                "post-exit disk recovery changed full original proposal/context",
            )?;
            println!(
                "driver context post-exit reopen node={id} length={} digest={}",
                recovered["entries"][6]["context_len"], recovered["entries"][6]["context_digest"]
            );
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    let mut forced = false;
    for child in &mut children {
        if !child.joined {
            forced = true;
            child.force_join().await.unwrap();
        }
    }
    match outcome {
        Ok(result) => result.unwrap(),
        Err(payload) => std::panic::resume_unwind(payload),
    }
    assert!(!forced, "forced process cleanup is not graceful success");
}

async fn restart_vote_checks(
    children: &mut Vec<Original>,
    config: &Config,
    path: &Path,
    prior: &Value,
) -> Result<()> {
    // These explicit protocol challenges test core vote refusal, not synthetic
    // election/quorum evidence. The fresh/healed elections above are core-run.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(8);
    let handler = move |message: Message| {
        let tx = tx.clone();
        async move {
            tx.try_send(message)
                .map_err(|_| cluster::transport::TransportError::Handler("vote capture bound"))
        }
    };
    let (mut one, send_one) = PeerTransport::bind(
        config.identity(),
        1,
        TransportLimits::default(),
        handler.clone(),
    )
    .await?;
    let (mut two, send_two) =
        PeerTransport::bind(config.identity(), 2, TransportLimits::default(), handler).await?;
    for pass in 0..2 {
        children.push(Original::spawn(config, path, 3, false)?);
        let node = children.last_mut().unwrap();
        ensure(node.event().await?["kind"] == "ready", "restart not ready")?;
        let initial = node.command(json!({"op":"status"})).await?;
        ensure(
            initial["commit"] == 5 && initial["vote"] == 2,
            "persisted vote/frontier lost on restart",
        )?;
        let term = prior["term"].as_u64().unwrap();
        let last_term = prior["entries"][4]["term"].as_u64().unwrap();
        for (from, log_term, index, rejected) in [
            (2, last_term - 1, 100, true),
            (2, last_term, 5, false),
            (1, last_term, 5, true),
        ] {
            let message = Message {
                from,
                to: 3,
                term,
                index,
                log_term,
                msg_type: MessageType::MsgRequestVote,
                ..Default::default()
            };
            let mut receipt = if from == 1 {
                send_one.try_send(message)?
            } else {
                send_two.try_send(message)?
            };
            let response=timeout(DEADLINE,async {tokio::select! {response=rx.recv()=>response.ok_or("missing response"),_=one.serve()=>Err("capture one stopped"),_=two.serve()=>Err("capture two stopped")}}).await??;
            ensure(
                response.get_msg_type() == MessageType::MsgRequestVoteResponse
                    && response.to == from
                    && response.reject == rejected,
                "core log freshness or one-vote fence failed",
            )?;
            tokio::select! {r=receipt.wait()=>r?,_=one.serve()=>return Err("one stopped".into()),_=two.serve()=>return Err("two stopped".into())};
        }
        // Unknown declared voter: positively observe the transport's refusal
        // and actual IO join, not just an elapsed absence of a core event.
        let before = node.command(json!({"op":"status"})).await?;
        let codec = FrameCodec::new(config.identity(), 1, TransportLimits::default())?;
        let mut frame = codec.encode(&Message {
            from: 1,
            to: 3,
            term,
            msg_type: MessageType::MsgRequestVote,
            ..Default::default()
        })?;
        frame[40..48].copy_from_slice(&99u64.to_be_bytes());
        let mut stream =
            tokio::net::TcpStream::connect(config.identity().voters()[2].endpoint).await?;
        stream.write_all(&frame).await?;
        drop(stream);
        let refused = node
            .wait("unknown voter refused before core", |s| {
                s["refused"].as_u64().unwrap() > before["refused"].as_u64().unwrap()
                    && s["io_joined"].as_u64().unwrap() > before["io_joined"].as_u64().unwrap()
            })
            .await?;
        ensure(
            refused["events"] == before["events"],
            "unknown voter reached core",
        )?;
        let inv = node.stop().await?;
        ensure(
            inv["vote"] == 2 && inv["entries"] == prior["entries"],
            "restart vote checks mutated history",
        )?;
        println!("driver restart pass={pass} vote/freshness checked");
    }
    let (a, b) = tokio::join!(one.finish(), two.finish());
    ensure(a.panics == 0 && b.panics == 0, "capture originals failed")?;
    Ok(())
}
