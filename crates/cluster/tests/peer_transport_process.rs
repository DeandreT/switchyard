//! Real separate-process TCP/store evidence, not a Raft election/quorum demo.
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use cluster::{
    consensus::{ConsensusIdentity, ConsensusStore, RetainedLogLimits, Voter},
    transport::{
        FrameCodec, PeerTransport, SendReceipt, TransportError, TransportLimits, TransportStatus,
    },
};
use futures_util::FutureExt;
use protobuf::ProtobufEnum;
use raft::eraftpb::{Entry, HardState, Message, MessageType};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Semaphore, mpsc},
    time::timeout,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const CONTROL_LIMIT: usize = 4096;
const CONTROL_DEADLINE: Duration = Duration::from_secs(120);

#[derive(Clone, Serialize, Deserialize)]
struct PublicConfig {
    nonce: String,
    endpoints: [String; 3],
    root: PathBuf,
}
impl PublicConfig {
    fn identity(&self) -> ConsensusIdentity {
        ConsensusIdentity::new(
            "local-fixture".into(),
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

// A retained bounded line buffer survives cancellation of next(), unlike a
// dropped read_line future with an unretained partially consumed String.
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
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    if self.partial.is_empty() {
                        return Ok(None);
                    }
                    return Err("truncated control line".into());
                }
                Err(e) => return Err(e.into()),
            };
            if byte == b'\n' {
                return Ok(Some(String::from_utf8(std::mem::take(&mut self.partial))?));
            }
            if self.partial.len() == CONTROL_LIMIT {
                return Err("oversized control line".into());
            }
            self.partial.push(byte);
        }
    }
}

async fn emit(config: &PublicConfig, id: u64, kind: &str, details: Value) -> Result<()> {
    let packet = json!({"nonce":config.nonce,"node":id,"pid":std::process::id(),"kind":kind,"details":details});
    let mut bytes = serde_json::to_vec(&packet)?;
    if bytes.len() > CONTROL_LIMIT {
        return Err("oversized fixture event".into());
    }
    bytes.push(b'\n');
    let mut output = tokio::io::stdout();
    output.write_all(&bytes).await?;
    output.flush().await?;
    Ok(())
}
fn proposal(index: u64) -> Vec<u8> {
    domain::DurableProposal::unbound(domain::Command::new(
        domain::NamespaceName::new("tenant").unwrap(),
        domain::EntityPath::new(format!("q{index}")).unwrap(),
        domain::Timestamp::from_millis(index),
        domain::CommandKind::CreateQueue {
            config: domain::QueueConfig::default(),
        },
    ))
    .encode()
    .unwrap()
}
fn packet(from: u64, to: u64, index: u64, reply: bool, held: bool) -> Message {
    let mut message = Message {
        from,
        to,
        term: 1,
        index,
        commit: if reply { 0 } else { index },
        msg_type: if reply {
            MessageType::MsgAppendResponse
        } else {
            MessageType::MsgAppend
        },
        ..Default::default()
    };
    if held {
        message.context = b"hold".to_vec().into();
    }
    if !reply {
        message.entries.push(Entry {
            term: 1,
            index,
            data: proposal(index).into(),
            ..Default::default()
        });
    }
    message
}
fn summary(status: &TransportStatus, persisted: usize) -> Value {
    json!({"retired":status.retired,"active":status.global.connections,"dispatching":status.dispatching,"started":status.started,"joined":status.report.joined,"refused":status.report.refused,"deadlines":status.report.deadlines,"panics":status.report.panics,"persisted":persisted})
}

async fn child(config: PublicConfig, id: u64) -> Result<()> {
    let identity = config.identity();
    let store = Arc::new(Mutex::new(ConsensusStore::bootstrap(
        storage::ConsensusFjallStore::open(config.store(id))?,
        identity.clone(),
        id,
        RetainedLogLimits::default(),
    )?));
    let gate = Arc::new(Semaphore::new(0));
    let release = gate.clone();
    let persisted = Arc::new(AtomicUsize::new(0));
    let completed = persisted.clone();
    let (events, mut received) = mpsc::channel::<(&'static str, Value)>(32);
    let limits = TransportLimits {
        max_connections: 4,
        per_peer_connections: 1,
        max_operations: 4,
        per_peer_operations: 1,
        io_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let(mut owner,sender)=PeerTransport::bind(identity,id,limits,move|message:Message|{
        let store=store.clone();let gate=gate.clone();let events=events.clone();let completed=completed.clone();
        async move{
            events.try_send(("started",json!({"from":message.from,"index":message.index}))).map_err(|_|TransportError::Handler("fixture event quota"))?;
            if message.context.as_ref()==b"hold"{gate.acquire().await.map_err(|_|TransportError::Handler("fixture gate closed"))?.forget();}
            if message.get_msg_type()==MessageType::MsgAppend{
                // Trusted test callback only: explicit local persistence of this
                // supplied update, NOT an election, vote decision or quorum ack.
                let mut store=store.lock().unwrap();
                store.persist(&message.entries,Some(&HardState{term:message.term,vote:0,commit:message.commit,..Default::default()})).map_err(|_|TransportError::Handler("fixture persistence refused"))?;
                completed.fetch_add(1,Ordering::SeqCst);
                events.try_send(("persisted",json!({"from":message.from,"index":message.index}))).map_err(|_|TransportError::Handler("fixture event quota"))?;
            }else{
                events.try_send(("reply",json!({"from":message.from,"index":message.index,"type":message.get_msg_type().value()}))).map_err(|_|TransportError::Handler("fixture event quota"))?;
            }
            Ok(())
        }
    }).await?;
    emit(
        &config,
        id,
        "ready",
        json!({"store":config.store(id),"endpoint":config.endpoints[id as usize-1]}),
    )
    .await?;
    let mut input = Lines::new(tokio::io::stdin());
    let mut updates = sender.subscribe();
    let mut drained = false;
    let mut pending: Option<SendReceipt> = None;
    let mut barrier: Option<(String, u64)> = None;
    loop {
        let snapshot = updates.borrow_and_update().clone();
        if let Some((name, value)) = &barrier {
            let met = match name.as_str() {
                "active" => snapshot.global.connections >= *value as usize,
                "idle" => snapshot.global.connections == 0,
                "joined" => snapshot.report.joined >= *value,
                "refused" => snapshot.report.refused >= *value,
                "deadline" => snapshot.report.deadlines >= *value,
                _ => false,
            };
            if met {
                emit(
                    &config,
                    id,
                    "barrier",
                    summary(&snapshot, persisted.load(Ordering::SeqCst)),
                )
                .await?;
                barrier = None;
            }
        }
        tokio::select! {
            report=owner.serve(),if !drained=>{
                drained=true;
                emit(&config,id,"drained",json!({"joined":report.joined,"dispatched":report.dispatched,"panics":report.panics,"cancelled":report.cancelled,"handler_errors":report.handler_errors,"active":sender.status().global.connections,"persisted":persisted.load(Ordering::SeqCst)})).await?;
            }
            outcome=async{pending.as_mut().expect("guarded receipt").wait().await},if pending.is_some()=>{
                pending=None;
                emit(&config,id,if outcome.is_ok(){"written"}else{"send_error"},json!({"ok":outcome.is_ok()})).await?;
            }
            event=received.recv()=>{if let Some((kind,value))=event{emit(&config,id,kind,value).await?;}}
            _=updates.changed()=>{}
            command=input.next()=>{
                let text=command?.ok_or("control owner disappeared without drain")?;
                let command:Value=serde_json::from_str(&text)?;
                match command["kind"].as_str().ok_or("missing command kind")?{
                    "send"=>{
                        if pending.is_some(){return Err("duplicate pending fixture send".into());}
                        let message=packet(id,command["to"].as_u64().ok_or("missing destination")?,command["index"].as_u64().ok_or("missing index")?,command["reply"].as_bool().unwrap_or(false),command["held"].as_bool().unwrap_or(false));
                        pending=Some(sender.try_send(message)?);emit(&config,id,"queued",json!({})).await?;
                    }
                    "release"=>{release.add_permits(1);emit(&config,id,"released",json!({})).await?;}
                    "retire"=>{sender.retire();emit(&config,id,"retiring",summary(&sender.status(),persisted.load(Ordering::SeqCst))).await?;}
                    "status"=>emit(&config,id,"status",summary(&sender.status(),persisted.load(Ordering::SeqCst))).await?,
                    "barrier"=>{if barrier.is_some(){return Err("duplicate fixture barrier".into());}barrier=Some((command["name"].as_str().ok_or("missing barrier")?.into(),command["value"].as_u64().ok_or("missing barrier value")?));}
                    "exit"=>{if !drained{return Err("exit before original drain".into());}emit(&config,id,"exiting",json!({})).await?;break;}
                    _=>return Err("unknown fixture command".into()),
                }
            }
        }
    }
    drop(owner);
    drop(sender);
    Ok(())
}

#[test]
fn peer_fixture_child() {
    let Ok(path) = std::env::var("SWITCHYARD_PEER_FIXTURE") else {
        return;
    };
    let bytes = std::fs::read(path).unwrap();
    assert!(bytes.len() < 8192);
    let config: PublicConfig = serde_json::from_slice(&bytes).unwrap();
    let id = std::env::var("SWITCHYARD_PEER_ID")
        .unwrap()
        .parse()
        .unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(child(config, id))
        .unwrap();
}

struct OriginalChild {
    child: Child,
    input: ChildStdin,
    output: Lines<ChildStdout>,
    pid: u32,
    id: u64,
    nonce: String,
    pending: VecDeque<Value>,
    joined: bool,
    drained: bool,
}
impl OriginalChild {
    fn spawn(config: &PublicConfig, path: &Path, id: u64) -> Result<Self> {
        let stderr = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(config.root.join(format!("peer-{id}.stderr")))?;
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "peer_fixture_child",
                "--nocapture",
                "--format=terse",
                "--test-threads=1",
            ])
            .env("SWITCHYARD_PEER_FIXTURE", path)
            .env("SWITCHYARD_PEER_ID", id.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true)
            .spawn()?;
        let pid = child.id().ok_or("missing original PID")?;
        let input = child.stdin.take().ok_or("missing child stdin")?;
        let output = Lines::new(child.stdout.take().ok_or("missing child stdout")?);
        Ok(Self {
            child,
            input,
            output,
            pid,
            id,
            nonce: config.nonce.clone(),
            pending: VecDeque::new(),
            joined: false,
            drained: false,
        })
    }
    async fn command(&mut self, command: Value) -> Result<()> {
        let mut data = serde_json::to_vec(&command)?;
        if data.len() > CONTROL_LIMIT {
            return Err("command quota".into());
        }
        data.push(b'\n');
        timeout(CONTROL_DEADLINE, self.input.write_all(&data)).await??;
        Ok(())
    }
    async fn event(&mut self, kind: &str) -> Result<Value> {
        if let Some(position) = self.pending.iter().position(|v| v["kind"] == kind) {
            return Ok(self.pending.remove(position).unwrap());
        }
        timeout(CONTROL_DEADLINE, async {
            for _ in 0..64 {
                let line = self
                    .output
                    .next()
                    .await?
                    .ok_or("original child EOF before event")?;
                if !line.starts_with('{') {
                    continue;
                }
                let event: Value = serde_json::from_str(&line)?;
                if event["nonce"] != self.nonce
                    || event["node"] != self.id
                    || event["pid"] != self.pid
                {
                    return Err("foreign child control packet".into());
                }
                if event["kind"] == kind {
                    return Ok(event);
                }
                if self.pending.len() == 64 {
                    return Err("fixture pending event quota".into());
                }
                self.pending.push_back(event);
            }
            Err("fixture control work quota".into())
        })
        .await?
    }
    async fn stop_and_join(&mut self) -> Result<()> {
        if self.joined {
            return Ok(());
        }
        if !self.drained {
            self.command(json!({"kind":"retire"})).await?;
            self.event("retiring").await?;
            let event = self.event("drained").await?;
            ensure(
                event["details"]["active"] == 0
                    && event["details"]["panics"] == 0
                    && event["details"]["cancelled"] == 0
                    && event["details"]["handler_errors"] == 0,
                "unclean original transport drain",
            )?;
            self.drained = true;
        }
        self.command(json!({"kind":"exit"})).await?;
        self.event("exiting").await?;
        let status = timeout(CONTROL_DEADLINE, self.child.wait()).await??;
        self.joined = true;
        ensure(status.success(), "original process failed")?;
        Ok(())
    }
    async fn force_join(&mut self) -> Result<()> {
        if !self.joined {
            if self.child.try_wait()?.is_none() {
                self.child.start_kill()?;
            }
            timeout(CONTROL_DEADLINE, self.child.wait()).await??;
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
async fn send(
    children: &mut [OriginalChild],
    from: usize,
    to: u64,
    index: u64,
    reply: bool,
    held: bool,
) -> Result<()> {
    children[from - 1]
        .command(json!({"kind":"send","to":to,"index":index,"reply":reply,"held":held}))
        .await?;
    children[from - 1].event("written").await?;
    Ok(())
}

async fn scenario(children: &mut [OriginalChild], config: &PublicConfig) -> Result<()> {
    for child in children.iter_mut() {
        let ready = child.event("ready").await?;
        ensure(
            ready["details"]["store"] == json!(config.store(child.id)),
            "wrong store custody",
        )?;
    }
    // All three real processes own distinct profile-4 stores and persist through
    // actual TCP frames. No core campaign, synthetic vote or quorum ack exists.
    send(children, 1, 2, 1, false, false).await?;
    children[1].event("persisted").await?;
    children[1]
        .command(json!({"kind":"barrier","name":"idle","value":0}))
        .await?;
    children[1].event("barrier").await?;
    send(children, 1, 3, 1, false, false).await?;
    children[2].event("persisted").await?;
    children[2]
        .command(json!({"kind":"barrier","name":"idle","value":0}))
        .await?;
    children[2].event("barrier").await?;
    send(children, 2, 1, 1, false, false).await?;
    children[0].event("persisted").await?;
    // Delayed, then duplicate core response messages cross separate connections.
    // They reach dispatch as messages, never as transport-generated durable acks.
    send(children, 3, 1, 7, true, true).await?;
    let started = children[0].event("started").await?;
    // Earlier persistence also emitted Started, so select the explicit reply index.
    if started["details"]["index"] != 7 {
        let next = children[0].event("started").await?;
        ensure(
            next["details"]["index"] == 7,
            "missing delayed reply barrier",
        )?;
    }
    children[0].command(json!({"kind":"release"})).await?;
    children[0].event("released").await?;
    let reply = children[0].event("reply").await?;
    ensure(reply["details"]["index"] == 7, "wrong delayed response")?;
    children[0]
        .command(json!({"kind":"barrier","name":"joined","value":2}))
        .await?;
    children[0].event("barrier").await?;
    send(children, 3, 1, 7, true, false).await?;
    let duplicate = children[0].event("reply").await?;
    ensure(
        duplicate["details"]["index"] == 7 && duplicate["details"]["from"] == 3,
        "wrong duplicate response",
    )?;
    children[0]
        .command(json!({"kind":"barrier","name":"idle","value":0}))
        .await?;
    children[0].event("barrier").await?;
    // Foreign fingerprint is refused before dispatch/persistence; its original
    // task is positively reaped, rather than inferring absence from a delay.
    let codec = FrameCodec::new(config.identity(), 1, TransportLimits::default())?;
    let mut frame = codec.encode(&packet(1, 3, 2, false, false))?;
    frame[8] ^= 1;
    let mut foreign = TcpStream::connect(config.identity().voters()[2].endpoint).await?;
    foreign.write_all(&frame).await?;
    drop(foreign);
    children[2]
        .command(json!({"kind":"barrier","name":"refused","value":1}))
        .await?;
    let refused = children[2].event("barrier").await?;
    ensure(
        refused["details"]["persisted"] == 1,
        "foreign frame reached persistence",
    )?;
    // Sender closes its whole one-frame socket before this callback is released.
    // A canceled control observer and retirement cannot cancel the original.
    send(children, 1, 2, 2, false, true).await?;
    loop {
        let begun = children[1].event("started").await?;
        if begun["details"]["index"] == 2 {
            break;
        }
    }
    children[1].command(json!({"kind":"retire"})).await?;
    let held = children[1].event("retiring").await?;
    ensure(
        held["details"]["dispatching"] == 1 && held["details"]["persisted"] == 1,
        "held original was lost or committed early",
    )?;
    // A different healthy peer still progresses while one original is held.
    send(children, 1, 3, 2, false, false).await?;
    let healthy = children[2].event("persisted").await?;
    ensure(
        healthy["details"]["index"] == 2,
        "healthy peer did not progress",
    )?;
    children[1].command(json!({"kind":"release"})).await?;
    children[1].event("released").await?;
    let persisted = children[1].event("persisted").await?;
    ensure(
        persisted["details"]["index"] == 2,
        "held original did not persist",
    )?;
    let drained = children[1].event("drained").await?;
    ensure(
        drained["details"]["persisted"] == 2
            && drained["details"]["active"] == 0
            && drained["details"]["panics"] == 0
            && drained["details"]["cancelled"] == 0
            && drained["details"]["handler_errors"] == 0,
        "held original did not join exactly once",
    )?;
    children[1].drained = true;
    children[1].stop_and_join().await?;
    {
        let store = ConsensusStore::open(
            storage::ConsensusFjallStore::open(config.store(2))?,
            config.identity(),
            2,
            RetainedLogLimits::default(),
        )?;
        ensure(
            store.hard_state()?.commit == 2 && store.read_entries(1, 64)?.len() == 2,
            "reopen after original PID join lost local committed history",
        )?;
    }
    // Partial-header expiry and retirement both close whole sockets while the
    // raw peer leaves its other direction open. Named accept/join barriers.
    children[2]
        .command(json!({"kind":"barrier","name":"idle","value":0}))
        .await?;
    children[2].event("barrier").await?;
    let mut idle = TcpStream::connect(config.identity().voters()[2].endpoint).await?;
    idle.write_all(b"SW").await?;
    children[2]
        .command(json!({"kind":"barrier","name":"active","value":1}))
        .await?;
    children[2].event("barrier").await?;
    let mut byte = [0];
    ensure(
        timeout(CONTROL_DEADLINE, idle.read(&mut byte)).await?? == 0,
        "idle partial frame retained a reader half",
    )?;
    children[2]
        .command(json!({"kind":"barrier","name":"deadline","value":1}))
        .await?;
    children[2].event("barrier").await?;
    let mut idle = TcpStream::connect(config.identity().voters()[0].endpoint).await?;
    children[0]
        .command(json!({"kind":"barrier","name":"active","value":1}))
        .await?;
    children[0].event("barrier").await?;
    children[0].command(json!({"kind":"retire"})).await?;
    children[0].event("retiring").await?;
    ensure(
        timeout(CONTROL_DEADLINE, idle.read(&mut byte)).await?? == 0,
        "retirement retained an idle reader",
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_original_processes_real_tcp_and_distinct_durable_owners() {
    static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
    let directory = tempfile::tempdir().unwrap();
    let sockets: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let config = PublicConfig {
        nonce: format!(
            "peer-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::SeqCst)
        ),
        endpoints: std::array::from_fn(|i| sockets[i].local_addr().unwrap().to_string()),
        root: directory.path().to_path_buf(),
    };
    drop(sockets);
    let path = directory.path().join("public-config.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
    println!(
        "peer-fixture public-config={}",
        serde_json::to_string(&config).unwrap()
    );
    let mut children = Vec::new();
    let outcome = std::panic::AssertUnwindSafe(async {
        for id in 1..=3 {
            children.push(OriginalChild::spawn(&config, &path, id)?);
        }
        let receipt: Vec<_> = children
            .iter()
            .map(|c| json!({"node":c.id,"pid":c.pid,"store":config.store(c.id)}))
            .collect();
        std::fs::write(
            config.root.join("original-pids.json"),
            serde_json::to_vec_pretty(&receipt)?,
        )?;
        scenario(&mut children, &config).await?;
        for child in &mut children {
            child.stop_and_join().await?;
        }
        for id in 1..=3 {
            let store = ConsensusStore::open(
                storage::ConsensusFjallStore::open(config.store(id))?,
                config.identity(),
                id,
                RetainedLogLimits::default(),
            )?;
            let expected = if id == 1 { 1 } else { 2 };
            ensure(
                store.last_index()? == expected && store.hard_state()?.commit == expected,
                "final reopen did not preserve exact unprojected local history",
            )?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    let mut forced = false;
    let mut cleanup_errors = Vec::new();
    for child in &mut children {
        if !child.joined {
            forced = true;
            if let Err(error) = child.force_join().await {
                cleanup_errors.push(error.to_string());
            }
        }
    }
    println!(
        "peer-fixture nonce={} originals={:?} joined={} forced_cleanup={forced}",
        config.nonce,
        children.iter().map(|c| c.pid).collect::<Vec<_>>(),
        children.iter().all(|c| c.joined)
    );
    if outcome.as_ref().is_err() || outcome.as_ref().is_ok_and(|r| r.is_err()) || forced {
        for id in 1..=3 {
            if let Ok(stderr) =
                std::fs::read_to_string(config.root.join(format!("peer-{id}.stderr")))
            {
                eprintln!(
                    "peer-{id}: {}",
                    stderr.chars().take(4096).collect::<String>()
                );
            }
        }
    }
    assert!(
        cleanup_errors.is_empty(),
        "failed original PID cleanup: {cleanup_errors:?}"
    );
    match outcome {
        Ok(result) => result.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    assert!(
        !forced,
        "forced process termination is not a successful drain"
    );
}
