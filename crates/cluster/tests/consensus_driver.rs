use cluster::{
    consensus::{
        ConsensusIdentity, Voter,
        driver::{AdmissionError, ConsensusDriver, DriverOptions},
    },
    transport::{FrameCodec, TransportLimits},
};
use futures_util::FutureExt;
use raft::eraftpb::{Message, MessageType};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::AsyncWriteExt;

#[derive(Default)]
struct FaultControl {
    armed: std::sync::atomic::AtomicBool,
    gate: std::sync::Mutex<bool>,
    release: std::sync::Condvar,
    entered: tokio::sync::Notify,
    after_apply: std::sync::atomic::AtomicBool,
}
enum Backend {
    Memory(storage::ConsensusMemoryStore),
    Fjall(storage::ConsensusFjallStore),
}
struct FaultBackend {
    inner: Backend,
    control: Arc<FaultControl>,
    fault: u8,
}
impl storage::ConsensusBackend for FaultBackend {
    fn get(&self, key: &[u8]) -> Result<Option<storage::Value>, storage::StorageError> {
        match &self.inner {
            Backend::Memory(b) => b.get(key),
            Backend::Fjall(b) => b.get(key),
        }
    }
    fn scan_from(
        &self,
        p: &[u8],
        s: &[u8],
        n: usize,
    ) -> Result<Vec<(storage::Key, storage::Value)>, storage::StorageError> {
        match &self.inner {
            Backend::Memory(b) => b.scan_from(p, s, n),
            Backend::Fjall(b) => b.scan_from(p, s, n),
        }
    }
    fn apply(&mut self, batch: storage::WriteBatch) -> Result<(), storage::StorageError> {
        let armed = self.control.armed.swap(false, Ordering::SeqCst);
        if armed {
            self.control.entered.notify_one();
            let mut released = self.control.gate.lock().unwrap();
            while !*released {
                released = self.control.release.wait(released).unwrap();
            }
        }
        if armed && self.fault == 1 {
            return Err(injected());
        }
        match &mut self.inner {
            Backend::Memory(b) => b.apply(batch)?,
            Backend::Fjall(b) => b.apply(batch)?,
        };
        if armed {
            self.control.after_apply.store(true, Ordering::SeqCst);
            if self.fault == 2 {
                return Err(injected());
            }
            assert_ne!(self.fault, 3, "original after-SyncAll panic");
        }
        Ok(())
    }
}
fn injected() -> storage::StorageError {
    storage::StorageError::Backend {
        operation: "driver TCP fault",
        detail: "original result ambiguous".into(),
    }
}

fn identity() -> ConsensusIdentity {
    let sockets: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ConsensusIdentity::new(
        "local".into(),
        format!("driver-{}", sockets[0].local_addr().unwrap().port()),
        domain::NamespaceName::new("tenant").unwrap(),
        std::array::from_fn(|i| Voter {
            id: i as u64 + 1,
            endpoint: sockets[i].local_addr().unwrap(),
        }),
    )
    .unwrap()
}
fn options() -> DriverOptions {
    DriverOptions {
        tick_interval: None,
        ..Default::default()
    }
}

#[tokio::test]
async fn every_bad_configuration_or_bind_refuses_before_backend_factory() {
    let identity = identity();
    let opened = Arc::new(AtomicUsize::new(0));
    let factory = || {
        opened.fetch_add(1, Ordering::SeqCst);
        Ok(storage::ConsensusMemoryStore::default())
    };
    let mut invalid = options();
    invalid.retained.max_entries = 0;
    assert!(
        ConsensusDriver::bind(identity.clone(), 1, invalid, true, factory)
            .await
            .is_err()
    );
    assert!(
        ConsensusDriver::bind(identity.clone(), 99, options(), true, factory)
            .await
            .is_err()
    );
    let busy = std::net::TcpListener::bind(identity.voters()[0].endpoint).unwrap();
    assert!(
        ConsensusDriver::bind(identity, 1, options(), true, factory)
            .await
            .is_err()
    );
    drop(busy);
    assert_eq!(opened.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn retirement_fences_queued_admissions_and_keeps_original_receipt_credits() {
    let (driver, handle) = ConsensusDriver::bind(identity(), 1, options(), true, || {
        Ok(storage::ConsensusMemoryStore::default())
    })
    .await
    .unwrap();
    let mut campaign = handle.campaign().unwrap();
    let mut tick = handle.tick().unwrap();
    // Extraction refuses without losing the owner or its queued originals.
    let mut driver = *driver.into_finished_store().err().unwrap();
    driver.finish().await;
    assert!(matches!(handle.campaign(), Err(AdmissionError::Retired)));
    assert_eq!(handle.status().pending_events, 2);
    assert_eq!(driver.status().advanced, 0);
    assert_eq!(driver.status().term, 0);
    assert_eq!(campaign.wait().await, Err(AdmissionError::Retired));
    assert_eq!(tick.wait().await, Err(AdmissionError::Retired));
    assert_eq!(handle.status().pending_events, 0);
    let store = driver.into_finished_store().ok().unwrap().reopen().unwrap();
    assert_eq!(store.hard_state().unwrap().term, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_run_borrower_keeps_original_core_store_and_io_custody() {
    let identity = identity();
    let (mut driver, handle) = ConsensusDriver::bind(identity.clone(), 2, options(), true, || {
        Ok(storage::ConsensusMemoryStore::default())
    })
    .await
    .unwrap();
    let codec = FrameCodec::new(identity.clone(), 1, TransportLimits::default()).unwrap();
    let frame = codec
        .encode(&Message {
            from: 1,
            to: 2,
            term: 7,
            msg_type: MessageType::MsgHeartbeat,
            ..Default::default()
        })
        .unwrap();
    let mut stream = tokio::net::TcpStream::connect(identity.voters()[1].endpoint)
        .await
        .unwrap();
    stream.write_all(&frame).await.unwrap();
    stream.shutdown().await.unwrap();
    let witness = async {
        for _ in 0..10000 {
            if handle.status().peer_events == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("named persisted peer event absent");
    };
    tokio::time::timeout(Duration::from_secs(60), async {
        tokio::select! {_=driver.run()=>panic!("unexpected retirement"),_=witness=>{}}
    })
    .await
    .unwrap();
    // The canceled run future leaves the same term/vote/store owner intact.
    assert_eq!(driver.status().term, 7);
    assert_eq!(driver.status().applied, 0);
    assert!(!driver.status().retired);
    let mut receipt = handle.tick().unwrap();
    tokio::select! {_=driver.run()=>panic!("unexpected retirement"),value=receipt.wait()=>value.unwrap()};
    // Establish an actually admitted partial-header original, not a sleep-based
    // claim that work is probably active. Serve borrower cancellation retains it.
    let mut idle = tokio::net::TcpStream::connect(identity.voters()[1].endpoint)
        .await
        .unwrap();
    idle.write_all(b"SW").await.unwrap();
    for _ in 0..10000 {
        assert!(driver.run().now_or_never().is_none());
        if driver.transport_status().global.operations > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(driver.transport_status().global.operations > 0);
    let report = driver.finish().await;
    assert!(matches!(handle.tick(), Err(AdmissionError::Retired)));
    assert_eq!(report.panics, 0);
    assert_eq!(report.cancelled, 0);
    assert!(driver.status().finished);
    assert_eq!(driver.transport_status().global.operations, 0);
    assert_eq!(driver.status().pending_outbound, 0);
    let store = driver.into_finished_store().ok().unwrap().reopen().unwrap();
    assert_eq!(store.hard_state().unwrap().term, 7);
    assert_eq!(store.last_index().unwrap(), 0);
}

#[cfg(feature = "consensus-driver-testkit")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_finish_must_retain_the_same_genuinely_held_dispatch_until_join() {
    let identity = identity();
    let (mut driver, handle) = ConsensusDriver::bind(identity.clone(), 2, options(), true, || {
        Ok(storage::ConsensusMemoryStore::default())
    })
    .await
    .unwrap();
    let gate = driver.dispatch_gate();
    assert!(gate.arm());
    let frame = FrameCodec::new(identity.clone(), 1, TransportLimits::default())
        .unwrap()
        .encode(&Message {
            from: 1,
            to: 2,
            term: 7,
            msg_type: MessageType::MsgHeartbeat,
            ..Default::default()
        })
        .unwrap();
    let mut stream = tokio::net::TcpStream::connect(identity.voters()[1].endpoint)
        .await
        .unwrap();
    stream.write_all(&frame).await.unwrap();
    stream.shutdown().await.unwrap();
    let witness = async {
        for _ in 0..10000 {
            if gate.held() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("named post-driver original dispatch gate was not reached");
    };
    tokio::time::timeout(Duration::from_secs(60), async {
        tokio::select! {_=driver.run()=>panic!("unexpected retirement"),_=witness=>{}}
    })
    .await
    .unwrap();
    let before = driver.transport_status();
    assert_eq!(before.dispatching, 1);
    assert!(before.global.operations >= 1);
    assert_eq!(driver.status().term, 7);
    assert_eq!(driver.status().peer_events, 1);
    assert!(!gate.returned());
    // This is the ACTUAL accepted transport dispatch, after its core/store work,
    // not idle IO or an unrelated artificially pending future. Its handler
    // cannot return until the explicit gate releases the retained original.
    assert!(
        driver.finish().now_or_never().is_none(),
        "finish must be Poll::Pending while its genuine original dispatch is held"
    );
    let canceled = driver.transport_status();
    assert!(driver.status().retired);
    assert!(!driver.status().finished);
    assert_eq!(canceled.started, before.started);
    assert_eq!(canceled.dispatching, 1);
    assert!(canceled.global.operations >= 1);
    assert!(gate.held());
    assert!(!gate.returned());
    assert!(matches!(handle.tick(), Err(AdmissionError::Retired)));
    // Extraction must also refuse while that SAME owner still owes the join.
    let mut driver = *driver.into_finished_store().err().unwrap();
    assert_eq!(driver.transport_status().dispatching, 1);
    assert!(gate.held());
    println!(
        "driver named finish frontier: pending borrower canceled; started={} dispatching={} originals retained",
        canceled.started, canceled.dispatching
    );
    assert!(gate.release());
    let report = driver.finish().await;
    let finished = driver.transport_status();
    assert!(gate.returned());
    assert_eq!(report.panics, 0);
    assert_eq!(report.cancelled, 0);
    assert_eq!(report.dispatched, 1);
    // `started` counts begun dispatches, not outbound originals. At the canceled
    // finish frontier admissions/queue are closed; each remaining operation is
    // a retained original task that must contribute one actual join.
    assert_eq!(report.dispatched, finished.started);
    assert_eq!(
        report.joined,
        canceled.report.joined + canceled.global.operations as u64
    );
    assert_eq!(finished.started, before.started);
    assert_eq!(finished.dispatching, 0);
    assert_eq!(finished.global.operations, 0);
    assert!(driver.status().finished);
    let store = driver.into_finished_store().ok().unwrap().reopen().unwrap();
    assert_eq!(store.hard_state().unwrap().term, 7);
    assert_eq!(store.last_index().unwrap(), 0);
    println!(
        "driver named finish frontier: same original returned and all {} tasks actually joined before reopen",
        report.joined
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_tcp_vote_append_faults_join_without_durable_replies_even_after_syncall() {
    for durable in [false, true] {
        for append in [false, true] {
            for fault in [1, 2, 3] {
                let identity = identity();
                let owner_identity = identity.clone();
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("store");
                let control = Arc::new(FaultControl::default());
                let original_control = control.clone();
                let (ready, started) = tokio::sync::oneshot::channel();
                // An explicitly retained original OS thread owns the async driver and
                // synchronous backend. It is never aborted/detached, including panic
                // injection. Parent cleanup releases the gate before joining it.
                let original = std::thread::spawn(move || {
                    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async move {
                let(mut driver,handle)=ConsensusDriver::bind(owner_identity,2,options(),true,||Ok(FaultBackend {inner:if durable{Backend::Fjall(storage::ConsensusFjallStore::open(path)?)}else{Backend::Memory(storage::ConsensusMemoryStore::default())},control:original_control.clone(),fault})).await.unwrap();
                original_control.armed.store(true,Ordering::SeqCst);ready.send(handle).ok().unwrap();
                let report=driver.run().await;let status=driver.status();let panic=driver.panic_payload().is_some();let transport=driver.transport_status();
                let store=driver.into_finished_store().ok().unwrap();(store,report,status,panic,transport)
            })
                });
                let handle = started.await.unwrap();
                let (tx, mut replies) = tokio::sync::mpsc::channel::<Message>(4);
                let (mut peer, _) = cluster::transport::PeerTransport::bind(
                    identity.clone(),
                    1,
                    TransportLimits::default(),
                    move |message: Message| {
                        let tx = tx.clone();
                        async move {
                            tx.try_send(message).map_err(|_| {
                                cluster::transport::TransportError::Handler("reply quota")
                            })
                        }
                    },
                )
                .await
                .unwrap();
                let outcome=std::panic::AssertUnwindSafe(async {
            let mut message=Message{from:1,to:2,term:1,msg_type:if append{MessageType::MsgAppend}else{MessageType::MsgRequestVote},..Default::default()};
            if append{message.entries=vec![raft::eraftpb::Entry{index:1,term:1,..Default::default()}].into();}
            let frame=FrameCodec::new(identity.clone(),1,TransportLimits::default()).unwrap().encode(&message).unwrap();
            let mut stream=tokio::net::TcpStream::connect(identity.voters()[1].endpoint).await.unwrap();stream.write_all(&frame).await.unwrap();stream.shutdown().await.unwrap();
            tokio::time::timeout(Duration::from_secs(60),async{tokio::select!{_=control.entered.notified()=>{},_=peer.serve()=>panic!("capture owner unexpectedly ended")}}).await.unwrap();
            let status=handle.status();assert!(status.processing);assert_eq!(status.advanced,0);assert_eq!(status.observed_vote,0);assert_eq!(status.observed_last,0);assert_eq!(status.pending_outbound,0);assert!(!control.after_apply.load(Ordering::SeqCst));
            println!("named pre-persist TCP barrier durable={durable} append={append} fault={fault}");
        }).catch_unwind().await;
                *control.gate.lock().unwrap() = true;
                control.release.notify_all();
                if outcome.is_err() {
                    handle.retire();
                }
                let (store, report, status, panic, transport) = original.join().unwrap();
                let cleanup = peer.finish().await;
                assert_eq!(cleanup.panics, 0);
                assert_eq!(transport.global.operations, 0);
                assert_eq!(
                    report.written, 0,
                    "no original durable reply may be emitted after a failed apply"
                );
                assert_eq!(report.handler_errors, 1);
                assert!(replies.try_recv().is_err());
                assert_eq!(status.advanced, 0);
                assert!(status.retired && status.finished && status.storage_uncertain);
                assert_eq!(panic, fault == 3);
                // Actual original driver thread has exited and relinquished its sole
                // store. Discover the committed batch rather than trust the old cache.
                let store = store.reopen().unwrap();
                assert_eq!(store.hard_state().unwrap().term, u64::from(fault != 1));
                assert_eq!(store.last_index().unwrap(), u64::from(fault != 1 && append));
                assert_eq!(
                    store.hard_state().unwrap().vote,
                    u64::from(fault != 1 && !append)
                );
                drop(store);
                if let Err(payload) = outcome {
                    std::panic::resume_unwind(payload);
                }
            }
        }
    }
}
