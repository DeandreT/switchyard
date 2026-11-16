//! Custody/fault fixtures use trusted peer input, not fabricated quorum evidence.
//! The separate process fixture establishes real majority/live leader results.
use cluster::{
    consensus::{
        ConsensusIdentity, Voter,
        driver::{
            DriverOptions,
            application::{ApplicationError, ApplicationLimits, ApplicationOwner},
        },
    },
    transport::{FrameCodec, TransportLimits},
};
use futures_util::FutureExt;
use raft::eraftpb::{Entry, Message, MessageType};
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use storage::{ConsensusBackend, Key, Mutation, StorageError, Value, WriteBatch};
use tokio::io::AsyncWriteExt;

fn identity() -> ConsensusIdentity {
    let sockets: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ConsensusIdentity::new(
        "application-custody".into(),
        format!("unit-{}", sockets[0].local_addr().unwrap().port()),
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
fn proposal() -> Vec<u8> {
    domain::DurableProposal::unbound(domain::Command::new(
        domain::NamespaceName::new("tenant").unwrap(),
        domain::EntityPath::new("q").unwrap(),
        domain::Timestamp::from_millis(1),
        domain::CommandKind::CreateQueue {
            config: Default::default(),
        },
    ))
    .encode()
    .unwrap()
}
async fn send(identity: &ConsensusIdentity, proposal: bool) {
    let frame = FrameCodec::new(identity.clone(), 1, TransportLimits::default())
        .unwrap()
        .encode(&Message {
            from: 1,
            to: 2,
            term: 1,
            msg_type: if proposal {
                MessageType::MsgAppend
            } else {
                MessageType::MsgHeartbeat
            },
            commit: if proposal { 1 } else { 0 },
            entries: if proposal {
                vec![Entry {
                    term: 1,
                    index: 1,
                    data: self::proposal().into(),
                    ..Default::default()
                }]
                .into()
            } else {
                Default::default()
            },
            ..Default::default()
        })
        .unwrap();
    let mut stream = tokio::net::TcpStream::connect(identity.voters()[1].endpoint)
        .await
        .unwrap();
    stream.write_all(&frame).await.unwrap();
    stream.shutdown().await.unwrap();
}
#[derive(Default)]
struct Control {
    entered: tokio::sync::Notify,
    release: Condvar,
    gate: Mutex<bool>,
    armed: AtomicBool,
    completed: AtomicUsize,
}
struct Backend {
    inner: Box<dyn ConsensusBackend>,
    control: Arc<Control>,
    tag: u8,
    skip: usize,
    fault: u8,
}
impl ConsensusBackend for Backend {
    fn get(&self, k: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(k)
    }
    fn scan_from(&self, p: &[u8], s: &[u8], n: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        assert!(n <= 64);
        self.inner.scan_from(p, s, n)
    }
    fn apply(&mut self, b: WriteBatch) -> Result<(), StorageError> {
        let matches = b.mutations().iter().any(|m| match m {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key.first() == Some(&self.tag),
        });
        let armed = if matches && self.control.armed.load(Ordering::SeqCst) {
            if self.skip > 0 {
                self.skip -= 1;
                false
            } else {
                self.control.armed.swap(false, Ordering::SeqCst)
            }
        } else {
            false
        };
        if armed {
            self.control.entered.notify_one();
            let mut gate = self.control.gate.lock().unwrap();
            while !*gate {
                gate = self.control.release.wait(gate).unwrap();
            }
        }
        if armed && self.fault == 1 {
            return Err(injected());
        }
        self.inner.apply(b)?;
        if armed {
            self.control.completed.fetch_add(1, Ordering::SeqCst);
            if self.fault == 2 {
                return Err(injected());
            }
            assert_ne!(self.fault, 3, "after full composed atomic apply panic");
        }
        Ok(())
    }
}
fn injected() -> StorageError {
    StorageError::Backend {
        operation: "application original fault",
        detail: "actual atomic return ambiguity".into(),
    }
}
fn release(control: &Control) {
    *control.gate.lock().unwrap() = true;
    control.release.notify_all();
}
fn plain(durable: bool, path: &std::path::Path) -> Result<Backend, StorageError> {
    Ok(Backend {
        inner: if durable {
            Box::new(storage::ConsensusFjallStore::open(path)?)
        } else {
            Box::new(storage::ConsensusMemoryStore::default())
        },
        control: Arc::new(Control::default()),
        tag: 0,
        skip: 0,
        fault: 0,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn begun_atomic_apply_keeps_same_owner_across_cancel_request_until_original_return() {
    for durable in [false, true] {
        let identity = identity();
        let directory = tempfile::tempdir().unwrap();
        let control = Arc::new(Control::default());
        let (owner, handle) = ApplicationOwner::bind(
            identity.clone(),
            2,
            options(),
            ApplicationLimits::default(),
            true,
            || {
                Ok(Backend {
                    inner: if durable {
                        Box::new(storage::ConsensusFjallStore::open(directory.path())?)
                    } else {
                        Box::new(storage::ConsensusMemoryStore::default())
                    },
                    control: control.clone(),
                    tag: 0xf1,
                    skip: 0,
                    fault: 0,
                })
            },
        )
        .await
        .unwrap();
        let mut queued = handle.submit(proposal(), vec![], 0).unwrap();
        // Queue ownership, not a leader result: peer/control capacity remains
        // independent while this original reserves the complete reply budget.
        assert_eq!(handle.status().originals, 1);
        assert!(matches!(
            handle.submit(proposal(), vec![], 0),
            Err(ApplicationError::Busy)
        ));
        control.armed.store(true, Ordering::SeqCst);
        let (cancel, mut cancellation) = tokio::sync::oneshot::channel();
        let returned = Arc::new(AtomicBool::new(false));
        let witness = returned.clone();
        let original = tokio::spawn(async move {
            let mut owner = owner;
            tokio::select! {biased;_= &mut cancellation=>{},_=owner.run()=>panic!("unexpected retirement")};
            witness.store(true, Ordering::SeqCst);
            owner
        });
        send(&identity, true).await;
        let outcome = std::panic::AssertUnwindSafe(async {
            tokio::time::timeout(Duration::from_secs(60), control.entered.notified())
                .await
                .unwrap();
            cancel.send(()).unwrap();
            assert!(!returned.load(Ordering::SeqCst));
            assert_eq!(control.completed.load(Ordering::SeqCst), 0);
            assert_eq!(
                handle.status().applied_ordinal,
                0,
                "a begun apply is not completion"
            );
            assert!(queued.wait().now_or_never().is_some()); // definite local NotLeader
            assert_eq!(handle.status().originals, 0);
        })
        .catch_unwind()
        .await;
        release(&control);
        let mut owner = tokio::time::timeout(Duration::from_secs(60), original)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(control.completed.load(Ordering::SeqCst), 1);
        assert_eq!(owner.status().applied_ordinal, 1);
        assert_eq!(owner.driver_status().applied, 1);
        owner = *owner
            .into_closed()
            .err()
            .expect("unfinished owner must not extract");
        let report = owner.finish().await;
        assert_eq!((report.panics, report.cancelled), (0, 0));
        assert_eq!(owner.transport_status().global.operations, 0);
        let closed = owner.into_closed().ok().unwrap();
        let (mut reopened, _) = closed.reopen().await.unwrap();
        assert_eq!(reopened.status().applied_ordinal, 1);
        assert_eq!(reopened.driver_status().applied, 1);
        reopened.finish().await;
        if let Err(p) = outcome {
            std::panic::resume_unwind(p);
        }
    }
}

#[tokio::test]
async fn closed_reopen_preserves_unobserved_queued_original_count_and_byte_family() {
    for durable in [false, true] {
        let d = tempfile::tempdir().unwrap();
        let identity = identity();
        let (mut owner, handle) = ApplicationOwner::bind(
            identity,
            2,
            options(),
            ApplicationLimits::default(),
            true,
            || plain(durable, d.path()),
        )
        .await
        .unwrap();
        let mut receipt = handle.submit(proposal(), vec![], 0).unwrap();
        assert!(receipt.wait().now_or_never().is_none());
        let bytes = handle.status().original_bytes;
        owner.finish().await;
        let closed = owner.into_closed().ok().unwrap();
        let (mut owner, new) = closed.reopen().await.unwrap();
        assert_eq!(
            (new.status().originals, new.status().original_bytes),
            (1, bytes)
        );
        assert!(matches!(
            new.submit(proposal(), vec![], 0),
            Err(ApplicationError::Busy)
        ));
        assert!(matches!(
            receipt.wait().await,
            Err(ApplicationError::Unknown)
        ));
        assert_eq!(new.status().originals, 0);
        let queued = new.submit(proposal(), vec![], 0).unwrap();
        drop(queued);
        assert_eq!(
            new.status().originals,
            1,
            "dropped receiver cannot refund queued work"
        );
        owner.finish().await;
        assert_eq!(new.status().originals, 0);
    }
}

#[cfg(feature = "consensus-driver-testkit")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_held_dispatch_requires_pending_finish_retained_extraction_and_same_joins() {
    for durable in [false, true] {
        let d = tempfile::tempdir().unwrap();
        let identity = identity();
        let (mut owner, handle) = ApplicationOwner::bind(
            identity.clone(),
            2,
            options(),
            ApplicationLimits::default(),
            true,
            || plain(durable, d.path()),
        )
        .await
        .unwrap();
        let gate = owner.dispatch_gate();
        assert!(gate.arm());
        send(&identity, false).await;
        let witness = async {
            for _ in 0..100000 {
                if gate.held() {
                    return;
                }
                tokio::task::yield_now().await;
            }
            panic!("actual dispatch not held");
        };
        tokio::time::timeout(Duration::from_secs(60), async {
            tokio::select! {_=owner.run()=>panic!("retired early"),_=witness=>{}}
        })
        .await
        .unwrap();
        let before = owner.transport_status();
        assert_eq!(before.dispatching, 1);
        assert!(!gate.returned());
        assert!(owner.finish().now_or_never().is_none());
        let canceled = owner.transport_status();
        assert!(!owner.driver_status().finished);
        assert_eq!(canceled.started, before.started);
        assert_eq!(canceled.dispatching, 1);
        assert!(matches!(
            handle.submit(proposal(), vec![], 1),
            Err(ApplicationError::Unavailable)
        ));
        owner = *owner.into_closed().err().unwrap();
        assert!(gate.held());
        assert!(gate.release());
        let report = owner.finish().await;
        assert!(gate.returned());
        assert_eq!((report.panics, report.cancelled), (0, 0));
        assert_eq!(
            report.joined,
            canceled.report.joined + canceled.global.operations as u64
        );
        assert_eq!(owner.transport_status().global.operations, 0);
        let (mut reopened, _) = owner.into_closed().ok().unwrap().reopen().await.unwrap();
        assert_eq!(reopened.driver_status().term, 1);
        reopened.finish().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composed_phase_before_after_errors_and_panics_retire_serving_then_reopen_actual_image() {
    for durable in [false, true] {
        for (tag, skip) in [(0xf2, 0), (0xf0, 0), (0xf0, 1), (0xf1, 0)] {
            for fault in 1..=3 {
                let d = tempfile::tempdir().unwrap();
                let identity = identity();
                let control = Arc::new(Control::default());
                let (mut owner, handle) = ApplicationOwner::bind(
                    identity.clone(),
                    2,
                    options(),
                    ApplicationLimits::default(),
                    true,
                    || {
                        Ok(Backend {
                            inner: if durable {
                                Box::new(storage::ConsensusFjallStore::open(d.path())?)
                            } else {
                                Box::new(storage::ConsensusMemoryStore::default())
                            },
                            control: control.clone(),
                            tag,
                            skip,
                            fault,
                        })
                    },
                )
                .await
                .unwrap();
                control.armed.store(true, Ordering::SeqCst);
                release(&control);
                send(&identity, true).await;
                let report = tokio::time::timeout(Duration::from_secs(60), owner.run())
                    .await
                    .unwrap();
                let status = owner.driver_status();
                assert!(status.retired && status.finished && status.storage_uncertain);
                assert!(owner.status().retired && owner.status().uncertain);
                assert_eq!(status.applied, 0);
                assert_eq!(owner.panic_payload().is_some(), fault == 3);
                assert!(owner.error().is_some());
                assert_eq!((report.panics, report.cancelled), (0, 0));
                assert!(matches!(
                    handle.submit(proposal(), vec![], 1),
                    Err(ApplicationError::Unavailable)
                ));
                let (mut reopened, _) = owner.into_closed().ok().unwrap().reopen().await.unwrap();
                let expected = u64::from(tag != 0xf2 || fault != 1);
                assert_eq!(reopened.status().applied_ordinal, expected);
                assert_eq!(reopened.driver_status().applied, expected);
                reopened.finish().await;
            }
        }
    }
}
