use super::*;
use super::{
    engine::Engine,
    input::{Event, Shared},
};
use crate::consensus::{ConsensusStore, Voter};
use raft::eraftpb::{Entry, Message, MessageType};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};
use std::time::Duration;
use storage::{
    ConsensusBackend, ConsensusFjallStore, ConsensusMemoryStore, Key, StorageError, Value,
    WriteBatch,
};

fn identity() -> ConsensusIdentity {
    ConsensusIdentity::new(
        "driver-test".into(),
        "unit".into(),
        domain::NamespaceName::new("tenant").unwrap(),
        std::array::from_fn(|i| Voter {
            id: i as u64 + 1,
            endpoint: format!("127.0.0.1:{}", 32101 + i).parse().unwrap(),
        }),
    )
    .unwrap()
}
fn proposal(namespace: &str, index: u64) -> Vec<u8> {
    // Repeated queue creation remains valid durable intent even though #361
    // would refuse the duplicate. No catalog lookup belongs in admission.
    domain::DurableProposal::unbound(domain::Command::new(
        domain::NamespaceName::new(namespace).unwrap(),
        domain::EntityPath::new(format!("q{index}")).unwrap(),
        domain::Timestamp::from_millis(index),
        domain::CommandKind::CreateQueue {
            config: domain::QueueConfig::default(),
        },
    ))
    .encode()
    .unwrap()
}
fn options() -> DriverOptions {
    DriverOptions {
        tick_interval: None,
        ..Default::default()
    }
}
fn engine<B: ConsensusBackend>(backend: B, id: u64) -> Engine<B> {
    let options = options();
    let identity = identity();
    let codec = options.validate(&identity, id).unwrap();
    Engine::new(
        ConsensusStore::bootstrap(backend, identity, id, options.retained).unwrap(),
        options,
        codec,
    )
    .unwrap()
}
fn step<B: ConsensusBackend>(e: &mut Engine<B>, message: Message) {
    e.guarded(|e| {
        e.core.step(message)?;
        e.persist_ready()
    })
    .unwrap();
}
fn settle(nodes: &mut [Engine<ConsensusMemoryStore>; 3]) {
    for _ in 0..1000 {
        let message = nodes.iter_mut().find_map(Engine::pop);
        let Some(message) = message else {
            return;
        };
        let target = (message.to - 1) as usize;
        step(&mut nodes[target], message);
    }
    panic!("core did not settle within bounded dispatches");
}
fn elected() -> [Engine<ConsensusMemoryStore>; 3] {
    let mut nodes = std::array::from_fn(|i| engine(ConsensusMemoryStore::default(), i as u64 + 1));
    nodes[0]
        .guarded(|e| {
            e.core.campaign()?;
            e.persist_ready()
        })
        .unwrap();
    settle(&mut nodes);
    assert_eq!(nodes[0].status.role, Role::Leader);
    assert_eq!(nodes[0].status.observed_commit, 1);
    nodes
}
fn inputs(
    e: &Engine<impl ConsensusBackend>,
    max_events: usize,
) -> (DriverHandle, tokio::sync::mpsc::Receiver<Event>) {
    let mut opts = options();
    opts.max_events = max_events;
    let shared = Shared::new(identity(), e.core.store().local_id(), &opts);
    let (queue, input) = tokio::sync::mpsc::channel(max_events);
    (DriverHandle { shared, queue }, input)
}

#[tokio::test]
async fn valid_invalid_valid_admission_never_poisoned_or_fictionally_applied() {
    let mut nodes = elected();
    let (handle, mut input) = inputs(&nodes[0], 4);
    let term = nodes[0].status.term;
    let first = proposal("tenant", 1);
    let mut a = handle.propose(first.clone(), vec![], term).unwrap();
    nodes[0].process(input.try_recv().unwrap());
    let a = a.wait().await.unwrap();
    let before = nodes[0].core.raft.raft_log.last_index();
    let advanced = nodes[0].status.advanced;
    let mut v11 = first.clone();
    v11[4..8].copy_from_slice(&11u32.to_be_bytes());
    let mut trailing = first.clone();
    trailing.push(0);
    for invalid in [
        vec![],
        vec![1, 2, 3],
        v11,
        trailing,
        proposal("foreign", 2),
        vec![0; crate::consensus::MAX_CONSENSUS_PROPOSAL_BYTES + 1],
    ] {
        assert!(matches!(
            handle.propose(invalid, vec![], term),
            Err(AdmissionError::InvalidProposal)
        ));
    }
    assert!(matches!(
        handle.propose(
            first.clone(),
            vec![0; crate::consensus::MAX_CONSENSUS_CONTEXT_BYTES + 1],
            term
        ),
        Err(AdmissionError::InvalidProposal)
    ));
    assert_eq!(nodes[0].core.raft.raft_log.last_index(), before);
    assert_eq!(nodes[0].status.advanced, advanced);
    assert!(!nodes[0].status.retired);
    assert_eq!(handle.status().pending_events, 0);
    let mut b = handle.propose(first, vec![], term).unwrap();
    nodes[0].process(input.try_recv().unwrap());
    let b = b.wait().await.unwrap();
    assert_eq!(b.raft_index, a.raft_index + 1);
    settle(&mut nodes);
    for node in nodes {
        assert_eq!(node.status.observed_commit, b.raft_index);
        assert_eq!(node.status.applied, 0);
        assert_eq!(node.core.raft.raft_log.applied, 0);
        assert!(!node.core.has_ready());
    }
}

#[tokio::test]
async fn completed_and_cancelled_borrowers_keep_original_input_credits() {
    let mut nodes = elected();
    let (handle, mut input) = inputs(&nodes[0], 1);
    let mut receipt = handle
        .propose(proposal("tenant", 1), vec![], nodes[0].status.term)
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(1), receipt.wait())
            .await
            .is_err()
    );
    assert!(matches!(handle.tick(), Err(AdmissionError::Busy)));
    nodes[0].process(input.try_recv().unwrap());
    assert_eq!(handle.status().pending_events, 1);
    assert!(matches!(handle.tick(), Err(AdmissionError::Busy)));
    receipt.wait().await.unwrap();
    assert_eq!(handle.status().pending_events, 0);
    let receipt = handle.tick().unwrap();
    drop(receipt);
    assert_eq!(handle.status().pending_events, 1);
    nodes[0].process(input.try_recv().unwrap());
    assert_eq!(handle.status().pending_events, 0);
}

#[tokio::test]
async fn stale_queued_admission_is_fenced_by_real_higher_term_stepdown() {
    let mut nodes = elected();
    let (handle, mut input) = inputs(&nodes[0], 4);
    let old = nodes[0].status.term;
    // Higher-term append, unlike a vote under a live check-quorum lease, is an
    // immediate standard-core stepdown. No custom vote/lease logic is used.
    step(
        &mut nodes[0],
        Message {
            from: 2,
            to: 1,
            term: old + 1,
            msg_type: MessageType::MsgHeartbeat,
            ..Default::default()
        },
    );
    let before = nodes[0].status.observed_last;
    let mut receipt = handle.propose(proposal("tenant", 2), vec![], old).unwrap();
    nodes[0].process(input.try_recv().unwrap());
    assert_eq!(receipt.wait().await, Err(AdmissionError::NotLeader));
    assert_eq!(nodes[0].status.observed_last, before);
    assert_eq!(nodes[0].status.role, Role::Follower);
}

#[derive(Default)]
struct Control {
    fault: AtomicU8,
    skip: AtomicUsize,
    applied: AtomicUsize,
    gate: Mutex<(bool, bool)>,
    changed: Condvar,
}
struct Observed<B> {
    inner: B,
    control: Arc<Control>,
}
fn failure() -> StorageError {
    StorageError::Backend {
        operation: "driver fault",
        detail: "original ambiguity".into(),
    }
}
impl<B: ConsensusBackend> ConsensusBackend for Observed<B> {
    fn get(&self, k: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(k)
    }
    fn scan_from(&self, p: &[u8], s: &[u8], n: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(p, s, n)
    }
    fn apply(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        let fault = if self
            .control
            .skip
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1))
            .is_ok()
        {
            0
        } else {
            self.control.fault.swap(0, Ordering::SeqCst)
        };
        if fault == 1 {
            return Err(failure());
        }
        if fault == 4 {
            let mut gate = self.control.gate.lock().unwrap();
            gate.0 = true;
            self.control.changed.notify_all();
            while !gate.1 {
                gate = self.control.changed.wait(gate).unwrap();
            }
        }
        self.inner.apply(batch)?;
        self.control.applied.fetch_add(1, Ordering::SeqCst);
        if fault == 5 {
            let mut gate = self.control.gate.lock().unwrap();
            gate.0 = true;
            self.control.changed.notify_all();
            while !gate.1 {
                gate = self.control.changed.wait(gate).unwrap();
            }
        }
        if fault == 2 {
            return Err(failure());
        }
        assert_ne!(fault, 3, "after full atomic apply panic");
        Ok(())
    }
}
fn persistence_faults<B: ConsensusBackend>(factory: impl Fn() -> B) {
    for fault in [1, 2, 3] {
        for append in [false, true] {
            let control = Arc::new(Control::default());
            let mut e = engine(
                Observed {
                    inner: factory(),
                    control: control.clone(),
                },
                2,
            );
            control.fault.store(fault, Ordering::SeqCst);
            let mut message = Message {
                from: 1,
                to: 2,
                term: 1,
                msg_type: if append {
                    MessageType::MsgAppend
                } else {
                    MessageType::MsgRequestVote
                },
                ..Default::default()
            };
            if append {
                message.entries = vec![Entry {
                    index: 1,
                    term: 1,
                    data: proposal("tenant", 1).into(),
                    ..Default::default()
                }]
                .into();
            }
            assert_eq!(
                e.guarded(|e| {
                    e.core.step(message)?;
                    e.persist_ready()
                }),
                Err(AdmissionError::Failed)
            );
            assert_eq!(e.status.advanced, 0);
            assert!(e.ready.is_some());
            assert!(e.status.storage_uncertain);
            assert!(e.status.retired);
            assert!(e.outbox.is_empty());
            assert_eq!(e.status.observed_last, 0);
            assert_eq!(e.status.observed_vote, 0);
            assert_eq!(e.tick(), Err(AdmissionError::Retired));
            let store = e.core.raft.r.raft_log.store.reopen().unwrap();
            assert_eq!(
                store.hard_state().unwrap().term,
                if fault == 1 { 0 } else { 1 }
            );
            assert_eq!(
                store.hard_state().unwrap().vote,
                if fault != 1 && !append { 1 } else { 0 }
            );
            assert_eq!(store.last_index().unwrap(), u64::from(fault != 1 && append));
        }
    }
}
#[test]
fn memory_before_after_atomic_failure_and_panic_never_advance_or_reply() {
    persistence_faults(ConsensusMemoryStore::default);
}
#[test]
fn fjall_before_after_syncall_failure_and_panic_never_advance_or_reply() {
    // Each owner retains its TempDir until its backend is destroyed.
    struct Native {
        inner: ConsensusFjallStore,
        _directory: tempfile::TempDir,
    }
    impl ConsensusBackend for Native {
        fn get(&self, k: &[u8]) -> Result<Option<Value>, StorageError> {
            self.inner.get(k)
        }
        fn scan_from(
            &self,
            p: &[u8],
            s: &[u8],
            n: usize,
        ) -> Result<Vec<(Key, Value)>, StorageError> {
            self.inner.scan_from(p, s, n)
        }
        fn apply(&mut self, b: WriteBatch) -> Result<(), StorageError> {
            self.inner.apply(b)
        }
    }
    persistence_faults(|| {
        let directory = tempfile::tempdir().unwrap();
        Native {
            inner: ConsensusFjallStore::open(directory.path()).unwrap(),
            _directory: directory,
        }
    });
}

#[test]
fn before_vote_and_append_atomic_barriers_hold_the_original_owner() {
    for append in [false, true] {
        for fault in [4, 5] {
            let control = Arc::new(Control::default());
            let mut e = engine(
                Observed {
                    inner: ConsensusMemoryStore::default(),
                    control: control.clone(),
                },
                2,
            );
            control.fault.store(fault, Ordering::SeqCst);
            let (done, observed) = std::sync::mpsc::sync_channel(1);
            let original = std::thread::spawn(move || {
                let mut message = Message {
                    from: 1,
                    to: 2,
                    term: 1,
                    msg_type: if append {
                        MessageType::MsgAppend
                    } else {
                        MessageType::MsgRequestVote
                    },
                    ..Default::default()
                };
                if append {
                    message.entries = vec![Entry {
                        index: 1,
                        term: 1,
                        ..Default::default()
                    }]
                    .into();
                }
                step(&mut e, message);
                done.send(()).unwrap();
                e
            });
            let mut gate = control.gate.lock().unwrap();
            while !gate.0 {
                gate = control.changed.wait(gate).unwrap();
            }
            assert_eq!(
                control.applied.load(Ordering::SeqCst),
                if fault == 4 { 1 } else { 2 }
            );
            println!(
                "original persistence barrier append={append} phase={fault} actual_apply_count={}",
                control.applied.load(Ordering::SeqCst)
            );
            assert!(
                observed.try_recv().is_err(),
                "no response frontier before original persistence returns"
            );
            gate.1 = true;
            control.changed.notify_all();
            drop(gate);
            observed.recv_timeout(Duration::from_secs(60)).unwrap();
            let mut e = original.join().unwrap();
            let response = e.pop().unwrap();
            assert_eq!(
                response.get_msg_type(),
                if append {
                    MessageType::MsgAppendResponse
                } else {
                    MessageType::MsgRequestVoteResponse
                }
            );
            assert!(!response.reject);
            assert_eq!(e.status.advanced, 1);
        }
    }
}

/// Seed a legitimate RawNode state in which one follower has durably replied
/// before local leader persistence. raft-rs permits that concurrency, though
/// this driver's stricter release ordering normally avoids it. No Ready is
/// collected until after that seed; every election/reply is core-generated.
fn light_ready_seed(control: Arc<Control>) -> Engine<Observed<ConsensusMemoryStore>> {
    let mut leader = engine(
        Observed {
            inner: ConsensusMemoryStore::default(),
            control,
        },
        1,
    );
    let mut peers = [
        engine(ConsensusMemoryStore::default(), 2),
        engine(ConsensusMemoryStore::default(), 3),
    ];
    leader
        .guarded(|e| {
            e.core.campaign()?;
            e.persist_ready()
        })
        .unwrap();
    for _ in 0..100 {
        if let Some(m) = leader.pop() {
            step(&mut peers[(m.to - 2) as usize], m);
        }
        if let Some(m) = peers.iter_mut().find_map(Engine::pop) {
            leader.core.step(m).unwrap();
            if leader.core.raft.state == raft::StateRole::Leader {
                break;
            }
            leader.persist_ready().unwrap();
        }
    }
    assert_eq!(leader.core.raft.state, raft::StateRole::Leader);
    let mut messages = std::mem::take(&mut leader.core.raft.msgs);
    let position = messages
        .iter()
        .position(|m| m.get_msg_type() == MessageType::MsgAppend && m.to == 2)
        .unwrap();
    let append = messages.remove(position);
    leader.core.raft.msgs = messages;
    step(&mut peers[0], append);
    let reply = peers[0].pop().unwrap();
    assert_eq!(reply.get_msg_type(), MessageType::MsgAppendResponse);
    leader.core.step(reply).unwrap();
    assert_eq!(leader.core.raft.raft_log.committed, 0);
    leader
}

#[test]
fn light_ready_new_commit_is_a_second_atomic_write_and_ambiguous_failure_retires() {
    for fault in [0, 1, 2, 3] {
        let control = Arc::new(Control::default());
        let mut e = light_ready_seed(control.clone());
        let advanced = e.status.advanced;
        control.skip.store(1, Ordering::SeqCst);
        control.fault.store(fault, Ordering::SeqCst);
        let result = e.guarded(|e| e.persist_ready());
        if fault == 0 {
            assert!(result.is_ok());
            assert_eq!(e.status.observed_commit, 1);
            assert_eq!(e.status.light_commits_persisted, 1);
            assert_eq!(e.core.raft.raft_log.applied, 0);
        } else {
            assert_eq!(result, Err(AdmissionError::Failed));
            assert!(e.status.retired);
            assert_eq!(e.status.advanced, advanced + 1);
            assert_eq!(e.status.observed_commit, 0);
            assert!(e.light.is_some());
            assert_eq!(e.status.light_commits_persisted, 0);
            // Any already staged first-stage messages remain local custody and
            // MUST NOT be flushed after failure. No retries or core steps.
            assert_eq!(e.tick(), Err(AdmissionError::Retired));
            let store = e.core.raft.r.raft_log.store.reopen().unwrap();
            assert_eq!(store.hard_state().unwrap().commit, u64::from(fault != 1));
        }
    }
}
