//! The pinned core's real LightReady, including a nonempty local original.
use super::*;
use crate::consensus::driver::{AdmissionError, Role, engine::Engine};
use raft::eraftpb::{Message, MessageType};
use std::sync::Mutex;
use storage::Mutation;

#[derive(Default)]
struct Fault {
    fault: AtomicU8,
    skip: AtomicU8,
    calls: Mutex<Vec<WriteBatch>>,
}
struct Backend {
    inner: Box<dyn ConsensusBackend>,
    fault: Arc<Fault>,
}
impl ConsensusBackend for Backend {
    fn get(&self, k: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(k)
    }
    fn scan_from(&self, p: &[u8], s: &[u8], n: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(p, s, n)
    }
    fn apply(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        let mut fault = self.fault.fault.load(Ordering::SeqCst);
        if fault != 0 {
            self.fault.calls.lock().unwrap().push(batch.clone());
            if self.fault.skip.load(Ordering::SeqCst) > 0 {
                self.fault.skip.fetch_sub(1, Ordering::SeqCst);
                fault = 0;
            } else {
                self.fault.fault.store(0, Ordering::SeqCst);
            }
        }
        assert_ne!(fault, 4, "before original LightReady atomic panic");
        if fault == 1 {
            return Err(composition::error(
                "before original LightReady atomic error",
            ));
        }
        self.inner.apply(batch)?;
        assert_ne!(fault, 3, "after original LightReady atomic panic");
        if fault == 2 {
            return Err(composition::error("after original LightReady atomic error"));
        }
        Ok(())
    }
}
fn options() -> DriverOptions {
    DriverOptions {
        retained: retained(),
        tick_interval: None,
        ..Default::default()
    }
}
fn app_engine(
    hub: &Hub,
    identity: &ConsensusIdentity,
    local: u64,
    shared: Arc<Shared>,
) -> Engine<F2View> {
    let options = options();
    let validated = validation::open(hub, identity.clone(), local, options.retained, true).unwrap();
    let (store, p) = Projection::new(validated, shared).unwrap();
    Engine::new_with_policy(
        store,
        options.clone(),
        options.validate(identity, local).unwrap(),
        Policy::Committed(Box::new(p)),
    )
    .unwrap()
}
fn step(e: &mut Engine<F2View>, m: Message) {
    e.guarded(|e| {
        e.core.step(m)?;
        e.persist_ready()
    })
    .unwrap();
}

#[tokio::test]
async fn application_light_ready_second_commit_ambiguity_fences_all_views_then_consuming_reopen() {
    for durable in [false, true] {
        for fault in 1..=4 {
            // Bind-free engine seeds use ephemeral endpoints so the consuming
            // production reopen can bind this same original identity later.
            let sockets: Vec<_> = (0..3)
                .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
                .collect();
            let identity = ConsensusIdentity::new(
                "light-application".into(),
                format!("unit-{}", sockets[0].local_addr().unwrap().port()),
                NamespaceName::new("tenant").unwrap(),
                std::array::from_fn(|i| Voter {
                    id: i as u64 + 1,
                    endpoint: sockets[i].local_addr().unwrap(),
                }),
            )
            .unwrap();
            drop(sockets);
            let (backend, _directory) = fixture(durable);
            let control = Arc::new(Fault::default());
            let hub = Hub::new(
                Box::new(Backend {
                    inner: backend,
                    fault: control.clone(),
                }),
                limits(),
            );
            let shared = shared();
            let mut leader = app_engine(&hub, &identity, 1, shared.clone());
            let peer_hubs: Vec<_> = (0..2)
                .map(|_| Hub::new(Box::new(ConsensusMemoryStore::default()), limits()))
                .collect();
            let mut peers: Vec<_> = peer_hubs
                .iter()
                .enumerate()
                .map(|(i, h)| app_engine(h, &identity, i as u64 + 2, self::shared()))
                .collect();
            leader
                .guarded(|e| {
                    e.core.campaign()?;
                    e.persist_ready()
                })
                .unwrap();
            for _ in 0..1000 {
                let m = leader
                    .pop()
                    .or_else(|| peers.iter_mut().find_map(Engine::pop));
                let Some(m) = m else { break };
                if m.to == 1 {
                    step(&mut leader, m);
                } else {
                    step(&mut peers[(m.to - 2) as usize], m);
                }
            }
            assert_eq!(leader.status.role, Role::Leader);
            assert_eq!(leader.status.observed_commit, 1);
            while leader.application_work() {
                leader.application_turn();
            }
            assert_eq!(leader.status.applied, 1);
            let data = proposal(
                CommandKind::CreateQueue {
                    config: Default::default(),
                },
                2,
            );
            let context = vec![17; 73];
            let (original, mut receipt) = Original::new(
                data.clone(),
                context.clone(),
                leader.core.raft.term,
                shared
                    .pool
                    .reserve(limits().original_charge().unwrap())
                    .unwrap(),
            );
            leader.core.propose(context.clone(), data.clone()).unwrap();
            leader.application.track(original, 2).unwrap();
            // The real peer durably answers the actual core-produced append
            // before leader Ready is collected. No fake Ready/progress/response.
            let mut messages = std::mem::take(&mut leader.core.raft.msgs);
            let pos = messages
                .iter()
                .position(|m| m.to == 2 && m.get_msg_type() == MessageType::MsgAppend)
                .unwrap();
            let append = messages.remove(pos);
            leader.core.raft.msgs = messages;
            step(&mut peers[0], append);
            let reply = peers[0].pop().unwrap();
            assert_eq!(reply.get_msg_type(), MessageType::MsgAppendResponse);
            assert_eq!(reply.index, 2);
            assert!(!reply.reject);
            assert_eq!(
                peers[0].core.store().read_entries(2, 1).unwrap()[0]
                    .data
                    .as_ref(),
                data.as_slice()
            );
            leader.core.step(reply).unwrap();
            assert_eq!(leader.core.raft.raft_log.committed, 1);
            let advanced = leader.status.advanced;
            let light_commits = leader.status.light_commits_persisted;
            control.skip.store(1, Ordering::SeqCst);
            control.fault.store(fault, Ordering::SeqCst);
            // This is the actual original synchronous Ready/LightReady caller.
            let original = std::thread::spawn(move || {
                assert_eq!(
                    leader.guarded(|e| e.persist_ready()),
                    Err(AdmissionError::Failed)
                );
                leader
            });
            let mut leader = original.join().unwrap();
            assert!(leader.status.retired && leader.status.storage_uncertain);
            assert_eq!(leader.panic.is_some(), fault >= 3);
            assert_eq!(leader.status.advanced, advanced + 1);
            assert_eq!(leader.status.light_commits_persisted, light_commits);
            assert!(
                leader
                    .light
                    .as_ref()
                    .is_some_and(|l| l.commit_index() == Some(2))
            );
            assert_eq!(leader.status.applied, 1);
            assert_eq!(leader.tick(), Err(AdmissionError::Retired));
            leader.application_turn();
            assert_eq!(leader.status.applied, 1);
            assert!(hub.uncertain());
            for view in [hub.readonly(), hub.journal(), hub.indexed()] {
                assert!(view.get(b"key").is_err());
                assert!(view.scan_from(b"", b"", 1).is_err());
                assert!(view.apply(WriteBatch::default()).is_err());
            }
            let mut f2 = hub.f2();
            assert!(f2.get(&[0xf2]).is_err());
            assert!(f2.scan_from(b"", b"", 1).is_err());
            assert!(f2.apply(WriteBatch::default()).is_err());
            drop(f2);
            {
                let calls = control.calls.lock().unwrap();
                assert_eq!(calls.len(), 2, "failure must be the SECOND atomic call");
                assert!(calls[0].mutations().iter().any(|m| matches!(m, Mutation::Put { key, .. } if key.ends_with(&2u64.to_be_bytes()) && key[key.len()-9] == 2)));
                assert_eq!(
                    calls[1].mutations().len(),
                    1,
                    "LightReady commit-only batch"
                );
            }
            leader.application.stop();
            drop(leader);
            let fresh_hub = Hub::new(hub.into_backend().unwrap(), limits());
            let rows = fresh_hub.image().unwrap();
            assert!(
                rows.iter().all(|(k, _)| k[0] == 0xf2),
                "no F0/F1/domain prefix after failure"
            );
            let (store, projection) = Projection::new(
                validation::open(&fresh_hub, identity.clone(), 1, retained(), false).unwrap(),
                self::shared(),
            )
            .unwrap();
            let expected_commit = if fault == 1 || fault == 4 { 1 } else { 2 };
            assert_eq!(store.hard_state().unwrap().commit, expected_commit);
            let expected_hub = Hub::new(Box::new(ConsensusMemoryStore::default()), limits());
            let (mut expected_store, expected_projection) = Projection::new(
                validation::open(&expected_hub, identity.clone(), 1, retained(), true).unwrap(),
                self::shared(),
            )
            .unwrap();
            expected_store
                .persist(
                    &[
                        Entry {
                            index: 1,
                            term: 1,
                            ..Default::default()
                        },
                        Entry {
                            index: 2,
                            term: 1,
                            data: data.clone().into(),
                            context: context.clone().into(),
                            ..Default::default()
                        },
                    ],
                    Some(&HardState {
                        term: 1,
                        vote: 1,
                        commit: expected_commit,
                        ..Default::default()
                    }),
                )
                .unwrap();
            assert_eq!(
                rows,
                expected_hub.image().unwrap(),
                "entire physical image is exactly F2, with no projection/effects"
            );
            drop((expected_store, expected_projection));
            let persisted = store.read_entries(2, 1).unwrap().remove(0);
            assert_eq!(
                (
                    persisted.index,
                    persisted.term,
                    persisted.data.as_ref(),
                    persisted.context.as_ref()
                ),
                (2, 1, data.as_slice(), context.as_slice())
            );
            assert_eq!(
                (
                    projection.a,
                    projection.r,
                    projection.journal.last_appended_index().unwrap(),
                    projection.journal.committed_index().unwrap()
                ),
                (0, 1, 0, 0)
            );
            drop((store, projection));
            let closed = ClosedApplicationOwner {
                hub: fresh_hub,
                pool: shared.pool.clone(),
                identity,
                local: 1,
                options: options(),
                limits: limits(),
            };
            let (mut reopened, handle) = closed.reopen().await.unwrap();
            let expected_a = u64::from(expected_commit == 2);
            assert_eq!(
                (
                    handle.status().applied_ordinal,
                    handle.status().projected,
                    handle.status().projected_commit,
                    handle.status().applied_raft
                ),
                (expected_a, expected_a, expected_a, expected_commit)
            );
            assert_eq!(handle.status().originals, 1);
            assert!(matches!(
                receipt.wait().await,
                Err(ApplicationError::Unknown)
            ));
            assert_eq!(
                (handle.status().originals, handle.status().original_bytes),
                (0, 0)
            );
            reopened.finish().await;
            println!(
                "application actual LightReady durable={durable} fault={fault} C={expected_commit} L=J=A={expected_a} original_joined=true"
            );
        }
    }
}
