//! These trusted-store seeds construct durable windows, not quorum evidence.
//! Separate process fixtures exercise the real core/TCP commitment path.
#[cfg(feature = "consensus-driver-testkit")]
mod leader;
mod light_ready;
use super::*;
use super::{
    composition::Hub,
    original::{Pool, Shared},
    projection::Projection,
};
use crate::consensus::{ConsensusStore, RetainedLogLimits, Voter};
use domain::{Command, CommandKind, DurableProposal, EntityPath, NamespaceName, Timestamp};
use raft::eraftpb::{Entry, HardState};
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
use storage::{
    ConsensusFjallStore, ConsensusMemoryStore, Key, StateStore, StorageError, Value, WriteBatch,
};

fn identity() -> ConsensusIdentity {
    ConsensusIdentity::new(
        "application".into(),
        "unit".into(),
        NamespaceName::new("tenant").unwrap(),
        std::array::from_fn(|i| Voter {
            id: i as u64 + 1,
            endpoint: format!("127.0.0.1:{}", 35101 + i).parse().unwrap(),
        }),
    )
    .unwrap()
}
fn limits() -> ApplicationLimits {
    let mut l = ApplicationLimits {
        max_image_bytes: 64 * 1024 * 1024,
        max_rows: 8192,
        ..Default::default()
    };
    l.original_bytes = l.original_charge().unwrap();
    l
}
fn retained() -> RetainedLogLimits {
    RetainedLogLimits {
        max_entries: 4096,
        max_bytes: 16 * 1024 * 1024,
    }
}
fn proposal(kind: CommandKind, t: u64) -> Vec<u8> {
    DurableProposal::unbound(Command::new(
        NamespaceName::new("tenant").unwrap(),
        EntityPath::new("q").unwrap(),
        Timestamp::from_millis(t),
        kind,
    ))
    .encode()
    .unwrap()
}
fn entry(index: u64, kind: Option<CommandKind>) -> Entry {
    Entry {
        index,
        term: 1,
        data: kind.map(|k| proposal(k, index)).unwrap_or_default().into(),
        context: vec![9; 32].into(),
        ..Default::default()
    }
}
fn shared() -> Arc<Shared> {
    Shared::new(Pool::new(1, limits().original_bytes))
}
fn fixture(durable: bool) -> (Box<dyn ConsensusBackend>, Option<tempfile::TempDir>) {
    let d = durable.then(|| tempfile::tempdir().unwrap());
    let b: Box<dyn ConsensusBackend> = match &d {
        Some(d) => Box::new(ConsensusFjallStore::open(d.path()).unwrap()),
        None => Box::new(ConsensusMemoryStore::default()),
    };
    (b, d)
}
fn seeded(
    b: Box<dyn ConsensusBackend>,
    entries: &[Entry],
) -> (Hub, ConsensusStore<F2View>, Projection) {
    let hub = Hub::new(b, limits());
    let validated = validation::open(&hub, identity(), 1, retained(), true).unwrap();
    let (mut store, p) = Projection::new(validated, shared()).unwrap();
    store
        .persist(
            entries,
            Some(&HardState {
                term: 1,
                vote: 1,
                commit: entries.len() as u64,
                ..Default::default()
            }),
        )
        .unwrap();
    (hub, store, p)
}
fn recovered(hub: &Hub) -> (ConsensusStore<F2View>, Projection) {
    Projection::new(
        validation::open(hub, identity(), 1, retained(), false).unwrap(),
        shared(),
    )
    .unwrap()
}

#[test]
fn all_four_durable_windows_recover_exactly_on_memory_and_native() {
    for durable in [false, true] {
        for window in 0..4 {
            let (b, d) = fixture(durable);
            let bytes = proposal(
                CommandKind::CreateQueue {
                    config: domain::QueueConfig::default(),
                },
                2,
            );
            let entries = [
                entry(1, None),
                entry(
                    2,
                    Some(CommandKind::CreateQueue {
                        config: domain::QueueConfig::default(),
                    }),
                ),
                entry(3, None),
            ];
            let (hub, store, mut p) = seeded(b, &entries);
            // 0 before projection; 1 append before J; 2 J before A; 3 actual A.
            while match window {
                0 => false,
                1 => p.shared.status.lock().unwrap().projected < 1,
                2 => p.shared.status.lock().unwrap().projected_commit < 1,
                _ => p.shared.status.lock().unwrap().applied_ordinal < 1,
            } {
                p.turn(&store, 0, false).unwrap();
            }
            drop(p);
            drop(store);
            let backend = hub.into_backend().unwrap();
            let backend = if let Some(d) = &d {
                drop(backend);
                Box::new(ConsensusFjallStore::open(d.path()).unwrap()) as Box<dyn ConsensusBackend>
            } else {
                backend
            };
            let hub = Hub::new(backend, limits());
            let (store, mut p) = recovered(&hub);
            p.recover(&store).unwrap();
            let s = p.shared.status.lock().unwrap().clone();
            assert_eq!(
                (
                    s.applied_ordinal,
                    s.applied_raft,
                    s.projected,
                    s.projected_commit
                ),
                (1, 3, 1, 1)
            );
            let f0 = crate::journal::entry_key(1);
            let raw = hub.get(&f0).unwrap().unwrap();
            assert_eq!(
                crate::journal::decode_entry(&f0, &raw).unwrap().payload(),
                bytes
            );
            assert_eq!(
                store.read_entries(2, 1).unwrap()[0].context,
                entries[1].context
            );
            assert_eq!(p.writer.applied_index().unwrap(), 1);
            p.writer
                .verify_latest_applied(&DurableProposal::decode(&bytes).unwrap())
                .unwrap();
        }
    }
}

#[test]
fn first_refusal_and_successful_no_effect_without_clock_are_valid_and_progress_after_reopen() {
    for durable in [false, true] {
        for refusal in [false, true] {
            let (b, d) = fixture(durable);
            let kind = if refusal {
                CommandKind::Complete {
                    sequence: domain::SequenceNumber::new(1),
                    lock_token: domain::LockToken::new(1),
                }
            } else {
                CommandKind::ExpireSessionLocks
            };
            let (hub, store, mut p) = seeded(b, &[entry(1, None), entry(2, Some(kind))]);
            p.recover(&store).unwrap();
            assert_eq!(p.a, 1);
            assert!(hub.get(&domain::keys::clock()).unwrap().is_none());
            assert_eq!(hub.readonly().scan_prefix(&[0xf1], 3).unwrap().len(), 2);
            drop(p);
            drop(store);
            let b = hub.into_backend().unwrap();
            let b = if let Some(d) = &d {
                drop(b);
                Box::new(ConsensusFjallStore::open(d.path()).unwrap()) as Box<dyn ConsensusBackend>
            } else {
                b
            };
            let hub = Hub::new(b, limits());
            let (mut store, mut p) = recovered(&hub);
            assert_eq!(p.actual(), 2);
            store
                .persist(
                    &[
                        entry(
                            3,
                            Some(CommandKind::CreateQueue {
                                config: domain::QueueConfig::default(),
                            }),
                        ),
                        entry(4, None),
                    ],
                    Some(&HardState {
                        term: 1,
                        vote: 1,
                        commit: 4,
                        ..Default::default()
                    }),
                )
                .unwrap();
            p.recover(&store).unwrap();
            assert_eq!((p.a, p.r), (2, 4));
            assert!(hub.get(&domain::keys::clock()).unwrap().is_some());
        }
    }
}

#[test]
fn real_timer_scans_256_logical_rows_through_bounded_physical_pages() {
    for durable in [false, true] {
        let (b, _d) = fixture(durable);
        let config = domain::QueueConfig {
            default_time_to_live_millis: Some(10),
            dead_lettering_on_message_expiration: false,
            ..Default::default()
        };
        let (hub, mut store, mut p) =
            seeded(b, &[entry(1, Some(CommandKind::CreateQueue { config }))]);
        p.recover(&store).unwrap();
        for i in 0..300u64 {
            let n = i + 2;
            let data = proposal(
                CommandKind::Send {
                    message_id: format!("m{i}"),
                    body: vec![7; 32],
                    time_to_live_millis: Some(10),
                    session_id: None,
                    scheduled_enqueue_at: None,
                    envelope: None,
                },
                2,
            );
            store
                .persist(
                    &[Entry {
                        index: n,
                        term: 1,
                        data: data.into(),
                        ..Default::default()
                    }],
                    Some(&HardState {
                        term: 1,
                        vote: 1,
                        commit: n,
                        ..Default::default()
                    }),
                )
                .unwrap();
            p.recover(&store).unwrap();
        }
        let ns = NamespaceName::new("tenant").unwrap();
        let q = EntityPath::new("q").unwrap();
        let prefix = domain::keys::expiry_prefix(&ns, &q);
        assert_eq!(hub.readonly().scan_prefix(&prefix, 256).unwrap().len(), 256);
        assert_eq!(hub.readonly().scan_prefix(&prefix, 301).unwrap().len(), 300);
        for (n, remaining) in [(302, 44), (303, 0)] {
            let data = proposal(CommandKind::ExpireMessages, 100);
            store
                .persist(
                    &[Entry {
                        index: n,
                        term: 1,
                        data: data.into(),
                        ..Default::default()
                    }],
                    Some(&HardState {
                        term: 1,
                        vote: 1,
                        commit: n,
                        ..Default::default()
                    }),
                )
                .unwrap();
            p.recover(&store).unwrap();
            assert_eq!(
                hub.readonly().scan_prefix(&prefix, 500).unwrap().len(),
                remaining
            );
        }
        drop(p);
        drop(store);
        let (store, mut p) = recovered(&hub);
        p.recover(&store).unwrap();
        assert_eq!(p.a, 303);
    }
}

#[test]
fn complete_physical_validation_refuses_hidden_foreign_rows_and_conflicting_tail_before_effects() {
    for durable in [false, true] {
        for corrupt in 0..8 {
            let (b, _d) = fixture(durable);
            let (hub, store, mut p) = seeded(
                b,
                &[entry(
                    1,
                    Some(CommandKind::CreateQueue {
                        config: domain::QueueConfig::default(),
                    }),
                )],
            );
            if corrupt == 0 {
                p.turn(&store, 0, false).unwrap();
                p.turn(&store, 0, false).unwrap();
            } // exact append, still uncommitted
            drop(p);
            drop(store);
            let mut b = hub.into_backend().unwrap();
            let key = match corrupt {
                0 => crate::journal::entry_key(1),
                1 => b"\xf0unknown".to_vec(),
                2 => b"\xf1unknown".to_vec(),
                3 => b"\xf2unknown".to_vec(),
                4 => vec![0x12],
                5 => vec![0x13],
                6 => vec![0x77],
                _ => vec![0],
            };
            b.apply(WriteBatch::default().put(key, vec![1])).unwrap();
            let hub = Hub::new(b, limits());
            assert!(validation::open(&hub, identity(), 1, retained(), false).is_err());
            assert!(
                hub.get(&domain::keys::queue_config(
                    &NamespaceName::new("tenant").unwrap(),
                    &EntityPath::new("q").unwrap()
                ))
                .unwrap()
                .is_none()
            );
        }
    }
}

#[derive(Default)]
struct Fault {
    mode: AtomicU8,
}
struct Observed {
    inner: Box<dyn ConsensusBackend>,
    fault: Arc<Fault>,
}
impl ConsensusBackend for Observed {
    fn get(&self, k: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(k)
    }
    fn scan_from(&self, p: &[u8], s: &[u8], n: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        assert!(n <= 64);
        self.inner.scan_from(p, s, n)
    }
    fn apply(&mut self, b: WriteBatch) -> Result<(), StorageError> {
        let m = self.fault.mode.swap(0, Ordering::SeqCst);
        if m == 1 {
            return Err(composition::error("before original apply"));
        }
        self.inner.apply(b)?;
        if m == 2 {
            return Err(composition::error("after full original apply"));
        }
        assert_ne!(m, 3, "after full original atomic apply panic");
        Ok(())
    }
}
#[test]
fn before_after_atomic_error_and_panic_poison_all_views_and_recover_actual_state() {
    for durable in [false, true] {
        for phase in 0..3 {
            for mode in 1..=3 {
                let (b, d) = fixture(durable);
                let fault = Arc::new(Fault::default());
                let (hub, store, mut p) = seeded(
                    Box::new(Observed {
                        inner: b,
                        fault: fault.clone(),
                    }),
                    &[entry(
                        1,
                        Some(CommandKind::CreateQueue {
                            config: domain::QueueConfig::default(),
                        }),
                    )],
                );
                p.turn(&store, 0, false).unwrap();
                for _ in 0..phase {
                    p.turn(&store, 0, false).unwrap();
                }
                fault.mode.store(mode, Ordering::SeqCst);
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    p.turn(&store, 0, false)
                }));
                assert!(!matches!(result, Ok(Ok(_))));
                p.retire();
                assert!(hub.get(b"anything").is_err());
                assert!(hub.journal().apply(WriteBatch::default()).is_err());
                drop(p);
                drop(store);
                let b = hub.into_backend().unwrap();
                let b = if let Some(d) = &d {
                    drop(b);
                    Box::new(ConsensusFjallStore::open(d.path()).unwrap())
                        as Box<dyn ConsensusBackend>
                } else {
                    b
                };
                let hub = Hub::new(b, limits());
                let (store, mut p) = recovered(&hub);
                let before = p.a;
                p.recover(&store).unwrap();
                assert_eq!(p.a, 1);
                if phase == 2 && mode != 1 {
                    assert_eq!(before, 1, "landed checkpoint must not be reexecuted");
                }
            }
        }
    }
}

#[test]
fn concrete_minima_cover_full_records_and_reply_larger_than_proposal() {
    let mut driver = DriverOptions {
        retained: retained(),
        ..Default::default()
    };
    let l = limits();
    assert!(l.validate(&driver).is_ok());
    assert!(
        l.reply_reservation().unwrap() > MAX_CONSENSUS_PROPOSAL_BYTES + MAX_CONSENSUS_CONTEXT_BYTES
    );
    for bad in [
        ApplicationLimits {
            max_value_bytes: MAX_CONSENSUS_ENTRY_BYTES + 39,
            ..l
        },
        ApplicationLimits {
            original_bytes: l.original_charge().unwrap() - 1,
            ..l
        },
        ApplicationLimits {
            max_image_bytes: 1024,
            ..l
        },
    ] {
        assert!(bad.validate(&driver).is_err());
    }
    let pool = Pool::new(2, l.original_charge().unwrap());
    let a = pool.reserve(l.original_charge().unwrap()).unwrap();
    assert!(pool.reserve(1).is_err());
    drop(a);
    assert!(pool.reserve(1).is_ok());
    driver.max_events = l.max_originals + 3;
    assert!(l.validate(&driver).is_err());
    driver.max_events = 16;
    driver.max_event_bytes = 4 * driver.transport.max_frame_bytes;
    assert!(
        l.validate(&driver).is_err(),
        "business events must leave independent peer/control bytes"
    );
}

#[test]
fn minimum_value_bound_retains_full_legal_data_and_independent_context() {
    for durable in [false, true] {
        let make = |n| {
            proposal(
                CommandKind::Send {
                    message_id: "m".into(),
                    body: vec![7; n],
                    time_to_live_millis: None,
                    session_id: None,
                    scheduled_enqueue_at: None,
                    envelope: None,
                },
                1,
            )
        };
        let trial = MAX_CONSENSUS_PROPOSAL_BYTES - 128;
        let data = make(MAX_CONSENSUS_PROPOSAL_BYTES - (make(trial).len() - trial));
        assert_eq!(data.len(), MAX_CONSENSUS_PROPOSAL_BYTES);
        let l = ApplicationLimits {
            max_value_bytes: MAX_CONSENSUS_ENTRY_BYTES + 40,
            ..limits()
        };
        assert!(
            l.validate(&DriverOptions {
                retained: retained(),
                ..Default::default()
            })
            .is_ok()
        );
        let (b, d) = fixture(durable);
        let hub = Hub::new(b, l);
        let (mut store, mut p) = Projection::new(
            validation::open(&hub, identity(), 1, retained(), true).unwrap(),
            shared(),
        )
        .unwrap();
        let original = Entry {
            index: 1,
            term: 1,
            data: data.into(),
            context: vec![8; MAX_CONSENSUS_CONTEXT_BYTES].into(),
            ..Default::default()
        };
        store
            .persist(
                std::slice::from_ref(&original),
                Some(&HardState {
                    term: 1,
                    vote: 1,
                    commit: 1,
                    ..Default::default()
                }),
            )
            .unwrap();
        p.recover(&store).unwrap();
        drop(p);
        drop(store);
        let b = hub.into_backend().unwrap();
        let b = if let Some(d) = &d {
            drop(b);
            Box::new(ConsensusFjallStore::open(d.path()).unwrap()) as Box<dyn ConsensusBackend>
        } else {
            b
        };
        let hub = Hub::new(b, l);
        let (store, p) = recovered(&hub);
        assert_eq!(store.read_entries(1, 1).unwrap(), vec![original]);
        assert_eq!((p.a, p.r), (1, 1));
    }
}

#[test]
fn logical_scan_exclusive_cursors_and_filtered_page_progress_do_not_clip() {
    for durable in [false, true] {
        let (mut b, _d) = fixture(durable);
        let mut batch = WriteBatch::default();
        for i in 0..300u16 {
            batch.push_put([vec![0x01], i.to_be_bytes().to_vec()].concat(), vec![1]);
            batch.push_put([vec![0xf2], i.to_be_bytes().to_vec()].concat(), vec![2]);
        }
        b.apply(batch).unwrap();
        let hub = Hub::new(b, limits());
        hub.image().unwrap();
        let view = hub.readonly();
        assert!(view.snapshot().is_err());
        assert!(view.apply(WriteBatch::default()).is_err());
        let first = view.scan_prefix(&[1], 256).unwrap();
        assert_eq!(first.len(), 256);
        let mut after = first.last().unwrap().0.clone();
        after.push(0);
        let rest = view.scan_from(&[1], &after, 256).unwrap();
        assert_eq!(rest.len(), 44);
        assert!(rest[0].0 > first[255].0);
        assert_eq!(
            hub.f2().scan_from(b"", b"", 300).unwrap().len(),
            300,
            "filtered rows must advance the physical cursor across multiple pages"
        );
        assert_eq!(hub.f2().scan_from(b"", b"", 301).unwrap().len(), 300);
    }
}

#[test]
fn malformed_committed_tail_and_valid_conflicting_projection_refuse_before_any_prefix_effect() {
    for durable in [false, true] {
        for kind in 0..4 {
            let (b, _d) = fixture(durable);
            let entries = [
                entry(
                    1,
                    Some(CommandKind::CreateQueue {
                        config: Default::default(),
                    }),
                ),
                entry(2, Some(CommandKind::ExpireSessionLocks)),
            ];
            let (hub, store, p) = seeded(b, &entries);
            if kind == 3 {
                let mut journal = crate::Journal::open(hub.journal()).unwrap();
                journal
                    .append(1, &proposal(CommandKind::ExpireSessionLocks, 99))
                    .unwrap();
            }
            drop(p);
            drop(store);
            let mut b = hub.into_backend().unwrap();
            if kind < 3 {
                let mut bad = entries[1].clone();
                let mut data = bad.data.to_vec();
                match kind {
                    0 => data = vec![1],
                    1 => data[4..8].copy_from_slice(&11u32.to_be_bytes()),
                    _ => {
                        let mut proposal =
                            DurableProposal::decode(&data).unwrap().command().clone();
                        proposal.namespace = NamespaceName::new("foreign").unwrap();
                        data = DurableProposal::unbound(proposal).encode().unwrap();
                    }
                }
                bad.data = data.into();
                b.apply(WriteBatch::default().put(
                    crate::consensus::entry_key(2),
                    crate::consensus::codec::encode_entry(&bad).unwrap(),
                ))
                .unwrap();
            }
            let hub = Hub::new(b, limits());
            let before = hub.image().unwrap();
            assert!(validation::open(&hub, identity(), 1, retained(), false).is_err());
            assert_eq!(
                hub.image().unwrap(),
                before,
                "validation cannot project the valid first command before finding the bad tail"
            );
            assert!(hub.get(&domain::keys::clock()).unwrap().is_none());
        }
    }
}

#[test]
fn physical_frontier_companion_namespace_clock_and_anchor_fences_are_read_only() {
    for durable in [false, true] {
        for case in 0..14 {
            let (b, _d) = fixture(durable);
            let (hub, store, mut p) = seeded(
                b,
                &[entry(
                    1,
                    Some(CommandKind::CreateQueue {
                        config: Default::default(),
                    }),
                )],
            );
            if case != 0 {
                p.recover(&store).unwrap();
            }
            match case {
                0 => {
                    let machine = domain::StateMachine::new(hub.indexed());
                    machine
                        .apply(
                            &DurableProposal::decode(&proposal(
                                CommandKind::CreateQueue {
                                    config: Default::default(),
                                },
                                1,
                            ))
                            .unwrap()
                            .command()
                            .clone(),
                        )
                        .unwrap();
                }
                1 => {
                    p.writer
                        .apply(
                            2,
                            &DurableProposal::decode(&proposal(CommandKind::ExpireSessionLocks, 2))
                                .unwrap(),
                        )
                        .unwrap();
                } // A > J
                2 => {
                    p.journal
                        .append(2, &proposal(CommandKind::ExpireSessionLocks, 2))
                        .unwrap();
                    p.journal.commit(2).unwrap();
                } // J > P(C)
                3 => {
                    p.journal
                        .append(2, &proposal(CommandKind::ExpireSessionLocks, 2))
                        .unwrap();
                } // structural tail > P(C)
                4 => {
                    let anchored = p
                        .writer
                        .prepare_snapshot_anchor_owner(
                            &DurableProposal::decode(&proposal(
                                CommandKind::CreateQueue {
                                    config: Default::default(),
                                },
                                1,
                            ))
                            .unwrap(),
                        )
                        .unwrap();
                    hub.indexed().apply(anchored).unwrap();
                }
                _ => {}
            }
            drop(p);
            drop(store);
            let mut b = hub.into_backend().unwrap();
            let batch = match case {
                5 | 6 => {
                    let rows = b.scan_from(&[0xf1], &[0xf1], 64).unwrap();
                    WriteBatch::default().delete(rows[(case - 5) as usize].0.clone())
                }
                7 => WriteBatch::default().delete(crate::consensus::key(crate::consensus::ROOT)),
                8 => WriteBatch::default().put(
                    domain::keys::clock(),
                    domain::codec::encode(&Timestamp::from_millis(99)).unwrap(),
                ),
                9 => {
                    let rows = b.scan_from(&[1], &[1], 64).unwrap();
                    let (k, v) = rows[0].clone();
                    let mut foreign = k;
                    let start = foreign.windows(6).position(|w| w == b"tenant").unwrap();
                    foreign[start..start + 6].copy_from_slice(b"otherx");
                    WriteBatch::default().put(foreign, v)
                }
                10 => {
                    let rows = b.scan_from(&[1], &[1], 64).unwrap();
                    WriteBatch::default().delete(rows[0].0.clone())
                }
                11 => {
                    let rows = b.scan_from(&[0x11], &[0x11], 64).unwrap();
                    WriteBatch::default().delete(rows[0].0.clone())
                }
                12 => {
                    let other = storage::MemoryStore::default();
                    let mut writer = domain::IndexedWriter::open(other.clone()).unwrap();
                    writer
                        .apply(
                            1,
                            &DurableProposal::decode(&proposal(CommandKind::ExpireSessionLocks, 1))
                                .unwrap(),
                        )
                        .unwrap();
                    let mut batch = WriteBatch::default();
                    for (k, v) in other.scan_prefix(&[0xf1], 4).unwrap() {
                        batch.push_put(k, v);
                    }
                    batch
                }
                13 => {
                    let owner = domain::retained_accounting::CapacityOwner::new(
                        NamespaceName::new("tenant").unwrap(),
                        EntityPath::new("q").unwrap(),
                        1,
                    )
                    .unwrap();
                    let config = domain::QueueConfig {
                        capacity: domain::QueueCapacity::Finite {
                            max_size_bytes: domain::QUEUE_CAPACITY_GRANULARITY_BYTES,
                        },
                        ..Default::default()
                    };
                    let mut batch = WriteBatch::default();
                    for (entity, config) in [
                        (owner.parent(), config),
                        (owner.shadow(), domain::QueueConfig::default()),
                    ] {
                        batch.push_put(
                            domain::keys::queue_config(owner.namespace(), entity),
                            domain::capacity_fence::encode_profile(&owner, entity, &config)
                                .unwrap(),
                        );
                    }
                    batch
                }
                _ => WriteBatch::default(),
            };
            b.apply(batch).unwrap();
            let hub = Hub::new(b, limits());
            let before = hub.image().unwrap();
            assert!(
                validation::open(&hub, identity(), 1, retained(), false).is_err(),
                "case {case}"
            );
            assert_eq!(hub.image().unwrap(), before);
        }
    }
}

#[test]
fn sparse_commit_and_all_blank_prefixes_derive_actual_raft_without_inventing_commands() {
    for durable in [false, true] {
        let (b, _d) = fixture(durable);
        let blanks: Vec<_> = (1..=40).map(|r| entry(r, None)).collect();
        let (hub, mut store, mut p) = seeded(b, &blanks);
        p.recover(&store).unwrap();
        assert_eq!((p.a, p.r), (0, 40));
        for tag in [0, 0xf0, 0xf1] {
            assert!(hub.readonly().scan_prefix(&[tag], 1).unwrap().is_empty());
        }
        store
            .persist(
                &[
                    entry(
                        41,
                        Some(CommandKind::CreateQueue {
                            config: Default::default(),
                        }),
                    ),
                    entry(42, None),
                    entry(43, Some(CommandKind::ExpireSessionLocks)),
                ],
                Some(&HardState {
                    term: 1,
                    vote: 1,
                    commit: 42,
                    ..Default::default()
                }),
            )
            .unwrap();
        p.recover(&store).unwrap();
        assert_eq!((p.a, p.r), (1, 42));
        assert_eq!(p.journal.last_appended_index().unwrap(), 1);
        assert_eq!(store.last_index().unwrap(), 43);
        drop(p);
        drop(store);
        let (mut store, mut p) = recovered(&hub);
        assert_eq!((p.a, p.r), (1, 42));
        store
            .persist(
                &[],
                Some(&HardState {
                    term: 1,
                    vote: 1,
                    commit: 43,
                    ..Default::default()
                }),
            )
            .unwrap();
        p.recover(&store).unwrap();
        assert_eq!((p.a, p.r), (2, 43));
    }
}

#[test]
fn corrupted_message_content_and_counter_companions_refuse_complete_image() {
    for durable in [false, true] {
        for counter in [false, true] {
            let (b, _d) = fixture(durable);
            let (hub, store, mut p) = seeded(
                b,
                &[
                    entry(
                        1,
                        Some(CommandKind::CreateQueue {
                            config: Default::default(),
                        }),
                    ),
                    entry(
                        2,
                        Some(CommandKind::Send {
                            message_id: "m".into(),
                            body: vec![1, 2, 3],
                            time_to_live_millis: None,
                            session_id: None,
                            scheduled_enqueue_at: None,
                            envelope: None,
                        }),
                    ),
                ],
            );
            p.recover(&store).unwrap();
            drop(p);
            drop(store);
            let mut b = hub.into_backend().unwrap();
            let tag = if counter { 2 } else { 3 };
            let rows = b.scan_from(&[tag], &[tag], 64).unwrap();
            assert!(!rows.is_empty());
            b.apply(WriteBatch::default().put(rows[0].0.clone(), vec![0xff]))
                .unwrap();
            let hub = Hub::new(b, limits());
            let before = hub.image().unwrap();
            assert!(validation::open(&hub, identity(), 1, retained(), false).is_err());
            assert_eq!(hub.image().unwrap(), before);
        }
    }
}

#[test]
fn valid_mixed_image_remains_closed_to_default_f2_opener_and_wrong_identity() {
    for durable in [false, true] {
        let (b, _d) = fixture(durable);
        let (hub, store, mut p) = seeded(
            b,
            &[entry(
                1,
                Some(CommandKind::CreateQueue {
                    config: Default::default(),
                }),
            )],
        );
        p.recover(&store).unwrap();
        drop(p);
        drop(store);
        let before = hub.image().unwrap();
        assert!(validation::open(&hub, identity(), 2, retained(), false).is_err());
        assert!(validation::open(&hub, identity(), 1, retained(), true).is_err());
        assert_eq!(hub.image().unwrap(), before);
        let b = hub.into_backend().unwrap();
        assert!(
            ConsensusStore::open(
                Observed {
                    inner: b,
                    fault: Arc::new(Fault::default())
                },
                identity(),
                1,
                retained()
            )
            .is_err()
        );
    }
}

#[test]
fn committed_image_exhaustion_retires_whole_engine_not_a_business_refusal() {
    for durable in [false, true] {
        let (b, _d) = fixture(durable);
        let l = ApplicationLimits {
            max_rows: 16,
            ..limits()
        };
        let hub = Hub::new(b, l);
        let validated = validation::open(&hub, identity(), 1, retained(), true).unwrap();
        let (mut store, p) = Projection::new(validated, shared()).unwrap();
        let entries: Vec<_> = (1..=6)
            .map(|r| {
                entry(
                    r,
                    (r == 6).then_some(CommandKind::CreateQueue {
                        config: Default::default(),
                    }),
                )
            })
            .collect();
        store
            .persist(
                &entries,
                Some(&HardState {
                    term: 1,
                    vote: 1,
                    commit: 6,
                    ..Default::default()
                }),
            )
            .unwrap();
        let opts = DriverOptions {
            retained: retained(),
            ..Default::default()
        };
        let codec = opts.validate(&identity(), 1).unwrap();
        let mut engine = super::super::engine::Engine::new_with_policy(
            store,
            opts,
            codec,
            Policy::Committed(Box::new(p)),
        )
        .unwrap();
        engine.guarded(|e| e.persist_ready()).unwrap();
        for _ in 0..20 {
            if engine.status.retired {
                break;
            }
            engine.application_turn();
        }
        assert!(engine.status.retired && engine.status.storage_uncertain);
        assert_eq!(
            engine.status.failure,
            Some(super::super::Failure::Application)
        );
        assert_eq!(engine.status.applied, 5);
        assert!(hub.uncertain());
        assert!(hub.get(b"anything").is_err());
        drop(engine);
        let b = hub.into_backend().unwrap();
        assert!(b.get(&domain::keys::clock()).unwrap().is_none());
        assert!(b.scan_from(&[0xf1], &[0xf1], 64).unwrap().is_empty());
        // Provisioned reopening is fresh physical validation, never a retry in
        // the failed owner. The real committed proposal survives exhaustion.
        let hub = Hub::new(b, limits());
        let (store, mut p) = recovered(&hub);
        p.recover(&store).unwrap();
        assert_eq!((p.a, p.r), (1, 6));
    }
}

#[tokio::test]
async fn fenced_and_dropped_original_keeps_credit_until_actual_prefix_or_close() {
    let charge = limits().original_charge().unwrap();
    for (count, bytes) in [(1, charge * 2), (2, charge)] {
        let pool = Pool::new(count, bytes);
        let shared = Shared::new(pool.clone());
        let (o, mut receipt) = Original::new(vec![1], vec![2], 1, pool.reserve(charge).unwrap());
        shared.track(o.clone(), 3);
        drop(o);
        shared.fence(2, false);
        assert!(matches!(
            receipt.wait().await,
            Err(ApplicationError::Unknown)
        ));
        drop(receipt);
        assert_eq!(pool.usage(), (1, charge));
        assert!(
            pool.reserve(charge).is_err(),
            "both count and bytes independently constrain originals"
        );
        shared.advance(2);
        assert_eq!(pool.usage(), (1, charge));
        shared.advance(3);
        assert_eq!(pool.usage(), (0, 0));
    }
}

#[test]
fn supported_original_versions_are_projected_without_reencoding_or_format_adoption() {
    let kinds = [
        CommandKind::CreateQueue {
            config: Default::default(),
        },
        CommandKind::UpdateTopic {
            update: Default::default(),
        },
        CommandKind::CreateTopic {
            config: domain::TopicConfig {
                requires_duplicate_detection: true,
                ..Default::default()
            },
        },
        CommandKind::SendContent {
            content: domain::MessageContent::Raw(domain::RawMessage {
                body: vec![1],
                ..Default::default()
            }),
        },
        CommandKind::CreateQueue {
            config: domain::QueueConfig {
                dead_lettering_on_message_expiration: false,
                ..Default::default()
            },
        },
        CommandKind::CreateRule {
            name: domain::RuleName::new("r").unwrap(),
            filter: domain::RuleFilter::Sql(domain::SqlRuleFilter::new("a = 1")),
        },
        CommandKind::CreateQueue {
            config: domain::QueueConfig {
                capacity: domain::QueueCapacity::Finite {
                    max_size_bytes: 1024,
                },
                ..Default::default()
            },
        },
        CommandKind::CreateSubscription {
            name: domain::SubscriptionName::new("s").unwrap(),
            config: domain::SubscriptionConfig {
                dead_lettering_on_filter_evaluation_exceptions: false,
                ..Default::default()
            },
        },
        CommandKind::SendContent {
            content: domain::MessageContent::AmqpTextIds(vec![0]),
        },
        CommandKind::CreateRuleWithAction {
            name: domain::RuleName::new("r").unwrap(),
            filter: domain::RuleFilter::True,
            action: domain::SqlRuleAction::new("REMOVE user.old"),
        },
    ];
    for durable in [false, true] {
        for (version, kind) in kinds.iter().enumerate() {
            let mut command =
                DurableProposal::decode(&proposal(CommandKind::ExpireSessionLocks, 17))
                    .unwrap()
                    .command()
                    .clone();
            command.kind = kind.clone();
            if matches!(version, 5 | 9) {
                command.entity = EntityPath::new("q/subscriptions/s").unwrap();
            }
            let original = DurableProposal::unbound(command).encode().unwrap();
            assert_eq!(
                u32::from_be_bytes(original[4..8].try_into().unwrap()),
                version as u32 + 1
            );
            let e = Entry {
                term: 1,
                index: 1,
                data: original.clone().into(),
                context: vec![0xff; 1024].into(),
                ..Default::default()
            };
            let (b, d) = fixture(durable);
            let (hub, store, mut p) = seeded(b, std::slice::from_ref(&e));
            p.recover(&store).unwrap();
            assert_eq!(p.a, 1);
            let key = crate::journal::entry_key(1);
            let raw = hub.get(&key).unwrap().unwrap();
            assert_eq!(
                crate::journal::decode_entry(&key, &raw).unwrap().payload(),
                original
            );
            p.writer
                .verify_latest_applied(&DurableProposal::decode(&original).unwrap())
                .unwrap();
            drop(p);
            drop(store);
            let backend = hub.into_backend().unwrap();
            let backend = if let Some(d) = &d {
                drop(backend);
                Box::new(ConsensusFjallStore::open(d.path()).unwrap()) as Box<dyn ConsensusBackend>
            } else {
                backend
            };
            let hub = Hub::new(backend, limits());
            let (store, p) = recovered(&hub);
            assert_eq!((p.a, p.r), (1, 1));
            assert_eq!(store.read_entries(1, 1).unwrap()[0], e);
        }
    }
}

#[test]
fn retained_authority_and_time_are_not_recaptured_and_refusals_checkpoint_between_valid_effects() {
    for durable in [false, true] {
        let (b, _d) = fixture(durable);
        let first = Entry {
            term: 1,
            index: 1,
            data: proposal(
                CommandKind::CreateQueue {
                    config: Default::default(),
                },
                10,
            )
            .into(),
            ..Default::default()
        };
        let (hub, mut store, mut p) = seeded(b, &[first]);
        p.recover(&store).unwrap();
        let ns = NamespaceName::new("tenant").unwrap();
        let q = EntityPath::new("q").unwrap();
        let machine = domain::StateMachine::new(hub.readonly());
        let binding = machine.bind_entity(&ns, &q).unwrap();
        // Shape-only persistence decoding is not authentication. A literal V1
        // authority prefix with another retained generation exercises the
        // existing stale-binding refusal without rewriting domain metadata.
        let mut alternate = DurableProposal::bound(domain::BoundCommand::new(
            binding.clone(),
            Command::new(
                ns.clone(),
                q.clone(),
                Timestamp::from_millis(11),
                CommandKind::ExpireSessionLocks,
            ),
        ))
        .unwrap()
        .encode()
        .unwrap();
        assert_eq!(&alternate[12..26], b"\x01\x06tenant\x01q\x01q\x00\x01");
        alternate[25] = 2;
        let stale = match DurableProposal::decode(&alternate).unwrap().authority() {
            domain::DurableProposalAuthority::Bound(binding) => binding.clone(),
            _ => unreachable!(),
        };
        for (index, time, captured, refused) in [
            (2, 9, binding.clone(), true),
            (3, 11, stale, true),
            (4, 11, binding, false),
        ] {
            let command = Command::new(
                ns.clone(),
                q.clone(),
                Timestamp::from_millis(time),
                CommandKind::Send {
                    message_id: format!("bound-{index}"),
                    body: vec![1],
                    time_to_live_millis: None,
                    session_id: None,
                    scheduled_enqueue_at: None,
                    envelope: None,
                },
            );
            let original =
                DurableProposal::bound(domain::BoundCommand::new(captured.clone(), command))
                    .unwrap();
            let bytes = original.encode().unwrap();
            let e = Entry {
                term: 1,
                index,
                data: bytes.clone().into(),
                context: vec![index as u8; 1024].into(),
                ..Default::default()
            };
            store
                .persist(
                    std::slice::from_ref(&e),
                    Some(&HardState {
                        term: 1,
                        vote: 1,
                        commit: index,
                        ..Default::default()
                    }),
                )
                .unwrap();
            let mut outcome = None;
            for _ in 0..5 {
                if let Some(proof) = p.turn(&store, 1, false).unwrap() {
                    outcome = proof.test_outcome();
                    break;
                }
            }
            assert_eq!(
                matches!(outcome, Some(domain::IndexedApplyOutcome::Refused(_))),
                refused
            );
            assert_eq!(p.a, index);
            let key = crate::journal::entry_key(index);
            let raw = hub.get(&key).unwrap().unwrap();
            assert_eq!(
                crate::journal::decode_entry(&key, &raw).unwrap().payload(),
                bytes
            );
            p.writer.verify_latest_applied(&original).unwrap();
            assert_eq!(store.read_entries(index, 1).unwrap()[0], e);
        }
        assert_eq!(
            machine.last_applied_time().unwrap(),
            Timestamp::from_millis(11)
        );
        drop(p);
        drop(store);
        let (store, mut p) = recovered(&hub);
        p.recover(&store).unwrap();
        assert_eq!((p.a, p.r), (4, 4));
    }
}
