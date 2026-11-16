//! Live originals use real elections/replication on the maintained TCP owners.
use super::*;
use crate::consensus::driver::Role;
use futures_util::FutureExt;
use std::{
    sync::{
        Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize},
    },
    time::Duration,
};
use storage::Mutation;

#[derive(Default)]
struct Gate {
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    changed: Condvar,
    returned: AtomicBool,
    applied: AtomicUsize,
}
struct Backend {
    inner: Box<dyn ConsensusBackend>,
    gate: Arc<Gate>,
    tag: u8,
    skip: usize,
    fault: u8,
}
impl ConsensusBackend for Backend {
    fn get(&self, k: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(k)
    }
    fn scan_from(&self, p: &[u8], s: &[u8], n: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(p, s, n)
    }
    fn apply(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        let matches = batch.mutations().iter().any(|m| match m {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key.first() == Some(&self.tag),
        });
        let armed = if matches && self.gate.armed.load(Ordering::SeqCst) {
            if self.skip > 0 {
                self.skip -= 1;
                false
            } else {
                self.gate.armed.swap(false, Ordering::SeqCst)
            }
        } else {
            false
        };
        struct Returned<'a>(&'a AtomicBool);
        impl Drop for Returned<'_> {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let _returned = armed.then(|| Returned(&self.gate.returned));
        if armed {
            self.gate.entered.notify_one();
            let mut released = self.gate.released.lock().unwrap();
            while !*released {
                released = self.gate.changed.wait(released).unwrap();
            }
            assert_ne!(self.fault, 4, "before original leader atomic panic");
            if self.fault == 1 {
                return Err(composition::error("before original leader atomic error"));
            }
        }
        self.inner.apply(batch)?;
        if armed {
            self.gate.applied.fetch_add(1, Ordering::SeqCst);
            assert_ne!(self.fault, 3, "after original leader atomic panic");
            if self.fault == 2 {
                return Err(composition::error("after original leader atomic error"));
            }
        }
        Ok(())
    }
}
fn release(gate: &Gate) {
    *gate.released.lock().unwrap() = true;
    gate.changed.notify_all();
}
async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(60), async {
        for _ in 0..1_000_000 {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("unmet causal leader frontier");
    })
    .await
    .unwrap();
}
async fn scenario(durable: bool, tag: u8, skip: usize, fault: u8, retire_held: bool) {
    let sockets: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let identity = ConsensusIdentity::new(
        "leader-custody".into(),
        format!("unit-{}", sockets[0].local_addr().unwrap().port()),
        NamespaceName::new("tenant").unwrap(),
        std::array::from_fn(|i| Voter {
            id: i as u64 + 1,
            endpoint: sockets[i].local_addr().unwrap(),
        }),
    )
    .unwrap();
    drop(sockets);
    let options = DriverOptions {
        tick_interval: None,
        ..Default::default()
    };
    let limits = ApplicationLimits::default();
    let charge = limits.original_charge().unwrap();
    let gate = Arc::new(Gate::default());
    let mut directories = Vec::new();
    let mut owners = Vec::new();
    let mut handles = Vec::new();
    for local in 1..=3 {
        let (backend, directory) = fixture(durable);
        directories.push(directory);
        let (owner, handle) = ApplicationOwner::bind(
            identity.clone(),
            local,
            options.clone(),
            limits,
            true,
            || {
                Ok(Backend {
                    inner: backend,
                    gate: if local == 1 {
                        gate.clone()
                    } else {
                        Arc::new(Gate::default())
                    },
                    tag,
                    skip,
                    fault,
                })
            },
        )
        .await
        .unwrap();
        owners.push(owner);
        handles.push(handle);
    }
    let mut leader = owners.remove(0);
    let network = leader.network_control();
    let dispatch = leader.dispatch_gate();
    let peers: Vec<_> = owners
        .into_iter()
        .map(|mut owner| {
            tokio::spawn(async move {
                owner.run().await;
                owner
            })
        })
        .collect();
    let (cancel, mut cancellation) = tokio::sync::oneshot::channel();
    let returned = Arc::new(AtomicBool::new(false));
    let witness = returned.clone();
    let original = tokio::spawn(async move {
        tokio::select! { biased; _ = &mut cancellation => {}, _ = leader.run() => {} }
        witness.store(true, Ordering::SeqCst);
        leader
    });
    let mut campaign = handles[0].campaign().unwrap();
    campaign.wait().await.unwrap();
    until(|| {
        handles[0].driver_status().role == Role::Leader && handles[0].driver_status().applied == 1
    })
    .await;
    // Node 2 alone supplies the missing durable majority. Observe the actual
    // processed response/progress, not a synthetic response or seeded match.
    network.block([3, 0, 0]);
    network.pause_application(true);
    network.observe_append(2, 2);
    assert!(dispatch.arm());
    gate.armed.store(true, Ordering::SeqCst);
    let data = proposal(
        CommandKind::CreateQueue {
            config: Default::default(),
        },
        2,
    );
    let context = vec![31; 137];
    let term = handles[0].driver_status().term;
    let mut receipt = handles[0]
        .submit(data.clone(), context.clone(), term)
        .unwrap();
    assert!(
        receipt.wait().now_or_never().is_none(),
        "cancel just the receipt borrower"
    );
    until(|| {
        handles[0].driver_status().observed_commit == 2
            && dispatch.held()
            && network.processed_append().is_some()
    })
    .await;
    let majority = network.processed_append().unwrap();
    assert_eq!(
        (
            majority.from,
            majority.term,
            majority.core_term,
            majority.index,
            majority.commit
        ),
        (2, term, term, 2, 2)
    );
    assert!(!majority.reject);
    assert_eq!(majority.voters, [1, 2, 3]);
    assert!(majority.matched[0] >= 2 && majority.matched[1] >= 2 && majority.matched[2] < 2);
    assert_eq!(
        (
            handles[0].status().originals,
            handles[0].status().original_bytes
        ),
        (1, charge)
    );
    assert!(receipt.wait().now_or_never().is_none());
    network.pause_application(false);
    let outcome = std::panic::AssertUnwindSafe(async {
        tokio::time::timeout(Duration::from_secs(60), gate.entered.notified())
            .await
            .unwrap();
        assert!(!gate.returned.load(Ordering::SeqCst));
        assert_eq!(gate.applied.load(Ordering::SeqCst), 0);
        assert!(!returned.load(Ordering::SeqCst));
        assert_eq!(handles[0].driver_status().applied, 1);
        assert!(receipt.wait().now_or_never().is_none());
        cancel.send(()).unwrap(); // cannot interrupt this original physical call
        if retire_held {
            handles[0].retire();
        }
        assert!(!returned.load(Ordering::SeqCst));
        assert_eq!(
            (
                handles[0].status().originals,
                handles[0].status().original_bytes
            ),
            (1, charge)
        );
        assert!(matches!(
            handles[0].submit(data.clone(), context.clone(), term),
            Err(ApplicationError::Busy)
        ));
    })
    .catch_unwind()
    .await;
    release(&gate);
    let mut owner = tokio::time::timeout(Duration::from_secs(60), original)
        .await
        .unwrap()
        .unwrap();
    assert!(gate.returned.load(Ordering::SeqCst));
    if let Err(p) = outcome {
        std::panic::resume_unwind(p);
    }
    assert_eq!(
        gate.applied.load(Ordering::SeqCst),
        usize::from(fault != 1 && fault != 4)
    );
    assert_eq!(owner.panic_payload().is_some(), fault >= 3);
    assert_eq!(owner.error().is_some(), fault != 0);
    let observed = owner.committed_observation().ok().flatten();
    if fault == 0 {
        let observed = observed.unwrap();
        assert_eq!(
            (
                observed.index,
                observed.term,
                observed.data_bytes,
                observed.context_bytes
            ),
            (2, term, data.len(), context.len())
        );
    }
    assert!(!owner.driver_status().finished);
    // This is a genuine response original held AFTER stepping the leader. It
    // forces finish Pending without inventing an unrelated pending future.
    assert!(owner.finish().now_or_never().is_none());
    assert!(dispatch.held() && !dispatch.returned());
    assert_eq!(
        (
            handles[0].status().originals,
            handles[0].status().original_bytes
        ),
        (1, charge)
    );
    owner = *owner
        .into_closed()
        .err()
        .expect("finish borrower cancellation cannot extract");
    // For the positive control F1 completed/published BEFORE this retirement.
    // An already transferred result must survive both retire and finish.
    handles[0].retire();
    assert!(dispatch.release());
    let report = owner.finish().await;
    assert!(dispatch.returned());
    assert_eq!(
        (
            report.panics,
            report.cancelled,
            owner.transport_status().global.operations
        ),
        (0, 0, 0)
    );
    let ClosedApplicationOwner {
        hub,
        pool,
        identity,
        local,
        options,
        limits,
    } = owner.into_closed().ok().unwrap();
    // All original backend/transport tasks have returned/joined. Consume the
    // sole backend into fresh siblings; never read through a failed old view.
    let hub = Hub::new(hub.into_backend().unwrap(), limits);
    let rows = hub.image().unwrap();
    let (store, projection) = Projection::new(
        validation::open(&hub, identity.clone(), local, options.retained, false).unwrap(),
        self::shared(),
    )
    .unwrap();
    let landed = fault != 1 && fault != 4;
    let (l, j, a) = match (tag, skip, landed) {
        (0xf0, 0, false) => (0, 0, 0),
        (0xf0, 0, true) => (1, 0, 0),
        (0xf0, 1, false) => (1, 0, 0),
        (0xf0, 1, true) | (0xf1, _, false) => (1, 1, 0),
        (0xf1, _, true) => (1, 1, 1),
        _ => unreachable!(),
    };
    assert_eq!(
        (
            store.hard_state().unwrap().commit,
            projection.journal.last_appended_index().unwrap(),
            projection.journal.committed_index().unwrap(),
            projection.a,
            projection.r
        ),
        (2, l, j, a, 1 + a)
    );
    let entry = store.read_entries(2, 1).unwrap().remove(0);
    assert_eq!(
        (
            entry.index,
            entry.term,
            entry.data.as_ref(),
            entry.context.as_ref()
        ),
        (2, term, data.as_slice(), context.as_slice())
    );
    if l == 1 {
        let key = crate::journal::entry_key(1);
        let raw = hub.readonly().get(&key).unwrap().unwrap();
        assert_eq!(
            crate::journal::decode_entry(&key, &raw).unwrap().payload(),
            data
        );
    }
    assert_eq!(rows.iter().any(|(k, _)| k[0] == 0xf1), a == 1);
    assert_eq!(rows.iter().any(|(k, _)| k[0] <= 0x11), a == 1);
    // Compare every domain/F1 and F0 byte with the ordinary maintained owners
    // for this one known instruction, not merely the presence of a checkpoint.
    let expected_domain = storage::MemoryStore::default();
    if a == 1 {
        let mut writer = domain::IndexedWriter::open(expected_domain.clone()).unwrap();
        assert!(matches!(
            writer
                .apply(1, &DurableProposal::decode(&data).unwrap())
                .unwrap(),
            domain::IndexedApplyOutcome::Applied(domain::CommandOutcome::QueueCreated)
        ));
    }
    assert_eq!(
        rows.iter()
            .filter(|(k, _)| k[0] <= 0x11 || k[0] == 0xf1)
            .cloned()
            .collect::<Vec<_>>(),
        expected_domain.scan_from(b"", b"", 64).unwrap()
    );
    let expected_journal = storage::MemoryStore::default();
    if l == 1 {
        let mut journal = crate::Journal::open(expected_journal.clone()).unwrap();
        journal.append(1, &data).unwrap();
        if j == 1 {
            journal.commit(1).unwrap();
        }
    }
    assert_eq!(
        rows.iter()
            .filter(|(k, _)| k[0] == 0xf0)
            .cloned()
            .collect::<Vec<_>>(),
        expected_journal.scan_from(b"", b"", 64).unwrap()
    );
    drop((store, projection));
    let closed = ClosedApplicationOwner {
        hub,
        pool,
        identity,
        local,
        options,
        limits,
    };
    let (mut reopened, handle) = closed.reopen().await.unwrap();
    assert_eq!(
        (
            handle.status().applied_ordinal,
            handle.status().projected,
            handle.status().projected_commit,
            handle.status().applied_raft
        ),
        (1, 1, 1, 2)
    );
    assert_eq!(
        (handle.status().originals, handle.status().original_bytes),
        (1, charge)
    );
    assert!(matches!(
        handle.submit(data, context, term),
        Err(ApplicationError::Busy)
    ));
    let result = receipt.wait().await;
    if fault == 0 && !retire_held {
        let result = result.unwrap();
        assert_eq!(
            (result.term, result.raft_index, result.application_ordinal),
            (term, 2, 1)
        );
        assert!(matches!(
            result.outcome,
            domain::IndexedApplyOutcome::Applied(domain::CommandOutcome::QueueCreated)
        ));
    } else {
        assert!(matches!(result, Err(ApplicationError::Unknown)));
    }
    assert_eq!(
        (handle.status().originals, handle.status().original_bytes),
        (0, 0)
    );
    assert!(matches!(
        receipt.wait().await,
        Err(ApplicationError::Receipt)
    ));
    reopened.finish().await;
    for h in &handles[1..] {
        h.retire();
    }
    for peer in peers {
        let owner = tokio::time::timeout(Duration::from_secs(60), peer)
            .await
            .unwrap()
            .unwrap();
        assert!(owner.driver_status().finished);
        assert_eq!(owner.transport_status().global.operations, 0);
    }
    println!(
        "real leader original durable={durable} tag={tag:x} skip={skip} fault={fault} retired_held={retire_held} physical_C=2 L={l} J={j} A={a} original_and_peers_joined=true"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_leader_begun_f0_f1_retirement_fences_publication_without_interrupt_or_credit_refund()
{
    for durable in [false, true] {
        for (tag, skip) in [(0xf0, 0), (0xf0, 1), (0xf1, 0)] {
            scenario(durable, tag, skip, 0, true).await;
        }
        scenario(durable, 0xf1, 0, 0, false).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_leader_f0_f1_before_after_error_panic_preserves_original_and_joins_before_reopen() {
    for durable in [false, true] {
        for (tag, skip) in [(0xf0, 0), (0xf0, 1), (0xf1, 0)] {
            for fault in 1..=4 {
                scenario(durable, tag, skip, fault, false).await;
            }
        }
    }
}
