//! Explicitly caught original apply unwinds, not panic-abort or power-cut faults.

use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};

#[derive(Clone, Copy)]
enum Boundary {
    Before = 1,
    After = 2,
}

#[derive(Default)]
struct ApplyWitness {
    calls: usize,
    batches: Vec<WriteBatch>,
}

struct PanicControl {
    armed: AtomicUsize,
    witness: Mutex<ApplyWitness>,
    payload: Arc<str>,
}

impl PanicControl {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            armed: AtomicUsize::new(0),
            witness: Mutex::new(ApplyWitness::default()),
            payload: Arc::from("original indexed external apply panic"),
        })
    }

    fn arm(&self, boundary: Boundary) {
        *self.witness.lock().unwrap() = ApplyWitness::default();
        assert_eq!(self.armed.swap(boundary as usize, Ordering::SeqCst), 0);
    }
}

#[derive(Clone)]
struct PanicApply<S> {
    inner: Observed<S>,
    control: Arc<PanicControl>,
}

impl<S: StateStore> StateStore for PanicApply<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        {
            let mut witness = self.control.witness.lock().unwrap();
            witness.calls += 1;
            witness.batches.push(batch.clone());
        }
        let boundary = self.control.armed.swap(0, Ordering::SeqCst);
        if boundary == Boundary::Before as usize {
            panic_any(Arc::clone(&self.control.payload));
        }
        self.inner.apply(batch)?;
        if boundary == Boundary::After as usize {
            panic_any(Arc::clone(&self.control.payload));
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
}

fn assert_no_io(trace: &Trace) {
    assert!(trace.gets.is_empty());
    assert!(trace.scans.is_empty());
    assert!(trace.batches.is_empty());
    assert_eq!(trace.snapshots, 0);
}

fn panic_apply_matrix<P: StoreProvider>(provider: P, later: bool) -> TestResult {
    let mut fixture = Fixture::new(provider);
    for shape in 0..3 {
        for boundary in [Boundary::Before, Boundary::After] {
            fixture.clear();
            let control = PanicControl::new();
            let store = PanicApply {
                inner: fixture.store().clone(),
                control: Arc::clone(&control),
            };
            let mut writer = IndexedWriter::open(store.clone())?;
            let index = if later {
                assert_eq!(
                    writer.apply(1, &proposal(&queue(), 10, create_queue()))?,
                    applied(CommandOutcome::QueueCreated)
                );
                2
            } else {
                1
            };
            let case = shape + usize::from(later) * 3;
            let (kind, expected) = match case {
                0 => (create_queue(), applied(CommandOutcome::QueueCreated)),
                1 => (
                    CommandKind::ExpireSessionLocks,
                    applied(CommandOutcome::SessionLocksExpired { released: 0 }),
                ),
                2 => (
                    receive(),
                    IndexedApplyOutcome::Refused(BrokerError::QueueNotFound),
                ),
                3 => (
                    send("panic-original"),
                    applied(CommandOutcome::Sent {
                        sequence: SequenceNumber::new(1),
                    }),
                ),
                4 => (receive(), applied(CommandOutcome::Received(None))),
                _ => (
                    create_queue(),
                    IndexedApplyOutcome::Refused(BrokerError::QueueAlreadyExists),
                ),
            };
            let original = proposal(&queue(), 30, kind);
            let before = fixture.snapshot();
            fixture.reset();
            control.arm(boundary);
            // The catch surrounds only the original writer call. The store
            // retains its full invocation witness before either panic side.
            let caught = catch_unwind(AssertUnwindSafe(|| writer.apply(index, &original)));
            let payload = caught.expect_err("original external apply panic escapes unchanged");
            assert!(Arc::ptr_eq(
                payload.downcast_ref::<Arc<str>>().unwrap(),
                &control.payload
            ));
            assert_eq!(control.armed.load(Ordering::SeqCst), 0);
            let batch = {
                let witness = control.witness.lock().unwrap();
                assert_eq!(witness.calls, 1);
                assert_eq!(witness.batches.len(), 1);
                witness.batches[0].clone()
            };
            let delegated = fixture.trace().batches;
            let after = matches!(boundary, Boundary::After);
            assert_eq!(
                delegated,
                if after {
                    vec![batch.clone()]
                } else {
                    Vec::new()
                }
            );
            let mut metadata = Vec::new();
            if !later {
                metadata.push(Mutation::Put {
                    key: key(0),
                    value: owner(1),
                });
            }
            metadata.push(Mutation::Put {
                key: key(1),
                value: checkpoint(index, proposal_hash(&original)),
            });
            let actual_metadata: Vec<_> = batch
                .mutations()
                .iter()
                .filter(|mutation| match mutation {
                    Mutation::Put { key, .. } | Mutation::Delete { key } => key.starts_with(PREFIX),
                })
                .cloned()
                .collect();
            assert_eq!(actual_metadata, metadata);
            assert_eq!(batch.mutations().last(), metadata.last());
            let domain_effects = effects(&batch);
            if matches!(case, 0 | 3) {
                assert!(!domain_effects.is_empty());
                assert!(domain_effects.mutations().contains(&Mutation::Put {
                    key: keys::clock(),
                    value: codec::encode(&Timestamp::from_millis(30))?,
                }));
            } else {
                assert!(domain_effects.is_empty());
            }
            let complete = apply_rows(&before, &batch);
            assert_ne!(
                before, complete,
                "even no-op/refusal advances its checkpoint"
            );
            let persisted = if after { &complete } else { &before };
            assert_eq!(fixture.snapshot(), *persisted);
            assert_eq!(
                fixture.machine().last_applied_time()?,
                Timestamp::from_millis(if after && matches!(case, 0 | 3) {
                    30
                } else if later {
                    10
                } else {
                    0
                })
            );

            fixture.reset();
            assert_eq!(writer.applied_index(), Err(IndexedApplyError::Unusable));
            assert_eq!(
                writer.apply(index, &original),
                Err(IndexedApplyError::Unusable)
            );
            assert_eq!(writer.apply(0, &original), Err(IndexedApplyError::Unusable));
            assert_eq!(
                writer.apply(index - 1, &original),
                Err(IndexedApplyError::Unusable)
            );
            let unrelated = proposal(&queue(), 40, CommandKind::ExpireSessionLocks);
            assert_eq!(
                writer.apply(index, &unrelated),
                Err(IndexedApplyError::Unusable)
            );
            assert_eq!(
                writer.apply(index + 1, &unrelated),
                Err(IndexedApplyError::Unusable)
            );
            let mut oversized = send("unusable-cannot-encode");
            if let CommandKind::Send { body, .. } = &mut oversized {
                *body = vec![0; domain::MAX_DURABLE_PROPOSAL_BYTES + 1];
            }
            let oversized = proposal(&queue(), 40, oversized);
            assert_eq!(
                oversized.encode(),
                Err(domain::DurableProposalError::TooLarge)
            );
            assert_eq!(
                writer.apply(index, &oversized),
                Err(IndexedApplyError::Unusable)
            );
            assert_no_io(&fixture.trace());
            assert_eq!(control.witness.lock().unwrap().calls, 1);
            assert_eq!(fixture.snapshot(), *persisted);

            // Neither the original writer nor this external wrapper survives
            // the fixture's actual Fjall handle drop and directory reopen.
            drop(writer);
            drop(store);
            fixture.reopen();
            assert_eq!(fixture.snapshot(), *persisted);
            let mut writer = fixture.writer();
            assert_eq!(
                writer.applied_index()?,
                if after { index } else { index - 1 }
            );
            fixture.reset();
            assert_eq!(
                writer.apply(index, &original)?,
                if after {
                    IndexedApplyOutcome::AlreadyApplied
                } else {
                    expected
                }
            );
            if after {
                assert_no_io(&fixture.trace());
            } else {
                assert_eq!(fixture.trace().batches, vec![batch]);
            }
            assert_eq!(fixture.snapshot(), complete);
            assert_eq!(writer.applied_index()?, index);
            fixture.reset();
            assert_eq!(
                writer.apply(index, &original)?,
                IndexedApplyOutcome::AlreadyApplied
            );
            assert_no_io(&fixture.trace());

            let (next_kind, next_outcome) = if matches!(case, 1 | 2) {
                (create_queue(), CommandOutcome::QueueCreated)
            } else {
                (
                    send("healthy-after-reopen"),
                    CommandOutcome::Sent {
                        sequence: SequenceNumber::new(if case == 3 { 2 } else { 1 }),
                    },
                )
            };
            let next = proposal(&queue(), 40, next_kind);
            fixture.reset();
            assert_eq!(writer.apply(index + 1, &next)?, applied(next_outcome));
            assert_eq!(writer.applied_index()?, index + 1);
            let next_batches = fixture.trace().batches;
            assert_eq!(next_batches.len(), 1);
            assert_eq!(
                next_batches[0].mutations().last(),
                Some(&Mutation::Put {
                    key: key(1),
                    value: checkpoint(index + 1, proposal_hash(&next)),
                })
            );
            assert_eq!(fixture.snapshot(), apply_rows(&complete, &next_batches[0]));
            assert_eq!(
                fixture.machine().last_applied_time()?,
                Timestamp::from_millis(40)
            );
            fixture.reset();
            assert_eq!(
                writer.apply(index + 1, &next)?,
                IndexedApplyOutcome::AlreadyApplied
            );
            assert_no_io(&fixture.trace());
            assert_eq!(control.witness.lock().unwrap().calls, 1);
            drop(writer);
        }
    }
    Ok(())
}

#[test]
fn memory_initial_apply_panics_retire_every_api_and_reopen_resolves_index() -> TestResult {
    panic_apply_matrix(MemoryProvider::new(), false)
}

#[test]
fn fjall_initial_apply_panics_retire_every_api_and_reopen_resolves_index() -> TestResult {
    panic_apply_matrix(DurableProvider::temporary()?, false)
}

#[test]
fn memory_later_apply_panics_retire_every_api_and_reopen_resolves_index() -> TestResult {
    panic_apply_matrix(MemoryProvider::new(), true)
}

#[test]
fn fjall_later_apply_panics_retire_every_api_and_reopen_resolves_index() -> TestResult {
    panic_apply_matrix(DurableProvider::temporary()?, true)
}
