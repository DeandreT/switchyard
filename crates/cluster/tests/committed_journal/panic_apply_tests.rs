//! Explicitly caught logical apply unwinds, not power cuts or panic recovery.
//! The outer wrapper records the original full batch before delegating once.

use super::*;
use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind, panic_any},
};

#[derive(Clone, Copy, Debug)]
enum Shape {
    FirstAppend,
    LaterAppend,
    FirstCommit,
    LaterCommit,
}

impl Shape {
    fn appends(self) -> bool {
        matches!(self, Self::FirstAppend | Self::LaterAppend)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct OuterTrace {
    gets: usize,
    scans: usize,
    snapshots: usize,
    batches: Vec<WriteBatch>,
}

struct Control {
    arm: AtomicUsize,
    payload: Arc<str>,
    trace: Mutex<OuterTrace>,
}

#[derive(Clone)]
struct PanicApply {
    inner: Observed,
    control: Arc<Control>,
}

impl PanicApply {
    fn new(actual: Backend, payload: Arc<str>) -> Self {
        Self {
            inner: Observed::new(actual),
            control: Arc::new(Control {
                arm: AtomicUsize::new(0),
                payload,
                trace: Mutex::new(OuterTrace::default()),
            }),
        }
    }

    fn arm(&self, after_apply: bool) {
        assert_eq!(
            self.control
                .arm
                .swap(if after_apply { 2 } else { 1 }, Ordering::SeqCst),
            0,
        );
    }

    fn reset(&self) {
        *self.control.trace.lock().unwrap() = OuterTrace::default();
        *self.inner.trace.lock().unwrap() = Trace::default();
    }

    fn trace(&self) -> OuterTrace {
        self.control.trace.lock().unwrap().clone()
    }
}

impl StateStore for PanicApply {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.control.trace.lock().unwrap().gets += 1;
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.control
            .trace
            .lock()
            .unwrap()
            .batches
            .push(batch.clone());
        let arm = self.control.arm.swap(0, Ordering::SeqCst);
        if arm == 1 {
            panic_any(Arc::clone(&self.control.payload));
        }
        self.inner.apply(batch)?;
        if arm == 2 {
            panic_any(Arc::clone(&self.control.payload));
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.control.trace.lock().unwrap().snapshots += 1;
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.control.trace.lock().unwrap().scans += 1;
        self.inner.scan_from(prefix, start, limit)
    }
}

fn entry_value(index: u64, payload: &[u8]) -> Value {
    let mut value = JOURNAL_FORMAT_VERSION.to_be_bytes().to_vec();
    value.extend_from_slice(&index.to_be_bytes());
    value.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    let mut hash = Sha256::new();
    hash.update(b"switchyard journal entry v1\0");
    hash.update(&value);
    hash.update(payload);
    value.extend_from_slice(&hash.finalize());
    value.extend_from_slice(payload);
    value
}

fn expected_batch(shape: Shape, index: u64, payload: &[u8]) -> WriteBatch {
    let mut batch = WriteBatch::default();
    if matches!(shape, Shape::FirstAppend) {
        batch.push_put(key(0), JOURNAL_FORMAT_VERSION.to_be_bytes().to_vec());
        batch.push_put(key(1), frontier_value(0));
    }
    if shape.appends() {
        batch.push_put(entry_key(index), entry_value(index, payload));
    } else {
        batch.push_put(key(1), frontier_value(index));
    }
    batch
}

fn apply_rows(before: &[(Key, Value)], batch: &WriteBatch) -> Vec<(Key, Value)> {
    let mut rows = before.iter().cloned().collect::<BTreeMap<_, _>>();
    for mutation in batch.mutations() {
        match mutation {
            Mutation::Put { key, value } => {
                rows.insert(key.clone(), value.clone());
            }
            Mutation::Delete { key } => {
                rows.remove(key);
            }
        }
    }
    rows.into_iter().collect()
}

fn no_io(observed: &PanicApply) {
    assert_eq!(observed.trace(), OuterTrace::default());
    let inner = observed.inner.trace.lock().unwrap();
    assert!(inner.gets.is_empty() && inner.scans.is_empty() && inner.batches.is_empty());
}

fn healthy_unstarted_controls(journal: &mut Journal<PanicApply>, observed: &PanicApply) {
    let committed = journal.committed_index().unwrap();
    let last = journal.last_appended_index().unwrap();
    for index in [0, last, last + 2, u64::MAX] {
        assert_eq!(
            journal.append(index, b"invalid"),
            Err(JournalError::InvalidAppendIndex {
                found: index,
                expected: last + 1,
            })
        );
    }
    let oversized = vec![0; MAX_JOURNAL_PAYLOAD_BYTES + 1];
    assert_eq!(
        journal.append(last + 1, &oversized),
        Err(JournalError::PayloadTooLarge {
            bytes: oversized.len(),
            maximum: MAX_JOURNAL_PAYLOAD_BYTES,
        })
    );
    assert_eq!(
        journal.commit(last + 1),
        Err(JournalError::InvalidCommit {
            requested: last + 1,
            committed,
            last_appended: last,
        })
    );
    if committed > 0 {
        assert_eq!(
            journal.commit(committed - 1),
            Err(JournalError::InvalidCommit {
                requested: committed - 1,
                committed,
                last_appended: last,
            })
        );
    }
    assert_eq!(journal.commit(committed), Ok(()));
    assert_eq!(
        journal.read_committed(0, 1),
        Err(JournalError::InvalidReadStart)
    );
    for limit in [0, MAX_JOURNAL_READ_ENTRIES + 1, usize::MAX] {
        assert_eq!(
            journal.read_committed(1, limit),
            Err(JournalError::InvalidReadLimit {
                requested: limit,
                maximum: MAX_JOURNAL_READ_ENTRIES,
            })
        );
    }
    assert!(journal.read_committed(last + 1, 1).unwrap().is_empty());
    assert_eq!(journal.committed_index(), Ok(committed));
    assert_eq!(journal.last_appended_index(), Ok(last));
    no_io(observed);
}

fn unusable_controls(journal: &mut Journal<PanicApply>, observed: &PanicApply, index: u64) {
    assert_eq!(journal.committed_index(), Err(JournalError::Unusable));
    assert_eq!(journal.last_appended_index(), Err(JournalError::Unusable));
    for (from, limit) in [(0, 0), (1, 1), (u64::MAX, usize::MAX)] {
        assert_eq!(
            journal.read_committed(from, limit),
            Err(JournalError::Unusable)
        );
    }
    let oversized = vec![0; MAX_JOURNAL_PAYLOAD_BYTES + 1];
    for (next, payload) in [
        (0, &[][..]),
        (index, &b"never replay"[..]),
        (u64::MAX, oversized.as_slice()),
    ] {
        assert_eq!(journal.append(next, payload), Err(JournalError::Unusable));
    }
    for frontier in [0, index - 1, index, u64::MAX] {
        assert_eq!(journal.commit(frontier), Err(JournalError::Unusable));
    }
    no_io(observed);
}

fn apply_unwind(durable: bool, shape: Shape) {
    for after_apply in [false, true] {
        let mut fixture = Fixture::new(durable);
        let others = seed_other_records(&fixture);
        let mut setup = Journal::open(fixture.store().clone()).unwrap();
        match shape {
            Shape::FirstAppend => {}
            Shape::LaterAppend => {
                setup.append(1, b"prior").unwrap();
                setup.commit(1).unwrap();
            }
            Shape::FirstCommit => {
                setup.append(1, b"pending first commit").unwrap();
            }
            Shape::LaterCommit => {
                setup.append(1, b"prior").unwrap();
                setup.commit(1).unwrap();
                setup.append(2, b"pending later commit").unwrap();
            }
        }
        let last_before = setup.last_appended_index().unwrap();
        let committed_before = setup.committed_index().unwrap();
        drop(setup);
        let before = fixture.snapshot();
        let payload: Arc<str> = Arc::from("controlled original journal apply unwind");
        let observed = PanicApply::new(fixture.store().clone(), Arc::clone(&payload));
        let mut journal = Journal::open(observed.clone()).unwrap();
        observed.reset();
        observed.arm(after_apply);
        healthy_unstarted_controls(&mut journal, &observed);
        assert_eq!(
            observed.control.arm.load(Ordering::SeqCst),
            if after_apply { 2 } else { 1 }
        );
        assert_eq!(fixture.snapshot(), before);
        let index = if shape.appends() {
            last_before + 1
        } else {
            last_before
        };
        let append_payload = b"original ambiguous append";
        let expected = expected_batch(shape, index, append_payload);
        // Only this original writer invocation is caught; production has no catch.
        let caught = catch_unwind(AssertUnwindSafe(|| {
            if shape.appends() {
                journal.append(index, append_payload)
            } else {
                journal.commit(index)
            }
        }))
        .expect_err("the original apply must unwind");
        let actual = caught.downcast::<Arc<str>>().unwrap();
        assert!(Arc::ptr_eq(&actual, &payload));
        assert_eq!(observed.control.arm.load(Ordering::SeqCst), 0);
        assert_eq!(
            observed.trace(),
            OuterTrace {
                batches: vec![expected.clone()],
                ..OuterTrace::default()
            }
        );
        assert_eq!(
            observed.inner.trace.lock().unwrap().batches,
            if after_apply {
                vec![expected.mutations().to_vec()]
            } else {
                Vec::new()
            }
        );
        let complete = apply_rows(&before, &expected);
        let physical = if after_apply { &complete } else { &before };
        assert_eq!(&fixture.snapshot(), physical);
        assert_eq!(other_records(physical), others);
        observed.reset();
        unusable_controls(&mut journal, &observed, index);
        assert_eq!(&fixture.snapshot(), physical);
        // Drop every original actual backend handle before durable reopen.
        drop(journal);
        drop(observed);
        fixture.reopen_store();
        assert_eq!(&fixture.snapshot(), physical);
        let reopened_store = PanicApply::new(fixture.store().clone(), Arc::clone(&payload));
        let mut reopened = Journal::open(reopened_store.clone()).unwrap();
        let last = last_before + u64::from(after_apply && shape.appends());
        let committed = if after_apply && !shape.appends() {
            index
        } else {
            committed_before
        };
        assert_eq!(reopened.last_appended_index(), Ok(last));
        assert_eq!(reopened.committed_index(), Ok(committed));
        reopened_store.reset();
        if shape.appends() && after_apply {
            assert_eq!(
                reopened.append(index, append_payload),
                Err(JournalError::InvalidAppendIndex {
                    found: index,
                    expected: index + 1,
                })
            );
            no_io(&reopened_store);
        } else {
            if shape.appends() {
                reopened.append(index, append_payload).unwrap();
            } else {
                reopened.commit(index).unwrap();
            }
            if after_apply {
                no_io(&reopened_store);
            } else {
                assert_eq!(
                    reopened_store.trace(),
                    OuterTrace {
                        batches: vec![expected.clone()],
                        ..OuterTrace::default()
                    }
                );
                assert_eq!(
                    reopened_store.inner.trace.lock().unwrap().batches,
                    vec![expected.mutations().to_vec()]
                );
            }
        }
        assert_eq!(fixture.snapshot(), complete);
        assert_eq!(other_records(&fixture.snapshot()), others);
        let last = if shape.appends() { index } else { last_before };
        let committed = if shape.appends() {
            committed_before
        } else {
            index
        };
        assert_eq!(reopened.last_appended_index(), Ok(last));
        assert_eq!(reopened.committed_index(), Ok(committed));
        assert_eq!(
            reopened
                .read_committed(1, MAX_JOURNAL_READ_ENTRIES)
                .unwrap()
                .len(),
            committed as usize
        );
        if shape.appends() {
            assert!(reopened.read_committed(index, 1).unwrap().is_empty());
        }
        reopened.append(last + 1, b"normal next append").unwrap();
        reopened.commit(last + 1).unwrap();
        assert_eq!(reopened.last_appended_index(), Ok(last + 1));
        assert_eq!(reopened.committed_index(), Ok(last + 1));
        assert_eq!(
            reopened.read_committed(last + 1, 1).unwrap()[0].payload(),
            b"normal next append"
        );
        reopened_store.reset();
        healthy_unstarted_controls(&mut reopened, &reopened_store);
        let final_rows = fixture.snapshot();
        drop(reopened);
        drop(reopened_store);
        let final_owner = fixture.reopen();
        assert_eq!(fixture.snapshot(), final_rows);
        assert_eq!(final_owner.last_appended_index(), Ok(last + 1));
        assert_eq!(final_owner.committed_index(), Ok(last + 1));
        assert_eq!(other_records(&fixture.snapshot()), others);
    }
}

#[test]
fn memory_first_append_apply_unwind_requires_reopen() {
    apply_unwind(false, Shape::FirstAppend);
}
#[test]
fn fjall_first_append_apply_unwind_requires_reopen() {
    apply_unwind(true, Shape::FirstAppend);
}
#[test]
fn memory_later_append_apply_unwind_requires_reopen() {
    apply_unwind(false, Shape::LaterAppend);
}
#[test]
fn fjall_later_append_apply_unwind_requires_reopen() {
    apply_unwind(true, Shape::LaterAppend);
}
#[test]
fn memory_first_commit_apply_unwind_requires_reopen() {
    apply_unwind(false, Shape::FirstCommit);
}
#[test]
fn fjall_first_commit_apply_unwind_requires_reopen() {
    apply_unwind(true, Shape::FirstCommit);
}
#[test]
fn memory_later_commit_apply_unwind_requires_reopen() {
    apply_unwind(false, Shape::LaterCommit);
}
#[test]
fn fjall_later_commit_apply_unwind_requires_reopen() {
    apply_unwind(true, Shape::LaterCommit);
}
