use storage::{MemoryReplicaStore, MemoryStore, StateStore, WriteBatch};

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn encoded(keys: &[&[u8]]) -> TestResult<crate::EncodedCommittedImage> {
    let stream = crate::CommittedStreamId::new([7; 16])?;
    let machine = crate::CommittedStateMachine::create(MemoryReplicaStore::new(), stream)?;
    let raw = MemoryStore::default();
    let mut batch = WriteBatch::default();
    for (key, value) in machine.reader().snapshot()?.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    for key in keys {
        batch.push_put(key.to_vec(), Vec::new());
    }
    raw.apply(batch)?;
    Ok(crate::EncodedCommittedImage::encode(
        crate::CommittedImageRole::CreateSendV1,
        stream,
        &raw.snapshot()?,
    )?)
}

#[test]
fn stale_merge_handles_empty_overlap_and_interleaved_before_after_keys() -> TestResult {
    // Structural fixtures isolate the merge, not target semantic admission.
    for (old, new, expected) in [
        (vec![], vec![], vec![]),
        (vec![b"a".as_slice()], vec![b"a".as_slice()], vec![]),
        (vec![b"a".as_slice()], vec![], vec![b"a".as_slice()]),
        (vec![], vec![b"a".as_slice()], vec![]),
        (
            vec![
                b"\x10".as_slice(),
                b"a".as_slice(),
                b"c".as_slice(),
                b"g".as_slice(),
                b"z".as_slice(),
            ],
            vec![
                b"\x11".as_slice(),
                b"b".as_slice(),
                b"c".as_slice(),
                b"d".as_slice(),
                b"y".as_slice(),
            ],
            vec![
                b"\x10".as_slice(),
                b"a".as_slice(),
                b"g".as_slice(),
                b"z".as_slice(),
            ],
        ),
    ] {
        let old_image = encoded(&old)?;
        let selected_image = encoded(&new)?;
        let old = DecodedCommittedImage::decode(old_image.as_bytes())?;
        let selected = DecodedCommittedImage::decode(selected_image.as_bytes())?;
        assert_eq!(
            StaleKeys::new(old.rows(), selected.rows()).collect::<Vec<_>>(),
            expected
        );
    }
    Ok(())
}

#[test]
fn exact_mutation_count_cap_refuses_the_next_count_without_changing_plan() {
    let mut plan = Plan::default();
    for _ in 0..MAX_MUTATIONS {
        assert_eq!(plan.add(0, 0), Ok(()));
    }
    assert_eq!(plan.mutations, 2 * crate::MAX_COMMITTED_IMAGE_ROWS);
    assert_eq!(plan.add(0, 0), Err(Error::LimitExceeded));
    assert_eq!(plan.mutations, MAX_MUTATIONS);
    assert_eq!(plan.bytes, 0);
}

#[test]
fn exact_payload_cap_and_overflow_refusals_leave_plan_unchanged() {
    let mut plan = Plan::default();
    assert_eq!(plan.add(MAX_PAYLOAD_BYTES, 0), Ok(()));
    assert_eq!(plan.bytes, 2 * crate::MAX_COMMITTED_IMAGE_BYTES);
    assert_eq!(plan.add(1, 0), Err(Error::LimitExceeded));
    assert_eq!(plan.add(usize::MAX, 1), Err(Error::LimitExceeded));
    assert_eq!(plan.bytes, MAX_PAYLOAD_BYTES);
    assert_eq!(plan.mutations, 1);
}

#[test]
fn fallible_mutation_reservation_returns_allocation_before_copying_a_row() -> TestResult {
    let image = encoded(&[])?;
    let decoded = DecodedCommittedImage::decode(image.as_bytes())?;
    let selected =
        ValidatedCreateSendImage::validate(DecodedCommittedImage::decode(image.as_bytes())?)?;
    // A private forged plan forces deterministic capacity overflow, not heap OOM.
    let plan = Plan {
        mutations: usize::MAX,
        bytes: 0,
    };
    assert_eq!(
        copy_deletes(&decoded, &selected, &plan).err(),
        Some(Error::Allocation)
    );
    Ok(())
}
