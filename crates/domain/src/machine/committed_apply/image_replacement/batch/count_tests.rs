use storage::{MemoryReplicaStore, MemoryStore, StateStore, WriteBatch};

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn structural_image(extra: &[&[u8]]) -> TestResult<crate::EncodedCommittedImage> {
    let stream = crate::CommittedStreamId::new([41; 16])?;
    let machine = crate::CommittedStateMachine::create(MemoryReplicaStore::new(), stream)?;
    let store = MemoryStore::default();
    let mut batch = WriteBatch::default();
    for (key, value) in machine.reader().snapshot()?.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    for key in extra {
        batch.push_put(key.to_vec(), Vec::new());
    }
    store.apply(batch)?;
    Ok(crate::EncodedCommittedImage::encode(
        crate::CommittedImageRole::CreateSendV1,
        stream,
        &store.snapshot()?,
    )?)
}

#[test]
fn private_count_core_matches_legacy_plan_and_refuses_wrong_cardinality() -> TestResult {
    // Extra keys isolate structural merge counts, not business admission.
    let old_bytes = structural_image(&[b"a", b"c", b"z"])?;
    let selected_bytes = structural_image(&[])?;
    let old = DecodedCommittedImage::decode(old_bytes.as_bytes())?;
    let selected = ValidatedCreateSendImage::validate(DecodedCommittedImage::decode(
        selected_bytes.as_bytes(),
    )?)?;
    let legacy = plan(&old, &selected)?;
    let counted = count_rows(
        old.rows(),
        selected.rows(),
        old.row_count(),
        selected.row_count(),
    )?;
    assert_eq!(
        counted.counts(),
        (3, selected.row_count(), legacy.mutations, legacy.bytes)
    );
    for (old_count, selected_count) in [
        (old.row_count() + 1, selected.row_count()),
        (old.row_count(), selected.row_count() + 1),
        (usize::MAX, selected.row_count()),
        (old.row_count(), usize::MAX),
    ] {
        assert_eq!(
            count_rows(old.rows(), selected.rows(), old_count, selected_count).err(),
            Some(Error::InvalidImage)
        );
    }
    let interleaved_bytes = structural_image(&[b"b", b"c", b"y"])?;
    let interleaved = DecodedCommittedImage::decode(interleaved_bytes.as_bytes())?;
    let counted = count_rows(
        old.rows(),
        interleaved.rows(),
        old.row_count(),
        interleaved.row_count(),
    )?;
    let put_bytes: usize = interleaved
        .rows()
        .map(|row| row.key().len() + row.value().len())
        .sum();
    assert_eq!(
        counted.counts(),
        (
            2,
            interleaved.row_count(),
            interleaved.row_count() + 2,
            put_bytes + 2
        )
    );
    Ok(())
}

#[test]
fn private_count_caps_overflow_and_refusals_leave_counts_unchanged() {
    let mut count = Plan::default();
    for _ in 0..MAX_MUTATIONS {
        assert_eq!(count.add(0, 0), Ok(()));
    }
    assert_eq!(count.add(0, 0), Err(Error::LimitExceeded));
    assert_eq!((count.mutations, count.bytes), (MAX_MUTATIONS, 0));
    let mut payload = Plan::default();
    assert_eq!(payload.add(MAX_PAYLOAD_BYTES, 0), Ok(()));
    for (key, value) in [(1, 0), (usize::MAX, 1)] {
        assert_eq!(payload.add(key, value), Err(Error::LimitExceeded));
        assert_eq!((payload.mutations, payload.bytes), (1, MAX_PAYLOAD_BYTES));
    }
    for mut forged in [
        Plan {
            mutations: usize::MAX,
            bytes: 0,
        },
        Plan {
            mutations: 0,
            bytes: usize::MAX,
        },
    ] {
        let before = (forged.mutations, forged.bytes);
        assert_eq!(forged.add(1, 0), Err(Error::LimitExceeded));
        assert_eq!((forged.mutations, forged.bytes), before);
    }
}
