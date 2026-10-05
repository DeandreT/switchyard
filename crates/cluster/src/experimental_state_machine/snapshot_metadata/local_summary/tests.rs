use super::*;
use crate::experimental_state_machine::{EncodedNativeSnapshotMetadata, captured_image_fixture};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn summary() -> TestResult<NativeCheckpointSummary> {
    let selected = captured_image_fixture::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(selected.image.as_bytes())?;
    let summary = NativeCheckpointSummary::decode(metadata.as_bytes())?;
    assert!(summary.matches(&selected.checkpoint));
    Ok(summary)
}

#[test]
fn checked_summary_preserves_refusal_watermark_and_exact_membership() -> TestResult {
    let original = summary()?;
    assert_eq!(original.highest_timestamp.as_millis(), 500);
    assert_eq!(original.membership.as_ref().unwrap().source.index, 0);
    assert_eq!(
        NativeCheckpointSummary::decode(&original.encode_for_test()?)?,
        original
    );
    assert!(original.recover()?.1.log_id().is_some());
    Ok(())
}

#[test]
fn checked_summary_refuses_missing_or_non_successor_previous_mark() -> TestResult {
    let original = summary()?;
    let mut missing = original.clone();
    missing.previous = None;
    assert_eq!(
        NativeCheckpointSummary::decode(&missing.encode_for_test()?).err(),
        Some(Error::IncompatibleCheckpoint)
    );
    let mut gap = original.clone();
    gap.previous.as_mut().unwrap().id.index -= 1;
    assert_eq!(
        NativeCheckpointSummary::decode(&gap.encode_for_test()?).err(),
        Some(Error::IncompatibleCheckpoint)
    );
    let mut regressed = original;
    regressed.previous.as_mut().unwrap().id.term = u64::MAX;
    assert_eq!(
        NativeCheckpointSummary::decode(&regressed.encode_for_test()?).err(),
        Some(Error::IncompatibleCheckpoint)
    );
    Ok(())
}

#[test]
fn checked_summary_refuses_membership_ahead_of_last_or_previous_identity() -> TestResult {
    let original = summary()?;
    for case in 0..3 {
        let mut bad = original.clone();
        let source = &mut bad.membership.as_mut().unwrap().source;
        match case {
            0 => source.index = 5,
            1 => {
                *source = bad.last.unwrap().id;
                source.node_id += 1;
            }
            _ => {
                *source = bad.previous.unwrap().id;
                source.term += 1;
            }
        }
        assert_eq!(
            NativeCheckpointSummary::decode(&bad.encode_for_test()?).err(),
            Some(Error::IncompatibleCheckpoint)
        );
    }
    Ok(())
}

#[test]
fn checked_summary_refuses_unknown_schema_and_noncanonical_membership_payload() -> TestResult {
    let original = summary()?;
    let mut schema = original.clone();
    schema.membership.as_mut().unwrap().schema_version += 1;
    assert_eq!(
        NativeCheckpointSummary::decode(&schema.encode_for_test()?).err(),
        Some(Error::IncompatibleCheckpoint)
    );
    let mut bytes = original;
    bytes.membership.as_mut().unwrap().payload.push(0);
    assert_eq!(
        NativeCheckpointSummary::decode(&bytes.encode_for_test()?).err(),
        Some(Error::IncompatibleCheckpoint)
    );
    Ok(())
}

#[test]
fn checked_summary_checks_empty_progress_and_zero_stream_before_use() -> TestResult {
    let original = summary()?;
    let original_bytes = original.encode_for_test()?;
    let mut wire = codec::decode(&original_bytes)?;
    wire.stream = [0; 16];
    let encoded = codec::encode(&wire)?;
    assert_eq!(
        NativeCheckpointSummary::decode(&encoded).err(),
        Some(Error::IncompatibleCheckpoint)
    );
    let mut empty = original;
    empty.last = None;
    empty.previous = None;
    empty.membership = None;
    assert_eq!(
        NativeCheckpointSummary::decode(&empty.encode_for_test()?).err(),
        Some(Error::IncompatibleCheckpoint)
    );
    empty.highest_timestamp = Timestamp::UNIX_EPOCH;
    assert!(NativeCheckpointSummary::decode(&empty.encode_for_test()?).is_ok());
    Ok(())
}
