use std::panic::{AssertUnwindSafe, catch_unwind};

use super::{fixture::*, *};
use crate::{
    MAX_CATALOG_ARTIFACT_BYTES, MAX_CATALOG_METADATA_BYTES, MAX_PROTECTED_STATE_BYTES,
    PROTECTED_STATE_RECORD_LIMITS, SnapshotCatalogRecord,
    protected_state::{check_components, combined_bytes, reserve},
};

#[test]
fn actual_row_limit_refuses_before_preparation() -> TestResult {
    let mut writer = initialized();
    let prior = view(&writer.reader())?;
    let count = counts(&writer);
    let keys: Vec<_> = (0u32..65_537).map(u32::to_be_bytes).collect();
    let rows: Vec<_> = keys
        .iter()
        .map(|key| (key.as_slice(), b"".as_slice()))
        .collect();
    let refused = publish(&mut writer, &rows, b"m", b"a", b"two");
    assert_eq!(refused, Err(ProtectedStateError::LimitExceeded));
    assert_eq!(counts(&writer), count);
    assert_eq!(view(&writer.reader())?, prior);
    assert!(check_components(65_536, 0, 0, 1, ProtectedStateError::InvalidInput).is_ok());
    assert_eq!(
        check_components(65_537, 0, 0, 1, ProtectedStateError::InvalidInput),
        Err(ProtectedStateError::LimitExceeded)
    );
    Ok(())
}

#[test]
fn actual_key_value_limits_refuse_before_preparation() -> TestResult {
    let mut writer = initialized();
    let prior = view(&writer.reader())?;
    let count = counts(&writer);
    let long_key = vec![1; 1_025];
    let long_value = vec![2; 266_241];
    let key_result = publish(&mut writer, &[(long_key.as_slice(), b"")], b"", b"", b"two");
    let value_result = publish(
        &mut writer,
        &[(b"k", long_value.as_slice())],
        b"",
        b"",
        b"two",
    );
    assert_eq!(key_result, Err(ProtectedStateError::LimitExceeded));
    assert_eq!(value_result, Err(ProtectedStateError::LimitExceeded));
    assert_eq!(counts(&writer), count);
    assert_eq!(view(&writer.reader())?, prior);
    let key = vec![1; 1_024];
    let value = vec![2; 266_240];
    publish(
        &mut writer,
        &[(key.as_slice(), value.as_slice())],
        b"",
        b"",
        b"two",
    )?;
    publish(&mut writer, &[(b"k", b"")], b"", b"", b"three")?;
    assert!(view(&writer.reader())?.rows[0].1.is_empty());
    Ok(())
}

#[test]
fn malformed_row_order_duplicates_and_empty_keys_refuse() -> TestResult {
    let mut writer = initialized();
    let prior = view(&writer.reader())?;
    let count = counts(&writer);
    for rows in [
        &[
            (b"z".as_slice(), b"".as_slice()),
            (b"a".as_slice(), b"".as_slice()),
        ][..],
        &[
            (b"a".as_slice(), b"".as_slice()),
            (b"a".as_slice(), b"v".as_slice()),
        ][..],
        &[(b"".as_slice(), b"".as_slice())][..],
    ] {
        assert_eq!(
            publish(&mut writer, rows, b"", b"", b"two"),
            Err(ProtectedStateError::InvalidInput)
        );
    }
    assert_eq!(counts(&writer), count);
    assert_eq!(view(&writer.reader())?, prior);
    Ok(())
}

#[test]
fn empty_or_oversized_fence_refuses_before_preparation() -> TestResult {
    let mut writer = initialized();
    let prior = view(&writer.reader())?;
    let count = counts(&writer);
    assert_eq!(
        publish(&mut writer, &[], b"", b"", b""),
        Err(ProtectedStateError::InvalidInput)
    );
    assert_eq!(
        publish(&mut writer, &[], b"", b"", &[1; 257]),
        Err(ProtectedStateError::LimitExceeded)
    );
    assert_eq!(counts(&writer), count);
    assert_eq!(view(&writer.reader())?, prior);
    publish(&mut writer, &[], b"", b"", &[1; 256])?;
    assert_eq!(view(&writer.reader())?.fence.unwrap().len(), 256);
    Ok(())
}

#[test]
fn catalog_factory_limits_precede_typed_input() {
    let metadata = vec![0; MAX_CATALOG_METADATA_BYTES + 1];
    assert!(SnapshotCatalogRecord::new(&metadata, b"").is_err());
    drop(metadata);
    // One serial oversized component, not an exact-maximum success/OOM test.
    let artifact = vec![0; MAX_CATALOG_ARTIFACT_BYTES + 1];
    assert!(SnapshotCatalogRecord::new(b"", &artifact).is_err());
    drop(artifact);
    assert_eq!(
        check_components(
            0,
            MAX_CATALOG_METADATA_BYTES + 1,
            0,
            1,
            ProtectedStateError::InvalidInput
        ),
        Err(ProtectedStateError::LimitExceeded)
    );
    assert_eq!(
        check_components(
            0,
            0,
            MAX_CATALOG_ARTIFACT_BYTES + 1,
            1,
            ProtectedStateError::InvalidInput
        ),
        Err(ProtectedStateError::LimitExceeded)
    );
}

#[test]
fn numeric_component_combined_caps_and_overflow() {
    let limits = PROTECTED_STATE_RECORD_LIMITS;
    assert_eq!(
        combined_bytes(
            limits.max_total_bytes,
            MAX_CATALOG_METADATA_BYTES,
            MAX_CATALOG_ARTIFACT_BYTES,
            256
        ),
        Ok(MAX_PROTECTED_STATE_BYTES)
    );
    assert_eq!(
        combined_bytes(
            limits.max_total_bytes,
            MAX_CATALOG_METADATA_BYTES,
            MAX_CATALOG_ARTIFACT_BYTES,
            257
        ),
        Err(ProtectedStateError::LimitExceeded)
    );
    assert_eq!(
        combined_bytes(usize::MAX, 1, 0, 0),
        Err(ProtectedStateError::LimitExceeded)
    );
    assert_eq!(
        combined_bytes(0, usize::MAX, 1, 0),
        Err(ProtectedStateError::LimitExceeded)
    );
    assert_eq!(
        check_components(usize::MAX, 0, 0, 1, ProtectedStateError::InvalidInput),
        Err(ProtectedStateError::LimitExceeded)
    );
    let mut budget = crate::ReadBudget::new(limits);
    for _ in 0..256 {
        budget.consume(1, 262_143).unwrap();
    }
    assert_eq!(
        budget.consume(1, 0),
        Err(crate::StorageError::ReadLimitExceeded)
    );
    let mut overflow = crate::ReadBudget::new(crate::ReadLimits {
        max_rows: usize::MAX,
        max_key_bytes: usize::MAX,
        max_value_bytes: usize::MAX,
        max_total_bytes: usize::MAX,
    });
    assert_eq!(
        overflow.consume(usize::MAX, 1),
        Err(crate::StorageError::ReadLimitExceeded)
    );
}

#[test]
fn impossible_output_capacity_is_fallible() {
    let mut output = Vec::<(Vec<u8>, Vec<u8>)>::new();
    assert_eq!(
        reserve(&mut output, usize::MAX),
        Err(ProtectedStateError::Allocation)
    );
    assert!(output.is_empty());
}

#[test]
fn preparation_refusal_preserves_prior_state() -> TestResult {
    let mut writer = initialized();
    let reader = writer.reader();
    let prior = view(&reader)?;
    let count = counts(&writer);
    writer.fault = Fault::Prepare;
    let refused = publish(&mut writer, &[(b"b", b"new")], b"m", b"a", b"two");
    let after_count = counts(&writer);
    writer.fault = Fault::None;
    assert_eq!(refused, Err(ProtectedStateError::Allocation));
    assert_eq!(count, after_count);
    assert_eq!(view(&reader)?, prior);
    publish(&mut writer, &[], b"", b"", b"two")?;
    Ok(())
}

#[test]
fn invalid_private_current_state_refuses_publication() -> TestResult {
    for case in 0..7 {
        let mut writer = initialized();
        let count = counts(&writer);
        {
            let mut cell = writer.cell.write().unwrap();
            match case {
                0 => cell.data.initialized = false,
                1 => cell.data.live = None,
                2 => cell.data.fence = None,
                3 => cell.data.fence = Some(vec![]),
                4 => {
                    cell.data = data(
                        vec![(b"z".to_vec(), vec![]), (b"a".to_vec(), vec![])],
                        b"one",
                    )
                }
                5 => {
                    cell.data = data(
                        vec![(b"a".to_vec(), vec![]), (b"a".to_vec(), vec![])],
                        b"one",
                    )
                }
                _ => cell.data = data(vec![(vec![], vec![])], b"one"),
            }
        }
        // Current validation wins even when the offered fence matches.
        assert_eq!(
            publish(&mut writer, &[], b"", b"", b"one"),
            Err(ProtectedStateError::InvalidState)
        );
        assert_eq!(counts(&writer), count);
    }
    Ok(())
}

#[test]
fn invalid_private_read_view_precedes_result_copy() {
    for case in 0..6 {
        let writer = initialized();
        let reader = writer.reader();
        let count = counts(&writer);
        let expected = {
            let mut cell = writer.cell.write().unwrap();
            match case {
                0 => {
                    cell.poisoned = true;
                    cell.data.live = None;
                    ProtectedStateError::Poisoned
                }
                1 => {
                    cell.data.live = None;
                    ProtectedStateError::InvalidState
                }
                2 => {
                    cell.data.fence = Some(vec![0; 257]);
                    ProtectedStateError::LimitExceeded
                }
                3 => {
                    cell.data = data(vec![(vec![0; 1_025], vec![])], b"one");
                    ProtectedStateError::LimitExceeded
                }
                4 => {
                    cell.data = data(vec![(b"a".to_vec(), vec![0; 266_241])], b"one");
                    ProtectedStateError::LimitExceeded
                }
                _ => {
                    cell.data.logical_bytes += 1;
                    ProtectedStateError::InvalidState
                }
            }
        };
        assert_eq!(reader.capture_protected_state().err(), Some(expected));
        assert_eq!(counts(&writer), count);
    }
}

#[test]
fn poisoned_lock_refuses_read_and_publication() {
    let mut writer = initialized();
    let reader = writer.reader();
    let count = counts(&writer);
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _guard = writer.cell.write().unwrap();
        panic!("lock fault");
    }));
    let read = reader.capture_protected_state();
    let refused = publish(&mut writer, &[], b"", b"", b"two");
    assert!(panic.is_err());
    assert_eq!(read.err(), Some(ProtectedStateError::Poisoned));
    assert_eq!(refused, Err(ProtectedStateError::Poisoned));
    assert_eq!(counts(&writer), count);
}

fn entered(fault: Fault) -> TestResult {
    let mut writer = initialized();
    let reader = writer.reader();
    let captured_before = reader.capture_protected_state()?;
    let count = counts(&writer);
    writer.fault = fault;
    let outcome = publish(&mut writer, &[(b"b", b"new")], b"m", b"a", b"two");
    let after = counts(&writer);
    writer.fault = Fault::None;
    let read = reader.capture_protected_state();
    let retry = publish(&mut writer, &[], b"", b"", b"three");
    assert_eq!(outcome, Err(ProtectedStateError::PublishUnknown));
    assert_eq!(after, (count.0 + 1, count.1 + 1));
    assert_eq!(counts(&writer), after);
    assert_eq!(read.err(), Some(ProtectedStateError::Poisoned));
    assert_eq!(retry, Err(ProtectedStateError::Poisoned));
    assert_eq!(captured_before.fence(), Some(b"one".as_slice()));
    Ok(())
}

#[test]
fn entered_before_swap_error_is_unknown_and_poisoned() -> TestResult {
    entered(Fault::Before)
}

#[test]
fn entered_after_swap_error_is_unknown_and_poisoned() -> TestResult {
    entered(Fault::After)
}

#[test]
fn entered_unwind_terminal_marks_before_unlock() -> TestResult {
    let mut writer = initialized();
    let reader = writer.reader();
    let count = counts(&writer);
    writer.fault = Fault::Panic;
    let original = catch_unwind(AssertUnwindSafe(|| {
        publish(&mut writer, &[], b"", b"", b"two")
    }));
    let cell = writer
        .cell
        .read()
        .err()
        .expect("entered unwind poisons write lock")
        .into_inner();
    let terminal_mark = cell.poisoned;
    drop(cell);
    let read = reader.capture_protected_state();
    let refused = publish(&mut writer, &[], b"", b"", b"three");
    assert!(original.is_err());
    assert!(terminal_mark);
    assert_eq!(read.err(), Some(ProtectedStateError::Poisoned));
    assert_eq!(refused, Err(ProtectedStateError::Poisoned));
    assert_eq!(counts(&writer), (count.0 + 1, count.1 + 1));
    Ok(())
}
