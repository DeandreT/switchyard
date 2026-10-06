use super::*;
use crate::{PROTECTED_STATE_RECORD_LIMITS, SnapshotCatalogRecord};

type OfferedCase<'a> = (&'a [(&'a [u8], &'a [u8])], &'a [u8], ProtectedStateError);

#[test]
fn offered_limits_and_order_refuse_before_preparation_or_entry() -> TestResult {
    let (parent, _, mut writer) = fresh()?;
    token(&mut writer, 1)?;
    let original = writer.reader().capture_protected_state()?;
    let before = counts(&writer);
    let long_key = vec![0; PROTECTED_STATE_RECORD_LIMITS.max_key_bytes + 1];
    let long_value = vec![0; PROTECTED_STATE_RECORD_LIMITS.max_value_bytes + 1];
    let long_fence = vec![0; 257];
    let many_rows = vec![(&b"k"[..], &b"v"[..]); 65_537];
    let key_rows: &[(&[u8], &[u8])] = &[(long_key.as_slice(), b"v")];
    let value_rows: &[(&[u8], &[u8])] = &[(b"k", long_value.as_slice())];
    let cases: Vec<OfferedCase<'_>> = vec![
        (&[(b"", b"v")], b"f", ProtectedStateError::InvalidInput),
        (
            &[(b"k", b"v"), (b"k", b"v")],
            b"f",
            ProtectedStateError::InvalidInput,
        ),
        (
            &[(b"z", b"v"), (b"a", b"v")],
            b"f",
            ProtectedStateError::InvalidInput,
        ),
        (key_rows, b"f", ProtectedStateError::LimitExceeded),
        (value_rows, b"f", ProtectedStateError::LimitExceeded),
        (&[], &long_fence, ProtectedStateError::LimitExceeded),
        (&[], b"", ProtectedStateError::InvalidInput),
        (&many_rows, b"f", ProtectedStateError::LimitExceeded),
    ];
    for (rows, fence, expected) in cases {
        let refused =
            ProtectedStatePublication::new(rows, SnapshotCatalogRecord::new(b"", b"")?, fence);
        assert_eq!(refused.err(), Some(expected));
    }
    assert!(SnapshotCatalogRecord::new(&vec![0; 8193], b"").is_err());
    assert_eq!(counts(&writer), before);
    let after = writer.reader().capture_protected_state()?;
    assert_eq!(logical(&after), logical(&original));
    drop(writer);
    drop(parent);
    Ok(())
}

#[test]
fn unchanged_fence_refuses_without_native_batch_or_poison() -> TestResult {
    let (parent, _, mut writer) = fresh()?;
    token(&mut writer, 1)?;
    let before = counts(&writer);
    assert_eq!(
        token(&mut writer, 1),
        Err(ProtectedStateError::UnchangedFence)
    );
    let after = counts(&writer);
    assert_eq!(
        (
            after.preparations,
            after.entries,
            after.native,
            after.copies
        ),
        (
            before.preparations,
            before.entries,
            before.native,
            before.copies
        )
    );
    assert!(!writer.inner.poisoned.load(Ordering::SeqCst));
    assert_token(&writer.reader().capture_protected_state()?, 1);
    token(&mut writer, 2)?;
    assert_token(&writer.reader().capture_protected_state()?, 2);
    // Current closure must be checked before equal-fence refusal.
    damage(&writer, Damage::PartialArtifact)?;
    let before_invalid = counts(&writer);
    assert_eq!(
        token(&mut writer, 2),
        Err(ProtectedStateError::InvalidState)
    );
    assert!(writer.inner.poisoned.load(Ordering::SeqCst));
    let after_invalid = counts(&writer);
    assert_eq!(
        (
            after_invalid.preparations,
            after_invalid.entries,
            after_invalid.native
        ),
        (
            before_invalid.preparations,
            before_invalid.entries,
            before_invalid.native
        )
    );
    drop(writer);
    drop(parent);
    Ok(())
}

#[test]
fn preparation_allocation_refusal_keeps_previous_generation_readable() -> TestResult {
    let (parent, _, mut writer) = fresh()?;
    token(&mut writer, 1)?;
    let retained = writer.reader().capture_protected_state()?;
    let before = counts(&writer);
    fault(&writer, Fault::Prepare);
    assert_eq!(token(&mut writer, 2), Err(ProtectedStateError::Allocation));
    assert_eq!(
        (counts(&writer).entries, counts(&writer).native),
        (before.entries, before.native)
    );
    assert!(!writer.inner.poisoned.load(Ordering::SeqCst));
    fault(&writer, Fault::CaptureAllocation);
    assert_eq!(
        writer.reader().capture_protected_state().err(),
        Some(ProtectedStateError::Allocation)
    );
    assert!(!writer.inner.poisoned.load(Ordering::SeqCst));
    fault(&writer, Fault::None);
    assert_token(&retained, 1);
    assert_token(&writer.reader().capture_protected_state()?, 1);
    token(&mut writer, 2)?;
    drop(writer);
    drop(parent);
    Ok(())
}

fn unknown_case(selected: Fault, native_delta: usize) -> TestResult {
    let (parent, _, mut writer) = fresh()?;
    token(&mut writer, 1)?;
    let reader = writer.reader();
    let clone = reader.clone();
    let retained = reader.capture_protected_state()?;
    let before = counts(&writer);
    fault(&writer, selected);
    let outcome = token(&mut writer, 2);
    let after = counts(&writer);
    assert_eq!(outcome, Err(ProtectedStateError::PublishUnknown));
    assert!(writer.inner.poisoned.load(Ordering::SeqCst));
    assert!(writer.inner.gate.try_read().is_ok());
    assert_eq!(after.entries, before.entries + 1);
    assert_eq!(after.native, before.native + native_delta);
    assert_eq!(after.views, before.views + 2);
    assert_eq!(
        reader.capture_protected_state().err(),
        Some(ProtectedStateError::Poisoned)
    );
    assert_eq!(
        clone.capture_protected_state().err(),
        Some(ProtectedStateError::Poisoned)
    );
    assert_eq!(token(&mut writer, 3), Err(ProtectedStateError::Poisoned));
    assert_eq!(counts(&writer), after);
    assert_token(&retained, 1);
    drop(clone);
    drop(reader);
    drop(writer);
    drop(parent);
    Ok(())
}

#[test]
fn entered_before_backend_error_is_unknown_and_shared_poisoned() -> TestResult {
    unknown_case(Fault::Before, 0)
}

#[test]
fn entered_after_sync_error_is_unknown_and_shared_poisoned() -> TestResult {
    unknown_case(Fault::After, 1)
}

#[test]
fn entered_unwind_before_and_after_backend_marks_before_unlock() -> TestResult {
    for (selected, native_delta) in [(Fault::PanicBefore, 0), (Fault::PanicAfter, 1)] {
        let (parent, _, mut writer) = fresh()?;
        token(&mut writer, 1)?;
        let reader = writer.reader();
        let before = counts(&writer);
        let retained = reader.capture_protected_state()?;
        fault(&writer, selected);
        let original_panic =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| token(&mut writer, 2)));
        // The original payload is still held; the synchronous guard has unwound.
        let after = counts(&writer);
        assert!(original_panic.is_err());
        assert!(writer.inner.poisoned.load(Ordering::SeqCst));
        assert!(writer.inner.gate.is_poisoned());
        assert!(!matches!(
            writer.inner.gate.try_write(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        assert_eq!(after.entries, before.entries + 1);
        assert_eq!(after.native, before.native + native_delta);
        assert_eq!(
            reader.capture_protected_state().err(),
            Some(ProtectedStateError::Poisoned)
        );
        assert_eq!(token(&mut writer, 3), Err(ProtectedStateError::Poisoned));
        assert_eq!(counts(&writer), after);
        assert_token(&retained, 1);
        drop(original_panic);
        drop(reader);
        drop(writer);
        drop(parent);
    }
    Ok(())
}

#[test]
fn current_backend_error_terminal_before_copy_or_commit() -> TestResult {
    for capture in [false, true] {
        let (parent, _, mut writer) = fresh()?;
        token(&mut writer, 1)?;
        let before = counts(&writer);
        fault(&writer, Fault::CurrentBackend);
        let outcome = if capture {
            writer.reader().capture_protected_state().map(|_| ())
        } else {
            token(&mut writer, 2)
        };
        assert_eq!(outcome, Err(ProtectedStateError::Poisoned));
        let after = counts(&writer);
        assert_eq!(
            (
                after.copies,
                after.preparations,
                after.entries,
                after.native
            ),
            (
                before.copies,
                before.preparations,
                before.entries,
                before.native
            )
        );
        assert!(writer.inner.poisoned.load(Ordering::SeqCst));
        assert_eq!(
            writer.reader().capture_protected_state().err(),
            Some(ProtectedStateError::Poisoned)
        );
        assert_eq!(counts(&writer), after);
        drop(writer);
        drop(parent);
    }
    Ok(())
}

#[test]
fn invalid_current_layout_or_payload_terminal_before_copy_or_commit() -> TestResult {
    let cases = [
        Damage::WrongFormat,
        Damage::WrongProfile,
        Damage::BadInit,
        Damage::Unknown,
        Damage::PartialMetadata,
        Damage::PartialArtifact,
        Damage::PartialFence,
        Damage::Orphan,
        Damage::LongKey,
        Damage::LongValue,
        Damage::LargeArtifact,
    ];
    for selected in cases {
        let (parent, _, mut writer) = fresh()?;
        token(&mut writer, 1)?;
        damage(&writer, selected)?;
        let before = counts(&writer);
        let expected = if matches!(
            selected,
            Damage::LongKey | Damage::LongValue | Damage::LargeArtifact
        ) {
            ProtectedStateError::LimitExceeded
        } else {
            ProtectedStateError::InvalidState
        };
        let outcome = writer.reader().capture_protected_state();
        assert_eq!(outcome.err(), Some(expected));
        let after = counts(&writer);
        assert_eq!(
            (
                after.copies,
                after.preparations,
                after.entries,
                after.native
            ),
            (
                before.copies,
                before.preparations,
                before.entries,
                before.native
            )
        );
        assert!(writer.inner.poisoned.load(Ordering::SeqCst));
        assert_eq!(token(&mut writer, 2), Err(ProtectedStateError::Poisoned));
        assert_eq!(counts(&writer), after);
        drop(writer);
        drop(parent);
    }
    Ok(())
}
