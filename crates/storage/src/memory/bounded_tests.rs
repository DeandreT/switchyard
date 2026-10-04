use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn zero() -> ReadLimits {
    ReadLimits {
        max_rows: 0,
        max_key_bytes: 0,
        max_value_bytes: 0,
        max_total_bytes: 0,
    }
}

fn exact() -> ReadLimits {
    ReadLimits {
        max_rows: 3,
        max_key_bytes: 1,
        max_value_bytes: 2,
        max_total_bytes: 5,
    }
}

fn populated() -> Result<MemoryStore, StorageError> {
    let store = MemoryStore::default();
    store.apply(
        WriteBatch::default()
            .put(b"b".to_vec(), b"22".to_vec())
            .put(Vec::new(), Vec::new())
            .put(b"a".to_vec(), b"1".to_vec()),
    )?;
    Ok(store)
}

#[test]
fn empty_and_zero_byte_records_obey_zero_limits() -> TestResult {
    let store = MemoryStore::default();
    assert!(store.snapshot_bounded(zero())?.entries().is_empty());
    store.apply(WriteBatch::default().put(Vec::new(), Vec::new()))?;
    assert_eq!(
        store.snapshot_bounded(zero()),
        Err(StorageError::ReadLimitExceeded)
    );
    let snapshot = store.snapshot_bounded(ReadLimits {
        max_rows: 1,
        ..zero()
    })?;
    assert_eq!(snapshot.entries(), &[(Vec::new(), Vec::new())]);
    Ok(())
}

#[test]
fn exact_limits_return_the_whole_sorted_view() -> TestResult {
    let store = populated()?;
    let bounded = store.snapshot_bounded(exact())?;
    assert_eq!(bounded, store.snapshot()?);
    assert_eq!(
        bounded.entries(),
        &[
            (Vec::new(), Vec::new()),
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"22".to_vec()),
        ]
    );
    Ok(())
}

#[test]
fn each_exceeded_limit_refuses_the_complete_read_without_mutation() -> TestResult {
    let store = populated()?;
    let before = store.snapshot()?;
    for limits in [
        ReadLimits {
            max_rows: 2,
            ..exact()
        },
        ReadLimits {
            max_key_bytes: 0,
            ..exact()
        },
        ReadLimits {
            max_value_bytes: 1,
            ..exact()
        },
        ReadLimits {
            max_total_bytes: 4,
            ..exact()
        },
    ] {
        assert_eq!(
            store.snapshot_bounded(limits),
            Err(StorageError::ReadLimitExceeded)
        );
        assert_eq!(store.snapshot()?, before);
    }
    assert_eq!(store.snapshot_bounded(exact())?, before);
    Ok(())
}

#[test]
fn completed_snapshot_is_owned_and_unchanged_by_later_batches() -> TestResult {
    let store = populated()?;
    let snapshot = store.snapshot_bounded(exact())?;
    store.apply(
        WriteBatch::default()
            .delete(Vec::new())
            .delete(b"a".to_vec())
            .put(b"b".to_vec(), b"new".to_vec()),
    )?;
    assert_eq!(snapshot.entries().len(), 3);
    assert_eq!(snapshot.entries()[2], (b"b".to_vec(), b"22".to_vec()));
    assert_ne!(snapshot, store.snapshot()?);
    Ok(())
}

#[test]
fn replica_reader_delegates_bounds_but_keeps_write_refusal() -> TestResult {
    let mut writer = MemoryReplicaStore::new();
    let reader = writer.reader();
    assert!(reader.snapshot_bounded(zero())?.entries().is_empty());
    writer.commit(WriteBatch::default().put(b"a".to_vec(), b"1".to_vec()))?;
    let limits = ReadLimits {
        max_rows: 1,
        max_key_bytes: 1,
        max_value_bytes: 1,
        max_total_bytes: 2,
    };
    let before = reader.snapshot_bounded(limits)?;
    assert_eq!(
        reader.snapshot_bounded(zero()),
        Err(StorageError::ReadLimitExceeded)
    );
    assert_eq!(
        reader.apply(WriteBatch::default()),
        Err(StorageError::ReplicaWriteRequired)
    );
    assert_eq!(
        reader.apply(WriteBatch::default().delete(b"a".to_vec())),
        Err(StorageError::ReplicaWriteRequired)
    );
    assert_eq!(reader.snapshot_bounded(limits)?, before);
    assert!(writer.is_initialized()?);
    Ok(())
}

#[test]
fn poisoned_read_lock_remains_a_storage_error() {
    let store = MemoryStore::default();
    let clone = store.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = clone.entries.write().expect("fresh lock");
            panic!("controlled read-lock poisoning");
        })
        .join()
        .is_err()
    );
    assert_eq!(
        store.snapshot_bounded(zero()),
        Err(StorageError::LockPoisoned)
    );
}

#[test]
fn oversized_later_value_cannot_be_returned_as_a_partial_success() -> TestResult {
    let store = MemoryStore::default();
    store.apply(
        WriteBatch::default()
            .put(b"a".to_vec(), b"ok".to_vec())
            .put(b"z".to_vec(), b"private-record-payload".to_vec()),
    )?;
    let before = store.snapshot()?;
    let error = store
        .snapshot_bounded(ReadLimits {
            max_rows: 2,
            max_key_bytes: 1,
            max_value_bytes: 2,
            max_total_bytes: 100,
        })
        .expect_err("the complete view must refuse");
    assert_eq!(error, StorageError::ReadLimitExceeded);
    assert_eq!(error.to_string(), "storage read limit exceeded");
    assert_eq!(store.snapshot()?, before);
    Ok(())
}
