use std::sync::mpsc;

use tempfile::TempDir;

use super::*;
use crate::CommittedStore;

fn exact_limits() -> ReadLimits {
    ReadLimits {
        max_rows: 3,
        max_key_bytes: 3,
        max_value_bytes: 5,
        max_total_bytes: 13,
    }
}

fn seed_batch() -> WriteBatch {
    WriteBatch::default()
        .put(b"z".to_vec(), b"abc".to_vec())
        .put(b"a".to_vec(), Vec::new())
        .put(b"mid".to_vec(), b"12345".to_vec())
}

fn populated() -> Result<(TempDir, FjallStore), StorageError> {
    let directory = TempDir::new().expect("a temporary directory");
    let store = FjallStore::open(directory.path())?;
    store.apply(seed_batch())?;
    Ok((directory, store))
}

#[test]
fn exact_limits_return_the_complete_ascending_snapshot() -> Result<(), StorageError> {
    let (_directory, store) = populated()?;
    let bounded = store.snapshot_bounded(exact_limits())?;
    assert_eq!(bounded, store.snapshot()?);
    assert_eq!(
        bounded.entries(),
        &[
            (b"a".to_vec(), Vec::new()),
            (b"mid".to_vec(), b"12345".to_vec()),
            (b"z".to_vec(), b"abc".to_vec()),
        ],
    );
    Ok(())
}

#[test]
fn each_exceeded_limit_refuses_without_changing_records() -> Result<(), StorageError> {
    let (_directory, store) = populated()?;
    let before = store.snapshot()?;
    let exact = exact_limits();
    for limits in [
        ReadLimits {
            max_rows: 2,
            ..exact
        },
        ReadLimits {
            max_key_bytes: 2,
            ..exact
        },
        ReadLimits {
            max_value_bytes: 4,
            ..exact
        },
        ReadLimits {
            max_total_bytes: 12,
            ..exact
        },
    ] {
        assert_eq!(
            store.snapshot_bounded(limits),
            Err(StorageError::ReadLimitExceeded),
        );
        assert_eq!(store.snapshot()?, before);
    }
    Ok(())
}

#[test]
fn zero_limits_accept_only_the_empty_record_set() -> Result<(), StorageError> {
    let directory = TempDir::new().expect("a temporary directory");
    let store = FjallStore::open(directory.path())?;
    let zero = ReadLimits {
        max_rows: 0,
        max_key_bytes: 0,
        max_value_bytes: 0,
        max_total_bytes: 0,
    };
    assert_eq!(store.snapshot_bounded(zero)?, StoreSnapshot::default());
    store.apply(WriteBatch::default().put(b"row".to_vec(), Vec::new()))?;
    assert_eq!(
        store.snapshot_bounded(zero),
        Err(StorageError::ReadLimitExceeded),
    );
    Ok(())
}

#[test]
fn zero_value_budget_allows_empty_index_values() -> Result<(), StorageError> {
    let directory = TempDir::new().expect("a temporary directory");
    let store = FjallStore::open(directory.path())?;
    store.apply(WriteBatch::default().put(b"index".to_vec(), Vec::new()))?;
    let limits = ReadLimits {
        max_rows: 1,
        max_key_bytes: 5,
        max_value_bytes: 0,
        max_total_bytes: 5,
    };
    assert_eq!(store.snapshot_bounded(limits)?, store.snapshot()?);
    Ok(())
}

#[test]
fn failed_read_releases_its_view_and_reopen_keeps_exact_records() -> Result<(), StorageError> {
    let (directory, store) = populated()?;
    let before = store.snapshot()?;
    assert_eq!(
        store.snapshot_bounded(ReadLimits {
            max_total_bytes: 12,
            ..exact_limits()
        }),
        Err(StorageError::ReadLimitExceeded),
    );
    drop(store);
    let reopened = FjallStore::open(directory.path())?;
    assert_eq!(reopened.snapshot_bounded(exact_limits())?, before);
    Ok(())
}

#[test]
fn committed_reader_delegates_bounds_without_gaining_write_authority() -> Result<(), StorageError> {
    let directory = TempDir::new().expect("a temporary directory");
    let mut writer = FjallReplicaStore::open(directory.path())?;
    writer.commit(seed_batch())?;
    let reader = writer.reader();
    let before = reader.snapshot_bounded(exact_limits())?;
    assert_eq!(before, reader.snapshot()?);
    assert_eq!(
        reader.apply(WriteBatch::default().put(b"other".to_vec(), b"data".to_vec())),
        Err(StorageError::ReplicaWriteRequired),
    );
    assert_eq!(
        reader.snapshot_bounded(ReadLimits {
            max_rows: 2,
            ..exact_limits()
        }),
        Err(StorageError::ReadLimitExceeded),
    );
    assert_eq!(reader.snapshot_bounded(exact_limits())?, before);
    drop(reader);
    drop(writer);
    let reopened = FjallReplicaStore::open(directory.path())?;
    assert_eq!(reopened.reader().snapshot_bounded(exact_limits())?, before);
    Ok(())
}

#[test]
fn concurrent_commit_cannot_change_the_pinned_size_and_value_reads() -> Result<(), StorageError> {
    let (_directory, store) = populated()?;
    let before = store.snapshot()?;
    let pinned = store.database.snapshot();
    let observed = std::thread::scope(|scope| {
        let concurrent = store.clone();
        let (committed, completion) = mpsc::channel();
        let worker = scope.spawn(move || {
            let result = concurrent.apply(
                WriteBatch::default()
                    .put(b"a".to_vec(), b"larger-than-the-old-size-budget".to_vec())
                    .delete(b"mid".to_vec())
                    .put(b"new".to_vec(), b"new".to_vec()),
            );
            committed.send(result).expect("the read thread still waits");
        });
        completion.recv().expect("the mutation worker completed")?;
        let result = read_bounded_snapshot(&pinned, &store.records, exact_limits());
        worker.join().expect("the mutation worker did not panic");
        result
    })?;
    assert_eq!(observed, before);
    assert_ne!(store.snapshot()?, before);
    assert_eq!(
        store.snapshot_bounded(exact_limits()),
        Err(StorageError::ReadLimitExceeded),
    );
    Ok(())
}
