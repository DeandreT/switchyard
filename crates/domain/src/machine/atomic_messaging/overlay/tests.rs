use super::*;
use storage::MemoryStore;

fn overflow(overlay: &AtomicOverlay<MemoryStore>, error: StorageError, limit: Limit) {
    assert_eq!(overlay.map_error(error.into()), limit.exceeded());
    assert_eq!(overlay.finish(), Err(limit.exceeded()));
}

#[test]
fn point_read_operations_include_repeated_misses() -> Result<(), StorageError> {
    let overlay = AtomicOverlay::new(MemoryStore::default());
    for _ in 0..Limit::ReadOperations.maximum() {
        assert_eq!(overlay.get(b"x")?, None);
    }
    let error = overlay
        .get(b"x")
        .expect_err("one more point read exceeds the bound");
    overflow(&overlay, error, Limit::ReadOperations);
    Ok(())
}

#[test]
fn point_read_key_bytes_include_repeated_keys() -> Result<(), StorageError> {
    let overlay = AtomicOverlay::new(MemoryStore::default());
    let key = vec![0; Limit::ReadKeyBytes.maximum()];
    assert_eq!(overlay.get(&key)?, None);
    let error = overlay
        .get(b"x")
        .expect_err("one more key byte exceeds the bound");
    overflow(&overlay, error, Limit::ReadKeyBytes);
    Ok(())
}

#[test]
fn cached_returned_values_charge_before_their_next_clone() -> Result<(), BrokerError> {
    let overlay = AtomicOverlay::new(MemoryStore::default());
    let chunk = 16 * 1024;
    overlay.stage(WriteBatch::default().put(b"x".to_vec(), vec![0; chunk]))?;
    for _ in 0..Limit::ReadValueBytes.maximum() / chunk {
        assert_eq!(overlay.get(b"x")?.map(|value| value.len()), Some(chunk));
    }
    let error = overlay
        .get(b"x")
        .expect_err("repeated returned bytes exceed the bound");
    overflow(&overlay, error, Limit::ReadValueBytes);
    Ok(())
}

#[test]
fn unique_mutation_keys_count_deletes_and_ignore_replacements() -> Result<(), BrokerError> {
    let overlay = AtomicOverlay::new(MemoryStore::default());
    let mut batch = WriteBatch::default();
    for index in 0..Limit::MutationKeys.maximum() {
        batch.push_delete(index.to_be_bytes().to_vec());
    }
    overlay.stage(batch)?;
    overlay.stage(WriteBatch::default().put(0_usize.to_be_bytes().to_vec(), vec![1]))?;
    assert_eq!(
        overlay.stage(WriteBatch::default().delete(b"extra".to_vec())),
        Err(Limit::MutationKeys.exceeded())
    );
    Ok(())
}

#[test]
fn unique_mutation_key_bytes_accept_exactly_the_limit() -> Result<(), BrokerError> {
    let overlay = AtomicOverlay::new(MemoryStore::default());
    let key = vec![0; Limit::MutationKeyBytes.maximum()];
    overlay.stage(WriteBatch::default().delete(key.clone()))?;
    overlay.stage(WriteBatch::default().put(key, Vec::new()))?;
    assert_eq!(
        overlay.stage(WriteBatch::default().delete(vec![1])),
        Err(Limit::MutationKeyBytes.exceeded())
    );
    Ok(())
}

#[test]
fn generated_put_bytes_count_values_that_are_later_overwritten() -> Result<(), BrokerError> {
    let overlay = AtomicOverlay::new(MemoryStore::default());
    let chunk = 16 * 1024;
    for _ in 0..Limit::MutationValueBytes.maximum() / chunk {
        overlay.stage(WriteBatch::default().put(b"x".to_vec(), vec![0; chunk]))?;
    }
    assert_eq!(overlay.lock()?.mutations.len(), 1);
    assert_eq!(
        overlay.stage(WriteBatch::default().put(b"x".to_vec(), vec![1])),
        Err(Limit::MutationValueBytes.exceeded())
    );
    Ok(())
}

#[test]
fn reads_see_puts_and_deletes_and_final_batch_is_sorted() -> Result<(), BrokerError> {
    let base = MemoryStore::default();
    base.apply(WriteBatch::default().put(b"b".to_vec(), vec![1]))?;
    let overlay = AtomicOverlay::new(base.clone());
    overlay.stage(
        WriteBatch::default()
            .put(b"z".to_vec(), vec![2])
            .delete(b"b".to_vec()),
    )?;
    assert_eq!(overlay.get(b"z")?, Some(vec![2]));
    assert_eq!(overlay.get(b"b")?, None);
    overlay.stage(
        WriteBatch::default()
            .put(b"a".to_vec(), vec![3])
            .put(b"z".to_vec(), vec![4]),
    )?;
    assert_eq!(overlay.get(b"z")?, Some(vec![4]));
    let batch = overlay.finish()?;
    let keys: Vec<_> = batch
        .mutations()
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key.as_slice(),
        })
        .collect();
    assert_eq!(
        keys,
        vec![b"a".as_slice(), b"b".as_slice(), b"z".as_slice()]
    );
    assert_eq!(base.get(b"b")?, Some(vec![1]));
    assert_eq!(base.get(b"z")?, None);
    Ok(())
}

#[test]
fn backing_writes_scans_and_snapshots_are_latched_internal_refusals() {
    for operation in 0..3 {
        let base = MemoryStore::default();
        let overlay = AtomicOverlay::new(base.clone());
        let error = match operation {
            0 => overlay
                .apply(WriteBatch::default())
                .expect_err("trait apply is forbidden"),
            1 => overlay.snapshot().expect_err("snapshot is forbidden"),
            _ => overlay.scan_prefix(b"x", 1).expect_err("scan is forbidden"),
        };
        assert_eq!(
            overlay.map_error(error.into()),
            BrokerError::InvalidAtomicMessagingCommand
        );
        assert_eq!(
            overlay.finish(),
            Err(BrokerError::InvalidAtomicMessagingCommand)
        );
        assert!(
            base.snapshot()
                .expect("memory snapshot")
                .entries()
                .is_empty()
        );
    }
}

#[derive(Clone)]
struct FailingGet;

impl StateStore for FailingGet {
    fn get(&self, _: &[u8]) -> Result<Option<Value>, StorageError> {
        Err(StorageError::LockPoisoned)
    }
    fn apply(&self, _: WriteBatch) -> Result<(), StorageError> {
        Err(StorageError::LockPoisoned)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        Err(StorageError::LockPoisoned)
    }
    fn scan_from(&self, _: &[u8], _: &[u8], _: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        Err(StorageError::LockPoisoned)
    }
}

#[test]
fn genuine_backing_errors_are_not_reclassified_as_limits() {
    let overlay = AtomicOverlay::new(FailingGet);
    let error = overlay.get(b"x").expect_err("backing read fails");
    assert_eq!(
        overlay.map_error(error.into()),
        BrokerError::Storage(StorageError::LockPoisoned)
    );
}
