use super::*;
use storage::MemoryStore;

fn overflow<S: StateStore>(overlay: &AtomicOverlay<S>, error: StorageError, limit: Limit) {
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

#[derive(Clone, Default)]
struct ScanStore {
    memory: MemoryStore,
    scans: Arc<std::sync::atomic::AtomicUsize>,
}

impl StateStore for ScanStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.memory.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.memory.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.memory.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.scans.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.memory.scan_from(prefix, start, limit)
    }
}

fn scope() -> (NamespaceName, EntityPath, Key) {
    let namespace = NamespaceName::new("test").expect("namespace");
    let owner = EntityPath::new("queue").expect("owner");
    let prefix = keys::subscription_topic_mode_prefix(&namespace, &owner);
    (namespace, owner, prefix)
}

fn scan_count(store: &ScanStore) -> usize {
    store.scans.load(std::sync::atomic::Ordering::SeqCst)
}

#[test]
fn scoped_metadata_probe_requires_exact_binding_prefix_start_and_limit() -> Result<(), BrokerError>
{
    let (namespace, owner, prefix) = scope();
    let base = ScanStore::default();
    let overlay = AtomicOverlay::for_queue(base.clone(), &namespace, &owner);
    assert!(overlay.scan_from(&prefix, &prefix, 1)?.is_empty());
    assert_eq!(scan_count(&base), 1);
    assert_eq!(overlay.lock()?.reads, 1);
    assert_eq!(overlay.lock()?.read_key_bytes, 2 * prefix.len());
    for variant in 0..5 {
        let base = ScanStore::default();
        let overlay = if variant == 4 {
            AtomicOverlay::new(base.clone())
        } else {
            AtomicOverlay::for_queue(base.clone(), &namespace, &owner)
        };
        let sibling =
            keys::subscription_topic_mode_prefix(&namespace, &EntityPath::new("queue-other")?);
        let mut later = prefix.clone();
        later.push(0);
        let (query, start, limit) = match variant {
            0 => (sibling.as_slice(), sibling.as_slice(), 1),
            1 => (prefix.as_slice(), later.as_slice(), 1),
            2 => (prefix.as_slice(), prefix.as_slice(), 0),
            3 => (prefix.as_slice(), prefix.as_slice(), 2),
            _ => (prefix.as_slice(), prefix.as_slice(), 1),
        };
        let error = overlay
            .scan_from(query, start, limit)
            .expect_err("only exact scoped probe");
        assert_eq!(
            overlay.map_error(error.into()),
            BrokerError::InvalidAtomicMessagingCommand
        );
        assert_eq!(
            overlay.finish(),
            Err(BrokerError::InvalidAtomicMessagingCommand)
        );
        assert_eq!(scan_count(&base), 0);
    }
    Ok(())
}

#[test]
fn scoped_metadata_mutations_are_latched_without_changing_backing_rows() -> Result<(), BrokerError>
{
    let (namespace, owner, prefix) = scope();
    let mut key = prefix.clone();
    key.extend_from_slice(b"ghost\0");
    for put in [false, true] {
        let base = ScanStore::default();
        base.apply(WriteBatch::default().put(key.clone(), vec![7]))?;
        let before = base.snapshot()?;
        let overlay = AtomicOverlay::for_queue(base.clone(), &namespace, &owner);
        overlay.stage(WriteBatch::default().put(b"ordinary".to_vec(), vec![1]))?;
        let batch = if put {
            WriteBatch::default().put(key.clone(), vec![9])
        } else {
            WriteBatch::default().delete(key.clone())
        };
        assert_eq!(
            overlay.stage(batch),
            Err(BrokerError::InvalidAtomicMessagingCommand)
        );
        assert_eq!(
            overlay.finish(),
            Err(BrokerError::InvalidAtomicMessagingCommand)
        );
        assert!(overlay.scan_from(&prefix, &prefix, 1).is_err());
        assert_eq!(scan_count(&base), 0);
        assert_eq!(base.snapshot()?, before);
    }
    Ok(())
}

#[test]
fn scoped_probe_shares_read_operation_and_requested_key_budgets() -> Result<(), BrokerError> {
    let (namespace, owner, prefix) = scope();
    let base = ScanStore::default();
    let overlay = AtomicOverlay::for_queue(base.clone(), &namespace, &owner);
    overlay.lock()?.reads = Limit::ReadOperations.maximum() - 1;
    assert!(overlay.scan_from(&prefix, &prefix, 1)?.is_empty());
    assert_eq!(overlay.lock()?.reads, Limit::ReadOperations.maximum());
    let error = overlay
        .get(b"point")
        .expect_err("probe consumed the final read");
    overflow(&overlay, error, Limit::ReadOperations);
    assert_eq!(scan_count(&base), 1);

    let base = ScanStore::default();
    let overlay = AtomicOverlay::for_queue(base.clone(), &namespace, &owner);
    overlay.lock()?.reads = Limit::ReadOperations.maximum() - 1;
    assert_eq!(overlay.get(b"point")?, None);
    let error = overlay
        .scan_from(&prefix, &prefix, 1)
        .expect_err("point consumed final read");
    overflow(&overlay, error, Limit::ReadOperations);
    assert_eq!(scan_count(&base), 0);

    for remaining in [2 * prefix.len(), 2 * prefix.len() - 1] {
        let base = ScanStore::default();
        let overlay = AtomicOverlay::for_queue(base.clone(), &namespace, &owner);
        overlay.lock()?.read_key_bytes = Limit::ReadKeyBytes.maximum() - remaining;
        if remaining == 2 * prefix.len() {
            assert!(overlay.scan_from(&prefix, &prefix, 1)?.is_empty());
            assert_eq!(
                overlay.lock()?.read_key_bytes,
                Limit::ReadKeyBytes.maximum()
            );
        }
        let error = overlay
            .scan_from(&prefix, &prefix, 1)
            .expect_err("query bytes exceed cap");
        overflow(&overlay, error, Limit::ReadKeyBytes);
        assert_eq!(
            scan_count(&base),
            usize::from(remaining == 2 * prefix.len())
        );
    }
    Ok(())
}

#[test]
fn scoped_probe_charges_returned_keys_and_values_before_further_io() -> Result<(), BrokerError> {
    let (namespace, owner, prefix) = scope();
    let mut key = prefix.clone();
    key.extend_from_slice(b"ghost\0");
    let value = vec![1, 2, 3, 4];
    for exact in [false, true] {
        let base = ScanStore::default();
        base.apply(WriteBatch::default().put(key.clone(), value.clone()))?;
        let overlay = AtomicOverlay::for_queue(base.clone(), &namespace, &owner);
        let bytes = 2 * prefix.len() + key.len();
        overlay.lock()?.read_key_bytes =
            Limit::ReadKeyBytes.maximum() - bytes + usize::from(!exact);
        if exact {
            assert_eq!(
                overlay.scan_from(&prefix, &prefix, 1)?,
                vec![(key.clone(), value.clone())]
            );
            assert_eq!(
                overlay.lock()?.read_key_bytes,
                Limit::ReadKeyBytes.maximum()
            );
            let error = overlay
                .get(b"point")
                .expect_err("returned key consumed final bytes");
            overflow(&overlay, error, Limit::ReadKeyBytes);
        } else {
            let error = overlay
                .scan_from(&prefix, &prefix, 1)
                .expect_err("returned key over cap");
            overflow(&overlay, error, Limit::ReadKeyBytes);
        }
        assert!(overlay.scan_from(&prefix, &prefix, 1).is_err());
        assert_eq!(scan_count(&base), 1);
    }
    for exact in [false, true] {
        let base = ScanStore::default();
        base.apply(WriteBatch::default().put(key.clone(), value.clone()))?;
        let overlay = AtomicOverlay::for_queue(base.clone(), &namespace, &owner);
        overlay.lock()?.read_value_bytes =
            Limit::ReadValueBytes.maximum() - value.len() + usize::from(!exact);
        if exact {
            assert_eq!(
                overlay.scan_from(&prefix, &prefix, 1)?,
                vec![(key.clone(), value.clone())]
            );
            assert_eq!(
                overlay.lock()?.read_value_bytes,
                Limit::ReadValueBytes.maximum()
            );
            let error = overlay
                .get(&key)
                .expect_err("point return shares returned-value cap");
            overflow(&overlay, error, Limit::ReadValueBytes);
        } else {
            let error = overlay
                .scan_from(&prefix, &prefix, 1)
                .expect_err("returned value over cap");
            overflow(&overlay, error, Limit::ReadValueBytes);
        }
        assert!(overlay.scan_from(&prefix, &prefix, 1).is_err());
        assert_eq!(scan_count(&base), 1);
    }
    Ok(())
}

#[test]
fn scoped_metadata_probe_preserves_genuine_backend_errors() {
    let (namespace, owner, prefix) = scope();
    let overlay = AtomicOverlay::for_queue(FailingGet, &namespace, &owner);
    let error = overlay
        .scan_from(&prefix, &prefix, 1)
        .expect_err("backend scan fails");
    assert_eq!(
        overlay.map_error(error.into()),
        BrokerError::Storage(StorageError::LockPoisoned)
    );
    assert!(
        overlay
            .finish()
            .expect("backend error is not a limit latch")
            .is_empty()
    );
}

#[test]
fn atomic_read_limit_labels_include_metadata_operations() {
    assert_eq!(Limit::ReadOperations.to_string(), "read operations");
    assert_eq!(Limit::ReadKeyBytes.to_string(), "read key bytes");
    assert_eq!(Limit::ReadValueBytes.to_string(), "read value bytes");
}
