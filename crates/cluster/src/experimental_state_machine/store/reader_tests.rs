use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use domain::CommittedStreamId;
use storage::{
    CommittedStore, MemoryReplicaStore, StateStore, StorageError, StoreSnapshot, WriteBatch,
};

use super::*;

fn stream() -> CommittedStreamId {
    CommittedStreamId::new([9; 16]).unwrap()
}

#[tokio::test]
async fn healthy_checkpoint_view_is_readonly_and_does_not_keep_retired_owner_open() {
    let writer = MemoryReplicaStore::new();
    let reader = writer.reader();
    let store = ExperimentalStateMachine::create(writer, stream()).unwrap();
    let baseline = reader.snapshot().unwrap();
    let view = store.checkpoint_reader();
    assert_eq!(view.checkpoint().await.unwrap().stream(), stream());
    assert_eq!(reader.snapshot().unwrap(), baseline);
    assert_eq!(
        store.workload().unwrap(),
        StateMachineWorkload {
            accepted_jobs: 0,
            encoded_bytes: 0
        }
    );
    store.shutdown().await.unwrap();
    assert_eq!(view.checkpoint().await, Err(StateMachineError::Closed));
}

struct ControlledWriter {
    inner: MemoryReplicaStore,
    corrupt: Arc<AtomicBool>,
}
#[derive(Clone)]
struct ControlledReader {
    inner: <MemoryReplicaStore as CommittedStore>::Reader,
    corrupt: Arc<AtomicBool>,
}

impl CommittedStore for ControlledWriter {
    type Reader = ControlledReader;
    fn reader(&self) -> Self::Reader {
        ControlledReader {
            inner: self.inner.reader(),
            corrupt: Arc::clone(&self.corrupt),
        }
    }
    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.inner.commit(batch)
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.inner.is_initialized()
    }
}

impl StateStore for ControlledReader {
    fn get(&self, key: &[u8]) -> Result<Option<storage::Value>, StorageError> {
        if key == [0x12] && self.corrupt.load(Ordering::SeqCst) {
            return Ok(Some(b"private corrupt checkpoint".to_vec()));
        }
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(storage::Key, storage::Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.inner.apply(batch)
    }
}

#[tokio::test]
async fn healthy_view_poisoning_cannot_be_laundered_as_a_diagnostic_checkpoint() {
    let corrupt = Arc::new(AtomicBool::new(false));
    let writer = ControlledWriter {
        inner: MemoryReplicaStore::new(),
        corrupt: Arc::clone(&corrupt),
    };
    let reader = writer.reader();
    let store = ExperimentalStateMachine::create(writer, stream()).unwrap();
    let view = store.checkpoint_reader();
    let baseline = reader.snapshot().unwrap();
    corrupt.store(true, Ordering::SeqCst);
    let error = view.checkpoint().await.unwrap_err();
    assert!(!error.to_string().contains("private corrupt"));
    corrupt.store(false, Ordering::SeqCst);
    assert_eq!(view.checkpoint().await, Err(StateMachineError::Poisoned));
    assert_eq!(reader.snapshot().unwrap(), baseline);
    store.shutdown().await.unwrap();
}
