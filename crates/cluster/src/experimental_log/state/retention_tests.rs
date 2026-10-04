use domain::CommittedStreamId;
use storage::{MemoryReplicaStore, StateStore};

use super::*;

fn profile() -> LogProfile {
    LogProfile::new(7, CommittedStreamId::new([7; 16]).unwrap()).unwrap()
}

#[test]
fn healthy_retention_queries_track_actual_canonical_rows_without_writes() {
    let writer = MemoryReplicaStore::new();
    let reader = writer.reader();
    let mut state = StoreState::create(writer, profile()).unwrap();
    let empty = reader.snapshot().unwrap();
    assert_eq!(
        state.retention().unwrap(),
        LogRetention {
            last_present: None,
            last_purged: None,
            retained_entries: 0,
            retained_bytes: 0
        }
    );
    assert_eq!(reader.snapshot().unwrap(), empty);
    let entry = LogEntry {
        log_id: LogId::default(),
        payload: openraft::EntryPayload::Blank,
    };
    let packet = EncodedAppend::from_entries([entry]).unwrap();
    let bytes = packet.encoded_bytes();
    state.append(&packet).unwrap();
    let baseline = reader.snapshot().unwrap();
    assert_eq!(
        state.retention().unwrap(),
        LogRetention {
            last_present: Some(LogId::default()),
            last_purged: None,
            retained_entries: 1,
            retained_bytes: bytes as u64
        }
    );
    assert_eq!(reader.snapshot().unwrap(), baseline);
}

#[test]
fn changed_progress_is_not_adopted_and_poisoned_query_never_repairs_it() {
    let writer = MemoryReplicaStore::new();
    let reader = writer.reader();
    let mut state = StoreState::create(writer, profile()).unwrap();
    let changed = LogProgress {
        vote: Some(LogVote::new(1, 7)),
        ..LogProgress::default()
    };
    state
        .writer
        .commit(WriteBatch::default().put(PROGRESS_KEY, codec::encode_progress(&changed).unwrap()))
        .unwrap();
    let corrupted = reader.snapshot().unwrap();
    assert_eq!(state.retention(), Err(LogStateError::Corrupt));
    assert_eq!(state.retention(), Err(LogStateError::Poisoned));
    assert_eq!(reader.snapshot().unwrap(), corrupted);
}

#[tokio::test]
async fn readonly_query_clone_does_not_keep_a_retired_owner_open() {
    let store = crate::ExperimentalLogStore::create(MemoryReplicaStore::new(), profile()).unwrap();
    let reader = store.log_reader();
    assert_eq!(reader.retention().await.unwrap().retained_entries, 0);
    assert_eq!(
        store.workload().unwrap(),
        crate::LogWorkload {
            accepted_jobs: 0,
            encoded_bytes: 0
        }
    );
    store.shutdown().await.unwrap();
    assert_eq!(
        reader.retention().await,
        Err(crate::LogStorageError::Closed)
    );
}
