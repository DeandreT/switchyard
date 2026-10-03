use cluster::{ExperimentalLogStore, LogStorageError, MAX_LOG_METADATA_BYTES};
use openraft::{
    RaftLogReader,
    storage::{RaftLogStorage, RaftLogStorageExt},
};
use serde::{Deserialize, Serialize};
use storage::{CommittedStore, StateStore, StoreSnapshot, WriteBatch};

use super::{TestResult, fixture::*};

const PROFILE: &[u8] = &[1];
const PROGRESS: &[u8] = &[2];

fn key(index: u64) -> Vec<u8> {
    let mut key = vec![0x10];
    key.extend_from_slice(&index.to_be_bytes());
    key
}

fn restore<W: CommittedStore>(control: &Control<W>, baseline: &StoreSnapshot) -> TestResult {
    let mut batch = WriteBatch::default();
    for (key, _) in control.reader().snapshot()?.entries() {
        batch.push_delete(key.clone());
    }
    for (key, value) in baseline.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    control.inject(batch)?;
    Ok(())
}

fn refuses_corrupt_without_commit<W: CommittedStore>(
    control: &Control<W>,
    batch: WriteBatch,
) -> TestResult {
    control.inject(batch)?;
    let before = control.reader().snapshot()?;
    let commits = control.commits();
    assert!(matches!(
        ExperimentalLogStore::open(control.recover_writer(), profile()?),
        Err(LogStorageError::Corrupt)
    ));
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(control.commits(), commits);
    Ok(())
}

async fn missing_unknown_and_malformed_metadata_are_never_adopted<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let store = ExperimentalLogStore::create(writer, profile()?)?;
    store.shutdown().await?;
    let baseline = control.reader().snapshot()?;
    let profile_bytes = control
        .reader()
        .get(PROFILE)?
        .ok_or("missing log profile")?;
    let progress_bytes = control
        .reader()
        .get(PROGRESS)?
        .ok_or("missing log progress")?;
    let mut bad_version = profile_bytes.clone();
    bad_version[4] = 2;
    let mut trailing = progress_bytes.clone();
    trailing.push(0);
    let mut wrong_role = profile_bytes.clone();
    let at = wrong_role
        .windows(b"queue-log-only".len())
        .position(|bytes| bytes == b"queue-log-only")
        .ok_or("profile role not encoded")?;
    wrong_role[at] = b'Q';
    let cases = [
        WriteBatch::default().delete(PROFILE),
        WriteBatch::default().delete(PROGRESS),
        WriteBatch::default().put(vec![0x99], vec![1]),
        WriteBatch::default().put(PROFILE, bad_version),
        WriteBatch::default().put(PROFILE, wrong_role),
        WriteBatch::default().put(PROFILE, profile_bytes[..profile_bytes.len() - 1].to_vec()),
        WriteBatch::default().put(PROGRESS, trailing),
        WriteBatch::default().put(PROGRESS, vec![0; MAX_LOG_METADATA_BYTES + 1]),
    ];
    for batch in cases {
        restore(&control, &baseline)?;
        refuses_corrupt_without_commit(&control, batch)?;
    }
    restore(&control, &baseline)?;
    let reopened = ExperimentalLogStore::open(control.recover_writer(), profile()?)?;
    reopened.shutdown().await?;
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
struct IdV1 {
    term: u64,
    node_id: u64,
    index: u64,
}
#[derive(Clone, Serialize, Deserialize)]
struct VoteV1 {
    term: u64,
    node_id: u64,
    committed: bool,
}
#[derive(Clone, Serialize, Deserialize)]
struct ProgressV1 {
    vote: Option<VoteV1>,
    last_purged: Option<IdV1>,
    last_present: Option<IdV1>,
    retained_entries: u64,
    retained_bytes: u64,
}

fn encode_progress(value: &ProgressV1) -> TestResult<Vec<u8>> {
    let mut bytes = b"SWLS\x01".to_vec();
    bytes.extend(postcard::to_allocvec(value)?);
    Ok(bytes)
}

async fn startup_scan_reconciles_exact_entries_stats_and_tail_identity<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    store
        .blocking_append([blank(0), blank(1), blank(2)])
        .await?;
    store.shutdown().await?;
    let baseline = control.reader().snapshot()?;
    let progress = control
        .reader()
        .get(PROGRESS)?
        .ok_or("missing progress record")?;
    assert_eq!(&progress[..5], b"SWLS\x01");
    let decoded: ProgressV1 = postcard::from_bytes(&progress[5..])?;
    let mut wrong_bytes = decoded.clone();
    wrong_bytes.retained_bytes += 1;
    let mut wrong_count = decoded.clone();
    wrong_count.retained_entries -= 1;
    let mut wrong_tail = decoded;
    wrong_tail
        .last_present
        .as_mut()
        .ok_or("missing expected tail")?
        .node_id += 1;
    let mut noncanonical = control.reader().get(&key(0))?.ok_or("missing entry zero")?;
    assert_eq!(&noncanonical[..5], b"SWLE\x01");
    assert_eq!(noncanonical[5], 1);
    noncanonical.splice(5..6, [0x81, 0]);
    let mut trailing = control.reader().get(&key(1))?.ok_or("missing entry one")?;
    trailing.push(0);
    let cases = [
        WriteBatch::default().delete(key(1)),
        WriteBatch::default().put(
            key(3),
            control.reader().get(&key(2))?.ok_or("missing entry two")?,
        ),
        WriteBatch::default().put(key(0), noncanonical),
        WriteBatch::default().put(key(1), trailing),
        WriteBatch::default().put(PROGRESS, encode_progress(&wrong_bytes)?),
        WriteBatch::default().put(PROGRESS, encode_progress(&wrong_count)?),
        WriteBatch::default().put(PROGRESS, encode_progress(&wrong_tail)?),
    ];
    for batch in cases {
        restore(&control, &baseline)?;
        refuses_corrupt_without_commit(&control, batch)?;
    }
    restore(&control, &baseline)?;
    let mut reopened = ExperimentalLogStore::open(control.recover_writer(), profile()?)?;
    assert_eq!(
        reopened.try_get_log_entries(..).await?,
        vec![blank(0), blank(1), blank(2)]
    );
    reopened.shutdown().await?;
    Ok(())
}

async fn touched_runtime_corruption_poisoning_prevents_later_writes<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    store.blocking_append([blank(0)]).await?;
    let original = control.reader().get(&key(0))?.ok_or("missing entry")?;
    let mut corrupt = original.clone();
    corrupt.push(0);
    control.inject(WriteBatch::default().put(key(0), corrupt))?;
    let before = control.reader().snapshot()?;
    let commits = control.commits();
    assert!(store.try_get_log_entries(..).await.is_err());
    assert!(store.read_vote().await.is_err());
    assert!(store.blocking_append([blank(1)]).await.is_err());
    assert_eq!(control.commits(), commits);
    assert_eq!(control.reader().snapshot()?, before);
    store.shutdown().await?;
    assert!(matches!(
        ExperimentalLogStore::open(control.recover_writer(), profile()?),
        Err(LogStorageError::Corrupt)
    ));
    control.inject(WriteBatch::default().put(key(0), original))?;
    let mut reopened = ExperimentalLogStore::open(control.recover_writer(), profile()?)?;
    reopened.blocking_append([blank(1)]).await?;
    reopened.shutdown().await?;
    Ok(())
}

for_each_backend!(
    missing_unknown_and_malformed_metadata_are_never_adopted,
    startup_scan_reconciles_exact_entries_stats_and_tail_identity,
    touched_runtime_corruption_poisoning_prevents_later_writes,
);
