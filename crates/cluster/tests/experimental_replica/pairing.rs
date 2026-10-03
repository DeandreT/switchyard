use cluster::{
    ExperimentalLogStore, ExperimentalReplicaStores, ExperimentalStateMachine, LogEntry, LogId,
    LogProfile, LogVote, ReplicaPreparationError as Error,
};
use domain::{CommittedEntryId, CommittedEntryMark, CommittedStreamId};
use openraft::{EntryPayload, StoredMembership, storage::RaftLogStorage};
use serde::{Deserialize, Serialize};
use storage::{CommittedStore, StateStore, WriteBatch};

use super::{TestResult, fixture::*};

async fn reject<W: CommittedStore>(
    expected: Error,
    node: u64,
    log: ExperimentalLogStore,
    state: ExperimentalStateMachine,
    log_control: &Control<W>,
    state_control: &Control<W>,
) -> TestResult {
    let snapshots = (
        log_control.reader().snapshot()?,
        state_control.reader().snapshot()?,
    );
    let commits = (log_control.commits(), state_control.commits());
    match ExperimentalReplicaStores::prepare(node, log, state).await {
        Err(error) => assert_eq!(error, expected),
        Ok(prepared) => {
            prepared.shutdown().await?;
            return Err("a mismatched replica pair was adopted".into());
        }
    }
    assert!(log_control.retired() && state_control.retired());
    assert_eq!((log_control.commits(), state_control.commits()), commits);
    assert_eq!(
        (
            log_control.reader().snapshot()?,
            state_control.reader().snapshot()?
        ),
        snapshots
    );
    Ok(())
}

async fn intended_node_mismatch_refuses_but_zero_is_not_invented_as_reserved<W: CommittedStore>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let (log, state, log_control, state_control) = seed_profiled(
        log_writer,
        state_writer,
        LogProfile::new(0, stream()?)?,
        stream()?,
        &[],
        0,
    )
    .await?;
    reject(
        Error::ProfileMismatch,
        NODE,
        log,
        state,
        &log_control,
        &state_control,
    )
    .await?;
    let log =
        ExperimentalLogStore::open(log_control.recover_writer(), LogProfile::new(0, stream()?)?)?;
    let state = ExperimentalStateMachine::open(state_control.recover_writer(), stream()?)?;
    let prepared = ExperimentalReplicaStores::prepare(0, log, state).await?;
    assert_eq!(prepared.progress().node_id(), 0);
    assert_eq!(prepared.progress().applied(), None);
    prepared.shutdown().await?;
    Ok(())
}

async fn independently_valid_streams_cannot_be_paired<W: CommittedStore>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let (log, state, log_control, state_control) = seed_profiled(
        log_writer,
        state_writer,
        profile()?,
        other_stream()?,
        &[],
        0,
    )
    .await?;
    reject(
        Error::ProfileMismatch,
        NODE,
        log,
        state,
        &log_control,
        &state_control,
    )
    .await?;
    let log = ExperimentalLogStore::open(log_control.recover_writer(), profile()?)?;
    let state = ExperimentalStateMachine::open(state_control.recover_writer(), other_stream()?)?;
    log.shutdown().await?;
    state.shutdown().await?;
    Ok(())
}

async fn row_zero_must_be_the_genuine_default_id_initial_membership<W: CommittedStore>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let cases = vec![
        create(0, 1)?,
        LogEntry {
            log_id: LogId::default(),
            payload: EntryPayload::Blank,
        },
        LogEntry {
            log_id: id(0),
            payload: EntryPayload::Membership(members()),
        },
    ];
    let (mut log, mut state, log_control, state_control) =
        seed(log_writer, state_writer, &[], 0).await?;
    for entry in cases {
        append_all(&mut log, &[entry]).await?;
        reject(
            Error::InvalidHistory,
            NODE,
            log,
            state,
            &log_control,
            &state_control,
        )
        .await?;
        log = ExperimentalLogStore::open(log_control.recover_writer(), profile()?)?;
        state = ExperimentalStateMachine::open(state_control.recover_writer(), stream()?)?;
        log.truncate(LogId::default()).await?;
    }
    append_all(&mut log, &[initial()]).await?;
    let prepared = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
    assert_eq!(prepared.progress().applied(), None);
    prepared.shutdown().await?;
    Ok(())
}

async fn purge_history_is_never_repaired_or_adopted_without_snapshots<W: CommittedStore>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let entries = vec![initial(), create(1, 1)?, send(2, 2, vec![1])?];
    let (mut log, state, log_control, state_control) =
        seed(log_writer, state_writer, &entries, entries.len()).await?;
    log.save_vote(&LogVote::new_committed(1, NODE)).await?;
    log.purge(LogId::default()).await?;
    reject(
        Error::PurgedHistory,
        NODE,
        log,
        state,
        &log_control,
        &state_control,
    )
    .await?;
    Ok(())
}

async fn applied_position_ahead_of_retained_tail_is_not_auto_purged<W: CommittedStore>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let entries = vec![initial(), create(1, 1)?, send(2, 2, vec![1])?];
    let (mut log, state, log_control, state_control) =
        seed(log_writer, state_writer, &entries, entries.len()).await?;
    log.save_vote(&LogVote::new_committed(1, NODE)).await?;
    log.truncate(id(2)).await?;
    reject(
        Error::AppliedAhead,
        NODE,
        log,
        state,
        &log_control,
        &state_control,
    )
    .await?;
    Ok(())
}

async fn exact_applied_identity_includes_its_original_leader<W: CommittedStore>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let entries = vec![initial(), create(1, 1)?, send(2, 2, vec![1])?];
    let (mut log, state, log_control, state_control) =
        seed(log_writer, state_writer, &entries, entries.len()).await?;
    log.truncate(id(2)).await?;
    let mut replacement = entries[2].clone();
    replacement.log_id = LogId::new(openraft::CommittedLeaderId::new(2, NODE + 1), 2);
    append_all(&mut log, &[replacement]).await?;
    log.save_vote(&LogVote::new_committed(2, NODE + 1)).await?;
    reject(
        Error::CheckpointMismatch,
        NODE,
        log,
        state,
        &log_control,
        &state_control,
    )
    .await?;
    Ok(())
}

async fn canonical_mark_chain_detects_latest_and_earlier_same_id_content_divergence<
    W: CommittedStore,
>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let entries = vec![
        initial(),
        create(1, 1)?,
        send(2, 2, b"old predecessor".to_vec())?,
        send(3, 3, b"same latest body".to_vec())?,
    ];
    let (mut log, mut state, log_control, state_control) =
        seed(log_writer, state_writer, &entries, entries.len()).await?;
    log.save_vote(&LogVote::new_committed(1, NODE)).await?;
    for changed in [
        vec![
            send(2, 2, b"different earlier body".to_vec())?,
            entries[3].clone(),
        ],
        vec![
            entries[2].clone(),
            send(3, 3, b"different latest body".to_vec())?,
        ],
    ] {
        log.truncate(id(2)).await?;
        append_all(&mut log, &changed).await?;
        reject(
            Error::CheckpointMismatch,
            NODE,
            log,
            state,
            &log_control,
            &state_control,
        )
        .await?;
        log = ExperimentalLogStore::open(log_control.recover_writer(), profile()?)?;
        state = ExperimentalStateMachine::open(state_control.recover_writer(), stream()?)?;
    }
    log.truncate(id(2)).await?;
    append_all(&mut log, &entries[2..]).await?;
    let prepared = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
    assert_eq!(prepared.progress().applied(), Some(id(3)));
    prepared.shutdown().await?;
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
struct CheckpointV1 {
    stream: CommittedStreamId,
    last: Option<CommittedEntryMark>,
    previous: Option<CommittedEntryMark>,
    highest_timestamp: u64,
    membership: Option<MembershipV1>,
}
#[derive(Clone, Serialize, Deserialize)]
struct MembershipV1 {
    source: CommittedEntryId,
    schema_version: u16,
    payload: Vec<u8>,
}
#[derive(Serialize, Deserialize)]
struct MembershipPayloadV1 {
    configs: Vec<Vec<u64>>,
    nodes: Vec<NodeV1>,
}
#[derive(Serialize, Deserialize)]
struct NodeV1 {
    id: u64,
    address: String,
}

fn checkpoint_bytes(checkpoint: &CheckpointV1) -> TestResult<Vec<u8>> {
    let mut bytes = b"SWYC\x01".to_vec();
    bytes.extend(postcard::to_allocvec(checkpoint)?);
    Ok(bytes)
}

async fn valid_checkpoint_fields_cannot_forge_watermark_predecessor_or_effective_membership<
    W: CommittedStore,
>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let entries = vec![initial(), create(1, 1)?, send(2, 3, vec![1])?];
    let (mut log, mut state, log_control, state_control) =
        seed(log_writer, state_writer, &entries, entries.len()).await?;
    log.save_vote(&LogVote::new_committed(1, NODE)).await?;
    let baseline = state_control.reader().snapshot()?;
    // Frozen SWYC key and schema are test-only corruption fixtures, not public raw writer APIs.
    let original: CheckpointV1 = postcard::from_bytes(
        &state_control
            .reader()
            .get(&[0x12])?
            .ok_or("missing paired checkpoint")?[5..],
    )?;
    let mut cases = Vec::new();
    let mut changed = original.clone();
    changed.highest_timestamp = 99;
    cases.push((changed, Error::CheckpointMismatch));
    let mut changed = original.clone();
    changed.highest_timestamp = 1;
    cases.push((changed, Error::CheckpointMismatch));
    let mut changed = original.clone();
    changed
        .previous
        .as_mut()
        .ok_or("missing predecessor mark")?
        .fingerprint[0] ^= 1;
    cases.push((changed, Error::CheckpointMismatch));
    let mut changed = original.clone();
    changed
        .last
        .as_mut()
        .ok_or("missing current mark")?
        .fingerprint[0] ^= 1;
    cases.push((changed, Error::CheckpointMismatch));
    let mut changed = original.clone();
    let member = changed
        .membership
        .as_mut()
        .ok_or("missing effective member")?;
    let mut payload: MembershipPayloadV1 = postcard::from_bytes(&member.payload)?;
    payload.nodes[0].address = "changed-address".into();
    member.payload = postcard::to_allocvec(&payload)?;
    cases.push((changed, Error::MembershipMismatch));
    let mut changed = original.clone();
    changed
        .membership
        .as_mut()
        .ok_or("missing effective member")?
        .source = CommittedEntryId {
        term: 1,
        node_id: NODE,
        index: 1,
    };
    cases.push((changed, Error::MembershipMismatch));
    for (changed, expected) in cases {
        state_control.inject(WriteBatch::default().put(vec![0x12], checkpoint_bytes(&changed)?))?;
        reject(expected, NODE, log, state, &log_control, &state_control).await?;
        restore(&state_control, &baseline)?;
        log = ExperimentalLogStore::open(log_control.recover_writer(), profile()?)?;
        state = ExperimentalStateMachine::open(state_control.recover_writer(), stream()?)?;
    }
    let prepared = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
    assert_eq!(
        prepared.progress().membership(),
        &StoredMembership::new(Some(LogId::default()), members())
    );
    prepared.shutdown().await?;
    Ok(())
}

async fn persisted_vote_must_cover_tail_and_initial_exception_cannot_cover_applied_work<
    W: CommittedStore,
>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let entries = vec![initial(), blank(1)];
    let (mut log, mut state, log_control, state_control) =
        seed(log_writer, state_writer, &entries, entries.len()).await?;
    for vote in [
        None,
        Some(LogVote::new(0, NODE)),
        Some(LogVote::new(1, NODE)),
    ] {
        if let Some(vote) = vote {
            log.save_vote(&vote).await?;
        }
        reject(
            Error::VoteMismatch,
            NODE,
            log,
            state,
            &log_control,
            &state_control,
        )
        .await?;
        log = ExperimentalLogStore::open(log_control.recover_writer(), profile()?)?;
        state = ExperimentalStateMachine::open(state_control.recover_writer(), stream()?)?;
    }
    log.save_vote(&LogVote::new(2, NODE)).await?;
    let prepared = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
    assert_eq!(prepared.progress().applied(), Some(id(1)));
    prepared.shutdown().await?;
    Ok(())
}

async fn stored_profile_changes_are_detected_by_the_owner_not_a_cached_observer<
    W: CommittedStore,
>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let (log, state, log_control, state_control) = seed(log_writer, state_writer, &[], 0).await?;
    let other = storage::MemoryReplicaStore::new();
    let other_reader = other.reader();
    let other_store = ExperimentalLogStore::create(other, LogProfile::new(NODE + 1, stream()?)?)?;
    let alternate = other_reader
        .get(&[1])?
        .ok_or("missing alternate immutable profile")?;
    other_store.shutdown().await?;
    log_control.inject(WriteBatch::default().put(vec![1], alternate))?;
    reject(
        Error::Storage,
        NODE,
        log,
        state,
        &log_control,
        &state_control,
    )
    .await?;
    Ok(())
}

for_each_backend!(
    intended_node_mismatch_refuses_but_zero_is_not_invented_as_reserved,
    independently_valid_streams_cannot_be_paired,
    row_zero_must_be_the_genuine_default_id_initial_membership,
    purge_history_is_never_repaired_or_adopted_without_snapshots,
    applied_position_ahead_of_retained_tail_is_not_auto_purged,
    exact_applied_identity_includes_its_original_leader,
    canonical_mark_chain_detects_latest_and_earlier_same_id_content_divergence,
    valid_checkpoint_fields_cannot_forge_watermark_predecessor_or_effective_membership,
    persisted_vote_must_cover_tail_and_initial_exception_cannot_cover_applied_work,
    stored_profile_changes_are_detected_by_the_owner_not_a_cached_observer,
);
