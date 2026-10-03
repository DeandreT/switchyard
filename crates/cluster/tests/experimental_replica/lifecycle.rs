use cluster::{
    ExperimentalLogStore, ExperimentalReplicaStores, ExperimentalStateMachine, LogApplication,
    LogEntry, LogVote, MAX_LOG_BODY_BYTES, MAX_REPLICA_PAYLOAD_ENTRIES,
};
use domain::Timestamp;
use openraft::{
    BasicNode, EntryPayload, Membership, RaftLogReader, StoredMembership,
    storage::{RaftLogStorage, RaftStateMachine},
};
use storage::{CommittedStore, FjallReplicaStore, StateStore};

use super::{DEADLINE, TestResult, fixture::*};

async fn pristine_separate_stores_prepare_without_initializing_membership_or_writing<
    W: CommittedStore,
>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let (log, state, log_control, state_control) = seed(log_writer, state_writer, &[], 0).await?;
    let mut old_reader = log.log_reader();
    let snapshots = (
        log_control.reader().snapshot()?,
        state_control.reader().snapshot()?,
    );
    let commits = (log_control.commits(), state_control.commits());
    let prepared = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
    let progress = prepared.progress();
    assert_eq!(progress.node_id(), NODE);
    assert_eq!(progress.stream(), stream()?);
    assert_eq!(progress.log_tail(), None);
    assert_eq!(progress.applied(), None);
    assert_eq!(progress.highest_timestamp(), Timestamp::from_millis(0));
    assert_eq!(progress.membership(), &StoredMembership::default());
    assert_eq!(
        prepared.replication_config().snapshot_policy,
        openraft::SnapshotPolicy::Never
    );
    assert_eq!(
        prepared.replication_config().max_payload_entries,
        MAX_REPLICA_PAYLOAD_ENTRIES
    );
    assert_eq!((log_control.commits(), state_control.commits()), commits);
    assert_eq!(
        (
            log_control.reader().snapshot()?,
            state_control.reader().snapshot()?
        ),
        snapshots
    );
    prepared.shutdown().await?;
    assert!(log_control.retired() && state_control.retired());
    assert!(old_reader.try_get_log_entries(..).await.is_err());
    Ok(())
}

async fn lone_unapplied_genuine_initial_membership_allows_only_initial_vote_exception<
    W: CommittedStore,
>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let (mut log, mut state, log_control, state_control) =
        seed(log_writer, state_writer, &[initial()], 0).await?;
    for vote in [None, Some(LogVote::new(0, 0))] {
        if let Some(vote) = vote {
            log.save_vote(&vote).await?;
        }
        let snapshots = (
            log_control.reader().snapshot()?,
            state_control.reader().snapshot()?,
        );
        let commits = (log_control.commits(), state_control.commits());
        let prepared = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
        assert_eq!(prepared.progress().log_tail(), Some(Default::default()));
        assert_eq!(prepared.progress().applied(), None);
        assert_eq!(
            prepared.progress().membership(),
            &StoredMembership::default()
        );
        assert_eq!(
            prepared.progress().highest_timestamp(),
            Timestamp::from_millis(0)
        );
        assert_eq!((log_control.commits(), state_control.commits()), commits);
        assert_eq!(
            (
                log_control.reader().snapshot()?,
                state_control.reader().snapshot()?
            ),
            snapshots
        );
        prepared.shutdown().await?;
        assert!(log_control.retired() && state_control.retired());
        log = ExperimentalLogStore::open(log_control.recover_writer(), profile()?)?;
        state = ExperimentalStateMachine::open(state_control.recover_writer(), stream()?)?;
    }
    log.shutdown().await?;
    state.shutdown().await?;
    Ok(())
}

async fn exact_applied_prefix_recovers_joint_membership_while_suffix_remains_unapplied<
    W: CommittedStore,
>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    let joint = Membership::new(
        vec![
            std::collections::BTreeSet::from([7, 8, 9]),
            std::collections::BTreeSet::from([8, 9, 10]),
        ],
        (7..=10)
            .map(|node| (node, BasicNode::new(format!("node-{node}"))))
            .collect::<std::collections::BTreeMap<_, _>>(),
    );
    let entries = vec![
        initial(),
        create(1, 2)?,
        send(2, 3, vec![0, 255, 17])?,
        LogEntry {
            log_id: id(3),
            payload: EntryPayload::Membership(joint.clone()),
        },
        send(4, 100, vec![99])?,
    ];
    let (mut log, mut state, log_control, state_control) =
        seed(log_writer, state_writer, &entries, 4).await?;
    for vote in [LogVote::new_committed(1, NODE), LogVote::new(1, NODE + 1)] {
        log.save_vote(&vote).await?;
        let snapshots = (
            log_control.reader().snapshot()?,
            state_control.reader().snapshot()?,
        );
        let commits = (log_control.commits(), state_control.commits());
        let prepared = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
        assert_eq!(prepared.progress().log_tail(), Some(id(4)));
        assert_eq!(prepared.progress().applied(), Some(id(3)));
        assert_eq!(
            prepared.progress().highest_timestamp(),
            Timestamp::from_millis(3)
        );
        assert_eq!(
            prepared.progress().membership(),
            &StoredMembership::new(Some(id(3)), joint.clone())
        );
        assert_eq!((log_control.commits(), state_control.commits()), commits);
        assert_eq!(
            (
                log_control.reader().snapshot()?,
                state_control.reader().snapshot()?
            ),
            snapshots
        );
        prepared.shutdown().await?;
        assert!(log_control.retired() && state_control.retired());
        log = ExperimentalLogStore::open(log_control.recover_writer(), profile()?)?;
        state = ExperimentalStateMachine::open(state_control.recover_writer(), stream()?)?;
    }
    assert_eq!(
        state.apply([entries[4].clone()]).await?,
        vec![LogApplication::Sent { sequence: 2 }]
    );
    log.shutdown().await?;
    state.shutdown().await?;
    Ok(())
}

async fn bounded_full_history_is_checked_across_count_and_byte_limited_chunks<W: CommittedStore>(
    log_writer: W,
    state_writer: W,
) -> TestResult {
    assert_eq!(MAX_REPLICA_PAYLOAD_ENTRIES, 15);
    let mut entries = vec![initial(), create(1, 1)?];
    for index in 2..256 {
        let body = if index < 19 {
            vec![index as u8; MAX_LOG_BODY_BYTES]
        } else {
            vec![index as u8]
        };
        entries.push(send(index, index + 1, body)?);
    }
    let (mut log, state, log_control, state_control) =
        seed(log_writer, state_writer, &entries, entries.len()).await?;
    log.save_vote(&LogVote::new_committed(1, NODE)).await?;
    let mut reader = log.log_reader();
    let prefix = reader.limited_get_log_entries(0, 256).await?;
    assert!(prefix.len() < 32 && !prefix.is_empty());
    assert!(prefix.len() < entries.len());
    let snapshots = (
        log_control.reader().snapshot()?,
        state_control.reader().snapshot()?,
    );
    let commits = (log_control.commits(), state_control.commits());
    let prepared = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
    assert_eq!(prepared.progress().log_tail(), Some(id(255)));
    assert_eq!(prepared.progress().applied(), Some(id(255)));
    assert_eq!(
        prepared.progress().highest_timestamp(),
        Timestamp::from_millis(256)
    );
    assert_eq!((log_control.commits(), state_control.commits()), commits);
    assert_eq!(
        (
            log_control.reader().snapshot()?,
            state_control.reader().snapshot()?
        ),
        snapshots
    );
    prepared.shutdown().await?;
    assert!(log_control.retired() && state_control.retired());
    assert!(reader.try_get_log_entries(..).await.is_err());
    Ok(())
}

#[tokio::test]
async fn explicit_shutdown_joins_both_durable_owners_before_immediate_directory_reopen()
-> TestResult {
    tokio::time::timeout(DEADLINE, async {
        let directory = testkit::DurableProvider::temporary()?;
        let log_path = directory.path().join("log");
        let state_path = directory.path().join("state");
        let log_writer = FjallReplicaStore::open(&log_path)?;
        let state_writer = FjallReplicaStore::open(&state_path)?;
        let log_reader = log_writer.reader();
        let state_reader = state_writer.reader();
        let mut log = ExperimentalLogStore::create(log_writer, profile()?)?;
        let mut state = ExperimentalStateMachine::create(state_writer, stream()?)?;
        let entries = vec![
            initial(),
            create(1, 1)?,
            send(2, 2, b"durable paired send".to_vec())?,
        ];
        append_all(&mut log, &entries).await?;
        log.save_vote(&LogVote::new_committed(1, NODE)).await?;
        state.apply(entries).await?;
        let snapshots = (log_reader.snapshot()?, state_reader.snapshot()?);
        drop(log_reader);
        drop(state_reader);
        let prepared = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
        assert_eq!(prepared.progress().applied(), Some(id(2)));
        prepared.shutdown().await?;
        let log_writer = FjallReplicaStore::open(&log_path)?;
        let state_writer = FjallReplicaStore::open(&state_path)?;
        assert_eq!(
            (
                log_writer.reader().snapshot()?,
                state_writer.reader().snapshot()?
            ),
            snapshots
        );
        let log = ExperimentalLogStore::open(log_writer, profile()?)?;
        let state = ExperimentalStateMachine::open(state_writer, stream()?)?;
        let reopened = ExperimentalReplicaStores::prepare(NODE, log, state).await?;
        assert_eq!(reopened.progress().log_tail(), Some(id(2)));
        assert_eq!(reopened.progress().applied(), Some(id(2)));
        reopened.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

for_each_backend!(
    pristine_separate_stores_prepare_without_initializing_membership_or_writing,
    lone_unapplied_genuine_initial_membership_allows_only_initial_vote_exception,
    exact_applied_prefix_recovers_joint_membership_while_suffix_remains_unapplied,
    bounded_full_history_is_checked_across_count_and_byte_limited_chunks,
);
