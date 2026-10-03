use std::collections::{BTreeMap, BTreeSet};

use cluster::{ExperimentalLogStore, LogEntry, LogProfile, LogStorageError, LogVote};
use domain::CommittedStreamId;
use openraft::{
    BasicNode, EntryPayload, Membership, RaftLogReader,
    storage::{RaftLogStorage, RaftLogStorageExt},
};
use storage::{CommittedStore, StateStore};

use super::{TestResult, fixture::*};

async fn profile_initialization_shutdown_and_wrong_role_are_fail_closed<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    assert!(matches!(
        ExperimentalLogStore::open(writer, profile()?),
        Err(LogStorageError::InvalidProfile)
    ));
    assert_eq!(control.commits(), 0);
    let mut store = ExperimentalLogStore::create(control.recover_writer(), profile()?)?;
    assert_eq!(control.commits(), 1);
    let baseline = control.reader().snapshot()?;
    assert_eq!(baseline.entries().len(), 2);
    assert_eq!(store.read_vote().await?, None);
    assert_eq!(store.get_log_state().await?.last_log_id, None);
    let mut reader = store.get_log_reader().await;
    assert!(reader.try_get_log_entries(..).await?.is_empty());
    assert_eq!(workload(&store, 0).await?.encoded_bytes, 0);
    store.shutdown().await?;
    assert!(reader.try_get_log_entries(..).await.is_err());
    assert_eq!(control.reader().snapshot()?, baseline);
    let commits = control.commits();
    assert!(matches!(
        ExperimentalLogStore::create(control.recover_writer(), profile()?),
        Err(LogStorageError::InvalidProfile)
    ));
    for wrong in [
        LogProfile::new(8, profile()?.stream())?,
        LogProfile::new(7, CommittedStreamId::new([8; 16])?)?,
    ] {
        assert!(matches!(
            ExperimentalLogStore::open(control.recover_writer(), wrong),
            Err(LogStorageError::InvalidProfile)
        ));
    }
    assert_eq!(control.commits(), commits);
    assert_eq!(control.reader().snapshot()?, baseline);
    ExperimentalLogStore::open(control.recover_writer(), profile()?)?
        .shutdown()
        .await?;
    Ok(())
}

async fn all_entry_kinds_and_votes_survive_reopen_without_domain_application<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    let membership = LogEntry {
        log_id: id(1, 1),
        payload: EntryPayload::Membership(Membership::new(
            vec![BTreeSet::from([1, 2, 3])],
            BTreeMap::from([
                (1, BasicNode::new("one")),
                (2, BasicNode::new("two")),
                (3, BasicNode::new("three")),
            ]),
        )),
    };
    let entries = vec![blank(0), membership, send(2, vec![0, 255, 17])?];
    store.blocking_append(entries.clone()).await?;
    let vote = LogVote::new_committed(4, 7);
    store.save_vote(&vote).await?;
    assert_eq!(store.read_vote().await?, Some(vote));
    let mut reader = store.get_log_reader().await;
    assert_eq!(reader.try_get_log_entries(..).await?, entries);
    assert_eq!(store.get_log_state().await?.last_log_id, Some(id(1, 2)));
    let commits = control.commits();
    let before = control.reader().snapshot()?;
    store.save_vote(&vote).await?;
    assert_eq!(control.commits(), commits);
    assert_eq!(control.reader().snapshot()?, before);
    assert!(store.save_vote(&LogVote::new(3, 7)).await.is_err());
    assert_eq!(control.commits(), commits);
    assert_eq!(store.read_vote().await?, Some(vote));
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(control.reader().get(&domain::keys::clock())?, None);
    assert!(
        control
            .reader()
            .scan_prefix(
                &domain::keys::message_prefix(
                    &domain::NamespaceName::new("tenant")?,
                    &domain::EntityPath::new("orders")?
                ),
                1
            )?
            .is_empty()
    );
    store.shutdown().await?;
    let mut reopened = ExperimentalLogStore::open(control.recover_writer(), profile()?)?;
    assert_eq!(reopened.read_vote().await?, Some(vote));
    assert_eq!(reopened.get_log_state().await?.last_log_id, Some(id(1, 2)));
    assert_eq!(reopened.try_get_log_entries(..).await?, entries);
    assert_eq!(control.reader().snapshot()?, before);
    reopened.shutdown().await?;
    Ok(())
}

async fn committed_business_store_is_never_adopted_as_a_log<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let machine = domain::CommittedStateMachine::create(writer, profile()?.stream())?;
    let before = control.reader().snapshot()?;
    drop(machine);
    let commits = control.commits();
    assert!(matches!(
        ExperimentalLogStore::open(control.recover_writer(), profile()?),
        Err(LogStorageError::Corrupt)
    ));
    assert!(matches!(
        ExperimentalLogStore::create(control.recover_writer(), profile()?),
        Err(LogStorageError::InvalidProfile)
    ));
    assert_eq!(control.commits(), commits);
    assert_eq!(control.reader().snapshot()?, before);
    Ok(())
}

for_each_backend!(
    profile_initialization_shutdown_and_wrong_role_are_fail_closed,
    all_entry_kinds_and_votes_survive_reopen_without_domain_application,
    committed_business_store_is_never_adopted_as_a_log,
);
