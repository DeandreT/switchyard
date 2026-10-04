use std::collections::{BTreeMap, BTreeSet};

use cluster::{
    BoundedSnapshotData, ExperimentalLogStore, ExperimentalStateMachine, LogApplication as A,
    LogEntry, LogProfile, LogQueueConfigRefusal, LogQueueRefusal, StateMachineError,
};
use domain::{CommittedStreamId, QueueConfig, Timestamp};
use openraft::{
    BasicNode, EntryPayload, Membership, SnapshotMeta, StoredMembership,
    storage::{RaftSnapshotBuilder, RaftStateMachine},
};
use storage::{CommittedStore, StateStore, WriteBatch};

use super::{TestResult, fixture::*};

async fn pristine_initialization_wrong_stream_and_joined_reopen<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    assert!(matches!(
        ExperimentalStateMachine::open(writer, stream()?),
        Err(StateMachineError::InvalidState)
    ));
    assert_eq!(control.counts().commits, 0);
    let mut machine = ExperimentalStateMachine::create(control.recover_writer(), stream()?)?;
    let baseline = control.reader().snapshot()?;
    assert_eq!(baseline.entries().len(), 1);
    assert_eq!(
        machine.applied_state().await?,
        (None, StoredMembership::default())
    );
    assert!(machine.get_current_snapshot().await?.is_none());
    assert_eq!(workload(&machine, 0).await?.encoded_bytes, 0);
    machine.shutdown().await?;
    assert!(matches!(
        ExperimentalStateMachine::create(control.recover_writer(), stream()?),
        Err(StateMachineError::InvalidState)
    ));
    assert!(matches!(
        ExperimentalStateMachine::open(control.recover_writer(), CommittedStreamId::new([8; 16])?),
        Err(StateMachineError::InvalidState)
    ));
    assert_eq!(control.reader().snapshot()?, baseline);
    assert_eq!(control.counts().commits, 1);
    let mut reopened = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
    assert_eq!(
        reopened.applied_state().await?,
        (None, StoredMembership::default())
    );
    reopened.shutdown().await?;
    Ok(())
}

async fn actual_log_role_is_not_adopted_by_the_state_machine<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let log = ExperimentalLogStore::create(writer, LogProfile::new(7, stream()?)?)?;
    log.shutdown().await?;
    let before = control.reader().snapshot()?;
    let commits = control.counts().commits;
    assert!(matches!(
        ExperimentalStateMachine::open(control.recover_writer(), stream()?),
        Err(StateMachineError::InvalidState)
    ));
    assert!(matches!(
        ExperimentalStateMachine::create(control.recover_writer(), stream()?),
        Err(StateMachineError::InvalidState)
    ));
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(control.counts().commits, commits);
    ExperimentalLogStore::open(control.recover_writer(), LogProfile::new(7, stream()?)?)?
        .shutdown()
        .await?;
    Ok(())
}

async fn actual_trait_preserves_typed_results_full_membership_and_recovery<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    let joint = Membership::new(
        vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([3, 4, 5])],
        (1..=5)
            .map(|node| (node, BasicNode::new(format!("node-{node}"))))
            .collect::<BTreeMap<_, _>>(),
    );
    let entries = vec![
        create(0, 1, QueueConfig::default())?,
        membership(1),
        blank(2),
        send(3, 3, vec![0, 255, 17])?,
        LogEntry {
            log_id: id(1, 4),
            payload: EntryPayload::Membership(joint.clone()),
        },
        create(5, 4, QueueConfig::default())?,
    ];
    assert_eq!(
        machine.apply(entries).await?,
        vec![
            A::QueueCreated,
            A::CheckpointOnly,
            A::CheckpointOnly,
            A::Sent { sequence: 1 },
            A::CheckpointOnly,
            A::Refused(LogQueueRefusal::QueueAlreadyExists)
        ]
    );
    assert_eq!(
        machine.applied_state().await?,
        (
            Some(id(1, 5)),
            StoredMembership::new(Some(id(1, 4)), joint.clone())
        )
    );
    assert_eq!(
        message(&control.reader(), 1)?
            .ok_or("missing first typed allocation")?
            .body,
        vec![0, 255, 17]
    );
    assert_eq!(
        machine.apply([send(6, 4, vec![22])?]).await?,
        vec![A::Sent { sequence: 2 }]
    );
    let snapshot = control.reader().snapshot()?;
    machine.shutdown().await?;
    let mut reopened = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
    assert_eq!(
        reopened.applied_state().await?,
        (Some(id(1, 6)), StoredMembership::new(Some(id(1, 4)), joint))
    );
    assert_eq!(control.reader().snapshot()?, snapshot);
    assert_eq!(
        message(&control.reader(), 2)?
            .ok_or("missing next typed allocation")?
            .body,
        vec![22]
    );
    assert_eq!(message(&control.reader(), 3)?, None);
    reopened.shutdown().await?;
    Ok(())
}

async fn normal_refusals_commit_only_progress_and_exact_replay_has_no_original_result<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    let invalid = QueueConfig {
        max_message_bytes: 0,
        ..QueueConfig::default()
    };
    assert_eq!(
        machine.apply([create(0, 5, invalid)?]).await?,
        vec![A::Refused(LogQueueRefusal::InvalidQueueConfiguration(
            LogQueueConfigRefusal::MaxMessageBytesTooSmall
        ))]
    );
    assert_eq!(control.reader().snapshot()?.entries().len(), 1);
    assert_eq!(control.last_batch().mutations().len(), 1);
    assert_eq!(
        machine.apply([send(1, 6, vec![1])?]).await?,
        vec![A::Refused(LogQueueRefusal::QueueNotFound)]
    );
    assert_eq!(control.last_batch().mutations().len(), 1);
    assert_eq!(
        machine
            .apply([create(2, 6, QueueConfig::default())?])
            .await?,
        vec![A::QueueCreated]
    );
    let before = control.reader().snapshot()?;
    assert_eq!(
        machine
            .apply([create(3, 7, QueueConfig::default())?])
            .await?,
        vec![A::Refused(LogQueueRefusal::QueueAlreadyExists)]
    );
    let key = checkpoint_key();
    assert_eq!(
        control
            .reader()
            .snapshot()?
            .entries()
            .iter()
            .filter(|(candidate, _)| *candidate != key)
            .collect::<Vec<_>>(),
        before
            .entries()
            .iter()
            .filter(|(candidate, _)| *candidate != key)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        machine.apply([send(4, 6, vec![1])?]).await?,
        vec![A::Refused(LogQueueRefusal::ClockRegression {
            last_applied_millis: 7,
            proposed_millis: 6
        })]
    );
    assert_eq!(control.last_batch().mutations().len(), 1);
    assert_eq!(
        domain::StateMachine::new(control.reader()).last_applied_time()?,
        Timestamp::from_millis(6)
    );
    let last = send(5, 7, vec![1])?;
    assert_eq!(
        machine.apply([last.clone()]).await?,
        vec![A::Sent { sequence: 1 }]
    );
    let before = control.reader().snapshot()?;
    let commits = control.counts().commits;
    control.reset_reads();
    assert_eq!(
        machine.apply([last.clone()]).await?,
        vec![A::AlreadyApplied { entry: entry_id(5) }]
    );
    let counts = control.counts();
    assert_eq!(counts.commits, commits);
    assert_eq!(counts.scans, 0);
    assert!(!counts.reads.is_empty());
    assert!(counts.reads.iter().all(|candidate| *candidate == key));
    assert_eq!(control.reader().snapshot()?, before);
    assert!(machine.apply([send(5, 7, vec![2])?]).await.is_err());
    assert!(machine.applied_state().await.is_err());
    assert_eq!(control.reader().snapshot()?, before);
    machine.shutdown().await?;
    let mut reopened = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
    assert_eq!(
        reopened.apply([last]).await?,
        vec![A::AlreadyApplied { entry: entry_id(5) }]
    );
    reopened.shutdown().await?;
    Ok(())
}

async fn older_history_gaps_and_full_leader_identity_regressions_refuse_without_writes<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    machine
        .apply([create(0, 1, QueueConfig::default())?, send(1, 2, vec![1])?])
        .await?;
    let before = control.reader().snapshot()?;
    let commits = control.counts().commits;
    for entries in [
        vec![blank(3)],
        vec![blank(0)],
        vec![LogEntry {
            log_id: id(0, 2),
            payload: EntryPayload::Blank,
        }],
        vec![LogEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 6), 2),
            payload: EntryPayload::Blank,
        }],
        vec![blank(2), blank(4)],
        vec![blank(2), blank(2)],
    ] {
        assert!(machine.apply(entries).await.is_err());
        assert_eq!(control.counts().commits, commits);
        assert_eq!(control.reader().snapshot()?, before);
        machine.shutdown().await?;
        machine = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
        assert_eq!(machine.applied_state().await?.0, Some(id(1, 1)));
    }
    assert_eq!(machine.apply([blank(2)]).await?, vec![A::CheckpointOnly]);
    machine.shutdown().await?;
    Ok(())
}

async fn unsupported_snapshot_methods_never_write_and_current_snapshot_checks_health<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    machine
        .apply([create(0, 1, QueueConfig::default())?])
        .await?;
    let before = control.reader().snapshot()?;
    let commits = control.counts().commits;
    let mut builder = machine.get_snapshot_builder().await;
    assert!(builder.build_snapshot().await.is_err());
    assert!(machine.begin_receiving_snapshot().await.is_err());
    let meta = SnapshotMeta {
        last_log_id: Some(id(1, 0)),
        last_membership: StoredMembership::default(),
        snapshot_id: "private-snapshot-name".into(),
    };
    let error = machine
        .install_snapshot(
            &meta,
            Box::new(BoundedSnapshotData::from_bytes(&[0x71; 1024])?),
        )
        .await
        .expect_err("snapshot installation must refuse");
    assert!(error.to_string().contains("does not support snapshots"));
    assert!(!error.to_string().contains("private-snapshot-name"));
    assert!(machine.get_current_snapshot().await?.is_none());
    assert_eq!(control.counts().commits, commits);
    assert_eq!(control.reader().snapshot()?, before);
    control.inject(WriteBatch::default().delete(checkpoint_key()))?;
    assert!(machine.get_current_snapshot().await.is_err());
    assert!(machine.applied_state().await.is_err());
    assert_eq!(control.counts().commits, commits);
    machine.shutdown().await?;
    Ok(())
}

for_each_backend!(
    pristine_initialization_wrong_stream_and_joined_reopen,
    actual_log_role_is_not_adopted_by_the_state_machine,
    actual_trait_preserves_typed_results_full_membership_and_recovery,
    normal_refusals_commit_only_progress_and_exact_replay_has_no_original_result,
    older_history_gaps_and_full_leader_identity_regressions_refuse_without_writes,
    unsupported_snapshot_methods_never_write_and_current_snapshot_checks_health,
);
