use cluster::{ExperimentalStateMachine, LogApplication as A, LogQueueRefusal};
use domain::QueueConfig;
use openraft::storage::RaftStateMachine;
use storage::{CommittedStore, StateStore, WriteBatch};

use super::{TestResult, fixture::*};

async fn middle_physical_failures_preserve_exact_prefix_and_poison_without_partial_response<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    let baseline = control.reader().snapshot()?;
    let entries = vec![
        create(0, 1, QueueConfig::default())?,
        send(1, 2, vec![1])?,
        send(2, 3, vec![2])?,
        blank(3),
    ];
    for fault in [Fault::Before, Fault::After] {
        control.fault_after(3, fault);
        let error = machine
            .apply(entries.clone())
            .await
            .expect_err("middle physical failure must not return a partial result vector");
        assert!(!error.to_string().contains("injected"));
        let prefix = if matches!(fault, Fault::Before) { 2 } else { 3 };
        let (expected, expected_state) = model(&entries[..prefix]).await?;
        assert_eq!(control.reader().snapshot()?, expected);
        assert_eq!(
            message(&control.reader(), 1)?
                .ok_or("missing durable prefix message")?
                .body,
            vec![1]
        );
        assert_eq!(
            message(&control.reader(), 2)?.map(|message| message.body),
            if matches!(fault, Fault::After) {
                Some(vec![2])
            } else {
                None
            }
        );
        let commits = control.counts().commits;
        assert!(machine.applied_state().await.is_err());
        assert!(machine.get_current_snapshot().await.is_err());
        assert!(machine.apply([blank(prefix as u64)]).await.is_err());
        assert_eq!(control.counts().commits, commits);
        machine.shutdown().await?;
        machine = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
        assert_eq!(machine.applied_state().await?, expected_state);
        control.reset_reads();
        assert_eq!(
            machine.apply([entries[prefix - 1].clone()]).await?,
            vec![A::AlreadyApplied {
                entry: entry_id(prefix as u64 - 1)
            }]
        );
        let replay = control.counts();
        assert_eq!(replay.commits, commits);
        assert_eq!(replay.scans, 0);
        assert!(replay.reads.iter().all(|key| *key == checkpoint_key()));
        assert_eq!(control.reader().snapshot()?, expected);
        let resumed = machine.apply(entries[prefix..].to_vec()).await?;
        assert_eq!(
            resumed,
            if matches!(fault, Fault::Before) {
                vec![A::Sent { sequence: 2 }, A::CheckpointOnly]
            } else {
                vec![A::CheckpointOnly]
            }
        );
        assert_eq!(control.reader().snapshot()?, model(&entries).await?.0);
        assert_eq!(
            machine.apply([send(4, 4, vec![3])?]).await?,
            vec![A::Sent { sequence: 3 }]
        );
        assert_eq!(
            counters(&control.reader())?
                .ok_or("missing continued sequence counter")?
                .next_sequence,
            4
        );
        assert_eq!(
            message(&control.reader(), 3)?
                .ok_or("missing next continued allocation")?
                .body,
            vec![3]
        );
        machine.shutdown().await?;
        restore(&control, &baseline)?;
        machine = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
    }
    machine.shutdown().await?;
    Ok(())
}

async fn checkpoint_only_refusal_write_is_still_physically_unknown_on_error<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    let baseline = control.reader().snapshot()?;
    let invalid = QueueConfig {
        max_message_bytes: 0,
        ..QueueConfig::default()
    };
    let entries = vec![blank(0), create(1, 5, invalid)?, blank(2)];
    for fault in [Fault::Before, Fault::After] {
        control.fault_after(2, fault);
        assert!(machine.apply(entries.clone()).await.is_err());
        assert_eq!(control.last_batch().mutations().len(), 1);
        let prefix = if matches!(fault, Fault::Before) { 1 } else { 2 };
        let (expected, state) = model(&entries[..prefix]).await?;
        assert_eq!(control.reader().snapshot()?, expected);
        assert_eq!(control.reader().snapshot()?.entries().len(), 1);
        assert!(machine.applied_state().await.is_err());
        machine.shutdown().await?;
        machine = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
        assert_eq!(machine.applied_state().await?, state);
        assert_eq!(
            machine.apply([entries[prefix - 1].clone()]).await?,
            vec![A::AlreadyApplied {
                entry: entry_id(prefix as u64 - 1)
            }]
        );
        let results = machine.apply(entries[prefix..].to_vec()).await?;
        if matches!(fault, Fault::Before) {
            assert!(matches!(
                results[0],
                A::Refused(LogQueueRefusal::InvalidQueueConfiguration(_))
            ));
        }
        assert_eq!(control.reader().snapshot()?, model(&entries).await?.0);
        assert_eq!(
            machine
                .apply([create(3, 5, QueueConfig::default())?, send(4, 6, vec![1])?])
                .await?,
            vec![A::QueueCreated, A::Sent { sequence: 1 }]
        );
        machine.shutdown().await?;
        restore(&control, &baseline)?;
        machine = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
    }
    machine.shutdown().await?;
    Ok(())
}

async fn touched_stored_configuration_is_fatal_not_a_normal_input_refusal<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    machine
        .apply([create(0, 1, QueueConfig::default())?, send(1, 2, vec![1])?])
        .await?;
    let baseline = control.reader().snapshot()?;
    let invalid = QueueConfig {
        max_message_bytes: 0,
        ..QueueConfig::default()
    };
    control.inject(WriteBatch::default().put(
        domain::keys::queue_config(&namespace()?, &entity()?),
        domain::codec::encode(&invalid)?,
    ))?;
    let corrupted = control.reader().snapshot()?;
    let commits = control.counts().commits;
    assert!(machine.apply([send(2, 3, vec![2])?]).await.is_err());
    assert!(machine.applied_state().await.is_err());
    assert_eq!(control.counts().commits, commits);
    assert_eq!(control.reader().snapshot()?, corrupted);
    machine.shutdown().await?;
    restore(&control, &baseline)?;
    let mut reopened = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
    assert_eq!(
        reopened.apply([send(2, 3, vec![2])?]).await?,
        vec![A::Sent { sequence: 2 }]
    );
    assert_eq!(message(&control.reader(), 3)?, None);
    reopened.shutdown().await?;
    Ok(())
}

for_each_backend!(
    middle_physical_failures_preserve_exact_prefix_and_poison_without_partial_response,
    checkpoint_only_refusal_write_is_still_physically_unknown_on_error,
    touched_stored_configuration_is_fatal_not_a_normal_input_refusal,
);
