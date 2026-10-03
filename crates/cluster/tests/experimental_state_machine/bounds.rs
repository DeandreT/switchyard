use std::collections::{BTreeMap, BTreeSet};

use cluster::{
    ExperimentalStateMachine, LogApplication as A, LogEntry, MAX_APPLY_ENTRIES, MAX_LOG_BODY_BYTES,
};
use domain::QueueConfig;
use openraft::{BasicNode, EntryPayload, Membership, storage::RaftStateMachine};
use storage::{CommittedStore, StateStore};

use super::{TestResult, fixture::*};

async fn complete_256_entry_packet_exceeds_append_limits_and_returns_one_typed_result_each<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    let before = control.reader().snapshot()?;
    let mut entries = Vec::with_capacity(MAX_APPLY_ENTRIES);
    entries.push(create(0, 1, QueueConfig::default())?);
    for index in 1..MAX_APPLY_ENTRIES as u64 {
        entries.push(send(index, 2, vec![0x41; MAX_LOG_BODY_BYTES])?);
    }
    let gate = control.gate_after(1);
    let mut apply = Box::pin(machine.apply(entries));
    pending(apply.as_mut()).await?;
    gate.entered().await?;
    assert_eq!(control.reader().snapshot()?, before);
    gate.release();
    let responses = apply.await?;
    assert_eq!(responses.len(), MAX_APPLY_ENTRIES);
    assert_eq!(responses[0], A::QueueCreated);
    for (index, response) in responses.iter().enumerate().skip(1) {
        assert_eq!(
            *response,
            A::Sent {
                sequence: index as u64
            }
        );
    }
    assert_eq!(
        machine.applied_state().await?.0,
        Some(id(1, MAX_APPLY_ENTRIES as u64 - 1))
    );
    assert_eq!(workload(&machine, 0).await?.encoded_bytes, 0);
    assert_eq!(control.counts().commits, MAX_APPLY_ENTRIES + 1);
    assert_eq!(
        message(&control.reader(), 255)?
            .ok_or("missing last full-packet allocation")?
            .body,
        vec![0x41; MAX_LOG_BODY_BYTES]
    );
    assert_eq!(message(&control.reader(), 256)?, None);
    machine.shutdown().await?;
    let mut reopened = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
    assert_eq!(
        reopened
            .apply([send(255, 2, vec![0x41; MAX_LOG_BODY_BYTES])?])
            .await?,
        vec![A::AlreadyApplied {
            entry: entry_id(255)
        }]
    );
    assert_eq!(
        reopened.apply([send(256, 3, vec![7])?]).await?,
        vec![A::Sent { sequence: 256 }]
    );
    reopened.shutdown().await?;
    Ok(())
}

async fn count_bytes_and_late_invalid_entry_refuse_the_entire_input_before_owner_io<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    let before = control.reader().snapshot()?;
    control.reset_reads();
    let counts = control.counts();
    assert!(
        machine
            .apply((0..=MAX_APPLY_ENTRIES as u64).map(blank))
            .await
            .is_err()
    );
    assert_eq!(control.counts(), counts);
    let full_bytes = (0..MAX_APPLY_ENTRIES as u64)
        .map(|index| send(index, 2, vec![0x51; MAX_LOG_BODY_BYTES]))
        .collect::<TestResult<Vec<_>>>()?;
    assert!(machine.apply(full_bytes).await.is_err());
    assert_eq!(control.counts(), counts);
    assert!(
        machine
            .apply([
                create(0, 1, QueueConfig::default())?,
                send(1, 2, vec![1])?,
                send(2, 2, vec![0; MAX_LOG_BODY_BYTES + 1])?
            ])
            .await
            .is_err()
    );
    assert_eq!(control.counts(), counts);
    let bad_member = Membership::new(
        vec![BTreeSet::from([1])],
        BTreeMap::from([(1, BasicNode::new("private-long-address".repeat(30)))]),
    );
    assert!(
        machine
            .apply([
                create(0, 1, QueueConfig::default())?,
                LogEntry {
                    log_id: id(1, 1),
                    payload: EntryPayload::Membership(bad_member)
                }
            ])
            .await
            .is_err()
    );
    assert_eq!(control.counts(), counts);
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(machine.applied_state().await?.0, None);
    assert_eq!(
        machine
            .apply([create(0, 1, QueueConfig::default())?])
            .await?,
        vec![A::QueueCreated]
    );
    machine.shutdown().await?;
    Ok(())
}

async fn one_large_trait_call_returns_all_results_not_an_append_sized_prefix<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    let mut entries = vec![create(0, 1, QueueConfig::default())?];
    for index in 1..40 {
        entries.push(send(index, 2, vec![0x61; MAX_LOG_BODY_BYTES / 2])?);
    }
    let responses = machine.apply(entries).await?;
    assert_eq!(responses.len(), 40);
    assert_eq!(responses[0], A::QueueCreated);
    for (index, response) in responses.iter().enumerate().skip(1) {
        assert_eq!(
            *response,
            A::Sent {
                sequence: index as u64
            }
        );
    }
    assert_eq!(control.counts().commits, 41);
    assert_eq!(machine.applied_state().await?.0, Some(id(1, 39)));
    assert_eq!(workload(&machine, 0).await?.encoded_bytes, 0);
    machine.shutdown().await?;
    Ok(())
}

for_each_backend!(
    complete_256_entry_packet_exceeds_append_limits_and_returns_one_typed_result_each,
    count_bytes_and_late_invalid_entry_refuse_the_entire_input_before_owner_io,
    one_large_trait_call_returns_all_results_not_an_append_sized_prefix,
);
