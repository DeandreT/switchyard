use cluster::{ExperimentalStateMachine, StateMachineError};
use domain::{CommittedEntryId, CommittedEntryMark, CommittedStreamId, QueueConfig};
use openraft::storage::RaftStateMachine;
use serde::{Deserialize, Serialize};
use storage::{CommittedStore, StateStore, WriteBatch};

use super::{TestResult, fixture::*};

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

fn encoded(checkpoint: &CheckpointV1) -> TestResult<Vec<u8>> {
    let mut bytes = b"SWYC\x01".to_vec();
    bytes.extend(postcard::to_allocvec(checkpoint)?);
    Ok(bytes)
}

async fn membership_schema_payload_and_full_id_corruption_refuse_open_without_repair<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    machine
        .apply([
            create(0, 1, QueueConfig::default())?,
            membership(1),
            blank(2),
        ])
        .await?;
    machine.shutdown().await?;
    let baseline = control.reader().snapshot()?;
    let bytes = control
        .reader()
        .get(&checkpoint_key())?
        .ok_or("missing checkpoint record")?;
    assert_eq!(&bytes[..5], b"SWYC\x01");
    let original: CheckpointV1 = postcard::from_bytes(&bytes[5..])?;
    let mut schema = original.clone();
    schema
        .membership
        .as_mut()
        .ok_or("missing membership source")?
        .schema_version = 2;
    let mut payload = original.clone();
    payload
        .membership
        .as_mut()
        .ok_or("missing membership payload")?
        .payload = vec![0];
    let mut full_id = original.clone();
    full_id
        .previous
        .as_mut()
        .ok_or("missing checkpoint predecessor")?
        .id
        .node_id = 8;
    full_id
        .membership
        .as_mut()
        .ok_or("missing membership predecessor source")?
        .source
        .node_id = 8;
    let mut wrong_stream = original;
    wrong_stream.stream = CommittedStreamId::new([8; 16])?;
    let mut trailing = bytes.clone();
    trailing.push(0);
    let mut header = bytes.clone();
    header[4] = 2;
    let cases = [
        WriteBatch::default().delete(checkpoint_key()),
        WriteBatch::default().put(checkpoint_key(), encoded(&schema)?),
        WriteBatch::default().put(checkpoint_key(), encoded(&payload)?),
        WriteBatch::default().put(checkpoint_key(), encoded(&full_id)?),
        WriteBatch::default().put(checkpoint_key(), encoded(&wrong_stream)?),
        WriteBatch::default().put(checkpoint_key(), trailing),
        WriteBatch::default().put(checkpoint_key(), header),
        WriteBatch::default().put(
            checkpoint_key(),
            vec![0; domain::MAX_COMMITTED_CHECKPOINT_BYTES + 1],
        ),
    ];
    for batch in cases {
        restore(&control, &baseline)?;
        control.inject(batch)?;
        let before = control.reader().snapshot()?;
        let commits = control.counts().commits;
        assert!(matches!(
            ExperimentalStateMachine::open(control.recover_writer(), stream()?),
            Err(StateMachineError::InvalidState)
        ));
        assert_eq!(control.counts().commits, commits);
        assert_eq!(control.reader().snapshot()?, before);
    }
    restore(&control, &baseline)?;
    let mut reopened = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
    assert_eq!(reopened.applied_state().await?.0, Some(id(1, 2)));
    reopened.shutdown().await?;
    Ok(())
}

async fn runtime_checkpoint_corruption_poisoning_never_publishes_healthy_snapshot_state<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    machine
        .apply([create(0, 1, QueueConfig::default())?, membership(1)])
        .await?;
    let baseline = control.reader().snapshot()?;
    let bytes = control
        .reader()
        .get(&checkpoint_key())?
        .ok_or("missing checkpoint")?;
    let mut checkpoint: CheckpointV1 = postcard::from_bytes(&bytes[5..])?;
    checkpoint
        .membership
        .as_mut()
        .ok_or("missing stored membership")?
        .schema_version = 99;
    control.inject(WriteBatch::default().put(checkpoint_key(), encoded(&checkpoint)?))?;
    let before = control.reader().snapshot()?;
    let commits = control.counts().commits;
    assert!(machine.applied_state().await.is_err());
    assert!(machine.get_current_snapshot().await.is_err());
    assert!(machine.apply([blank(2)]).await.is_err());
    assert_eq!(control.counts().commits, commits);
    assert_eq!(control.reader().snapshot()?, before);
    machine.shutdown().await?;
    restore(&control, &baseline)?;
    let mut reopened = ExperimentalStateMachine::open(control.recover_writer(), stream()?)?;
    assert_eq!(
        reopened.apply([blank(2)]).await?,
        vec![cluster::LogApplication::CheckpointOnly]
    );
    reopened.shutdown().await?;
    Ok(())
}

for_each_backend!(
    membership_schema_payload_and_full_id_corruption_refuse_open_without_repair,
    runtime_checkpoint_corruption_poisoning_never_publishes_healthy_snapshot_state,
);
