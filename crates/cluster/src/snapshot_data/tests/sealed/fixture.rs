use domain::{
    CommittedCheckpointUpdate, CommittedEntryId, CommittedImageRole, CommittedQueueCommand,
    CommittedQueueWork, CommittedSend, CommittedStateMachine, CommittedStreamId,
    EncodedCommittedImage, EntityPath, MAX_COMMITTED_BODY_BYTES, NamespaceName, QueueConfig,
    Timestamp,
};
use storage::{CommittedStore, MemoryReplicaStore, MemoryStore, StateStore, WriteBatch};

use super::TestResult;

pub(super) fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([7; 16])?)
}

pub(super) fn image(body: &[u8]) -> TestResult<EncodedCommittedImage> {
    let mut machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
    populate(&mut machine, body)?;
    Ok(machine.export_create_send_image()?)
}

pub(super) fn populate<W: CommittedStore>(
    machine: &mut CommittedStateMachine<W>,
    body: &[u8],
) -> TestResult {
    let works = [
        CommittedQueueWork::Membership {
            schema_version: 999,
            payload: b"private-opaque-membership".to_vec(),
        },
        CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(10),
            QueueConfig {
                max_message_bytes: MAX_COMMITTED_BODY_BYTES,
                ..QueueConfig::default()
            },
        )),
        CommittedQueueWork::Queue(CommittedQueueCommand::send(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(11),
            CommittedSend {
                message_id: "private-message-id".into(),
                body: body.to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    ];
    for (index, work) in works.iter().enumerate() {
        machine.apply_committed(
            &CommittedCheckpointUpdate {
                stream: stream()?,
                expected_previous: machine.checkpoint()?.last(),
                entry: CommittedEntryId {
                    term: 1,
                    node_id: 9,
                    index: index as u64,
                },
            },
            work,
        )?;
    }
    Ok(())
}

pub(super) fn structural_only_image() -> TestResult<EncodedCommittedImage> {
    let machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
    let source = machine.reader().snapshot()?;
    let raw = MemoryStore::default();
    let mut batch = WriteBatch::default();
    for (key, value) in source.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    batch.push_put(vec![0x7f], b"private-unsupported-business-row".to_vec());
    raw.apply(batch)?;
    Ok(EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendV1,
        stream()?,
        &raw.snapshot()?,
    )?)
}
