use domain::{
    CommittedCheckpointUpdate, CommittedEntryId, CommittedQueueCommand, CommittedQueueWork,
    CommittedSend, CommittedStateMachine, QueueConfig, SessionId, Timestamp,
};
use storage::{BoundedStateStore, CommittedStore};

use super::{TestResult, bootstrap_fixture as captured};

pub(super) fn populate<W>(writer: W) -> TestResult<(CommittedStateMachine<W>, W::Reader)>
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let reader = writer.reader();
    let mut machine = CommittedStateMachine::create(writer, captured::stream()?)?;
    let session = SessionId::new("s".repeat(domain::MAX_SESSION_ID_BYTES))?;
    let config = QueueConfig {
        max_message_bytes: domain::MAX_COMMITTED_BODY_BYTES,
        default_time_to_live_millis: Some(1000),
        requires_session: true,
        requires_duplicate_detection: true,
        duplicate_detection_history_time_window_millis: 20_000,
        ..QueueConfig::default()
    };
    let message = |body, session_id| CommittedSend {
        message_id: "\u{0800}".repeat(domain::MAX_MESSAGE_ID_LENGTH),
        body,
        time_to_live_millis: Some(123),
        session_id,
    };
    let body = (0..domain::MAX_COMMITTED_BODY_BYTES)
        .map(|index| (index % 251) as u8)
        .collect();
    for (index, work) in [
        CommittedQueueWork::Membership {
            schema_version: crate::experimental_log::MEMBERSHIP_SCHEMA_VERSION,
            payload: crate::experimental_log::encode_membership(&captured::members())?,
        },
        CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
            captured::namespace()?,
            captured::entity()?,
            Timestamp::from_millis(10),
            config,
        )),
        CommittedQueueWork::Queue(CommittedQueueCommand::send(
            captured::namespace()?,
            captured::entity()?,
            Timestamp::from_millis(11),
            message(body, Some(session.clone())),
        )),
        CommittedQueueWork::Queue(CommittedQueueCommand::send(
            captured::namespace()?,
            captured::entity()?,
            Timestamp::from_millis(12),
            message(b"PRIVATE-duplicate".to_vec(), Some(session)),
        )),
        CommittedQueueWork::Queue(CommittedQueueCommand::send(
            captured::namespace()?,
            captured::entity()?,
            Timestamp::from_millis(500),
            message(b"PRIVATE-refused".to_vec(), None),
        )),
    ]
    .into_iter()
    .enumerate()
    {
        machine.apply_committed(
            &CommittedCheckpointUpdate {
                stream: captured::stream()?,
                expected_previous: machine.checkpoint()?.last(),
                entry: CommittedEntryId {
                    term: 1,
                    node_id: 7,
                    index: index as u64,
                },
            },
            &work,
        )?;
    }
    Ok((machine, reader))
}

pub(super) fn reseal(bytes: &mut [u8]) {
    use sha2::{Digest, Sha256};
    let end = bytes.len() - super::super::codec::CHECKSUM_BYTES;
    let checksum = Sha256::digest(&bytes[..end]);
    bytes[end..].copy_from_slice(&checksum);
}

pub(super) fn frame(payload: &[u8]) -> Vec<u8> {
    let mut bytes = b"SWYM\x00\x01\x00\x01".to_vec();
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes.extend_from_slice(&[0; 32]);
    reseal(&mut bytes);
    bytes
}
