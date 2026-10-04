use std::collections::{BTreeMap, BTreeSet};

use domain::{
    CommittedCheckpoint, CommittedCheckpointUpdate, CommittedEntryId, CommittedEntryMark,
    CommittedImageRole, CommittedQueueCommand, CommittedQueueWork, CommittedSend,
    CommittedStateMachine, CommittedStreamId, DecodedCommittedImage, EncodedCommittedImage,
    EntityPath, NamespaceName, QueueConfig, SessionId, Timestamp, TrustedCreateSendBootstrap,
    ValidatedCreateSendImage,
};
use openraft::{BasicNode, EntryPayload, Membership};
use serde::Serialize;
use sha2::{Digest, Sha256};
use storage::{
    CommittedStore, MemoryReplicaStore, MemoryStore, StateStore, StoreSnapshot, WriteBatch,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub(super) struct Selected {
    pub image: EncodedCommittedImage,
    pub checkpoint: CommittedCheckpoint,
    pub snapshot: StoreSnapshot,
}

impl Selected {
    pub fn request(&self) -> TrustedCreateSendBootstrap<'_> {
        TrustedCreateSendBootstrap::new(
            self.checkpoint.stream(),
            &self.checkpoint,
            digest(self.image.as_bytes()),
            self.image.as_bytes(),
        )
    }
}

pub(super) fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(super) fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([7; 16])?)
}

pub(super) fn namespace() -> TestResult<NamespaceName> {
    Ok(NamespaceName::new("tenant")?)
}

pub(super) fn entity() -> TestResult<EntityPath> {
    Ok(EntityPath::new("orders")?)
}

pub(super) fn members() -> Membership<u64, BasicNode> {
    Membership::new(
        vec![BTreeSet::from([7, 8, 9])],
        BTreeMap::from([
            (7, BasicNode::new("node-7")),
            (8, BasicNode::new("node-8")),
            (9, BasicNode::new("node-9")),
        ]),
    )
}

pub(super) fn selected(maximum: bool) -> TestResult<Selected> {
    let writer = MemoryReplicaStore::new();
    let reader = writer.reader();
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let session = SessionId::new("s".repeat(domain::MAX_SESSION_ID_BYTES))?;
    let body = if maximum {
        (0..domain::MAX_COMMITTED_BODY_BYTES)
            .map(|i| (i % 251) as u8)
            .collect()
    } else {
        b"PRIVATE-original-body".to_vec()
    };
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
    for (index, work) in [
        CommittedQueueWork::Membership {
            schema_version: crate::experimental_log::MEMBERSHIP_SCHEMA_VERSION,
            payload: crate::experimental_log::encode_membership(&members())?,
        },
        CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
            namespace()?,
            entity()?,
            Timestamp::from_millis(10),
            config,
        )),
        CommittedQueueWork::Queue(CommittedQueueCommand::send(
            namespace()?,
            entity()?,
            Timestamp::from_millis(11),
            message(body, Some(session.clone())),
        )),
        CommittedQueueWork::Queue(CommittedQueueCommand::send(
            namespace()?,
            entity()?,
            Timestamp::from_millis(12),
            message(b"PRIVATE-duplicate".to_vec(), Some(session)),
        )),
        CommittedQueueWork::Queue(CommittedQueueCommand::send(
            namespace()?,
            entity()?,
            Timestamp::from_millis(500),
            message(b"PRIVATE-refused".to_vec(), None),
        )),
    ]
    .into_iter()
    .enumerate()
    {
        machine.apply_committed(
            &CommittedCheckpointUpdate {
                stream: stream()?,
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
    from_snapshot(reader.snapshot()?)
}

pub(super) fn initial() -> TestResult<Selected> {
    let writer = MemoryReplicaStore::new();
    let reader = writer.reader();
    let _machine = CommittedStateMachine::create(writer, stream()?)?;
    from_snapshot(reader.snapshot()?)
}

pub(super) fn from_snapshot(snapshot: StoreSnapshot) -> TestResult<Selected> {
    let image =
        EncodedCommittedImage::encode(CommittedImageRole::CreateSendV1, stream()?, &snapshot)?;
    let checkpoint = DecodedCommittedImage::decode(image.as_bytes())?
        .checkpoint()
        .clone();
    Ok(Selected {
        image,
        checkpoint,
        snapshot,
    })
}

pub(super) fn with_record(source: &Selected, key: &[u8], value: Vec<u8>) -> TestResult<Selected> {
    let store = MemoryStore::default();
    let mut batch = WriteBatch::default();
    for (key, value) in source.snapshot.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    batch.push_put(key.to_vec(), value);
    store.apply(batch)?;
    from_snapshot(store.snapshot()?)
}

// Frozen canonical SWYC1 is mirrored only to construct test-owned checkpoints.
// No production checkpoint mutation or raw writer capability is exposed.
#[derive(Serialize)]
pub(super) struct CheckpointWire {
    pub stream: CommittedStreamId,
    pub last: Option<CommittedEntryMark>,
    pub previous: Option<CommittedEntryMark>,
    pub highest_timestamp: u64,
    pub membership: Option<MembershipWire>,
}

#[derive(Serialize)]
pub(super) struct MembershipWire {
    pub source: CommittedEntryId,
    pub schema_version: u16,
    pub payload: Vec<u8>,
}

impl CheckpointWire {
    pub fn from_checkpoint(checkpoint: &CommittedCheckpoint) -> Self {
        Self {
            stream: checkpoint.stream(),
            last: checkpoint.last(),
            previous: checkpoint.previous(),
            highest_timestamp: checkpoint.highest_timestamp().as_millis(),
            membership: checkpoint.membership().map(|value| MembershipWire {
                source: value.source,
                schema_version: value.schema_version,
                payload: value.payload.clone(),
            }),
        }
    }
}

pub(super) fn altered_checkpoint(
    source: &Selected,
    change: impl FnOnce(&mut CheckpointWire),
) -> TestResult<Selected> {
    let mut wire = CheckpointWire::from_checkpoint(&source.checkpoint);
    change(&mut wire);
    let mut value = b"SWYC\x01".to_vec();
    value.extend_from_slice(&postcard::to_stdvec(&wire)?);
    with_record(source, &[0x12], value)
}

pub(super) fn supported(source: &Selected) -> TestResult {
    let checked = ValidatedCreateSendImage::validate(DecodedCommittedImage::decode(
        source.image.as_bytes(),
    )?)?;
    assert_eq!(checked.checkpoint(), &source.checkpoint);
    Ok(())
}

pub(super) fn next_send() -> TestResult<crate::LogEntry> {
    Ok(crate::LogEntry {
        log_id: crate::LogId::new(openraft::CommittedLeaderId::new(1, 7), 5),
        payload: EntryPayload::Normal(crate::QueueLogCommand::send(
            namespace()?,
            entity()?,
            Timestamp::from_millis(501),
            CommittedSend {
                message_id: "PRIVATE-next-id".into(),
                body: b"PRIVATE-next-body".to_vec(),
                time_to_live_millis: Some(123),
                session_id: Some(SessionId::new("s".repeat(domain::MAX_SESSION_ID_BYTES))?),
            },
        )),
    })
}
