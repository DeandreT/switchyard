//! Test-owned current-role artifacts and captures, never admission authority.

use sha2::{Digest, Sha256};
use storage::{
    MemoryProtectedStateStore, MemoryReplicaStore, MemoryStore, ProtectedStatePublication,
    ProtectedStateReader, SnapshotCatalogRecord, StateStore, StoredProtectedState, WriteBatch,
};

use crate::{
    CommittedCheckpoint, CommittedCheckpointUpdate, CommittedEntryId, CommittedImageRole,
    CommittedQueueCommand, CommittedQueueWork, CommittedSend, CommittedStateMachine,
    CommittedStreamId, CreateSendImageExpectation, DecodedCommittedImage, EncodedCommittedImage,
    EntityPath, NamespaceName, QueueConfig, Timestamp,
};

pub(crate) type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
pub(crate) type Rows = Vec<(Vec<u8>, Vec<u8>)>;

pub(crate) struct Image {
    pub(crate) artifact: Vec<u8>,
    pub(crate) checkpoint: CommittedCheckpoint,
}

impl Image {
    pub(crate) fn expectation(&self) -> CreateSendImageExpectation<'_> {
        CreateSendImageExpectation {
            checkpoint: &self.checkpoint,
            artifact_bytes: self.artifact.len(),
            artifact_sha256: Sha256::digest(&self.artifact).into(),
        }
    }

    pub(crate) fn rows(&self) -> TestResult<Rows> {
        Ok(DecodedCommittedImage::decode(&self.artifact)?
            .rows()
            .map(|row| (row.key().to_vec(), row.value().to_vec()))
            .collect())
    }

    pub(crate) fn capture(
        &self,
        metadata: &[u8],
        fence: &[u8],
    ) -> TestResult<StoredProtectedState> {
        capture(&self.rows()?, &self.artifact, metadata, fence)
    }
}

pub(crate) fn current(names: &[&str], body: &[u8]) -> TestResult<Image> {
    let stream = CommittedStreamId::new([47; 16])?;
    let mut machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream)?;
    let namespace = NamespaceName::new("layout17-private-tenant")?;
    let mut works = vec![CommittedQueueWork::Membership {
        schema_version: 777,
        payload: b"layout17-private-opaque-member".to_vec(),
    }];
    for name in names {
        works.push(CommittedQueueWork::Queue(
            CommittedQueueCommand::create_queue(
                namespace.clone(),
                EntityPath::new(*name)?,
                Timestamp::from_millis(10),
                QueueConfig::default(),
            ),
        ));
    }
    for (index, name) in names.iter().enumerate() {
        works.push(CommittedQueueWork::Queue(CommittedQueueCommand::send(
            namespace.clone(),
            EntityPath::new(*name)?,
            Timestamp::from_millis(11 + index as u64),
            CommittedSend {
                message_id: format!("layout17-private-id-{index}"),
                body: body.to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        )));
    }
    for (index, work) in works.iter().enumerate() {
        machine.apply_committed(
            &CommittedCheckpointUpdate {
                stream,
                expected_previous: machine.checkpoint()?.last(),
                entry: CommittedEntryId {
                    term: 3,
                    node_id: 19,
                    index: index as u64,
                },
            },
            work,
        )?;
    }
    let artifact = machine.export_create_send_image()?.as_bytes().to_vec();
    assert_eq!(
        DecodedCommittedImage::decode(&artifact)?.role(),
        CommittedImageRole::CreateSendLayout17V1
    );
    Ok(Image {
        artifact,
        checkpoint: machine.checkpoint()?,
    })
}

pub(crate) fn initial(role: CommittedImageRole) -> TestResult<Image> {
    let stream = CommittedStreamId::new([47; 16])?;
    let machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream)?;
    let encoded = EncodedCommittedImage::encode(role, stream, &machine.reader().snapshot()?)?;
    Ok(Image {
        artifact: encoded.as_bytes().to_vec(),
        checkpoint: machine.checkpoint()?,
    })
}

// Rebuilds synthetic test artifacts; originating stores and live export code stay unchanged.
pub(crate) fn from_rows(
    role: CommittedImageRole,
    rows: Rows,
    checkpoint: CommittedCheckpoint,
) -> TestResult<Image> {
    let store = MemoryStore::default();
    let mut batch = WriteBatch::default();
    for (key, value) in rows {
        batch.push_put(key, value);
    }
    store.apply(batch)?;
    let encoded = EncodedCommittedImage::encode(role, checkpoint.stream(), &store.snapshot()?)?;
    Ok(Image {
        artifact: encoded.as_bytes().to_vec(),
        checkpoint,
    })
}

pub(crate) fn capture(
    rows: &Rows,
    artifact: &[u8],
    metadata: &[u8],
    fence: &[u8],
) -> TestResult<StoredProtectedState> {
    let borrowed: Vec<_> = rows
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect();
    let mut writer = MemoryProtectedStateStore::new();
    let catalog = SnapshotCatalogRecord::new(metadata, artifact)?;
    writer.publish(ProtectedStatePublication::new(&borrowed, catalog, fence)?)?;
    Ok(writer.reader().capture_protected_state()?)
}
