use sha2::{Digest, Sha256};
use storage::{
    MemoryProtectedStateStore, MemoryReplicaStore, MemoryStore, ProtectedStatePublication,
    ProtectedStateReader, SnapshotCatalogRecord, StateStore, StoredProtectedState, WriteBatch,
};

use crate::{
    CommittedCheckpoint, CommittedCheckpointUpdate, CommittedEntryId, CommittedImageRole,
    CommittedQueueCommand, CommittedQueueWork, CommittedSend, CommittedStateMachine,
    CommittedStreamId, CreateSendImageExpectation, DecodedCommittedImage, EncodedCommittedImage,
    EntityPath, NamespaceName, ProtectedCreateSendImageError as Error, QueueConfig, Timestamp,
    codec, keys,
};

pub(super) type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
pub(super) type Rows = Vec<(Vec<u8>, Vec<u8>)>;

pub(super) struct Image {
    pub(super) artifact: Vec<u8>,
    pub(super) checkpoint: CommittedCheckpoint,
}

impl Image {
    pub(super) fn initial() -> TestResult<Self> {
        let mut machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
        from_machine(&mut machine)
    }

    pub(super) fn populated(names: &[&str], body: &[u8]) -> TestResult<Self> {
        let mut machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
        append(
            &mut machine,
            CommittedQueueWork::Membership {
                schema_version: 777,
                payload: b"opaque-protected-member-sentinel".to_vec(),
            },
        )?;
        let namespace = NamespaceName::new("protected-domain-sentinel")?;
        for name in names {
            append(
                &mut machine,
                CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
                    namespace.clone(),
                    EntityPath::new(*name)?,
                    Timestamp::from_millis(10),
                    QueueConfig::default(),
                )),
            )?;
        }
        for (index, name) in names.iter().enumerate() {
            append(
                &mut machine,
                CommittedQueueWork::Queue(CommittedQueueCommand::send(
                    namespace.clone(),
                    EntityPath::new(*name)?,
                    Timestamp::from_millis(11 + index as u64),
                    CommittedSend {
                        message_id: format!("protected-message-sentinel-{index}"),
                        body: body.to_vec(),
                        time_to_live_millis: None,
                        session_id: None,
                    },
                )),
            )?;
        }
        from_machine(&mut machine)
    }

    pub(super) fn one() -> TestResult<Self> {
        Self::populated(&["protected-queue-sentinel"], b"protected-body-sentinel")
    }

    pub(super) fn expectation(&self) -> CreateSendImageExpectation<'_> {
        CreateSendImageExpectation {
            checkpoint: &self.checkpoint,
            artifact_bytes: self.artifact.len(),
            artifact_sha256: digest(&self.artifact),
        }
    }

    pub(super) fn rows(&self) -> TestResult<Rows> {
        Ok(DecodedCommittedImage::decode(&self.artifact)?
            .rows()
            .map(|row| (row.key().to_vec(), row.value().to_vec()))
            .collect())
    }

    pub(super) fn capture(
        &self,
        metadata: &[u8],
        fence: &[u8],
    ) -> TestResult<StoredProtectedState> {
        capture(&self.rows()?, &self.artifact, metadata, fence)
    }

    pub(super) fn no_member(&self) -> TestResult<Self> {
        let mut checkpoint = self.checkpoint.clone();
        checkpoint.membership = None;
        let mut rows = self.rows()?;
        rows.iter_mut()
            .find(|(key, _)| *key == keys::committed_checkpoint())
            .ok_or("missing checkpoint")?
            .1 = crate::committed::encode_checkpoint(&checkpoint)?;
        from_rows(rows, checkpoint)
    }

    pub(super) fn different_body(&self) -> TestResult<Self> {
        let mut rows = self.rows()?;
        let row = rows
            .iter_mut()
            .find(|(key, _)| key.first() == Some(&3))
            .ok_or("missing message")?;
        let mut message: crate::MessageRecord = codec::decode(&row.1)?;
        *message.body.first_mut().ok_or("missing body")? ^= 1;
        row.1 = codec::encode(&message)?;
        from_rows(rows, self.checkpoint.clone())
    }
}

pub(super) fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([41; 16])?)
}

fn append(
    machine: &mut CommittedStateMachine<MemoryReplicaStore>,
    work: CommittedQueueWork,
) -> TestResult {
    let previous = machine.checkpoint()?.last();
    machine.apply_committed(
        &CommittedCheckpointUpdate {
            stream: stream()?,
            expected_previous: previous,
            entry: CommittedEntryId {
                term: 3,
                node_id: 19,
                index: previous.map_or(0, |mark| mark.id.index + 1),
            },
        },
        &work,
    )?;
    Ok(())
}

fn from_machine(machine: &mut CommittedStateMachine<MemoryReplicaStore>) -> TestResult<Image> {
    // Ordinary replica/storage is fixture setup, never a protected writer adapter.
    Ok(Image {
        checkpoint: machine.checkpoint()?,
        artifact: machine.export_create_send_image()?.as_bytes().to_vec(),
    })
}

pub(super) fn from_rows(rows: Rows, checkpoint: CommittedCheckpoint) -> TestResult<Image> {
    let store = MemoryStore::default();
    let mut batch = WriteBatch::default();
    for (key, value) in rows {
        batch.push_put(key, value);
    }
    store.apply(batch)?;
    let encoded = EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendV1,
        checkpoint.stream(),
        &store.snapshot()?,
    )?;
    Ok(Image {
        artifact: encoded.as_bytes().to_vec(),
        checkpoint,
    })
}

pub(super) fn publish(
    writer: &mut MemoryProtectedStateStore,
    rows: &Rows,
    artifact: &[u8],
    metadata: &[u8],
    fence: &[u8],
) -> TestResult {
    let borrowed: Vec<_> = rows
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect();
    let catalog = SnapshotCatalogRecord::new(metadata, artifact)?;
    writer.publish(ProtectedStatePublication::new(&borrowed, catalog, fence)?)?;
    Ok(())
}

pub(super) fn capture(
    rows: &Rows,
    artifact: &[u8],
    metadata: &[u8],
    fence: &[u8],
) -> TestResult<StoredProtectedState> {
    let mut writer = MemoryProtectedStateStore::new();
    publish(&mut writer, rows, artifact, metadata, fence)?;
    Ok(writer.reader().capture_protected_state()?)
}

// Test-owned malformed framing only; production always uses the existing decoder.
fn unchecked_rows(rows: &Rows, stream: CommittedStreamId) -> TestResult<Vec<u8>> {
    let mut bytes = b"SWYI".to_vec();
    bytes.extend_from_slice(&1u16.to_be_bytes());
    bytes.extend_from_slice(&1u16.to_be_bytes());
    bytes.extend_from_slice(stream.as_bytes());
    bytes.extend_from_slice(&u32::try_from(rows.len())?.to_be_bytes());
    for (key, value) in rows {
        bytes.extend_from_slice(&u32::try_from(key.len())?.to_be_bytes());
        bytes.extend_from_slice(&u32::try_from(value.len())?.to_be_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
    }
    bytes.extend_from_slice(&digest(&bytes));
    Ok(bytes)
}

pub(super) fn malformed_containers(image: &Image) -> TestResult<Vec<(Vec<u8>, Error)>> {
    let mut cases = vec![
        (Vec::new(), Error::InvalidImage),
        (
            image.artifact[..image.artifact.len() - 1].to_vec(),
            Error::InvalidImage,
        ),
    ];
    let mut bytes = image.artifact.clone();
    bytes.push(0);
    cases.push((bytes, Error::InvalidImage));
    let mut bytes = image.artifact.clone();
    bytes[0] ^= 1;
    cases.push((bytes, Error::UnsupportedProfile));
    let mut bytes = image.artifact.clone();
    let end = bytes.len();
    bytes[end - 1] ^= 1;
    cases.push((bytes, Error::InvalidImage));
    let mut bytes = image.artifact.clone();
    bytes[24..28].copy_from_slice(&u32::MAX.to_be_bytes());
    cases.push((bytes, Error::LimitExceeded));
    let mut rows = image.rows()?;
    rows.reverse();
    cases.push((
        unchecked_rows(&rows, image.checkpoint.stream())?,
        Error::InvalidImage,
    ));
    let mut rows = image.rows()?;
    rows.iter_mut()
        .find(|(key, _)| *key == keys::committed_checkpoint())
        .ok_or("missing checkpoint")?
        .1 = b"SWYC".to_vec();
    cases.push((
        unchecked_rows(&rows, image.checkpoint.stream())?,
        Error::InvalidImage,
    ));
    let mut rows = image.rows()?;
    rows.push(rows.last().ok_or("missing last row")?.clone());
    cases.push((
        unchecked_rows(&rows, image.checkpoint.stream())?,
        Error::InvalidImage,
    ));
    Ok(cases)
}

pub(super) fn malformed_business(image: &Image) -> TestResult<Vec<Image>> {
    let mut clock = image.rows()?;
    clock
        .iter_mut()
        .find(|(key, _)| *key == keys::clock())
        .ok_or("missing clock")?
        .1 = vec![11, 0x80];
    let mut index = image.rows()?;
    index
        .iter_mut()
        .find(|(key, _)| key.first() == Some(&4))
        .ok_or("missing ready")?
        .1 = vec![1];
    let mut relation = image.rows()?;
    let message = relation
        .iter()
        .position(|(key, _)| key.first() == Some(&3))
        .ok_or("missing message")?;
    relation.remove(message);
    Ok(vec![
        from_rows(clock, image.checkpoint.clone())?,
        from_rows(index, image.checkpoint.clone())?,
        from_rows(relation, image.checkpoint.clone())?,
    ])
}
