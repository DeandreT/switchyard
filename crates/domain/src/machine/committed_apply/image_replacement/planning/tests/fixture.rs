use sha2::{Digest, Sha256};
use storage::{MemoryReplicaStore, MemoryStore, StateStore, WriteBatch};

use crate::{
    CommittedCheckpoint, CommittedCheckpointUpdate, CommittedEntryId, CommittedImageRole,
    CommittedQueueCommand, CommittedQueueWork, CommittedSend, CommittedStateMachine,
    CommittedStreamId, DecodedCommittedImage, EncodedCommittedImage, EntityPath, NamespaceName,
    QueueConfig, SequenceNumber, Timestamp, codec, keys,
};

use super::super::{CreateSendImageExpectation, CreateSendReplacementPlanError as Error};

pub(super) type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
pub(super) type Rows = Vec<(Vec<u8>, Vec<u8>)>;

pub(super) struct Image {
    pub(super) artifact: Vec<u8>,
    pub(super) checkpoint: CommittedCheckpoint,
}

impl Image {
    pub(super) fn initial() -> TestResult<Self> {
        let mut machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
        from_machine(&mut machine, &[])
    }

    pub(super) fn populated(names: &[&str], body: &[u8]) -> TestResult<Self> {
        let mut machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
        append(
            &mut machine,
            CommittedQueueWork::Membership {
                schema_version: 999,
                payload: b"opaque-domain-member-sentinel".to_vec(),
            },
        )?;
        let namespace = NamespaceName::new("domain-plan-sentinel")?;
        let mut queues = Vec::new();
        for name in names {
            let entity = EntityPath::new(*name)?;
            queues.push((namespace.clone(), entity.clone()));
            append(
                &mut machine,
                CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
                    namespace.clone(),
                    entity,
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
                        message_id: format!("message-sentinel-{index}"),
                        body: body.to_vec(),
                        time_to_live_millis: None,
                        session_id: None,
                    },
                )),
            )?;
        }
        from_machine(&mut machine, &queues)
    }

    pub(super) fn one() -> TestResult<Self> {
        Self::populated(&["queue-sentinel"], b"body-sentinel")
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

    pub(super) fn with_checkpoint(&self, checkpoint: CommittedCheckpoint) -> TestResult<Self> {
        let mut rows = self.rows()?;
        let value = crate::committed::encode_checkpoint(&checkpoint)?;
        let row = rows
            .iter_mut()
            .find(|(key, _)| *key == keys::committed_checkpoint())
            .ok_or("missing checkpoint fixture row")?;
        row.1 = value;
        from_rows(rows, checkpoint)
    }

    pub(super) fn different_body(&self) -> TestResult<Self> {
        let mut rows = self.rows()?;
        let row = rows
            .iter_mut()
            .find(|(key, _)| key.first() == Some(&3))
            .ok_or("missing message fixture row")?;
        let mut message: crate::MessageRecord = codec::decode(&row.1)?;
        *message.body.first_mut().ok_or("missing body byte")? ^= 1;
        row.1 = codec::encode(&message)?;
        from_rows(rows, self.checkpoint.clone())
    }

    pub(super) fn no_member(&self) -> TestResult<Self> {
        let mut checkpoint = self.checkpoint.clone();
        checkpoint.membership = None;
        self.with_checkpoint(checkpoint)
    }

    pub(super) fn earlier(&self) -> TestResult<Self> {
        let mut checkpoint = self.checkpoint.clone();
        checkpoint.last = checkpoint.previous;
        checkpoint.previous = None;
        // A one-queue fixture has member/create/send indices 0/1/2.
        let last = checkpoint.last.as_mut().ok_or("missing predecessor")?;
        last.id.index = 0;
        let member = checkpoint.membership.as_mut().ok_or("missing member")?;
        member.source = last.id;
        self.with_checkpoint(checkpoint)
    }
}

pub(super) fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(super) fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([39; 16])?)
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

fn from_machine(
    machine: &mut CommittedStateMachine<MemoryReplicaStore>,
    queues: &[(NamespaceName, EntityPath)],
) -> TestResult<Image> {
    // Build test-owned historical rows, not a relabeled current export.
    let snapshot = machine.reader().snapshot()?;
    let mut historical_keys = vec![keys::committed_checkpoint()];
    if !queues.is_empty() {
        historical_keys.push(keys::clock());
    }
    let mut mode_keys = Vec::new();
    for (namespace, entity) in queues {
        let sequence = SequenceNumber::new(1);
        historical_keys.extend([
            keys::queue_config(namespace, entity),
            keys::queue_config(namespace, &entity.dead_letter_queue()?),
            keys::queue_counters(namespace, entity),
            keys::message(namespace, entity, sequence),
            keys::ready(namespace, entity, sequence),
            keys::entity_incarnation(namespace, entity),
        ]);
        mode_keys.push(keys::queue_capacity_mode(namespace, entity));
    }
    historical_keys.sort();
    let mut current_keys = historical_keys.clone();
    current_keys.extend(mode_keys);
    current_keys.sort();
    assert_eq!(
        snapshot
            .entries()
            .iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>(),
        current_keys.iter().collect::<Vec<_>>(),
        "historical fixture setup contains unexpected or missing current rows"
    );
    let mut rows = Vec::new();
    for key in historical_keys {
        rows.push(
            snapshot
                .entries()
                .iter()
                .find(|(found, _)| found == &key)
                .ok_or("missing historical fixture row")?
                .clone(),
        );
    }
    from_rows(rows, machine.checkpoint()?)
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

// Frozen test-owned framing permits malformed container cases. Production uses
// only the established container/business parsers, never this fixture helper.
pub(super) fn unchecked_rows(
    rows: &[(Vec<u8>, Vec<u8>)],
    stream: CommittedStreamId,
) -> TestResult<Vec<u8>> {
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
    bytes[28..32].copy_from_slice(&u32::MAX.to_be_bytes());
    cases.push((bytes, Error::LimitExceeded));
    let mut rows = image.rows()?;
    rows.reverse();
    cases.push((
        unchecked_rows(&rows, image.checkpoint.stream())?,
        Error::InvalidImage,
    ));
    let mut rows = image.rows()?;
    let checkpoint = rows
        .iter_mut()
        .find(|(key, _)| *key == keys::committed_checkpoint())
        .ok_or("missing checkpoint row")?;
    checkpoint.1 = b"SWYC".to_vec();
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

pub(super) fn checkpoint_variants(
    checkpoint: &CommittedCheckpoint,
) -> TestResult<Vec<CommittedCheckpoint>> {
    let mut variants = Vec::new();
    for part in 0..19 {
        let mut changed = checkpoint.clone();
        match part {
            0 => changed.last.as_mut().ok_or("last")?.id.term += 1,
            1 => changed.last.as_mut().ok_or("last")?.id.node_id += 1,
            2 => changed.last.as_mut().ok_or("last")?.id.index += 1,
            3 => changed.last.as_mut().ok_or("last")?.fingerprint[0] ^= 1,
            4 => changed.previous.as_mut().ok_or("previous")?.id.term += 1,
            5 => changed.previous.as_mut().ok_or("previous")?.id.node_id += 1,
            6 => changed.previous.as_mut().ok_or("previous")?.id.index += 1,
            7 => changed.previous.as_mut().ok_or("previous")?.fingerprint[0] ^= 1,
            8 => changed.highest_timestamp = Timestamp::from_millis(100),
            9 => changed.membership.as_mut().ok_or("member")?.source.term += 1,
            10 => changed.membership.as_mut().ok_or("member")?.source.node_id += 1,
            11 => changed.membership.as_mut().ok_or("member")?.source.index += 1,
            12 => changed.membership.as_mut().ok_or("member")?.schema_version += 1,
            13 => changed.membership.as_mut().ok_or("member")?.payload.push(0),
            14 => changed.last = None,
            15 => changed.previous = None,
            16 => changed.membership = None,
            17 => changed.membership.as_mut().ok_or("member")?.schema_version = 0,
            _ => changed.stream = CommittedStreamId::new([40; 16])?,
        }
        variants.push(changed);
    }
    Ok(variants)
}
