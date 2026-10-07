use super::*;

use crate::DecodedCommittedImage;
use sha2::{Digest, Sha256};

use crate::{
    CommittedCheckpointUpdate, CommittedEntryId, CommittedQueueCommand, CommittedQueueWork,
    CommittedSend, EntityPath, NamespaceName, QueueConfig, Timestamp,
};
use storage::MemoryReplicaStore;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn selected() -> TestResult<(crate::EncodedCommittedImage, CommittedCheckpoint)> {
    let stream = CommittedStreamId::new([9; 16])?;
    let mut machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream)?;
    for (index, work) in [
        CommittedQueueWork::Membership {
            schema_version: 7,
            payload: b"PRIVATE-MEMBER".to_vec(),
        },
        CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(10),
            QueueConfig::default(),
        )),
        CommittedQueueWork::Queue(CommittedQueueCommand::send(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(11),
            CommittedSend {
                message_id: "PRIVATE-ID".into(),
                body: b"PRIVATE-BODY".to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    ]
    .iter()
    .enumerate()
    {
        machine.apply_committed(
            &CommittedCheckpointUpdate {
                stream,
                expected_previous: machine.checkpoint()?.last(),
                entry: CommittedEntryId {
                    term: 1,
                    node_id: 7,
                    index: index as u64,
                },
            },
            work,
        )?;
    }
    let image = machine.export_create_send_image()?;
    let checkpoint = DecodedCommittedImage::decode(image.as_bytes())?
        .checkpoint()
        .clone();
    Ok((image, checkpoint))
}

struct NoTargetIo;

impl CommittedStore for NoTargetIo {
    type Reader = <MemoryReplicaStore as CommittedStore>::Reader;
    fn reader(&self) -> Self::Reader {
        panic!("selection refusal reached target reader")
    }
    fn is_initialized(&self) -> std::result::Result<bool, storage::StorageError> {
        panic!("selection refusal reached target metadata")
    }
    fn commit(&mut self, _: WriteBatch) -> std::result::Result<(), storage::StorageError> {
        panic!("selection refusal reached target commit")
    }
}

#[test]
fn every_full_checkpoint_component_is_pinned_before_target_access() -> TestResult {
    let (image, checkpoint) = selected()?;
    let digest = Sha256::digest(image.as_bytes()).into();
    let mut variants = Vec::new();
    let mut changed = checkpoint.clone();
    changed.last.as_mut().ok_or("missing last")?.id.node_id += 1;
    variants.push(changed);
    let mut changed = checkpoint.clone();
    changed.last.as_mut().ok_or("missing last")?.id.term += 1;
    variants.push(changed);
    let mut changed = checkpoint.clone();
    changed.last.as_mut().ok_or("missing last")?.id.index += 1;
    variants.push(changed);
    let mut changed = checkpoint.clone();
    changed.last.as_mut().ok_or("missing last")?.fingerprint[0] ^= 1;
    variants.push(changed);
    let mut changed = checkpoint.clone();
    changed.previous.as_mut().ok_or("missing previous")?.id.term += 1;
    variants.push(changed);
    let mut changed = checkpoint.clone();
    changed
        .previous
        .as_mut()
        .ok_or("missing previous")?
        .fingerprint[0] ^= 1;
    variants.push(changed);
    let mut changed = checkpoint.clone();
    changed.highest_timestamp = Timestamp::from_millis(12);
    variants.push(changed);
    let mut changed = checkpoint.clone();
    changed
        .membership
        .as_mut()
        .ok_or("missing membership")?
        .source
        .node_id += 1;
    variants.push(changed);
    let mut changed = checkpoint.clone();
    changed
        .membership
        .as_mut()
        .ok_or("missing membership")?
        .schema_version += 1;
    variants.push(changed);
    let mut changed = checkpoint.clone();
    changed
        .membership
        .as_mut()
        .ok_or("missing membership")?
        .payload
        .push(0);
    variants.push(changed);
    for changed in variants {
        let selection = TrustedCreateSendBootstrap::new(
            checkpoint.stream(),
            &changed,
            digest,
            image.as_bytes(),
        );
        assert_eq!(
            CommittedStateMachine::bootstrap_create_send_image(NoTargetIo, selection).err(),
            Some(CommittedImageBootstrapError::SelectionMismatch)
        );
    }
    Ok(())
}

#[test]
fn digest_names_the_complete_artifact_including_its_checksum() -> TestResult {
    let (image, checkpoint) = selected()?;
    let wrong = Sha256::digest(&image.as_bytes()[..image.len() - 32]).into();
    let selection =
        TrustedCreateSendBootstrap::new(checkpoint.stream(), &checkpoint, wrong, image.as_bytes());
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(NoTargetIo, selection).err(),
        Some(CommittedImageBootstrapError::SelectionMismatch)
    );
    let selection = TrustedCreateSendBootstrap::new(
        checkpoint.stream(),
        &checkpoint,
        Sha256::digest(image.as_bytes()).into(),
        image.as_bytes(),
    );
    assert_eq!(validate_selection(&selection)?.checkpoint(), &checkpoint);
    Ok(())
}

#[test]
fn invalid_deserialized_stream_selection_reaches_no_target_api() -> TestResult {
    let (image, checkpoint) = selected()?;
    let invalid: CommittedStreamId = postcard::from_bytes(&postcard::to_stdvec(&[0u8; 16])?)?;
    let selection = TrustedCreateSendBootstrap::new(
        invalid,
        &checkpoint,
        Sha256::digest(image.as_bytes()).into(),
        image.as_bytes(),
    );
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(NoTargetIo, selection).err(),
        Some(CommittedImageBootstrapError::InvalidSelection)
    );
    Ok(())
}

#[test]
fn selection_and_every_error_are_static_and_source_private() -> TestResult {
    let (image, checkpoint) = selected()?;
    let selection = TrustedCreateSendBootstrap::new(
        checkpoint.stream(),
        &checkpoint,
        Sha256::digest(image.as_bytes()).into(),
        image.as_bytes(),
    );
    assert_eq!(
        format!("{selection:?}"),
        "TrustedCreateSendBootstrap { .. }"
    );
    for error in [
        CommittedImageBootstrapError::InvalidSelection,
        CommittedImageBootstrapError::SelectionMismatch,
        CommittedImageBootstrapError::LimitExceeded,
        CommittedImageBootstrapError::Allocation,
        CommittedImageBootstrapError::UnsupportedProfile,
        CommittedImageBootstrapError::InvalidImage,
        CommittedImageBootstrapError::TargetNotPristine,
        CommittedImageBootstrapError::TargetReadFailed,
        CommittedImageBootstrapError::CommitUnknown,
    ] {
        assert!(!format!("{error:?}: {error}").contains("PRIVATE"));
    }
    Ok(())
}

// Test-owned malformed source artifacts, never a live export or admission path.
pub(super) fn closed_profile_refusals() -> TestResult<
    Vec<(
        crate::EncodedCommittedImage,
        CommittedCheckpoint,
        CommittedImageBootstrapError,
    )>,
> {
    let mut cases = Vec::new();
    let stream = CommittedStreamId::new([9; 16])?;
    let machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream)?;
    let initial = crate::EncodedCommittedImage::encode(
        crate::CommittedImageRole::CreateSendV1,
        stream,
        &machine.reader().snapshot()?,
    )?;
    let decoded = DecodedCommittedImage::decode(initial.as_bytes())?;
    let checkpoint = decoded.checkpoint().clone();
    assert!(crate::ValidatedCreateSendImage::validate(decoded).is_ok());
    cases.push((
        initial,
        checkpoint,
        CommittedImageBootstrapError::UnsupportedProfile,
    ));
    let (current, checkpoint) = selected()?;
    for (mode, expected) in [
        (None, CommittedImageBootstrapError::InvalidImage),
        (
            Some(vec![11, 1, 1, 0, 0]),
            CommittedImageBootstrapError::InvalidImage,
        ),
        (
            Some(vec![11, 1, 1, 1, 1]),
            CommittedImageBootstrapError::UnsupportedProfile,
        ),
    ] {
        let store = storage::MemoryStore::default();
        let mut batch = WriteBatch::default();
        let decoded = DecodedCommittedImage::decode(current.as_bytes())?;
        let mut found = 0;
        for row in decoded.rows() {
            if row.key().first() == Some(&0x16) {
                found += 1;
                if let Some(value) = &mode {
                    batch.push_put(row.key().to_vec(), value.clone());
                }
            } else {
                batch.push_put(row.key().to_vec(), row.value().to_vec());
            }
        }
        assert_eq!(found, 1);
        store.apply(batch)?;
        let image = crate::EncodedCommittedImage::encode(
            crate::CommittedImageRole::CreateSendLayout17V1,
            stream,
            &store.snapshot()?,
        )?;
        cases.push((image, checkpoint.clone(), expected));
    }
    Ok(cases)
}

#[test]
fn historical_role1_and_missing_malformed_or_finite_modes_reach_no_target_api() -> TestResult {
    for (image, checkpoint, expected) in closed_profile_refusals()? {
        let request = TrustedCreateSendBootstrap::new(
            checkpoint.stream(),
            &checkpoint,
            Sha256::digest(image.as_bytes()).into(),
            image.as_bytes(),
        );
        assert_eq!(
            CommittedStateMachine::bootstrap_create_send_image(NoTargetIo, request).err(),
            Some(expected)
        );
    }
    Ok(())
}
