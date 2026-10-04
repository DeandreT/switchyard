use sha2::{Digest, Sha256};
use storage::{CommittedStore, MAX_CATALOG_METADATA_BYTES, MemoryCatalogReplicaStore, WriteBatch};

use super::*;
use crate::{
    CommittedCheckpoint, CommittedCheckpointUpdate, CommittedEntryId, CommittedQueueCommand,
    CommittedQueueWork, CommittedSend, CommittedStreamId, EncodedCommittedImage, EntityPath,
    NamespaceName, QueueConfig, Timestamp,
};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn selected() -> TestResult<(EncodedCommittedImage, CommittedCheckpoint)> {
    let stream = CommittedStreamId::new([9; 16])?;
    let mut machine = CommittedStateMachine::create(MemoryCatalogReplicaStore::new(), stream)?;
    let works = [
        CommittedQueueWork::Membership {
            schema_version: 999,
            payload: b"PRIVATE-OPAQUE-MEMBER".to_vec(),
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
    ];
    for (index, work) in works.iter().enumerate() {
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
    let checkpoint = crate::DecodedCommittedImage::decode(image.as_bytes())?
        .checkpoint()
        .clone();
    Ok((image, checkpoint))
}

struct NoTargetIo;

impl CommittedStore for NoTargetIo {
    type Reader = <MemoryCatalogReplicaStore as CommittedStore>::Reader;

    fn reader(&self) -> Self::Reader {
        panic!("source refusal reached target reader")
    }

    fn is_initialized(&self) -> std::result::Result<bool, storage::StorageError> {
        panic!("source refusal reached target initialization")
    }

    fn commit(&mut self, _: WriteBatch) -> std::result::Result<(), storage::StorageError> {
        panic!("combined source refusal reached ordinary commit")
    }
}

impl CatalogCommittedStore for NoTargetIo {
    type CatalogReader = <MemoryCatalogReplicaStore as CatalogCommittedStore>::CatalogReader;

    fn catalog_reader(&self) -> Self::CatalogReader {
        panic!("source refusal reached catalog reader")
    }

    fn commit_with_catalog(
        &mut self,
        _: WriteBatch,
        _: SnapshotCatalogRecord<'_>,
    ) -> std::result::Result<(), storage::StorageError> {
        panic!("source refusal reached catalog commit")
    }
}

#[test]
fn metadata_limit_precedes_invalid_selection_or_image_without_any_target_capability_call()
-> TestResult {
    let (image, checkpoint) = selected()?;
    let invalid: CommittedStreamId = postcard::from_bytes(&postcard::to_stdvec(&[0u8; 16])?)?;
    let oversized = vec![0; MAX_CATALOG_METADATA_BYTES + 1];
    for (stream, artifact) in [
        (invalid, image.as_bytes()),
        (checkpoint.stream(), b"malformed".as_slice()),
    ] {
        let selection = TrustedCreateSendBootstrap::new(stream, &checkpoint, [0; 32], artifact);
        assert_eq!(
            CommittedStateMachine::bootstrap_create_send_image_with_catalog(
                NoTargetIo, selection, &oversized
            )
            .err(),
            Some(CommittedImageBootstrapError::LimitExceeded)
        );
    }
    Ok(())
}

#[test]
fn legal_empty_metadata_still_requires_valid_selection_and_container_before_target_access()
-> TestResult {
    let (image, checkpoint) = selected()?;
    let invalid: CommittedStreamId = postcard::from_bytes(&postcard::to_stdvec(&[0u8; 16])?)?;
    let invalid = TrustedCreateSendBootstrap::new(
        invalid,
        &checkpoint,
        Sha256::digest(image.as_bytes()).into(),
        image.as_bytes(),
    );
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image_with_catalog(NoTargetIo, invalid, &[])
            .err(),
        Some(CommittedImageBootstrapError::InvalidSelection)
    );
    let malformed = TrustedCreateSendBootstrap::new(checkpoint.stream(), &checkpoint, [0; 32], &[]);
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image_with_catalog(NoTargetIo, malformed, &[])
            .err(),
        Some(CommittedImageBootstrapError::InvalidImage)
    );
    Ok(())
}

#[test]
fn combined_bootstrap_pins_every_checkpoint_component_using_the_shared_preflight() -> TestResult {
    let (image, checkpoint) = selected()?;
    let digest = Sha256::digest(image.as_bytes()).into();
    let mut variants = Vec::new();
    let mut changed = checkpoint.clone();
    changed.last.as_mut().ok_or("missing last")?.id.term += 1;
    variants.push(changed);
    let mut changed = checkpoint.clone();
    changed.last.as_mut().ok_or("missing last")?.id.node_id += 1;
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
        .id
        .node_id += 1;
    variants.push(changed);
    let mut changed = checkpoint.clone();
    changed
        .previous
        .as_mut()
        .ok_or("missing previous")?
        .id
        .index += 1;
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
        .term += 1;
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
        .source
        .index += 1;
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
            CommittedStateMachine::bootstrap_create_send_image_with_catalog(
                NoTargetIo,
                selection,
                b"PRIVATE-META"
            )
            .err(),
            Some(CommittedImageBootstrapError::SelectionMismatch)
        );
    }
    Ok(())
}

#[test]
fn shared_bootstrap_errors_keep_static_source_private_diagnostics() {
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
        assert!(std::error::Error::source(&error).is_none());
        assert!(!format!("{error:?}: {error}").contains("PRIVATE"));
    }
}
