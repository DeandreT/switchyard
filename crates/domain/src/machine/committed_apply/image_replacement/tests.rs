use storage::{CatalogCommittedStore, MemoryCatalogReplicaStore, StateStore};

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn populated() -> TestResult<CommittedStateMachine<MemoryCatalogReplicaStore>> {
    let stream = CommittedStreamId::new([7; 16])?;
    let mut machine = CommittedStateMachine::create(MemoryCatalogReplicaStore::new(), stream)?;
    let works = [
        crate::CommittedQueueWork::Membership {
            schema_version: 999,
            payload: b"private-member".to_vec(),
        },
        crate::CommittedQueueWork::Queue(crate::CommittedQueueCommand::create_queue(
            crate::NamespaceName::new("tenant")?,
            crate::EntityPath::new("orders")?,
            crate::Timestamp::from_millis(10),
            crate::QueueConfig::default(),
        )),
        crate::CommittedQueueWork::Queue(crate::CommittedQueueCommand::send(
            crate::NamespaceName::new("tenant")?,
            crate::EntityPath::new("orders")?,
            crate::Timestamp::from_millis(11),
            crate::CommittedSend {
                message_id: "private-id".into(),
                body: b"private-body".to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    ];
    for (index, work) in works.iter().enumerate() {
        machine.apply_committed(
            &crate::CommittedCheckpointUpdate {
                stream,
                expected_previous: machine.checkpoint()?.last(),
                entry: crate::CommittedEntryId {
                    term: 1,
                    node_id: 9,
                    index: index as u64,
                },
            },
            work,
        )?;
    }
    Ok(machine)
}

#[test]
fn every_full_expected_target_checkpoint_component_is_compared() -> TestResult {
    let mut machine = populated()?;
    let image = machine.export_create_send_image()?;
    let checkpoint = machine.checkpoint()?;
    let digest = <[u8; 32]>::from(sha2::Sha256::digest(image.as_bytes()));
    let before = machine.reader().snapshot()?;
    let mut variants = Vec::new();
    for part in 0..17 {
        let mut changed = checkpoint.clone();
        match part {
            0 => changed.last.as_mut().ok_or("missing last")?.id.term += 1,
            1 => changed.last.as_mut().ok_or("missing last")?.id.node_id += 1,
            2 => changed.last.as_mut().ok_or("missing last")?.id.index += 1,
            3 => changed.last.as_mut().ok_or("missing last")?.fingerprint[0] ^= 1,
            4 => changed.previous.as_mut().ok_or("missing previous")?.id.term += 1,
            5 => {
                changed
                    .previous
                    .as_mut()
                    .ok_or("missing previous")?
                    .id
                    .node_id += 1
            }
            6 => {
                changed
                    .previous
                    .as_mut()
                    .ok_or("missing previous")?
                    .id
                    .index += 1
            }
            7 => {
                changed
                    .previous
                    .as_mut()
                    .ok_or("missing previous")?
                    .fingerprint[0] ^= 1
            }
            8 => changed.highest_timestamp = crate::Timestamp::from_millis(12),
            9 => {
                changed
                    .membership
                    .as_mut()
                    .ok_or("missing membership")?
                    .source
                    .node_id += 1
            }
            10 => {
                changed
                    .membership
                    .as_mut()
                    .ok_or("missing membership")?
                    .source
                    .term += 1
            }
            11 => {
                changed
                    .membership
                    .as_mut()
                    .ok_or("missing membership")?
                    .source
                    .index += 1
            }
            12 => {
                changed
                    .membership
                    .as_mut()
                    .ok_or("missing membership")?
                    .schema_version += 1
            }
            13 => changed
                .membership
                .as_mut()
                .ok_or("missing membership")?
                .payload
                .push(0),
            14 => changed.last = None,
            15 => changed.previous = None,
            _ => changed.membership = None,
        }
        variants.push(changed);
    }
    for changed in variants {
        assert_eq!(
            machine.replace_create_send_image_with_catalog(
                TrustedCreateSendReplacement::new(
                    checkpoint.stream(),
                    &changed,
                    &checkpoint,
                    digest,
                    image.as_bytes()
                ),
                b"opaque",
            ),
            Err(CommittedImageReplacementError::TargetMismatch)
        );
        assert_eq!(machine.reader().snapshot()?, before);
        assert_eq!(machine.checkpoint()?, checkpoint);
    }
    assert!(machine.writer.catalog_reader().read_catalog()?.is_none());
    Ok(())
}

#[test]
fn flat_source_target_and_container_error_mappings_preserve_nonfatal_limits() {
    for (source, expected) in [
        (
            SelectionError::InvalidSelection,
            CommittedImageReplacementError::InvalidSelection,
        ),
        (
            SelectionError::SelectionMismatch,
            CommittedImageReplacementError::SelectionMismatch,
        ),
        (
            SelectionError::LimitExceeded,
            CommittedImageReplacementError::LimitExceeded,
        ),
        (
            SelectionError::Allocation,
            CommittedImageReplacementError::Allocation,
        ),
        (
            SelectionError::UnsupportedProfile,
            CommittedImageReplacementError::UnsupportedProfile,
        ),
        (
            SelectionError::InvalidImage,
            CommittedImageReplacementError::InvalidImage,
        ),
    ] {
        assert_eq!(selection_error(source), expected);
    }
    for (source, expected) in [
        (
            CommittedImageExportError::Poisoned,
            CommittedImageReplacementError::Poisoned,
        ),
        (
            CommittedImageExportError::ReadFailed,
            CommittedImageReplacementError::TargetReadFailed,
        ),
        (
            CommittedImageExportError::LimitExceeded,
            CommittedImageReplacementError::LimitExceeded,
        ),
        (
            CommittedImageExportError::Allocation,
            CommittedImageReplacementError::Allocation,
        ),
        (
            CommittedImageExportError::UnsupportedProfile,
            CommittedImageReplacementError::UnsupportedTargetProfile,
        ),
        (
            CommittedImageExportError::InvalidImage,
            CommittedImageReplacementError::InvalidTarget,
        ),
    ] {
        assert_eq!(target_export_error(source), expected);
    }
    for (source, expected) in [
        (
            CommittedImageError::LimitExceeded,
            CommittedImageReplacementError::LimitExceeded,
        ),
        (
            CommittedImageError::Allocation,
            CommittedImageReplacementError::Allocation,
        ),
        (
            CommittedImageError::UnsupportedFormat,
            CommittedImageReplacementError::UnsupportedTargetProfile,
        ),
        (
            CommittedImageError::InvalidCheckpoint,
            CommittedImageReplacementError::InvalidTarget,
        ),
    ] {
        assert_eq!(target_container_error(source), expected);
    }
}

use sha2::Digest;
use storage::SnapshotCatalogReader;
