use domain::{CommittedCheckpointUpdate, CommittedEntryId, CommittedStateMachine, Timestamp};
use storage::{BoundedStateStore, CommittedStore, StateStore};

use super::{bootstrap_fixture as captured, *};

pub(super) fn native_pair_preserves_exact_capture_and_projects_only_captured_checkpoint<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, reader) = fixture::populate(writer)?;
    let before = reader.snapshot()?;
    let image = machine.export_create_send_image()?;
    let encoded = EncodedNativeSnapshotMetadata::encode(image.as_bytes())?;
    let repeated = EncodedNativeSnapshotMetadata::encode(image.as_bytes())?;
    assert_eq!(encoded.as_bytes(), repeated.as_bytes());
    assert!(encoded.len() <= super::super::codec::MAX_FROZEN_METADATA_BYTES);
    assert!(!encoded.is_empty());
    let pair = DecodedNativeSnapshotPair::decode(encoded.as_bytes(), image.as_bytes())?;
    assert!(std::ptr::eq(
        pair.metadata_bytes().as_ptr(),
        encoded.as_bytes().as_ptr()
    ));
    assert!(std::ptr::eq(
        pair.artifact_bytes().as_ptr(),
        image.as_bytes().as_ptr()
    ));
    assert_eq!(pair.metadata_bytes().len(), encoded.len());
    assert_eq!(pair.artifact_bytes().len(), image.len());
    assert_eq!(pair.checkpoint(), &machine.checkpoint()?);
    assert_eq!(
        pair.checkpoint().highest_timestamp(),
        Timestamp::from_millis(500)
    );
    assert_eq!(
        (pair.image().queue_count(), pair.image().message_count()),
        (1, 1)
    );
    let meta = pair.snapshot_meta()?;
    assert_eq!(
        meta.last_log_id,
        Some(crate::LogId::new(openraft::CommittedLeaderId::new(1, 7), 4))
    );
    assert_eq!(
        meta.last_membership.log_id(),
        &Some(crate::LogId::new(openraft::CommittedLeaderId::new(1, 7), 0))
    );
    assert_eq!(meta.last_membership.membership(), &captured::members());
    assert_eq!(
        meta.snapshot_id,
        super::super::codec::snapshot_id(&captured::digest(image.as_bytes()))?
    );
    assert_eq!(
        meta.snapshot_id.len(),
        super::super::codec::SNAPSHOT_ID_BYTES
    );
    assert_eq!(reader.snapshot()?, before);
    Ok(())
}

pub(super) fn initial_pair_has_no_log_or_membership<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let reader = writer.reader();
    let mut machine = CommittedStateMachine::create(writer, captured::stream()?)?;
    let before = reader.snapshot()?;
    let image = machine.export_create_send_image()?;
    assert_eq!(image.as_bytes(), captured::initial()?.image.as_bytes());
    let encoded = EncodedNativeSnapshotMetadata::encode(image.as_bytes())?;
    let pair = DecodedNativeSnapshotPair::decode(encoded.as_bytes(), image.as_bytes())?;
    assert_eq!(pair.checkpoint().last(), None);
    assert_eq!(pair.checkpoint().previous(), None);
    assert_eq!(pair.checkpoint().highest_timestamp(), Timestamp::UNIX_EPOCH);
    assert_eq!(pair.snapshot_meta()?.last_log_id, None);
    assert_eq!(
        pair.snapshot_meta()?.last_membership,
        openraft::StoredMembership::default()
    );
    assert_eq!(
        (pair.image().queue_count(), pair.image().message_count()),
        (0, 0)
    );
    assert_eq!(reader.snapshot()?, before);
    Ok(())
}

pub(super) fn retained_older_pair_survives_later_applied_progress<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, reader) = fixture::populate(writer)?;
    let image = machine.export_create_send_image()?;
    let encoded = EncodedNativeSnapshotMetadata::encode(image.as_bytes())?;
    let next = captured::next_send()?;
    let openraft::EntryPayload::Normal(command) = next.payload else {
        return Err("fixture send changed kind".into());
    };
    let work = command.into_committed_work();
    machine.apply_committed(
        &CommittedCheckpointUpdate {
            stream: captured::stream()?,
            expected_previous: machine.checkpoint()?.last(),
            entry: CommittedEntryId {
                term: 1,
                node_id: 7,
                index: 5,
            },
        },
        &work,
    )?;
    let current = machine.checkpoint()?;
    let newer = machine.export_create_send_image()?;
    let before = reader.snapshot()?;
    let old = DecodedNativeSnapshotPair::decode(encoded.as_bytes(), image.as_bytes())?;
    assert_ne!(old.checkpoint(), &current);
    assert_eq!(old.snapshot_meta()?.last_log_id.map(|id| id.index), Some(4));
    assert_eq!(current.last().map(|mark| mark.id.index), Some(5));
    assert_eq!(
        DecodedNativeSnapshotPair::decode(encoded.as_bytes(), newer.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::ImageMismatch)
    );
    assert_eq!(reader.snapshot()?, before);
    Ok(())
}
