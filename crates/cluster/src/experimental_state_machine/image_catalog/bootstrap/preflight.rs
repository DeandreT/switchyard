use domain::{CommittedImageBootstrapError as DomainError, TrustedCreateSendBootstrap};
use storage::{
    CatalogCommittedStore, CommittedStore, MemoryCatalogReplicaStore, SnapshotCatalogRecord,
    StorageError, WriteBatch,
};

use super::{TestResult, captured, *};

struct NoTargetIo;
impl CommittedStore for NoTargetIo {
    type Reader = <MemoryCatalogReplicaStore as CommittedStore>::Reader;
    fn reader(&self) -> Self::Reader {
        panic!("reader factory before source refusal");
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        panic!("init read before source refusal");
    }
    fn commit(&mut self, _: WriteBatch) -> Result<(), StorageError> {
        panic!("ordinary commit before source refusal");
    }
}
impl CatalogCommittedStore for NoTargetIo {
    type CatalogReader = <MemoryCatalogReplicaStore as CatalogCommittedStore>::CatalogReader;
    fn catalog_reader(&self) -> Self::CatalogReader {
        panic!("catalog reader before source refusal");
    }
    fn commit_with_catalog(
        &mut self,
        _: WriteBatch,
        _: SnapshotCatalogRecord<'_>,
    ) -> Result<(), StorageError> {
        panic!("combined commit before source refusal");
    }
}

#[test]
fn actual_pair_framing_caps_and_digest_mismatch_precede_every_target_api() -> TestResult {
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    for invalid in [
        vec![],
        b"PRIVATE-invalid-metadata".to_vec(),
        vec![0; crate::MAX_NATIVE_SNAPSHOT_METADATA_BYTES + 1],
    ] {
        let expected = if invalid.len() > crate::MAX_NATIVE_SNAPSHOT_METADATA_BYTES {
            NativeSnapshotMetadataError::LimitExceeded
        } else {
            NativeSnapshotMetadataError::InvalidMetadata
        };
        assert_eq!(
            ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
                NoTargetIo,
                source.request(),
                &invalid,
            )
            .err(),
            Some(StateMachineCatalogBootstrapError::Metadata(expected))
        );
    }
    let mut unsupported = metadata.as_bytes().to_vec();
    unsupported[4..6].copy_from_slice(&2_u16.to_be_bytes());
    assert_eq!(
        ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
            NoTargetIo,
            source.request(),
            &unsupported,
        )
        .err(),
        Some(StateMachineCatalogBootstrapError::Metadata(
            NativeSnapshotMetadataError::UnsupportedFormat
        ))
    );
    let mut bad_checksum = metadata.as_bytes().to_vec();
    *bad_checksum.last_mut().unwrap() ^= 1;
    assert_eq!(
        ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
            NoTargetIo,
            source.request(),
            &bad_checksum,
        )
        .err(),
        Some(StateMachineCatalogBootstrapError::Metadata(
            NativeSnapshotMetadataError::InvalidMetadata
        ))
    );
    let changed = captured::altered_checkpoint(&source, |wire| {
        wire.highest_timestamp += 1;
    })?;
    let changed_metadata = EncodedNativeSnapshotMetadata::encode(changed.image.as_bytes())?;
    assert_eq!(
        ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
            NoTargetIo,
            source.request(),
            changed_metadata.as_bytes(),
        )
        .err(),
        Some(StateMachineCatalogBootstrapError::Metadata(
            NativeSnapshotMetadataError::ImageMismatch
        ))
    );
    let key = domain::keys::message(
        &captured::namespace()?,
        &captured::entity()?,
        domain::SequenceNumber::new(1),
    );
    let value = source
        .snapshot
        .entries()
        .iter()
        .find(|(stored, _)| stored == &key)
        .ok_or("missing original body row")?
        .1
        .clone();
    let mut record: domain::MessageRecord = domain::codec::decode(&value)?;
    record.body = b"PRIVATE-different-valid-body".to_vec();
    let other_body = captured::with_record(&source, &key, domain::codec::encode(&record)?)?;
    captured::supported(&other_body)?;
    assert_eq!(other_body.checkpoint, source.checkpoint);
    assert_ne!(other_body.image.as_bytes(), source.image.as_bytes());
    assert_eq!(
        ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
            NoTargetIo,
            other_body.request(),
            metadata.as_bytes(),
        )
        .err(),
        Some(StateMachineCatalogBootstrapError::Metadata(
            NativeSnapshotMetadataError::ImageMismatch
        ))
    );
    Ok(())
}

#[test]
fn every_domain_valid_native_incompatible_checkpoint_is_refused_before_target() -> TestResult {
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let variants = [
        captured::altered_checkpoint(&source, |wire| {
            wire.previous.as_mut().unwrap().id.node_id = 9;
        })?,
        captured::altered_checkpoint(&source, |wire| {
            wire.last.as_mut().unwrap().id.node_id = 6;
        })?,
        captured::altered_checkpoint(&source, |wire| {
            wire.membership.as_mut().unwrap().source.node_id = 9;
        })?,
        captured::altered_checkpoint(&source, |wire| {
            wire.membership.as_mut().unwrap().schema_version = 2;
        })?,
        captured::altered_checkpoint(&source, |wire| {
            wire.membership.as_mut().unwrap().payload = b"PRIVATE-opaque-member".to_vec();
        })?,
    ];
    for source in variants {
        captured::supported(&source)?;
        assert_eq!(
            ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
                NoTargetIo,
                source.request(),
                metadata.as_bytes(),
            )
            .err(),
            Some(StateMachineCatalogBootstrapError::Metadata(
                NativeSnapshotMetadataError::IncompatibleCheckpoint
            ))
        );
    }
    Ok(())
}

#[test]
fn structural_role_does_not_bypass_actual_business_validation_before_target() -> TestResult {
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let unknown = captured::with_record(
        &source,
        &[0x7f],
        b"PRIVATE-unsupported-business-row".to_vec(),
    )?;
    assert!(domain::DecodedCommittedImage::decode(unknown.image.as_bytes()).is_ok());
    assert_eq!(
        ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
            NoTargetIo,
            unknown.request(),
            metadata.as_bytes(),
        )
        .err(),
        Some(StateMachineCatalogBootstrapError::Metadata(
            NativeSnapshotMetadataError::InvalidImage
        ))
    );
    Ok(())
}

#[test]
fn healthy_pair_still_requires_the_exact_full_trusted_expectation_before_target() -> TestResult {
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let mut bad_digest = captured::digest(source.image.as_bytes());
    bad_digest[0] ^= 1;
    let wrong_digest = TrustedCreateSendBootstrap::new(
        captured::stream()?,
        &source.checkpoint,
        bad_digest,
        source.image.as_bytes(),
    );
    assert_eq!(
        ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
            NoTargetIo,
            wrong_digest,
            metadata.as_bytes(),
        )
        .err(),
        Some(StateMachineCatalogBootstrapError::Domain(
            DomainError::SelectionMismatch
        ))
    );
    let wrong_checkpoint = captured::altered_checkpoint(&source, |wire| {
        wire.previous.as_mut().unwrap().fingerprint[0] ^= 1;
    })?;
    let request = TrustedCreateSendBootstrap::new(
        captured::stream()?,
        &wrong_checkpoint.checkpoint,
        captured::digest(source.image.as_bytes()),
        source.image.as_bytes(),
    );
    assert_eq!(
        ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
            NoTargetIo,
            request,
            metadata.as_bytes(),
        )
        .err(),
        Some(StateMachineCatalogBootstrapError::Domain(
            DomainError::SelectionMismatch
        ))
    );

    let foreign_stream = domain::CommittedStreamId::new([8; 16])?;
    let writer = storage::MemoryReplicaStore::new();
    let reader = writer.reader();
    let _machine = domain::CommittedStateMachine::create(writer, foreign_stream)?;
    let foreign = domain::EncodedCommittedImage::encode(
        domain::CommittedImageRole::CreateSendLayout17V1,
        foreign_stream,
        &storage::StateStore::snapshot(&reader)?,
    )?;
    let foreign_metadata = EncodedNativeSnapshotMetadata::encode(foreign.as_bytes())?;
    let expected = captured::initial()?;
    let wrong_stream = TrustedCreateSendBootstrap::new(
        captured::stream()?,
        &expected.checkpoint,
        captured::digest(foreign.as_bytes()),
        foreign.as_bytes(),
    );
    assert_eq!(
        ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
            NoTargetIo,
            wrong_stream,
            foreign_metadata.as_bytes(),
        )
        .err(),
        Some(StateMachineCatalogBootstrapError::Domain(
            DomainError::SelectionMismatch
        ))
    );

    let zero_stream: domain::CommittedStreamId = postcard::from_bytes(&[0; 16])?;
    let invalid = TrustedCreateSendBootstrap::new(
        zero_stream,
        &source.checkpoint,
        captured::digest(source.image.as_bytes()),
        source.image.as_bytes(),
    );
    assert_eq!(
        ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
            NoTargetIo,
            invalid,
            metadata.as_bytes(),
        )
        .err(),
        Some(StateMachineCatalogBootstrapError::Domain(
            DomainError::InvalidSelection
        ))
    );
    Ok(())
}
