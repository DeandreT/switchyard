use super::*;

#[test]
fn container_and_semantic_causes_have_flat_static_export_errors() {
    assert_eq!(
        container_error(CommittedImageError::LimitExceeded),
        CommittedImageExportError::LimitExceeded
    );
    assert_eq!(
        container_error(CommittedImageError::Allocation),
        CommittedImageExportError::Allocation
    );
    assert_eq!(
        container_error(CommittedImageError::UnsupportedFormat),
        CommittedImageExportError::UnsupportedProfile
    );
    for error in [
        CommittedImageError::Malformed,
        CommittedImageError::InvalidStream,
        CommittedImageError::InvalidRows,
        CommittedImageError::InvalidCheckpoint,
        CommittedImageError::CheckpointStreamMismatch,
        CommittedImageError::ChecksumMismatch,
    ] {
        assert_eq!(
            container_error(error),
            CommittedImageExportError::InvalidImage
        );
    }
    assert_eq!(
        validation_error(CommittedImageValidationError::UnsupportedProfile),
        CommittedImageExportError::UnsupportedProfile
    );
    for error in [
        CommittedImageValidationError::InvalidKey,
        CommittedImageValidationError::InvalidRecord,
        CommittedImageValidationError::InconsistentMetadata,
        CommittedImageValidationError::InconsistentMessage,
        CommittedImageValidationError::InconsistentIndex,
        CommittedImageValidationError::InconsistentHistory,
        CommittedImageValidationError::InvalidClock,
    ] {
        assert_eq!(
            validation_error(error),
            CommittedImageExportError::InvalidImage
        );
    }
}

#[test]
fn export_error_display_and_debug_are_static() {
    for error in [
        CommittedImageExportError::Poisoned,
        CommittedImageExportError::ReadFailed,
        CommittedImageExportError::LimitExceeded,
        CommittedImageExportError::Allocation,
        CommittedImageExportError::UnsupportedProfile,
        CommittedImageExportError::InvalidImage,
    ] {
        assert!(!format!("{error:?}: {error}").contains("secret-backend-detail"));
    }
}
