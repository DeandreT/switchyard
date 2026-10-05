use std::error::Error;

use domain::{CommittedCheckpoint, CommittedImageReplacementError};
use storage::{
    BoundedStateStore, CatalogCommittedStore, FjallCatalogReplicaStore, MemoryCatalogReplicaStore,
};

use super::super::{
    captured_image_fixture as captured,
    image_catalog::tests::{fixture as catalog_fixture, observed},
};
use super::{
    CommittedNativeReplacement, DecodedNativeSnapshotPair, EncodedNativeSnapshotMetadata,
    ExperimentalStateMachine, NativeSnapshotMetadataError, OwnedTrustedNativeReplacement,
    StateMachineError, StateMachineImageReplacementError,
};

type TestResult<T = ()> = std::result::Result<T, Box<dyn Error>>;

mod bounds;
mod custody;
mod fixture;
mod physical;
mod workflow;

mod scenarios {
    use super::*;

    macro_rules! backend_cases {
        ($group:ident; $($case:ident),+ $(,)?) => {
            mod $group {
                use super::*;
                mod memory {
                    use super::*;
                    $(#[tokio::test]
                    async fn $case() -> TestResult {
                        super::super::super::$group::$case(MemoryCatalogReplicaStore::new()).await
                    })+
                }
                mod durable {
                    use super::*;
                    $(#[tokio::test]
                    async fn $case() -> TestResult {
                        let directory = testkit::DurableProvider::temporary()?;
                        super::super::super::$group::$case(FjallCatalogReplicaStore::open(directory.path())?).await
                    })+
                }
            }
        };
    }

    backend_cases!(workflow;
        exact_selected_body_moves_without_cursor_or_postcommit_io,
        initial_image_removes_populated_rows_and_preserves_default_membership,
        independent_selection_fields_are_not_inferred_from_actual_pair,
        same_full_checkpoint_different_valid_body_still_requires_exact_trusted_digest,
        standalone_builder_carrier_is_consumed_without_body_copy,
        every_actual_native_meta_field_must_match_before_target_capture,
        invalid_and_native_incompatible_sources_refuse_before_target_io,
        old_checkpoint_mismatch_and_target_limits_are_nonfatal,
        physical_capture_and_all_commit_errors_poison_without_later_io,
        all_existing_constructors_keep_replacement_disabled,
        sealed_catalog_owner_refuses_replacement_before_disabled_without_io,
    );
    backend_cases!(custody;
        accepted_lost_waiter_keeps_custody_refunds_and_blocks_real_join,
        unpolled_and_closed_future_inputs_never_keep_database_handles_alive,
        capture_panic_refunds_and_really_joins_owner,
    );
}

#[test]
fn replacement_errors_and_receipt_are_static_redacted_and_source_free() {
    for error in [
        StateMachineImageReplacementError::Disabled,
        StateMachineImageReplacementError::Owner(StateMachineError::Panicked),
        StateMachineImageReplacementError::Domain(CommittedImageReplacementError::CommitUnknown),
        StateMachineImageReplacementError::Domain(CommittedImageReplacementError::TargetReadFailed),
        StateMachineImageReplacementError::Metadata(NativeSnapshotMetadataError::ImageMismatch),
    ] {
        let diagnostic = format!("{error:?}: {error}");
        for secret in ["PRIVATE", "tenant", "orders", "node-7", "swyi-v1-sha256:"] {
            assert!(!diagnostic.contains(secret));
        }
        assert!(Error::source(&error).is_none());
    }
    assert_eq!(
        format!("{:?}", CommittedNativeReplacement { _private: () }),
        "CommittedNativeReplacement { _private: () }"
    );
}
