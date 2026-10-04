use super::*;
use crate::{
    EncodedNativeSnapshotMetadata, NativeSnapshotMetadataError, StateMachineCatalogError,
    StateMachineImageExportError,
};
use storage::{FjallCatalogReplicaStore, MemoryCatalogReplicaStore};

use super::super::super::captured_image_fixture as captured;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[path = "construction.rs"]
mod construction;
#[path = "observed.rs"]
mod observed;
#[path = "preflight.rs"]
mod preflight;
#[path = "recovery.rs"]
mod recovery;

async fn finish(machine: ExperimentalStateMachine, result: TestResult) -> TestResult {
    let joined = machine.shutdown().await;
    result?;
    joined?;
    Ok(())
}

async fn refused(
    result: Result<ExperimentalStateMachine, StateMachineCatalogBootstrapError>,
) -> TestResult<StateMachineCatalogBootstrapError> {
    match result {
        Err(error) => Ok(error),
        Ok(machine) => {
            machine.shutdown().await?;
            Err("unexpected successful bootstrap (owner joined)".into())
        }
    }
}

macro_rules! cases {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            use super::*;
            $(#[tokio::test]
            async fn $case() -> TestResult {
                construction::$case(MemoryCatalogReplicaStore::new()).await
            })+
        }
        mod durable {
            use super::*;
            $(#[tokio::test]
            async fn $case() -> TestResult {
                let directory = testkit::DurableProvider::temporary()?;
                construction::$case(FjallCatalogReplicaStore::open(directory.path())?).await
            })+
        }
    };
}
cases!(
    plain_bootstrap_combines_exact_rows_and_pair_without_postcommit_reads,
    plain_bootstrap_accepts_a_reader_without_bounded_capability,
    initial_checkpoint_catalog_bootstrap_has_no_hidden_capture,
    explicit_catalog_operations_read_original_pair_and_capture_only_when_requested,
    valid_pair_does_not_override_exact_trusted_selection,
    initialized_target_is_never_replaced,
    target_read_failures_are_static_before_any_commit,
    combined_commit_errors_are_unknown_without_starting_native_owner,
    combined_commit_before_error_leaves_pristine_without_starting_owner,
    simulated_startup_refusal_follows_known_combined_commit,
);

#[test]
fn unvalidated_artifact_getter_and_static_errors_grant_no_authority() -> TestResult {
    let source = captured::selected(false)?;
    let selection = source.request();
    assert!(std::ptr::eq(
        selection.artifact_bytes(),
        source.image.as_bytes()
    ));
    assert!(std::ptr::eq(
        selection.expected_checkpoint(),
        &source.checkpoint
    ));
    assert_eq!(
        format!("{selection:?}"),
        "TrustedCreateSendBootstrap { .. }"
    );
    for error in [
        StateMachineCatalogBootstrapError::Metadata(
            NativeSnapshotMetadataError::IncompatibleCheckpoint,
        ),
        StateMachineCatalogBootstrapError::Metadata(NativeSnapshotMetadataError::ImageMismatch),
        StateMachineCatalogBootstrapError::Domain(
            domain::CommittedImageBootstrapError::SelectionMismatch,
        ),
        StateMachineCatalogBootstrapError::Domain(
            domain::CommittedImageBootstrapError::CommitUnknown,
        ),
        StateMachineCatalogBootstrapError::OwnerStartAfterCommit,
    ] {
        assert!(std::error::Error::source(&error).is_none());
        let output = format!("{error:?}: {error}");
        for secret in [
            "PRIVATE",
            "tenant",
            "orders",
            "node-7",
            "/tmp/",
            "swyi-v1-sha256:",
        ] {
            assert!(!output.contains(secret));
        }
    }
    assert_ne!(
        StateMachineCatalogBootstrapError::OwnerStartAfterCommit,
        StateMachineCatalogBootstrapError::Domain(
            domain::CommittedImageBootstrapError::CommitUnknown
        )
    );
    Ok(())
}
