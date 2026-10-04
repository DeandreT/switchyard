use super::*;

use storage::{BoundedStateStore, CommittedStore, FjallReplicaStore, MemoryReplicaStore};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[path = "construction.rs"]
mod construction;
use super::super::captured_image_fixture as fixture;
#[path = "observed.rs"]
mod observed;
#[path = "recovery.rs"]
mod recovery;

macro_rules! cases {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            use super::*;
            $(#[tokio::test]
            async fn $case() -> TestResult {
                construction::$case(MemoryReplicaStore::new()).await
            })+
        }
        mod durable {
            use super::*;
            $(#[tokio::test]
            async fn $case() -> TestResult {
                let directory = testkit::DurableProvider::temporary()?;
                construction::$case(FjallReplicaStore::open(directory.path())?).await
            })+
        }
    };
}

cases!(
    plain_bootstrap_performs_no_postcommit_read_and_keeps_export_disabled,
    explicit_bootstrap_exports_exact_captured_bytes,
    native_incompatible_full_identity_and_membership_never_touch_target,
    native_compatible_source_refusals_never_touch_target,
    initialized_and_orphan_targets_are_never_replaced,
    target_read_failures_are_static_and_precommit,
    physical_commit_errors_remain_unknown,
    simulated_startup_refusal_preserves_the_known_commit,
    initial_checkpoint_bootstrap_is_native_compatible,
);

#[test]
fn selected_checkpoint_getter_and_errors_are_opaque_and_static() -> TestResult {
    let source = fixture::selected(false)?;
    let request = source.request();
    assert!(std::ptr::eq(
        request.expected_checkpoint(),
        &source.checkpoint
    ));
    assert_eq!(format!("{request:?}"), "TrustedCreateSendBootstrap { .. }");
    for error in [
        StateMachineImageBootstrapError::IncompatibleMetadata,
        StateMachineImageBootstrapError::Domain(CommittedImageBootstrapError::InvalidSelection),
        StateMachineImageBootstrapError::Domain(CommittedImageBootstrapError::SelectionMismatch),
        StateMachineImageBootstrapError::Domain(CommittedImageBootstrapError::CommitUnknown),
        StateMachineImageBootstrapError::OwnerStartAfterCommit,
    ] {
        let diagnostic = format!("{error:?}: {error}");
        for secret in ["PRIVATE", "tenant", "orders", "node-7", "/tmp/"] {
            assert!(!diagnostic.contains(secret));
        }
    }
    assert_ne!(
        StateMachineImageBootstrapError::Domain(CommittedImageBootstrapError::CommitUnknown),
        StateMachineImageBootstrapError::OwnerStartAfterCommit,
    );
    Ok(())
}
