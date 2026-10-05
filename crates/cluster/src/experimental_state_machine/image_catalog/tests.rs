use super::{
    CommittedCatalogError, DecodedNativeSnapshotPair, EncodedNativeSnapshotMetadata,
    ExperimentalStateMachine, MAX_NATIVE_CATALOG_METADATA_OVERHEAD_BYTES,
    NativeSnapshotMetadataError, StateMachineCatalogError, StateMachineError,
};
use storage::{FjallCatalogReplicaStore, MemoryCatalogReplicaStore};

use super::super::captured_image_fixture as captured;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[path = "custody.rs"]
mod custody;
#[path = "failures.rs"]
mod failures;
#[path = "fixture.rs"]
pub(in crate::experimental_state_machine) mod fixture;
#[path = "observed.rs"]
pub(in crate::experimental_state_machine) mod observed;
#[path = "workflow.rs"]
mod workflow;

const _: () = {
    assert!(MAX_NATIVE_CATALOG_METADATA_OVERHEAD_BYTES == storage::MAX_CATALOG_METADATA_BYTES);
    assert!(domain::MAX_COMMITTED_IMAGE_BYTES == super::super::MAX_STATE_MACHINE_OWNER_BYTES);
};

// Keep helpers separate from wrapper modules so no source file is loaded twice.
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
        legacy_owners_refuse_catalog_operations_without_source_io,
        build_moves_exact_capture_and_read_uses_only_retained_pair,
        older_catalog_stays_valid_after_current_progress_advances,
        opaque_mismatched_and_ahead_pairs_do_not_acquire_frontier_authority,
        native_incompatible_capture_is_refused_before_retention_without_poison,
    );
    backend_cases!(failures;
        bounded_and_allocation_refusals_are_nonfatal_without_fallback,
        physical_capture_and_catalog_reads_poison_before_any_later_io,
        retention_error_is_unknown_before_or_after_actual_catalog_commit,
        capture_and_catalog_backend_panics_refund_and_really_join,
    );
    backend_cases!(custody;
        lost_capture_waiter_keeps_full_custody_and_delays_real_join,
        lost_catalog_read_waiter_keeps_owned_output_until_actual_join,
        unpolled_owned_factories_do_not_keep_the_backend_owner_alive,
    );
}

#[tokio::test]
async fn legacy_bootstrap_never_enables_catalog_operations() -> TestResult {
    let source = captured::selected(false)?;
    let (writer, control) = observed::observed(MemoryCatalogReplicaStore::new());
    let mut machine =
        ExperimentalStateMachine::bootstrap_create_send_image(writer, source.request())?;
    control.reset();
    let result = async {
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Disabled)
        );
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Disabled)
        );
        assert_eq!(control.counts(), observed::Counts::default());
        Ok(())
    }
    .await;
    fixture::finish(machine, result).await
}

#[test]
fn catalog_errors_are_static_and_do_not_expose_backend_or_projection_data() {
    for error in [
        StateMachineCatalogError::Disabled,
        StateMachineCatalogError::Owner(StateMachineError::Panicked),
        StateMachineCatalogError::Domain(CommittedCatalogError::CommitUnknown),
        StateMachineCatalogError::Domain(CommittedCatalogError::ReadFailed),
        StateMachineCatalogError::Metadata(NativeSnapshotMetadataError::IncompatibleCheckpoint),
    ] {
        let diagnostic = format!("{error:?}: {error}");
        for secret in ["PRIVATE", "tenant", "orders", "node-7", "swyi-v1-sha256:"] {
            assert!(!diagnostic.contains(secret));
        }
    }
}
