use storage::{FjallCatalogReplicaStore, MemoryCatalogReplicaStore};

use super::super::{
    DecodedNativeSnapshotPair, StateMachineCatalogError, StateMachineError,
    captured_image_fixture as captured,
    image_catalog::tests::{fixture, observed},
};
use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

mod custody;
mod failures;
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
        factory_and_default_disabled_build_are_inert,
        trait_build_moves_exact_whole_frame_and_projection_into_sealed_transport,
        standalone_success_does_not_enable_any_engine_snapshot_method,
    );
    backend_cases!(failures;
        bounded_refusal_is_nonfatal_and_does_not_fallback,
        native_incompatible_capture_is_nonfatal_without_retention,
        physical_capture_poison_prevents_later_source_io,
        retention_errors_stay_unknown_before_and_after_actual_commit,
        capture_panic_refunds_and_really_joins_the_owner,
    );
    backend_cases!(custody;
        lost_build_waiter_keeps_full_custody_until_real_retirement,
    );
}

#[test]
fn builder_debug_and_trait_errors_are_static() {
    let diagnostic = format!("{}: {:?}", build_error(), build_error());
    for secret in ["PRIVATE", "tenant", "orders", "node-7", "swyi-v1-sha256:"] {
        assert!(!diagnostic.contains(secret));
    }
}
