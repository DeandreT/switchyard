use super::*;
use storage::{
    BoundedStateStore, CatalogCommittedStore, CommittedStore, FjallCatalogReplicaStore,
    FjallReplicaStore, MemoryCatalogReplicaStore, MemoryReplicaStore,
};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
mod crash;
mod custody;
mod failures;
mod fixture;
mod frontier;
mod observed;
mod reopen;
mod runtimes;
mod workflow;

macro_rules! paired {
    ($name:ident, $case:path) => {
        mod $name {
            use super::*;
            #[tokio::test]
            async fn memory() -> TestResult {
                $case(MemoryReplicaStore::new(), MemoryCatalogReplicaStore::new()).await
            }
            #[tokio::test]
            async fn durable() -> TestResult {
                let log = testkit::DurableProvider::temporary()?;
                let state = testkit::DurableProvider::temporary()?;
                $case(
                    FjallReplicaStore::open(log.path())?,
                    FjallCatalogReplicaStore::open(state.path())?,
                )
                .await
            }
        }
    };
}
paired!(
    whole_prefix_and_cached_no_write,
    workflow::whole_prefix_and_cached_no_write
);
paired!(
    retained_generic_builder_is_denied,
    workflow::retained_generic_builder_is_denied
);
paired!(
    content_mismatch_refuses_before_retention,
    workflow::content_mismatch_refuses_before_retention
);
paired!(caller_loss_and_busy, custody::caller_loss_and_busy);
paired!(close_before_claim, custody::close_before_claim);
paired!(close_after_claim, custody::close_after_claim);
paired!(catalog_unknown_before, failures::catalog_unknown_before);
paired!(catalog_unknown_after, failures::catalog_unknown_after);
paired!(log_unknown_before, failures::log_unknown_before);
paired!(log_unknown_after, failures::log_unknown_after);
paired!(log_panic_joins_both, failures::log_panic_joins_both);
paired!(read_poison_before_claim, frontier::read_poison_before_claim);
paired!(
    export_poison_before_claim,
    frontier::export_poison_before_claim
);
paired!(read_panic_before_claim, frontier::read_panic_before_claim);
paired!(
    export_panic_before_claim,
    frontier::export_panic_before_claim
);

#[test]
fn static_errors_have_no_diagnostic_source() {
    use std::error::Error as _;
    for error in [
        LocalCompactionError::InvalidPair,
        LocalCompactionError::CommitUnknown,
        LocalCompactionError::OwnerFailure,
        LocalCompactionError::TaskFailed,
    ] {
        assert!(error.source().is_none());
        let text = format!("{error:?}: {error}");
        for private in ["PRIVATE", "tenant", "orders", "node-7", "swyi-v1-sha256:"] {
            assert!(!text.contains(private));
        }
    }
}
