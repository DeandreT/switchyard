use std::time::Duration;

#[path = "experimental_runtime_startup/fixture.rs"]
mod fixture;
#[path = "experimental_runtime_startup/lifecycle.rs"]
mod lifecycle;
#[path = "experimental_runtime_startup/validation.rs"]
mod validation;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(30);

macro_rules! both {
    ($module:ident, $case:ident) => {
        mod $case {
            use super::*;

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn memory() -> TestResult {
                tokio::time::timeout(DEADLINE, $module::$case(&fixture::Memory)).await??;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn durable() -> TestResult {
                let directory = testkit::DurableProvider::temporary()?;
                let backend = fixture::Durable(directory.path().to_path_buf());
                tokio::time::timeout(DEADLINE, $module::$case(&backend)).await??;
                Ok(())
            }
        }
    };
}

both!(validation, empty_open_refuses_without_mutation);
both!(
    validation,
    identity_and_stream_mismatch_refuse_without_mutation
);
both!(validation, create_requires_no_prior_vote_or_history);
both!(validation, all_retained_memberships_must_match_fixed_routes);
both!(
    validation,
    finite_startup_headroom_refuses_before_engine_start
);
both!(lifecycle, unpolled_create_does_not_start_or_mutate);
both!(lifecycle, actual_create_uses_public_bounded_handle);
both!(lifecycle, actual_open_uses_existing_fixed_history);
both!(
    lifecycle,
    second_node_start_failure_joins_every_storage_owner
);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn joined_shutdown_releases_durable_direct_stores_for_open() -> TestResult {
    tokio::time::timeout(DEADLINE, lifecycle::durable_direct_reopen()).await??;
    Ok(())
}
