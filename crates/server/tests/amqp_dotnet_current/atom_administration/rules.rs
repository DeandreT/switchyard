//! Official SDK rule gates reuse the existing private-CA fixture.

use std::{panic::AssertUnwindSafe, path::Path};

use domain::NamespaceName;
use futures_util::FutureExt;
use storage::StateStore;
use testkit::StoreProvider;

use super::{
    CURRENT_SDK, PREVIOUS_SDK, TestResult, evidence, fixture::Fixture, postconditions::Oracle,
    process,
};

#[path = "rules/stages.rs"]
mod stages;
#[path = "rules/state.rs"]
mod state;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_stable_dotnet_client_administers_subscription_rules_over_strict_https()
-> TestResult {
    run_gate(CURRENT_SDK).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn previous_stable_dotnet_client_administers_subscription_rules_over_strict_https()
-> TestResult {
    run_gate(PREVIOUS_SDK).await
}

async fn run_gate(sdk: &str) -> TestResult {
    eprintln!("atom-rule-sdk build-start sdk={sdk}");
    let artifacts = process::build_atom_client(sdk).await?;
    let dll = artifacts
        .path()
        .join("bin/Switchyard.Conformance.DotNetCurrent.dll");
    run_backend(testkit::MemoryProvider::new(), &dll, sdk, "memory").await?;
    run_backend(testkit::DurableProvider::temporary()?, &dll, sdk, "fjall").await
}

async fn run_backend<P: StoreProvider>(
    provider: P,
    dll: &Path,
    sdk: &str,
    backend: &str,
) -> TestResult {
    let oracle = Oracle::new();
    let mut fixture = Fixture::start(provider, NamespaceName::new("tenant")?).await?;
    let started = std::time::Instant::now();
    let outcome = AssertUnwindSafe(stages::run(&fixture, &oracle, dll))
        .catch_unwind()
        .await;
    let cleanup = fixture.stop().await;
    eprintln!(
        "atom-rule-sdk finish sdk={sdk} backend={backend} stage_ok={} cleanup_ok={} elapsed_ms={}",
        matches!(&outcome, Ok(Ok(()))),
        cleanup.is_ok(),
        started.elapsed().as_millis()
    );
    evidence::finish(outcome, cleanup)?;
    let (provider, store, namespace) = fixture.into_stopped_parts();
    state::check_final(&store, &namespace)?;
    oracle.compare(&store.snapshot()?)?;
    let before = store.snapshot()?;
    drop(store);
    let reopened = provider.open()?;
    assert_eq!(
        reopened.snapshot()?,
        before,
        "{sdk} {backend} rule reopen changed committed state"
    );
    state::check_final(&reopened, &namespace)?;
    oracle.compare(&reopened.snapshot()?)?;
    Ok(())
}
