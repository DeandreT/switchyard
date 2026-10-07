//! Opt-in official SDK Atom administration gates on a private-CA HTTPS listener.

use std::{error::Error, panic::AssertUnwindSafe, path::Path};

use domain::NamespaceName;
use futures_util::FutureExt;
use storage::StateStore;
use testkit::StoreProvider;

use super::{CURRENT_SDK, KEY, PREVIOUS_SDK, atomic_messaging::process};

#[path = "atom_administration/evidence.rs"]
mod evidence;
#[path = "atom_administration/fixture.rs"]
mod fixture;
#[path = "atom_administration/postconditions.rs"]
mod postconditions;
#[path = "atom_administration/rules.rs"]
mod rules;
#[path = "atom_administration/stages.rs"]
mod stages;
#[path = "atom_administration/subscriptions.rs"]
mod subscriptions;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_stable_dotnet_client_administers_finite_queues_over_strict_https() -> TestResult {
    run_gate(CURRENT_SDK).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn previous_stable_dotnet_client_administers_finite_queues_over_strict_https() -> TestResult {
    run_gate(PREVIOUS_SDK).await
}

async fn run_gate(sdk_version: &str) -> TestResult {
    eprintln!("atom-sdk build-start sdk={sdk_version}");
    let artifacts = process::build_atom_client(sdk_version).await?;
    let dll = artifacts
        .path()
        .join("bin/Switchyard.Conformance.DotNetCurrent.dll");
    run_backend(testkit::MemoryProvider::new(), &dll, sdk_version, "memory").await?;
    run_backend(
        testkit::DurableProvider::temporary()?,
        &dll,
        sdk_version,
        "fjall",
    )
    .await
}

async fn run_backend<P: StoreProvider>(
    provider: P,
    dll: &Path,
    sdk: &str,
    backend: &str,
) -> TestResult {
    let oracle = postconditions::Oracle::new();
    let mut fixture = fixture::Fixture::start(provider, NamespaceName::new("tenant")?).await?;
    let started = std::time::Instant::now();
    let outcome = AssertUnwindSafe(stages::crud(&fixture, &oracle, dll))
        .catch_unwind()
        .await;
    let cleanup = fixture.stop().await;
    eprintln!(
        "atom-sdk crud-finish sdk={sdk} backend={backend} stage_ok={} cleanup_ok={} elapsed_ms={}",
        matches!(&outcome, Ok(Ok(()))),
        cleanup.is_ok(),
        started.elapsed().as_millis()
    );
    evidence::finish(outcome, cleanup)?;
    let (provider, store, namespace) = fixture.into_stopped_parts();
    postconditions::check_retained(&store, &namespace)?;
    let before = store.snapshot()?;
    drop(store);
    let reopened = provider.open()?;
    assert_eq!(
        reopened.snapshot()?,
        before,
        "{sdk} {backend} CRUD reopen changed committed state"
    );
    postconditions::check_retained(&reopened, &namespace)?;
    drop(reopened);

    // Paging owns an initially empty namespace without erasing the CRUD evidence.
    let mut fixture = fixture::Fixture::start(provider, NamespaceName::new("atom-paging")?).await?;
    let started = std::time::Instant::now();
    let outcome = AssertUnwindSafe(stages::paging(&fixture, &oracle, dll))
        .catch_unwind()
        .await;
    let cleanup = fixture.stop().await;
    eprintln!(
        "atom-sdk paging-finish sdk={sdk} backend={backend} stage_ok={} cleanup_ok={} elapsed_ms={}",
        matches!(&outcome, Ok(Ok(()))),
        cleanup.is_ok(),
        started.elapsed().as_millis()
    );
    evidence::finish(outcome, cleanup)?;
    let (provider, store, _) = fixture.into_stopped_parts();
    postconditions::check_retained(&store, &namespace)?;
    let before = store.snapshot()?;
    drop(store);
    let reopened = provider.open()?;
    assert_eq!(
        reopened.snapshot()?,
        before,
        "{sdk} {backend} paging reopen changed committed state"
    );
    postconditions::check_retained(&reopened, &namespace)?;
    oracle.compare(&reopened.snapshot()?)?;
    Ok(())
}
