use std::{panic::AssertUnwindSafe, path::Path};

use domain::NamespaceName;
use futures_util::FutureExt;
use server::{Clock, SystemClock};
use storage::StateStore;
use testkit::StoreProvider;

use super::{CURRENT_SDK, PREVIOUS_SDK, TestResult, evidence, fixture::Fixture, process};

#[path = "rule_message_flow/state.rs"]
mod state;

pub(super) const HOST: &str = "tenant.servicebus.windows.net";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_stable_dotnet_client_applies_https_rules_to_amqp_messages() -> TestResult {
    run_gate(CURRENT_SDK).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn previous_stable_dotnet_client_applies_https_rules_to_amqp_messages() -> TestResult {
    run_gate(PREVIOUS_SDK).await
}

async fn run_gate(sdk: &str) -> TestResult {
    eprintln!("atom-rule-flow build-start sdk={sdk}");
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
    let mut fixture = Fixture::start_joint(provider, NamespaceName::new("tenant")?).await?;
    let started = std::time::Instant::now();
    let outcome = AssertUnwindSafe(async {
        state::seed(&fixture.handle(), &fixture.namespace)?;
        let oracle = state::Oracle::new(
            &fixture.snapshot()?,
            fixture.batches().len(),
            fixture.clock_readings().len(),
        )?;
        process::run_atom_rule_message_flow_client(
            dll,
            &fixture.https_endpoint,
            &fixture.amqp_endpoint,
            &fixture.ca_file,
            &fixture.ca_directory,
            HOST,
            state::TOPIC,
            "manage",
            super::KEY,
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error>>(oracle)
    })
    .catch_unwind()
    .await;
    let cleanup = fixture.stop().await;
    let finished_at = SystemClock.now();
    let batches = fixture.batches();
    let readings = fixture.clock_readings();
    eprintln!(
        "atom-rule-flow finish sdk={sdk} backend={backend} stage_ok={} cleanup_ok={} elapsed_ms={}",
        matches!(&outcome, Ok(Ok(_))),
        cleanup.is_ok(),
        started.elapsed().as_millis(),
    );
    let mut oracle = None;
    let outcome = outcome.map(|result| {
        result.map(|value| {
            oracle = Some(value);
        })
    });
    evidence::finish(outcome, cleanup)?;
    let oracle = oracle.expect("successful bridge oracle");
    let (provider, store, namespace) = fixture.into_stopped_parts();
    oracle.check(&store, &namespace, &batches, &readings, finished_at)?;
    let before = store.snapshot()?;
    drop(store);
    let reopened = provider.open()?;
    assert_eq!(
        reopened.snapshot()?,
        before,
        "{sdk} {backend} bridge reopen changed exact committed state"
    );
    oracle.check(&reopened, &namespace, &batches, &readings, finished_at)?;
    Ok(())
}
