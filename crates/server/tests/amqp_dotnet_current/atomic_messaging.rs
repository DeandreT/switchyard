//! Warmed, same-queue ambient transactions on the explicit raw TLS endpoint.

use std::{error::Error, path::Path, time::Duration};

use super::{CURRENT_SDK, HOST, KEY, PREVIOUS_SDK, RULE, websocket};

#[path = "atomic_messaging/fixture.rs"]
mod fixture;
#[path = "atomic_messaging/postconditions.rs"]
mod postconditions;
#[path = "atomic_messaging/process.rs"]
mod process;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const SEND_QUEUE: &str = "atomic-sdk-send";
const HELD_QUEUE: &str = "atomic-sdk-held";
const CONTROL_QUEUE: &str = "atomic-sdk-control";
const SUCCESS: &str = "official .NET warmed same-queue transaction batch/rollback/complete/rearm/default refusal passed";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_stable_dotnet_client_completes_warmed_same_queue_transactions() -> TestResult {
    run_gate(CURRENT_SDK).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn previous_stable_dotnet_client_completes_warmed_same_queue_transactions() -> TestResult {
    run_gate(PREVIOUS_SDK).await
}

async fn run_gate(sdk_version: &'static str) -> TestResult {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let artifacts = process::build_client(sdk_version).await?;
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

async fn run_backend<P: testkit::StoreProvider>(
    provider: P,
    dll: &Path,
    sdk_version: &str,
    backend: &str,
) -> TestResult {
    let mut fixture = fixture::Fixture::start(provider).await?;
    let result = async {
        let output = process::run_client(
            dll,
            &fixture.atomic_endpoint,
            &fixture.ordinary_endpoint,
            &fixture.ca_file,
            &fixture.ca_directory,
        ).await?;
        if !output.status.success() || !output.stdout.lines().any(|line| line == SUCCESS) {
            return Err(std::io::Error::other(format!(
                "official .NET {sdk_version} {backend} atomic gate failed ({})\nstdout:\n{}\nstderr:\n{}",
                output.status, output.stdout, output.stderr,
            )).into());
        }
        Ok::<_, Box<dyn Error>>(())
    }.await;
    let cleanup = fixture.stop().await;
    result?;
    cleanup?;
    let (provider, store, namespace) = fixture.into_stopped_parts();
    postconditions::check(&store, &namespace)?;
    let before = storage::StateStore::snapshot(&store)?;
    drop(store);
    let reopened = provider.open()?;
    assert_eq!(
        storage::StateStore::snapshot(&reopened)?,
        before,
        "{backend} reopen changed committed state"
    );
    postconditions::check(&reopened, &namespace)?;
    Ok(())
}
