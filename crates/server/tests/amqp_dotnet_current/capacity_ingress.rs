//! Opt-in official SDK reservation and message-size gates over private-CA WSS.

use std::{
    any::Any,
    error::Error,
    panic::{AssertUnwindSafe, resume_unwind},
    path::Path,
};

use domain::NamespaceName;
use futures_util::FutureExt;
use storage::StateStore;
use testkit::StoreProvider;

use super::{CURRENT_SDK, HOST, KEY, PREVIOUS_SDK, RULE, atomic_messaging::process};

#[path = "capacity_ingress/fixture.rs"]
mod fixture;
#[path = "capacity_ingress/stages.rs"]
mod stages;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_stable_dotnet_client_observes_finite_queue_capacity_over_wss() -> TestResult {
    gate(CURRENT_SDK).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn previous_stable_dotnet_client_observes_finite_queue_capacity_over_wss() -> TestResult {
    gate(PREVIOUS_SDK).await
}

async fn gate(sdk: &str) -> TestResult {
    // Build/restore before any measured listener, owner or backend starts.
    let artifacts = process::build_capacity_client(sdk).await?;
    let dll = artifacts
        .path()
        .join("bin/Switchyard.Conformance.DotNetCurrent.dll");
    backend(testkit::MemoryProvider::new(), &dll, sdk, "memory").await?;
    backend(testkit::DurableProvider::temporary()?, &dll, sdk, "fjall").await
}

async fn backend<P: StoreProvider>(provider: P, dll: &Path, sdk: &str, name: &str) -> TestResult {
    eprintln!("capacity-sdk backend-start sdk={sdk} backend={name}");
    let mut fixture = fixture::Fixture::start(provider, NamespaceName::new("tenant")?)?;
    let outcome = AssertUnwindSafe(stages::workflows(&mut fixture, dll))
        .catch_unwind()
        .await;
    let cleanup = fixture.stop().await;
    finish(outcome, cleanup)?;
    let before = fixture.snapshot()?;
    let (provider, store, namespace) = fixture.into_stopped_parts();
    stages::clean(&store, &namespace)?;
    drop(store);
    let reopened = provider.open()?;
    assert_eq!(
        reopened.snapshot()?,
        before,
        "{sdk} {name} reopened image changed"
    );
    stages::clean(&reopened, &namespace)?;
    eprintln!("capacity-sdk backend-finish sdk={sdk} backend={name}");
    Ok(())
}

fn finish(outcome: Result<TestResult, Box<dyn Any + Send>>, cleanup: TestResult) -> TestResult {
    match (outcome, cleanup) {
        (Err(payload), cleanup) => {
            if let Err(error) = cleanup {
                eprintln!("Capacity SDK cleanup failed before original panic: {error}");
            }
            resume_unwind(payload)
        }
        (Ok(Err(error)), Err(cleanup)) => Err(std::io::Error::other(format!(
            "Capacity SDK stage failed: {error}; original cleanup also failed: {cleanup}"
        ))
        .into()),
        (Ok(Err(error)), Ok(())) => Err(error),
        (Ok(Ok(())), cleanup) => cleanup,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_stage_and_cleanup_failures_are_both_preserved() {
        let result = finish(
            Ok(Err("stage sentinel".into())),
            Err("cleanup sentinel".into()),
        );
        let text = result.unwrap_err().to_string();
        assert!(text.contains("stage sentinel") && text.contains("cleanup sentinel"));
        assert!(finish(Ok(Ok(())), Err("cleanup sentinel".into())).is_err());
        assert!(finish(Ok(Err("stage sentinel".into())), Ok(())).is_err());
        assert!(finish(Ok(Ok(())), Ok(())).is_ok());
    }
}
