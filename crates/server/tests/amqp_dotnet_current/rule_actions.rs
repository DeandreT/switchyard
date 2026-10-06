//! Isolated official SDK gate for REMOVE and the bounded local literal SET subset.

use std::{error::Error, path::Path, time::Duration};

use super::{CURRENT_SDK, HOST, KEY, PREVIOUS_SDK, RULE, atomic_messaging::process, websocket};

#[path = "rule_actions/fixture.rs"]
mod fixture;
#[path = "rule_actions/postconditions.rs"]
mod postconditions;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const TOPIC: &str = "sdk-rule-actions";
const SUCCESS: &str = "official .NET SQL REMOVE/literal SET actions/source/independent copies/conversion DLQ/unsupported system SET passed";

fn success_marker_present(stdout: &str) -> bool {
    stdout.split_inclusive('\n').any(|line| {
        line.strip_suffix('\n')
            .map(|line| line.strip_suffix('\r').unwrap_or(line))
            == Some(SUCCESS)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_stable_dotnet_client_completes_sql_remove_action_workflows() -> TestResult {
    run_gate(CURRENT_SDK).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn previous_stable_dotnet_client_completes_sql_remove_action_workflows() -> TestResult {
    run_gate(PREVIOUS_SDK).await
}

async fn run_gate(sdk_version: &'static str) -> TestResult {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let artifacts = process::build_rule_action_client(sdk_version).await?;
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
        let output = process::run_rule_action_client(
            dll,
            &fixture.endpoint,
            TOPIC,
            &fixture.ca_file,
            &fixture.ca_directory,
        )
        .await?;
        if !output.status.success() || !success_marker_present(&output.stdout) {
            return Err(std::io::Error::other(format!(
                "official .NET {sdk_version} {backend} SQL action gate failed ({})\nstdout:\n{}\nstderr:\n{}",
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
        "{backend} reopen changed rule action cleanup state"
    );
    postconditions::check(&reopened, &namespace)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_exact_completed_marker_is_accepted() {
        assert!(success_marker_present(&format!("{SUCCESS}\n")));
        assert!(success_marker_present(&format!(
            "diagnostic\r\n{SUCCESS}\r\n"
        )));
        for output in [
            String::new(),
            SUCCESS.to_owned(),
            format!("{SUCCESS}\r"),
            format!("prefix {SUCCESS}\n"),
            format!("{SUCCESS} suffix\n"),
            format!(" {}\n", SUCCESS),
            format!("{}\n", &SUCCESS[..SUCCESS.len() - 1]),
        ] {
            assert!(
                !success_marker_present(&output),
                "invalid marker: {output:?}"
            );
        }
    }
}
