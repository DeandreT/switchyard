use std::{error::Error, fmt, time::Duration};

use super::TestResult;

#[derive(Debug)]
struct BackendFailure {
    sdk_version: String,
    backend: String,
    client_elapsed_ms: u128,
    cleanup_elapsed_ms: u128,
    client: Option<Box<dyn Error>>,
    cleanup: Option<Box<dyn Error>>,
}

impl fmt::Display for BackendFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "official .NET {} {} atomic gate failed; client_elapsed_ms={}; cleanup_elapsed_ms={}",
            self.sdk_version, self.backend, self.client_elapsed_ms, self.cleanup_elapsed_ms,
        )?;
        if let Some(error) = &self.client {
            write!(formatter, "\nclient: {error}")?;
        }
        if let Some(error) = &self.cleanup {
            write!(formatter, "\ncleanup: {error}")?;
        }
        Ok(())
    }
}

impl Error for BackendFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.client.as_deref().or(self.cleanup.as_deref())
    }
}

pub(super) fn finish(
    sdk_version: &str,
    backend: &str,
    client_elapsed: Duration,
    cleanup_elapsed: Duration,
    client: TestResult,
    cleanup: TestResult,
) -> TestResult {
    let client = client.err();
    let cleanup = cleanup.err();
    if client.is_none() && cleanup.is_none() {
        return Ok(());
    }
    Err(Box::new(BackendFailure {
        sdk_version: sdk_version.to_owned(),
        backend: backend.to_owned(),
        client_elapsed_ms: client_elapsed.as_millis(),
        cleanup_elapsed_ms: cleanup_elapsed.as_millis(),
        client,
        cleanup,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failed(message: &str) -> TestResult {
        Err(std::io::Error::other(message.to_owned()).into())
    }

    #[test]
    fn client_and_cleanup_failures_both_keep_their_context() {
        let error = finish(
            "sdk-sentinel",
            "backend-sentinel",
            Duration::from_millis(11),
            Duration::from_millis(22),
            failed("client-sentinel"),
            failed("cleanup-sentinel"),
        )
        .expect_err("both failures must be reported");
        for detail in [format!("{error}"), format!("{error:?}")] {
            for expected in [
                "sdk-sentinel",
                "backend-sentinel",
                "client-sentinel",
                "cleanup-sentinel",
                "client_elapsed_ms",
                "cleanup_elapsed_ms",
                "11",
                "22",
            ] {
                assert!(detail.contains(expected), "missing {expected}: {detail}");
            }
        }
        assert_eq!(
            error.source().expect("primary failure source").to_string(),
            "client-sentinel"
        );
    }

    #[test]
    fn cleanup_only_failure_is_not_discarded() {
        let error = finish(
            "sdk",
            "fjall",
            Duration::ZERO,
            Duration::ZERO,
            Ok(()),
            failed("cleanup-only"),
        )
        .expect_err("cleanup failure must fail the gate");
        assert!(error.to_string().contains("cleanup-only"));
        assert_eq!(
            error.source().expect("cleanup failure source").to_string(),
            "cleanup-only"
        );
    }

    #[test]
    fn client_only_failure_is_not_discarded() {
        let error = finish(
            "sdk",
            "memory",
            Duration::ZERO,
            Duration::ZERO,
            failed("client-only"),
            Ok(()),
        )
        .expect_err("client failure must fail the gate");
        assert!(error.to_string().contains("client-only"));
    }

    #[test]
    fn healthy_client_and_cleanup_remain_successful() {
        finish(
            "sdk",
            "memory",
            Duration::ZERO,
            Duration::ZERO,
            Ok(()),
            Ok(()),
        )
        .expect("both operations succeeded");
    }
}
