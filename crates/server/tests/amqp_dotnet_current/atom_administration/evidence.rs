use std::{any::Any, error::Error, panic::resume_unwind};

use super::TestResult;

pub(super) fn finish(
    outcome: Result<TestResult, Box<dyn Any + Send>>,
    cleanup: TestResult,
) -> TestResult {
    match (outcome, cleanup) {
        (Err(panic), cleanup) => {
            if let Err(error) = cleanup { eprintln!("Atom SDK cleanup failed before resuming original panic: {error}"); }
            resume_unwind(panic)
        }
        (Ok(Err(error)), Err(cleanup)) => Err(std::io::Error::other(format!(
            "Atom SDK stage failed: {error}; original listener/broker cleanup also failed: {cleanup}"
        )).into()),
        (Ok(Err(error)), Ok(())) => Err(error),
        (Ok(Ok(())), cleanup) => cleanup,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(message: &str) -> Box<dyn Error> {
        std::io::Error::other(message).into()
    }

    #[test]
    fn original_stage_and_cleanup_failures_are_both_retained() {
        let error = finish(
            Ok(Err(failure("stage sentinel"))),
            Err(failure("cleanup sentinel")),
        )
        .unwrap_err();
        assert!(error.to_string().contains("stage sentinel"));
        assert!(error.to_string().contains("cleanup sentinel"));
    }

    #[test]
    fn cleanup_failure_cannot_turn_into_success() {
        assert!(finish(Ok(Ok(())), Err(failure("cleanup sentinel"))).is_err());
        assert!(finish(Ok(Err(failure("stage sentinel"))), Ok(())).is_err());
        assert!(finish(Ok(Ok(())), Ok(())).is_ok());
    }
}
