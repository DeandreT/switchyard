use std::{error::Error, ffi::OsString, future::Future};

use tokio::process::Command;

#[path = "../../../native_admin_offline_jwt_cli/process.rs"]
mod owned;

type TestResult = Result<(), Box<dyn Error>>;

pub(crate) async fn with_atom_cli_child(
    arguments: &[OsString],
    sensitive: &[&str],
    probe: impl Future<Output = TestResult>,
) -> TestResult {
    let mut command = Command::new(env!("CARGO_BIN_EXE_switchyard"));
    command
        .env("TOKIO_WORKER_THREADS", "2")
        .env("RUST_LOG", "off")
        .args(arguments);
    owned::run(command, sensitive, probe).await
}

pub(crate) fn atom_cli_at<T, E: Error + 'static>(
    stage: &'static str,
    result: Result<T, E>,
) -> Result<T, Box<dyn Error>> {
    owned::at(stage, result)
}
