//! Real binary startup refusal, not an AMQP authentication or transport proof.

use std::{
    error::Error,
    fs, io,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use tempfile::TempDir;

#[path = "../src/test_deadline.rs"]
mod test_deadline;

use test_deadline::WAIT;

struct StartupChild(Option<Child>);

impl Drop for StartupChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn bounded_output(command: &mut Command) -> io::Result<Output> {
    let mut process = StartupChild(Some(command.spawn()?));
    let deadline = Instant::now() + WAIT;
    loop {
        if process
            .0
            .as_mut()
            .expect("the startup child is owned")
            .try_wait()?
            .is_some()
        {
            return process
                .0
                .take()
                .expect("the exited startup child is owned")
                .wait_with_output();
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "startup did not refuse before the deadline",
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

struct StartupFixture {
    _directory: TempDir,
    certificate: PathBuf,
    private_key: PathBuf,
    shared_access_key: PathBuf,
    data_directory: PathBuf,
    occupied_listener: TcpListener,
}

impl StartupFixture {
    fn new() -> Result<Self, Box<dyn Error>> {
        let directory = TempDir::new()?;
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        let shared_access_key = directory.path().join("shared-access-key.txt");
        let data_directory = directory.path().join("must-not-be-created");
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec![String::from("localhost")])?;
        fs::write(&certificate, cert.pem())?;
        fs::write(&private_key, key_pair.serialize_pem())?;
        fs::write(&shared_access_key, "startup-test-key\n")?;
        let occupied_listener = TcpListener::bind("127.0.0.1:0")?;
        Ok(Self {
            _directory: directory,
            certificate,
            private_key,
            shared_access_key,
            data_directory,
            occupied_listener,
        })
    }

    fn command(&self, storage: &str, voters: u16) -> io::Result<Command> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_switchyard"));
        command
            .args(["--mode", "production", "--storage", storage])
            .arg("--voters")
            .arg(voters.to_string())
            .arg("--data-dir")
            .arg(&self.data_directory)
            .arg("--tls-certificate")
            .arg(&self.certificate)
            .arg("--tls-private-key")
            .arg(&self.private_key)
            .arg("--shared-access-key-name")
            .arg("RootManageSharedAccessKey")
            .arg("--shared-access-key-file")
            .arg(&self.shared_access_key)
            .args(["--namespace", "startup-guard"])
            .arg("--listen")
            .arg(self.occupied_listener.local_addr()?.to_string())
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Ok(command)
    }

    fn assert_refusal(&self, output: Output, expected: &str) -> Result<(), Box<dyn Error>> {
        assert_eq!(output.status.code(), Some(1));
        let stderr = String::from_utf8(output.stderr)?;
        let stdout = String::from_utf8(output.stdout)?;
        assert_eq!(stderr.trim(), format!("switchyard: {expected}"));
        assert!(!stdout.contains("configuration is valid"));
        assert!(!stdout.contains("accepting AMQP connections"));
        assert!(
            !self.data_directory.exists(),
            "startup opened the data directory"
        );
        Ok(())
    }
}

#[test]
fn production_durable_cli_refuses_before_storage_or_listening() -> Result<(), Box<dyn Error>> {
    let fixture = StartupFixture::new()?;
    for voters in [3, 5] {
        let mut command = fixture.command("fjall", voters)?;
        let output = bounded_output(&mut command)?;
        fixture.assert_refusal(
            output,
            "production mode requires quorum replication, which is not implemented",
        )?;
    }
    Ok(())
}

#[test]
fn production_memory_cli_keeps_its_existing_backend_refusal() -> Result<(), Box<dyn Error>> {
    let fixture = StartupFixture::new()?;
    for voters in [3, 5] {
        let mut command = fixture.command("memory", voters)?;
        let output = bounded_output(&mut command)?;
        fixture.assert_refusal(output, "production mode cannot run on in-memory storage")?;
    }
    Ok(())
}

/// Rewrites only the marker, as an older build's store would hold it.
fn stamp_store_format(directory: &std::path::Path, version: u32) -> Result<(), Box<dyn Error>> {
    let database = fjall::Database::builder(directory).open()?;
    let meta = database.keyspace("meta", fjall::KeyspaceCreateOptions::default)?;
    let mut batch = database
        .batch()
        .durability(Some(fjall::PersistMode::SyncAll));
    batch.insert(&meta, b"format_version", version.to_be_bytes());
    batch.commit()?;
    Ok(())
}

fn stored_format(directory: &std::path::Path) -> Result<Vec<u8>, Box<dyn Error>> {
    use fjall::Readable;
    let database = fjall::Database::builder(directory).open()?;
    let meta = database.keyspace("meta", fjall::KeyspaceCreateOptions::default)?;
    let marker = database.snapshot().get(&meta, b"format_version")?;
    Ok(marker.map(|marker| marker.to_vec()).unwrap_or_default())
}

#[test]
fn development_durable_cli_refuses_older_stores_before_listening() -> Result<(), Box<dyn Error>> {
    let fixture = StartupFixture::new()?;
    let directory = TempDir::new()?;
    drop(storage::FjallStore::open(directory.path())?);
    for (version, expected) in [
        (
            2_u32,
            format!(
                "store directory {0} holds format 2; stop every writer, then run `switchyardctl store upgrade --data-dir {0}`",
                directory.path().display()
            ),
        ),
        (
            1,
            String::from(
                "store directory holds format version 1, but this build reads and writes version 3",
            ),
        ),
    ] {
        stamp_store_format(directory.path(), version)?;
        // The occupied listener proves the storage refusal precedes listening.
        let output = bounded_output(
            Command::new(env!("CARGO_BIN_EXE_switchyard"))
                .args(["--mode", "development", "--storage", "fjall"])
                .arg("--data-dir")
                .arg(directory.path())
                .arg("--listen")
                .arg(fixture.occupied_listener.local_addr()?.to_string())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped()),
        )?;
        assert_eq!(output.status.code(), Some(1));
        let stderr = String::from_utf8(output.stderr)?;
        assert_eq!(stderr.trim(), format!("switchyard: {expected}"));
        assert!(!String::from_utf8(output.stdout)?.contains("accepting AMQP connections"));
        assert_eq!(stored_format(directory.path())?, version.to_be_bytes());
    }
    Ok(())
}
