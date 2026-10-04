use std::{
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

use super::*;

const DIRECTORY: &str = "SWITCHYARD_CATALOG_TEST_DIRECTORY";
const STAGE: &str = "SWITCHYARD_CATALOG_TEST_EXIT_STAGE";
const CHILD_TEST: &str = "crash::child_process_exit_boundary";

#[test]
fn child_process_exit_boundary() -> TestResult {
    let Some(directory) = std::env::var_os(DIRECTORY) else {
        return Ok(());
    };
    let stage = std::env::var(STAGE)?;
    let mut writer = FjallCatalogReplicaStore::open(directory)?;
    if stage == "before" {
        std::process::exit(71);
    }
    if stage != "after" {
        return Err("unknown catalog child stage".into());
    }
    writer.commit_with_catalog(
        WriteBatch::default()
            .put(b"business", b"new")
            .put(b"second", b"complete"),
        SnapshotCatalogRecord::new(b"new metadata", b"new artifact")?,
    )?;
    // No writer/database destructor runs after the real SyncAll commit.
    std::process::exit(72);
}

fn recover_boundary(stage: &str, committed: bool) -> TestResult {
    for initialized in [false, true] {
        let directory = TempDir::new()?;
        let mut writer = FjallCatalogReplicaStore::open(directory.path())?;
        let reader = writer.reader();
        let catalog_reader = writer.catalog_reader();
        if initialized {
            writer.commit_with_catalog(
                WriteBatch::default().put(b"business", b"old"),
                SnapshotCatalogRecord::new(b"old metadata", b"old artifact")?,
            )?;
        }
        let before = reader.snapshot()?;
        let before_catalog = catalog_reader.read_catalog()?;
        drop(reader);
        drop(catalog_reader);
        drop(writer);

        // These are before-commit/after-SyncAll exits, not mid-sync or power loss.
        assert_eq!(
            run_child(directory.path(), stage)?.code(),
            Some(if committed { 72 } else { 71 })
        );
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        let reader = writer.reader();
        let catalog_reader = writer.catalog_reader();
        assert_eq!(writer.is_initialized()?, initialized || committed);
        if committed {
            assert_eq!(
                reader.snapshot()?.entries(),
                &[
                    (b"business".to_vec(), b"new".to_vec()),
                    (b"second".to_vec(), b"complete".to_vec())
                ]
            );
            let catalog = catalog_reader
                .read_catalog()?
                .ok_or("missing recovered whole catalog")?;
            assert_eq!(catalog.metadata(), b"new metadata");
            assert_eq!(catalog.artifact(), b"new artifact");
        } else {
            assert_eq!(reader.snapshot()?, before);
            match (catalog_reader.read_catalog()?, before_catalog.as_ref()) {
                (None, None) => {}
                (Some(actual), Some(expected)) => {
                    assert_eq!(actual.metadata(), expected.metadata());
                    assert_eq!(actual.artifact(), expected.artifact());
                }
                _ => return Err("before-commit exit changed catalog presence".into()),
            }
        }
        drop(reader);
        drop(catalog_reader);
        drop(writer);
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        assert_eq!(writer.is_initialized()?, initialized || committed);
        assert_eq!(writer.reader().get(b"second")?.is_some(), committed);
    }
    Ok(())
}

#[test]
fn before_commit_abrupt_exit_preserves_the_complete_old_state() -> TestResult {
    recover_boundary("before", false)
}

#[test]
fn after_sync_abrupt_exit_recovers_the_complete_new_state() -> TestResult {
    recover_boundary("after", true)
}

fn run_child(directory: &Path, stage: &str) -> TestResult<ExitStatus> {
    let child = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg(CHILD_TEST)
        .arg("--test-threads=1")
        .env(DIRECTORY, directory)
        .env(STAGE, stage)
        // No pipes can retain the child or accumulate unbounded diagnostics.
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut child = ChildGuard(Some(child));
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let process = child.0.as_mut().ok_or("catalog child is unavailable")?;
        if let Some(status) = process.try_wait()? {
            child.0.take();
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err("catalog child exceeded its deadline".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

struct ChildGuard(Option<Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
