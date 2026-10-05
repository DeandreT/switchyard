use super::{
    fixture::*,
    observed::{Mode, Observed},
    *,
};
use std::{
    collections::VecDeque,
    io::{self, Read},
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use storage::{SnapshotCatalogReader, StateStore};

const DIRECTORY: &str = "SWITCHYARD_LOCAL_COMPACTION_DIRECTORY";
const STAGE: &str = "SWITCHYARD_LOCAL_COMPACTION_EXIT_STAGE";
const CHILD_TEST: &str = "experimental_local_compaction::tests::crash::child_process_exit_boundary";
const DEADLINE: Duration = Duration::from_secs(30);
const OUTPUT_BYTES: usize = 16 * 1024;

#[tokio::test]
async fn child_process_exit_boundary() -> TestResult {
    let Some(directory) = std::env::var_os(DIRECTORY) else {
        return Ok(());
    };
    let directory = std::path::PathBuf::from(directory);
    let stage = std::env::var(STAGE)?;
    let mode = if stage.ends_with("before") {
        Mode::ExitBefore
    } else if stage.ends_with("after") {
        Mode::ExitAfter
    } else {
        return Err("unknown local compaction child stage".into());
    };
    let (writer, log_control) = Observed::new(FjallReplicaStore::open(directory.join("log"))?);
    let log = ExperimentalCompactionLogStore::open(writer, profile()?)?;
    let opened = (|| -> TestResult<_> {
        let (writer, control) =
            Observed::new(FjallCatalogReplicaStore::open(directory.join("state"))?);
        Ok((
            ExperimentalStateMachine::open_with_snapshot_catalog(writer, stream()?)?,
            control,
        ))
    })();
    let (state, state_control) = match opened {
        Ok(value) => value,
        Err(error) => {
            log.shutdown().await?;
            return Err(error);
        }
    };
    let mut pair = pair(log, state).await?;
    if stage.starts_with("catalog-") {
        state_control.fail(mode);
    } else if stage.starts_with("log-") {
        log_control.fail(mode);
    } else {
        pair.shutdown().await?;
        return Err("unknown local compaction child owner".into());
    }
    let result = pair.compact().await;
    let joined = pair.shutdown().await;
    result?;
    joined?;
    Err("the local compaction process exit did not occur".into())
}

#[tokio::test]
async fn exit_before_source_commit_preserves_old_catalog_and_log() -> TestResult {
    recover("catalog-before", 91, false, false).await
}
#[tokio::test]
async fn exit_after_source_sync_preserves_log_and_recovers_catalog() -> TestResult {
    recover("catalog-after", 92, true, false).await
}
#[tokio::test]
async fn exit_before_log_commit_recovers_catalog_with_retained_proof() -> TestResult {
    recover("log-before", 91, true, false).await
}
#[tokio::test]
async fn exit_after_log_sync_recovers_whole_baseline_delete_progress() -> TestResult {
    recover("log-after", 92, true, true).await
}

async fn recover(stage: &str, code: i32, catalog_present: bool, compacted: bool) -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let log_path = directory.path().join("log");
    let state_path = directory.path().join("state");
    let log_writer = FjallReplicaStore::open(&log_path)?;
    let log_reader = log_writer.reader();
    let state_writer = FjallCatalogReplicaStore::open(&state_path)?;
    let business = state_writer.reader();
    let (log, state, _, _) = seed(log_writer, state_writer, 2, 4).await?;
    let setup: TestResult<_> = async {
        Ok((
            log_reader.snapshot()?,
            business.snapshot()?,
            state.checkpoint().await?,
        ))
    }
    .await;
    let (a, b) = tokio::join!(log.shutdown(), state.shutdown());
    let (old_log, old_business, old_checkpoint) = setup?;
    a?;
    b?;
    drop(log_reader);
    drop(business);
    let expected_image = domain::EncodedCommittedImage::encode(
        domain::CommittedImageRole::CreateSendV1,
        stream()?,
        &old_business,
    )?;
    let expected_metadata =
        crate::EncodedNativeSnapshotMetadata::encode(expected_image.as_bytes())?;
    assert_eq!(
        crate::DecodedNativeSnapshotPair::decode(
            expected_metadata.as_bytes(),
            expected_image.as_bytes()
        )?
        .checkpoint(),
        &old_checkpoint
    );
    let retained_bytes = old_log
        .entries()
        .iter()
        .filter(|(key, _)| {
            key.first() == Some(&0x10) && u64::from_be_bytes(key[1..].try_into().unwrap()) >= 3
        })
        .map(|(_, value)| value.len() as u64)
        .sum();
    let (expected_baseline, expected_progress) =
        crate::experimental_log::local_compaction::encode_expected_controls_for_test(
            &profile()?,
            1,
            expected_metadata.as_bytes(),
            Some(crate::LogVote::new_committed(1, 7)),
            Some(id(4)),
            2,
            retained_bytes,
        )?;
    let mut expected_log = old_log.entries().to_vec();
    let covered = expected_log
        .iter()
        .filter(|(key, _)| {
            key.first() == Some(&0x10) && u64::from_be_bytes(key[1..].try_into().unwrap()) <= 2
        })
        .map(|(key, _)| u64::from_be_bytes(key[1..].try_into().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(covered, vec![0, 1, 2]);
    expected_log.retain(|(key, _)| {
        key.first() != Some(&0x10) || u64::from_be_bytes(key[1..].try_into().unwrap()) > 2
    });
    for (key, value) in &mut expected_log {
        if key == &[2] {
            *value = expected_progress.clone();
        }
        if key == &[3] {
            *value = expected_baseline.clone();
        }
    }
    let (status, output) = run_child(directory.path(), stage)?;
    assert_eq!(
        status.code(),
        Some(code),
        "local compaction child: {output}"
    );

    let writer = FjallReplicaStore::open(&log_path)?;
    let reader = writer.reader();
    let log = ExperimentalCompactionLogStore::open(writer, profile()?)?;
    let opened = (|| -> TestResult<_> {
        let writer = FjallCatalogReplicaStore::open(&state_path)?;
        let business = writer.reader();
        let catalog = writer.catalog_reader();
        Ok((
            ExperimentalStateMachine::open_with_snapshot_catalog(writer, stream()?)?,
            business,
            catalog,
        ))
    })();
    let (state, business, catalog) = match opened {
        Ok(value) => value,
        Err(error) => {
            log.shutdown().await?;
            return Err(error);
        }
    };
    let result: TestResult = async {
        assert_eq!(business.snapshot()?, old_business);
        assert_eq!(state.checkpoint().await?, old_checkpoint);
        let actual = catalog.read_catalog()?;
        assert_eq!(actual.is_some(), catalog_present);
        if let Some(actual) = actual {
            assert_eq!(actual.metadata(), expected_metadata.as_bytes());
            assert_eq!(actual.artifact(), expected_image.as_bytes());
            assert_eq!(
                crate::DecodedNativeSnapshotPair::decode(actual.metadata(), actual.artifact())?
                    .checkpoint(),
                &old_checkpoint
            );
        }
        let snapshot = reader.snapshot()?;
        if compacted {
            assert_eq!(snapshot.entries(), expected_log);
            assert_eq!(
                log.read_limited(0, u64::MAX)
                    .await?
                    .iter()
                    .map(|entry| entry.log_id.index)
                    .collect::<Vec<_>>(),
                vec![3, 4]
            );
        } else {
            assert_eq!(snapshot, old_log);
            assert_eq!(log.read_limited(0, u64::MAX).await?.len(), 5);
        }
        Ok(())
    }
    .await;
    let (log, state, ()) = sources_or_cleanup(result, log, state).await?;
    drop(reader);
    drop(business);
    drop(catalog);
    let mut pair = pair(log, state).await?;
    let result: TestResult = async {
        assert_eq!(pair.compact().await?.ordinal(), 1);
        Ok(())
    }
    .await;
    let joined = pair.shutdown().await;
    result?;
    joined?;
    // The recovered owner replies are not used as durable truth. This second
    // independent open revalidates the actual whole log and retained pair.
    super::reopen::reacquire(&log_path, &state_path).await
}

fn capture(mut pipe: impl Read) -> io::Result<Vec<u8>> {
    let mut tail = VecDeque::with_capacity(OUTPUT_BYTES);
    let mut bytes = [0; 4096];
    loop {
        let count = pipe.read(&mut bytes)?;
        if count == 0 {
            break;
        }
        for byte in &bytes[..count] {
            if tail.len() == OUTPUT_BYTES {
                tail.pop_front();
            }
            tail.push_back(*byte);
        }
    }
    Ok(tail.into_iter().collect())
}
struct ChildGuard {
    child: Option<Child>,
    stdout: Option<JoinHandle<io::Result<Vec<u8>>>>,
    stderr: Option<JoinHandle<io::Result<Vec<u8>>>>,
}
impl ChildGuard {
    fn reap(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    fn output(&mut self) -> TestResult<String> {
        let stdout = self.stdout.take();
        let stderr = self.stderr.take();
        let stdout = match stdout {
            Some(reader) => reader
                .join()
                .map_err(|_| "local child stdout reader panicked"),
            None => Err("missing local child stdout"),
        };
        let stderr = match stderr {
            Some(reader) => reader
                .join()
                .map_err(|_| "local child stderr reader panicked"),
            None => Err("missing local child stderr"),
        };
        let stdout = stdout??;
        let stderr = stderr??;
        Ok(format!(
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        ))
    }
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.reap();
        if let Some(reader) = self.stdout.take() {
            let _ = reader.join();
        }
        if let Some(reader) = self.stderr.take() {
            let _ = reader.join();
        }
    }
}
fn run_child(directory: &Path, stage: &str) -> TestResult<(ExitStatus, String)> {
    let child = Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD_TEST, "--test-threads=1", "--nocapture"])
        .env(DIRECTORY, directory)
        .env(STAGE, stage)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut child = ChildGuard {
        child: Some(child),
        stdout: None,
        stderr: None,
    };
    let stdout = child
        .child
        .as_mut()
        .ok_or("missing local child")?
        .stdout
        .take()
        .ok_or("missing local child stdout")?;
    child.stdout = Some(thread::spawn(move || capture(stdout)));
    let stderr = child
        .child
        .as_mut()
        .ok_or("missing local child")?
        .stderr
        .take()
        .ok_or("missing local child stderr")?;
    child.stderr = Some(thread::spawn(move || capture(stderr)));
    let deadline = Instant::now() + DEADLINE;
    let status: TestResult<ExitStatus> = loop {
        match child
            .child
            .as_mut()
            .ok_or("missing local child")?
            .try_wait()
        {
            Ok(Some(status)) => {
                child.child.take();
                break Ok(status);
            }
            Err(error) => break Err(error.into()),
            Ok(None) => {}
        }
        if Instant::now() >= deadline {
            break Err("local child exceeded its deadline".into());
        }
        thread::sleep(Duration::from_millis(10));
    };
    if status.is_err() {
        child.reap();
    }
    let output = child.output();
    match status {
        Ok(status) => Ok((status, output?)),
        Err(error) => Err(format!(
            "{error}\n{}",
            output.unwrap_or_else(|_| "local child diagnostics unavailable".into())
        )
        .into()),
    }
}
