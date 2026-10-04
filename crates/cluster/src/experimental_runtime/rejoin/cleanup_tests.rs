use std::{
    future::poll_fn,
    io::Read,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};

use tokio::{
    runtime::{Builder, Handle},
    sync::oneshot,
};

use super::{AttemptReceipt, Continuity, Error, GuardedReadyNode, Resources, RetirementCleanup};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(5);
const CHILD_ENV: &str = "SWITCHYARD_CLEANUP_RESCUE_CHILD";
const CHILD_MARKER: &str = "closed-runtime cleanup spawn regression passed";
const CHILD_TEST: &str =
    "experimental_runtime::rejoin::cleanup_tests::closed_runtime_spawn_is_bounded";

fn empty_resources(engine_possible: bool) -> Resources {
    Resources {
        stores: None,
        pending: None,
        starting: None,
        node: None,
        retirement: None,
        shutting_down: None,
        engine_possible,
        failure: None,
    }
}

fn closed_runtime() -> TestResult<Handle> {
    let runtime = Builder::new_current_thread().build()?;
    let handle = runtime.handle().clone();
    drop(runtime);
    Ok(handle)
}

fn assert_unavailable(receipt: &AttemptReceipt) -> TestResult {
    let completion = receipt
        .completed()
        .ok_or("cleanup publisher remained open")?;
    assert_eq!(completion.cleanup, Err(Error::TaskFailed));
    assert!(matches!(completion.continuity, Continuity::Unavailable));
    Ok(())
}

#[test]
fn an_unpolled_cleanup_drops_without_claiming_joined_continuity() -> TestResult {
    let runtime = closed_runtime()?;
    let (receipt, publisher) = AttemptReceipt::new(8);
    let (reply, mut response) = oneshot::channel::<Result<GuardedReadyNode, Error>>();
    let cleanup = RetirementCleanup {
        resources: Some(empty_resources(false)),
        completion: Some(publisher),
        runtime,
        reply: Some(reply),
        error: Error::Closed,
        polled: false,
    };
    drop(cleanup);
    assert_unavailable(&receipt)?;
    assert!(matches!(
        response.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
    Ok(())
}

fn closed_runtime_spawn_child() -> TestResult {
    let runtime = closed_runtime()?;
    let (receipt, publisher) = AttemptReceipt::new(8);
    let (reply, mut response) = oneshot::channel::<Result<GuardedReadyNode, Error>>();
    let cleanup = RetirementCleanup {
        resources: Some(empty_resources(true)),
        completion: Some(publisher),
        runtime: runtime.clone(),
        reply: Some(reply),
        error: Error::Closed,
        polled: false,
    };
    // Tokio drops this unpolled task synchronously when its task list is
    // closed. The child process isolates any future stack-overflow regression.
    let task = runtime.spawn(cleanup.run());
    assert!(task.is_finished());
    assert_unavailable(&receipt)?;
    assert!(matches!(
        response.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
    println!("\n{CHILD_MARKER}");
    Ok(())
}

#[test]
fn closed_runtime_spawn_is_bounded() -> TestResult {
    if std::env::var_os(CHILD_ENV).is_some() {
        return closed_runtime_spawn_child();
    }
    let mut child = Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD_TEST, "--nocapture"])
        .env(CHILD_ENV, "1")
        .current_dir(std::env::temp_dir())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let deadline = Instant::now() + DEADLINE;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err("closed-runtime cleanup child exceeded its deadline".into());
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error.into());
            }
        }
    };
    let mut output = String::new();
    child
        .stdout
        .take()
        .ok_or("cleanup child stdout unavailable")?
        .take(4096)
        .read_to_string(&mut output)?;
    let mut errors = String::new();
    child
        .stderr
        .take()
        .ok_or("cleanup child stderr unavailable")?
        .take(4096)
        .read_to_string(&mut errors)?;
    child.wait()?;
    assert!(
        status.success(),
        "closed-runtime cleanup child failed: {status}; stdout={output:?}; stderr={errors:?}"
    );
    assert!(output.lines().any(|line| line == CHILD_MARKER));
    Ok(())
}

struct ReleaseOnDrop(Option<oneshot::Sender<()>>);

impl ReleaseOnDrop {
    fn release(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.release();
    }
}

#[tokio::test]
async fn a_polled_cleanup_transfers_its_pending_join_to_a_live_rescue() -> TestResult {
    let runtime = Handle::current();
    let (receipt, publisher) = AttemptReceipt::new(8);
    let (release, released) = oneshot::channel();
    let mut release = ReleaseOnDrop(Some(release));
    let finished = Arc::new(AtomicBool::new(false));
    let did_finish = finished.clone();
    let starting = runtime.spawn(async move {
        released.await.map_err(|_| Error::TaskFailed)?;
        did_finish.store(true, Ordering::Release);
        Err(Error::CoreFailure)
    });
    let mut resources = empty_resources(true);
    resources.starting = Some(starting);
    let cleanup = RetirementCleanup {
        resources: Some(resources),
        completion: Some(publisher),
        runtime,
        reply: None,
        error: Error::Closed,
        polled: false,
    };
    let mut running = cleanup.run();
    poll_fn(|cx| match running.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(Ok::<_, &'static str>(())),
        Poll::Ready(()) => Poll::Ready(Err("cleanup finished before its startup join")),
    })
    .await?;
    drop(running);
    assert!(receipt.completed().is_none());
    assert!(!finished.load(Ordering::Acquire));
    release.release();
    let completion = tokio::time::timeout(DEADLINE, receipt.join()).await?;
    assert!(finished.load(Ordering::Acquire));
    assert_eq!(completion.cleanup, Err(Error::TaskFailed));
    assert!(matches!(completion.continuity, Continuity::Unavailable));
    // This proves transfer of the exact async join, not native-storage joins.
    Ok(())
}
