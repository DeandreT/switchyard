use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::mpsc::{self, SyncSender},
    task::Poll,
    thread::{self, ThreadId},
    time::Duration,
};

use tokio::sync::oneshot;

use super::{OwnerJoinError, RetiredOwner};

const DEADLINE: Duration = Duration::from_secs(5);

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(DEADLINE, future)
        .await
        .expect("controlled owner boundary exceeded its deadline")
}

async fn pending<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("join completed before its controlled boundary"),
    })
    .await;
}

struct ThreadRelease(Option<SyncSender<()>>);

impl ThreadRelease {
    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.try_send(());
        }
    }
}

impl Drop for ThreadRelease {
    fn drop(&mut self) {
        self.release();
    }
}

struct TestOwner<E> {
    owner: RetiredOwner<E>,
    retired: oneshot::Sender<()>,
    gate: ThreadRelease,
    entered: oneshot::Receiver<()>,
    returned: oneshot::Receiver<()>,
    owner_id: ThreadId,
}

fn gated_owner<E: Send + 'static>(result: Result<(), E>) -> TestOwner<E> {
    let (release, wait) = mpsc::sync_channel(1);
    let (entered, observed) = oneshot::channel();
    let (returned, completed) = oneshot::channel();
    let thread = thread::spawn(move || {
        let _ = entered.send(());
        wait.recv_timeout(DEADLINE)
            .expect("controlled owner thread was not released");
        let _ = returned.send(());
        result
    });
    let owner_id = thread.thread().id();
    let (retired, retirement) = oneshot::channel();
    TestOwner {
        owner: RetiredOwner::new(retirement, thread),
        retired,
        gate: ThreadRelease(Some(release)),
        entered: observed,
        returned: completed,
        owner_id,
    }
}

#[tokio::test]
async fn completed_thread_still_waits_for_actual_adapter_retirement() {
    let mut fixture = gated_owner(Ok::<(), ()>(()));
    bounded(fixture.entered).await.expect("owner entered");
    fixture.gate.release();
    bounded(fixture.returned).await.expect("owner returned");
    let mut join = Box::pin(fixture.owner.join());
    pending(join.as_mut()).await;
    pending(join.as_mut()).await;
    fixture
        .retired
        .send(())
        .expect("retirement observer exists");
    assert_eq!(bounded(join).await, Ok(()));
}

#[tokio::test]
async fn reported_retirement_does_not_skip_the_blocking_thread_join() {
    let mut fixture = gated_owner(Ok::<(), ()>(()));
    bounded(fixture.entered).await.expect("owner entered");
    fixture
        .retired
        .send(())
        .expect("retirement observer exists");
    let mut join = Box::pin(fixture.owner.join());
    pending(join.as_mut()).await;
    fixture.gate.release();
    assert_eq!(bounded(join).await, Ok(()));
}

struct DroppedResult(Option<oneshot::Sender<ThreadId>>);

impl Drop for DroppedResult {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(thread::current().id());
        }
    }
}

#[tokio::test]
async fn once_polled_waiter_loss_preserves_retirement_wait_and_join() {
    let (dropped, observed) = oneshot::channel();
    let mut fixture = gated_owner(Err(DroppedResult(Some(dropped))));
    bounded(fixture.entered).await.expect("owner entered");
    let mut join = Box::pin(fixture.owner.join());
    pending(join.as_mut()).await;
    drop(join);
    fixture
        .retired
        .send(())
        .expect("supervisor retains retirement");
    fixture.gate.release();
    let drop_thread = bounded(observed)
        .await
        .expect("supervisor consumed the joined result");
    // Dropping the native handle while this thread is gated would release its
    // eventual result on that thread, not on the supervisor that joined it.
    assert_ne!(drop_thread, fixture.owner_id);
}

#[tokio::test]
async fn once_polled_waiter_loss_after_retirement_preserves_join() {
    let (dropped, observed) = oneshot::channel();
    let mut fixture = gated_owner(Err(DroppedResult(Some(dropped))));
    bounded(fixture.entered).await.expect("owner entered");
    fixture
        .retired
        .send(())
        .expect("retirement observer exists");
    let mut join = Box::pin(fixture.owner.join());
    pending(join.as_mut()).await;
    drop(join);
    fixture.gate.release();
    let drop_thread = bounded(observed)
        .await
        .expect("supervisor consumed the joined result");
    assert_ne!(drop_thread, fixture.owner_id);
}

#[derive(Eq, PartialEq)]
struct PrivateError(&'static str);

#[tokio::test]
async fn exact_owner_error_is_preserved_without_diagnostic_payload() {
    let mut fixture = gated_owner(Err(PrivateError("private owner error")));
    fixture
        .retired
        .send(())
        .expect("retirement observer exists");
    fixture.gate.release();
    let error = bounded(fixture.owner.join())
        .await
        .expect_err("owner failed");
    assert_eq!(
        error,
        OwnerJoinError::Owner(PrivateError("private owner error"))
    );
    assert_eq!(format!("{error:?}"), "Owner(..)");
    assert_eq!(error.to_string(), "blocking storage owner failed");
    assert!(std::error::Error::source(&error).is_none());
}

#[tokio::test]
async fn native_thread_panic_is_not_an_owner_error_or_lost_retirement() {
    let (retired, retirement) = oneshot::channel();
    let thread = thread::spawn(|| -> Result<(), PrivateError> {
        panic!("controlled native owner panic");
    });
    let owner = RetiredOwner::new(retirement, thread);
    retired.send(()).expect("retirement observer exists");
    let error = bounded(owner.join()).await.expect_err("thread panicked");
    assert_eq!(error, OwnerJoinError::ThreadPanicked);
    assert_eq!(error.to_string(), "blocking storage owner panicked");
    assert!(!format!("{error:?}").contains("controlled"));
}

#[tokio::test]
async fn lost_retirement_is_reported_only_after_the_thread_is_joined() {
    let mut fixture = gated_owner(Ok::<(), ()>(()));
    bounded(fixture.entered).await.expect("owner entered");
    drop(fixture.retired);
    let mut join = Box::pin(fixture.owner.join());
    pending(join.as_mut()).await;
    fixture.gate.release();
    assert_eq!(bounded(join).await, Err(OwnerJoinError::RetirementLost));
    bounded(fixture.returned)
        .await
        .expect("owner returned before result");
}

#[tokio::test]
async fn lost_retirement_does_not_hide_the_exact_owner_failure() {
    let mut fixture = gated_owner(Err(PrivateError("preserved owner failure")));
    drop(fixture.retired);
    fixture.gate.release();
    assert_eq!(
        bounded(fixture.owner.join()).await,
        Err(OwnerJoinError::Owner(PrivateError(
            "preserved owner failure"
        )))
    );
}

#[tokio::test]
async fn opaque_token_and_error_debug_do_not_require_owner_error_debug() {
    let mut fixture = gated_owner(Err(PrivateError("secret diagnostic")));
    assert_eq!(format!("{:?}", fixture.owner), "RetiredOwner { .. }");
    fixture
        .retired
        .send(())
        .expect("retirement observer exists");
    fixture.gate.release();
    let error = bounded(fixture.owner.join())
        .await
        .expect_err("owner failed");
    assert_eq!(format!("{error:?}"), "Owner(..)");
}

#[tokio::test]
async fn unpolled_join_drop_has_no_authority_to_release_or_close_the_owner() {
    let mut fixture = gated_owner(Ok::<(), ()>(()));
    bounded(fixture.entered).await.expect("owner entered");
    fn owned<F: Future + Send + 'static>(future: F) -> F {
        future
    }
    drop(owned(fixture.owner.join()));
    assert!(fixture.retired.send(()).is_err());
    assert!(matches!(
        fixture.returned.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    fixture.gate.release();
    bounded(fixture.returned)
        .await
        .expect("owner remains independently controlled");
}
