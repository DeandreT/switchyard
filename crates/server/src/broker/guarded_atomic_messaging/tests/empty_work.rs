use super::*;
use protocol_amqp::{
    AtomicMessagingWorkUsage, AtomicTransactionDischarge, AtomicTransactionRegistry,
    AtomicTransactionRegistryHandle, AtomicTransactionSubmission,
    OwnedEmptyAtomicMessagingSubmission,
};
use std::task::{Context, Wake, Waker};

type EmptyWork = (
    AtomicTransactionRegistry,
    AtomicTransactionRegistryHandle,
    OwnedEmptyAtomicMessagingSubmission,
);

fn empty_work() -> TestResult<EmptyWork> {
    let (mut registry, handle) = AtomicTransactionRegistry::new();
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    let AtomicTransactionDischarge::Submit(AtomicTransactionSubmission::Empty(submission)) =
        registry.discharge(&controller, &id, false)?
    else {
        return Err("fresh unbound commit must hand off empty work".into());
    };
    Ok((registry, handle, submission))
}

fn charged(handle: &AtomicTransactionRegistryHandle) -> AtomicMessagingWorkUsage {
    let usage = handle.work_usage();
    assert_eq!(usage.groups(), 1);
    assert_eq!(usage.content_bytes(), 0);
    assert_eq!(usage.value_items(), 0);
    usage
}

fn refunded(handle: &AtomicTransactionRegistryHandle) {
    assert_eq!(handle.work_usage(), AtomicMessagingWorkUsage::default());
}

async fn empty_owned_and_blocking_success_do_no_owner_io<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let broker = node.broker.handle();
    let ready = broker.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let (_registry, handle, submission) = empty_work()?;
    charged(&handle);
    let observer = submission.permit().clone();
    let application = timeout(
        DEADLINE,
        broker.submit_empty_atomic_messaging_owned(submission),
    )
    .await??;
    assert_eq!(
        application,
        AtomicMessagingApplication {
            outcomes: vec![],
            enqueue_targets: vec![]
        }
    );
    assert_eq!(observer.state(), AtomicCommitState::Committed);
    node.assert_no_work(&before)?;
    node.fence().await?;
    refunded(&handle);
    assert_eq!(
        node.controls.reads.load(Ordering::SeqCst),
        1,
        "only the explicit fence reads"
    );
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());

    node.controls.reset_counts();
    let (_registry, handle, submission) = empty_work()?;
    charged(&handle);
    let observer = submission.permit().clone();
    let blocking_broker = broker.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        blocking_broker.submit_empty_atomic_messaging_owned_blocking(submission)
    });
    let application = timeout(DEADLINE, blocking).await???;
    assert!(application.outcomes.is_empty());
    assert!(application.enqueue_targets.is_empty());
    assert_eq!(observer.state(), AtomicCommitState::Committed);
    node.assert_no_work(&before)?;
    node.fence().await?;
    refunded(&handle);
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    Ok(())
}

async fn preclaim_empty_abort_does_no_owner_io<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let broker = node.broker.handle();
    let ready = broker.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let (_registry, handle, submission) = empty_work()?;
    charged(&handle);
    let observer = submission.permit().clone();
    assert!(observer.abort());
    assert_eq!(
        timeout(
            DEADLINE,
            broker.submit_empty_atomic_messaging_owned(submission)
        )
        .await?,
        Err(GuardedAtomicSubmitError::Permit(
            AtomicCommitClaimError::Aborted
        ))
    );
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    node.assert_no_work(&before)?;
    node.fence().await?;
    refunded(&handle);
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 1);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    Ok(())
}

async fn unpolled_empty_factory_drop_aborts_and_refunds<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let broker = node.broker.handle();
    let (_registry, handle, submission) = empty_work()?;
    charged(&handle);
    let observer = submission.permit().clone();
    let future = broker.submit_empty_atomic_messaging_owned(submission);
    assert_eq!(observer.state(), AtomicCommitState::Pending);
    drop(future);
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    refunded(&handle);
    assert!(broker.requests.is_empty());
    node.assert_no_work(&before)?;
    Ok(())
}

async fn queued_empty_caller_drop_retains_charge_until_owner_discard<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let (resume, parked) = node.park_owner().await?;
    let before = node.snapshot()?;
    node.controls.reset_counts();
    let broker = node.broker.handle();
    let ready = broker.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let (_registry, handle, submission) = empty_work()?;
    let reserved = charged(&handle);
    let observer = submission.permit().clone();
    let mut future = Box::pin(broker.submit_empty_atomic_messaging_owned(submission));
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(broker.requests.len(), 1);
    drop(future);
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    assert_eq!(
        handle.work_usage(),
        reserved,
        "queued owner work still reserves the empty slot"
    );
    node.assert_no_work(&before)?;
    resume.release();
    timeout(DEADLINE, parked.recv_async()).await???;
    node.fence().await?;
    refunded(&handle);
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    Ok(())
}

async fn owner_stop_refunds_empty_queue_without_caller_drop<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let (resume, parked) = node.park_owner().await?;
    let before = node.snapshot()?;
    node.controls.reset_counts();
    let retained = node.broker.handle();
    retained.requests.send(Request::Stop)?;
    let (_registry, handle, submission) = empty_work()?;
    let reserved = charged(&handle);
    let observer = submission.permit().clone();
    let mut future = Box::pin(retained.submit_empty_atomic_messaging_owned(submission));
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(retained.requests.len(), 2);
    assert_eq!(handle.work_usage(), reserved);
    resume.release();
    timeout(DEADLINE, parked.recv_async()).await???;
    assert_eq!(
        timeout(DEADLINE, &mut future).await?,
        Err(GuardedAtomicSubmitError::BrokerStopped)
    );
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    refunded(&handle);
    assert!(retained.requests.is_empty());
    node.assert_no_work(&before)?;
    Ok(())
}

struct ReplyWake {
    permit: AtomicCommitPermit,
    handle: AtomicTransactionRegistryHandle,
    observations: Mutex<Vec<(AtomicCommitState, AtomicMessagingWorkUsage)>>,
}

impl ReplyWake {
    fn record(&self) {
        self.observations
            .lock()
            .unwrap()
            .push((self.permit.state(), self.handle.work_usage()));
    }
}

impl Wake for ReplyWake {
    fn wake(self: Arc<Self>) {
        self.record();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.record();
    }
}

#[test]
fn empty_reply_publication_retains_charge_for_success_and_preclaim_abort() -> TestResult {
    for aborted in [false, true] {
        let (_registry, handle, submission) = empty_work()?;
        let reserved = charged(&handle);
        let observer = submission.permit().clone();
        if aborted {
            assert!(observer.abort());
        }
        let probe = Arc::new(ReplyWake {
            permit: observer.clone(),
            handle: handle.clone(),
            observations: Mutex::new(vec![]),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut context = Context::from_waker(&waker);
        let (reply, response) = flume::bounded(1);
        let mut future = Box::pin(response.recv_async());
        assert!(future.as_mut().poll(&mut context).is_pending());
        crate::broker::atomic_work::apply_empty_owned(submission, reply);
        let expected = if aborted {
            AtomicCommitState::Aborted
        } else {
            AtomicCommitState::Committed
        };
        assert_eq!(
            probe.observations.lock().unwrap().first().copied(),
            Some((expected, reserved)),
            "empty work stays owned while the terminal reply wakes its caller"
        );
        assert_eq!(observer.state(), expected);
        refunded(&handle);
        let Poll::Ready(response) = future.as_mut().poll(&mut context) else {
            panic!("empty reply must already be available")
        };
        let response = response?;
        if aborted {
            assert_eq!(
                response,
                Err(GuardedAtomicSubmitError::Permit(
                    AtomicCommitClaimError::Aborted
                ))
            );
        } else {
            assert_eq!(
                response?,
                AtomicMessagingApplication {
                    outcomes: vec![],
                    enqueue_targets: vec![]
                }
            );
        }
    }
    Ok(())
}

macro_rules! suite {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;

            #[tokio::test]
            async fn empty_owned_and_blocking_success_do_no_owner_io() -> TestResult {
                super::empty_owned_and_blocking_success_do_no_owner_io($provider).await
            }
            #[tokio::test]
            async fn preclaim_empty_abort_does_no_owner_io() -> TestResult {
                super::preclaim_empty_abort_does_no_owner_io($provider).await
            }
            #[tokio::test]
            async fn unpolled_empty_factory_drop_aborts_and_refunds() -> TestResult {
                super::unpolled_empty_factory_drop_aborts_and_refunds($provider).await
            }
            #[tokio::test]
            async fn queued_empty_caller_drop_retains_charge_until_owner_discard() -> TestResult {
                super::queued_empty_caller_drop_retains_charge_until_owner_discard($provider).await
            }
            #[tokio::test]
            async fn owner_stop_refunds_empty_queue_without_caller_drop() -> TestResult {
                super::owner_stop_refunds_empty_queue_without_caller_drop($provider).await
            }
        }
    };
}

suite!(memory, testkit::MemoryProvider::new());
suite!(durable, testkit::DurableProvider::temporary()?);
