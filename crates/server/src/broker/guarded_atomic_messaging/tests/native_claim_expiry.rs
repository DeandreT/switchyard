use super::native_ready_fixture::NativeFixture;
use super::*;

use amqp::NativeTransactionState;
use protocol_amqp::{
    AtomicMessagingWorkBudget, AtomicMessagingWorkUsage, AtomicTransactionDischarge,
    AtomicTransactionRegistry, AtomicTransactionSubmission, OwnedNativeAtomicMessagingSubmission,
};

use crate::NativeAtomicSubmitError;

async fn expired_bound_claim_is_compensated_before_owner_io<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    let (mut native, ready) = NativeFixture::ready(false).await?;
    let budget = AtomicMessagingWorkBudget::new();
    let (observer, ticket) = permit();
    let mut staged = budget.stage(node.binding.clone())?;
    staged.try_push(send("expired-bound"))?;
    let mut owned = staged.into_submission(ticket);
    owned.restrict_claim_expiry_epoch_seconds(0);
    let mut logical = AtomicTransactionSubmission::Bound(owned);
    logical.restrict_claim_expiry_epoch_seconds(u64::MAX);
    let submission = OwnedNativeAtomicMessagingSubmission::new(ready, logical);
    let charged = budget.usage();
    assert_eq!(charged.groups(), 1);
    assert!(charged.content_bytes() > 0);

    let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(wake);
    let (release, parked) = node.park_owner().await?;
    let before = node.snapshot()?;
    node.controls.reset_counts();
    let mut response = Box::pin(handle.submit_native_atomic_messaging_owned(submission));
    std::future::poll_fn(|cx| {
        assert!(response.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(handle.requests.len(), 1);
    assert_eq!(observer.state(), AtomicCommitState::Pending);
    assert_eq!(native.observer.state(), NativeTransactionState::Ready);
    assert_eq!(budget.usage(), charged);
    node.assert_no_work(&before)?;

    release.release();
    assert_eq!(
        timeout(DEADLINE, parked.recv_async()).await???,
        Timestamp::from_millis(1_000)
    );
    let completion = timeout(DEADLINE, response).await??;
    assert!(matches!(
        completion.application(),
        Err(NativeAtomicSubmitError::Guarded(
            GuardedAtomicSubmitError::Permit(AtomicCommitClaimError::Aborted)
        ))
    ));
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    assert_eq!(native.observer.state(), NativeTransactionState::Aborted);
    node.assert_no_work(&before)?;
    assert!(timeout(NO_WAKE, &mut wake).await.is_err());
    let (_, resources) = completion.into_parts();
    native
        .finish(resources, NativeTransactionState::Aborted)
        .await?;
    node.fence().await?;
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 1);
    assert_eq!(node.snapshot()?, before);
    assert!(node.messages()?.is_empty());
    native.shutdown().await
}

async fn expired_empty_claim_is_compensated_without_fake_entity_validation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    let (mut native, ready) = NativeFixture::ready(true).await?;
    let (mut registry, usage) = AtomicTransactionRegistry::new();
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    let AtomicTransactionDischarge::Submit(mut logical) =
        registry.discharge(&controller, &id, false)?
    else {
        return Err("empty logical submission missing".into());
    };
    let AtomicTransactionSubmission::Empty(empty) = &mut logical else {
        return Err("a declaration without postings must stay unbound".into());
    };
    empty.restrict_claim_expiry_epoch_seconds(0);
    logical.restrict_claim_expiry_epoch_seconds(u64::MAX);
    let observer = logical.permit().clone();
    let submission = OwnedNativeAtomicMessagingSubmission::new(ready, logical);
    let charged = usage.work_usage();
    assert_eq!(charged.groups(), 1);
    assert_eq!(charged.content_bytes(), 0);
    assert_eq!(charged.value_items(), 0);
    let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(wake);
    let (release, parked) = node.park_owner().await?;
    let before = node.snapshot()?;
    node.controls.reset_counts();
    let mut response = Box::pin(handle.submit_native_atomic_messaging_owned(submission));
    std::future::poll_fn(|cx| {
        assert!(response.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(handle.requests.len(), 1);
    assert_eq!(observer.state(), AtomicCommitState::Pending);
    assert_eq!(native.observer.state(), NativeTransactionState::Ready);
    assert_eq!(usage.work_usage(), charged);
    node.assert_no_work(&before)?;
    release.release();
    assert_eq!(
        timeout(DEADLINE, parked.recv_async()).await???,
        Timestamp::from_millis(1_000)
    );
    let completion = timeout(DEADLINE, response).await??;
    assert!(matches!(
        completion.application(),
        Err(NativeAtomicSubmitError::Guarded(
            GuardedAtomicSubmitError::Permit(AtomicCommitClaimError::Aborted)
        ))
    ));
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    assert_eq!(native.observer.state(), NativeTransactionState::Aborted);
    node.assert_no_work(&before)?;
    assert!(timeout(NO_WAKE, &mut wake).await.is_err());
    let (_, resources) = completion.into_parts();
    native
        .finish(resources, NativeTransactionState::Aborted)
        .await?;
    node.fence().await?;
    assert_eq!(usage.work_usage(), AtomicMessagingWorkUsage::default());
    assert_eq!(
        registry.state(&controller, &id)?,
        AtomicCommitState::Aborted
    );
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 1);
    assert_eq!(node.snapshot()?, before);
    native.shutdown().await
}

async fn maximum_epoch_cap_preserves_bound_and_empty_commits<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    for empty in [false, true] {
        let (mut native, ready) = NativeFixture::ready(empty).await?;
        let budget = AtomicMessagingWorkBudget::new();
        let (mut registry, usage) = AtomicTransactionRegistry::new();
        let controller = registry.controller()?;
        let mut logical = if empty {
            let id = registry.declare(&controller)?;
            let AtomicTransactionDischarge::Submit(logical) =
                registry.discharge(&controller, &id, false)?
            else {
                return Err("empty logical submission missing".into());
            };
            logical
        } else {
            let (_, mut ticket) = permit();
            ticket.restrict_claim_expiry_epoch_seconds(u64::MAX);
            let mut staged = budget.stage(node.binding.clone())?;
            staged.try_push(send("maximum-horizon"))?;
            AtomicTransactionSubmission::Bound(staged.into_submission(ticket))
        };
        logical.restrict_claim_expiry_epoch_seconds(u64::MAX);
        let observer = logical.permit().clone();
        let submission = OwnedNativeAtomicMessagingSubmission::new(ready, logical);
        let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
        tokio::pin!(wake);
        let before = node.snapshot()?;
        node.controls.reset_counts();
        let completion = timeout(
            DEADLINE,
            handle.submit_native_atomic_messaging_owned(submission),
        )
        .await??;
        assert_eq!(observer.state(), AtomicCommitState::Committed);
        assert_eq!(native.observer.state(), NativeTransactionState::Committed);
        let (application, resources) = completion.into_parts();
        let application = application?;
        if empty {
            assert!(application.outcomes.is_empty());
            assert!(application.enqueue_targets.is_empty());
            node.assert_no_work(&before)?;
            assert!(timeout(NO_WAKE, &mut wake).await.is_err());
        } else {
            assert_eq!(
                application.outcomes,
                vec![CommandOutcome::Sent {
                    sequence: SequenceNumber::new(1)
                }]
            );
            assert_eq!(
                application.enqueue_targets,
                vec![node.binding.target().clone()]
            );
            assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
            assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 1);
            timeout(DEADLINE, &mut wake).await?;
        }
        native
            .finish(resources, NativeTransactionState::Committed)
            .await?;
        node.fence().await?;
        assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
        assert_eq!(usage.work_usage(), AtomicMessagingWorkUsage::default());
        native.shutdown().await?;
    }
    let before = node.snapshot()?;
    assert_eq!(node.messages()?.len(), 1);
    let (provider, reopened) = node.reopen()?;
    assert_eq!(reopened.snapshot()?, before);
    drop(reopened);
    drop(provider);
    Ok(())
}

macro_rules! backend_cases {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;

            #[tokio::test]
            async fn expired_bound_claim_is_compensated_before_owner_io() -> TestResult {
                super::expired_bound_claim_is_compensated_before_owner_io($provider).await
            }

            #[tokio::test]
            async fn expired_empty_claim_is_compensated_without_fake_entity_validation()
            -> TestResult {
                super::expired_empty_claim_is_compensated_without_fake_entity_validation($provider)
                    .await
            }

            #[tokio::test]
            async fn maximum_epoch_cap_preserves_bound_and_empty_commits() -> TestResult {
                super::maximum_epoch_cap_preserves_bound_and_empty_commits($provider).await
            }
        }
    };
}

backend_cases!(memory, testkit::MemoryProvider::new());
backend_cases!(durable, testkit::DurableProvider::temporary()?);
