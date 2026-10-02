use super::native_ready_fixture::NativeFixture;
use super::*;

use crate::{NativeAtomicMessagingCompletion, NativeAtomicSubmitError};
use amqp::{
    NativeFault, NativeTransactionError, NativeTransactionIdentity, NativeTransactionState,
};
use protocol_amqp::{
    AtomicMessagingWorkBudget, AtomicMessagingWorkUsage, AtomicTransactionDischarge,
    AtomicTransactionRegistry, AtomicTransactionSubmission, OwnedNativeAtomicMessagingSubmission,
};
use std::task::{Context, Wake, Waker};

fn posted(id: &str) -> CommandKind {
    CommandKind::SendEnvelope {
        message_id: id.to_owned(),
        body: id.as_bytes().to_vec(),
        time_to_live_millis: None,
        session_id: None,
        envelope: Box::new(domain::MessageEnvelope {
            body: domain::MessageBody::Value(domain::MessageValue::Bool(true)),
            ..domain::MessageEnvelope::default()
        }),
    }
}

fn paired(
    native: amqp::NativeReadySubmission,
    binding: &EntityBinding,
    kinds: Vec<CommandKind>,
    ticket: AtomicCommitTicket,
    budget: &AtomicMessagingWorkBudget,
) -> TestResult<OwnedNativeAtomicMessagingSubmission> {
    let mut staged = budget.stage(binding.clone())?;
    for kind in kinds {
        staged.try_push(kind)?;
    }
    Ok(OwnedNativeAtomicMessagingSubmission::new(
        native,
        AtomicTransactionSubmission::Bound(staged.into_submission(ticket)),
    ))
}

fn charge(budget: &AtomicMessagingWorkBudget) -> AtomicMessagingWorkUsage {
    let usage = budget.usage();
    assert_eq!(usage.groups(), 1);
    assert!(usage.content_bytes() > 0);
    assert_eq!(usage.value_items(), 1);
    usage
}

fn refunded(budget: &AtomicMessagingWorkBudget) {
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

fn enqueue(
    handle: &crate::BrokerHandle,
    submission: OwnedNativeAtomicMessagingSubmission,
) -> TestResult<flume::Receiver<NativeAtomicMessagingCompletion>> {
    let (reply, response) = flume::bounded(1);
    handle
        .requests
        .send(Request::ApplyNativeAtomicMessagingOwned {
            submission: Box::new(submission),
            reply,
        })?;
    Ok(response)
}

async fn failed_claims_have_no_owner_io<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    for failure in 0..3 {
        let (mut native, ready) = NativeFixture::ready(false).await?;
        let budget = AtomicMessagingWorkBudget::new();
        let (logical, ticket) = if failure == 2 {
            AtomicCommitPermit::new(Instant::now() - Duration::from_secs(1))
        } else {
            permit()
        };
        let submission = paired(
            ready,
            &node.binding,
            vec![posted("unclaimed")],
            ticket,
            &budget,
        )?;
        charge(&budget);
        if failure == 0 {
            native.close_data().await?;
        }
        if failure == 1 {
            assert!(logical.abort());
        }
        let before = node.snapshot()?;
        node.controls.reset_counts();
        let handle = node.broker.handle();
        let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
        tokio::pin!(wake);
        let completion = timeout(
            DEADLINE,
            handle.submit_native_atomic_messaging_owned(submission),
        )
        .await??;
        if failure == 0 {
            assert!(matches!(
                completion.application(),
                Err(NativeAtomicSubmitError::NativeClaim(
                    NativeTransactionError::Faulted(NativeFault::Closed)
                ))
            ));
            assert_eq!(native.observer.state(), NativeTransactionState::Faulted);
        } else {
            assert!(matches!(
                completion.application(),
                Err(NativeAtomicSubmitError::Guarded(
                    GuardedAtomicSubmitError::Permit(AtomicCommitClaimError::Aborted)
                ))
            ));
            assert_eq!(native.observer.state(), NativeTransactionState::Aborted);
        }
        assert_eq!(logical.state(), AtomicCommitState::Aborted);
        node.assert_no_work(&before)?;
        assert!(timeout(NO_WAKE, &mut wake).await.is_err());
        drop(completion.into_parts());
        node.fence().await?;
        refunded(&budget);
        assert_eq!(node.controls.reads.load(Ordering::SeqCst), 1);
        native.barrier().await?;
        native.shutdown().await?;
    }
    Ok(())
}

async fn empty_and_rejected_groups_publish_exact_decisions<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    let before = node.snapshot()?;
    for blocking in [false, true] {
        let (mut native, ready) = NativeFixture::ready(true).await?;
        let (mut registry, work) = AtomicTransactionRegistry::new();
        let controller = registry.controller()?;
        let id = registry.declare(&controller)?;
        let AtomicTransactionDischarge::Submit(logical_work) =
            registry.discharge(&controller, &id, false)?
        else {
            return Err("empty submission missing".into());
        };
        assert!(matches!(
            &logical_work,
            AtomicTransactionSubmission::Empty(_)
        ));
        let logical = logical_work.permit().clone();
        let submission = OwnedNativeAtomicMessagingSubmission::new(ready, logical_work);
        assert_eq!(work.work_usage().groups(), 1);
        node.controls.reset_counts();
        let completion = if blocking {
            let handle = handle.clone();
            timeout(
                DEADLINE,
                tokio::task::spawn_blocking(move || {
                    handle.submit_native_atomic_messaging_owned_blocking(submission)
                }),
            )
            .await???
        } else {
            timeout(
                DEADLINE,
                handle.submit_native_atomic_messaging_owned(submission),
            )
            .await??
        };
        assert_eq!(logical.state(), AtomicCommitState::Committed);
        assert_eq!(native.observer.state(), NativeTransactionState::Committed);
        let (application, resources) = completion.into_parts();
        let application = application?;
        assert!(application.outcomes.is_empty());
        assert!(application.enqueue_targets.is_empty());
        node.assert_no_work(&before)?;
        node.fence().await?;
        assert_eq!(work.work_usage(), AtomicMessagingWorkUsage::default());
        native
            .finish(resources, NativeTransactionState::Committed)
            .await?;
        native.shutdown().await?;
    }

    let (mut native, ready) = NativeFixture::ready(false).await?;
    let budget = AtomicMessagingWorkBudget::new();
    let (logical, ticket) = permit();
    let submission = paired(
        ready,
        &node.binding,
        vec![
            posted("rolled-back"),
            CommandKind::Complete {
                sequence: SequenceNumber::new(99),
                lock_token: LockToken::new(1),
            },
        ],
        ticket,
        &budget,
    )?;
    charge(&budget);
    node.controls.reset_counts();
    let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(wake);
    let completion = timeout(
        DEADLINE,
        handle.submit_native_atomic_messaging_owned(submission),
    )
    .await??;
    assert!(matches!(
        completion.application(),
        Err(NativeAtomicSubmitError::Guarded(
            GuardedAtomicSubmitError::Propose(ProposeError::Broker(
                BrokerError::MessageNotFound { .. }
            ))
        ))
    ));
    assert_eq!(logical.state(), AtomicCommitState::Rejected);
    assert_eq!(native.observer.state(), NativeTransactionState::Rejected);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert!(timeout(NO_WAKE, &mut wake).await.is_err());
    let (_, resources) = completion.into_parts();
    node.fence().await?;
    refunded(&budget);
    native
        .finish(resources, NativeTransactionState::Rejected)
        .await?;
    assert_eq!(
        node.controls.writes.load(Ordering::SeqCst),
        0,
        "native completion never reapplies the proposer"
    );
    native.shutdown().await?;
    Ok(())
}

async fn storage_failure_and_unwind_are_jointly_indeterminate<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    for failure in 0..4 {
        let (mut native, ready) = NativeFixture::ready(false).await?;
        let budget = AtomicMessagingWorkBudget::new();
        let (logical, ticket) = permit();
        let submission = paired(
            ready,
            &node.binding,
            vec![posted("uncertain")],
            ticket,
            &budget,
        )?;
        charge(&budget);
        let before = node.snapshot()?;
        node.controls.reset_counts();
        match failure {
            0 => node.controls.fail_read.store(true, Ordering::SeqCst),
            1 => node
                .controls
                .fail_before_commit
                .store(true, Ordering::SeqCst),
            2 => node
                .controls
                .fail_after_commit
                .store(true, Ordering::SeqCst),
            _ => node.controls.panic_read.store(true, Ordering::SeqCst),
        }
        {
            let handle = node.broker.handle();
            let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
            tokio::pin!(wake);
            let result = timeout(
                DEADLINE,
                handle.submit_native_atomic_messaging_owned(submission),
            )
            .await?;
            assert_eq!(logical.state(), AtomicCommitState::Indeterminate);
            assert_eq!(
                native.observer.state(),
                NativeTransactionState::Indeterminate
            );
            assert!(!logical.abort());
            if failure == 3 {
                assert!(matches!(
                    result,
                    Err(NativeAtomicSubmitError::Guarded(
                        GuardedAtomicSubmitError::BrokerStopped
                    ))
                ));
                assert_eq!(node.controls.reads.load(Ordering::SeqCst), 1);
                assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 0);
                assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
                refunded(&budget);
                assert_eq!(node.snapshot()?, before);
                native.barrier().await?;
            } else {
                let completion = result?;
                assert!(matches!(
                    completion.application(),
                    Err(NativeAtomicSubmitError::Guarded(
                        GuardedAtomicSubmitError::Propose(ProposeError::Broker(
                            BrokerError::Storage(_)
                        ))
                    ))
                ));
                let (_, resources) = completion.into_parts();
                node.fence().await?;
                refunded(&budget);
                if failure == 2 {
                    assert_ne!(node.snapshot()?, before);
                    let messages = node.messages()?;
                    assert_eq!(messages.len(), 1);
                    assert_eq!(messages[0].sequence, SequenceNumber::new(1));
                    assert_eq!(messages[0].message_id, "uncertain");
                    assert_eq!(messages[0].body, b"uncertain");
                } else {
                    assert_eq!(node.snapshot()?, before);
                }
                let writes = node.controls.writes.load(Ordering::SeqCst);
                native
                    .finish(resources, NativeTransactionState::Indeterminate)
                    .await?;
                assert_eq!(node.controls.writes.load(Ordering::SeqCst), writes);
            }
            assert!(timeout(NO_WAKE, &mut wake).await.is_err());
            native.shutdown().await?;
        }
    }
    let committed = node.snapshot()?;
    let (_provider, reopened) = node.reopen()?;
    assert_eq!(
        reopened.snapshot()?,
        committed,
        "whole post-apply commit survives reported failure and owner unwind"
    );
    Ok(())
}

async fn cancellation_and_owner_stop_release_the_whole_pair<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    for path in 0..4 {
        let (mut native, ready) = NativeFixture::ready(false).await?;
        let budget = AtomicMessagingWorkBudget::new();
        let (logical, ticket) = permit();
        let submission = paired(
            ready,
            &node.binding,
            vec![posted("canceled")],
            ticket,
            &budget,
        )?;
        let reserved = charge(&budget);
        let handle = node.broker.handle();
        let before = node.snapshot()?;
        let pause = if path == 0 {
            None
        } else {
            Some(node.park_owner().await?)
        };
        node.controls.reset_counts();
        if path == 2 {
            for _ in 0..COMMAND_QUEUE_DEPTH {
                let (reply, response) = flume::bounded(1);
                handle.requests.send(Request::LastApplied { reply })?;
                drop(response);
            }
        }
        if path == 3 {
            handle.requests.send(Request::Stop)?;
        }
        let mut future = Box::pin(handle.submit_native_atomic_messaging_owned(submission));
        if path != 0 {
            std::future::poll_fn(|cx| {
                assert!(future.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert_eq!(native.observer.state(), NativeTransactionState::Ready);
        }
        if path == 3 {
            assert_eq!(handle.requests.len(), 2);
            let (resume, parked) = pause.ok_or("owner pause missing")?;
            resume.release();
            timeout(DEADLINE, parked.recv_async()).await???;
            assert!(matches!(
                timeout(DEADLINE, &mut future).await?,
                Err(NativeAtomicSubmitError::Guarded(
                    GuardedAtomicSubmitError::BrokerStopped
                ))
            ));
            assert!(handle.requests.is_empty());
            assert_eq!(logical.state(), AtomicCommitState::Aborted);
            assert_eq!(native.observer.state(), NativeTransactionState::Faulted);
            refunded(&budget);
            node.assert_no_work(&before)?;
        } else {
            drop(future);
            assert_eq!(logical.state(), AtomicCommitState::Aborted);
            if path == 1 {
                assert_eq!(
                    budget.usage(),
                    reserved,
                    "queued pair retains logical work and native resources"
                );
                assert_eq!(native.observer.state(), NativeTransactionState::Ready);
            } else {
                refunded(&budget);
                assert_eq!(native.observer.state(), NativeTransactionState::Faulted);
            }
            node.assert_no_work(&before)?;
            if let Some((resume, parked)) = pause {
                resume.release();
                timeout(DEADLINE, parked.recv_async()).await???;
                node.fence().await?;
                assert_eq!(
                    node.controls.reads.load(Ordering::SeqCst),
                    if path == 2 {
                        COMMAND_QUEUE_DEPTH + 1
                    } else {
                        1
                    }
                );
                assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 0);
                assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
                assert_eq!(node.snapshot()?, before);
            }
            if path == 1 {
                assert_eq!(native.observer.state(), NativeTransactionState::Aborted);
            }
            refunded(&budget);
        }
        native.barrier().await?;
        native.shutdown().await?;
    }
    Ok(())
}

async fn closed_admission_returns_unique_resources<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    let before = node.snapshot()?;
    let binding = node.binding.clone();
    let store = node.store.clone();
    let controls = Arc::clone(&node.controls);
    // Keep the remaining Node fields alive so the durable directory survives
    // the owner's synchronous shutdown and both closed-admission requests.
    drop(node.broker);
    assert!(handle.requests.is_empty());
    controls.reset_counts();
    for blocking in [false, true] {
        let (mut native, ready) = NativeFixture::ready(false).await?;
        let budget = AtomicMessagingWorkBudget::new();
        let (logical, ticket) = permit();
        let submission = paired(
            ready,
            &binding,
            vec![posted("not-admitted")],
            ticket,
            &budget,
        )?;
        charge(&budget);
        let wake = handle.deliverable(binding.namespace(), binding.target());
        let completion = if blocking {
            let handle = handle.clone();
            timeout(
                DEADLINE,
                tokio::task::spawn_blocking(move || {
                    handle.submit_native_atomic_messaging_owned_blocking(submission)
                }),
            )
            .await???
        } else {
            timeout(
                DEADLINE,
                handle.submit_native_atomic_messaging_owned(submission),
            )
            .await??
        };
        assert!(matches!(
            completion.application(),
            Err(NativeAtomicSubmitError::Guarded(
                GuardedAtomicSubmitError::BrokerStopped
            ))
        ));
        assert_eq!(logical.state(), AtomicCommitState::Aborted);
        assert_eq!(native.observer.state(), NativeTransactionState::Faulted);
        refunded(&budget);
        let (application, resources) = completion.into_parts();
        assert!(matches!(
            application,
            Err(NativeAtomicSubmitError::Guarded(
                GuardedAtomicSubmitError::BrokerStopped
            ))
        ));
        drop(resources);
        assert!(handle.requests.is_empty());
        assert_eq!(controls.reads.load(Ordering::SeqCst), 0);
        assert_eq!(controls.writes.load(Ordering::SeqCst), 0);
        assert_eq!(controls.clock_calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.snapshot()?, before);
        assert!(timeout(NO_WAKE, wake).await.is_err());
        native.barrier().await?;
        native.shutdown().await?;
    }
    Ok(())
}

async fn started_close_and_caller_drop_cannot_revoke_commit<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let (mut native, ready) = NativeFixture::ready(false).await?;
    let before = node.snapshot()?;
    let budget = AtomicMessagingWorkBudget::new();
    let (logical, ticket) = permit();
    let submission = paired(
        ready,
        &node.binding,
        vec![posted("started")],
        ticket,
        &budget,
    )?;
    let reserved = charge(&budget);
    let (entered, resume) = node.controls.pause_commit();
    let handle = node.broker.handle();
    let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(wake);
    let mut future = Box::pin(handle.submit_native_atomic_messaging_owned(submission));
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    timeout(DEADLINE, entered.recv_async()).await??;
    assert_eq!(logical.state(), AtomicCommitState::Started);
    assert_eq!(
        native.observer.state(),
        NativeTransactionState::OwnerStarted
    );
    assert!(!logical.abort());
    native.close_data().await?;
    assert_eq!(
        native.observer.state(),
        NativeTransactionState::OwnerStarted
    );
    drop(future);
    assert_eq!(budget.usage(), reserved);
    assert_eq!(node.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut wake).await.is_err());
    resume.release();
    timeout(DEADLINE, &mut wake).await?;
    assert_eq!(logical.state(), AtomicCommitState::Committed);
    assert_eq!(native.observer.state(), NativeTransactionState::Committed);
    node.fence().await?;
    refunded(&budget);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 1);
    assert_eq!(node.messages()?.len(), 1);
    native.barrier().await?;
    native.shutdown().await?;
    Ok(())
}

async fn dropped_reply_still_commits_and_wakes_once<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    let (mut native, ready) = NativeFixture::ready(false).await?;
    let budget = AtomicMessagingWorkBudget::new();
    let (logical, ticket) = permit();
    let submission = paired(
        ready,
        &node.binding,
        vec![posted("lost-reply")],
        ticket,
        &budget,
    )?;
    let reserved = charge(&budget);
    let (entered, resume) = node.controls.pause_commit();
    let handle = node.broker.handle();
    let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(wake);
    let reply = enqueue(&handle, submission)?;
    timeout(DEADLINE, entered.recv_async()).await??;
    drop(reply);
    assert_eq!(logical.state(), AtomicCommitState::Started);
    assert_eq!(
        native.observer.state(),
        NativeTransactionState::OwnerStarted
    );
    assert_eq!(budget.usage(), reserved);
    resume.release();
    timeout(DEADLINE, &mut wake).await?;
    assert_eq!(logical.state(), AtomicCommitState::Committed);
    assert_eq!(native.observer.state(), NativeTransactionState::Committed);
    node.fence().await?;
    refunded(&budget);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.messages()?.len(), 1);
    let later = handle.deliverable(node.binding.namespace(), node.binding.target());
    assert!(
        timeout(NO_WAKE, later).await.is_err(),
        "no stale wake or repeated apply escapes lost reply"
    );
    native.barrier().await?;
    native.shutdown().await?;
    Ok(())
}

struct ReplyWake {
    logical: AtomicCommitPermit,
    native: NativeTransactionIdentity,
    budget: AtomicMessagingWorkBudget,
    observed: flume::Sender<(
        AtomicCommitState,
        NativeTransactionState,
        AtomicMessagingWorkUsage,
    )>,
}

impl ReplyWake {
    fn record(&self) {
        let _ = self.observed.try_send((
            self.logical.state(),
            self.native.state(),
            self.budget.usage(),
        ));
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

async fn reply_wake_observes_joint_decisions_before_refund<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    for aborted in [false, true] {
        let (mut native, ready) = NativeFixture::ready(false).await?;
        let budget = AtomicMessagingWorkBudget::new();
        let (logical, ticket) = permit();
        let submission = paired(
            ready,
            &node.binding,
            vec![posted("publication")],
            ticket,
            &budget,
        )?;
        let reserved = charge(&budget);
        let (resume, parked) = node.park_owner().await?;
        if aborted {
            assert!(logical.abort());
        }
        let (observed, observations) = flume::bounded(1);
        let probe = Arc::new(ReplyWake {
            logical: logical.clone(),
            native: native.observer.clone(),
            budget: budget.clone(),
            observed,
        });
        let waker = Waker::from(probe);
        let response = enqueue(&node.broker.handle(), submission)?;
        let mut receive = Box::pin(response.recv_async());
        assert!(
            receive
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        resume.release();
        timeout(DEADLINE, parked.recv_async()).await???;
        let (logical_state, native_state, charged) =
            timeout(DEADLINE, observations.recv_async()).await??;
        assert_eq!(
            logical_state,
            if aborted {
                AtomicCommitState::Aborted
            } else {
                AtomicCommitState::Committed
            }
        );
        assert_eq!(
            native_state,
            if aborted {
                NativeTransactionState::Aborted
            } else {
                NativeTransactionState::Committed
            }
        );
        assert_eq!(
            charged, reserved,
            "reply publication retains logical reservation and native resource bundle"
        );
        let completion = timeout(DEADLINE, receive).await??;
        let (application, resources) = completion.into_parts();
        if aborted {
            assert!(matches!(
                application,
                Err(NativeAtomicSubmitError::Guarded(
                    GuardedAtomicSubmitError::Permit(AtomicCommitClaimError::Aborted)
                ))
            ));
            drop(resources);
        } else {
            application?;
            native
                .finish(resources, NativeTransactionState::Committed)
                .await?;
        }
        node.fence().await?;
        refunded(&budget);
        native.shutdown().await?;
    }
    Ok(())
}

macro_rules! backend_cases {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;
            #[tokio::test]
            async fn failed_claims_have_no_owner_io() -> TestResult {
                super::failed_claims_have_no_owner_io($provider).await
            }
            #[tokio::test]
            async fn empty_and_rejected_groups_publish_exact_decisions() -> TestResult {
                super::empty_and_rejected_groups_publish_exact_decisions($provider).await
            }
            #[tokio::test]
            async fn storage_failure_and_unwind_are_jointly_indeterminate() -> TestResult {
                super::storage_failure_and_unwind_are_jointly_indeterminate($provider).await
            }
            #[tokio::test]
            async fn cancellation_and_owner_stop_release_the_whole_pair() -> TestResult {
                super::cancellation_and_owner_stop_release_the_whole_pair($provider).await
            }
            #[tokio::test]
            async fn closed_admission_returns_unique_resources() -> TestResult {
                super::closed_admission_returns_unique_resources($provider).await
            }
            #[tokio::test]
            async fn started_close_and_caller_drop_cannot_revoke_commit() -> TestResult {
                super::started_close_and_caller_drop_cannot_revoke_commit($provider).await
            }
            #[tokio::test]
            async fn dropped_reply_still_commits_and_wakes_once() -> TestResult {
                super::dropped_reply_still_commits_and_wakes_once($provider).await
            }
            #[tokio::test]
            async fn reply_wake_observes_joint_decisions_before_refund() -> TestResult {
                super::reply_wake_observes_joint_decisions_before_refund($provider).await
            }
        }
    };
}

backend_cases!(memory, testkit::MemoryProvider::new());
backend_cases!(durable, testkit::DurableProvider::temporary()?);
