use super::native_ready_fixture::NativeFixture;
use super::*;

use amqp::{NativeFault, NativeTransactionError, NativeTransactionState};
use protocol_amqp::{
    AtomicMessagingWorkBudget, AtomicMessagingWorkUsage, AtomicTransactionDischarge,
    AtomicTransactionRegistry, AtomicTransactionSubmission, NativeAtomicBroker,
    NativeAtomicBrokerCompletion, NativeAtomicIndeterminateCause, NativeAtomicOwnerError,
    NativeAtomicResponseUnavailable, OwnedNativeAtomicMessagingSubmission,
};

use crate::broker::native_atomic_protocol::owner_error;

fn submit<B: NativeAtomicBroker>(
    broker: &B,
    submission: OwnedNativeAtomicMessagingSubmission,
) -> impl Future<Output = Result<NativeAtomicBrokerCompletion, NativeAtomicResponseUnavailable>>
+ Send
+ 'static {
    NativeAtomicBroker::submit_native_atomic_messaging_owned(broker, submission)
}

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
    let mut work = budget.stage(binding.clone())?;
    for kind in kinds {
        work.try_push(kind)?;
    }
    Ok(OwnedNativeAtomicMessagingSubmission::new(
        native,
        AtomicTransactionSubmission::Bound(work.into_submission(ticket)),
    ))
}

fn charged(budget: &AtomicMessagingWorkBudget) -> AtomicMessagingWorkUsage {
    let usage = budget.usage();
    assert_eq!(usage.groups(), 1);
    assert!(usage.content_bytes() > 0);
    assert_eq!(usage.value_items(), 1);
    usage
}

fn refunded(budget: &AtomicMessagingWorkBudget) {
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn portable_mapper_preserves_logical_errors_and_redacts_owner_details() {
    let logical = [
        AtomicCommitClaimError::Aborted,
        AtomicCommitClaimError::Unavailable,
    ];
    for error in logical {
        assert_eq!(
            owner_error(GuardedAtomicSubmitError::Permit(error).into()),
            NativeAtomicOwnerError::LogicalClaim(error),
        );
    }
    let native = NativeTransactionError::Faulted(NativeFault::Closed);
    assert_eq!(
        owner_error(native.into()),
        NativeAtomicOwnerError::NativeClaim(native)
    );
    assert_eq!(
        owner_error(GuardedAtomicSubmitError::BrokerStopped.into()),
        NativeAtomicOwnerError::OwnerStopped,
    );
    assert_eq!(
        owner_error(
            GuardedAtomicSubmitError::Propose(ProposeError::Broker(BrokerError::QueueNotFound))
                .into()
        ),
        NativeAtomicOwnerError::Refused(BrokerError::QueueNotFound),
    );
    assert_eq!(
        owner_error(
            GuardedAtomicSubmitError::Propose(ProposeError::ClockWentBackward {
                last_applied: Timestamp::from_millis(8_000),
                now: Timestamp::from_millis(1),
                allowed_millis: 500,
            })
            .into()
        ),
        NativeAtomicOwnerError::ClockRegression,
    );
    let sensitive = "private backend path and outcome content";
    let cases = [
        (
            GuardedAtomicSubmitError::Propose(ProposeError::Broker(BrokerError::Storage(
                StorageError::Backend {
                    operation: "private storage operation",
                    detail: sensitive.to_owned(),
                },
            ))),
            NativeAtomicIndeterminateCause::Storage,
        ),
        (
            GuardedAtomicSubmitError::Propose(ProposeError::UnexpectedOutcome {
                outcome: sensitive.to_owned(),
            }),
            NativeAtomicIndeterminateCause::UnexpectedOutcome,
        ),
        (
            GuardedAtomicSubmitError::WorkUnavailable,
            NativeAtomicIndeterminateCause::WorkUnavailable,
        ),
    ];
    for (error, cause) in cases {
        let portable = owner_error(error.into());
        assert_eq!(portable, NativeAtomicOwnerError::Indeterminate(cause));
        for rendered in [portable.to_string(), format!("{portable:?}")] {
            assert!(!rendered.contains(sensitive));
            assert!(!rendered.contains("private storage operation"));
        }
    }
    assert_eq!(
        format!("{NativeAtomicResponseUnavailable:?}"),
        "NativeAtomicResponseUnavailable"
    );
}

async fn generic_success_and_empty_completion<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    let (mut native, ready) = NativeFixture::ready(false).await?;
    let budget = AtomicMessagingWorkBudget::new();
    let (logical, ticket) = permit();
    let submission = paired(
        ready,
        &node.binding,
        vec![posted("portable-success")],
        ticket,
        &budget,
    )?;
    charged(&budget);
    let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
    let completion = timeout(DEADLINE, submit(&handle, submission)).await??;
    assert_eq!(logical.state(), AtomicCommitState::Committed);
    assert_eq!(native.observer.state(), NativeTransactionState::Committed);
    assert_eq!(
        format!("{completion:?}"),
        "NativeAtomicBrokerCompletion { .. }"
    );
    let application = completion.application().as_ref().map_err(Clone::clone)?;
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
    timeout(DEADLINE, wake).await?;
    let (application, resources) = completion.into_parts();
    application?;
    node.fence().await?;
    refunded(&budget);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 1);
    native
        .finish(resources, NativeTransactionState::Committed)
        .await?;
    native.shutdown().await?;

    let before = node.snapshot()?;
    let (mut native, ready) = NativeFixture::ready(true).await?;
    let (mut registry, work) = AtomicTransactionRegistry::new();
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    let AtomicTransactionDischarge::Submit(submission) =
        registry.discharge(&controller, &id, false)?
    else {
        return Err("empty logical submission missing".into());
    };
    let logical = submission.permit().clone();
    let submission = OwnedNativeAtomicMessagingSubmission::new(ready, submission);
    node.controls.reset_counts();
    let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
    let completion = timeout(DEADLINE, submit(&handle, submission)).await??;
    assert_eq!(logical.state(), AtomicCommitState::Committed);
    assert_eq!(native.observer.state(), NativeTransactionState::Committed);
    let (application, resources) = completion.into_parts();
    let application = application?;
    assert!(application.outcomes.is_empty());
    assert!(application.enqueue_targets.is_empty());
    node.assert_no_work(&before)?;
    assert!(timeout(NO_WAKE, wake).await.is_err());
    node.fence().await?;
    assert_eq!(work.work_usage(), AtomicMessagingWorkUsage::default());
    native
        .finish(resources, NativeTransactionState::Committed)
        .await?;
    native.shutdown().await?;
    Ok(())
}

async fn generic_known_refusals_keep_unique_resources<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    for clock in [false, true] {
        let (mut native, ready) = NativeFixture::ready(false).await?;
        let budget = AtomicMessagingWorkBudget::new();
        let (logical, ticket) = permit();
        let kinds = if clock {
            node.controls.clock_millis.store(0, Ordering::SeqCst);
            vec![posted("regressed-clock")]
        } else {
            vec![
                posted("refused"),
                CommandKind::Complete {
                    sequence: SequenceNumber::new(99),
                    lock_token: LockToken::new(1),
                },
            ]
        };
        let submission = paired(ready, &node.binding, kinds, ticket, &budget)?;
        charged(&budget);
        let before = node.snapshot()?;
        node.controls.reset_counts();
        let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
        let completion = timeout(DEADLINE, submit(&handle, submission)).await??;
        if clock {
            assert!(matches!(
                completion.application(),
                Err(NativeAtomicOwnerError::ClockRegression)
            ));
        } else {
            assert!(matches!(
                completion.application(),
                Err(NativeAtomicOwnerError::Refused(
                    BrokerError::MessageNotFound { .. }
                ))
            ));
        }
        assert_eq!(logical.state(), AtomicCommitState::Rejected);
        assert_eq!(native.observer.state(), NativeTransactionState::Rejected);
        assert_eq!(node.snapshot()?, before);
        assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
        assert!(timeout(NO_WAKE, wake).await.is_err());
        let (_, resources) = completion.into_parts();
        node.fence().await?;
        refunded(&budget);
        native
            .finish(resources, NativeTransactionState::Rejected)
            .await?;
        assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
        native.shutdown().await?;
    }
    Ok(())
}

async fn generic_claim_failures_precede_owner_io<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
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
            vec![posted("no-claim")],
            ticket,
            &budget,
        )?;
        if failure == 0 {
            native.close_data().await?;
        }
        if failure == 1 {
            assert!(logical.abort());
        }
        let before = node.snapshot()?;
        node.controls.reset_counts();
        let completion = timeout(DEADLINE, submit(&handle, submission)).await??;
        if failure == 0 {
            assert!(matches!(
                completion.application(),
                Err(NativeAtomicOwnerError::NativeClaim(
                    NativeTransactionError::Faulted(NativeFault::Closed)
                ))
            ));
            assert_eq!(native.observer.state(), NativeTransactionState::Faulted);
        } else {
            assert!(matches!(
                completion.application(),
                Err(NativeAtomicOwnerError::LogicalClaim(
                    AtomicCommitClaimError::Aborted
                ))
            ));
            assert_eq!(native.observer.state(), NativeTransactionState::Aborted);
        }
        assert_eq!(logical.state(), AtomicCommitState::Aborted);
        node.assert_no_work(&before)?;
        let (_, resources) = completion.into_parts();
        node.fence().await?;
        refunded(&budget);
        if failure == 0 {
            assert!(
                timeout(DEADLINE, resources.finish()).await?.is_err(),
                "faulted resources promise no negative wire completion"
            );
            native.barrier().await?;
        } else {
            native
                .finish(resources, NativeTransactionState::Aborted)
                .await?;
        }
        native.shutdown().await?;
    }
    Ok(())
}

async fn generic_unpolled_and_queue_cancellation_remain_armed<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    for path in 0..3 {
        let (mut native, ready) = NativeFixture::ready(false).await?;
        let budget = AtomicMessagingWorkBudget::new();
        let (logical, ticket) = permit();
        let submission = paired(
            ready,
            &node.binding,
            vec![posted("portable-cancel")],
            ticket,
            &budget,
        )?;
        let reserved = charged(&budget);
        let paused = if path == 0 {
            None
        } else {
            Some(node.park_owner().await?)
        };
        if path == 2 {
            for _ in 0..COMMAND_QUEUE_DEPTH {
                let (reply, response) = flume::bounded(1);
                handle.requests.send(Request::LastApplied { reply })?;
                drop(response);
            }
        }
        let before = node.snapshot()?;
        node.controls.reset_counts();
        let mut future = Box::pin(submit(&handle, submission));
        if path != 0 {
            std::future::poll_fn(|cx| {
                assert!(future.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert_eq!(logical.state(), AtomicCommitState::Pending);
        drop(future);
        assert_eq!(logical.state(), AtomicCommitState::Aborted);
        node.assert_no_work(&before)?;
        if path == 1 {
            assert_eq!(budget.usage(), reserved);
            assert_eq!(native.observer.state(), NativeTransactionState::Ready);
        } else {
            refunded(&budget);
            assert_eq!(native.observer.state(), NativeTransactionState::Faulted);
        }
        if let Some((resume, parked)) = paused {
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
        native.barrier().await?;
        native.shutdown().await?;
    }
    Ok(())
}

async fn generic_closed_admission_returns_inner_owner_stopped<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    let binding = node.binding.clone();
    let before = node.snapshot()?;
    // The provider remains alive after stopping only the owner.
    drop(node.broker);
    node.controls.reset_counts();
    let (mut native, ready) = NativeFixture::ready(false).await?;
    let budget = AtomicMessagingWorkBudget::new();
    let (logical, ticket) = permit();
    let submission = paired(
        ready,
        &binding,
        vec![posted("closed-admission")],
        ticket,
        &budget,
    )?;
    charged(&budget);
    let wake = handle.deliverable(binding.namespace(), binding.target());
    let completion = timeout(DEADLINE, submit(&handle, submission)).await??;
    assert!(
        matches!(
            completion.application(),
            Err(NativeAtomicOwnerError::OwnerStopped)
        ),
        "recovered native resources keep this failure inside the completion"
    );
    assert_eq!(logical.state(), AtomicCommitState::Aborted);
    assert_eq!(native.observer.state(), NativeTransactionState::Faulted);
    refunded(&budget);
    let (application, resources) = completion.into_parts();
    assert!(matches!(
        application,
        Err(NativeAtomicOwnerError::OwnerStopped)
    ));
    assert!(timeout(DEADLINE, resources.finish()).await?.is_err());
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.store.snapshot()?, before);
    assert!(handle.requests.is_empty());
    assert!(timeout(NO_WAKE, wake).await.is_err());
    native.barrier().await?;
    native.shutdown().await?;
    Ok(())
}

async fn generic_storage_errors_are_static_and_indeterminate<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    for phase in 0..3 {
        let (mut native, ready) = NativeFixture::ready(false).await?;
        let budget = AtomicMessagingWorkBudget::new();
        let (logical, ticket) = permit();
        let submission = paired(
            ready,
            &node.binding,
            vec![posted("portable-uncertain")],
            ticket,
            &budget,
        )?;
        let before = node.snapshot()?;
        node.controls.reset_counts();
        match phase {
            0 => node.controls.fail_read.store(true, Ordering::SeqCst),
            1 => node
                .controls
                .fail_before_commit
                .store(true, Ordering::SeqCst),
            _ => node
                .controls
                .fail_after_commit
                .store(true, Ordering::SeqCst),
        }
        let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
        let completion = timeout(DEADLINE, submit(&handle, submission)).await??;
        assert!(matches!(
            completion.application(),
            Err(NativeAtomicOwnerError::Indeterminate(
                NativeAtomicIndeterminateCause::Storage
            ))
        ));
        assert!(!format!("{:?}", completion.application()).contains("controlled test failure"));
        assert_eq!(logical.state(), AtomicCommitState::Indeterminate);
        assert_eq!(
            native.observer.state(),
            NativeTransactionState::Indeterminate
        );
        assert!(!logical.abort());
        let (_, resources) = completion.into_parts();
        node.fence().await?;
        refunded(&budget);
        if phase < 2 {
            assert_eq!(node.snapshot()?, before);
        } else {
            assert_ne!(
                node.snapshot()?,
                before,
                "reported failure can follow a complete physical commit"
            );
            let messages = node.messages()?;
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].message_id, "portable-uncertain");
            assert_eq!(messages[0].sequence, SequenceNumber::new(1));
        }
        assert!(timeout(NO_WAKE, wake).await.is_err());
        let writes = node.controls.writes.load(Ordering::SeqCst);
        native
            .finish(resources, NativeTransactionState::Indeterminate)
            .await?;
        assert_eq!(node.controls.writes.load(Ordering::SeqCst), writes);
        native.shutdown().await?;
    }
    drop(handle);
    let committed = node.snapshot()?;
    let (_provider, reopened) = node.reopen()?;
    assert_eq!(
        reopened.snapshot()?,
        committed,
        "no retry or physical rollback is implied by an indeterminate response"
    );
    Ok(())
}

async fn generic_missing_reply_distinguishes_pending_from_started<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    let (resume, parked) = node.park_owner().await?;
    let (mut native, ready) = NativeFixture::ready(false).await?;
    let budget = AtomicMessagingWorkBudget::new();
    let (logical, ticket) = permit();
    let submission = paired(
        ready,
        &node.binding,
        vec![posted("stopped-queued")],
        ticket,
        &budget,
    )?;
    let before = node.snapshot()?;
    node.controls.reset_counts();
    handle.requests.send(Request::Stop)?;
    let mut future = Box::pin(submit(&handle, submission));
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(handle.requests.len(), 2);
    resume.release();
    timeout(DEADLINE, parked.recv_async()).await???;
    assert!(matches!(
        timeout(DEADLINE, future).await?,
        Err(NativeAtomicResponseUnavailable)
    ));
    assert_eq!(logical.state(), AtomicCommitState::Aborted);
    assert_eq!(native.observer.state(), NativeTransactionState::Faulted);
    refunded(&budget);
    assert!(handle.requests.is_empty());
    node.assert_no_work(&before)?;
    native.barrier().await?;
    native.shutdown().await?;
    Ok(())
}

async fn generic_unwind_loses_resources_without_claiming_rollback<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    let (mut native, ready) = NativeFixture::ready(false).await?;
    let budget = AtomicMessagingWorkBudget::new();
    let (logical, ticket) = permit();
    let submission = paired(
        ready,
        &node.binding,
        vec![posted("owner-unwind")],
        ticket,
        &budget,
    )?;
    let before = node.snapshot()?;
    node.controls.reset_counts();
    node.controls.panic_read.store(true, Ordering::SeqCst);
    let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
    assert!(matches!(
        timeout(DEADLINE, submit(&handle, submission)).await?,
        Err(NativeAtomicResponseUnavailable)
    ));
    assert_eq!(logical.state(), AtomicCommitState::Indeterminate);
    assert_eq!(
        native.observer.state(),
        NativeTransactionState::Indeterminate
    );
    assert!(!logical.abort());
    refunded(&budget);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert!(timeout(NO_WAKE, wake).await.is_err());
    native.barrier().await?;
    native.shutdown().await?;
    Ok(())
}

async fn generic_started_reply_drop_does_not_revoke_commit<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    let (mut native, ready) = NativeFixture::ready(false).await?;
    let budget = AtomicMessagingWorkBudget::new();
    let (logical, ticket) = permit();
    let submission = paired(
        ready,
        &node.binding,
        vec![posted("started-portable")],
        ticket,
        &budget,
    )?;
    let reserved = charged(&budget);
    let before = node.snapshot()?;
    let (entered, resume) = node.controls.pause_commit();
    let wake = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(wake);
    let mut future = Box::pin(submit(&handle, submission));
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
    drop(future);
    assert_eq!(logical.state(), AtomicCommitState::Started);
    assert_eq!(
        native.observer.state(),
        NativeTransactionState::OwnerStarted
    );
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
    let later = handle.deliverable(node.binding.namespace(), node.binding.target());
    assert!(
        timeout(NO_WAKE, later).await.is_err(),
        "no second publication follows the lost response"
    );
    native.barrier().await?;
    native.shutdown().await?;
    Ok(())
}

macro_rules! backend_cases {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;
            #[tokio::test]
            async fn generic_success_and_empty_completion() -> TestResult {
                super::generic_success_and_empty_completion($provider).await
            }
            #[tokio::test]
            async fn generic_known_refusals_keep_unique_resources() -> TestResult {
                super::generic_known_refusals_keep_unique_resources($provider).await
            }
            #[tokio::test]
            async fn generic_claim_failures_precede_owner_io() -> TestResult {
                super::generic_claim_failures_precede_owner_io($provider).await
            }
            #[tokio::test]
            async fn generic_unpolled_and_queue_cancellation_remain_armed() -> TestResult {
                super::generic_unpolled_and_queue_cancellation_remain_armed($provider).await
            }
            #[tokio::test]
            async fn generic_closed_admission_returns_inner_owner_stopped() -> TestResult {
                super::generic_closed_admission_returns_inner_owner_stopped($provider).await
            }
            #[tokio::test]
            async fn generic_storage_errors_are_static_and_indeterminate() -> TestResult {
                super::generic_storage_errors_are_static_and_indeterminate($provider).await
            }
            #[tokio::test]
            async fn generic_missing_reply_distinguishes_pending_from_started() -> TestResult {
                super::generic_missing_reply_distinguishes_pending_from_started($provider).await
            }
            #[tokio::test]
            async fn generic_unwind_loses_resources_without_claiming_rollback() -> TestResult {
                super::generic_unwind_loses_resources_without_claiming_rollback($provider).await
            }
            #[tokio::test]
            async fn generic_started_reply_drop_does_not_revoke_commit() -> TestResult {
                super::generic_started_reply_drop_does_not_revoke_commit($provider).await
            }
        }
    };
}

backend_cases!(memory, testkit::MemoryProvider::new());
backend_cases!(durable, testkit::DurableProvider::temporary()?);
