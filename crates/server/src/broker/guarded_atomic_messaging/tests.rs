use std::{
    error::Error,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};

use domain::{
    AtomicMessagingApplication, BrokerError, Command, CommandKind, CommandOutcome, EntityBinding,
    EntityIncarnationKind, EntityPath, LockToken, NamespaceName, QueueConfig, ReceiveMode,
    SequenceNumber, StateMachine, Timestamp,
};
use protocol_amqp::{
    AtomicCommitClaimError, AtomicCommitDecision, AtomicCommitPermit, AtomicCommitState,
    AtomicCommitTicket, Broker as _,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value as StoredValue, WriteBatch};
use testkit::StoreProvider;
use tokio::time::timeout;

use crate::broker::{COMMAND_QUEUE_DEPTH, Request};
use crate::{Broker, Clock, GuardedAtomicSubmitError, LocalProposer, ProposeError};

#[path = "tests/fixture.rs"]
mod fixture;
use fixture::*;

#[path = "tests/atomic_work.rs"]
mod atomic_work;

#[path = "tests/empty_work.rs"]
mod empty_work;

#[path = "tests/native_ready_fixture.rs"]
mod native_ready_fixture;

#[path = "tests/native_work.rs"]
mod native_work;

#[path = "tests/native_protocol.rs"]
mod native_protocol;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);
const NO_WAKE: Duration = Duration::from_millis(30);

#[test]
fn unexpected_owner_outcome_is_indeterminate() {
    assert_eq!(
        super::commit_decision(&Err(ProposeError::UnexpectedOutcome {
            outcome: "unexpected owner result".to_owned()
        })),
        AtomicCommitDecision::Indeterminate
    );
}

async fn invalid_local_kind_aborts_before_request_admission<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let handle = node.broker.handle();
    let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let (observer, ticket) = permit();
    let result = timeout(
        DEADLINE,
        handle.submit_atomic_messaging_guarded(
            node.binding.clone(),
            vec![CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            }],
            ticket,
        ),
    )
    .await?;
    assert_eq!(
        result,
        Err(GuardedAtomicSubmitError::Propose(ProposeError::Broker(
            BrokerError::AtomicMessagingOperationNotSupported
        )))
    );
    assert_eq!(
        observer.state(),
        AtomicCommitState::Aborted,
        "local refusal never starts owner authority"
    );
    assert!(handle.requests.is_empty());
    node.assert_no_work(&before)?;
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    Ok(())
}

async fn queued_abort_and_expiry_precede_every_owner_read<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let handle = node.broker.handle();
    let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    for expired in [false, true] {
        // The owner is parked in an unrelated pure read, not in a mutation.
        // Its pause guard releases it before Node drops on any assertion error.
        let (resume, parked) = node.park_owner().await?;
        let before = node.snapshot()?;
        node.controls.reset_counts();
        let (observer, ticket) = if expired {
            AtomicCommitPermit::new(Instant::now() - Duration::from_secs(1))
        } else {
            permit()
        };
        let response = node.enqueue(ticket, vec![send("never-started")])?;
        if !expired {
            assert!(observer.abort());
            assert!(!observer.abort());
        }
        resume.release();
        assert_eq!(
            timeout(DEADLINE, parked.recv_async()).await???,
            Timestamp::from_millis(1_000)
        );
        assert_eq!(
            timeout(DEADLINE, response.recv_async()).await??,
            Err(GuardedAtomicSubmitError::Permit(
                AtomicCommitClaimError::Aborted
            ))
        );
        assert_eq!(observer.state(), AtomicCommitState::Aborted);
        node.assert_no_work(&before)?;
        assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    }
    let (observer, ticket) = permit();
    let applied = timeout(
        DEADLINE,
        handle.submit_atomic_messaging_guarded(
            node.binding.clone(),
            vec![send("healthy-retry")],
            ticket,
        ),
    )
    .await??;
    assert_eq!(observer.state(), AtomicCommitState::Committed);
    assert_eq!(
        applied.outcomes,
        vec![CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }]
    );
    timeout(DEADLINE, &mut ready).await?;
    Ok(())
}

async fn dropping_an_unpolled_factory_aborts_without_enqueue<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let handle = node.broker.handle();
    let (observer, ticket) = permit();
    let future = handle.submit_atomic_messaging_guarded(
        node.binding.clone(),
        vec![send("unpolled")],
        ticket,
    );
    assert_eq!(observer.state(), AtomicCommitState::Pending);
    drop(future);
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    assert!(handle.requests.is_empty());
    node.assert_no_work(&before)?;
    Ok(())
}

async fn queue_blocked_send_cancellation_does_not_leave_a_request<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let (resume, parked) = node.park_owner().await?;
    let before = node.snapshot()?;
    node.controls.reset_counts();
    let handle = node.broker.handle();
    let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    for _ in 0..COMMAND_QUEUE_DEPTH {
        let (reply, response) = flume::bounded(1);
        handle.requests.send(Request::LastApplied { reply })?;
        drop(response);
    }
    assert_eq!(handle.requests.len(), COMMAND_QUEUE_DEPTH);
    let (observer, ticket) = permit();
    let mut future = Box::pin(handle.submit_atomic_messaging_guarded(
        node.binding.clone(),
        vec![send("queue-blocked")],
        ticket,
    ));
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(observer.state(), AtomicCommitState::Pending);
    assert_eq!(handle.requests.len(), COMMAND_QUEUE_DEPTH);
    drop(future);
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    resume.release();
    timeout(DEADLINE, parked.recv_async()).await???;
    node.fence().await?;
    assert_eq!(
        node.controls.reads.load(Ordering::SeqCst),
        COMMAND_QUEUE_DEPTH + 1
    );
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    Ok(())
}

async fn started_caller_cancellation_cannot_revoke_commit<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let (entered, resume) = node.controls.pause_commit();
    let handle = node.broker.handle();
    let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let (observer, ticket) = permit();
    let mut future = Box::pin(handle.submit_atomic_messaging_guarded(
        node.binding.clone(),
        vec![send("claimed")],
        ticket,
    ));
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    timeout(DEADLINE, entered.recv_async()).await??;
    assert_eq!(observer.state(), AtomicCommitState::Started);
    assert!(!observer.abort());
    assert_eq!(node.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    drop(future);
    assert_eq!(observer.state(), AtomicCommitState::Started);
    resume.release();
    timeout(DEADLINE, &mut ready).await?;
    assert_eq!(
        observer.state(),
        AtomicCommitState::Committed,
        "decision must precede the committed wake"
    );
    node.fence().await?;
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        node.messages()?
            .iter()
            .map(|message| message.message_id.as_str())
            .collect::<Vec<_>>(),
        vec!["claimed"]
    );
    assert!(!observer.abort());
    Ok(())
}

async fn dropped_reply_does_not_drop_the_owner_decision<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let (entered, resume) = node.controls.pause_commit();
    let handle = node.broker.handle();
    let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let (observer, ticket) = permit();
    let response = node.enqueue(ticket, vec![send("reply-gone")])?;
    timeout(DEADLINE, entered.recv_async()).await??;
    assert_eq!(observer.state(), AtomicCommitState::Started);
    drop(response);
    assert!(!observer.abort());
    assert_eq!(observer.state(), AtomicCommitState::Started);
    resume.release();
    timeout(DEADLINE, &mut ready).await?;
    assert_eq!(observer.state(), AtomicCommitState::Committed);
    node.fence().await?;
    assert_eq!(node.messages()?.len(), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    Ok(())
}

async fn logical_and_clock_refusals_are_rejected_not_indeterminate<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let handle = node.broker.handle();
    let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let (observer, ticket) = permit();
    let response = node.enqueue(
        ticket,
        vec![
            send("early"),
            CommandKind::Complete {
                sequence: SequenceNumber::new(99),
                lock_token: LockToken::new(1),
            },
        ],
    )?;
    assert!(matches!(
        timeout(DEADLINE, response.recv_async()).await??,
        Err(GuardedAtomicSubmitError::Propose(ProposeError::Broker(
            BrokerError::MessageNotFound { .. }
        )))
    ));
    assert_eq!(observer.state(), AtomicCommitState::Rejected);
    assert!(!observer.abort());
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    node.controls.clock_millis.store(0, Ordering::SeqCst);
    let (observer, ticket) = permit();
    let response = node.enqueue(ticket, vec![send("clock-refused")])?;
    assert!(matches!(
        timeout(DEADLINE, response.recv_async()).await??,
        Err(GuardedAtomicSubmitError::Propose(
            ProposeError::ClockWentBackward { .. }
        ))
    ));
    assert_eq!(observer.state(), AtomicCommitState::Rejected);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    Ok(())
}

async fn every_claimed_storage_error_is_indeterminate<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    let initial = node.snapshot()?;
    {
        let handle = node.broker.handle();
        let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
        tokio::pin!(ready);
        for phase in 0..3 {
            node.controls.reset_counts();
            match phase {
                0 => node.controls.fail_read.store(true, Ordering::SeqCst),
                1 => node
                    .controls
                    .fail_before_commit
                    .store(true, Ordering::SeqCst),
                2 => node
                    .controls
                    .fail_after_commit
                    .store(true, Ordering::SeqCst),
                _ => unreachable!(),
            }
            let (observer, ticket) = permit();
            let response = node.enqueue(ticket, vec![send("uncertain")])?;
            assert!(matches!(
                timeout(DEADLINE, response.recv_async()).await??,
                Err(GuardedAtomicSubmitError::Propose(ProposeError::Broker(
                    BrokerError::Storage(_)
                )))
            ));
            assert_eq!(observer.state(), AtomicCommitState::Indeterminate);
            assert!(!observer.abort());
            assert!(timeout(NO_WAKE, &mut ready).await.is_err());
            if phase < 2 {
                assert_eq!(node.snapshot()?, initial);
            } else {
                assert_ne!(node.snapshot()?, initial);
                assert_eq!(node.messages()?.len(), 1);
            }
            assert_eq!(
                node.controls.writes.load(Ordering::SeqCst),
                usize::from(phase != 0)
            );
            assert_eq!(
                node.controls.clock_calls.load(Ordering::SeqCst),
                usize::from(phase != 0)
            );
        }
    }
    let committed = node.snapshot()?;
    let (_provider, reopened) = node.reopen()?;
    assert_eq!(
        reopened.snapshot()?,
        committed,
        "unknown is not a guarantee of physical rollback"
    );
    Ok(())
}

async fn empty_success_and_blocking_success_finalize_committed<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let handle = node.broker.handle();
    let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let (observer, ticket) = permit();
    let applied = timeout(
        DEADLINE,
        handle.submit_atomic_messaging_guarded(node.binding.clone(), Vec::new(), ticket),
    )
    .await??;
    assert!(applied.outcomes.is_empty());
    assert!(applied.enqueue_targets.is_empty());
    assert_eq!(observer.state(), AtomicCommitState::Committed);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 0);
    assert_eq!(node.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    let (observer, ticket) = permit();
    let binding = node.binding.clone();
    let submit = handle.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        submit.submit_atomic_messaging_guarded_blocking(binding, vec![send("blocking")], ticket)
    });
    let applied = timeout(DEADLINE, blocking).await???;
    assert_eq!(observer.state(), AtomicCommitState::Committed);
    assert_eq!(
        applied.outcomes,
        vec![CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }]
    );
    timeout(DEADLINE, &mut ready).await?;
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    Ok(())
}

async fn owner_shutdown_releases_queued_tickets_without_caller_drop<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let (resume, parked) = node.park_owner().await?;
    let before = node.snapshot()?;
    node.controls.reset_counts();
    let retained = node.broker.handle();
    retained.requests.send(Request::Stop)?;
    let (observer, ticket) = permit();
    let mut future = Box::pin(retained.submit_atomic_messaging_guarded(
        node.binding.clone(),
        vec![send("behind-stop")],
        ticket,
    ));
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(retained.requests.len(), 2);
    resume.release();
    timeout(DEADLINE, parked.recv_async()).await???;
    assert_eq!(
        timeout(DEADLINE, &mut future).await?,
        Err(GuardedAtomicSubmitError::BrokerStopped)
    );
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    assert!(
        retained.requests.is_empty(),
        "external Sender must not retain stopped jobs"
    );
    node.assert_no_work(&before)?;
    let (observer, ticket) = permit();
    assert_eq!(
        timeout(
            DEADLINE,
            retained.submit_atomic_messaging_guarded(
                node.binding.clone(),
                vec![send("already-stopped")],
                ticket
            )
        )
        .await?,
        Err(GuardedAtomicSubmitError::BrokerStopped)
    );
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    Ok(())
}

async fn owner_unwind_releases_queued_tickets_without_caller_drop<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let (resume, parked) = node.park_owner().await?;
    let before = node.snapshot()?;
    node.controls.reset_counts();
    let retained = node.broker.handle();
    let (observer, ticket) = permit();
    let mut future = Box::pin(retained.submit_atomic_messaging_guarded(
        node.binding.clone(),
        vec![send("behind-unwind")],
        ticket,
    ));
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    node.controls.panic_read.store(true, Ordering::SeqCst);
    resume.release();
    assert!(timeout(DEADLINE, parked.recv_async()).await?.is_err());
    assert_eq!(
        timeout(DEADLINE, &mut future).await?,
        Err(GuardedAtomicSubmitError::BrokerStopped)
    );
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    assert!(retained.requests.is_empty());
    node.assert_no_work(&before)?;
    Ok(())
}

async fn owner_unwind_after_claim_is_indeterminate<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let retained = node.broker.handle();
    let ready = retained.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let (observer, ticket) = permit();
    node.controls.panic_read.store(true, Ordering::SeqCst);
    assert_eq!(
        timeout(
            DEADLINE,
            retained.submit_atomic_messaging_guarded(
                node.binding.clone(),
                vec![send("claimed-unwind")],
                ticket
            )
        )
        .await?,
        Err(GuardedAtomicSubmitError::BrokerStopped)
    );
    assert_eq!(observer.state(), AtomicCommitState::Indeterminate);
    assert!(!observer.abort());
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    Ok(())
}

macro_rules! suite {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;

            #[tokio::test]
            async fn invalid_local_kind_aborts_before_request_admission() -> TestResult {
                super::invalid_local_kind_aborts_before_request_admission($provider).await
            }
            #[tokio::test]
            async fn queued_abort_and_expiry_precede_every_owner_read() -> TestResult {
                super::queued_abort_and_expiry_precede_every_owner_read($provider).await
            }
            #[tokio::test]
            async fn dropping_an_unpolled_factory_aborts_without_enqueue() -> TestResult {
                super::dropping_an_unpolled_factory_aborts_without_enqueue($provider).await
            }
            #[tokio::test]
            async fn queue_blocked_send_cancellation_does_not_leave_a_request() -> TestResult {
                super::queue_blocked_send_cancellation_does_not_leave_a_request($provider).await
            }
            #[tokio::test]
            async fn started_caller_cancellation_cannot_revoke_commit() -> TestResult {
                super::started_caller_cancellation_cannot_revoke_commit($provider).await
            }
            #[tokio::test]
            async fn dropped_reply_does_not_drop_the_owner_decision() -> TestResult {
                super::dropped_reply_does_not_drop_the_owner_decision($provider).await
            }
            #[tokio::test]
            async fn logical_and_clock_refusals_are_rejected_not_indeterminate() -> TestResult {
                super::logical_and_clock_refusals_are_rejected_not_indeterminate($provider).await
            }
            #[tokio::test]
            async fn every_claimed_storage_error_is_indeterminate() -> TestResult {
                super::every_claimed_storage_error_is_indeterminate($provider).await
            }
            #[tokio::test]
            async fn empty_success_and_blocking_success_finalize_committed() -> TestResult {
                super::empty_success_and_blocking_success_finalize_committed($provider).await
            }
            #[tokio::test]
            async fn owner_shutdown_releases_queued_tickets_without_caller_drop() -> TestResult {
                super::owner_shutdown_releases_queued_tickets_without_caller_drop($provider).await
            }
            #[tokio::test]
            async fn owner_unwind_releases_queued_tickets_without_caller_drop() -> TestResult {
                super::owner_unwind_releases_queued_tickets_without_caller_drop($provider).await
            }
            #[tokio::test]
            async fn owner_unwind_after_claim_is_indeterminate() -> TestResult {
                super::owner_unwind_after_claim_is_indeterminate($provider).await
            }
        }
    };
}

suite!(memory, testkit::MemoryProvider::new());
suite!(durable, testkit::DurableProvider::temporary()?);
