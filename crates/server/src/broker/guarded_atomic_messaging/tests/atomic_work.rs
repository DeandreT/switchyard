use super::*;
use protocol_amqp::{
    AtomicMessagingWorkBudget, AtomicMessagingWorkUsage, OwnedAtomicMessagingSubmission,
};
use std::task::{Context, Wake, Waker};

fn posted(id: &str) -> CommandKind {
    let CommandKind::Send {
        message_id,
        body,
        time_to_live_millis,
        session_id,
    } = send(id)
    else {
        unreachable!("send helper constructs legacy ingress")
    };
    CommandKind::SendEnvelope {
        message_id,
        body,
        time_to_live_millis,
        session_id,
        envelope: Box::new(domain::MessageEnvelope {
            body: domain::MessageBody::Value(domain::MessageValue::Bool(true)),
            ..domain::MessageEnvelope::default()
        }),
    }
}

fn owned(
    budget: &AtomicMessagingWorkBudget,
    binding: &EntityBinding,
    kinds: Vec<CommandKind>,
    ticket: AtomicCommitTicket,
) -> TestResult<OwnedAtomicMessagingSubmission> {
    let mut staged = budget.stage(binding.clone())?;
    for kind in kinds {
        staged.try_push(kind)?;
    }
    Ok(staged.into_submission(ticket))
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

async fn queued_cancellation_retains_work_until_owner_discard<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let (resume, parked) = node.park_owner().await?;
    let before = node.snapshot()?;
    node.controls.reset_counts();
    let handle = node.broker.handle();
    let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let budget = AtomicMessagingWorkBudget::new();
    let (observer, ticket) = permit();
    let submission = owned(&budget, &node.binding, vec![posted("queued")], ticket)?;
    let reserved = charged(&budget);
    let mut future = Box::pin(handle.submit_atomic_messaging_owned(submission));
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(handle.requests.len(), 1);
    assert_eq!(observer.state(), AtomicCommitState::Pending);
    assert!(observer.abort());
    drop(future);
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    assert_eq!(
        budget.usage(),
        reserved,
        "queued payload still owns its charge"
    );
    node.assert_no_work(&before)?;
    resume.release();
    timeout(DEADLINE, parked.recv_async()).await???;
    node.fence().await?;
    refunded(&budget);
    assert_eq!(
        node.controls.reads.load(Ordering::SeqCst),
        1,
        "only the explicit fence reads"
    );
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    Ok(())
}

async fn started_caller_drop_retains_work_through_commit<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let (entered, resume) = node.controls.pause_commit();
    let handle = node.broker.handle();
    let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let budget = AtomicMessagingWorkBudget::new();
    let (observer, ticket) = permit();
    let submission = owned(&budget, &node.binding, vec![posted("started")], ticket)?;
    let reserved = charged(&budget);
    let mut future = Box::pin(handle.submit_atomic_messaging_owned(submission));
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    timeout(DEADLINE, entered.recv_async()).await??;
    assert_eq!(observer.state(), AtomicCommitState::Started);
    assert!(!observer.abort());
    drop(future);
    assert_eq!(observer.state(), AtomicCommitState::Started);
    assert_eq!(budget.usage(), reserved);
    assert_eq!(node.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    resume.release();
    timeout(DEADLINE, &mut ready).await?;
    assert_eq!(observer.state(), AtomicCommitState::Committed);
    node.fence().await?;
    refunded(&budget);
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        node.messages()?
            .iter()
            .map(|message| message.message_id.as_str())
            .collect::<Vec<_>>(),
        vec!["started"]
    );
    Ok(())
}

async fn owner_stop_refunds_queued_work_without_caller_drop<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let (resume, parked) = node.park_owner().await?;
    let before = node.snapshot()?;
    node.controls.reset_counts();
    let retained = node.broker.handle();
    retained.requests.send(Request::Stop)?;
    let budget = AtomicMessagingWorkBudget::new();
    let (observer, ticket) = permit();
    let submission = owned(&budget, &node.binding, vec![posted("behind-stop")], ticket)?;
    let reserved = charged(&budget);
    let mut future = Box::pin(retained.submit_atomic_messaging_owned(submission));
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(retained.requests.len(), 2);
    assert_eq!(budget.usage(), reserved);
    resume.release();
    timeout(DEADLINE, parked.recv_async()).await???;
    assert_eq!(
        timeout(DEADLINE, &mut future).await?,
        Err(GuardedAtomicSubmitError::BrokerStopped)
    );
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    assert!(retained.requests.is_empty());
    refunded(&budget);
    node.assert_no_work(&before)?;
    Ok(())
}

async fn reported_storage_failure_refunds_without_promising_rollback<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let budget = AtomicMessagingWorkBudget::new();
    {
        let handle = node.broker.handle();
        let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
        tokio::pin!(ready);
        node.controls
            .fail_after_commit
            .store(true, Ordering::SeqCst);
        let (observer, ticket) = permit();
        let submission = owned(&budget, &node.binding, vec![posted("uncertain")], ticket)?;
        charged(&budget);
        assert!(matches!(
            timeout(DEADLINE, handle.submit_atomic_messaging_owned(submission)).await?,
            Err(GuardedAtomicSubmitError::Propose(ProposeError::Broker(
                BrokerError::Storage(_)
            )))
        ));
        assert_eq!(observer.state(), AtomicCommitState::Indeterminate);
        assert!(!observer.abort());
        node.fence().await?;
        refunded(&budget);
        assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
        assert_ne!(node.snapshot()?, before);
        assert_eq!(node.messages()?.len(), 1);
        assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    }
    let committed = node.snapshot()?;
    let (_provider, reopened) = node.reopen()?;
    assert_eq!(
        reopened.snapshot()?,
        committed,
        "whole physical commit survives reopen despite reported error"
    );
    refunded(&budget);
    Ok(())
}

async fn late_logical_refusal_refunds_work_without_partial_mutation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let handle = node.broker.handle();
    let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let budget = AtomicMessagingWorkBudget::new();
    let (observer, ticket) = permit();
    let submission = owned(
        &budget,
        &node.binding,
        vec![
            posted("early"),
            CommandKind::Complete {
                sequence: SequenceNumber::new(99),
                lock_token: LockToken::new(1),
            },
        ],
        ticket,
    )?;
    charged(&budget);
    assert!(matches!(
        timeout(DEADLINE, handle.submit_atomic_messaging_owned(submission)).await?,
        Err(GuardedAtomicSubmitError::Propose(ProposeError::Broker(
            BrokerError::MessageNotFound { .. }
        )))
    ));
    assert_eq!(observer.state(), AtomicCommitState::Rejected);
    assert!(!observer.abort());
    node.fence().await?;
    refunded(&budget);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    Ok(())
}

async fn unpolled_factory_drop_aborts_and_refunds_immediately<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let handle = node.broker.handle();
    let budget = AtomicMessagingWorkBudget::new();
    let (observer, ticket) = permit();
    let submission = owned(&budget, &node.binding, vec![posted("unpolled")], ticket)?;
    charged(&budget);
    let future = handle.submit_atomic_messaging_owned(submission);
    assert_eq!(observer.state(), AtomicCommitState::Pending);
    drop(future);
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    refunded(&budget);
    assert!(handle.requests.is_empty());
    node.assert_no_work(&before)?;
    Ok(())
}

async fn claimed_owner_unwind_refunds_and_preserves_unknown_decision<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let retained = node.broker.handle();
    let ready = retained.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let budget = AtomicMessagingWorkBudget::new();
    let (observer, ticket) = permit();
    let submission = owned(
        &budget,
        &node.binding,
        vec![posted("claimed-unwind")],
        ticket,
    )?;
    charged(&budget);
    node.controls.panic_read.store(true, Ordering::SeqCst);
    assert_eq!(
        timeout(DEADLINE, retained.submit_atomic_messaging_owned(submission)).await?,
        Err(GuardedAtomicSubmitError::BrokerStopped)
    );
    assert_eq!(observer.state(), AtomicCommitState::Indeterminate);
    assert!(!observer.abort());
    refunded(&budget);
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.snapshot()?, before);
    assert!(retained.requests.is_empty());
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    Ok(())
}

async fn empty_and_blocking_owned_success_finalize_and_refund<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let before = node.snapshot()?;
    let handle = node.broker.handle();
    let ready = handle.deliverable(node.binding.namespace(), node.binding.target());
    tokio::pin!(ready);
    let budget = AtomicMessagingWorkBudget::new();
    let (observer, ticket) = permit();
    let submission = owned(&budget, &node.binding, vec![], ticket)?;
    assert_eq!(budget.usage().groups(), 1);
    assert_eq!(budget.usage().content_bytes(), 0);
    assert_eq!(budget.usage().value_items(), 0);
    let applied = timeout(DEADLINE, handle.submit_atomic_messaging_owned(submission)).await??;
    assert!(applied.outcomes.is_empty());
    assert!(applied.enqueue_targets.is_empty());
    assert_eq!(observer.state(), AtomicCommitState::Committed);
    node.fence().await?;
    refunded(&budget);
    assert!(
        node.controls.reads.load(Ordering::SeqCst) > 0,
        "empty work still validates owner identity and configuration"
    );
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    let (observer, ticket) = permit();
    let submission = owned(&budget, &node.binding, vec![posted("blocking")], ticket)?;
    charged(&budget);
    let submit = handle.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        submit.submit_atomic_messaging_owned_blocking(submission)
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
    node.fence().await?;
    refunded(&budget);
    assert_eq!(node.controls.clock_calls.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    Ok(())
}

#[derive(Default)]
struct ProbeCounts {
    reads: AtomicUsize,
    writes: AtomicUsize,
    clock: AtomicUsize,
}

#[derive(Clone)]
struct ProbeStore {
    inner: storage::MemoryStore,
    counts: Arc<ProbeCounts>,
}

impl StateStore for ProbeStore {
    fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StorageError> {
        self.counts.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.counts.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, StoredValue)>, StorageError> {
        self.counts.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
}

#[derive(Clone)]
struct ProbeClock(Arc<ProbeCounts>);

impl Clock for ProbeClock {
    fn now(&self) -> Timestamp {
        self.0.clock.fetch_add(1, Ordering::SeqCst);
        Timestamp::from_millis(2_000)
    }
}

struct ReplyWake {
    permit: AtomicCommitPermit,
    budget: AtomicMessagingWorkBudget,
    observations: Mutex<Vec<(AtomicCommitState, AtomicMessagingWorkUsage)>>,
}

impl ReplyWake {
    fn record(&self) {
        self.observations
            .lock()
            .unwrap()
            .push((self.permit.state(), self.budget.usage()));
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
fn owner_reply_wake_observes_terminal_decision_before_work_refund() -> TestResult {
    for aborted in [false, true] {
        let inner = storage::MemoryStore::default();
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        let machine = StateMachine::new(inner.clone());
        machine.apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(1_000),
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ))?;
        let binding = machine
            .bind_entity(&namespace, &entity, &entity, EntityIncarnationKind::Queue)?
            .ok_or("queue binding missing")?;
        let before = inner.snapshot()?;
        let counts = Arc::new(ProbeCounts::default());
        let proposer = LocalProposer::new(
            StateMachine::new(ProbeStore {
                inner: inner.clone(),
                counts: Arc::clone(&counts),
            }),
            ProbeClock(Arc::clone(&counts)),
        );
        let budget = AtomicMessagingWorkBudget::new();
        let (observer, ticket) = permit();
        let submission = owned(&budget, &binding, vec![posted("reply-probe")], ticket)?;
        let reserved = charged(&budget);
        if aborted {
            assert!(observer.abort());
        }
        let probe = Arc::new(ReplyWake {
            permit: observer.clone(),
            budget: budget.clone(),
            observations: Mutex::new(vec![]),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut context = Context::from_waker(&waker);
        let (reply, response) = flume::bounded(1);
        let mut future = Box::pin(response.recv_async());
        assert!(future.as_mut().poll(&mut context).is_pending());
        crate::broker::atomic_work::apply_owned(
            &proposer,
            &crate::broker::Watchers::default(),
            submission,
            reply,
        );
        let expected = if aborted {
            AtomicCommitState::Aborted
        } else {
            AtomicCommitState::Committed
        };
        assert_eq!(
            probe.observations.lock().unwrap().first().copied(),
            Some((expected, reserved)),
            "reply publication must still own all charged work"
        );
        assert_eq!(observer.state(), expected);
        refunded(&budget);
        let Poll::Ready(response) = future.as_mut().poll(&mut context) else {
            panic!("owner reply must be immediately available")
        };
        let response = response?;
        if aborted {
            assert_eq!(
                response,
                Err(GuardedAtomicSubmitError::Permit(
                    AtomicCommitClaimError::Aborted
                ))
            );
            assert_eq!(counts.reads.load(Ordering::SeqCst), 0);
            assert_eq!(counts.writes.load(Ordering::SeqCst), 0);
            assert_eq!(counts.clock.load(Ordering::SeqCst), 0);
            assert_eq!(inner.snapshot()?, before);
        } else {
            assert_eq!(
                response?.outcomes,
                vec![CommandOutcome::Sent {
                    sequence: SequenceNumber::new(1)
                }]
            );
            assert_eq!(counts.writes.load(Ordering::SeqCst), 1);
            assert_eq!(counts.clock.load(Ordering::SeqCst), 1);
        }
    }
    Ok(())
}

macro_rules! suite {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;

            #[tokio::test]
            async fn queued_cancellation_retains_work_until_owner_discard() -> TestResult {
                super::queued_cancellation_retains_work_until_owner_discard($provider).await
            }
            #[tokio::test]
            async fn started_caller_drop_retains_work_through_commit() -> TestResult {
                super::started_caller_drop_retains_work_through_commit($provider).await
            }
            #[tokio::test]
            async fn owner_stop_refunds_queued_work_without_caller_drop() -> TestResult {
                super::owner_stop_refunds_queued_work_without_caller_drop($provider).await
            }
            #[tokio::test]
            async fn reported_storage_failure_refunds_without_promising_rollback() -> TestResult {
                super::reported_storage_failure_refunds_without_promising_rollback($provider).await
            }
            #[tokio::test]
            async fn late_logical_refusal_refunds_work_without_partial_mutation() -> TestResult {
                super::late_logical_refusal_refunds_work_without_partial_mutation($provider).await
            }
            #[tokio::test]
            async fn unpolled_factory_drop_aborts_and_refunds_immediately() -> TestResult {
                super::unpolled_factory_drop_aborts_and_refunds_immediately($provider).await
            }
            #[tokio::test]
            async fn claimed_owner_unwind_refunds_and_preserves_unknown_decision() -> TestResult {
                super::claimed_owner_unwind_refunds_and_preserves_unknown_decision($provider).await
            }
            #[tokio::test]
            async fn empty_and_blocking_owned_success_finalize_and_refund() -> TestResult {
                super::empty_and_blocking_owned_success_finalize_and_refund($provider).await
            }
        }
    };
}

suite!(memory, testkit::MemoryProvider::new());
suite!(durable, testkit::DurableProvider::temporary()?);
