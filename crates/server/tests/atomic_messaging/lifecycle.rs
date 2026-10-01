use super::*;

pub(super) async fn ambiguous_commit<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider).await?;
    let before = node.store.snapshot()?;
    let handle = node.broker.handle();
    let ready = handle.deliverable(&node.namespace, &node.entity);
    tokio::pin!(ready);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    node.error_after_commit.store(true, Ordering::Relaxed);
    assert!(matches!(
        node.apply(vec![send("unknown-one"), send("unknown-two")])
            .await,
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::Storage(_)
        )))
    ));
    assert_ne!(node.store.snapshot()?, before);
    assert_eq!(node.peek(node.entity.clone()).await?.len(), 2);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    Ok(())
}

pub(super) async fn mixed_commit<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider).await?;
    let held = node.seed_held().await?;
    let handle = node.broker.handle();
    let neighbor_entity = EntityPath::new("neighbor")?;
    let foreign_namespace = NamespaceName::new("foreign")?;
    let ready = handle.deliverable(&node.namespace, &node.entity);
    let neighbor = handle.deliverable(&node.namespace, &neighbor_entity);
    let foreign = handle.deliverable(&foreign_namespace, &node.entity);
    tokio::pin!(ready, neighbor, foreign);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    let commits = node.commits.load(Ordering::Relaxed);
    let applied = node
        .apply(vec![send("one"), complete(&held), send("two")])
        .await?;
    assert_eq!(node.commits.load(Ordering::Relaxed), commits + 1);
    assert_eq!(
        applied.outcomes,
        vec![
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(2)
            },
            CommandOutcome::Completed,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(3)
            }
        ]
    );
    assert_eq!(applied.enqueue_targets, vec![node.entity.clone()]);
    timeout(DEADLINE, &mut ready).await?;
    assert!(timeout(NO_WAKE, &mut neighbor).await.is_err());
    assert!(timeout(NO_WAKE, &mut foreign).await.is_err());
    assert_eq!(
        node.peek(node.entity.clone())
            .await?
            .iter()
            .map(|message| message.message_id.as_str())
            .collect::<Vec<_>>(),
        vec!["one", "two"]
    );
    Ok(())
}

pub(super) async fn dead_letter_commit<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider).await?;
    let held = node.seed_held().await?;
    let handle = node.broker.handle();
    let shadow = node.entity.dead_letter_queue()?;
    let ready = handle.deliverable(&node.namespace, &node.entity);
    let dead = handle.deliverable(&node.namespace, &shadow);
    tokio::pin!(ready, dead);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    assert!(timeout(NO_WAKE, &mut dead).await.is_err());
    let applied = node
        .apply(vec![
            CommandKind::DeadLetter {
                sequence: held.sequence,
                lock_token: held.lock.unwrap().token,
                reason: "atomic".to_owned(),
                description: "failed processing".to_owned(),
            },
            send("replacement"),
        ])
        .await?;
    assert_eq!(
        applied.enqueue_targets,
        vec![node.entity.clone(), shadow.clone()]
    );
    timeout(DEADLINE, &mut ready).await?;
    timeout(DEADLINE, &mut dead).await?;
    assert_eq!(node.peek(node.entity.clone()).await?.len(), 1);
    let letters = node.peek(shadow.clone()).await?;
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].sequence, held.sequence);
    assert_eq!(
        letters[0].dead_letter.as_ref().unwrap().reason.as_str(),
        "atomic"
    );
    Ok(())
}

pub(super) async fn failed_commit<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider).await?;
    let held = node.seed_held().await?;
    let before = node.store.snapshot()?;
    let handle = node.broker.handle();
    let ready = handle.deliverable(&node.namespace, &node.entity);
    tokio::pin!(ready);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    node.clock.set(2_000);
    node.fail_next.store(true, Ordering::Relaxed);
    assert!(matches!(
        node.apply(vec![complete(&held), send("one"), send("two")])
            .await,
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::Storage(_)
        )))
    ));
    assert_eq!(node.store.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    let commits = node.commits.load(Ordering::Relaxed);
    assert!(matches!(
        node.apply(vec![
            send("early"),
            CommandKind::Complete {
                sequence: held.sequence,
                lock_token: LockToken::new(999)
            }
        ])
        .await,
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::LockTokenMismatch { .. }
        )))
    ));
    assert_eq!(node.commits.load(Ordering::Relaxed), commits);
    assert_eq!(node.store.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    let applied = node.apply(vec![complete(&held), send("retry")]).await?;
    assert_eq!(
        applied.outcomes,
        vec![
            CommandOutcome::Completed,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(2)
            }
        ]
    );
    timeout(DEADLINE, &mut ready).await?;
    assert_eq!(node.peek(node.entity.clone()).await?.len(), 1);
    Ok(())
}

pub(super) async fn empty_group<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider).await?;
    let before = node.store.snapshot()?;
    let commits = node.commits.load(Ordering::Relaxed);
    let handle = node.broker.handle();
    let ready = handle.deliverable(&node.namespace, &node.entity);
    tokio::pin!(ready);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    node.clock.set(2_000);
    for kinds in [
        Vec::new(),
        vec![CommandKind::SendBatch {
            messages: Vec::new(),
        }],
    ] {
        let applied = node.apply(kinds).await?;
        assert!(applied.enqueue_targets.is_empty());
        assert_eq!(node.store.snapshot()?, before);
        assert_eq!(node.commits.load(Ordering::Relaxed), commits);
        assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    }
    Ok(())
}

pub(super) async fn concurrent_groups<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider).await?;
    let commits = node.commits.load(Ordering::Relaxed);
    let mut tasks = Vec::new();
    for index in 0..8 {
        let handle = node.broker.handle();
        let binding = node.binding.clone();
        tasks.push(tokio::spawn(async move {
            handle
                .submit_atomic_messaging(
                    binding,
                    vec![
                        send(&format!("{index}-first")),
                        send(&format!("{index}-second")),
                    ],
                )
                .await
        }));
    }
    for task in tasks {
        let applied = timeout(DEADLINE, task).await???;
        let [
            CommandOutcome::Sent { sequence: first },
            CommandOutcome::Sent { sequence: second },
        ] = applied.outcomes.as_slice()
        else {
            return Err("send outcomes missing".into());
        };
        assert_eq!(second.as_u64(), first.as_u64() + 1);
    }
    assert_eq!(node.commits.load(Ordering::Relaxed), commits + 8);
    let messages = node.peek(node.entity.clone()).await?;
    assert_eq!(messages.len(), 16);
    for pair in messages.chunks_exact(2) {
        assert_eq!(
            pair[0].message_id.split('-').next(),
            pair[1].message_id.split('-').next()
        );
    }
    Ok(())
}

pub(super) async fn duplicate_only<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::with_config(
        provider,
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )
    .await?;
    node.apply(vec![send("duplicate")]).await?;
    let handle = node.broker.handle();
    let ready = handle.deliverable(&node.namespace, &node.entity);
    tokio::pin!(ready);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    let commits = node.commits.load(Ordering::Relaxed);
    node.clock.set(2_000);
    let applied = node
        .apply(vec![send("duplicate"), send("duplicate")])
        .await?;
    assert_eq!(
        applied.outcomes,
        vec![
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(2)
            },
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(3)
            }
        ]
    );
    assert!(applied.enqueue_targets.is_empty());
    assert_eq!(node.commits.load(Ordering::Relaxed), commits + 1);
    assert_eq!(node.peek(node.entity.clone()).await?.len(), 1);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    let applied = node.apply(vec![send("duplicate"), send("fresh")]).await?;
    assert_eq!(
        applied.outcomes,
        vec![
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(4)
            },
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(5)
            }
        ]
    );
    assert_eq!(applied.enqueue_targets, vec![node.entity.clone()]);
    timeout(DEADLINE, &mut ready).await?;
    assert_eq!(node.peek(node.entity.clone()).await?.len(), 2);
    Ok(())
}

pub(super) async fn canceled_caller<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider).await?;
    let before = node.store.snapshot()?;
    let (started, resume) = node.pause_commit();
    let handle = node.broker.handle();
    let ready = handle.deliverable(&node.namespace, &node.entity);
    tokio::pin!(ready);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    let submit = node.broker.handle();
    let binding = node.binding.clone();
    let task = tokio::spawn(async move {
        submit
            .submit_atomic_messaging(binding, vec![send("canceled")])
            .await
    });
    timeout(DEADLINE, started.recv_async()).await??;
    assert_eq!(node.store.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    task.abort();
    assert!(timeout(DEADLINE, task).await?.unwrap_err().is_cancelled());
    resume.send(())?;
    timeout(DEADLINE, &mut ready).await?;
    assert_eq!(node.peek(node.entity.clone()).await?.len(), 1);
    let retry = node.apply(vec![send("canceled")]).await?;
    assert_eq!(
        retry.outcomes,
        vec![CommandOutcome::Sent {
            sequence: SequenceNumber::new(2)
        }]
    );
    assert_eq!(node.peek(node.entity.clone()).await?.len(), 2);
    Ok(())
}
