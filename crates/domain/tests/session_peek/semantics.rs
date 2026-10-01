use super::*;

fn entity_wide_browsing_orders_all_states_without_acquiring_or_changing_holds<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for (index, target) in node.targets().into_iter().enumerate() {
        let base = index as u64 * 1_000;
        for (sequence, session) in [(1, "A"), (2, "B"), (3, "A"), (4, "B")] {
            assert_eq!(
                node.send(
                    &target,
                    base + sequence,
                    &format!("id-{sequence}"),
                    session,
                    None,
                    8
                )?,
                SequenceNumber::new(sequence)
            );
        }
        let a = node.accept(&target, base + 5, "A")?;
        let b = node.accept(&target, base + 6, "B")?;
        let locked = node.receive(&target, base + 7, &a.hold(), None)?;
        let deferred = node.receive(&target, base + 8, &b.hold(), None)?;
        node.at(
            &target,
            base + 9,
            CommandKind::Defer {
                sequence: deferred.sequence,
                lock_token: deferred.lock.expect("lock").token,
            },
        )?;
        node.at(
            &target,
            base + 10,
            CommandKind::SetSessionState {
                session: a.hold(),
                state: b"unchanged-a".to_vec(),
            },
        )?;
        if target == node.queue {
            let CommandOutcome::Scheduled { sequences } = node.at(
                &target,
                base + 11,
                CommandKind::Schedule {
                    messages: vec![ScheduledMessage {
                        message_id: "later".into(),
                        body: vec![9],
                        time_to_live_millis: None,
                        session_id: Some(SessionId::new("C")?),
                        enqueue_at: Timestamp::from_millis(base + 10_000),
                    }],
                },
            )?
            else {
                panic!("scheduled outcome")
            };
            assert_eq!(sequences, vec![SequenceNumber::new(5)]);
        }
        let deliveries = node.peek(&target, base + 50, 1, u32::MAX, None, None)?;
        assert_eq!(
            sequences(&deliveries),
            if target == node.queue {
                vec![1, 2, 3, 4, 5]
            } else {
                vec![1, 2, 3, 4]
            }
        );
        assert_eq!(deliveries[0].status, MessageStatus::Active);
        assert_eq!(deliveries[0].delivery_count, 1);
        assert_eq!(deliveries[1].status, MessageStatus::Deferred);
        assert_eq!(deliveries[2].status, MessageStatus::Active);
        assert_eq!(deliveries[2].delivery_count, 0);
        if target == node.queue {
            assert_eq!(deliveries[4].status, MessageStatus::Scheduled);
        }
        assert_eq!(
            node.peek(&target, base + 50, 1, u32::MAX, None, Some(u64::MAX))?,
            deliveries
        );
        assert!(matches!(
            node.fixture
                .machine
                .message(&node.fixture.namespace, &target, locked.sequence)?
                .expect("locked record")
                .state,
            MessageState::Locked { .. }
        ));
        assert_eq!(
            node.fixture.machine.session(
                &node.fixture.namespace,
                &target,
                &SessionId::new("C")?
            )?,
            None
        );
        assert_eq!(
            node.readonly(
                &target,
                base + 50,
                CommandKind::GetSessionState { session: a.hold() }
            )?,
            CommandOutcome::SessionState(b"unchanged-a".to_vec())
        );
    }
    Ok(())
}

fn named_filters_and_sequence_count_bounds_remain_exact<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for (index, target) in node.targets().into_iter().enumerate() {
        let base = index as u64 * 1_000;
        for (sequence, session) in [(1, "A"), (2, "B"), (3, "A")] {
            node.send(
                &target,
                base + sequence,
                &format!("id-{sequence}"),
                session,
                None,
                8,
            )?;
        }
        for budget in [None, Some(u64::MAX)] {
            assert_eq!(
                sequences(&node.peek(&target, base + 50, 1, 10, Some("A"), budget)?),
                [1, 3]
            );
            assert_eq!(
                sequences(&node.peek(&target, base + 50, 2, 10, Some("A"), budget)?),
                [3]
            );
            assert!(
                node.peek(&target, base + 50, 1, 10, Some("a"), budget)?
                    .is_empty()
            );
            assert_eq!(
                sequences(&node.peek(&target, base + 50, 2, 2, None, budget)?),
                [2, 3]
            );
            assert!(
                node.peek(&target, base + 50, 1, 0, None, budget)?
                    .is_empty()
            );
            assert!(
                node.peek(&target, base + 50, 4, 10, None, budget)?
                    .is_empty()
            );
        }
        for session in ["A", "B"] {
            assert_eq!(
                node.fixture.machine.session(
                    &node.fixture.namespace,
                    &target,
                    &SessionId::new(session)?
                )?,
                None
            );
        }
        assert_eq!(
            node.peek(&target, base + 2, 1, 10, None, None),
            Err(BrokerError::ClockRegression {
                last_applied: Timestamp::from_millis(base + 3),
                proposed: Timestamp::from_millis(base + 2),
            })
        );
    }
    Ok(())
}

fn future_peek_skips_expired_ready_and_old_locks_without_destructive_cleanup<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for (index, target) in node.targets().into_iter().enumerate() {
        let base = index as u64 * 1_000;
        for (sequence, session) in [(1, "A"), (2, "B"), (3, "C"), (4, "D")] {
            node.send(
                &target,
                base + sequence,
                &format!("id-{sequence}"),
                session,
                Some(10),
                8,
            )?;
        }
        let b = node.accept(&target, base + 5, "B")?;
        let c = node.accept(&target, base + 6, "C")?;
        let d = node.accept(&target, base + 7, "D")?;
        let deferred = node.receive(&target, base + 8, &b.hold(), None)?;
        node.at(
            &target,
            base + 9,
            CommandKind::Defer {
                sequence: deferred.sequence,
                lock_token: deferred.lock.expect("lock").token,
            },
        )?;
        node.receive(&target, base + 10, &c.hold(), Some(1_000))?;
        node.receive(&target, base + 10, &d.hold(), Some(5))?;
        for budget in [None, Some(u64::MAX)] {
            let deliveries = node.peek(&target, base + 50, 1, 10, None, budget)?;
            assert_eq!(sequences(&deliveries), [2, 3]);
            assert_eq!(deliveries[0].status, MessageStatus::Deferred);
            assert_eq!(deliveries[1].status, MessageStatus::Active);
            assert_eq!(
                sequences(&node.peek(&target, base + 50, 1, 10, Some("B"), budget)?),
                [2]
            );
        }
        for sequence in 1..=4 {
            assert!(
                node.fixture
                    .machine
                    .message(
                        &node.fixture.namespace,
                        &target,
                        SequenceNumber::new(sequence)
                    )?
                    .is_some()
            );
        }
    }
    Ok(())
}

fn read_only_relaxation_does_not_relax_ordinary_filters_or_destructive_session_checks<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for target in [
        node.fixture.entity.clone(),
        node.ordinary_subscription.clone(),
    ] {
        for budget in [None, Some(u64::MAX)] {
            assert!(node.peek(&target, 50, 1, 0, None, budget)?.is_empty());
            assert_eq!(
                node.peek(&target, 50, 1, 0, Some("A"), budget),
                Err(BrokerError::SessionNotSupported)
            );
        }
    }
    for target in node.targets() {
        assert!(node.peek(&target, 50, 1, 0, None, None)?.is_empty());
        for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
            for command in [
                CommandKind::Receive {
                    mode,
                    lock_duration_millis: None,
                    session: None,
                },
                CommandKind::ReceiveDeferred {
                    sequences: vec![],
                    mode,
                    lock_duration_millis: None,
                    session_id: None,
                },
                CommandKind::ReceiveDeferredBounded {
                    sequences: vec![],
                    mode,
                    lock_duration_millis: None,
                    session_id: None,
                    budget: DeliveryBudget {
                        max_bytes: 0,
                        per_message_overhead_bytes: 0,
                    },
                },
                CommandKind::ReceiveDeferredHeld {
                    sequences: vec![],
                    mode,
                    lock_duration_millis: None,
                    session: None,
                    budget: DeliveryBudget {
                        max_bytes: 0,
                        per_message_overhead_bytes: 0,
                    },
                },
            ] {
                assert_eq!(
                    node.readonly(&target, 50, command),
                    Err(BrokerError::SessionRequired)
                );
            }
        }
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}
for_each_backend! {
    entity_wide_browsing_orders_all_states_without_acquiring_or_changing_holds,
    named_filters_and_sequence_count_bounds_remain_exact,
    future_peek_skips_expired_ready_and_old_locks_without_destructive_cleanup,
    read_only_relaxation_does_not_relax_ordinary_filters_or_destructive_session_checks,
}
