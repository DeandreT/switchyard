use super::*;

fn message_scans<P: StoreProvider>(node: &Node<P>, target: &EntityPath) -> Vec<Scan> {
    let mut mode_prefix = domain::keys::topic_mode(&node.fixture.namespace, node.source(target));
    assert_eq!(mode_prefix.pop(), Some(0));
    mode_prefix.extend_from_slice(b"/subscriptions/");
    let (metadata, messages): (Vec<_>, Vec<_>) = node
        .scans()
        .into_iter()
        .partition(|scan| scan.prefix == mode_prefix);
    assert_eq!(metadata.len(), 1, "one completed capacity-profile probe");
    assert_eq!(metadata[0].start, mode_prefix);
    assert_eq!(metadata[0].limit, 1);
    assert_eq!(metadata[0].returned, 0);
    let prefix = domain::keys::message_prefix(&node.fixture.namespace, target);
    assert!(messages.iter().all(|scan| scan.prefix == prefix));
    messages
}

fn byte_budgets_fit_exact_prefixes_and_reject_an_oversized_first_record<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for (index, target) in node.targets().into_iter().enumerate() {
        let base = index as u64 * 1_000;
        for (sequence, session, size) in [(1, "A", 30), (2, "A", 80), (3, "B", 150)] {
            node.send(
                &target,
                base + sequence,
                &format!("id-{sequence}"),
                session,
                None,
                size,
            )?;
        }
        let charges: Vec<_> = (1..=3)
            .map(|sequence| {
                node.fixture
                    .machine
                    .message(
                        &node.fixture.namespace,
                        &target,
                        SequenceNumber::new(sequence),
                    )
                    .expect("record query")
                    .expect("record")
                    .delivery_size_upper_bound()
                    + ENTRY_OVERHEAD
            })
            .collect();
        node.scans();
        assert_eq!(
            sequences(&node.peek(
                &target,
                base + 50,
                1,
                3,
                None,
                Some(charges[0] + charges[1])
            )?),
            [1, 2]
        );
        let scans = message_scans(&node, &target);
        assert_eq!(scans.len(), 3);
        assert!(
            scans
                .iter()
                .all(|scan| scan.limit == 1 && scan.returned == 1)
        );
        assert_eq!(
            sequences(&node.peek(&target, base + 50, 1, 3, None, Some(charges[0]))?),
            [1]
        );
        assert_eq!(
            node.peek(&target, base + 50, 1, 3, None, Some(charges[0] - 1)),
            Err(BrokerError::MessageTooLarge {
                body_bytes: charges[0] as usize,
                maximum_bytes: (charges[0] - 1) as usize
            })
        );
        assert_eq!(
            sequences(&node.peek(&target, base + 50, 1, 3, Some("B"), Some(charges[2]))?),
            [3]
        );
    }
    Ok(())
}

fn physical_scan_cap_applies_to_all_sessions_and_sparse_named_filters<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for (index, target) in node.targets().into_iter().enumerate() {
        let base = index as u64 * 1_000;
        let mut messages: Vec<_> = (0..=MAX_INSPECTED)
            .map(|index| IngressEnvelope {
                message_id: format!("id-{index}"),
                body: vec![0],
                time_to_live_millis: None,
                session_id: Some(
                    SessionId::new(if index == MAX_INSPECTED {
                        "target"
                    } else {
                        "excluded"
                    })
                    .expect("session"),
                ),
                scheduled_enqueue_time: None,
                envelope: MessageEnvelope::default(),
            })
            .collect();
        let target_message = messages.pop().expect("last target message");
        node.at(
            node.source(&target),
            base + 1,
            CommandKind::SendBatch { messages },
        )?;
        node.at(
            node.source(&target),
            base + 1,
            CommandKind::SendBatch {
                messages: vec![target_message],
            },
        )?;
        node.scans();
        let deliveries = node.peek(&target, base + 50, 1, u32::MAX, None, None)?;
        assert_eq!(
            sequences(&deliveries),
            (1..=MAX_INSPECTED as u64).collect::<Vec<_>>()
        );
        let scans = message_scans(&node, &target);
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].limit, MAX_INSPECTED);
        assert_eq!(scans[0].returned, MAX_INSPECTED);
        let deliveries = node.peek(&target, base + 50, 1, u32::MAX, None, Some(u64::MAX))?;
        assert_eq!(deliveries.len(), MAX_INSPECTED);
        let scans = message_scans(&node, &target);
        assert_eq!(scans.len(), MAX_INSPECTED);
        assert!(
            scans
                .iter()
                .all(|scan| scan.limit == 1 && scan.returned == 1)
        );
        for budget in [None, Some(u64::MAX)] {
            assert!(
                node.peek(&target, base + 50, 1, 1, Some("target"), budget)?
                    .is_empty()
            );
            let scans = message_scans(&node, &target);
            assert_eq!(
                scans.iter().map(|scan| scan.returned).sum::<usize>(),
                MAX_INSPECTED
            );
            if budget.is_some() {
                assert!(scans.iter().all(|scan| scan.limit == 1));
            }
            assert_eq!(
                sequences(&node.peek(
                    &target,
                    base + 50,
                    MAX_INSPECTED as u64 + 1,
                    1,
                    Some("target"),
                    budget
                )?),
                [MAX_INSPECTED as u64 + 1]
            );
            let scans = message_scans(&node, &target);
            assert_eq!(scans.len(), 1);
            assert_eq!(scans[0].returned, 1);
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
    byte_budgets_fit_exact_prefixes_and_reject_an_oversized_first_record,
    physical_scan_cap_applies_to_all_sessions_and_sparse_named_filters,
}
