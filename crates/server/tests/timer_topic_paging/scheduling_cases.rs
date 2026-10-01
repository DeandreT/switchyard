use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use domain::{
    BrokerError, MAX_TOPIC_FANOUT_COPIES, MAX_TOPIC_SUBSCRIPTIONS, MessageRecord, MessageState,
    ScheduledMessage, SequenceNumber,
};
use protocol_amqp::Broker as _;
use tokio::time::timeout;

use super::*;

fn scheduled(id: &str) -> ScheduledMessage {
    ScheduledMessage {
        message_id: id.to_owned(),
        body: id.as_bytes().to_vec(),
        time_to_live_millis: None,
        session_id: None,
        enqueue_at: Timestamp::from_millis(2_000),
    }
}

fn schedule<P: StoreProvider>(
    node: &Node<P>,
    entity: &str,
    ids: &[String],
) -> TestResult<Vec<SequenceNumber>> {
    let outcome = node.submit(
        "tenant",
        entity,
        CommandKind::Schedule {
            messages: ids.iter().map(|id| scheduled(id)).collect(),
        },
    )?;
    let CommandOutcome::Scheduled { sequences } = outcome else {
        panic!("schedule returned {outcome:?}");
    };
    Ok(sequences)
}

fn records<P: StoreProvider>(
    node: &Node<P>,
    entity: &EntityPath,
) -> TestResult<Vec<MessageRecord>> {
    Ok(node
        .store
        .scan_prefix(
            &keys::message_prefix(&NamespaceName::new("tenant")?, entity),
            usize::MAX,
        )?
        .into_iter()
        .map(|(_, bytes)| MessageRecord::decode(&bytes))
        .collect::<Result<_, _>>()?)
}

fn fitting_prefixes_continue_until_the_round_cap_then_resume<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("tenant", "topic")?;
    node.queue("queue")?;
    let mut subscriptions = Vec::new();
    for index in 0..MAX_TOPIC_SUBSCRIPTIONS {
        subscriptions.push(node.subscription(
            "tenant",
            "topic",
            &format!("sub-{index:02}"),
            SubscriptionConfig::default(),
        )?);
    }
    let per_command = MAX_TOPIC_FANOUT_COPIES / subscriptions.len();
    let maximum = MAX_ROUNDS_PER_INDEX * per_command;
    let ids: Vec<_> = (0..=maximum)
        .map(|index| format!("scheduled-{index:04}"))
        .collect();
    let mut handles = Vec::new();
    for chunk in ids.chunks(per_command) {
        handles.extend(schedule(&node, "topic", chunk)?);
    }
    schedule(&node, "queue", &["queue-scheduled".into()])?;
    let parent = EntityPath::new("topic")?;
    assert_eq!(records(&node, &parent)?.len(), maximum + 1);
    for entity in &subscriptions {
        assert!(records(&node, entity)?.is_empty());
    }

    node.clock.inner.set(2_000);
    node.store.clear();
    let handle = node.handle();
    let worker = TimerWorker::new(&handle);
    let first = worker.sweep_once()?;
    assert_eq!(first.messages_activated as usize, maximum + 1);
    let pending = records(&node, &parent)?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].sequence, handles[maximum]);
    assert!(matches!(pending[0].state, MessageState::Scheduled { .. }));
    let scans = node
        .store
        .observed
        .entity_scans
        .lock()
        .expect("entity scans");
    let activation_scans: Vec<_> = scans
        .iter()
        .filter(|(tag, entity, _)| *tag == 11 && entity == &parent)
        .collect();
    assert_eq!(activation_scans.len(), MAX_ROUNDS_PER_INDEX);
    assert!(
        activation_scans
            .iter()
            .all(|(_, _, limit)| *limit <= TIMER_SCAN_LIMIT)
    );
    drop(scans);
    let first_sequences: Vec<_> = records(&node, &subscriptions[0])?
        .into_iter()
        .map(|record| record.sequence)
        .collect();
    assert_eq!(first_sequences.len(), maximum);
    assert!(
        first_sequences
            .iter()
            .all(|sequence| *sequence > handles[maximum])
    );
    for entity in &subscriptions[1..] {
        assert_eq!(
            records(&node, entity)?
                .into_iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            first_sequences
        );
    }
    let queue_records = records(&node, &EntityPath::new("queue")?)?;
    assert_eq!(queue_records.len(), 1);
    assert_eq!(queue_records[0].state, MessageState::Ready);
    assert_eq!(queue_records[0].sequence, SequenceNumber::new(2));

    node.store.clear();
    assert_eq!(worker.sweep_once()?.messages_activated, 1);
    assert!(records(&node, &parent)?.is_empty());
    for entity in &subscriptions {
        assert_eq!(records(&node, entity)?.len(), maximum + 1);
    }
    assert!(worker.sweep_once()?.is_idle());
    Ok(())
}

fn failed_topic_activation_advances_discovery_and_preserves_queue_progress<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("tenant", "a-dangling")?;
    node.topic("tenant", "z-healthy")?;
    node.queue("queue")?;
    let child = node.subscription("tenant", "z-healthy", "sub", SubscriptionConfig::default())?;
    schedule(&node, "z-healthy", &["healthy".into()])?;
    schedule(&node, "queue", &["queue".into()])?;
    let namespace = NamespaceName::new("tenant")?;
    let broken = EntityPath::new("a-dangling")?;
    let dangling = keys::scheduled(
        &namespace,
        &broken,
        Timestamp::from_millis(2_000),
        SequenceNumber::new(99),
    );
    node.store
        .apply(WriteBatch::default().put(dangling.clone(), Vec::new()))?;
    node.clock.inner.set(2_000);
    let handle = node.handle();
    let worker = TimerWorker::new(&handle);
    assert!(
        matches!(worker.sweep_once(), Err(server::SubmitError::Propose(server::ProposeError::Broker(BrokerError::DanglingIndexEntry { sequence }))) if sequence == SequenceNumber::new(99))
    );
    assert_eq!(node.store.get(&dangling)?, Some(Vec::new()));
    let queue_records = records(&node, &EntityPath::new("queue")?)?;
    assert_eq!(queue_records.len(), 1);
    assert_eq!(queue_records[0].state, MessageState::Ready);
    assert_eq!(queue_records[0].sequence, SequenceNumber::new(2));
    assert!(records(&node, &child)?.is_empty());
    node.store.clear();
    let resumed = worker.sweep_once()?;
    assert_eq!(resumed.topics_swept, 1);
    assert_eq!(resumed.messages_activated, 1);
    assert_eq!(records(&node, &child)?.len(), 1);
    assert_eq!(node.store.get(&dangling)?, Some(Vec::new()));
    let mut after_broken = keys::topic_config(&namespace, &broken);
    after_broken.push(0);
    assert_eq!(node.store.pages(14)[0].start, after_broken);
    Ok(())
}

fn poll<F: Future<Output = ()>>(future: Pin<&mut F>) -> Poll<()> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

async fn scheduled_admission_and_cancel_are_quiet_then_only_committed_destinations_wake<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("tenant", "topic")?;
    node.topic("tenant", "other")?;
    let required = node.subscription(
        "tenant",
        "topic",
        "required",
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
    )?;
    let ordinary =
        node.subscription("tenant", "topic", "ordinary", SubscriptionConfig::default())?;
    let unrelated = node.subscription("tenant", "other", "sub", SubscriptionConfig::default())?;
    let parent = EntityPath::new("topic")?;
    let watched = [
        ordinary,
        required.dead_letter_queue()?,
        required,
        unrelated,
        parent,
    ];
    let namespace = NamespaceName::new("tenant")?;
    let handle = node.handle();
    let mut waits: Vec<_> = watched
        .iter()
        .map(|entity| Box::pin(handle.deliverable(&namespace, entity)))
        .collect();
    assert!(
        waits
            .iter_mut()
            .all(|wait| poll(wait.as_mut()).is_pending())
    );
    let sequences = schedule(&node, "topic", &["kept".into(), "cancelled".into()])?;
    assert!(
        waits
            .iter_mut()
            .all(|wait| poll(wait.as_mut()).is_pending())
    );
    assert_eq!(
        node.submit(
            "tenant",
            "topic",
            CommandKind::CancelScheduled {
                sequences: vec![sequences[1]]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert!(
        waits
            .iter_mut()
            .all(|wait| poll(wait.as_mut()).is_pending())
    );
    node.clock.inner.set(2_000);
    let before_activation = node.store.snapshot()?;
    node.store.observed.fail_apply.store(true, Ordering::SeqCst);
    assert!(
        node.submit("tenant", "topic", CommandKind::ActivateScheduled)
            .is_err()
    );
    assert_eq!(node.store.snapshot()?, before_activation);
    assert!(
        waits
            .iter_mut()
            .all(|wait| poll(wait.as_mut()).is_pending())
    );
    node.store
        .observed
        .observe_commit_reads
        .store(true, Ordering::SeqCst);
    assert_eq!(
        node.submit("tenant", "topic", CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    assert_eq!(
        node.store.observed.post_commit_reads.load(Ordering::SeqCst),
        0
    );
    for wait in waits.iter_mut().take(2) {
        timeout(Duration::from_secs(1), wait.as_mut()).await?;
    }
    assert!(
        waits
            .iter_mut()
            .skip(2)
            .all(|wait| poll(wait.as_mut()).is_pending())
    );
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    fitting_prefixes_continue_until_the_round_cap_then_resume,
    failed_topic_activation_advances_discovery_and_preserves_queue_progress,
}

#[tokio::test]
async fn memory_scheduled_wakeups() -> TestResult {
    scheduled_admission_and_cancel_are_quiet_then_only_committed_destinations_wake(
        testkit::MemoryProvider::new(),
    )
    .await
}

#[tokio::test]
async fn durable_scheduled_wakeups() -> TestResult {
    scheduled_admission_and_cancel_are_quiet_then_only_committed_destinations_wake(
        testkit::DurableProvider::temporary()?,
    )
    .await
}
