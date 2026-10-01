use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use domain::{IngressEnvelope, MessageBody, MessageEnvelope};
use protocol_amqp::Broker as _;
use tokio::time::timeout;

use super::*;

const DEADLINE: Duration = Duration::from_secs(10);

fn poll<F: Future<Output = ()>>(future: Pin<&mut F>) -> Poll<()> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn send(id: &str, bytes: usize) -> CommandKind {
    CommandKind::Send {
        message_id: id.to_owned(),
        body: vec![42; bytes],
        time_to_live_millis: None,
        session_id: None,
    }
}

fn envelope(id: &str) -> IngressEnvelope {
    IngressEnvelope {
        message_id: id.to_owned(),
        body: vec![42; 8],
        time_to_live_millis: None,
        session_id: None,
        envelope: MessageEnvelope {
            body: MessageBody::Data(vec![vec![42; 8]]),
            ..MessageEnvelope::default()
        },
        scheduled_enqueue_time: None,
    }
}

async fn committed_fanout_wakes_only_destinations_without_postcommit_reads<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("tenant", "topic")?;
    node.topic("other", "topic")?;
    let a = node.subscription("tenant", "topic", "a", SubscriptionConfig::default())?;
    let b = node.subscription("tenant", "topic", "b", SubscriptionConfig::default())?;
    let other = node.subscription("other", "topic", "a", SubscriptionConfig::default())?;
    node.queue("queue")?;
    let watched = [
        (NamespaceName::new("tenant")?, a.clone()),
        (NamespaceName::new("tenant")?, b),
        (NamespaceName::new("tenant")?, EntityPath::new("topic")?),
        (NamespaceName::new("other")?, other),
        (NamespaceName::new("tenant")?, EntityPath::new("queue")?),
        (NamespaceName::new("tenant")?, a.dead_letter_queue()?),
    ];
    let handle = node.handle();
    let mut waits = watched
        .iter()
        .map(|(namespace, entity)| Box::pin(handle.deliverable(namespace, entity)))
        .collect::<Vec<_>>();
    assert!(
        waits
            .iter_mut()
            .all(|waiting| poll(waiting.as_mut()).is_pending())
    );
    node.store
        .observed
        .observe_commit_reads
        .store(true, Ordering::SeqCst);
    node.submit("tenant", "topic", send("first", 8))?;
    assert_eq!(
        node.store.observed.post_commit_reads.load(Ordering::SeqCst),
        0
    );
    for waiting in waits.iter_mut().take(2) {
        timeout(DEADLINE, waiting.as_mut()).await?;
    }
    assert!(
        waits
            .iter_mut()
            .skip(2)
            .all(|waiting| poll(waiting.as_mut()).is_pending())
    );
    Ok(())
}

async fn duplicates_only_and_zero_subscriptions_publish_no_wakeups<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("tenant", "topic")?;
    node.topic("tenant", "empty")?;
    let sub = node.subscription("tenant", "topic", "a", SubscriptionConfig::default())?;
    node.submit("tenant", "topic", send("duplicate", 8))?;
    let namespace = NamespaceName::new("tenant")?;
    let parent = EntityPath::new("topic")?;
    let empty = EntityPath::new("empty")?;
    let handle = node.handle();
    let mut sub_wait = Box::pin(handle.deliverable(&namespace, &sub));
    let mut parent_wait = Box::pin(handle.deliverable(&namespace, &parent));
    let mut empty_wait = Box::pin(handle.deliverable(&namespace, &empty));
    assert!(poll(sub_wait.as_mut()).is_pending());
    node.submit("tenant", "topic", send("duplicate", 8))?;
    node.submit(
        "tenant",
        "topic",
        CommandKind::SendBatch {
            messages: vec![envelope("duplicate"), envelope("duplicate")],
        },
    )?;
    node.submit("tenant", "empty", send("accepted-without-subscriptions", 8))?;
    assert!(poll(sub_wait.as_mut()).is_pending());
    assert!(poll(parent_wait.as_mut()).is_pending());
    assert!(poll(empty_wait.as_mut()).is_pending());
    node.submit(
        "tenant",
        "topic",
        CommandKind::SendBatch {
            messages: vec![envelope("duplicate"), envelope("new")],
        },
    )?;
    timeout(DEADLINE, sub_wait.as_mut()).await?;
    assert!(poll(parent_wait.as_mut()).is_pending());
    assert!(poll(empty_wait.as_mut()).is_pending());
    Ok(())
}

async fn atomic_refusal_and_failed_commit_wake_nobody<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("tenant", "topic")?;
    let a = node.subscription("tenant", "topic", "a", SubscriptionConfig::default())?;
    let b = node.subscription(
        "tenant",
        "topic",
        "b",
        SubscriptionConfig {
            max_message_bytes: 64,
            ..SubscriptionConfig::default()
        },
    )?;
    let watched = [a, b, EntityPath::new("topic")?];
    let namespace = NamespaceName::new("tenant")?;
    let handle = node.handle();
    let mut waits = watched
        .iter()
        .map(|entity| Box::pin(handle.deliverable(&namespace, entity)))
        .collect::<Vec<_>>();
    assert!(
        waits
            .iter_mut()
            .all(|waiting| poll(waiting.as_mut()).is_pending())
    );
    let snapshot = node.store.snapshot()?;
    assert!(
        node.submit("tenant", "topic", send("too-big", 128))
            .is_err()
    );
    assert_eq!(node.store.snapshot()?, snapshot);
    assert!(
        waits
            .iter_mut()
            .all(|waiting| poll(waiting.as_mut()).is_pending())
    );
    node.store.observed.fail_apply.store(true, Ordering::SeqCst);
    assert!(node.submit("tenant", "topic", send("retry", 8)).is_err());
    assert_eq!(node.store.snapshot()?, snapshot);
    assert!(
        waits
            .iter_mut()
            .all(|waiting| poll(waiting.as_mut()).is_pending())
    );
    node.submit("tenant", "topic", send("retry", 8))?;
    for waiting in waits.iter_mut().take(2) {
        timeout(DEADLINE, waiting.as_mut()).await?;
    }
    assert!(poll(waits[2].as_mut()).is_pending());
    Ok(())
}

async fn ordinary_queue_and_dlq_notifications_remain_unchanged<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.submit(
        "tenant",
        "queue",
        CommandKind::CreateQueue {
            config: QueueConfig {
                default_time_to_live_millis: Some(1),
                dead_lettering_on_message_expiration: true,
                ..QueueConfig::default()
            },
        },
    )?;
    let namespace = NamespaceName::new("tenant")?;
    let queue = EntityPath::new("queue")?;
    let shadow = queue.dead_letter_queue()?;
    let handle = node.handle();
    let mut queue_wait = Box::pin(handle.deliverable(&namespace, &queue));
    let mut shadow_wait = Box::pin(handle.deliverable(&namespace, &shadow));
    node.submit("tenant", "queue", send("ordinary", 8))?;
    timeout(DEADLINE, queue_wait.as_mut()).await?;
    assert!(poll(shadow_wait.as_mut()).is_pending());
    let mut queue_wait = Box::pin(handle.deliverable(&namespace, &queue));
    node.clock.inner.set(1_002);
    assert!(matches!(
        node.submit("tenant", "queue", CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 1,
            ..
        }
    ));
    timeout(DEADLINE, shadow_wait.as_mut()).await?;
    assert!(poll(queue_wait.as_mut()).is_pending());
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE, super::$case(::testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE, super::$case(::testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

for_each_backend! {
    committed_fanout_wakes_only_destinations_without_postcommit_reads,
    duplicates_only_and_zero_subscriptions_publish_no_wakeups,
    atomic_refusal_and_failed_commit_wake_nobody,
    ordinary_queue_and_dlq_notifications_remain_unchanged,
}
