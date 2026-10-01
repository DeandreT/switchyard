use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::Poll,
    time::Duration,
};

use domain::{
    DeleteEntityTarget, QueueConfig, StateMachine, SubscriptionConfig, SubscriptionName,
    TopicConfig,
};
use protocol_amqp::Broker as _;
use storage::{Key, MemoryStore, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};

use super::*;
use crate::ManualClock;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn ready(wait: impl Future<Output = ()>) {
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .expect("committed deletion wakes a registered receiver");
}

async fn pending<F: Future<Output = ()>>(wait: &mut Pin<Box<F>>) {
    poll_fn(|context| {
        assert!(wait.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
}

fn names() -> (NamespaceName, EntityPath) {
    (
        NamespaceName::new("tenant").expect("namespace"),
        EntityPath::new("Orders").expect("entity"),
    )
}

#[tokio::test]
async fn deleting_a_queue_wakes_its_receivers_and_shadow_only() -> TestResult {
    let (namespace, entity) = names();
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(MemoryStore::default()),
        ManualClock::at(1_000),
    ));
    let handle = broker.handle();
    handle.submit_blocking(
        namespace.clone(),
        entity.clone(),
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let shadow = entity.dead_letter_queue()?;
    let waiting = (0..4)
        .map(|_| handle.deliverable(&namespace, &entity))
        .collect::<Vec<_>>();
    let shadow_wait = handle.deliverable(&namespace, &shadow);
    let other_namespace = NamespaceName::new("tenant-old")?;
    let mut foreign_wait = Box::pin(handle.deliverable(&other_namespace, &entity));
    let case_entity = EntityPath::new("orders")?;
    let mut case_wait = Box::pin(handle.deliverable(&namespace, &case_entity));
    assert_eq!(
        handle
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Queue,
                }
            )
            .await?,
        CommandOutcome::QueueDeleted
    );
    for wait in waiting {
        ready(wait).await;
    }
    ready(shadow_wait).await;
    pending(&mut foreign_wait).await;
    pending(&mut case_wait).await;
    assert_eq!(
        handle
            .queue_config(namespace.clone(), entity.clone())
            .await?,
        None
    );
    let mut late = Box::pin(handle.deliverable(&namespace, &entity));
    pending(&mut late).await;
    drop(late);
    drop(case_wait);
    drop(foreign_wait);
    assert_eq!(handle.watchers.entry_count(), 0);
    Ok(())
}

#[tokio::test]
async fn targeted_subscription_deletion_and_topic_cascade_have_exact_wakeups() -> TestResult {
    let (namespace, entity) = names();
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(MemoryStore::default()),
        ManualClock::at(1_000),
    ));
    let handle = broker.handle();
    handle.submit_blocking(
        namespace.clone(),
        entity.clone(),
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let first = SubscriptionName::new("Accounting")?;
    let second = SubscriptionName::new("accounting")?;
    for name in [&first, &second] {
        handle.submit_blocking(
            namespace.clone(),
            entity.clone(),
            CommandKind::CreateSubscription {
                name: name.clone(),
                config: SubscriptionConfig::default(),
            },
        )?;
    }
    let child = entity.subscription(&first)?;
    let sibling = entity.subscription(&second)?;
    let child_shadow = child.dead_letter_queue()?;
    let sibling_shadow = sibling.dead_letter_queue()?;
    let child_wait = handle.deliverable(&namespace, &child);
    let child_shadow_wait = handle.deliverable(&namespace, &child_shadow);
    let mut parent_wait = Box::pin(handle.deliverable(&namespace, &entity));
    let mut sibling_wait = Box::pin(handle.deliverable(&namespace, &sibling));
    let mut sibling_shadow_wait = Box::pin(handle.deliverable(&namespace, &sibling_shadow));
    assert_eq!(
        handle
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Subscription { name: first },
                }
            )
            .await?,
        CommandOutcome::SubscriptionDeleted
    );
    ready(child_wait).await;
    ready(child_shadow_wait).await;
    pending(&mut parent_wait).await;
    pending(&mut sibling_wait).await;
    pending(&mut sibling_shadow_wait).await;
    let mut removed_wait = Box::pin(handle.deliverable(&namespace, &child));
    assert_eq!(
        handle
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Auto,
                }
            )
            .await?,
        CommandOutcome::TopicDeleted
    );
    ready(parent_wait).await;
    ready(sibling_wait).await;
    ready(sibling_shadow_wait).await;
    pending(&mut removed_wait).await;
    drop(removed_wait);
    assert_eq!(handle.watchers.entry_count(), 0);
    Ok(())
}

#[derive(Clone, Default)]
struct FailingStore {
    inner: MemoryStore,
    fail_next: Arc<AtomicBool>,
}

impl StateStore for FailingStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected deletion failure".into(),
            });
        }
        self.inner.apply(batch)
    }
}

#[tokio::test]
async fn refused_and_failed_deletions_never_wake_receivers() -> TestResult {
    let (namespace, entity) = names();
    let store = FailingStore::default();
    let clock = ManualClock::at(1_000);
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(store.clone()),
        clock.clone(),
    ));
    let handle = broker.handle();
    handle.submit_blocking(
        namespace.clone(),
        entity.clone(),
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let before = store.snapshot()?;
    let mut waiting = Box::pin(handle.deliverable(&namespace, &entity));
    let shadow = entity.dead_letter_queue()?;
    let mut shadow_wait = Box::pin(handle.deliverable(&namespace, &shadow));
    for target in [
        DeleteEntityTarget::Topic,
        DeleteEntityTarget::Subscription {
            name: SubscriptionName::new("Missing")?,
        },
    ] {
        assert!(
            handle
                .submit(
                    namespace.clone(),
                    entity.clone(),
                    CommandKind::DeleteEntity { target }
                )
                .await
                .is_err()
        );
        assert_eq!(store.snapshot()?, before);
        pending(&mut waiting).await;
        pending(&mut shadow_wait).await;
    }
    clock.set(2_000);
    store.fail_next.store(true, Ordering::SeqCst);
    assert!(
        handle
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Queue,
                }
            )
            .await
            .is_err()
    );
    assert_eq!(store.snapshot()?, before);
    assert_eq!(handle.last_applied_blocking()?.as_millis(), 1_000);
    pending(&mut waiting).await;
    pending(&mut shadow_wait).await;
    handle
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Auto,
            },
        )
        .await?;
    ready(waiting).await;
    ready(shadow_wait).await;
    assert_eq!(handle.last_applied_blocking()?.as_millis(), 2_000);
    Ok(())
}
