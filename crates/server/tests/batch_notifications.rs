//! Atomic ingress wakes waiting receivers only after a successful commit.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use domain::{
    BrokerError, CommandKind, CommandOutcome, EntityPath, IngressEnvelope, MessageBody,
    MessageEnvelope, MessageIdentifier, MessageProperties, NamespaceName, QueueConfig,
    SequenceNumber, StateMachine,
};
use protocol_amqp::Broker as _;
use server::{Broker, LocalProposer, ManualClock, ProposeError, SubmitError};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;
use tokio::time::timeout;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const NO_WAKE: Duration = Duration::from_millis(30);
const WAKE: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
struct FailingStore<S> {
    inner: S,
    fail_next: Arc<AtomicBool>,
}

impl<S: StateStore> StateStore for FailingStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected failure".to_owned(),
            });
        }
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
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
}

struct Node<P: StoreProvider> {
    broker: Broker,
    store: P::Store,
    clock: ManualClock,
    namespace: NamespaceName,
    entity: EntityPath,
    fail_next: Arc<AtomicBool>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    async fn new(provider: P) -> TestResult<Self> {
        let store = provider.open()?;
        let fail_next = Arc::new(AtomicBool::new(false));
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(FailingStore {
                inner: store.clone(),
                fail_next: fail_next.clone(),
            }),
            clock.clone(),
        ));
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        for path in [entity.clone(), EntityPath::new("neighbor")?] {
            broker
                .handle()
                .submit(
                    namespace.clone(),
                    path,
                    CommandKind::CreateQueue {
                        config: QueueConfig::default(),
                    },
                )
                .await?;
        }
        Ok(Self {
            broker,
            store,
            clock,
            namespace,
            entity,
            fail_next,
            _provider: provider,
        })
    }

    async fn send(&self, messages: Vec<IngressEnvelope>) -> Result<CommandOutcome, SubmitError> {
        self.broker
            .handle()
            .submit(
                self.namespace.clone(),
                self.entity.clone(),
                CommandKind::SendBatch { messages },
            )
            .await
    }

    async fn retained(&self) -> TestResult<Vec<domain::Delivery>> {
        let CommandOutcome::Peeked(messages) = self
            .broker
            .handle()
            .submit(
                self.namespace.clone(),
                self.entity.clone(),
                CommandKind::Peek {
                    from_sequence: SequenceNumber::new(0),
                    max_messages: 10,
                    session_id: None,
                },
            )
            .await?
        else {
            panic!("peek outcome")
        };
        Ok(messages)
    }
}

fn message(id: &str) -> IngressEnvelope {
    IngressEnvelope {
        message_id: id.to_owned(),
        body: id.as_bytes().to_vec(),
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_time: None,
        envelope: MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String(id.to_owned())),
                ..MessageProperties::default()
            },
            body: MessageBody::Data(vec![id.as_bytes().to_vec()]),
            ..MessageEnvelope::default()
        },
    }
}

async fn committed_batch<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider).await?;
    let handle = node.broker.handle();
    let neighbor_entity = EntityPath::new("neighbor")?;
    let foreign_namespace = NamespaceName::new("foreign")?;
    let ready = handle.deliverable(&node.namespace, &node.entity);
    let neighbor = handle.deliverable(&node.namespace, &neighbor_entity);
    let foreign = handle.deliverable(&foreign_namespace, &node.entity);
    tokio::pin!(ready, neighbor, foreign);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    assert_eq!(
        node.send(vec![message("first"), message("second")]).await?,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)],
        }
    );
    timeout(WAKE, &mut ready).await?;
    let retained = node.retained().await?;
    assert_eq!(
        retained
            .iter()
            .map(|message| message.message_id.as_str())
            .collect::<Vec<_>>(),
        vec!["first", "second"]
    );
    assert!(timeout(NO_WAKE, &mut neighbor).await.is_err());
    assert!(timeout(NO_WAKE, &mut foreign).await.is_err());
    Ok(())
}

async fn no_op_and_refused_batch<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider).await?;
    let handle = node.broker.handle();
    let ready = handle.deliverable(&node.namespace, &node.entity);
    tokio::pin!(ready);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    let before = node.store.snapshot()?;
    node.clock.set(2_000);
    assert_eq!(
        node.send(Vec::new()).await?,
        CommandOutcome::BatchSent {
            sequences: Vec::new()
        }
    );
    assert_eq!(node.store.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    let error = node
        .send(vec![message("valid"), message(&"x".repeat(129))])
        .await
        .expect_err("invalid final member rejects the batch");
    assert!(matches!(
        error,
        SubmitError::Propose(ProposeError::Broker(BrokerError::MessageIdTooLong { .. }))
    ));
    assert_eq!(node.store.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    assert!(node.retained().await?.is_empty());
    assert_eq!(
        node.send(vec![message("retry")]).await?,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(1)]
        }
    );
    timeout(WAKE, &mut ready).await?;
    assert_eq!(node.retained().await?.len(), 1);
    Ok(())
}

async fn failed_commit_and_retry<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider).await?;
    let handle = node.broker.handle();
    let ready = handle.deliverable(&node.namespace, &node.entity);
    tokio::pin!(ready);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    let before = node.store.snapshot()?;
    node.clock.set(2_000);
    node.fail_next.store(true, Ordering::Relaxed);
    let error = node
        .send(vec![message("first"), message("second")])
        .await
        .expect_err("the commit fails");
    assert!(matches!(
        error,
        SubmitError::Propose(ProposeError::Broker(BrokerError::Storage(_)))
    ));
    assert_eq!(node.store.snapshot()?, before);
    assert!(timeout(NO_WAKE, &mut ready).await.is_err());
    assert_eq!(
        node.send(vec![message("first"), message("second")]).await?,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)],
        }
    );
    timeout(WAKE, &mut ready).await?;
    assert_eq!(node.retained().await?.len(), 2);
    Ok(())
}

macro_rules! suite {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;
            #[tokio::test]
            async fn a_committed_batch_wakes_only_its_entity() -> TestResult {
                committed_batch($provider).await
            }
            #[tokio::test]
            async fn empty_and_refused_batches_do_not_wake_a_receiver() -> TestResult {
                no_op_and_refused_batch($provider).await
            }
            #[tokio::test]
            async fn a_failed_commit_does_not_wake_and_a_retry_can_commit() -> TestResult {
                failed_commit_and_retry($provider).await
            }
        }
    };
}

suite!(memory, testkit::MemoryProvider::new());
suite!(durable, testkit::DurableProvider::temporary()?);
