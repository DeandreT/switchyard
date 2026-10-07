//! Finite reservations stay behind the same serialized owner as messaging.

use std::{
    error::Error,
    fmt::Display,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use amqp::{
    ClientConnection, ClientReceiver, ClientSender, ClientSession, Message, Outcome, Properties,
    Symbol,
};

use domain::{
    BrokerError, CommandKind, CommandOutcome, DeleteEntityTarget, EntityPath, FiniteQueueCapacity,
    NamespaceName, QueueCapacityStatus, QueueConfig, ReceiveMode, StateMachine, Timestamp, keys,
};
use futures_util::FutureExt;
use protocol_amqp::{Attachment, EntityMetadata};
use server::{AdminTarget, Broker, Clock, LocalProposer, ManualClock, ProposeError, SubmitError};
use storage::{StateStore, WriteBatch};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);

async fn bounded<T, E: Display>(
    operation: &str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T> {
    tokio::time::timeout(DEADLINE, future)
        .await
        .map_err(|error| std::io::Error::other(format!("{operation}: {error}")))?
        .map_err(|error| std::io::Error::other(format!("{operation}: {error}")).into())
}

struct ListenerGuard(JoinHandle<()>);
impl ListenerGuard {
    async fn shutdown(&mut self) -> TestResult {
        self.0.abort();
        match tokio::time::timeout(DEADLINE, &mut self.0).await {
            Ok(Err(error)) if error.is_cancelled() => Ok(()),
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error.into()),
            Err(error) => Err(error.into()),
        }
    }
}
impl Drop for ListenerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone)]
struct ProbeClock {
    manual: ManualClock,
    reads: Arc<AtomicUsize>,
    forbidden: Arc<AtomicBool>,
}

impl Clock for ProbeClock {
    fn now(&self) -> Timestamp {
        assert!(
            !self.forbidden.load(Ordering::SeqCst),
            "read or stale fence consulted host clock"
        );
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.manual.now()
    }
}

async fn owner_capacity_operations_serialize_with_messaging_and_preserve_stale_fences<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let store = provider.open()?;
    let clock = ProbeClock {
        manual: ManualClock::at(1_000),
        reads: Arc::default(),
        forbidden: Arc::default(),
    };
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(store.clone()),
        clock.clone(),
    ));
    let handle = broker.handle();
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("orders")?;
    let view = handle.create_finite_queue_blocking(
        namespace.clone(),
        entity.clone(),
        QueueConfig::default(),
        FiniteQueueCapacity::new(522)?,
    )?;
    assert_eq!(
        view.capacity,
        QueueCapacityStatus::FiniteV1 {
            limit: FiniteQueueCapacity::new(522)?,
            reserved_bytes: 0,
            message_count: 0
        }
    );
    handle.submit_blocking(
        namespace.clone(),
        entity.clone(),
        CommandKind::Send {
            message_id: "one".into(),
            body: vec![1, 2],
            time_to_live_millis: None,
            session_id: None,
        },
    )?;
    let before = store.snapshot()?;
    clock.forbidden.store(true, Ordering::SeqCst);
    let read = tokio::time::timeout(
        DEADLINE,
        handle.describe_queue_capacity(namespace.clone(), entity.clone()),
    )
    .await??
    .unwrap();
    assert_eq!(
        read.capacity,
        QueueCapacityStatus::FiniteV1 {
            limit: FiniteQueueCapacity::new(522)?,
            reserved_bytes: 522,
            message_count: 1
        }
    );
    assert_eq!(store.snapshot()?, before);
    assert_eq!(
        handle.describe_queue_capacity_blocking(namespace.clone(), entity.clone())?,
        Some(read)
    );
    clock.forbidden.store(false, Ordering::SeqCst);
    assert_eq!(
        handle.submit_blocking(
            namespace.clone(),
            entity.clone(),
            CommandKind::Send {
                message_id: "two".into(),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
            }
        ),
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::QueueCapacityFull
        )))
    );
    assert_eq!(store.snapshot()?, before);
    tokio::time::timeout(
        DEADLINE,
        handle.set_queue_capacity_limit_fenced(
            view.binding.clone(),
            FiniteQueueCapacity::new(2_000)?,
        ),
    )
    .await??;
    let CommandOutcome::Received(Some(_)) = handle.submit_blocking(
        namespace.clone(),
        entity.clone(),
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("delivery")
    };
    handle.set_queue_capacity_limit_fenced_blocking(
        view.binding.clone(),
        FiniteQueueCapacity::new(522)?,
    )?;
    store.apply(
        WriteBatch::default().put(keys::queue_capacity_usage(&namespace, &entity), vec![255]),
    )?;
    handle.submit_fenced_blocking(
        view.binding.clone(),
        entity.clone(),
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )?;
    let recreated = tokio::time::timeout(
        DEADLINE,
        handle.create_finite_queue(
            namespace.clone(),
            entity.clone(),
            QueueConfig::default(),
            FiniteQueueCapacity::new(522)?,
        ),
    )
    .await??;
    assert_eq!(recreated.binding.generation(), 2);
    let before = store.snapshot()?;
    let reads = clock.reads.load(Ordering::SeqCst);
    clock.forbidden.store(true, Ordering::SeqCst);
    assert_eq!(
        handle.set_queue_capacity_limit_fenced_blocking(
            view.binding,
            FiniteQueueCapacity::new(1000)?
        ),
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::EntityBindingStale
        )))
    );
    assert_eq!(clock.reads.load(Ordering::SeqCst), reads);
    assert_eq!(store.snapshot()?, before);
    Ok(())
}

async fn shared_admin_and_amqp_metadata_refuse_missing_capacity_without_a_clock_read<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let store = provider.open()?;
    let clock = ProbeClock {
        manual: ManualClock::at(1_000),
        reads: Arc::default(),
        forbidden: Arc::default(),
    };
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(store.clone()),
        clock.clone(),
    ));
    let handle = broker.handle();
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("orders")?;
    handle.create_finite_queue_blocking(
        namespace.clone(),
        entity.clone(),
        QueueConfig::default(),
        FiniteQueueCapacity::new(522)?,
    )?;
    clock.forbidden.store(true, Ordering::SeqCst);
    assert_eq!(
        handle.admin_entity_metadata_blocking(
            namespace.clone(),
            AdminTarget::Primary(entity.clone())
        )?,
        Some(EntityMetadata::Queue(QueueConfig::default()))
    );
    assert!(
        handle
            .entity_metadata_blocking(namespace.clone(), Attachment::Queue(entity.clone()))?
            .is_some()
    );
    assert!(
        tokio::time::timeout(
            DEADLINE,
            handle.bind_admin(namespace.clone(), AdminTarget::Primary(entity.clone()))
        )
        .await??
        .is_some()
    );
    assert!(
        handle
            .bind_blocking(namespace.clone(), Attachment::Queue(entity.clone()))?
            .is_some()
    );
    store.apply(WriteBatch::default().delete(keys::queue_capacity_mode(&namespace, &entity)))?;
    let before = store.snapshot()?;
    let expected = Err(SubmitError::Propose(ProposeError::Broker(
        BrokerError::QueueCapacityCorrupt,
    )));
    assert_eq!(
        handle.admin_entity_metadata_blocking(
            namespace.clone(),
            AdminTarget::Primary(entity.clone())
        ),
        expected
    );
    assert_eq!(
        handle.entity_metadata_blocking(namespace.clone(), Attachment::Queue(entity.clone())),
        expected
    );
    assert_eq!(
        tokio::time::timeout(
            DEADLINE,
            handle.bind_admin(namespace.clone(), AdminTarget::Primary(entity.clone()))
        )
        .await?,
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::QueueCapacityCorrupt
        )))
    );
    assert_eq!(
        handle.bind_blocking(namespace, Attachment::Queue(entity)),
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::QueueCapacityCorrupt
        )))
    );
    assert_eq!(store.snapshot()?, before);
    Ok(())
}

async fn amqp_full_rejection_preserves_state_and_completion_restores_send_capacity<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let store = provider.open()?;
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(store.clone()),
        ManualClock::at(1_000),
    ));
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("orders")?;
    let handle = broker.handle();
    let created = handle.create_finite_queue_blocking(
        namespace.clone(),
        entity.clone(),
        QueueConfig::default(),
        FiniteQueueCapacity::new(4_096)?,
    )?;
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let listener_namespace = namespace.clone();
    let listener_handle = handle.clone();
    let mut listener = ListenerGuard(tokio::spawn(async move {
        let _ = protocol_amqp::AmqpListener::new(listener_handle, listener_namespace)
            .serve(socket)
            .await;
    }));
    let observed = AssertUnwindSafe(async {
        let mut connection = bounded(
            "connect",
            ClientConnection::builder()
                .container_id("capacity-client")
                .open(&format!("amqp://{address}")),
        )
        .await?;
        let mut session = bounded("begin", ClientSession::begin(&mut connection)).await?;
        let mut sender = bounded(
            "attach sender",
            ClientSender::attach(&mut session, "capacity-send", "orders"),
        )
        .await?;
        let message = |id: &str| {
            let mut message = Message::data(vec![1, 2]);
            message.properties = Some(Properties {
                message_id: Some(id.to_owned().into()),
                ..Properties::default()
            });
            message
        };
        assert!(matches!(
            bounded("send first", sender.send(message("one"))).await?,
            Outcome::Accepted(_)
        ));
        let view = handle
            .describe_queue_capacity_blocking(namespace.clone(), entity.clone())?
            .unwrap();
        let QueueCapacityStatus::FiniteV1 {
            reserved_bytes,
            message_count,
            ..
        } = view.capacity
        else {
            panic!("finite")
        };
        assert_eq!(message_count, 1);
        assert!(reserved_bytes > 0);
        handle.set_queue_capacity_limit_fenced_blocking(
            created.binding,
            FiniteQueueCapacity::new(reserved_bytes)?,
        )?;
        let before = store.snapshot()?;
        let Outcome::Rejected(rejected) =
            bounded("reject at full capacity", sender.send(message("two"))).await?
        else {
            panic!("capacity rejection")
        };
        assert_eq!(
            rejected
                .error
                .as_ref()
                .map(|error| error.condition.as_symbol()),
            Some(Symbol::from(protocol_amqp::RESOURCE_LIMIT_EXCEEDED))
        );
        assert_eq!(store.snapshot()?, before);
        let mut receiver = bounded(
            "attach receiver",
            ClientReceiver::attach(&mut session, "capacity-receive", "orders"),
        )
        .await?;
        let delivery = bounded("receive", receiver.recv()).await?;
        assert_eq!(
            handle
                .describe_queue_capacity_blocking(namespace.clone(), entity.clone())?
                .unwrap()
                .capacity,
            QueueCapacityStatus::FiniteV1 {
                limit: FiniteQueueCapacity::new(reserved_bytes)?,
                reserved_bytes,
                message_count: 1
            }
        );
        bounded("complete", receiver.accept(&delivery)).await?;
        bounded("wait for credit refund", async {
            loop {
                let Some(view) = handle
                    .describe_queue_capacity(namespace.clone(), entity.clone())
                    .await?
                else {
                    panic!("queue")
                };
                if matches!(
                    view.capacity,
                    QueueCapacityStatus::FiniteV1 {
                        reserved_bytes: 0,
                        message_count: 0,
                        ..
                    }
                ) {
                    return Ok::<(), SubmitError>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(matches!(
            bounded("send after completion", sender.send(message("two"))).await?,
            Outcome::Accepted(_)
        ));
        bounded("close sender", sender.close()).await?;
        bounded("close receiver", receiver.close()).await?;
        bounded("end", session.end()).await?;
        bounded("close connection", connection.close()).await?;
        Ok::<(), Box<dyn Error>>(())
    })
    .catch_unwind()
    .await;
    let cleanup = listener.shutdown().await;
    match observed {
        Ok(Ok(())) => cleanup,
        Ok(Err(error)) => Err(error),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread")] async fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()).await })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread")] async fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?).await })+ }
    };
}

for_each_backend! {
    owner_capacity_operations_serialize_with_messaging_and_preserve_stale_fences,
    shared_admin_and_amqp_metadata_refuse_missing_capacity_without_a_clock_read,
    amqp_full_rejection_preserves_state_and_completion_restores_send_capacity,
}

#[path = "queue_capacity/definition.rs"]
mod definition;

#[path = "queue_capacity/atom_owner.rs"]
mod atom_owner;
#[path = "queue_capacity/atom_subscriptions.rs"]
mod atom_subscriptions;
