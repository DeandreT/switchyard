#![cfg(feature = "capacity-conservation-test-support")]

#[path = "../../domain/tests/support/capacity_oracle.rs"]
mod oracle;
#[path = "../src/test_deadline.rs"]
mod test_deadline;

use amqp::{
    ApplicationProperties, Array, Body, ClientConnection as Connection, ClientDelivery,
    ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session, Message,
    Modified, OrderedMap, Outcome, Properties, SenderSettleMode, Value,
};
use domain::{
    BrokerError, Command, CommandKind as C, EntityPath, MessageState, NamespaceName, QueueCapacity,
    QueueConfig, StateMachine, Timestamp,
    capacity_conservation::{CapacityHarness, TestGenesis},
};
use protocol_amqp::{AmqpListener, AmqpListenerExit, AmqpListenerRetirement};
use server::{Broker, CapacityTestProposer, LocalProposer, ManualClock};
use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use storage::{
    Key, Mutation, StateStore, StorageError, StoreSnapshot, Value as StoredValue, WriteBatch,
};
use test_deadline::WAIT;
use testkit::{DurableProvider, MemoryProvider, StoreProvider};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};
type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

#[derive(Default)]
struct Control {
    histories: Mutex<Vec<StoreSnapshot>>,
    changed: Notify,
    complete: AtomicUsize,
    fail_at: AtomicUsize,
    mode: AtomicUsize,
}
#[derive(Clone)]
struct Observed<S> {
    inner: S,
    control: Arc<Control>,
    genesis: TestGenesis,
}
impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, k: &[u8]) -> std::result::Result<Option<StoredValue>, StorageError> {
        self.inner.get(k)
    }
    fn scan_from(
        &self,
        p: &[u8],
        s: &[u8],
        l: usize,
    ) -> std::result::Result<Vec<(Key, StoredValue)>, StorageError> {
        self.inner.scan_from(p, s, l)
    }
    fn snapshot(&self) -> std::result::Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, b: WriteBatch) -> std::result::Result<(), StorageError> {
        let deletes_lock = b
            .mutations()
            .iter()
            .any(|m| matches!(m,Mutation::Delete{key}if key.starts_with(&[0x13,5])));
        let deletes_message = b
            .mutations()
            .iter()
            .any(|m| matches!(m,Mutation::Delete{key}if key.starts_with(&[0x13,3])));
        let writes_message = b
            .mutations()
            .iter()
            .any(|m| matches!(m,Mutation::Put{key,..}if key.starts_with(&[0x13,3])));
        let mut failure = 0;
        if deletes_lock && deletes_message && !writes_message {
            let number = self.control.complete.fetch_add(1, Ordering::SeqCst) + 1;
            if number == self.control.fail_at.load(Ordering::SeqCst) {
                failure = self.control.mode.load(Ordering::SeqCst);
            }
        }
        if failure == 1 {
            return Err(injected());
        }
        self.inner.apply(b)?;
        let snapshot = self.inner.snapshot()?;
        oracle::observe(snapshot.entries(), self.genesis.owner());
        self.control.histories.lock().unwrap().push(snapshot);
        self.control.changed.notify_one();
        if failure == 2 {
            Err(injected())
        } else {
            Ok(())
        }
    }
}
fn injected() -> StorageError {
    StorageError::Backend {
        operation: "staged Complete fixture",
        detail: "injected".into(),
    }
}

struct Node<P: StoreProvider> {
    provider: P,
    genesis: TestGenesis,
    store: Option<Observed<P::Store>>,
    control: Arc<Control>,
    broker: Option<Broker>,
    listener: Option<JoinHandle<std::result::Result<(), String>>>,
    retirement: Option<AmqpListenerRetirement>,
    clock: ManualClock,
    address: String,
}
impl<P: StoreProvider> Node<P> {
    async fn new(provider: P) -> Result<Self> {
        let genesis = TestGenesis::new(
            ns(),
            EntityPath::new("orders")?,
            QueueConfig {
                capacity: QueueCapacity::Finite {
                    max_size_bytes: 1024 * 1024,
                },
                max_delivery_count: 10,
                ..QueueConfig::default()
            },
        )?;
        let store = provider.open()?;
        genesis.initialize(&store)?;
        drop(store);
        let mut node = Self {
            provider,
            genesis,
            store: None,
            control: Arc::new(Control::default()),
            broker: None,
            listener: None,
            retirement: None,
            clock: ManualClock::at(1000),
            address: String::new(),
        };
        node.start().await?;
        Ok(node)
    }
    async fn start(&mut self) -> Result {
        let store = Observed {
            inner: self.provider.open()?,
            control: self.control.clone(),
            genesis: self.genesis.clone(),
        };
        let harness = CapacityHarness::open(store.clone(), self.genesis.clone())?;
        let broker =
            Broker::spawn_capacity_test(CapacityTestProposer::new(harness, self.clock.clone()));
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        self.address = format!("amqp://{}", socket.local_addr()?);
        let mut service = AmqpListener::new(broker.handle(), ns()).into_tcp_service(socket);
        self.retirement = Some(service.retirement_handle());
        self.listener = Some(tokio::spawn(async move {
            service.serve().await;
            service.finish().await;
            match service.take_exit() {
                Some(AmqpListenerExit::Complete(Ok(()))) => Ok(()),
                Some(AmqpListenerExit::Complete(Err(e))) => Err(e.to_string()),
                _ => Err("original listener did not drain".into()),
            }
        }));
        self.broker = Some(broker);
        self.store = Some(store);
        Ok(())
    }
    async fn stop(&mut self) -> Result {
        self.retirement.as_ref().unwrap().request();
        // Borrow the exact original handle through its join. Deadline failure is
        // not drain/reopen permission, and no replacement owner starts here.
        if let Some(listener) = self.listener.as_mut() {
            tokio::time::timeout(WAIT, &mut *listener).await???;
        }
        self.listener = None;
        self.retirement = None;
        drop(self.broker.take());
        drop(self.store.take());
        Ok(())
    }
    async fn reopen(&mut self) -> Result {
        let before = self.snapshot()?;
        self.stop().await?;
        {
            let store = self.provider.open()?;
            assert_eq!(store.snapshot()?, before);
            oracle::observe(before.entries(), self.genesis.owner());
        }
        self.start().await?;
        Ok(())
    }
    fn snapshot(&self) -> Result<StoreSnapshot> {
        Ok(self.store.as_ref().unwrap().snapshot()?)
    }
    fn observation(&self) -> oracle::Observation {
        oracle::observe(self.snapshot().unwrap().entries(), self.genesis.owner())
    }
    async fn wait(&self, predicate: impl Fn(&oracle::Observation) -> bool) -> Result {
        tokio::time::timeout(WAIT, async {
            loop {
                let notified = self.control.changed.notified();
                if predicate(&self.observation()) {
                    return;
                }
                notified.await;
            }
        })
        .await?;
        Ok(())
    }
    async fn connect(&self) -> Result<(Connection, Session)> {
        let mut c = Connection::builder()
            .container_id("owned-staged-client")
            .open(&self.address)
            .await?;
        let s = Session::begin(&mut c).await?;
        Ok((c, s))
    }
    async fn expire(&self) -> Result {
        self.clock.advance(60_001);
        self.broker
            .as_ref()
            .unwrap()
            .handle()
            .submit(ns(), EntityPath::new("orders")?, C::ExpireLocks)
            .await?;
        self.observation();
        Ok(())
    }
}
fn ns() -> NamespaceName {
    NamespaceName::new("tenant").unwrap()
}
async fn send(session: &mut Session, count: usize) -> Result {
    let mut sender = Sender::attach(session, "twins", "orders").await?;
    for _ in 0..count {
        assert!(matches!(
            sender
                .send(
                    Message::builder()
                        .body(Body::Data(vec![vec![b'x'; 900].into()]))
                        .build()
                )
                .await?,
            Outcome::Accepted(_)
        ));
    }
    sender.close().await?;
    Ok(())
}
async fn receiver(
    session: &mut Session,
    name: &str,
    mode: SenderSettleMode,
    size: Option<u64>,
) -> Result<Receiver> {
    Ok(Receiver::builder()
        .name(name)
        .source("orders")
        .sender_settle_mode(mode)
        .max_message_size(size)
        .attach(session)
        .await?)
}
async fn recv(receiver: &mut Receiver) -> Result<ClientDelivery> {
    Ok(tokio::time::timeout(WAIT, receiver.recv()).await??)
}
async fn close(connection: Connection) -> Result {
    connection.close().await?;
    Ok(())
}

async fn ordinary<P: StoreProvider>(provider: P) -> Result {
    eprintln!("staged ordinary: start");
    let mut node = Node::new(provider).await?;
    let (connection, mut session) = node.connect().await?;
    send(&mut session, 1).await?;
    let mut unlimited = receiver(&mut session, "measure", SenderSettleMode::Settled, None).await?;
    let size = recv(&mut unlimited).await?.encoded_message().len() as u64;
    eprintln!("staged ordinary: measured initial transfer {size}");
    unlimited.close().await?;
    node.wait(|o| o.messages.is_empty()).await?;
    send(&mut session, 1).await?;
    let ready = node.observation();
    let mut short = receiver(
        &mut session,
        "too-small",
        SenderSettleMode::Settled,
        Some(size - 1),
    )
    .await?;
    assert!(matches!(
        tokio::time::timeout(WAIT, short.recv()).await?,
        Err(amqp::EngineError::RemoteDetached)
    ));
    assert_eq!(
        short.detach_error().unwrap().condition.as_symbol().as_str(),
        protocol_amqp::MESSAGE_SIZE_EXCEEDED
    );
    node.wait(|o| {
        o.messages
            .values()
            .all(|(_, r)| matches!(r.state, MessageState::Locked { .. }))
    })
    .await?;
    let locked = node.observation();
    eprintln!("staged ordinary: refused original remains locked");
    assert_eq!(locked.usage.message_count(), 1);
    assert_eq!(
        locked.usage.committed_bytes()?,
        ready.usage.committed_bytes()?
    );
    drop(short);
    drop(session);
    close(connection).await?;
    node.reopen().await?;
    assert_eq!(node.observation().usage, locked.usage);
    node.expire().await?;
    eprintln!("staged ordinary: reopened original lock and returned it");
    assert_eq!(node.observation().usage.message_count(), 1);
    assert!(
        node.observation()
            .messages
            .values()
            .all(|(_, r)| r.state == MessageState::Ready)
    );
    let (connection, mut session) = node.connect().await?;
    node.control.histories.lock().unwrap().clear();
    // Redelivery changes the AMQP header's encoded delivery count. Measure
    // the actual next lock-free rendered candidate, not the first transfer's
    // size (the zero-to-one integer boundary is not fixed-width on AMQP).
    let next = node.observation().messages.into_values().next().unwrap().1;
    let rendered = protocol_amqp::write_delivery(&domain::Delivery {
        sequence: next.sequence,
        message_id: next.message_id,
        body: next.body,
        enqueued_at: next.enqueued_at,
        expires_at: next.expires_at,
        scheduled_enqueue_at: next.scheduled_enqueue_at,
        delivery_count: next.delivery_count + 1,
        origin: domain::DeliveryOrigin::Ready,
        lock: None,
        session_id: next.session_id,
        dead_letter: next.dead_letter,
        envelope: next.envelope,
    })?;
    let size = amqp::encode_message(&rendered)?.len() as u64;
    let mut exact = receiver(&mut session, "exact", SenderSettleMode::Settled, Some(size)).await?;
    assert_eq!(recv(&mut exact).await?.encoded_message().len() as u64, size);
    node.wait(|o| o.messages.is_empty()).await?;
    let history = node.control.histories.lock().unwrap().clone();
    assert_eq!(
        history.len(),
        2,
        "real staged PeekLock then Complete commits"
    );
    assert_eq!(
        oracle::observe(history[0].entries(), node.genesis.owner())
            .usage
            .message_count(),
        1
    );
    assert_eq!(
        oracle::observe(history[1].entries(), node.genesis.owner())
            .usage
            .message_count(),
        0
    );
    exact.close().await?;
    drop(session);
    close(connection).await?;
    node.reopen().await?;
    node.stop().await?;
    eprintln!("staged ordinary: joined all originals");
    Ok(())
}

struct Management {
    requests: Sender,
    id: u32,
    last_count: usize,
}
impl Management {
    async fn attach(session: &mut Session) -> Result<Self> {
        Ok(Self {
            requests: Sender::attach(session, "requests", "orders/$management").await?,
            id: 0,
            last_count: 0,
        })
    }
    async fn replies(session: &mut Session, name: &str, size: Option<u64>) -> Result<Receiver> {
        Ok(Receiver::builder()
            .name(format!("{name}-link"))
            .source("orders/$management")
            .target(name)
            .max_message_size(size)
            .attach(session)
            .await?)
    }
    async fn request(
        &mut self,
        replies: &mut Receiver,
        name: &str,
        sequences: &[i64],
    ) -> Result<(i32, u64)> {
        self.id += 1;
        let mut body = OrderedMap::new();
        body.insert(
            Value::String("sequence-numbers".into()),
            Value::Array(Array::from(
                sequences
                    .iter()
                    .map(|s| Value::Long(*s))
                    .collect::<Vec<_>>(),
            )),
        );
        body.insert(Value::String("receiver-settle-mode".into()), Value::Uint(0));
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(format!("request-{:04}", self.id).into()),
                reply_to: Some(name.into()),
                ..Properties::default()
            })
            .application_properties(
                ApplicationProperties::builder()
                    .insert(
                        protocol_amqp::OPERATION_PROPERTY,
                        protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
                    )
                    .build(),
            )
            .body(Body::Value(Value::Map(body)))
            .build();
        assert!(matches!(
            self.requests.send(request).await?,
            Outcome::Accepted(_)
        ));
        let response = recv(replies).await?;
        let size = response.encoded_message().len() as u64;
        self.last_count = match &response.message().body {
            Body::Value(Value::Map(body)) => match body.get(&Value::String("messages".into())) {
                Some(Value::List(entries)) => entries.len(),
                _ => 0,
            },
            _ => 0,
        };
        let status = match response
            .message()
            .application_properties
            .as_ref()
            .and_then(|p| p.get(protocol_amqp::STATUS_CODE_PROPERTY))
        {
            Some(Value::Int(n)) => *n,
            other => panic!("{other:?}"),
        };
        replies.accept(&response).await?;
        Ok((status, size))
    }
}
async fn defer<P: StoreProvider>(
    node: &Node<P>,
    session: &mut Session,
    count: usize,
) -> Result<Vec<i64>> {
    send(session, count).await?;
    let mut receiver = receiver(session, "deferrer", SenderSettleMode::Unsettled, None).await?;
    for _ in 0..count {
        let delivery = recv(&mut receiver).await?;
        receiver
            .modify(
                &delivery,
                Modified {
                    undeliverable_here: Some(true),
                    ..Modified::default()
                },
            )
            .await?;
    }
    receiver.close().await?;
    node.wait(|o| {
        o.messages
            .values()
            .all(|(_, r)| r.state == MessageState::Deferred)
    })
    .await?;
    Ok(node
        .observation()
        .messages
        .keys()
        .map(|s| s.as_u64() as i64)
        .collect())
}
async fn deferred<P: StoreProvider>(provider: P) -> Result {
    let mut node = Node::new(provider).await?;
    let (connection, mut session) = node.connect().await?;
    let sequences = defer(&node, &mut session, 4).await?;
    let mut management = Management::attach(&mut session).await?;
    let mut unlimited = Management::replies(&mut session, "unlimited", None).await?;
    let (status, size) = management
        .request(&mut unlimited, "unlimited", &sequences[..2])
        .await?;
    assert_eq!(status, 200);
    node.wait(|o| o.messages.len() == 2).await?;
    let before = node.observation();
    let mut short = Management::replies(&mut session, "short", Some(size - 1)).await?;
    assert_eq!(
        management
            .request(&mut short, "short", &sequences[2..])
            .await?
            .0,
        403
    );
    node.wait(|o| {
        o.messages
            .values()
            .all(|(_, r)| matches!(r.state, MessageState::Locked { .. }))
    })
    .await?;
    assert_eq!(
        node.observation().usage.committed_bytes()?,
        before.usage.committed_bytes()?
    );
    drop(management);
    drop(unlimited);
    drop(short);
    drop(session);
    close(connection).await?;
    let locked = node.observation().usage;
    node.reopen().await?;
    assert_eq!(node.observation().usage, locked);
    node.expire().await?;
    assert!(
        node.observation()
            .messages
            .values()
            .all(|(_, r)| r.state == MessageState::Deferred)
    );
    let (connection, mut session) = node.connect().await?;
    let mut management = Management::attach(&mut session).await?;
    let mut exact = Management::replies(&mut session, "exact", Some(size)).await?;
    node.control.histories.lock().unwrap().clear();
    assert_eq!(
        management
            .request(&mut exact, "exact", &sequences[2..])
            .await?,
        (200, size)
    );
    node.wait(|o| o.messages.is_empty()).await?;
    let counts = node
        .control
        .histories
        .lock()
        .unwrap()
        .iter()
        .map(|s| {
            oracle::observe(s.entries(), node.genesis.owner())
                .usage
                .message_count()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        counts,
        vec![2, 1, 0],
        "one real batched deferred lock, then per-message Complete"
    );
    drop(management);
    drop(exact);
    drop(session);
    close(connection).await?;
    node.reopen().await?;
    node.stop().await?;
    Ok(())
}

async fn complete_failure<P: StoreProvider>(provider: P) -> Result {
    let mut node = Node::new(provider).await?;
    for mode in [1, 2] {
        let (connection, mut session) = node.connect().await?;
        send(&mut session, 1).await?;
        node.control.complete.store(0, Ordering::SeqCst);
        node.control.fail_at.store(1, Ordering::SeqCst);
        node.control.mode.store(mode, Ordering::SeqCst);
        let mut receiver = receiver(
            &mut session,
            "failed-complete",
            SenderSettleMode::Settled,
            Some(65536),
        )
        .await?;
        assert!(tokio::time::timeout(WAIT, receiver.recv()).await?.is_err());
        let observed = node.observation();
        assert_eq!(observed.messages.len(), if mode == 1 { 1 } else { 0 });
        drop(receiver);
        drop(session);
        let _ = connection.close().await;
        node.control.fail_at.store(0, Ordering::SeqCst);
        node.reopen().await?;
        assert_eq!(node.observation().usage, observed.usage);
        if mode == 1 {
            let (sequence, (_, record)) = node.observation().messages.into_iter().next().unwrap();
            let MessageState::Locked { token, .. } = record.state else {
                panic!()
            };
            // An actual corrected Complete after reopen refunds exactly once.
            let binding = node
                .broker
                .as_ref()
                .unwrap()
                .handle()
                .bind_entity(ns(), EntityPath::new("orders")?)
                .await?;
            node.broker
                .as_ref()
                .unwrap()
                .handle()
                .submit_bound(
                    binding.clone(),
                    C::Complete {
                        sequence,
                        lock_token: token,
                    },
                )
                .await?;
            let before = node.snapshot()?;
            assert!(
                node.broker
                    .as_ref()
                    .unwrap()
                    .handle()
                    .submit_bound(
                        binding,
                        C::Complete {
                            sequence,
                            lock_token: token
                        }
                    )
                    .await
                    .is_err()
            );
            assert_eq!(node.snapshot()?, before);
            assert!(node.observation().messages.is_empty());
        }
    }
    node.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_staged_ordinary() -> Result {
    ordinary(MemoryProvider::new()).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_staged_ordinary() -> Result {
    ordinary(DurableProvider::temporary()?).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_staged_deferred() -> Result {
    deferred(MemoryProvider::new()).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_staged_deferred() -> Result {
    deferred(DurableProvider::temporary()?).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_failed_complete() -> Result {
    complete_failure(MemoryProvider::new()).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_failed_complete() -> Result {
    complete_failure(DurableProvider::temporary()?).await
}

async fn deferred_partial_failure<P: StoreProvider>(provider: P) -> Result {
    let mut node = Node::new(provider).await?;
    let (connection, mut session) = node.connect().await?;
    let sequences = defer(&node, &mut session, 2).await?;
    let before = node.observation();
    node.control.complete.store(0, Ordering::SeqCst);
    node.control.fail_at.store(2, Ordering::SeqCst);
    node.control.mode.store(1, Ordering::SeqCst);
    let mut management = Management::attach(&mut session).await?;
    let mut replies = Management::replies(&mut session, "partial", Some(65_536)).await?;
    let (status, _) = management
        .request(&mut replies, "partial", &sequences)
        .await?;
    // Existing management semantics return only the successfully deleted
    // prefix as a 200 response; the failed copy keeps its original lock.
    assert_eq!((status, management.last_count), (200, 1));
    assert_eq!(node.control.complete.load(Ordering::SeqCst), 2);
    let after = node.observation();
    assert_eq!(after.usage.message_count(), 1);
    assert!(after.usage.committed_bytes()? < before.usage.committed_bytes()?);
    assert_eq!(
        after.messages.keys().next().unwrap().as_u64() as i64,
        sequences[1]
    );
    assert!(matches!(
        after.messages.values().next().unwrap().1.state,
        MessageState::Locked {
            origin: domain::DeliveryOrigin::Deferred,
            ..
        }
    ));
    drop(management);
    drop(replies);
    drop(session);
    let _ = connection.close().await;
    node.control.fail_at.store(0, Ordering::SeqCst);
    node.reopen().await?;
    assert_eq!(node.observation().usage, after.usage);
    node.expire().await?;
    assert_eq!(
        node.observation().messages.values().next().unwrap().1.state,
        MessageState::Deferred
    );
    node.stop().await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_deferred_partial_complete_failure() -> Result {
    deferred_partial_failure(MemoryProvider::new()).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_deferred_partial_complete_failure() -> Result {
    deferred_partial_failure(DurableProvider::temporary()?).await
}

#[test]
fn ordinary_constructors_remain_fenced_with_support_compiled() -> Result {
    let store = storage::MemoryStore::default();
    let genesis = TestGenesis::new(
        ns(),
        EntityPath::new("orders")?,
        QueueConfig {
            capacity: QueueCapacity::Finite {
                max_size_bytes: 1024 * 1024,
            },
            ..QueueConfig::default()
        },
    )?;
    genesis.initialize(&store)?;
    let before = store.snapshot()?;
    let clock = ManualClock::at(1000);
    let proposer = LocalProposer::new(StateMachine::new(store.clone()), clock);
    assert!(matches!(
        proposer.propose(&ns(), genesis.owner().parent(), C::ExpireLocks),
        Err(server::ProposeError::Broker(
            BrokerError::CapacityStateUnsupported
        ))
    ));
    let broker = Broker::spawn(proposer);
    assert!(
        broker
            .handle()
            .submit_blocking(ns(), genesis.owner().parent().clone(), C::ExpireMessages)
            .is_err()
    );
    drop(broker);
    assert_eq!(
        StateMachine::new(store.clone()).apply(&Command::new(
            ns(),
            genesis.owner().shadow().clone(),
            Timestamp::from_millis(1000),
            C::ExpireLocks
        )),
        Err(BrokerError::CapacityStateUnsupported)
    );
    assert_eq!(store.snapshot()?, before);
    Ok(())
}
