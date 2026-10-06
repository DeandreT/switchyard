//! Next-available session paging across real owner calls and raw AMQP on both stores.
use amqp::{
    Attach, Begin, ClientConnection, ClientReceiver, End, FilterSet, Frame, Open, Performative,
    ProtocolHeader, ReceiverSettleMode, Role, SenderSettleMode, Source, Symbol, Value, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use domain::{
    CommandKind, CommandOutcome, EntityBinding, EntityPath, NamespaceName, QueueConfig,
    SessionHold, SessionId, SessionPageOutcome, StateMachine,
};
use futures_util::FutureExt;
use protocol_amqp::{Attachment, BrokerRejection, EntityMetadata};
use server::{Broker, BrokerHandle, LocalProposer, ManualClock};
use std::{
    error::Error,
    future::Future,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use storage::StateStore;
use testkit::StoreProvider;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore},
    task::{JoinError, JoinHandle},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);
type Decision = (
    EntityBinding,
    CommandKind,
    Result<CommandOutcome, BrokerRejection>,
);
#[derive(Clone)]
struct Recorded {
    inner: BrokerHandle,
    rows: Arc<Mutex<Vec<Decision>>>,
    changed: Arc<Notify>,
    pause_grant: Arc<AtomicBool>,
    granted: Arc<Notify>,
    proceed: Arc<Semaphore>,
}
impl protocol_amqp::Broker for Recorded {
    fn receive_fenced_owned(
        &self,
        submission: protocol_amqp::OwnedReceiveSubmission,
    ) -> impl Future<Output = Result<Option<domain::Delivery>, protocol_amqp::ReceiveSubmitError>>
    + Send
    + 'static {
        protocol_amqp::Broker::receive_fenced_owned(&self.inner, submission)
    }
    fn bind(
        &self,
        ns: NamespaceName,
        target: Attachment,
    ) -> impl Future<Output = Result<Option<protocol_amqp::EntityAdmission>, BrokerRejection>> + Send
    {
        protocol_amqp::Broker::bind(&self.inner, ns, target)
    }
    async fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let record = matches!(
            &kind,
            CommandKind::AcceptNextSessionPage { .. } | CommandKind::ReleaseSession { .. }
        );
        let captured = record.then(|| (binding.clone(), kind.clone()));
        let result = protocol_amqp::Broker::submit_fenced(&self.inner, binding, entity, kind).await;
        if let Some((binding, kind)) = captured {
            {
                let mut rows = self.rows.lock().expect("bounded decision records");
                assert!(rows.len() < 16);
                rows.push((binding, kind, result.clone()));
            }
            self.changed.notify_waiters();
        }
        if matches!(
            &result,
            Ok(CommandOutcome::SessionPage(SessionPageOutcome::Accepted(_)))
        ) && self.pause_grant.swap(false, Ordering::SeqCst)
        {
            // The actual committed reply, including its original hold, remains local across this await.
            self.granted.notify_one();
            self.proceed
                .acquire()
                .await
                .expect("return original reply")
                .forget();
        }
        result
    }
    fn rules_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        subscription: domain::SubscriptionName,
    ) -> impl Future<Output = Result<Vec<domain::RuleDefinition>, BrokerRejection>> + Send {
        protocol_amqp::Broker::rules_fenced(&self.inner, binding, entity, subscription)
    }
    fn rules(
        &self,
        ns: NamespaceName,
        entity: EntityPath,
        subscription: domain::SubscriptionName,
    ) -> impl Future<Output = Result<Vec<domain::RuleDefinition>, BrokerRejection>> + Send {
        protocol_amqp::Broker::rules(&self.inner, ns, entity, subscription)
    }
    fn entity_metadata(
        &self,
        ns: NamespaceName,
        target: Attachment,
    ) -> impl Future<Output = Result<Option<EntityMetadata>, BrokerRejection>> + Send {
        protocol_amqp::Broker::entity_metadata(&self.inner, ns, target)
    }
    fn submit(
        &self,
        ns: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        protocol_amqp::Broker::submit(&self.inner, ns, entity, kind)
    }
    fn deliverable(
        &self,
        ns: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        protocol_amqp::Broker::deliverable(&self.inner, ns, entity)
    }
}
struct Node<P: StoreProvider> {
    store: P::Store,
    namespace: NamespaceName,
    entity: EntityPath,
    address: String,
    recorded: Recorded,
    broker: Broker,
    listener: JoinHandle<std::io::Result<()>>,
    provider: P,
}
impl<P: StoreProvider> Node<P> {
    async fn start(provider: P) -> TestResult<Self> {
        let store = provider.open()?;
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            ManualClock::at(1000),
        ));
        broker.handle().submit_blocking(
            namespace.clone(),
            entity.clone(),
            CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_session: true,
                    ..QueueConfig::default()
                },
            },
        )?;
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?.to_string();
        let recorded = Recorded {
            inner: broker.handle(),
            rows: Arc::default(),
            changed: Arc::default(),
            pause_grant: Arc::default(),
            granted: Arc::default(),
            proceed: Arc::new(Semaphore::new(0)),
        };
        let serving = recorded.clone();
        let ns = namespace.clone();
        let listener = tokio::spawn(async move {
            protocol_amqp::AmqpListener::new(serving, ns)
                .serve(socket)
                .await
        });
        Ok(Self {
            store,
            namespace,
            entity,
            address,
            recorded,
            broker,
            listener,
            provider,
        })
    }
    fn submit(&self, kind: CommandKind) -> TestResult<CommandOutcome> {
        Ok(self.broker.handle().submit_blocking(
            self.namespace.clone(),
            self.entity.clone(),
            kind,
        )?)
    }
    fn machine(&self) -> StateMachine<P::Store> {
        StateMachine::new(self.store.clone())
    }
    fn seed(&self, count: usize, held: usize) -> TestResult<Vec<SessionHold>> {
        let mut holds = Vec::new();
        for index in 0..count {
            let id = session_id(index)?;
            self.submit(CommandKind::Send {
                message_id: id.as_str().into(),
                body: vec![index as u8],
                time_to_live_millis: None,
                session_id: Some(id.clone()),
            })?;
            if index < held {
                let CommandOutcome::SessionAccepted(Some(accepted)) =
                    self.submit(CommandKind::AcceptSession {
                        session_id: Some(id),
                        lock_duration_millis: None,
                    })?
                else {
                    panic!("named hold")
                };
                holds.push(accepted.hold());
            }
        }
        Ok(holds)
    }
    async fn released(&self, hold: &SessionHold) -> TestResult {
        loop {
            let changed = self.recorded.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let completed = {
                let rows = self.recorded.rows.lock().expect("completed release rows");
                rows.iter().find_map(|(binding, _, result)| match result {
                    Ok(CommandOutcome::SessionPage(SessionPageOutcome::Accepted(accepted)))
                        if &accepted.hold() == hold => Some(binding),
                    _ => None,
                }).is_some_and(|admitted| rows.iter().any(|(binding, kind, result)|
                    binding == admitted
                        && matches!(kind, CommandKind::ReleaseSession { session } if session == hold)
                        && matches!(result, Ok(CommandOutcome::SessionReleased))))
            };
            if completed
                && self
                    .machine()
                    .session(&self.namespace, &self.entity, &hold.session_id)?
                    .is_some_and(|record| record.lock.is_none())
            {
                return Ok(());
            }
            changed.await;
        }
    }
    async fn stop(self) -> Result<std::io::Result<()>, JoinError> {
        self.recorded.proceed.add_permits(1);
        self.listener.abort();
        let joined = self.listener.await;
        drop(self.broker);
        drop(self.provider);
        joined
    }
}
fn session_id(index: usize) -> TestResult<SessionId> {
    Ok(SessionId::new(format!("s{index:03}"))?)
}
fn source(id: Option<&SessionId>) -> Source {
    let mut filter = FilterSet::default();
    filter.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        id.map(|id| Value::String(id.as_str().into()))
            .unwrap_or(Value::Null),
    );
    Source::builder().address("orders").filter(filter).build()
}
type Observed =
    Result<Result<TestResult, tokio::time::error::Elapsed>, Box<dyn std::any::Any + Send>>;
async fn finish<P: StoreProvider>(
    node: Node<P>,
    mut connection: Option<ClientConnection>,
    mut peer: Option<TcpStream>,
    observed: Observed,
) -> TestResult {
    node.recorded.proceed.add_permits(1);
    let closing = AssertUnwindSafe(timeout(DEADLINE, async {
        if let Some(connection) = connection.as_mut() {
            connection.close().await?;
        }
        if let Some(peer) = peer.as_mut() {
            peer.shutdown().await?;
        }
        Ok::<(), Box<dyn Error>>(())
    }))
    .catch_unwind()
    .await;
    let joined = node.stop().await;
    match observed {
        Err(payload) => resume_unwind(payload),
        Ok(result) => result??,
    }
    match closing {
        Err(payload) => resume_unwind(payload),
        Ok(result) => result??,
    }
    match joined {
        Ok(result) => result?,
        Err(error) if error.is_cancelled() => (),
        Err(error) => return Err(Box::new(error)),
    }
    Ok(())
}
async fn next_available_reaches_33rd_and_65th_and_echoes_granted_filter<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = None;
    let observed = AssertUnwindSafe(timeout(DEADLINE, async {
        for held in [32, 64] {
            // The same 65 groups are reused after releasing every prior session hold.
            let holds = if held == 32 { node.seed(65, 32)? } else {
                let mut holds = Vec::new();
                for index in 0..64 {
                    let CommandOutcome::SessionAccepted(Some(accepted)) = node.submit(CommandKind::AcceptSession {
                        session_id: Some(session_id(index)?), lock_duration_millis: None })? else { panic!("named hold") };
                    holds.push(accepted.hold());
                }
                holds
            };
            for index in 0..65 {
                assert!(!node.machine().session_ready_sequences(
                    &node.namespace, &node.entity, &session_id(index)?, 1,
                )?.is_empty(), "every session group must have ready work before the walk");
            }
            connection = Some(ClientConnection::open(TcpStream::connect(&node.address).await?, "paged-peer", None).await?);
            let client = connection.as_mut().expect("original connection");
            let mut session = client.begin().await?;
            let mut receiver = ClientReceiver::builder().name(format!("next-{held}"))
                .source(source(None)).receiver_settle_mode(ReceiverSettleMode::Second).attach(&mut session).await?;
            let expected = session_id(held)?;
            assert_eq!(protocol_amqp::read_session_filter(receiver.source().as_ref())?, protocol_amqp::SessionRequest::Named(expected.clone()));
            let held_record = node.machine().session(&node.namespace, &node.entity, &expected)?.expect("granted record");
            let granted = SessionHold::new(expected, held_record.lock.expect("actual lock").token);
            let rows = node.recorded.rows.lock().expect("decisions").clone();
            let page_rows: Vec<_> = rows.iter().filter(|(_, kind, _)| matches!(kind, CommandKind::AcceptNextSessionPage { .. })).collect();
            assert_eq!(page_rows.len(), if held == 32 { 2 } else { 5 });
            assert!(page_rows.windows(2).all(|rows| rows[0].0 == rows[1].0));
            assert!(matches!(&page_rows.last().expect("page").2,
                Ok(CommandOutcome::SessionPage(SessionPageOutcome::Accepted(accepted))) if accepted.hold() == granted));
            let delivery = receiver.recv().await?;
            assert_eq!(delivery.message().body, amqp::Message::data(vec![held as u8]).body);
            receiver.release(&delivery).await?;
            receiver.close().await?;
            timeout(DEADLINE, node.released(&granted)).await??;
            client.close().await?;
            drop(connection.take());
            for hold in holds { assert_eq!(node.submit(CommandKind::ReleaseSession { session: hold })?, CommandOutcome::SessionReleased); }
            if held == 32 {
                // Retire any actual original lock left by release/prefetch before the second walk.
                let sequence = domain::SequenceNumber::new(33);
                let record = node.machine().message(&node.namespace, &node.entity, sequence)?.expect("original s032 message");
                assert_eq!(record.session_id, Some(session_id(32)?));
                match record.state {
                    domain::MessageState::Locked { token, .. } => {
                        assert_eq!(node.submit(CommandKind::Settle { sequence, lock_token: token,
                            disposition: domain::SettlementDisposition::Complete, properties_to_modify: Default::default(),
                        })?, CommandOutcome::Completed);
                        assert!(node.machine().message(&node.namespace, &node.entity, sequence)?.is_none());
                    }
                    domain::MessageState::Ready => (),
                    other => panic!("unexpected original s032 state: {other:?}"),
                }
                // An additional ready copy retains all 65 groups regardless of prefetch.
                node.submit(CommandKind::Send {
                    message_id: "ready-for-second-walk".into(), body: vec![32],
                    time_to_live_millis: None, session_id: Some(session_id(32)?),
                })?;
            }
        }
        Ok::<(), Box<dyn Error>>(())
    })).catch_unwind().await;
    finish(node, connection, None, observed).await
}
async fn all_held_pages_reach_end_without_mutation_and_keep_session_usable<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = None;
    let observed = AssertUnwindSafe(timeout(DEADLINE, async {
        let holds = node.seed(65, 65)?;
        let before = node.store.snapshot()?;
        connection = Some(ClientConnection::open(TcpStream::connect(&node.address).await?, "held-peer", None).await?);
        let client = connection.as_mut().expect("connection");
        let mut session = client.begin().await?;
        let mut refused = ClientReceiver::builder().name("all-held").source(source(None)).attach(&mut session).await?;
        assert!(refused.source().is_none());
        assert!(matches!(refused.recv().await, Err(amqp::EngineError::RemoteDetached)));
        assert_eq!(node.store.snapshot()?, before);
        let rows = node.recorded.rows.lock().expect("decisions").clone();
        assert_eq!(rows.len(), 3);
        assert!(matches!(&rows[0].2, Ok(CommandOutcome::SessionPage(SessionPageOutcome::Continue(cursor))) if cursor.session_id == session_id(31)?));
        assert!(matches!(&rows[1].2, Ok(CommandOutcome::SessionPage(SessionPageOutcome::Continue(cursor))) if cursor.session_id == session_id(63)?));
        assert_eq!(rows[2].2, Ok(CommandOutcome::SessionPage(SessionPageOutcome::End)));
        let hold = holds.last().expect("last held").clone();
        node.submit(CommandKind::ReleaseSession { session: hold.clone() })?;
        // A timed-out next-available link must not end the healthy AMQP session.
        let mut receiver = ClientReceiver::builder().name("healthy-named")
            .source(source(Some(&hold.session_id))).attach(&mut session).await?;
        let delivery = receiver.recv().await?;
        assert_eq!(delivery.message().body, amqp::Message::data(vec![64]).body);
        receiver.release(&delivery).await?;
        receiver.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })).catch_unwind().await;
    finish(node, connection, None, observed).await
}
async fn peer_write(peer: &mut TcpStream, performative: Performative) -> TestResult {
    write_frame(
        peer,
        &Frame::Amqp {
            channel: 0,
            performative: Some(performative),
            payload: Vec::new(),
        },
    )
    .await?;
    Ok(())
}
async fn paused_original_page_grant_is_released_after_retirement_without_clock_advance<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut peer = None;
    let mut connection = None;
    let observed = AssertUnwindSafe(timeout(DEADLINE, async {
        let sibling = node.seed(2, 1)?.remove(0);
        let sibling_before = node.machine().session(&node.namespace, &node.entity, &sibling.session_id)?;
        node.recorded.pause_grant.store(true, Ordering::SeqCst);
        peer = Some(TcpStream::connect(&node.address).await?);
        let raw = peer.as_mut().expect("original raw peer");
        write_protocol_header(raw, ProtocolHeader::AMQP).await?;
        assert_eq!(read_protocol_header(raw).await?, ProtocolHeader::AMQP);
        peer_write(raw, Performative::Open(Open::new("retiring-peer"))).await?;
        assert!(matches!(read_frame(raw).await?, Frame::Amqp { performative: Some(Performative::Open(_)), .. }));
        peer_write(raw, Performative::Begin(Begin::default())).await?;
        assert!(matches!(read_frame(raw).await?, Frame::Amqp { performative: Some(Performative::Begin(_)), .. }));
        peer_write(raw, Performative::Attach(Box::new(Attach {
            name: "paused-next".into(), handle: 0, role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Unsettled, rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(source(None)), target: None, unsettled: None, incomplete_unsettled: false,
            initial_delivery_count: None, max_message_size: None, offered_capabilities: None,
            desired_capabilities: None, properties: None,
        }))).await?;
        node.recorded.granted.notified().await;
        let accepted = match &node.recorded.rows.lock().expect("rows")[0].2 {
            Ok(CommandOutcome::SessionPage(SessionPageOutcome::Accepted(accepted))) => accepted.clone(),
            _ => panic!("actual paused owner result"),
        };
        assert_eq!(accepted.session_id, session_id(1)?);
        assert!(node.machine().session(&node.namespace, &node.entity, &accepted.session_id)?.expect("session").lock.is_some());
        // Native End stops/retires the original session before writing this acknowledgment.
        peer_write(raw, Performative::End(End::default())).await?;
        assert!(matches!(read_frame(raw).await?, Frame::Amqp { performative: Some(Performative::End(_)), .. }));
        node.recorded.proceed.add_permits(1);
        node.released(&accepted.hold()).await?;
        let rows = node.recorded.rows.lock().expect("rows").clone();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, rows[1].0);
        assert!(matches!(&rows[1].1, CommandKind::ReleaseSession { session } if session == &accepted.hold()));
        assert_eq!(rows[1].2, Ok(CommandOutcome::SessionReleased));
        assert_eq!(node.machine().session(&node.namespace, &node.entity, &sibling.session_id)?, sibling_before);
        // ManualClock stayed at 1000: only the exact release, not expiry, can permit this reuse.
        connection = Some(ClientConnection::open(TcpStream::connect(&node.address).await?, "replacement", None).await?);
        let client = connection.as_mut().expect("original replacement connection");
        let mut session = client.begin().await?;
        let mut receiver = ClientReceiver::builder().name("replacement-next").source(source(None)).attach(&mut session).await?;
        assert_eq!(protocol_amqp::read_session_filter(receiver.source().as_ref())?, protocol_amqp::SessionRequest::Named(session_id(1)?));
        let delivery = receiver.recv().await?;
        receiver.release(&delivery).await?;
        receiver.close().await?;
        assert_eq!(node.machine().session(&node.namespace, &node.entity, &sibling.session_id)?, sibling_before);
        Ok::<(), Box<dyn Error>>(())
    })).catch_unwind().await;
    finish(node, connection, peer, observed).await
}
macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()).await })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?).await })+ }
    };
}
for_each_backend!(
    next_available_reaches_33rd_and_65th_and_echoes_granted_filter,
    all_held_pages_reach_end_without_mutation_and_keep_session_usable,
    paused_original_page_grant_is_released_after_retirement_without_clock_advance,
);
