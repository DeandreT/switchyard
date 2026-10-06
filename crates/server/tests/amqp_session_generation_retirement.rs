//! Actual wire generations become deliverable only after owner-driven retirement.

use amqp::{ClientConnection, ClientReceiver, ClientSession, FilterSet, Source, Symbol, Value};
use domain::{
    BrokerError, CommandKind, CommandOutcome, EntityBinding, EntityPath, MessageRecord,
    MessageState, NamespaceName, QueueConfig, SequenceNumber, SessionHold, SessionId,
    SessionPageOutcome, SettlementDisposition, StateMachine, keys,
};
use futures_util::FutureExt;
use protocol_amqp::{Attachment, BrokerRejection, EntityMetadata};
use server::{
    Broker, BrokerHandle, LocalProposer, ManualClock, ProposeError, SubmitError, TimerWorker,
};
use std::{
    error::Error,
    future::Future,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::{Arc, Mutex},
    time::Duration,
};
use storage::StateStore;
use testkit::StoreProvider;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{JoinError, JoinHandle},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Decision = (
    EntityBinding,
    CommandKind,
    Result<CommandOutcome, BrokerRejection>,
);
const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct Recorded {
    inner: BrokerHandle,
    rows: Arc<Mutex<Vec<Decision>>>,
    changed: Arc<Notify>,
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
        namespace: NamespaceName,
        target: Attachment,
    ) -> impl Future<Output = Result<Option<protocol_amqp::EntityAdmission>, BrokerRejection>> + Send
    {
        protocol_amqp::Broker::bind(&self.inner, namespace, target)
    }

    async fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let captured = matches!(
            &kind,
            CommandKind::AcceptSession { .. }
                | CommandKind::AcceptNextSessionPage { .. }
                | CommandKind::ReleaseSession { .. }
                | CommandKind::SettleHeld { .. }
        )
        .then(|| (binding.clone(), kind.clone()));
        let result = protocol_amqp::Broker::submit_fenced(&self.inner, binding, entity, kind).await;
        if let Some((binding, kind)) = captured {
            let mut rows = self.rows.lock().expect("actual owner decisions");
            assert!(rows.len() < 32);
            rows.push((binding, kind, result.clone()));
        }
        self.changed.notify_waiters();
        result
    }

    fn rules_fenced(
        &self,
        binding: EntityBinding,
        topic: EntityPath,
        subscription: domain::SubscriptionName,
    ) -> impl Future<Output = Result<Vec<domain::RuleDefinition>, BrokerRejection>> + Send {
        protocol_amqp::Broker::rules_fenced(&self.inner, binding, topic, subscription)
    }

    fn rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: domain::SubscriptionName,
    ) -> impl Future<Output = Result<Vec<domain::RuleDefinition>, BrokerRejection>> + Send {
        protocol_amqp::Broker::rules(&self.inner, namespace, topic, subscription)
    }

    fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> impl Future<Output = Result<Option<EntityMetadata>, BrokerRejection>> + Send {
        protocol_amqp::Broker::entity_metadata(&self.inner, namespace, target)
    }

    fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        protocol_amqp::Broker::submit(&self.inner, namespace, entity, kind)
    }

    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        protocol_amqp::Broker::deliverable(&self.inner, namespace, entity)
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
    clock: ManualClock,
}

impl<P: StoreProvider> Node<P> {
    async fn start(provider: P) -> TestResult<Self> {
        let store = provider.open()?;
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        broker.handle().submit_blocking(
            namespace.clone(),
            entity.clone(),
            CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_session: true,
                    lock_duration_millis: 300_000,
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
        };
        let serving = recorded.clone();
        let listener_namespace = namespace.clone();
        let listener = tokio::spawn(async move {
            protocol_amqp::AmqpListener::new(serving, listener_namespace)
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
            clock,
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

    fn record(&self, sequence: SequenceNumber) -> TestResult<MessageRecord> {
        Ok(self
            .machine()
            .message(&self.namespace, &self.entity, sequence)?
            .expect("original message"))
    }

    fn send(&self, name: &str, id: &SessionId, body: Vec<u8>) -> TestResult<SequenceNumber> {
        let CommandOutcome::Sent { sequence } = self.submit(CommandKind::Send {
            message_id: name.into(),
            body,
            time_to_live_millis: None,
            session_id: Some(id.clone()),
        })?
        else {
            panic!("actual send")
        };
        Ok(sequence)
    }

    fn hold(&self, id: &SessionId) -> TestResult<SessionHold> {
        let record = self
            .machine()
            .session(&self.namespace, &self.entity, id)?
            .expect("session");
        Ok(SessionHold::new(
            id.clone(),
            record.lock.expect("actual original lock").token,
        ))
    }

    async fn released(&self, original: &SessionHold) -> TestResult {
        loop {
            let notified = self.recorded.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let completed = {
                let rows = self.recorded.rows.lock().expect("release decisions");
                rows.iter().find_map(|(binding, _, result)| match result {
                    Ok(CommandOutcome::SessionAccepted(Some(accepted))) if &accepted.hold() == original => Some(binding),
                    Ok(CommandOutcome::SessionPage(SessionPageOutcome::Accepted(accepted))) if &accepted.hold() == original => Some(binding),
                    _ => None,
                }).is_some_and(|admitted| rows.iter().any(|(binding, kind, result)|
                    binding == admitted && matches!(kind, CommandKind::ReleaseSession { session } if session == original)
                        && matches!(result, Ok(CommandOutcome::SessionReleased))))
            };
            if completed
                && self
                    .machine()
                    .session(&self.namespace, &self.entity, &original.session_id)?
                    .is_some_and(|record| record.lock.is_none())
            {
                return Ok(());
            }
            notified.await;
        }
    }

    async fn removed(&self, sequence: SequenceNumber) -> TestResult {
        loop {
            let notified = self.recorded.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .machine()
                .message(&self.namespace, &self.entity, sequence)?
                .is_none()
            {
                return Ok(());
            }
            notified.await;
        }
    }

    async fn stop(self) -> Result<std::io::Result<()>, JoinError> {
        self.listener.abort();
        let joined = self.listener.await;
        drop(self.broker);
        drop(self.provider);
        joined
    }
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

async fn receiving(
    session: &mut ClientSession,
    name: &str,
    id: Option<&SessionId>,
) -> TestResult<ClientReceiver> {
    Ok(ClientReceiver::builder()
        .name(name)
        .source(source(id))
        .receiver_settle_mode(amqp::ReceiverSettleMode::Second)
        .attach(session)
        .await?)
}

fn message_token(record: &MessageRecord) -> domain::LockToken {
    let MessageState::Locked { token, .. } = &record.state else {
        panic!("actual original message lock")
    };
    *token
}

fn restored(original: &MessageRecord, actual: &MessageRecord) {
    let mut expected = original.clone();
    expected.state = MessageState::Ready;
    assert_eq!(actual, &expected);
}

fn reacquired(original: &MessageRecord, actual: &MessageRecord) {
    let mut expected = original.clone();
    expected.state = actual.state.clone();
    expected.delivery_count += 1;
    assert_eq!(actual, &expected);
    assert_ne!(message_token(actual), message_token(original));
}

async fn actual_link_close_releases_original_generation_then_timer_restores_delivery<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = None;
    let observed = AssertUnwindSafe(timeout(DEADLINE, async {
        let a_id = SessionId::new("A")?;
        let b_id = SessionId::new("B")?;
        let a_sequence = node.send("released-A", &a_id, vec![0xA])?;
        let b_sequence = node.send("healthy-B", &b_id, vec![0xB])?;
        connection = Some(ClientConnection::open(TcpStream::connect(&node.address).await?, "retirement-close", None).await?);
        let mut session = connection.as_mut().expect("original connection").begin().await?;
        let mut original = receiving(&mut session, "original-A", Some(&a_id)).await?;
        let delivery = original.recv().await?;
        assert_eq!(delivery.message().body, amqp::Message::data(vec![0xA]).body);
        let original_hold = node.hold(&a_id)?;
        let locked = node.record(a_sequence)?;
        let mut sibling = receiving(&mut session, "healthy-B", Some(&b_id)).await?;
        let b_delivery = sibling.recv().await?;
        assert_eq!(b_delivery.message().body, amqp::Message::data(vec![0xB]).body);
        let b_hold = node.hold(&b_id)?;
        let b_record = node.record(b_sequence)?;
        let b_session = node.machine().session(&node.namespace, &node.entity, &b_id)?;
        original.close().await?;
        node.released(&original_hold).await?;
        assert_eq!(node.record(a_sequence)?, locked);
        let mut refused = receiving(&mut session, "before-retirement-A", Some(&a_id)).await?;
        assert!(refused.source().is_none());
        assert!(matches!(refused.recv().await, Err(amqp::EngineError::RemoteDetached)));
        assert!(node.recorded.rows.lock().expect("actual refusal").iter().any(|(_, kind, result)|
            matches!(kind, CommandKind::AcceptSession { session_id: Some(id), .. } if id == &a_id)
                && matches!(result, Err(BrokerRejection::Refused(BrokerError::SessionTakeoverPending { session_id })) if session_id == &a_id)));
        let handle = node.broker.handle();
        let timer = TimerWorker::new(&handle);
        let report = timer.sweep_once()?;
        assert_eq!(report.locks_returned_to_ready, 1);
        assert_eq!(report.sessions_released, 0);
        assert_eq!(report.messages_dead_lettered, 0);
        restored(&locked, &node.record(a_sequence)?);
        for key in [keys::session_message_lock_reverse(&node.namespace, &node.entity, a_sequence),
            keys::session_message_lock_forward(&node.namespace, &node.entity, &a_id, Some(original_hold.token), a_sequence),
            keys::session_message_lock_summary(&node.namespace, &node.entity, &a_id)] {
            assert!(node.store.get(&key)?.is_none());
        }
        assert_eq!(node.record(b_sequence)?, b_record);
        assert_eq!(node.machine().session(&node.namespace, &node.entity, &b_id)?, b_session);
        let mut replacement = receiving(&mut session, "replacement-A", Some(&a_id)).await?;
        let replacement_hold = node.hold(&a_id)?;
        assert_ne!(replacement_hold.token, original_hold.token);
        let redelivered = replacement.recv().await?;
        assert_eq!(redelivered.message().body, delivery.message().body);
        reacquired(&locked, &node.record(a_sequence)?);
        replacement.accept(&redelivered).await?;
        node.removed(a_sequence).await?;
        replacement.close().await?;
        node.released(&replacement_hold).await?;
        assert_eq!(node.record(b_sequence)?, b_record);
        sibling.accept(&b_delivery).await?;
        node.removed(b_sequence).await?;
        sibling.close().await?;
        node.released(&b_hold).await?;
        Ok::<(), Box<dyn Error>>(())
    })).catch_unwind().await;
    finish(node, connection, observed).await
}

async fn actual_session_deadline_retires_only_original_owned_generation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = None;
    let observed = AssertUnwindSafe(timeout(DEADLINE, async {
        let a_id = SessionId::new("A")?;
        let b_id = SessionId::new("B")?;
        let a_sequence = node.send("deadline-A", &a_id, vec![0xA])?;
        let b_sequence = node.send("live-B", &b_id, vec![0xB])?;
        connection = Some(ClientConnection::open(TcpStream::connect(&node.address).await?, "retirement-deadline", None).await?);
        let mut session = connection.as_mut().expect("original connection").begin().await?;
        let mut original = receiving(&mut session, "original-deadline-A", Some(&a_id)).await?;
        let delivery = original.recv().await?;
        assert_eq!(delivery.message().body, amqp::Message::data(vec![0xA]).body);
        let original_hold = node.hold(&a_id)?;
        let locked = node.record(a_sequence)?;
        let mut sibling = receiving(&mut session, "original-live-B", Some(&b_id)).await?;
        let b_delivery = sibling.recv().await?;
        let b_hold = node.hold(&b_id)?;
        let b_record = node.record(b_sequence)?;
        let b_session = node.machine().session(&node.namespace, &node.entity, &b_id)?;
        node.submit(CommandKind::SetSessionState { session: original_hold.clone(), state: vec![7, 0, 255] })?;
        node.submit(CommandKind::RenewSessionLock { session: original_hold.clone(), lock_duration_millis: Some(100) })?;
        let deadline = node.machine().session(&node.namespace, &node.entity, &a_id)?.expect("original deadline").lock.expect("original session lock").locked_until;
        let MessageState::Locked { locked_until, .. } = &locked.state else { panic!("original message deadline") };
        assert!(deadline < *locked_until);
        node.clock.set(deadline.as_millis());
        let before = node.store.snapshot()?;
        assert!(matches!(node.broker.handle().submit_blocking(node.namespace.clone(), node.entity.clone(),
            CommandKind::RenewLockHeld { sequence: a_sequence, lock_token: message_token(&locked),
                session: Some(original_hold.clone()), lock_duration_millis: None }),
            Err(SubmitError::Propose(ProposeError::Broker(BrokerError::SessionLockExpired { .. })))));
        assert_eq!(node.store.snapshot()?, before);
        let handle = node.broker.handle();
        let timer = TimerWorker::new(&handle);
        let report = timer.sweep_once()?;
        assert_eq!(report.sessions_released, 1);
        assert_eq!(report.locks_returned_to_ready, 1);
        assert_eq!(report.messages_dead_lettered, 0);
        restored(&locked, &node.record(a_sequence)?);
        let state = node.machine().session(&node.namespace, &node.entity, &a_id)?.expect("retained opaque state");
        assert_eq!(state.state, vec![7, 0, 255]);
        assert!(state.lock.is_none());
        assert_eq!(node.record(b_sequence)?, b_record);
        assert_eq!(node.machine().session(&node.namespace, &node.entity, &b_id)?, b_session);
        let mut replacement = receiving(&mut session, "next-after-retirement", None).await?;
        assert_eq!(protocol_amqp::read_session_filter(replacement.source().as_ref())?, protocol_amqp::SessionRequest::Named(a_id.clone()));
        let replacement_hold = node.hold(&a_id)?;
        assert_ne!(replacement_hold.token, original_hold.token);
        let redelivered = replacement.recv().await?;
        assert_eq!(redelivered.message().body, delivery.message().body);
        reacquired(&locked, &node.record(a_sequence)?);
        let before = node.store.snapshot()?;
        assert!(matches!(node.broker.handle().submit_blocking(node.namespace.clone(), node.entity.clone(),
            CommandKind::SettleHeld { sequence: a_sequence, lock_token: message_token(&locked),
                session: Some(original_hold.clone()), disposition: SettlementDisposition::Complete,
                properties_to_modify: Default::default() }),
            Err(SubmitError::Propose(ProposeError::Broker(BrokerError::SessionLockNotHeld { session_id }))) if session_id == a_id));
        assert_eq!(node.store.snapshot()?, before);
        replacement.accept(&redelivered).await?;
        node.removed(a_sequence).await?;
        replacement.close().await?;
        node.released(&replacement_hold).await?;
        assert_eq!(node.record(b_sequence)?, b_record);
        sibling.accept(&b_delivery).await?;
        node.removed(b_sequence).await?;
        sibling.close().await?;
        node.released(&b_hold).await?;
        // The obsolete original link is disposed by the retained connection below.
        drop(original);
        Ok::<(), Box<dyn Error>>(())
    })).catch_unwind().await;
    finish(node, connection, observed).await
}

type Observed =
    Result<Result<TestResult, tokio::time::error::Elapsed>, Box<dyn std::any::Any + Send>>;

async fn finish<P: StoreProvider>(
    node: Node<P>,
    mut connection: Option<ClientConnection>,
    observed: Observed,
) -> TestResult {
    let closing = AssertUnwindSafe(timeout(DEADLINE, async {
        if let Some(connection) = connection.as_mut() {
            connection.close().await?;
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

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()).await })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?).await })+ }
    };
}

for_each_backend!(
    actual_link_close_releases_original_generation_then_timer_restores_delivery,
    actual_session_deadline_retires_only_original_owned_generation,
);
