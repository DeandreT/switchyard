//! Shared real-wire and actual-owner fixtures for receiving-link custody tests.

#[path = "receiving_leaf_fault_tests.rs"]
mod leaf_fault_tests;

use std::{
    future::{Future, poll_fn},
    io,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use amqp::{
    Accepted, Attach, Begin, Detach, Disposition, End, Flow, Frame, LinkEndpoint, Message, Open,
    PendingDelivery, Performative, ProtocolHeader, ReceiverSettleMode, Role, Sender,
    SenderSettleMode, ServerConnection, ServerSession, Source, read_frame, read_protocol_header,
    write_frame, write_protocol_header,
};
use domain::{
    CommandKind, CommandOutcome, Delivery, EntityPath, NamespaceName, QueueConfig, ReceiveMode,
    SequenceNumber, SessionId, StateMachine,
};
use storage::{
    FjallStore, Key, MemoryStore, Mutation, StateStore, StorageError, StoreSnapshot, Value,
    WriteBatch,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, DuplexStream, ReadBuf, duplex},
    sync::Notify,
    task::Id,
    time::timeout,
};

use crate::listener::settlement::{SettlementContext, lock_delivery_tag};
use crate::management::ConnectionManagement;
use crate::{Broker, BrokerRejection};

pub(super) const WAIT: Duration = Duration::from_secs(5);
pub(super) const CHANNEL: u16 = 1;
pub(super) const HANDLE: u32 = 1;
pub(super) const LINK: &str = "owned-settlement";

#[derive(Default)]
pub(super) struct CommitState {
    key: Vec<u8>,
    put: bool,
    before: bool,
    after: bool,
    allow_before: bool,
    allow_after: bool,
    pub(super) commits: usize,
}

#[derive(Default)]
pub(super) struct CommitGate {
    pub(super) state: Mutex<CommitState>,
    released: Condvar,
    changed: Notify,
}

impl CommitGate {
    pub(super) fn arm(&self, key: Vec<u8>) {
        *self.state.lock().unwrap() = CommitState {
            key,
            ..CommitState::default()
        };
    }

    pub(super) fn arm_put(&self, key: Vec<u8>) {
        *self.state.lock().unwrap() = CommitState {
            key,
            put: true,
            ..CommitState::default()
        };
    }

    fn matches(&self, batch: &WriteBatch) -> bool {
        let state = self.state.lock().unwrap();
        !state.key.is_empty()
            && batch.mutations().iter().any(|mutation| match mutation {
                Mutation::Delete { key } => !state.put && *key == state.key,
                Mutation::Put { key, .. } => state.put && *key == state.key,
            })
    }

    fn pause(&self, after: bool) {
        let mut state = self.state.lock().unwrap();
        if after {
            state.after = true;
            state.commits += 1;
        } else {
            state.before = true;
        }
        self.changed.notify_waiters();
        while !(if after {
            state.allow_after
        } else {
            state.allow_before
        }) {
            state = self.released.wait(state).unwrap();
        }
    }

    pub(super) async fn reached(&self, after: bool) {
        timeout(WAIT, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let reached = {
                    let state = self.state.lock().unwrap();
                    if after { state.after } else { state.before }
                };
                if reached {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("actual broker apply reached gate");
    }

    pub(super) fn release(&self, after: bool) {
        let mut state = self.state.lock().unwrap();
        if after {
            state.allow_after = true;
        } else {
            state.allow_before = true;
        }
        self.released.notify_all();
    }

    pub(super) fn release_all(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.allow_before = true;
        state.allow_after = true;
        self.released.notify_all();
    }
}

#[derive(Clone)]
enum Backend {
    Memory(MemoryStore),
    Durable(FjallStore),
}

#[derive(Clone)]
pub(super) struct GatedStore {
    inner: Backend,
    gate: Arc<CommitGate>,
}

impl StateStore for GatedStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        match &self.inner {
            Backend::Memory(store) => store.get(key),
            Backend::Durable(store) => store.get(key),
        }
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let gated = self.gate.matches(&batch);
        if gated {
            self.gate.pause(false);
        }
        match &self.inner {
            Backend::Memory(store) => store.apply(batch)?,
            Backend::Durable(store) => store.apply(batch)?,
        }
        if gated {
            self.gate.pause(true);
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        match &self.inner {
            Backend::Memory(store) => store.snapshot(),
            Backend::Durable(store) => store.snapshot(),
        }
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        match &self.inner {
            Backend::Memory(store) => store.scan_from(prefix, start, limit),
            Backend::Durable(store) => store.scan_from(prefix, start, limit),
        }
    }
}

#[derive(Clone)]
pub(super) struct Submission {
    pub(super) worker: Id,
    pub(super) kind: CommandKind,
    pub(super) returned: bool,
}

#[derive(Clone)]
pub(super) struct ActualBroker {
    handle: server::BrokerHandle,
    log: Arc<Mutex<Vec<Submission>>>,
    panic_complete: Arc<AtomicBool>,
}

impl ActualBroker {
    pub(super) fn fail_complete(&self) {
        self.panic_complete.store(true, Ordering::SeqCst);
    }
}

// A unit-test crate and server's normal protocol dependency have distinct
// Broker trait identities. Delegate inherent methods, not the foreign impl.
impl Broker for ActualBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let worker = tokio::task::id();
        self.log.lock().unwrap().push(Submission {
            worker,
            kind: kind.clone(),
            returned: false,
        });
        if matches!(kind, CommandKind::Complete { .. })
            && self.panic_complete.load(Ordering::SeqCst)
        {
            panic!("older-settlement-failure");
        }
        let result = self.handle.submit(namespace, entity, kind.clone()).await;
        self.log.lock().unwrap().push(Submission {
            worker,
            kind,
            returned: true,
        });
        result.map_err(|error| match error {
            server::SubmitError::Propose(server::ProposeError::Broker(error)) => {
                BrokerRejection::Refused(error)
            }
            other => BrokerRejection::Unavailable(other.to_string()),
        })
    }

    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        // These fixtures preload all messages. No wakeup behavior is claimed.
        std::future::pending()
    }
}

pub(super) struct Actor {
    owner: Option<server::Broker>,
    pub(super) broker: Option<ActualBroker>,
    store: Option<GatedStore>,
    memory: Option<MemoryStore>,
    directory: Option<tempfile::TempDir>,
    pub(super) gate: Arc<CommitGate>,
    pub(super) log: Arc<Mutex<Vec<Submission>>>,
    pub(super) namespace: NamespaceName,
    pub(super) entity: EntityPath,
    pub(super) clock: server::ManualClock,
}

impl Actor {
    pub(super) fn new(durable: bool, session: bool) -> Self {
        let gate = Arc::new(CommitGate::default());
        let directory = durable.then(|| tempfile::tempdir().unwrap());
        let memory = (!durable).then(MemoryStore::default);
        let inner = match &directory {
            Some(directory) => Backend::Durable(FjallStore::open(directory.path()).unwrap()),
            None => Backend::Memory(memory.as_ref().unwrap().clone()),
        };
        let store = GatedStore {
            inner,
            gate: Arc::clone(&gate),
        };
        let clock = server::ManualClock::at(1_000);
        let owner = server::Broker::spawn(server::LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = ActualBroker {
            handle: owner.handle(),
            log: Arc::clone(&log),
            panic_complete: Arc::new(AtomicBool::new(false)),
        };
        let actor = Self {
            owner: Some(owner),
            broker: Some(broker),
            store: Some(store),
            memory,
            directory,
            gate,
            log,
            namespace: NamespaceName::new("tenant").unwrap(),
            entity: EntityPath::new("orders").unwrap(),
            clock,
        };
        assert_eq!(
            actor.intent(CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_session: session,
                    ..QueueConfig::default()
                },
            }),
            CommandOutcome::QueueCreated
        );
        actor
    }

    pub(super) fn intent(&self, kind: CommandKind) -> CommandOutcome {
        self.broker
            .as_ref()
            .unwrap()
            .handle
            .submit_blocking(self.namespace.clone(), self.entity.clone(), kind)
            .unwrap()
    }

    pub(super) fn send(&self, marker: &str, session: Option<SessionId>) -> SequenceNumber {
        let CommandOutcome::Sent { sequence } = self.intent(CommandKind::Send {
            message_id: marker.to_owned(),
            body: marker.as_bytes().to_vec(),
            time_to_live_millis: None,
            session_id: session,
            scheduled_enqueue_at: None,
            envelope: None,
        }) else {
            panic!("send committed");
        };
        sequence
    }

    pub(super) fn receive(&self) -> Delivery {
        let CommandOutcome::Received(Some(delivery)) = self.intent(CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        }) else {
            panic!("receive committed a lock");
        };
        delivery
    }

    pub(super) fn key(&self, sequence: SequenceNumber) -> Vec<u8> {
        domain::keys::message(&self.namespace, &self.entity, sequence)
    }

    pub(super) fn store(&self) -> &GatedStore {
        self.store.as_ref().unwrap()
    }

    pub(super) fn context(
        &self,
        management: Arc<ConnectionManagement>,
    ) -> SettlementContext<ActualBroker> {
        SettlementContext {
            namespace: self.namespace.clone(),
            entity: self.entity.clone(),
            broker: self.broker.as_ref().unwrap().clone(),
            authorization: None,
            management,
        }
    }

    pub(super) fn complete_count(&self) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|submission| {
                !submission.returned && matches!(submission.kind, CommandKind::Complete { .. })
            })
            .count()
    }

    pub(super) fn reopen(&mut self) {
        self.gate.release_all();
        drop(self.broker.take());
        drop(self.owner.take());
        drop(self.store.take());
        let inner = match &self.directory {
            Some(directory) => Backend::Durable(FjallStore::open(directory.path()).unwrap()),
            None => Backend::Memory(self.memory.as_ref().unwrap().clone()),
        };
        self.store = Some(GatedStore {
            inner,
            gate: Arc::clone(&self.gate),
        });
    }
    pub(super) fn receive_count(&self) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| !entry.returned && matches!(entry.kind, CommandKind::Receive { .. }))
            .count()
    }

    pub(super) fn restart(&mut self) {
        self.reopen();
        let owner = server::Broker::spawn(server::LocalProposer::new(
            StateMachine::new(self.store().clone()),
            self.clock.clone(),
        ));
        self.broker = Some(ActualBroker {
            handle: owner.handle(),
            log: Arc::clone(&self.log),
            panic_complete: Arc::new(AtomicBool::new(false)),
        });
        self.owner = Some(owner);
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        // Release synchronous owner-thread barriers before its blocking join.
        self.gate.release_all();
        drop(self.broker.take());
        drop(self.owner.take());
    }
}

#[derive(Default)]
struct WriteState {
    blocked: bool,
    entered: bool,
    waker: Option<Waker>,
}

#[derive(Default)]
pub(super) struct WriteGate {
    state: Mutex<WriteState>,
    changed: Notify,
}

impl WriteGate {
    pub(super) fn block(&self) {
        self.state.lock().unwrap().blocked = true;
    }

    pub(super) fn release(&self) {
        let waker = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.blocked = false;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(super) fn release_on_drop(self: &Arc<Self>) -> WriteReleaseGuard {
        WriteReleaseGuard(Arc::clone(self))
    }

    pub(super) async fn reached(&self) {
        timeout(WAIT, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.state.lock().unwrap().entered {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("actual confirmation write reached gate");
    }
}

pub(super) struct WriteReleaseGuard(Arc<WriteGate>);

impl Drop for WriteReleaseGuard {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct GatedIo {
    inner: DuplexStream,
    gate: Arc<WriteGate>,
}

impl AsyncRead for GatedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for GatedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.gate.state.lock().unwrap();
        if state.blocked {
            state.entered = true;
            state.waker = Some(cx.waker().clone());
            self.gate.changed.notify_waiters();
            return Poll::Pending;
        }
        drop(state);
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub(super) fn frame(performative: Performative) -> Frame {
    Frame::Amqp {
        channel: CHANNEL,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

pub(super) async fn performative(peer: &mut DuplexStream) -> Performative {
    let Frame::Amqp {
        performative: Some(performative),
        ..
    } = timeout(WAIT, read_frame(peer))
        .await
        .expect("wire response deadline")
        .unwrap()
    else {
        panic!("actual AMQP frame");
    };
    performative
}

pub(super) struct Wire {
    connection: ServerConnection,
    _session: ServerSession,
    pub(super) sender: Option<Sender>,
    pub(super) peer: DuplexStream,
    pub(super) writes: Arc<WriteGate>,
    pub(super) mode: ReceiverSettleMode,
}

impl Wire {
    pub(super) async fn new(mode: ReceiverSettleMode) -> Self {
        Self::new_with_credit(mode, 64).await
    }

    pub(super) async fn new_with_credit(mode: ReceiverSettleMode, credit: u32) -> Self {
        let (inner, mut peer) = duplex(64 * 1024);
        let writes = Arc::new(WriteGate::default());
        let stream = GatedIo {
            inner,
            gate: Arc::clone(&writes),
        };
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept(stream, "settlement-server", None),
                async {
                    write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                        .await
                        .unwrap();
                    assert_eq!(
                        read_protocol_header(&mut peer).await.unwrap(),
                        ProtocolHeader::AMQP
                    );
                    let mut open = frame(Performative::Open(Open::new("settlement-peer")));
                    let Frame::Amqp { channel, .. } = &mut open else {
                        unreachable!()
                    };
                    *channel = 0;
                    write_frame(&mut peer, &open).await.unwrap();
                    assert!(matches!(
                        performative(&mut peer).await,
                        Performative::Open(_)
                    ));
                }
            )
        })
        .await
        .expect("Open deadline");
        let mut connection = connection.unwrap();
        write_frame(&mut peer, &frame(Performative::Begin(Begin::default())))
            .await
            .unwrap();
        let incoming = timeout(WAIT, connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let mut session = connection.accept_session(incoming).await.unwrap();
        assert!(matches!(
            performative(&mut peer).await,
            Performative::Begin(_)
        ));
        let sender = Self::attach(&mut session, &mut peer, mode.clone(), credit).await;
        Self {
            connection,
            _session: session,
            sender: Some(sender),
            peer,
            writes,
            mode,
        }
    }

    async fn attach(
        session: &mut ServerSession,
        peer: &mut DuplexStream,
        mode: ReceiverSettleMode,
        credit: u32,
    ) -> Sender {
        write_frame(
            peer,
            &frame(Performative::Attach(Box::new(Attach {
                name: LINK.to_owned(),
                handle: HANDLE,
                role: Role::Receiver,
                snd_settle_mode: SenderSettleMode::Unsettled,
                rcv_settle_mode: mode.clone(),
                source: Some(Source::new("orders")),
                target: None,
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: None,
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            }))),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, session.next_incoming_attach())
            .await
            .unwrap()
            .unwrap();
        let LinkEndpoint::Sender(sender) =
            session.accept_attach(incoming, 1024 * 1024).await.unwrap()
        else {
            panic!("actual native sender");
        };
        let Performative::Attach(response) = performative(peer).await else {
            panic!("Attach response");
        };
        assert_eq!(response.rcv_settle_mode, mode);
        write_frame(
            peer,
            &frame(Performative::Flow(Flow {
                handle: Some(HANDLE),
                delivery_count: Some(0),
                link_credit: Some(credit),
                incoming_window: 2048,
                outgoing_window: 2048,
                ..Flow::default()
            })),
        )
        .await
        .unwrap();
        sender
    }

    pub(super) async fn reattach(&mut self, credit: u32) -> Sender {
        Self::attach(
            &mut self._session,
            &mut self.peer,
            self.mode.clone(),
            credit,
        )
        .await
    }

    pub(super) async fn session_barrier(&mut self, channel: u16) -> ServerSession {
        let mut begin = frame(Performative::Begin(Begin::default()));
        let Frame::Amqp {
            channel: actual, ..
        } = &mut begin
        else {
            unreachable!()
        };
        *actual = channel;
        write_frame(&mut self.peer, &begin).await.unwrap();
        let incoming = timeout(WAIT, self.connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let session = timeout(WAIT, self.connection.accept_session(incoming))
            .await
            .unwrap()
            .unwrap();
        let Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Begin(_)),
            payload,
        } = timeout(WAIT, read_frame(&mut self.peer))
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("original native command FIFO Begin barrier");
        };
        assert_eq!(actual, channel);
        assert!(payload.is_empty());
        session
    }

    pub(super) async fn detach(&mut self) {
        write_frame(
            &mut self.peer,
            &frame(Performative::Detach(Detach {
                handle: HANDLE,
                closed: true,
                error: None,
            })),
        )
        .await
        .unwrap();
        assert!(matches!(
            performative(&mut self.peer).await,
            Performative::Detach(_)
        ));
    }

    pub(super) async fn end(&mut self) {
        write_frame(
            &mut self.peer,
            &frame(Performative::End(End { error: None })),
        )
        .await
        .unwrap();
        assert!(matches!(
            performative(&mut self.peer).await,
            Performative::End(_)
        ));
    }

    pub(super) async fn start(&mut self, delivery: &Delivery) -> (PendingDelivery, u32) {
        let message: Message = crate::message::write_delivery_from(delivery, None).unwrap();
        let token = delivery.lock.unwrap().token;
        let pending = timeout(
            WAIT,
            self.sender
                .as_ref()
                .unwrap()
                .send_pending(message, lock_delivery_tag(token)),
        )
        .await
        .unwrap()
        .unwrap();
        let Frame::Amqp {
            performative: Some(Performative::Transfer(transfer)),
            payload,
            ..
        } = timeout(WAIT, read_frame(&mut self.peer))
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("actual Transfer");
        };
        assert!(!payload.is_empty());
        assert_eq!(
            transfer.delivery_tag.as_ref(),
            Some(&lock_delivery_tag(token))
        );
        (pending, transfer.delivery_id.unwrap())
    }

    pub(super) async fn accepted(&mut self, id: u32) {
        write_frame(
            &mut self.peer,
            &frame(Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled: self.mode == ReceiverSettleMode::First,
                state: Some(amqp::DeliveryState::Accepted(Accepted)),
                batchable: false,
            })),
        )
        .await
        .unwrap();
    }

    pub(super) async fn confirmed(&mut self, id: u32) {
        let Performative::Disposition(disposition) = performative(&mut self.peer).await else {
            panic!("durable settlement confirmation");
        };
        assert_eq!(disposition.role, Role::Sender);
        assert_eq!(disposition.first, id);
        assert!(disposition.settled);
        assert!(matches!(
            disposition.state,
            Some(amqp::DeliveryState::Accepted(_))
        ));
    }

    pub(super) async fn no_queued_frame(&mut self) {
        let reservation = timeout(WAIT, self.sender.as_ref().unwrap().on_credit())
            .await
            .expect("actual native command FIFO barrier")
            .unwrap();
        let mut byte = [0];
        let mut read = Box::pin(self.peer.read(&mut byte));
        pending_once(read.as_mut()).await;
        drop(read);
        timeout(WAIT, reservation.release())
            .await
            .expect("actual reservation release")
            .unwrap();
    }

    pub(super) async fn stop(&mut self) {
        self.connection.stop();
        timeout(WAIT, self.connection.shutdown())
            .await
            .expect("original native tasks joined")
            .unwrap();
    }
}

pub(super) async fn pending_once<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    let polled =
        tokio::task::unconstrained(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))).await;
    assert!(
        matches!(polled, Poll::Pending),
        "positive gate must still hold original work"
    );
}

#[derive(Clone, Copy)]
pub(super) enum EagerTarget {
    Grant,
    Receive,
}

#[derive(Clone, Debug)]
pub(super) enum EagerEvent {
    Invoked(CommandKind),
    Captured(CommandOutcome),
    FuturePolled,
    RawReturned(CommandOutcome),
    Completed(Box<CommandKind>, Result<CommandOutcome, BrokerRejection>),
}

#[derive(Default)]
struct EagerState {
    events: Vec<EagerEvent>,
    allow_invocation: bool,
    allow_result: bool,
}

#[derive(Default)]
struct EagerGates {
    state: Mutex<EagerState>,
    changed: Notify,
    invocation: Condvar,
}

impl EagerGates {
    async fn reached(&self, first_poll: bool) {
        timeout(WAIT, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let reached = self.state.lock().unwrap().events.iter().any(|event| {
                    if first_poll {
                        matches!(event, EagerEvent::FuturePolled)
                    } else {
                        matches!(event, EagerEvent::Captured(_))
                    }
                });
                if reached {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("positive eager acquisition frontier");
    }

    fn release_invocation(&self) {
        self.state.lock().unwrap().allow_invocation = true;
        self.invocation.notify_all();
    }

    fn release_result(&self) {
        self.state.lock().unwrap().allow_result = true;
        self.changed.notify_waiters();
    }

    async fn result_ready(&self) {
        {
            let mut state = self.state.lock().unwrap();
            state.events.push(EagerEvent::FuturePolled);
        }
        self.changed.notify_waiters();
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().unwrap().allow_result {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Clone)]
pub(super) struct EagerAcquisitionBroker {
    actor: Arc<Actor>,
    target: EagerTarget,
    gates: Arc<EagerGates>,
}

impl EagerAcquisitionBroker {
    pub(super) fn new(actor: Arc<Actor>, target: EagerTarget) -> Self {
        Self {
            actor,
            target,
            gates: Arc::new(EagerGates::default()),
        }
    }

    pub(super) fn guard(&self) -> EagerReleaseGuard {
        EagerReleaseGuard {
            actor: Arc::clone(&self.actor),
            gates: Arc::clone(&self.gates),
        }
    }

    pub(super) async fn captured(&self) -> CommandOutcome {
        self.gates.reached(false).await;
        self.events()
            .into_iter()
            .find_map(|event| match event {
                EagerEvent::Captured(outcome) => Some(outcome),
                _ => None,
            })
            .unwrap()
    }

    pub(super) async fn first_poll(&self) {
        self.gates.reached(true).await;
    }

    pub(super) fn release_invocation(&self) {
        self.gates.release_invocation();
    }

    pub(super) fn release_result(&self) {
        self.gates.release_result();
    }

    pub(super) fn events(&self) -> Vec<EagerEvent> {
        self.gates.state.lock().unwrap().events.clone()
    }
}

impl Broker for EagerAcquisitionBroker {
    fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        assert_eq!(namespace, self.actor.namespace);
        assert_eq!(entity, self.actor.entity);
        self.gates
            .state
            .lock()
            .unwrap()
            .events
            .push(EagerEvent::Invoked(kind.clone()));
        let eager = matches!(
            (self.target, &kind),
            (EagerTarget::Grant, CommandKind::AcceptSession { .. })
                | (EagerTarget::Receive, CommandKind::Receive { .. })
        );
        // Actor.intent applies on the real owner thread during method invocation,
        // before a future exists. It intentionally bypasses ActualBroker's log.
        let captured = eager.then(|| {
            let outcome = self.actor.intent(kind.clone());
            let mut state = self.gates.state.lock().unwrap();
            state.events.push(EagerEvent::Captured(outcome.clone()));
            self.gates.changed.notify_waiters();
            while !state.allow_invocation {
                state = self.gates.invocation.wait(state).unwrap();
            }
            outcome
        });
        let actual = self.actor.broker.as_ref().unwrap().clone();
        let gates = Arc::clone(&self.gates);
        async move {
            if let Some(outcome) = captured {
                gates.result_ready().await;
                gates
                    .state
                    .lock()
                    .unwrap()
                    .events
                    .push(EagerEvent::RawReturned(outcome.clone()));
                Ok(outcome)
            } else {
                let result = actual.submit(namespace, entity, kind.clone()).await;
                gates
                    .state
                    .lock()
                    .unwrap()
                    .events
                    .push(EagerEvent::Completed(Box::new(kind), result.clone()));
                result
            }
        }
    }

    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

pub(super) struct EagerReleaseGuard {
    actor: Arc<Actor>,
    gates: Arc<EagerGates>,
}

impl Drop for EagerReleaseGuard {
    fn drop(&mut self) {
        self.actor.gate.release_all();
        self.gates.release_invocation();
        self.gates.release_result();
    }
}

pub(super) fn assert_eager_release(
    events: &[EagerEvent],
    expected: &CommandOutcome,
    hold: &domain::SessionHold,
) -> CommandKind {
    let [
        EagerEvent::Invoked(kind),
        EagerEvent::Captured(captured),
        EagerEvent::FuturePolled,
        EagerEvent::RawReturned(returned),
        EagerEvent::Invoked(release),
        EagerEvent::Completed(completed, result),
    ] = events
    else {
        panic!("one eager result must return before exact cleanup: {events:?}");
    };
    assert_eq!(captured, expected);
    assert_eq!(returned, expected);
    let release_kind = CommandKind::ReleaseSession {
        session: hold.clone(),
    };
    assert_eq!(release, &release_kind);
    assert_eq!(completed.as_ref(), &release_kind);
    assert_eq!(result, &Ok(CommandOutcome::SessionReleased));
    kind.clone()
}

pub(super) fn acquisition_counters(actor: &Actor) -> domain::QueueCounters {
    actor
        .store()
        .get(&domain::keys::queue_counters(
            &actor.namespace,
            &actor.entity,
        ))
        .unwrap()
        .map(|bytes| domain::codec::decode(&bytes).unwrap())
        .unwrap_or_default()
}
