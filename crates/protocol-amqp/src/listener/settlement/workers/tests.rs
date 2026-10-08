//! Original settlement workers on real native links and the actual broker owner.

use std::{
    collections::HashSet,
    future::{Future, poll_fn},
    io,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use amqp::{
    Accepted, Attach, Begin, Detach, Disposition, Flow, Frame, LinkEndpoint, Message, Open,
    PendingDelivery, Performative, ProtocolHeader, ReceiverSettleMode, Role, Sender,
    SenderSettleMode, ServerConnection, ServerSession, Source, read_frame, read_protocol_header,
    write_frame, write_protocol_header,
};
use domain::{
    CommandKind, CommandOutcome, Delivery, EntityPath, NamespaceName, QueueConfig, ReceiveMode,
    SequenceNumber, SessionHold, SessionId, StateMachine,
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

use super::*;
use crate::listener::{
    ReceivingLinkProtocol,
    settlement::{
        SettlementContext, lock_delivery_tag, serve_receiving_client, settle_started_delivery,
    },
};
use crate::management::ConnectionManagement;
use crate::{Broker, BrokerRejection};

const WAIT: Duration = Duration::from_secs(5);
const CHANNEL: u16 = 1;
const HANDLE: u32 = 1;
const LINK: &str = "owned-settlement";

#[derive(Default)]
struct CommitState {
    key: Vec<u8>,
    before: bool,
    after: bool,
    allow_before: bool,
    allow_after: bool,
    commits: usize,
}

#[derive(Default)]
struct CommitGate {
    state: Mutex<CommitState>,
    released: Condvar,
    changed: Notify,
}

impl CommitGate {
    fn arm(&self, key: Vec<u8>) {
        *self.state.lock().unwrap() = CommitState {
            key,
            ..CommitState::default()
        };
    }

    fn matches(&self, batch: &WriteBatch) -> bool {
        let state = self.state.lock().unwrap();
        !state.key.is_empty()
            && batch
                .mutations()
                .iter()
                .any(|mutation| matches!(mutation, Mutation::Delete { key } if *key == state.key))
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

    async fn reached(&self, after: bool) {
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

    fn release(&self, after: bool) {
        let mut state = self.state.lock().unwrap();
        if after {
            state.allow_after = true;
        } else {
            state.allow_before = true;
        }
        self.released.notify_all();
    }

    fn release_all(&self) {
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
struct GatedStore {
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
struct Submission {
    worker: Id,
    kind: CommandKind,
    returned: bool,
}

#[derive(Clone)]
struct ActualBroker {
    handle: server::BrokerHandle,
    log: Arc<Mutex<Vec<Submission>>>,
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

struct Actor {
    owner: Option<server::Broker>,
    broker: Option<ActualBroker>,
    store: Option<GatedStore>,
    memory: Option<MemoryStore>,
    directory: Option<tempfile::TempDir>,
    gate: Arc<CommitGate>,
    log: Arc<Mutex<Vec<Submission>>>,
    namespace: NamespaceName,
    entity: EntityPath,
}

impl Actor {
    fn new(durable: bool, session: bool) -> Self {
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
        let owner = server::Broker::spawn(server::LocalProposer::new(
            StateMachine::new(store.clone()),
            server::ManualClock::at(1_000),
        ));
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = ActualBroker {
            handle: owner.handle(),
            log: Arc::clone(&log),
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

    fn intent(&self, kind: CommandKind) -> CommandOutcome {
        self.broker
            .as_ref()
            .unwrap()
            .handle
            .submit_blocking(self.namespace.clone(), self.entity.clone(), kind)
            .unwrap()
    }

    fn send(&self, marker: &str, session: Option<SessionId>) -> SequenceNumber {
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

    fn receive(&self) -> Delivery {
        let CommandOutcome::Received(Some(delivery)) = self.intent(CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        }) else {
            panic!("receive committed a lock");
        };
        delivery
    }

    fn key(&self, sequence: SequenceNumber) -> Vec<u8> {
        domain::keys::message(&self.namespace, &self.entity, sequence)
    }

    fn store(&self) -> &GatedStore {
        self.store.as_ref().unwrap()
    }

    fn context(&self, management: Arc<ConnectionManagement>) -> SettlementContext<ActualBroker> {
        SettlementContext {
            namespace: self.namespace.clone(),
            entity: self.entity.clone(),
            broker: self.broker.as_ref().unwrap().clone(),
            authorization: None,
            management,
            link_name: LINK.to_owned(),
        }
    }

    fn complete_count(&self) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|submission| {
                !submission.returned && matches!(submission.kind, CommandKind::Complete { .. })
            })
            .count()
    }

    fn reopen(&mut self) {
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
}

#[derive(Default)]
struct WriteGate {
    state: Mutex<WriteState>,
    changed: Notify,
}

impl WriteGate {
    fn block(&self) {
        self.state.lock().unwrap().blocked = true;
    }

    async fn reached(&self) {
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

fn frame(performative: Performative) -> Frame {
    Frame::Amqp {
        channel: CHANNEL,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

async fn performative(peer: &mut DuplexStream) -> Performative {
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

struct Wire {
    connection: ServerConnection,
    _session: ServerSession,
    sender: Option<Sender>,
    peer: DuplexStream,
    writes: Arc<WriteGate>,
    mode: ReceiverSettleMode,
}

impl Wire {
    async fn new(mode: ReceiverSettleMode) -> Self {
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
        write_frame(
            &mut peer,
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
        let Performative::Attach(response) = performative(&mut peer).await else {
            panic!("Attach response");
        };
        assert_eq!(response.rcv_settle_mode, mode);
        write_frame(
            &mut peer,
            &frame(Performative::Flow(Flow {
                handle: Some(HANDLE),
                delivery_count: Some(0),
                link_credit: Some(64),
                incoming_window: 2048,
                outgoing_window: 2048,
                ..Flow::default()
            })),
        )
        .await
        .unwrap();
        Self {
            connection,
            _session: session,
            sender: Some(sender),
            peer,
            writes,
            mode,
        }
    }

    async fn start(&mut self, delivery: &Delivery) -> (PendingDelivery, u32) {
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

    async fn accepted(&mut self, id: u32) {
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

    async fn confirmed(&mut self, id: u32) {
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

    async fn no_queued_frame(&mut self) {
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

    async fn stop(&mut self) {
        self.connection.stop();
        timeout(WAIT, self.connection.shutdown())
            .await
            .expect("original native tasks joined")
            .unwrap();
    }
}

async fn pending_once<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    let polled =
        tokio::task::unconstrained(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))).await;
    assert!(
        matches!(polled, Poll::Pending),
        "positive gate must still hold original work"
    );
}

async fn start_worker(
    workers: &mut SettlementWorkers,
    wire: &mut Wire,
    actor: &Actor,
    delivery: Delivery,
    management: Arc<ConnectionManagement>,
) -> (Id, u32) {
    let token = delivery.lock.unwrap().token;
    management
        .register_delivery(LINK, actor.entity.clone(), delivery.sequence, token)
        .await;
    let (pending, wire_id) = wire.start(&delivery).await;
    let retired = workers.subscribe();
    let id = workers.spawn(
        Some(token),
        settle_started_delivery(pending, delivery, actor.context(management), retired),
    );
    (id, wire_id)
}

fn assert_original_finished(workers: &SettlementWorkers, id: Id, token: domain::LockToken) {
    assert!(workers.is_empty());
    assert_eq!(workers.finished().len(), 1);
    let joined = &workers.finished()[0];
    assert_eq!(joined.id, id);
    assert_eq!(joined.lock_token, Some(token));
    let completion = joined
        .result
        .as_ref()
        .expect("original worker joined normally");
    assert_eq!(completion.lock_token, Some(token));
    assert!(completion.result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn unanswered_actual_outcomes_retire_without_new_settlement_commands() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut actor = Actor::new(durable, false);
            actor.send("unanswered", None);
            let delivery = actor.receive();
            let token = delivery.lock.unwrap().token;
            let before = actor.store().snapshot().unwrap();
            let mut wire = Wire::new(mode).await;
            let mut workers = SettlementWorkers::new();
            let (id, _) = start_worker(
                &mut workers,
                &mut wire,
                &actor,
                delivery,
                ConnectionManagement::new(),
            )
            .await;
            let mut observer = Box::pin(workers.finish());
            pending_once(observer.as_mut()).await;
            drop(observer);
            assert_eq!(workers.len(), 1);
            assert_eq!(workers.pending.iter().next().unwrap().id, id);
            timeout(WAIT, workers.finish()).await.unwrap();
            assert_original_finished(&workers, id, token);
            assert_eq!(actor.complete_count(), 0);
            assert_eq!(actor.store().snapshot().unwrap(), before);
            timeout(WAIT, workers.finish()).await.unwrap();
            assert_original_finished(&workers, id, token);
            wire.stop().await;
            drop(workers);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_broker_submission_drains_across_cancelled_finish_and_reopen() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            for after_commit in [false, true] {
                let mut actor = Actor::new(durable, false);
                actor.send("commit", None);
                let delivery = actor.receive();
                let sequence = delivery.sequence;
                let token = delivery.lock.unwrap().token;
                let key = actor.key(sequence);
                let lock_key = domain::keys::lock(
                    &actor.namespace,
                    &actor.entity,
                    delivery.lock.unwrap().locked_until,
                    sequence,
                );
                let before = actor.store().snapshot().unwrap();
                let expected: Vec<_> = before
                    .entries()
                    .iter()
                    .filter(|(stored, _)| stored != &key && stored != &lock_key)
                    .cloned()
                    .collect();
                actor.gate.arm(key.clone());
                let mut wire = Wire::new(mode.clone()).await;
                let mut workers = SettlementWorkers::new();
                let (id, wire_id) = start_worker(
                    &mut workers,
                    &mut wire,
                    &actor,
                    delivery,
                    ConnectionManagement::new(),
                )
                .await;
                wire.accepted(wire_id).await;
                actor.gate.reached(false).await;
                if after_commit {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                    assert!(actor.store().get(&key).unwrap().is_none());
                } else {
                    assert_eq!(actor.store().snapshot().unwrap(), before);
                }
                assert_eq!(actor.complete_count(), 1);
                {
                    let log = actor.log.lock().unwrap();
                    assert!(
                        log.iter()
                            .any(|entry| entry.worker == id && !entry.returned)
                    );
                    assert!(!log.iter().any(|entry| entry.returned));
                }
                wire.no_queued_frame().await;
                let mut observer = Box::pin(workers.finish());
                pending_once(observer.as_mut()).await;
                drop(observer);
                assert_eq!(workers.len(), 1);
                assert_eq!(workers.pending.iter().next().unwrap().id, id);
                assert!(workers.finished().is_empty());
                if !after_commit {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                }
                actor.gate.release(true);
                timeout(WAIT, workers.finish()).await.unwrap();
                assert_original_finished(&workers, id, token);
                wire.no_queued_frame().await;
                assert_eq!(actor.complete_count(), 1);
                assert_eq!(actor.gate.state.lock().unwrap().commits, 1);
                assert!(
                    actor
                        .log
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|entry| entry.worker == id && entry.returned)
                );
                let committed = actor.store().snapshot().unwrap();
                assert_eq!(committed.entries(), expected.as_slice());
                assert!(actor.store().get(&key).unwrap().is_none());
                timeout(WAIT, workers.finish()).await.unwrap();
                assert_original_finished(&workers, id, token);
                wire.stop().await;
                drop(workers);
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), committed);
                assert!(actor.store().get(&key).unwrap().is_none());
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_second_confirmation_write_retires_without_resubmission() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        actor.send("confirmation", None);
        let delivery = actor.receive();
        let token = delivery.lock.unwrap().token;
        let key = actor.key(delivery.sequence);
        actor.gate.arm(key.clone());
        let mut wire = Wire::new(ReceiverSettleMode::Second).await;
        let mut workers = SettlementWorkers::new();
        let (id, wire_id) = start_worker(
            &mut workers,
            &mut wire,
            &actor,
            delivery,
            ConnectionManagement::new(),
        )
        .await;
        wire.accepted(wire_id).await;
        actor.gate.reached(false).await;
        actor.gate.release(false);
        actor.gate.reached(true).await;
        wire.writes.block();
        actor.gate.release(true);
        wire.writes.reached().await;
        assert_eq!(actor.complete_count(), 1);
        assert!(actor.store().get(&key).unwrap().is_none());
        timeout(WAIT, workers.finish()).await.unwrap();
        assert_original_finished(&workers, id, token);
        assert_eq!(actor.complete_count(), 1);
        let committed = actor.store().snapshot().unwrap();
        wire.stop().await;
        drop(workers);
        actor.reopen();
        assert_eq!(actor.store().snapshot().unwrap(), committed);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_ready_remote_outcome_wins_sticky_retirement() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut actor = Actor::new(durable, false);
            actor.send("ready-frontier", None);
            let delivery = actor.receive();
            let token = delivery.lock.unwrap().token;
            let key = actor.key(delivery.sequence);
            let mut wire = Wire::new(mode).await;
            let management = ConnectionManagement::new();
            management
                .register_delivery(LINK, actor.entity.clone(), delivery.sequence, token)
                .await;
            // Retain the genuine PendingDelivery without polling its outcome.
            let (pending, wire_id) = wire.start(&delivery).await;
            wire.accepted(wire_id).await;
            write_frame(
                &mut wire.peer,
                &frame(Performative::Flow(Flow {
                    handle: Some(HANDLE),
                    delivery_count: Some(1),
                    link_credit: Some(0),
                    drain: true,
                    incoming_window: 2048,
                    outgoing_window: 2048,
                    ..Flow::default()
                })),
            )
            .await
            .unwrap();
            let Performative::Flow(drained) = performative(&mut wire.peer).await else {
                panic!("positive native drain response");
            };
            assert_eq!(drained.handle, Some(HANDLE));
            assert_eq!(drained.delivery_count, Some(1));
            assert_eq!(drained.link_credit, Some(0));
            assert!(drained.drain);
            // The engine processed Accepted before this actual Flow response.
            let mut workers = SettlementWorkers::new();
            let retirement = workers.subscribe();
            let id = workers.spawn(
                Some(token),
                settle_started_delivery(pending, delivery, actor.context(management), retirement),
            );
            workers.retire();
            timeout(WAIT, workers.finish()).await.unwrap();
            assert_original_finished(&workers, id, token);
            assert_eq!(actor.complete_count(), 1);
            assert!(actor.store().get(&key).unwrap().is_none());
            let committed = actor.store().snapshot().unwrap();
            wire.stop().await;
            drop(workers);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), committed);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn later_delivery_settles_while_an_earlier_remote_outcome_is_unanswered() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut actor = Actor::new(durable, false);
            actor.send("earlier", None);
            actor.send("later", None);
            let earlier = actor.receive();
            let later = actor.receive();
            let earlier_key = actor.key(earlier.sequence);
            let later_key = actor.key(later.sequence);
            let mut wire = Wire::new(mode).await;
            let mut workers = SettlementWorkers::new();
            start_worker(
                &mut workers,
                &mut wire,
                &actor,
                earlier,
                ConnectionManagement::new(),
            )
            .await;
            let (_, wire_id) = start_worker(
                &mut workers,
                &mut wire,
                &actor,
                later,
                ConnectionManagement::new(),
            )
            .await;
            wire.accepted(wire_id).await;
            let completion = timeout(WAIT, workers.next()).await.unwrap().unwrap();
            assert!(completion.result.is_ok());
            if wire.mode == ReceiverSettleMode::Second {
                wire.confirmed(wire_id).await;
            }
            assert_eq!(workers.len(), 1);
            assert!(actor.store().get(&earlier_key).unwrap().is_some());
            assert!(actor.store().get(&later_key).unwrap().is_none());
            assert_eq!(actor.complete_count(), 1);
            timeout(WAIT, workers.finish()).await.unwrap();
            assert_eq!(actor.complete_count(), 1);
            wire.stop().await;
            drop(workers);
            actor.reopen();
            assert!(actor.store().get(&earlier_key).unwrap().is_some());
            assert!(actor.store().get(&later_key).unwrap().is_none());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn original_real_worker_panic_retains_token_id_and_payload_through_next() {
    let actor = Actor::new(false, false);
    actor.send("aborted-worker", None);
    let delivery = actor.receive();
    let token = delivery.lock.unwrap().token;
    let mut wire = Wire::new(ReceiverSettleMode::Second).await;
    let mut workers = SettlementWorkers::new();
    let (pending, wire_id) = wire.start(&delivery).await;
    let retirement = workers.subscribe();
    let context = actor.context(ConnectionManagement::new());
    let id = workers.spawn(Some(token), async move {
        settle_started_delivery(pending, delivery, context, retirement).await?;
        panic!("original-settlement-panic");
    });
    wire.accepted(wire_id).await;
    wire.confirmed(wire_id).await;
    let completion = timeout(WAIT, workers.next()).await.unwrap().unwrap();
    assert!(completion.lock_token.is_none());
    assert!(matches!(
        &completion.result,
        Err(super::super::SettlementFailure::Engine(
            amqp::EngineError::Stopped
        ))
    ));
    let mut registered_locks = HashSet::from([token]);
    assert!(matches!(
        super::super::handle_completion(completion, &mut registered_locks),
        Some(super::super::PumpExit::Clean)
    ));
    assert!(registered_locks.contains(&token));
    assert_eq!(workers.failures().len(), 1);
    let failure = &workers.failures()[0];
    assert_eq!(failure.id, id);
    assert_eq!(failure.lock_token, Some(token));
    assert_eq!(failure.error.id(), id);
    assert!(failure.error.is_panic());
    timeout(WAIT, workers.finish()).await.unwrap();
    assert_eq!(workers.failures()[0].id, id);
    let error = workers.into_join_error().expect("original raw panic");
    assert_eq!(error.id(), id);
    let payload = error.into_panic();
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"original-settlement-panic")
    );
    assert_eq!(actor.complete_count(), 1);
    wire.stop().await;
}

async fn originals_completed(workers: &SettlementWorkers) {
    timeout(WAIT, async {
        while workers
            .pending
            .iter()
            .any(|task| !task.handle.is_finished())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual original tasks reached completion");
}

#[tokio::test(flavor = "current_thread")]
async fn finish_retains_a_completed_original_before_a_cancelled_pending_join() {
    let actor = Actor::new(false, false);
    actor.send("already-completed", None);
    actor.send("still-submitted", None);
    let completed = actor.receive();
    let submitted = actor.receive();
    actor.gate.arm(actor.key(submitted.sequence));
    let mut wire = Wire::new(ReceiverSettleMode::First).await;
    let mut workers = SettlementWorkers::new();
    let (completed_id, wire_id) = start_worker(
        &mut workers,
        &mut wire,
        &actor,
        completed,
        ConnectionManagement::new(),
    )
    .await;
    wire.accepted(wire_id).await;
    originals_completed(&workers).await;
    let (submitted_id, wire_id) = start_worker(
        &mut workers,
        &mut wire,
        &actor,
        submitted,
        ConnectionManagement::new(),
    )
    .await;
    wire.accepted(wire_id).await;
    actor.gate.reached(false).await;
    let mut observer = Box::pin(workers.finish());
    pending_once(observer.as_mut()).await;
    drop(observer);
    assert_eq!(workers.len(), 1);
    assert_eq!(workers.pending.iter().next().unwrap().id, submitted_id);
    assert_eq!(workers.finished().len(), 1);
    assert_eq!(workers.finished()[0].id, completed_id);
    assert!(
        workers.finished()[0]
            .result
            .as_ref()
            .unwrap()
            .result
            .is_ok()
    );
    actor.gate.release_all();
    timeout(WAIT, workers.finish()).await.unwrap();
    assert_eq!(workers.finished().len(), 2);
    assert_eq!(workers.finished()[0].id, completed_id);
    assert_eq!(workers.finished()[1].id, submitted_id);
    assert!(
        workers
            .finished()
            .iter()
            .all(|joined| joined.result.as_ref().unwrap().result.is_ok())
    );
    timeout(WAIT, workers.finish()).await.unwrap();
    assert_eq!(workers.finished().len(), 2);
    assert_eq!(actor.complete_count(), 2);
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn thirty_two_completed_unreaped_real_workers_still_consume_the_bound() {
    let actor = Actor::new(false, false);
    for marker in 0..33 {
        actor.send(&format!("bounded-{marker}"), None);
    }
    let mut wire = Wire::new(ReceiverSettleMode::First).await;
    let mut workers = SettlementWorkers::new();
    let mut ids = Vec::new();
    for _ in 0..32 {
        let (id, wire_id) = start_worker(
            &mut workers,
            &mut wire,
            &actor,
            actor.receive(),
            ConnectionManagement::new(),
        )
        .await;
        ids.push(id);
        wire.accepted(wire_id).await;
    }
    originals_completed(&workers).await;
    assert_eq!(workers.len(), 32);
    let extra = actor.receive();
    let key = actor.key(extra.sequence);
    let (pending, _) = wire.start(&extra).await;
    let token = extra.lock.unwrap().token;
    let retirement = workers.subscribe();
    let context = actor.context(ConnectionManagement::new());
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        workers.spawn(
            Some(token),
            settle_started_delivery(pending, extra, context, retirement),
        );
    }));
    assert!(
        refused.is_err(),
        "completed originals must be reaped before slot reuse"
    );
    assert_eq!(workers.len(), 32);
    timeout(WAIT, workers.finish()).await.unwrap();
    assert!(workers.is_empty());
    assert_eq!(workers.finished().len(), 32);
    assert!(
        workers
            .finished()
            .iter()
            .all(|joined| ids.contains(&joined.id))
    );
    assert_eq!(actor.complete_count(), 32);
    assert!(actor.store().get(&key).unwrap().is_some());
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn natural_pump_joins_commit_before_releasing_its_held_session() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut actor = Actor::new(durable, true);
            let session_id = SessionId::new("session").unwrap();
            let sequence = actor.send("pump-commit", Some(session_id.clone()));
            actor.send("pump-unanswered", Some(session_id.clone()));
            let CommandOutcome::SessionAccepted(Some(accepted)) =
                actor.intent(CommandKind::AcceptSession {
                    session_id: Some(session_id),
                    lock_duration_millis: None,
                })
            else {
                panic!("session accepted by actual broker");
            };
            let hold: SessionHold = accepted.hold();
            actor.gate.arm(actor.key(sequence));
            let mut wire = Wire::new(mode).await;
            let management = ConnectionManagement::new();
            let sender = wire.sender.take().unwrap();
            let broker = actor.broker.as_ref().unwrap().clone();
            let namespace = actor.namespace.clone();
            let entity = actor.entity.clone();
            let protocol = ReceivingLinkProtocol {
                authorization: None,
                management: Arc::clone(&management),
            };
            let mut pump = tokio::spawn(serve_receiving_client(
                sender,
                namespace,
                entity,
                broker,
                ReceiveMode::PeekLock,
                Some(hold),
                protocol,
            ));
            let mut ids = Vec::new();
            let mut unanswered_token = None;
            for _ in 0..2 {
                let Frame::Amqp {
                    performative: Some(Performative::Transfer(transfer)),
                    ..
                } = timeout(WAIT, read_frame(&mut wire.peer))
                    .await
                    .unwrap()
                    .unwrap()
                else {
                    panic!("actual pump Transfer");
                };
                ids.push(transfer.delivery_id.unwrap());
                let tag: &[u8] = transfer.delivery_tag.as_ref().unwrap().as_ref();
                unanswered_token = Some(domain::LockToken::new(u64::from_be_bytes(
                    tag[8..].try_into().unwrap(),
                )));
            }
            let unanswered_token = unanswered_token.unwrap();
            assert!(management.delivery(LINK, unanswered_token).await.is_some());
            wire.accepted(ids[0]).await;
            actor.gate.reached(false).await;
            write_frame(
                &mut wire.peer,
                &frame(Performative::Detach(Detach {
                    handle: HANDLE,
                    closed: true,
                    error: None,
                })),
            )
            .await
            .unwrap();
            assert!(matches!(
                performative(&mut wire.peer).await,
                Performative::Detach(_)
            ));
            pending_once(Pin::new(&mut pump)).await;
            assert!(
                !actor
                    .log
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|entry| matches!(entry.kind, CommandKind::ReleaseSession { .. }))
            );
            actor.gate.release(false);
            actor.gate.reached(true).await;
            pending_once(Pin::new(&mut pump)).await;
            assert!(
                !actor
                    .log
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|entry| matches!(entry.kind, CommandKind::ReleaseSession { .. }))
            );
            actor.gate.release(true);
            timeout(WAIT, &mut pump)
                .await
                .expect("natural original pump finished")
                .unwrap()
                .unwrap();
            assert!(management.delivery(LINK, unanswered_token).await.is_none());
            {
                let log = actor.log.lock().unwrap();
                let committed = log
                    .iter()
                    .position(|entry| {
                        entry.returned && matches!(entry.kind, CommandKind::Complete { .. })
                    })
                    .unwrap();
                let release = log
                    .iter()
                    .position(|entry| {
                        !entry.returned && matches!(entry.kind, CommandKind::ReleaseSession { .. })
                    })
                    .unwrap();
                assert!(committed < release);
            }
            assert_eq!(actor.complete_count(), 1);
            let committed = actor.store().snapshot().unwrap();
            wire.stop().await;
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), committed);
        }
    }
}
