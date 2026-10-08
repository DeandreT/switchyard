use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, Delivery, DeliveryBudget, EntityPath,
    LockToken, NamespaceName, QueueConfig, ReceiveMode, SequenceNumber, SessionHold, SessionId,
    SettlementDisposition, StateMachine, Timestamp, codec, keys,
};
use serde::{Deserialize, Serialize};
use storage::{StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn Error>>;

// Tuple fields deliberately mirror the private postcard sidecars, not a public authority API.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum Owner {
    TrustedUnowned,
    HeldGeneration(LockToken),
}
pub(super) type Row = (
    NamespaceName,
    EntityPath,
    SessionId,
    SequenceNumber,
    Owner,
    LockToken,
    Timestamp,
);
pub(super) type Summary = (
    NamespaceName,
    EntityPath,
    SessionId,
    Option<LockToken>,
    u64,
    u64,
);
pub(super) type Scan = (Vec<u8>, Vec<u8>, usize, usize);
pub(super) type ScanCalls = Arc<Mutex<Vec<Scan>>>;

#[derive(Clone)]
pub(super) struct ObservedStore<S> {
    inner: S,
    pub(super) commits: Arc<AtomicUsize>,
    pub(super) scans: ScanCalls,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
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
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let rows = self.inner.scan_from(prefix, start, limit)?;
        self.scans.lock().expect("scan recorder").push((
            prefix.to_vec(),
            start.to_vec(),
            limit,
            rows.len(),
        ));
        Ok(rows)
    }
}

pub(super) struct Node<P: StoreProvider> {
    pub(super) machine: StateMachine<ObservedStore<P::Store>>,
    pub(super) namespace: NamespaceName,
    pub(super) entity: EntityPath,
    pub(super) commits: Arc<AtomicUsize>,
    pub(super) scans: ScanCalls,
    provider: P,
}

impl<P: StoreProvider> Node<P> {
    pub(super) fn new(provider: P) -> TestResult<Self> {
        let commits = Arc::new(AtomicUsize::new(0));
        let scans = Arc::new(Mutex::new(Vec::new()));
        let node = Self {
            machine: StateMachine::new(ObservedStore {
                inner: provider.open()?,
                commits: commits.clone(),
                scans: scans.clone(),
            }),
            namespace: NamespaceName::new("tenant")?,
            entity: EntityPath::new("orders")?,
            commits,
            scans,
            provider,
        };
        node.create(&node.entity, required())?;
        Ok(node)
    }
    pub(super) fn restart(self) -> TestResult<Self> {
        let Self {
            machine,
            namespace,
            entity,
            commits,
            scans,
            provider,
        } = self;
        drop(machine);
        let machine = StateMachine::new(ObservedStore {
            inner: provider.open()?,
            commits: commits.clone(),
            scans: scans.clone(),
        });
        Ok(Self {
            machine,
            namespace,
            entity,
            commits,
            scans,
            provider,
        })
    }
    pub(super) fn at(&self, at: u64, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.at_entity(&self.entity, at, kind)
    }
    pub(super) fn at_entity(
        &self,
        entity: &EntityPath,
        at: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        ))
    }
    pub(super) fn create(&self, entity: &EntityPath, config: QueueConfig) -> TestResult {
        self.at_entity(
            entity,
            self.machine.last_applied_time()?.as_millis(),
            CommandKind::CreateQueue { config },
        )?;
        Ok(())
    }
    pub(super) fn send(
        &self,
        entity: &EntityPath,
        at: u64,
        name: &str,
        sid: &SessionId,
        ttl: Option<u64>,
    ) -> TestResult<SequenceNumber> {
        let CommandOutcome::Sent { sequence } = self.at_entity(
            entity,
            at,
            CommandKind::Send {
                message_id: name.to_owned(),
                body: vec![1],
                time_to_live_millis: ttl,
                session_id: Some(sid.clone()),
            },
        )?
        else {
            panic!("sent message")
        };
        Ok(sequence)
    }
    pub(super) fn accept(
        &self,
        entity: &EntityPath,
        at: u64,
        sid: &SessionId,
    ) -> TestResult<SessionHold> {
        let CommandOutcome::SessionAccepted(Some(accepted)) = self.at_entity(
            entity,
            at,
            CommandKind::AcceptSession {
                session_id: Some(sid.clone()),
                lock_duration_millis: Some(300_000),
            },
        )?
        else {
            panic!("named hold")
        };
        Ok(accepted.hold())
    }
    pub(super) fn receive(
        &self,
        entity: &EntityPath,
        at: u64,
        hold: Option<SessionHold>,
        duration: u64,
    ) -> TestResult<Delivery> {
        let CommandOutcome::Received(Some(delivery)) = self.at_entity(
            entity,
            at,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: Some(duration),
                session: hold,
            },
        )?
        else {
            panic!("message delivery")
        };
        Ok(delivery)
    }
    pub(super) fn deferred(
        &self,
        at: u64,
        sequences: Vec<SequenceNumber>,
        hold: Option<SessionHold>,
    ) -> TestResult<Vec<Delivery>> {
        let CommandOutcome::DeferredReceived(deliveries) = self.at(
            at,
            CommandKind::ReceiveDeferredHeld {
                sequences,
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: Some(5),
                session: hold,
                budget: unlimited(),
            },
        )?
        else {
            panic!("held deferred batch")
        };
        Ok(deliveries)
    }
    pub(super) fn row(
        &self,
        entity: &EntityPath,
        sequence: SequenceNumber,
    ) -> TestResult<Option<Row>> {
        self.machine
            .store()
            .get(&keys::session_message_lock_reverse(
                &self.namespace,
                entity,
                sequence,
            ))?
            .map(|bytes| Ok(codec::decode(&bytes)?))
            .transpose()
    }
    pub(super) fn summary(
        &self,
        entity: &EntityPath,
        sid: &SessionId,
    ) -> TestResult<Option<Summary>> {
        self.machine
            .store()
            .get(&keys::session_message_lock_summary(
                &self.namespace,
                entity,
                sid,
            ))?
            .map(|bytes| Ok(codec::decode(&bytes)?))
            .transpose()
    }
    pub(super) fn assert_row(
        &self,
        entity: &EntityPath,
        delivery: &Delivery,
        owner: Owner,
    ) -> TestResult {
        let lock = delivery.lock.expect("original message lock");
        let sid = delivery.session_id.clone().expect("original session ID");
        let expected = (
            self.namespace.clone(),
            entity.clone(),
            sid.clone(),
            delivery.sequence,
            owner,
            lock.token,
            lock.locked_until,
        );
        assert_eq!(self.row(entity, delivery.sequence)?, Some(expected.clone()));
        let generation = match owner {
            Owner::TrustedUnowned => None,
            Owner::HeldGeneration(token) => Some(token),
        };
        let bytes = self
            .machine
            .store()
            .get(&keys::session_message_lock_forward(
                &self.namespace,
                entity,
                &sid,
                generation,
                delivery.sequence,
            ))?
            .expect("exact forward row");
        assert_eq!(bytes[0], 11);
        assert_eq!(codec::decode::<Row>(&bytes)?, expected);
        Ok(())
    }
    pub(super) fn assert_counts(
        &self,
        entity: &EntityPath,
        sid: &SessionId,
        generation: Option<LockToken>,
        owned: u64,
        unowned: u64,
    ) -> TestResult {
        let expected = (owned + unowned > 0).then(|| {
            (
                self.namespace.clone(),
                entity.clone(),
                sid.clone(),
                generation,
                owned,
                unowned,
            )
        });
        assert_eq!(self.summary(entity, sid)?, expected);
        Ok(())
    }
    pub(super) fn assert_empty(
        &self,
        entity: &EntityPath,
        sid: &SessionId,
        sequence: SequenceNumber,
    ) -> TestResult {
        assert_eq!(self.row(entity, sequence)?, None);
        assert!(
            self.machine
                .store()
                .scan_prefix(
                    &keys::session_message_lock_forward_prefix(&self.namespace, entity, sid,),
                    1
                )?
                .is_empty()
        );
        self.assert_counts(entity, sid, None, 0, 0)
    }
    pub(super) fn raw(&self, batch: WriteBatch) -> TestResult {
        self.machine.store().inner.apply(batch)?;
        Ok(())
    }
    pub(super) fn reset(&self) {
        self.commits.store(0, Ordering::SeqCst);
        self.scans.lock().expect("scan recorder").clear();
    }
    pub(super) fn refuses(
        &self,
        entity: &EntityPath,
        at: u64,
        kind: CommandKind,
        expected: BrokerError,
    ) -> TestResult {
        let snapshot = self.machine.store().snapshot()?;
        let clock = self.machine.last_applied_time()?;
        self.reset();
        assert_eq!(self.at_entity(entity, at, kind), Err(expected));
        assert_eq!(self.machine.store().snapshot()?, snapshot);
        assert_eq!(self.machine.last_applied_time()?, clock);
        assert_eq!(self.commits.load(Ordering::SeqCst), 0);
        Ok(())
    }
}

pub(super) fn required() -> QueueConfig {
    QueueConfig {
        requires_session: true,
        ..QueueConfig::default()
    }
}
pub(super) fn unlimited() -> DeliveryBudget {
    DeliveryBudget {
        max_bytes: u64::MAX,
        per_message_overhead_bytes: 0,
    }
}
pub(super) fn defer(delivery: &Delivery) -> CommandKind {
    CommandKind::Defer {
        sequence: delivery.sequence,
        lock_token: delivery.lock.expect("original lock").token,
    }
}
pub(super) fn settle(
    delivery: &Delivery,
    hold: Option<SessionHold>,
    disposition: SettlementDisposition,
) -> CommandKind {
    CommandKind::SettleHeld {
        sequence: delivery.sequence,
        lock_token: delivery.lock.expect("original lock").token,
        session: hold,
        disposition,
        properties_to_modify: BTreeMap::new(),
    }
}
pub(super) fn renew(delivery: &Delivery, hold: SessionHold, duration: u64) -> CommandKind {
    CommandKind::RenewLockHeld {
        sequence: delivery.sequence,
        lock_token: delivery.lock.expect("original lock").token,
        session: Some(hold),
        lock_duration_millis: Some(duration),
    }
}
pub(super) fn named(sid: &SessionId) -> CommandKind {
    CommandKind::AcceptSession {
        session_id: Some(sid.clone()),
        lock_duration_millis: None,
    }
}
