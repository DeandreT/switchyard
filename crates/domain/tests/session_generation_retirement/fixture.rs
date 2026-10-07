use std::{
    collections::BTreeMap,
    error::Error,
    sync::{Arc, Mutex},
};

use domain::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::{Mutation, StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn Error>>;

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

#[derive(Clone, Debug, Default)]
pub(super) struct Reads {
    pub(super) gets: Vec<Vec<u8>>,
    pub(super) values: Vec<(Vec<u8>, usize)>,
    pub(super) scan_value_bytes: usize,
    pub(super) scans: Vec<(Vec<u8>, Vec<u8>, usize, usize)>,
    pub(super) commits: Vec<WriteBatch>,
}
#[derive(Clone)]
pub(super) struct Observed<S> {
    inner: S,
    reads: Arc<Mutex<Reads>>,
}
impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.reads
            .lock()
            .expect("read recorder")
            .gets
            .push(key.to_vec());
        let result = self.inner.get(key)?;
        self.reads
            .lock()
            .expect("value recorder")
            .values
            .push((key.to_vec(), result.as_ref().map_or(0, Vec::len)));
        Ok(result)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let result = self.inner.scan_from(prefix, start, limit)?;
        self.reads
            .lock()
            .expect("scan value recorder")
            .scan_value_bytes += result.iter().map(|(_, value)| value.len()).sum::<usize>();
        self.reads.lock().expect("scan recorder").scans.push((
            prefix.to_vec(),
            start.to_vec(),
            limit,
            result.len(),
        ));
        Ok(result)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.reads
            .lock()
            .expect("commit recorder")
            .commits
            .push(batch.clone());
        self.inner.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
}

pub(super) struct Node<P: StoreProvider> {
    pub(super) machine: StateMachine<Observed<P::Store>>,
    pub(super) namespace: NamespaceName,
    pub(super) entity: EntityPath,
    reads: Arc<Mutex<Reads>>,
    provider: P,
}
impl<P: StoreProvider> Node<P> {
    pub(super) fn new(provider: P) -> TestResult<Self> {
        let reads = Arc::new(Mutex::new(Reads::default()));
        let node = Self {
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                reads: reads.clone(),
            }),
            namespace: NamespaceName::new("tenant")?,
            entity: EntityPath::new("orders")?,
            reads,
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
            reads,
            provider,
        } = self;
        drop(machine);
        let machine = StateMachine::new(Observed {
            inner: provider.open()?,
            reads: reads.clone(),
        });
        Ok(Self {
            machine,
            namespace,
            entity,
            reads,
            provider,
        })
    }
    pub(super) fn command(&self, entity: &EntityPath, at: u64, kind: CommandKind) -> Command {
        Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        )
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
        self.machine.apply(&self.command(entity, at, kind))
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
        sid: Option<SessionId>,
        id: &str,
    ) -> TestResult<SequenceNumber> {
        self.send_ttl(entity, sid, id, None)
    }
    pub(super) fn send_ttl(
        &self,
        entity: &EntityPath,
        sid: Option<SessionId>,
        id: &str,
        ttl: Option<u64>,
    ) -> TestResult<SequenceNumber> {
        let CommandOutcome::Sent { sequence } = self.at_entity(
            entity,
            10,
            CommandKind::SendEnvelope {
                message_id: id.to_owned(),
                body: vec![1, 2, 3],
                time_to_live_millis: ttl,
                session_id: sid,
                envelope: Box::new(MessageEnvelope {
                    body: MessageBody::Data(vec![vec![1, 2, 3]]),
                    application_properties: BTreeMap::from([(
                        "kept".to_owned(),
                        MessageValue::String("original".to_owned()),
                    )]),
                    ..MessageEnvelope::default()
                }),
            },
        )?
        else {
            panic!("sent original message")
        };
        Ok(sequence)
    }
    pub(super) fn accept(
        &self,
        entity: &EntityPath,
        sid: &SessionId,
        duration: u64,
    ) -> TestResult<SessionHold> {
        let CommandOutcome::SessionAccepted(Some(accepted)) = self.at_entity(
            entity,
            10,
            CommandKind::AcceptSession {
                session_id: Some(sid.clone()),
                lock_duration_millis: Some(duration),
            },
        )?
        else {
            panic!("actual session grant")
        };
        Ok(accepted.hold())
    }
    pub(super) fn receive(
        &self,
        entity: &EntityPath,
        hold: Option<SessionHold>,
    ) -> TestResult<Delivery> {
        let CommandOutcome::Received(Some(delivery)) = self.at_entity(
            entity,
            10,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: Some(200_000),
                session: hold,
            },
        )?
        else {
            panic!("original held delivery")
        };
        Ok(delivery)
    }
    pub(super) fn owned(
        &self,
        sid: &SessionId,
        count: usize,
        duration: u64,
    ) -> TestResult<(SessionHold, Vec<Delivery>)> {
        for i in 0..count {
            self.send(
                &self.entity,
                Some(sid.clone()),
                &format!("{}-{i}", sid.as_str()),
            )?;
        }
        let hold = self.accept(&self.entity, sid, duration)?;
        let mut deliveries = Vec::new();
        for _ in 0..count {
            deliveries.push(self.receive(&self.entity, Some(hold.clone()))?);
        }
        Ok((hold, deliveries))
    }
    pub(super) fn other_namespace_owned(
        &self,
        sid: &SessionId,
    ) -> TestResult<(NamespaceName, Delivery, MessageRecord)> {
        let namespace = NamespaceName::new("other")?;
        let command = |kind| {
            Command::new(
                namespace.clone(),
                self.entity.clone(),
                Timestamp::from_millis(10),
                kind,
            )
        };
        self.machine
            .apply(&command(CommandKind::CreateQueue { config: required() }))?;
        self.machine.apply(&command(CommandKind::Send {
            message_id: "foreign original".to_owned(),
            body: vec![8],
            time_to_live_millis: None,
            session_id: Some(sid.clone()),
        }))?;
        let CommandOutcome::SessionAccepted(Some(accepted)) =
            self.machine.apply(&command(CommandKind::AcceptSession {
                session_id: Some(sid.clone()),
                lock_duration_millis: Some(100),
            }))?
        else {
            panic!("foreign actual grant")
        };
        let CommandOutcome::Received(Some(delivery)) =
            self.machine.apply(&command(CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: Some(200_000),
                session: Some(accepted.hold()),
            }))?
        else {
            panic!("foreign original delivery")
        };
        let record = self
            .machine
            .message(&namespace, &self.entity, delivery.sequence)?
            .expect("foreign record");
        Ok((namespace, delivery, record))
    }
    pub(super) fn release(&self, hold: &SessionHold) -> TestResult {
        assert_eq!(
            self.at(
                11,
                CommandKind::ReleaseSession {
                    session: hold.clone()
                }
            )?,
            CommandOutcome::SessionReleased
        );
        Ok(())
    }
    pub(super) fn retire(
        &self,
        at: u64,
        after: Option<SessionRetirementCursor>,
    ) -> TestResult<SessionRetirementOutcome> {
        self.retire_entity(&self.entity, at, after)
    }
    pub(super) fn retire_entity(
        &self,
        entity: &EntityPath,
        at: u64,
        after: Option<SessionRetirementCursor>,
    ) -> TestResult<SessionRetirementOutcome> {
        let CommandOutcome::SessionRetired(outcome) =
            self.at_entity(entity, at, retirement(after))?
        else {
            panic!("retirement result")
        };
        Ok(outcome)
    }
    pub(super) fn record(
        &self,
        entity: &EntityPath,
        sequence: SequenceNumber,
    ) -> TestResult<MessageRecord> {
        Ok(self
            .machine
            .message(&self.namespace, entity, sequence)?
            .expect("original message record"))
    }
    pub(super) fn row(&self, sequence: SequenceNumber) -> TestResult<Row> {
        let bytes = self
            .machine
            .store()
            .get(&keys::session_message_lock_reverse(
                &self.namespace,
                &self.entity,
                sequence,
            ))?
            .expect("reverse row");
        Ok(codec::decode(&bytes)?)
    }
    pub(super) fn summary(&self, sid: &SessionId) -> TestResult<Option<Summary>> {
        self.machine
            .store()
            .get(&keys::session_message_lock_summary(
                &self.namespace,
                &self.entity,
                sid,
            ))?
            .map(|b| Ok(codec::decode(&b)?))
            .transpose()
    }
    pub(super) fn forward(&self, row: &Row) -> Vec<u8> {
        let generation = match row.4 {
            Owner::TrustedUnowned => None,
            Owner::HeldGeneration(token) => Some(token),
        };
        keys::session_message_lock_forward(&row.0, &row.1, &row.2, generation, row.3)
    }
    pub(super) fn put_record(&self, record: &MessageRecord) -> TestResult {
        self.raw(WriteBatch::default().put(
            keys::message(&self.namespace, &self.entity, record.sequence),
            codec::encode(record)?,
        ))
    }
    pub(super) fn raw(&self, batch: WriteBatch) -> TestResult {
        self.machine.store().inner.apply(batch)?;
        Ok(())
    }
    pub(super) fn restore(&self, snapshot: &StoreSnapshot) -> TestResult {
        let mut batch = WriteBatch::default();
        for (key, _) in self.snapshot()?.entries() {
            batch.push_delete(key.clone());
        }
        for (key, value) in snapshot.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        self.raw(batch)
    }
    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.machine.store().snapshot()?)
    }
    pub(super) fn reset(&self) {
        *self.reads.lock().expect("reset recorder") = Reads::default();
    }
    pub(super) fn reads(&self) -> Reads {
        self.reads.lock().expect("read recorder").clone()
    }
    pub(super) fn refuses(&self, at: u64, kind: CommandKind, expected: BrokerError) -> TestResult {
        let snapshot = self.snapshot()?;
        let clock = self.machine.last_applied_time()?;
        self.reset();
        assert_eq!(self.at(at, kind), Err(expected));
        assert_eq!(self.snapshot()?, snapshot);
        assert_eq!(self.machine.last_applied_time()?, clock);
        assert!(self.reads().commits.is_empty());
        Ok(())
    }
    pub(super) fn assert_ready(&self, delivery: &Delivery, before: &MessageRecord) -> TestResult {
        let mut expected = before.clone();
        expected.state = MessageState::Ready;
        assert_eq!(self.record(&self.entity, delivery.sequence)?, expected);
        assert!(
            self.machine
                .store()
                .get(&keys::session_message_lock_reverse(
                    &self.namespace,
                    &self.entity,
                    delivery.sequence
                ))?
                .is_none()
        );
        let lock = delivery.lock.expect("original message lock");
        assert!(
            self.machine
                .store()
                .get(&keys::lock(
                    &self.namespace,
                    &self.entity,
                    lock.locked_until,
                    delivery.sequence
                ))?
                .is_none()
        );
        assert_eq!(
            self.machine.store().get(&keys::session_ready(
                &self.namespace,
                &self.entity,
                delivery.session_id.as_ref().expect("ID"),
                delivery.sequence
            ))?,
            Some(Vec::new())
        );
        Ok(())
    }
}

pub(super) fn required() -> QueueConfig {
    QueueConfig {
        requires_session: true,
        ..QueueConfig::default()
    }
}
pub(super) fn retirement(after: Option<SessionRetirementCursor>) -> CommandKind {
    CommandKind::RetireSessionGenerationPage { after }
}
pub(super) fn renewal(delivery: &Delivery, token: LockToken) -> CommandKind {
    CommandKind::RenewLock {
        sequence: delivery.sequence,
        lock_token: token,
        lock_duration_millis: Some(100),
    }
}
pub(super) fn defer(delivery: &Delivery) -> CommandKind {
    CommandKind::Defer {
        sequence: delivery.sequence,
        lock_token: delivery.lock.expect("original token").token,
    }
}
pub(super) fn complete(delivery: &Delivery) -> CommandKind {
    CommandKind::Complete {
        sequence: delivery.sequence,
        lock_token: delivery.lock.expect("original token").token,
    }
}
pub(super) fn continuation(outcome: &SessionRetirementOutcome) -> SessionRetirementCursor {
    let SessionRetirementPage::Continue(cursor) = &outcome.page else {
        panic!("finite continuation")
    };
    cursor.clone()
}
pub(super) fn counts(outcome: &SessionRetirementOutcome, ready: u32, dlq: u32, dropped: u32) {
    assert_eq!(
        (
            outcome.returned_to_ready,
            outcome.dead_lettered,
            outcome.dropped
        ),
        (ready, dlq, dropped)
    );
}
pub(super) fn batch_usage(batch: &WriteBatch) -> (usize, usize, usize) {
    batch
        .mutations()
        .iter()
        .fold((0, 0, 0), |(n, k, v), m| match m {
            Mutation::Put { key, value } => (n + 1, k + key.len(), v + value.len()),
            Mutation::Delete { key } => (n + 1, k + key.len(), v),
        })
}

pub(super) fn old_commands() -> TestResult<Vec<CommandKind>> {
    let sequence = SequenceNumber::new(7);
    let lock_token = LockToken::new(9);
    let session = SessionHold::new(SessionId::new("A")?, LockToken::new(11));
    let budget = DeliveryBudget {
        max_bytes: 13,
        per_message_overhead_bytes: 1,
    };
    let name = SubscriptionName::new("sub")?;
    let rule = RuleName::new("rule")?;
    Ok(vec![
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
        CommandKind::Send {
            message_id: "m".into(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
        },
        CommandKind::Schedule {
            messages: Vec::new(),
        },
        CommandKind::CancelScheduled {
            sequences: vec![sequence],
        },
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
        CommandKind::Peek {
            from_sequence: sequence,
            max_messages: 1,
            session_id: None,
        },
        CommandKind::Complete {
            sequence,
            lock_token,
        },
        CommandKind::Abandon {
            sequence,
            lock_token,
        },
        CommandKind::DeadLetter {
            sequence,
            lock_token,
            reason: "reason".into(),
            description: "description".into(),
        },
        CommandKind::Defer {
            sequence,
            lock_token,
        },
        CommandKind::RenewLock {
            sequence,
            lock_token,
            lock_duration_millis: None,
        },
        CommandKind::ReceiveDeferred {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session_id: None,
        },
        CommandKind::AcceptSession {
            session_id: None,
            lock_duration_millis: None,
        },
        CommandKind::ReleaseSession {
            session: session.clone(),
        },
        CommandKind::RenewSessionLock {
            session: session.clone(),
            lock_duration_millis: None,
        },
        CommandKind::SetSessionState {
            session: session.clone(),
            state: vec![1],
        },
        CommandKind::GetSessionState { session },
        CommandKind::ExpireLocks,
        CommandKind::ExpireMessages,
        CommandKind::ExpireSessionLocks,
        CommandKind::ActivateScheduled,
        CommandKind::ExpireDuplicateHistory,
        CommandKind::SendEnvelope {
            message_id: "m".into(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
            envelope: Box::new(MessageEnvelope::default()),
        },
        CommandKind::ScheduleEnvelopes {
            messages: Vec::new(),
        },
        CommandKind::Settle {
            sequence,
            lock_token,
            disposition: SettlementDisposition::Complete,
            properties_to_modify: BTreeMap::new(),
        },
        CommandKind::ReceiveDeferredBounded {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session_id: None,
            budget,
        },
        CommandKind::PeekBounded {
            from_sequence: sequence,
            max_messages: 1,
            session_id: None,
            budget,
        },
        CommandKind::ReceiveDeferredHeld {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
            budget,
        },
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate::default(),
        },
        CommandKind::SendBatch {
            messages: Vec::new(),
        },
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
        CommandKind::CreateRule {
            subscription: name.clone(),
            name: rule.clone(),
            filter: RuleFilter::True,
        },
        CommandKind::DeleteRule {
            subscription: name.clone(),
            name: rule.clone(),
        },
        CommandKind::UpdateTopic {
            update: TopicConfigUpdate::default(),
        },
        CommandKind::UpdateSubscription {
            name: name.clone(),
            update: SubscriptionConfigUpdate::default(),
        },
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
        CommandKind::CreateRuleWithAction {
            subscription: name,
            name: rule,
            filter: RuleFilter::True,
            action: SqlAction::new("REMOVE user.x;")?,
        },
        CommandKind::SettleHeld {
            sequence,
            lock_token,
            session: None,
            disposition: SettlementDisposition::Complete,
            properties_to_modify: BTreeMap::new(),
        },
        CommandKind::RenewLockHeld {
            sequence,
            lock_token,
            session: None,
            lock_duration_millis: None,
        },
        CommandKind::AcceptNextSessionPage {
            after: None,
            lock_duration_millis: None,
        },
    ])
}

pub(super) fn assert_serialization<P: StoreProvider>(node: &Node<P>) -> TestResult {
    let fixture = include_str!("fixture.rs");
    let frozen = fixture
        .rsplit_once("pub(super) enum OldCommandKind {")
        .expect("old enum declaration")
        .1
        .rsplit_once("\n}")
        .expect("last enum brace")
        .0;
    let old_interior = format!("{frozen}\n");
    assert_eq!(old_interior.len(), 8_688);
    assert_eq!(
        format!("{:x}", Sha256::digest(old_interior.as_bytes())),
        "83c064230a76b009df6f578eb2964badf6a101628ed581bdf2533f8e4ca7768c"
    );
    assert!(
        include_str!("../../src/command.rs")
            .split("pub enum CommandKind {")
            .nth(1)
            .expect("current enum")
            .starts_with(&old_interior)
    );
    for (ordinal, command) in old_commands()?.into_iter().enumerate() {
        let bytes = postcard::to_stdvec(&command)?;
        assert_eq!(bytes[0], ordinal as u8);
        let old: OldCommandKind = codec::decode_payload(&bytes)?;
        assert_eq!(postcard::to_stdvec(&old)?, bytes);
        assert_eq!(postcard::from_bytes::<CommandKind>(&bytes)?, command);
    }
    let sid = SessionId::new("A")?;
    for after in [
        None,
        Some(SessionRetirementCursor {
            namespace: node.namespace.clone(),
            entity: node.entity.clone(),
            session_id: sid.clone(),
            position: SessionRetirementPosition::AfterSession,
        }),
        Some(SessionRetirementCursor {
            namespace: node.namespace.clone(),
            entity: node.entity.clone(),
            session_id: sid,
            position: SessionRetirementPosition::OwnedRows {
                generation: LockToken::new(7),
                after_sequence: SequenceNumber::new(9),
            },
        }),
    ] {
        let command = retirement(after);
        let bytes = postcard::to_stdvec(&command)?;
        assert_eq!(bytes[0], 41);
        assert_eq!(postcard::from_bytes::<CommandKind>(&bytes)?, command);
        assert_eq!(
            codec::decode_payload::<OldCommandKind>(&bytes),
            Err(CodecError::Decode)
        );
    }
    assert_eq!(codec::ACTIVE_VALUE_FORMAT, 11);
    assert_eq!(storage::ACTIVE_STORE_FORMAT, 17);
    assert_eq!(
        keys::session_message_lock_reverse_prefix(&node.namespace, &node.entity)[0],
        0x13
    );
    assert_eq!(
        keys::session_message_lock_forward_prefix(
            &node.namespace,
            &node.entity,
            &SessionId::new("A")?
        )[0],
        0x14
    );
    assert_eq!(
        keys::session_message_lock_summary_prefix(&node.namespace, &node.entity)[0],
        0x15
    );
    assert_eq!(
        format!(
            "{:x}",
            Sha256::digest(include_bytes!("../../src/committed/fingerprint.rs"))
        ),
        "2db5c742e24e0f601f08ae6ab8da63abdda779a995d329b0a7f08737f2b4f7b9"
    );
    Ok(())
}

// Frozen predecessor enum: decoder coverage only, not a new domain authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum OldCommandKind {
    CreateQueue {
        config: QueueConfig,
    },
    Send {
        message_id: String,
        body: Vec<u8>,
        /// Requests a lifetime, capped by the queue default when it is finite.
        time_to_live_millis: Option<u64>,
        /// Required on session-required queues. On ordinary queues this is
        /// optional metadata, not session ownership or a FIFO guarantee.
        session_id: Option<SessionId>,
    },
    Schedule {
        messages: Vec<ScheduledMessage>,
    },
    /// Removes scheduled messages before activation. Every sequence must
    /// still identify a scheduled message, otherwise the batch changes nothing.
    CancelScheduled {
        sequences: Vec<SequenceNumber>,
    },
    Receive {
        mode: ReceiveMode,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
        /// The session lock this receive draws from. Required on a queue that
        /// requires sessions, and refused on one that does not.
        session: Option<SessionHold>,
    },
    /// Browses messages without changing their state.
    Peek {
        /// First sequence number to inspect, inclusive.
        from_sequence: SequenceNumber,
        max_messages: u32,
        /// Narrows the browse to one session. Required on a session queue, and
        /// refused on a non-session queue.
        session_id: Option<SessionId>,
    },
    Complete {
        sequence: SequenceNumber,
        lock_token: LockToken,
    },
    Abandon {
        sequence: SequenceNumber,
        lock_token: LockToken,
    },
    DeadLetter {
        sequence: SequenceNumber,
        lock_token: LockToken,
        reason: String,
        description: String,
    },
    Defer {
        sequence: SequenceNumber,
        lock_token: LockToken,
    },
    /// Extends a message lock without changing its token.
    RenewLock {
        sequence: SequenceNumber,
        lock_token: LockToken,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
    },
    ReceiveDeferred {
        sequences: Vec<SequenceNumber>,
        mode: ReceiveMode,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
        /// Narrows deferred receive to one session. Required on a session
        /// queue, and refused on a non-session queue.
        session_id: Option<SessionId>,
    },
    /// Takes exclusive ownership of a session.
    AcceptSession {
        /// `None` accepts the next session that has a ready message and is not
        /// already held.
        session_id: Option<SessionId>,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
    },
    /// Gives up a session so another receiver can take it. Messages already
    /// locked inside the session keep their own locks.
    ReleaseSession {
        session: SessionHold,
    },
    /// Extends a session lock without changing its token.
    RenewSessionLock {
        session: SessionHold,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
    },
    /// Replaces the opaque state stored alongside a session.
    SetSessionState {
        session: SessionHold,
        state: Vec<u8>,
    },
    /// Reads the opaque state stored alongside a session.
    GetSessionState {
        session: SessionHold,
    },
    /// Proposed by the leader's timer worker. Returns messages whose lock has
    /// elapsed, or dead-letters them once they reach the delivery limit.
    ExpireLocks,
    /// Proposed by the leader's timer worker. Expires ready messages whose time
    /// to live has elapsed. Live locks and deferred messages are not swept.
    ExpireMessages,
    /// Proposed by the leader's timer worker. Releases sessions whose lock has
    /// elapsed.
    ExpireSessionLocks,
    /// Proposed by the leader's timer worker. Enqueues scheduled messages
    /// whose requested enqueue time has arrived.
    ActivateScheduled,
    /// Proposed by the leader's timer worker. Discards message identifiers
    /// whose duplicate-detection history window has elapsed.
    ExpireDuplicateHistory,
    SendEnvelope {
        message_id: String,
        body: Vec<u8>,
        /// Requests a lifetime, capped by the queue default when it is finite.
        time_to_live_millis: Option<u64>,
        session_id: Option<SessionId>,
        envelope: Box<MessageEnvelope>,
    },
    ScheduleEnvelopes {
        messages: Vec<ScheduledEnvelope>,
    },
    /// Settles one delivery while atomically replacing the supplied
    /// application properties. Unmentioned properties are retained.
    Settle {
        sequence: SequenceNumber,
        lock_token: LockToken,
        disposition: SettlementDisposition,
        properties_to_modify: BTreeMap<String, MessageValue>,
    },
    /// Retrieves an atomic batch only when all inspected messages, including
    /// expired messages cleaned up by the receive, fit the delivery budget.
    ReceiveDeferredBounded {
        sequences: Vec<SequenceNumber>,
        mode: ReceiveMode,
        lock_duration_millis: Option<u64>,
        session_id: Option<SessionId>,
        budget: DeliveryBudget,
    },
    /// Browses a fitting prefix while reading at most one stored message at
    /// a time. An oversized first result is rejected rather than omitted.
    PeekBounded {
        from_sequence: SequenceNumber,
        max_messages: u32,
        session_id: Option<SessionId>,
        budget: DeliveryBudget,
    },
    /// Retrieves a bounded batch while proving live session ownership before
    /// inspecting or expiring any message. A non-session queue requires None.
    ReceiveDeferredHeld {
        sequences: Vec<SequenceNumber>,
        mode: ReceiveMode,
        lock_duration_millis: Option<u64>,
        session: Option<SessionHold>,
        budget: DeliveryBudget,
    },
    /// Replaces only the supplied mutable settings. Existing message and lock
    /// deadlines, counters, and duplicate-history entries are retained.
    UpdateQueue {
        update: QueueConfigUpdate,
    },
    /// Enqueues every member atomically after validating the whole bounded
    /// batch. Duplicate drops still consume their own acknowledged sequences.
    SendBatch {
        messages: Vec<IngressEnvelope>,
    },
    /// Creates a topic at Command.entity.
    CreateTopic {
        config: TopicConfig,
    },
    /// Creates one subscription under the parent topic named by Command.entity.
    CreateSubscription {
        name: SubscriptionName,
        config: SubscriptionConfig,
    },
    /// Creates a no-action rule on a subscription of `Command::entity`.
    CreateRule {
        subscription: SubscriptionName,
        name: RuleName,
        filter: RuleFilter,
    },
    DeleteRule {
        subscription: SubscriptionName,
        name: RuleName,
    },
    /// Replaces mutable topic settings without rewriting retained state.
    UpdateTopic {
        update: TopicConfigUpdate,
    },
    /// Updates one subscription under the parent topic at `Command::entity`.
    UpdateSubscription {
        name: SubscriptionName,
        update: SubscriptionConfigUpdate,
    },
    /// Purges bounded owned state while retaining monotonic counter fences.
    DeleteEntity {
        target: DeleteEntityTarget,
    },
    /// Creates one independently copied action rule without changing older
    /// command variant indices or positional payloads.
    CreateRuleWithAction {
        subscription: SubscriptionName,
        name: RuleName,
        filter: RuleFilter,
        action: SqlAction,
    },
    /// Settles a protocol delivery under its original session authority.
    /// None is valid only for ordinary queues and dead-letter shadows.
    SettleHeld {
        sequence: SequenceNumber,
        lock_token: LockToken,
        session: Option<SessionHold>,
        disposition: SettlementDisposition,
        properties_to_modify: BTreeMap<String, MessageValue>,
    },
    /// Renews a protocol delivery under its original session authority.
    /// None is valid only for ordinary queues and dead-letter shadows.
    RenewLockHeld {
        sequence: SequenceNumber,
        lock_token: LockToken,
        session: Option<SessionHold>,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
    },
    /// Inspects one bounded page of ready session groups and grants the first
    /// one not held at this command's owner-authoritative timestamp.
    AcceptNextSessionPage {
        after: Option<SessionCursor>,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
    },
}
