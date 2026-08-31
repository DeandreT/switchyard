//! The deterministic broker state machine.
//!
//! [`StateMachine::apply`] is the only entry point that mutates state. It reads
//! the records a command touches, folds every resulting change into a single
//! [`WriteBatch`], and commits that batch atomically. A command therefore
//! either takes effect completely or not at all, and two replicas applying the
//! same command in the same order reach byte-identical state.
//!
//! Nothing here reads a clock, generates a random value, or performs I/O beyond
//! the injected store.

mod deferred;
mod send;

use serde::de::DeserializeOwned;
use storage::{StateStore, WriteBatch};

use crate::{
    AcceptedSession, BrokerError, Command, CommandKind, CommandOutcome, DeadLetterInfo,
    DeadLetterReason, Delivery, DeliveryLock, DeliveryOrigin, EntityPath, LockToken,
    MessageEnvelope, MessageRecord, MessageState, NamespaceName, QueueConfig, QueueCounters,
    ReceiveMode, SequenceNumber, SessionHold, SessionId, SessionLock, SessionRecord, Timestamp,
    codec, keys,
};

use self::deferred::replace_envelope;
use self::send::SendInput;

/// Ready entries a single receive may walk past while discarding expired
/// messages. Bounds the work one command performs so a large backlog of
/// expired messages cannot stall the group.
const MAX_RECEIVE_SCAN: usize = 32;

/// Sequence numbers accepted by one atomic deferred receive.
///
/// This matches the receiving-link in-flight bound and prevents one management
/// request from constructing an unbounded storage batch.
pub const MAX_DEFERRED_RECEIVE_BATCH: usize = 32;

/// Index entries a single timer sweep may process. A sweep that reports this
/// many may have more waiting, so the worker proposes another command.
pub const TIMER_SCAN_LIMIT: usize = 256;

/// Sessions one acceptance may examine before giving up. A queue whose first
/// `MAX_SESSION_SCAN` sessions are all held reports none available rather than
/// walking an unbounded number of them, and the receiver retries.
const MAX_SESSION_SCAN: usize = 32;

#[derive(Clone, Debug)]
pub struct StateMachine<S> {
    store: S,
}

impl<S: StateStore> StateMachine<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    /// Applies one replicated command.
    ///
    /// On error nothing is written, so a rejection leaves state untouched on
    /// every replica.
    pub fn apply(&self, command: &Command) -> Result<CommandOutcome, BrokerError> {
        let last_applied = self.last_applied_time()?;
        if command.issued_at < last_applied {
            return Err(BrokerError::ClockRegression {
                last_applied,
                proposed: command.issued_at,
            });
        }

        let mut batch = WriteBatch::default();
        let outcome = match &command.kind {
            CommandKind::CreateQueue { config } => {
                self.create_queue(command, *config, &mut batch)?
            }
            CommandKind::Send {
                message_id,
                body,
                time_to_live_millis,
                session_id,
                envelope,
            } => self.send(
                command,
                SendInput {
                    message_id,
                    body,
                    time_to_live_millis: *time_to_live_millis,
                    session_id: session_id.as_ref(),
                    envelope: envelope.as_ref(),
                },
                &mut batch,
            )?,
            CommandKind::SendBatch { messages } => {
                self.send_batch(command, messages, &mut batch)?
            }
            CommandKind::Receive {
                mode,
                lock_duration_millis,
                session,
            } => self.receive(
                command,
                *mode,
                *lock_duration_millis,
                session.as_ref(),
                &mut batch,
            )?,
            CommandKind::Complete {
                sequence,
                lock_token,
            } => self.complete(command, *sequence, *lock_token, &mut batch)?,
            CommandKind::Abandon {
                sequence,
                lock_token,
                replacement_envelope,
            } => self.abandon(
                command,
                *sequence,
                *lock_token,
                replacement_envelope.as_ref(),
                &mut batch,
            )?,
            CommandKind::Defer {
                sequence,
                lock_token,
                replacement_envelope,
            } => self.defer(
                command,
                *sequence,
                *lock_token,
                replacement_envelope.as_ref(),
                &mut batch,
            )?,
            CommandKind::ReceiveDeferred {
                sequences,
                mode,
                lock_duration_millis,
                session,
            } => self.receive_deferred(
                command,
                sequences,
                *mode,
                *lock_duration_millis,
                session.as_ref(),
                &mut batch,
            )?,
            CommandKind::DeadLetter {
                sequence,
                lock_token,
                reason,
                description,
                replacement_envelope,
            } => self.dead_letter(
                command,
                *sequence,
                *lock_token,
                reason,
                description,
                replacement_envelope.as_ref(),
                &mut batch,
            )?,
            CommandKind::RenewLock {
                sequence,
                lock_token,
                lock_duration_millis,
            } => self.renew_lock(
                command,
                *sequence,
                *lock_token,
                *lock_duration_millis,
                &mut batch,
            )?,
            CommandKind::AcceptSession {
                session_id,
                lock_duration_millis,
            } => self.accept_session(
                command,
                session_id.as_ref(),
                *lock_duration_millis,
                &mut batch,
            )?,
            CommandKind::ReleaseSession { session } => {
                self.release_session(command, session, &mut batch)?
            }
            CommandKind::RenewSessionLock {
                session,
                lock_duration_millis,
            } => self.renew_session_lock(command, session, *lock_duration_millis, &mut batch)?,
            CommandKind::SetSessionState { session, state } => {
                self.set_session_state(command, session, state, &mut batch)?
            }
            CommandKind::GetSessionState { session } => self.get_session_state(command, session)?,
            CommandKind::ExpireLocks => self.expire_locks(command, &mut batch)?,
            CommandKind::ExpireMessages => self.expire_messages(command, &mut batch)?,
            CommandKind::ExpireSessionLocks => self.expire_session_locks(command, &mut batch)?,
        };

        // A command that changed nothing commits nothing. The clock advance is
        // bookkeeping for the mutations alongside it, and committing it alone
        // would turn every empty receive and every idle timer sweep into a
        // durable write — an fsync apiece on the durable backend. Skipping is
        // deterministic: every replica computes the same empty batch, so every
        // replica skips the same commands.
        if !batch.is_empty() {
            // Advancing the clock in the same batch keeps the applied timestamp
            // and the state it produced consistent under a crash.
            batch.push_put(keys::clock(), codec::encode(&command.issued_at)?);
            self.store.apply(batch)?;
        }
        Ok(outcome)
    }

    // ---- reads -------------------------------------------------------------

    pub fn last_applied_time(&self) -> Result<Timestamp, BrokerError> {
        Ok(self.read(&keys::clock())?.unwrap_or(Timestamp::UNIX_EPOCH))
    }

    pub fn queue_config(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<Option<QueueConfig>, BrokerError> {
        self.read(&keys::queue_config(namespace, entity))
    }

    /// Every queue in the store, in key order, across every namespace. The timer
    /// worker walks this to learn what there is to sweep.
    pub fn queues(&self, limit: usize) -> Result<Vec<(NamespaceName, EntityPath)>, BrokerError> {
        self.store
            .scan_prefix(&keys::queue_config_prefix(), limit)?
            .iter()
            .map(|(key, _)| {
                let (namespace, entity) =
                    keys::entity_scope_parts(key).ok_or(BrokerError::MalformedIndexKey)?;
                Ok((NamespaceName::new(namespace)?, EntityPath::new(entity)?))
            })
            .collect()
    }

    pub fn message(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        sequence: SequenceNumber,
    ) -> Result<Option<MessageRecord>, BrokerError> {
        self.read_message(&keys::message(namespace, entity, sequence))
    }

    pub fn dead_lettered_message(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        sequence: SequenceNumber,
    ) -> Result<Option<MessageRecord>, BrokerError> {
        self.message(namespace, &entity.dead_letter_queue()?, sequence)
    }

    /// The stored state of one session. `None` means the session has never been
    /// locked or given state, which is indistinguishable from one that was.
    pub fn session(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        session_id: &SessionId,
    ) -> Result<Option<SessionRecord>, BrokerError> {
        self.read(&keys::session(namespace, entity, session_id))
    }

    /// The opaque state stored alongside a session, empty when it has none.
    pub fn session_state(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        session_id: &SessionId,
    ) -> Result<Vec<u8>, BrokerError> {
        Ok(self
            .session(namespace, entity, session_id)?
            .map(|record| record.state)
            .unwrap_or_default())
    }

    pub fn session_ready_sequences(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        session_id: &SessionId,
        limit: usize,
    ) -> Result<Vec<SequenceNumber>, BrokerError> {
        self.index_sequences(
            &keys::session_ready_prefix(namespace, entity, session_id),
            limit,
        )
    }

    pub fn ready_sequences(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        limit: usize,
    ) -> Result<Vec<SequenceNumber>, BrokerError> {
        self.index_sequences(&keys::ready_prefix(namespace, entity), limit)
    }

    /// Deferred sequences remain entity-local but outside the ordinary ready
    /// index until explicitly requested.
    pub fn deferred_sequences(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        limit: usize,
    ) -> Result<Vec<SequenceNumber>, BrokerError> {
        self.index_sequences(&keys::deferred_prefix(namespace, entity), limit)
    }

    /// Sequences ready in the entity's dead-letter queue, in order.
    pub fn dead_lettered_sequences(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        limit: usize,
    ) -> Result<Vec<SequenceNumber>, BrokerError> {
        self.ready_sequences(namespace, &entity.dead_letter_queue()?, limit)
    }

    fn index_sequences(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<SequenceNumber>, BrokerError> {
        self.store
            .scan_prefix(prefix, limit)?
            .iter()
            .map(|(key, _)| keys::trailing_sequence(key).ok_or(BrokerError::MalformedIndexKey))
            .collect()
    }

    fn read<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>, BrokerError> {
        match self.store.get(key)? {
            Some(bytes) => Ok(Some(codec::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Reads the complete durable message representation.
    fn read_message(&self, key: &[u8]) -> Result<Option<MessageRecord>, BrokerError> {
        match self.store.get(key)? {
            Some(bytes) => Ok(Some(MessageRecord::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    fn load_session(
        &self,
        command: &Command,
        session_id: &SessionId,
    ) -> Result<SessionRecord, BrokerError> {
        Ok(self
            .session(&command.namespace, &command.entity, session_id)?
            .unwrap_or_default())
    }

    /// The index a ready message sits in: its own session's on a session queue,
    /// and the entity-wide ready index otherwise.
    fn ready_key(&self, command: &Command, record: &MessageRecord) -> Vec<u8> {
        let namespace = &command.namespace;
        let entity = &command.entity;
        match &record.session_id {
            Some(session_id) => keys::session_ready(namespace, entity, session_id, record.sequence),
            None => keys::ready(namespace, entity, record.sequence),
        }
    }

    fn load_config(&self, command: &Command) -> Result<QueueConfig, BrokerError> {
        self.queue_config(&command.namespace, &command.entity)?
            .ok_or(BrokerError::QueueNotFound)
    }

    fn load_counters(&self, command: &Command) -> Result<QueueCounters, BrokerError> {
        Ok(self
            .read(&keys::queue_counters(&command.namespace, &command.entity))?
            .unwrap_or_default())
    }

    fn load_message(
        &self,
        command: &Command,
        sequence: SequenceNumber,
    ) -> Result<MessageRecord, BrokerError> {
        self.message(&command.namespace, &command.entity, sequence)?
            .ok_or(BrokerError::MessageNotFound { sequence })
    }

    // ---- handlers ----------------------------------------------------------

    fn create_queue(
        &self,
        command: &Command,
        config: QueueConfig,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        if command.entity.is_dead_letter_queue() {
            return Err(BrokerError::DeadLetterQueueIsReserved);
        }
        let key = keys::queue_config(&command.namespace, &command.entity);
        if self.store.get(&key)?.is_some() {
            return Err(BrokerError::QueueAlreadyExists);
        }
        let config = config.validate()?;

        // Every queue casts a dead-letter shadow: a queue with the same limits
        // that ignores lifetimes and sessions and never dead-letters again.
        // Failing here, rather than at the first dead-lettering, is why a
        // parent whose shadow path would be too long cannot be created.
        let dead_letter_queue = command.entity.dead_letter_queue()?;
        let shadow = QueueConfig {
            max_delivery_count: u32::MAX,
            default_time_to_live_millis: None,
            requires_session: false,
            ..config
        };
        batch.push_put(key, codec::encode(&config)?);
        batch.push_put(
            keys::queue_config(&command.namespace, &dead_letter_queue),
            codec::encode(&shadow)?,
        );
        Ok(CommandOutcome::QueueCreated)
    }

    fn receive(
        &self,
        command: &Command,
        mode: ReceiveMode,
        lock_duration_millis: Option<u64>,
        session: Option<&SessionHold>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        require_session_agreement(&config, session.is_some())?;
        let namespace = &command.namespace;
        let entity = &command.entity;

        // A session receiver only ever sees its own session's messages, and only
        // for as long as it holds the session.
        let ready_prefix = match session {
            Some(hold) => {
                self.held_session(command, hold)?;
                keys::session_ready_prefix(namespace, entity, &hold.session_id)
            }
            None => keys::ready_prefix(namespace, entity),
        };
        let ready = self.store.scan_prefix(&ready_prefix, MAX_RECEIVE_SCAN)?;

        for (key, _) in ready {
            let sequence = keys::trailing_sequence(&key).ok_or(BrokerError::MalformedIndexKey)?;
            let mut record = self
                .message(namespace, entity, sequence)?
                .ok_or(BrokerError::DanglingIndexEntry { sequence })?;

            // A timer sweep normally reaps these, but a receive must never hand
            // out a message whose lifetime has already elapsed.
            if record.is_expired_at(command.issued_at) {
                self.move_to_dead_letter(
                    command,
                    record,
                    DeadLetterReason::TimeToLiveExpired,
                    String::from("the message exceeded its time to live"),
                    batch,
                )?;
                continue;
            }

            record.delivery_count = record.delivery_count.saturating_add(1);
            let delivery_count = record.delivery_count;
            let ready_key = self.ready_key(command, &record);

            let lock = match mode {
                ReceiveMode::PeekLock => {
                    let mut counters = self.load_counters(command)?;
                    let token = LockToken::new(counters.next_lock_token);
                    counters.next_lock_token = counters.next_lock_token.saturating_add(1);

                    let lock_duration_millis =
                        lock_duration_millis.unwrap_or(config.lock_duration_millis);
                    let locked_until = command
                        .issued_at
                        .saturating_add_millis(lock_duration_millis);
                    record.state = MessageState::Locked {
                        token,
                        locked_until,
                        origin: DeliveryOrigin::Ready,
                    };

                    batch.push_delete(ready_key.clone());
                    // TTL does not invalidate a live lock. The expiry index is
                    // restored on abandon/lock expiry, or discarded on a
                    // successful settlement.
                    if let Some(expires_at) = record.expires_at {
                        batch.push_delete(keys::expiry(namespace, entity, expires_at, sequence));
                    }
                    batch.push_put(
                        keys::message(namespace, entity, sequence),
                        codec::encode(&record)?,
                    );
                    batch.push_put(
                        keys::lock(namespace, entity, locked_until, sequence),
                        Vec::new(),
                    );
                    batch.push_put(
                        keys::queue_counters(namespace, entity),
                        codec::encode(&counters)?,
                    );
                    Some(DeliveryLock {
                        token,
                        locked_until,
                        lock_duration_millis,
                    })
                }
                // At-most-once: the deletion commits before the transfer, so a
                // client that never receives the reply loses this delivery.
                ReceiveMode::ReceiveAndDelete => {
                    batch.push_delete(ready_key.clone());
                    batch.push_delete(keys::message(namespace, entity, sequence));
                    if let Some(expires_at) = record.expires_at {
                        batch.push_delete(keys::expiry(namespace, entity, expires_at, sequence));
                    }
                    None
                }
            };

            return Ok(CommandOutcome::Received(Some(Delivery {
                sequence,
                message_id: record.message_id,
                body: record.body,
                enqueued_at: record.enqueued_at,
                expires_at: record.expires_at,
                delivery_count,
                origin: DeliveryOrigin::Ready,
                lock,
                session_id: record.session_id,
                dead_letter: record.dead_letter,
                envelope: record.envelope,
            })));
        }

        Ok(CommandOutcome::Received(None))
    }

    fn complete(
        &self,
        command: &Command,
        sequence: SequenceNumber,
        lock_token: LockToken,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let (record, locked_until, _) = self.held_lock(command, sequence, lock_token)?;
        let namespace = &command.namespace;
        let entity = &command.entity;

        batch.push_delete(keys::message(namespace, entity, sequence));
        batch.push_delete(keys::lock(namespace, entity, locked_until, sequence));
        if let Some(expires_at) = record.expires_at {
            batch.push_delete(keys::expiry(namespace, entity, expires_at, sequence));
        }
        Ok(CommandOutcome::Completed)
    }

    fn abandon(
        &self,
        command: &Command,
        sequence: SequenceNumber,
        lock_token: LockToken,
        replacement_envelope: Option<&MessageEnvelope>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        let (mut record, locked_until, origin) = self.held_lock(command, sequence, lock_token)?;
        replace_envelope(&config, &mut record, replacement_envelope)?;

        // Expiration is suspended while a receiver owns the lock. Once that
        // receiver gives it up, TTL takes precedence over redelivery.
        if record.is_expired_at(command.issued_at) {
            self.move_to_dead_letter(
                command,
                record,
                DeadLetterReason::TimeToLiveExpired,
                String::from("the message exceeded its time to live"),
                batch,
            )?;
            return Ok(CommandOutcome::Abandoned {
                dead_lettered: true,
            });
        }

        if exceeded_delivery_limit(command, &config, &record) {
            self.move_to_dead_letter(
                command,
                record,
                DeadLetterReason::MaxDeliveryCountExceeded,
                String::from("the message reached its maximum delivery count"),
                batch,
            )?;
            return Ok(CommandOutcome::Abandoned {
                dead_lettered: true,
            });
        }

        self.return_unsettled(command, record, locked_until, origin, batch)?;
        Ok(CommandOutcome::Abandoned {
            dead_lettered: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn dead_letter(
        &self,
        command: &Command,
        sequence: SequenceNumber,
        lock_token: LockToken,
        reason: &str,
        description: &str,
        replacement_envelope: Option<&MessageEnvelope>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        let (mut record, _, _) = self.held_lock(command, sequence, lock_token)?;
        replace_envelope(&config, &mut record, replacement_envelope)?;
        self.move_to_dead_letter(
            command,
            record,
            DeadLetterReason::Application(reason.to_owned()),
            description.to_owned(),
            batch,
        )?;
        Ok(CommandOutcome::DeadLettered)
    }

    fn expire_locks(
        &self,
        command: &Command,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        let namespace = &command.namespace;
        let entity = &command.entity;
        let locks = self
            .store
            .scan_prefix(&keys::lock_prefix(namespace, entity), TIMER_SCAN_LIMIT)?;

        let mut returned_to_ready = 0;
        let mut dead_lettered = 0;
        for (key, _) in locks {
            let (locked_until, sequence) =
                keys::trailing_deadline(&key).ok_or(BrokerError::MalformedIndexKey)?;
            // The index is ordered by deadline, so the first lock still held
            // ends the sweep.
            if locked_until > command.issued_at {
                break;
            }

            let record = self
                .message(namespace, entity, sequence)?
                .ok_or(BrokerError::DanglingIndexEntry { sequence })?;
            let origin = match record.state {
                MessageState::Locked { origin, .. } => origin,
                _ => return Err(BrokerError::MessageNotLocked { sequence }),
            };

            if record.is_expired_at(command.issued_at) {
                self.move_to_dead_letter(
                    command,
                    record,
                    DeadLetterReason::TimeToLiveExpired,
                    String::from("the message exceeded its time to live"),
                    batch,
                )?;
                dead_lettered += 1;
            } else if exceeded_delivery_limit(command, &config, &record) {
                self.move_to_dead_letter(
                    command,
                    record,
                    DeadLetterReason::MaxDeliveryCountExceeded,
                    String::from("the message reached its maximum delivery count"),
                    batch,
                )?;
                dead_lettered += 1;
            } else {
                self.return_unsettled(command, record, locked_until, origin, batch)?;
                returned_to_ready += 1;
            }
        }

        Ok(CommandOutcome::LocksExpired {
            returned_to_ready,
            dead_lettered,
        })
    }

    fn expire_messages(
        &self,
        command: &Command,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let namespace = &command.namespace;
        let entity = &command.entity;
        let expiring = self
            .store
            .scan_prefix(&keys::expiry_prefix(namespace, entity), TIMER_SCAN_LIMIT)?;

        let mut dead_lettered = 0;
        for (key, _) in expiring {
            let (expires_at, sequence) =
                keys::trailing_deadline(&key).ok_or(BrokerError::MalformedIndexKey)?;
            if expires_at > command.issued_at {
                break;
            }

            let record = self
                .message(namespace, entity, sequence)?
                .ok_or(BrokerError::DanglingIndexEntry { sequence })?;
            match record.state {
                MessageState::Ready => {
                    self.move_to_dead_letter(
                        command,
                        record,
                        DeadLetterReason::TimeToLiveExpired,
                        String::from("the message exceeded its time to live"),
                        batch,
                    )?;
                    dead_lettered += 1;
                }
                // A lock protects a message from TTL until settlement, abandon,
                // or lock expiry. Deferred messages are checked only when a
                // client explicitly retrieves their sequence. Removing a stale
                // expiry entry prevents either state from blocking later
                // deadlines in this ordered index.
                MessageState::Locked { .. } | MessageState::Deferred => {
                    batch.push_delete(key);
                }
            }
        }

        Ok(CommandOutcome::MessagesExpired { dead_lettered })
    }

    // ---- sessions ----------------------------------------------------------

    fn accept_session(
        &self,
        command: &Command,
        session_id: Option<&SessionId>,
        lock_duration_millis: Option<u64>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        if !config.requires_session {
            return Err(BrokerError::SessionNotSupported);
        }
        let lock_duration_millis = lock_duration_millis.unwrap_or(config.lock_duration_millis);
        let locked_until = command
            .issued_at
            .saturating_add_millis(lock_duration_millis);

        let Some(session_id) = session_id else {
            return self.accept_next_session(command, locked_until, batch);
        };

        // A named session can be accepted even when it holds nothing, which is
        // how a receiver waits on a session it knows is coming.
        let record = self.load_session(command, session_id)?;
        if record.live_lock_at(command.issued_at).is_some() {
            return Err(BrokerError::SessionAlreadyLocked {
                session_id: session_id.clone(),
            });
        }
        let accepted = self.lock_session(command, session_id, record, locked_until, batch)?;
        Ok(CommandOutcome::SessionAccepted(Some(accepted)))
    }

    /// Walks the entity's ready messages grouped by session, taking the first
    /// session nobody holds.
    fn accept_next_session(
        &self,
        command: &Command,
        locked_until: Timestamp,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let namespace = &command.namespace;
        let entity = &command.entity;
        let prefix = keys::entity_session_ready_prefix(namespace, entity);
        let mut start = prefix.clone();

        for _ in 0..MAX_SESSION_SCAN {
            let Some((key, _)) = self.store.scan_from(&prefix, &start, 1)?.into_iter().next()
            else {
                return Ok(CommandOutcome::SessionAccepted(None));
            };

            let session_id = SessionId::new(
                keys::session_id_after(&prefix, &key).ok_or(BrokerError::MalformedIndexKey)?,
            )?;
            let record = self.load_session(command, &session_id)?;
            if record.live_lock_at(command.issued_at).is_none() {
                let accepted =
                    self.lock_session(command, &session_id, record, locked_until, batch)?;
                return Ok(CommandOutcome::SessionAccepted(Some(accepted)));
            }

            // Held by someone else: resume past every message of this session
            // rather than reading them only to reject them again.
            start = keys::after_session_ready(namespace, entity, &session_id);
        }

        Ok(CommandOutcome::SessionAccepted(None))
    }

    fn lock_session(
        &self,
        command: &Command,
        session_id: &SessionId,
        mut record: SessionRecord,
        locked_until: Timestamp,
        batch: &mut WriteBatch,
    ) -> Result<AcceptedSession, BrokerError> {
        let namespace = &command.namespace;
        let entity = &command.entity;
        let mut counters = self.load_counters(command)?;
        let token = LockToken::new(counters.next_lock_token);
        counters.next_lock_token = counters.next_lock_token.saturating_add(1);

        // An elapsed lock still owns an index entry, which the sweep may not
        // have reached yet.
        if let Some(previous) = record.lock {
            batch.push_delete(keys::session_lock(
                namespace,
                entity,
                previous.locked_until,
                session_id,
            ));
        }

        let lock = SessionLock {
            token,
            locked_until,
        };
        record.lock = Some(lock);
        batch.push_put(
            keys::session(namespace, entity, session_id),
            codec::encode(&record)?,
        );
        batch.push_put(
            keys::session_lock(namespace, entity, locked_until, session_id),
            Vec::new(),
        );
        batch.push_put(
            keys::queue_counters(namespace, entity),
            codec::encode(&counters)?,
        );

        Ok(AcceptedSession {
            session_id: session_id.clone(),
            lock,
            state: record.state,
        })
    }

    fn release_session(
        &self,
        command: &Command,
        session: &SessionHold,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let record = self.held_session(command, session)?;
        self.clear_session_lock(command, &session.session_id, record, batch)?;
        Ok(CommandOutcome::SessionReleased)
    }

    fn renew_session_lock(
        &self,
        command: &Command,
        session: &SessionHold,
        lock_duration_millis: Option<u64>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        let mut record = self.held_session(command, session)?;
        let namespace = &command.namespace;
        let entity = &command.entity;

        let previous = record.lock.ok_or(BrokerError::SessionLockNotHeld {
            session_id: session.session_id.clone(),
        })?;
        let locked_until = command
            .issued_at
            .saturating_add_millis(lock_duration_millis.unwrap_or(config.lock_duration_millis));

        // The token is unchanged, so a receiver mid-renewal keeps working.
        record.lock = Some(SessionLock {
            token: previous.token,
            locked_until,
        });
        batch.push_delete(keys::session_lock(
            namespace,
            entity,
            previous.locked_until,
            &session.session_id,
        ));
        batch.push_put(
            keys::session_lock(namespace, entity, locked_until, &session.session_id),
            Vec::new(),
        );
        batch.push_put(
            keys::session(namespace, entity, &session.session_id),
            codec::encode(&record)?,
        );
        Ok(CommandOutcome::SessionLockRenewed { locked_until })
    }

    fn renew_lock(
        &self,
        command: &Command,
        sequence: SequenceNumber,
        lock_token: LockToken,
        lock_duration_millis: Option<u64>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        let (mut record, previous_locked_until, origin) =
            self.held_lock(command, sequence, lock_token)?;
        let lock_duration_millis = lock_duration_millis.unwrap_or(config.lock_duration_millis);
        let locked_until = command
            .issued_at
            .saturating_add_millis(lock_duration_millis);

        record.state = MessageState::Locked {
            token: lock_token,
            locked_until,
            origin,
        };
        batch.push_delete(keys::lock(
            &command.namespace,
            &command.entity,
            previous_locked_until,
            sequence,
        ));
        batch.push_put(
            keys::lock(&command.namespace, &command.entity, locked_until, sequence),
            Vec::new(),
        );
        batch.push_put(
            keys::message(&command.namespace, &command.entity, sequence),
            codec::encode(&record)?,
        );
        Ok(CommandOutcome::LockRenewed {
            locked_until,
            lock_duration_millis,
        })
    }

    fn set_session_state(
        &self,
        command: &Command,
        session: &SessionHold,
        state: &[u8],
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let mut record = self.held_session(command, session)?;
        record.state = state.to_vec();
        batch.push_put(
            keys::session(&command.namespace, &command.entity, &session.session_id),
            codec::encode(&record)?,
        );
        Ok(CommandOutcome::SessionStateSet)
    }

    fn get_session_state(
        &self,
        command: &Command,
        session: &SessionHold,
    ) -> Result<CommandOutcome, BrokerError> {
        let record = self.held_session(command, session)?;
        Ok(CommandOutcome::SessionState(record.state))
    }

    fn expire_session_locks(
        &self,
        command: &Command,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let namespace = &command.namespace;
        let entity = &command.entity;
        let prefix = keys::session_lock_prefix(namespace, entity);
        let locks = self.store.scan_prefix(&prefix, TIMER_SCAN_LIMIT)?;

        let mut released = 0;
        for (key, _) in locks {
            let (locked_until, session_id) =
                keys::session_lock_parts(&prefix, &key).ok_or(BrokerError::MalformedIndexKey)?;
            // Ordered by deadline, so the first lock still held ends the sweep.
            if locked_until > command.issued_at {
                break;
            }

            let session_id = SessionId::new(session_id)?;
            let record = self.load_session(command, &session_id)?;
            self.clear_session_lock(command, &session_id, record, batch)?;
            released += 1;
        }

        Ok(CommandOutcome::SessionLocksExpired { released })
    }

    /// Drops a session's lock while keeping its state, which outlives any one
    /// receiver. Messages locked inside the session keep their own locks.
    fn clear_session_lock(
        &self,
        command: &Command,
        session_id: &SessionId,
        mut record: SessionRecord,
        batch: &mut WriteBatch,
    ) -> Result<(), BrokerError> {
        let namespace = &command.namespace;
        let entity = &command.entity;
        if let Some(lock) = record.lock.take() {
            batch.push_delete(keys::session_lock(
                namespace,
                entity,
                lock.locked_until,
                session_id,
            ));
        }
        batch.push_put(
            keys::session(namespace, entity, session_id),
            codec::encode(&record)?,
        );
        Ok(())
    }

    /// Resolves a command's session hold, rejecting a token that does not match
    /// a lock that is still live.
    fn held_session(
        &self,
        command: &Command,
        session: &SessionHold,
    ) -> Result<SessionRecord, BrokerError> {
        let record = self.load_session(command, &session.session_id)?;
        let lock = record.lock.ok_or_else(|| BrokerError::SessionLockNotHeld {
            session_id: session.session_id.clone(),
        })?;
        if lock.token != session.token {
            return Err(BrokerError::SessionLockNotHeld {
                session_id: session.session_id.clone(),
            });
        }
        if lock.locked_until <= command.issued_at {
            return Err(BrokerError::SessionLockExpired {
                session_id: session.session_id.clone(),
                locked_until: lock.locked_until,
            });
        }
        Ok(record)
    }

    // ---- shared transitions ------------------------------------------------

    /// Resolves a settlement command to the message it names, rejecting a
    /// token that does not match the live lock.
    fn held_lock(
        &self,
        command: &Command,
        sequence: SequenceNumber,
        lock_token: LockToken,
    ) -> Result<(MessageRecord, Timestamp, DeliveryOrigin), BrokerError> {
        let record = self.load_message(command, sequence)?;
        match record.state {
            MessageState::Locked {
                token,
                locked_until,
                origin,
            } => {
                if token != lock_token {
                    return Err(BrokerError::LockTokenMismatch { sequence });
                }
                if locked_until <= command.issued_at {
                    return Err(BrokerError::LockExpired {
                        sequence,
                        locked_until,
                    });
                }
                Ok((record, locked_until, origin))
            }
            _ => Err(BrokerError::MessageNotLocked { sequence }),
        }
    }

    /// Moves a message out of the active keyspace and into the dead-letter
    /// keyspace, clearing whichever index currently references it.
    fn move_to_dead_letter(
        &self,
        command: &Command,
        mut record: MessageRecord,
        reason: DeadLetterReason,
        description: String,
        batch: &mut WriteBatch,
    ) -> Result<(), BrokerError> {
        let namespace = &command.namespace;
        let entity = &command.entity;
        let sequence = record.sequence;

        match record.state {
            MessageState::Ready => {
                batch.push_delete(self.ready_key(command, &record));
            }
            MessageState::Deferred => {
                batch.push_delete(keys::deferred(namespace, entity, sequence));
            }
            MessageState::Locked { locked_until, .. } => {
                batch.push_delete(keys::lock(namespace, entity, locked_until, sequence));
            }
        }
        batch.push_delete(keys::message(namespace, entity, sequence));
        if let Some(expires_at) = record.expires_at {
            batch.push_delete(keys::expiry(namespace, entity, expires_at, sequence));
        }

        // Into the shadow queue as an ordinary ready message under its original
        // sequence — the same receive and settlement machinery drains it.
        // Lifetime and session are stripped: time to live does not apply in a
        // dead-letter queue, and its receivers hold no session.
        let dead_letter_queue = entity.dead_letter_queue()?;
        record.state = MessageState::Ready;
        record.expires_at = None;
        record.session_id = None;
        record.dead_letter = Some(DeadLetterInfo {
            reason,
            description,
            dead_lettered_at: command.issued_at,
        });
        batch.push_put(
            keys::message(namespace, &dead_letter_queue, sequence),
            codec::encode(&record)?,
        );
        batch.push_put(
            keys::ready(namespace, &dead_letter_queue, sequence),
            Vec::new(),
        );
        Ok(())
    }
}

/// Whether abandoning or lock expiry should dead-letter rather than return the
/// message.
///
/// Never true inside a dead-letter queue: its shadow config carries an
/// unreachable delivery limit, and this guard keeps even a saturated delivery
/// count from cascading a message into a shadow of a shadow.
fn exceeded_delivery_limit(
    command: &Command,
    config: &QueueConfig,
    record: &MessageRecord,
) -> bool {
    !command.entity.is_dead_letter_queue() && record.delivery_count >= config.max_delivery_count
}

/// Rejects a command whose session argument disagrees with the queue.
///
/// A session identifier on a queue that does not use sessions is refused rather
/// than ignored: accepting it would promise an ordering the queue cannot keep.
fn require_session_agreement(config: &QueueConfig, names_session: bool) -> Result<(), BrokerError> {
    match (config.requires_session, names_session) {
        (true, false) => Err(BrokerError::SessionRequired),
        (false, true) => Err(BrokerError::SessionNotSupported),
        _ => Ok(()),
    }
}
