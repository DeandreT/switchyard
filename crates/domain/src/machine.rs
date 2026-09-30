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

use std::collections::{BTreeMap, BTreeSet};

use serde::de::DeserializeOwned;
use storage::{Mutation, StateStore, WriteBatch};

use crate::{
    AcceptedSession, BrokerError, Command, CommandKind, CommandOutcome, DeadLetterInfo,
    DeadLetterReason, Delivery, DeliveryBudget, DeliveryLock, EntityPath, LockToken,
    MAX_MESSAGE_HEADER_BYTES, MAX_MESSAGE_ID_LENGTH, MessageBody, MessageEnvelope,
    MessageIdentifier, MessageProperties, MessageRecord, MessageState, MessageStatus, MessageValue,
    NamespaceName, QueueConfig, QueueCounters, ReceiveMode, SequenceNumber, SessionHold, SessionId,
    SessionLock, SessionRecord, SettlementDisposition, Timestamp, codec, keys,
};

/// Ready entries a single receive may walk past while discarding expired
/// messages. Bounds the work one command performs so a large backlog of
/// expired messages cannot stall the group.
const MAX_RECEIVE_SCAN: usize = 32;

/// Stored messages a single peek may inspect. Peeking is read-only, but it
/// still has to be bounded because expired messages and session filters can be
/// skipped before enough results are collected.
const MAX_PEEK_SCAN: usize = 256;

/// Index entries a single timer sweep may process. A sweep that reports this
/// many may have more waiting, so the worker proposes another command.
pub const TIMER_SCAN_LIMIT: usize = 256;

/// Sessions one acceptance may examine before giving up. A queue whose first
/// `MAX_SESSION_SCAN` sessions are all held reports none available rather than
/// walking an unbounded number of them, and the receiver retries.
const MAX_SESSION_SCAN: usize = 32;

/// Local headroom for the broker's fixed header fields and annotations. This
/// is reserved only against the header limit, not the queue's content limit.
pub const BROKER_HEADER_RESERVE_BYTES: usize = 512;

const BROKER_BASE_HEADER_RESERVE_BYTES: usize = 256;

/// SDK dead-letter reason and description limits, measured in UTF-16 units.
pub const MAX_DEAD_LETTER_DETAIL_LENGTH: usize = 4_096;

/// Configurations inspected by one queue page, excluding its single lookahead.
pub const MAX_QUEUE_PAGE_SIZE: usize = 1_024;

/// Exclusive position in the queue-configuration key order. The named queue
/// need not still exist when the next page is requested.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueCursor {
    pub namespace: NamespaceName,
    pub entity: EntityPath,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueuePage {
    pub queues: Vec<(NamespaceName, EntityPath)>,
    /// The last returned queue, only when the page has more entries after it.
    pub continuation: Option<QueueCursor>,
}

/// A command's ordinary result and effects observed from its committed batch.
/// This is application metadata, not part of the replicated or stored format.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandApplication {
    pub outcome: CommandOutcome,
    /// The final batch retained a ready-index Put in this entity's canonical
    /// dead-letter queue. Only a successful commit can publish this effect.
    pub dead_letters_enqueued: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ExpirationOutcome {
    Dropped,
    DeadLettered,
}

#[derive(Clone, Copy)]
struct MessageInput<'a> {
    message_id: &'a str,
    body: &'a [u8],
    time_to_live_millis: Option<u64>,
    session_id: Option<&'a SessionId>,
    envelope: Option<&'a MessageEnvelope>,
}

struct ScheduledInput<'a> {
    message: MessageInput<'a>,
    enqueue_at: Timestamp,
}

struct DeferredReceiveInput<'a> {
    sequences: &'a [SequenceNumber],
    mode: ReceiveMode,
    lock_duration_millis: Option<u64>,
    session_id: Option<&'a SessionId>,
    budget: Option<DeliveryBudget>,
}

struct ResponseBudget {
    limits: DeliveryBudget,
    used: u64,
}

impl ResponseBudget {
    fn new(limits: DeliveryBudget) -> Self {
        Self { limits, used: 0 }
    }

    fn charge(&mut self, record: &MessageRecord) -> Result<(), BrokerError> {
        let estimate = record.delivery_size_upper_bound();
        let total = estimate
            .checked_add(self.limits.per_message_overhead_bytes)
            .and_then(|bytes| self.used.checked_add(bytes));
        if estimate == u64::MAX || total.is_none_or(|bytes| bytes > self.limits.max_bytes) {
            return Err(BrokerError::MessageTooLarge {
                body_bytes: usize::try_from(total.unwrap_or(u64::MAX)).unwrap_or(usize::MAX),
                maximum_bytes: usize::try_from(self.limits.max_bytes).unwrap_or(usize::MAX),
            });
        }
        self.used = total.expect("the delivery budget total was checked");
        Ok(())
    }
}

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
        Ok(self.apply_with_effects(command)?.outcome)
    }

    /// Applies one command and reports its committed dead-letter enqueue
    /// effect without additional store reads or changes to ordinary outcomes.
    pub fn apply_with_effects(&self, command: &Command) -> Result<CommandApplication, BrokerError> {
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
            } => self.send(
                command,
                MessageInput {
                    message_id,
                    body,
                    time_to_live_millis: *time_to_live_millis,
                    session_id: session_id.as_ref(),
                    envelope: None,
                },
                &mut batch,
            )?,
            CommandKind::SendEnvelope {
                message_id,
                body,
                time_to_live_millis,
                session_id,
                envelope,
            } => self.send(
                command,
                MessageInput {
                    message_id,
                    body,
                    time_to_live_millis: *time_to_live_millis,
                    session_id: session_id.as_ref(),
                    envelope: Some(envelope.as_ref()),
                },
                &mut batch,
            )?,
            CommandKind::Schedule { messages } => self.schedule(
                command,
                messages.iter().map(|message| ScheduledInput {
                    message: MessageInput {
                        message_id: &message.message_id,
                        body: &message.body,
                        time_to_live_millis: message.time_to_live_millis,
                        session_id: message.session_id.as_ref(),
                        envelope: None,
                    },
                    enqueue_at: message.enqueue_at,
                }),
                &mut batch,
            )?,
            CommandKind::ScheduleEnvelopes { messages } => self.schedule(
                command,
                messages.iter().map(|message| ScheduledInput {
                    message: MessageInput {
                        message_id: &message.message_id,
                        body: &message.body,
                        time_to_live_millis: message.time_to_live_millis,
                        session_id: message.session_id.as_ref(),
                        envelope: Some(&message.envelope),
                    },
                    enqueue_at: message.enqueue_at,
                }),
                &mut batch,
            )?,
            CommandKind::CancelScheduled { sequences } => {
                self.cancel_scheduled(command, sequences, &mut batch)?
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
            CommandKind::Peek {
                from_sequence,
                max_messages,
                session_id,
            } => self.peek(
                command,
                *from_sequence,
                *max_messages,
                session_id.as_ref(),
                None,
            )?,
            CommandKind::PeekBounded {
                from_sequence,
                max_messages,
                session_id,
                budget,
            } => self.peek(
                command,
                *from_sequence,
                *max_messages,
                session_id.as_ref(),
                Some(*budget),
            )?,
            CommandKind::Complete {
                sequence,
                lock_token,
            } => self.settle(
                command,
                *sequence,
                *lock_token,
                &SettlementDisposition::Complete,
                None,
                &mut batch,
            )?,
            CommandKind::Abandon {
                sequence,
                lock_token,
            } => self.settle(
                command,
                *sequence,
                *lock_token,
                &SettlementDisposition::Abandon,
                None,
                &mut batch,
            )?,
            CommandKind::DeadLetter {
                sequence,
                lock_token,
                reason,
                description,
            } => self.settle(
                command,
                *sequence,
                *lock_token,
                &SettlementDisposition::DeadLetter {
                    reason: reason.clone(),
                    description: description.clone(),
                },
                None,
                &mut batch,
            )?,
            CommandKind::Defer {
                sequence,
                lock_token,
            } => self.settle(
                command,
                *sequence,
                *lock_token,
                &SettlementDisposition::Defer,
                None,
                &mut batch,
            )?,
            CommandKind::Settle {
                sequence,
                lock_token,
                disposition,
                properties_to_modify,
            } => self.settle(
                command,
                *sequence,
                *lock_token,
                disposition,
                Some(properties_to_modify),
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
            CommandKind::ReceiveDeferred {
                sequences,
                mode,
                lock_duration_millis,
                session_id,
            } => self.receive_deferred(
                command,
                DeferredReceiveInput {
                    sequences,
                    mode: *mode,
                    lock_duration_millis: *lock_duration_millis,
                    session_id: session_id.as_ref(),
                    budget: None,
                },
                &mut batch,
            )?,
            CommandKind::ReceiveDeferredBounded {
                sequences,
                mode,
                lock_duration_millis,
                session_id,
                budget,
            } => self.receive_deferred(
                command,
                DeferredReceiveInput {
                    sequences,
                    mode: *mode,
                    lock_duration_millis: *lock_duration_millis,
                    session_id: session_id.as_ref(),
                    budget: Some(*budget),
                },
                &mut batch,
            )?,
            CommandKind::ReceiveDeferredHeld {
                sequences,
                mode,
                lock_duration_millis,
                session,
                budget,
            } => {
                let config = self.load_config(command)?;
                require_session_agreement(&config, session.is_some())?;
                if let Some(hold) = session {
                    self.held_session(command, hold)?;
                }
                self.receive_deferred(
                    command,
                    DeferredReceiveInput {
                        sequences,
                        mode: *mode,
                        lock_duration_millis: *lock_duration_millis,
                        session_id: session.as_ref().map(|hold| &hold.session_id),
                        budget: Some(*budget),
                    },
                    &mut batch,
                )?
            }
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
            CommandKind::ActivateScheduled => self.activate_scheduled(command, &mut batch)?,
            CommandKind::ExpireDuplicateHistory => {
                self.expire_duplicate_history(command, &mut batch)?
            }
        };

        // A command that changed nothing commits nothing. The clock advance is
        // bookkeeping for the mutations alongside it, and committing it alone
        // would turn every empty receive and every idle timer sweep into a
        // durable write — an fsync apiece on the durable backend. Skipping is
        // deterministic: every replica computes the same empty batch, so every
        // replica skips the same commands.
        let dead_letters_enqueued = committed_dead_letter_put(command, &batch);
        if !batch.is_empty() {
            // Advancing the clock in the same batch keeps the applied timestamp
            // and the state it produced consistent under a crash.
            batch.push_put(keys::clock(), codec::encode(&command.issued_at)?);
            self.store.apply(batch)?;
        }
        Ok(CommandApplication {
            outcome,
            dead_letters_enqueued,
        })
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
        match self.store.get(&keys::queue_config(namespace, entity))? {
            Some(bytes) => Ok(Some(QueueConfig::decode(&bytes)?)),
            None => Ok(None),
        }
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

    /// One bounded keyset page of queue configurations, including DLQ shadows.
    /// Pages reflect their individual reads, not a frozen cross-page snapshot.
    pub fn queues_page(
        &self,
        namespace: Option<&NamespaceName>,
        after: Option<&QueueCursor>,
        limit: usize,
    ) -> Result<QueuePage, BrokerError> {
        if limit > MAX_QUEUE_PAGE_SIZE {
            return Err(BrokerError::QueuePageLimitExceeded {
                limit,
                maximum: MAX_QUEUE_PAGE_SIZE,
            });
        }
        if let (Some(namespace), Some(after)) = (namespace, after)
            && namespace != &after.namespace
        {
            return Err(BrokerError::QueueCursorNamespaceMismatch {
                namespace: namespace.clone(),
                cursor_namespace: after.namespace.clone(),
            });
        }
        if limit == 0 {
            return Ok(QueuePage {
                queues: Vec::new(),
                continuation: None,
            });
        }

        let prefix = namespace.map_or_else(
            keys::queue_config_prefix,
            keys::namespace_queue_config_prefix,
        );
        let start = after.map_or_else(
            || prefix.clone(),
            |after| {
                let mut start = keys::queue_config(&after.namespace, &after.entity);
                start.push(0);
                start
            },
        );
        let records = self.store.scan_from(&prefix, &start, limit + 1)?;
        let has_more = records.len() > limit;
        let queues = records
            .into_iter()
            .take(limit)
            .map(|(key, _)| {
                let (namespace, entity) =
                    keys::entity_scope_parts(&key).ok_or(BrokerError::MalformedIndexKey)?;
                let namespace = NamespaceName::new(namespace)?;
                let entity = EntityPath::new(entity)?;
                if keys::queue_config(&namespace, &entity) != key {
                    return Err(BrokerError::MalformedIndexKey);
                }
                Ok((namespace, entity))
            })
            .collect::<Result<Vec<_>, BrokerError>>()?;
        let continuation = if has_more {
            queues.last().map(|(namespace, entity)| QueueCursor {
                namespace: namespace.clone(),
                entity: entity.clone(),
            })
        } else {
            None
        };
        Ok(QueuePage {
            queues,
            continuation,
        })
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

    /// Reads messages through their version-aware decode rather than treating
    /// their stored shape as stable.
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
            requires_duplicate_detection: false,
            dead_lettering_on_message_expiration: false,
            ..config
        };
        batch.push_put(key, codec::encode(&config)?);
        batch.push_put(
            keys::queue_config(&command.namespace, &dead_letter_queue),
            codec::encode(&shadow)?,
        );
        Ok(CommandOutcome::QueueCreated)
    }

    fn send(
        &self,
        command: &Command,
        message: MessageInput<'_>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        if command.entity.is_dead_letter_queue() {
            return Err(BrokerError::DeadLetterQueueIsReserved);
        }
        let config = self.load_config(command)?;
        validate_message_input(&config, message)?;

        let mut counters = self.load_counters(command)?;
        let sequence = SequenceNumber::new(counters.next_sequence);
        counters.next_sequence = counters.next_sequence.saturating_add(1);

        batch.push_put(
            keys::queue_counters(&command.namespace, &command.entity),
            codec::encode(&counters)?,
        );
        if self.record_message_id(
            command,
            &config,
            message.message_id,
            &mut BTreeSet::new(),
            batch,
        )? {
            return Ok(CommandOutcome::Sent { sequence });
        }

        let expires_at = effective_time_to_live_millis(&config, message.time_to_live_millis)
            .map(|millis| command.issued_at.saturating_add_millis(millis));

        let record = MessageRecord {
            sequence,
            message_id: message.message_id.to_owned(),
            body: message.body.to_vec(),
            enqueued_at: command.issued_at,
            expires_at,
            delivery_count: 0,
            state: MessageState::Ready,
            session_id: message.session_id.cloned(),
            dead_letter: None,
            scheduled_enqueue_time: None,
            envelope: message.envelope.cloned().map(Box::new),
        };

        let namespace = &command.namespace;
        let entity = &command.entity;
        batch.push_put(
            keys::message(namespace, entity, sequence),
            codec::encode(&record)?,
        );
        batch.push_put(self.ready_key(command, &record), Vec::new());
        index_ready_expiry(command, &record, batch);
        Ok(CommandOutcome::Sent { sequence })
    }

    fn schedule<'a>(
        &self,
        command: &Command,
        messages: impl ExactSizeIterator<Item = ScheduledInput<'a>>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        if command.entity.is_dead_letter_queue() {
            return Err(BrokerError::DeadLetterQueueIsReserved);
        }
        let config = self.load_config(command)?;
        let mut counters = self.load_counters(command)?;
        let namespace = &command.namespace;
        let entity = &command.entity;
        let message_count = messages.len();
        let mut sequences = Vec::with_capacity(message_count);
        let mut staged_history = BTreeSet::new();

        for scheduled in messages {
            let message = scheduled.message;
            validate_message_input(&config, message)?;
            let sequence = SequenceNumber::new(counters.next_sequence);
            counters.next_sequence = counters.next_sequence.saturating_add(1);
            sequences.push(sequence);
            if self.record_message_id(
                command,
                &config,
                message.message_id,
                &mut staged_history,
                batch,
            )? {
                continue;
            }
            let time_to_live_millis =
                effective_time_to_live_millis(&config, message.time_to_live_millis);
            let future = scheduled.enqueue_at > command.issued_at;
            let record = MessageRecord {
                sequence,
                message_id: message.message_id.to_owned(),
                body: message.body.to_vec(),
                enqueued_at: command.issued_at,
                expires_at: if future {
                    None
                } else {
                    time_to_live_millis
                        .map(|millis| command.issued_at.saturating_add_millis(millis))
                },
                delivery_count: 0,
                state: if future {
                    MessageState::Scheduled {
                        enqueue_at: scheduled.enqueue_at,
                        time_to_live_millis,
                    }
                } else {
                    MessageState::Ready
                },
                session_id: message.session_id.cloned(),
                dead_letter: None,
                scheduled_enqueue_time: Some(scheduled.enqueue_at),
                envelope: message.envelope.cloned().map(Box::new),
            };
            batch.push_put(
                keys::message(namespace, entity, sequence),
                codec::encode(&record)?,
            );
            if future {
                batch.push_put(
                    keys::scheduled(namespace, entity, scheduled.enqueue_at, sequence),
                    Vec::new(),
                );
            } else {
                batch.push_put(self.ready_key(command, &record), Vec::new());
                index_ready_expiry(command, &record, batch);
            }
        }
        if message_count != 0 {
            batch.push_put(
                keys::queue_counters(namespace, entity),
                codec::encode(&counters)?,
            );
        }
        Ok(CommandOutcome::Scheduled { sequences })
    }

    /// Returns whether this is a duplicate and remembers only newly accepted
    /// identifiers. The overlay makes earlier messages in an uncommitted batch
    /// visible to later messages in that same batch.
    fn record_message_id(
        &self,
        command: &Command,
        config: &QueueConfig,
        message_id: &str,
        staged: &mut BTreeSet<String>,
        batch: &mut WriteBatch,
    ) -> Result<bool, BrokerError> {
        // Empty identifiers represent an omitted message-id at the wire edge;
        // anonymous submissions must not all collapse into one message.
        if !config.requires_duplicate_detection || message_id.is_empty() {
            return Ok(false);
        }
        if staged.contains(message_id) {
            return Ok(true);
        }
        let namespace = &command.namespace;
        let entity = &command.entity;
        let key = keys::duplicate_history(namespace, entity, message_id);
        if let Some(expires_at) = self.read::<Timestamp>(&key)? {
            if expires_at > command.issued_at {
                return Ok(true);
            }
            batch.push_delete(keys::duplicate_history_expiry(
                namespace, entity, expires_at, message_id,
            ));
        }
        let expires_at = command
            .issued_at
            .saturating_add_millis(config.duplicate_detection_history_time_window_millis);
        batch.push_put(key, codec::encode(&expires_at)?);
        batch.push_put(
            keys::duplicate_history_expiry(namespace, entity, expires_at, message_id),
            Vec::new(),
        );
        staged.insert(message_id.to_owned());
        Ok(false)
    }

    fn expire_duplicate_history(
        &self,
        command: &Command,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        self.load_config(command)?;
        let namespace = &command.namespace;
        let entity = &command.entity;
        let prefix = keys::duplicate_history_expiry_prefix(namespace, entity);
        let entries = self.store.scan_prefix(&prefix, TIMER_SCAN_LIMIT)?;
        let mut expired = 0;

        for (key, _) in entries {
            let (expires_at, message_id) = keys::duplicate_history_expiry_parts(&prefix, &key)
                .ok_or(BrokerError::MalformedIndexKey)?;
            if expires_at > command.issued_at {
                break;
            }
            let history_key = keys::duplicate_history(namespace, entity, message_id);
            // A stale cleanup entry must never remove a more recent retention.
            if self.read::<Timestamp>(&history_key)? == Some(expires_at) {
                batch.push_delete(history_key);
            }
            batch.push_delete(key);
            expired += 1;
        }
        Ok(CommandOutcome::DuplicateHistoryExpired { expired })
    }

    fn cancel_scheduled(
        &self,
        command: &Command,
        sequences: &[SequenceNumber],
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        self.load_config(command)?;
        let namespace = &command.namespace;
        let entity = &command.entity;
        let mut cancelled = 0_u32;

        for sequence in sequences.iter().copied().collect::<BTreeSet<_>>() {
            let record = self.load_message(command, sequence)?;
            let MessageState::Scheduled { enqueue_at, .. } = record.state else {
                return Err(BrokerError::MessageNotScheduled { sequence });
            };
            batch.push_delete(keys::message(namespace, entity, sequence));
            batch.push_delete(keys::scheduled(namespace, entity, enqueue_at, sequence));
            cancelled = cancelled.saturating_add(1);
        }
        Ok(CommandOutcome::ScheduledCancelled { cancelled })
    }

    fn activate_scheduled(
        &self,
        command: &Command,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        self.load_config(command)?;
        let namespace = &command.namespace;
        let entity = &command.entity;
        let scheduled = self
            .store
            .scan_prefix(&keys::scheduled_prefix(namespace, entity), TIMER_SCAN_LIMIT)?;
        let mut counters = self.load_counters(command)?;
        let mut activated = 0;

        for (key, _) in scheduled {
            let (enqueue_at, scheduled_sequence) =
                keys::trailing_deadline(&key).ok_or(BrokerError::MalformedIndexKey)?;
            if enqueue_at > command.issued_at {
                break;
            }
            let mut record = self.message(namespace, entity, scheduled_sequence)?.ok_or(
                BrokerError::DanglingIndexEntry {
                    sequence: scheduled_sequence,
                },
            )?;
            let time_to_live_millis = match record.state {
                MessageState::Scheduled {
                    enqueue_at: stored_enqueue_at,
                    time_to_live_millis,
                } if stored_enqueue_at == enqueue_at => time_to_live_millis,
                _ => return Err(BrokerError::MalformedIndexKey),
            };

            // The scheduling sequence is only a cancellation handle. Activation
            // gets a new queue position so older scheduled work cannot jump
            // ahead of messages that became active first.
            record.sequence = SequenceNumber::new(counters.next_sequence);
            counters.next_sequence = counters.next_sequence.saturating_add(1);
            record.state = MessageState::Ready;
            record.enqueued_at = command.issued_at;
            record.expires_at =
                time_to_live_millis.map(|millis| command.issued_at.saturating_add_millis(millis));
            batch.push_delete(key);
            batch.push_delete(keys::message(namespace, entity, scheduled_sequence));
            batch.push_put(
                keys::message(namespace, entity, record.sequence),
                codec::encode(&record)?,
            );
            batch.push_put(self.ready_key(command, &record), Vec::new());
            index_ready_expiry(command, &record, batch);
            activated += 1;
        }
        if activated != 0 {
            batch.push_put(
                keys::queue_counters(namespace, entity),
                codec::encode(&counters)?,
            );
        }
        Ok(CommandOutcome::ScheduledActivated { activated })
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
                self.expire_message(command, &config, record, batch)?;
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

                    let locked_until = command.issued_at.saturating_add_millis(
                        lock_duration_millis.unwrap_or(config.lock_duration_millis),
                    );
                    record.state = MessageState::Locked {
                        token,
                        locked_until,
                    };

                    batch.push_delete(ready_key.clone());
                    remove_expiry_index(command, &record, batch);
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

            let time_to_live_millis = record.time_to_live_millis();
            return Ok(CommandOutcome::Received(Some(Delivery {
                sequence,
                message_id: record.message_id,
                body: record.body,
                enqueued_at: record.enqueued_at,
                expires_at: record.expires_at,
                time_to_live_millis,
                envelope: record.envelope,
                delivery_count,
                status: MessageStatus::Active,
                scheduled_enqueue_time: record.scheduled_enqueue_time,
                lock,
                session_id: record.session_id,
                dead_letter: record.dead_letter,
            })));
        }

        Ok(CommandOutcome::Received(None))
    }

    fn peek(
        &self,
        command: &Command,
        from_sequence: SequenceNumber,
        max_messages: u32,
        session_id: Option<&SessionId>,
        budget: Option<DeliveryBudget>,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        require_session_agreement(&config, session_id.is_some())?;
        if max_messages == 0 {
            return Ok(CommandOutcome::Peeked(Vec::new()));
        }

        let namespace = &command.namespace;
        let entity = &command.entity;
        let limit = usize::try_from(max_messages)
            .unwrap_or(usize::MAX)
            .min(MAX_PEEK_SCAN);
        let prefix = keys::message_prefix(namespace, entity);
        let mut start = keys::message(namespace, entity, from_sequence);
        let mut deliveries = Vec::with_capacity(limit);
        let mut response_budget = budget.map(ResponseBudget::new);
        let mut inspected = 0;

        while inspected < MAX_PEEK_SCAN && deliveries.len() < limit {
            // Bounded browsing must not materialize an entire page of large
            // records before discovering that only its first entry fits.
            let scan_limit = if budget.is_some() { 1 } else { MAX_PEEK_SCAN };
            let records =
                self.store
                    .scan_from(&prefix, &start, scan_limit.min(MAX_PEEK_SCAN - inspected))?;
            if records.is_empty() {
                break;
            }
            for (key, value) in records {
                inspected += 1;
                start = key.clone();
                start.push(0);
                let sequence =
                    keys::trailing_sequence(&key).ok_or(BrokerError::MalformedIndexKey)?;
                let record = MessageRecord::decode(&value)?;

                let protected_from_expiry = match record.state {
                    MessageState::Deferred => true,
                    MessageState::Locked { locked_until, .. } => locked_until > command.issued_at,
                    _ => false,
                };
                if record.is_expired_at(command.issued_at) && !protected_from_expiry {
                    continue;
                }
                if session_id
                    .is_some_and(|session_id| record.session_id.as_ref() != Some(session_id))
                {
                    continue;
                }
                if let Some(budget) = &mut response_budget
                    && let Err(error) = budget.charge(&record)
                {
                    if deliveries.is_empty() {
                        return Err(error);
                    }
                    return Ok(CommandOutcome::Peeked(deliveries));
                }

                let status = record.status();
                let scheduled_enqueue_time = record.scheduled_enqueue_time;
                let time_to_live_millis = record.time_to_live_millis();
                deliveries.push(Delivery {
                    sequence,
                    message_id: record.message_id,
                    body: record.body,
                    enqueued_at: record.enqueued_at,
                    expires_at: record.expires_at,
                    time_to_live_millis,
                    envelope: record.envelope,
                    delivery_count: record.delivery_count,
                    status,
                    scheduled_enqueue_time,
                    lock: None,
                    session_id: record.session_id,
                    dead_letter: record.dead_letter,
                });
                if deliveries.len() == limit {
                    break;
                }
            }
        }

        Ok(CommandOutcome::Peeked(deliveries))
    }

    fn settle(
        &self,
        command: &Command,
        sequence: SequenceNumber,
        lock_token: LockToken,
        disposition: &SettlementDisposition,
        properties_to_modify: Option<&BTreeMap<String, MessageValue>>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let (mut record, locked_until) = self.held_lock(command, sequence, lock_token)?;
        if command.entity.is_dead_letter_queue()
            && matches!(disposition, SettlementDisposition::DeadLetter { .. })
        {
            return Err(BrokerError::DeadLetterQueueIsReserved);
        }
        let config = self.load_config(command)?;
        if let Some(properties) = properties_to_modify.filter(|properties| !properties.is_empty()) {
            MessageEnvelope::validate_application_property_updates(properties)?;
            if record.envelope.is_none() {
                record.envelope = Some(Box::new(legacy_envelope(&record)));
            }
            record
                .envelope
                .as_mut()
                .expect("the content envelope was initialized")
                .application_properties
                .extend(properties.clone());
            validate_message_input(
                &config,
                MessageInput {
                    message_id: &record.message_id,
                    body: &record.body,
                    time_to_live_millis: record.time_to_live_millis(),
                    session_id: record.session_id.as_ref(),
                    envelope: record.envelope.as_deref(),
                },
            )?;
        }
        let namespace = &command.namespace;
        let entity = &command.entity;
        match disposition {
            SettlementDisposition::Complete => {
                self.remove_message(command, &record, batch);
                Ok(CommandOutcome::Completed)
            }
            SettlementDisposition::Abandon => {
                if record.is_expired_at(command.issued_at) {
                    let expiration = self.expire_message(command, &config, record, batch)?;
                    return Ok(CommandOutcome::Abandoned {
                        dead_lettered: expiration == ExpirationOutcome::DeadLettered,
                        dropped: expiration == ExpirationOutcome::Dropped,
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
                        dropped: false,
                    });
                }
                record.state = MessageState::Ready;
                batch.push_delete(keys::lock(namespace, entity, locked_until, sequence));
                batch.push_put(
                    keys::message(namespace, entity, sequence),
                    codec::encode(&record)?,
                );
                batch.push_put(self.ready_key(command, &record), Vec::new());
                index_ready_expiry(command, &record, batch);
                Ok(CommandOutcome::Abandoned {
                    dead_lettered: false,
                    dropped: false,
                })
            }
            SettlementDisposition::Defer => {
                record.state = MessageState::Deferred;
                remove_expiry_index(command, &record, batch);
                batch.push_delete(keys::lock(namespace, entity, locked_until, sequence));
                batch.push_put(
                    keys::message(namespace, entity, sequence),
                    codec::encode(&record)?,
                );
                Ok(CommandOutcome::Deferred)
            }
            SettlementDisposition::DeadLetter {
                reason,
                description,
            } => {
                validate_dead_letter_detail("reason", reason)?;
                validate_dead_letter_detail("description", description)?;
                self.move_to_dead_letter(
                    command,
                    record,
                    DeadLetterReason::Application(reason.clone()),
                    description.clone(),
                    batch,
                )?;
                Ok(CommandOutcome::DeadLettered)
            }
        }
    }

    fn receive_deferred(
        &self,
        command: &Command,
        input: DeferredReceiveInput<'_>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let DeferredReceiveInput {
            sequences,
            mode,
            lock_duration_millis,
            session_id,
            budget,
        } = input;
        let mut unique = BTreeSet::new();
        if sequences.iter().any(|sequence| !unique.insert(*sequence)) {
            return Err(BrokerError::InvalidMessageContent {
                reason: String::from("deferred receive sequence numbers must be unique"),
            });
        }
        let config = self.load_config(command)?;
        require_session_agreement(&config, session_id.is_some())?;
        let namespace = &command.namespace;
        let entity = &command.entity;
        let mut deliveries = if budget.is_some() {
            Vec::new()
        } else {
            Vec::with_capacity(sequences.len())
        };
        let mut counters = None;
        let mut response_budget = budget.map(ResponseBudget::new);

        for sequence in sequences {
            let mut record = self.load_message(command, *sequence)?;
            if record.state != MessageState::Deferred {
                return Err(BrokerError::MessageNotDeferred {
                    sequence: *sequence,
                });
            }
            if session_id.is_some_and(|session_id| record.session_id.as_ref() != Some(session_id)) {
                return Err(BrokerError::MessageNotDeferred {
                    sequence: *sequence,
                });
            }
            if let Some(budget) = &mut response_budget {
                budget.charge(&record)?;
            }
            if record.is_expired_at(command.issued_at) {
                self.expire_message(command, &config, record, batch)?;
                continue;
            }

            record.delivery_count = record.delivery_count.saturating_add(1);
            let delivery_count = record.delivery_count;
            let lock = match mode {
                ReceiveMode::PeekLock => {
                    let counters = counters.get_or_insert(self.load_counters(command)?);
                    let token = LockToken::new(counters.next_lock_token);
                    counters.next_lock_token = counters.next_lock_token.saturating_add(1);
                    let locked_until = command.issued_at.saturating_add_millis(
                        lock_duration_millis.unwrap_or(config.lock_duration_millis),
                    );
                    record.state = MessageState::Locked {
                        token,
                        locked_until,
                    };
                    remove_expiry_index(command, &record, batch);
                    batch.push_put(
                        keys::message(namespace, entity, *sequence),
                        codec::encode(&record)?,
                    );
                    batch.push_put(
                        keys::lock(namespace, entity, locked_until, *sequence),
                        Vec::new(),
                    );
                    Some(DeliveryLock {
                        token,
                        locked_until,
                    })
                }
                ReceiveMode::ReceiveAndDelete => {
                    batch.push_delete(keys::message(namespace, entity, *sequence));
                    if let Some(expires_at) = record.expires_at {
                        batch.push_delete(keys::expiry(namespace, entity, expires_at, *sequence));
                    }
                    None
                }
            };

            let time_to_live_millis = record.time_to_live_millis();
            deliveries.push(Delivery {
                sequence: *sequence,
                message_id: record.message_id,
                body: record.body,
                enqueued_at: record.enqueued_at,
                expires_at: record.expires_at,
                time_to_live_millis,
                envelope: record.envelope,
                delivery_count,
                status: MessageStatus::Active,
                scheduled_enqueue_time: record.scheduled_enqueue_time,
                lock,
                session_id: record.session_id,
                dead_letter: record.dead_letter,
            });
        }

        if let Some(counters) = counters {
            batch.push_put(
                keys::queue_counters(namespace, entity),
                codec::encode(&counters)?,
            );
        }
        Ok(CommandOutcome::DeferredReceived(deliveries))
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
        let mut dropped = 0;
        for (key, _) in locks {
            let (locked_until, sequence) =
                keys::trailing_deadline(&key).ok_or(BrokerError::MalformedIndexKey)?;
            // The index is ordered by deadline, so the first lock still held
            // ends the sweep.
            if locked_until > command.issued_at {
                break;
            }

            let mut record = self
                .message(namespace, entity, sequence)?
                .ok_or(BrokerError::DanglingIndexEntry { sequence })?;

            if record.is_expired_at(command.issued_at) {
                match self.expire_message(command, &config, record, batch)? {
                    ExpirationOutcome::DeadLettered => dead_lettered += 1,
                    ExpirationOutcome::Dropped => dropped += 1,
                }
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
                record.state = MessageState::Ready;
                batch.push_delete(key);
                batch.push_put(
                    keys::message(namespace, entity, sequence),
                    codec::encode(&record)?,
                );
                batch.push_put(self.ready_key(command, &record), Vec::new());
                index_ready_expiry(command, &record, batch);
                returned_to_ready += 1;
            }
        }

        Ok(CommandOutcome::LocksExpired {
            returned_to_ready,
            dead_lettered,
            dropped,
        })
    }

    fn expire_messages(
        &self,
        command: &Command,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        let namespace = &command.namespace;
        let entity = &command.entity;
        let expiring = self
            .store
            .scan_prefix(&keys::expiry_prefix(namespace, entity), TIMER_SCAN_LIMIT)?;

        let mut dead_lettered = 0;
        let mut dropped = 0;
        let mut processed = 0;
        for (key, _) in expiring {
            let (expires_at, sequence) =
                keys::trailing_deadline(&key).ok_or(BrokerError::MalformedIndexKey)?;
            if expires_at > command.issued_at {
                break;
            }

            let record = self
                .message(namespace, entity, sequence)?
                .ok_or(BrokerError::DanglingIndexEntry { sequence })?;
            if record.expires_at != Some(expires_at) {
                return Err(BrokerError::MalformedIndexKey);
            }
            match record.state {
                MessageState::Ready => {
                    match self.expire_message(command, &config, record, batch)? {
                        ExpirationOutcome::DeadLettered => dead_lettered += 1,
                        ExpirationOutcome::Dropped => dropped += 1,
                    }
                }
                // Older snapshots indexed every finite lifetime. Suspend those
                // entries without letting protected locks pin later expirations.
                MessageState::Locked { .. } | MessageState::Deferred => batch.push_delete(key),
                MessageState::Scheduled { .. } => return Err(BrokerError::MalformedIndexKey),
            }
            processed += 1;
        }

        Ok(CommandOutcome::MessagesExpired {
            dead_lettered,
            dropped,
            processed,
        })
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
        let locked_until = command
            .issued_at
            .saturating_add_millis(lock_duration_millis.unwrap_or(config.lock_duration_millis));

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
        let (mut record, previous_locked_until) = self.held_lock(command, sequence, lock_token)?;
        let locked_until = command
            .issued_at
            .saturating_add_millis(lock_duration_millis.unwrap_or(config.lock_duration_millis));

        record.state = MessageState::Locked {
            token: lock_token,
            locked_until,
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
        Ok(CommandOutcome::LockRenewed { locked_until })
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
    ) -> Result<(MessageRecord, Timestamp), BrokerError> {
        let record = self.load_message(command, sequence)?;
        match record.state {
            MessageState::Locked {
                token,
                locked_until,
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
                Ok((record, locked_until))
            }
            _ => Err(BrokerError::MessageNotLocked { sequence }),
        }
    }

    fn expire_message(
        &self,
        command: &Command,
        config: &QueueConfig,
        record: MessageRecord,
        batch: &mut WriteBatch,
    ) -> Result<ExpirationOutcome, BrokerError> {
        if config.dead_lettering_on_message_expiration {
            self.move_to_dead_letter(
                command,
                record,
                DeadLetterReason::TimeToLiveExpired,
                String::from("the message exceeded its time to live"),
                batch,
            )?;
            Ok(ExpirationOutcome::DeadLettered)
        } else {
            self.remove_message(command, &record, batch);
            Ok(ExpirationOutcome::Dropped)
        }
    }

    fn remove_message(&self, command: &Command, record: &MessageRecord, batch: &mut WriteBatch) {
        let namespace = &command.namespace;
        let entity = &command.entity;
        let sequence = record.sequence;
        match record.state {
            MessageState::Ready => batch.push_delete(self.ready_key(command, record)),
            MessageState::Locked { locked_until, .. } => {
                batch.push_delete(keys::lock(namespace, entity, locked_until, sequence));
            }
            MessageState::Deferred => {}
            MessageState::Scheduled { enqueue_at, .. } => {
                batch.push_delete(keys::scheduled(namespace, entity, enqueue_at, sequence));
            }
        }
        batch.push_delete(keys::message(namespace, entity, sequence));
        remove_expiry_index(command, record, batch);
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
        validate_dead_letter_projection(&record, &reason, &description)?;
        let namespace = &command.namespace;
        let entity = &command.entity;
        let sequence = record.sequence;

        self.remove_message(command, &record, batch);

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

fn committed_dead_letter_put(command: &Command, batch: &WriteBatch) -> bool {
    if batch.is_empty() || command.entity.is_dead_letter_queue() {
        return false;
    }
    let Ok(shadow) = command.entity.dead_letter_queue() else {
        return false;
    };
    let prefix = keys::ready_prefix(&command.namespace, &shadow);
    let mut seen = BTreeSet::new();
    // Backends apply in batch order. A later Delete must suppress an earlier
    // Put; unrelated mutations do not contribute an enqueue effect.
    batch.mutations().iter().rev().any(|mutation| {
        let key = match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key,
        };
        key.len() == prefix.len() + 8
            && key.starts_with(&prefix)
            && seen.insert(key.as_slice())
            && matches!(mutation, Mutation::Put { .. })
    })
}

fn effective_time_to_live_millis(config: &QueueConfig, requested: Option<u64>) -> Option<u64> {
    match (requested, config.default_time_to_live_millis) {
        (Some(requested), Some(ceiling)) => Some(requested.min(ceiling)),
        (requested, ceiling) => requested.or(ceiling),
    }
}

fn index_ready_expiry(command: &Command, record: &MessageRecord, batch: &mut WriteBatch) {
    if let (MessageState::Ready, Some(expires_at)) = (&record.state, record.expires_at) {
        batch.push_put(
            keys::expiry(
                &command.namespace,
                &command.entity,
                expires_at,
                record.sequence,
            ),
            Vec::new(),
        );
    }
}

fn remove_expiry_index(command: &Command, record: &MessageRecord, batch: &mut WriteBatch) {
    if let Some(expires_at) = record.expires_at {
        batch.push_delete(keys::expiry(
            &command.namespace,
            &command.entity,
            expires_at,
            record.sequence,
        ));
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

fn validate_message_id(message_id: &str) -> Result<(), BrokerError> {
    let length = message_id.encode_utf16().count();
    if length > MAX_MESSAGE_ID_LENGTH {
        return Err(BrokerError::MessageIdTooLong {
            length,
            maximum: MAX_MESSAGE_ID_LENGTH,
        });
    }
    Ok(())
}

fn legacy_envelope(record: &MessageRecord) -> MessageEnvelope {
    MessageEnvelope {
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::String(record.message_id.clone())),
            ..MessageProperties::default()
        },
        body: MessageBody::Data(vec![record.body.clone()]),
        ..MessageEnvelope::default()
    }
}

fn validate_dead_letter_detail(field: &str, value: &str) -> Result<(), BrokerError> {
    let length = value.encode_utf16().count();
    if length > MAX_DEAD_LETTER_DETAIL_LENGTH {
        return Err(BrokerError::InvalidMessageContent {
            reason: format!(
                "dead-letter {field} length of {length} exceeds the {MAX_DEAD_LETTER_DETAIL_LENGTH}-character limit"
            ),
        });
    }
    Ok(())
}

fn validate_dead_letter_projection(
    record: &MessageRecord,
    reason: &DeadLetterReason,
    description: &str,
) -> Result<(), BrokerError> {
    let mut projected = record
        .envelope
        .as_deref()
        .cloned()
        .unwrap_or_else(|| legacy_envelope(record));
    projected.application_properties.insert(
        String::from("DeadLetterReason"),
        MessageValue::String(reason.as_str().to_owned()),
    );
    projected.application_properties.insert(
        String::from("DeadLetterErrorDescription"),
        MessageValue::String(description.to_owned()),
    );
    projected.properties.absolute_expiry_time = None;
    // Only the emitted canonical fields count here. The producer envelope is
    // retained unchanged; the ingress reserve already covered these additions.
    projected.validate_property_quotas()?;
    validate_broker_header_reserve(
        &projected,
        &record.message_id,
        None,
        BROKER_BASE_HEADER_RESERVE_BYTES,
    )
}

fn authoritative_property_overhead(
    envelope: &MessageEnvelope,
    message_id: &str,
    session_id: Option<&SessionId>,
) -> usize {
    let mut size = if envelope.properties.message_id.is_none() {
        5_usize.saturating_add(message_id.len())
    } else {
        0
    };
    if let Some(session_id) = session_id {
        size = size
            .saturating_add(5)
            .saturating_add(session_id.as_str().len());
    }
    size
}

fn validate_envelope_content(
    envelope: &MessageEnvelope,
    message_id: &str,
    session_id: Option<&SessionId>,
) -> Result<(), BrokerError> {
    envelope.validate()?;
    validate_broker_header_reserve(
        envelope,
        message_id,
        session_id,
        BROKER_HEADER_RESERVE_BYTES,
    )
}

fn validate_broker_header_reserve(
    envelope: &MessageEnvelope,
    message_id: &str,
    session_id: Option<&SessionId>,
    reserve_bytes: usize,
) -> Result<(), BrokerError> {
    let header_bytes = envelope
        .header_content_size()
        .saturating_add(authoritative_property_overhead(
            envelope, message_id, session_id,
        ))
        .saturating_add(reserve_bytes);
    if header_bytes > MAX_MESSAGE_HEADER_BYTES {
        return Err(BrokerError::MessageHeaderTooLarge {
            header_bytes,
            maximum_bytes: MAX_MESSAGE_HEADER_BYTES,
        });
    }
    Ok(())
}

fn validate_message_input(
    config: &QueueConfig,
    message: MessageInput<'_>,
) -> Result<(), BrokerError> {
    require_session_agreement(config, message.session_id.is_some())?;
    validate_message_id(message.message_id)?;
    if let Some(envelope) = message.envelope {
        if let Some(crate::MessageIdentifier::String(message_id)) = &envelope.properties.message_id
        {
            validate_message_id(message_id)?;
        }
        validate_envelope_content(envelope, message.message_id, message.session_id)?;
    }
    let content_bytes = message.envelope.map_or(message.body.len(), |envelope| {
        let size = envelope
            .content_size()
            .saturating_add(authoritative_property_overhead(
                envelope,
                message.message_id,
                message.session_id,
            ));
        // The byte body is only a compatibility view when typed content exists.
        // Count the larger representation, not both copies of the same body.
        size.max(message.body.len())
    });
    if content_bytes > config.max_message_bytes {
        return Err(BrokerError::MessageTooLarge {
            body_bytes: content_bytes,
            maximum_bytes: config.max_message_bytes,
        });
    }
    Ok(())
}

#[cfg(test)]
mod effect_tests {
    use super::*;

    #[test]
    fn dead_letter_effect_uses_final_exact_shadow_ready_mutations() {
        let namespace = NamespaceName::new("tenant").expect("namespace");
        let entity = EntityPath::new("orders").expect("entity");
        let shadow = entity.dead_letter_queue().expect("shadow");
        let command = Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::UNIX_EPOCH,
            CommandKind::ExpireMessages,
        );
        let first = keys::ready(&namespace, &shadow, SequenceNumber::new(1));
        let second = keys::ready(&namespace, &shadow, SequenceNumber::new(2));
        let mut short = first.clone();
        short.pop();
        let mut extended = first.clone();
        extended.push(0);
        for (name, batch, expected) in [
            ("empty", WriteBatch::default(), false),
            (
                "shadow config",
                WriteBatch::default().put(keys::queue_config(&namespace, &shadow), Vec::new()),
                false,
            ),
            (
                "shadow message",
                WriteBatch::default().put(
                    keys::message(&namespace, &shadow, SequenceNumber::new(1)),
                    Vec::new(),
                ),
                false,
            ),
            (
                "source ready",
                WriteBatch::default().put(
                    keys::ready(&namespace, &entity, SequenceNumber::new(1)),
                    Vec::new(),
                ),
                false,
            ),
            (
                "other namespace",
                WriteBatch::default().put(
                    keys::ready(
                        &NamespaceName::new("tenant-two").expect("namespace"),
                        &shadow,
                        SequenceNumber::new(1),
                    ),
                    Vec::new(),
                ),
                false,
            ),
            (
                "other entity",
                WriteBatch::default().put(
                    keys::ready(
                        &namespace,
                        &EntityPath::new("other/$deadletterqueue").expect("entity"),
                        SequenceNumber::new(1),
                    ),
                    Vec::new(),
                ),
                false,
            ),
            (
                "shadow of shadow",
                WriteBatch::default().put(
                    keys::ready(
                        &namespace,
                        &shadow.dead_letter_queue().expect("nested shadow"),
                        SequenceNumber::new(1),
                    ),
                    Vec::new(),
                ),
                false,
            ),
            (
                "short key",
                WriteBatch::default().put(short, Vec::new()),
                false,
            ),
            (
                "extended key",
                WriteBatch::default().put(extended, Vec::new()),
                false,
            ),
            (
                "surviving put",
                WriteBatch::default().put(first.clone(), Vec::new()),
                true,
            ),
            (
                "put then delete",
                WriteBatch::default()
                    .put(first.clone(), Vec::new())
                    .delete(first.clone()),
                false,
            ),
            (
                "delete then put",
                WriteBatch::default()
                    .delete(first.clone())
                    .put(first.clone(), Vec::new()),
                true,
            ),
            (
                "put overwritten",
                WriteBatch::default()
                    .put(first.clone(), vec![1])
                    .put(first.clone(), vec![2]),
                true,
            ),
            (
                "all puts deleted",
                WriteBatch::default()
                    .put(first.clone(), Vec::new())
                    .put(second.clone(), Vec::new())
                    .delete(first.clone())
                    .delete(second.clone()),
                false,
            ),
            (
                "another put remains",
                WriteBatch::default()
                    .put(first.clone(), Vec::new())
                    .delete(first.clone())
                    .put(second, Vec::new()),
                true,
            ),
        ] {
            assert_eq!(
                committed_dead_letter_put(&command, &batch),
                expected,
                "{name}"
            );
        }
        let mut shadow_command = command;
        shadow_command.entity = shadow.clone();
        assert!(!committed_dead_letter_put(
            &shadow_command,
            &WriteBatch::default().put(first, Vec::new())
        ));
        assert!(!committed_dead_letter_put(
            &shadow_command,
            &WriteBatch::default().put(
                keys::ready(
                    &namespace,
                    &shadow.dead_letter_queue().expect("nested shadow"),
                    SequenceNumber::new(1)
                ),
                Vec::new(),
            )
        ));
    }
}
