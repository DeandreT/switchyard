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

use serde::de::DeserializeOwned;
use storage::{StateStore, WriteBatch};

use crate::{
    BrokerError, Command, CommandKind, CommandOutcome, DeadLetterInfo, DeadLetterReason, Delivery,
    DeliveryLock, EntityPath, LockToken, MessageRecord, MessageState, NamespaceName, QueueConfig,
    QueueCounters, ReceiveMode, SequenceNumber, Timestamp, codec, keys,
};

/// Ready entries a single receive may walk past while discarding expired
/// messages. Bounds the work one command performs so a large backlog of
/// expired messages cannot stall the group.
const MAX_RECEIVE_SCAN: usize = 32;

/// Index entries a single timer sweep may process. The worker proposes another
/// command when it reaches this limit.
const MAX_TIMER_SCAN: usize = 256;

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
            } => self.send(command, message_id, body, *time_to_live_millis, &mut batch)?,
            CommandKind::Receive {
                mode,
                lock_duration_millis,
            } => self.receive(command, *mode, *lock_duration_millis, &mut batch)?,
            CommandKind::Complete {
                sequence,
                lock_token,
            } => self.complete(command, *sequence, *lock_token, &mut batch)?,
            CommandKind::Abandon {
                sequence,
                lock_token,
            } => self.abandon(command, *sequence, *lock_token, &mut batch)?,
            CommandKind::DeadLetter {
                sequence,
                lock_token,
                reason,
                description,
            } => self.dead_letter(
                command,
                *sequence,
                *lock_token,
                reason,
                description,
                &mut batch,
            )?,
            CommandKind::ExpireLocks => self.expire_locks(command, &mut batch)?,
            CommandKind::ExpireMessages => self.expire_messages(command, &mut batch)?,
        };

        // Advancing the clock in the same batch keeps the applied timestamp and
        // the state it produced consistent under a crash.
        batch.push_put(keys::clock(), codec::encode(&command.issued_at)?);
        self.store.apply(batch)?;
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

    pub fn message(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        sequence: SequenceNumber,
    ) -> Result<Option<MessageRecord>, BrokerError> {
        self.read(&keys::message(namespace, entity, sequence))
    }

    pub fn dead_lettered_message(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        sequence: SequenceNumber,
    ) -> Result<Option<MessageRecord>, BrokerError> {
        self.read(&keys::dead_letter(namespace, entity, sequence))
    }

    pub fn ready_sequences(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        limit: usize,
    ) -> Result<Vec<SequenceNumber>, BrokerError> {
        self.index_sequences(&keys::ready_prefix(namespace, entity), limit)
    }

    pub fn dead_lettered_sequences(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        limit: usize,
    ) -> Result<Vec<SequenceNumber>, BrokerError> {
        self.index_sequences(&keys::dead_letter_prefix(namespace, entity), limit)
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
        let key = keys::queue_config(&command.namespace, &command.entity);
        if self.store.get(&key)?.is_some() {
            return Err(BrokerError::QueueAlreadyExists);
        }
        let config = config.validate()?;
        batch.push_put(key, codec::encode(&config)?);
        Ok(CommandOutcome::QueueCreated)
    }

    fn send(
        &self,
        command: &Command,
        message_id: &str,
        body: &[u8],
        time_to_live_millis: Option<u64>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        if body.len() > config.max_message_bytes {
            return Err(BrokerError::MessageTooLarge {
                body_bytes: body.len(),
                maximum_bytes: config.max_message_bytes,
            });
        }

        let mut counters = self.load_counters(command)?;
        let sequence = SequenceNumber::new(counters.next_sequence);
        counters.next_sequence = counters.next_sequence.saturating_add(1);

        let expires_at = time_to_live_millis
            .or(config.default_time_to_live_millis)
            .map(|millis| command.issued_at.saturating_add_millis(millis));

        let record = MessageRecord {
            sequence,
            message_id: message_id.to_owned(),
            body: body.to_vec(),
            enqueued_at: command.issued_at,
            expires_at,
            delivery_count: 0,
            state: MessageState::Ready,
        };

        let namespace = &command.namespace;
        let entity = &command.entity;
        batch.push_put(
            keys::message(namespace, entity, sequence),
            codec::encode(&record)?,
        );
        batch.push_put(keys::ready(namespace, entity, sequence), Vec::new());
        if let Some(expires_at) = expires_at {
            batch.push_put(
                keys::expiry(namespace, entity, expires_at, sequence),
                Vec::new(),
            );
        }
        batch.push_put(
            keys::queue_counters(namespace, entity),
            codec::encode(&counters)?,
        );
        Ok(CommandOutcome::Sent { sequence })
    }

    fn receive(
        &self,
        command: &Command,
        mode: ReceiveMode,
        lock_duration_millis: Option<u64>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        let namespace = &command.namespace;
        let entity = &command.entity;
        let ready = self
            .store
            .scan_prefix(&keys::ready_prefix(namespace, entity), MAX_RECEIVE_SCAN)?;

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

                    batch.push_delete(keys::ready(namespace, entity, sequence));
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
                    batch.push_delete(keys::ready(namespace, entity, sequence));
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
                delivery_count,
                lock,
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
        let (record, locked_until) = self.held_lock(command, sequence, lock_token)?;
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
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        let (mut record, locked_until) = self.held_lock(command, sequence, lock_token)?;

        if record.delivery_count >= config.max_delivery_count {
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

        let namespace = &command.namespace;
        let entity = &command.entity;
        record.state = MessageState::Ready;
        batch.push_delete(keys::lock(namespace, entity, locked_until, sequence));
        batch.push_put(
            keys::message(namespace, entity, sequence),
            codec::encode(&record)?,
        );
        batch.push_put(keys::ready(namespace, entity, sequence), Vec::new());
        Ok(CommandOutcome::Abandoned {
            dead_lettered: false,
        })
    }

    fn dead_letter(
        &self,
        command: &Command,
        sequence: SequenceNumber,
        lock_token: LockToken,
        reason: &str,
        description: &str,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let (record, _) = self.held_lock(command, sequence, lock_token)?;
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
            .scan_prefix(&keys::lock_prefix(namespace, entity), MAX_TIMER_SCAN)?;

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

            let mut record = self
                .message(namespace, entity, sequence)?
                .ok_or(BrokerError::DanglingIndexEntry { sequence })?;

            if record.delivery_count >= config.max_delivery_count {
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
                batch.push_put(keys::ready(namespace, entity, sequence), Vec::new());
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
            .scan_prefix(&keys::expiry_prefix(namespace, entity), MAX_TIMER_SCAN)?;

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
            self.move_to_dead_letter(
                command,
                record,
                DeadLetterReason::TimeToLiveExpired,
                String::from("the message exceeded its time to live"),
                batch,
            )?;
            dead_lettered += 1;
        }

        Ok(CommandOutcome::MessagesExpired { dead_lettered })
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
                batch.push_delete(keys::ready(namespace, entity, sequence));
            }
            MessageState::Locked { locked_until, .. } => {
                batch.push_delete(keys::lock(namespace, entity, locked_until, sequence));
            }
            MessageState::DeadLettered(_) => {}
        }
        batch.push_delete(keys::message(namespace, entity, sequence));
        if let Some(expires_at) = record.expires_at {
            batch.push_delete(keys::expiry(namespace, entity, expires_at, sequence));
        }

        record.state = MessageState::DeadLettered(DeadLetterInfo {
            reason,
            description,
            dead_lettered_at: command.issued_at,
        });
        batch.push_put(
            keys::dead_letter(namespace, entity, sequence),
            codec::encode(&record)?,
        );
        Ok(())
    }
}
