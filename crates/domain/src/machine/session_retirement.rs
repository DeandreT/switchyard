use std::sync::{Arc, Mutex, MutexGuard};

use serde::{Deserialize, Serialize};
use storage::{Key, StorageError, StoreSnapshot, Value};

use super::*;

#[cfg(test)]
mod tests;

pub const MAX_SESSION_RETIREMENT_GROUPS: usize = 32;
pub const MAX_SESSION_RETIREMENT_ROWS: usize = 32;
pub const MAX_SESSION_RETIREMENT_READ_OPERATIONS: usize = 1_024;
pub const MAX_SESSION_RETIREMENT_READ_KEY_BYTES: usize = 256 * 1024;
pub const MAX_SESSION_RETIREMENT_READ_VALUE_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_SESSION_RETIREMENT_MUTATION_ENTRIES: usize = 512;
pub const MAX_SESSION_RETIREMENT_MUTATION_KEY_BYTES: usize = 256 * 1024;
pub const MAX_SESSION_RETIREMENT_MUTATION_VALUE_BYTES: usize = 32 * 1024 * 1024;

/// Scoped progress only; original ownership is revalidated on every page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionRetirementCursor {
    pub namespace: NamespaceName,
    pub entity: EntityPath,
    pub session_id: SessionId,
    pub position: SessionRetirementPosition,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SessionRetirementPosition {
    AfterSession,
    OwnedRows {
        generation: LockToken,
        after_sequence: SequenceNumber,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionRetirementPage {
    Continue(SessionRetirementCursor),
    End,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionRetirementOutcome {
    pub returned_to_ready: u32,
    pub dead_lettered: u32,
    pub dropped: u32,
    pub page: SessionRetirementPage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionRetirementLimit {
    ReadOperations,
    ReadKeyBytes,
    ReadValueBytes,
    MutationEntries,
    MutationKeyBytes,
    MutationValueBytes,
}

impl std::fmt::Display for SessionRetirementLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ReadOperations => "read operations",
            Self::ReadKeyBytes => "read key bytes",
            Self::ReadValueBytes => "read value bytes",
            Self::MutationEntries => "mutation entries",
            Self::MutationKeyBytes => "mutation key bytes",
            Self::MutationValueBytes => "mutation value bytes",
        })
    }
}

impl SessionRetirementLimit {
    fn exceeded(self, maximum: usize) -> BrokerError {
        BrokerError::SessionRetirementTooLarge {
            limit: self,
            maximum,
        }
    }
}

struct ReadBudget {
    used: [usize; 3],
    maxima: [usize; 3],
    failure: Option<(SessionRetirementLimit, usize)>,
}

impl ReadBudget {
    fn charge(&mut self, index: usize, amount: usize) -> Result<(), StorageError> {
        if self.failure.is_some() {
            return Err(budget_error());
        }
        match self.used[index].checked_add(amount) {
            Some(total) if total <= self.maxima[index] => self.used[index] = total,
            _ => {
                let limit = [
                    SessionRetirementLimit::ReadOperations,
                    SessionRetirementLimit::ReadKeyBytes,
                    SessionRetirementLimit::ReadValueBytes,
                ][index];
                self.failure = Some((limit, self.maxima[index]));
                return Err(budget_error());
            }
        }
        Ok(())
    }
}

fn budget_error() -> StorageError {
    StorageError::Backend {
        operation: "prepare session retirement",
        detail: String::from("the retirement read budget refused an operation"),
    }
}

fn forbidden_error() -> StorageError {
    StorageError::Backend {
        operation: "prepare session retirement",
        detail: String::from(
            "the retirement reader cannot write, snapshot, or scan a multi-row page",
        ),
    }
}

#[derive(Clone)]
struct RetirementReadStore<S> {
    base: S,
    budget: Arc<Mutex<ReadBudget>>,
}

impl<S: StateStore> RetirementReadStore<S> {
    fn new(base: S) -> Self {
        Self::with_maxima(
            base,
            [
                MAX_SESSION_RETIREMENT_READ_OPERATIONS,
                MAX_SESSION_RETIREMENT_READ_KEY_BYTES,
                MAX_SESSION_RETIREMENT_READ_VALUE_BYTES,
            ],
        )
    }

    fn with_maxima(base: S, maxima: [usize; 3]) -> Self {
        Self {
            base,
            budget: Arc::new(Mutex::new(ReadBudget {
                used: [0; 3],
                maxima,
                failure: None,
            })),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, ReadBudget>, StorageError> {
        self.budget.lock().map_err(|_| StorageError::LockPoisoned)
    }

    fn admit(&self, keys: &[&[u8]]) -> Result<(), StorageError> {
        let mut budget = self.lock()?;
        budget.charge(0, 1)?;
        for key in keys {
            budget.charge(1, key.len())?;
        }
        Ok(())
    }

    fn map_error(&self, original: BrokerError) -> BrokerError {
        if original != BrokerError::Storage(budget_error()) {
            return original;
        }
        match self.lock() {
            Ok(budget) => budget
                .failure
                .map_or(original, |(limit, maximum)| limit.exceeded(maximum)),
            Err(error) => error.into(),
        }
    }
}

impl<S: StateStore> StateStore for RetirementReadStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.admit(&[key])?;
        let result = self.base.get(key)?;
        self.lock()?
            .charge(2, result.as_ref().map_or(0, Vec::len))?;
        Ok(result)
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        if limit != 1 {
            return Err(forbidden_error());
        }
        self.admit(&[prefix, start])?;
        let rows = self.base.scan_from(prefix, start, limit)?;
        let mut budget = self.lock()?;
        for (key, value) in &rows {
            budget.charge(1, key.len())?;
            budget.charge(2, value.len())?;
        }
        Ok(rows)
    }

    fn apply(&self, _batch: WriteBatch) -> Result<(), StorageError> {
        Err(forbidden_error())
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        Err(forbidden_error())
    }
}

struct MutationBudget {
    used: [usize; 3],
    maxima: [usize; 3],
    charged: usize,
}

impl Default for MutationBudget {
    fn default() -> Self {
        Self {
            used: [0; 3],
            charged: 0,
            maxima: [
                MAX_SESSION_RETIREMENT_MUTATION_ENTRIES,
                MAX_SESSION_RETIREMENT_MUTATION_KEY_BYTES,
                MAX_SESSION_RETIREMENT_MUTATION_VALUE_BYTES,
            ],
        }
    }
}

impl MutationBudget {
    fn charge(
        &mut self,
        entries: usize,
        key_bytes: usize,
        value_bytes: usize,
    ) -> Result<(), BrokerError> {
        for (index, amount) in [entries, key_bytes, value_bytes].into_iter().enumerate() {
            self.used[index] = self.used[index]
                .checked_add(amount)
                .filter(|total| *total <= self.maxima[index])
                .ok_or_else(|| {
                    [
                        SessionRetirementLimit::MutationEntries,
                        SessionRetirementLimit::MutationKeyBytes,
                        SessionRetirementLimit::MutationValueBytes,
                    ][index]
                        .exceeded(self.maxima[index])
                })?;
        }
        Ok(())
    }

    fn charge_new(&mut self, batch: &WriteBatch) -> Result<(), BrokerError> {
        for mutation in &batch.mutations()[self.charged..] {
            let (key, value_bytes) = match mutation {
                Mutation::Put { key, value } => (key, value.len()),
                Mutation::Delete { key } => (key, 0),
            };
            self.charge(1, key.len(), value_bytes)?;
        }
        self.charged = batch.mutations().len();
        Ok(())
    }

    fn reserve_clock(&mut self, command: &Command, batch: &WriteBatch) -> Result<(), BrokerError> {
        if !batch.is_empty() {
            self.charge(
                1,
                keys::clock().len(),
                codec::encode(&command.issued_at)?.len(),
            )?;
        }
        Ok(())
    }

    fn check_clock(&self, command: &Command, batch: &WriteBatch) -> Result<(), BrokerError> {
        let mut projected = Self {
            used: self.used,
            maxima: self.maxima,
            charged: self.charged,
        };
        projected.reserve_clock(command, batch)
    }
}

fn validate_cursor(command: &Command, cursor: &SessionRetirementCursor) -> Result<(), BrokerError> {
    if NamespaceName::new(cursor.namespace.as_str()).is_err()
        || EntityPath::new(cursor.entity.as_str()).is_err()
        || SessionId::new(cursor.session_id.as_str()).is_err()
        || matches!(cursor.position, SessionRetirementPosition::OwnedRows { generation, after_sequence }
            if generation.as_u64() == 0 || after_sequence.as_u64() == 0)
    {
        return Err(BrokerError::InvalidSessionCursor);
    }
    if cursor.namespace != command.namespace || cursor.entity != command.entity {
        return Err(BrokerError::SessionCursorScopeMismatch {
            namespace: command.namespace.clone(),
            entity: command.entity.clone(),
            cursor_namespace: cursor.namespace.clone(),
            cursor_entity: cursor.entity.clone(),
        });
    }
    Ok(())
}

fn continuation(
    command: &Command,
    session_id: SessionId,
    position: SessionRetirementPosition,
) -> SessionRetirementPage {
    SessionRetirementPage::Continue(SessionRetirementCursor {
        namespace: command.namespace.clone(),
        entity: command.entity.clone(),
        session_id,
        position,
    })
}

impl<S: StateStore> StateMachine<S> {
    pub(super) fn retire_session_generation_page(
        &self,
        command: &Command,
        after: Option<&SessionRetirementCursor>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let reader = RetirementReadStore::new(self.store.clone());
        let machine = StateMachine::new(reader.clone());
        let mut mutations = MutationBudget::default();
        let outcome = machine
            .retire_session_generations(command, after, batch, &mut mutations)
            .map_err(|error| reader.map_error(error))?;
        mutations.reserve_clock(command, batch)?;
        Ok(CommandOutcome::SessionRetired(outcome))
    }

    fn retire_session_generations(
        &self,
        command: &Command,
        after: Option<&SessionRetirementCursor>,
        batch: &mut WriteBatch,
        mutations: &mut MutationBudget,
    ) -> Result<SessionRetirementOutcome, BrokerError> {
        let config = self.load_config(command)?;
        let mut outcome = SessionRetirementOutcome {
            returned_to_ready: 0,
            dead_lettered: 0,
            dropped: 0,
            page: SessionRetirementPage::End,
        };
        if !config.requires_session || command.entity.is_dead_letter_queue() {
            return Ok(outcome);
        }
        if let Some(cursor) = after {
            validate_cursor(command, cursor)?;
        }
        let namespace = &command.namespace;
        let entity = &command.entity;
        let prefix = keys::session_message_lock_summary_prefix(namespace, entity);
        let mut start = after.map_or_else(
            || prefix.clone(),
            |cursor| {
                keys::session_message_lock_exclusive_start(&keys::session_message_lock_summary(
                    namespace,
                    entity,
                    &cursor.session_id,
                ))
            },
        );
        let mut resumed = after.filter(|cursor| {
            matches!(cursor.position, SessionRetirementPosition::OwnedRows { .. })
        });
        let mut retired = 0;
        for group_index in 0..MAX_SESSION_RETIREMENT_GROUPS {
            let resume = resumed.take();
            let selected = if resume.is_some() {
                None
            } else {
                self.store.scan_from(&prefix, &start, 1)?.into_iter().next()
            };
            let session_id = match (resume, selected.as_ref()) {
                (Some(cursor), _) => cursor.session_id.clone(),
                (_, Some((key, _))) => SessionId::new(
                    keys::session_message_lock_summary_parts(&prefix, key)
                        .ok_or(BrokerError::MalformedIndexKey)?,
                )?,
                _ => return Ok(outcome),
            };
            let group = SessionMessageLocks::new(&self.store, command, &config, batch)
                .retirement_group(
                    &session_id,
                    selected
                        .as_ref()
                        .map(|(key, value)| (key.as_slice(), value.as_slice())),
                )?;
            let original_position = resume.map(|cursor| &cursor.position);
            if let Some(group) = group.filter(|group| group.eligible)
                && let Some(generation) = group.generation
                && !matches!(original_position, Some(SessionRetirementPosition::OwnedRows {
                    generation: previous, ..
                }) if *previous != generation)
            {
                let forward = keys::session_message_lock_generation_prefix(
                    namespace,
                    entity,
                    &session_id,
                    generation,
                );
                let mut row_start = match original_position {
                    Some(SessionRetirementPosition::OwnedRows { after_sequence, .. }) => {
                        keys::session_message_lock_exclusive_start(
                            &keys::session_message_lock_forward(
                                namespace,
                                entity,
                                &session_id,
                                Some(generation),
                                *after_sequence,
                            ),
                        )
                    }
                    _ => forward.clone(),
                };
                let mut remaining = group.owned_count;
                while remaining > 0 {
                    let (key, value) = self
                        .store
                        .scan_from(&forward, &row_start, 1)?
                        .into_iter()
                        .next()
                        .ok_or(BrokerError::MalformedIndexKey)?;
                    let (entry, mut record) =
                        SessionMessageLocks::new(&self.store, command, &config, batch)
                            .retirement_entry(&session_id, generation, (&key, &value))?;
                    if entry.owned_count != remaining
                        || !matches!(record.state,
                        MessageState::Locked { token, locked_until }
                            if token == entry.message_token && locked_until == entry.locked_until)
                    {
                        return Err(BrokerError::MalformedIndexKey);
                    }
                    if record.is_expired_at(command.issued_at) {
                        match self.expire_message(command, &config, record, batch)? {
                            ExpirationOutcome::Dropped => outcome.dropped += 1,
                            ExpirationOutcome::DeadLettered => outcome.dead_lettered += 1,
                        }
                    } else if exceeded_delivery_limit(command, &config, &record) {
                        self.move_to_dead_letter(
                            command,
                            &config,
                            record,
                            DeadLetterReason::MaxDeliveryCountExceeded,
                            String::from("the message reached its maximum delivery count"),
                            batch,
                        )?;
                        outcome.dead_lettered += 1;
                    } else {
                        SessionMessageLocks::new(&self.store, command, &config, batch)
                            .leave_locked(&record)?;
                        record.state = MessageState::Ready;
                        batch.push_delete(keys::lock(
                            namespace,
                            entity,
                            entry.locked_until,
                            entry.sequence,
                        ));
                        batch.push_put(
                            keys::message(namespace, entity, entry.sequence),
                            codec::encode(&record)?,
                        );
                        batch
                            .push_put(self.ready_key(command.into(), &config, &record), Vec::new());
                        index_ready_expiry(command.into(), &record, batch);
                        outcome.returned_to_ready += 1;
                    }
                    mutations.charge_new(batch)?;
                    mutations.check_clock(command, batch)?;
                    remaining -= 1;
                    retired += 1;
                    if retired == MAX_SESSION_RETIREMENT_ROWS {
                        let position = if remaining == 0 {
                            SessionRetirementPosition::AfterSession
                        } else {
                            SessionRetirementPosition::OwnedRows {
                                generation,
                                after_sequence: entry.sequence,
                            }
                        };
                        outcome.page = continuation(command, session_id, position);
                        return Ok(outcome);
                    }
                    row_start = keys::session_message_lock_exclusive_start(&key);
                }
            }
            start = keys::session_message_lock_exclusive_start(
                &keys::session_message_lock_summary(namespace, entity, &session_id),
            );
            if group_index + 1 == MAX_SESSION_RETIREMENT_GROUPS {
                outcome.page =
                    continuation(command, session_id, SessionRetirementPosition::AfterSession);
            }
        }
        Ok(outcome)
    }
}
