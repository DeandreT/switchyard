use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::*;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Owner {
    TrustedUnowned,
    HeldGeneration(LockToken),
}

impl Owner {
    fn generation(self) -> Option<LockToken> {
        match self {
            Self::TrustedUnowned => None,
            Self::HeldGeneration(token) => Some(token),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Row {
    namespace: NamespaceName,
    entity: EntityPath,
    session_id: SessionId,
    sequence: SequenceNumber,
    owner: Owner,
    message_token: LockToken,
    locked_until: Timestamp,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Summary {
    namespace: NamespaceName,
    entity: EntityPath,
    session_id: SessionId,
    owned_generation: Option<LockToken>,
    owned_count: u64,
    unowned_count: u64,
}

impl Summary {
    fn total(&self) -> Result<u64, BrokerError> {
        if (self.owned_count > 0) != self.owned_generation.is_some()
            || self
                .owned_generation
                .is_some_and(|token| token.as_u64() == 0)
        {
            return Err(BrokerError::MalformedIndexKey);
        }
        self.owned_count
            .checked_add(self.unowned_count)
            .filter(|count| *count > 0)
            .ok_or(BrokerError::MalformedIndexKey)
    }

    fn add(&mut self, owner: Owner) -> Result<(), BrokerError> {
        match owner {
            Owner::TrustedUnowned => {
                self.unowned_count = self
                    .unowned_count
                    .checked_add(1)
                    .ok_or(BrokerError::MalformedIndexKey)?;
            }
            Owner::HeldGeneration(token) => {
                if token.as_u64() == 0
                    || self
                        .owned_generation
                        .is_some_and(|previous| previous != token)
                {
                    return Err(BrokerError::MalformedIndexKey);
                }
                self.owned_generation = Some(token);
                self.owned_count = self
                    .owned_count
                    .checked_add(1)
                    .ok_or(BrokerError::MalformedIndexKey)?;
            }
        }
        self.total()?;
        Ok(())
    }

    fn contains(&self, owner: Owner) -> bool {
        match owner {
            Owner::TrustedUnowned => self.unowned_count > 0,
            Owner::HeldGeneration(token) => {
                self.owned_generation == Some(token) && self.owned_count > 0
            }
        }
    }

    fn remove(&mut self, owner: Owner) -> Result<bool, BrokerError> {
        self.total()?;
        if !self.contains(owner) {
            return Err(BrokerError::MalformedIndexKey);
        }
        match owner {
            Owner::TrustedUnowned => self.unowned_count -= 1,
            Owner::HeldGeneration(_) => {
                self.owned_count -= 1;
                if self.owned_count == 0 {
                    self.owned_generation = None;
                }
            }
        }
        let empty = self.owned_count == 0 && self.unowned_count == 0;
        if !empty {
            self.total()?;
        }
        Ok(empty)
    }
}

/// One ordinary command's tracking changes, on its original atomic batch.
pub(super) struct SessionMessageLocks<'a, S> {
    store: &'a S,
    command: &'a Command,
    tracked: bool,
    batch: &'a mut WriteBatch,
}

impl<'a, S: StateStore> SessionMessageLocks<'a, S> {
    pub(super) fn new(
        store: &'a S,
        command: &'a Command,
        config: &QueueConfig,
        batch: &'a mut WriteBatch,
    ) -> Self {
        Self {
            store,
            command,
            tracked: config.requires_session && !command.entity.is_dead_letter_queue(),
            batch,
        }
    }

    fn read_staged(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BrokerError> {
        for mutation in self.batch.mutations().iter().rev() {
            match mutation {
                Mutation::Put {
                    key: candidate,
                    value,
                } if candidate == key => return Ok(Some(value.clone())),
                Mutation::Delete { key: candidate } if candidate == key => return Ok(None),
                _ => {}
            }
        }
        Ok(self.store.get(key)?)
    }

    fn read<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>, BrokerError> {
        self.read_staged(key)?
            .map(|raw| {
                let (version, payload) = codec::split(&raw)?;
                if version != codec::VALUE_FORMAT_V11 {
                    return Err(BrokerError::Codec(crate::CodecError::UnsupportedVersion {
                        version,
                    }));
                }
                Ok(codec::decode_payload(payload)?)
            })
            .transpose()
    }

    fn valid_session(&self, session_id: &SessionId) -> Result<(), BrokerError> {
        if NamespaceName::new(self.command.namespace.as_str()).is_err()
            || EntityPath::new(self.command.entity.as_str()).is_err()
            || SessionId::new(session_id.as_str()).is_err()
        {
            return Err(BrokerError::MalformedIndexKey);
        }
        Ok(())
    }

    fn summary(&self, session_id: &SessionId) -> Result<Option<Summary>, BrokerError> {
        self.valid_session(session_id)?;
        let key = keys::session_message_lock_summary(
            &self.command.namespace,
            &self.command.entity,
            session_id,
        );
        let summary: Option<Summary> = self.read(&key)?;
        if let Some(summary) = &summary {
            if summary.namespace != self.command.namespace
                || summary.entity != self.command.entity
                || &summary.session_id != session_id
            {
                return Err(BrokerError::MalformedIndexKey);
            }
            summary.total()?;
        }
        Ok(summary)
    }

    fn reverse_key(&self, sequence: SequenceNumber) -> Vec<u8> {
        keys::session_message_lock_reverse(&self.command.namespace, &self.command.entity, sequence)
    }

    fn forward_key(&self, row: &Row) -> Vec<u8> {
        keys::session_message_lock_forward(
            &self.command.namespace,
            &self.command.entity,
            &row.session_id,
            row.owner.generation(),
            row.sequence,
        )
    }

    pub(super) fn ensure_unlocked(&self, record: &MessageRecord) -> Result<(), BrokerError> {
        if self.tracked
            && self
                .read_staged(&self.reverse_key(record.sequence))?
                .is_some()
        {
            return Err(BrokerError::MalformedIndexKey);
        }
        Ok(())
    }

    pub(super) fn install_locked(
        &mut self,
        original: &MessageRecord,
        token: LockToken,
        locked_until: Timestamp,
        hold: Option<&SessionHold>,
    ) -> Result<(), BrokerError> {
        if !self.tracked {
            return Ok(());
        }
        if !matches!(original.state, MessageState::Ready | MessageState::Deferred)
            || token.as_u64() == 0
        {
            return Err(BrokerError::MalformedIndexKey);
        }
        self.ensure_unlocked(original)?;
        let session_id = original
            .session_id
            .as_ref()
            .ok_or(BrokerError::MalformedIndexKey)?;
        self.valid_session(session_id)?;
        let owner = match hold {
            Some(hold) if &hold.session_id == session_id && hold.token.as_u64() > 0 => {
                Owner::HeldGeneration(hold.token)
            }
            Some(_) => return Err(BrokerError::MalformedIndexKey),
            None => Owner::TrustedUnowned,
        };
        let row = Row {
            namespace: self.command.namespace.clone(),
            entity: self.command.entity.clone(),
            session_id: session_id.clone(),
            sequence: original.sequence,
            owner,
            message_token: token,
            locked_until,
        };
        let forward = self.forward_key(&row);
        if self.read_staged(&forward)?.is_some() {
            return Err(BrokerError::MalformedIndexKey);
        }
        let mut summary = self.summary(session_id)?.unwrap_or_else(|| Summary {
            namespace: self.command.namespace.clone(),
            entity: self.command.entity.clone(),
            session_id: session_id.clone(),
            owned_generation: None,
            owned_count: 0,
            unowned_count: 0,
        });
        summary.add(owner)?;
        let bytes = codec::encode(&row)?;
        let summary_bytes = codec::encode(&summary)?;
        self.batch
            .push_put(self.reverse_key(row.sequence), bytes.clone());
        self.batch.push_put(forward, bytes);
        self.batch.push_put(
            keys::session_message_lock_summary(&row.namespace, &row.entity, session_id),
            summary_bytes,
        );
        Ok(())
    }

    fn validated(
        &self,
        record: &MessageRecord,
        hold: Option<&SessionHold>,
    ) -> Result<Option<(Row, Summary)>, BrokerError> {
        if !self.tracked {
            return Ok(None);
        }
        let MessageState::Locked {
            token,
            locked_until,
        } = record.state
        else {
            return Err(BrokerError::MalformedIndexKey);
        };
        let session_id = record
            .session_id
            .as_ref()
            .ok_or(BrokerError::MalformedIndexKey)?;
        self.valid_session(session_id)?;
        let row: Row = self
            .read(&self.reverse_key(record.sequence))?
            .ok_or(BrokerError::MalformedIndexKey)?;
        if row.namespace != self.command.namespace
            || row.entity != self.command.entity
            || &row.session_id != session_id
            || row.sequence != record.sequence
            || row.message_token != token
            || token.as_u64() == 0
            || row.locked_until != locked_until
            || row
                .owner
                .generation()
                .is_some_and(|generation| generation.as_u64() == 0)
        {
            return Err(BrokerError::MalformedIndexKey);
        }
        let forward: Row = self
            .read(&self.forward_key(&row))?
            .ok_or(BrokerError::MalformedIndexKey)?;
        if forward != row {
            return Err(BrokerError::MalformedIndexKey);
        }
        let summary = self
            .summary(session_id)?
            .ok_or(BrokerError::MalformedIndexKey)?;
        if !summary.contains(row.owner) {
            return Err(BrokerError::MalformedIndexKey);
        }
        if let Some(hold) = hold
            && (hold.token.as_u64() == 0
                || &hold.session_id != session_id
                || row
                    .owner
                    .generation()
                    .is_some_and(|generation| generation != hold.token))
        {
            return Err(BrokerError::SessionLockNotHeld {
                session_id: hold.session_id.clone(),
            });
        }
        Ok(Some((row, summary)))
    }

    pub(super) fn validate_locked(
        &self,
        record: &MessageRecord,
        hold: Option<&SessionHold>,
    ) -> Result<(), BrokerError> {
        self.validated(record, hold)?;
        Ok(())
    }

    pub(super) fn renew_locked(
        &mut self,
        record: &MessageRecord,
        locked_until: Timestamp,
        hold: Option<&SessionHold>,
    ) -> Result<(), BrokerError> {
        if let Some((mut row, _)) = self.validated(record, hold)? {
            row.locked_until = locked_until;
            let bytes = codec::encode(&row)?;
            self.batch
                .push_put(self.reverse_key(row.sequence), bytes.clone());
            self.batch.push_put(self.forward_key(&row), bytes);
        }
        Ok(())
    }

    pub(super) fn leave_locked(&mut self, record: &MessageRecord) -> Result<(), BrokerError> {
        if let Some((row, mut summary)) = self.validated(record, None)? {
            let empty = summary.remove(row.owner)?;
            let summary_key =
                keys::session_message_lock_summary(&row.namespace, &row.entity, &row.session_id);
            let summary_bytes = if empty {
                None
            } else {
                Some(codec::encode(&summary)?)
            };
            self.batch.push_delete(self.reverse_key(row.sequence));
            self.batch.push_delete(self.forward_key(&row));
            match summary_bytes {
                Some(bytes) => self.batch.push_put(summary_key, bytes),
                None => self.batch.push_delete(summary_key),
            }
        }
        Ok(())
    }

    /// Grant entry only: no tracking mutations have been staged at this boundary.
    pub(super) fn takeover_pending(&self, session_id: &SessionId) -> Result<bool, BrokerError> {
        if self.summary(session_id)?.is_some() {
            return Ok(true);
        }
        let prefix = keys::session_message_lock_forward_prefix(
            &self.command.namespace,
            &self.command.entity,
            session_id,
        );
        if !self.store.scan_prefix(&prefix, 1)?.is_empty() {
            return Err(BrokerError::MalformedIndexKey);
        }
        Ok(false)
    }
}
