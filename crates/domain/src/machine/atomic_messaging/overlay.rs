use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};

use crate::{AtomicMessagingLimit as Limit, BrokerError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failure {
    Limit(Limit),
    ForbiddenOperation,
}

#[derive(Default)]
struct OverlayState {
    mutations: BTreeMap<Key, Option<Value>>,
    reads: usize,
    read_key_bytes: usize,
    read_value_bytes: usize,
    mutation_key_bytes: usize,
    put_bytes: usize,
    failure: Option<Failure>,
}

impl OverlayState {
    fn fail(&mut self, failure: Failure) -> StorageError {
        self.failure.get_or_insert(failure);
        adapter_error()
    }

    fn charge(&mut self, used: usize, amount: usize, limit: Limit) -> Result<usize, StorageError> {
        if self.failure.is_some() {
            return Err(adapter_error());
        }
        used.checked_add(amount)
            .filter(|total| *total <= limit.maximum())
            .ok_or_else(|| self.fail(Failure::Limit(limit)))
    }
}

fn adapter_error() -> StorageError {
    StorageError::Backend {
        operation: "prepare atomic messaging",
        detail: String::from("bounded atomic overlay refused an operation"),
    }
}

/// The allowed handlers use only point reads. No backing writes or scans can
/// escape this adapter while the transaction is being prepared.
#[derive(Clone)]
pub(super) struct AtomicOverlay<S> {
    base: S,
    state: Arc<Mutex<OverlayState>>,
}

impl<S: StateStore> AtomicOverlay<S> {
    pub(super) fn new(base: S) -> Self {
        Self {
            base,
            state: Arc::new(Mutex::new(OverlayState::default())),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, OverlayState>, StorageError> {
        self.state.lock().map_err(|_| StorageError::LockPoisoned)
    }

    pub(super) fn map_error(&self, original: BrokerError) -> BrokerError {
        match self.lock() {
            Ok(state) => match state.failure {
                Some(Failure::Limit(limit)) => limit.exceeded(),
                Some(Failure::ForbiddenOperation) => BrokerError::InvalidAtomicMessagingCommand,
                None => original,
            },
            Err(error) => error.into(),
        }
    }

    pub(super) fn stage(&self, batch: WriteBatch) -> Result<(), BrokerError> {
        self.stage_inner(batch)
            .map_err(|error| self.map_error(error.into()))
    }

    fn stage_inner(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let mut state = self.lock()?;
        if state.failure.is_some() {
            return Err(adapter_error());
        }
        let mut new_keys = BTreeSet::new();
        let mut key_bytes = state.mutation_key_bytes;
        let mut put_bytes = state.put_bytes;
        for mutation in batch.mutations() {
            let key = match mutation {
                Mutation::Put { key, value } => {
                    put_bytes = state.charge(put_bytes, value.len(), Limit::MutationValueBytes)?;
                    key
                }
                Mutation::Delete { key } => key,
            };
            if !state.mutations.contains_key(key) && new_keys.insert(key.as_slice()) {
                let existing_keys = state.mutations.len();
                state.charge(existing_keys, new_keys.len(), Limit::MutationKeys)?;
                key_bytes = state.charge(key_bytes, key.len(), Limit::MutationKeyBytes)?;
            }
        }
        state.mutation_key_bytes = key_bytes;
        state.put_bytes = put_bytes;
        drop(new_keys);
        for mutation in batch.into_mutations() {
            match mutation {
                Mutation::Put { key, value } => {
                    state.mutations.insert(key, Some(value));
                }
                Mutation::Delete { key } => {
                    state.mutations.insert(key, None);
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(&self) -> Result<WriteBatch, BrokerError> {
        let mut state = self.lock()?;
        if let Some(failure) = state.failure {
            return Err(match failure {
                Failure::Limit(limit) => limit.exceeded(),
                Failure::ForbiddenOperation => BrokerError::InvalidAtomicMessagingCommand,
            });
        }
        let mut batch = WriteBatch::default();
        for (key, value) in std::mem::take(&mut state.mutations) {
            match value {
                Some(value) => batch.push_put(key, value),
                None => batch.push_delete(key),
            }
        }
        Ok(batch)
    }

    fn forbidden<T>(&self) -> Result<T, StorageError> {
        let mut state = self.lock()?;
        Err(state.fail(Failure::ForbiddenOperation))
    }
}

impl<S: StateStore> StateStore for AtomicOverlay<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        {
            let mut state = self.lock()?;
            let reads = state.reads;
            state.reads = state.charge(reads, 1, Limit::ReadOperations)?;
            let key_bytes = state.read_key_bytes;
            state.read_key_bytes = state.charge(key_bytes, key.len(), Limit::ReadKeyBytes)?;
            if let Some(value) = state.mutations.get(key) {
                let bytes = value.as_ref().map_or(0, Vec::len);
                let value_bytes = state.read_value_bytes;
                state.read_value_bytes = state.charge(value_bytes, bytes, Limit::ReadValueBytes)?;
                return Ok(state.mutations.get(key).and_then(Clone::clone));
            }
        }
        // A backend materializes one row before its length can be checked.
        // The byte ceiling bounds decode/copy work, not backend allocation RSS.
        let value = self.base.get(key)?;
        let mut state = self.lock()?;
        let value_bytes = state.read_value_bytes;
        state.read_value_bytes = state.charge(
            value_bytes,
            value.as_ref().map_or(0, Vec::len),
            Limit::ReadValueBytes,
        )?;
        Ok(value)
    }

    fn apply(&self, _batch: WriteBatch) -> Result<(), StorageError> {
        self.forbidden()
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.forbidden()
    }

    fn scan_from(
        &self,
        _prefix: &[u8],
        _start: &[u8],
        _limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.forbidden()
    }
}

#[cfg(test)]
mod tests;
