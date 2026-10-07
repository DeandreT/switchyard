use std::sync::{Arc, Mutex};

use storage::{Key, StorageError, StoreSnapshot, Value, WriteBatch};

use super::*;

const MAX_TOP: usize = 100;
const MAX_SKIP: usize = 1_000;
const RAW_PAGE_SIZE: usize = 128;
const MAX_ROWS: usize = 4_096;
const MAX_SCANS: usize = 64;
const MAX_GETS: usize = 16_384;
const MAX_KEY_BYTES: usize = 4 * 1_024 * 1_024;
const MAX_VALUE_BYTES: usize = 16 * 1_024 * 1_024;

#[derive(Clone, Default)]
struct ReadBudget {
    rows: usize,
    scans: usize,
    gets: usize,
    key_bytes: usize,
    value_bytes: usize,
}

fn add(value: usize, amount: usize, maximum: usize) -> Result<usize, StorageError> {
    value
        .checked_add(amount)
        .filter(|value| *value <= maximum)
        .ok_or(StorageError::ReadLimitExceeded)
}

// Clones share logical read credit across discovery and profile proof. Returned
// data is observed after backend materialization, not before copying or allocation.
#[derive(Clone)]
struct PageStore<S> {
    inner: S,
    budget: Arc<Mutex<ReadBudget>>,
}

impl<S: StateStore> PageStore<S> {
    fn reserve_read(&self, key_bytes: usize, scan: bool) -> Result<(), StorageError> {
        let mut budget = self.budget.lock().map_err(|_| StorageError::LockPoisoned)?;
        let next_key_bytes = add(budget.key_bytes, key_bytes, MAX_KEY_BYTES)?;
        let next = if scan {
            add(budget.scans, 1, MAX_SCANS)?
        } else {
            add(budget.gets, 1, MAX_GETS)?
        };
        budget.key_bytes = next_key_bytes;
        if scan {
            budget.scans = next;
        } else {
            budget.gets = next;
        }
        Ok(())
    }

    fn raw_page_limit(&self) -> Result<usize, StorageError> {
        let budget = self.budget.lock().map_err(|_| StorageError::LockPoisoned)?;
        // The domain asks for one extra row. Never turn a clamped backend scan
        // into an apparent exhaustion proof, including when headroom is zero.
        let headroom = MAX_ROWS - budget.rows;
        let limit = RAW_PAGE_SIZE.min(headroom.saturating_sub(1));
        if limit == 0 || budget.scans == MAX_SCANS {
            return Err(StorageError::ReadLimitExceeded);
        }
        Ok(limit)
    }
}

impl<S: StateStore> StateStore for PageStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.reserve_read(key.len(), false)?;
        let value = self.inner.get(key)?;
        if let Some(value) = &value {
            let mut budget = self.budget.lock().map_err(|_| StorageError::LockPoisoned)?;
            budget.value_bytes = add(budget.value_bytes, value.len(), MAX_VALUE_BYTES)?;
        }
        Ok(value)
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let input_bytes = prefix
            .len()
            .checked_add(start.len())
            .ok_or(StorageError::ReadLimitExceeded)?;
        {
            let budget = self.budget.lock().map_err(|_| StorageError::LockPoisoned)?;
            if limit > MAX_ROWS - budget.rows {
                return Err(StorageError::ReadLimitExceeded);
            }
        }
        self.reserve_read(input_bytes, true)?;
        let rows = self.inner.scan_from(prefix, start, limit)?;
        let mut budget = self.budget.lock().map_err(|_| StorageError::LockPoisoned)?;
        let mut next = budget.clone();
        next.rows = add(next.rows, rows.len(), MAX_ROWS)?;
        for (key, value) in &rows {
            next.key_bytes = add(next.key_bytes, key.len(), MAX_KEY_BYTES)?;
            next.value_bytes = add(next.value_bytes, value.len(), MAX_VALUE_BYTES)?;
        }
        if rows.len() > limit {
            return Err(StorageError::ReadLimitExceeded);
        }
        *budget = next;
        Ok(rows)
    }

    fn apply(&self, _batch: WriteBatch) -> Result<(), StorageError> {
        Err(StorageError::ReplicaWriteRequired)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        Err(StorageError::ReadLimitExceeded)
    }
}

struct ReadOnlyClock;

impl Clock for ReadOnlyClock {
    fn now(&self) -> Timestamp {
        panic!("a finite queue page cannot stamp a command")
    }
}

pub(super) fn read<S: StateStore, C: Clock>(
    owner: &LocalProposer<S, C>,
    namespace: &NamespaceName,
    skip: usize,
    top: usize,
) -> Result<Vec<QueueCapacityView>, AtomQueueOwnerError> {
    if top == 0 || top > MAX_TOP || skip > MAX_SKIP {
        return Err(AtomQueueOwnerError::InvalidPageBounds);
    }
    NamespaceName::new(namespace.as_str()).map_err(BrokerError::from)?;
    let store = PageStore {
        inner: owner.machine.store().clone(),
        budget: Arc::default(),
    };
    let reader = LocalProposer::new(StateMachine::new(store.clone()), ReadOnlyClock);
    let result = (|| {
        let mut after = None;
        let mut remaining_skip = skip;
        let mut views = Vec::with_capacity(top);
        loop {
            let limit = store.raw_page_limit().map_err(BrokerError::from)?;
            let page = reader.queues_page(Some(namespace), after.as_ref(), limit)?;
            for (row_namespace, entity) in page.queues {
                if entity.is_dead_letter_queue() || entity.is_subscription_path() {
                    continue;
                }
                // Validate consumed candidates even when the caller skips them.
                // The separately budgeted backend lookahead is not consumed here.
                let view = reader
                    .get_atom_finite_queue(&row_namespace, &entity)?
                    .ok_or(BrokerError::DanglingEntityMetadata)?;
                if remaining_skip != 0 {
                    remaining_skip -= 1;
                } else {
                    views.push(view);
                    if views.len() == top {
                        return Ok(views);
                    }
                }
            }
            match page.continuation {
                None => return Ok(views),
                Some(next) if after.as_ref() != Some(&next) => after = Some(next),
                Some(_) => return Err(AtomQueueOwnerError::WorkLimitExceeded),
            }
        }
    })();
    result.map_err(|error| match error {
        AtomQueueOwnerError::Submit(crate::SubmitError::Propose(ProposeError::Broker(
            BrokerError::Storage(StorageError::ReadLimitExceeded),
        ))) => AtomQueueOwnerError::WorkLimitExceeded,
        error => error,
    })
}

#[cfg(test)]
mod tests;
