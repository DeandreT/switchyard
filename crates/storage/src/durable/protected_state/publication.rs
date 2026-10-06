use std::sync::RwLockWriteGuard;

use fjall::Readable;

use crate::protected_state::{check_rows, reserve};
use crate::{ProtectedStateError as Error, ProtectedStatePublication};

use super::{ARTIFACT_KEY, FENCE_KEY, INITIALIZED_KEY, Inner, METADATA_KEY, ReadGuard, view};

const MAX_MUTATIONS: usize = 131_076;
const MAX_MUTATION_BYTES: usize = 201_335_108;
const DYNAMIC_KEY_BYTES: usize =
    INITIALIZED_KEY.len() + METADATA_KEY.len() + ARTIFACT_KEY.len() + FENCE_KEY.len();

pub(super) fn mutation_budget(
    old_rows: usize,
    old_key_bytes: usize,
    new_rows: usize,
    offered_bytes: usize,
) -> Result<(usize, usize), Error> {
    let count = old_rows
        .checked_add(new_rows)
        .and_then(|n| n.checked_add(4))
        .filter(|n| *n <= MAX_MUTATIONS)
        .ok_or(Error::LimitExceeded)?;
    let bytes = old_key_bytes
        .checked_add(offered_bytes)
        .and_then(|n| n.checked_add(1))
        .and_then(|n| n.checked_add(DYNAMIC_KEY_BYTES))
        .filter(|n| *n <= MAX_MUTATION_BYTES)
        .ok_or(Error::LimitExceeded)?;
    Ok((count, bytes))
}

fn admit(current: &view::View, fence: &[u8]) -> Result<(), Error> {
    if current
        .fence
        .as_ref()
        .is_some_and(|old| old.as_ref() == fence)
    {
        Err(Error::UnchangedFence)
    } else {
        Ok(())
    }
}

fn prepare(
    inner: &Inner,
    input: &ProtectedStatePublication<'_>,
) -> Result<fjall::OwnedWriteBatch, Error> {
    let guard = ReadGuard::acquire(inner)?;
    let result = (|| {
        let snapshot = inner.database.snapshot();
        let current = view::measure(inner, &snapshot)?;
        admit(&current, input.fence)?;
        let (count, _) = mutation_budget(
            current.shape.rows,
            current.key_bytes,
            input.shape.rows,
            input.shape.total,
        )?;
        #[cfg(test)]
        {
            if inner.controls.fault() == super::Fault::Prepare {
                return Err(Error::Allocation);
            }
            inner
                .controls
                .preparations
                .fetch_add(1, super::Ordering::SeqCst);
        }
        let mut old_keys = Vec::new();
        reserve(&mut old_keys, current.shape.rows)?;
        let mut observed_bytes = 0usize;
        let mut previous: Option<fjall::UserKey> = None;
        for item in snapshot.iter(&inner.records) {
            let key = item.key().map_err(|_| Error::Poisoned)?;
            if old_keys.len() >= current.shape.rows
                || key.is_empty()
                || previous
                    .as_ref()
                    .is_some_and(|old| old.as_ref() >= key.as_ref())
            {
                return Err(Error::InvalidState);
            }
            // Reconcile sizes even though only delete keys are copied here.
            view::size(&snapshot, &inner.records, &key)?;
            observed_bytes = observed_bytes
                .checked_add(key.len())
                .ok_or(Error::LimitExceeded)?;
            old_keys.push(view::copy(&key)?);
            previous = Some(key);
        }
        if old_keys.len() != current.shape.rows || observed_bytes != current.key_bytes {
            return Err(Error::InvalidState);
        }
        let mut batch = inner
            .database
            .batch()
            .durability(Some(fjall::PersistMode::SyncAll));
        for key in old_keys {
            batch.remove(&inner.records, key);
        }
        for &(key, value) in input.rows {
            batch.insert(&inner.records, key, value);
        }
        for (key, value) in [
            (INITIALIZED_KEY, &[1][..]),
            (METADATA_KEY, input.live.metadata()),
            (ARTIFACT_KEY, input.live.artifact()),
            (FENCE_KEY, input.fence),
        ] {
            batch.insert(&inner.meta, key, value);
        }
        if batch.len() != count {
            return Err(Error::InvalidState);
        }
        Ok(batch)
    })();
    guard.finish(result)
}

struct Entry<'a> {
    inner: &'a Inner,
    gate: Option<RwLockWriteGuard<'a, ()>>,
    batch: Option<fjall::OwnedWriteBatch>,
    completed: bool,
}

impl Drop for Entry<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.inner.poison();
        }
        drop(self.gate.take());
        drop(self.batch.take());
    }
}

pub(super) fn publish(inner: &Inner, input: ProtectedStatePublication<'_>) -> Result<(), Error> {
    inner.healthy()?;
    // Recheck borrowed shape without output/preparation or native entry.
    let shape = check_rows(
        input.rows.iter().copied(),
        input.rows.len(),
        input.live.metadata().len(),
        input.live.artifact().len(),
        input.fence.len(),
        Error::InvalidInput,
    )?;
    if shape != input.shape {
        return Err(Error::InvalidInput);
    }
    let batch = prepare(inner, &input)?;
    let gate = inner.gate.write().map_err(|_| {
        inner.poison();
        Error::Poisoned
    })?;
    inner.healthy()?;
    // This guard first protects the current read, not commit entry. It marks
    // returned native failures and destructor unwinds before releasing admission.
    let mut entry = Entry {
        inner,
        gate: Some(gate),
        batch: Some(batch),
        completed: false,
    };
    let recheck = (|| {
        let snapshot = inner.database.snapshot();
        let current = view::measure(inner, &snapshot)?;
        admit(&current, input.fence)
    })();
    if let Err(error) = recheck {
        if error == Error::UnchangedFence {
            entry.completed = true;
        }
        drop(entry);
        return Err(error);
    }
    // No snapshot remains when the actual publication is marked entered.
    #[cfg(test)]
    {
        inner.controls.entries.fetch_add(1, super::Ordering::SeqCst);
        match inner.controls.fault() {
            super::Fault::Before => return Err(Error::PublishUnknown),
            super::Fault::PanicBefore => panic!("protected publication before backend"),
            super::Fault::ExitBefore => std::process::exit(71),
            _ => {}
        }
        inner
            .controls
            .native_calls
            .fetch_add(1, super::Ordering::SeqCst);
    }
    let batch = entry.batch.take().ok_or(Error::PublishUnknown)?;
    batch.commit().map_err(|_| Error::PublishUnknown)?;
    #[cfg(test)]
    match inner.controls.fault() {
        super::Fault::After => return Err(Error::PublishUnknown),
        super::Fault::PanicAfter => panic!("protected publication after SyncAll"),
        super::Fault::ExitAfter => std::process::exit(72),
        _ => {}
    }
    entry.completed = true;
    drop(entry);
    Ok(())
}
