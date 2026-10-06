use fjall::Readable;

use crate::protected_state::{Shape, check_components, combined_bytes, reserve};
use crate::{
    CatalogReadError, PROTECTED_STATE_RECORD_LIMITS, ProtectedStateError as Error, ReadBudget,
    StoreSnapshot, StoredProtectedState, StoredSnapshotCatalog,
};

use super::{ACTIVE_PROTECTED_STATE_STORE_FORMAT, Inner, KEYS, PROFILE, ReadGuard};

pub(super) struct View {
    pub(super) initialized: bool,
    pub(super) shape: Shape,
    pub(super) lengths: [usize; 3],
    pub(super) fence: Option<fjall::UserValue>,
    pub(super) key_bytes: usize,
}

pub(super) fn size(
    snapshot: &fjall::Snapshot,
    space: &fjall::Keyspace,
    key: &[u8],
) -> Result<usize, Error> {
    let size = snapshot
        .size_of(space, key)
        .map_err(|_| Error::Poisoned)?
        .ok_or(Error::InvalidState)?;
    usize::try_from(size).map_err(|_| Error::LimitExceeded)
}

pub(super) fn required(
    snapshot: &fjall::Snapshot,
    space: &fjall::Keyspace,
    key: &[u8],
    expected: usize,
) -> Result<fjall::UserValue, Error> {
    if size(snapshot, space, key)? != expected {
        return Err(Error::InvalidState);
    }
    let value = snapshot
        .get(space, key)
        .map_err(|_| Error::Poisoned)?
        .ok_or(Error::InvalidState)?;
    if value.len() != expected {
        return Err(Error::InvalidState);
    }
    Ok(value)
}

pub(super) fn measure(inner: &Inner, snapshot: &fjall::Snapshot) -> Result<View, Error> {
    #[cfg(test)]
    {
        inner.controls.views.fetch_add(1, super::Ordering::SeqCst);
        if inner.controls.fault() == super::Fault::CurrentBackend {
            return Err(Error::Poisoned);
        }
    }
    let mut lengths = [None; 6];
    let mut previous: Option<fjall::UserKey> = None;
    for guard in snapshot.iter(&inner.meta) {
        let key = guard.key().map_err(|_| Error::Poisoned)?;
        if previous
            .as_ref()
            .is_some_and(|p| p.as_ref() >= key.as_ref())
        {
            return Err(Error::InvalidState);
        }
        let index = KEYS
            .iter()
            .position(|known| *known == key.as_ref())
            .ok_or(Error::InvalidState)?;
        if lengths[index].is_some() {
            return Err(Error::InvalidState);
        }
        lengths[index] = Some(size(snapshot, &inner.meta, &key)?);
        previous = Some(key);
    }
    if lengths[0] != Some(4) || lengths[1] != Some(PROFILE.len()) || lengths[2] != Some(1) {
        return Err(Error::InvalidState);
    }
    let format = required(snapshot, &inner.meta, KEYS[0], 4)?;
    let profile = required(snapshot, &inner.meta, KEYS[1], PROFILE.len())?;
    let initialization = required(snapshot, &inner.meta, KEYS[2], 1)?;
    if format.as_ref() != ACTIVE_PROTECTED_STATE_STORE_FORMAT.to_be_bytes().as_slice()
        || profile.as_ref() != PROFILE
    {
        return Err(Error::InvalidState);
    }
    let initialized = match initialization.as_ref() {
        [0] => false,
        [1] => true,
        _ => return Err(Error::InvalidState),
    };
    let components = if initialized {
        let [Some(metadata), Some(artifact), Some(fence)] = [lengths[3], lengths[4], lengths[5]]
        else {
            return Err(Error::InvalidState);
        };
        check_components(0, metadata, artifact, fence, Error::InvalidState)?;
        [metadata, artifact, fence]
    } else {
        if lengths[3..].iter().any(Option::is_some) {
            return Err(Error::InvalidState);
        }
        [0; 3]
    };

    let mut budget = ReadBudget::new(PROTECTED_STATE_RECORD_LIMITS);
    let mut rows = 0usize;
    let mut business = 0usize;
    let mut key_bytes = 0usize;
    previous = None;
    for guard in snapshot.iter(&inner.records) {
        let key = guard.key().map_err(|_| Error::Poisoned)?;
        if key.is_empty()
            || previous
                .as_ref()
                .is_some_and(|p| p.as_ref() >= key.as_ref())
        {
            return Err(Error::InvalidState);
        }
        let value = size(snapshot, &inner.records, &key)?;
        budget
            .consume(key.len(), value)
            .map_err(|_| Error::LimitExceeded)?;
        rows = rows.checked_add(1).ok_or(Error::LimitExceeded)?;
        business = business
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value))
            .ok_or(Error::LimitExceeded)?;
        key_bytes = key_bytes
            .checked_add(key.len())
            .ok_or(Error::LimitExceeded)?;
        previous = Some(key);
    }
    if !initialized && rows != 0 {
        return Err(Error::InvalidState);
    }
    let total = combined_bytes(business, components[0], components[1], components[2])?;
    let fence = if initialized {
        Some(required(snapshot, &inner.meta, KEYS[5], components[2])?)
    } else {
        None
    };
    Ok(View {
        initialized,
        shape: Shape {
            rows,
            business,
            total,
        },
        lengths: components,
        fence,
        key_bytes,
    })
}

pub(super) fn copy(value: &[u8]) -> Result<Vec<u8>, Error> {
    let mut result = Vec::new();
    reserve(&mut result, value.len())?;
    result.extend_from_slice(value);
    Ok(result)
}

fn copy_view(
    inner: &Inner,
    snapshot: &fjall::Snapshot,
    view: &View,
) -> Result<StoredProtectedState, Error> {
    if !view.initialized {
        return Ok(StoredProtectedState::pristine());
    }
    #[cfg(test)]
    {
        inner.controls.copies.fetch_add(1, super::Ordering::SeqCst);
        if inner.controls.fault() == super::Fault::CaptureAllocation {
            return Err(Error::Allocation);
        }
    }
    let mut entries = Vec::new();
    reserve(&mut entries, view.shape.rows)?;
    let mut budget = ReadBudget::new(PROTECTED_STATE_RECORD_LIMITS);
    let mut business = 0usize;
    let mut key_bytes = 0usize;
    let mut previous: Option<fjall::UserKey> = None;
    for guard in snapshot.iter(&inner.records) {
        let key = guard.key().map_err(|_| Error::Poisoned)?;
        if entries.len() >= view.shape.rows
            || key.is_empty()
            || previous
                .as_ref()
                .is_some_and(|p| p.as_ref() >= key.as_ref())
        {
            return Err(Error::InvalidState);
        }
        let expected = size(snapshot, &inner.records, &key)?;
        budget
            .consume(key.len(), expected)
            .map_err(|_| Error::LimitExceeded)?;
        let value = required(snapshot, &inner.records, &key, expected)?;
        business = business
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or(Error::LimitExceeded)?;
        key_bytes = key_bytes
            .checked_add(key.len())
            .ok_or(Error::LimitExceeded)?;
        entries.push((copy(&key)?, copy(&value)?));
        previous = Some(key);
    }
    if entries.len() != view.shape.rows
        || business != view.shape.business
        || key_bytes != view.key_bytes
    {
        return Err(Error::InvalidState);
    }
    let metadata = required(snapshot, &inner.meta, KEYS[3], view.lengths[0])?;
    let artifact = required(snapshot, &inner.meta, KEYS[4], view.lengths[1])?;
    let fence = required(snapshot, &inner.meta, KEYS[5], view.lengths[2])?;
    if view.fence.as_ref().map(|v| v.as_ref()) != Some(fence.as_ref()) {
        return Err(Error::InvalidState);
    }
    let live =
        StoredSnapshotCatalog::copy_from_parts(&metadata, &artifact).map_err(
            |error| match error {
                CatalogReadError::Allocation => Error::Allocation,
                CatalogReadError::LimitExceeded => Error::LimitExceeded,
                CatalogReadError::Storage(_) => Error::InvalidState,
            },
        )?;
    let total = combined_bytes(business, metadata.len(), artifact.len(), fence.len())?;
    if total != view.shape.total {
        return Err(Error::InvalidState);
    }
    Ok(StoredProtectedState {
        initialized: true,
        records: StoreSnapshot { entries },
        live: Some(live),
        fence: Some(copy(&fence)?),
        logical_bytes: total,
    })
}

pub(super) fn capture(inner: &Inner) -> Result<StoredProtectedState, Error> {
    let guard = ReadGuard::acquire(inner)?;
    // The closure drops every native temporary before the final poison decision.
    let result = (|| {
        let snapshot = inner.database.snapshot();
        let view = measure(inner, &snapshot)?;
        copy_view(inner, &snapshot, &view)
    })();
    guard.finish(result)
}
