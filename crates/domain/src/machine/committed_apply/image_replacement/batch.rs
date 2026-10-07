use std::iter::Peekable;

use storage::WriteBatch;

use crate::CommittedImageRows;

#[cfg(test)]
use crate::{DecodedCommittedImage, ValidatedCreateSendImage};

use super::{CommittedImageReplacementError as Error, Result};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod count_tests;

pub(super) const MAX_MUTATIONS: usize = 2 * crate::MAX_COMMITTED_IMAGE_ROWS;
pub(super) const MAX_PAYLOAD_BYTES: usize = 2 * crate::MAX_COMMITTED_IMAGE_BYTES;

#[derive(Default)]
pub(super) struct Plan {
    mutations: usize,
    bytes: usize,
}

impl Plan {
    pub(super) fn add(&mut self, key: usize, value: usize) -> Result<()> {
        let mutations = self
            .mutations
            .checked_add(1)
            .filter(|count| *count <= MAX_MUTATIONS)
            .ok_or(Error::LimitExceeded)?;
        let bytes = key
            .checked_add(value)
            .and_then(|size| self.bytes.checked_add(size))
            .filter(|count| *count <= MAX_PAYLOAD_BYTES)
            .ok_or(Error::LimitExceeded)?;
        self.mutations = mutations;
        self.bytes = bytes;
        Ok(())
    }
}

#[cfg(test)]
pub(super) fn plan(
    old: &DecodedCommittedImage<'_>,
    selected: &ValidatedCreateSendImage<'_>,
) -> Result<Plan> {
    plan_rows(
        old.rows(),
        selected.rows(),
        old.row_count(),
        selected.row_count(),
    )
}

pub(super) fn plan_rows(
    old_rows: CommittedImageRows<'_>,
    selected_rows: CommittedImageRows<'_>,
    old_row_count: usize,
    selected_row_count: usize,
) -> Result<Plan> {
    Ok(count_rows(old_rows, selected_rows, old_row_count, selected_row_count)?.plan)
}

pub(super) struct CountedRows {
    plan: Plan,
    puts: usize,
    deletes: usize,
}

impl CountedRows {
    pub(super) fn counts(&self) -> (usize, usize, usize, usize) {
        (
            self.deletes,
            self.puts,
            self.plan.mutations,
            self.plan.bytes,
        )
    }
}

pub(super) fn count_rows(
    old_rows: CommittedImageRows<'_>,
    selected_rows: CommittedImageRows<'_>,
    old_row_count: usize,
    selected_row_count: usize,
) -> Result<CountedRows> {
    let mut plan = Plan::default();
    let mut selected_rows = selected_rows.peekable();
    let mut old_count = 0usize;
    let mut puts = 0usize;
    for old_row in old_rows {
        old_count = old_count.checked_add(1).ok_or(Error::LimitExceeded)?;
        while selected_rows
            .peek()
            .is_some_and(|row| row.key() < old_row.key())
        {
            let row = selected_rows.next().ok_or(Error::InvalidImage)?;
            plan.add(row.key().len(), row.value().len())?;
            puts = puts.checked_add(1).ok_or(Error::LimitExceeded)?;
        }
        if selected_rows
            .peek()
            .is_some_and(|row| row.key() == old_row.key())
        {
            let row = selected_rows.next().ok_or(Error::InvalidImage)?;
            plan.add(row.key().len(), row.value().len())?;
            puts = puts.checked_add(1).ok_or(Error::LimitExceeded)?;
        } else {
            plan.add(old_row.key().len(), 0)?;
        }
    }
    for row in selected_rows {
        plan.add(row.key().len(), row.value().len())?;
        puts = puts.checked_add(1).ok_or(Error::LimitExceeded)?;
    }
    if puts != selected_row_count || old_count != old_row_count {
        return Err(Error::InvalidImage);
    }
    let deletes = plan
        .mutations
        .checked_sub(puts)
        .ok_or(Error::InvalidImage)?;
    Ok(CountedRows {
        plan,
        puts,
        deletes,
    })
}

#[cfg(test)]
pub(super) fn copy_deletes(
    old: &DecodedCommittedImage<'_>,
    selected: &ValidatedCreateSendImage<'_>,
    plan: &Plan,
) -> Result<WriteBatch> {
    copy_delete_rows(old.rows(), selected.rows(), plan)
}

pub(super) fn copy_delete_rows(
    old_rows: CommittedImageRows<'_>,
    selected_rows: CommittedImageRows<'_>,
    plan: &Plan,
) -> Result<WriteBatch> {
    let mut batch = WriteBatch::default();
    batch
        .try_reserve_mutations(plan.mutations)
        .map_err(|_| Error::Allocation)?;
    for key in StaleKeys::new(old_rows, selected_rows) {
        batch.push_delete(copy_bytes(key)?);
    }
    Ok(batch)
}

pub(super) fn copy_put_rows(
    batch: &mut WriteBatch,
    selected_rows: CommittedImageRows<'_>,
    plan: &Plan,
) -> Result<()> {
    for row in selected_rows {
        batch.push_put(copy_bytes(row.key())?, copy_bytes(row.value())?);
    }
    if batch.mutations().len() != plan.mutations {
        return Err(Error::InvalidImage);
    }
    Ok(())
}

fn copy_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(bytes.len())
        .map_err(|_| Error::Allocation)?;
    copied.extend_from_slice(bytes);
    Ok(copied)
}

// Canonical sorted rows permit a linear merge without copied keys or a key set.
struct StaleKeys<'old, 'selected> {
    old: CommittedImageRows<'old>,
    selected: Peekable<CommittedImageRows<'selected>>,
}

impl<'old, 'selected> StaleKeys<'old, 'selected> {
    fn new(old: CommittedImageRows<'old>, selected: CommittedImageRows<'selected>) -> Self {
        Self {
            old,
            selected: selected.peekable(),
        }
    }
}

impl<'old> Iterator for StaleKeys<'old, '_> {
    type Item = &'old [u8];

    fn next(&mut self) -> Option<Self::Item> {
        for old in self.old.by_ref() {
            while self
                .selected
                .peek()
                .is_some_and(|selected| selected.key() < old.key())
            {
                let _ = self.selected.next();
            }
            if self
                .selected
                .peek()
                .is_some_and(|selected| selected.key() == old.key())
            {
                let _ = self.selected.next();
            } else {
                return Some(old.key());
            }
        }
        None
    }
}
