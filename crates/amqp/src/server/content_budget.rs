use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub(super) const MAX_RETAINED_CONTENT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(super) struct ContentBudget(Arc<BudgetState>);

#[derive(Debug)]
struct BudgetState {
    maximum: usize,
    retained: AtomicUsize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error(
    "cannot retain {additional_bytes} more encoded bytes; the connection holds {retained_bytes} of {maximum_bytes} bytes"
)]
pub(super) struct ContentBudgetError {
    pub maximum_bytes: usize,
    pub retained_bytes: usize,
    pub additional_bytes: usize,
}

#[derive(Debug)]
pub(super) struct ContentLease {
    budget: ContentBudget,
    bytes: usize,
}

impl Default for ContentBudget {
    fn default() -> Self {
        Self::new(MAX_RETAINED_CONTENT_BYTES)
    }
}

impl ContentBudget {
    pub(super) fn new(maximum: usize) -> Self {
        Self(Arc::new(BudgetState {
            maximum,
            retained: AtomicUsize::new(0),
        }))
    }

    pub(super) fn try_reserve(&self, bytes: usize) -> Result<ContentLease, ContentBudgetError> {
        self.charge(bytes)?;
        Ok(ContentLease {
            budget: self.clone(),
            bytes,
        })
    }

    fn charge(&self, bytes: usize) -> Result<(), ContentBudgetError> {
        self.0
            .retained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |retained| {
                retained
                    .checked_add(bytes)
                    .filter(|next| *next <= self.0.maximum)
            })
            .map(|_| ())
            .map_err(|retained_bytes| ContentBudgetError {
                maximum_bytes: self.0.maximum,
                retained_bytes,
                additional_bytes: bytes,
            })
    }

    #[cfg(test)]
    pub(super) fn retained_bytes(&self) -> usize {
        self.0.retained.load(Ordering::Acquire)
    }
}

impl ContentLease {
    pub(super) fn try_grow(&mut self, bytes: usize) -> Result<(), ContentBudgetError> {
        let next = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| ContentBudgetError {
                maximum_bytes: self.budget.0.maximum,
                retained_bytes: self.budget.0.retained.load(Ordering::Acquire),
                additional_bytes: bytes,
            })?;
        self.budget.charge(bytes)?;
        self.bytes = next;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for ContentLease {
    fn drop(&mut self) {
        let previous = self
            .budget
            .0
            .retained
            .fetch_sub(self.bytes, Ordering::AcqRel);
        debug_assert!(previous >= self.bytes);
    }
}
