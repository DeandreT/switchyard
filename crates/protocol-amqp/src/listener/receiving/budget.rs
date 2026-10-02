use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::*;

pub(super) const MAX_RECEIVING_WORK: usize = 32;
pub(super) const MAX_RECEIVING_CONTENT_BYTES: usize = 4 * 1024 * 1024;
const CONTENT_DESCRIPTION: &str = "the acquired delivery exceeds the receiving content capacity";

#[derive(Clone, Default)]
pub(super) struct ContentBudget(Arc<AtomicUsize>);

pub(super) struct ContentLease {
    used: Arc<AtomicUsize>,
    bytes: usize,
}

impl ContentBudget {
    pub(super) fn is_full(&self) -> bool {
        self.0.load(Ordering::Acquire) >= MAX_RECEIVING_CONTENT_BYTES
    }

    pub(super) fn try_acquire(&self, bytes: usize) -> Option<ContentLease> {
        self.0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|total| *total <= MAX_RECEIVING_CONTENT_BYTES)
            })
            .ok()?;
        Some(ContentLease {
            used: Arc::clone(&self.0),
            bytes,
        })
    }
}

impl Drop for ContentLease {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

pub(super) fn projected_delivery_bytes(delivery: &Delivery) -> Result<usize, ReceiveExit> {
    usize::try_from(delivery.delivery_size_upper_bound())
        .ok()
        .filter(|bytes| *bytes <= MAX_RECEIVING_CONTENT_BYTES)
        .ok_or_else(|| {
            ReceiveExit::Refused(error_for(
                AmqpError::ResourceLimitExceeded,
                CONTENT_DESCRIPTION.to_owned(),
            ))
        })
}

#[cfg(test)]
mod tests;
