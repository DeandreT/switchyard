use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use tokio::sync::Notify;

pub(super) struct Consumption {
    count: AtomicU64,
    wake: Arc<Notify>,
}

impl Consumption {
    pub(super) fn new(wake: Arc<Notify>) -> Self {
        Self {
            count: AtomicU64::new(0),
            wake,
        }
    }

    // Called synchronously after popping a delivery, before recv can yield.
    pub(super) fn consumed(&self) {
        self.count.fetch_add(1, Ordering::Release);
        self.wake.notify_one();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReceiveSnapshot {
    pub delivery_count: u32,
    pub link_credit: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(super) enum ReceiveCreditError {
    #[error("the peer exceeded the receiving link's delivery credit")]
    TransferLimitExceeded,
    #[error("the receiving link has no delivery reservation to release")]
    NoReservedDelivery,
    #[error("the consumed delivery watermark exceeds the receiving link's occupied slots")]
    ConsumptionExceedsOccupancy,
    #[error("sender delivery count is {actual}, expected {expected}")]
    SenderCountMismatch { expected: u32, actual: u32 },
}

pub(super) struct ReceiveCredit {
    delivery_count: u32,
    capacity: u32,
    remaining_credit: u32,
    occupied: u32,
    consumption: Arc<Consumption>,
    observed_consumption: u64,
    dirty: bool,
}

impl ReceiveCredit {
    pub(super) fn new(initial_count: u32, capacity: u32, consumption: Arc<Consumption>) -> Self {
        Self {
            delivery_count: initial_count,
            capacity,
            remaining_credit: 0,
            occupied: 0,
            consumption,
            observed_consumption: 0,
            dirty: true,
        }
    }

    #[cfg(test)]
    pub(super) fn delivery_count(&self) -> u32 {
        self.delivery_count
    }

    #[cfg(test)]
    pub(super) fn occupied(&self) -> u32 {
        self.occupied
    }

    pub(super) fn snapshot(&self) -> ReceiveSnapshot {
        ReceiveSnapshot {
            delivery_count: self.delivery_count,
            link_credit: self.remaining_credit,
        }
    }

    pub(super) fn try_begin_delivery(&mut self) -> Result<(), ReceiveCreditError> {
        if self.remaining_credit == 0 || self.occupied == self.capacity {
            return Err(ReceiveCreditError::TransferLimitExceeded);
        }
        self.remaining_credit -= 1;
        self.occupied += 1;
        self.delivery_count = self.delivery_count.wrapping_add(1);
        Ok(())
    }

    pub(super) fn abort_delivery(&mut self) -> Result<(), ReceiveCreditError> {
        if self.occupied == 0 {
            return Err(ReceiveCreditError::NoReservedDelivery);
        }
        self.occupied -= 1;
        self.dirty = true;
        Ok(())
    }

    pub(super) fn apply_consumed(&mut self) -> Result<bool, ReceiveCreditError> {
        let count = self.consumption.count.load(Ordering::Acquire);
        // A watermark can wrap, but a live generation can advance by no more
        // than its bounded occupied slots before the driver observes it.
        let consumed = count.wrapping_sub(self.observed_consumption);
        if consumed > u64::from(self.occupied) {
            return Err(ReceiveCreditError::ConsumptionExceedsOccupancy);
        }
        if consumed == 0 {
            return Ok(false);
        }
        self.observed_consumption = count;
        self.occupied -= consumed as u32;
        self.dirty = true;
        Ok(true)
    }

    pub(super) fn take_refill(&mut self) -> Option<ReceiveSnapshot> {
        if !self.dirty {
            return None;
        }
        self.dirty = false;
        self.remaining_credit = self.capacity - self.occupied;
        Some(self.snapshot())
    }

    pub(super) fn check_sender_count(&self, actual: u32) -> Result<(), ReceiveCreditError> {
        if actual != self.delivery_count {
            return Err(ReceiveCreditError::SenderCountMismatch {
                expected: self.delivery_count,
                actual,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(initial: u32, capacity: u32) -> (ReceiveCredit, Arc<Consumption>, Arc<Notify>) {
        let wake = Arc::new(Notify::new());
        let consumption = Arc::new(Consumption::new(wake.clone()));
        let credit = ReceiveCredit::new(initial, capacity, consumption.clone());
        (credit, consumption, wake)
    }

    #[test]
    fn initial_credit_is_not_available_until_published() {
        let (mut credit, _, _) = fixture(17, 32);
        assert_eq!(
            credit.try_begin_delivery(),
            Err(ReceiveCreditError::TransferLimitExceeded)
        );
        assert_eq!(credit.delivery_count(), 17);
        assert_eq!(credit.occupied(), 0);
        assert_eq!(
            credit.take_refill(),
            Some(ReceiveSnapshot {
                delivery_count: 17,
                link_credit: 32,
            })
        );
        assert_eq!(credit.take_refill(), None);
        credit.try_begin_delivery().expect("published credit");
    }

    #[test]
    fn paused_consumers_cannot_exceed_the_slot_capacity() {
        let (mut credit, _, _) = fixture(0, 32);
        credit.take_refill();
        for _ in 0..32 {
            credit.try_begin_delivery().expect("free slot");
        }
        assert_eq!(credit.occupied(), 32);
        assert_eq!(credit.delivery_count(), 32);
        assert_eq!(credit.take_refill(), None);
        assert_eq!(
            credit.try_begin_delivery(),
            Err(ReceiveCreditError::TransferLimitExceeded)
        );
        assert_eq!(credit.delivery_count(), 32);
        assert_eq!(credit.occupied(), 32);
    }

    #[tokio::test]
    async fn consumption_is_coalesced_and_replenishes_only_freed_slots() {
        let (mut credit, consumed, wake) = fixture(9, 32);
        credit.take_refill();
        for _ in 0..32 {
            credit.try_begin_delivery().expect("free slot");
        }
        for _ in 0..3 {
            consumed.consumed();
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), wake.notified())
            .await
            .expect("a synchronous pop leaves a wake permit");
        assert_eq!(credit.apply_consumed(), Ok(true));
        assert_eq!(credit.occupied(), 29);
        assert_eq!(credit.apply_consumed(), Ok(false));
        assert_eq!(
            credit.take_refill(),
            Some(ReceiveSnapshot {
                delivery_count: 41,
                link_credit: 3,
            })
        );
        for _ in 0..3 {
            credit.try_begin_delivery().expect("replenished slot");
        }
        assert_eq!(
            credit.try_begin_delivery(),
            Err(ReceiveCreditError::TransferLimitExceeded)
        );
    }

    #[test]
    fn abort_releases_a_slot_but_does_not_rewind_delivery_count() {
        let (mut credit, _, _) = fixture(u32::MAX, 2);
        credit.take_refill();
        credit.try_begin_delivery().expect("partial reservation");
        assert_eq!(credit.delivery_count(), 0);
        credit.abort_delivery().expect("discard partial delivery");
        assert_eq!(credit.occupied(), 0);
        assert_eq!(
            credit.take_refill(),
            Some(ReceiveSnapshot {
                delivery_count: 0,
                link_credit: 2,
            })
        );
        for _ in 0..2 {
            credit.try_begin_delivery().expect("free slot");
        }
        assert_eq!(credit.delivery_count(), 2);
    }

    #[test]
    fn repeated_refills_preserve_unconsumed_credit_without_amplification() {
        let (mut credit, consumed, _) = fixture(0, 4);
        credit.take_refill();
        credit.try_begin_delivery().expect("first delivery");
        credit.try_begin_delivery().expect("second delivery");
        consumed.consumed();
        credit.apply_consumed().expect("one delivery consumed");
        assert_eq!(credit.take_refill().expect("refill").link_credit, 3);
        assert_eq!(credit.take_refill(), None);
        for _ in 0..3 {
            credit.try_begin_delivery().expect("available grant");
        }
        assert_eq!(credit.occupied(), 4);
        assert_eq!(
            credit.try_begin_delivery(),
            Err(ReceiveCreditError::TransferLimitExceeded)
        );
    }

    #[test]
    fn long_lived_receivers_replenish_beyond_the_old_fixed_grant() {
        let (mut credit, consumed, _) = fixture(u32::MAX - 2, 32);
        credit.take_refill();
        for delivered in 1_u32..=6_000 {
            credit.try_begin_delivery().expect("available slot");
            consumed.consumed();
            assert_eq!(credit.apply_consumed(), Ok(true));
            let snapshot = credit.take_refill().expect("replacement grant");
            assert_eq!(snapshot.link_credit, 32);
            assert_eq!(
                snapshot.delivery_count,
                (u32::MAX - 2).wrapping_add(delivered)
            );
            assert_eq!(credit.occupied(), 0);
        }
    }

    #[test]
    fn echo_snapshot_does_not_publish_an_unannounced_refill() {
        let (mut credit, consumed, _) = fixture(0, 2);
        credit.take_refill();
        credit.try_begin_delivery().expect("first delivery");
        consumed.consumed();
        credit.apply_consumed().expect("consumed delivery");
        assert_eq!(
            credit.snapshot(),
            ReceiveSnapshot {
                delivery_count: 1,
                link_credit: 1,
            }
        );
        assert_eq!(credit.take_refill().expect("publish refill").link_credit, 2);
        assert_eq!(credit.snapshot().link_credit, 2);
    }

    #[test]
    fn bad_consumption_is_rejected_without_mutating_credit() {
        let (mut credit, consumed, _) = fixture(0, 2);
        credit.take_refill();
        credit.try_begin_delivery().expect("one delivery");
        consumed.consumed();
        consumed.consumed();
        assert_eq!(
            credit.apply_consumed(),
            Err(ReceiveCreditError::ConsumptionExceedsOccupancy)
        );
        assert_eq!(credit.occupied(), 1);
        assert_eq!(credit.observed_consumption, 0);
        assert_eq!(credit.remaining_credit, 1);
        assert_eq!(credit.take_refill(), None);
    }

    #[test]
    fn a_stale_generation_cannot_refill_a_reattached_handle() {
        let (_, old_consumed, wake) = fixture(0, 1);
        let new_consumed = Arc::new(Consumption::new(wake));
        let mut credit = ReceiveCredit::new(99, 1, new_consumed.clone());
        credit.take_refill();
        credit.try_begin_delivery().expect("current reservation");
        old_consumed.consumed();
        assert_eq!(credit.apply_consumed(), Ok(false));
        assert_eq!(credit.occupied(), 1);
        assert_eq!(credit.take_refill(), None);
        new_consumed.consumed();
        assert_eq!(credit.apply_consumed(), Ok(true));
        assert_eq!(credit.take_refill().expect("current refill").link_credit, 1);
    }

    #[test]
    fn consumption_watermark_wrap_has_a_bounded_delta() {
        let (mut credit, consumed, _) = fixture(0, 2);
        credit.take_refill();
        for _ in 0..2 {
            credit.try_begin_delivery().expect("reservation");
        }
        credit.observed_consumption = u64::MAX - 1;
        consumed.count.store(u64::MAX - 1, Ordering::Release);
        consumed.consumed();
        consumed.consumed();
        assert_eq!(credit.apply_consumed(), Ok(true));
        assert_eq!(credit.observed_consumption, 0);
        assert_eq!(credit.occupied(), 0);
        assert_eq!(credit.take_refill().expect("refill").link_credit, 2);
    }

    #[test]
    fn sender_counts_follow_actual_first_transfers_including_abort() {
        let (mut credit, _, _) = fixture(u32::MAX, 1);
        assert_eq!(credit.check_sender_count(u32::MAX), Ok(()));
        credit.take_refill();
        credit.try_begin_delivery().expect("reservation");
        credit.abort_delivery().expect("abort");
        assert_eq!(credit.check_sender_count(0), Ok(()));
        assert_eq!(
            credit.check_sender_count(u32::MAX),
            Err(ReceiveCreditError::SenderCountMismatch {
                expected: 0,
                actual: u32::MAX,
            })
        );
    }

    #[test]
    fn abort_without_a_reservation_and_zero_capacity_are_safe() {
        let (mut credit, _, _) = fixture(1, 0);
        assert_eq!(
            credit.abort_delivery(),
            Err(ReceiveCreditError::NoReservedDelivery)
        );
        assert_eq!(credit.take_refill().expect("zero grant").link_credit, 0);
        assert_eq!(
            credit.try_begin_delivery(),
            Err(ReceiveCreditError::TransferLimitExceeded)
        );
        assert_eq!(credit.delivery_count(), 1);
    }
}
