//! Link credit is an integer budget, not an ordering of wrapped endpoints.
//!
//! Rebase at confirmed peer progress. The causal publication window is shorter
//! than one complete u32 cycle, even for a full-uint grant. A raw count is used
//! only if it has a unique position in progress actually published by us.

const MAX_SPAN: u64 = u32::MAX as u64;
const MAX_SERIAL_STEP: u32 = (1_u32 << 31) - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::server) struct SendingCredit {
    initial_wire: u32,
    peer_wire: u32,
    peer_still_initial: bool,
    count_wire: u32,
    started: u64,
    published: u64,
    edge: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(initial: u32, credit: u32) -> SendingCredit {
        SendingCredit::new(initial)
            .update(Some(initial), Some(credit), 0)
            .unwrap()
    }

    fn finish_drain(mut credit: SendingCredit) -> (SendingCredit, Vec<(u32, u32)>) {
        let target = credit.drain_target();
        let mut written = Vec::new();
        loop {
            let step = credit.drain_step(target).unwrap();
            written.push((step.delivery_count, step.remaining));
            credit = step.progress;
            if step.remaining == 0 {
                return (credit, written);
            }
        }
    }

    #[test]
    fn one_credit_crosses_max_once_and_only_a_new_grant_resumes() {
        let mut credit = grant(u32::MAX, 1);
        assert!(credit.can_reserve(0));
        credit.start_delivery(0).unwrap();
        assert_eq!(credit.count(), 0);
        assert!(!credit.can_reserve(0));
        let snapshot = credit;
        assert!(
            credit.update(Some(0), Some(1), 0).is_err(),
            "a start is not a published write"
        );
        assert_eq!(credit, snapshot);
        credit.first_transfer_written();
        credit = credit.update(Some(u32::MAX), Some(1), 0).unwrap();
        assert!(
            !credit.can_reserve(0),
            "an identical old snapshot cannot top up"
        );
        credit = credit.update(Some(0), Some(1), 0).unwrap();
        assert!(credit.can_reserve(0));
        credit.start_delivery(0).unwrap();
        assert_eq!(credit.count(), 1);
        assert!(!credit.can_reserve(0));
    }

    #[test]
    fn unchanged_peer_count_can_legitimately_grow_or_revoke_the_edge() {
        let mut credit = grant(9, 1);
        credit.start_delivery(0).unwrap();
        credit.first_transfer_written();
        credit = credit.update(Some(9), Some(2), 0).unwrap();
        assert_eq!(credit.available(), 1);
        credit = credit.update(Some(9), Some(0), 0).unwrap();
        assert_eq!(credit.edge, 0);
        assert_eq!(credit.available(), 0);
        let step = credit.drain_step(credit.drain_target()).unwrap();
        assert_eq!(
            step.delivery_count, 10,
            "debt never moves the actual count backward"
        );
        assert_eq!(step.remaining, 0);
        assert_eq!(
            step.progress, credit,
            "reporting zero does not forgive debt into a grant"
        );
    }

    #[test]
    fn omitted_count_is_initial_not_current_and_count_only_grants_nothing() {
        let mut credit = SendingCredit::new(73).update(None, Some(2), 0).unwrap();
        credit.start_delivery(0).unwrap();
        credit.first_transfer_written();
        credit = credit.update(None, Some(2), 0).unwrap();
        assert_eq!(credit.available(), 1);
        credit = credit.update(Some(74), None, 0).unwrap();
        assert_eq!(
            credit.available(),
            1,
            "count-only state preserves the existing absolute edge"
        );
        assert!(credit.update(None, Some(u32::MAX), 0).is_err());
        credit.start_delivery(0).unwrap();
        credit.first_transfer_written();
        credit = credit.update(Some(75), None, 0).unwrap();
        assert_eq!(credit.available(), 0);
    }

    #[test]
    fn full_uint_credit_and_known_half_range_progress_are_not_arbitrary_caps() {
        for size in [0, 1, MAX_SERIAL_STEP, 1 << 31, u32::MAX] {
            let credit = grant(0, size);
            assert_eq!(credit.available(), size);
            let (drained, writes) = finish_drain(credit);
            assert_eq!(drained.count(), size);
            assert_eq!(drained.available(), 0);
            let mut previous = 0_u32;
            for (count, _) in writes {
                assert!(count.wrapping_sub(previous) <= MAX_SERIAL_STEP);
                previous = count;
            }
            assert!(
                drained
                    .update(Some(size), Some(1), 0)
                    .unwrap()
                    .can_reserve(0)
            );
        }
        let (drained, writes) = finish_drain(grant(0, u32::MAX));
        assert_eq!(
            writes,
            vec![(MAX_SERIAL_STEP, 1 << 31), (u32::MAX - 1, 1), (u32::MAX, 0)]
        );
        let half = drained.update(Some(1 << 31), Some(1 << 31), 0).unwrap();
        assert_eq!(
            half.available(),
            1,
            "the causal window resolves a raw RFC-ambiguous pair"
        );
        assert!(
            SendingCredit::new(0)
                .update(Some(1 << 31), Some(u32::MAX), 0)
                .is_err()
        );
    }

    #[test]
    fn repeated_rebases_do_not_use_a_lifetime_epoch_or_reinterpret_initial_omission() {
        let mut credit = grant(17, u32::MAX);
        for _ in 0..8 {
            let (drained, _) = finish_drain(credit);
            credit = drained
                .update(Some(drained.count()), Some(u32::MAX), 0)
                .unwrap();
            assert_eq!((credit.started, credit.published), (0, 0));
        }
        let (drained, _) = finish_drain(grant(0, u32::MAX));
        let last = drained.update(Some(u32::MAX), Some(1), 0).unwrap();
        let (wrapped, _) = finish_drain(last);
        let confirmed = wrapped.update(Some(0), Some(1), 0).unwrap();
        assert_eq!(confirmed.peer_wire, confirmed.initial_wire);
        assert!(!confirmed.peer_still_initial);
        assert!(confirmed.update(None, Some(1), 0).is_err());
    }

    #[test]
    fn impossible_ahead_and_stale_reports_are_transactional() {
        let mut credit = grant(100, 3);
        credit.start_delivery(0).unwrap();
        credit.first_transfer_written();
        credit = credit.update(Some(101), Some(3), 0).unwrap();
        for bad in [100, 102, 101_u32.wrapping_add(1 << 31)] {
            let before = credit;
            assert!(credit.update(Some(bad), Some(u32::MAX), 0).is_err());
            assert_eq!(credit, before);
        }
        if let Ok(too_many) = usize::try_from(MAX_SPAN + 1) {
            assert!(credit.update(Some(101), Some(u32::MAX), too_many).is_err());
        }
    }

    #[test]
    fn reservations_hold_old_admission_but_release_never_refunds_a_revoke() {
        let admitted = grant(u32::MAX, 1);
        assert!(!admitted.can_reserve(1));
        assert!(admitted.can_reserve(0));
        let mut revoked = admitted.update(Some(u32::MAX), Some(0), 1).unwrap();
        assert!(
            !revoked.can_reserve(0),
            "releasing against the current zero grant creates nothing"
        );
        revoked.start_delivery(0).unwrap(); // consume the already-held token
        assert_eq!(revoked.count(), 0);
        assert!(!revoked.can_reserve(0));
        assert!(revoked.drain_step(revoked.drain_target()).is_err());
        revoked.first_transfer_written();
        let step = revoked.drain_step(revoked.drain_target()).unwrap();
        assert_eq!((step.delivery_count, step.remaining), (0, 0));
        assert_eq!(step.progress.edge, 0);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DrainTarget(i64);

pub(super) struct DrainStep {
    pub(super) progress: SendingCredit,
    pub(super) delivery_count: u32,
    pub(super) remaining: u32,
}

impl SendingCredit {
    pub(in crate::server) fn new(initial: u32) -> Self {
        Self {
            initial_wire: initial,
            peer_wire: initial,
            peer_still_initial: true,
            count_wire: initial,
            started: 0,
            published: 0,
            edge: 0,
        }
    }

    fn validate(&self, reservations: usize) -> Result<(), &'static str> {
        let reserved =
            u64::try_from(reservations).map_err(|_| "unrepresentable reservation count")?;
        if self
            .started
            .checked_add(reserved)
            .is_none_or(|sum| sum > MAX_SPAN)
            || self.published > self.started
            || !(-(MAX_SPAN as i64)..=MAX_SPAN as i64).contains(&self.edge)
            || self.count_wire != self.peer_wire.wrapping_add(self.started as u32)
            || (self.peer_still_initial && self.peer_wire != self.initial_wire)
        {
            return Err("invalid bounded sending-credit state");
        }
        Ok(())
    }

    /// Propose all link changes before mutating the actor or its drain state.
    pub(super) fn update(
        &self,
        delivery_count: Option<u32>,
        credit: Option<u32>,
        reservations: usize,
    ) -> Result<Self, &'static str> {
        self.validate(reservations)?;
        let peer_wire = match delivery_count {
            Some(count) => count,
            None if self.peer_still_initial => self.initial_wire,
            None => return Err("omitted delivery count would roll back confirmed progress"),
        };
        let delta = u64::from(peer_wire.wrapping_sub(self.peer_wire));
        if delta > self.published {
            return Err("receiver delivery count is outside published sender progress");
        }
        let mut next = *self;
        next.peer_wire = peer_wire;
        next.peer_still_initial &= delta == 0;
        next.started -= delta;
        next.published -= delta;
        next.edge = match credit {
            Some(credit) => i64::from(credit),
            None => self
                .edge
                .checked_sub(delta as i64)
                .ok_or("unrepresentable credit edge")?,
        };
        next.validate(reservations)?;
        Ok(next)
    }

    pub(super) fn count(&self) -> u32 {
        self.count_wire
    }

    pub(super) fn available(&self) -> u32 {
        let remaining = self.edge - self.started as i64;
        if remaining > 0 { remaining as u32 } else { 0 }
    }

    pub(super) fn can_reserve(&self, reservations: usize) -> bool {
        u64::try_from(reservations).is_ok_and(|reserved| u64::from(self.available()) > reserved)
    }

    /// Caller checks ordinary admission or consumes an existing valid guard.
    pub(super) fn start_delivery(&mut self, reservations_after: usize) -> Result<(), &'static str> {
        let mut next = *self;
        next.started = next
            .started
            .checked_add(1)
            .ok_or("unrepresentable sender progress")?;
        next.count_wire = next.count_wire.wrapping_add(1);
        next.validate(reservations_after)?;
        *self = next;
        Ok(())
    }

    pub(super) fn first_transfer_written(&mut self) {
        self.published = self.started;
    }

    pub(super) fn drain_target(&self) -> DrainTarget {
        DrainTarget(self.edge)
    }

    pub(super) fn unused(&self, target: DrainTarget) -> u32 {
        let remaining = target.0 - self.started as i64;
        if remaining > 0 { remaining as u32 } else { 0 }
    }

    /// A proposed, observable RFC-1982 step. Commit only after its Flow write.
    /// Up to three such steps return a full uint of unused permission.
    pub(super) fn drain_step(&self, target: DrainTarget) -> Result<DrainStep, &'static str> {
        self.validate(0)?;
        if target != self.drain_target() || self.published != self.started {
            return Err("drain target or publication frontier is not current");
        }
        let unused = self.unused(target);
        let advance = unused.min(MAX_SERIAL_STEP);
        let mut next = *self;
        next.started = next
            .started
            .checked_add(u64::from(advance))
            .ok_or("unrepresentable drain progress")?;
        next.published = next.started;
        next.count_wire = next.count_wire.wrapping_add(advance);
        next.validate(0)?;
        Ok(DrainStep {
            progress: next,
            delivery_count: next.count_wire,
            remaining: unused - advance,
        })
    }
}
