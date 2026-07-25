//! The one place the process reads real time.
//!
//! The state machine never consults a clock: every timestamp it sees was stamped
//! onto a command before that command was proposed. Keeping the reading behind
//! this trait is what lets a test drive the whole runtime by hand and still
//! exercise the real code path.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use domain::Timestamp;

pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Timestamp;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        // A reading before the epoch, or one too large for the timestamp, means
        // the host clock is badly wrong. Saturating rather than panicking leaves
        // the proposer's regression check as the thing that notices and refuses.
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        Timestamp::from_millis(millis)
    }
}

/// A clock a test sets by hand.
///
/// Cloning shares one reading, so a test can hold a handle and move time under a
/// worker that is already running.
#[derive(Clone, Debug, Default)]
pub struct ManualClock {
    millis: Arc<AtomicU64>,
}

impl ManualClock {
    pub fn at(millis: u64) -> Self {
        Self {
            millis: Arc::new(AtomicU64::new(millis)),
        }
    }

    pub fn set(&self, millis: u64) {
        self.millis.store(millis, Ordering::SeqCst);
    }

    pub fn advance(&self, millis: u64) {
        self.millis.fetch_add(millis, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(self.millis.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_system_clock_reads_a_plausible_wall_time() {
        // Well past 2020 and short of the u64 saturation point, which is enough
        // to show the conversion is not silently producing a degenerate value.
        let now = SystemClock.now().as_millis();
        assert!(now > 1_600_000_000_000, "got {now}");
        assert!(now < u64::MAX);
    }

    #[test]
    fn a_manual_clock_is_shared_by_its_clones() {
        let clock = ManualClock::at(100);
        let handle = clock.clone();
        clock.advance(50);
        assert_eq!(handle.now(), Timestamp::from_millis(150));

        handle.set(10);
        assert_eq!(clock.now(), Timestamp::from_millis(10));
    }
}
