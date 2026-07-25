use std::fmt;

use serde::{Deserialize, Serialize};

/// Milliseconds since the Unix epoch, assigned by the proposing leader and
/// carried in the replicated command.
///
/// The state machine never reads a local clock. Every deadline it evaluates is
/// derived from the timestamp on the command being applied, so a follower
/// replaying the log reaches the same state as the leader that produced it.
#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const UNIX_EPOCH: Self = Self(0);

    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// Saturates rather than wrapping so that a very large configured lifetime
    /// produces a deadline that never elapses instead of one already in the
    /// past.
    pub const fn saturating_add_millis(self, millis: u64) -> Self {
        Self(self.0.saturating_add(millis))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}ms", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadlines_saturate_instead_of_wrapping() {
        let deadline = Timestamp::from_millis(u64::MAX - 1).saturating_add_millis(1_000);
        assert_eq!(deadline, Timestamp::from_millis(u64::MAX));
        assert!(deadline > Timestamp::from_millis(u64::MAX - 1));
    }
}
