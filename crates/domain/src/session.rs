//! Session ownership and session state.
//!
//! A session is the only scope in which Switchyard guarantees FIFO order. It
//! exists implicitly: sending with a session identifier puts messages in it, and
//! a record appears here the first time the session is locked or given state. A
//! session with no record is unlocked and holds no state.
//!
//! A session lock is exclusive — one receiver owns the session until it releases
//! the lock or the deadline elapses. It is separate from the per-message locks a
//! receiver takes inside the session: releasing a session does not settle the
//! messages already locked in it, which keep their own deadlines.

use serde::{Deserialize, Serialize};

use crate::{LockToken, SessionId, Timestamp};

/// One receiver's exclusive claim on a session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionLock {
    pub token: LockToken,
    pub locked_until: Timestamp,
}

/// The replicated state of one session.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionRecord {
    /// `None` when no receiver has ever held the session. A lock whose deadline
    /// has passed stays here until a sweep or the next acceptance clears it, so
    /// this being `Some` does not mean the session is held.
    pub lock: Option<SessionLock>,
    /// Opaque state a receiver keeps alongside the session. It outlives any one
    /// receiver: releasing the session leaves it in place.
    pub state: Vec<u8>,
}

impl SessionRecord {
    /// The lock, if one is actually held at `now`.
    pub fn live_lock_at(&self, now: Timestamp) -> Option<SessionLock> {
        self.lock.filter(|lock| lock.locked_until > now)
    }
}

/// The session lock a command presents in order to act inside a session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionHold {
    pub session_id: SessionId,
    pub token: LockToken,
}

impl SessionHold {
    pub fn new(session_id: SessionId, token: LockToken) -> Self {
        Self { session_id, token }
    }
}

/// One accepted session, handed to the receiver that now owns it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedSession {
    pub session_id: SessionId,
    pub lock: SessionLock,
    pub state: Vec<u8>,
}

impl AcceptedSession {
    /// The hold a receiver presents on subsequent commands.
    pub fn hold(&self) -> SessionHold {
        SessionHold::new(self.session_id.clone(), self.lock.token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lock_is_held_up_to_but_not_past_its_deadline() {
        let record = SessionRecord {
            lock: Some(SessionLock {
                token: LockToken::new(1),
                locked_until: Timestamp::from_millis(100),
            }),
            state: Vec::new(),
        };
        assert!(record.live_lock_at(Timestamp::from_millis(99)).is_some());
        // Settlement and expiry agree on the boundary: at the deadline the lock
        // is gone, matching how a message lock is judged.
        assert!(record.live_lock_at(Timestamp::from_millis(100)).is_none());
    }

    #[test]
    fn a_session_that_was_never_locked_holds_nothing() {
        let record = SessionRecord::default();
        assert_eq!(record.live_lock_at(Timestamp::UNIX_EPOCH), None);
        assert_eq!(record.state, Vec::<u8>::new());
    }
}
