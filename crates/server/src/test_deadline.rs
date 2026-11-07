//! The bound on test waits for events that must happen.

use std::time::Duration;

/// Bounds a wait for an event the test requires.
///
/// Reaching it means the code under test hung, so it only needs to be long
/// enough that a loaded host never trips it. Waits asserting that something
/// does not happen keep their own short windows.
pub(crate) const WAIT: Duration = Duration::from_secs(120);
