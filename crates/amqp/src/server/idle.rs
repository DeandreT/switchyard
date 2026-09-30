//! Connection activity is shared with a watchdog outside the driver future.

use std::{future::pending, io, time::Duration};

use tokio::{sync::watch, time::Instant};

use super::{DEFAULT_CLOSE_TIMEOUT, EngineError, invalid_state};

const DEFAULT_IDLE_TIMEOUT_MILLIS: u32 = 60_000;
const MIN_IDLE_TIMEOUT_MILLIS: u32 = 1_000;
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Independent receive-idle and transport-write limits for an AMQP connection.
#[derive(Clone, Copy, Debug)]
pub struct ConnectionOptions {
    idle_timeout_millis: u32,
    write_timeout: Duration,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            idle_timeout_millis: DEFAULT_IDLE_TIMEOUT_MILLIS,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
        }
    }
}

impl ConnectionOptions {
    /// Advertises this interval; actual receive silence is allowed for twice it.
    /// Zero disables the local receive-idle direction, not peer keepalives.
    pub fn idle_timeout_millis(mut self, millis: u32) -> Self {
        self.idle_timeout_millis = millis;
        self
    }

    /// Bounds a complete frame write and flush. Zero refuses negotiation.
    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Checks configuration before the caller admits a socket or negotiates.
    pub fn validate(&self) -> Result<(), EngineError> {
        validate_idle_timeout(self.idle_timeout_millis)?;
        if self.write_timeout.is_zero() {
            return Err(EngineError::Timeout("write"));
        }
        if Instant::now().checked_add(self.write_timeout).is_none() {
            return Err(invalid_state(
                "write timeout cannot be represented by the clock",
            ));
        }
        Ok(())
    }

    pub(super) fn advertised_idle_timeout(&self) -> u32 {
        self.idle_timeout_millis
    }

    pub(super) fn write_limit(&self) -> Duration {
        self.write_timeout
    }

    fn receive_timeout(&self) -> Option<Duration> {
        (self.idle_timeout_millis != 0)
            .then(|| Duration::from_millis(u64::from(self.idle_timeout_millis) * 2))
    }
}

pub(super) fn validate_idle_timeout(millis: u32) -> Result<(), EngineError> {
    if millis != 0 && millis < MIN_IDLE_TIMEOUT_MILLIS {
        return Err(invalid_state(
            "positive idle-time-out must be at least 1000 milliseconds",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActivityState {
    received: Instant,
    transmitted: Instant,
    tainted: bool,
    failed: bool,
    receive_expired: bool,
    closing: Option<Instant>,
}

#[derive(Clone)]
pub(super) struct Activity {
    state: watch::Sender<ActivityState>,
    receive_timeout: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ActivityTimeout {
    Receive,
    Peer,
    Close,
    WriteFailed,
}

impl ActivityTimeout {
    pub fn description(self) -> &'static str {
        match self {
            Self::Receive => "no complete AMQP frame received within the idle timeout",
            Self::Peer => "unable to transmit a complete AMQP frame within the peer idle timeout",
            Self::Close => "AMQP Close acknowledgment timed out",
            Self::WriteFailed => "AMQP transport write failed",
        }
    }
}

impl Activity {
    pub fn new() -> Self {
        Self::configured(ConnectionOptions::default().idle_timeout_millis(0))
    }

    pub fn configured(options: ConnectionOptions) -> Self {
        let now = Instant::now();
        let (state, _) = watch::channel(ActivityState {
            received: now,
            transmitted: now,
            tainted: false,
            failed: false,
            receive_expired: false,
            closing: None,
        });
        Self {
            state,
            receive_timeout: options.receive_timeout(),
        }
    }

    pub fn received_frame(&self) -> bool {
        let now = Instant::now();
        let mut accepted = true;
        self.state.send_modify(|state| {
            if state.closing.is_none()
                && (state.receive_expired
                    || self.receive_timeout.is_some_and(|timeout| {
                        state
                            .received
                            .checked_add(timeout)
                            .is_none_or(|deadline| deadline <= now)
                    }))
            {
                state.receive_expired = true;
                accepted = false;
            } else {
                state.received = now;
            }
        });
        accepted
    }

    pub fn begin_write(&self, close: bool) {
        self.state.send_modify(|state| {
            state.tainted = true;
            if close && state.closing.is_none() {
                state.closing = Some(Instant::now());
            }
        });
    }

    pub fn completed_write(&self) {
        self.state.send_modify(|state| {
            state.tainted = false;
            state.transmitted = Instant::now();
        });
    }

    pub fn failed_write(&self) {
        self.state.send_modify(|state| state.failed = true);
    }

    pub fn is_closing(&self) -> bool {
        self.state.borrow().closing.is_some()
    }

    pub fn is_tainted(&self) -> bool {
        self.state.borrow().tainted
    }

    pub fn heartbeat_is_due(&self, peer_idle_millis: u32) -> bool {
        let state = self.state.borrow();
        peer_idle_millis != 0
            && state.closing.is_none()
            && state
                .transmitted
                .checked_add(Duration::from_micros(u64::from(peer_idle_millis) * 500))
                .is_some_and(|deadline| deadline <= Instant::now())
    }

    pub fn write_deadline(
        &self,
        options: ConnectionOptions,
        peer_idle_millis: u32,
        close: bool,
    ) -> io::Result<Instant> {
        let now = Instant::now();
        let local_limit = if close {
            options.write_timeout.min(DEFAULT_CLOSE_TIMEOUT)
        } else {
            options.write_timeout
        };
        let mut deadline = now.checked_add(local_limit).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "write timeout is not representable",
            )
        })?;
        if !close && peer_idle_millis != 0 {
            deadline = deadline.min(
                self.state
                    .borrow()
                    .transmitted
                    .checked_add(Duration::from_millis(u64::from(peer_idle_millis)))
                    .ok_or_else(|| io::Error::other("peer idle deadline is not representable"))?,
            );
        }
        if deadline <= now {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "AMQP write timed out",
            ));
        }
        Ok(deadline)
    }

    pub async fn heartbeat_due(&self, peer_idle_millis: u32) {
        let mut changes = self.state.subscribe();
        loop {
            let state = *changes.borrow_and_update();
            let deadline = if peer_idle_millis == 0 || state.closing.is_some() {
                None
            } else {
                // Microseconds retain the half-millisecond for odd intervals.
                state
                    .transmitted
                    .checked_add(Duration::from_micros(u64::from(peer_idle_millis) * 500))
            };
            if deadline.is_some_and(|deadline| deadline <= Instant::now())
                && *self.state.borrow() == state
            {
                return;
            }
            tokio::select! {
                biased;
                changed = changes.changed() => { if changed.is_err() { return; } }
                () = wait_until(deadline) => {
                    if *self.state.borrow() != state { continue; }
                    if deadline.is_some_and(|deadline| deadline <= Instant::now()) { return; }
                }
            }
        }
    }

    pub async fn timeout(
        &self,
        options: ConnectionOptions,
        peer_idle_millis: u32,
    ) -> ActivityTimeout {
        let mut changes = self.state.subscribe();
        loop {
            let state = *changes.borrow_and_update();
            if state.failed {
                return ActivityTimeout::WriteFailed;
            }
            if state.receive_expired && state.closing.is_none() {
                return ActivityTimeout::Receive;
            }
            let mut due = None;
            if let Some(closing) = state.closing {
                due = closing
                    .checked_add(DEFAULT_CLOSE_TIMEOUT)
                    .map(|deadline| (deadline, ActivityTimeout::Close));
            } else {
                if let Some(timeout) = options.receive_timeout() {
                    due = state
                        .received
                        .checked_add(timeout)
                        .map(|deadline| (deadline, ActivityTimeout::Receive));
                }
                if peer_idle_millis != 0
                    && let Some(deadline) = state
                        .transmitted
                        .checked_add(Duration::from_millis(u64::from(peer_idle_millis)))
                    && due.is_none_or(|(previous, _)| deadline < previous)
                {
                    due = Some((deadline, ActivityTimeout::Peer));
                }
            }
            if let Some((deadline, reason)) = due
                && deadline <= Instant::now()
                && *self.state.borrow() == state
            {
                return reason;
            }
            tokio::select! {
                biased;
                changed = changes.changed() => { if changed.is_err() { return ActivityTimeout::WriteFailed; } }
                () = wait_until(due.map(|(deadline, _)| deadline)) => {
                    if *self.state.borrow() != state { continue; }
                    if let Some((deadline, reason)) = due
                        && deadline <= Instant::now()
                    {
                        return reason;
                    }
                }
            }
        }
    }
}

async fn wait_until(deadline: Option<Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        pending::<()>().await;
    }
}
