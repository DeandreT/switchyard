use std::sync::{Arc, Mutex};

use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
};

use super::locked;

pub(super) enum State {
    Dormant,
    Claimed,
    Pending(JoinHandle<()>),
    Leased,
    Joined(Result<(), JoinError>),
    Absent,
    Reported,
}

pub(super) struct Role {
    pub(super) state: Mutex<State>,
    changed: watch::Sender<()>,
}

impl Role {
    pub(super) fn new() -> Self {
        let (changed, _) = watch::channel(());
        Self {
            state: Mutex::new(State::Dormant),
            changed,
        }
    }

    pub(super) fn pulse(&self) {
        self.changed.send_replace(());
    }

    pub(super) async fn join(self: &Arc<Self>, abort: bool, finalize_absent: bool) {
        let mut changed = self.changed.subscribe();
        loop {
            let lease = {
                let mut state = locked(&self.state);
                match &*state {
                    State::Pending(_) => {
                        let State::Pending(handle) = std::mem::replace(&mut *state, State::Leased)
                        else {
                            unreachable!("checked pending role")
                        };
                        Some(Lease {
                            role: self.clone(),
                            handle: Some(handle),
                            outcome: None,
                        })
                    }
                    State::Dormant if finalize_absent => {
                        *state = State::Absent;
                        return;
                    }
                    State::Joined(_) | State::Absent | State::Reported => return,
                    State::Dormant | State::Claimed | State::Leased => None,
                }
            };
            if let Some(mut lease) = lease {
                if abort {
                    lease.handle.as_ref().expect("leased token").abort();
                }
                lease.outcome = Some(lease.handle.as_mut().expect("leased token").await);
                drop(lease);
                return;
            }
            let _ = changed.changed().await;
        }
    }

    pub(super) fn take_result(&self) -> Option<Result<(), JoinError>> {
        let state = {
            let mut state = locked(&self.state);
            assert!(matches!(*state, State::Joined(_) | State::Absent));
            std::mem::replace(&mut *state, State::Reported)
        };
        match state {
            State::Joined(result) => Some(result),
            State::Absent => None,
            _ => unreachable!("all actual barriers before report"),
        }
    }
}

pub(super) struct Installation {
    role: Arc<Role>,
    pub(super) handle: Option<JoinHandle<()>>,
}

impl Installation {
    pub(super) fn new(role: Arc<Role>) -> Self {
        Self { role, handle: None }
    }
}

impl Drop for Installation {
    fn drop(&mut self) {
        let next = self.handle.take().map_or(State::Absent, State::Pending);
        {
            let mut state = locked(&self.role.state);
            *state = next;
        }
        self.role.pulse();
    }
}

struct Lease {
    role: Arc<Role>,
    handle: Option<JoinHandle<()>>,
    outcome: Option<Result<(), JoinError>>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let next = match self.outcome.take() {
            Some(outcome) => State::Joined(outcome),
            None => State::Pending(self.handle.take().expect("unfinished leased token")),
        };
        {
            let mut state = locked(&self.role.state);
            *state = next;
        }
        self.role.pulse();
        // The remaining token is dropped only after its actual Ready was stored.
    }
}
