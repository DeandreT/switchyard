use std::sync::{Arc, Mutex};

use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
};

use super::locked;

enum State {
    Dormant,
    Claimed,
    Pending(JoinHandle<()>),
    Leased,
    Joined(Result<(), JoinError>),
    Absent,
    Reported,
}

struct Custody {
    sealed: bool,
    state: State,
}

pub(super) struct Wrapper {
    custody: Mutex<Custody>,
    changed: watch::Sender<()>,
}

impl Wrapper {
    pub(super) fn new() -> Self {
        let (changed, _) = watch::channel(());
        Self {
            custody: Mutex::new(Custody {
                sealed: false,
                state: State::Dormant,
            }),
            changed,
        }
    }

    pub(super) fn seal(&self) {
        locked(&self.custody).sealed = true;
        self.pulse();
    }

    pub(super) fn claim(&self) -> bool {
        let mut custody = locked(&self.custody);
        if custody.sealed || !matches!(custody.state, State::Dormant) {
            false
        } else {
            custody.state = State::Claimed;
            true
        }
    }

    fn pulse(&self) {
        self.changed.send_replace(());
    }

    pub(super) async fn join(self: &Arc<Self>) {
        let mut changed = self.changed.subscribe();
        loop {
            let lease = {
                let mut custody = locked(&self.custody);
                match &custody.state {
                    State::Pending(_) => {
                        let State::Pending(handle) =
                            std::mem::replace(&mut custody.state, State::Leased)
                        else {
                            unreachable!("checked pending wrapper")
                        };
                        Some(Lease {
                            wrapper: self.clone(),
                            handle: Some(handle),
                            result: None,
                        })
                    }
                    State::Dormant => {
                        custody.state = State::Absent;
                        return;
                    }
                    State::Joined(_) | State::Absent | State::Reported => return,
                    State::Claimed | State::Leased => None,
                }
            };
            if let Some(mut lease) = lease {
                lease.result = Some(lease.handle.as_mut().expect("original wrapper token").await);
                drop(lease);
                return;
            }
            let _ = changed.changed().await;
        }
    }

    pub(super) fn take_result(&self) -> Option<Result<(), JoinError>> {
        let state = {
            let mut custody = locked(&self.custody);
            assert!(matches!(custody.state, State::Joined(_) | State::Absent));
            std::mem::replace(&mut custody.state, State::Reported)
        };
        match state {
            State::Joined(result) => Some(result),
            State::Absent => None,
            _ => unreachable!("actual wrapper barrier before report"),
        }
    }

    #[cfg(test)]
    pub(super) fn abort_handle(&self) -> Option<tokio::task::AbortHandle> {
        let custody = locked(&self.custody);
        match &custody.state {
            State::Pending(handle) => Some(handle.abort_handle()),
            _ => None,
        }
    }
}

pub(super) struct Installation {
    wrapper: Arc<Wrapper>,
    pub(super) handle: Option<JoinHandle<()>>,
}

impl Installation {
    pub(super) fn new(wrapper: Arc<Wrapper>) -> Self {
        Self {
            wrapper,
            handle: None,
        }
    }

    pub(super) fn pulse(&self) {
        self.wrapper.pulse();
    }
}

impl Drop for Installation {
    fn drop(&mut self) {
        let next = self.handle.take().map_or(State::Absent, State::Pending);
        {
            let mut custody = locked(&self.wrapper.custody);
            custody.state = next;
        }
        self.wrapper.pulse();
    }
}

struct Lease {
    wrapper: Arc<Wrapper>,
    handle: Option<JoinHandle<()>>,
    result: Option<Result<(), JoinError>>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let next = match self.result.take() {
            Some(result) => State::Joined(result),
            None => State::Pending(self.handle.take().expect("unfinished original wrapper")),
        };
        {
            let mut custody = locked(&self.wrapper.custody);
            custody.state = next;
        }
        self.wrapper.pulse();
    }
}
