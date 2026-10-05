use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::listener::atomic_ingress) enum Closed {
    Sealed,
    Budget,
}

struct State {
    limit: usize,
    reserved: usize,
    committed: usize,
    sealed: bool,
}

pub(in crate::listener::atomic_ingress) struct Budget(Mutex<State>);

pub(in crate::listener::atomic_ingress) struct Ticket {
    budget: Arc<Budget>,
    reserved: bool,
}
pub(in crate::listener::atomic_ingress) struct Claim {
    budget: Arc<Budget>,
    reserved: bool,
}

impl Budget {
    pub(in crate::listener::atomic_ingress) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self(Mutex::new(State {
            limit,
            reserved: 0,
            committed: 0,
            sealed: false,
        })))
    }
    pub(in crate::listener::atomic_ingress) fn seal(&self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).sealed = true;
    }
    pub(in crate::listener::atomic_ingress) fn counts(&self) -> (usize, usize, bool) {
        let state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        (state.reserved, state.committed, state.sealed)
    }
    pub(in crate::listener::atomic_ingress) fn reserve(self: &Arc<Self>) -> Result<Ticket, Closed> {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.sealed {
            return Err(Closed::Sealed);
        }
        if state.reserved + state.committed == state.limit {
            return Err(Closed::Budget);
        }
        state.reserved += 1;
        Ok(Ticket {
            budget: Arc::clone(self),
            reserved: true,
        })
    }
}

impl Ticket {
    pub(in crate::listener::atomic_ingress) fn claim(mut self) -> Result<Claim, Closed> {
        {
            let state = self.budget.0.lock().unwrap_or_else(|e| e.into_inner());
            if state.sealed {
                Err(Closed::Sealed)
            } else {
                self.reserved = false;
                Ok(Claim {
                    budget: Arc::clone(&self.budget),
                    reserved: true,
                })
            }
        }
    }
}

impl Claim {
    pub(in crate::listener::atomic_ingress) fn commit(mut self) -> usize {
        {
            let mut state = self.budget.0.lock().unwrap_or_else(|e| e.into_inner());
            let ordinal = state.committed;
            state.reserved -= 1;
            state.committed += 1;
            self.reserved = false;
            ordinal
        }
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        if self.reserved {
            self.budget
                .0
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .reserved -= 1;
        }
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if self.reserved {
            self.budget
                .0
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .reserved -= 1;
        }
    }
}
