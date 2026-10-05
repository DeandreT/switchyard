use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Closed {
    Sealed,
    Budget,
}

struct State {
    limit: usize,
    reserved: usize,
    committed: usize,
    sealed: bool,
}

pub(super) struct Budget(Mutex<State>);

pub(super) struct Ticket {
    budget: Arc<Budget>,
    reserved: bool,
}
pub(super) struct Claim {
    budget: Arc<Budget>,
    ordinal: usize,
    reserved: bool,
}

impl Budget {
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self(Mutex::new(State {
            limit,
            reserved: 0,
            committed: 0,
            sealed: false,
        })))
    }
    pub(super) fn seal(&self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).sealed = true;
    }
    pub(super) fn counts(&self) -> (usize, usize, bool) {
        let state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        (state.reserved, state.committed, state.sealed)
    }
    pub(super) fn reserve(self: &Arc<Self>) -> Result<Ticket, Closed> {
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
    pub(super) fn claim(mut self) -> Result<Claim, Closed> {
        {
            let state = self.budget.0.lock().unwrap_or_else(|e| e.into_inner());
            if state.sealed {
                Err(Closed::Sealed)
            } else {
                let ordinal = state.committed;
                self.reserved = false;
                Ok(Claim {
                    budget: Arc::clone(&self.budget),
                    ordinal,
                    reserved: true,
                })
            }
        }
    }
}

impl Claim {
    pub(super) fn commit(mut self) -> usize {
        {
            let mut state = self.budget.0.lock().unwrap_or_else(|e| e.into_inner());
            state.reserved -= 1;
            state.committed += 1;
            self.reserved = false;
        }
        self.ordinal
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
