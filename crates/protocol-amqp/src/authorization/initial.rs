use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InitialControlState {
    Disabled,
    Initial { deadline: Instant },
    Authorized,
    InitialExpired,
}

#[derive(Debug)]
pub(super) struct InitialControlGrace {
    state: InitialControlState,
    previously_authorized: bool,
}

impl InitialControlGrace {
    pub(super) fn new(previously_authorized: bool) -> Self {
        Self {
            state: InitialControlState::Disabled,
            previously_authorized,
        }
    }

    pub(super) fn enable(&mut self, deadline: Instant, now: Instant) {
        if self.state != InitialControlState::Disabled {
            return;
        }
        self.state = if self.previously_authorized {
            InitialControlState::Authorized
        } else if now >= deadline {
            InitialControlState::InitialExpired
        } else {
            InitialControlState::Initial { deadline }
        };
    }

    pub(super) fn state(&mut self, now: Instant) -> InitialControlState {
        if matches!(self.state, InitialControlState::Initial { deadline } if now >= deadline) {
            self.state = InitialControlState::InitialExpired;
        }
        self.state
    }

    pub(super) fn authorized(&mut self, now: Instant) {
        self.previously_authorized = true;
        if matches!(self.state(now), InitialControlState::Initial { .. }) {
            self.state = InitialControlState::Authorized;
        }
    }
}

#[cfg(test)]
mod tests;
