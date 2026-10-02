use super::*;
use std::sync::{Mutex, MutexGuard};

pub(in crate::server) struct ConsumerState {
    pub(in crate::server) alive: bool,
    pub(in crate::server) attempt: Option<NativeRetirementAttempt>,
}

pub(in crate::server) struct ConsumerControl(Mutex<ConsumerState>);

impl ConsumerControl {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new(ConsumerState {
            alive: true,
            attempt: None,
        })))
    }

    pub(in crate::server) fn lock(&self) -> MutexGuard<'_, ConsumerState> {
        match self.0.lock() {
            Ok(guard) => guard,
            Err(error) => error.into_inner(),
        }
    }

    pub(in crate::server) fn clear(&self, attempt: &NativeRetirementAttempt) {
        let previous = {
            let mut state = self.lock();
            if state
                .attempt
                .as_ref()
                .is_some_and(|current| current.same_attempt(attempt))
            {
                state.attempt.take()
            } else {
                None
            }
        };
        drop(previous);
    }

    fn close(&self) {
        let attempt = {
            let mut state = self.lock();
            state.alive = false;
            state.attempt.take()
        };
        if let Some(attempt) = attempt {
            attempt.fault(NativeFault::Dropped);
        }
    }
}

pub(in crate::server) struct ConsumerGuard {
    control: Arc<ConsumerControl>,
    armed: bool,
}

impl ConsumerGuard {
    pub(super) fn new(control: Arc<ConsumerControl>) -> Self {
        Self {
            control,
            armed: true,
        }
    }
    pub(super) fn control(&self) -> &Arc<ConsumerControl> {
        &self.control
    }
    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
    pub(super) fn close(&mut self) {
        if self.armed {
            self.control.close();
            self.armed = false;
        }
    }
}

impl Drop for ConsumerGuard {
    fn drop(&mut self) {
        self.close();
    }
}
