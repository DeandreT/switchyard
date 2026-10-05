use std::sync::{Arc, Mutex};

#[cfg(test)]
use super::Controls;
use super::{
    RetainedConnectionOutcome, RetainedConnectionOutcomes, RetainedConnectionResult, locked,
};

/// Data only: no root, runtime, task slot/token, transport or anchor.
pub(super) struct Outcomes {
    values: Mutex<RetainedConnectionOutcomes>,
}

impl Outcomes {
    pub(super) fn new() -> Self {
        Self {
            values: Mutex::new(RetainedConnectionOutcomes {
                primary: None,
                websocket_close: None,
            }),
        }
    }

    pub(super) fn publisher(self: &Arc<Self>, #[cfg(test)] controls: Arc<Controls>) -> Publisher {
        Publisher {
            primary: PrimaryPublisher(self.clone()),
            close: ClosePublisher(self.clone()),
            #[cfg(test)]
            controls,
        }
    }

    pub(super) fn take(&self) -> RetainedConnectionOutcomes {
        let mut values = locked(&self.values);
        RetainedConnectionOutcomes {
            primary: values.primary.take(),
            websocket_close: values.websocket_close.take(),
        }
    }

    #[cfg(test)]
    pub(super) fn published(&self) -> (bool, bool) {
        let values = locked(&self.values);
        (values.primary.is_some(), values.websocket_close.is_some())
    }
}

/// Unique data capabilities only; no reference to the wrapper's own token.
pub(in crate::listener) struct Publisher {
    pub(in crate::listener) primary: PrimaryPublisher,
    pub(in crate::listener) close: ClosePublisher,
    #[cfg(test)]
    pub(in crate::listener) controls: Arc<Controls>,
}

pub(in crate::listener) struct PrimaryPublisher(Arc<Outcomes>);
pub(in crate::listener) struct ClosePublisher(Arc<Outcomes>);

impl PrimaryPublisher {
    pub(in crate::listener) fn publish(self, value: RetainedConnectionOutcome) {
        // Consuming one factory-created capability makes this cell single-write.
        // There is no await, callback, allocation or duplicate assertion.
        locked(&self.0.values).primary = Some(value);
    }
}

impl ClosePublisher {
    pub(in crate::listener) fn publish(self, value: RetainedConnectionResult) {
        locked(&self.0.values).websocket_close = Some(value);
    }
}
