use std::fmt;

use tokio::sync::watch;

use super::{Error, wait_completed};

/// Completion of the owned lifecycle cleanup, with no close or engine handle.
#[derive(Clone)]
pub(in crate::experimental_runtime) struct NodeRetirement {
    completed: watch::Receiver<Option<Result<(), Error>>>,
}

impl NodeRetirement {
    pub(super) fn new(completed: watch::Receiver<Option<Result<(), Error>>>) -> Self {
        Self { completed }
    }

    pub(in crate::experimental_runtime) async fn join(mut self) -> Result<(), Error> {
        wait_completed(&mut self.completed).await
    }
}

impl fmt::Debug for NodeRetirement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeRetirement")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
