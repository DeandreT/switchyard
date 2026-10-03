use std::{fmt, thread::JoinHandle};

use tokio::sync::oneshot;

/// Unique join ownership only. The adapter, not this token, closes admission.
pub(crate) struct RetiredOwner<E> {
    retired: oneshot::Receiver<()>,
    thread: JoinHandle<Result<(), E>>,
}

impl<E> RetiredOwner<E> {
    pub(crate) fn new(retired: oneshot::Receiver<()>, thread: JoinHandle<Result<(), E>>) -> Self {
        Self { retired, thread }
    }
}

impl<E: Send + 'static> RetiredOwner<E> {
    /// After the first poll, losing this waiter cannot cancel retirement or the
    /// blocking join. An unpolled token still has only ordinary detach-on-drop.
    pub(crate) async fn join(self) -> Result<(), OwnerJoinError<E>> {
        let supervisor = tokio::spawn(async move {
            let retirement = self.retired.await;
            let result = tokio::task::spawn_blocking(move || self.thread.join())
                .await
                .map_err(|_| OwnerJoinError::TaskFailed)?;
            match result {
                Err(_) => Err(OwnerJoinError::ThreadPanicked),
                Ok(Err(error)) => Err(OwnerJoinError::Owner(error)),
                Ok(Ok(())) => retirement.map_err(|_| OwnerJoinError::RetirementLost),
            }
        });
        supervisor.await.map_err(|_| OwnerJoinError::TaskFailed)?
    }
}

impl<E> fmt::Debug for RetiredOwner<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetiredOwner")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum OwnerJoinError<E> {
    Owner(E),
    ThreadPanicked,
    TaskFailed,
    RetirementLost,
}

impl<E> fmt::Debug for OwnerJoinError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Owner(_) => "Owner(..)",
            Self::ThreadPanicked => "ThreadPanicked",
            Self::TaskFailed => "TaskFailed",
            Self::RetirementLost => "RetirementLost",
        })
    }
}

impl<E> fmt::Display for OwnerJoinError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Owner(_) => "blocking storage owner failed",
            Self::ThreadPanicked => "blocking storage owner panicked",
            Self::TaskFailed => "storage owner join task failed",
            Self::RetirementLost => "storage adapter retirement was not reported",
        })
    }
}

impl<E> std::error::Error for OwnerJoinError<E> {}

#[cfg(test)]
mod tests;
