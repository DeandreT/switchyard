//! Original settlement-task custody for one receiving link.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use amqp::EngineError;
use domain::LockToken;
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::{
    sync::watch,
    task::{Id, JoinError, JoinHandle},
};

use super::{MAX_IN_FLIGHT_DELIVERIES, SettlementCompletion, SettlementFailure};

#[cfg(test)]
mod tests;

pub(super) struct SettlementJoin {
    pub(super) id: Id,
    pub(super) lock_token: Option<LockToken>,
    pub(super) result: Result<SettlementCompletion, JoinError>,
}

pub(super) struct SettlementJoinFailure {
    pub(super) id: Id,
    pub(super) lock_token: Option<LockToken>,
    pub(super) error: JoinError,
}

struct SettlementTask {
    id: Id,
    lock_token: Option<LockToken>,
    handle: JoinHandle<SettlementCompletion>,
}

impl Future for SettlementTask {
    type Output = SettlementJoin;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let result = match Pin::new(&mut self.handle).poll(context) {
            Poll::Ready(result) => result,
            Poll::Pending => return Poll::Pending,
        };
        Poll::Ready(SettlementJoin {
            id: self.id,
            lock_token: self.lock_token,
            result,
        })
    }
}

pub(super) struct SettlementWorkers {
    pending: FuturesUnordered<SettlementTask>,
    retirement: watch::Sender<bool>,
    finished: Vec<SettlementJoin>,
    failures: Vec<SettlementJoinFailure>,
    finish_started: bool,
    late_adopted: bool,
}

impl SettlementWorkers {
    pub(super) fn new() -> Self {
        let (retirement, _) = watch::channel(false);
        Self {
            pending: FuturesUnordered::new(),
            retirement,
            finished: Vec::new(),
            failures: Vec::new(),
            finish_started: false,
            late_adopted: false,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<bool> {
        self.retirement.subscribe()
    }

    pub(super) fn spawn<F>(&mut self, lock_token: Option<LockToken>, settlement: F) -> Id
    where
        F: Future<Output = Result<(), SettlementFailure>> + Send + 'static,
    {
        assert!(
            !*self.retirement.borrow(),
            "cannot spawn a retired settlement worker"
        );
        self.push(lock_token, settlement)
    }

    /// Adopts the pump's one retained native start after retirement, but before
    /// any finish poll can have joined and cached the original worker set.
    pub(super) fn adopt_retired<F>(&mut self, lock_token: Option<LockToken>, settlement: F) -> Id
    where
        F: Future<Output = Result<(), SettlementFailure>> + Send + 'static,
    {
        assert!(
            *self.retirement.borrow(),
            "late adoption requires settlement retirement"
        );
        assert!(
            !self.finish_started && self.finished.is_empty(),
            "cannot adopt after settlement finish starts"
        );
        assert!(
            !self.late_adopted,
            "cannot adopt more than one late delivery"
        );
        assert!(
            self.len() + self.failures.len() < MAX_IN_FLIGHT_DELIVERIES,
            "too many retained original settlement workers"
        );
        self.late_adopted = true;
        self.push(lock_token, settlement)
    }

    fn push<F>(&mut self, lock_token: Option<LockToken>, settlement: F) -> Id
    where
        F: Future<Output = Result<(), SettlementFailure>> + Send + 'static,
    {
        assert!(
            self.len() + self.failures.len() < MAX_IN_FLIGHT_DELIVERIES,
            "too many outstanding settlement workers"
        );
        let handle = tokio::spawn(async move {
            let result = settlement.await;
            SettlementCompletion { lock_token, result }
        });
        let id = handle.id();
        self.pending.push(SettlementTask {
            id,
            lock_token,
            handle,
        });
        id
    }

    pub(super) async fn next(&mut self) -> Option<SettlementCompletion> {
        let joined = self.pending.next().await?;
        match joined.result {
            Ok(completion) => Some(completion),
            Err(error) => {
                self.failures.push(SettlementJoinFailure {
                    id: joined.id,
                    lock_token: joined.lock_token,
                    error,
                });
                self.retire();
                // Keep the original token registered until residual cleanup:
                // a panicked task may not have unregistered its delivery.
                Some(SettlementCompletion {
                    lock_token: None,
                    result: Err(SettlementFailure::Engine(EngineError::Stopped)),
                })
            }
        }
    }

    pub(super) fn retire(&self) {
        self.retirement.send_replace(true);
    }

    /// Retains each original result before the next await, including when this
    /// borrowed finish waiter is dropped and a later waiter retries.
    pub(super) async fn finish(&mut self) -> &[SettlementJoin] {
        self.finish_started = true;
        self.retire();
        while let Some(joined) = self.pending.next().await {
            self.finished.push(joined);
        }
        &self.finished
    }

    pub(super) fn finished(&self) -> &[SettlementJoin] {
        &self.finished
    }

    pub(super) fn failures(&self) -> &[SettlementJoinFailure] {
        &self.failures
    }

    /// Consumes retained evidence only after all originals have been joined.
    pub(super) fn into_join_error(mut self) -> Option<JoinError> {
        assert!(
            self.is_empty(),
            "settlement workers must finish before reporting"
        );
        if let Some(failure) = std::mem::take(&mut self.failures).into_iter().next() {
            return Some(failure.error);
        }
        std::mem::take(&mut self.finished)
            .into_iter()
            .find_map(|joined| joined.result.err())
    }
}

impl Drop for SettlementWorkers {
    fn drop(&mut self) {
        // Dropping original JoinHandles detaches rather than aborts. Ancestor
        // custody supplies the normal join; Drop only requests retirement.
        self.retire();
    }
}
