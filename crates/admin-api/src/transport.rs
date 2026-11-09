//! Original-task custody shared by native administration and Atom HTTPS.
//! Completion is not join: a retained record owns its permit until the original
//! Tokio task's ID is observed by a real join, including panic exits.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
    task::{Context, Poll, ready},
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit},
    task::{Id, JoinError, JoinSet},
};

#[derive(Default)]
struct Originals {
    tasks: JoinSet<()>,
    permits: HashMap<Id, OwnedSemaphorePermit>,
}

impl Originals {
    fn poll_join_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<(), JoinError>>> {
        let Some(result) = ready!(self.tasks.poll_join_next_with_id(cx)) else {
            assert!(
                self.permits.is_empty(),
                "no permit outlives its joined original"
            );
            return Poll::Ready(None);
        };
        let id = match &result {
            Ok((id, ())) => *id,
            Err(error) => error.id(),
        };
        drop(self.permits.remove(&id));
        Poll::Ready(Some(result.map(|(_, ())| ())))
    }
}

#[derive(Default)]
struct Family {
    originals: Mutex<Originals>,
    changed: Notify,
}

/// Cloneable producer handles share ONE retained family. Only one borrower
/// observes joins at a time; cancelled borrowers may retry the same originals.
#[derive(Clone, Default)]
pub struct TaskFamily(Arc<Family>);

impl<F> hyper::rt::Executor<F> for TaskFamily
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, future: F) {
        self.spawn(future);
    }
}

impl TaskFamily {
    pub fn spawn(&self, work: impl Future<Output = ()> + Send + 'static) {
        self.spawn_retained(work, None);
    }

    pub fn spawn_retained(
        &self,
        work: impl Future<Output = ()> + Send + 'static,
        permit: Option<OwnedSemaphorePermit>,
    ) {
        {
            let mut originals = self.0.originals.lock().expect("administration task family");
            let id = originals.tasks.spawn(work).id();
            if let Some(permit) = permit {
                assert!(
                    originals.permits.insert(id, permit).is_none(),
                    "one permit per original"
                );
            }
        }
        self.0.changed.notify_one();
    }

    pub async fn join_next(&self) -> Option<Result<(), JoinError>> {
        std::future::poll_fn(|cx| {
            self.0
                .originals
                .lock()
                .expect("administration task family")
                .poll_join_next(cx)
        })
        .await
    }

    pub async fn next_live(&self) -> Result<(), JoinError> {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(result) = self.join_next().await {
                return result;
            }
            changed.await;
        }
    }

    pub fn is_empty(&self) -> bool {
        let originals = self.0.originals.lock().expect("administration task family");
        originals.tasks.is_empty() && originals.permits.is_empty()
    }

    /// Retained originals, including completed-but-unjoined tasks.
    pub fn len(&self) -> usize {
        self.0
            .originals
            .lock()
            .expect("administration task family")
            .tasks
            .len()
    }

    pub fn permit_count(&self) -> usize {
        self.0
            .originals
            .lock()
            .expect("administration task family")
            .permits
            .len()
    }
}
