use std::{
    convert::Infallible,
    future::Future,
    task::{Context, Poll},
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

use super::super::{Event, IngressError};

pub(in super::super) enum RouteError<E, C> {
    Routing(IngressError),
    Worker(E),
    Closed { reason: C, acknowledged: bool },
}

impl<E, C> RouteError<E, C> {
    pub(super) fn closed(reason: C) -> Self {
        Self::Closed {
            reason,
            acknowledged: false,
        }
    }
}

impl<E, C> From<IngressError> for RouteError<E, C> {
    fn from(error: IngressError) -> Self {
        Self::Routing(error)
    }
}

impl<E, C> From<amqp::EngineError> for RouteError<E, C> {
    fn from(error: amqp::EngineError) -> Self {
        Self::Routing(error.into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Branch {
    Controller,
    CbsRequests,
    CbsReplies,
    Consumer,
    Producer,
}

pub(in super::super) trait WorkerTasks {
    type Ticket;
    type Failure;
    type Closed;
    fn is_empty(&self) -> bool;
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<(), Self::Failure>>>;
    fn reserve(&mut self) -> Result<Self::Ticket, Self::Closed>;
    fn begin_peer_end_drain(&mut self) {}
    fn launch<F>(
        &mut self,
        ticket: Self::Ticket,
        future: F,
        branch: Branch,
    ) -> Result<(), (Self::Closed, F)>
    where
        F: Future<Output = Result<(), IngressError>> + Send + 'static;

    #[cfg(test)]
    fn controls(
        &self,
    ) -> Option<std::sync::Arc<super::super::retained_session::controls::Controls>> {
        None
    }
}

pub(super) struct DefaultWorkers(JoinSet<Result<(), IngressError>>);

impl DefaultWorkers {
    pub(super) fn new() -> Self {
        Self(JoinSet::new())
    }
}

impl WorkerTasks for DefaultWorkers {
    type Ticket = ();
    type Failure = IngressError;
    type Closed = Infallible;
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<(), IngressError>>> {
        self.0.poll_join_next(cx).map(|row| {
            row.map(|result| match result {
                Ok(result) => result,
                Err(error) => Err(error.into()),
            })
        })
    }
    fn reserve(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
    fn launch<F>(&mut self, (): (), future: F, _branch: Branch) -> Result<(), (Infallible, F)>
    where
        F: Future<Output = Result<(), IngressError>> + Send + 'static,
    {
        self.0.spawn(future);
        Ok(())
    }
}

pub(in super::super) async fn stop_connection(events: &mpsc::Sender<Event>) {
    let (reply, stopped) = oneshot::channel();
    if events.send(Event::StopConnection { reply }).await.is_ok() {
        let _ = stopped.await;
    }
}

pub(super) async fn accepted<W: WorkerTasks, F: Future>(workers: &W, future: F) -> F::Output {
    #[cfg(test)]
    if let Some(controls) = workers.controls() {
        return controls.accept(future).await;
    }
    #[cfg(not(test))]
    let _ = workers;
    future.await
}

pub(super) async fn launch<W: WorkerTasks, F>(
    workers: &mut W,
    ticket: W::Ticket,
    future: F,
    events: &mpsc::Sender<Event>,
    branch: Branch,
) -> Result<(), RouteError<W::Failure, W::Closed>>
where
    F: Future<Output = Result<(), IngressError>> + Send + 'static,
{
    #[cfg(test)]
    if let Some(controls) = workers.controls() {
        controls.before_claim(branch).await;
    }
    match workers.launch(ticket, future, branch) {
        Ok(()) => Ok(()),
        Err((reason, original)) => {
            // The rejected original endpoint/future survives logical close + ack.
            stop_connection(events).await;
            drop(original);
            #[cfg(test)]
            if let Some(controls) = workers.controls() {
                controls.refused_disposed();
            }
            Err(RouteError::Closed {
                reason,
                acknowledged: true,
            })
        }
    }
}
