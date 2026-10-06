use std::sync::Arc;

use amqp::ServerSession;
use tokio::sync::{Semaphore, mpsc};

use super::super::{
    Event, IngressError, IngressMode,
    owner::Owner,
    routing::{self, RouteError},
};
use super::{
    bridge::Open,
    worker_history::{Closed, Loan},
};
use crate::NativeAtomicBroker;

pub(super) enum SessionExit {
    Completed,
    Routing(IngressError),
    WorkerFailed(usize),
    Closed(Closed),
}

pub(super) struct Collector<B: NativeAtomicBroker> {
    pub(super) owner: Owner<B>,
    pub(super) open: Open<B>,
    pub(super) sender: mpsc::Sender<Event>,
    pub(super) links: Arc<Semaphore>,
}

impl<B: NativeAtomicBroker> Collector<B> {
    pub(super) fn prepare_owner(open: &Open<B>) -> Owner<B> {
        Owner::new(open.identity.clone(), open.broker.clone())
    }

    pub(super) fn new(
        open: Open<B>,
        owner: Owner<B>,
        sender: mpsc::Sender<Event>,
        links: Arc<Semaphore>,
    ) -> Self {
        Self {
            owner,
            open,
            sender,
            links,
        }
    }

    pub(super) fn prepare_launch(&self) -> Launch<B> {
        Launch {
            namespace: self.open.namespace.clone(),
            broker: self.open.broker.clone(),
            authorization: self.open.authorization.clone(),
            sender: self.sender.clone(),
            links: Arc::clone(&self.links),
        }
    }
}

pub(super) struct Launch<B: NativeAtomicBroker> {
    namespace: domain::NamespaceName,
    broker: B,
    authorization: Option<Arc<crate::authorization::ConnectionAuthorization>>,
    sender: mpsc::Sender<Event>,
    links: Arc<Semaphore>,
}

impl<B: NativeAtomicBroker> Launch<B> {
    pub(super) fn run(
        self,
        session: ServerSession,
        packet: Loan,
    ) -> impl std::future::Future<Output = SessionExit> + Send + 'static + use<B> {
        run_session(
            session,
            self.namespace,
            self.broker,
            self.authorization,
            self.sender,
            self.links,
            packet,
        )
    }
}

async fn run_session<B: NativeAtomicBroker>(
    mut session: ServerSession,
    namespace: domain::NamespaceName,
    broker: B,
    authorization: Option<Arc<crate::authorization::ConnectionAuthorization>>,
    sender: mpsc::Sender<Event>,
    links: Arc<Semaphore>,
    mut packet: Loan,
) -> SessionExit {
    #[cfg(test)]
    if let Some(error) = packet.session_fault() {
        return SessionExit::Routing(error);
    }
    let result = routing::route_session(
        &mut session,
        &namespace,
        &broker,
        &authorization,
        &sender,
        &links,
        IngressMode::Messaging,
        &mut packet,
    )
    .await;
    let acknowledged = matches!(
        &result,
        Err(RouteError::Closed {
            acknowledged: true,
            ..
        })
    );
    if result.is_err() && !acknowledged {
        routing::stop_connection(&sender).await;
    }
    match result {
        Ok(()) => SessionExit::Completed,
        Err(RouteError::Routing(error)) => SessionExit::Routing(error),
        Err(RouteError::Worker(index)) => SessionExit::WorkerFailed(index),
        Err(RouteError::Closed { reason, .. }) => SessionExit::Closed(reason),
    }
}
