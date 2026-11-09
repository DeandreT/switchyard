//! Opt-in TLS/h2 entity administration. Original connection, h2 stream and RPC
//! tasks remain owned until joined; cancellation of a client does not cancel an
//! accepted broker operation. No authorization decisions are cached here.

use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use admin_api::{
    rpc::{BackendErrorKind, EntityRpc},
    transport::TaskFamily as Tasks,
    wire,
};
use auth::{JwtPolicy, ResourceScope};
use domain::{BrokerError, NamespaceName};
use hyper::server::conn::http2;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use rustls::ServerConfig;
#[cfg(test)]
use tokio::sync::{Notify, OwnedSemaphorePermit};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::{JoinError, JoinSet},
};
use tokio_rustls::TlsAcceptor;
use tonic::{Request, Response, Status};
use tower::ServiceExt;

use crate::{AdminService, BrokerHandle, ProposeError, SubmitError};

// Match the broker's bounded command channel. This bounds retained RPC originals,
// from admission through actual join, across the ENTIRE listener.
const DEFAULT_MAX_IN_FLIGHT_RPCS: usize = 1_024;

/// Prepared before the durable backend is opened. The only supported ALPN is h2.
pub struct AdminTlsConfig {
    tls: Arc<ServerConfig>,
    policy: JwtPolicy,
    root: ResourceScope,
    max_in_flight_rpcs: usize,
}

impl AdminTlsConfig {
    pub fn new(
        mut tls: ServerConfig,
        policy: JwtPolicy,
        root: ResourceScope,
    ) -> Result<Self, &'static str> {
        if root.path().next().is_some() {
            return Err("native administration requires a namespace-root resource scope");
        }
        tls.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Self {
            tls: Arc::new(tls),
            policy,
            root,
            max_in_flight_rpcs: DEFAULT_MAX_IN_FLIGHT_RPCS,
        })
    }

    /// Bounds accepted RPCs across all connections, not per HTTP/2 peer. This
    /// prepared configuration owns no live tasks; invalid limits fail safely.
    pub fn with_max_in_flight_rpcs(mut self, limit: usize) -> Result<Self, &'static str> {
        if !(1..=Semaphore::MAX_PERMITS).contains(&limit) {
            return Err("native administration RPC limit is outside the supported range");
        }
        self.max_in_flight_rpcs = limit;
        Ok(self)
    }
}

struct Admission {
    retired: bool,
    permits: Arc<Semaphore>,
    // A completed JoinHandle is still retained until explicitly joined.
    calls: Tasks,
    #[cfg(test)]
    admitted: Arc<Notify>,
}

#[derive(Clone)]
pub struct AdminRetirement {
    admission: Arc<Mutex<Admission>>,
    signal: watch::Sender<bool>,
}

impl AdminRetirement {
    /// Linearizes retirement against RPC acceptance, including existing h2 peers.
    pub fn request(&self) {
        self.admission.lock().expect("admin admission lock").retired = true;
        self.signal.send_replace(true);
    }

    async fn requested(&self) {
        requested(self.signal.subscribe()).await;
    }
}

async fn requested(mut signal: watch::Receiver<bool>) {
    if *signal.borrow_and_update() {
        return;
    }
    let _ = signal.changed().await;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReapKind {
    Rpc,
    Stream,
    Connection,
}

impl ReapKind {
    fn next(self) -> Self {
        match self {
            Self::Rpc => Self::Stream,
            Self::Stream => Self::Connection,
            Self::Connection => Self::Rpc,
        }
    }

    fn failure(self) -> &'static str {
        match self {
            Self::Rpc => "admin RPC task failed",
            Self::Stream => "admin stream task failed",
            Self::Connection => "admin connection task failed",
        }
    }
}

/// Rotating priority gives every continuously ready family a turn within three
/// joins. An always-ready stream family cannot starve completed RPC originals.
async fn reap_tasks(
    turn: ReapKind,
    calls: &Tasks,
    streams: &Tasks,
    connections: &mut JoinSet<()>,
) -> (ReapKind, Result<(), JoinError>) {
    let call = calls.next_live();
    let stream = streams.next_live();
    let connection = async {
        match connections.join_next().await {
            Some(joined) => joined,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(call, stream, connection);
    let mut futures: [Pin<&mut (dyn Future<Output = Result<(), JoinError>> + Send)>; 3] =
        [call.as_mut(), stream.as_mut(), connection.as_mut()];
    std::future::poll_fn(|cx| {
        for offset in 0..3 {
            let index = (turn as usize + offset) % 3;
            if let Poll::Ready(result) = futures[index].as_mut().poll(cx) {
                return Poll::Ready((
                    [ReapKind::Rpc, ReapKind::Stream, ReapKind::Connection][index],
                    result,
                ));
            }
        }
        Poll::Pending
    })
    .await
}

type Rpc = EntityRpc<BrokerHandle, fn(&SubmitError) -> BackendErrorKind>;

#[derive(Clone)]
struct OwnedRpc {
    rpc: Arc<Rpc>,
    retirement: AdminRetirement,
}

impl OwnedRpc {
    async fn call<T: Send + 'static>(
        &self,
        work: impl Future<Output = Result<T, Status>> + Send + 'static,
    ) -> Result<T, Status> {
        let (send, receive) = tokio::sync::oneshot::channel();
        {
            let admission = self
                .retirement
                .admission
                .lock()
                .expect("admin admission lock");
            if admission.retired {
                return Err(Status::unavailable("administration is retiring"));
            }
            // No waiting task is spawned for an excess request. The same lock
            // linearizes the retiring gate, global capacity and original spawn.
            let permit = admission.permits.clone().try_acquire_owned().map_err(|_| {
                Status::resource_exhausted("native administration RPC capacity reached")
            })?;
            admission.calls.spawn_retained(
                async move {
                    // This original task survives stream cancellation and retains
                    // the backend response until it can answer or observe disconnect.
                    let result = work.await;
                    let _ = send.send(result);
                },
                Some(permit),
            );
            #[cfg(test)]
            admission.admitted.notify_one();
        }
        receive
            .await
            .map_err(|_| Status::internal("entity operation failed"))?
    }
}

#[tonic::async_trait]
impl wire::entity_service_server::EntityService for OwnedRpc {
    async fn create_entity(
        &self,
        request: Request<wire::CreateEntityRequest>,
    ) -> Result<Response<wire::Entity>, Status> {
        let rpc = self.rpc.clone();
        self.call(async move { rpc.create_entity(request).await })
            .await
    }
    async fn get_entity(
        &self,
        request: Request<wire::GetEntityRequest>,
    ) -> Result<Response<wire::Entity>, Status> {
        let rpc = self.rpc.clone();
        self.call(async move { rpc.get_entity(request).await })
            .await
    }
    async fn list_entities(
        &self,
        request: Request<wire::ListEntitiesRequest>,
    ) -> Result<Response<wire::ListEntitiesResponse>, Status> {
        let rpc = self.rpc.clone();
        self.call(async move { rpc.list_entities(request).await })
            .await
    }
    async fn delete_entity(
        &self,
        request: Request<wire::DeleteEntityRequest>,
    ) -> Result<Response<wire::Operation>, Status> {
        let rpc = self.rpc.clone();
        self.call(async move { rpc.delete_entity(request).await })
            .await
    }
}

fn classify(error: &SubmitError) -> BackendErrorKind {
    match error {
        SubmitError::BrokerStopped => BackendErrorKind::Unavailable,
        SubmitError::Propose(ProposeError::ClockWentBackward { .. }) => {
            BackendErrorKind::FailedPrecondition
        }
        SubmitError::Propose(ProposeError::Broker(error)) => match error {
            BrokerError::QueueAlreadyExists
            | BrokerError::TopicAlreadyExists
            | BrokerError::SubscriptionAlreadyExists
            | BrokerError::EntityAlreadyExists => BackendErrorKind::AlreadyExists,
            BrokerError::EntityNotFound
            | BrokerError::QueueNotFound
            | BrokerError::TopicNotFound
            | BrokerError::SubscriptionNotFound => BackendErrorKind::NotFound,
            BrokerError::QueueConfig(_)
            | BrokerError::TopicConfig(_)
            | BrokerError::SubscriptionConfig(_)
            | BrokerError::EntityPathReserved
            | BrokerError::DeadLetterQueueIsReserved => BackendErrorKind::InvalidArgument,
            BrokerError::SubscriptionLimitExceeded { .. } => BackendErrorKind::FailedPrecondition,
            _ => BackendErrorKind::Internal,
        },
        _ => BackendErrorKind::Internal,
    }
}

pub struct AdminListenerService {
    listener: Option<TcpListener>,
    config: AdminTlsConfig,
    service: OwnedRpc,
    retirement: AdminRetirement,
    // Separate from admission: even a local transport failure cannot begin
    // protocol cleanup before the coordinator retires BOTH admissions.
    cleanup: watch::Sender<bool>,
    connections: JoinSet<()>,
    streams: Tasks,
    calls: Tasks,
    reap_turn: ReapKind,
    admission_timeout: Duration,
    failure: Option<io::Error>,
    finished: bool,
}

impl AdminListenerService {
    pub fn new(
        listener: TcpListener,
        config: AdminTlsConfig,
        broker: BrokerHandle,
        namespace: NamespaceName,
        admission_timeout: Duration,
    ) -> Self {
        let (signal, _) = watch::channel(false);
        let (cleanup, _) = watch::channel(false);
        let retirement = AdminRetirement {
            admission: Arc::new(Mutex::new(Admission {
                retired: false,
                permits: Arc::new(Semaphore::new(config.max_in_flight_rpcs)),
                calls: Tasks::default(),
                #[cfg(test)]
                admitted: Arc::default(),
            })),
            signal,
        };
        let calls = retirement
            .admission
            .lock()
            .expect("admin admission lock")
            .calls
            .clone();
        let rpc = EntityRpc::new(
            AdminService::new(namespace, broker),
            config.policy.clone(),
            config.root.clone(),
            classify as fn(&SubmitError) -> BackendErrorKind,
        )
        .expect("prepared namespace root");
        Self {
            listener: Some(listener),
            config,
            service: OwnedRpc {
                rpc: Arc::new(rpc),
                retirement: retirement.clone(),
            },
            retirement,
            cleanup,
            connections: JoinSet::new(),
            streams: Tasks::default(),
            calls,
            reap_turn: ReapKind::Rpc,
            admission_timeout,
            failure: None,
            finished: false,
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener
            .as_ref()
            .ok_or_else(|| io::Error::other("admin listener retired"))?
            .local_addr()
    }

    pub fn retirement_handle(&self) -> AdminRetirement {
        self.retirement.clone()
    }

    /// Cancellation-safe accept pump: all admitted sockets belong to `self`.
    pub async fn serve(&mut self) {
        let Some(listener) = self.listener.as_ref() else {
            return;
        };
        loop {
            tokio::select! {
                biased;
                () = self.retirement.requested() => break,
                // Reap before accepts, fairly across all original families.
                (kind, joined) = reap_tasks(self.reap_turn, &self.calls, &self.streams, &mut self.connections) => {
                    self.reap_turn = kind.next();
                    if joined.is_err() { self.failure = Some(io::Error::other(kind.failure())); self.retirement.request(); break; }
                },
                accepted = listener.accept() => {
                    let (socket, _) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => { self.failure = Some(error); self.retirement.request(); break; }
                    };
                    let acceptor = TlsAcceptor::from(self.config.tls.clone());
                    let cleanup = self.cleanup.subscribe();
                    let streams = self.streams.clone();
                    let service = wire::entity_service_server::EntityServiceServer::new(self.service.clone());
                    let accepted_at = tokio::time::Instant::now();
                    let deadline = accepted_at.checked_add(self.admission_timeout).unwrap_or_else(|| accepted_at + Duration::from_secs(60 * 60 * 24 * 365));
                    self.connections.spawn(async move {
                        let tls = tokio::select! {
                            biased;
                            () = requested(cleanup.clone()) => return,
                            result = tokio::time::timeout_at(deadline, acceptor.accept(socket)) => match result {
                                Ok(Ok(tls)) => tls,
                                _ => return,
                            },
                        };
                        if tls.get_ref().1.alpn_protocol() != Some(b"h2") { return; }
                        let service = service.map_request(|request: hyper::Request<hyper::body::Incoming>| request.map(tonic::body::Body::new));
                        let mut builder = http2::Builder::new(streams);
                        builder.timer(TokioTimer::new());
                        let connection = builder.serve_connection(TokioIo::new(tls), TowerToHyperService::new(service));
                        tokio::pin!(connection);
                        tokio::select! {
                            biased;
                            () = requested(cleanup) => {
                                connection.as_mut().graceful_shutdown();
                                let _ = connection.await;
                            },
                            _ = &mut connection => {},
                        }
                    });
                },
            }
        }
        // Close TCP admission before either transport's cleanup stage starts.
        self.listener.take();
    }

    pub async fn finish(&mut self) {
        self.retirement.request();
        self.serve().await;
        self.cleanup.send_replace(true);
        while let Some(joined) = self.connections.join_next().await {
            if joined.is_err() {
                self.failure
                    .get_or_insert_with(|| io::Error::other("admin connection task failed"));
            }
        }
        // Connections can spawn streams, streams can spawn calls. Join in that
        // order, never abort or substitute a cancellation receipt for completion.
        while let Some(stream) = self.streams.join_next().await {
            if stream.is_err() {
                self.failure
                    .get_or_insert_with(|| io::Error::other("admin stream task failed"));
            }
        }
        while let Some(call) = self.calls.join_next().await {
            if call.is_err() {
                self.failure
                    .get_or_insert_with(|| io::Error::other("admin RPC task failed"));
            }
        }
        self.finished = true;
    }

    /// Extracts a terminal failure only after every original task was joined.
    pub fn take_failure(&mut self) -> Option<io::Error> {
        assert!(
            self.finished && self.listener.is_none() && self.connections.is_empty(),
            "admin listener drains before extraction"
        );
        assert!(
            self.streams.is_empty() && self.calls.is_empty(),
            "admin stream and RPC originals drain before extraction"
        );
        self.failure.take()
    }
}

#[cfg(test)]
#[path = "admin_listener_tests.rs"]
mod tests;
