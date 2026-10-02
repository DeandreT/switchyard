use std::{future::Future, sync::Arc};

use amqp::ServerConnection;
use domain::NamespaceName;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::Semaphore,
};
use tracing::{debug, warn};

use super::{AmqpListener, atomic_ingress, serve_open_connection, websocket};
use crate::{
    Broker, NativeAtomicBroker, SharedAccessAuthentication,
    authorization::{ConnectionAuthorization, SharedAccessSaslAcceptor},
};

impl<B: Broker> AmqpListener<B> {
    /// Accepts connections until the listener fails.
    ///
    /// A connection that fails takes only itself down: one client's protocol
    /// error is not the node's.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        self.serve_with_driver(listener, OrdinaryDriver).await
    }

    async fn serve_with_driver<D: ConnectionDriver<B>>(
        self,
        listener: TcpListener,
        driver: D,
    ) -> std::io::Result<()> {
        self.connection_options
            .validate()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
        let admission = Arc::new(Semaphore::new(
            self.max_connections.get().min(Semaphore::MAX_PERMITS),
        ));
        loop {
            let (stream, peer) = listener.accept().await?;
            let Ok(permit) = Arc::clone(&admission).try_acquire_owned() else {
                debug!(%peer, "connection refused: admission limit reached");
                continue;
            };
            if let Err(error) = stream.set_nodelay(true) {
                warn!(%peer, %error, "connection refused: could not configure TCP");
                continue;
            }
            let deadline = tokio::time::Instant::now()
                .checked_add(self.handshake_timeout)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "the configured handshake timeout cannot be represented",
                    )
                })?;
            debug!(%peer, "connection accepted");

            let broker = self.broker.clone();
            let namespace = self.namespace.clone();
            let container_id = self.container_id.clone();
            let tls_acceptor = self.tls_acceptor.clone();
            let shared_access_authentication = self.shared_access_authentication.clone();
            let connection_options = self.connection_options;
            let websocket = self.websocket;
            tokio::spawn(async move {
                let _permit = permit;
                if deadline <= tokio::time::Instant::now() {
                    return;
                }
                let result = match tls_acceptor {
                    Some(acceptor) => {
                        match tokio::time::timeout_at(deadline, acceptor.accept(stream)).await {
                            Ok(Ok(stream)) => {
                                debug!(%peer, "TLS established");
                                serve_transport_connection(
                                    stream,
                                    websocket,
                                    ConnectionSettings {
                                        container_id,
                                        namespace,
                                        broker,
                                        shared_access_authentication,
                                        connection_options,
                                        deadline,
                                    },
                                    driver,
                                )
                                .await
                            }
                            Ok(Err(error)) => Err(error.into()),
                            Err(_) => Err(handshake_timeout_error()),
                        }
                    }
                    None => {
                        serve_transport_connection(
                            stream,
                            websocket,
                            ConnectionSettings {
                                container_id,
                                namespace,
                                broker,
                                shared_access_authentication,
                                connection_options,
                                deadline,
                            },
                            driver,
                        )
                        .await
                    }
                };
                if let Err(error) = result {
                    warn!(%peer, %error, "connection ended");
                }
            });
        }
    }
}

impl<B: NativeAtomicBroker> AmqpListener<B> {
    /// Explicitly serves coordinator and primary non-session queue posting links.
    /// Ordinary `serve` remains transaction-disabled. This endpoint does not
    /// support transactional receiving, management links, or SDK transaction scopes.
    pub async fn serve_atomic_posting_ingress(self, listener: TcpListener) -> std::io::Result<()> {
        self.serve_with_driver(listener, AtomicPostingDriver).await
    }

    /// Explicitly serves primary non-session queue postings and PeekLock retirement.
    /// Ordinary `serve` and the posting-only endpoint retain their existing policies.
    /// Management, session queues, and SDK transaction scopes remain unsupported.
    pub async fn serve_atomic_messaging_ingress(
        self,
        listener: TcpListener,
    ) -> std::io::Result<()> {
        self.serve_with_driver(listener, AtomicMessagingDriver)
            .await
    }
}

#[derive(Clone, Copy)]
enum AdmissionMode {
    Ordinary,
    AtomicPosting,
    AtomicMessaging,
}

trait ConnectionDriver<B: Broker>: Copy + Send + 'static {
    const ADMISSION: AdmissionMode;

    fn serve_open<'a>(
        self,
        connection: &'a mut ServerConnection,
        namespace: NamespaceName,
        broker: B,
        authorization: Option<Arc<ConnectionAuthorization>>,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a;
}

#[derive(Clone, Copy)]
struct OrdinaryDriver;

impl<B: Broker> ConnectionDriver<B> for OrdinaryDriver {
    const ADMISSION: AdmissionMode = AdmissionMode::Ordinary;

    fn serve_open<'a>(
        self,
        connection: &'a mut ServerConnection,
        namespace: NamespaceName,
        broker: B,
        authorization: Option<Arc<ConnectionAuthorization>>,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a
    {
        serve_open_connection(connection, namespace, broker, authorization)
    }
}

#[derive(Clone, Copy)]
struct AtomicPostingDriver;

impl<B: NativeAtomicBroker> ConnectionDriver<B> for AtomicPostingDriver {
    const ADMISSION: AdmissionMode = AdmissionMode::AtomicPosting;

    fn serve_open<'a>(
        self,
        connection: &'a mut ServerConnection,
        namespace: NamespaceName,
        broker: B,
        authorization: Option<Arc<ConnectionAuthorization>>,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a
    {
        atomic_ingress::serve_atomic_posting_connection(
            connection,
            namespace,
            broker,
            authorization,
        )
    }
}

#[derive(Clone, Copy)]
struct AtomicMessagingDriver;

impl<B: NativeAtomicBroker> ConnectionDriver<B> for AtomicMessagingDriver {
    const ADMISSION: AdmissionMode = AdmissionMode::AtomicMessaging;

    fn serve_open<'a>(
        self,
        connection: &'a mut ServerConnection,
        namespace: NamespaceName,
        broker: B,
        authorization: Option<Arc<ConnectionAuthorization>>,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a
    {
        atomic_ingress::serve_atomic_messaging_connection(
            connection,
            namespace,
            broker,
            authorization,
        )
    }
}

struct ConnectionSettings<B> {
    container_id: String,
    namespace: NamespaceName,
    broker: B,
    shared_access_authentication: Option<SharedAccessAuthentication>,
    connection_options: amqp::ConnectionOptions,
    deadline: tokio::time::Instant,
}

async fn serve_transport_connection<Io, B, D>(
    stream: Io,
    websocket: bool,
    settings: ConnectionSettings<B>,
    driver: D,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    B: Broker,
    D: ConnectionDriver<B>,
{
    if !websocket {
        return serve_connection(stream, settings, driver).await;
    }
    let (stream, close) = tokio::time::timeout_at(
        settings.deadline,
        websocket::upgrade(stream, settings.shared_access_authentication.is_some()),
    )
    .await
    .map_err(|_| handshake_timeout_error())??;
    let result = serve_connection(stream, settings, driver).await;
    let closed = close.finish().await;
    result.and(closed)
}

async fn serve_connection<Io, B, D>(
    stream: Io,
    settings: ConnectionSettings<B>,
    driver: D,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    B: Broker,
    D: ConnectionDriver<B>,
{
    let ConnectionSettings {
        container_id,
        namespace,
        broker,
        shared_access_authentication,
        connection_options,
        deadline,
    } = settings;
    if deadline <= tokio::time::Instant::now() {
        return Err(handshake_timeout_error());
    }
    let opened = async {
        let (connection, authorization) = match shared_access_authentication {
            Some(config) => {
                let sasl_acceptor = SharedAccessSaslAcceptor::new(&config);
                let connection = accept_connection::<Io, B, D>(
                    stream,
                    container_id,
                    Some(Arc::new(sasl_acceptor.clone())),
                    connection_options,
                )
                .await?;
                let authorization = ConnectionAuthorization::new(config, sasl_acceptor.grant());
                (connection, Some(authorization))
            }
            None => (
                accept_connection::<Io, B, D>(stream, container_id, None, connection_options)
                    .await?,
                None,
            ),
        };
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((connection, authorization))
    };
    let (mut connection, authorization) = tokio::time::timeout_at(deadline, opened)
        .await
        .map_err(|_| handshake_timeout_error())??;
    if deadline <= tokio::time::Instant::now() {
        connection.shutdown().await;
        return Err(handshake_timeout_error());
    }
    let result = driver
        .serve_open(&mut connection, namespace, broker, authorization)
        .await;
    // Dropping a connection initiates cancellation, but admission is not freed
    // until both engine tasks have relinquished their halves of the socket.
    connection.shutdown().await;
    result
}

async fn accept_connection<Io, B, D>(
    stream: Io,
    container_id: String,
    sasl: Option<Arc<dyn amqp::SaslAuthenticator>>,
    options: amqp::ConnectionOptions,
) -> Result<ServerConnection, amqp::EngineError>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    B: Broker,
    D: ConnectionDriver<B>,
{
    match D::ADMISSION {
        AdmissionMode::Ordinary => {
            ServerConnection::accept_with_options(stream, container_id, sasl, options).await
        }
        AdmissionMode::AtomicPosting => {
            ServerConnection::accept_with_transactional_ingress(stream, container_id, sasl, options)
                .await
        }
        AdmissionMode::AtomicMessaging => {
            ServerConnection::accept_with_transactional_work(stream, container_id, sasl, options)
                .await
        }
    }
}

fn handshake_timeout_error() -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "TLS/HTTP/SASL/AMQP Open negotiation exceeded the handshake deadline",
    )
    .into()
}
