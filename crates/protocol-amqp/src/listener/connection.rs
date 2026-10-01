use std::sync::Arc;

use amqp::ServerConnection;
use domain::NamespaceName;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::Semaphore,
};
use tracing::{debug, warn};

use super::{AmqpListener, serve_open_connection, websocket};
use crate::{
    Broker, SharedAccessAuthentication,
    authorization::{ConnectionAuthorization, SharedAccessSaslAcceptor},
};

impl<B: Broker> AmqpListener<B> {
    /// Accepts connections until the listener fails.
    ///
    /// A connection that fails takes only itself down: one client's protocol
    /// error is not the node's.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
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

struct ConnectionSettings<B> {
    container_id: String,
    namespace: NamespaceName,
    broker: B,
    shared_access_authentication: Option<SharedAccessAuthentication>,
    connection_options: amqp::ConnectionOptions,
    deadline: tokio::time::Instant,
}

async fn serve_transport_connection<Io, B>(
    stream: Io,
    websocket: bool,
    settings: ConnectionSettings<B>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    B: Broker,
{
    if !websocket {
        return serve_connection(
            stream,
            settings.container_id,
            settings.namespace,
            settings.broker,
            settings.shared_access_authentication,
            settings.connection_options,
            settings.deadline,
        )
        .await;
    }
    let (stream, close) = tokio::time::timeout_at(
        settings.deadline,
        websocket::upgrade(stream, settings.shared_access_authentication.is_some()),
    )
    .await
    .map_err(|_| handshake_timeout_error())??;
    let result = serve_connection(
        stream,
        settings.container_id,
        settings.namespace,
        settings.broker,
        settings.shared_access_authentication,
        settings.connection_options,
        settings.deadline,
    )
    .await;
    let closed = close.finish().await;
    result.and(closed)
}

async fn serve_connection<Io, B>(
    stream: Io,
    container_id: String,
    namespace: NamespaceName,
    broker: B,
    shared_access_authentication: Option<SharedAccessAuthentication>,
    connection_options: amqp::ConnectionOptions,
    deadline: tokio::time::Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    B: Broker,
{
    if deadline <= tokio::time::Instant::now() {
        return Err(handshake_timeout_error());
    }
    let opened = async {
        let (connection, authorization) = match shared_access_authentication {
            Some(config) => {
                let sasl_acceptor = SharedAccessSaslAcceptor::new(&config);
                let connection = ServerConnection::accept_with_options(
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
                ServerConnection::accept_with_options(
                    stream,
                    container_id,
                    None,
                    connection_options,
                )
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
    let result = serve_open_connection(&mut connection, namespace, broker, authorization).await;
    // Dropping a connection initiates cancellation, but admission is not freed
    // until both engine tasks have relinquished their halves of the socket.
    connection.shutdown().await;
    result
}

fn handshake_timeout_error() -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "TLS/HTTP/SASL/AMQP Open negotiation exceeded the handshake deadline",
    )
    .into()
}
