use std::{
    io,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use admin_api::v1::{
    entity_service_server::EntityServiceServer,
    finite_queue_service_server::FiniteQueueServiceServer, rule_service_server::RuleServiceServer,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_stream::Stream;
use tonic::transport::{
    Identity, Server, ServerTlsConfig,
    server::{Connected, TcpConnectInfo},
};

use crate::NativeAdminService;

pub const NATIVE_ADMIN_TLS_PORT: u16 = 9443;
pub const NATIVE_ADMIN_DEVELOPMENT_PORT: u16 = 9080;
pub const DEFAULT_NATIVE_ADMIN_CONNECTION_LIMIT: usize = 128;
pub const NATIVE_ADMIN_REQUEST_LIMIT: usize = 64 * 1024;
pub const NATIVE_ADMIN_RESPONSE_LIMIT: usize = 1024 * 1024;

/// The native HTTP/2 endpoint, independent of the AMQP listener and its ALPN.
pub struct NativeAdminListener {
    service: NativeAdminService,
    identity: Option<Identity>,
    connection_limit: NonZeroUsize,
    handshake_timeout: Duration,
}

impl NativeAdminListener {
    pub fn new(service: NativeAdminService) -> Self {
        Self {
            service,
            identity: None,
            connection_limit: NonZeroUsize::new(DEFAULT_NATIVE_ADMIN_CONNECTION_LIMIT)
                .expect("the connection limit is nonzero"),
            handshake_timeout: Duration::from_secs(10),
        }
    }

    /// Validates the identity before retaining it for the HTTP/2 TLS stack.
    pub fn with_tls(
        mut self,
        certificate_chain_pem: &[u8],
        private_key_pem: &[u8],
    ) -> Result<Self, protocol_amqp::TlsConfigurationError> {
        protocol_amqp::tls_server_config(certificate_chain_pem, private_key_pem)?;
        self.identity = Some(Identity::from_pem(certificate_chain_pem, private_key_pem));
        Ok(self)
    }

    pub fn with_connection_limit(mut self, limit: NonZeroUsize) -> Self {
        self.connection_limit = limit;
        self
    }

    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    pub async fn serve(self, listener: TcpListener) -> Result<(), NativeAdminError> {
        if self.service.requires_authentication() && self.identity.is_none() {
            return Err(NativeAdminError::AuthenticationRequiresTls);
        }
        let mut server = Server::builder()
            .concurrency_limit_per_connection(32)
            .max_concurrent_streams(32)
            .load_shed(true)
            .timeout(Duration::from_secs(30))
            .http2_keepalive_interval(Some(Duration::from_secs(30)))
            .http2_keepalive_timeout(Some(Duration::from_secs(10)))
            .max_connection_age(Duration::from_secs(300))
            .max_connection_age_grace(Duration::from_secs(30))
            .http2_max_pending_accept_reset_streams(Some(32))
            .http2_max_local_error_reset_streams(Some(32));
        if let Some(identity) = self.identity {
            server = server.tls_config(
                ServerTlsConfig::new()
                    .identity(identity)
                    .timeout(self.handshake_timeout),
            )?;
        }
        let incoming = AdmittedConnections {
            listener,
            permits: Arc::new(Semaphore::new(
                self.connection_limit.get().min(Semaphore::MAX_PERMITS),
            )),
        };
        let maintenance = self.service.development_maintenance_service();
        server
            .add_service(
                EntityServiceServer::new(self.service.clone())
                    .max_decoding_message_size(NATIVE_ADMIN_REQUEST_LIMIT)
                    .max_encoding_message_size(NATIVE_ADMIN_RESPONSE_LIMIT),
            )
            .add_service(
                FiniteQueueServiceServer::new(self.service.clone())
                    .max_decoding_message_size(NATIVE_ADMIN_REQUEST_LIMIT)
                    .max_encoding_message_size(NATIVE_ADMIN_RESPONSE_LIMIT),
            )
            .add_service(
                RuleServiceServer::new(self.service)
                    .max_decoding_message_size(NATIVE_ADMIN_REQUEST_LIMIT)
                    .max_encoding_message_size(NATIVE_ADMIN_RESPONSE_LIMIT),
            )
            .add_optional_service(maintenance)
            .serve_with_incoming(incoming)
            .await?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum NativeAdminError {
    #[error("native administration authentication requires a TLS listener")]
    AuthenticationRequiresTls,
    #[error("native administration transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
}

struct AdmittedConnections {
    listener: TcpListener,
    permits: Arc<Semaphore>,
}

impl Stream for AdmittedConnections {
    type Item = Result<AdmittedTcpStream, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Bound work when excess peers continuously arrive, not only live sockets.
        for _ in 0..32 {
            match this.listener.poll_accept(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Ok((stream, _))) => {
                    if let Ok(permit) = Arc::clone(&this.permits).try_acquire_owned() {
                        if let Err(error) = stream.set_nodelay(true) {
                            return Poll::Ready(Some(Err(error)));
                        }
                        return Poll::Ready(Some(Ok(AdmittedTcpStream {
                            stream,
                            _permit: permit,
                        })));
                    }
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

struct AdmittedTcpStream {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
}

impl Connected for AdmittedTcpStream {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.stream.connect_info()
    }
}

impl AsyncRead for AdmittedTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for AdmittedTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write_vectored(cx, bufs)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;
    use tokio_stream::StreamExt;

    use super::*;

    #[tokio::test]
    async fn admission_is_held_by_the_socket_and_released_on_drop() -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let permits = Arc::new(Semaphore::new(1));
        let mut incoming = AdmittedConnections {
            listener,
            permits: Arc::clone(&permits),
        };
        let _first_peer = TcpStream::connect(address).await?;
        let first = incoming.next().await.expect("the first accepted socket")?;
        assert_eq!(permits.available_permits(), 0);

        let mut second_peer = TcpStream::connect(address).await?;
        let rejected = tokio::time::timeout(Duration::from_millis(25), incoming.next()).await;
        assert!(rejected.is_err(), "the second socket must not be yielded");
        let mut byte = [0];
        assert_eq!(second_peer.read(&mut byte).await?, 0);
        drop(first);
        assert_eq!(permits.available_permits(), 1);

        let _third_peer = TcpStream::connect(address).await?;
        let third = incoming.next().await.expect("a replacement socket")?;
        assert_eq!(permits.available_permits(), 0);
        drop(third);
        assert_eq!(permits.available_permits(), 1);
        Ok(())
    }
}
