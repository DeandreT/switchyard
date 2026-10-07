use std::{future::Future, sync::Arc, time::Duration};

use auth::{ResourceScope, SharedAccessPolicy};
use domain::NamespaceName;
use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    time::{Instant, timeout, timeout_at},
};
use tokio_rustls::TlsAcceptor;

use super::request::{self, RequestContext};
use crate::BrokerHandle;

const CONNECTION_LIMIT: usize = 128;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);
const REJECTION_YIELD_INTERVAL: usize = 32;

/// A fixed-namespace, fixed-audience HTTP/1 listener that accepts only TLS.
/// Listener cancellation drops its owned connection futures, not detached tasks.
pub struct AtomAdminListener {
    context: Arc<RequestContext>,
    tls: TlsAcceptor,
}

#[derive(Debug, Error)]
pub enum AtomAdminError {
    #[error("the Atom administration namespace is invalid")]
    InvalidNamespace,
    #[error("Atom administration requires a namespace-only fixed audience")]
    InvalidAudience,
    #[error("the Atom administration socket could not accept a connection")]
    Accept(#[source] std::io::Error),
}

impl AtomAdminListener {
    pub fn new(
        broker: BrokerHandle,
        namespace: NamespaceName,
        policy: SharedAccessPolicy,
        fixed_namespace_scope: ResourceScope,
        mut tls: rustls::ServerConfig,
    ) -> Result<Self, AtomAdminError> {
        let namespace =
            NamespaceName::new(namespace.as_str()).map_err(|_| AtomAdminError::InvalidNamespace)?;
        if fixed_namespace_scope.path().next().is_some() {
            return Err(AtomAdminError::InvalidAudience);
        }
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            context: Arc::new(RequestContext {
                broker,
                namespace,
                policy,
                audience: fixed_namespace_scope,
            }),
            tls: TlsAcceptor::from(Arc::new(tls)),
        })
    }

    pub async fn serve(self, listener: TcpListener) -> Result<(), AtomAdminError> {
        self.serve_until(listener, std::future::pending()).await
    }

    /// A completed shutdown stops admission and drops all accepted connections.
    /// An already admitted owner job may still commit after its response is lost.
    pub async fn serve_until(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), AtomAdminError> {
        tokio::pin!(shutdown);
        let mut connections: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();
        let mut rejected = 0;
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = connections.next(), if !connections.is_empty() => {},
                accepted = listener.accept() => {
                    let (stream, _) = accepted.map_err(AtomAdminError::Accept)?;
                    let accepted_at = Instant::now();
                    if connections.len() >= CONNECTION_LIMIT {
                        drop(stream);
                        rejected += 1;
                        if rejected == REJECTION_YIELD_INTERVAL {
                            rejected = 0;
                            tokio::task::yield_now().await;
                        }
                        continue;
                    }
                    rejected = 0;
                    let tls = self.tls.clone();
                    let context = self.context.clone();
                    connections.push(Box::pin(async move {
                        let _ = timeout_at(
                            accepted_at + CONNECTION_TIMEOUT,
                            serve_connection(stream, tls, context),
                        ).await;
                    }));
                }
            }
        }
        drop(connections);
        Ok(())
    }
}

async fn serve_connection(stream: TcpStream, tls: TlsAcceptor, context: Arc<RequestContext>) {
    let Ok(Ok(stream)) = timeout(HANDSHAKE_TIMEOUT, tls.accept(stream)).await else {
        return;
    };
    let service = service_fn(move |request: hyper::Request<Incoming>| {
        let context = context.clone();
        async move { Ok::<_, std::convert::Infallible>(request::handle(request, &context).await) }
    });
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_TIMEOUT)
        .max_headers(request::MAX_HEADER_COUNT)
        .max_buf_size(request::MAX_HEADER_BYTES)
        .half_close(false)
        .keep_alive(false);
    let _ = builder
        .serve_connection(TokioIo::new(stream), service)
        .await;
}

#[cfg(test)]
mod tests;
