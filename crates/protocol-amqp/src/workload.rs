//! Workload configuration and a connection-local, final broker admission fence.

use std::{future::pending, num::NonZeroUsize, time::Duration};

use auth::{
    Permission, ResourceScope,
    workload::{AdmittedWorkloadStream, PreparedWorkloadTls, VerifiedWorkloadPeer},
};
use domain::{CommandKind, CommandOutcome, EntityBinding, EntityPath, NamespaceName};
use tokio::{net::TcpListener, time::Instant};

use crate::{AmqpListener, AmqpListenerService, Broker, BrokerRejection};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WorkloadListenerError {
    #[error("workload TLS conflicts with an existing TLS configuration")]
    ConflictingTls,
    #[error("workload TLS and listener namespaces do not match")]
    NamespaceMismatch,
    #[error(
        "optional client certificates require explicitly configured alternative authentication"
    )]
    AlternativeCredentialRequired,
}

/// Validated opt-in with frozen TLS and credential sources. No raw inner
/// listener, Deref, TLS replacement or credential replacement is exposed.
///
/// ```compile_fail
/// use protocol_amqp::WorkloadAmqpListener;
/// fn downgrade<B: protocol_amqp::Broker>(listener: WorkloadAmqpListener<B>, tls: rustls::ServerConfig) {
///     listener.with_tls(tls);
/// }
/// ```
/// ```compile_fail
/// use protocol_amqp::{WorkloadAmqpListener, SharedAccessAuthentication};
/// fn replace<B: protocol_amqp::Broker>(listener: WorkloadAmqpListener<B>, auth: SharedAccessAuthentication) {
///     listener.with_shared_access_authentication(auth);
/// }
/// ```
pub struct WorkloadAmqpListener<B> {
    inner: AmqpListener<B>,
}

impl<B: Broker> WorkloadAmqpListener<B> {
    pub(crate) fn new(inner: AmqpListener<B>) -> Self {
        Self { inner }
    }
    pub fn with_max_connections(mut self, limit: NonZeroUsize) -> Self {
        self.inner = self.inner.with_max_connections(limit);
        self
    }
    pub fn with_admission_timeout(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.with_admission_timeout(timeout);
        self
    }
    pub fn into_tcp_service(self, listener: TcpListener) -> AmqpListenerService<B> {
        self.inner.into_tcp_service(listener)
    }
    pub fn into_wss_service(self, listener: TcpListener) -> AmqpListenerService<B> {
        self.inner.into_wss_service(listener)
    }
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        self.inner.serve(listener).await
    }
    pub async fn serve_websockets(self, listener: TcpListener) -> std::io::Result<()> {
        self.inner.serve_websockets(listener).await
    }
}

#[derive(Clone, Debug)]
pub(crate) struct WorkloadPeer {
    endpoint: PreparedWorkloadTls,
    peer: VerifiedWorkloadPeer,
}

impl WorkloadPeer {
    // Called only with the actual stream returned by this endpoint's handshake;
    // never constructed from exported certificates, booleans or wire input.
    pub(crate) fn from_admitted<Io>(
        endpoint: PreparedWorkloadTls,
        stream: &AdmittedWorkloadStream<Io>,
    ) -> Option<Self> {
        stream.peer().cloned().map(|peer| Self { endpoint, peer })
    }
    pub(crate) fn is_live(&self) -> bool {
        self.peer.ensure_live().is_ok()
    }
    pub(crate) fn allows(&self, resource: &ResourceScope, permission: Permission) -> bool {
        self.endpoint
            .authorize(&self.peer, resource, permission)
            .is_ok()
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.peer.lease_deadline()
    }
    pub(crate) async fn expired(&self) {
        self.peer.wait_until_expired().await;
    }
}

/// Wrap exactly once at the actual connection boundary. The check is INSIDE
/// the future, immediately before invoking the real Broker method, not at a
/// prior attachment/authorization frontier or while constructing this future.
/// Once invoked, the same original runs to completion without a new check or
/// cancellation. Captured EntityBinding is forwarded unchanged, never recaptured.
#[derive(Clone)]
pub(crate) struct LeaseBroker<B> {
    inner: B,
    peer: Option<WorkloadPeer>,
}

impl<B: Broker> LeaseBroker<B> {
    pub(crate) fn new(inner: B, peer: Option<WorkloadPeer>) -> Self {
        Self { inner, peer }
    }
    fn admit(&self) -> Result<(), BrokerRejection> {
        if self.peer.as_ref().is_none_or(WorkloadPeer::is_live) {
            Ok(())
        } else {
            Err(BrokerRejection::AuthorizationExpired)
        }
    }
}

impl<B: Broker> Broker for LeaseBroker<B> {
    async fn bind_entity(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<EntityBinding, BrokerRejection> {
        self.admit()?;
        self.inner.bind_entity(namespace, entity).await
    }
    async fn submit_bound(
        &self,
        binding: EntityBinding,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.admit()?;
        self.inner.submit_bound(binding, kind).await
    }
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.admit()?;
        self.inner.submit(namespace, entity, kind).await
    }
    async fn deliverable(&self, namespace: &NamespaceName, entity: &EntityPath) {
        if self.admit().is_err() {
            return pending().await;
        }
        self.inner.deliverable(namespace, entity).await;
    }
}
