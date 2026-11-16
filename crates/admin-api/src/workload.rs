//! Opaque connection context derived only from an actual admitted TLS stream.
use auth::{
    Permission, ResourceScope,
    workload::{AdmittedWorkloadStream, PreparedWorkloadTls, VerifiedWorkloadPeer, WorkloadError},
};

/// Trusted transport context, never read from request metadata or certificates.
/// An absent optional certificate supplies no certificate authority or lease.
///
/// ```compile_fail
/// use admin_api::workload::Connection;
/// let context = Connection { peer: None };
/// ```
#[derive(Clone, Debug)]
pub struct Connection {
    endpoint: PreparedWorkloadTls,
    peer: Option<VerifiedWorkloadPeer>,
}
impl Connection {
    /// The caller must keep this context with THIS stream through its protocol
    /// adapter. No arbitrary peer, raw TLS stream or secure flag is accepted.
    pub fn from_admitted<Io>(
        endpoint: PreparedWorkloadTls,
        stream: &AdmittedWorkloadStream<Io>,
    ) -> Result<Self, WorkloadError> {
        if let Some(peer) = stream.peer() {
            match endpoint.authorize(peer, endpoint.namespace(), Permission::Manage) {
                Ok(()) | Err(WorkloadError::PermissionDenied) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(Self {
            endpoint,
            peer: stream.peer().cloned(),
        })
    }
    pub fn ensure_live(&self) -> Result<(), WorkloadError> {
        self.peer
            .as_ref()
            .map_or(Ok(()), VerifiedWorkloadPeer::ensure_live)
    }
    pub fn authorize(&self, scope: &ResourceScope) -> Result<(), WorkloadError> {
        self.endpoint.authorize(
            self.peer.as_ref().ok_or(WorkloadError::IdentityDenied)?,
            scope,
            Permission::Manage,
        )
    }
    pub async fn expired(&self) {
        match &self.peer {
            Some(peer) => peer.wait_until_expired().await,
            None => std::future::pending().await,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
