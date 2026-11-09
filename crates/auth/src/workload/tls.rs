use std::{
    fmt, io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rustls::{
    DigitallySignedStruct, DistinguishedName, HandshakeKind, RootCertStore, ServerConfig,
    SignatureScheme,
    client::danger::HandshakeSignatureValid,
    crypto::ring,
    pki_types::{CertificateDer, UnixTime},
    server::{
        NoServerSessionStorage, WebPkiClientVerifier,
        danger::{ClientCertVerified, ClientCertVerifier},
    },
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::{Instant, sleep, timeout_at},
};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use x509_parser::extensions::GeneralName;

use super::{
    Binding, MAX_CHAIN_BYTES, MAX_CHAIN_CERTS, WorkloadError, WorkloadPolicy,
    certificate::{Interval, certificate, interval, pem_certificates, require_ca},
    fingerprint, valid_principal,
};
use crate::{Permission, ResourceScope};

const MAX_HANDSHAKE_SECONDS: u64 = 60;
const IO_BUFFER_LIMIT: usize = 64 * 1024;
const WALL_RECHECK: Duration = Duration::from_secs(1);

/// An immutable endpoint that owns its verifier, not a caller-asserted TLS flag.
/// Clone shares identity; preparing another endpoint creates a different identity.
#[derive(Clone)]
pub struct PreparedWorkloadTls {
    policy: WorkloadPolicy,
    config: Arc<ServerConfig>,
    identity: Arc<()>,
    handshake_timeout: Duration,
}

impl fmt::Debug for PreparedWorkloadTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedWorkloadTls")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl PreparedWorkloadTls {
    /// The locally configured audience root, for consumer/listener configuration
    /// checks. Peer-controlled hostnames do not select this namespace.
    pub fn namespace(&self) -> &ResourceScope {
        self.policy.namespace()
    }

    /// Consumers must require another configured credential source when false;
    /// optional certificate absence is not authenticated workload authority.
    pub fn requires_client_certificate(&self) -> bool {
        self.policy.requires_client_certificate()
    }

    /// Builds a ring/rustls endpoint with no sessions, tickets, early data or
    /// configurable verification bypass. ALPN has at most eight nonempty values
    /// of at most 255 bytes. Admission timeout is nonzero and at most 60 seconds.
    pub fn new(
        policy: WorkloadPolicy,
        certificate_chain_pem: &[u8],
        private_key_pem: &[u8],
        alpn: Vec<Vec<u8>>,
        handshake_timeout: Duration,
    ) -> Result<Self, WorkloadError> {
        policy.0.root_interval.check(epoch_seconds()?)?;
        if handshake_timeout.is_zero()
            || handshake_timeout > Duration::from_secs(MAX_HANDSHAKE_SECONDS)
            || alpn.len() > 8
            || alpn.iter().any(|p| p.is_empty() || p.len() > 255)
            || private_key_pem.len() > 32 * 1024
        {
            return Err(WorkloadError::InvalidTlsConfiguration);
        }
        let chain = pem_certificates(certificate_chain_pem, MAX_CHAIN_CERTS)?;
        // Validate bounded, full-consumption DER for our own prepared identity too.
        for der in &chain {
            certificate(der)?;
        }
        let key_pem = private_key_pem.trim_ascii();
        if ![
            b"-----BEGIN PRIVATE KEY-----".as_slice(),
            b"-----BEGIN RSA PRIVATE KEY-----",
            b"-----BEGIN EC PRIVATE KEY-----",
        ]
        .iter()
        .any(|header| key_pem.starts_with(header))
        {
            return Err(WorkloadError::InvalidTlsConfiguration);
        }
        let (key, remaining) = rustls_pemfile::read_one_from_slice(key_pem)
            .map_err(|_| WorkloadError::InvalidTlsConfiguration)?
            .ok_or(WorkloadError::InvalidTlsConfiguration)?;
        if !remaining.trim_ascii().is_empty() {
            return Err(WorkloadError::InvalidTlsConfiguration);
        }
        let key = match key {
            rustls_pemfile::Item::Pkcs1Key(key) => key.into(),
            rustls_pemfile::Item::Pkcs8Key(key) => key.into(),
            rustls_pemfile::Item::Sec1Key(key) => key.into(),
            _ => return Err(WorkloadError::InvalidTlsConfiguration),
        };
        let provider = Arc::new(ring::default_provider());
        let mut roots = RootCertStore::empty();
        for der in &policy.0.roots {
            roots
                .add(CertificateDer::from(der.clone()))
                .map_err(|_| WorkloadError::InvalidTlsConfiguration)?;
        }
        let delegate =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                .build()
                .map_err(|_| WorkloadError::InvalidTlsConfiguration)?;
        let verifier = ProfileVerifier {
            policy: policy.clone(),
            delegate,
        };
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|_| WorkloadError::InvalidTlsConfiguration)?
            .with_client_cert_verifier(Arc::new(verifier))
            .with_single_cert(chain.into_iter().map(CertificateDer::from).collect(), key)
            .map_err(|_| WorkloadError::InvalidTlsConfiguration)?;
        config.session_storage = Arc::new(NoServerSessionStorage {});
        // The fresh builder's ticketer is disabled; never inherit a caller's config.
        config.send_tls13_tickets = 0;
        config.max_tls13_tickets = 0;
        config.max_early_data_size = 0;
        config.send_half_rtt_data = false;
        config.alpn_protocols = alpn;
        Ok(Self {
            policy,
            config: Arc::new(config),
            identity: Arc::new(()),
            handshake_timeout,
        })
    }

    /// Accepts the actual stream under the earlier of the original caller
    /// deadline and this endpoint's bounded timeout. No task is spawned. Dropping
    /// this future closes its owned stream; it is not a joined consumer drain.
    pub async fn accept_before<Io>(
        &self,
        stream: Io,
        original_deadline: Instant,
    ) -> Result<AdmittedWorkloadStream<Io>, WorkloadError>
    where
        Io: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let deadline = original_deadline.min(Instant::now() + self.handshake_timeout);
        if Instant::now() >= deadline {
            return Err(WorkloadError::DeadlineExpired);
        }
        self.policy.0.root_interval.check(epoch_seconds()?)?;
        let stream = timeout_at(
            deadline,
            TlsAcceptor::from(self.config.clone()).accept_with(stream, |connection| {
                // This limit applies to application writes, NOT handshake allocations.
                connection.set_buffer_limit(Some(IO_BUFFER_LIMIT));
            }),
        )
        .await
        .map_err(|_| WorkloadError::DeadlineExpired)?
        .map_err(|_| WorkloadError::HandshakeFailed)?;
        if Instant::now() >= deadline {
            return Err(WorkloadError::DeadlineExpired);
        }
        if stream.get_ref().1.handshake_kind() == Some(HandshakeKind::Resumed) {
            return Err(WorkloadError::HandshakeFailed);
        }
        let now = epoch_seconds()?;
        self.policy.0.root_interval.check(now)?;
        let peer = match stream.get_ref().1.peer_certificates() {
            Some(chain) if !chain.is_empty() => {
                let checked = inspect_chain(&self.policy, &chain[0], &chain[1..], now)?;
                // Start from the beginning of this wall second, not one second
                // after its certificate's exclusive notAfter boundary.
                let remaining = remaining_wall(checked.interval.until)?
                    .min(Duration::from_secs(self.policy.0.max_connection_seconds));
                if remaining.is_zero() {
                    return Err(WorkloadError::NotValid);
                }
                Some(VerifiedWorkloadPeer(Arc::new(PeerLease {
                    endpoint: self.identity.clone(),
                    principal: checked.principal,
                    binding: checked.binding,
                    interval: checked.interval,
                    deadline: Instant::now() + remaining,
                    expired: AtomicBool::new(false),
                })))
            }
            _ if self.requires_client_certificate() => {
                return Err(WorkloadError::IdentityDenied);
            }
            _ => None,
        };
        Ok(AdmittedWorkloadStream { stream, peer })
    }

    /// Checks endpoint provenance, current wall time, the fixed monotonic lease,
    /// local scope and rights. No time/freshness assertion is accepted from callers.
    pub fn authorize(
        &self,
        peer: &VerifiedWorkloadPeer,
        resource: &ResourceScope,
        permission: Permission,
    ) -> Result<(), WorkloadError> {
        if !Arc::ptr_eq(&self.identity, &peer.0.endpoint) {
            return Err(WorkloadError::WrongEndpoint);
        }
        peer.ensure_live()?;
        if peer.0.binding.scope.contains(resource) && peer.0.binding.permissions.allows(permission)
        {
            Ok(())
        } else {
            Err(WorkloadError::PermissionDenied)
        }
    }
}

struct PeerLease {
    endpoint: Arc<()>,
    principal: String,
    binding: Binding,
    interval: Interval,
    deadline: Instant,
    expired: AtomicBool,
}

/// Evidence of a completed chain- and private-key-verified handshake. No public
/// constructor, deserialization, certificate export or renewal API exists.
/// Clones share the SAME deadline and sticky invalidation, not a fresh lease.
/// It is not proof that a socket is still open; consumers retain stream custody.
///
/// ```compile_fail
/// use auth::workload::VerifiedWorkloadPeer;
/// let peer = VerifiedWorkloadPeer::new(true);
/// ```
/// ```compile_fail
/// use auth::workload::VerifiedWorkloadPeer;
/// let peer: VerifiedWorkloadPeer = serde_json::from_str("{}").unwrap();
/// ```
/// ```compile_fail
/// use auth::workload::VerifiedWorkloadPeer;
/// let peer: VerifiedWorkloadPeer = b"certificate DER".as_slice().into();
/// ```
#[derive(Clone)]
pub struct VerifiedWorkloadPeer(Arc<PeerLease>);

impl fmt::Debug for VerifiedWorkloadPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VerifiedWorkloadPeer(<redacted>)")
    }
}

impl VerifiedWorkloadPeer {
    pub fn principal(&self) -> &str {
        &self.0.principal
    }
    pub fn lease_deadline(&self) -> Instant {
        self.0.deadline
    }
    /// Exclusive certificate/chain/root-bundle wall limit. The monotonic
    /// `lease_deadline` can be earlier because of the connection-lifetime cap.
    pub fn expires_at_epoch_seconds(&self) -> u64 {
        self.0.interval.until
    }

    pub fn ensure_live(&self) -> Result<(), WorkloadError> {
        let wall = epoch_seconds().inspect_err(|_| {
            self.0.expired.store(true, Ordering::Release);
        })?;
        self.check_at(wall, Instant::now())
    }

    fn check_at(&self, wall: u64, monotonic: Instant) -> Result<(), WorkloadError> {
        if self.0.expired.load(Ordering::Acquire)
            || monotonic >= self.0.deadline
            || self.0.interval.check(wall).is_err()
        {
            self.0.expired.store(true, Ordering::Release);
            Err(WorkloadError::NotValid)
        } else {
            Ok(())
        }
    }

    /// A task-free future for the owning connection's select loop. Idle expiry
    /// wakes at the fixed deadline; wall jumps are noticed within one second.
    /// This does not itself retire a socket, link, HTTP/2 stream or broker task.
    pub async fn wait_until_expired(&self) {
        loop {
            if self.ensure_live().is_err() {
                return;
            }
            sleep(
                self.0
                    .deadline
                    .saturating_duration_since(Instant::now())
                    .min(WALL_RECHECK),
            )
            .await;
        }
    }
}

/// Owns the actual admitted stream and its associated optional peer proof.
/// Implements IO but exposes no underlying rustls connection/raw certificates.
/// Absence in optional mode is never an authenticated anonymous workload.
///
/// ```compile_fail
/// use auth::workload::AdmittedWorkloadStream;
/// let stream = AdmittedWorkloadStream::from_parts((), None);
/// ```
pub struct AdmittedWorkloadStream<Io> {
    stream: TlsStream<Io>,
    peer: Option<VerifiedWorkloadPeer>,
}

impl<Io> AdmittedWorkloadStream<Io> {
    pub fn peer(&self) -> Option<&VerifiedWorkloadPeer> {
        self.peer.as_ref()
    }
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.stream.get_ref().1.alpn_protocol()
    }
}

impl<Io: AsyncRead + AsyncWrite + Unpin> AsyncRead for AdmittedWorkloadStream<Io> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl<Io: AsyncRead + AsyncWrite + Unpin> AsyncWrite for AdmittedWorkloadStream<Io> {
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
}

struct CheckedPeer {
    principal: String,
    binding: Binding,
    interval: Interval,
}

fn inspect_chain(
    policy: &WorkloadPolicy,
    leaf: &CertificateDer<'_>,
    intermediates: &[CertificateDer<'_>],
    now: u64,
) -> Result<CheckedPeer, WorkloadError> {
    if intermediates.len() >= MAX_CHAIN_CERTS
        || leaf.len() > super::MAX_CERT_BYTES
        || intermediates
            .iter()
            .any(|der| der.len() > super::MAX_CERT_BYTES)
        || leaf.len() + intermediates.iter().map(|der| der.len()).sum::<usize>() > MAX_CHAIN_BYTES
    {
        return Err(WorkloadError::TooLarge);
    }
    let cert = certificate(leaf)?;
    let mut validity = policy.0.root_interval.intersection(interval(&cert)?)?;
    let mut hashes = std::collections::HashSet::new();
    hashes.insert(fingerprint(leaf));
    for der in intermediates {
        if !hashes.insert(fingerprint(der)) {
            return Err(WorkloadError::InvalidCertificate);
        }
        let cert = certificate(der)?;
        require_ca(&cert)?;
        validity = validity.intersection(interval(&cert)?)?;
    }
    validity.check(now)?;
    let bc = cert
        .basic_constraints()
        .map_err(|_| WorkloadError::InvalidCertificate)?;
    let ku = cert
        .key_usage()
        .map_err(|_| WorkloadError::InvalidCertificate)?;
    let eku = cert
        .extended_key_usage()
        .map_err(|_| WorkloadError::InvalidCertificate)?;
    if bc.is_some_and(|bc| bc.value.ca)
        || ku.is_none_or(|ku| !ku.value.digital_signature())
        || eku.is_none_or(|eku| !eku.value.client_auth || eku.value.any)
    {
        return Err(WorkloadError::InvalidCertificate);
    }
    let san = cert
        .subject_alternative_name()
        .map_err(|_| WorkloadError::InvalidCertificate)?
        .ok_or(WorkloadError::InvalidCertificate)?;
    let [GeneralName::URI(principal)] = san.value.general_names.as_slice() else {
        return Err(WorkloadError::InvalidCertificate);
    };
    if !valid_principal(principal, &policy.0.trust_domain)
        || policy.0.revoked.contains(&fingerprint(leaf))
    {
        return Err(WorkloadError::IdentityDenied);
    }
    let binding = policy
        .0
        .bindings
        .get(*principal)
        .ok_or(WorkloadError::IdentityDenied)?
        .clone();
    Ok(CheckedPeer {
        principal: principal.to_string(),
        binding,
        interval: validity,
    })
}

struct ProfileVerifier {
    policy: WorkloadPolicy,
    delegate: Arc<dyn ClientCertVerifier>,
}

impl fmt::Debug for ProfileVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WorkloadClientVerifier(<redacted>)")
    }
}

impl ClientCertVerifier for ProfileVerifier {
    fn client_auth_mandatory(&self) -> bool {
        self.policy.requires_client_certificate()
    }
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.delegate.root_hint_subjects()
    }
    fn verify_client_cert(
        &self,
        leaf: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        inspect_chain(&self.policy, leaf, intermediates, now.as_secs()).map_err(|_| {
            rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            )
        })?;
        self.delegate.verify_client_cert(leaf, intermediates, now)
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.delegate.verify_tls12_signature(message, cert, dss)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.delegate.verify_tls13_signature(message, cert, dss)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.delegate.supported_verify_schemes()
    }
}

fn epoch_seconds() -> Result<u64, WorkloadError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_secs())
        .map_err(|_| WorkloadError::ClockUnavailable)
}

fn remaining_wall(expiry: u64) -> Result<Duration, WorkloadError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WorkloadError::ClockUnavailable)?;
    Duration::from_secs(expiry)
        .checked_sub(now)
        .ok_or(WorkloadError::NotValid)
}

#[cfg(test)]
mod tests;
