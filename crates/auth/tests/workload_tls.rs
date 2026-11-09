//! Real loopback TLS, normal server chain/hostname verification, no detached tasks.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::{io, sync::Arc, time::Duration};

use auth::{
    Permission, ResourceScope,
    workload::{self, AdmittedWorkloadStream, PreparedWorkloadTls, WorkloadError},
};
use rcgen::{CustomExtension, ExtendedKeyUsagePurpose, IsCa, KeyUsagePurpose, SanType};
use rustls::{
    ClientConfig, HandshakeKind, RootCertStore,
    client::ResolvesClientCert,
    crypto::ring,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName},
    sign::CertifiedKey,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Instant, timeout},
};
use tokio_rustls::{TlsConnector, client::TlsStream};

mod workload_support;
use workload_support::*;

const WAIT: Duration = Duration::from_secs(120);
type ServerResult = Result<AdmittedWorkloadStream<TcpStream>, WorkloadError>;
type ClientResult = Result<TlsStream<TcpStream>, io::Error>;

async fn exchange(
    endpoint: &PreparedWorkloadTls,
    config: Arc<ClientConfig>,
) -> (ServerResult, ClientResult) {
    exchange_hostname(endpoint, config, "localhost").await
}

async fn exchange_hostname(
    endpoint: &PreparedWorkloadTls,
    config: Arc<ClientConfig>,
    hostname: &'static str,
) -> (ServerResult, ClientResult) {
    exchange_with_early_attempt(endpoint, config, hostname, Arc::new(AtomicBool::new(false))).await
}

async fn exchange_with_early_attempt(
    endpoint: &PreparedWorkloadTls,
    config: Arc<ClientConfig>,
    hostname: &'static str,
    attempted: Arc<AtomicBool>,
) -> (ServerResult, ClientResult) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = async {
        let (socket, _) = listener.accept().await.unwrap();
        let mut admitted = endpoint
            .accept_before(socket, Instant::now() + WAIT)
            .await?;
        admitted
            .write_all(b"verified")
            .await
            .map_err(|_| WorkloadError::HandshakeFailed)?;
        admitted
            .flush()
            .await
            .map_err(|_| WorkloadError::HandshakeFailed)?;
        Ok(admitted)
    };
    let client = async {
        let socket = TcpStream::connect(address).await?;
        let mut tls = TlsConnector::from(config)
            .connect_with(
                ServerName::try_from(hostname).unwrap(),
                socket,
                |connection| {
                    if let Some(mut early) = connection.early_data() {
                        io::Write::write_all(&mut early, b"untrusted-early").unwrap();
                        attempted.store(true, Ordering::Release);
                    }
                },
            )
            .await?;
        // In TLS 1.3 client connect alone may precede server rejection. This
        // authenticated application byte is evidence that SERVER accept finished.
        let mut bytes = [0; 8];
        tls.read_exact(&mut bytes).await?;
        assert_eq!(&bytes, b"verified");
        Ok(tls)
    };
    timeout(WAIT, async { tokio::join!(server, client) })
        .await
        .expect("both original handshake futures completed")
}

async fn denied(endpoint: &PreparedWorkloadTls, client: Arc<ClientConfig>) {
    let (server, client) = exchange(endpoint, client).await;
    assert!(server.is_err(), "bad peer minted admitted authority");
    assert!(client.is_err(), "bad peer received server acceptance");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_handshake_yields_scoped_endpoint_bound_nonrenewable_proof() {
    let f = Fixture::new();
    let endpoint = f.endpoint();
    assert_eq!(endpoint.namespace(), &ResourceScope::parse(ROOT).unwrap());
    assert!(endpoint.requires_client_certificate());
    let (server, client) = exchange(&endpoint, f.client_config(Some(&f.client))).await;
    let server = server.unwrap();
    let _client = client.unwrap();
    assert_eq!(server.alpn_protocol(), Some(b"test-workload".as_slice()));
    let peer = server.peer().unwrap();
    assert_eq!(peer.principal(), PRINCIPAL);
    let resource = ResourceScope::parse(RESOURCE).unwrap();
    assert!(
        endpoint
            .authorize(peer, &resource, Permission::Send)
            .is_ok()
    );
    assert!(
        endpoint
            .clone()
            .authorize(peer, &resource, Permission::Send)
            .is_ok()
    );
    for (resource, permission) in [
        (RESOURCE, Permission::Listen),
        (RESOURCE, Permission::Manage),
        (RESOURCE, Permission::Cluster),
        ("amqps://tenant.example/orders-archive", Permission::Send),
        ("amqps://other.example/orders", Permission::Send),
        (ROOT, Permission::Send),
    ] {
        assert_eq!(
            endpoint.authorize(peer, &ResourceScope::parse(resource).unwrap(), permission),
            Err(WorkloadError::PermissionDenied)
        );
    }
    assert!(
        endpoint
            .authorize(
                peer,
                &ResourceScope::parse("amqps://tenant.example/ORDERS/$DeadLetterQueue").unwrap(),
                Permission::Send
            )
            .is_ok()
    );
    assert_eq!(
        f.endpoint().authorize(peer, &resource, Permission::Send),
        Err(WorkloadError::WrongEndpoint)
    );
    let copied = peer.clone();
    assert_eq!(peer.lease_deadline(), copied.lease_deadline());
    assert!(peer.lease_deadline() <= Instant::now() + Duration::from_secs(3600));
    assert!(!format!("{peer:?}").contains(PRINCIPAL));
    // The same certificate obtains different rights only through a separately
    // prepared local policy, never through peer claims or a mutable old grant.
    let mut config = f.config();
    config["bindings"][0]["scope"] = json!(ROOT);
    config["bindings"][0]["permissions"] = json!(["manage"]);
    let manager = workload_support::endpoint(&config, &f.server);
    let (managed_stream, managed_client) =
        exchange(&manager, f.client_config(Some(&f.client))).await;
    let managed_stream = managed_stream.unwrap();
    let _managed_client = managed_client.unwrap();
    let managed = managed_stream.peer().unwrap();
    assert!(
        manager
            .authorize(
                managed,
                &ResourceScope::parse(ROOT).unwrap(),
                Permission::Manage
            )
            .is_ok()
    );
    for right in [Permission::Send, Permission::Listen] {
        assert!(manager.authorize(managed, &resource, right).is_ok());
    }
    for right in [Permission::Audit, Permission::Cluster] {
        assert_eq!(
            manager.authorize(managed, &resource, right),
            Err(WorkloadError::PermissionDenied)
        );
    }
    assert_eq!(
        manager.authorize(peer, &resource, Permission::Manage),
        Err(WorkloadError::WrongEndpoint)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn required_absence_fails_optional_absence_has_no_workload_proof_and_invalid_never_falls_back()
 {
    let f = Fixture::new();
    denied(&f.endpoint(), f.client_config(None)).await;
    let mut config = f.config();
    config["client_auth"] = json!("optional");
    let optional = endpoint(&config, &f.server);
    assert!(!optional.requires_client_certificate());
    let (admitted, client) = exchange(&optional, f.client_config(None)).await;
    assert!(admitted.unwrap().peer().is_none());
    assert!(client.is_ok());
    let foreign = signed(client_parameters(PRINCIPAL), &f.other_ca);
    denied(&optional, f.client_config(Some(&foreign))).await;
    let mut config = f.config();
    config["bindings"] = json!([]);
    denied(
        &endpoint(&config, &f.server),
        f.client_config(Some(&f.client)),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn san_identity_ambiguity_and_usage_errors_fail_actual_handshakes() {
    let f = Fixture::new();
    let endpoint = f.endpoint();
    let mut variants = Vec::new();
    for sans in [
        vec![],
        vec![SanType::DnsName("example.test".try_into().unwrap())],
        vec![SanType::URI(PRINCIPAL.try_into().unwrap()); 2],
        vec![
            SanType::URI(PRINCIPAL.try_into().unwrap()),
            SanType::DnsName("example.test".try_into().unwrap()),
        ],
        vec![
            SanType::URI(PRINCIPAL.try_into().unwrap()),
            SanType::IpAddress("127.0.0.1".parse().unwrap()),
        ],
        vec![
            SanType::URI(PRINCIPAL.try_into().unwrap()),
            SanType::URI("spiffe://other.test/a".try_into().unwrap()),
        ],
    ] {
        let mut params = client_parameters(PRINCIPAL);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, PRINCIPAL);
        params.subject_alt_names = sans;
        variants.push(params);
    }
    for principal in [
        "spiffe://other.test/a",
        "spiffe://example.test/unbound",
        "spiffe://example.test/ns/ORDERS/sa/producer",
        "spiffe://example.test/a%2fb",
        "spiffe://example.test/a/../b",
        "spiffe://EXAMPLE.test/a",
        "https://example.test/a",
    ] {
        variants.push(client_parameters(principal));
    }
    for rights in [
        vec![],
        vec![KeyUsagePurpose::KeyEncipherment],
        vec![KeyUsagePurpose::KeyCertSign],
    ] {
        let mut params = client_parameters(PRINCIPAL);
        params.key_usages = rights;
        variants.push(params);
    }
    for eku in [
        vec![],
        vec![ExtendedKeyUsagePurpose::ServerAuth],
        vec![ExtendedKeyUsagePurpose::Any],
    ] {
        let mut params = client_parameters(PRINCIPAL);
        params.extended_key_usages = eku;
        variants.push(params);
    }
    let mut params = client_parameters(PRINCIPAL);
    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    variants.push(params);
    let mut params = client_parameters(PRINCIPAL);
    params
        .custom_extensions
        .push(CustomExtension::from_oid_content(
            &[2, 5, 29, 17],
            vec![0x30, 0],
        ));
    variants.push(params);
    let mut params = client_parameters(PRINCIPAL);
    params
        .custom_extensions
        .push(CustomExtension::from_oid_content(
            &[2, 5, 29, 15],
            vec![0x03, 0x02, 0x07, 0x80],
        ));
    variants.push(params);
    let mut params = client_parameters(PRINCIPAL);
    params
        .custom_extensions
        .push(CustomExtension::from_oid_content(
            &[1, 2, 3, 4],
            vec![0; 17 * 1024],
        ));
    variants.push(params);
    for (index, params) in variants.into_iter().enumerate() {
        eprintln!("reject profile variant {index}");
        let client = signed(params, &f.ca);
        denied(
            &endpoint,
            adversarial_client(&f, vec![client.cert.der().clone()], &client.key),
        )
        .await;
    }
}

#[derive(Debug)]
struct DeliberatelyInconsistentClientKey(Arc<CertifiedKey>);
impl ResolvesClientCert for DeliberatelyInconsistentClientKey {
    fn resolve(&self, _: &[&[u8]], _: &[rustls::SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        Some(self.0.clone())
    }
    fn has_certs(&self) -> bool {
        true
    }
}

fn adversarial_client(
    f: &Fixture,
    chain: Vec<CertificateDer<'static>>,
    key: &rcgen::KeyPair,
) -> Arc<ClientConfig> {
    adversarial_client_versions(
        f,
        chain,
        key,
        &[&rustls::version::TLS12, &rustls::version::TLS13],
    )
}

fn adversarial_client_versions(
    f: &Fixture,
    chain: Vec<CertificateDer<'static>>,
    key: &rcgen::KeyPair,
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(f.server_ca.cert.der().clone()).unwrap();
    let signing_key =
        ring::sign::any_supported_type(&PrivatePkcs8KeyDer::from(key.serialize_der()).into())
            .unwrap();
    let mut client = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_protocol_versions(versions)
        .unwrap()
        .with_root_certificates(roots)
        .with_client_cert_resolver(Arc::new(DeliberatelyInconsistentClientKey(Arc::new(
            CertifiedKey::new(chain, signing_key),
        ))));
    client.alpn_protocols = vec![b"test-workload".to_vec()];
    Arc::new(client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_signature_private_key_possession_and_normal_server_trust_are_required() {
    let f = Fixture::new();
    let endpoint = f.endpoint();
    let wrong_key = rcgen::KeyPair::generate().unwrap();
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        denied(
            &endpoint,
            adversarial_client_versions(
                &f,
                vec![f.client.cert.der().clone()],
                &wrong_key,
                &[version],
            ),
        )
        .await;
    }
    let mut bad_signature = f.client.cert.der().to_vec();
    *bad_signature.last_mut().unwrap() ^= 1;
    denied(
        &endpoint,
        adversarial_client(&f, vec![CertificateDer::from(bad_signature)], &f.client.key),
    )
    .await;
    let mut trailing = f.client.cert.der().to_vec();
    trailing.push(0);
    denied(
        &endpoint,
        adversarial_client(&f, vec![CertificateDer::from(trailing)], &f.client.key),
    )
    .await;
    denied(
        &endpoint,
        adversarial_client(&f, vec![CertificateDer::from(vec![0; 100])], &f.client.key),
    )
    .await;
    denied(
        &endpoint,
        adversarial_client(&f, vec![f.client.cert.der().clone(); 9], &f.client.key),
    )
    .await;
    denied(
        &endpoint,
        client_config(&f.other_ca, Some(&f.client), vec![]),
    )
    .await;
    let (server, client) =
        exchange_hostname(&endpoint, f.client_config(Some(&f.client)), "wrong.example").await;
    assert!(server.is_err());
    assert!(client.is_err());
    // Bad clients did not mutate or retire this prepared endpoint.
    let (server, client) = exchange(&endpoint, f.client_config(Some(&f.client))).await;
    assert!(server.unwrap().peer().is_some());
    assert!(client.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leaf_intermediate_and_conservative_root_bundle_dates_bound_every_new_handshake() {
    let f = Fixture::new();
    let endpoint_a = f.endpoint();
    let current = now() as i64;
    for (from, until) in [(current - 60, current - 1), (current + 60, current + 120)] {
        let mut params = client_parameters(PRINCIPAL);
        params.not_before = time::OffsetDateTime::from_unix_timestamp(from).unwrap();
        params.not_after = time::OffsetDateTime::from_unix_timestamp(until).unwrap();
        let leaf = signed(params, &f.ca);
        denied(&endpoint_a, f.client_config(Some(&leaf))).await;
        let intermediate = signed(ca_parameters("intermediate", from, until), &f.ca);
        let leaf = signed(client_parameters(PRINCIPAL), &intermediate);
        denied(
            &endpoint_a,
            client_config(
                &f.server_ca,
                Some(&leaf),
                vec![intermediate.cert.der().clone()],
            ),
        )
        .await;
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = ca_parameters("invalid-root", from, until)
            .self_signed(&key)
            .unwrap();
        let root = Identity { cert, key };
        assert!(
            PreparedWorkloadTls::new(
                policy(&configuration(&[&f.ca, &root])),
                f.server.cert.pem().as_bytes(),
                f.server.key.serialize_pem().as_bytes(),
                vec![],
                Duration::from_secs(30)
            )
            .is_err()
        );
    }
    let intermediate = signed(
        ca_parameters("valid-intermediate", current - 60, current + 120),
        &f.ca,
    );
    let leaf = signed(client_parameters(PRINCIPAL), &intermediate);
    let (stream, client) = exchange(
        &endpoint_a,
        client_config(
            &f.server_ca,
            Some(&leaf),
            vec![intermediate.cert.der().clone()],
        ),
    )
    .await;
    assert!(client.is_ok());
    assert_eq!(
        stream.unwrap().peer().unwrap().expires_at_epoch_seconds(),
        (current + 120) as u64
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_peer_expiry_and_connection_cap_cannot_be_renewed_by_copying_or_reuse() {
    let f = Fixture::new();
    let mut config = f.config();
    config["max_connection_seconds"] = json!(1);
    let endpoint = endpoint(&config, &f.server);
    let (stream, client) = exchange(&endpoint, f.client_config(Some(&f.client))).await;
    let stream = stream.unwrap();
    let _client = client.unwrap();
    let peer = stream.peer().unwrap().clone();
    let copied = peer.clone();
    assert!(
        endpoint
            .authorize(
                &peer,
                &ResourceScope::parse(RESOURCE).unwrap(),
                Permission::Send
            )
            .is_ok()
    );
    timeout(WAIT, peer.wait_until_expired()).await.unwrap();
    assert_eq!(
        endpoint.authorize(
            &copied,
            &ResourceScope::parse(RESOURCE).unwrap(),
            Permission::Send
        ),
        Err(WorkloadError::NotValid)
    );
    assert_eq!(peer.lease_deadline(), copied.lease_deadline());
    // A separately authenticated connection obtains a new, distinct lease.
    let (fresh, client) = exchange(&endpoint, f.client_config(Some(&f.client))).await;
    assert!(fresh.unwrap().peer().unwrap().ensure_live().is_ok());
    assert!(client.is_ok());
    let current = now() as i64;
    let mut params = client_parameters(PRINCIPAL);
    params.not_after = time::OffsetDateTime::from_unix_timestamp(current + 3).unwrap();
    let short = signed(params, &f.ca);
    let endpoint = f.endpoint();
    let (stream, client) = exchange(&endpoint, f.client_config(Some(&short))).await;
    let stream = stream.unwrap();
    let _client = client.unwrap();
    let peer = stream.peer().unwrap();
    assert_eq!(peer.expires_at_epoch_seconds(), (current + 3) as u64);
    timeout(WAIT, peer.wait_until_expired()).await.unwrap();
    assert_eq!(peer.ensure_live(), Err(WorkloadError::NotValid));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_root_overlap_removal_revocation_and_client_resumption_attempts_are_isolated() {
    let f = Fixture::new();
    let old = f.endpoint();
    let client = f.client_config(Some(&f.client));
    let (first, tls) = exchange(&old, client.clone()).await;
    let first = first.unwrap();
    let tls = tls.unwrap();
    assert_ne!(
        tls.get_ref().1.handshake_kind(),
        Some(HandshakeKind::Resumed)
    );
    drop(tls);
    let mut config = configuration(&[&f.ca, &f.other_ca]);
    let overlap = endpoint(&config, &f.server);
    let (second, tls) = exchange(&overlap, client.clone()).await;
    assert!(second.is_ok());
    assert_ne!(
        tls.unwrap().get_ref().1.handshake_kind(),
        Some(HandshakeKind::Resumed)
    );
    let new_leaf = signed(client_parameters(PRINCIPAL), &f.other_ca);
    let new_client = f.client_config(Some(&new_leaf));
    let (second, tls) = exchange(&overlap, new_client.clone()).await;
    assert!(second.is_ok());
    assert!(tls.is_ok());
    config["roots_pem"] = json!([f.other_ca.cert.pem()]);
    let replaced = endpoint(&config, &f.server);
    denied(&replaced, client.clone()).await;
    let (admitted, tls) = exchange(&replaced, new_client.clone()).await;
    assert!(admitted.is_ok());
    assert!(tls.is_ok());
    assert_eq!(
        replaced.authorize(
            first.peer().unwrap(),
            &ResourceScope::parse(RESOURCE).unwrap(),
            Permission::Send
        ),
        Err(WorkloadError::WrongEndpoint)
    );
    // Editing candidate JSON does NOT mutate an old prepared endpoint or its peers.
    config["revoked_leaf_sha256"] = json!([format!("{:x}", Sha256::digest(new_leaf.cert.der()))]);
    let revoked = endpoint(&config, &f.server);
    denied(&revoked, new_client.clone()).await;
    let (admitted, tls) = exchange(&replaced, new_client).await;
    assert!(admitted.is_ok());
    assert!(tls.is_ok());
    let reissued = signed(client_parameters(PRINCIPAL), &f.other_ca);
    let reissued = f.client_config(Some(&reissued));
    let (admitted, tls) = exchange(&revoked, reissued.clone()).await;
    assert!(admitted.is_ok());
    assert!(tls.is_ok());
    config["bindings"] = json!([]);
    denied(&endpoint(&config, &f.server), reissued).await;
    assert!(
        old.authorize(
            first.peer().unwrap(),
            &ResourceScope::parse(RESOURCE).unwrap(),
            Permission::Send
        )
        .is_ok(),
        "foundation replacement does not itself retire old consumer connections"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls12_and_tls13_reconnections_never_resume_or_accept_early_data() {
    let f = Fixture::new();
    let endpoint = f.endpoint();
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let mut roots = RootCertStore::empty();
        roots.add(f.server_ca.cert.der().clone()).unwrap();
        let mut client = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_protocol_versions(&[version])
            .unwrap()
            .with_root_certificates(roots)
            .with_client_auth_cert(
                vec![f.client.cert.der().clone()],
                PrivatePkcs8KeyDer::from(f.client.key.serialize_der()).into(),
            )
            .unwrap();
        client.enable_early_data = true;
        client.alpn_protocols = vec![b"test-workload".to_vec()];
        let client = Arc::new(client);
        for _ in 0..3 {
            let (stream, tls) = exchange(&endpoint, client.clone()).await;
            assert!(stream.unwrap().peer().is_some());
            let tls = tls.unwrap();
            assert_ne!(
                tls.get_ref().1.handshake_kind(),
                Some(HandshakeKind::Resumed)
            );
            assert!(!tls.get_ref().1.is_early_data_accepted());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn incomplete_and_already_expired_admission_is_bounded_without_background_tasks() {
    let f = Fixture::new();
    let endpoint = f.endpoint();
    let (_client, socket) = tokio::io::duplex(64);
    let began = Instant::now();
    let result = endpoint
        .accept_before(socket, began + Duration::from_secs(2))
        .await;
    assert!(matches!(result, Err(WorkloadError::DeadlineExpired)));
    assert_eq!(Instant::now() - began, Duration::from_secs(2));
    let (_client, socket) = tokio::io::duplex(64);
    assert!(matches!(
        endpoint.accept_before(socket, Instant::now()).await,
        Err(WorkloadError::DeadlineExpired)
    ));
    let (_client, socket) = tokio::io::duplex(64);
    let began = Instant::now();
    assert!(matches!(
        endpoint
            .accept_before(socket, began + Duration::from_secs(3600))
            .await,
        Err(WorkloadError::DeadlineExpired)
    ));
    assert_eq!(Instant::now() - began, Duration::from_secs(30));
}

#[derive(Debug)]
struct ObservedCache {
    inner: rustls::client::ClientSessionMemoryCache,
    inserted: AtomicUsize,
    offered: AtomicUsize,
}

impl rustls::client::ClientSessionStore for ObservedCache {
    fn set_kx_hint(&self, name: ServerName<'static>, group: rustls::NamedGroup) {
        self.inner.set_kx_hint(name, group);
    }
    fn kx_hint(&self, name: &ServerName<'_>) -> Option<rustls::NamedGroup> {
        self.inner.kx_hint(name)
    }
    fn set_tls12_session(
        &self,
        name: ServerName<'static>,
        value: rustls::client::Tls12ClientSessionValue,
    ) {
        self.inserted.fetch_add(1, Ordering::AcqRel);
        self.inner.set_tls12_session(name, value);
    }
    fn tls12_session(
        &self,
        name: &ServerName<'_>,
    ) -> Option<rustls::client::Tls12ClientSessionValue> {
        let value = self.inner.tls12_session(name);
        if value.is_some() {
            self.offered.fetch_add(1, Ordering::AcqRel);
        }
        value
    }
    fn remove_tls12_session(&self, name: &ServerName<'static>) {
        self.inner.remove_tls12_session(name);
    }
    fn insert_tls13_ticket(
        &self,
        name: ServerName<'static>,
        value: rustls::client::Tls13ClientSessionValue,
    ) {
        self.inserted.fetch_add(1, Ordering::AcqRel);
        self.inner.insert_tls13_ticket(name, value);
    }
    fn take_tls13_ticket(
        &self,
        name: &ServerName<'static>,
    ) -> Option<rustls::client::Tls13ClientSessionValue> {
        let value = self.inner.take_tls13_ticket(name);
        if value.is_some() {
            self.offered.fetch_add(1, Ordering::AcqRel);
        }
        value
    }
}

async fn seed_resumable_session(
    f: &Fixture,
    client: Arc<ClientConfig>,
    version: &'static rustls::SupportedProtocolVersion,
) {
    let mut roots = RootCertStore::empty();
    roots.add(f.ca.cert.der().clone()).unwrap();
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(ring::default_provider()),
    )
    .build()
    .unwrap();
    let mut config =
        rustls::ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_protocol_versions(&[version])
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![f.server.cert.der().clone()],
                PrivatePkcs8KeyDer::from(f.server.key.serialize_der()).into(),
            )
            .unwrap();
    config.max_early_data_size = 1024;
    config.alpn_protocols = vec![b"test-workload".to_vec()];
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = async {
        let (socket, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(socket).await.unwrap();
        stream.write_all(b"seed").await.unwrap();
        stream.flush().await.unwrap();
        stream
    };
    let client = async {
        let socket = TcpStream::connect(address).await.unwrap();
        let mut stream = TlsConnector::from(client)
            .connect(ServerName::try_from("localhost").unwrap(), socket)
            .await
            .unwrap();
        let mut bytes = [0; 4];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"seed");
        stream
    };
    let (_server, _client) = timeout(WAIT, async { tokio::join!(server, client) })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_real_cached_session_and_early_data_offer_cannot_bypass_replacement_revocation() {
    let f = Fixture::new();
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let cache = Arc::new(ObservedCache {
            // Rustls groups eight tickets per server and reserves cache capacity
            // for the next insertion; keep enough for an actual retained entry.
            inner: rustls::client::ClientSessionMemoryCache::new(32),
            inserted: AtomicUsize::new(0),
            offered: AtomicUsize::new(0),
        });
        let mut roots = RootCertStore::empty();
        roots.add(f.server_ca.cert.der().clone()).unwrap();
        let mut config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_protocol_versions(&[version])
            .unwrap()
            .with_root_certificates(roots)
            .with_client_auth_cert(
                vec![f.client.cert.der().clone()],
                PrivatePkcs8KeyDer::from(f.client.key.serialize_der()).into(),
            )
            .unwrap();
        config.resumption = rustls::client::Resumption::store(cache.clone());
        config.enable_early_data = true;
        config.alpn_protocols = vec![b"test-workload".to_vec()];
        let config = Arc::new(config);
        seed_resumable_session(&f, config.clone(), version).await;
        assert!(
            cache.inserted.load(Ordering::Acquire) > 0,
            "positive control actually stored a session"
        );
        let mut policy = f.config();
        policy["revoked_leaf_sha256"] =
            json!([format!("{:x}", Sha256::digest(f.client.cert.der()))]);
        let endpoint = endpoint(&policy, &f.server);
        let attempted = Arc::new(AtomicBool::new(false));
        let (server, client) =
            exchange_with_early_attempt(&endpoint, config, "localhost", attempted.clone()).await;
        assert!(server.is_err());
        assert!(client.is_err());
        assert!(
            cache.offered.load(Ordering::Acquire) > 0,
            "cached session was offered to replacement endpoint: {version:?}"
        );
        if version == &rustls::version::TLS13 {
            assert!(
                attempted.load(Ordering::Acquire),
                "0-RTT was actually attempted"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn even_an_unused_overlap_root_expires_retained_proof_and_new_admission() {
    let f = Fixture::new();
    let current = now() as i64;
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = ca_parameters("short-overlap", current - 60, current + 6)
        .self_signed(&key)
        .unwrap();
    let short = Identity { cert, key };
    let endpoint = endpoint(&configuration(&[&f.ca, &short]), &f.server);
    let (stream, client) = exchange(&endpoint, f.client_config(Some(&f.client))).await;
    let stream = stream.unwrap();
    let _client = client.unwrap();
    let peer = stream.peer().unwrap();
    assert_eq!(peer.expires_at_epoch_seconds(), (current + 6) as u64);
    timeout(WAIT, peer.wait_until_expired()).await.unwrap();
    assert_eq!(peer.ensure_live(), Err(WorkloadError::NotValid));
    denied(&endpoint, f.client_config(Some(&f.client))).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_previously_valid_cached_client_certificate_cannot_resume_after_expiry() {
    let f = Fixture::new();
    let expires = now() + 6;
    let mut params = client_parameters(PRINCIPAL);
    params.not_after = time::OffsetDateTime::from_unix_timestamp(expires as i64).unwrap();
    let short = signed(params, &f.ca);
    let cache = Arc::new(ObservedCache {
        inner: rustls::client::ClientSessionMemoryCache::new(32),
        inserted: AtomicUsize::new(0),
        offered: AtomicUsize::new(0),
    });
    let mut client = (*f.client_config(Some(&short))).clone();
    client.resumption = rustls::client::Resumption::store(cache.clone());
    client.enable_early_data = true;
    let client = Arc::new(client);
    seed_resumable_session(&f, client.clone(), &rustls::version::TLS13).await;
    assert!(cache.inserted.load(Ordering::Acquire) > 0);
    tokio::time::sleep(Duration::from_secs(expires.saturating_sub(now()) + 1)).await;
    let attempted = Arc::new(AtomicBool::new(false));
    let (server, client) =
        exchange_with_early_attempt(&f.endpoint(), client, "localhost", attempted.clone()).await;
    assert!(server.is_err());
    assert!(client.is_err());
    assert!(cache.offered.load(Ordering::Acquire) > 0);
    assert!(attempted.load(Ordering::Acquire));
}

#[tokio::test(start_paused = true)]
async fn cancelling_an_owned_pending_handshake_releases_its_stream_without_a_hidden_task() {
    let f = Fixture::new();
    let endpoint = f.endpoint();
    let (mut client, socket) = tokio::io::duplex(64);
    let mut admission = Box::pin(endpoint.accept_before(socket, Instant::now() + WAIT));
    assert!(
        timeout(Duration::from_millis(10), &mut admission)
            .await
            .is_err()
    );
    drop(admission);
    // No acceptor task was spawned and nothing keeps the other duplex endpoint.
    assert_eq!(
        timeout(Duration::from_secs(1), client.read_u8())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::UnexpectedEof
    );
}
