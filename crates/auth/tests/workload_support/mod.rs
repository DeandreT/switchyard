#![allow(dead_code)] // Shared between pure-policy, private-kernel and wire tests.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "workload-tls")]
use crate::workload::PreparedWorkloadTls;
use crate::{ResourceScope, workload::WorkloadPolicy};
use rcgen::{
    Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use rustls::{
    ClientConfig, RootCertStore,
    crypto::ring,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer},
};
use serde_json::{Value, json};
use time::OffsetDateTime;

pub const PRINCIPAL: &str = "spiffe://example.test/ns/orders/sa/producer";
pub const ROOT: &str = "amqps://tenant.example";
pub const RESOURCE: &str = "amqps://tenant.example/orders";

pub struct Identity {
    pub cert: Certificate,
    pub key: KeyPair,
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub fn parameters(name: &str, from: i64, until: i64) -> CertificateParams {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, name);
    params.not_before = OffsetDateTime::from_unix_timestamp(from).unwrap();
    params.not_after = OffsetDateTime::from_unix_timestamp(until).unwrap();
    params
}

pub fn ca_parameters(name: &str, from: i64, until: i64) -> CertificateParams {
    let mut params = parameters(name, from, until);
    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
}

pub fn root(name: &str) -> Identity {
    let now = now() as i64;
    let key = KeyPair::generate().unwrap();
    let cert = ca_parameters(name, now - 60, now + 7200)
        .self_signed(&key)
        .unwrap();
    Identity { cert, key }
}

pub fn signed(params: CertificateParams, issuer: &Identity) -> Identity {
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &issuer.cert, &issuer.key).unwrap();
    Identity { cert, key }
}

pub fn client_parameters(principal: &str) -> CertificateParams {
    let now = now() as i64;
    let mut params = parameters("ignored-client-cn", now - 60, now + 7200);
    params.subject_alt_names = vec![SanType::URI(principal.try_into().unwrap())];
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    params
}

pub fn configuration(roots: &[&Identity]) -> Value {
    json!({"version":1,"trust_domain":"example.test",
        "roots_pem":roots.iter().map(|id| id.cert.pem()).collect::<Vec<_>>(),
        "bindings":[{"principal":PRINCIPAL,"scope":RESOURCE,"permissions":["send"]}]})
}

pub fn policy(config: &Value) -> WorkloadPolicy {
    WorkloadPolicy::from_json(&config.to_string(), ResourceScope::parse(ROOT).unwrap()).unwrap()
}

#[cfg(feature = "workload-tls")]
pub fn endpoint(config: &Value, server: &Identity) -> PreparedWorkloadTls {
    PreparedWorkloadTls::new(
        policy(config),
        server.cert.pem().as_bytes(),
        server.key.serialize_pem().as_bytes(),
        vec![b"test-workload".to_vec()],
        std::time::Duration::from_secs(30),
    )
    .unwrap()
}

pub struct Fixture {
    pub ca: Identity,
    pub other_ca: Identity,
    pub server_ca: Identity,
    pub server: Identity,
    pub client: Identity,
}

impl Fixture {
    pub fn new() -> Self {
        let ca = root("workload-root-a");
        let other_ca = root("workload-root-b");
        let server_ca = root("server-root");
        let mut params = parameters("localhost", now() as i64 - 60, now() as i64 + 7200);
        params.subject_alt_names = vec![SanType::DnsName("localhost".try_into().unwrap())];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server = signed(params, &server_ca);
        let client = signed(client_parameters(PRINCIPAL), &ca);
        Self {
            ca,
            other_ca,
            server_ca,
            server,
            client,
        }
    }

    pub fn config(&self) -> Value {
        configuration(&[&self.ca])
    }
    #[cfg(feature = "workload-tls")]
    pub fn endpoint(&self) -> PreparedWorkloadTls {
        endpoint(&self.config(), &self.server)
    }
    pub fn client_config(&self, client: Option<&Identity>) -> Arc<ClientConfig> {
        client_config(&self.server_ca, client, vec![])
    }
}

pub fn client_config(
    server_ca: &Identity,
    client: Option<&Identity>,
    intermediates: Vec<CertificateDer<'static>>,
) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(server_ca.cert.der().clone()).unwrap();
    let builder = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
    let mut config = match client {
        Some(id) => {
            let mut chain = vec![id.cert.der().clone()];
            chain.extend(intermediates);
            builder
                .with_client_auth_cert(
                    chain,
                    PrivatePkcs8KeyDer::from(id.key.serialize_der()).into(),
                )
                .unwrap()
        }
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![b"test-workload".to_vec()];
    Arc::new(config)
}
