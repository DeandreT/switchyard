//! Rust HTTP/1 requests over verified private-CA TLS; not an SDK conformance gate.

use std::{
    error::Error,
    net::SocketAddr,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{
    CommandKind, EntityPath, FiniteQueueCapacity, NamespaceName, QueueConfig, StateMachine,
    Timestamp,
};
use futures_util::FutureExt;
use hmac::{Hmac, Mac};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Method, Request, StatusCode, body::Bytes};
use hyper_util::rt::TokioIo;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName},
};
use server::{AtomAdminListener, Broker, Clock, LocalProposer};
use sha2::Sha256;
use storage::{Key, MemoryStore, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};
use tokio_rustls::TlsConnector;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "atom-transport-secret";
const DEADLINE: Duration = Duration::from_secs(5);
const MIB: u64 = 1_048_576;
const CREATE: &[u8] = br#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><MaxSizeInMegabytes>1</MaxSizeInMegabytes><DefaultMessageTimeToLive>PT60S</DefaultMessageTimeToLive></QueueDescription></content></entry>"#;
const REPLACE: &[u8] = br#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><MaxSizeInMegabytes>2</MaxSizeInMegabytes><MaxDeliveryCount>4</MaxDeliveryCount></QueueDescription></content></entry>"#;

#[path = "atom_admin_transport/cases.rs"]
mod cases;
#[path = "atom_admin_transport/fixture.rs"]
mod fixture;

use fixture::*;

macro_rules! backend_tests {
    ($name:ident, $provider:expr) => {
        mod $name {
            use super::*;
            #[tokio::test]
            async fn finite_queue_crud_full_put_and_reopen_cross_verified_tls() -> TestResult {
                cases::crud($provider).await
            }
            #[tokio::test]
            async fn denied_and_malformed_requests_have_no_owner_effects() -> TestResult {
                cases::refusals($provider).await
            }
            #[tokio::test]
            async fn literal_paths_and_http_capacity_refusal_keep_their_state() -> TestResult {
                cases::literal_and_quota($provider).await
            }
        }
    };
}

backend_tests!(memory, testkit::MemoryProvider::default());
backend_tests!(durable, testkit::DurableProvider::temporary()?);

#[tokio::test]
async fn invalid_tls_trust_or_name_and_plaintext_never_enter_http() -> TestResult {
    cases::tls_refusals(testkit::MemoryProvider::default()).await
}

#[tokio::test]
async fn listener_shutdown_drops_original_stalled_tls_connections() -> TestResult {
    cases::shutdown(testkit::MemoryProvider::default()).await
}

#[tokio::test]
async fn tls_handshake_and_http_header_deadlines_close_idle_connections() -> TestResult {
    cases::deadlines(testkit::MemoryProvider::default()).await
}

#[tokio::test]
async fn accepted_tls_connections_share_the_fixed_admission_limit() -> TestResult {
    cases::admission_limit(testkit::MemoryProvider::default()).await
}
