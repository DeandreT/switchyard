use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use http_body_util::{BodyExt, Empty};
use hyper::{Request, body::Bytes, client::conn::http1};
use hyper_util::rt::TokioIo;
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use url::Url;

use super::OidcError;

const MAX_URL: usize = 2048;
const MAX_BODY: usize = 64 * 1024;
const MAX_HTTP_BUFFER: usize = 8192;
const MAX_HEADERS: usize = 32;

/// An exact HTTPS URL and one operator-pinned dial IP. There is deliberately no
/// DNS resolver (including blocking system DNS), fallback address, redirect or
/// proxy. Host/SNI/certificate name remain the URL host, not the dial address.
/// Update local configuration when the issuer's approved address changes.
#[derive(Clone)]
pub struct PinnedEndpoint {
    url: Url,
    address: IpAddr,
}

impl fmt::Debug for PinnedEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedEndpoint").finish_non_exhaustive()
    }
}

impl PinnedEndpoint {
    pub fn new(url: &str, address: IpAddr) -> Result<Self, OidcError> {
        let url = checked_url(url)?;
        if !public_address(address) {
            return Err(OidcError::Destination);
        }
        Self::checked(url, address)
    }

    fn checked(url: Url, address: IpAddr) -> Result<Self, OidcError> {
        if let Some(host) = url.host() {
            let literal = match host {
                url::Host::Ipv4(ip) => Some(IpAddr::V4(ip)),
                url::Host::Ipv6(ip) => Some(IpAddr::V6(ip)),
                url::Host::Domain(_) => None,
            };
            if literal.is_some_and(|ip| ip != address) {
                return Err(OidcError::Destination);
            }
        }
        Ok(Self { url, address })
    }

    // Unit-fixture-only exception: absent from production builds/API/features.
    #[cfg(test)]
    pub(super) fn loopback_fixture(url: &str) -> Result<Self, OidcError> {
        Self::checked(checked_url(url)?, IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
    }
}

/// A dedicated HTTP/1 TLS client with explicit roots and no ambient credentials.
/// Both URLs and dial addresses are local pins; discovery cannot expand them.
pub struct HttpsTransport {
    discovery: PinnedEndpoint,
    jwks: PinnedEndpoint,
    tls: Arc<ClientConfig>,
}

impl fmt::Debug for HttpsTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpsTransport").finish_non_exhaustive()
    }
}

impl HttpsTransport {
    /// Only these 1-8 DER roots are trusted; machine trust is never consulted.
    pub fn new(
        discovery: PinnedEndpoint,
        jwks: PinnedEndpoint,
        roots: Vec<CertificateDer<'static>>,
    ) -> Result<Self, OidcError> {
        if roots.is_empty()
            || roots.len() > 8
            || roots.iter().map(|cert| cert.len()).sum::<usize>() > MAX_BODY
        {
            return Err(OidcError::Configuration);
        }
        let mut store = RootCertStore::empty();
        for root in roots {
            store.add(root).map_err(|_| OidcError::Configuration)?;
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut tls = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| OidcError::Configuration)?
            .with_root_certificates(store)
            .with_no_client_auth();
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        // No resumable TLS identity survives across refreshes.
        tls.resumption = rustls::client::Resumption::disabled();
        Ok(Self {
            discovery,
            jwks,
            tls: Arc::new(tls),
        })
    }

    pub(super) fn check_issuer(&self, issuer: &str) -> Result<(), OidcError> {
        let issuer_url = checked_url(issuer)?;
        let expected = checked_url(&format!(
            "{}/.well-known/openid-configuration",
            issuer.trim_end_matches('/')
        ))?;
        if self.discovery.url != expected || self.jwks.url.origin() != issuer_url.origin() {
            return Err(OidcError::Configuration);
        }
        Ok(())
    }

    pub(super) fn matches_jwks(&self, uri: &str) -> bool {
        uri == self.jwks.url.as_str()
    }
    pub(super) async fn discovery(&self) -> Result<Vec<u8>, OidcError> {
        self.get(&self.discovery).await
    }
    pub(super) async fn jwks(&self) -> Result<Vec<u8>, OidcError> {
        self.get(&self.jwks).await
    }

    async fn get(&self, endpoint: &PinnedEndpoint) -> Result<Vec<u8>, OidcError> {
        let host = endpoint.url.host_str().ok_or(OidcError::Configuration)?;
        // Url::host_str includes brackets for IPv6; rustls expects the bare IP.
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let name = ServerName::try_from(host.to_owned()).map_err(|_| OidcError::Configuration)?;
        let port = endpoint
            .url
            .port_or_known_default()
            .ok_or(OidcError::Configuration)?;
        let stream = TcpStream::connect(SocketAddr::new(endpoint.address, port))
            .await
            .map_err(|_| OidcError::Transport)?;
        let stream = TlsConnector::from(self.tls.clone())
            .connect(name, stream)
            .await
            .map_err(|_| OidcError::Transport)?;
        let (mut sender, connection) = http1::Builder::new()
            .max_headers(MAX_HEADERS)
            .max_buf_size(MAX_HTTP_BUFFER)
            .handshake(TokioIo::new(stream))
            .await
            .map_err(|_| OidcError::Transport)?;
        let authority = &endpoint.url[url::Position::BeforeHost..url::Position::AfterPort];
        let request = Request::builder()
            .method("GET")
            .uri(endpoint.url.path())
            .header("host", authority)
            .header("accept", "application/json")
            .header("accept-encoding", "identity")
            .header("connection", "close")
            .body(Empty::<Bytes>::new())
            .map_err(|_| OidcError::Configuration)?;
        let exchange = async {
            let response = sender
                .send_request(request)
                .await
                .map_err(|_| OidcError::Transport)?;
            let headers = response.headers();
            if response.status() != 200
                || headers.contains_key("content-encoding")
                || headers.get_all("content-type").iter().count() != 1
                || headers
                    .get("content-type")
                    .and_then(|h| h.to_str().ok())
                    .is_none_or(|h| {
                        !h.split(';')
                            .next()
                            .unwrap_or_default()
                            .trim()
                            .eq_ignore_ascii_case("application/json")
                    })
                || headers.get_all("content-length").iter().count() > 1
            {
                return Err(OidcError::Response);
            }
            if let Some(length) = headers.get("content-length") {
                let length = length
                    .to_str()
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .ok_or(OidcError::Response)?;
                if length > MAX_BODY as u64 {
                    return Err(OidcError::TooLarge);
                }
            }
            let mut body = response.into_body();
            let mut bytes = Vec::with_capacity(MAX_BODY);
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|_| OidcError::Transport)?;
                let data = frame.into_data().map_err(|_| OidcError::Response)?;
                if data.len() > MAX_BODY - bytes.len() {
                    return Err(OidcError::TooLarge);
                }
                bytes.extend_from_slice(&data);
            }
            Ok(bytes)
        };
        tokio::pin!(connection);
        tokio::pin!(exchange);
        // Drive Hyper in this same future, not a detached connection task. The
        // enclosing shared deadline/cancellation drops both and closes the socket.
        tokio::select! {
            biased;
            result = &mut exchange => result,
            result = &mut connection => {
                result.map_err(|_| OidcError::Transport)?;
                // A Connection: close peer can finish the driver on the same
                // poll that delivers the final response frame to the exchange.
                exchange.await
            },
        }
    }
}

fn checked_url(input: &str) -> Result<Url, OidcError> {
    if input.len() > MAX_URL
        || input
            .bytes()
            .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
    {
        return Err(OidcError::Configuration);
    }
    let url = Url::parse(input).map_err(|_| OidcError::Configuration)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.as_str().len() > MAX_URL
    {
        return Err(OidcError::Configuration);
    }
    Ok(url)
}

fn public_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            let [a, b, c, _] = address.octets();
            !(matches!(a, 0 | 10 | 127 | 224..=255)
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && (b == 168 || (b == 0 && matches!(c, 0 | 2))))
                || (a == 192 && b == 88 && c == 99)
                || (a == 198 && (matches!(b, 18 | 19) || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(address) => {
            let [a, b, ..] = address.segments();
            // Conservative global unicast only; refuse special-purpose 2001::/23,
            // documentation, 6to4, all mapped/compatible IPs and local ranges.
            a & 0xe000 == 0x2000
                && !(a == 0x2001 && (b < 0x0200 || b == 0x0db8))
                && a != 0x2002
                && !(a == 0x3fff && b < 0x1000)
        }
    }
}
