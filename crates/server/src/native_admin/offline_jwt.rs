use std::time::{SystemTime, UNIX_EPOCH};

use auth::{JwtError, JwtPolicy, Permission, ResourceScope, ResourceScopeError};
use tonic::{
    Request, Status,
    transport::server::{TcpConnectInfo, TlsConnectInfo},
};

const MAX_BEARER_TOKEN_BYTES: usize = 8 * 1024;
const MAX_AUTHORIZATION_BYTES: usize = MAX_BEARER_TOKEN_BYTES + "Bearer ".len();

#[derive(Clone)]
pub(super) struct Authentication {
    policy: JwtPolicy,
    scope: ResourceScope,
}

impl Authentication {
    pub(super) fn new(
        policy: JwtPolicy,
        audience_host: impl AsRef<str>,
    ) -> Result<Self, ResourceScopeError> {
        Ok(Self {
            policy,
            scope: ResourceScope::namespace(audience_host)?,
        })
    }

    // False selects the existing SAS branch; it never authorizes a request.
    pub(super) fn authorize<T>(
        &self,
        request: &Request<T>,
        entity_path: Option<&str>,
    ) -> Result<bool, Status> {
        let mut values = request.metadata().get_all("authorization").iter();
        let value = values
            .next()
            .ok_or_else(|| Status::unauthenticated("authorization credential required"))?;
        if values.next().is_some() || value.as_encoded_bytes().len() > MAX_AUTHORIZATION_BYTES {
            return Err(Status::unauthenticated("invalid authorization credential"));
        }
        let value = value
            .to_str()
            .map_err(|_| Status::unauthenticated("invalid authorization credential"))?;
        if value.starts_with("SharedAccessSignature ") {
            return Ok(false);
        }
        let token = value
            .strip_prefix("Bearer ")
            .filter(|token| {
                !token.is_empty()
                    && token.len() <= MAX_BEARER_TOKEN_BYTES
                    && !token.bytes().any(|byte| byte.is_ascii_whitespace())
            })
            .ok_or_else(|| Status::unauthenticated("invalid authorization credential"))?;
        // This extension is supplied by the actual Tonic TLS transport, not metadata.
        // In-process callers remain trusted; cloned extensions are not an attestation API.
        if request
            .extensions()
            .get::<TlsConnectInfo<TcpConnectInfo>>()
            .is_none()
        {
            return Err(Status::unauthenticated(
                "offline JWT requires a TLS transport",
            ));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::unavailable("authentication clock unavailable"))?
            .as_secs();
        let scope = match entity_path {
            Some(path) => ResourceScope::entity(self.scope.host(), path)
                .map_err(|_| Status::invalid_argument("invalid entity resource path"))?,
            None => self.scope.clone(),
        };
        let grant = self
            .policy
            .validate_native_scope(token, &scope, now)
            .map_err(|error| match error {
                JwtError::ScopeMismatch => {
                    Status::permission_denied("entity management permission required")
                }
                _ => Status::unauthenticated("invalid or expired offline JWT"),
            })?;
        if !grant.allows(&scope, Permission::Manage, now) {
            return Err(Status::permission_denied(
                "entity management permission required",
            ));
        }
        Ok(true)
    }
}
