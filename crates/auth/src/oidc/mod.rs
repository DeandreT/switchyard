//! Explicit bounded discovery for the Switchyard JWT profile, not generic OAuth.
//!
//! Refresh is an application-controlled future, never triggered by a token. No
//! worker is spawned: dropping that future synchronously drops its connection.
//! All clones share one cache and admission budget; no policy snapshots escape.

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::Deserialize;
use thiserror::Error;
use tokio::time::Instant;

use crate::{AccessGrant, JwtError, JwtPolicy, ResourceScope};

mod transport;
pub use transport::{HttpsTransport, PinnedEndpoint};

/// Hard limits apply in addition to these local timing choices.
#[derive(Clone, Copy, Debug)]
pub struct CachePolicy {
    pub maximum_freshness: Duration,
    pub refresh_interval: Duration,
    pub failure_backoff: Duration,
    /// One shared deadline for discovery, JWKS and publication, not per request.
    pub refresh_timeout: Duration,
}

impl Default for CachePolicy {
    fn default() -> Self {
        Self {
            maximum_freshness: Duration::from_secs(300),
            refresh_interval: Duration::from_secs(60),
            failure_backoff: Duration::from_secs(10),
            refresh_timeout: Duration::from_secs(5),
        }
    }
}

impl CachePolicy {
    fn check(self) -> Result<Self, OidcError> {
        if !(Duration::from_secs(1)..=Duration::from_secs(3600)).contains(&self.maximum_freshness)
            || !(Duration::from_secs(1)..=self.maximum_freshness).contains(&self.refresh_interval)
            || !(Duration::from_secs(1)..=Duration::from_secs(300)).contains(&self.failure_backoff)
            || !(Duration::from_millis(10)..=Duration::from_secs(30))
                .contains(&self.refresh_timeout)
        {
            return Err(OidcError::Configuration);
        }
        Ok(self)
    }
}

/// Static categories only: no URL, claims, token, peer or TLS diagnostic content.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OidcError {
    #[error("OIDC local configuration is invalid")]
    Configuration,
    #[error("OIDC destination is not permitted")]
    Destination,
    #[error("OIDC HTTPS transport failed")]
    Transport,
    #[error("OIDC response is outside the bounded HTTP profile")]
    Response,
    #[error("OIDC input exceeds its bounds")]
    TooLarge,
    #[error("OIDC JSON or signing-key set is invalid")]
    Document,
    #[error("OIDC discovery does not match local pins")]
    PinMismatch,
    #[error("OIDC refresh deadline expired")]
    Timeout,
    #[error("OIDC refresh is already in progress")]
    Busy,
    #[error("OIDC refresh is not due")]
    Backoff,
    #[error("OIDC signing-key cache is unavailable or expired")]
    Unavailable,
    #[error("OIDC cache state is unavailable")]
    State,
    #[error("OIDC JWT verification failed: {0}")]
    Jwt(#[from] JwtError),
}

struct State {
    cached: Option<(JwtPolicy, Instant)>,
    next_attempt: Instant,
    refreshing: bool,
}

struct Inner {
    local: JwtPolicy,
    transport: HttpsTransport,
    timing: CachePolicy,
    state: Mutex<State>,
}

/// A shared, initially closed verifier. The supplied offline policy contributes
/// only issuer/audience/local bindings; its keys do not bootstrap this cache.
#[derive(Clone)]
pub struct OidcVerifier(Arc<Inner>);

impl fmt::Debug for OidcVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OidcVerifier").finish_non_exhaustive()
    }
}

impl OidcVerifier {
    pub fn new(
        local: JwtPolicy,
        transport: HttpsTransport,
        timing: CachePolicy,
    ) -> Result<Self, OidcError> {
        let timing = timing.check()?;
        transport.check_issuer(local.issuer())?;
        Ok(Self(Arc::new(Inner {
            local,
            transport,
            timing,
            state: Mutex::new(State {
                cached: None,
                next_attempt: Instant::now(),
                refreshing: false,
            }),
        })))
    }

    /// No I/O, no async work, and no unknown-kid refresh. Cache freshness uses
    /// monotonic time independently of the caller's JWT epoch. The cache lock
    /// covers signature verification and the final freshness check, so a removed
    /// key cannot be used by a new call through a retained snapshot or clone.
    pub fn validate(
        &self,
        token: &str,
        requested: &ResourceScope,
        now_epoch_seconds: u64,
    ) -> Result<AccessGrant, OidcError> {
        let state = self.0.state.lock().map_err(|_| OidcError::State)?;
        let (policy, refreshed) = state.cached.as_ref().ok_or(OidcError::Unavailable)?;
        let expires = *refreshed + self.0.timing.maximum_freshness;
        if Instant::now() >= expires {
            return Err(OidcError::Unavailable);
        }
        let result = policy.validate(token, requested, now_epoch_seconds);
        if Instant::now() >= expires {
            return Err(OidcError::Unavailable);
        }
        result.map_err(OidcError::Jwt)
    }

    /// Explicit admission: at most one refresh, no waiting queue and no force
    /// bypass. Errors retain the previous set only until its original deadline.
    /// Cancellation also keeps the shared backoff and releases admission.
    pub async fn refresh(&self) -> Result<(), OidcError> {
        let started = Instant::now();
        {
            let mut state = self.0.state.lock().map_err(|_| OidcError::State)?;
            if state.refreshing {
                return Err(OidcError::Busy);
            }
            if started < state.next_attempt {
                return Err(OidcError::Backoff);
            }
            state.refreshing = true;
            // Conservative cancellation backoff; completion can shorten this.
            state.next_attempt =
                started + self.0.timing.refresh_timeout + self.0.timing.failure_backoff;
        }
        let _admission = Admission(&self.0);
        let deadline = started + self.0.timing.refresh_timeout;
        let result = tokio::time::timeout_at(deadline, self.fetch_policy())
            .await
            .map_err(|_| OidcError::Timeout)
            .and_then(|result| result);
        let mut state = self.0.state.lock().map_err(|_| OidcError::State)?;
        let now = Instant::now();
        let result = if now >= deadline {
            Err(OidcError::Timeout)
        } else {
            result
        };
        state.next_attempt = now
            + if result.is_ok() {
                self.0.timing.refresh_interval
            } else {
                self.0.timing.failure_backoff
            };
        if let Ok(policy) = &result {
            state.cached = Some((policy.clone(), now));
        }
        result.map(|_| ())
    }

    async fn fetch_policy(&self) -> Result<JwtPolicy, OidcError> {
        #[derive(Deserialize)]
        struct Discovery {
            issuer: String,
            jwks_uri: String,
        }
        let bytes = self.0.transport.discovery().await?;
        // Bound and reject duplicate names even in ignored provider metadata.
        let value = crate::jwt::bounded_json(&bytes, 2048).map_err(|_| OidcError::Document)?;
        if !value.is_object() {
            return Err(OidcError::Document);
        }
        let discovery: Discovery =
            serde_json::from_value(value).map_err(|_| OidcError::Document)?;
        if discovery.issuer != self.0.local.issuer()
            || !self.0.transport.matches_jwks(&discovery.jwks_uri)
        {
            return Err(OidcError::PinMismatch);
        }
        let bytes = self.0.transport.jwks().await?;
        self.0
            .local
            .replace_jwks(&bytes)
            .map_err(|_| OidcError::Document)
    }
}

struct Admission<'a>(&'a Inner);
impl Drop for Admission<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.refreshing = false;
        }
    }
}

#[cfg(test)]
mod tests;
