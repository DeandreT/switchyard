use std::time::{SystemTime, UNIX_EPOCH};

use auth::{JwtError, JwtPolicy, ResourceScope};

use super::{ConnectionAuthorization, SharedAccessAuthentication};

// This fact is private to the protocol adapter. Only successful TLS handshake
// branches supply TlsEstablished in production; HTTP/WebSocket flags cannot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectionTransport {
    Plaintext,
    TlsEstablished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JwtAuthorizationError {
    Disabled,
    TransportRequired,
    InvalidAudience,
    ClockUnavailable,
    InvalidToken(JwtError),
}

impl From<JwtError> for JwtAuthorizationError {
    fn from(error: JwtError) -> Self {
        Self::InvalidToken(error)
    }
}

fn checked_epoch_seconds() -> Result<u64, JwtAuthorizationError> {
    checked_epoch_seconds_at(SystemTime::now())
}

fn checked_epoch_seconds_at(now: SystemTime) -> Result<u64, JwtAuthorizationError> {
    now.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| JwtAuthorizationError::ClockUnavailable)
}

impl SharedAccessAuthentication {
    /// Also accepts the narrow offline JWT profile through CBS on a TLS listener.
    ///
    /// The existing shared-access policy and PLAIN behavior remain unchanged.
    /// Keys and rights are local, immutable policy inputs. This adds no discovery,
    /// cloud identity integration, JWT-only constructor or CLI startup flag.
    /// The existing policy may be empty; JWT then supplies the configured grants.
    pub fn with_offline_jwt_policy(mut self, policy: JwtPolicy) -> Self {
        self.offline_jwt_policy = Some(policy);
        self
    }

    pub(crate) fn requires_tls(&self) -> bool {
        self.offline_jwt_policy.is_some()
    }
}

impl ConnectionAuthorization {
    pub(crate) fn new_on_transport(
        config: SharedAccessAuthentication,
        initial_grant: Option<auth::AccessGrant>,
        transport: ConnectionTransport,
    ) -> std::sync::Arc<Self> {
        let mut authorization = Self::new(config, initial_grant);
        std::sync::Arc::get_mut(&mut authorization)
            .expect("fresh unpublished connection authorization")
            .transport = transport;
        authorization
    }

    pub(crate) async fn validate_jwt_and_add(
        &self,
        token: &str,
        audience: &str,
    ) -> Result<(), JwtAuthorizationError> {
        self.validate_jwt_with_clock(token, audience, checked_epoch_seconds)
            .await
    }

    async fn validate_jwt_with_clock<F: Fn() -> Result<u64, JwtAuthorizationError>>(
        &self,
        token: &str,
        audience: &str,
        clock: F,
    ) -> Result<(), JwtAuthorizationError> {
        let policy = self
            .offline_jwt_policy
            .as_ref()
            .ok_or(JwtAuthorizationError::Disabled)?;
        if self.transport != ConnectionTransport::TlsEstablished {
            return Err(JwtAuthorizationError::TransportRequired);
        }
        let requested = ResourceScope::parse(audience)
            .map_err(|_| JwtAuthorizationError::InvalidAudience)?
            .into_amqp_scope();
        if requested.host() != self.audience_host {
            return Err(JwtAuthorizationError::InvalidAudience);
        }
        policy.validate(token, &requested, clock()?)?;
        let mut grants = self.grants.write().await;
        let mut initial_control = self.initial_control.lock().await;
        // Waiting for either lock may outlive the token or a clock adjustment.
        // Revalidate before changing either grant storage or control history.
        let grant = policy.validate(token, &requested, clock()?)?;
        grants.retain(|existing| {
            !existing.same_principal(&grant) || existing.scope() != grant.scope()
        });
        grants.push(grant);
        initial_control.authorized(tokio::time::Instant::now());
        drop(initial_control);
        drop(grants);
        self.grant_changed.notify_waiters();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
