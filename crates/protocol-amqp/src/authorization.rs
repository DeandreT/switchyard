use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use amqp::{SaslAuthenticator, SaslCode, SaslInit};
use auth::{
    AccessGrant, JwtPolicy, Permission, ResourceScope, ResourceScopeError, SharedAccessPolicy,
};
use serde_amqp::primitives::Symbol;
use tokio::sync::{Mutex, Notify, RwLock, mpsc};

use crate::cbs::CbsResponse;

const DEFAULT_CBS_AUTHORIZATION_TIMEOUT: Duration = Duration::from_secs(20);
const CBS_REPLY_ROUTE_TIMEOUT: Duration = Duration::from_secs(2);
const CBS_REPLY_BUFFER: usize = 16;
const MICROSOFT_CBS_SASL_MECHANISM: &str = "MSSBCBS";
// SAS expiry values can exceed the timer's representable Instant range. Bound
// each sleep, not the grant interval, and recheck authorization after waking.
const MAX_AUTHORIZATION_WAIT: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug)]
pub struct SharedAccessAuthentication {
    policy: SharedAccessPolicy,
    audience_host: String,
    authorization_timeout: Duration,
    jwt_policy: Option<JwtPolicy>,
}

impl SharedAccessAuthentication {
    #[cfg(feature = "workload-tls")]
    pub(crate) fn audience_host(&self) -> &str {
        &self.audience_host
    }
    pub fn new(
        policy: SharedAccessPolicy,
        audience_host: impl AsRef<str>,
    ) -> Result<Self, ResourceScopeError> {
        let namespace = ResourceScope::namespace(audience_host)?;
        Ok(Self {
            policy,
            audience_host: namespace.host().to_owned(),
            authorization_timeout: DEFAULT_CBS_AUTHORIZATION_TIMEOUT,
            jwt_policy: None,
        })
    }

    pub fn with_authorization_timeout(mut self, timeout: Duration) -> Self {
        self.authorization_timeout = timeout;
        self
    }

    /// Accepts the local offline JWT profile alongside SAS on connections that
    /// this library actually admits through TLS. Plain TCP/WebSocket connections
    /// still refuse JWT, even when this policy is configured. Rights come only
    /// from the policy's verified local bindings, never from token role claims.
    pub fn with_jwt_policy(mut self, policy: JwtPolicy) -> Self {
        self.jwt_policy = Some(policy);
        self
    }
}

#[derive(Debug)]
struct ReplyRoutes {
    senders: HashMap<String, mpsc::Sender<CbsResponse>>,
}

#[derive(Debug)]
pub(crate) struct ConnectionAuthorization {
    policy: SharedAccessPolicy,
    jwt_policy: Option<JwtPolicy>,
    audience_host: String,
    authorization_timeout: Duration,
    grants: RwLock<Vec<AccessGrant>>,
    grant_changed: Notify,
    routes: Mutex<ReplyRoutes>,
    route_changed: Notify,
    #[cfg(feature = "workload-tls")]
    workload: Option<crate::workload::WorkloadPeer>,
}

impl ConnectionAuthorization {
    #[cfg(test)]
    pub(crate) fn new(
        config: SharedAccessAuthentication,
        initial_grant: Option<AccessGrant>,
    ) -> Arc<Self> {
        Self::new_for_transport(config, initial_grant, None)
    }

    #[cfg(any(not(feature = "workload-tls"), test))]
    pub(crate) fn new_for_transport(
        config: SharedAccessAuthentication,
        initial_grant: Option<AccessGrant>,
        tls: Option<crate::listener::service::TlsAdmission>,
    ) -> Arc<Self> {
        Self::prepare(config, initial_grant, tls)
    }

    fn prepare(
        config: SharedAccessAuthentication,
        initial_grant: Option<AccessGrant>,
        tls: Option<crate::listener::service::TlsAdmission>,
    ) -> Arc<Self> {
        Arc::new(Self {
            policy: config.policy,
            jwt_policy: tls.and(config.jwt_policy),
            audience_host: config.audience_host,
            authorization_timeout: config.authorization_timeout,
            grants: RwLock::new(initial_grant.into_iter().collect()),
            grant_changed: Notify::new(),
            routes: Mutex::new(ReplyRoutes {
                senders: HashMap::new(),
            }),
            route_changed: Notify::new(),
            #[cfg(feature = "workload-tls")]
            workload: None,
        })
    }

    #[cfg(feature = "workload-tls")]
    pub(crate) fn new_for_workload_transport(
        config: SharedAccessAuthentication,
        initial: Option<AccessGrant>,
        tls: Option<crate::listener::service::TlsAdmission>,
        workload: Option<crate::workload::WorkloadPeer>,
    ) -> Arc<Self> {
        let mut authorization = Self::prepare(config, initial, tls);
        Arc::get_mut(&mut authorization)
            .expect("new connection authorization is exclusive")
            .workload = workload;
        authorization
    }

    fn session_live(&self) -> bool {
        #[cfg(feature = "workload-tls")]
        if let Some(peer) = &self.workload {
            return peer.is_live();
        }
        true
    }

    fn workload_grant_live(&self) -> bool {
        #[cfg(feature = "workload-tls")]
        if let Some(peer) = &self.workload {
            return peer.is_live();
        }
        false
    }

    fn workload_allows(&self, resource: &ResourceScope, permissions: &[Permission]) -> bool {
        #[cfg(feature = "workload-tls")]
        if let Some(peer) = &self.workload {
            return permissions
                .iter()
                .any(|permission| peer.allows(resource, *permission));
        }
        let _ = (resource, permissions);
        false
    }

    async fn session_expired(&self) {
        #[cfg(feature = "workload-tls")]
        if let Some(peer) = &self.workload {
            return peer.expired().await;
        }
        std::future::pending().await
    }

    pub(crate) fn authorization_timeout(&self) -> Duration {
        self.authorization_timeout
    }

    pub(crate) fn audience_host(&self) -> &str {
        &self.audience_host
    }

    pub(crate) async fn has_valid_grant(&self) -> bool {
        self.has_valid_grant_with_clock(epoch_seconds).await
    }

    #[cfg(test)]
    async fn has_valid_grant_at(&self, now: u64) -> bool {
        self.has_valid_grant_with_clock(|| now).await
    }

    async fn has_valid_grant_with_clock(&self, now: impl Fn() -> u64) -> bool {
        let grants = self.grants.read().await;
        let now = now(); // All freshness checks follow the potentially blocked lock.
        self.session_live()
            && (self.workload_grant_live() || grants.iter().any(|grant| grant.is_valid_at(now)))
    }

    pub(crate) async fn wait_for_grant(&self) {
        loop {
            let changed = self.grant_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.has_valid_grant().await {
                return;
            }
            tokio::select! { () = changed => {}, () = self.session_expired() => std::future::pending().await }
        }
    }

    #[cfg(test)]
    pub(crate) async fn validate_and_add(
        &self,
        token: &str,
        audience: &str,
    ) -> Result<(), auth::SasError> {
        let grant = self.policy.validate_sas(token, audience, epoch_seconds())?;
        self.add_verified_grant(grant).await;
        Ok(())
    }

    pub(crate) async fn validate_token_and_add(
        &self,
        token_type: &str,
        token: &str,
        resource: &str,
    ) -> Result<(), AuthorizationError> {
        let grant = self.verify_token(token_type, token, resource, epoch_seconds())?;
        self.try_add_verified_grant(grant).await
    }

    fn verify_token(
        &self,
        token_type: &str,
        token: &str,
        resource: &str,
        now: u64,
    ) -> Result<AccessGrant, AuthorizationError> {
        match token_type {
            crate::cbs::SAS_TOKEN_TYPE => self
                .policy
                .validate_sas(token, resource, now)
                .map_err(|_| AuthorizationError),
            crate::cbs::JWT_TOKEN_TYPE => {
                let policy = self.jwt_policy.as_ref().ok_or(AuthorizationError)?;
                let resource = ResourceScope::parse(resource).map_err(|_| AuthorizationError)?;
                policy
                    .validate(token, &resource, now)
                    .map_err(|_| AuthorizationError)
            }
            _ => Err(AuthorizationError),
        }
    }

    // Accept only grants produced by an authentication policy's verification path.
    #[cfg(test)]
    async fn add_verified_grant(&self, grant: AccessGrant) {
        self.try_add_verified_grant(grant)
            .await
            .expect("test grant admission");
    }

    async fn try_add_verified_grant(&self, grant: AccessGrant) -> Result<(), AuthorizationError> {
        let mut grants = self.grants.write().await;
        if !self.session_live() {
            return Err(AuthorizationError);
        }
        grants.retain(|existing| {
            !existing.same_principal(&grant) || existing.scope() != grant.scope()
        });
        grants.push(grant);
        drop(grants);
        self.grant_changed.notify_waiters();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn grant_write_lock(
        &self,
    ) -> tokio::sync::RwLockWriteGuard<'_, Vec<AccessGrant>> {
        self.grants.write().await
    }

    #[cfg(test)]
    pub(crate) async fn grant_snapshot(&self) -> Vec<AccessGrant> {
        self.grants.read().await.clone()
    }

    pub(crate) async fn authorize_entity(
        &self,
        entity_path: &str,
        permission: Permission,
    ) -> Result<ResourceScope, AuthorizationError> {
        let resource = ResourceScope::entity(&self.audience_host, entity_path)
            .map_err(|_| AuthorizationError)?;
        self.authorize_resource(&resource, permission).await?;
        Ok(resource)
    }

    pub(crate) async fn authorize_entity_any(
        &self,
        entity_path: &str,
        permissions: &[Permission],
    ) -> Result<ResourceScope, AuthorizationError> {
        let resource = ResourceScope::entity(&self.audience_host, entity_path)
            .map_err(|_| AuthorizationError)?;
        self.authorize_resource_any(&resource, permissions).await?;
        Ok(resource)
    }

    pub(crate) async fn authorize_resource(
        &self,
        resource: &ResourceScope,
        permission: Permission,
    ) -> Result<(), AuthorizationError> {
        self.authorize_resource_any(resource, &[permission]).await
    }

    pub(crate) async fn authorize_resource_any(
        &self,
        resource: &ResourceScope,
        permissions: &[Permission],
    ) -> Result<(), AuthorizationError> {
        self.authorize_resource_any_with_clock(resource, permissions, epoch_seconds)
            .await
    }

    #[cfg(test)]
    async fn authorize_resource_any_at(
        &self,
        resource: &ResourceScope,
        permissions: &[Permission],
        now: u64,
    ) -> Result<(), AuthorizationError> {
        self.authorize_resource_any_with_clock(resource, permissions, || now)
            .await
    }

    async fn authorize_resource_any_with_clock(
        &self,
        resource: &ResourceScope,
        permissions: &[Permission],
        now: impl Fn() -> u64,
    ) -> Result<(), AuthorizationError> {
        let grants = self.grants.read().await;
        let now = now();
        if self.session_live()
            && (self.workload_allows(resource, permissions)
                || grants.iter().any(|grant| {
                    permissions
                        .iter()
                        .any(|permission| grant.allows(resource, *permission, now))
                }))
        {
            Ok(())
        } else {
            Err(AuthorizationError)
        }
    }

    pub(crate) async fn wait_until_unauthorized(
        &self,
        resource: &ResourceScope,
        permission: Permission,
    ) {
        self.wait_until_unauthorized_any(resource, &[permission])
            .await;
    }

    pub(crate) async fn wait_until_unauthorized_any(
        &self,
        resource: &ResourceScope,
        permissions: &[Permission],
    ) {
        self.wait_until_unauthorized_with_clock(resource, permissions, epoch_seconds)
            .await;
    }

    async fn wait_until_unauthorized_with_clock(
        &self,
        resource: &ResourceScope,
        permissions: &[Permission],
        now: impl Fn() -> u64,
    ) {
        loop {
            let changed = self.grant_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let grants = self.grants.read().await;
            let now = now();
            if !self.session_live() {
                return;
            }
            let workload = self.workload_allows(resource, permissions);
            let expiry = grants
                .iter()
                .filter(|grant| {
                    permissions
                        .iter()
                        .any(|permission| grant.allows(resource, *permission, now))
                })
                .map(AccessGrant::expires_at_epoch_seconds)
                .max();
            if expiry.is_none() && !workload {
                return;
            }
            let expiry = expiry.unwrap_or(u64::MAX);
            drop(grants);
            tokio::select! {
                () = tokio::time::sleep(
                    Duration::from_secs(expiry.saturating_sub(now)).min(MAX_AUTHORIZATION_WAIT),
                ) => {}
                () = changed => {}
                () = self.session_expired() => return,
            }
        }
    }

    pub(crate) async fn register_reply_route(
        &self,
        address: String,
    ) -> (mpsc::Sender<CbsResponse>, mpsc::Receiver<CbsResponse>) {
        let (sender, receiver) = mpsc::channel(CBS_REPLY_BUFFER);
        self.routes
            .lock()
            .await
            .senders
            .insert(address, sender.clone());
        self.route_changed.notify_waiters();
        (sender, receiver)
    }

    pub(crate) async fn unregister_reply_route(
        &self,
        address: &str,
        sender: &mpsc::Sender<CbsResponse>,
    ) {
        let mut routes = self.routes.lock().await;
        if routes
            .senders
            .get(address)
            .is_some_and(|current| current.same_channel(sender))
        {
            routes.senders.remove(address);
        }
    }

    #[cfg(test)]
    pub(crate) async fn reply_route_lock(&self) -> impl Send + '_ {
        self.routes.lock().await
    }

    pub(crate) async fn route_response(
        &self,
        address: &str,
        response: CbsResponse,
    ) -> Result<(), RouteError> {
        let deadline = tokio::time::Instant::now() + CBS_REPLY_ROUTE_TIMEOUT;
        loop {
            let changed = self.route_changed.notified();
            let route = self.routes.lock().await.senders.get(address).cloned();
            if let Some(route) = route {
                return route.send(response).await.map_err(|_| RouteError);
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|_| RouteError)?;
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SharedAccessSaslAcceptor {
    policy: SharedAccessPolicy,
    grant: Arc<std::sync::Mutex<Option<AccessGrant>>>,
}

impl SharedAccessSaslAcceptor {
    pub(crate) fn new(authentication: &SharedAccessAuthentication) -> Self {
        Self {
            policy: authentication.policy.clone(),
            grant: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub(crate) fn grant(&self) -> Option<AccessGrant> {
        self.grant.lock().ok()?.clone()
    }
}

impl SaslAuthenticator for SharedAccessSaslAcceptor {
    fn mechanisms(&self) -> Vec<Symbol> {
        vec![
            Symbol::from(MICROSOFT_CBS_SASL_MECHANISM),
            Symbol::from("ANONYMOUS"),
            Symbol::from("PLAIN"),
        ]
    }

    fn authenticate(&self, init: &SaslInit) -> SaslCode {
        match init.mechanism.as_str() {
            MICROSOFT_CBS_SASL_MECHANISM | "ANONYMOUS" => SaslCode::Ok,
            "PLAIN" => {
                self.validate_plain(init.initial_response.as_ref().map(|value| value.as_slice()))
            }
            _ => SaslCode::Auth,
        }
    }
}

impl SharedAccessSaslAcceptor {
    fn validate_plain(&self, response: Option<&[u8]>) -> SaslCode {
        let Some(response) = response else {
            return SaslCode::Auth;
        };
        let fields = response.split(|byte| *byte == 0).collect::<Vec<_>>();
        let [authzid, authcid, password] = fields.as_slice() else {
            return SaslCode::Auth;
        };
        if !authzid.is_empty() {
            return SaslCode::Auth;
        }
        let (Ok(authcid), Ok(password)) =
            (std::str::from_utf8(authcid), std::str::from_utf8(password))
        else {
            return SaslCode::Auth;
        };
        match self.policy.authenticate_plain(authcid, password) {
            Ok(grant) => {
                let Ok(mut outcome) = self.grant.lock() else {
                    return SaslCode::Sys;
                };
                *outcome = Some(grant);
                SaslCode::Ok
            }
            Err(_) => SaslCode::Auth,
        }
    }
}

fn epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AuthorizationError;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RouteError;

#[cfg(test)]
mod tests {
    use auth::{PermissionSet, SharedAccessKey, SharedAccessRule};

    use super::*;

    fn authorization(permissions: PermissionSet) -> Arc<ConnectionAuthorization> {
        let scope = ResourceScope::namespace("tenant.servicebus.windows.net").expect("scope");
        let rule = SharedAccessRule::new(
            "rule",
            scope,
            SharedAccessKey::new("secret").expect("key"),
            None,
            permissions,
        )
        .expect("rule");
        let policy = SharedAccessPolicy::new([rule]).expect("policy");
        let grant = policy.authenticate_plain("rule", "secret").expect("grant");
        ConnectionAuthorization::new(
            SharedAccessAuthentication::new(policy, "tenant.servicebus.windows.net")
                .expect("authentication"),
            Some(grant),
        )
    }

    #[tokio::test]
    async fn management_attachment_accepts_either_send_or_listen_without_widening_rights() {
        for (permissions, allowed, refused) in [
            (PermissionSet::SEND, Permission::Send, Permission::Listen),
            (PermissionSet::LISTEN, Permission::Listen, Permission::Send),
        ] {
            let authorization = authorization(permissions);
            let resource = authorization
                .authorize_entity_any("orders", &[Permission::Send, Permission::Listen])
                .await
                .expect("one management permission is enough to attach");
            assert!(
                authorization
                    .authorize_resource(&resource, allowed)
                    .await
                    .is_ok()
            );
            assert!(
                authorization
                    .authorize_resource(&resource, refused)
                    .await
                    .is_err(),
                "attaching a management link must not widen the grant"
            );
        }
    }

    #[tokio::test]
    async fn management_attachment_refuses_a_connection_with_neither_right() {
        let policy = SharedAccessPolicy::new([]).expect("empty policy");
        let authorization = ConnectionAuthorization::new(
            SharedAccessAuthentication::new(policy, "tenant.servicebus.windows.net")
                .expect("authentication"),
            None,
        );
        assert!(
            authorization
                .authorize_entity_any("orders", &[Permission::Send, Permission::Listen])
                .await
                .is_err()
        );
    }
}

#[cfg(test)]
mod grant_tests;
