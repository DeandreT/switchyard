use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use amqp::{SaslAuthenticator, SaslCode, SaslInit};
use auth::{AccessGrant, Permission, ResourceScope, ResourceScopeError, SharedAccessPolicy};
use serde_amqp::primitives::Symbol;
use tokio::sync::{Mutex, Notify, RwLock, mpsc};

use crate::cbs::CbsResponse;

mod initial;
mod jwt;
pub(crate) use jwt::ConnectionTransport;

use initial::InitialControlGrace;
pub(crate) use initial::InitialControlState;

const DEFAULT_CBS_AUTHORIZATION_TIMEOUT: Duration = Duration::from_secs(20);
const CBS_REPLY_ROUTE_TIMEOUT: Duration = Duration::from_secs(2);
const CBS_REPLY_BUFFER: usize = 16;
const MICROSOFT_CBS_SASL_MECHANISM: &str = "MSSBCBS";

#[derive(Clone, Debug)]
pub struct SharedAccessAuthentication {
    policy: SharedAccessPolicy,
    audience_host: String,
    authorization_timeout: Duration,
    offline_jwt_policy: Option<auth::JwtPolicy>,
}

impl SharedAccessAuthentication {
    pub fn new(
        policy: SharedAccessPolicy,
        audience_host: impl AsRef<str>,
    ) -> Result<Self, ResourceScopeError> {
        let namespace = ResourceScope::namespace(audience_host)?;
        Ok(Self {
            policy,
            audience_host: namespace.host().to_owned(),
            authorization_timeout: DEFAULT_CBS_AUTHORIZATION_TIMEOUT,
            offline_jwt_policy: None,
        })
    }

    pub fn with_authorization_timeout(mut self, timeout: Duration) -> Self {
        self.authorization_timeout = timeout;
        self
    }

    pub fn policy(&self) -> &SharedAccessPolicy {
        &self.policy
    }

    pub fn audience_host(&self) -> &str {
        &self.audience_host
    }
}

#[derive(Debug)]
struct ReplyRoutes {
    senders: HashMap<String, mpsc::Sender<CbsResponse>>,
}

#[derive(Debug)]
pub(crate) struct ConnectionAuthorization {
    policy: SharedAccessPolicy,
    audience_host: String,
    authorization_timeout: Duration,
    offline_jwt_policy: Option<auth::JwtPolicy>,
    transport: ConnectionTransport,
    grants: RwLock<Vec<AccessGrant>>,
    initial_control: Mutex<InitialControlGrace>,
    grant_changed: Notify,
    routes: Mutex<ReplyRoutes>,
    route_changed: Notify,
}

impl ConnectionAuthorization {
    pub(crate) fn new(
        config: SharedAccessAuthentication,
        initial_grant: Option<AccessGrant>,
    ) -> Arc<Self> {
        let previously_authorized = initial_grant.is_some();
        Arc::new(Self {
            policy: config.policy,
            audience_host: config.audience_host,
            authorization_timeout: config.authorization_timeout,
            offline_jwt_policy: config.offline_jwt_policy,
            transport: ConnectionTransport::Plaintext,
            grants: RwLock::new(
                initial_grant
                    .into_iter()
                    .map(AccessGrant::into_amqp_scope)
                    .collect(),
            ),
            initial_control: Mutex::new(InitialControlGrace::new(previously_authorized)),
            grant_changed: Notify::new(),
            routes: Mutex::new(ReplyRoutes {
                senders: HashMap::new(),
            }),
            route_changed: Notify::new(),
        })
    }

    #[cfg(test)]
    pub(crate) async fn invalidate_grants_for_test(&self) {
        self.grants.write().await.clear();
        self.grant_changed.notify_waiters();
    }

    pub(crate) fn authorization_timeout(&self) -> Duration {
        self.authorization_timeout
    }

    /// Installs the explicit Messaging deadline once; other listeners do not call this.
    pub(crate) async fn enable_initial_control_grace(&self, deadline: tokio::time::Instant) {
        let _grants = self.grants.read().await;
        self.initial_control
            .lock()
            .await
            .enable(deadline, tokio::time::Instant::now());
    }

    pub(crate) async fn initial_control_state(&self) -> InitialControlState {
        self.initial_control
            .lock()
            .await
            .state(tokio::time::Instant::now())
    }

    /// Declaration metadata grace does not authorize queue access or commit.
    pub(crate) async fn can_control_metadata(&self) -> bool {
        let grants = self.grants.read().await;
        let state = self
            .initial_control
            .lock()
            .await
            .state(tokio::time::Instant::now());
        match state {
            InitialControlState::Initial { .. } => true,
            InitialControlState::InitialExpired => false,
            InitialControlState::Disabled | InitialControlState::Authorized => {
                any_grant_claim_expiry_epoch_seconds_at(&grants, SystemTime::now()).is_ok()
            }
        }
    }

    pub(crate) async fn can_control_commit(&self) -> bool {
        let grants = self.grants.read().await;
        let state = self
            .initial_control
            .lock()
            .await
            .state(tokio::time::Instant::now());
        // A late token cannot revive a coordinator after initial expiry.
        state != InitialControlState::InitialExpired
            && any_grant_claim_expiry_epoch_seconds_at(&grants, SystemTime::now()).is_ok()
    }

    pub(crate) async fn wait_until_control_unauthorized(&self) {
        loop {
            let changed = self.grant_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.initial_control_state().await {
                InitialControlState::Initial { deadline } => {
                    tokio::select! {
                        () = tokio::time::sleep_until(deadline) => {}
                        () = &mut changed => {}
                    }
                }
                InitialControlState::InitialExpired => return,
                InitialControlState::Disabled | InitialControlState::Authorized => {
                    self.wait_until_no_valid_grant().await;
                    return;
                }
            }
        }
    }

    pub(crate) async fn has_valid_grant(&self) -> bool {
        let now = epoch_seconds();
        self.grants
            .read()
            .await
            .iter()
            .any(|grant| grant.expires_at_epoch_seconds() > now)
    }

    pub(crate) async fn wait_for_grant(&self) {
        loop {
            let changed = self.grant_changed.notified();
            if self.has_valid_grant().await {
                return;
            }
            changed.await;
        }
    }

    pub(crate) async fn wait_until_no_valid_grant(&self) {
        loop {
            let changed = self.grant_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let now = epoch_seconds();
            let expiry = self
                .grants
                .read()
                .await
                .iter()
                .map(AccessGrant::expires_at_epoch_seconds)
                .filter(|expiry| *expiry > now)
                .max();
            let Some(expiry) = expiry else { return };
            if expiry == u64::MAX {
                changed.await;
                continue;
            }
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(expiry.saturating_sub(now))) => {}
                () = &mut changed => {}
            }
        }
    }

    pub(crate) async fn validate_and_add(
        &self,
        token: &str,
        audience: &str,
    ) -> Result<(), auth::SasError> {
        let grant = self.policy.validate_sas(token, audience, epoch_seconds())?;
        let mut grants = self.grants.write().await;
        let mut initial_control = self.initial_control.lock().await;
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

    pub(crate) async fn authorize_entity(
        &self,
        entity_path: &str,
        permission: Permission,
    ) -> Result<ResourceScope, AuthorizationError> {
        self.authorize_entity_any(entity_path, &[permission]).await
    }

    pub(crate) async fn authorize_entity_any(
        &self,
        entity_path: &str,
        permissions: &[Permission],
    ) -> Result<ResourceScope, AuthorizationError> {
        let resource = ResourceScope::entity(&self.audience_host, entity_path)
            .map_err(|_| AuthorizationError)?
            .into_amqp_scope();
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
        let now = epoch_seconds();
        if self.grants.read().await.iter().any(|grant| {
            permissions
                .iter()
                .any(|permission| grant.allows(resource, *permission, now))
        }) {
            Ok(())
        } else {
            Err(AuthorizationError)
        }
    }

    /// Snapshots the longest-lived matching grant, not a revocation lease.
    pub(crate) async fn claim_expiry_epoch_seconds(
        &self,
        resource: &ResourceScope,
        permission: Permission,
    ) -> Result<u64, AuthorizationError> {
        let grants = self.grants.read().await;
        claim_expiry_epoch_seconds_at(&grants, resource, permission, SystemTime::now())
    }

    /// Empty work requires a currently valid connection grant of any permission.
    pub(crate) async fn any_grant_claim_expiry_epoch_seconds(
        &self,
    ) -> Result<u64, AuthorizationError> {
        let grants = self.grants.read().await;
        any_grant_claim_expiry_epoch_seconds_at(&grants, SystemTime::now())
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
        loop {
            let changed = self.grant_changed.notified();
            let now = epoch_seconds();
            let expiry = self
                .grants
                .read()
                .await
                .iter()
                .filter(|grant| {
                    grant.scope().contains(resource)
                        && permissions
                            .iter()
                            .any(|permission| grant.permissions().allows(*permission))
                })
                .map(AccessGrant::expires_at_epoch_seconds)
                .filter(|expiry| *expiry > now)
                .max();
            let Some(expiry) = expiry else { return };
            if expiry == u64::MAX {
                changed.await;
                continue;
            }

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(expiry.saturating_sub(now))) => {}
                () = changed => {}
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

fn claim_expiry_epoch_seconds_at(
    grants: &[AccessGrant],
    resource: &ResourceScope,
    permission: Permission,
    now: SystemTime,
) -> Result<u64, AuthorizationError> {
    let now = checked_epoch_seconds_at(now)?;
    grants
        .iter()
        .filter(|grant| grant.allows(resource, permission, now))
        .map(AccessGrant::expires_at_epoch_seconds)
        .max()
        .ok_or(AuthorizationError)
}

fn any_grant_claim_expiry_epoch_seconds_at(
    grants: &[AccessGrant],
    now: SystemTime,
) -> Result<u64, AuthorizationError> {
    let now = checked_epoch_seconds_at(now)?;
    grants
        .iter()
        .map(AccessGrant::expires_at_epoch_seconds)
        .filter(|expiry| *expiry > now)
        .max()
        .ok_or(AuthorizationError)
}

fn checked_epoch_seconds_at(now: SystemTime) -> Result<u64, AuthorizationError> {
    now.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .map_err(|_| AuthorizationError)
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AuthorizationError;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RouteError;

#[cfg(test)]
mod tests {
    use auth::{PermissionSet, SharedAccessKey, SharedAccessRule};

    use super::*;

    const HOST: &str = "tenant.servicebus.windows.net";
    const CLAIM_EXPIRY: u64 = 2_000_000_000;

    fn plain_claim_grant(path: &str, permissions: PermissionSet) -> AccessGrant {
        let policy = SharedAccessPolicy::new([SharedAccessRule::new(
            "claim-rule",
            ResourceScope::entity(HOST, path).expect("claim scope"),
            SharedAccessKey::new("claim-secret").expect("claim key"),
            None,
            permissions,
        )
        .expect("claim rule")])
        .expect("claim policy");
        policy
            .authenticate_plain("claim-rule", "claim-secret")
            .expect("authenticated claim grant")
            .into_amqp_scope()
    }

    fn finite_claim_grant(permissions: PermissionSet) -> AccessGrant {
        const TOKEN: &str = "SharedAccessSignature sr=amqps%3A%2F%2Ftenant.servicebus.windows.net%2Forders&sig=R8KtgcCb7NeOCrECrMXtQ13KLGC8CiJYw0fUnUQCznw%3D&se=2000000000&skn=send";
        let policy = SharedAccessPolicy::new([SharedAccessRule::new(
            "send",
            ResourceScope::namespace(HOST).expect("namespace scope"),
            SharedAccessKey::new("secret").expect("claim key"),
            None,
            permissions,
        )
        .expect("claim rule")])
        .expect("claim policy");
        policy
            .authenticate_sas(TOKEN, 0)
            .expect("authenticated finite claim grant")
            .into_amqp_scope()
    }

    fn claim_time(epoch_seconds: u64) -> SystemTime {
        UNIX_EPOCH
            .checked_add(Duration::from_secs(epoch_seconds))
            .expect("representable claim time")
    }

    fn connection(permissions: PermissionSet) -> Arc<ConnectionAuthorization> {
        let policy = SharedAccessPolicy::new([SharedAccessRule::new(
            "test-rule",
            ResourceScope::entity(HOST, "orders").expect("valid scope"),
            SharedAccessKey::new("test-secret").expect("valid key"),
            None,
            permissions,
        )
        .expect("valid rule")])
        .expect("valid policy");
        let grant = policy
            .authenticate_plain("test-rule", "test-secret")
            .expect("valid grant");
        ConnectionAuthorization::new(
            SharedAccessAuthentication::new(policy, HOST).expect("valid authentication"),
            Some(grant),
        )
    }

    #[tokio::test]
    async fn initial_plain_grants_use_explicit_amqp_controls_without_broadening_endpoints() {
        for (sdk, canonical) in [
            (
                "Orders/Subscriptions/Accounting",
                "Orders/subscriptions/Accounting",
            ),
            (
                "Orders/Subscriptions/Accounting/$Management",
                "Orders/subscriptions/Accounting/$management",
            ),
        ] {
            let literal = ResourceScope::entity(HOST, sdk).expect("literal rule scope");
            let expected = ResourceScope::entity(HOST, canonical).expect("canonical scope");
            let policy = SharedAccessPolicy::new([SharedAccessRule::new(
                "test-rule",
                literal.clone(),
                SharedAccessKey::new("test-secret").expect("key"),
                None,
                PermissionSet::MANAGE,
            )
            .expect("rule")])
            .expect("policy");
            let plain = policy
                .authenticate_plain("test-rule", "test-secret")
                .expect("PLAIN credential");
            assert_eq!(plain.scope(), &literal);
            assert!(!plain.allows(&expected, Permission::Manage, epoch_seconds()));
            let connection = ConnectionAuthorization::new(
                SharedAccessAuthentication::new(policy, HOST).expect("authentication"),
                Some(plain),
            );
            assert_eq!(connection.grants.read().await[0].scope(), &expected);
            for requested in [sdk, canonical] {
                assert_eq!(
                    connection
                        .authorize_entity(requested, Permission::Manage)
                        .await
                        .expect("AMQP control alias"),
                    expected
                );
            }
            for denied in [
                "Orders",
                "Orders/subscriptions/Billing",
                "orders/subscriptions/Accounting",
                "Orders/subscriptions/accounting",
            ] {
                assert!(
                    connection
                        .authorize_entity(denied, Permission::Manage)
                        .await
                        .is_err()
                );
            }
            if canonical.ends_with("/$management") {
                for denied in [
                    "Orders/subscriptions/Accounting",
                    "Orders/subscriptions/Accounting/$deadletterqueue",
                ] {
                    assert!(
                        connection
                            .authorize_entity(denied, Permission::Manage)
                            .await
                            .is_err()
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn cbs_control_aliases_replace_the_same_stored_grant_key() {
        const SDK_TOKEN: &str = "SharedAccessSignature sr=amqps%3A%2F%2Ftenant.servicebus.windows.net%2FOrders%2FSubscriptions%2FAccounting%2F%24Management&sig=EC1J/CJScR/nGDN3aGox0IJnJisSGuV780wvOcxCU1E=&se=2000000000&skn=test-rule";
        const CANONICAL_TOKEN: &str = "SharedAccessSignature sr=amqps%3A%2F%2Ftenant.servicebus.windows.net%2FOrders%2Fsubscriptions%2FAccounting%2F%24management&sig=XR8TnIBfC+cslXwvRG7lu8ZKbRfYP8+TG6aTXmZZtjU=&se=2000000000&skn=test-rule";
        let sdk = "Orders/Subscriptions/Accounting/$Management";
        let canonical = "Orders/subscriptions/Accounting/$management";
        let expected = ResourceScope::entity(HOST, canonical).expect("canonical scope");
        let policy = SharedAccessPolicy::new([SharedAccessRule::new(
            "test-rule",
            ResourceScope::entity(HOST, sdk).expect("literal rule scope"),
            SharedAccessKey::new("test-secret").expect("key"),
            None,
            PermissionSet::MANAGE,
        )
        .expect("rule")])
        .expect("policy");
        let connection = ConnectionAuthorization::new(
            SharedAccessAuthentication::new(policy, HOST).expect("authentication"),
            None,
        );
        for (token, requested) in [(SDK_TOKEN, canonical), (CANONICAL_TOKEN, sdk)] {
            connection
                .validate_and_add(token, &format!("amqps://{HOST}/{requested}"))
                .await
                .expect("valid original signed bytes");
            let grants = connection.grants.read().await;
            assert_eq!(grants.len(), 1, "semantic aliases renew one grant");
            assert_eq!(grants[0].scope(), &expected);
        }
        assert!(
            connection
                .authorize_entity(canonical, Permission::Manage)
                .await
                .is_ok()
        );
        assert!(
            connection
                .authorize_entity("Orders/subscriptions/Accounting", Permission::Manage)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn any_permission_accepts_send_listen_or_manage_without_broadening_scope() {
        for permission in [
            PermissionSet::SEND,
            PermissionSet::LISTEN,
            PermissionSet::MANAGE,
        ] {
            let connection = connection(permission);
            let requested = [Permission::Send, Permission::Listen];
            assert!(
                connection
                    .authorize_entity_any("orders", &requested)
                    .await
                    .is_ok()
            );
            assert!(
                connection
                    .authorize_entity_any("other", &requested)
                    .await
                    .is_err()
            );
            assert!(
                connection
                    .authorize_entity_any("orders", &[])
                    .await
                    .is_err()
            );
        }

        let connection = connection(PermissionSet::SEND);
        assert!(
            connection
                .authorize_entity("orders", Permission::Send)
                .await
                .is_ok()
        );
        assert!(
            connection
                .authorize_entity("orders", Permission::Listen)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn any_permission_expiry_waits_until_the_last_matching_grant_is_gone() {
        let connection = connection(PermissionSet::SEND);
        let resource = ResourceScope::entity(HOST, "orders").expect("valid scope");
        let requested = [Permission::Send, Permission::Listen];
        let waiting = connection.wait_until_unauthorized_any(&resource, &requested);
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waiting)
                .await
                .is_err()
        );

        connection.grants.write().await.clear();
        connection.grant_changed.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), &mut waiting)
            .await
            .expect("removing the grant ends the wait");
    }

    #[tokio::test]
    async fn unmatched_and_empty_permission_sets_are_immediately_unauthorized() {
        let connection = connection(PermissionSet::SEND);
        let resource = ResourceScope::entity(HOST, "orders").expect("valid scope");
        tokio::time::timeout(
            Duration::from_secs(1),
            connection.wait_until_unauthorized_any(&resource, &[Permission::Listen]),
        )
        .await
        .expect("an unmatched permission does not wait");
        tokio::time::timeout(
            Duration::from_secs(1),
            connection.wait_until_unauthorized_any(&resource, &[]),
        )
        .await
        .expect("an empty permission set does not wait");
    }

    #[tokio::test]
    async fn coordinator_wait_ends_only_after_the_last_valid_grant_is_gone() {
        let connection = connection(PermissionSet::LISTEN);
        let grant = connection.grants.read().await[0].clone();
        connection.grants.write().await.push(grant);
        let mut waiting = Box::pin(connection.wait_until_no_valid_grant());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(waiting.as_mut(), &mut context).is_pending());
        let _ = connection.grants.write().await.pop();
        connection.grant_changed.notify_waiters();
        assert!(std::future::Future::poll(waiting.as_mut(), &mut context).is_pending());
        connection.grants.write().await.clear();
        connection.grant_changed.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("the registered watcher observes grant removal");
        tokio::time::timeout(
            Duration::from_secs(1),
            connection.wait_until_no_valid_grant(),
        )
        .await
        .expect("a connection without grants is immediately unauthorized");
    }

    #[test]
    fn claim_snapshots_preserve_exact_resource_case_and_permission_boundaries() {
        let now = claim_time(1);
        let grants = [plain_claim_grant(
            "Orders/subscriptions/Accounting/$management",
            PermissionSet::SEND,
        )];
        let allowed = ResourceScope::entity(HOST, "Orders/subscriptions/Accounting/$management")
            .expect("exact resource")
            .into_amqp_scope();
        assert_eq!(
            claim_expiry_epoch_seconds_at(&grants, &allowed, Permission::Send, now)
                .expect("exact Send grant"),
            u64::MAX
        );
        for denied in [
            "Orders",
            "Orders/subscriptions/Accounting",
            "Orders/subscriptions/Billing/$management",
            "orders/subscriptions/Accounting/$management",
            "Orders/subscriptions/accounting/$management",
        ] {
            let resource = ResourceScope::entity(HOST, denied)
                .expect("denied resource")
                .into_amqp_scope();
            assert!(
                claim_expiry_epoch_seconds_at(&grants, &resource, Permission::Send, now).is_err()
            );
        }
        for permission in [Permission::Listen, Permission::Manage, Permission::Audit] {
            assert!(claim_expiry_epoch_seconds_at(&grants, &allowed, permission, now).is_err());
        }
    }

    #[test]
    fn claim_snapshot_uses_maximum_only_among_matching_live_grants() {
        let resource = ResourceScope::entity(HOST, "orders").expect("resource");
        let mut grants = vec![
            finite_claim_grant(PermissionSet::SEND),
            plain_claim_grant("orders", PermissionSet::LISTEN),
            plain_claim_grant("orders-archive", PermissionSet::SEND),
        ];
        let now = claim_time(CLAIM_EXPIRY - 1);
        assert_eq!(
            claim_expiry_epoch_seconds_at(&grants, &resource, Permission::Send, now)
                .expect("finite matching grant"),
            CLAIM_EXPIRY
        );
        grants.push(plain_claim_grant("orders", PermissionSet::MANAGE));
        for _ in 0..2 {
            assert_eq!(
                claim_expiry_epoch_seconds_at(&grants, &resource, Permission::Send, now)
                    .expect("overlapping Manage grant includes Send"),
                u64::MAX
            );
            grants.reverse();
        }
    }

    #[test]
    fn claim_snapshots_refuse_expiry_equality_expired_and_empty_grants() {
        let grants = [finite_claim_grant(PermissionSet::SEND)];
        let resource = ResourceScope::entity(HOST, "orders").expect("resource");
        assert_eq!(
            claim_expiry_epoch_seconds_at(
                &grants,
                &resource,
                Permission::Send,
                claim_time(CLAIM_EXPIRY - 1),
            )
            .expect("not expired"),
            CLAIM_EXPIRY
        );
        for now in [claim_time(CLAIM_EXPIRY), claim_time(CLAIM_EXPIRY + 1)] {
            assert!(
                claim_expiry_epoch_seconds_at(&grants, &resource, Permission::Send, now).is_err()
            );
            assert!(any_grant_claim_expiry_epoch_seconds_at(&grants, now).is_err());
        }
        assert!(
            claim_expiry_epoch_seconds_at(&[], &resource, Permission::Send, claim_time(0)).is_err()
        );
        assert!(any_grant_claim_expiry_epoch_seconds_at(&[], claim_time(0)).is_err());
    }

    #[test]
    fn empty_work_snapshot_accepts_any_live_grant_and_uses_its_maximum_expiry() {
        let resource = ResourceScope::entity(HOST, "other").expect("unmatched resource");
        let now = claim_time(CLAIM_EXPIRY - 1);
        let mut grants = vec![finite_claim_grant(PermissionSet::LISTEN)];
        assert!(claim_expiry_epoch_seconds_at(&grants, &resource, Permission::Send, now).is_err());
        assert_eq!(
            any_grant_claim_expiry_epoch_seconds_at(&grants, now).expect("any Listen grant"),
            CLAIM_EXPIRY
        );
        grants.push(plain_claim_grant("unrelated", PermissionSet::LISTEN));
        assert_eq!(
            any_grant_claim_expiry_epoch_seconds_at(&grants, now).expect("longest any grant"),
            u64::MAX
        );
    }

    #[test]
    fn claim_snapshots_fail_closed_when_epoch_time_cannot_be_represented() {
        let now = UNIX_EPOCH
            .checked_sub(Duration::from_nanos(1))
            .expect("representable pre-epoch time");
        let grants = [plain_claim_grant("orders", PermissionSet::MANAGE)];
        let resource = ResourceScope::entity(HOST, "orders").expect("resource");
        assert!(claim_expiry_epoch_seconds_at(&grants, &resource, Permission::Send, now).is_err());
        assert!(any_grant_claim_expiry_epoch_seconds_at(&grants, now).is_err());
    }

    #[tokio::test]
    async fn public_claim_snapshots_return_numbers_without_changing_existing_authorization() {
        let connection = connection(PermissionSet::SEND);
        let resource = ResourceScope::entity(HOST, "orders").expect("resource");
        let before = connection.grants.read().await.clone();
        assert_eq!(
            connection
                .claim_expiry_epoch_seconds(&resource, Permission::Send)
                .await
                .expect("current Send snapshot"),
            u64::MAX
        );
        assert_eq!(
            connection
                .any_grant_claim_expiry_epoch_seconds()
                .await
                .expect("current connection snapshot"),
            u64::MAX
        );
        assert!(
            connection
                .claim_expiry_epoch_seconds(&resource, Permission::Listen)
                .await
                .is_err()
        );
        assert!(
            connection
                .authorize_resource(&resource, Permission::Send)
                .await
                .is_ok()
        );
        assert!(
            connection
                .authorize_resource(&resource, Permission::Listen)
                .await
                .is_err()
        );
        assert_eq!(*connection.grants.read().await, before);
    }
}
