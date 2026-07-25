use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use auth::{AccessGrant, Permission, ResourceScope, ResourceScopeError, SharedAccessPolicy};
use amqp_runtime::{
    acceptor::{SaslAcceptor, sasl_acceptor::SaslServerFrame},
    types::{
        primitives::{Array, Symbol},
        sasl::{SaslCode, SaslInit, SaslOutcome, SaslResponse},
    },
};
use tokio::sync::{Mutex, Notify, RwLock, mpsc};

use crate::cbs::CbsResponse;

const DEFAULT_CBS_AUTHORIZATION_TIMEOUT: Duration = Duration::from_secs(20);
const CBS_REPLY_ROUTE_TIMEOUT: Duration = Duration::from_secs(2);
const CBS_REPLY_BUFFER: usize = 16;
const MICROSOFT_CBS_SASL_MECHANISM: &str = "MSSBCBS";

#[derive(Clone, Debug)]
pub struct SharedAccessAuthentication {
    policy: SharedAccessPolicy,
    audience_host: String,
    authorization_timeout: Duration,
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
        })
    }

    pub fn with_authorization_timeout(mut self, timeout: Duration) -> Self {
        self.authorization_timeout = timeout;
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
    audience_host: String,
    authorization_timeout: Duration,
    grants: RwLock<Vec<AccessGrant>>,
    grant_changed: Notify,
    routes: Mutex<ReplyRoutes>,
    route_changed: Notify,
}

impl ConnectionAuthorization {
    pub(crate) fn new(
        config: SharedAccessAuthentication,
        initial_grant: Option<AccessGrant>,
    ) -> Arc<Self> {
        Arc::new(Self {
            policy: config.policy,
            audience_host: config.audience_host,
            authorization_timeout: config.authorization_timeout,
            grants: RwLock::new(initial_grant.into_iter().collect()),
            grant_changed: Notify::new(),
            routes: Mutex::new(ReplyRoutes {
                senders: HashMap::new(),
            }),
            route_changed: Notify::new(),
        })
    }

    pub(crate) fn authorization_timeout(&self) -> Duration {
        self.authorization_timeout
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

    pub(crate) async fn validate_and_add(
        &self,
        token: &str,
        audience: &str,
    ) -> Result<(), auth::SasError> {
        let grant = self.policy.validate_sas(token, audience, epoch_seconds())?;
        let mut grants = self.grants.write().await;
        grants.retain(|existing| {
            existing.subject() != grant.subject() || existing.scope() != grant.scope()
        });
        grants.push(grant);
        drop(grants);
        self.grant_changed.notify_waiters();
        Ok(())
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

    pub(crate) async fn authorize_resource(
        &self,
        resource: &ResourceScope,
        permission: Permission,
    ) -> Result<(), AuthorizationError> {
        if self
            .grants
            .read()
            .await
            .iter()
            .any(|grant| grant.allows(resource, permission, epoch_seconds()))
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
        loop {
            let changed = self.grant_changed.notified();
            let now = epoch_seconds();
            let expiry = self
                .grants
                .read()
                .await
                .iter()
                .filter(|grant| {
                    grant.scope().contains(resource) && grant.permissions().allows(permission)
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

impl SaslAcceptor for SharedAccessSaslAcceptor {
    fn mechanisms(&self) -> Array<Symbol> {
        Array::from(vec![
            Symbol::from(MICROSOFT_CBS_SASL_MECHANISM),
            Symbol::from("ANONYMOUS"),
            Symbol::from("PLAIN"),
        ])
    }

    fn on_init(&mut self, init: SaslInit) -> SaslServerFrame {
        let code = match init.mechanism.as_str() {
            MICROSOFT_CBS_SASL_MECHANISM | "ANONYMOUS" => SaslCode::Ok,
            "PLAIN" => {
                self.validate_plain(init.initial_response.as_ref().map(|value| value.as_slice()))
            }
            _ => SaslCode::Auth,
        };
        SaslServerFrame::Outcome(SaslOutcome {
            code,
            additional_data: None,
        })
    }

    fn on_response(&mut self, _response: SaslResponse) -> SaslServerFrame {
        SaslServerFrame::Outcome(SaslOutcome {
            code: SaslCode::Sys,
            additional_data: None,
        })
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
