use std::{
    collections::HashMap,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{Arc, Mutex as StdMutex, Weak},
    time::Duration,
};

use amqp::{
    AmqpError, ApplicationProperties, Body, DeliveryState, EngineError, Error as AmqpProtocolError,
    Message, MessageId, Outcome, Receiver, Sender,
};
use auth::{Permission, ResourceScope};
use domain::{
    CommandKind, CommandOutcome, Delivery, EntityPath, LockToken, NamespaceName, SequenceNumber,
    SessionHold,
};
use futures_util::FutureExt;
use serde_amqp::{
    Value,
    primitives::{Array, Binary, OrderedMap, Timestamp as AmqpTimestamp, Uuid},
};
use tokio::{
    sync::{Mutex, Notify, RwLock, mpsc},
    time::Instant,
};
use tracing::debug;

use crate::listener::connection_custody::ConnectionRetirementRequest;
use crate::{Broker, BrokerRejection, authorization::ConnectionAuthorization};

mod custody;
mod deferred;
mod peek;
mod response;
mod rules;
mod scheduled;

use self::custody::{
    OperationControl, PanicPayload, PendingOperation, PumpPoint, ReplyCustody, RequestBroker,
    RequestCustody,
};
pub(crate) use self::response::ManagementResponse;

pub use deferred::{RECEIVE_BY_SEQUENCE_NUMBER_OPERATION, UPDATE_DISPOSITION_OPERATION};
pub use peek::PEEK_MESSAGE_OPERATION;
pub use rules::{ADD_RULE_OPERATION, ENUMERATE_RULES_OPERATION, REMOVE_RULE_OPERATION};
pub use scheduled::{CANCEL_SCHEDULED_MESSAGE_OPERATION, SCHEDULE_MESSAGE_OPERATION};

pub const RENEW_LOCK_OPERATION: &str = "com.microsoft:renew-lock";
pub const RENEW_SESSION_LOCK_OPERATION: &str = "com.microsoft:renew-session-lock";
pub const GET_SESSION_STATE_OPERATION: &str = "com.microsoft:get-session-state";
pub const SET_SESSION_STATE_OPERATION: &str = "com.microsoft:set-session-state";
pub const OPERATION_PROPERTY: &str = "operation";
pub const ASSOCIATED_LINK_NAME_PROPERTY: &str = "associated-link-name";
pub const STATUS_CODE_PROPERTY: &str = "statusCode";
pub const STATUS_DESCRIPTION_PROPERTY: &str = "statusDescription";
pub const ERROR_CONDITION_PROPERTY: &str = "errorCondition";
pub const TRACKING_ID_PROPERTY: &str = "com.microsoft:tracking-id";
pub const LOCK_TOKENS: &str = "lock-tokens";
pub const EXPIRATIONS: &str = "expirations";
pub const EXPIRATION: &str = "expiration";
pub const SESSION_ID: &str = "session-id";
pub const SESSION_STATE: &str = "session-state";

const REPLY_ROUTE_TIMEOUT: Duration = Duration::from_secs(2);
const REPLY_BUFFER: usize = 16;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DeliveryKey {
    link_name: String,
    lock_token: LockToken,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RequestResponseDeliveryKey {
    entity: EntityPath,
    lock_token: LockToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManagedDelivery {
    entity: EntityPath,
    sequence: SequenceNumber,
    /// Present for deliveries returned inside a management response. It is the
    /// sender envelope before broker overlays, needed if a later request asks
    /// to change application properties while settling the lock.
    delivery: Option<Delivery>,
}

#[derive(Clone, Debug)]
pub(crate) struct DeliveryRegistration {
    key: DeliveryKey,
    managed: ManagedDelivery,
    identity: Arc<()>,
}

impl DeliveryRegistration {
    pub(crate) fn lock_token(&self) -> LockToken {
        self.key.lock_token
    }
}

impl PartialEq for DeliveryRegistration {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity) && self.key == other.key
    }
}

impl Eq for DeliveryRegistration {}

#[derive(Clone, Debug)]
struct RequestResponseDeliveryRegistration {
    key: RequestResponseDeliveryKey,
    identity: Arc<()>,
}

impl PartialEq for RequestResponseDeliveryRegistration {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity) && self.key == other.key
    }
}

impl Eq for RequestResponseDeliveryRegistration {}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManagedDeliverySelection {
    managed: ManagedDelivery,
    ordinary: Option<DeliveryRegistration>,
    request_response: Option<RequestResponseDeliveryRegistration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RequestResponseDelivery {
    managed: ManagedDelivery,
    registration: RequestResponseDeliveryRegistration,
    /// A protocol-local deadline. Domain timestamps may come from a replay or
    /// test clock, so they are never compared with this process's wall clock.
    expires_at: Instant,
}

fn request_response_deadline(now: Instant, lock_duration_millis: u64) -> Instant {
    now.checked_add(Duration::from_millis(lock_duration_millis))
        // Queue validation caps lock durations well below Instant's range. If
        // a future caller violates that invariant, fail closed rather than
        // retaining an immortal registry entry.
        .unwrap_or(now)
}

fn purge_request_response_deliveries(
    deliveries: &mut HashMap<RequestResponseDeliveryKey, RequestResponseDelivery>,
    now: Instant,
) {
    deliveries.retain(|_, delivery| delivery.expires_at > now);
}

fn definitive_message_lock_loss(rejection: &BrokerRejection) -> bool {
    matches!(
        rejection,
        BrokerRejection::Refused(
            domain::BrokerError::MessageNotFound { .. }
                | domain::BrokerError::MessageNotLocked { .. }
                | domain::BrokerError::LockTokenMismatch { .. }
                | domain::BrokerError::LockExpired { .. }
        )
    )
}

#[derive(Debug)]
pub(crate) struct SessionClaim {
    management: Weak<ConnectionManagement>,
    link_name: String,
    entity: EntityPath,
    identity: Arc<()>,
}

impl Drop for SessionClaim {
    fn drop(&mut self) {
        if let Some(management) = self.management.upgrade() {
            let mut claims = management
                .session_claims
                .lock()
                .expect("the pending session claim lock is not poisoned");
            if claims
                .get(&self.link_name)
                .is_some_and(|identity| Arc::ptr_eq(identity, &self.identity))
            {
                claims.remove(&self.link_name);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SessionRegistration {
    link_name: String,
    entity: EntityPath,
    hold: SessionHold,
    identity: Arc<()>,
}

impl PartialEq for SessionRegistration {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity)
            && self.link_name == other.link_name
            && self.entity == other.entity
            && self.hold == other.hold
    }
}

impl Eq for SessionRegistration {}

#[derive(Debug, Default)]
struct ReplyRoutes {
    senders: HashMap<String, mpsc::Sender<ManagementResponse>>,
}

/// Protocol-only state shared by every session on one AMQP connection.
///
/// Delivery tags and dynamic reply addresses have connection scope. Neither is
/// replicated broker state, and both disappear when the connection does.
#[derive(Debug, Default)]
pub(crate) struct ConnectionManagement {
    deliveries: RwLock<HashMap<DeliveryKey, DeliveryRegistration>>,
    request_response_deliveries:
        RwLock<HashMap<RequestResponseDeliveryKey, RequestResponseDelivery>>,
    session_claims: StdMutex<HashMap<String, Arc<()>>>,
    sessions: RwLock<HashMap<String, SessionRegistration>>,
    routes: Mutex<ReplyRoutes>,
    route_changed: Notify,
}

impl ConnectionManagement {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) async fn register_delivery(
        &self,
        link_name: &str,
        entity: EntityPath,
        sequence: SequenceNumber,
        lock_token: LockToken,
    ) -> DeliveryRegistration {
        let registration = DeliveryRegistration {
            key: DeliveryKey {
                link_name: link_name.to_owned(),
                lock_token,
            },
            managed: ManagedDelivery {
                entity,
                sequence,
                delivery: None,
            },
            identity: Arc::new(()),
        };
        self.deliveries
            .write()
            .await
            .insert(registration.key.clone(), registration.clone());
        registration
    }

    pub(crate) async fn unregister_delivery(&self, registration: &DeliveryRegistration) {
        let mut deliveries = self.deliveries.write().await;
        if deliveries
            .get(&registration.key)
            .is_some_and(|current| current == registration)
        {
            deliveries.remove(&registration.key);
        }
    }

    #[cfg(test)]
    pub(crate) async fn delivery_write_lock(&self) -> impl Send + '_ {
        self.deliveries.write().await
    }

    #[cfg(test)]
    pub(crate) async fn delivery(
        &self,
        link_name: &str,
        lock_token: LockToken,
    ) -> Option<ManagedDelivery> {
        self.deliveries
            .read()
            .await
            .get(&DeliveryKey {
                link_name: link_name.to_owned(),
                lock_token,
            })
            .map(|registered| registered.managed.clone())
    }

    async fn register_request_response_delivery(
        &self,
        entity: EntityPath,
        delivery: Delivery,
    ) -> RequestResponseDeliveryRegistration {
        self.register_request_response_delivery_at(entity, delivery, Instant::now())
            .await
    }

    async fn register_request_response_delivery_at(
        &self,
        entity: EntityPath,
        delivery: Delivery,
        now: Instant,
    ) -> RequestResponseDeliveryRegistration {
        let sequence = delivery.sequence;
        let lock = delivery
            .lock
            .expect("only a locked management delivery is registered");
        let registration = RequestResponseDeliveryRegistration {
            key: RequestResponseDeliveryKey {
                entity: entity.clone(),
                lock_token: lock.token,
            },
            identity: Arc::new(()),
        };
        let mut deliveries = self.request_response_deliveries.write().await;
        purge_request_response_deliveries(&mut deliveries, now);
        deliveries.insert(
            registration.key.clone(),
            RequestResponseDelivery {
                managed: ManagedDelivery {
                    entity,
                    sequence,
                    delivery: Some(delivery),
                },
                registration: registration.clone(),
                expires_at: request_response_deadline(now, lock.lock_duration_millis),
            },
        );
        registration
    }

    #[cfg(test)]
    async fn request_response_delivery(
        &self,
        entity: &EntityPath,
        lock_token: LockToken,
    ) -> Option<ManagedDelivery> {
        self.request_response_delivery_at(entity, lock_token, Instant::now())
            .await
    }

    #[cfg(test)]
    async fn request_response_delivery_at(
        &self,
        entity: &EntityPath,
        lock_token: LockToken,
        now: Instant,
    ) -> Option<ManagedDelivery> {
        let mut deliveries = self.request_response_deliveries.write().await;
        purge_request_response_deliveries(&mut deliveries, now);
        deliveries
            .get(&RequestResponseDeliveryKey {
                entity: entity.clone(),
                lock_token,
            })
            .map(|delivery| delivery.managed.clone())
    }

    async fn refresh_request_response_delivery(
        &self,
        registration: Option<&RequestResponseDeliveryRegistration>,
        locked_until: domain::Timestamp,
        lock_duration_millis: u64,
    ) {
        self.refresh_request_response_delivery_at(
            registration,
            locked_until,
            lock_duration_millis,
            Instant::now(),
        )
        .await;
    }

    async fn refresh_request_response_delivery_at(
        &self,
        registration: Option<&RequestResponseDeliveryRegistration>,
        locked_until: domain::Timestamp,
        lock_duration_millis: u64,
        now: Instant,
    ) {
        let mut deliveries = self.request_response_deliveries.write().await;
        if let Some(registration) = registration
            && let Some(registered) = deliveries.get_mut(&registration.key)
            && registered.registration == *registration
        {
            registered.expires_at = request_response_deadline(now, lock_duration_millis);
            if let Some(delivery) = registered.managed.delivery.as_mut() {
                delivery.lock = Some(domain::DeliveryLock {
                    token: registration.key.lock_token,
                    locked_until,
                    lock_duration_millis,
                });
            }
        }
        // Update the successfully renewed target before purging: the broker is
        // authoritative even if its old local deadline elapsed in flight.
        purge_request_response_deliveries(&mut deliveries, now);
    }

    async fn unregister_request_response_delivery(
        &self,
        registration: &RequestResponseDeliveryRegistration,
    ) {
        let mut deliveries = self.request_response_deliveries.write().await;
        if deliveries
            .get(&registration.key)
            .is_some_and(|current| current.registration == *registration)
        {
            deliveries.remove(&registration.key);
        }
    }

    async fn unregister_managed_delivery(&self, selection: &ManagedDeliverySelection) {
        if let Some(registration) = selection.request_response.as_ref() {
            self.unregister_request_response_delivery(registration)
                .await;
        }
        if let Some(registration) = selection.ordinary.as_ref() {
            self.unregister_delivery(registration).await;
        }
    }

    /// Finds a delivery managed through either an ordinary receive link or a
    /// request/response receive. The .NET client omits `associated-link-name`
    /// when it retrieves a deferred message before opening its receive link,
    /// so the entity-scoped registry is the authority for that path.
    async fn managed_delivery(
        &self,
        entity: &EntityPath,
        link_name: Option<&str>,
        lock_token: LockToken,
    ) -> Option<ManagedDeliverySelection> {
        let ordinary = if let Some(link_name) = link_name {
            self.deliveries
                .read()
                .await
                .get(&DeliveryKey {
                    link_name: link_name.to_owned(),
                    lock_token,
                })
                .filter(|registered| &registered.managed.entity == entity)
                .cloned()
        } else {
            None
        };
        let key = RequestResponseDeliveryKey {
            entity: entity.clone(),
            lock_token,
        };
        let mut deliveries = self.request_response_deliveries.write().await;
        // Ordinary authority can renew an alias whose local deadline expired.
        // Fallback authority still requires the existing live-row check.
        if ordinary.is_none() {
            purge_request_response_deliveries(&mut deliveries, Instant::now());
        }
        let request_response = deliveries.get(&key);
        let managed = ordinary
            .as_ref()
            .map(|registered| registered.managed.clone())
            .or_else(|| request_response.map(|registered| registered.managed.clone()))?;
        Some(ManagedDeliverySelection {
            managed,
            ordinary,
            request_response: request_response.map(|registered| registered.registration.clone()),
        })
    }

    pub(crate) fn claim_session(
        self: &Arc<Self>,
        link_name: &str,
        entity: EntityPath,
    ) -> SessionClaim {
        let identity = Arc::new(());
        self.session_claims
            .lock()
            .expect("the pending session claim lock is not poisoned")
            .insert(link_name.to_owned(), Arc::clone(&identity));
        SessionClaim {
            management: Arc::downgrade(self),
            link_name: link_name.to_owned(),
            entity,
            identity,
        }
    }

    pub(crate) async fn install_session(
        &self,
        claim: &SessionClaim,
        hold: SessionHold,
        is_live: impl FnOnce() -> bool,
    ) -> Option<SessionRegistration> {
        let mut sessions = self.sessions.write().await;
        // Claiming does not await this row lock. Hold the synchronous claim
        // lock through insertion so another runtime thread cannot supersede
        // the owner between the check and the write.
        let mut claims = self
            .session_claims
            .lock()
            .expect("the pending session claim lock is not poisoned");
        if !claims
            .get(&claim.link_name)
            .is_some_and(|identity| Arc::ptr_eq(identity, &claim.identity))
            || !is_live()
        {
            return None;
        }
        let registration = SessionRegistration {
            link_name: claim.link_name.clone(),
            entity: claim.entity.clone(),
            hold,
            identity: Arc::clone(&claim.identity),
        };
        sessions.insert(claim.link_name.clone(), registration.clone());
        claims.remove(&claim.link_name);
        Some(registration)
    }

    pub(crate) async fn unregister_session(&self, registration: &SessionRegistration) {
        let mut sessions = self.sessions.write().await;
        if sessions
            .get(&registration.link_name)
            .is_some_and(|session| session == registration)
        {
            sessions.remove(&registration.link_name);
        }
    }

    async fn session(&self, link_name: &str) -> Option<SessionRegistration> {
        self.sessions.read().await.get(link_name).cloned()
    }

    #[cfg(test)]
    pub(crate) async fn session_write_lock(
        &self,
    ) -> tokio::sync::RwLockWriteGuard<'_, HashMap<String, SessionRegistration>> {
        self.sessions.write().await
    }

    #[cfg(test)]
    pub(crate) async fn registered_session_owner(
        &self,
        link_name: &str,
    ) -> Option<SessionRegistration> {
        self.session(link_name).await
    }

    #[cfg(test)]
    pub(crate) async fn registered_session(
        &self,
        link_name: &str,
    ) -> Option<(EntityPath, SessionHold)> {
        self.session(link_name)
            .await
            .map(|session| (session.entity, session.hold))
    }

    pub(crate) async fn register_reply_route(
        &self,
        address: String,
    ) -> (
        mpsc::Sender<ManagementResponse>,
        mpsc::Receiver<ManagementResponse>,
    ) {
        let (sender, receiver) = mpsc::channel(REPLY_BUFFER);
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
        sender: &mpsc::Sender<ManagementResponse>,
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

    async fn route_response(
        &self,
        address: &str,
        response: ManagementResponse,
    ) -> Result<(), RouteError> {
        let deadline = tokio::time::Instant::now() + REPLY_ROUTE_TIMEOUT;
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

#[derive(Clone)]
pub(crate) struct ManagementAuthorization {
    connection: Arc<ConnectionAuthorization>,
    resource: ResourceScope,
}

impl ManagementAuthorization {
    pub(crate) fn new(connection: Arc<ConnectionAuthorization>, resource: ResourceScope) -> Self {
        Self {
            connection,
            resource,
        }
    }

    async fn ensure(&self, permission: Permission) -> Result<(), AmqpProtocolError> {
        self.connection
            .authorize_resource(&self.resource, permission)
            .await
            .map_err(|_| unauthorized_error("the management link's authorization has expired"))
    }

    async fn ensure_any(&self) -> Result<(), AmqpProtocolError> {
        self.connection
            .authorize_resource_any(&self.resource, &[Permission::Send, Permission::Listen])
            .await
            .map_err(|_| unauthorized_error("the management link's authorization has expired"))
    }

    async fn wait_until_unauthorized(&self) {
        self.connection
            .wait_until_unauthorized_any(&self.resource, &[Permission::Send, Permission::Listen])
            .await;
    }
}

#[cfg(test)]
pub(crate) async fn serve_management_requests<B: Broker>(
    receiver: Receiver,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: B,
    management: Arc<ConnectionManagement>,
    authorization: Option<ManagementAuthorization>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    serve_management_requests_with_retirement(
        receiver,
        namespace,
        entity,
        broker,
        management,
        authorization,
        None,
    )
    .await
}

pub(crate) async fn serve_management_requests_with_retirement<B: Broker>(
    mut receiver: Receiver,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: B,
    management: Arc<ConnectionManagement>,
    authorization: Option<ManagementAuthorization>,
    notice: Option<ConnectionRetirementRequest>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let detached = receiver.on_detach_owned();
    let retirement = async {
        tokio::select! {
            biased;
            () = detached => ManagementRetirement::Detached,
            () = wait_until_unauthorized(authorization.as_ref()) => ManagementRetirement::Unauthorized,
        }
    };
    tokio::pin!(retirement);
    loop {
        let received = tokio::select! {
            biased;
            exit = &mut retirement => {
                if matches!(exit, ManagementRetirement::Unauthorized) {
                    let mut custody = RequestCustody::default();
                    custody.close = Some(native_operation(receiver.close_with_error(unauthorized_error(
                        "the management link's authorization has expired",
                    ))));
                    finish_request_with_retirement(&mut custody, &receiver, Ok(Ok(Some(exit))), notice.as_ref()).await?;
                }
                return Ok(());
            },
            received = receiver.recv() => received,
        };
        let delivery = match received {
            Ok(delivery) => delivery,
            Err(
                amqp::EngineError::RemoteClosed
                | amqp::EngineError::RemoteDetached
                | amqp::EngineError::Stopped,
            ) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if let Some(authorization) = authorization.as_ref() {
            let prepared = tokio::select! {
                biased;
                exit = &mut retirement => Err(exit),
                prepared = authorization.ensure_any() => prepared.map_err(|_| ManagementRetirement::Unauthorized),
            };
            if let Err(exit) = prepared {
                if matches!(exit, ManagementRetirement::Unauthorized) {
                    let mut custody = RequestCustody::default();
                    custody.close = Some(native_operation(receiver.close_with_error(
                        unauthorized_error("the management link's authorization has expired"),
                    )));
                    finish_request_with_retirement(
                        &mut custody,
                        &receiver,
                        Ok(Ok(Some(exit))),
                        notice.as_ref(),
                    )
                    .await?;
                }
                return Ok(());
            }
        }
        let request_broker = RequestBroker::new(broker.clone());
        let mut custody = RequestCustody::default();
        let result = AssertUnwindSafe(management_request_pump(
            &receiver,
            &delivery,
            &namespace,
            &entity,
            &request_broker,
            &management,
            authorization.as_ref(),
            &mut custody,
            retirement.as_mut(),
        ))
        .catch_unwind()
        .await;
        if matches!(&result, Ok(Ok(Some(ManagementRetirement::Unauthorized)))) {
            custody.close = Some(native_operation(receiver.close_with_error(
                unauthorized_error("the management link's authorization has expired"),
            )));
        }
        if finish_request_with_retirement(&mut custody, &receiver, result, notice.as_ref())
            .await?
            .is_some()
        {
            return Ok(());
        }
    }
}

async fn finish_request_with_retirement(
    custody: &mut RequestCustody<'_>,
    receiver: &Receiver,
    primary: std::thread::Result<Result<Option<ManagementRetirement>, ManagementError>>,
    retirement: Option<&ConnectionRetirementRequest>,
) -> Result<Option<ManagementRetirement>, ManagementError> {
    if matches!(&primary, Err(_) | Ok(Err(_)))
        && let Some(retirement) = retirement
    {
        retirement.request();
    }
    let cleanup =
        AssertUnwindSafe(custody.finish_with_retirement(receiver.on_detach_owned(), retirement))
            .catch_unwind()
            .await;
    if cleanup.is_err()
        && let Some(retirement) = retirement
    {
        retirement.request();
    }
    let diagnostics = if cleanup.is_ok() {
        catch_unwind(AssertUnwindSafe(|| custody.report())).err()
    } else {
        None
    };
    let cleanup_panic = cleanup
        .err()
        .or_else(|| custody.take_cleanup_panic())
        .or(diagnostics);
    finish_pump(primary, cleanup_panic, custody.take_native_error())
}

#[expect(clippy::too_many_arguments)]
async fn management_request_pump<'a, B: Broker>(
    receiver: &'a Receiver,
    delivery: &'a amqp::Delivery,
    namespace: &'a NamespaceName,
    entity: &'a EntityPath,
    request_broker: &'a RequestBroker<B>,
    management: &'a ConnectionManagement,
    authorization: Option<&'a ManagementAuthorization>,
    custody: &mut RequestCustody<'a>,
    mut retirement: Pin<&mut (impl Future<Output = ManagementRetirement> + Send)>,
) -> Result<Option<ManagementRetirement>, ManagementError> {
    let correlation = delivery
        .message()
        .properties
        .as_ref()
        .and_then(|properties| {
            Some((properties.message_id.clone()?, properties.reply_to.clone()?))
        });
    let Some((message_id, reply_to)) = correlation else {
        custody.native = Some(native_operation(receiver.reject(delivery, None)));
        if let Some(exit) = observe_or_retire(
            custody.native.as_mut().unwrap(),
            retirement.as_mut(),
            PumpPoint::RequestNative,
        )
        .await
        {
            return Ok(Some(exit));
        }
        custody.capture_native();
        if let Some(error) = custody.take_native_error() {
            return Err(error.into());
        }
        return Ok(None);
    };
    custody.request = Some(PendingOperation::new(
        process_request(
            delivery.message(),
            message_id,
            namespace,
            entity,
            request_broker,
            management,
            authorization,
        ),
        request_broker.control(),
    ));
    #[cfg(test)]
    custody::pump_checkpoint(PumpPoint::RequestPrepared).await;
    if let Some(exit) = observe_or_retire(
        custody.request.as_mut().unwrap(),
        retirement.as_mut(),
        PumpPoint::RequestBroker,
    )
    .await
    {
        return Ok(Some(exit));
    }
    custody.capture_request();
    custody.response = Some(
        custody
            .request_packet
            .as_mut()
            .unwrap()
            .result
            .take()
            .expect("active management request has a response"),
    );
    #[cfg(test)]
    custody::pump_checkpoint(PumpPoint::RequestResponse).await;
    let response = custody.response.as_ref().unwrap();
    debug!(correlation_id = ?response.correlation_id, %reply_to, status_code = response.status_code, "management request processed");
    custody.native = Some(native_operation(receiver.accept(delivery)));
    if let Some(exit) = observe_or_retire(
        custody.native.as_mut().unwrap(),
        retirement.as_mut(),
        PumpPoint::RequestNative,
    )
    .await
    {
        return Ok(Some(exit));
    }
    custody.capture_native();
    if let Some(error) = custody.take_native_error() {
        return Err(error.into());
    }
    let routed = tokio::select! {
        biased;
        exit = retirement.as_mut() => return Ok(Some(exit)),
        () = custody::pump_fault(PumpPoint::RequestRouting) => unreachable!("management fault checkpoint panics"),
        routed = management.route_response(&reply_to, custody.response.as_ref().unwrap().clone()) => routed,
    };
    if routed.is_err() {
        debug!(%reply_to, "management reply route disappeared");
    } else {
        debug!(%reply_to, "management response routed");
    }
    Ok(None)
}

#[derive(Clone, Copy, Debug)]
enum ManagementRetirement {
    Detached,
    Unauthorized,
}

async fn observe_or_retire<T: Send>(
    original: &mut PendingOperation<'_, T>,
    retirement: Pin<&mut (impl Future<Output = ManagementRetirement> + Send)>,
    point: PumpPoint,
) -> Option<ManagementRetirement> {
    tokio::select! {
        biased;
        exit = retirement => { original.retire(); Some(exit) }
        () = custody::pump_fault(point) => unreachable!("management fault checkpoint panics"),
        _ = original.observe() => None,
    }
}

fn native_operation<'a, T: Send + 'a>(
    actual: impl Future<Output = Result<T, EngineError>> + Send + 'a,
) -> PendingOperation<'a, Result<T, EngineError>> {
    let control = OperationControl::new();
    let frontier = control.clone();
    PendingOperation::new(
        async move {
            if !frontier.begin() {
                return Err(EngineError::Stopped);
            }
            actual.await
        },
        control,
    )
}

#[cfg(test)]
fn consume_native_result<T: Send>(
    original: &mut PendingOperation<'_, Result<T, EngineError>>,
    cleanup: Result<(), EngineError>,
) -> Result<(), EngineError> {
    let packet = original
        .take_packet()
        .expect("finished native result is consumed once");
    debug!(
        started = packet.started,
        retired = packet.retired,
        "original management native result observed"
    );
    match packet.result {
        None | Some(Ok(_)) => cleanup,
        Some(Err(error)) => {
            if let Err(cleanup_error) = cleanup {
                debug!(%cleanup_error, "secondary management native cleanup failed");
            }
            Err(error)
        }
    }
}

#[cfg(test)]
async fn guarded_close(
    actual: impl Future<Output = Result<(), EngineError>> + Send,
    detached: impl Future<Output = ()> + Send,
) -> Result<(), EngineError> {
    let mut original = native_operation(actual);
    tokio::pin!(detached);
    tokio::select! {
        biased;
        () = &mut detached => { let _ = original.finish().await; }
        _ = original.observe() => {}
    }
    consume_native_result(&mut original, Ok(()))
}

async fn process_request<B: Broker>(
    message: &Message,
    message_id: MessageId,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
    authorization: Option<&ManagementAuthorization>,
) -> ManagementResponse {
    let tracking_id = message
        .application_properties
        .as_ref()
        .and_then(|properties| string_property(properties, TRACKING_ID_PROPERTY))
        .map(str::to_owned);
    let Some(properties) = message.application_properties.as_ref() else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "application properties are required",
        );
    };
    let Some(operation) = string_property(properties, OPERATION_PROPERTY) else {
        return ManagementResponse::bad_request(message_id, tracking_id, "operation is required");
    };
    let permission = management_permission(operation);
    if let Some(authorization) = authorization
        && authorization.ensure(permission).await.is_err()
    {
        return ManagementResponse::unauthorized(
            message_id,
            tracking_id,
            format!("{permission:?} is not authorized for this management operation"),
        );
    }
    match operation {
        RENEW_LOCK_OPERATION => {
            renew_message_lock(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        RENEW_SESSION_LOCK_OPERATION => {
            renew_session_lock(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        GET_SESSION_STATE_OPERATION => {
            get_session_state(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        SET_SESSION_STATE_OPERATION => {
            set_session_state(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        RECEIVE_BY_SEQUENCE_NUMBER_OPERATION => {
            deferred::receive_by_sequence_number(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        UPDATE_DISPOSITION_OPERATION => {
            deferred::update_disposition(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        PEEK_MESSAGE_OPERATION => {
            peek::peek(
                message,
                message_id,
                tracking_id,
                namespace,
                entity,
                broker,
                management,
            )
            .await
        }
        ADD_RULE_OPERATION => {
            rules::add(message, message_id, tracking_id, namespace, entity, broker).await
        }
        REMOVE_RULE_OPERATION => {
            rules::remove(message, message_id, tracking_id, namespace, entity, broker).await
        }
        ENUMERATE_RULES_OPERATION => {
            rules::enumerate(message, message_id, tracking_id, namespace, entity, broker).await
        }
        SCHEDULE_MESSAGE_OPERATION => {
            scheduled::schedule(message, message_id, tracking_id, namespace, entity, broker).await
        }
        CANCEL_SCHEDULED_MESSAGE_OPERATION => {
            scheduled::cancel(message, message_id, tracking_id, namespace, entity, broker).await
        }
        _ => ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "unsupported management operation",
        ),
    }
}

fn management_permission(operation: &str) -> Permission {
    match operation {
        SCHEDULE_MESSAGE_OPERATION | CANCEL_SCHEDULED_MESSAGE_OPERATION => Permission::Send,
        _ => Permission::Listen,
    }
}

#[allow(clippy::too_many_arguments)]
async fn renew_message_lock<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
) -> ManagementResponse {
    let Some(properties) = message.application_properties.as_ref() else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "application properties are required",
        );
    };
    let link_name = string_property(properties, ASSOCIATED_LINK_NAME_PROPERTY);
    let Some(tokens) = lock_tokens(&message.body) else {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "lock-tokens must be an AMQP value array of UUIDs",
        );
    };
    if tokens.len() != 1 {
        return ManagementResponse::bad_request(
            message_id,
            tracking_id,
            "exactly one lock token is required",
        );
    }

    let lock_token = tokens[0];
    let Some(delivery) = management
        .managed_delivery(entity, link_name, lock_token)
        .await
    else {
        return ManagementResponse::lock_lost(
            message_id,
            tracking_id,
            "the lock token is not active for this entity",
        );
    };

    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::RenewLock {
                sequence: delivery.managed.sequence,
                lock_token,
                lock_duration_millis: None,
            },
        )
        .await
    {
        Ok(CommandOutcome::LockRenewed {
            locked_until,
            lock_duration_millis,
        }) => {
            management
                .refresh_request_response_delivery(
                    delivery.request_response.as_ref(),
                    locked_until,
                    lock_duration_millis,
                )
                .await;
            ManagementResponse::accepted(
                message_id,
                tracking_id,
                map_body(
                    EXPIRATIONS,
                    Value::Array(Array::from(vec![timestamp_value(locked_until)])),
                ),
            )
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("renewing a lock produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => {
            if definitive_message_lock_loss(&rejection) {
                management.unregister_managed_delivery(&delivery).await;
                return ManagementResponse::lock_lost(
                    message_id,
                    tracking_id,
                    rejection.to_string(),
                );
            }
            ManagementResponse::from_rejection(message_id, tracking_id, &rejection)
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SessionLookupError {
    BadRequest(&'static str),
    LockLost(&'static str),
}

async fn requested_session(
    message: &Message,
    entity: &EntityPath,
    management: &ConnectionManagement,
) -> Result<SessionRegistration, SessionLookupError> {
    let properties =
        message
            .application_properties
            .as_ref()
            .ok_or(SessionLookupError::BadRequest(
                "application properties are required",
            ))?;
    let link_name = string_property(properties, ASSOCIATED_LINK_NAME_PROPERTY).ok_or(
        SessionLookupError::BadRequest("the associated receive link name is required"),
    )?;
    let session_id = string_map_value(&message.body, SESSION_ID).ok_or(
        SessionLookupError::BadRequest("session-id must be an AMQP value string"),
    )?;
    let session = management
        .session(link_name)
        .await
        .ok_or(SessionLookupError::LockLost(
            "the associated link does not hold a session",
        ))?;
    if &session.entity != entity || session.hold.session_id.as_str() != session_id {
        return Err(SessionLookupError::LockLost(
            "the associated link does not hold the named session",
        ));
    }
    Ok(session)
}

fn session_lookup_response(
    message_id: MessageId,
    tracking_id: Option<String>,
    error: SessionLookupError,
) -> ManagementResponse {
    match error {
        SessionLookupError::BadRequest(description) => {
            ManagementResponse::bad_request(message_id, tracking_id, description)
        }
        SessionLookupError::LockLost(description) => {
            ManagementResponse::session_lock_lost(message_id, tracking_id, description)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn renew_session_lock<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
) -> ManagementResponse {
    let session = match requested_session(message, entity, management).await {
        Ok(session) => session,
        Err(error) => return session_lookup_response(message_id, tracking_id, error),
    };
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::RenewSessionLock {
                session: session.hold,
                lock_duration_millis: None,
            },
        )
        .await
    {
        Ok(CommandOutcome::SessionLockRenewed { locked_until }) => ManagementResponse::accepted(
            message_id,
            tracking_id,
            map_body(EXPIRATION, timestamp_value(locked_until)),
        ),
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("renewing a session lock produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

#[allow(clippy::too_many_arguments)]
async fn get_session_state<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
) -> ManagementResponse {
    let session = match requested_session(message, entity, management).await {
        Ok(session) => session,
        Err(error) => return session_lookup_response(message_id, tracking_id, error),
    };
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::GetSessionState {
                session: session.hold,
            },
        )
        .await
    {
        Ok(CommandOutcome::SessionState(state)) => {
            let state = if state.is_empty() {
                Value::Null
            } else {
                Value::Binary(Binary::from(state))
            };
            ManagementResponse::accepted(message_id, tracking_id, map_body(SESSION_STATE, state))
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("reading session state produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

#[allow(clippy::too_many_arguments)]
async fn set_session_state<B: Broker>(
    message: &Message,
    message_id: MessageId,
    tracking_id: Option<String>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    management: &ConnectionManagement,
) -> ManagementResponse {
    let session = match requested_session(message, entity, management).await {
        Ok(session) => session,
        Err(error) => return session_lookup_response(message_id, tracking_id, error),
    };
    let state = match map_value(&message.body, SESSION_STATE) {
        Some(Value::Binary(state)) => state.to_vec(),
        Some(Value::Null) => Vec::new(),
        _ => {
            return ManagementResponse::bad_request(
                message_id,
                tracking_id,
                "session-state must be an AMQP binary value or null",
            );
        }
    };
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::SetSessionState {
                session: session.hold,
                state,
            },
        )
        .await
    {
        Ok(CommandOutcome::SessionStateSet) => {
            ManagementResponse::accepted(message_id, tracking_id, Value::Null)
        }
        Ok(other) => ManagementResponse::internal(
            message_id,
            tracking_id,
            format!("setting session state produced an unexpected outcome: {other:?}"),
        ),
        Err(rejection) => ManagementResponse::from_rejection(message_id, tracking_id, &rejection),
    }
}

fn string_property<'a>(properties: &'a ApplicationProperties, name: &str) -> Option<&'a str> {
    match properties.get(name) {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
}

fn map_value<'a>(body: &'a Body, name: &str) -> Option<&'a Value> {
    let Body::Value(Value::Map(map)) = body else {
        return None;
    };
    map.iter().find_map(|(key, value)| match key {
        Value::String(key) if key == name => Some(value),
        Value::Symbol(key) if key.as_str() == name => Some(value),
        _ => None,
    })
}

fn string_map_value<'a>(body: &'a Body, name: &str) -> Option<&'a str> {
    match map_value(body, name) {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
}

fn map_body(name: &str, value: Value) -> Value {
    let mut map = OrderedMap::new();
    map.insert(Value::String(name.to_owned()), value);
    Value::Map(map)
}

fn timestamp_value(timestamp: domain::Timestamp) -> Value {
    Value::Timestamp(AmqpTimestamp::from_milliseconds(
        i64::try_from(timestamp.as_millis()).unwrap_or(i64::MAX),
    ))
}

fn lock_tokens(body: &Body) -> Option<Vec<LockToken>> {
    let value = map_value(body, LOCK_TOKENS)?;
    let Value::Array(values) = value else {
        return None;
    };
    values
        .iter()
        .map(|value| match value {
            Value::Uuid(value) => lock_token(value),
            _ => None,
        })
        .collect()
}

fn lock_token(uuid: &Uuid) -> Option<LockToken> {
    let bytes = uuid.as_inner();
    if bytes[..8] != [0; 8] {
        return None;
    }
    Some(LockToken::new(u64::from_be_bytes(
        bytes[8..].try_into().ok()?,
    )))
}

#[cfg(test)]
pub(crate) async fn serve_management_replies(
    sender: Sender,
    address: String,
    route: mpsc::Sender<ManagementResponse>,
    responses: mpsc::Receiver<ManagementResponse>,
    management: Arc<ConnectionManagement>,
    authorization: Option<ManagementAuthorization>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    serve_management_replies_with_retirement(
        sender,
        address,
        route,
        responses,
        management,
        authorization,
        None,
    )
    .await
}

pub(crate) async fn serve_management_replies_with_retirement(
    sender: Sender,
    address: String,
    route: mpsc::Sender<ManagementResponse>,
    responses: mpsc::Receiver<ManagementResponse>,
    management: Arc<ConnectionManagement>,
    authorization: Option<ManagementAuthorization>,
    retirement: Option<ConnectionRetirementRequest>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut custody = ReplyCustody::new(responses, address, route, &management);
    let result = AssertUnwindSafe(management_reply_loop(
        &sender,
        &mut custody,
        authorization.as_ref(),
    ))
    .catch_unwind()
    .await;
    if matches!(&result, Err(_) | Ok(Err(_)))
        && let Some(retirement) = retirement.as_ref()
    {
        retirement.request();
    }
    let cleanup = AssertUnwindSafe(
        custody.finish_with_retirement(sender.on_detach_owned(), retirement.as_ref()),
    )
    .catch_unwind()
    .await;
    if cleanup.is_err()
        && let Some(retirement) = retirement.as_ref()
    {
        retirement.request();
    }
    let diagnostics = if cleanup.is_ok() {
        catch_unwind(AssertUnwindSafe(|| custody.report())).err()
    } else {
        None
    };
    let cleanup_panic = cleanup
        .err()
        .or_else(|| custody.take_cleanup_panic())
        .or(diagnostics);
    finish_pump(result, cleanup_panic, custody.take_native_error())
}

async fn management_reply_loop<'a>(
    sender: &'a Sender,
    custody: &mut ReplyCustody<'a>,
    authorization: Option<&ManagementAuthorization>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let detached = sender.on_detach_owned();
    let retirement = async {
        tokio::select! {
            biased;
            () = detached => ManagementRetirement::Detached,
            () = wait_until_unauthorized(authorization) => ManagementRetirement::Unauthorized,
        }
    };
    tokio::pin!(retirement);
    loop {
        #[cfg(test)]
        custody::pump_checkpoint(PumpPoint::ReplyIdle).await;
        let response = tokio::select! {
            biased;
            exit = &mut retirement => {
                if matches!(exit, ManagementRetirement::Unauthorized) {
                    custody.close = Some(native_operation(sender.close_with_error(unauthorized_error(
                        "the management link's authorization has expired",
                    ))));
                }
                return Ok(());
            }
            response = custody.responses.recv() => response,
        };
        let Some(response) = response else {
            return Ok(());
        };
        let control = OperationControl::new();
        custody.original = Some(PendingOperation::new(
            send_management_response(sender, response, control.clone()),
            control,
        ));
        #[cfg(test)]
        custody::pump_checkpoint(PumpPoint::ReplyPrepared).await;
        if let Some(exit) = observe_or_retire(
            custody.original.as_mut().unwrap(),
            retirement.as_mut(),
            PumpPoint::ReplyNative,
        )
        .await
        {
            if matches!(exit, ManagementRetirement::Unauthorized) {
                custody.close = Some(native_operation(sender.close_with_error(
                    unauthorized_error("the management link's authorization has expired"),
                )));
            }
            return Ok(());
        }
        custody.capture_packet();
        #[cfg(test)]
        custody::pump_checkpoint(PumpPoint::ReplyResult).await;
        if let Some(error) = custody.take_native_error() {
            return Err(error.into());
        }
        custody.packet = None;
        debug!("management response sent");
    }
}

type ManagementError = Box<dyn std::error::Error + Send + Sync>;

fn finish_pump<T>(
    primary: std::thread::Result<Result<T, ManagementError>>,
    cleanup_panic: Option<PanicPayload>,
    native_error: Option<EngineError>,
) -> Result<T, ManagementError> {
    match primary {
        Err(payload) => resume_unwind(payload),
        Ok(Err(error)) => Err(error),
        Ok(Ok(value)) => {
            if let Some(error) = native_error {
                return Err(error.into());
            }
            if let Some(payload) = cleanup_panic {
                resume_unwind(payload);
            }
            Ok(value)
        }
    }
}

async fn send_management_response(
    sender: &Sender,
    response: ManagementResponse,
    control: OperationControl,
) -> Result<Outcome, EngineError> {
    if !control.begin() {
        return Err(EngineError::Stopped);
    }
    let tag = response_delivery_tag(&response.correlation_id);
    let pending = sender.send_pending(response.into_message(), tag).await?;
    let observed = pending.await?;
    let (_, outcome, confirmation) = observed.into_parts();
    if let Some(confirmation) = confirmation
        && control.begin()
    {
        let state = match &outcome {
            Outcome::Accepted(value) => DeliveryState::Accepted(value.clone()),
            Outcome::Rejected(value) => DeliveryState::Rejected(value.clone()),
            Outcome::Released(value) => DeliveryState::Released(value.clone()),
            Outcome::Modified(value) => DeliveryState::Modified(value.clone()),
        };
        confirmation.confirm(state).await?;
    }
    Ok(outcome)
}

async fn wait_until_unauthorized(authorization: Option<&ManagementAuthorization>) {
    match authorization {
        Some(authorization) => authorization.wait_until_unauthorized().await,
        None => std::future::pending().await,
    }
}

fn response_delivery_tag(message_id: &MessageId) -> Binary {
    Binary::from(format!("{message_id:?}").into_bytes())
}

fn unauthorized_error(description: impl Into<String>) -> AmqpProtocolError {
    AmqpProtocolError::new(AmqpError::UnauthorizedAccess, description.into(), None)
}

#[derive(Clone, Copy, Debug)]
struct RouteError;

#[cfg(test)]
mod session_registry_tests;

#[cfg(test)]
mod request_retirement_tests;

#[cfg(test)]
mod reply_retirement_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_tokens_are_read_from_guid_sized_delivery_tags() {
        let mut bytes = [0_u8; 16];
        bytes[8..].copy_from_slice(&42_u64.to_be_bytes());
        let mut map = OrderedMap::new();
        map.insert(
            Value::String(String::from(LOCK_TOKENS)),
            Value::Array(Array::from(vec![Value::Uuid(Uuid::from(bytes))])),
        );

        assert_eq!(
            lock_tokens(&Body::Value(Value::Map(map))),
            Some(vec![LockToken::new(42)])
        );
    }

    #[test]
    fn a_success_response_uses_the_management_contract_shapes() {
        let response = ManagementResponse::accepted(
            MessageId::Ulong(7),
            Some(String::from("trace-1")),
            map_body(
                EXPIRATIONS,
                Value::Array(Array::from(vec![timestamp_value(
                    domain::Timestamp::from_millis(12_345),
                )])),
            ),
        )
        .into_message();

        assert_eq!(
            response
                .application_properties
                .as_ref()
                .and_then(|properties| properties.get(STATUS_CODE_PROPERTY)),
            Some(&Value::Int(200))
        );
        let Body::Value(Value::Map(body)) = response.body else {
            panic!("the response must carry an AMQP value map");
        };
        assert_eq!(
            body.iter().find_map(|(key, value)| {
                (key == &Value::String(String::from(EXPIRATIONS))).then_some(value)
            }),
            Some(&Value::Array(Array::from(vec![Value::Timestamp(
                AmqpTimestamp::from_milliseconds(12_345)
            )])))
        );
    }

    #[test]
    fn scheduling_uses_send_permission_and_receive_management_uses_listen() {
        assert_eq!(
            management_permission(SCHEDULE_MESSAGE_OPERATION),
            Permission::Send
        );
        assert_eq!(
            management_permission(CANCEL_SCHEDULED_MESSAGE_OPERATION),
            Permission::Send
        );
        for operation in [
            RENEW_LOCK_OPERATION,
            RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            UPDATE_DISPOSITION_OPERATION,
            PEEK_MESSAGE_OPERATION,
            RENEW_SESSION_LOCK_OPERATION,
            GET_SESSION_STATE_OPERATION,
            SET_SESSION_STATE_OPERATION,
            ADD_RULE_OPERATION,
            REMOVE_RULE_OPERATION,
            ENUMERATE_RULES_OPERATION,
        ] {
            assert_eq!(management_permission(operation), Permission::Listen);
        }
    }
}
