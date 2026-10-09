//! One control-link admission owns originals before a leaf can adopt them.

use std::{
    any::Any,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use amqp::{Attach, EngineError, LinkEndpoint, Role, Sender, ServerSession};
use auth::Permission;
use domain::NamespaceName;
use futures_util::FutureExt;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::debug;

use super::session_custody::{LeafKind, LeafResult, PreparedLeafTask};
use super::{ConnectionRetirementRequest, error_for, management_entity, unauthorized_error};
use crate::{
    Broker,
    authorization::ConnectionAuthorization,
    cbs::{CbsResponse, serve_cbs_replies_with_retirement, serve_cbs_requests_with_retirement},
    management::{
        ConnectionManagement, ManagementAuthorization, ManagementResponse,
        serve_management_replies_with_retirement, serve_management_requests_with_retirement,
    },
};

pub(crate) type PanicPayload = Box<dyn Any + Send>;
type Primary = std::thread::Result<Result<(), EngineError>>;

struct Original<'a, T> {
    actual: Option<Pin<Box<dyn Future<Output = T> + Send + 'a>>>,
    result: Option<T>,
    panic: Option<PanicPayload>,
    started: bool,
    poisoned: bool,
}

impl<'a, T> Original<'a, T> {
    fn new(actual: impl Future<Output = T> + Send + 'a) -> Self {
        Self {
            actual: Some(Box::pin(actual)),
            result: None,
            panic: None,
            started: false,
            poisoned: false,
        }
    }

    fn retire(&mut self) {
        if !self.started {
            self.actual = None;
        }
    }

    fn poll_original(&mut self, context: &mut std::task::Context<'_>) -> Poll<()> {
        let Some(actual) = self.actual.as_mut() else {
            return Poll::Ready(());
        };
        self.started = true;
        match catch_unwind(AssertUnwindSafe(|| actual.as_mut().poll(context))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => {
                self.result = Some(result);
                self.actual = None;
                Poll::Ready(())
            }
            Err(payload) => {
                self.panic = Some(payload);
                self.poisoned = true;
                self.actual = None;
                Poll::Ready(())
            }
        }
    }

    async fn observe(&mut self, point: PumpPoint) {
        let fault = pump_fault(point);
        tokio::pin!(fault);
        poll_fn(|context| match self.poll_original(context) {
            Poll::Ready(()) => Poll::Ready(()),
            Poll::Pending => {
                // A begun checkpoint follows a poll of the actual original,
                // not preparation or an inference about native queue admission.
                let _ = fault.as_mut().poll(context);
                Poll::Pending
            }
        })
        .await;
    }

    async fn finish(&mut self) {
        poll_fn(|context| self.poll_original(context)).await;
    }
}

pub(crate) enum RoutePacket {
    Cbs {
        registry: Arc<ConnectionAuthorization>,
        address: String,
        route: mpsc::Sender<CbsResponse>,
        responses: mpsc::Receiver<CbsResponse>,
    },
    Management {
        registry: Arc<ConnectionManagement>,
        address: String,
        route: mpsc::Sender<ManagementResponse>,
        responses: mpsc::Receiver<ManagementResponse>,
    },
}

pub(crate) struct AdmittedTask {
    pub(crate) task: JoinHandle<()>,
    pub(crate) kind: LeafKind,
    pub(crate) result: oneshot::Receiver<LeafResult>,
}

pub(crate) struct ControlContext<'a, B> {
    pub(crate) session: &'a ServerSession,
    pub(crate) broker: &'a B,
    pub(crate) namespace: &'a NamespaceName,
    pub(crate) authorization: Option<&'a Arc<ConnectionAuthorization>>,
    pub(crate) management: &'a Arc<ConnectionManagement>,
    pub(crate) retirement: Option<&'a ConnectionRetirementRequest>,
}

pub(crate) struct ControlAttachmentCustody<'a> {
    session: &'a ServerSession,
    retirement: Option<ConnectionRetirementRequest>,
    retirement_observer: Pin<Box<dyn Future<Output = ()> + Send>>,
    acceptance: Option<Original<'a, Result<LinkEndpoint, EngineError>>>,
    route: Option<Original<'a, RoutePacket>>,
    refusal: Option<Original<'a, Result<(), EngineError>>>,
    unregister: Option<Original<'a, ()>>,
    primary: Option<Primary>,
    cleanup_panic: Option<PanicPayload>,
    admitted: Option<AdmittedTask>,
    retired: bool,
    selected_retirement: bool,
    refused: bool,
    prepared_cleanup: bool,
    finished: bool,
    reported: bool,
}

impl<'a> ControlAttachmentCustody<'a> {
    pub(crate) fn new(
        session: &'a ServerSession,
        retirement: Option<&ConnectionRetirementRequest>,
    ) -> Self {
        let ended = session.on_end_owned();
        let requested = retirement.map(ConnectionRetirementRequest::observer);
        let retirement_observer = Box::pin(async move {
            match requested {
                Some(requested) => tokio::select! {
                    biased;
                    () = requested => {},
                    () = ended => {},
                },
                None => ended.await,
            }
        });
        Self {
            session,
            retirement: retirement.cloned(),
            retirement_observer,
            acceptance: None,
            route: None,
            refusal: None,
            unregister: None,
            primary: None,
            cleanup_panic: None,
            admitted: None,
            retired: false,
            selected_retirement: false,
            refused: false,
            prepared_cleanup: false,
            finished: false,
            reported: false,
        }
    }

    pub(crate) fn is_retired(&mut self) -> bool {
        if !self.retired
            && (self.session.is_ended()
                || self.retirement_observer.as_mut().now_or_never().is_some())
        {
            self.retire();
        }
        self.retired
    }

    pub(crate) fn retire(&mut self) {
        self.selected_retirement = true;
        self.retire_originals();
    }

    fn retire_originals(&mut self) {
        self.retired = true;
        if let Some(original) = self.acceptance.as_mut() {
            original.retire();
        }
        if let Some(original) = self.route.as_mut() {
            original.retire();
        }
        if let Some(original) = self.refusal.as_mut() {
            original.retire();
        }
    }

    pub(crate) fn begin_acceptance(&mut self, attach: Attach) -> bool {
        if self.is_retired() || self.acceptance.is_some() {
            return false;
        }
        self.acceptance = Some(Original::new(
            self.session
                .accept_attach(attach, crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64),
        ));
        true
    }

    pub(crate) async fn observe_acceptance(&mut self) -> bool {
        if self.is_retired() {
            return false;
        }
        let Some(original) = self.acceptance.as_mut() else {
            return false;
        };
        let completed = tokio::select! {
            biased;
            () = &mut self.retirement_observer => false,
            () = original.observe(PumpPoint::AcceptanceBegun) => true,
        };
        if !completed {
            self.retire();
        } else if let Some(payload) = original.panic.take() {
            resume_unwind(payload);
        }
        completed
    }

    pub(crate) fn begin_cbs_route(
        &mut self,
        registry: Arc<ConnectionAuthorization>,
        address: String,
    ) -> bool {
        if self.is_retired() || self.route.is_some() {
            return false;
        }
        self.route = Some(Original::new(async move {
            let (route, responses) = registry.register_reply_route(address.clone()).await;
            RoutePacket::Cbs {
                registry,
                address,
                route,
                responses,
            }
        }));
        true
    }

    pub(crate) fn begin_management_route(
        &mut self,
        registry: Arc<ConnectionManagement>,
        address: String,
    ) -> bool {
        if self.is_retired() || self.route.is_some() {
            return false;
        }
        self.route = Some(Original::new(async move {
            let (route, responses) = registry.register_reply_route(address.clone()).await;
            RoutePacket::Management {
                registry,
                address,
                route,
                responses,
            }
        }));
        true
    }

    pub(crate) async fn observe_route(&mut self) -> bool {
        if self.is_retired() {
            return false;
        }
        let Some(original) = self.route.as_mut() else {
            return false;
        };
        let completed = tokio::select! {
            biased;
            () = &mut self.retirement_observer => false,
            () = original.observe(PumpPoint::RouteBegun) => true,
        };
        if !completed {
            self.retire();
        } else if let Some(payload) = original.panic.take() {
            resume_unwind(payload);
        }
        completed
    }

    fn endpoint(&self) -> Option<&LinkEndpoint> {
        self.acceptance.as_ref()?.result.as_ref()?.as_ref().ok()
    }

    fn take_endpoint(&mut self) -> Option<LinkEndpoint> {
        let original = self.acceptance.as_mut()?;
        match original.result.take() {
            Some(Ok(endpoint)) => Some(endpoint),
            other => {
                original.result = other;
                None
            }
        }
    }

    fn take_reply(&mut self, cbs: bool) -> Option<(Sender, RoutePacket)> {
        let acceptance = self.acceptance.as_mut()?;
        let route = self.route.as_mut()?;
        let matching_route = matches!(
            (&route.result, cbs),
            (Some(RoutePacket::Cbs { .. }), true) | (Some(RoutePacket::Management { .. }), false)
        );
        if !matches!(acceptance.result, Some(Ok(LinkEndpoint::Sender(_)))) || !matching_route {
            return None;
        }
        match (acceptance.result.take(), route.result.take()) {
            (Some(Ok(LinkEndpoint::Sender(sender))), Some(packet)) => Some((sender, packet)),
            (endpoint, packet) => {
                acceptance.result = endpoint;
                route.result = packet;
                None
            }
        }
    }

    fn take_acceptance_error(&mut self) -> Option<EngineError> {
        let original = self.acceptance.as_mut()?;
        if !matches!(original.result, Some(Err(_))) {
            return None;
        }
        match original.result.take() {
            Some(Err(error)) => Some(error),
            other => {
                original.result = other;
                None
            }
        }
    }

    pub(crate) fn begin_refusal(&mut self, error: amqp::Error) -> bool {
        if self.is_retired() || self.refusal.is_some() {
            return false;
        }
        let Some(endpoint) = self.take_endpoint() else {
            return false;
        };
        self.refused = true;
        self.refusal = Some(Original::new(async move {
            match endpoint {
                LinkEndpoint::Sender(sender) => sender.close_with_error(error).await,
                LinkEndpoint::Receiver(receiver) => receiver.close_with_error(error).await,
            }
        }));
        true
    }

    pub(crate) async fn observe_refusal(&mut self) -> bool {
        if self.is_retired() {
            return false;
        }
        let Some(original) = self.refusal.as_mut() else {
            return false;
        };
        let completed = tokio::select! {
            biased;
            () = &mut self.retirement_observer => false,
            () = original.observe(PumpPoint::RefusalBegun) => true,
        };
        if !completed {
            self.retire();
        } else if let Some(payload) = original.panic.take() {
            resume_unwind(payload);
        }
        completed
    }

    pub(crate) fn set_primary(&mut self, primary: Primary) {
        let fault = matches!(&primary, Err(_) | Ok(Err(_)));
        if self.primary.is_none() {
            self.primary = Some(primary);
            if fault && let Some(retirement) = self.retirement.as_ref() {
                retirement.request();
            }
        }
    }

    pub(crate) async fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.retire_originals();
        if self.admitted.is_some() {
            self.finished = true;
            return;
        }
        if let Some(original) = self.acceptance.as_mut() {
            original.finish().await;
            if self.cleanup_panic.is_none() {
                self.cleanup_panic = original.panic.take();
            }
        }
        if let Some(original) = self.route.as_mut() {
            original.finish().await;
            if self.cleanup_panic.is_none() {
                self.cleanup_panic = original.panic.take();
            }
        }
        if let Some(original) = self.refusal.as_mut() {
            original.finish().await;
            if self.cleanup_panic.is_none() {
                self.cleanup_panic = original.panic.take();
            }
        }
        if !self.prepared_cleanup {
            if let Some(packet) = self
                .route
                .as_mut()
                .and_then(|original| original.result.as_mut())
            {
                let actual = match packet {
                    RoutePacket::Cbs {
                        registry,
                        address,
                        route,
                        responses,
                    } => {
                        responses.close();
                        let registry = Arc::clone(registry);
                        let address = address.clone();
                        let route = route.clone();
                        Box::pin(
                            async move { registry.unregister_reply_route(&address, &route).await },
                        ) as Pin<Box<dyn Future<Output = ()> + Send + 'a>>
                    }
                    RoutePacket::Management {
                        registry,
                        address,
                        route,
                        responses,
                    } => {
                        responses.close();
                        let registry = Arc::clone(registry);
                        let address = address.clone();
                        let route = route.clone();
                        Box::pin(
                            async move { registry.unregister_reply_route(&address, &route).await },
                        ) as Pin<Box<dyn Future<Output = ()> + Send + 'a>>
                    }
                };
                if self.unregister.is_none() {
                    self.unregister = Some(Original::new(actual));
                }
            }
            self.prepared_cleanup = true;
        }
        if let Some(original) = self.unregister.as_mut() {
            original.finish().await;
            if self.cleanup_panic.is_none() {
                self.cleanup_panic = original.panic.take();
            }
        }
        // The retired endpoint is not a fresh Close. Native End/Stop owns that
        // boundary; exact route removal completes before this packet is cleared.
        if let Some(original) = self.acceptance.as_mut()
            && matches!(original.result, Some(Ok(_)))
        {
            original.result = None;
        }
        self.finished = true;
    }

    fn report(&mut self) {
        if self.reported || self.admitted.is_some() {
            return;
        }
        self.reported = true;
        debug!(
            retired = self.retired,
            acceptance_started = self
                .acceptance
                .as_ref()
                .is_some_and(|original| original.started),
            acceptance_poisoned = self
                .acceptance
                .as_ref()
                .is_some_and(|original| original.poisoned),
            route_started = self.route.as_ref().is_some_and(|original| original.started),
            route_poisoned = self
                .route
                .as_ref()
                .is_some_and(|original| original.poisoned),
            "original control-link admission retained"
        );
    }

    pub(crate) fn resolve(
        &mut self,
        diagnostics: Option<PanicPayload>,
    ) -> Result<Option<AdmittedTask>, EngineError> {
        match self.primary.take() {
            Some(Err(payload)) => resume_unwind(payload),
            Some(Ok(Err(error))) => Err(error),
            Some(Ok(Ok(()))) => {
                if let Some(error) = self.take_acceptance_error() {
                    return match error {
                        EngineError::RemoteDetached => Ok(self.admitted.take()),
                        error => Err(error),
                    };
                }
                if let Some(payload) = self.cleanup_panic.take() {
                    resume_unwind(payload);
                }
                if !self.selected_retirement
                    && !self.refused
                    && let Some(payload) = diagnostics
                {
                    resume_unwind(payload);
                }
                Ok(self.admitted.take())
            }
            None => Err(EngineError::InvalidState(
                "control admission has no retained primary".into(),
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn acceptance_result(&self) -> Option<&Result<LinkEndpoint, EngineError>> {
        self.acceptance.as_ref()?.result.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn route_result(&self) -> Option<&RoutePacket> {
        self.route.as_ref()?.result.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn refusal_result(&self) -> Option<&Result<(), EngineError>> {
        self.refusal.as_ref()?.result.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn seed_acceptance(
        &mut self,
        actual: impl Future<Output = Result<LinkEndpoint, EngineError>> + Send + 'a,
    ) {
        self.acceptance = Some(Original::new(actual));
    }

    #[cfg(test)]
    pub(crate) fn seed_route(&mut self, actual: impl Future<Output = RoutePacket> + Send + 'a) {
        self.route = Some(Original::new(actual));
    }

    #[cfg(test)]
    pub(crate) fn seed_refusal(
        &mut self,
        actual: impl Future<Output = Result<(), EngineError>> + Send + 'a,
    ) {
        self.refusal = Some(Original::new(actual));
    }

    #[cfg(test)]
    pub(crate) fn seed_unregister(&mut self, actual: impl Future<Output = ()> + Send + 'a) {
        self.unregister = Some(Original::new(actual));
    }

    #[cfg(test)]
    pub(crate) async fn observe_unregister(&mut self) {
        if let Some(original) = self.unregister.as_mut() {
            original.finish().await;
        }
    }

    #[cfg(test)]
    pub(crate) fn cleanup_payload(&self) -> Option<&PanicPayload> {
        self.cleanup_panic.as_ref()
    }
}

async fn accept_native(
    custody: &mut ControlAttachmentCustody<'_>,
    attach: Attach,
) -> Result<bool, EngineError> {
    if !custody.begin_acceptance(attach) || !custody.observe_acceptance().await {
        return Ok(false);
    }
    pump_checkpoint(PumpPoint::AcceptancePacket).await;
    if matches!(
        custody
            .acceptance
            .as_ref()
            .and_then(|original| original.result.as_ref()),
        Some(Err(EngineError::RemoteDetached))
    ) {
        return Ok(false);
    }
    if let Some(error) = custody.take_acceptance_error() {
        return Err(error);
    }
    Ok(custody.endpoint().is_some() && !custody.is_retired())
}

async fn refuse(custody: &mut ControlAttachmentCustody<'_>, error: amqp::Error) {
    if custody.begin_refusal(error) {
        custody.observe_refusal().await;
    }
}

pub(crate) async fn serve_control_attachment<B: Broker>(
    context: ControlContext<'_, B>,
    mut attach: Attach,
) -> Result<Option<AdmittedTask>, EngineError> {
    let mut custody = ControlAttachmentCustody::new(context.session, context.retirement);
    let primary = AssertUnwindSafe(async {
        let source = attach.source.as_ref().and_then(|source| source.address.clone()).unwrap_or_default();
        let target = attach.target.as_ref().and_then(|target| target.address.clone()).unwrap_or_default();
        if attach.role == Role::Sender && attach.initial_delivery_count.is_none() {
            attach.initial_delivery_count = Some(0);
        }
        if let Some(authorization) = context.authorization
            && (target == crate::CBS_NODE || source == crate::CBS_NODE)
        {
            debug!(?attach, "accepting CBS link");
            if !accept_native(&mut custody, attach).await? {
                return Ok(());
            }
            match custody.endpoint() {
                Some(LinkEndpoint::Receiver(_)) if target == crate::CBS_NODE => {
                    let authorization = Arc::clone(authorization);
                    let retirement = context.retirement.cloned();
                    let prepared_task = PreparedLeafTask::new(LeafKind::CbsRequest);
                    pump_checkpoint(PumpPoint::Prepared).await;
                    if custody.is_retired() { return Ok(()); }
                    if let Some(LinkEndpoint::Receiver(receiver)) = custody.take_endpoint() {
                        custody.admitted = Some(prepared_task.spawn(async move {
                            serve_cbs_requests_with_retirement(receiver, authorization, retirement).await
                        }));
                    }
                }
                Some(LinkEndpoint::Sender(_)) if source == crate::CBS_NODE && !target.is_empty() => {
                    if !custody.begin_cbs_route(Arc::clone(authorization), target) || !custody.observe_route().await {
                        return Ok(());
                    }
                    pump_checkpoint(PumpPoint::RoutePacket).await;
                    let retirement = context.retirement.cloned();
                    let prepared_task = PreparedLeafTask::new(LeafKind::CbsReply);
                    pump_checkpoint(PumpPoint::Prepared).await;
                    if custody.is_retired() { return Ok(()); }
                    if let Some((sender, RoutePacket::Cbs { registry, address, route, responses })) = custody.take_reply(true) {
                        custody.admitted = Some(prepared_task.spawn(async move {
                            serve_cbs_replies_with_retirement(sender, address, route, responses, registry, retirement).await
                        }));
                    }
                }
                _ => refuse(&mut custody, error_for(amqp::AmqpError::InvalidField, "invalid CBS link".into())).await,
            }
            return Ok(());
        }

        let address = super::address_for_role(&attach.role, &source, &target);
        let Some(entity) = management_entity(address) else {
            return Err(EngineError::InvalidState("control admission requires a service address".into()));
        };
        let plan = match entity {
            Ok(entity) => {
                let link_authorization = match context.authorization {
                    Some(authorization) => {
                        let result = tokio::select! {
                            biased;
                            () = &mut custody.retirement_observer => { custody.retire(); return Ok(()); },
                            result = authorization.authorize_entity_any(entity.as_str(), &[Permission::Send, Permission::Listen]) => result,
                        };
                        match result {
                            Ok(resource) => Some(ManagementAuthorization::new(Arc::clone(authorization), resource)),
                            Err(_) => {
                                if accept_native(&mut custody, attach).await? {
                                    refuse(&mut custody, unauthorized_error(format!("neither Send nor Listen is authorized for {entity}"))).await;
                                }
                                return Ok(());
                            }
                        }
                    }
                    None => None,
                };
                Ok((entity, link_authorization))
            }
            Err(error) => Err(error_for(amqp::AmqpError::InvalidField, error.to_string())),
        };
        debug!(%address, ?attach, "accepting management link");
        if !accept_native(&mut custody, attach).await? {
            return Ok(());
        }
        let (entity, link_authorization) = match plan {
            Ok(plan) => plan,
            Err(error) => { refuse(&mut custody, error).await; return Ok(()); }
        };
        match custody.endpoint() {
            Some(LinkEndpoint::Receiver(_)) if target == address => {
                let namespace = context.namespace.clone();
                let broker = context.broker.clone();
                let management = Arc::clone(context.management);
                let retirement = context.retirement.cloned();
                let prepared_task = PreparedLeafTask::new(LeafKind::ManagementRequest);
                pump_checkpoint(PumpPoint::Prepared).await;
                if custody.is_retired() { return Ok(()); }
                if let Some(LinkEndpoint::Receiver(receiver)) = custody.take_endpoint() {
                    custody.admitted = Some(prepared_task.spawn(async move {
                        serve_management_requests_with_retirement(receiver, namespace, entity, broker, management, link_authorization, retirement).await
                    }));
                }
            }
            Some(LinkEndpoint::Sender(_)) if source == address && !target.is_empty() => {
                if !custody.begin_management_route(Arc::clone(context.management), target) || !custody.observe_route().await {
                    return Ok(());
                }
                pump_checkpoint(PumpPoint::RoutePacket).await;
                let retirement = context.retirement.cloned();
                let prepared_task = PreparedLeafTask::new(LeafKind::ManagementReply);
                pump_checkpoint(PumpPoint::Prepared).await;
                if custody.is_retired() { return Ok(()); }
                if let Some((sender, RoutePacket::Management { registry, address, route, responses })) = custody.take_reply(false) {
                    custody.admitted = Some(prepared_task.spawn(async move {
                        serve_management_replies_with_retirement(sender, address, route, responses, registry, link_authorization, retirement).await
                    }));
                }
            }
            _ => refuse(&mut custody, error_for(amqp::AmqpError::InvalidField, "invalid management link".into())).await,
        }
        Ok(())
    }).catch_unwind().await;
    custody.set_primary(primary);
    let cleanup = AssertUnwindSafe(custody.finish()).catch_unwind().await;
    let diagnostics = if cleanup.is_ok() {
        catch_unwind(AssertUnwindSafe(|| custody.report())).err()
    } else {
        None
    };
    if custody.cleanup_panic.is_none() {
        custody.cleanup_panic = cleanup.err();
    }
    custody.resolve(diagnostics)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpPoint {
    AcceptanceBegun,
    AcceptancePacket,
    RouteBegun,
    RoutePacket,
    Prepared,
    RefusalBegun,
}

async fn pump_fault(point: PumpPoint) {
    #[cfg(test)]
    if let Ok(fault) = PUMP_FAULT.try_with(Arc::clone)
        && fault.point == point
    {
        fault.reached.notify_one();
        fault.trigger.notified().await;
        std::panic::panic_any(Arc::clone(&fault.payload));
    }
    let _ = point;
    std::future::pending::<()>().await;
}

async fn pump_checkpoint(point: PumpPoint) {
    #[cfg(test)]
    if PUMP_FAULT
        .try_with(|fault| fault.point == point)
        .unwrap_or(false)
    {
        pump_fault(point).await;
    }
    let _ = point;
}

#[cfg(test)]
pub(crate) struct PumpFault {
    pub(crate) point: PumpPoint,
    pub(crate) reached: tokio::sync::Notify,
    pub(crate) trigger: tokio::sync::Notify,
    pub(crate) payload: Arc<str>,
}

#[cfg(test)]
impl PumpFault {
    pub(crate) fn new(point: PumpPoint) -> Arc<Self> {
        Arc::new(Self {
            point,
            reached: tokio::sync::Notify::new(),
            trigger: tokio::sync::Notify::new(),
            payload: Arc::from("controlled outer control-admission panic"),
        })
    }
}

#[cfg(test)]
tokio::task_local! { pub(crate) static PUMP_FAULT: Arc<PumpFault>; }
