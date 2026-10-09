//! Original session-grant and native-accept custody for one entity attach.

use std::{
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use amqp::{
    AmqpError, Attach, EngineError, Error as AmqpProtocolError, ErrorCondition, LinkEndpoint, Role,
    SenderSettleMode, ServerSession,
};
use auth::Permission;
use domain::{
    AcceptedSession, CommandKind, CommandOutcome, EntityPath, NamespaceName, ReceiveMode,
};
use futures_util::FutureExt;
use serde_amqp::primitives::Symbol;
use tracing::{info, warn};

use crate::{
    Broker, BrokerRejection, SessionRequest,
    authorization::ConnectionAuthorization,
    management::{ConnectionManagement, SessionClaim, SessionRegistration},
    read_session_filter, stamp_session_filter,
};

use super::{
    LinkAuthorization, ReceivingLinkProtocol, error_for, rejection_error, resolve_entity,
    session_attach_properties, settlement, unauthorized_error,
};

pub(super) mod custody;
use custody::{AttachmentCustody, PumpPoint, pump_checkpoint, pump_fault};

pub(super) type RawGrantResult = Result<CommandOutcome, BrokerRejection>;
pub(super) type RawAttachResult = Result<LinkEndpoint, EngineError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HandoffPhase {
    Idle,
    Grant,
    Native,
}

pub(super) enum HandoffStep {
    Grant(RawGrantResult),
    Native(RawAttachResult),
}

pub(super) struct HandoffPacket {
    pub(super) phase: HandoffPhase,
    pub(super) started: bool,
    pub(super) retired: bool,
    pub(super) panicked: bool,
    pub(super) accepted: Option<AcceptedSession>,
    pub(super) step: Option<HandoffStep>,
}

pub(super) struct AttachmentHandoff<'a> {
    session: &'a ServerSession,
    phase: HandoffPhase,
    actual: Option<Pin<Box<dyn Future<Output = HandoffStep> + Send + 'a>>>,
    started: bool,
    retired: bool,
    step_open: bool,
    available: bool,
    result: Option<HandoffStep>,
    accepted: Option<AcceptedSession>,
    panicked: bool,
}

impl<'a> AttachmentHandoff<'a> {
    pub(super) fn new(session: &'a ServerSession) -> Self {
        Self {
            session,
            phase: HandoffPhase::Idle,
            actual: None,
            started: false,
            retired: false,
            step_open: false,
            available: true,
            result: None,
            accepted: None,
            panicked: false,
        }
    }

    pub(super) fn begin_grant(&mut self, actual: impl Future<Output = RawGrantResult> + Send + 'a) {
        assert_eq!(
            self.phase,
            HandoffPhase::Idle,
            "a grant may start only once"
        );
        self.begin(HandoffPhase::Grant, async move {
            HandoffStep::Grant(actual.await)
        });
    }

    pub(super) fn begin_accept(
        &mut self,
        actual: impl Future<Output = RawAttachResult> + Send + 'a,
    ) {
        assert_ne!(
            self.phase,
            HandoffPhase::Native,
            "native acceptance may start only once"
        );
        self.begin(HandoffPhase::Native, async move {
            HandoffStep::Native(actual.await)
        });
    }

    fn begin(
        &mut self,
        phase: HandoffPhase,
        actual: impl Future<Output = HandoffStep> + Send + 'a,
    ) {
        assert!(
            self.available && !self.retired,
            "cannot start retired or consumed handoff work"
        );
        assert!(
            !self.step_open && self.actual.is_none(),
            "must consume the preceding phase first"
        );
        self.phase = phase;
        self.actual = Some(Box::pin(actual));
        self.started = false;
        self.step_open = true;
    }

    pub(super) fn remember_session(&mut self, accepted: AcceptedSession) {
        assert!(
            self.available && self.accepted.is_none(),
            "a handoff holds one exact session grant"
        );
        self.accepted = Some(accepted);
    }

    pub(super) fn accepted(&self) -> Option<&AcceptedSession> {
        self.accepted.as_ref()
    }

    /// Canceling this borrowed observer preserves the pinned original and any
    /// raw result. None means the configured phase retired before its first poll.
    pub(super) async fn observe(&mut self) -> Option<&HandoffStep> {
        assert!(
            self.available && self.step_open,
            "cannot observe an absent or consumed phase"
        );
        if self.session.is_ended() {
            self.retire();
        }
        if self.panicked || (self.retired && !self.started) {
            return None;
        }
        if self.result.is_none() {
            poll_fn(|context| {
                if self.session.is_ended() {
                    self.retire();
                }
                if self.retired && !self.started {
                    return Poll::Ready(());
                }
                self.started = true;
                let polled = catch_unwind(AssertUnwindSafe(|| {
                    self.actual
                        .as_mut()
                        .expect("handoff retains its original phase")
                        .as_mut()
                        .poll(context)
                }));
                match polled {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(result)) => {
                        self.result = Some(result);
                        self.actual = None;
                        Poll::Ready(())
                    }
                    Err(payload) => {
                        self.panicked = true;
                        self.actual = None;
                        resume_unwind(payload)
                    }
                }
            })
            .await;
        }
        self.result.as_ref()
    }

    pub(super) fn retire(&mut self) {
        self.retired = true;
        if !self.started {
            self.actual = None;
        }
    }

    /// Drains only an already configured phase. In particular, finish never
    /// starts native acceptance after a grant or starts work for an idle plan.
    pub(super) async fn finish(&mut self) -> Option<&HandoffStep> {
        self.retire();
        if self.step_open {
            self.observe().await
        } else {
            None
        }
    }

    pub(super) fn take_step(&mut self) -> Option<HandoffStep> {
        if self.actual.is_some() || !self.step_open {
            return None;
        }
        self.step_open = false;
        self.result.take()
    }

    pub(super) fn take_packet(&mut self) -> Option<HandoffPacket> {
        if self.actual.is_some() || !self.available {
            return None;
        }
        self.available = false;
        Some(HandoffPacket {
            phase: self.phase,
            started: self.started,
            retired: self.retired,
            panicked: self.panicked,
            accepted: self.accepted.take(),
            step: self.result.take(),
        })
    }
}

pub(super) struct EntityLink {
    pub(super) endpoint: LinkEndpoint,
    pub(super) entity: EntityPath,
    pub(super) accepted: Option<AcceptedSession>,
    pub(super) registration: Option<SessionRegistration>,
    pub(super) authorization: Option<LinkAuthorization>,
    pub(super) mode: ReceiveMode,
}

struct PreparedLink {
    entity: EntityPath,
    authorization: Option<LinkAuthorization>,
    session: SessionRequest,
}

async fn prepare_link(
    entity: EntityPath,
    attach: &Attach,
    authorization: Option<&Arc<ConnectionAuthorization>>,
) -> Result<PreparedLink, AmqpProtocolError> {
    let authorization = match authorization {
        Some(authorization) => {
            let permission = match attach.role {
                Role::Sender => Permission::Send,
                Role::Receiver => Permission::Listen,
            };
            let resource = authorization
                .authorize_entity(entity.as_str(), permission)
                .await
                .map_err(|_| {
                    unauthorized_error(format!("{permission:?} is not authorized for {entity}"))
                })?;
            Some(LinkAuthorization {
                connection: Arc::clone(authorization),
                resource,
                permission,
            })
        }
        None => None,
    };
    let session = if attach.role == Role::Receiver {
        read_session_filter(attach.source.as_ref())
            .map_err(|error| error_for(AmqpError::InvalidField, error.to_string()))?
    } else {
        SessionRequest::None
    };
    Ok(PreparedLink {
        entity,
        authorization,
        session,
    })
}

#[cfg(test)]
pub(super) async fn accept_entity_link<B: Broker>(
    session: &ServerSession,
    broker: &B,
    namespace: &NamespaceName,
    address: &str,
    attach: Attach,
    authorization: Option<&Arc<ConnectionAuthorization>>,
    management: &Arc<ConnectionManagement>,
) -> Result<Option<EntityLink>, EngineError> {
    run_entity_handoff(
        session,
        broker,
        namespace,
        address,
        attach,
        authorization,
        management,
        false,
        None,
    )
    .await
}

#[cfg(test)]
pub(super) async fn serve_entity_attachment<B: Broker>(
    session: &ServerSession,
    broker: &B,
    namespace: &NamespaceName,
    address: &str,
    attach: Attach,
    authorization: Option<&Arc<ConnectionAuthorization>>,
    management: &Arc<ConnectionManagement>,
) -> Result<(), EngineError> {
    serve_entity_attachment_with_retirement(
        session,
        broker,
        namespace,
        address,
        attach,
        authorization,
        management,
        None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn serve_entity_attachment_with_retirement<B: Broker>(
    session: &ServerSession,
    broker: &B,
    namespace: &NamespaceName,
    address: &str,
    attach: Attach,
    authorization: Option<&Arc<ConnectionAuthorization>>,
    management: &Arc<ConnectionManagement>,
    retirement: Option<&super::ConnectionRetirementRequest>,
) -> Result<(), EngineError> {
    run_entity_handoff(
        session,
        broker,
        namespace,
        address,
        attach,
        authorization,
        management,
        true,
        retirement,
    )
    .await
    .map(|_| ())
}

#[expect(clippy::too_many_arguments)]
async fn run_entity_handoff<B: Broker>(
    session: &ServerSession,
    broker: &B,
    namespace: &NamespaceName,
    address: &str,
    attach: Attach,
    authorization: Option<&Arc<ConnectionAuthorization>>,
    management: &Arc<ConnectionManagement>,
    serve: bool,
    retirement: Option<&super::ConnectionRetirementRequest>,
) -> Result<Option<EntityLink>, EngineError> {
    let resolved = resolve_entity(address, attach.role.clone())
        .map_err(|error| error_for(AmqpError::InvalidField, error.to_string()));
    // Capture attachment ordering before authorization, grant, or native work
    // can await. Filling in a hold later must not mint a newer ownership claim.
    let claim = if attach.role == Role::Receiver && !session.is_ended() {
        resolved
            .as_ref()
            .ok()
            .map(|entity| management.claim_session(&attach.name, entity.clone()))
    } else {
        None
    };
    let mode = match attach.snd_settle_mode {
        SenderSettleMode::Settled => ReceiveMode::ReceiveAndDelete,
        SenderSettleMode::Unsettled | SenderSettleMode::Mixed => ReceiveMode::PeekLock,
    };
    let mut custody = AttachmentCustody::new(session).with_retirement(retirement);
    let primary = AssertUnwindSafe(async {
        let ready = entity_attachment_pump(
            session,
            broker,
            namespace,
            address,
            attach,
            authorization,
            management,
            resolved,
            claim.as_ref(),
            mode,
            &mut custody,
        )
        .await?;
        if !ready {
            return Ok(None);
        }
        pump_checkpoint(PumpPoint::Ready).await;
        if serve {
            prepare_and_adopt_with_retirement(
                session,
                broker,
                namespace,
                address,
                management,
                &mut custody,
                retirement,
            )
            .await;
            Ok(None)
        } else {
            Ok(Some(custody.adopt()))
        }
    })
    .catch_unwind()
    .await;
    if matches!(&primary, Err(_) | Ok(Err(_)))
        && let Some(retirement) = retirement
    {
        retirement.request();
    }
    let cleanup = AssertUnwindSafe(custody.finish(broker, namespace, management))
        .catch_unwind()
        .await;
    let diagnostics = if cleanup.is_ok() {
        catch_unwind(AssertUnwindSafe(|| custody.report())).err()
    } else {
        None
    };
    let secondary = cleanup
        .err()
        .or_else(|| custody.take_cleanup_panic())
        .or(diagnostics);
    match primary {
        Err(payload) => resume_unwind(payload),
        Ok(Err(error)) => Err(error),
        Ok(Ok(result)) => {
            if let Some(error) = custody.take_native_error() {
                return match error {
                    EngineError::RemoteDetached => Ok(result),
                    error => Err(error),
                };
            }
            if let Some(payload) = secondary {
                resume_unwind(payload);
            }
            Ok(result)
        }
    }
}

#[expect(clippy::too_many_arguments)]
async fn entity_attachment_pump<'a, B: Broker>(
    session: &'a ServerSession,
    broker: &'a B,
    namespace: &NamespaceName,
    address: &str,
    mut attach: Attach,
    authorization: Option<&Arc<ConnectionAuthorization>>,
    management: &Arc<ConnectionManagement>,
    resolved: Result<EntityPath, AmqpProtocolError>,
    claim: Option<&SessionClaim>,
    mode: ReceiveMode,
    custody: &mut AttachmentCustody<'a>,
) -> Result<bool, EngineError> {
    let ended = session.on_end_owned();
    tokio::pin!(ended);
    let mut plan = tokio::select! {
        biased;
        () = &mut ended => {
            return Ok(false);
        }
        plan = async {
            match resolved {
                Ok(entity) => prepare_link(entity, &attach, authorization).await,
                Err(error) => Err(error),
            }
        } => plan,
    };
    if session.is_ended() {
        return Ok(false);
    }

    if let Ok(prepared) = &plan {
        custody.entity = Some(prepared.entity.clone());
        let session_id = match &prepared.session {
            SessionRequest::None => None,
            SessionRequest::NextAvailable => Some(None),
            SessionRequest::Named(session_id) => Some(Some(session_id.clone())),
        };
        if let Some(session_id) = session_id {
            let grant_namespace = namespace.clone();
            let grant_entity = prepared.entity.clone();
            let command = CommandKind::AcceptSession {
                session_id,
                lock_duration_millis: None,
            };
            custody.handoff.begin_grant(async move {
                broker.submit(grant_namespace, grant_entity, command).await
            });
            pump_checkpoint(PumpPoint::Prepared).await;
            tokio::select! {
                biased;
                () = pump_fault(PumpPoint::Grant) => unreachable!("attachment fault checkpoint panics"),
                _ = custody.handoff.observe() => {},
            }
            custody.capture_grant();
            pump_checkpoint(PumpPoint::GrantResult).await;
            match custody.grant.as_ref() {
                None => return Ok(false),
                Some(Ok(CommandOutcome::SessionAccepted(Some(accepted)))) => {
                    custody.handoff.remember_session(accepted.clone())
                }
                Some(Ok(CommandOutcome::SessionAccepted(None))) => {
                    plan = Err(AmqpProtocolError::new(
                        ErrorCondition::Custom(Symbol::from(crate::TIMEOUT)),
                        String::from("no session is available to accept"),
                        None,
                    ))
                }
                Some(Ok(other)) => {
                    plan = Err(error_for(
                        AmqpError::InternalError,
                        format!("accepting a session produced an unexpected outcome: {other:?}"),
                    ))
                }
                Some(Err(rejection)) => plan = Err(rejection_error(rejection)),
            }
        }
    }

    if session.is_ended() {
        return Ok(false);
    }
    if let Some(accepted) = custody.handoff.accepted()
        && let Some(source) = attach.source.as_mut()
    {
        stamp_session_filter(source, &accepted.session_id);
    }
    let properties = custody.handoff.accepted().map(session_attach_properties);
    custody
        .handoff
        .begin_accept(session.accept_attach_with_properties(
            attach,
            crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64,
            properties,
        ));
    tokio::select! {
        biased;
        () = pump_fault(PumpPoint::Native) => unreachable!("attachment fault checkpoint panics"),
        _ = custody.handoff.observe() => {},
    }
    custody.capture_native();
    pump_checkpoint(PumpPoint::NativeResult).await;
    if !matches!(custody.native, Some(Ok(_))) {
        if matches!(custody.native, Some(Err(EngineError::RemoteDetached))) {
            return Ok(false);
        }
        return match custody.native.take() {
            None => Ok(false),
            Some(Err(error)) => Err(error),
            Some(Ok(_)) => unreachable!("successful native result remains captured"),
        };
    }
    if session.is_ended() {
        // Observed End/driver exit has retired this endpoint. Dropping an
        // otherwise-live endpoint is not a Detach and is not used for refusal.
        return Ok(false);
    }
    let prepared = match plan {
        Ok(prepared) => prepared,
        Err(error) => {
            warn!(%address, condition = ?error.condition, "refusing link");
            let Some(Ok(endpoint)) = custody.native.take() else {
                unreachable!("captured native endpoint")
            };
            custody.begin_refusal(endpoint, error);
            tokio::select! {
                biased;
                () = pump_fault(PumpPoint::Refusal) => unreachable!("attachment fault checkpoint panics"),
                _ = custody.observe_refusal() => {},
            }
            return Ok(false);
        }
    };
    let registration = match (custody.native.as_ref(), custody.handoff.accepted()) {
        (Some(Ok(LinkEndpoint::Sender(sender))), Some(accepted)) => {
            let mut detached = std::pin::pin!(sender.on_detach_owned());
            let mut retired = false;
            let registration = tokio::select! {
                biased;
                () = &mut ended => None,
                () = pump_fault(PumpPoint::Registry) => unreachable!("attachment fault checkpoint panics"),
                registration = management.install_session(
                    claim.expect("a receiving attachment captured its claim"),
                    accepted.hold(),
                    || {
                        retired = session.is_ended() || detached.as_mut().now_or_never().is_some();
                        !retired
                    },
                ) => registration,
            };
            if registration.is_none() {
                if !(retired || session.is_ended() || detached.as_mut().now_or_never().is_some()) {
                    let Some(Ok(endpoint)) = custody.native.take() else {
                        unreachable!("captured native endpoint")
                    };
                    custody.begin_refusal(
                        endpoint,
                        error_for(
                            AmqpError::IllegalState,
                            "session attachment was superseded".to_owned(),
                        ),
                    );
                    tokio::select! {
                        biased;
                        () = pump_fault(PumpPoint::Refusal) => unreachable!("attachment fault checkpoint panics"),
                        _ = custody.observe_refusal() => {},
                    }
                }
                return Ok(false);
            }
            registration
        }
        _ => None,
    };
    custody.registration = registration;
    pump_checkpoint(PumpPoint::Installed).await;
    if session.is_ended() {
        return Ok(false);
    }
    custody.capture_packet();
    let Some(Ok(endpoint)) = custody.native.take() else {
        unreachable!("captured native endpoint")
    };
    custody.ready = Some(EntityLink {
        endpoint,
        entity: prepared.entity,
        accepted: custody.packet.as_mut().unwrap().accepted.take(),
        registration: custody.registration.take(),
        authorization: prepared.authorization,
        mode,
    });
    Ok(true)
}

#[cfg(test)]
pub(super) async fn prepare_and_adopt<B: Broker>(
    session: &ServerSession,
    broker: &B,
    namespace: &NamespaceName,
    address: &str,
    management: &Arc<ConnectionManagement>,
    custody: &mut AttachmentCustody<'_>,
) {
    prepare_and_adopt_with_retirement(
        session, broker, namespace, address, management, custody, None,
    )
    .await
}

async fn prepare_and_adopt_with_retirement<B: Broker>(
    session: &ServerSession,
    broker: &B,
    namespace: &NamespaceName,
    address: &str,
    management: &Arc<ConnectionManagement>,
    custody: &mut AttachmentCustody<'_>,
    retirement: Option<&super::ConnectionRetirementRequest>,
) {
    let ready = custody
        .ready
        .as_ref()
        .expect("entry preparation borrows its ready packet");
    match &ready.endpoint {
        LinkEndpoint::Receiver(receiver) => {
            let namespace = namespace.clone();
            let entity = ready.entity.clone();
            let broker = broker.clone();
            let authorization = ready.authorization.clone();
            let retirement = retirement.cloned();
            pump_checkpoint(PumpPoint::EntryPrepared).await;
            info!(%address, entity = %ready.entity, "link attached");
            if session.is_ended() || receiver.on_detach_owned().now_or_never().is_some() {
                return;
            }
            let EntityLink {
                endpoint: LinkEndpoint::Receiver(receiver),
                ..
            } = custody.adopt()
            else {
                unreachable!("ready receiving endpoint retains its role")
            };
            tokio::spawn(async move {
                if let Err(error) = super::serve_sending_client_with_retirement(
                    receiver,
                    namespace,
                    entity,
                    broker,
                    authorization,
                    retirement,
                )
                .await
                {
                    warn!(%error, "sending link ended");
                }
            });
        }
        LinkEndpoint::Sender(sender) => {
            let entry = settlement::prepare_receiving_entry(
                sender,
                namespace.clone(),
                ready.entity.clone(),
                broker.clone(),
                ready.mode,
                ready.accepted.as_ref().map(AcceptedSession::hold),
                ReceivingLinkProtocol {
                    authorization: ready.authorization.clone(),
                    management: Arc::clone(management),
                    session_registration: ready.registration.clone(),
                },
            )
            .with_retirement(retirement.cloned());
            pump_checkpoint(PumpPoint::EntryPrepared).await;
            info!(%address, entity = %ready.entity,
                session = ready.accepted.as_ref().map(|accepted| accepted.session_id.as_str()), "link attached");
            if session.is_ended() || sender.on_detach_owned().now_or_never().is_some() {
                return;
            }
            let EntityLink {
                endpoint: LinkEndpoint::Sender(sender),
                ..
            } = custody.adopt()
            else {
                unreachable!("ready sending endpoint retains its role")
            };
            tokio::spawn(async move {
                if let Err(error) = settlement::serve_receiving_entry(sender, entry).await {
                    warn!(%error, "receiving link ended");
                }
            });
        }
    }
}
