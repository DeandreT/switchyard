//! Original session-grant and native-accept custody for one entity attach.

use std::{
    future::{Future, poll_fn},
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
use tracing::{debug, warn};

use crate::{
    Broker, BrokerRejection, SessionRequest,
    authorization::ConnectionAuthorization,
    management::{ConnectionManagement, SessionRegistration},
    read_session_filter, stamp_session_filter,
};

use super::{
    LinkAuthorization, detach_with, error_for, rejection_error, resolve_entity,
    session_attach_properties, settlement, unauthorized_error,
};

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
        if self.retired && !self.started {
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
                match self
                    .actual
                    .as_mut()
                    .expect("handoff retains its original phase")
                    .as_mut()
                    .poll(context)
                {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(result) => {
                        self.result = Some(result);
                        self.actual = None;
                        Poll::Ready(())
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

pub(super) async fn accept_entity_link<B: Broker>(
    session: &ServerSession,
    broker: &B,
    namespace: &NamespaceName,
    address: &str,
    mut attach: Attach,
    authorization: Option<&Arc<ConnectionAuthorization>>,
    management: &Arc<ConnectionManagement>,
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
    let mut handoff = AttachmentHandoff::new(session);
    let ended = session.on_end_owned();
    tokio::pin!(ended);
    let mut plan = tokio::select! {
        biased;
        () = &mut ended => {
            retire_handoff(&mut handoff, broker, namespace, None, management, None).await;
            return Ok(None);
        }
        plan = async {
            match resolved {
                Ok(entity) => prepare_link(entity, &attach, authorization).await,
                Err(error) => Err(error),
            }
        } => plan,
    };
    if session.is_ended() {
        retire_handoff(&mut handoff, broker, namespace, None, management, None).await;
        return Ok(None);
    }

    if let Ok(prepared) = &plan {
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
            handoff.begin_grant(async move {
                broker.submit(grant_namespace, grant_entity, command).await
            });
            let _ = handoff.observe().await;
            let grant = match handoff.take_step() {
                Some(HandoffStep::Grant(grant)) => grant,
                None => {
                    retire_handoff(
                        &mut handoff,
                        broker,
                        namespace,
                        Some(&prepared.entity),
                        management,
                        None,
                    )
                    .await;
                    return Ok(None);
                }
                Some(HandoffStep::Native(_)) => unreachable!("grant phase returns a grant"),
            };
            match grant {
                Ok(CommandOutcome::SessionAccepted(Some(accepted))) => {
                    handoff.remember_session(accepted)
                }
                Ok(CommandOutcome::SessionAccepted(None)) => {
                    plan = Err(AmqpProtocolError::new(
                        ErrorCondition::Custom(Symbol::from(crate::TIMEOUT)),
                        String::from("no session is available to accept"),
                        None,
                    ))
                }
                Ok(other) => {
                    plan = Err(error_for(
                        AmqpError::InternalError,
                        format!("accepting a session produced an unexpected outcome: {other:?}"),
                    ))
                }
                Err(rejection) => plan = Err(rejection_error(&rejection)),
            }
        }
    }

    if session.is_ended() {
        let entity = plan.as_ref().ok().map(|prepared| &prepared.entity);
        retire_handoff(&mut handoff, broker, namespace, entity, management, None).await;
        return Ok(None);
    }
    if let Some(accepted) = handoff.accepted()
        && let Some(source) = attach.source.as_mut()
    {
        stamp_session_filter(source, &accepted.session_id);
    }
    let properties = handoff.accepted().map(session_attach_properties);
    handoff.begin_accept(session.accept_attach_with_properties(
        attach,
        crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64,
        properties,
    ));
    let _ = handoff.observe().await;
    let native = match handoff.take_step() {
        Some(HandoffStep::Native(native)) => native,
        None => {
            let entity = plan.as_ref().ok().map(|prepared| &prepared.entity);
            retire_handoff(&mut handoff, broker, namespace, entity, management, None).await;
            return Ok(None);
        }
        Some(HandoffStep::Grant(_)) => unreachable!("native phase returns a native result"),
    };
    let endpoint = match native {
        Ok(endpoint) => endpoint,
        Err(error) => {
            let entity = plan.as_ref().ok().map(|prepared| &prepared.entity);
            retire_handoff(&mut handoff, broker, namespace, entity, management, None).await;
            return match error {
                EngineError::RemoteDetached => Ok(None),
                error => Err(error),
            };
        }
    };
    if session.is_ended() {
        // Observed End/driver exit has retired this endpoint. Dropping an
        // otherwise-live endpoint is not a Detach and is not used for refusal.
        drop(endpoint);
        let entity = plan.as_ref().ok().map(|prepared| &prepared.entity);
        retire_handoff(&mut handoff, broker, namespace, entity, management, None).await;
        return Ok(None);
    }
    let prepared = match plan {
        Ok(prepared) => prepared,
        Err(error) => {
            warn!(%address, condition = ?error.condition, "refusing link");
            detach_with(endpoint, error).await;
            retire_handoff(&mut handoff, broker, namespace, None, management, None).await;
            return Ok(None);
        }
    };
    let registration = match (&endpoint, handoff.accepted()) {
        (LinkEndpoint::Sender(sender), Some(accepted)) => {
            let mut detached = std::pin::pin!(sender.on_detach_owned());
            let mut retired = false;
            let registration = tokio::select! {
                biased;
                () = &mut ended => None,
                registration = management.install_session(
                    claim
                        .as_ref()
                        .expect("a receiving attachment captured its claim"),
                    accepted.hold(),
                    || {
                        retired = session.is_ended() || detached.as_mut().now_or_never().is_some();
                        !retired
                    },
                ) => registration,
            };
            if registration.is_none() {
                if retired || session.is_ended() || detached.as_mut().now_or_never().is_some() {
                    drop(endpoint);
                } else {
                    detach_with(
                        endpoint,
                        error_for(
                            AmqpError::IllegalState,
                            "session attachment was superseded".to_owned(),
                        ),
                    )
                    .await;
                }
                retire_handoff(
                    &mut handoff,
                    broker,
                    namespace,
                    Some(&prepared.entity),
                    management,
                    None,
                )
                .await;
                return Ok(None);
            }
            registration
        }
        _ => None,
    };
    if session.is_ended() {
        drop(endpoint);
        retire_handoff(
            &mut handoff,
            broker,
            namespace,
            Some(&prepared.entity),
            management,
            registration.as_ref(),
        )
        .await;
        return Ok(None);
    }
    let packet = handoff
        .take_packet()
        .expect("all original handoff work has been observed");
    Ok(Some(EntityLink {
        endpoint,
        entity: prepared.entity,
        accepted: packet.accepted,
        registration,
        authorization: prepared.authorization,
        mode,
    }))
}

async fn retire_handoff<B: Broker>(
    handoff: &mut AttachmentHandoff<'_>,
    broker: &B,
    namespace: &NamespaceName,
    entity: Option<&EntityPath>,
    management: &Arc<ConnectionManagement>,
    registration: Option<&SessionRegistration>,
) {
    let _ = handoff.finish().await;
    let packet = handoff
        .take_packet()
        .expect("retired handoff has no unobserved original work");
    debug!(phase = ?packet.phase, started = packet.started, retired = packet.retired, "retiring entity attach handoff");
    if let Some(HandoffStep::Native(Ok(endpoint))) = packet.step {
        drop(endpoint);
    }
    if let Some(accepted) = packet.accepted {
        let hold = accepted.hold();
        if let Some(registration) = registration {
            management.unregister_session(registration).await;
        }
        settlement::release_session(
            broker,
            namespace,
            entity.expect("a captured grant has an entity"),
            Some(&hold),
        )
        .await;
    }
}
