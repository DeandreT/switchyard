use super::*;
use domain::{SessionCursor, SessionPageOutcome};
use std::fmt;
use tokio::time::Instant;

// Admission of another page is bounded; an admitted owner result is never timed out.
const PAGE_ADMISSION_BUDGET: Duration = Duration::from_secs(10);
type ReleaseResult = Result<CommandOutcome, BrokerRejection>;

pub(super) struct ProtocolPrimary(AmqpProtocolError);
impl std::ops::Deref for ProtocolPrimary {
    type Target = AmqpProtocolError;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl ProtocolPrimary {
    pub fn refusal(&self) -> AmqpProtocolError {
        self.0.clone()
    }
    #[cfg(test)]
    pub fn into_error(self) -> AmqpProtocolError {
        self.0
    }
}
impl fmt::Debug for ProtocolPrimary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProtocolPrimary")
            .field("condition", &self.0.condition.as_symbol())
            .finish()
    }
}
impl fmt::Display for ProtocolPrimary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0.condition.as_symbol().as_str())
    }
}
impl std::error::Error for ProtocolPrimary {}

pub(super) struct PlanningFailure {
    pub primary: ProtocolPrimary,
    pub origin: Option<EngineError>,
    pub release: Option<ReleaseResult>,
}
impl From<AmqpProtocolError> for PlanningFailure {
    fn from(primary: AmqpProtocolError) -> Self {
        Self {
            primary: ProtocolPrimary(primary),
            origin: None,
            release: None,
        }
    }
}
impl fmt::Debug for PlanningFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlanningFailure")
            .field("primary", &self.primary)
            .field("origin", &self.origin.as_ref().map(native_class))
            .field("release", &release_class(self.release.as_ref()))
            .finish()
    }
}
impl fmt::Display for PlanningFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "link planning refused ({}); release {}",
            self.primary,
            release_class(self.release.as_ref()).0
        )
    }
}
impl std::error::Error for PlanningFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.origin.as_ref() {
            Some(origin) => Some(origin),
            None => Some(&self.primary),
        }
    }
}
impl PlanningFailure {
    pub fn cleanup_failed(&self) -> bool {
        cleanup_failed(self.release.as_ref())
    }
    pub fn report(&self) {
        let (release_outcome, release_condition) = release_class(self.release.as_ref());
        debug!(primary_condition = %self.primary.condition.as_symbol().as_str(),
            origin = self.origin.as_ref().map(native_class), release_outcome, release_condition,
            "link planning refusal completed");
    }
}

pub(super) struct AcceptanceFailure {
    pub primary: EngineError,
    pub release: ReleaseResult,
}
impl fmt::Debug for AcceptanceFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AcceptanceFailure")
            .field("primary", &native_class(&self.primary))
            .field("release", &release_class(Some(&self.release)))
            .finish()
    }
}
impl fmt::Display for AcceptanceFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "native link acceptance failed ({}); release {}",
            native_class(&self.primary),
            release_class(Some(&self.release)).0
        )
    }
}
impl std::error::Error for AcceptanceFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.primary)
    }
}
impl AcceptanceFailure {
    pub fn cleanup_failed(&self) -> bool {
        cleanup_failed(Some(&self.release))
    }
    pub fn report(&self) {
        let (release_outcome, release_condition) = release_class(Some(&self.release));
        debug!(
            primary = native_class(&self.primary),
            release_outcome, release_condition, "native acceptance failure release completed"
        );
    }
}
fn cleanup_failed(release: Option<&ReleaseResult>) -> bool {
    release.is_some_and(|result| !matches!(result, Ok(CommandOutcome::SessionReleased)))
}

fn native_class(error: &EngineError) -> &'static str {
    match error {
        EngineError::Io(_) => "io",
        EngineError::RemoteClosed => "remote-closed",
        EngineError::RemoteDetached => "remote-detached",
        EngineError::Stopped => "stopped",
        EngineError::RemoteSettledWithoutOutcome => "remote-settled-without-outcome",
        EngineError::SendReservationRevoked => "send-reservation-revoked",
        EngineError::InvalidState(_) => "invalid-state",
        EngineError::SaslAuthentication(_) => "sasl-authentication",
        EngineError::MessageSizeExceeded { .. } => "message-size-exceeded",
        EngineError::Timeout(_) => "timeout",
    }
}
fn release_class(release: Option<&ReleaseResult>) -> (&'static str, Option<&'static str>) {
    match release {
        None => ("not-attempted", None),
        Some(Err(BrokerRejection::Unavailable(_))) => {
            ("broker-unavailable", Some(crate::RESOURCE_LOCKED))
        }
        Some(Err(BrokerRejection::Refused(error))) => {
            ("broker-refused", Some(crate::condition_for(error)))
        }
        Some(Ok(outcome)) => (outcome_class(outcome), None),
    }
}
fn outcome_class(outcome: &CommandOutcome) -> &'static str {
    match outcome {
        CommandOutcome::QueueCreated => "queue-created",
        CommandOutcome::Sent { .. } => "sent",
        CommandOutcome::Scheduled { .. } => "scheduled",
        CommandOutcome::ScheduledCancelled { .. } => "scheduled-cancelled",
        CommandOutcome::ScheduledActivated { .. } => "scheduled-activated",
        CommandOutcome::DuplicateHistoryExpired { .. } => "duplicate-history-expired",
        CommandOutcome::Received(_) => "received",
        CommandOutcome::Peeked(_) => "peeked",
        CommandOutcome::Completed => "completed",
        CommandOutcome::Abandoned { .. } => "abandoned",
        CommandOutcome::DeadLettered => "dead-lettered",
        CommandOutcome::Deferred => "deferred",
        CommandOutcome::LockRenewed { .. } => "lock-renewed",
        CommandOutcome::DeferredReceived(_) => "deferred-received",
        CommandOutcome::LocksExpired { .. } => "locks-expired",
        CommandOutcome::MessagesExpired { .. } => "messages-expired",
        CommandOutcome::SessionAccepted(_) => "session-accepted",
        CommandOutcome::SessionReleased => "session-released",
        CommandOutcome::SessionLockRenewed { .. } => "session-lock-renewed",
        CommandOutcome::SessionStateSet => "session-state-set",
        CommandOutcome::SessionState(_) => "session-state",
        CommandOutcome::SessionLocksExpired { .. } => "session-locks-expired",
        CommandOutcome::QueueUpdated => "queue-updated",
        CommandOutcome::BatchSent { .. } => "batch-sent",
        CommandOutcome::TopicCreated => "topic-created",
        CommandOutcome::SubscriptionCreated => "subscription-created",
        CommandOutcome::RuleCreated => "rule-created",
        CommandOutcome::RuleDeleted => "rule-deleted",
        CommandOutcome::TopicUpdated => "topic-updated",
        CommandOutcome::SubscriptionUpdated => "subscription-updated",
        CommandOutcome::QueueDeleted => "queue-deleted",
        CommandOutcome::TopicDeleted => "topic-deleted",
        CommandOutcome::SubscriptionDeleted => "subscription-deleted",
        CommandOutcome::SessionPage(_) => "session-page",
    }
}

pub(super) async fn release_accepted<B: Broker>(
    broker: &BoundBroker<B>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    accepted: &AcceptedSession,
) -> ReleaseResult {
    broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::ReleaseSession {
                session: accepted.hold(),
            },
        )
        .await
}

async fn check_original(
    original: Option<(&ServerSession, &amqp::IncomingAttach)>,
    authorization: Option<&LinkAuthorization>,
) -> Result<(), PlanningFailure> {
    if let Some(authorization) = authorization {
        authorization.ensure().await?;
    }
    let Some((session, attach)) = original else {
        return Err(error_for(
            AmqpError::InvalidField,
            "next-available acceptance requires an original incoming attach".into(),
        )
        .into());
    };
    session
        .validate_incoming_attach_origin(attach)
        .map_err(|origin| PlanningFailure {
            primary: ProtocolPrimary(error_for(
                AmqpError::InvalidField,
                "incoming attach origin is no longer valid".into(),
            )),
            origin: Some(origin),
            release: None,
        })
}
fn unavailable(description: &str) -> PlanningFailure {
    AmqpProtocolError::new(
        ErrorCondition::Custom(Symbol::from(crate::TIMEOUT)),
        description.to_owned(),
        None,
    )
    .into()
}

pub(super) async fn next_session<B: Broker>(
    broker: &BoundBroker<B>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    original: Option<(&ServerSession, &amqp::IncomingAttach)>,
    authorization: Option<&LinkAuthorization>,
) -> Result<AcceptedSession, PlanningFailure> {
    next_session_until(
        broker,
        namespace,
        entity,
        original,
        authorization,
        Instant::now() + PAGE_ADMISSION_BUDGET,
    )
    .await
}

pub(super) async fn next_session_until<B: Broker>(
    broker: &BoundBroker<B>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    original: Option<(&ServerSession, &amqp::IncomingAttach)>,
    authorization: Option<&LinkAuthorization>,
    cutoff: Instant,
) -> Result<AcceptedSession, PlanningFailure> {
    let mut after: Option<SessionCursor> = None;
    loop {
        check_original(original, authorization).await?;
        if Instant::now() >= cutoff {
            return Err(unavailable("session page admission budget expired"));
        }
        // No select/timeout surrounds an admitted command: its original reply may own a hold.
        let outcome = broker
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::AcceptNextSessionPage {
                    after,
                    lock_duration_millis: None,
                },
            )
            .await
            .map_err(|rejection| PlanningFailure::from(rejection_error(&rejection)))?;
        match outcome {
            CommandOutcome::SessionPage(SessionPageOutcome::Accepted(accepted)) => {
                if let Err(mut failure) = check_original(original, authorization).await {
                    failure.release =
                        Some(release_accepted(broker, namespace, entity, &accepted).await);
                    return Err(failure);
                }
                // A valid admitted result may be handed off after the page-admission cutoff.
                return Ok(accepted);
            }
            CommandOutcome::SessionPage(SessionPageOutcome::Continue(cursor)) => {
                after = Some(cursor)
            }
            CommandOutcome::SessionPage(SessionPageOutcome::End) => {
                return Err(unavailable("no session is available to accept"));
            }
            _ => {
                return Err(error_for(
                    AmqpError::InternalError,
                    "accepting a session page produced an unexpected outcome".into(),
                )
                .into());
            }
        }
    }
}
