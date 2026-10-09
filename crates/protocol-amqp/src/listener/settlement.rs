//! Receiving-link delivery and settlement.

use std::time::Duration;

use amqp::{
    AmqpError, DeliveryConfirmation, DeliveryState, DeliveryTag, EngineError, Fields, Outcome,
    PendingDelivery, Sender,
};
use domain::{
    CommandKind, CommandOutcome, Delivery, EntityPath, LockToken, NamespaceName, ReceiveMode,
    SessionHold,
};
use futures_util::FutureExt;
use serde_amqp::{Value, primitives::Symbol};
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::{
    Broker, BrokerRejection,
    management::{ConnectionManagement, DeliveryRegistration},
};

use super::{
    LinkAuthorization, ReceivingLinkProtocol, error_for, rejection_error, unauthorized_error,
};

#[cfg(test)]
mod attachment_handoff_tests;
mod custody;
#[cfg(test)]
mod delivery_owner_tests;
#[cfg(test)]
mod ingress_tests;
mod intake;
mod pending_transfer;
#[cfg(test)]
mod receiving_custody_tests;
#[cfg(test)]
mod test_support;
mod workers;
use custody::{OriginalCleanup, ReceivingCustody};
use intake::ReceiveIntake;
use pending_transfer::PendingTransfer;
#[cfg(test)]
use workers::SettlementWorkers;

/// How long a receiving link waits on a wakeup before asking the broker anyway.
///
/// The wakeup is the mechanism; this is the net under it. A notification can be
/// lost when several links wait on one entity, so a waiter re-asks on a coarse
/// interval rather than trusting the signal absolutely.
const EMPTY_QUEUE_FALLBACK: Duration = Duration::from_secs(3);

/// Bounds broker locks retained by one link even when the peer grants a very
/// large credit window and delays every disposition.
const MAX_IN_FLIGHT_DELIVERIES: usize = 32;

struct SettlementCompletion {
    registration: Option<DeliveryRegistration>,
    result: Result<(), SettlementFailure>,
}

#[derive(Clone)]
struct SettlementContext<B> {
    namespace: NamespaceName,
    entity: EntityPath,
    broker: B,
    authorization: Option<LinkAuthorization>,
    management: std::sync::Arc<ConnectionManagement>,
}

enum SettlementFailure {
    Unauthorized,
    Engine(EngineError),
    Protocol(crate::ProtocolError),
}

enum PumpExit {
    Clean,
    Unauthorized,
    Broker(BrokerRejection),
    Engine(EngineError),
    Protocol(crate::ProtocolError),
}

/// Drives a link the client receives on with bounded, credit-driven concurrency.
///
/// A broker delivery is fetched only after the preceding transfer consumed
/// remote credit and reached the wire. Once started, its remote disposition is
/// independent: several peek locks may remain outstanding and settle in any
/// order without stalling new credit.
#[cfg(test)]
pub(super) async fn serve_receiving_client<B: Broker>(
    sender: Sender,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: B,
    mode: ReceiveMode,
    session: Option<SessionHold>,
    protocol: ReceivingLinkProtocol,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let entry =
        prepare_receiving_entry(&sender, namespace, entity, broker, mode, session, protocol);
    serve_receiving_entry(sender, entry).await
}

pub(super) struct PreparedReceivingEntry<B> {
    namespace: NamespaceName,
    entity: EntityPath,
    broker: B,
    mode: ReceiveMode,
    session: Option<SessionHold>,
    authorization: Option<LinkAuthorization>,
    management: std::sync::Arc<ConnectionManagement>,
    link_name: String,
    context: SettlementContext<B>,
    custody: ReceivingCustody<'static>,
    detached: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
}

/// Prepare every callback/allocation-prone receiving-entry field before the
/// attachment owner transfers its ready Sender and exact session grant.
pub(super) fn prepare_receiving_entry<B: Broker>(
    sender: &Sender,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: B,
    mode: ReceiveMode,
    session: Option<SessionHold>,
    protocol: ReceivingLinkProtocol,
) -> PreparedReceivingEntry<B> {
    let ReceivingLinkProtocol {
        authorization,
        management,
        session_registration,
    } = protocol;
    let link_name = sender.name().to_owned();
    let settlement_context = SettlementContext {
        namespace: namespace.clone(),
        entity: entity.clone(),
        broker: broker.clone(),
        authorization: authorization.clone(),
        management: management.clone(),
    };
    let custody = ReceivingCustody::new(&settlement_context, session.clone(), session_registration);
    let detached = Box::pin(sender.on_detach_owned());
    PreparedReceivingEntry {
        namespace,
        entity,
        broker,
        mode,
        session,
        authorization,
        management,
        link_name,
        context: settlement_context,
        custody,
        detached,
    }
}

pub(super) async fn serve_receiving_entry<B: Broker>(
    sender: Sender,
    entry: PreparedReceivingEntry<B>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let PreparedReceivingEntry {
        namespace,
        entity,
        broker,
        mode,
        session,
        authorization,
        management,
        link_name,
        context: settlement_context,
        custody,
        mut detached,
    } = entry;
    let mut custody = custody.into_borrowed();

    // The pump borrows custody: unwinding it cannot drop admitted originals.
    let pumped = std::panic::AssertUnwindSafe(observe_pump(async {
    let ReceivingCustody {
        workers: in_flight, intake, received, transfer, transferred,
        transfer_registration, registrations: registered_deliveries,
        credit_release, ..
    } = &mut custody;
    'pump: loop {
        if in_flight.len() == MAX_IN_FLIGHT_DELIVERIES {
            tokio::select! {
                biased;
                () = wait_until_link_unauthorized(authorization.as_ref()), if authorization.is_some() => {
                    break 'pump PumpExit::Unauthorized;
                }
                drain = sender.on_drain() => match drain {
                    Ok(request) => {
                        if let Err(error) = sender.drained(request).await {
                            break 'pump PumpExit::Engine(error);
                        }
                    }
                    Err(error) => break 'pump PumpExit::Engine(error),
                },
                completion = in_flight.next() => {
                    let completion = completion
                        .expect("a full in-flight set cannot end before yielding a completion");
                    if let Some(exit) = handle_completion(completion, registered_deliveries) {
                        break 'pump exit;
                    }
                }
            }
            continue;
        }

        // Reserve before touching broker state. In receive-and-delete mode the
        // following Receive is irreversible, so readiness alone is not enough:
        // a concurrent drain must not revoke the credit that will carry it.
        let reservation = {
            let credit = sender.on_credit();
            tokio::pin!(credit);
            loop {
                tokio::select! {
                    biased;
                    () = wait_until_link_unauthorized(authorization.as_ref()), if authorization.is_some() => {
                        break 'pump PumpExit::Unauthorized;
                    }
                    completion = in_flight.next(), if !in_flight.is_empty() => {
                        let completion = completion
                            .expect("a non-empty in-flight set must yield a completion");
                        if let Some(exit) = handle_completion(completion, registered_deliveries) {
                            break 'pump exit;
                        }
                    }
                    credit = &mut credit => match credit {
                        Ok(reservation) => break reservation,
                        Err(error) => break 'pump PumpExit::Engine(error),
                    },
                }
            }
        };

        // Authorization preparation is not the broker's first-poll frontier.
        if let Some(authorization) = authorization.as_ref() {
            let authorized = tokio::select! {
                biased;
                () = &mut detached => break 'pump PumpExit::Clean,
                authorized = authorization.ensure() => authorized,
            };
            if authorized.is_err() {
                break 'pump PumpExit::Unauthorized;
            }
        }
        let wakeup = broker.deliverable(&namespace, &entity);
        tokio::pin!(wakeup);
        *intake = Some(ReceiveIntake::new(reservation, async {
            broker
                .submit(
                    namespace.clone(),
                    entity.clone(),
                    CommandKind::Receive {
                        mode,
                        lock_duration_millis: None,
                        session: session.clone(),
                    },
                )
                .await
        }));
        {
            let original = intake.as_mut().expect("one reserved Receive attempt");
            loop {
                tokio::select! {
                    biased;
                    () = &mut detached => break 'pump PumpExit::Clean,
                    () = wait_until_link_unauthorized(authorization.as_ref()), if authorization.is_some() => {
                        break 'pump PumpExit::Unauthorized;
                    }
                    completion = in_flight.next(), if !in_flight.is_empty() => {
                        let completion = completion
                            .expect("a non-empty in-flight set must yield a completion");
                        if let Some(exit) = handle_completion(completion, registered_deliveries) {
                            break 'pump exit;
                        }
                    }
                    _ = original.observe() => break,
                }
            }
        }
        *received = intake
            .as_mut()
            .expect("one reserved Receive attempt")
            .take_packet();
        #[cfg(test)]
        custody::pump_checkpoint(custody::PanicFrontier::ReceivePacket);
        let packet = received.as_ref().expect("the observed Receive owns a packet");
        let fetched = packet.result.as_ref().expect("active Receive was not retired").clone();
        let delivery = match received_delivery(fetched) {
            Ok(Some(delivery)) => delivery,
            Ok(None) => {
                let packet = received.take().expect("retained empty Receive packet");
                *credit_release = Some(OriginalCleanup::new(packet.reservation.release()));
                *intake = None;
                let original = credit_release.as_mut().expect("retained original credit release");
                original.finish().await;
                if let Some(payload) = original.take_panic() { std::panic::resume_unwind(payload); }
                if let Some(Err(error)) = original.take_result() {
                    break 'pump PumpExit::Engine(error);
                }
                *credit_release = None;
                let fallback = tokio::time::sleep(EMPTY_QUEUE_FALLBACK);
                tokio::pin!(fallback);
                loop {
                    tokio::select! {
                        biased;
                        () = wait_until_link_unauthorized(authorization.as_ref()), if authorization.is_some() => {
                            break 'pump PumpExit::Unauthorized;
                        }
                        completion = in_flight.next(), if !in_flight.is_empty() => {
                            let completion = completion
                                .expect("a non-empty in-flight set must yield a completion");
                            if let Some(exit) = handle_completion(completion, registered_deliveries) {
                                break 'pump exit;
                            }
                        }
                        drain = sender.on_drain() => match drain {
                            Ok(request) => {
                                if let Err(error) = sender.drained(request).await {
                                    break 'pump PumpExit::Engine(error);
                                }
                                break;
                            }
                            Err(error) => break 'pump PumpExit::Engine(error),
                        },
                        () = &mut wakeup => break,
                        () = &mut fallback => break,
                    }
                }
                continue 'pump;
            }
            Err(rejection) => {
                break 'pump PumpExit::Broker(rejection);
            }
        };

        // Azure sets DeadLetterSource only after a DLQ message has been
        // auto-forwarded to another entity, not while it is drained directly.
        let message = match crate::message::write_delivery_from(&delivery, None) {
            Ok(message) => message,
            Err(error) => break 'pump PumpExit::Protocol(error),
        };
        let lock_token = delivery.lock.map(|lock| lock.token);
        *transfer_registration = if let Some(lock) = delivery.lock {
            let registration = management
                .register_delivery(&link_name, entity.clone(), delivery.sequence, lock.token)
                .await;
            registered_deliveries.push(registration.clone());
            Some(registration)
        } else {
            None
        };
        let delivery_tag = match lock_token {
            Some(token) => lock_delivery_tag(token),
            None => sequence_delivery_tag(delivery.sequence),
        };

        // `send_pending` resolves only after this transfer consumed remote
        // credit and was written. Existing remote outcomes remain live while
        // it waits, so slow credit cannot serialize unrelated settlements.
        let packet = received.take().expect("retained Receive packet before native handoff");
        *transfer = Some(PendingTransfer::new(
            delivery,
            sender.send_pending_with_credit(packet.reservation, message, delivery_tag),
        ));
        *intake = None;
        {
            let original = transfer.as_mut().expect("one retained native start");
            loop {
                tokio::select! {
                    biased;
                    () = &mut detached => break 'pump PumpExit::Clean,
                    () = wait_until_link_unauthorized(authorization.as_ref()), if authorization.is_some() => {
                        break 'pump PumpExit::Unauthorized;
                    }
                    completion = in_flight.next(), if !in_flight.is_empty() => {
                        let completion = completion
                            .expect("a non-empty in-flight set must yield a completion");
                        if let Some(exit) = handle_completion(completion, registered_deliveries) {
                            break 'pump exit;
                        }
                    }
                    _ = original.observe() => break,
                }
            }
        }
        *transferred = transfer
            .as_mut()
            .expect("one retained native start")
            .take_packet();
        #[cfg(test)]
        custody::pump_checkpoint(custody::PanicFrontier::TransferPacket);
        let packet = transferred.as_ref().expect("the observed native start owns a packet");
        debug_assert!(packet.started && !packet.retired);
        if let Some(Err(error)) = packet.result.as_ref() {
                debug!(sequence = %packet.delivery.sequence, %error, "native start failed after broker delivery");
                let packet = transferred.take().expect("retained failed native packet");
                let Err(error) = packet.result.expect("failed native result") else { unreachable!() };
                *transfer = None;
                break 'pump PumpExit::Engine(error);
        }
        let packet = transferred.take().expect("retained successful native packet");
        *transfer = None;
        let Ok(pending) = packet.result.expect("active native start was not retired") else { unreachable!() };
        let delivery = packet.delivery;
        let registration = transfer_registration.take();
        let retirement = in_flight.subscribe();
        in_flight.spawn(
            registration.clone(),
            settle_started_delivery(
                pending,
                delivery,
                registration,
                settlement_context.clone(),
                retirement,
            ),
        );
    }
    })).catch_unwind().await;
    let exit = match pumped {
        Ok(exit) => Some(exit),
        Err(payload) => {
            custody.record_primary(payload);
            None
        }
    };

    custody.finish(&settlement_context).await;
    if let Some(payload) = custody.take_panic() {
        std::panic::resume_unwind(payload);
    }
    let join_error = custody.workers.into_join_error();
    let exit = exit.expect("a panicked pump is reported after original cleanup");

    match exit {
        // The engine already answered a remote Detach before surfacing the
        // notification. Closing this stale endpoint after the asynchronous
        // lock/session cleanup could target a new link that reused its handle.
        PumpExit::Clean => match join_error {
            Some(error) => Err(error.into()),
            None => Ok(()),
        },
        PumpExit::Unauthorized => {
            // A processed original Detach wins over a new late auth Close.
            tokio::select! {
                biased;
                () = &mut detached => {}
                closed = sender.close_with_error(unauthorized_error(
                    "the link's authorization has expired",
                )) => closed?,
            }
            Ok(())
        }
        PumpExit::Broker(rejection) => {
            sender.close_with_error(rejection_error(&rejection)).await?;
            Ok(())
        }
        PumpExit::Protocol(error) => {
            sender
                .close_with_error(error_for(AmqpError::InternalError, error.to_string()))
                .await?;
            Err(error.into())
        }
        PumpExit::Engine(
            EngineError::RemoteClosed | EngineError::RemoteDetached | EngineError::Stopped,
        ) => match join_error {
            Some(error) => Err(error.into()),
            None => Ok(()),
        },
        PumpExit::Engine(error) => {
            let _ = sender.close().await;
            Err(error.into())
        }
    }
}

async fn observe_pump<F: std::future::Future>(pump: F) -> F::Output {
    #[cfg(test)]
    {
        tokio::pin!(pump);
        std::future::poll_fn(|context| {
            custody::pump_poll(context);
            pump.as_mut().poll(context)
        })
        .await
    }
    #[cfg(not(test))]
    {
        pump.await
    }
}

fn handle_completion(
    completion: SettlementCompletion,
    registered_deliveries: &mut Vec<DeliveryRegistration>,
) -> Option<PumpExit> {
    if let Some(registration) = completion.registration.as_ref() {
        registered_deliveries.retain(|retained| retained != registration);
    }
    match completion.result {
        Ok(()) => None,
        Err(SettlementFailure::Unauthorized) => Some(PumpExit::Unauthorized),
        Err(SettlementFailure::Engine(
            EngineError::RemoteClosed | EngineError::RemoteDetached | EngineError::Stopped,
        )) => Some(PumpExit::Clean),
        Err(SettlementFailure::Engine(error)) => Some(PumpExit::Engine(error)),
        Err(SettlementFailure::Protocol(error)) => Some(PumpExit::Protocol(error)),
    }
}

async fn unregister_deliveries(
    management: &ConnectionManagement,
    registered_deliveries: &mut Vec<DeliveryRegistration>,
) {
    while let Some(registration) = registered_deliveries.last() {
        management.unregister_delivery(registration).await;
        registered_deliveries.pop();
    }
}

async fn wait_until_link_unauthorized(authorization: Option<&LinkAuthorization>) {
    match authorization {
        Some(authorization) => authorization.wait_until_unauthorized().await,
        None => std::future::pending().await,
    }
}

/// Frees the session a link held, so the next receiver need not wait out the
/// lock. Failure is survivable: expiry frees it anyway.
#[cfg(test)]
pub(super) async fn release_session<B: Broker>(
    broker: &B,
    namespace: &NamespaceName,
    entity: &EntityPath,
    session: Option<&SessionHold>,
) {
    let Some(hold) = session else { return };
    if let Err(rejection) = broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::ReleaseSession {
                session: hold.clone(),
            },
        )
        .await
    {
        debug!(session = %hold.session_id, %rejection, "session not released, leaving it to expire");
    }
}

fn received_delivery(
    result: intake::RawReceiveResult,
) -> Result<Option<Delivery>, BrokerRejection> {
    match result? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => {
            // A receive that produced anything else means the broker and the
            // edge disagree about the command, which is not a client problem.
            Err(BrokerRejection::Unavailable(format!(
                "receive produced an unexpected outcome: {other:?}"
            )))
        }
    }
}

/// Awaits and applies one independently identified remote disposition.
///
/// The lock is already committed and the transfer is already on the wire. A
/// peer that never answers therefore costs a redelivery rather than a lost
/// message, without preventing later delivery identities from settling first.
async fn settle_started_delivery<B: Broker>(
    pending: PendingDelivery,
    delivery: Delivery,
    registration: Option<DeliveryRegistration>,
    context: SettlementContext<B>,
    mut retirement: watch::Receiver<bool>,
) -> Result<(), SettlementFailure> {
    let SettlementContext {
        namespace,
        entity,
        broker,
        authorization,
        management,
    } = context;
    tokio::pin!(pending);
    // Observe an already-ready disposition even when retirement is sticky;
    // only a genuinely unanswered transport wait can be discarded.
    let remote = tokio::select! {
        biased;
        remote = &mut pending => Some(remote),
        () = wait_for_retirement(&mut retirement) => None,
    };
    if let Some(registration) = registration.as_ref() {
        management.unregister_delivery(registration).await;
    }
    let Some(remote) = remote else { return Ok(()) };
    let remote = remote.map_err(SettlementFailure::Engine)?;
    let (_, outcome, confirmation) = remote.into_parts();

    if let Some(authorization) = authorization.as_ref()
        && authorization.ensure().await.is_err()
    {
        confirm_if_needed(
            confirmation,
            DeliveryState::Rejected(amqp::Rejected {
                error: Some(unauthorized_error(
                    "the link's authorization expired before settlement committed",
                )),
            }),
            &mut retirement,
        )
        .await?;
        return Err(SettlementFailure::Unauthorized);
    }

    let Some(lock) = delivery.lock else {
        // Receive-and-delete is already durable. In receiver settle mode second
        // the peer still expects its outcome to be acknowledged, so echo the
        // state against this delivery's independent identity.
        return confirm_if_needed(
            confirmation,
            delivery_state_for_outcome(&outcome),
            &mut retirement,
        )
        .await;
    };
    let sequence = delivery.sequence;
    let (kind, expected) = match settlement_command(outcome, &delivery, lock.token) {
        Ok(settlement) => settlement,
        Err(error) => {
            confirm_if_needed(
                confirmation,
                DeliveryState::Rejected(amqp::Rejected {
                    error: Some(error_for(AmqpError::InternalError, error.to_string())),
                }),
                &mut retirement,
            )
            .await?;
            return Err(SettlementFailure::Protocol(error));
        }
    };

    // Service Bus treats the second-mode confirmation as the result of the
    // durable broker operation, not as an echo of the requested disposition.
    // A polled submission may already be queued, so retirement cannot cancel it.
    match broker.submit(namespace, entity, kind).await {
        Ok(outcome) if expected.matches(&outcome) => {
            confirm_if_needed(
                confirmation,
                DeliveryState::Accepted(amqp::Accepted),
                &mut retirement,
            )
            .await?
        }
        Ok(other) => {
            confirm_if_needed(
                confirmation,
                DeliveryState::Rejected(amqp::Rejected {
                    error: Some(error_for(
                        AmqpError::InternalError,
                        format!("settlement produced an unexpected outcome: {other:?}"),
                    )),
                }),
                &mut retirement,
            )
            .await?;
            warn!(%sequence, ?other, "settlement produced an unexpected broker outcome");
        }
        Err(rejection) => {
            confirm_if_needed(
                confirmation,
                DeliveryState::Rejected(amqp::Rejected {
                    error: Some(rejection_error(&rejection)),
                }),
                &mut retirement,
            )
            .await?;
            // A settlement that fails is not fatal to the link: the lock
            // expires and the message comes round again.
            warn!(%sequence, %rejection, "settlement failed, leaving the lock to expire");
        }
    }
    Ok(())
}

async fn confirm_if_needed(
    confirmation: Option<DeliveryConfirmation>,
    state: DeliveryState,
    retirement: &mut watch::Receiver<bool>,
) -> Result<(), SettlementFailure> {
    match confirmation {
        Some(confirmation) => tokio::select! {
            biased;
            () = wait_for_retirement(retirement) => Ok(()),
            result = confirmation.confirm(state) => result.map_err(SettlementFailure::Engine),
        },
        None => Ok(()),
    }
}

async fn wait_for_retirement(retirement: &mut watch::Receiver<bool>) {
    loop {
        if *retirement.borrow_and_update() || retirement.changed().await.is_err() {
            return;
        }
    }
}

fn settlement_command(
    outcome: Outcome,
    delivery: &Delivery,
    lock_token: LockToken,
) -> Result<(CommandKind, SettlementOutcome), crate::ProtocolError> {
    let sequence = delivery.sequence;
    match outcome {
        Outcome::Accepted(_) => Ok((
            CommandKind::Complete {
                sequence,
                lock_token,
            },
            SettlementOutcome::Completed,
        )),
        // Rejected means the client will never process it, so it goes to the
        // dead-letter queue rather than round again.
        Outcome::Rejected(rejected) => {
            let DeadLetterDisposition {
                reason,
                description,
                properties,
            } = dead_letter_disposition(rejected);
            let replacement_envelope = properties
                .as_ref()
                .map(|properties| crate::message::replacement_envelope(delivery, properties))
                .transpose()?;
            Ok((
                CommandKind::DeadLetter {
                    sequence,
                    lock_token,
                    reason,
                    description,
                    replacement_envelope,
                },
                SettlementOutcome::DeadLettered,
            ))
        }
        // Service Bus uses this Modified outcome for deferral. Property
        // changes ride in its message-annotations map even though the service
        // applies them to application properties.
        Outcome::Modified(modified) if modified.undeliverable_here == Some(true) => {
            let replacement_envelope = modified
                .message_annotations
                .as_ref()
                .map(|properties| crate::message::replacement_envelope(delivery, properties))
                .transpose()?;
            Ok((
                CommandKind::Defer {
                    sequence,
                    lock_token,
                    replacement_envelope,
                },
                SettlementOutcome::Deferred,
            ))
        }
        // Released means "not now": back to the queue, with the delivery
        // count already incremented by receive.
        Outcome::Released(_) => Ok((
            CommandKind::Abandon {
                sequence,
                lock_token,
                replacement_envelope: None,
            },
            SettlementOutcome::Abandoned,
        )),
        // The official .NET client carries Abandon properties-to-modify in the
        // Modified outcome's message-annotations field.
        Outcome::Modified(modified) => {
            let replacement_envelope = modified
                .message_annotations
                .as_ref()
                .filter(|properties| !properties.is_empty())
                .map(|properties| crate::message::replacement_envelope(delivery, properties))
                .transpose()?;
            Ok((
                CommandKind::Abandon {
                    sequence,
                    lock_token,
                    replacement_envelope,
                },
                SettlementOutcome::Abandoned,
            ))
        }
    }
}

fn delivery_state_for_outcome(outcome: &Outcome) -> DeliveryState {
    match outcome {
        Outcome::Accepted(value) => DeliveryState::Accepted(value.clone()),
        Outcome::Rejected(value) => DeliveryState::Rejected(value.clone()),
        Outcome::Released(value) => DeliveryState::Released(value.clone()),
        Outcome::Modified(value) => DeliveryState::Modified(value.clone()),
    }
}

#[derive(Clone, Copy)]
enum SettlementOutcome {
    Completed,
    Abandoned,
    DeadLettered,
    Deferred,
}

impl SettlementOutcome {
    fn matches(self, outcome: &CommandOutcome) -> bool {
        matches!(
            (self, outcome),
            (Self::Completed, CommandOutcome::Completed)
                | (Self::Abandoned, CommandOutcome::Abandoned { .. })
                | (Self::DeadLettered, CommandOutcome::DeadLettered)
                | (Self::Deferred, CommandOutcome::Deferred)
        )
    }
}

fn lock_delivery_tag(token: LockToken) -> DeliveryTag {
    let mut tag = [0_u8; 16];
    tag[8..].copy_from_slice(&token.as_u64().to_be_bytes());
    tag.to_vec().into()
}

fn sequence_delivery_tag(sequence: domain::SequenceNumber) -> DeliveryTag {
    sequence.as_u64().to_be_bytes().to_vec().into()
}

/// Reads the Service Bus dead-letter contract from a rejected delivery.
///
/// The official clients put an application-supplied reason and description in
/// the AMQP error's info map. A generic AMQP client may reject without either,
/// in which case the stable Switchyard fallback still explains how the message
/// reached the dead-letter queue.
#[derive(Debug, Eq, PartialEq)]
struct DeadLetterDisposition {
    reason: String,
    description: String,
    properties: Option<Fields>,
}

fn dead_letter_disposition(rejected: amqp::Rejected) -> DeadLetterDisposition {
    let Some(error) = rejected.error else {
        return DeadLetterDisposition {
            reason: String::from("RejectedByReceiver"),
            description: String::from("the receiver rejected the message"),
            properties: None,
        };
    };

    let reason = error
        .info
        .as_ref()
        .and_then(|info| string_field(info, crate::DEAD_LETTER_REASON_PROPERTY))
        .unwrap_or_else(|| String::from("RejectedByReceiver"));
    let description = error
        .info
        .as_ref()
        .and_then(|info| string_field(info, crate::DEAD_LETTER_DESCRIPTION_PROPERTY))
        .or(error.description)
        .unwrap_or_else(|| String::from("the receiver rejected the message"));
    // Azure's direct-link dead-letter outcome uses this condition and places
    // both reserved dead-letter metadata and application-property changes in
    // Error.info. Keep the custom fields in the durable envelope; reason and
    // description remain broker-owned dead-letter metadata.
    let properties = matches!(
        &error.condition,
        amqp::ErrorCondition::Custom(condition) if condition.as_str() == "com.microsoft:dead-letter"
    )
    .then(|| error.info.unwrap_or_default())
    .and_then(|mut properties| {
        properties.shift_remove(&Symbol::from(crate::DEAD_LETTER_REASON_PROPERTY));
        properties.shift_remove(&Symbol::from(crate::DEAD_LETTER_DESCRIPTION_PROPERTY));
        (!properties.is_empty()).then_some(properties)
    });
    DeadLetterDisposition {
        reason,
        description,
        properties,
    }
}

fn string_field(fields: &Fields, name: &str) -> Option<String> {
    fields
        .get(&Symbol::from(name))
        .and_then(|value| match value {
            Value::String(value) => Some(value.clone()),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use amqp::{Error as AmqpProtocolError, ErrorCondition, Modified, Released};
    use domain::{DeliveryOrigin, MessageEnvelope, Timestamp};

    use super::*;

    fn delivery(sequence: domain::SequenceNumber) -> Delivery {
        Delivery {
            sequence,
            message_id: String::from("message-7"),
            body: b"body".to_vec(),
            enqueued_at: Timestamp::from_millis(10),
            scheduled_enqueue_at: None,
            expires_at: None,
            delivery_count: 1,
            lock: None,
            session_id: None,
            dead_letter: None,
            envelope: None,
            origin: DeliveryOrigin::Ready,
        }
    }

    #[test]
    fn a_lock_token_is_a_guid_sized_delivery_tag() {
        let tag = lock_delivery_tag(LockToken::new(42));
        assert_eq!(tag.len(), 16);
        assert_eq!(&tag[8..], &42_u64.to_be_bytes());
    }

    #[test]
    fn a_service_bus_rejection_keeps_details_and_property_changes() {
        let sequence = domain::SequenceNumber::new(7);
        let token = LockToken::new(9);
        let mut original = amqp::Message::data(b"body".to_vec());
        original.application_properties = Some(
            amqp::ApplicationProperties::builder()
                .insert("existing", "kept")
                .build(),
        );
        let mut delivery = delivery(sequence);
        delivery.envelope = Some(MessageEnvelope::new(
            amqp::encode_message(&original).expect("the original message encodes"),
        ));
        let mut info = Fields::default();
        info.insert(
            Symbol::from(crate::DEAD_LETTER_REASON_PROPERTY),
            Value::String(String::from("InvalidOrder")),
        );
        info.insert(
            Symbol::from(crate::DEAD_LETTER_DESCRIPTION_PROPERTY),
            Value::String(String::from("the order has no customer")),
        );
        info.insert(
            Symbol::from("reviewed-by"),
            Value::String(String::from("fraud-team")),
        );
        let rejected = amqp::Rejected {
            error: Some(AmqpProtocolError::new(
                ErrorCondition::Custom(Symbol::from("com.microsoft:dead-letter")),
                "the receiver rejected the message",
                Some(info),
            )),
        };

        let (command, expected) = settlement_command(Outcome::Rejected(rejected), &delivery, token)
            .expect("the dead-letter envelope update is valid");
        let CommandKind::DeadLetter {
            sequence: dead_lettered_sequence,
            lock_token,
            reason,
            description,
            replacement_envelope: Some(replacement),
        } = command
        else {
            panic!("a Service Bus rejection must carry its property changes")
        };
        assert_eq!(dead_lettered_sequence, sequence);
        assert_eq!(lock_token, token);
        assert_eq!(reason, "InvalidOrder");
        assert_eq!(description, "the order has no customer");
        assert!(expected.matches(&CommandOutcome::DeadLettered));

        let replacement =
            amqp::decode_message(replacement.as_bytes()).expect("the replacement envelope decodes");
        let properties = replacement
            .application_properties
            .expect("application properties exist");
        assert_eq!(
            properties.get("existing"),
            Some(&Value::String("kept".into()))
        );
        assert_eq!(
            properties.get("reviewed-by"),
            Some(&Value::String("fraud-team".into()))
        );
    }

    #[test]
    fn a_generic_rejection_gets_stable_dead_letter_details() {
        assert_eq!(
            dead_letter_disposition(amqp::Rejected::default()),
            DeadLetterDisposition {
                reason: String::from("RejectedByReceiver"),
                description: String::from("the receiver rejected the message"),
                properties: None,
            }
        );
    }

    #[test]
    fn independent_outcomes_map_to_their_own_broker_commands() {
        let sequence = domain::SequenceNumber::new(7);
        let token = LockToken::new(9);
        let delivery = delivery(sequence);

        let (complete, expected) =
            settlement_command(Outcome::Accepted(amqp::Accepted), &delivery, token)
                .expect("accepted maps to complete");
        assert_eq!(
            complete,
            CommandKind::Complete {
                sequence,
                lock_token: token
            }
        );
        assert!(expected.matches(&CommandOutcome::Completed));

        for outcome in [
            Outcome::Released(Released),
            Outcome::Modified(Modified::default()),
        ] {
            let (abandon, expected) = settlement_command(outcome, &delivery, token)
                .expect("released outcomes map to abandon");
            assert_eq!(
                abandon,
                CommandKind::Abandon {
                    sequence,
                    lock_token: token,
                    replacement_envelope: None,
                }
            );
            assert!(expected.matches(&CommandOutcome::Abandoned {
                dead_lettered: false
            }));
        }
    }

    #[test]
    fn ordinary_modified_abandon_persists_property_changes() {
        let sequence = domain::SequenceNumber::new(7);
        let token = LockToken::new(9);
        let mut delivery = delivery(sequence);
        delivery.envelope = Some(MessageEnvelope::new(
            amqp::encode_message(&amqp::Message::data(b"body".to_vec()))
                .expect("the original message encodes"),
        ));
        let mut properties = Fields::new();
        properties.insert(Symbol::from("attempt"), Value::Int(2));

        let (command, expected) = settlement_command(
            Outcome::Modified(Modified {
                message_annotations: Some(properties),
                ..Modified::default()
            }),
            &delivery,
            token,
        )
        .expect("the abandon envelope update is valid");
        let CommandKind::Abandon {
            replacement_envelope: Some(replacement),
            ..
        } = command
        else {
            panic!("Modified abandon must carry a replacement envelope")
        };
        assert!(expected.matches(&CommandOutcome::Abandoned {
            dead_lettered: false
        }));
        let replacement =
            amqp::decode_message(replacement.as_bytes()).expect("the replacement envelope decodes");
        assert_eq!(
            replacement
                .application_properties
                .expect("application properties exist")
                .get("attempt"),
            Some(&Value::Int(2))
        );
    }

    #[test]
    fn service_bus_modified_defers_and_persists_property_changes() {
        let sequence = domain::SequenceNumber::new(7);
        let token = LockToken::new(9);
        let mut original = amqp::Message::data(b"body".to_vec());
        original.application_properties = Some(
            amqp::ApplicationProperties::builder()
                .insert("existing", "kept")
                .build(),
        );
        let mut delivery = delivery(sequence);
        delivery.envelope = Some(MessageEnvelope::new(
            amqp::encode_message(&original).expect("the original message encodes"),
        ));
        let mut properties = Fields::new();
        properties.insert(
            Symbol::from("deferred-by"),
            Value::String(String::from("dotnet")),
        );

        let (command, expected) = settlement_command(
            Outcome::Modified(Modified {
                undeliverable_here: Some(true),
                message_annotations: Some(properties),
                ..Modified::default()
            }),
            &delivery,
            token,
        )
        .expect("the envelope update is valid");
        let CommandKind::Defer {
            sequence: deferred_sequence,
            lock_token,
            replacement_envelope: Some(replacement),
        } = command
        else {
            panic!("Modified with undeliverable-here must defer")
        };
        assert_eq!(deferred_sequence, sequence);
        assert_eq!(lock_token, token);
        assert!(expected.matches(&CommandOutcome::Deferred));

        let replacement =
            amqp::decode_message(replacement.as_bytes()).expect("the replacement envelope decodes");
        let properties = replacement
            .application_properties
            .expect("application properties exist");
        assert_eq!(
            properties.get("existing"),
            Some(&Value::String("kept".into()))
        );
        assert_eq!(
            properties.get("deferred-by"),
            Some(&Value::String("dotnet".into()))
        );
    }

    #[tokio::test]
    async fn an_active_settlement_survives_its_pump_waiter_being_dropped() {
        let (started_tx, started) = tokio::sync::oneshot::channel();
        let (release_tx, release) = tokio::sync::oneshot::channel();
        let (finished_tx, finished) = tokio::sync::oneshot::channel();
        let mut workers = SettlementWorkers::new();
        workers.spawn(None, async move {
            let _ = started_tx.send(());
            let _ = release.await;
            let _ = finished_tx.send(());
            Ok(())
        });

        started.await.expect("the shielded settlement starts");
        drop(workers);
        release_tx
            .send(())
            .expect("dropping the waiter leaves the settlement alive");
        tokio::time::timeout(Duration::from_secs(1), finished)
            .await
            .expect("the detached settlement finishes promptly")
            .expect("the detached settlement retains its completion channel");
    }
}
