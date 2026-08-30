//! Receiving-link delivery and settlement.

use std::time::Duration;

use amqp::{AmqpError, DeliveryState, DeliveryTag, Fields, Outcome, Sender};
use domain::{
    CommandKind, CommandOutcome, Delivery, EntityPath, LockToken, NamespaceName, ReceiveMode,
    SessionHold,
};
use serde_amqp::{Value, primitives::Symbol};
use tracing::{debug, warn};

use crate::{Broker, BrokerRejection, management::ConnectionManagement};

use super::{
    LinkAuthorization, ReceivingLinkProtocol, error_for, rejection_error, unauthorized_error,
};

/// How long a receiving link waits on a wakeup before asking the broker anyway.
///
/// The wakeup is the mechanism; this is the net under it. A notification can be
/// lost when several links wait on one entity, so a waiter re-asks on a coarse
/// interval rather than trusting the signal absolutely.
const EMPTY_QUEUE_FALLBACK: Duration = Duration::from_secs(3);

/// Drives a link the client receives on: fetch, deliver, then settle as the
/// client's disposition says.
pub(super) async fn serve_receiving_client<B: Broker>(
    mut sender: Sender,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: B,
    mode: ReceiveMode,
    session: Option<SessionHold>,
    protocol: ReceivingLinkProtocol,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ReceivingLinkProtocol {
        authorization,
        management,
    } = protocol;

    loop {
        // The link is watched the whole time a message is being waited for. A
        // client that detaches while the queue is empty is waiting for an
        // answer, and a task that only polls the broker would never send one.
        let fetched = tokio::select! {
            biased;
            _ = sender.on_detach() => {
                release_session(&broker, &namespace, &entity, session.as_ref()).await;
                let _ = sender.close().await;
                return Ok(());
            }
            () = wait_until_link_unauthorized(authorization.as_ref()), if authorization.is_some() => {
                release_session(&broker, &namespace, &entity, session.as_ref()).await;
                sender
                    .close_with_error(unauthorized_error("the link's authorization has expired"))
                    .await?;
                return Ok(());
            }
            fetched = next_delivery(
                &broker,
                &namespace,
                &entity,
                mode,
                session.as_ref(),
                authorization.as_ref(),
            ) => fetched,
        };

        match fetched {
            Ok(delivery) => {
                if !settle(
                    &mut sender,
                    &namespace,
                    &entity,
                    &broker,
                    delivery,
                    authorization.as_ref(),
                    &management,
                )
                .await?
                {
                    release_session(&broker, &namespace, &entity, session.as_ref()).await;
                    sender
                        .close_with_error(unauthorized_error(
                            "the link's authorization expired before settlement",
                        ))
                        .await?;
                    return Ok(());
                }
            }
            Err(NextDeliveryError::Broker(rejection)) => {
                release_session(&broker, &namespace, &entity, session.as_ref()).await;
                sender.close_with_error(rejection_error(&rejection)).await?;
                return Ok(());
            }
            Err(NextDeliveryError::Unauthorized) => {
                release_session(&broker, &namespace, &entity, session.as_ref()).await;
                sender
                    .close_with_error(unauthorized_error("the link's authorization has expired"))
                    .await?;
                return Ok(());
            }
        }
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
async fn release_session<B: Broker>(
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

/// The next message the queue will part with, however long that takes.
async fn next_delivery<B: Broker>(
    broker: &B,
    namespace: &NamespaceName,
    entity: &EntityPath,
    mode: ReceiveMode,
    session: Option<&SessionHold>,
    authorization: Option<&LinkAuthorization>,
) -> Result<Delivery, NextDeliveryError> {
    loop {
        if let Some(authorization) = authorization
            && authorization.ensure().await.is_err()
        {
            return Err(NextDeliveryError::Unauthorized);
        }
        // Armed before the receive: a message that lands between the empty
        // answer below and the wait leaves a stored notification, so the wait
        // returns at once instead of sleeping on a queue that is not empty.
        let wakeup = broker.deliverable(namespace, entity);
        let outcome = broker
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::Receive {
                    mode,
                    lock_duration_millis: None,
                    session: session.cloned(),
                },
            )
            .await
            .map_err(NextDeliveryError::Broker)?;

        match outcome {
            CommandOutcome::Received(Some(delivery)) => return Ok(delivery),
            CommandOutcome::Received(None) => {
                tokio::select! {
                    () = wakeup => {}
                    () = tokio::time::sleep(EMPTY_QUEUE_FALLBACK) => {}
                }
            }
            other => {
                // A receive that produced anything else means the broker and the
                // edge disagree about the command, which is not a client problem.
                return Err(NextDeliveryError::Broker(BrokerRejection::Unavailable(
                    format!("receive produced an unexpected outcome: {other:?}"),
                )));
            }
        }
    }
}

enum NextDeliveryError {
    Broker(BrokerRejection),
    Unauthorized,
}

/// Hands one message to the client and applies whatever it said about it.
///
/// The lock is already committed, so a client that never answers costs a
/// redelivery rather than a lost message.
async fn settle<B: Broker>(
    sender: &mut Sender,
    namespace: &NamespaceName,
    entity: &EntityPath,
    broker: &B,
    delivery: Delivery,
    authorization: Option<&LinkAuthorization>,
    management: &ConnectionManagement,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let dead_letter_source = delivery
        .dead_letter
        .as_ref()
        .and_then(|_| entity.as_str().strip_suffix(crate::DEAD_LETTER_SUFFIX));
    let message = match crate::message::write_delivery_from(&delivery, dead_letter_source) {
        Ok(message) => message,
        Err(error) => {
            sender
                .close_with_error(error_for(AmqpError::InternalError, error.to_string()))
                .await?;
            return Err(error.into());
        }
    };
    let Some(lock) = delivery.lock else {
        let delivery_tag = sequence_delivery_tag(delivery.sequence);
        // Receive-and-delete: the message is already gone, so there is nothing
        // to settle after the transfer.
        let sent = match authorization {
            Some(authorization) => {
                tokio::select! {
                    outcome = sender.send(message, delivery_tag.clone()) => {
                        outcome?;
                        true
                    }
                    () = authorization.wait_until_unauthorized() => false,
                }
            }
            None => {
                sender.send(message, delivery_tag).await?;
                true
            }
        };
        return Ok(sent);
    };
    let sequence = delivery.sequence;
    let delivery_tag = lock_delivery_tag(lock.token);
    let link_name = sender.name().to_owned();
    management
        .register_delivery(&link_name, entity.clone(), sequence, lock.token)
        .await;
    let outcome = match authorization {
        Some(authorization) => {
            tokio::select! {
                outcome = sender.send_unconfirmed(message, delivery_tag.clone()) => Some(outcome),
                () = authorization.wait_until_unauthorized() => None,
            }
        }
        None => Some(sender.send_unconfirmed(message, delivery_tag).await),
    };
    management.unregister_delivery(&link_name, lock.token).await;
    let Some(outcome) = outcome else {
        return Ok(false);
    };
    let outcome = outcome?;
    if let Some(authorization) = authorization
        && authorization.ensure().await.is_err()
    {
        sender
            .confirm(DeliveryState::Rejected(amqp::Rejected {
                error: Some(unauthorized_error(
                    "the link's authorization expired before settlement committed",
                )),
            }))
            .await?;
        return Ok(false);
    }

    // Service Bus treats the sender's second-mode disposition as the result of
    // applying the requested settlement operation, not as an echo of that
    // request. Accepted means Complete, Abandon, or DeadLetter committed.
    let confirmation = DeliveryState::Accepted(amqp::Accepted);

    let (kind, expected) = match outcome {
        Outcome::Accepted(_) => (
            CommandKind::Complete {
                sequence,
                lock_token: lock.token,
            },
            SettlementOutcome::Completed,
        ),
        // Rejected means the client will never process it, so it goes to the
        // dead-letter queue rather than round again.
        Outcome::Rejected(rejected) => {
            let (reason, description) = dead_letter_details(rejected);
            (
                CommandKind::DeadLetter {
                    sequence,
                    lock_token: lock.token,
                    reason,
                    description,
                },
                SettlementOutcome::DeadLettered,
            )
        }
        // Released and modified both mean "not now": back to the queue, with the
        // delivery count already incremented by the receive.
        Outcome::Released(_) | Outcome::Modified(_) => (
            CommandKind::Abandon {
                sequence,
                lock_token: lock.token,
            },
            SettlementOutcome::Abandoned,
        ),
    };

    match broker.submit(namespace.clone(), entity.clone(), kind).await {
        Ok(outcome) if expected.matches(&outcome) => sender.confirm(confirmation).await?,
        Ok(other) => {
            sender
                .confirm(DeliveryState::Rejected(amqp::Rejected {
                    error: Some(error_for(
                        AmqpError::InternalError,
                        format!("settlement produced an unexpected outcome: {other:?}"),
                    )),
                }))
                .await?;
            warn!(%sequence, ?other, "settlement produced an unexpected broker outcome");
        }
        Err(rejection) => {
            sender
                .confirm(DeliveryState::Rejected(amqp::Rejected {
                    error: Some(rejection_error(&rejection)),
                }))
                .await?;
            // A settlement that fails is not fatal to the link: the lock expires
            // and the message comes round again.
            warn!(%sequence, %rejection, "settlement failed, leaving the lock to expire");
        }
    }
    Ok(true)
}

#[derive(Clone, Copy)]
enum SettlementOutcome {
    Completed,
    Abandoned,
    DeadLettered,
}

impl SettlementOutcome {
    fn matches(self, outcome: &CommandOutcome) -> bool {
        matches!(
            (self, outcome),
            (Self::Completed, CommandOutcome::Completed)
                | (Self::Abandoned, CommandOutcome::Abandoned { .. })
                | (Self::DeadLettered, CommandOutcome::DeadLettered)
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
fn dead_letter_details(rejected: amqp::Rejected) -> (String, String) {
    let Some(error) = rejected.error else {
        return (
            String::from("RejectedByReceiver"),
            String::from("the receiver rejected the message"),
        );
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
    (reason, description)
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
    use amqp::{Error as AmqpProtocolError, ErrorCondition};

    use super::*;

    #[test]
    fn a_lock_token_is_a_guid_sized_delivery_tag() {
        let tag = lock_delivery_tag(LockToken::new(42));
        assert_eq!(tag.len(), 16);
        assert_eq!(&tag[8..], &42_u64.to_be_bytes());
    }

    #[test]
    fn a_service_bus_rejection_keeps_its_dead_letter_details() {
        let mut info = Fields::default();
        info.insert(
            Symbol::from(crate::DEAD_LETTER_REASON_PROPERTY),
            Value::String(String::from("InvalidOrder")),
        );
        info.insert(
            Symbol::from(crate::DEAD_LETTER_DESCRIPTION_PROPERTY),
            Value::String(String::from("the order has no customer")),
        );
        let rejected = amqp::Rejected {
            error: Some(AmqpProtocolError::new(
                ErrorCondition::Custom(Symbol::from("com.microsoft:dead-letter")),
                "the receiver rejected the message",
                Some(info),
            )),
        };

        assert_eq!(
            dead_letter_details(rejected),
            (
                String::from("InvalidOrder"),
                String::from("the order has no customer")
            )
        );
    }

    #[test]
    fn a_generic_rejection_gets_stable_dead_letter_details() {
        assert_eq!(
            dead_letter_details(amqp::Rejected::default()),
            (
                String::from("RejectedByReceiver"),
                String::from("the receiver rejected the message")
            )
        );
    }
}
