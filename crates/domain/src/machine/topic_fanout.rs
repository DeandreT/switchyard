use crate::TopicConfig;

use super::*;

/// Copies retained by one immediate topic publication, across all subscribers.
pub const MAX_TOPIC_FANOUT_COPIES: usize = 1_024;
/// Typed content, compatibility bodies, normalized IDs, and DLQ details.
pub const MAX_TOPIC_FANOUT_CONTENT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_TOPIC_FANOUT_VALUE_ITEMS: usize = 65_536;

const MISSING_SESSION_ID_DESCRIPTION: &str =
    "Session enabled entity doesn't allow a message whose session identifier is null.";

struct TopicMessagePlan<'a> {
    message: MessageInput<'a>,
    content_bytes: usize,
    value_items: usize,
    duplicate: bool,
    previous_history: Option<Timestamp>,
}

impl<S: StateStore> StateMachine<S> {
    pub(super) fn topic_ingress_config(
        &self,
        command: &Command,
    ) -> Result<Option<TopicConfig>, BrokerError> {
        if command.entity.is_dead_letter_queue() {
            return Err(BrokerError::DeadLetterQueueIsReserved);
        }
        if command.entity.is_subscription_path() {
            return Err(BrokerError::SubscriptionPathIsReserved);
        }
        self.topic_config(&command.namespace, &command.entity)
    }

    pub(super) fn publish_topic<'a>(
        &self,
        command: &Command,
        config: TopicConfig,
        messages: impl ExactSizeIterator<Item = (MessageInput<'a>, Option<Timestamp>)>,
        batch: &mut WriteBatch,
        subscription_enqueues: &mut Option<Vec<EntityPath>>,
    ) -> Result<Vec<SequenceNumber>, BrokerError> {
        enforce_input_limit(
            IngressBatchLimit::Messages,
            messages.len(),
            MAX_INGRESS_BATCH_MESSAGES,
        )?;
        let messages: Vec<_> = messages.collect();
        let subscriptions = self.subscriptions(&command.namespace, &command.entity)?;
        if messages.iter().any(|(_, scheduled)| scheduled.is_some()) {
            return Err(BrokerError::TopicDataPlaneNotImplemented);
        }
        let shadows = subscriptions
            .iter()
            .map(|subscription| subscription.entity.dead_letter_queue())
            .collect::<Result<Vec<_>, _>>()?;

        let queue_config = config.to_queue_config();
        let mut plans = Vec::with_capacity(messages.len());
        let mut input_bytes = 0_usize;
        let mut input_items = 0_usize;
        // The admission plan borrows every payload. Even compound map-key
        // validation waits until input and retained-copy budgets are known.
        for (message, _) in messages {
            let value_items = match message.envelope {
                Some(envelope) => envelope.validate_value_limits()?,
                None => 0,
            };
            input_items = input_items.saturating_add(value_items);
            enforce_input_limit(
                IngressBatchLimit::ValueItems,
                input_items,
                MAX_INGRESS_BATCH_VALUE_ITEMS,
            )?;
            let content_bytes = message
                .envelope
                .map_or(0, MessageEnvelope::content_size)
                .saturating_add(message.body.len())
                .saturating_add(message.message_id.len())
                .saturating_add(
                    message
                        .session_id
                        .map_or(0, |session| session.as_str().len()),
                );
            input_bytes = input_bytes.saturating_add(content_bytes);
            enforce_input_limit(
                IngressBatchLimit::ContentBytes,
                input_bytes,
                MAX_INGRESS_BATCH_CONTENT_BYTES,
            )?;
            plans.push(TopicMessagePlan {
                message,
                content_bytes,
                value_items,
                duplicate: false,
                previous_history: None,
            });
        }

        let mut accepted_ids = BTreeSet::new();
        let mut retained_copies = 0_usize;
        let mut retained_bytes = 0_usize;
        let mut retained_items = 0_usize;
        for plan in &mut plans {
            let message_id = plan.message.message_id;
            validate_message_id(message_id)?;
            let deduplicate = config.requires_duplicate_detection && !message_id.is_empty();
            if deduplicate && !accepted_ids.contains(message_id) {
                plan.previous_history = self.read::<Timestamp>(&keys::duplicate_history(
                    &command.namespace,
                    &command.entity,
                    message_id,
                ))?;
            }
            plan.duplicate = deduplicate
                && (accepted_ids.contains(message_id)
                    || plan
                        .previous_history
                        .is_some_and(|expires_at| expires_at > command.issued_at));
            if !plan.duplicate {
                if deduplicate {
                    accepted_ids.insert(message_id);
                }
                retained_copies = checked_retained_total(
                    retained_copies,
                    1,
                    subscriptions.len(),
                    IngressBatchLimit::Messages,
                    MAX_TOPIC_FANOUT_COPIES,
                )?;
                for subscription in &subscriptions {
                    let missing_session =
                        subscription.config.requires_session && plan.message.session_id.is_none();
                    let (extra_bytes, extra_items) = if missing_session {
                        (
                            DeadLetterReason::MissingSessionId.as_str().len()
                                + MISSING_SESSION_ID_DESCRIPTION.len(),
                            2,
                        )
                    } else {
                        (0, 0)
                    };
                    retained_bytes = checked_retained_total(
                        retained_bytes,
                        plan.content_bytes.saturating_add(extra_bytes),
                        1,
                        IngressBatchLimit::ContentBytes,
                        MAX_TOPIC_FANOUT_CONTENT_BYTES,
                    )?;
                    retained_items = checked_retained_total(
                        retained_items,
                        plan.value_items.saturating_add(extra_items),
                        1,
                        IngressBatchLimit::ValueItems,
                        MAX_TOPIC_FANOUT_VALUE_ITEMS,
                    )?;
                }
            }
        }

        for plan in &plans {
            validate_message_content(&queue_config, plan.message)?;
            let content_bytes = message_content_bytes(plan.message);
            for subscription in &subscriptions {
                if content_bytes > subscription.config.max_message_bytes {
                    return Err(BrokerError::MessageTooLarge {
                        body_bytes: content_bytes,
                        maximum_bytes: subscription.config.max_message_bytes,
                    });
                }
            }
        }

        let mut counters = self.load_counters(command)?;
        let mut sequences = Vec::with_capacity(plans.len());
        for _ in &plans {
            sequences.push(counters.allocate_sequence()?);
        }
        let mut enqueued = BTreeSet::new();
        for (plan, sequence) in plans.iter().zip(sequences.iter().copied()) {
            if plan.duplicate {
                continue;
            }
            if config.requires_duplicate_detection && !plan.message.message_id.is_empty() {
                stage_message_id(
                    command,
                    &queue_config,
                    plan.message.message_id,
                    plan.previous_history,
                    batch,
                )?;
            }
            let message = MessageInput {
                time_to_live_millis: effective_time_to_live_millis(
                    &queue_config,
                    plan.message.time_to_live_millis,
                ),
                ..plan.message
            };
            for (subscription, shadow) in subscriptions.iter().zip(&shadows) {
                let subscription_config = subscription.config.to_queue_config();
                if subscription.config.requires_session && message.session_id.is_none() {
                    let scope = EnqueueScope {
                        namespace: &command.namespace,
                        entity: shadow,
                        issued_at: command.issued_at,
                    };
                    let mut record = message_record(
                        scope,
                        &subscription_config.dead_letter_shadow(),
                        MessageInput {
                            time_to_live_millis: None,
                            session_id: None,
                            ..message
                        },
                        sequence,
                        None,
                    );
                    // These fixed fields fit the ingress header reserve; their
                    // content and projected value nodes were budgeted above.
                    record.dead_letter = Some(DeadLetterInfo {
                        reason: DeadLetterReason::MissingSessionId,
                        description: MISSING_SESSION_ID_DESCRIPTION.to_owned(),
                        dead_lettered_at: command.issued_at,
                    });
                    batch.push_put(
                        keys::message(&command.namespace, shadow, sequence),
                        codec::encode(&record)?,
                    );
                    batch.push_put(
                        keys::ready(&command.namespace, shadow, sequence),
                        Vec::new(),
                    );
                    enqueued.insert(shadow.clone());
                } else {
                    self.enqueue_message(
                        EnqueueScope {
                            namespace: &command.namespace,
                            entity: &subscription.entity,
                            issued_at: command.issued_at,
                        },
                        &subscription_config,
                        message,
                        sequence,
                        None,
                        batch,
                    )?;
                    enqueued.insert(subscription.entity.clone());
                }
            }
        }
        if !plans.is_empty() {
            batch.push_put(
                keys::queue_counters(&command.namespace, &command.entity),
                codec::encode(&counters)?,
            );
        }
        *subscription_enqueues = Some(enqueued.into_iter().collect());
        Ok(sequences)
    }
}

fn enforce_input_limit(
    limit: IngressBatchLimit,
    actual: usize,
    maximum: usize,
) -> Result<(), BrokerError> {
    if actual > maximum {
        Err(BrokerError::IngressBatchLimitExceeded {
            limit,
            actual,
            maximum,
        })
    } else {
        Ok(())
    }
}

fn checked_retained_total(
    previous: usize,
    per_message: usize,
    destinations: usize,
    limit: IngressBatchLimit,
    maximum: usize,
) -> Result<usize, BrokerError> {
    match per_message
        .checked_mul(destinations)
        .and_then(|bytes| previous.checked_add(bytes))
    {
        Some(total) if total <= maximum => Ok(total),
        _ => Err(BrokerError::TopicFanoutTooLarge { limit, maximum }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_session_fields_fit_the_ingress_header_reserve() -> Result<(), BrokerError> {
        let original = MessageEnvelope::default();
        let mut projected = original.clone();
        projected.application_properties.insert(
            String::from("DeadLetterReason"),
            MessageValue::String(DeadLetterReason::MissingSessionId.as_str().to_owned()),
        );
        projected.application_properties.insert(
            String::from("DeadLetterErrorDescription"),
            MessageValue::String(MISSING_SESSION_ID_DESCRIPTION.to_owned()),
        );
        let extra = projected.header_content_size() - original.header_content_size();
        assert!(extra <= BROKER_HEADER_RESERVE_BYTES - BROKER_BASE_HEADER_RESERVE_BYTES);
        projected.validate_property_quotas()
    }
}
