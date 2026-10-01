use crate::TopicConfig;

use super::*;

/// Copies retained by one immediate topic publication, across all subscribers.
pub const MAX_TOPIC_FANOUT_COPIES: usize = 1_024;
/// Typed content, compatibility bodies, and normalized IDs retained by copies.
pub const MAX_TOPIC_FANOUT_CONTENT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_TOPIC_FANOUT_VALUE_ITEMS: usize = 65_536;

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
        if subscriptions
            .iter()
            .any(|subscription| subscription.config.requires_session)
            || messages
                .iter()
                .any(|(message, scheduled)| message.session_id.is_some() || scheduled.is_some())
        {
            return Err(BrokerError::TopicDataPlaneNotImplemented);
        }

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
                .saturating_add(message.message_id.len());
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
                retained_bytes = checked_retained_total(
                    retained_bytes,
                    plan.content_bytes,
                    subscriptions.len(),
                    IngressBatchLimit::ContentBytes,
                    MAX_TOPIC_FANOUT_CONTENT_BYTES,
                )?;
                retained_items = checked_retained_total(
                    retained_items,
                    plan.value_items,
                    subscriptions.len(),
                    IngressBatchLimit::ValueItems,
                    MAX_TOPIC_FANOUT_VALUE_ITEMS,
                )?;
            }
        }

        for plan in &plans {
            validate_message_input(&queue_config, plan.message)?;
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
            for subscription in &subscriptions {
                self.enqueue_message(
                    EnqueueScope {
                        namespace: &command.namespace,
                        entity: &subscription.entity,
                        issued_at: command.issued_at,
                    },
                    &subscription.config.to_queue_config(),
                    message,
                    sequence,
                    None,
                    batch,
                )?;
            }
        }
        if !plans.is_empty() {
            batch.push_put(
                keys::queue_counters(&command.namespace, &command.entity),
                codec::encode(&counters)?,
            );
        }
        *subscription_enqueues = Some(if plans.iter().any(|plan| !plan.duplicate) {
            subscriptions
                .into_iter()
                .map(|subscription| subscription.entity)
                .collect()
        } else {
            Vec::new()
        });
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
