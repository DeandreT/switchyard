use crate::TopicConfig;

use super::*;

/// Copies retained by one topic publication or scheduled activation.
pub const MAX_TOPIC_FANOUT_COPIES: usize = 1_024;
/// Typed content, compatibility bodies, normalized IDs, and DLQ details.
pub const MAX_TOPIC_FANOUT_CONTENT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_TOPIC_FANOUT_VALUE_ITEMS: usize = 65_536;

const MISSING_SESSION_ID_DESCRIPTION: &str =
    "Session enabled entity doesn't allow a message whose session identifier is null.";

struct TopicMessagePlan<'a> {
    message: MessageInput<'a>,
    scheduled_enqueue_time: Option<Timestamp>,
    cost: TopicMessageCost,
    duplicate: bool,
    previous_history: Option<Timestamp>,
}

#[derive(Clone, Copy)]
pub(super) struct TopicMessageCost {
    content_bytes: usize,
    value_items: usize,
}

pub(super) struct TopicTargets {
    subscriptions: Vec<crate::SubscriptionDefinition>,
    shadows: Vec<EntityPath>,
}

pub(super) struct TopicEmission<'a> {
    pub(super) message: MessageInput<'a>,
    pub(super) sequence: SequenceNumber,
    pub(super) scheduled_enqueue_time: Option<Timestamp>,
}

#[derive(Clone, Copy, Default)]
pub(super) struct TopicBudget {
    input_messages: usize,
    input_bytes: usize,
    input_items: usize,
    retained_copies: usize,
    retained_bytes: usize,
    retained_items: usize,
}

impl TopicBudget {
    pub(super) fn charge_input(&mut self, cost: TopicMessageCost) -> Result<(), BrokerError> {
        self.input_messages = self.input_messages.saturating_add(1);
        self.input_bytes = self.input_bytes.saturating_add(cost.content_bytes);
        self.input_items = self.input_items.saturating_add(cost.value_items);
        enforce_input_limit(
            IngressBatchLimit::Messages,
            self.input_messages,
            MAX_INGRESS_BATCH_MESSAGES,
        )?;
        enforce_input_limit(
            IngressBatchLimit::ContentBytes,
            self.input_bytes,
            MAX_INGRESS_BATCH_CONTENT_BYTES,
        )?;
        enforce_input_limit(
            IngressBatchLimit::ValueItems,
            self.input_items,
            MAX_INGRESS_BATCH_VALUE_ITEMS,
        )
    }

    fn charge_copy(&mut self, cost: TopicMessageCost) -> Result<(), BrokerError> {
        self.retained_copies = checked_retained_total(
            self.retained_copies,
            1,
            1,
            IngressBatchLimit::Messages,
            MAX_TOPIC_FANOUT_COPIES,
        )?;
        self.retained_bytes = checked_retained_total(
            self.retained_bytes,
            cost.content_bytes,
            1,
            IngressBatchLimit::ContentBytes,
            MAX_TOPIC_FANOUT_CONTENT_BYTES,
        )?;
        self.retained_items = checked_retained_total(
            self.retained_items,
            cost.value_items,
            1,
            IngressBatchLimit::ValueItems,
            MAX_TOPIC_FANOUT_VALUE_ITEMS,
        )?;
        Ok(())
    }

    pub(super) fn charge_fanout(
        &mut self,
        cost: TopicMessageCost,
        message: MessageInput<'_>,
        targets: &TopicTargets,
    ) -> Result<(), BrokerError> {
        for subscription in &targets.subscriptions {
            let missing_session =
                subscription.config.requires_session && message.session_id.is_none();
            self.charge_copy(TopicMessageCost {
                content_bytes: cost.content_bytes.saturating_add(if missing_session {
                    DeadLetterReason::MissingSessionId.as_str().len()
                        + MISSING_SESSION_ID_DESCRIPTION.len()
                } else {
                    0
                }),
                value_items: cost
                    .value_items
                    .saturating_add(usize::from(missing_session) * 2),
            })?;
        }
        Ok(())
    }
}

pub(super) fn topic_message_cost(
    message: MessageInput<'_>,
) -> Result<TopicMessageCost, BrokerError> {
    Ok(TopicMessageCost {
        value_items: match message.envelope {
            Some(envelope) => envelope.validate_value_limits()?,
            None => 0,
        },
        content_bytes: message
            .envelope
            .map_or(0, MessageEnvelope::content_size)
            .saturating_add(message.body.len())
            .saturating_add(message.message_id.len())
            .saturating_add(
                message
                    .session_id
                    .map_or(0, |session| session.as_str().len()),
            ),
    })
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
        let targets = self.topic_targets(command)?;

        let queue_config = config.to_queue_config();
        let mut plans = Vec::with_capacity(messages.len());
        let mut budget = TopicBudget::default();
        // The admission plan borrows every payload. Even compound map-key
        // validation waits until input and retained-copy budgets are known.
        for (message, scheduled_enqueue_time) in messages {
            let cost = topic_message_cost(message)?;
            budget.charge_input(cost)?;
            plans.push(TopicMessagePlan {
                message,
                scheduled_enqueue_time,
                cost,
                duplicate: false,
                previous_history: None,
            });
        }

        let mut accepted_ids = BTreeSet::new();
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
            let future = plan
                .scheduled_enqueue_time
                .is_some_and(|enqueue_at| enqueue_at > command.issued_at);
            if future {
                // A future batch need not fit one activation, but each item
                // must be activatable against the topology known at admission.
                TopicBudget::default().charge_fanout(plan.cost, plan.message, &targets)?;
            }
            if !plan.duplicate {
                if deduplicate {
                    accepted_ids.insert(message_id);
                }
                if future {
                    budget.charge_copy(plan.cost)?;
                } else {
                    budget.charge_fanout(plan.cost, plan.message, &targets)?;
                }
            }
        }

        for plan in &plans {
            self.validate_topic_message(&queue_config, plan.message, &targets)?;
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
            if plan
                .scheduled_enqueue_time
                .is_some_and(|enqueue_at| enqueue_at > command.issued_at)
            {
                self.enqueue_message(
                    command.into(),
                    &queue_config,
                    plan.message,
                    sequence,
                    plan.scheduled_enqueue_time,
                    batch,
                )?;
            } else {
                self.emit_topic_message(
                    command,
                    &queue_config,
                    &targets,
                    TopicEmission {
                        message: plan.message,
                        sequence,
                        scheduled_enqueue_time: plan.scheduled_enqueue_time,
                    },
                    batch,
                    &mut enqueued,
                )?;
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

    pub(super) fn topic_targets(&self, command: &Command) -> Result<TopicTargets, BrokerError> {
        let subscriptions = self.subscriptions(&command.namespace, &command.entity)?;
        let shadows = subscriptions
            .iter()
            .map(|subscription| subscription.entity.dead_letter_queue())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TopicTargets {
            subscriptions,
            shadows,
        })
    }

    pub(super) fn validate_topic_message(
        &self,
        config: &QueueConfig,
        message: MessageInput<'_>,
        targets: &TopicTargets,
    ) -> Result<(), BrokerError> {
        validate_message_content(config, message)?;
        let content_bytes = message_content_bytes(message);
        for subscription in &targets.subscriptions {
            if content_bytes > subscription.config.max_message_bytes {
                return Err(BrokerError::MessageTooLarge {
                    body_bytes: content_bytes,
                    maximum_bytes: subscription.config.max_message_bytes,
                });
            }
        }
        Ok(())
    }

    pub(super) fn emit_topic_message(
        &self,
        command: &Command,
        config: &QueueConfig,
        targets: &TopicTargets,
        emission: TopicEmission<'_>,
        batch: &mut WriteBatch,
        enqueued: &mut BTreeSet<EntityPath>,
    ) -> Result<(), BrokerError> {
        let message = MessageInput {
            time_to_live_millis: effective_time_to_live_millis(
                config,
                emission.message.time_to_live_millis,
            ),
            ..emission.message
        };
        for (subscription, shadow) in targets.subscriptions.iter().zip(&targets.shadows) {
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
                    emission.sequence,
                    None,
                );
                // These fixed fields fit the ingress header reserve; their
                // content and projected value nodes were budgeted beforehand.
                record.dead_letter = Some(DeadLetterInfo {
                    reason: DeadLetterReason::MissingSessionId,
                    description: MISSING_SESSION_ID_DESCRIPTION.to_owned(),
                    dead_lettered_at: command.issued_at,
                });
                record.scheduled_enqueue_time = emission.scheduled_enqueue_time;
                batch.push_put(
                    keys::message(&command.namespace, shadow, emission.sequence),
                    codec::encode(&record)?,
                );
                batch.push_put(
                    keys::ready(&command.namespace, shadow, emission.sequence),
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
                    emission.sequence,
                    emission.scheduled_enqueue_time,
                    batch,
                )?;
                enqueued.insert(subscription.entity.clone());
            }
        }
        Ok(())
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
