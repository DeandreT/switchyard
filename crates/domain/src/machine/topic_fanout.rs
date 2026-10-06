use crate::TopicConfig;

use super::rules::{LoadedRules, RuleMatchBudget, SubscriptionMatch, matching_subscriptions};
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
    matches: Vec<SubscriptionMatch>,
}

#[derive(Clone, Copy)]
pub(super) struct TopicMessageCost {
    content_bytes: usize,
    value_items: usize,
}

pub(super) struct TopicTargets {
    subscriptions: Vec<crate::SubscriptionDefinition>,
    shadows: Vec<EntityPath>,
    rules: Vec<LoadedRules>,
}

pub(super) struct TopicEmission<'a> {
    pub(super) message: MessageInput<'a>,
    pub(super) sequence: SequenceNumber,
    pub(super) scheduled_enqueue_time: Option<Timestamp>,
    pub(super) matches: &'a [SubscriptionMatch],
    pub(super) counters: &'a mut QueueCounters,
}

struct TopicCopy<'a> {
    config: &'a QueueConfig,
    entity: &'a EntityPath,
    shadow: &'a EntityPath,
    message: MessageInput<'a>,
    sequence: SequenceNumber,
    scheduled_enqueue_time: Option<Timestamp>,
    route: RetentionRoute,
    envelope: Option<MessageEnvelope>,
}

const SQL_FILTER_ERROR_REASON: &str = "SwitchyardSqlFilterError";
const SQL_ACTION_ERROR_REASON: &str = "SwitchyardSqlActionError";

#[derive(Clone, Copy)]
enum RetentionRoute {
    None,
    Active,
    DeadLetter(TopicDeadLetter),
}

#[derive(Clone, Copy)]
enum TopicDeadLetter {
    MissingSession,
    Sql(crate::SqlEvaluationError),
    Action(crate::rule::SqlActionError),
}

impl TopicDeadLetter {
    fn reason_str(self) -> &'static str {
        match self {
            Self::MissingSession => "Session ID is null",
            Self::Sql(_) => SQL_FILTER_ERROR_REASON,
            Self::Action(_) => SQL_ACTION_ERROR_REASON,
        }
    }

    fn reason(self) -> DeadLetterReason {
        match self {
            Self::MissingSession => DeadLetterReason::MissingSessionId,
            Self::Sql(_) => DeadLetterReason::Application(SQL_FILTER_ERROR_REASON.to_owned()),
            Self::Action(_) => DeadLetterReason::Application(SQL_ACTION_ERROR_REASON.to_owned()),
        }
    }

    fn description(self) -> &'static str {
        use crate::SqlEvaluationError;
        match self {
            Self::MissingSession => MISSING_SESSION_ID_DESCRIPTION,
            Self::Action(error) => error.description(),
            Self::Sql(SqlEvaluationError::TypeMismatch) => {
                "SQL filter operands have incompatible types."
            }
            Self::Sql(SqlEvaluationError::UnsupportedValue) => {
                "SQL filter references an unsupported message value."
            }
            Self::Sql(SqlEvaluationError::NumericOverflow) => {
                "SQL filter integer arithmetic overflowed."
            }
            Self::Sql(SqlEvaluationError::DivisionByZero) => {
                "SQL filter integer arithmetic divided by zero."
            }
            Self::Sql(SqlEvaluationError::InvalidEscape) => "SQL filter LIKE escape is invalid.",
            Self::Sql(SqlEvaluationError::AmbiguousProperty) => {
                "SQL filter property names collide under lowercase comparison."
            }
            Self::Sql(SqlEvaluationError::StringOrderingUnsupported) => {
                "SQL filter string ordering is unsupported."
            }
            Self::Sql(SqlEvaluationError::NonPredicate) => {
                "SQL filter result is not a Boolean predicate."
            }
            Self::Sql(SqlEvaluationError::Limit { .. }) => "SQL filter evaluation failed.",
        }
    }
}

fn action_route(route: RetentionRoute, checked: &crate::rule::CheckedSqlAction) -> RetentionRoute {
    checked.error().map_or(route, |error| {
        RetentionRoute::DeadLetter(TopicDeadLetter::Action(error))
    })
}

fn retention_route(
    config: &crate::SubscriptionConfig,
    message: MessageInput<'_>,
    outcome: &SubscriptionMatch,
) -> RetentionRoute {
    match outcome {
        SubscriptionMatch::FilterError(error)
            if config.dead_lettering_on_filter_evaluation_exceptions =>
        {
            RetentionRoute::DeadLetter(TopicDeadLetter::Sql(*error))
        }
        SubscriptionMatch::NoMatch | SubscriptionMatch::FilterError(_) => RetentionRoute::None,
        SubscriptionMatch::Matched { .. }
            if config.requires_session && message.session_id.is_none() =>
        {
            RetentionRoute::DeadLetter(TopicDeadLetter::MissingSession)
        }
        SubscriptionMatch::Matched { .. } => RetentionRoute::Active,
    }
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
        matches: &[SubscriptionMatch],
    ) -> Result<(), BrokerError> {
        if matches.len() != targets.subscriptions.len() {
            return Err(BrokerError::DanglingRuleMetadata);
        }
        for (index, subscription) in targets.subscriptions.iter().enumerate() {
            let route = retention_route(&subscription.config, message, &matches[index]);
            match &matches[index] {
                SubscriptionMatch::Matched { no_action, actions } => {
                    if *no_action {
                        self.charge_route(cost, message, route)?;
                    }
                    for action in actions {
                        let (program, name) = targets.rules[index].action(action.rule)?;
                        let projected = action_cost(
                            message,
                            program,
                            &action.checked,
                            name,
                            &subscription.config.to_queue_config(),
                        )?;
                        self.charge_route(
                            projected,
                            message,
                            action_route(route, &action.checked),
                        )?;
                    }
                }
                _ => self.charge_route(cost, message, route)?,
            }
        }
        Ok(())
    }

    fn charge_route(
        &mut self,
        cost: TopicMessageCost,
        message: MessageInput<'_>,
        route: RetentionRoute,
    ) -> Result<(), BrokerError> {
        match route {
            RetentionRoute::None => Ok(()),
            RetentionRoute::Active => self.charge_copy(cost),
            RetentionRoute::DeadLetter(info) => self.charge_copy(TopicMessageCost {
                content_bytes: cost
                    .content_bytes
                    .saturating_sub(
                        message
                            .session_id
                            .map_or(0, |session| session.as_str().len()),
                    )
                    .saturating_add(info.reason_str().len())
                    .saturating_add(info.description().len()),
                value_items: cost.value_items.saturating_add(2),
            }),
        }
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
        let config = self.topic_config(&command.namespace, &command.entity)?;
        if config.is_some()
            && self
                .store
                .get(&keys::queue_config(&command.namespace, &command.entity))?
                .is_some()
        {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        Ok(config)
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
        let mut match_budget = RuleMatchBudget::default();
        // The admission plan borrows every payload. Even compound map-key
        // validation waits until input and retained-copy budgets are known.
        for (message, scheduled_enqueue_time) in messages {
            let cost = topic_message_cost(message)?;
            budget.charge_input(cost)?;
            let matches = self.topic_matches(message, &targets, &mut match_budget)?;
            plans.push(TopicMessagePlan {
                message,
                scheduled_enqueue_time,
                cost,
                duplicate: false,
                previous_history: None,
                matches,
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
                TopicBudget::default().charge_fanout(
                    plan.cost,
                    plan.message,
                    &targets,
                    &plan.matches,
                )?;
            }
            if !plan.duplicate {
                if deduplicate {
                    accepted_ids.insert(message_id);
                }
                if future {
                    budget.charge_copy(plan.cost)?;
                } else {
                    budget.charge_fanout(plan.cost, plan.message, &targets, &plan.matches)?;
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
                        matches: &plan.matches,
                        counters: &mut counters,
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
        let mut compile_budget = crate::SqlCompileBudget::default();
        let rules = subscriptions
            .iter()
            .map(|subscription| {
                self.load_rules(
                    &command.namespace,
                    &command.entity,
                    &subscription.name,
                    &mut compile_budget,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TopicTargets {
            subscriptions,
            shadows,
            rules,
        })
    }

    pub(super) fn topic_matches(
        &self,
        message: MessageInput<'_>,
        targets: &TopicTargets,
        budget: &mut RuleMatchBudget,
    ) -> Result<Vec<SubscriptionMatch>, BrokerError> {
        matching_subscriptions(message, &targets.rules, budget)
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
        if emission.matches.len() != targets.subscriptions.len() {
            return Err(BrokerError::DanglingRuleMetadata);
        }
        for (index, (subscription, shadow)) in targets
            .subscriptions
            .iter()
            .zip(&targets.shadows)
            .enumerate()
        {
            let subscription_config = subscription.config.to_queue_config();
            let outcome = &emission.matches[index];
            let route = retention_route(&subscription.config, message, outcome);
            if matches!(route, RetentionRoute::None) {
                continue;
            }
            let (base, actions) = match outcome {
                SubscriptionMatch::Matched { no_action, actions } => {
                    (*no_action, actions.as_slice())
                }
                _ => (true, &[][..]),
            };
            if base {
                self.emit_topic_copy(
                    command,
                    TopicCopy {
                        config: &subscription_config,
                        entity: &subscription.entity,
                        shadow,
                        message,
                        sequence: emission.sequence,
                        scheduled_enqueue_time: emission.scheduled_enqueue_time,
                        route,
                        envelope: None,
                    },
                    batch,
                    enqueued,
                )?;
            }
            for action in actions {
                let (program, name) = targets.rules[index].action(action.rule)?;
                let sequence = emission.counters.allocate_sequence()?;
                let envelope = action_envelope(message, program, &action.checked, name);
                self.emit_topic_copy(
                    command,
                    TopicCopy {
                        config: &subscription_config,
                        entity: &subscription.entity,
                        shadow,
                        message,
                        sequence,
                        scheduled_enqueue_time: emission.scheduled_enqueue_time,
                        route: action_route(route, &action.checked),
                        envelope: Some(envelope),
                    },
                    batch,
                    enqueued,
                )?;
            }
        }
        Ok(())
    }

    fn emit_topic_copy(
        &self,
        command: &Command,
        copy: TopicCopy<'_>,
        batch: &mut WriteBatch,
        enqueued: &mut BTreeSet<EntityPath>,
    ) -> Result<(), BrokerError> {
        let TopicCopy {
            config: subscription_config,
            entity,
            shadow,
            message,
            sequence,
            scheduled_enqueue_time,
            route,
            envelope,
        } = copy;
        if let RetentionRoute::DeadLetter(info) = route {
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
                    envelope: if envelope.is_some() {
                        None
                    } else {
                        message.envelope
                    },
                    ..message
                },
                sequence,
                None,
            );
            // These fixed fields fit the ingress header reserve; their
            // content and projected value nodes were budgeted beforehand.
            record.dead_letter = Some(DeadLetterInfo {
                reason: info.reason(),
                description: info.description().to_owned(),
                dead_lettered_at: command.issued_at,
            });
            if let Some(envelope) = envelope {
                record.envelope = Some(Box::new(envelope));
            }
            record.scheduled_enqueue_time = scheduled_enqueue_time;
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
            let scope = EnqueueScope {
                namespace: &command.namespace,
                entity,
                issued_at: command.issued_at,
            };
            if let Some(envelope) = envelope {
                let mut record = message_record(
                    scope,
                    subscription_config,
                    MessageInput {
                        envelope: None,
                        ..message
                    },
                    sequence,
                    scheduled_enqueue_time,
                );
                record.envelope = Some(Box::new(envelope));
                self.enqueue_record(scope, subscription_config, record, batch)?;
            } else {
                self.enqueue_message(
                    scope,
                    subscription_config,
                    message,
                    sequence,
                    scheduled_enqueue_time,
                    batch,
                )?;
            }
            enqueued.insert(entity.clone());
        }
        Ok(())
    }
}

fn action_cost(
    message: MessageInput<'_>,
    program: &crate::rule::SqlActionProgram,
    checked: &crate::rule::CheckedSqlAction,
    name: &str,
    config: &QueueConfig,
) -> Result<TopicMessageCost, BrokerError> {
    let (content, value_items, header) = MessageEnvelope::checked_action_projection(
        message.envelope,
        program,
        checked,
        "RuleName",
        name,
    )?;
    let overhead = message.envelope.map_or_else(
        || {
            message
                .session_id
                .map_or(0, |session| 5_usize.saturating_add(session.as_str().len()))
        },
        |envelope| {
            authoritative_property_overhead(envelope, message.message_id, message.session_id)
        },
    );
    let header_bytes = header
        .max(checked.peak().2)
        .saturating_add(overhead)
        .saturating_add(BROKER_HEADER_RESERVE_BYTES);
    if header_bytes > MAX_MESSAGE_HEADER_BYTES {
        return Err(BrokerError::MessageHeaderTooLarge {
            header_bytes,
            maximum_bytes: MAX_MESSAGE_HEADER_BYTES,
        });
    }
    let body_bytes = content
        .max(checked.peak().0)
        .saturating_add(overhead)
        .max(message.body.len());
    if body_bytes > config.max_message_bytes {
        return Err(BrokerError::MessageTooLarge {
            body_bytes,
            maximum_bytes: config.max_message_bytes,
        });
    }
    Ok(TopicMessageCost {
        content_bytes: content
            .saturating_add(message.body.len())
            .saturating_add(message.message_id.len())
            .saturating_add(
                message
                    .session_id
                    .map_or(0, |session| session.as_str().len()),
            ),
        value_items,
    })
}

fn action_envelope(
    message: MessageInput<'_>,
    program: &crate::rule::SqlActionProgram,
    checked: &crate::rule::CheckedSqlAction,
    name: &str,
) -> MessageEnvelope {
    let mut envelope = message
        .envelope
        .cloned()
        .unwrap_or_else(|| MessageEnvelope {
            properties: crate::MessageProperties {
                message_id: Some(MessageIdentifier::String(message.message_id.to_owned())),
                ..crate::MessageProperties::default()
            },
            body: crate::MessageBody::Data(vec![message.body.to_vec()]),
            ..MessageEnvelope::default()
        });
    program.apply_checked(checked, &mut envelope);
    envelope
        .application_properties
        .insert("RuleName".to_owned(), MessageValue::String(name.to_owned()));
    envelope
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
    fn finite_sql_error_fields_fit_the_ingress_header_reserve() -> Result<(), BrokerError> {
        use crate::SqlEvaluationError;
        for error in [
            SqlEvaluationError::TypeMismatch,
            SqlEvaluationError::UnsupportedValue,
            SqlEvaluationError::NumericOverflow,
            SqlEvaluationError::DivisionByZero,
            SqlEvaluationError::InvalidEscape,
            SqlEvaluationError::AmbiguousProperty,
            SqlEvaluationError::StringOrderingUnsupported,
            SqlEvaluationError::NonPredicate,
        ] {
            let info = TopicDeadLetter::Sql(error);
            let original = MessageEnvelope::default();
            let mut projected = original.clone();
            projected.application_properties.insert(
                "DeadLetterReason".into(),
                MessageValue::String(info.reason_str().into()),
            );
            projected.application_properties.insert(
                "DeadLetterErrorDescription".into(),
                MessageValue::String(info.description().into()),
            );
            let extra = projected.header_content_size() - original.header_content_size();
            assert!(extra <= BROKER_HEADER_RESERVE_BYTES - BROKER_BASE_HEADER_RESERVE_BYTES);
            projected.validate_property_quotas()?;
        }
        for error in [
            crate::rule::SqlActionError::TypeMismatch,
            crate::rule::SqlActionError::UnsupportedTargetType,
            crate::rule::SqlActionError::NumericOverflow,
        ] {
            let info = TopicDeadLetter::Action(error);
            let original = MessageEnvelope::default();
            let mut projected = original.clone();
            projected.application_properties.insert(
                "DeadLetterReason".into(),
                MessageValue::String(info.reason_str().into()),
            );
            projected.application_properties.insert(
                "DeadLetterErrorDescription".into(),
                MessageValue::String(info.description().into()),
            );
            let extra = projected.header_content_size() - original.header_content_size();
            assert!(extra <= BROKER_HEADER_RESERVE_BYTES - BROKER_BASE_HEADER_RESERVE_BYTES);
            projected.validate_property_quotas()?;
        }
        Ok(())
    }

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
