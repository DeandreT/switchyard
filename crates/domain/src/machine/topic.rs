//! Durable topics and atomic, rule-routed subscription fanout.

use storage::{StateStore, WriteBatch};

use crate::{
    BrokerError, Command, CommandOutcome, EntityPath, FilterProperties, MAX_MESSAGE_ID_CHARACTERS,
    MAX_TOPIC_SUBSCRIPTIONS, MessageEnvelope, QueueConfig, SequenceNumber, SubscriptionConfig,
    SubscriptionName, TopicConfig, codec, keys,
};

use super::{
    PreparationFailure, StateMachine,
    incarnations::Kind,
    send::{SendInput, effective_time_to_live, message_record},
    topic_routing::{
        CompiledRules, ProgramCache, Route, TypedProperties, stage_filter_dead_letter,
    },
};

type SubscriptionFanoutState = (QueueConfig, CompiledRules);
type TopicFanoutState = (Vec<EntityPath>, Vec<SubscriptionFanoutState>);

/// Action work retained by one ordinary/batch/activation command. Discarded
/// attempts count too, so error policy cannot bypass the planning bound.
#[derive(Default)]
pub(super) struct ActionPlanBudget {
    attempts: usize,
    bytes: usize,
}

impl ActionPlanBudget {
    fn charge(
        &mut self,
        record: &crate::MessageRecord,
        properties: Option<&FilterProperties>,
    ) -> Result<(), BrokerError> {
        self.attempts = self.attempts.saturating_add(1);
        let original = record
            .envelope
            .as_ref()
            .map_or(record.body.len(), MessageEnvelope::len);
        let projection = properties.map_or(0, |p| {
            let system = [
                &p.correlation_id,
                &p.message_id,
                &p.to,
                &p.reply_to,
                &p.subject,
                &p.session_id,
                &p.reply_to_session_id,
                &p.content_type,
            ]
            .into_iter()
            .flatten()
            .fold(0usize, |n, s| n.saturating_add(s.len()));
            p.application_properties.iter().fold(system, |n, (k, v)| {
                n.saturating_add(k.len()).saturating_add(v.as_bytes().len())
            })
        });
        let bytes = original.saturating_add(projection);
        self.bytes = self
            .bytes
            .saturating_add(bytes)
            .saturating_add(crate::PROPERTY_PATCH_HEADROOM_BYTES);
        if self.attempts > 4096 || self.bytes > 64 * 1024 * 1024 {
            return Err(crate::RuleConfigError::SqlActionPlanLimit.into());
        }
        Ok(())
    }
}

pub(super) struct PlannedCopy {
    index: usize,
    sequence: SequenceNumber,
    lifetime: Option<u64>,
    envelope: Option<MessageEnvelope>,
    action: bool,
    failure: Option<(String, &'static str)>,
}

pub(super) fn reserve_sequence(
    counters: &mut crate::QueueCounters,
) -> Result<SequenceNumber, BrokerError> {
    let sequence = SequenceNumber::new(counters.next_sequence);
    counters.next_sequence = counters
        .next_sequence
        .checked_add(1)
        .ok_or(BrokerError::SequenceNumberExhausted)?;
    Ok(sequence)
}

/// Pure full-copy selection: every filter observes the original input and
/// every action starts from a separate original copy. No writes or clocks.
pub(super) fn plan_copies(
    record: &crate::MessageRecord,
    properties: &FilterProperties,
    state: &[SubscriptionFanoutState],
    topic_lifetime: Option<u64>,
    counters: &mut crate::QueueCounters,
    budget: &mut ActionPlanBudget,
) -> Result<Vec<PlannedCopy>, BrokerError> {
    let typed = TypedProperties::new(properties, uses_sql(state))?;
    let routing = typed.input();
    // Actions use typed canonical content, not an arbitrary legacy caller's
    // projection. A malformed envelope refuses the whole command; a raw message
    // has no application-property content and fails only matching action copies.
    let action_properties = if state.iter().any(|(_, rules)| rules.maximum_copies() > 1) {
        record
            .envelope
            .as_ref()
            .map(|envelope| {
                crate::content::effective_projection_with_removals(
                    envelope.as_bytes(),
                    envelope.overlay(),
                    envelope.removals(),
                    record.format.uses_text_ids(),
                )
            })
            .transpose()?
    } else {
        None
    };
    let action_typed = action_properties
        .as_ref()
        .map(|p| TypedProperties::new(p, true))
        .transpose()?;
    let mut plan = Vec::new();
    for (index, (config, rules)) in state.iter().enumerate() {
        for route in rules.route(&routing) {
            let action = matches!(route, Route::Action { .. } | Route::ActionFilterError(_));
            if action {
                budget.charge(record, action_properties.as_ref())?;
            }
            let mut envelope = if action {
                record.envelope.as_ref().map(|e| {
                    e.clone().with_filter_properties(
                        action_properties
                            .as_ref()
                            .expect("AMQP action input was derived")
                            .clone(),
                    )
                })
            } else {
                None
            };
            let failure = match route {
                Route::Deliver => None,
                Route::DeadLetter(description) | Route::ActionFilterError(description) => {
                    Some((description, crate::FILTER_EVALUATION_DEAD_LETTER_REASON))
                }
                Route::Action {
                    program,
                    dead_letter_on_failure,
                } => {
                    let mut copy = record.clone();
                    let result = action_typed
                        .as_ref()
                        .ok_or(())
                        .and_then(|typed| {
                            program
                                .evaluate(
                                    typed.input().context(),
                                    &mut crate::SqlEvaluationBudget::default(),
                                )
                                .map_err(|_| ())
                        })
                        .and_then(|patch| {
                            super::patch::apply_action(config, &mut copy, patch).map_err(|_| ())
                        });
                    match result {
                        Ok(()) => {
                            envelope = copy.envelope;
                            None
                        }
                        Err(()) if !dead_letter_on_failure => continue,
                        Err(()) => Some((
                            "SQL action failed; no changes were applied".to_owned(),
                            "ActionException",
                        )),
                    }
                }
            };
            if action
                && record.envelope.is_none()
                && record.body.len() > crate::CONTENT_LIMITS.max_bytes
            {
                return Err(BrokerError::MessageTooLarge {
                    body_bytes: record.body.len(),
                    maximum_bytes: crate::CONTENT_LIMITS.max_bytes,
                });
            }
            let sequence = if action {
                reserve_sequence(counters)?
            } else {
                record.sequence
            };
            plan.push(PlannedCopy {
                index,
                sequence,
                lifetime: effective_time_to_live(
                    topic_lifetime,
                    config.default_time_to_live_millis,
                ),
                envelope,
                action,
                failure,
            });
        }
    }
    Ok(plan)
}

pub(super) fn stage_copies(
    command: &Command,
    record: &crate::MessageRecord,
    subscriptions: &[EntityPath],
    plan: Vec<PlannedCopy>,
    populated: &mut [bool],
    dead_lettered: &mut [bool],
    batch: &mut WriteBatch,
) -> Result<(), BrokerError> {
    for planned in plan {
        let subscription = &subscriptions[planned.index];
        let mut copy = record.clone();
        copy.sequence = planned.sequence;
        if planned.action {
            copy.format = crate::MessageRecordFormat::V4 {
                text_ids: record.format.uses_text_ids(),
            };
            if copy.envelope.is_some() {
                copy.body.clear();
            }
        }
        if let Some(envelope) = planned.envelope {
            copy.envelope = Some(envelope);
            copy.body.clear();
        }
        if let Some((description, reason)) = planned.failure {
            dead_lettered[planned.index] = true;
            stage_filter_dead_letter(
                &command.namespace,
                subscription,
                copy,
                description,
                reason,
                command.issued_at,
                batch,
            )?;
            continue;
        }
        populated[planned.index] = true;
        copy.expires_at = planned
            .lifetime
            .map(|ms| command.issued_at.saturating_add_millis(ms));
        batch.push_put(
            keys::message(&command.namespace, subscription, copy.sequence),
            codec::message::encode(&copy)?,
        );
        batch.push_put(
            keys::ready(&command.namespace, subscription, copy.sequence),
            Vec::new(),
        );
        if let Some(expires_at) = copy.expires_at {
            batch.push_put(
                keys::expiry(&command.namespace, subscription, expires_at, copy.sequence),
                Vec::new(),
            );
        }
    }
    Ok(())
}

/// Captured by the complete bounded topology proof, before any update is staged.
pub(super) struct SubscriptionProfile {
    pub entity: EntityPath,
    pub backing: QueueConfig,
    pub shadow: QueueConfig,
    /// Recorded by the membership, never by the backing queue profile.
    pub dead_lettering_on_filter_evaluation_exceptions: bool,
}

impl<S: StateStore> StateMachine<S> {
    pub fn topic_config(
        &self,
        namespace: &crate::NamespaceName,
        topic: &EntityPath,
    ) -> Result<Option<TopicConfig>, BrokerError> {
        let Some(raw) = self.store().get(&keys::topic_config(namespace, topic))? else {
            return Ok(None);
        };
        let config = codec::topic::decode(&raw)?;
        if config.validate().is_err()
            || topic.is_subscription()
            || topic.is_dead_letter_queue()
            || topic.is_management()
            || self
                .store()
                .get(&keys::queue_config(namespace, topic))?
                .is_some()
        {
            return Err(BrokerError::TopicTopologyCorrupt);
        }
        self.require_live_owner(namespace, topic, Kind::Topic)?;
        Ok(Some(config))
    }

    /// Validates complete bounded membership before returning the requested prefix.
    pub fn subscriptions(
        &self,
        namespace: &crate::NamespaceName,
        topic: &EntityPath,
        limit: usize,
    ) -> Result<Vec<EntityPath>, BrokerError> {
        Ok(self
            .subscription_topology(namespace, topic)?
            .into_iter()
            .take(limit)
            .map(|profile| profile.entity)
            .collect())
    }

    pub(super) fn subscription_topology(
        &self,
        namespace: &crate::NamespaceName,
        topic: &EntityPath,
    ) -> Result<Vec<SubscriptionProfile>, BrokerError> {
        let prefix = keys::topic_subscription_prefix(namespace, topic);
        let entries = self
            .store()
            .scan_prefix(&prefix, MAX_TOPIC_SUBSCRIPTIONS + 1)?;
        let Some(parent) = self.topic_config(namespace, topic)? else {
            return Err(if entries.is_empty() {
                BrokerError::TopicNotFound
            } else {
                BrokerError::TopicTopologyCorrupt
            });
        };
        if entries.len() > MAX_TOPIC_SUBSCRIPTIONS {
            return Err(BrokerError::SubscriptionLimitExceeded {
                maximum: MAX_TOPIC_SUBSCRIPTIONS,
            });
        }
        let mut subscriptions = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let name = keys::subscription_name_parts(&prefix, &key)
                .ok_or(BrokerError::MalformedIndexKey)?;
            let name = SubscriptionName::new(name).map_err(|_| BrokerError::MalformedIndexKey)?;
            if keys::topic_subscription(namespace, topic, &name) != key {
                return Err(BrokerError::MalformedIndexKey);
            }
            let entity = topic.subscription(&name)?;
            // Only the exact canonical record of this child is a membership.
            let dead_lettering_on_filter_evaluation_exceptions =
                codec::membership::decode(&entity, &value)
                    .map_err(|_| BrokerError::TopicTopologyCorrupt)?;
            let config = self.validate_subscription_topology(namespace, &entity, parent)?;
            subscriptions.push(SubscriptionProfile {
                entity,
                backing: config,
                shadow: config.dead_letter_shadow(),
                dead_lettering_on_filter_evaluation_exceptions,
            });
        }
        Ok(subscriptions)
    }

    fn validate_subscription_topology(
        &self,
        namespace: &crate::NamespaceName,
        entity: &EntityPath,
        parent: TopicConfig,
    ) -> Result<QueueConfig, BrokerError> {
        let backing = self.raw_queue_config(namespace, entity)?.ok_or_else(|| {
            BrokerError::DanglingSubscription {
                entity: entity.clone(),
            }
        })?;
        // The membership's filter-error setting never shapes the backing.
        let config = SubscriptionConfig::from_profile(backing, true);
        let expected = config
            .validate()
            .map_err(|_| BrokerError::TopicTopologyCorrupt)?
            .queue_config(parent);
        let shadow = entity.dead_letter_queue()?;
        let expected_shadow = expected.dead_letter_shadow();
        if backing != expected
            || self.raw_queue_config(namespace, &shadow)? != Some(expected_shadow)
            || self
                .store()
                .get(&keys::topic_config(namespace, entity))?
                .is_some()
            || self
                .store()
                .get(&keys::topic_config(namespace, &shadow))?
                .is_some()
        {
            return Err(BrokerError::TopicTopologyCorrupt);
        }
        self.require_live_owner(namespace, entity, Kind::Subscription)?;
        Ok(backing)
    }

    pub(super) fn create_topic(
        &self,
        command: &Command,
        config: TopicConfig,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        if command.entity.is_dead_letter_queue()
            || command.entity.is_subscription()
            || command.entity.is_management()
        {
            return Err(BrokerError::EntityPathReserved);
        }
        let key = keys::topic_config(&command.namespace, &command.entity);
        if self.store().get(&key)?.is_some() {
            return Err(BrokerError::TopicAlreadyExists);
        }
        if self
            .store()
            .get(&keys::queue_config(&command.namespace, &command.entity))?
            .is_some()
        {
            return Err(BrokerError::EntityAlreadyExists);
        }

        let config = config.validate()?;
        if !self
            .store()
            .scan_prefix(
                &keys::topic_subscription_prefix(&command.namespace, &command.entity),
                1,
            )?
            .is_empty()
        {
            return Err(BrokerError::TopicTopologyCorrupt);
        }
        self.stage_new_owner(&command.namespace, &command.entity, Kind::Topic, batch)?;
        batch.push_put(key, codec::topic::encode(&config)?);
        Ok(CommandOutcome::TopicCreated)
    }

    pub(super) fn create_subscription(
        &self,
        command: &Command,
        name: &SubscriptionName,
        config: SubscriptionConfig,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, PreparationFailure> {
        let topic = self.load_topic_config(command)?;
        let index_key = keys::topic_subscription(&command.namespace, &command.entity, name);
        if self
            .store()
            .get(&index_key)
            .map_err(BrokerError::from)?
            .is_some()
        {
            return Err(BrokerError::SubscriptionAlreadyExists.into());
        }

        let subscriptions = self.subscriptions(
            &command.namespace,
            &command.entity,
            MAX_TOPIC_SUBSCRIPTIONS + 1,
        )?;
        if subscriptions.len() >= MAX_TOPIC_SUBSCRIPTIONS {
            // The complete bounded topology proof leaves only exact-full capacity.
            return Err(PreparationFailure::HealthySubscriptionCapacity);
        }

        let entity = command
            .entity
            .subscription(name)
            .map_err(BrokerError::from)?;
        let queue_key = keys::queue_config(&command.namespace, &entity);
        if self
            .store()
            .get(&queue_key)
            .map_err(BrokerError::from)?
            .is_some()
            || self
                .store()
                .get(&keys::topic_config(&command.namespace, &entity))
                .map_err(BrokerError::from)?
                .is_some()
        {
            return Err(BrokerError::EntityAlreadyExists.into());
        }

        // Validate the DLQ path now so a valid subscription can never discover
        // that its shadow is unaddressable only when the first message fails.
        let dead_letter_queue = entity.dead_letter_queue().map_err(BrokerError::from)?;
        let queue = config
            .validate()
            .map_err(BrokerError::from)?
            .queue_config(topic)
            .validate()
            .map_err(BrokerError::from)?;
        let shadow = queue.dead_letter_shadow();

        if self
            .store()
            .get(&keys::queue_config(&command.namespace, &dead_letter_queue))
            .map_err(BrokerError::from)?
            .is_some()
            || self
                .store()
                .get(&keys::topic_config(&command.namespace, &dead_letter_queue))
                .map_err(BrokerError::from)?
                .is_some()
        {
            return Err(BrokerError::TopicTopologyCorrupt.into());
        }

        self.stage_new_owner(&command.namespace, &entity, Kind::Subscription, batch)?;
        batch.push_put(
            index_key,
            codec::membership::encode(
                &entity,
                config.dead_lettering_on_filter_evaluation_exceptions,
            )
            .map_err(BrokerError::from)?,
        );
        batch.push_put(
            queue_key,
            codec::queue::encode(&queue).map_err(BrokerError::from)?,
        );
        batch.push_put(
            keys::queue_config(&command.namespace, &dead_letter_queue),
            codec::queue::encode(&shadow).map_err(BrokerError::from)?,
        );
        self.stage_default_rule(command, &entity, batch)?;
        Ok(CommandOutcome::SubscriptionCreated { entity })
    }

    pub(super) fn publish(
        &self,
        command: &Command,
        inputs: &[SendInput<'_>],
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        if inputs.is_empty() {
            return Err(BrokerError::EmptyMessageBatch);
        }
        let topic = self.load_topic_config(command)?;
        for input in inputs {
            validate_topic_input(&topic, input)?;
        }
        // Scheduled publications are routed only when they activate, but their
        // filter projection must be valid before accepting the placeholder. A
        // malformed durable projection must never poison every future timer
        // attempt for this topic.
        let properties = inputs
            .iter()
            .map(|input| filter_properties(input))
            .collect::<Result<Vec<_>, _>>()?;

        // An all-scheduled request must not snapshot the topology or its rules.
        // Those are read at the activation command's replicated-log position.
        let (subscriptions, subscription_state) = if inputs
            .iter()
            .any(|input| input.scheduled_enqueue_at.is_none())
        {
            self.topic_fanout_state(command)?
        } else {
            (Vec::new(), Vec::new())
        };

        let mut history = WriteBatch::default();
        let stored = self.stage_topic_duplicate_history(
            command,
            &topic,
            inputs.iter().map(|input| input.message_id),
            &mut history,
        )?;

        let mut counters = self.load_counters(command)?;
        let mut sequences = Vec::with_capacity(inputs.len());
        let mut populated = vec![false; subscriptions.len()];
        let mut dead_lettered = vec![false; subscriptions.len()];
        let mut budget = ActionPlanBudget::default();
        let mut prepared = Vec::new();
        for ((input, properties), stored) in inputs.iter().zip(&properties).zip(stored) {
            let sequence = reserve_sequence(&mut counters)?;
            sequences.push(sequence);
            if !stored {
                continue;
            }

            let topic_lifetime = effective_time_to_live(
                input.time_to_live_millis,
                topic.default_time_to_live_millis,
            );
            if let Some(enqueue_at) = input.scheduled_enqueue_at {
                let record = message_record(command, *input, sequence, topic_lifetime);
                prepared.push((record, Some(enqueue_at), Vec::new()));
                continue;
            }
            let record = message_record(command, *input, sequence, topic_lifetime);
            let plan = plan_copies(
                &record,
                properties,
                &subscription_state,
                topic_lifetime,
                &mut counters,
                &mut budget,
            )?;
            prepared.push((record, None, plan));
        }
        // Full reservation and bounded action evaluation precede every staged
        // message/history write, including the last child of a batch.
        for mutation in history.into_mutations() {
            match mutation {
                storage::Mutation::Put { key, value } => batch.push_put(key, value),
                storage::Mutation::Delete { key } => batch.push_delete(key),
            }
        }
        for (record, scheduled, plan) in prepared {
            if let Some(enqueue_at) = scheduled {
                batch.push_put(
                    keys::message(&command.namespace, &command.entity, record.sequence),
                    codec::message::encode(&record)?,
                );
                batch.push_put(
                    keys::scheduled(
                        &command.namespace,
                        &command.entity,
                        enqueue_at,
                        record.sequence,
                    ),
                    Vec::new(),
                );
            } else {
                stage_copies(
                    command,
                    &record,
                    &subscriptions,
                    plan,
                    &mut populated,
                    &mut dead_lettered,
                    batch,
                )?;
            }
        }

        // Topic sequences advance even with no subscriptions. A subscription
        // created later therefore cannot mistake a later message for history it
        // was never entitled to receive.
        batch.push_put(
            keys::queue_counters(&command.namespace, &command.entity),
            codec::encode(&counters)?,
        );
        Ok(CommandOutcome::Published {
            sequences,
            subscriptions: populated_entities(&subscriptions, &populated, &dead_lettered)?,
        })
    }

    pub(super) fn topic_exists(&self, command: &Command) -> Result<bool, BrokerError> {
        Ok(self
            .topic_config(&command.namespace, &command.entity)?
            .is_some())
    }

    pub(super) fn load_topic_config(&self, command: &Command) -> Result<TopicConfig, BrokerError> {
        self.topic_config(&command.namespace, &command.entity)?
            .ok_or(BrokerError::TopicNotFound)
    }

    pub(super) fn topic_fanout_state(
        &self,
        command: &Command,
    ) -> Result<TopicFanoutState, BrokerError> {
        let topology = self.subscription_topology(&command.namespace, &command.entity)?;
        let mut subscriptions = Vec::with_capacity(topology.len());
        let mut state = Vec::with_capacity(topology.len());
        let mut cache = ProgramCache::default();
        let mut sql_rules = 0usize;
        for profile in topology {
            let checked = self.checked_rules(&command.namespace, &profile.entity, &mut cache)?;
            sql_rules = sql_rules.saturating_add(
                checked
                    .iter()
                    .filter(|(d, _, _)| {
                        d.action.is_some() || matches!(d.filter, crate::RuleFilter::Sql(_))
                    })
                    .count(),
            );
            if sql_rules > crate::MAX_TOPIC_SQL_RULES {
                return Err(BrokerError::EntityMetadataCorrupt);
            }
            let rules = CompiledRules::new(
                checked,
                profile.dead_lettering_on_filter_evaluation_exceptions,
            )?;
            subscriptions.push(profile.entity);
            state.push((profile.backing, rules));
        }
        Ok((subscriptions, state))
    }
}

pub(super) fn filter_properties(input: &SendInput<'_>) -> Result<FilterProperties, BrokerError> {
    let mut properties = input
        .envelope
        .map(|envelope| envelope.filter_properties().clone())
        .unwrap_or_default();
    // V1/V2 routing historically used the duplicate identity here. V3 keeps
    // the typed-ID-derived text in its projection instead.
    if !input.format.uses_text_ids() && !input.message_id.is_empty() {
        properties.message_id = Some(input.message_id.to_owned());
    }
    if let Some(session_id) = input.session_id {
        properties.session_id = Some(session_id.as_str().to_owned());
    }
    Ok(properties.canonicalized()?)
}

pub(super) fn uses_sql(state: &[SubscriptionFanoutState]) -> bool {
    state.iter().any(|(_, rules)| rules.uses_sql())
}

/// Delivered subscriptions in topology order, then the dead-letter queues that
/// received filter-error copies, each reported once.
pub(super) fn populated_entities(
    subscriptions: &[EntityPath],
    populated: &[bool],
    dead_lettered: &[bool],
) -> Result<Vec<EntityPath>, BrokerError> {
    let mut entities = subscriptions
        .iter()
        .zip(populated)
        .filter(|(_, populated)| **populated)
        .map(|(subscription, _)| subscription.clone())
        .collect::<Vec<_>>();
    for (subscription, dead_lettered) in subscriptions.iter().zip(dead_lettered) {
        if *dead_lettered {
            entities.push(subscription.dead_letter_queue()?);
        }
    }
    Ok(entities)
}

fn validate_topic_input(config: &TopicConfig, input: &SendInput<'_>) -> Result<(), BrokerError> {
    if input.session_id.is_some() {
        return Err(BrokerError::TopicSessionNotSupported);
    }
    let message_id_characters = input.message_id.chars().count();
    if message_id_characters > MAX_MESSAGE_ID_CHARACTERS {
        return Err(BrokerError::MessageIdTooLong {
            characters: message_id_characters,
            maximum: MAX_MESSAGE_ID_CHARACTERS,
        });
    }
    let message_bytes = input
        .envelope
        .map_or(input.body.len(), MessageEnvelope::len);
    if message_bytes > config.max_message_bytes {
        return Err(BrokerError::MessageTooLarge {
            body_bytes: message_bytes,
            maximum_bytes: config.max_message_bytes,
        });
    }
    Ok(())
}
