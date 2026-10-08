//! Durable match-all topics and atomic subscription fanout.

use storage::{StateStore, WriteBatch};

use crate::{
    BrokerError, Command, CommandOutcome, EntityPath, FilterProperties, MAX_MESSAGE_ID_CHARACTERS,
    MAX_TOPIC_SUBSCRIPTIONS, MessageEnvelope, QueueConfig, RuleDefinition, SequenceNumber,
    SubscriptionConfig, SubscriptionName, TopicConfig, codec, keys,
};

use super::{
    StateMachine,
    send::{SendInput, effective_time_to_live, message_record},
};

type SubscriptionFanoutState = (QueueConfig, Vec<RuleDefinition>);
type TopicFanoutState = (Vec<EntityPath>, Vec<SubscriptionFanoutState>);

impl<S: StateStore> StateMachine<S> {
    pub fn topic_config(
        &self,
        namespace: &crate::NamespaceName,
        topic: &EntityPath,
    ) -> Result<Option<TopicConfig>, BrokerError> {
        let Some(config) = self.read::<TopicConfig>(&keys::topic_config(namespace, topic))? else {
            return Ok(None);
        };
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
            .map(|(entity, _)| entity)
            .collect())
    }

    fn subscription_topology(
        &self,
        namespace: &crate::NamespaceName,
        topic: &EntityPath,
    ) -> Result<Vec<(EntityPath, QueueConfig)>, BrokerError> {
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
            // Deserialization folds ASCII case, so decoded equality is not a
            // proof that the stored membership names this canonical child.
            if value != codec::encode(&entity)? {
                return Err(BrokerError::TopicTopologyCorrupt);
            }
            let config = self.validate_subscription_topology(namespace, &entity, parent)?;
            subscriptions.push((entity, config));
        }
        Ok(subscriptions)
    }

    fn validate_subscription_topology(
        &self,
        namespace: &crate::NamespaceName,
        entity: &EntityPath,
        parent: TopicConfig,
    ) -> Result<QueueConfig, BrokerError> {
        let backing = self.queue_config(namespace, entity)?.ok_or_else(|| {
            BrokerError::DanglingSubscription {
                entity: entity.clone(),
            }
        })?;
        let config = SubscriptionConfig {
            lock_duration_millis: backing.lock_duration_millis,
            max_delivery_count: backing.max_delivery_count,
            default_time_to_live_millis: backing.default_time_to_live_millis,
        };
        let expected = config
            .validate()
            .map_err(|_| BrokerError::TopicTopologyCorrupt)?
            .queue_config(parent);
        let shadow = entity.dead_letter_queue()?;
        let expected_shadow = QueueConfig {
            max_delivery_count: u32::MAX,
            default_time_to_live_millis: None,
            requires_session: false,
            requires_duplicate_detection: false,
            ..expected
        };
        if backing != expected
            || self.queue_config(namespace, &shadow)? != Some(expected_shadow)
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
        batch.push_put(key, codec::encode(&config)?);
        Ok(CommandOutcome::TopicCreated)
    }

    pub(super) fn create_subscription(
        &self,
        command: &Command,
        name: &SubscriptionName,
        config: SubscriptionConfig,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let topic = self.load_topic_config(command)?;
        let index_key = keys::topic_subscription(&command.namespace, &command.entity, name);
        if self.store().get(&index_key)?.is_some() {
            return Err(BrokerError::SubscriptionAlreadyExists);
        }

        let subscriptions = self.subscriptions(
            &command.namespace,
            &command.entity,
            MAX_TOPIC_SUBSCRIPTIONS + 1,
        )?;
        if subscriptions.len() >= MAX_TOPIC_SUBSCRIPTIONS {
            return Err(BrokerError::SubscriptionLimitExceeded {
                maximum: MAX_TOPIC_SUBSCRIPTIONS,
            });
        }

        let entity = command.entity.subscription(name)?;
        let queue_key = keys::queue_config(&command.namespace, &entity);
        if self.store().get(&queue_key)?.is_some()
            || self
                .store()
                .get(&keys::topic_config(&command.namespace, &entity))?
                .is_some()
        {
            return Err(BrokerError::EntityAlreadyExists);
        }

        // Validate the DLQ path now so a valid subscription can never discover
        // that its shadow is unaddressable only when the first message fails.
        let dead_letter_queue = entity.dead_letter_queue()?;
        let queue = config.validate()?.queue_config(topic).validate()?;
        let shadow = QueueConfig {
            max_delivery_count: u32::MAX,
            default_time_to_live_millis: None,
            requires_session: false,
            requires_duplicate_detection: false,
            ..queue
        };

        if self
            .store()
            .get(&keys::queue_config(&command.namespace, &dead_letter_queue))?
            .is_some()
            || self
                .store()
                .get(&keys::topic_config(&command.namespace, &dead_letter_queue))?
                .is_some()
        {
            return Err(BrokerError::TopicTopologyCorrupt);
        }

        batch.push_put(index_key, codec::encode(&entity)?);
        batch.push_put(queue_key, codec::encode(&queue)?);
        batch.push_put(
            keys::queue_config(&command.namespace, &dead_letter_queue),
            codec::encode(&shadow)?,
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

        let mut counters = self.load_counters(command)?;
        let mut sequences = Vec::with_capacity(inputs.len());
        let mut populated = vec![false; subscriptions.len()];
        for (input, properties) in inputs.iter().zip(&properties) {
            let sequence = SequenceNumber::new(counters.next_sequence);
            counters.next_sequence = counters.next_sequence.saturating_add(1);
            sequences.push(sequence);

            let topic_lifetime = effective_time_to_live(
                input.time_to_live_millis,
                topic.default_time_to_live_millis,
            );
            if let Some(enqueue_at) = input.scheduled_enqueue_at {
                let record = message_record(command, *input, sequence, topic_lifetime);
                batch.push_put(
                    keys::message(&command.namespace, &command.entity, sequence),
                    codec::encode(&record)?,
                );
                batch.push_put(
                    keys::scheduled(&command.namespace, &command.entity, enqueue_at, sequence),
                    Vec::new(),
                );
                continue;
            }

            for (index, (subscription, (config, rules))) in
                subscriptions.iter().zip(&subscription_state).enumerate()
            {
                if !matches_any(rules, properties) {
                    continue;
                }
                populated[index] = true;
                let lifetime =
                    effective_time_to_live(topic_lifetime, config.default_time_to_live_millis);
                let record = message_record(command, *input, sequence, lifetime);
                batch.push_put(
                    keys::message(&command.namespace, subscription, sequence),
                    codec::encode(&record)?,
                );
                batch.push_put(
                    keys::ready(&command.namespace, subscription, sequence),
                    Vec::new(),
                );
                if let Some(expires_at) = record.expires_at {
                    batch.push_put(
                        keys::expiry(&command.namespace, subscription, expires_at, sequence),
                        Vec::new(),
                    );
                }
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
            subscriptions: subscriptions
                .into_iter()
                .zip(populated)
                .filter_map(|(subscription, populated)| populated.then_some(subscription))
                .collect(),
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
        for (entity, config) in topology {
            let rules = self.all_rules(&command.namespace, &entity)?;
            subscriptions.push(entity);
            state.push((config, rules));
        }
        Ok((subscriptions, state))
    }
}

pub(super) fn filter_properties(input: &SendInput<'_>) -> Result<FilterProperties, BrokerError> {
    let mut properties = input
        .envelope
        .map(|envelope| envelope.filter_properties().clone())
        .unwrap_or_default();
    if !input.message_id.is_empty() {
        properties.message_id = Some(input.message_id.to_owned());
    }
    if let Some(session_id) = input.session_id {
        properties.session_id = Some(session_id.as_str().to_owned());
    }
    Ok(properties.canonicalized()?)
}

pub(super) fn matches_any(rules: &[RuleDefinition], properties: &FilterProperties) -> bool {
    rules
        .iter()
        .any(|definition| definition.filter.matches(properties))
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
