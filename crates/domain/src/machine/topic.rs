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

impl<S: StateStore> StateMachine<S> {
    pub fn topic_config(
        &self,
        namespace: &crate::NamespaceName,
        topic: &EntityPath,
    ) -> Result<Option<TopicConfig>, BrokerError> {
        self.read(&keys::topic_config(namespace, topic))
    }

    /// Durable subscriptions below `topic`, ordered by validated name.
    pub fn subscriptions(
        &self,
        namespace: &crate::NamespaceName,
        topic: &EntityPath,
        limit: usize,
    ) -> Result<Vec<EntityPath>, BrokerError> {
        self.store()
            .scan_prefix(&keys::topic_subscription_prefix(namespace, topic), limit)?
            .into_iter()
            .map(|(_, value)| codec::decode(&value).map_err(BrokerError::from))
            .collect()
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

        batch.push_put(key, codec::encode(&config.validate()?)?);
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

        let subscriptions = self.subscriptions(
            &command.namespace,
            &command.entity,
            MAX_TOPIC_SUBSCRIPTIONS + 1,
        )?;
        if subscriptions.len() > MAX_TOPIC_SUBSCRIPTIONS {
            return Err(BrokerError::SubscriptionLimitExceeded {
                maximum: MAX_TOPIC_SUBSCRIPTIONS,
            });
        }
        let subscription_state = subscriptions
            .iter()
            .map(|entity| {
                let config = self
                    .queue_config(&command.namespace, entity)?
                    .ok_or_else(|| BrokerError::DanglingSubscription {
                        entity: entity.clone(),
                    })?;
                let rules = self.all_rules(&command.namespace, entity)?;
                Ok::<_, BrokerError>((config, rules))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut counters = self.load_counters(command)?;
        let mut sequences = Vec::with_capacity(inputs.len());
        let mut populated = vec![false; subscriptions.len()];
        for input in inputs {
            let sequence = SequenceNumber::new(counters.next_sequence);
            counters.next_sequence = counters.next_sequence.saturating_add(1);
            sequences.push(sequence);
            let properties = filter_properties(input)?;

            let topic_lifetime = effective_time_to_live(
                input.time_to_live_millis,
                topic.default_time_to_live_millis,
            );
            for (index, (subscription, (config, rules))) in
                subscriptions.iter().zip(&subscription_state).enumerate()
            {
                if !matches_any(rules, &properties) {
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

    fn load_topic_config(&self, command: &Command) -> Result<TopicConfig, BrokerError> {
        self.topic_config(&command.namespace, &command.entity)?
            .ok_or(BrokerError::TopicNotFound)
    }
}

fn filter_properties(input: &SendInput<'_>) -> Result<FilterProperties, BrokerError> {
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

fn matches_any(rules: &[RuleDefinition], properties: &FilterProperties) -> bool {
    rules
        .iter()
        .any(|definition| definition.filter.matches(properties))
}

fn validate_topic_input(config: &TopicConfig, input: &SendInput<'_>) -> Result<(), BrokerError> {
    if input.scheduled_enqueue_at.is_some() {
        return Err(BrokerError::TopicSchedulingNotSupported);
    }
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
