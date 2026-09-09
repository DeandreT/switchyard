//! Topic-owned scheduled placeholders and due-time subscription fanout.

use storage::{StateStore, WriteBatch};

use crate::{
    BrokerError, Command, CommandOutcome, MessageRecord, MessageState, SequenceNumber, codec, keys,
};

use super::{
    StateMachine, TIMER_SCAN_LIMIT,
    scheduling::{scheduled_lifetime, validate_cancellation},
    send::{SendInput, effective_time_to_live},
    topic::{filter_properties, matches_any},
};

impl<S: StateStore> StateMachine<S> {
    pub(super) fn cancel_topic_scheduled(
        &self,
        command: &Command,
        sequences: &[SequenceNumber],
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        validate_cancellation(sequences)?;
        self.load_topic_config(command)?;

        // Resolve the complete request before staging any delete. Cancellation
        // remains all-or-nothing when one placeholder is stale or already due.
        let mut scheduled = Vec::with_capacity(sequences.len());
        for &sequence in sequences {
            let record = self.load_message(command, sequence)?;
            if record.state != MessageState::Scheduled {
                return Err(BrokerError::MessageNotScheduled { sequence });
            }
            let enqueue_at = record
                .scheduled_enqueue_at
                .ok_or(BrokerError::ScheduledEnqueueTimeMissing { sequence })?;
            scheduled.push((sequence, enqueue_at));
        }

        for (sequence, enqueue_at) in scheduled {
            batch.push_delete(keys::scheduled(
                &command.namespace,
                &command.entity,
                enqueue_at,
                sequence,
            ));
            batch.push_delete(keys::message(&command.namespace, &command.entity, sequence));
        }

        Ok(CommandOutcome::ScheduledCancelled {
            cancelled: u32::try_from(sequences.len()).unwrap_or(u32::MAX),
        })
    }

    pub(super) fn activate_topic_scheduled(
        &self,
        command: &Command,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        self.load_topic_config(command)?;
        let namespace = &command.namespace;
        let topic = &command.entity;
        let scheduled_prefix = keys::scheduled_prefix(namespace, topic);
        let Some((first_key, _)) = self
            .store()
            .scan_prefix(&scheduled_prefix, 1)?
            .into_iter()
            .next()
        else {
            return Ok(no_topic_activation());
        };
        let (first_enqueue_at, _) =
            keys::trailing_deadline(&first_key).ok_or(BrokerError::MalformedIndexKey)?;
        if first_enqueue_at > command.issued_at {
            return Ok(no_topic_activation());
        }

        // Every message activated by this command observes this one durable
        // topology/rule snapshot. Divide the normal timer budget by fanout so
        // the number of subscription copies stays bounded. One publication is
        // still atomic even when the topic itself has more than the budgeted
        // number of subscriptions.
        let (subscriptions, subscription_state) = self.topic_fanout_state(command)?;
        let activation_limit = topic_activation_limit(subscriptions.len());
        let due = self
            .store()
            .scan_prefix(&scheduled_prefix, activation_limit)?;
        let mut prepared = Vec::with_capacity(due.len());
        for (index_key, _) in due {
            let (enqueue_at, placeholder_sequence) =
                keys::trailing_deadline(&index_key).ok_or(BrokerError::MalformedIndexKey)?;
            if enqueue_at > command.issued_at {
                break;
            }
            let record = self
                .message(namespace, topic, placeholder_sequence)?
                .ok_or(BrokerError::DanglingIndexEntry {
                    sequence: placeholder_sequence,
                })?;
            validate_placeholder(&record, placeholder_sequence, enqueue_at)?;
            let properties = filter_properties(&record_input(&record))?;
            let topic_lifetime = scheduled_lifetime(&record, enqueue_at);
            prepared.push((
                index_key,
                placeholder_sequence,
                record,
                properties,
                topic_lifetime,
            ));
        }
        if prepared.is_empty() {
            return Ok(no_topic_activation());
        }

        let mut counters = self.load_counters(command)?;
        let mut populated = vec![false; subscriptions.len()];
        let activated = u32::try_from(prepared.len()).unwrap_or(u32::MAX);
        for (index_key, placeholder_sequence, mut record, properties, topic_lifetime) in prepared {
            let active_sequence = SequenceNumber::new(counters.next_sequence);
            counters.next_sequence = counters.next_sequence.saturating_add(1);
            batch.push_delete(index_key);
            batch.push_delete(keys::message(namespace, topic, placeholder_sequence));

            record.sequence = active_sequence;
            record.enqueued_at = command.issued_at;
            record.state = MessageState::Ready;
            for (index, (subscription, (config, rules))) in
                subscriptions.iter().zip(&subscription_state).enumerate()
            {
                if !matches_any(rules, &properties) {
                    continue;
                }

                populated[index] = true;
                let mut copy = record.clone();
                let lifetime =
                    effective_time_to_live(topic_lifetime, config.default_time_to_live_millis);
                copy.expires_at =
                    lifetime.map(|millis| command.issued_at.saturating_add_millis(millis));
                batch.push_put(
                    keys::message(namespace, subscription, active_sequence),
                    codec::encode(&copy)?,
                );
                batch.push_put(
                    keys::ready(namespace, subscription, active_sequence),
                    Vec::new(),
                );
                if let Some(expires_at) = copy.expires_at {
                    batch.push_put(
                        keys::expiry(namespace, subscription, expires_at, active_sequence),
                        Vec::new(),
                    );
                }
            }
        }
        batch.push_put(
            keys::queue_counters(namespace, topic),
            codec::encode(&counters)?,
        );

        Ok(CommandOutcome::ScheduledActivated {
            activated,
            deliverable_entities: subscriptions
                .into_iter()
                .zip(populated)
                .filter_map(|(subscription, populated)| populated.then_some(subscription))
                .collect(),
        })
    }
}

fn topic_activation_limit(subscription_count: usize) -> usize {
    (TIMER_SCAN_LIMIT / subscription_count.max(1)).clamp(1, TIMER_SCAN_LIMIT)
}

fn no_topic_activation() -> CommandOutcome {
    CommandOutcome::ScheduledActivated {
        activated: 0,
        deliverable_entities: Vec::new(),
    }
}

fn validate_placeholder(
    record: &MessageRecord,
    sequence: SequenceNumber,
    enqueue_at: crate::Timestamp,
) -> Result<(), BrokerError> {
    if record.state != MessageState::Scheduled {
        return Err(BrokerError::MessageNotScheduled { sequence });
    }
    let recorded_enqueue_at = record
        .scheduled_enqueue_at
        .ok_or(BrokerError::ScheduledEnqueueTimeMissing { sequence })?;
    if recorded_enqueue_at != enqueue_at {
        return Err(BrokerError::MalformedIndexKey);
    }
    Ok(())
}

fn record_input(record: &MessageRecord) -> SendInput<'_> {
    SendInput {
        message_id: &record.message_id,
        body: &record.body,
        time_to_live_millis: None,
        session_id: record.session_id.as_ref(),
        scheduled_enqueue_at: record.scheduled_enqueue_at,
        envelope: record.envelope.as_ref(),
    }
}
