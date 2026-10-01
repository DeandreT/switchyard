use super::topic_fanout::{TopicBudget, TopicEmission, topic_message_cost};
use super::*;

struct ScheduledTopicMessage {
    key: Vec<u8>,
    record: MessageRecord,
    time_to_live_millis: Option<u64>,
}

impl ScheduledTopicMessage {
    fn input(&self) -> MessageInput<'_> {
        MessageInput {
            message_id: &self.record.message_id,
            body: &self.record.body,
            time_to_live_millis: self.time_to_live_millis,
            session_id: self.record.session_id.as_ref(),
            envelope: self.record.envelope.as_deref(),
        }
    }
}

impl<S: StateStore> StateMachine<S> {
    pub(super) fn activate_topic_scheduled(
        &self,
        command: &Command,
        config: QueueConfig,
        batch: &mut WriteBatch,
        subscription_enqueues: &mut Option<Vec<EntityPath>>,
    ) -> Result<CommandOutcome, BrokerError> {
        let targets = self.topic_targets(command)?;
        let namespace = &command.namespace;
        let entity = &command.entity;
        let scheduled = self
            .store
            .scan_prefix(&keys::scheduled_prefix(namespace, entity), TIMER_SCAN_LIMIT)?;
        let mut selected = Vec::new();
        let mut budget = TopicBudget::default();

        for (key, _) in scheduled {
            let (enqueue_at, sequence) =
                keys::trailing_deadline(&key).ok_or(BrokerError::MalformedIndexKey)?;
            if key != keys::scheduled(namespace, entity, enqueue_at, sequence) {
                return Err(BrokerError::MalformedIndexKey);
            }
            if enqueue_at > command.issued_at {
                break;
            }
            let record = self
                .message(namespace, entity, sequence)?
                .ok_or(BrokerError::DanglingIndexEntry { sequence })?;
            let time_to_live_millis = match record.state {
                MessageState::Scheduled {
                    enqueue_at: stored_enqueue_at,
                    time_to_live_millis,
                } if stored_enqueue_at == enqueue_at
                    && record.sequence == sequence
                    && record.scheduled_enqueue_time == Some(enqueue_at) =>
                {
                    time_to_live_millis
                }
                _ => return Err(BrokerError::MalformedIndexKey),
            };
            let candidate = ScheduledTopicMessage {
                key,
                record,
                time_to_live_millis,
            };
            let message = candidate.input();
            let cost = topic_message_cost(message)?;
            let mut next_budget = budget;
            let admission = next_budget
                .charge_input(cost)
                .and_then(|()| next_budget.charge_fanout(cost, message, &targets));
            if let Err(error) = admission {
                if selected.is_empty() {
                    return Err(error);
                }
                // The first item outside this command's envelope remains at
                // the head of the scheduled index for a later activation.
                break;
            }
            budget = next_budget;
            selected.push(candidate);
        }

        // All borrowed budgets are settled before shape validation can clone
        // compound keys, or emission can clone any retained message content.
        for candidate in &selected {
            self.validate_topic_message(&config, candidate.input(), &targets)?;
        }
        let mut counters = self.load_counters(command)?;
        let mut sequences = Vec::with_capacity(selected.len());
        for _ in &selected {
            sequences.push(counters.allocate_sequence()?);
        }
        let mut enqueued = BTreeSet::new();
        for (candidate, sequence) in selected.iter().zip(sequences) {
            batch.push_delete(candidate.key.clone());
            batch.push_delete(keys::message(namespace, entity, candidate.record.sequence));
            self.emit_topic_message(
                command,
                &config,
                &targets,
                TopicEmission {
                    message: candidate.input(),
                    sequence,
                    scheduled_enqueue_time: candidate.record.scheduled_enqueue_time,
                },
                batch,
                &mut enqueued,
            )?;
        }
        if !selected.is_empty() {
            batch.push_put(
                keys::queue_counters(namespace, entity),
                codec::encode(&counters)?,
            );
        }
        *subscription_enqueues = Some(enqueued.into_iter().collect());
        Ok(CommandOutcome::ScheduledActivated {
            activated: selected.len() as u32,
        })
    }
}
