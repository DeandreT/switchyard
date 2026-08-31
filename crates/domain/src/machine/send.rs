//! Singular and atomic batch sends.

use storage::{StateStore, WriteBatch};

use crate::{
    BrokerError, Command, CommandOutcome, MessageEnvelope, MessageInput, MessageRecord,
    MessageState, QueueConfig, SequenceNumber, codec, keys,
};

use super::{StateMachine, require_session_agreement};

#[derive(Clone, Copy)]
pub(super) struct SendInput<'a> {
    pub(super) message_id: &'a str,
    pub(super) body: &'a [u8],
    pub(super) time_to_live_millis: Option<u64>,
    pub(super) session_id: Option<&'a crate::SessionId>,
    pub(super) envelope: Option<&'a MessageEnvelope>,
}

impl<'a> From<&'a MessageInput> for SendInput<'a> {
    fn from(input: &'a MessageInput) -> Self {
        Self {
            message_id: &input.message_id,
            body: &input.body,
            time_to_live_millis: input.time_to_live_millis,
            session_id: input.session_id.as_ref(),
            envelope: input.envelope.as_ref(),
        }
    }
}

struct PreparedMessage {
    record: MessageRecord,
    encoded: Vec<u8>,
}

impl<S: StateStore> StateMachine<S> {
    pub(super) fn send(
        &self,
        command: &Command,
        input: SendInput<'_>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let sequence = self.persist_messages(command, &[input], batch)?[0];
        Ok(CommandOutcome::Sent { sequence })
    }

    pub(super) fn send_batch(
        &self,
        command: &Command,
        messages: &[MessageInput],
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        if messages.is_empty() {
            return Err(BrokerError::EmptyMessageBatch);
        }
        let inputs = messages.iter().map(SendInput::from).collect::<Vec<_>>();
        let sequences = self.persist_messages(command, &inputs, batch)?;
        Ok(CommandOutcome::BatchSent { sequences })
    }

    /// Validates and encodes every child before adding any storage mutation.
    fn persist_messages(
        &self,
        command: &Command,
        inputs: &[SendInput<'_>],
        batch: &mut WriteBatch,
    ) -> Result<Vec<SequenceNumber>, BrokerError> {
        if command.entity.is_dead_letter_queue() {
            return Err(BrokerError::DeadLetterQueueIsReserved);
        }
        let config = self.load_config(command)?;
        for input in inputs {
            validate_input(&config, input)?;
        }
        validate_batch_session(&config, inputs)?;

        let mut counters = self.load_counters(command)?;
        let mut prepared = Vec::with_capacity(inputs.len());
        for input in inputs {
            let sequence = SequenceNumber::new(counters.next_sequence);
            counters.next_sequence = counters.next_sequence.saturating_add(1);
            let record = message_record(command, &config, *input, sequence);
            let encoded = codec::encode(&record)?;
            prepared.push(PreparedMessage { record, encoded });
        }
        let encoded_counters = codec::encode(&counters)?;
        let sequences = prepared
            .iter()
            .map(|message| message.record.sequence)
            .collect();

        let namespace = &command.namespace;
        let entity = &command.entity;
        for PreparedMessage { record, encoded } in prepared {
            batch.push_put(keys::message(namespace, entity, record.sequence), encoded);
            batch.push_put(self.ready_key(command, &record), Vec::new());
            if let Some(expires_at) = record.expires_at {
                batch.push_put(
                    keys::expiry(namespace, entity, expires_at, record.sequence),
                    Vec::new(),
                );
            }
        }
        batch.push_put(keys::queue_counters(namespace, entity), encoded_counters);
        Ok(sequences)
    }
}

fn validate_input(config: &QueueConfig, input: &SendInput<'_>) -> Result<(), BrokerError> {
    require_session_agreement(config, input.session_id.is_some())?;
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

fn validate_batch_session(
    config: &QueueConfig,
    inputs: &[SendInput<'_>],
) -> Result<(), BrokerError> {
    if !config.requires_session || inputs.len() < 2 {
        return Ok(());
    }
    let session_id = inputs[0]
        .session_id
        .expect("session agreement is validated before batch agreement");
    if inputs[1..]
        .iter()
        .any(|input| input.session_id != Some(session_id))
    {
        return Err(BrokerError::MessageBatchSessionMismatch);
    }
    Ok(())
}

fn message_record(
    command: &Command,
    config: &QueueConfig,
    input: SendInput<'_>,
    sequence: SequenceNumber,
) -> MessageRecord {
    let expires_at = input
        .time_to_live_millis
        .or(config.default_time_to_live_millis)
        .map(|millis| command.issued_at.saturating_add_millis(millis));
    MessageRecord {
        sequence,
        message_id: input.message_id.to_owned(),
        body: input.body.to_vec(),
        enqueued_at: command.issued_at,
        expires_at,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: input.session_id.cloned(),
        dead_letter: None,
        envelope: input.envelope.cloned(),
    }
}
