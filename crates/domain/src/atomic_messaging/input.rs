use std::collections::BTreeMap;

use crate::{
    BrokerError, CommandKind, MAX_MESSAGE_VALUE_DEPTH, MessageBody, MessageEnvelope, MessageValue,
    SettlementDisposition,
};

use super::AtomicMessagingLimit as Limit;

/// Payload-free borrowed input usage for one bounded atomic messaging group.
/// This does not validate message shape, entity configuration, locks, or authorization.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AtomicMessagingInputUsage {
    actions: usize,
    messages: usize,
    content_bytes: usize,
    value_items: usize,
}

fn add(used: &mut usize, amount: usize, limit: Limit) -> Result<(), BrokerError> {
    *used = used
        .checked_add(amount)
        .filter(|total| *total <= limit.maximum())
        .ok_or_else(|| limit.exceeded())?;
    Ok(())
}

impl AtomicMessagingInputUsage {
    pub const fn actions(&self) -> usize {
        self.actions
    }

    pub const fn messages(&self) -> usize {
        self.messages
    }

    pub const fn content_bytes(&self) -> usize {
        self.content_bytes
    }

    pub const fn value_items(&self) -> usize {
        self.value_items
    }

    /// Adds one supported action without retaining or cloning its payload.
    /// Failed additions leave all usage unchanged; successful usage is cumulative.
    pub fn try_extend(&mut self, kind: &CommandKind) -> Result<(), BrokerError> {
        let mut candidate = *self;
        add(&mut candidate.actions, 1, Limit::Actions)?;
        candidate.extend_kind(kind)?;
        *self = candidate;
        Ok(())
    }

    fn bytes(&mut self, bytes: usize) -> Result<(), BrokerError> {
        add(&mut self.content_bytes, bytes, Limit::ContentBytes)
    }

    fn value(&mut self, value: &MessageValue, depth: usize) -> Result<(), BrokerError> {
        if depth > MAX_MESSAGE_VALUE_DEPTH {
            return Err(BrokerError::InvalidMessageContent {
                reason: format!("message value depth exceeds {MAX_MESSAGE_VALUE_DEPTH}"),
            });
        }
        add(&mut self.value_items, 1, Limit::ValueItems)?;
        match value {
            MessageValue::List(values) | MessageValue::Array(values) => {
                for value in values {
                    self.value(value, depth + 1)?;
                }
            }
            MessageValue::Map(entries) => {
                for (key, value) in entries {
                    self.value(key, depth + 1)?;
                    self.value(value, depth + 1)?;
                }
            }
            MessageValue::Described { value, .. } => self.value(value, depth + 1)?,
            _ => {}
        }
        Ok(())
    }

    fn envelope(&mut self, envelope: &MessageEnvelope) -> Result<(), BrokerError> {
        // Empty sections have no value nodes. Their retained section overhead
        // bounds iteration before the ordinary content tally walks them.
        let minimum_sections = match &envelope.body {
            MessageBody::Data(sections) => sections.len().checked_mul(15),
            MessageBody::Sequence(sections) => sections.len().checked_mul(19),
            _ => Some(0),
        }
        .ok_or_else(|| Limit::ContentBytes.exceeded())?;
        if minimum_sections > Limit::ContentBytes.maximum() - self.content_bytes {
            return Err(Limit::ContentBytes.exceeded());
        }
        for value in envelope
            .application_properties
            .values()
            .chain(envelope.message_annotations.values())
            .chain(envelope.footer.values())
        {
            self.value(value, 0)?;
        }
        match &envelope.body {
            MessageBody::Empty | MessageBody::Data(_) => {}
            MessageBody::Sequence(sections) => {
                for value in sections.iter().flatten() {
                    self.value(value, 0)?;
                }
            }
            MessageBody::Value(value) => self.value(value, 0)?,
        }
        self.bytes(envelope.content_size())
    }

    fn message(
        &mut self,
        id: &str,
        body: &[u8],
        envelope: Option<&MessageEnvelope>,
    ) -> Result<(), BrokerError> {
        add(&mut self.messages, 1, Limit::Messages)?;
        self.bytes(id.len())?;
        self.bytes(body.len())?;
        if let Some(envelope) = envelope {
            self.envelope(envelope)?;
        }
        Ok(())
    }

    fn properties(
        &mut self,
        properties: &BTreeMap<String, MessageValue>,
    ) -> Result<(), BrokerError> {
        for (key, value) in properties {
            self.value(value, 0)?;
            self.bytes(key.len())?;
            self.bytes(value.content_size())?;
        }
        Ok(())
    }

    fn disposition(&mut self, disposition: &SettlementDisposition) -> Result<(), BrokerError> {
        if let SettlementDisposition::DeadLetter {
            reason,
            description,
        } = disposition
        {
            self.bytes(reason.len())?;
            self.bytes(description.len())?;
        }
        Ok(())
    }

    fn extend_kind(&mut self, kind: &CommandKind) -> Result<(), BrokerError> {
        match kind {
            CommandKind::Send {
                message_id,
                body,
                session_id,
                ..
            } => {
                if session_id.is_some() {
                    return Err(BrokerError::AtomicMessagingOperationNotSupported);
                }
                self.message(message_id, body, None)?;
            }
            CommandKind::SendEnvelope {
                message_id,
                body,
                session_id,
                envelope,
                ..
            } => {
                if session_id.is_some() {
                    return Err(BrokerError::AtomicMessagingOperationNotSupported);
                }
                self.message(message_id, body, Some(envelope))?;
            }
            CommandKind::SendBatch { messages } => {
                if messages.len() > Limit::Messages.maximum() - self.messages {
                    return Err(Limit::Messages.exceeded());
                }
                for message in messages {
                    if message.scheduled_enqueue_time.is_some() || message.session_id.is_some() {
                        return Err(BrokerError::AtomicMessagingOperationNotSupported);
                    }
                    self.message(&message.message_id, &message.body, Some(&message.envelope))?;
                }
            }
            CommandKind::Complete { .. }
            | CommandKind::Abandon { .. }
            | CommandKind::Defer { .. } => {}
            CommandKind::DeadLetter {
                reason,
                description,
                ..
            } => {
                self.bytes(reason.len())?;
                self.bytes(description.len())?;
            }
            CommandKind::Settle {
                disposition,
                properties_to_modify,
                ..
            } => {
                self.properties(properties_to_modify)?;
                self.disposition(disposition)?;
            }
            _ => return Err(BrokerError::AtomicMessagingOperationNotSupported),
        }
        Ok(())
    }
}

pub(super) fn validate<'a>(
    kinds: impl Iterator<Item = &'a CommandKind>,
    actions: usize,
) -> Result<(), BrokerError> {
    if actions > Limit::Actions.maximum() {
        return Err(Limit::Actions.exceeded());
    }
    let mut usage = AtomicMessagingInputUsage::default();
    for kind in kinds {
        usage.try_extend(kind)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
