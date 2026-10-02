use super::{
    COLLECTION_OVERHEAD, MAX_MESSAGE_HEADER_BYTES, MAX_MESSAGE_VALUE_ITEMS, MessageEnvelope,
    MessageValue, SECTION_OVERHEAD, VARIABLE_OVERHEAD, validate_property_size,
};
use crate::BrokerError;

impl MessageEnvelope {
    /// Measures an exact-key removal and final string annotation without
    /// copying retained message values or bodies.
    pub(crate) fn removal_projection(
        &self,
        targets: &[String],
        annotation_key: &str,
        annotation: &str,
    ) -> Result<(usize, usize, usize), BrokerError> {
        let mut value_items = self.validate_value_limits()?;
        let old_properties = self.application_properties_content_size();
        let mut properties = SECTION_OVERHEAD.saturating_add(COLLECTION_OVERHEAD);
        for (key, value) in &self.application_properties {
            if key == annotation_key || targets.iter().any(|target| target == key) {
                value_items = value_items.saturating_sub(value_node_count(value));
            } else {
                properties = properties
                    .saturating_add(VARIABLE_OVERHEAD)
                    .saturating_add(key.len())
                    .saturating_add(value.content_size());
            }
        }
        let key_size = VARIABLE_OVERHEAD.saturating_add(annotation_key.len());
        let value_size = VARIABLE_OVERHEAD.saturating_add(annotation.len());
        validate_property_size(annotation_key, key_size, value_size)?;
        properties = properties
            .saturating_add(key_size)
            .saturating_add(value_size);
        let header_bytes = self
            .header_content_size()
            .saturating_sub(old_properties)
            .saturating_add(properties);
        if header_bytes > MAX_MESSAGE_HEADER_BYTES {
            return Err(BrokerError::MessageHeaderTooLarge {
                header_bytes,
                maximum_bytes: MAX_MESSAGE_HEADER_BYTES,
            });
        }
        value_items = value_items.saturating_add(1);
        if value_items > MAX_MESSAGE_VALUE_ITEMS {
            return Err(BrokerError::InvalidMessageContent {
                reason: format!("message value count exceeds {MAX_MESSAGE_VALUE_ITEMS}"),
            });
        }
        Ok((
            self.content_size()
                .saturating_sub(old_properties)
                .saturating_add(properties),
            value_items,
            header_bytes,
        ))
    }

    pub(crate) fn legacy_annotation_projection(
        message_id: &str,
        body_bytes: usize,
        annotation_key: &str,
        annotation: &str,
    ) -> Result<(usize, usize, usize), BrokerError> {
        let key_size = VARIABLE_OVERHEAD.saturating_add(annotation_key.len());
        let value_size = VARIABLE_OVERHEAD.saturating_add(annotation.len());
        validate_property_size(annotation_key, key_size, value_size)?;
        let header = Self::default()
            .header_content_size()
            .saturating_sub(1)
            .saturating_add(VARIABLE_OVERHEAD)
            .saturating_add(message_id.len())
            .saturating_add(SECTION_OVERHEAD)
            .saturating_add(COLLECTION_OVERHEAD)
            .saturating_add(key_size)
            .saturating_add(value_size);
        Ok((
            header
                .saturating_add(SECTION_OVERHEAD)
                .saturating_add(VARIABLE_OVERHEAD)
                .saturating_add(body_bytes),
            1,
            header,
        ))
    }

    fn application_properties_content_size(&self) -> usize {
        if self.application_properties.is_empty() {
            return 0;
        }
        self.application_properties.iter().fold(
            SECTION_OVERHEAD.saturating_add(COLLECTION_OVERHEAD),
            |size, (key, value)| {
                size.saturating_add(VARIABLE_OVERHEAD)
                    .saturating_add(key.len())
                    .saturating_add(value.content_size())
            },
        )
    }
}

fn value_node_count(value: &MessageValue) -> usize {
    let mut pending = vec![value];
    let mut items = 0_usize;
    while let Some(value) = pending.pop() {
        items += 1;
        match value {
            MessageValue::List(values) | MessageValue::Array(values) => pending.extend(values),
            MessageValue::Map(entries) => {
                pending.extend(entries.iter().flat_map(|(key, value)| [key, value]))
            }
            MessageValue::Described { value, .. } => pending.push(value),
            _ => {}
        }
    }
    items
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::{MessageBody, MessageDescriptor, MessageIdentifier, MessageProperties};
    use super::*;

    #[test]
    fn removal_projection_matches_materialized_typed_and_legacy_envelopes()
    -> Result<(), BrokerError> {
        let envelope = MessageEnvelope {
            application_properties: BTreeMap::from([
                ("color".to_owned(), MessageValue::Binary(vec![1; 70])),
                ("Color".to_owned(), MessageValue::String("kept".to_owned())),
                (
                    "RuleName".to_owned(),
                    MessageValue::Described {
                        descriptor: MessageDescriptor::Code(1),
                        value: Box::new(MessageValue::Null),
                    },
                ),
            ]),
            body: MessageBody::Data(vec![vec![2; 40]]),
            ..MessageEnvelope::default()
        };
        let measured = envelope.removal_projection(&["color".to_owned()], "RuleName", "rule")?;
        let mut actual = envelope.clone();
        actual.application_properties.remove("color");
        actual.application_properties.insert(
            "RuleName".to_owned(),
            MessageValue::String("rule".to_owned()),
        );
        assert_eq!(
            measured,
            (
                actual.content_size(),
                actual.validate_value_limits()?,
                actual.header_content_size()
            )
        );
        assert!(actual.application_properties.contains_key("Color"));
        let legacy = MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String("id".to_owned())),
                ..MessageProperties::default()
            },
            application_properties: BTreeMap::from([(
                "RuleName".to_owned(),
                MessageValue::String("rule".to_owned()),
            )]),
            body: MessageBody::Data(vec![vec![3; 40]]),
            ..MessageEnvelope::default()
        };
        assert_eq!(
            MessageEnvelope::legacy_annotation_projection("id", 40, "RuleName", "rule")?,
            (
                legacy.content_size(),
                legacy.validate_value_limits()?,
                legacy.header_content_size()
            )
        );
        Ok(())
    }

    #[test]
    fn final_annotation_charges_a_value_node_and_removed_values_refund_only_the_projection()
    -> Result<(), BrokerError> {
        let envelope = MessageEnvelope {
            application_properties: BTreeMap::from([("value".to_owned(), MessageValue::Null)]),
            body: MessageBody::Sequence(vec![vec![
                MessageValue::Null;
                MAX_MESSAGE_VALUE_ITEMS - 1
            ]]),
            ..MessageEnvelope::default()
        };
        assert_eq!(envelope.validate_value_limits()?, MAX_MESSAGE_VALUE_ITEMS);
        assert!(matches!(
            envelope.removal_projection(&[], "RuleName", "rule"),
            Err(BrokerError::InvalidMessageContent { .. })
        ));
        assert_eq!(
            envelope
                .removal_projection(&["value".to_owned()], "RuleName", "rule")?
                .1,
            MAX_MESSAGE_VALUE_ITEMS
        );
        assert!(envelope.application_properties.contains_key("value"));
        Ok(())
    }
}
