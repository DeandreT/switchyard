use super::{
    COLLECTION_OVERHEAD, MAX_MESSAGE_HEADER_BYTES, MAX_MESSAGE_VALUE_ITEMS, MessageEnvelope,
    MessageValue, SECTION_OVERHEAD, VARIABLE_OVERHEAD, validate_property_size,
};
use crate::{
    BrokerError,
    rule::{CheckedSqlAction, SqlActionProgram},
};

type ActionMeasurementBaseline = ((usize, usize, usize), usize, usize);

impl MessageEnvelope {
    pub(crate) fn action_measurement_baseline(
        message_id: &str,
        body_bytes: usize,
        envelope: Option<&Self>,
    ) -> Result<ActionMeasurementBaseline, BrokerError> {
        if let Some(envelope) = envelope {
            let properties = if envelope.application_properties.is_empty() {
                0
            } else {
                envelope.application_properties.iter().fold(
                    SECTION_OVERHEAD.saturating_add(COLLECTION_OVERHEAD),
                    |bytes, (key, value)| {
                        bytes
                            .saturating_add(VARIABLE_OVERHEAD)
                            .saturating_add(key.len())
                            .saturating_add(value.content_size())
                    },
                )
            };
            Ok((
                (
                    envelope.content_size(),
                    envelope.validate_value_limits()?,
                    envelope.header_content_size(),
                ),
                properties,
                envelope.application_properties.len(),
            ))
        } else {
            let header = Self::default()
                .header_content_size()
                .saturating_sub(1)
                .saturating_add(VARIABLE_OVERHEAD)
                .saturating_add(message_id.len());
            Ok((
                (
                    header
                        .saturating_add(SECTION_OVERHEAD)
                        .saturating_add(VARIABLE_OVERHEAD)
                        .saturating_add(body_bytes),
                    0,
                    header,
                ),
                0,
                0,
            ))
        }
    }

    pub(crate) fn checked_action_projection(
        envelope: Option<&Self>,
        program: &SqlActionProgram,
        checked: &CheckedSqlAction,
        annotation_key: &str,
        annotation: &str,
    ) -> Result<(usize, usize, usize), BrokerError> {
        let measured = checked.measurements();
        let old = checked.property_cost(program, envelope, annotation_key);
        let key_bytes = VARIABLE_OVERHEAD.saturating_add(annotation_key.len());
        let value_bytes = VARIABLE_OVERHEAD.saturating_add(annotation.len());
        validate_property_size(annotation_key, key_bytes, value_bytes)?;
        let section = SECTION_OVERHEAD.saturating_add(COLLECTION_OVERHEAD);
        // A vanished application map must acquire its section when RuleName is added.
        let add_section = if checked.has_properties() { 0 } else { section };
        let replacement = key_bytes
            .saturating_add(value_bytes)
            .saturating_add(add_section);
        let result = (
            measured.0.saturating_sub(old.0).saturating_add(replacement),
            measured.1.saturating_sub(old.1).saturating_add(1),
            measured.2.saturating_sub(old.0).saturating_add(replacement),
        );
        Self::require_action_measurement(result)?;
        Ok(result)
    }

    pub(crate) fn require_action_measurement(
        (_, value_items, header_bytes): (usize, usize, usize),
    ) -> Result<(), BrokerError> {
        if header_bytes > MAX_MESSAGE_HEADER_BYTES {
            return Err(BrokerError::MessageHeaderTooLarge {
                header_bytes,
                maximum_bytes: MAX_MESSAGE_HEADER_BYTES,
            });
        }
        if value_items > MAX_MESSAGE_VALUE_ITEMS {
            return Err(BrokerError::InvalidMessageContent {
                reason: format!("message value count exceeds {MAX_MESSAGE_VALUE_ITEMS}"),
            });
        }
        Ok(())
    }

    pub(crate) fn action_value_items(value: &MessageValue) -> usize {
        let mut pending = vec![value];
        let mut items = 0_usize;
        while let Some(value) = pending.pop() {
            items = items.saturating_add(1);
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
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{MessageBody, MessageIdentifier, MessageProperties};

    fn assert_projection(
        original: Option<&MessageEnvelope>,
        source: &str,
    ) -> Result<MessageEnvelope, BrokerError> {
        let program = SqlActionProgram::compile(source).expect("bounded literal source");
        let checked = program.check("id", 3, original)?;
        let projection = MessageEnvelope::checked_action_projection(
            original, &program, &checked, "RuleName", "rule",
        )?;
        let mut actual = original.cloned().unwrap_or_else(|| MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String("id".into())),
                ..MessageProperties::default()
            },
            body: MessageBody::Data(vec![b"abc".to_vec()]),
            ..MessageEnvelope::default()
        });
        program.apply_checked(&checked, &mut actual);
        actual
            .application_properties
            .insert("RuleName".into(), MessageValue::String("rule".into()));
        assert_eq!(
            projection,
            (
                actual.content_size(),
                actual.validate_value_limits()?,
                actual.header_content_size()
            )
        );
        Ok(actual)
    }

    #[test]
    fn checked_action_projection_matches_materialized_typed_and_legacy_copies()
    -> Result<(), BrokerError> {
        let original = MessageEnvelope {
            application_properties: BTreeMap::from([
                ("number".into(), MessageValue::Byte(7)),
                (
                    "text".into(),
                    MessageValue::String("long original value".into()),
                ),
                ("RuleName".into(), MessageValue::Null),
                ("rulename".into(), MessageValue::String("case kept".into())),
            ]),
            body: MessageBody::Sequence(vec![vec![MessageValue::Long(3)]]),
            ..MessageEnvelope::default()
        };
        for source in [
            "SET number=12;REMOVE text;SET added=TRUE;REMOVE RuleName",
            "REMOVE number;REMOVE text;REMOVE RuleName;REMOVE rulename",
            "SET text='x';SET text='replacement';SET RuleName='ignored'",
        ] {
            let actual = assert_projection(Some(&original), source)?;
            assert_eq!(actual.body, original.body);
            assert_eq!(
                actual.application_properties["RuleName"],
                MessageValue::String("rule".into())
            );
        }
        let legacy = assert_projection(None, "SET added=17;SET flag=FALSE")?;
        assert_eq!(
            legacy.application_properties["added"],
            MessageValue::Long(17)
        );
        assert_eq!(legacy.body, MessageBody::Data(vec![b"abc".to_vec()]));
        assert_projection(None, "REMOVE missing")?;
        Ok(())
    }

    #[test]
    fn failed_action_projection_uses_original_content_and_final_annotation()
    -> Result<(), BrokerError> {
        let original = MessageEnvelope {
            application_properties: BTreeMap::from([
                (
                    "removed".into(),
                    MessageValue::String("original content".into()),
                ),
                ("flag".into(), MessageValue::Bool(true)),
            ]),
            body: MessageBody::Data(vec![vec![7; 300]]),
            ..MessageEnvelope::default()
        };
        let actual = assert_projection(Some(&original), "REMOVE removed;SET flag='incompatible'")?;
        for (key, value) in &original.application_properties {
            assert_eq!(actual.application_properties[key], *value);
        }
        assert_eq!(actual.body, original.body);
        let full = MessageEnvelope {
            application_properties: BTreeMap::from([
                ("removed".into(), MessageValue::Null),
                ("flag".into(), MessageValue::Bool(true)),
            ]),
            body: MessageBody::Sequence(vec![vec![
                MessageValue::Null;
                MAX_MESSAGE_VALUE_ITEMS - 2
            ]]),
            ..MessageEnvelope::default()
        };
        let program =
            SqlActionProgram::compile("REMOVE removed;SET flag='bad'").expect("bounded source");
        let checked = program.check("id", 0, Some(&full))?;
        assert_eq!(
            checked.error(),
            Some(crate::rule::SqlActionError::TypeMismatch)
        );
        assert!(matches!(
            MessageEnvelope::checked_action_projection(
                Some(&full),
                &program,
                &checked,
                "RuleName",
                "rule"
            ),
            Err(BrokerError::InvalidMessageContent { .. })
        ));
        assert_eq!(full.validate_value_limits()?, MAX_MESSAGE_VALUE_ITEMS);
        Ok(())
    }
}
