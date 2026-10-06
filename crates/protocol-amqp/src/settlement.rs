use std::collections::BTreeMap;

use amqp::{Fields, Outcome, Value};
use domain::{CommandKind, LockToken, MessageValue, SequenceNumber, SettlementDisposition};
use thiserror::Error;

use crate::message::read_value;

const DEAD_LETTER_CONDITION: &str = "com.microsoft:dead-letter";
const DEAD_LETTER_REASON_PROPERTY: &str = "DeadLetterReason";
const DEAD_LETTER_DESCRIPTION_PROPERTY: &str = "DeadLetterErrorDescription";
const PROPERTIES_TO_MODIFY: &str = "properties-to-modify";

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum SettlementError {
    #[error("{field} requires {expected}")]
    InvalidField {
        field: &'static str,
        expected: &'static str,
    },
}

pub(crate) fn settlement_command(
    sequence: SequenceNumber,
    lock_token: LockToken,
    outcome: Outcome,
) -> Result<CommandKind, SettlementError> {
    let (disposition, properties_to_modify) = match outcome {
        Outcome::Declared(_) => {
            return Err(SettlementError::InvalidField {
                field: "state",
                expected: "an ordinary messaging outcome",
            });
        }
        Outcome::Accepted(_) => (SettlementDisposition::Complete, BTreeMap::new()),
        Outcome::Released(_) => (SettlementDisposition::Abandon, BTreeMap::new()),
        Outcome::Modified(modified) => {
            let disposition = if modified.undeliverable_here == Some(true) {
                SettlementDisposition::Defer
            } else {
                SettlementDisposition::Abandon
            };
            // Service Bus uses these disposition fields for application-property
            // updates, not updates to the message-annotations section.
            (disposition, read_fields(modified.message_annotations))
        }
        Outcome::Rejected(rejected) => match rejected.error {
            Some(error) if error.condition.as_symbol().as_str() == DEAD_LETTER_CONDITION => {
                let mut properties = read_fields(error.info);
                let description = error
                    .description
                    .unwrap_or_else(|| "the receiver rejected the message".to_owned());
                let disposition = dead_letter_disposition(
                    None,
                    None,
                    &mut properties,
                    "RejectedByReceiver",
                    &description,
                )?;
                (disposition, properties)
            }
            error => (
                SettlementDisposition::DeadLetter {
                    reason: "RejectedByReceiver".to_owned(),
                    description: error
                        .and_then(|error| error.description)
                        .unwrap_or_else(|| "the receiver rejected the message".to_owned()),
                },
                BTreeMap::new(),
            ),
        },
    };
    Ok(CommandKind::Settle {
        sequence,
        lock_token,
        disposition,
        properties_to_modify,
    })
}

pub(crate) fn held_settlement_command(
    sequence: SequenceNumber,
    lock_token: LockToken,
    session: Option<domain::SessionHold>,
    outcome: Outcome,
) -> Result<CommandKind, SettlementError> {
    let CommandKind::Settle {
        sequence,
        lock_token,
        disposition,
        properties_to_modify,
    } = settlement_command(sequence, lock_token, outcome)?
    else {
        unreachable!("the legacy mapper produces only Settle")
    };
    Ok(CommandKind::SettleHeld {
        sequence,
        lock_token,
        session,
        disposition,
        properties_to_modify,
    })
}

pub(crate) fn read_properties_to_modify(
    value: Option<&Value>,
) -> Result<BTreeMap<String, MessageValue>, SettlementError> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    let Value::Map(entries) = value else {
        return Err(SettlementError::InvalidField {
            field: PROPERTIES_TO_MODIFY,
            expected: "a map with string keys",
        });
    };
    entries
        .iter()
        .map(|(key, value)| {
            let Value::String(key) = key else {
                return Err(SettlementError::InvalidField {
                    field: PROPERTIES_TO_MODIFY,
                    expected: "a map with string keys",
                });
            };
            Ok((key.clone(), read_value(value)))
        })
        .collect()
}

pub(crate) fn dead_letter_disposition(
    reason: Option<String>,
    description: Option<String>,
    properties: &mut BTreeMap<String, MessageValue>,
    fallback_reason: &str,
    fallback_description: &str,
) -> Result<SettlementDisposition, SettlementError> {
    let property_reason = optional_string(properties, DEAD_LETTER_REASON_PROPERTY)?;
    let property_description = optional_string(properties, DEAD_LETTER_DESCRIPTION_PROPERTY)?;
    let disposition = SettlementDisposition::DeadLetter {
        reason: reason
            .or(property_reason)
            .unwrap_or_else(|| fallback_reason.to_owned()),
        description: description
            .or(property_description)
            .unwrap_or_else(|| fallback_description.to_owned()),
    };
    properties.remove(DEAD_LETTER_REASON_PROPERTY);
    properties.remove(DEAD_LETTER_DESCRIPTION_PROPERTY);
    Ok(disposition)
}

fn optional_string(
    properties: &BTreeMap<String, MessageValue>,
    name: &'static str,
) -> Result<Option<String>, SettlementError> {
    match properties.get(name) {
        None => Ok(None),
        Some(MessageValue::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(SettlementError::InvalidField {
            field: name,
            expected: "a string",
        }),
    }
}

fn read_fields(fields: Option<Fields>) -> BTreeMap<String, MessageValue> {
    fields.map_or_else(BTreeMap::new, |fields| {
        fields
            .iter()
            .map(|(key, value)| (key.as_str().to_owned(), read_value(value)))
            .collect()
    })
}

#[cfg(test)]
mod held_tests;

#[cfg(test)]
mod tests {
    use amqp::{
        Accepted, AmqpError, Declared, Described, Descriptor, Error as AmqpErrorValue,
        ErrorCondition, Modified, OrderedMap, Rejected, Released, Symbol, TransactionId,
    };
    use domain::MessageDescriptor;

    use super::*;

    fn parts(outcome: Outcome) -> (SettlementDisposition, BTreeMap<String, MessageValue>) {
        let sequence = SequenceNumber::new(7);
        let lock_token = LockToken::new(11);
        let CommandKind::Settle {
            sequence: actual_sequence,
            lock_token: actual_token,
            disposition,
            properties_to_modify,
        } = settlement_command(sequence, lock_token, outcome).expect("valid settlement")
        else {
            panic!("a settlement command is returned");
        };
        assert_eq!(actual_sequence, sequence);
        assert_eq!(actual_token, lock_token);
        (disposition, properties_to_modify)
    }

    fn rejection(info: Fields) -> Outcome {
        Outcome::Rejected(Rejected {
            error: Some(AmqpErrorValue {
                condition: ErrorCondition::Custom(Symbol::from(DEAD_LETTER_CONDITION)),
                description: None,
                info: Some(info),
            }),
        })
    }

    #[test]
    fn ordinary_outcomes_keep_existing_settlements() {
        for (outcome, expected) in [
            (Outcome::Accepted(Accepted), SettlementDisposition::Complete),
            (Outcome::Released(Released), SettlementDisposition::Abandon),
            (
                Outcome::Modified(Modified::default()),
                SettlementDisposition::Abandon,
            ),
        ] {
            let (actual, properties) = parts(outcome);
            assert_eq!(actual, expected);
            assert!(properties.is_empty());
        }
    }

    #[test]
    fn a_declared_transaction_is_not_an_ordinary_message_settlement() {
        let outcome = Outcome::Declared(Declared {
            txn_id: TransactionId::new([1]).expect("bounded transaction identifier"),
        });
        assert_eq!(
            settlement_command(SequenceNumber::new(7), LockToken::new(11), outcome),
            Err(SettlementError::InvalidField {
                field: "state",
                expected: "an ordinary messaging outcome",
            })
        );
    }

    #[test]
    fn modified_updates_application_properties_for_abandon_and_defer() {
        for undeliverable_here in [false, true] {
            let mut fields = Fields::new();
            fields.insert(Symbol::from("retry"), Value::Uint(2));
            fields.insert(Symbol::from("nullable"), Value::Null);
            fields.insert(
                Symbol::from("bits"),
                Value::Double(f64::from_bits(0xfff8_0123_4567_89ab).into()),
            );
            let (disposition, properties) = parts(Outcome::Modified(Modified {
                delivery_failed: Some(true),
                undeliverable_here: Some(undeliverable_here),
                message_annotations: Some(fields),
            }));
            assert_eq!(
                disposition,
                if undeliverable_here {
                    SettlementDisposition::Defer
                } else {
                    SettlementDisposition::Abandon
                }
            );
            assert_eq!(properties.get("retry"), Some(&MessageValue::Uint(2)));
            assert_eq!(properties.get("nullable"), Some(&MessageValue::Null));
            assert_eq!(
                properties.get("bits"),
                Some(&MessageValue::Double(0xfff8_0123_4567_89ab))
            );
        }
    }

    #[test]
    fn sdk_rejection_extracts_reason_and_description_from_info() {
        let mut info = Fields::new();
        info.insert(
            Symbol::from(DEAD_LETTER_REASON_PROPERTY),
            Value::String("invalid-order".to_owned()),
        );
        info.insert(
            Symbol::from(DEAD_LETTER_DESCRIPTION_PROPERTY),
            Value::String("the order is incomplete".to_owned()),
        );
        info.insert(Symbol::from("attempts"), Value::Int(3));
        let (disposition, properties) = parts(rejection(info));
        assert_eq!(
            disposition,
            SettlementDisposition::DeadLetter {
                reason: "invalid-order".to_owned(),
                description: "the order is incomplete".to_owned(),
            }
        );
        assert_eq!(
            properties,
            BTreeMap::from([("attempts".to_owned(), MessageValue::Int(3))])
        );
    }

    #[test]
    fn sdk_rejection_without_reserved_info_keeps_existing_fallback() {
        let (disposition, properties) = parts(rejection(Fields::new()));
        assert_eq!(
            disposition,
            SettlementDisposition::DeadLetter {
                reason: "RejectedByReceiver".to_owned(),
                description: "the receiver rejected the message".to_owned(),
            }
        );
        assert!(properties.is_empty());
    }

    #[test]
    fn generic_rejection_keeps_diagnostic_description_without_applying_info() {
        let mut info = Fields::new();
        info.insert(Symbol::from("retry"), Value::Uint(99));
        info.insert(Symbol::from(DEAD_LETTER_REASON_PROPERTY), Value::Null);
        let (disposition, properties) = parts(Outcome::Rejected(Rejected {
            error: Some(AmqpErrorValue::new(
                AmqpError::InvalidField,
                "bad value",
                Some(info),
            )),
        }));
        assert_eq!(
            disposition,
            SettlementDisposition::DeadLetter {
                reason: "RejectedByReceiver".to_owned(),
                description: "bad value".to_owned(),
            }
        );
        assert!(properties.is_empty());
    }

    #[test]
    fn sdk_rejection_refuses_malformed_reserved_values() {
        for name in [
            DEAD_LETTER_REASON_PROPERTY,
            DEAD_LETTER_DESCRIPTION_PROPERTY,
        ] {
            for value in [
                Value::Null,
                Value::Uint(1),
                Value::Symbol(Symbol::from("reason")),
            ] {
                let mut info = Fields::new();
                info.insert(Symbol::from(name), value);
                assert_eq!(
                    settlement_command(SequenceNumber::new(7), LockToken::new(11), rejection(info)),
                    Err(SettlementError::InvalidField {
                        field: name,
                        expected: "a string"
                    })
                );
            }
        }
    }

    #[test]
    fn management_properties_use_string_keys_and_preserve_described_simple_values() {
        let mut entries = OrderedMap::new();
        entries.insert(Value::String("nullable".to_owned()), Value::Null);
        entries.insert(
            Value::String("elapsed".to_owned()),
            Value::Described(Box::new(Described {
                descriptor: Descriptor::Name(Symbol::from("com.microsoft:timespan")),
                value: Value::Long(123),
            })),
        );
        let properties = read_properties_to_modify(Some(&Value::Map(entries))).expect("valid map");
        assert_eq!(properties.get("nullable"), Some(&MessageValue::Null));
        assert_eq!(
            properties.get("elapsed"),
            Some(&MessageValue::Described {
                descriptor: MessageDescriptor::Name("com.microsoft:timespan".to_owned()),
                value: Box::new(MessageValue::Long(123)),
            })
        );
        assert!(
            read_properties_to_modify(None)
                .expect("absent map")
                .is_empty()
        );
    }

    #[test]
    fn management_properties_refuse_non_map_or_non_string_keys() {
        let mut entries = OrderedMap::new();
        entries.insert(Value::Symbol(Symbol::from("retry")), Value::Uint(2));
        for value in [Value::Null, Value::List(Vec::new()), Value::Map(entries)] {
            assert_eq!(
                read_properties_to_modify(Some(&value)),
                Err(SettlementError::InvalidField {
                    field: PROPERTIES_TO_MODIFY,
                    expected: "a map with string keys",
                })
            );
        }
    }

    #[test]
    fn management_explicit_dead_letter_fields_override_reserved_property_updates() {
        let mut properties = BTreeMap::from([
            (
                DEAD_LETTER_REASON_PROPERTY.to_owned(),
                MessageValue::String("property reason".to_owned()),
            ),
            (
                DEAD_LETTER_DESCRIPTION_PROPERTY.to_owned(),
                MessageValue::String("property description".to_owned()),
            ),
            ("retry".to_owned(), MessageValue::Uint(2)),
        ]);
        let disposition = dead_letter_disposition(
            Some("explicit reason".to_owned()),
            Some("explicit description".to_owned()),
            &mut properties,
            "fallback reason",
            "fallback description",
        )
        .expect("valid dead letter fields");
        assert_eq!(
            disposition,
            SettlementDisposition::DeadLetter {
                reason: "explicit reason".to_owned(),
                description: "explicit description".to_owned(),
            }
        );
        assert_eq!(
            properties,
            BTreeMap::from([("retry".to_owned(), MessageValue::Uint(2))])
        );
    }

    #[test]
    fn management_reserved_fields_are_promoted_when_explicit_fields_are_absent() {
        let mut properties = BTreeMap::from([
            (
                DEAD_LETTER_REASON_PROPERTY.to_owned(),
                MessageValue::String(String::new()),
            ),
            (
                DEAD_LETTER_DESCRIPTION_PROPERTY.to_owned(),
                MessageValue::String("property description".to_owned()),
            ),
        ]);
        let disposition = dead_letter_disposition(
            None,
            None,
            &mut properties,
            "fallback reason",
            "fallback description",
        )
        .expect("valid reserved fields");
        assert_eq!(
            disposition,
            SettlementDisposition::DeadLetter {
                reason: String::new(),
                description: "property description".to_owned(),
            }
        );
        assert!(properties.is_empty());
    }

    #[test]
    fn malformed_dead_letter_fields_do_not_partially_remove_properties() {
        let mut properties = BTreeMap::from([
            (
                DEAD_LETTER_REASON_PROPERTY.to_owned(),
                MessageValue::String("reason".to_owned()),
            ),
            (
                DEAD_LETTER_DESCRIPTION_PROPERTY.to_owned(),
                MessageValue::Null,
            ),
        ]);
        let original = properties.clone();
        assert!(
            dead_letter_disposition(
                None,
                None,
                &mut properties,
                "fallback reason",
                "fallback description",
            )
            .is_err()
        );
        assert_eq!(properties, original);
    }
}
