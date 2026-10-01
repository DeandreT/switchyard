use super::*;

pub(super) fn target_terminus_to_value(target: &TargetTerminus) -> io::Result<Value> {
    match target {
        TargetTerminus::Target(target) => Ok(target_to_value(target)),
        TargetTerminus::Coordinator(coordinator) => {
            let capabilities = match &coordinator.capabilities {
                Some(capabilities) if !capabilities.is_empty() => {
                    validate_capabilities(capabilities)?;
                    symbol_array(&coordinator.capabilities)
                }
                _ => Value::Null,
            };
            Ok(described(COORDINATOR, list(vec![capabilities])))
        }
    }
}

pub(super) fn target_terminus_from_value(value: Value) -> io::Result<TargetTerminus> {
    let (descriptor, value) = take_described(value)?;
    let fields = take_list(value)?;
    match descriptor {
        TARGET => Ok(TargetTerminus::Target(target_from_fields(fields)?)),
        COORDINATOR => Ok(TargetTerminus::Coordinator(Coordinator {
            capabilities: coordinator_capabilities(
                fields.into_iter().next().unwrap_or(Value::Null),
            )?,
        })),
        _ => Err(invalid_data(
            "terminus is not an AMQP target or coordinator",
        )),
    }
}

fn coordinator_capabilities(value: Value) -> io::Result<Option<Array<Symbol>>> {
    let symbols = match value {
        Value::Null => return Ok(None),
        Value::Symbol(symbol) => vec![symbol],
        // The generic Value representation does not retain an empty array's
        // constructor. Locally any decoded empty array normalizes to absence.
        Value::Array(values) if values.is_empty() => return Ok(None),
        Value::Array(values) => {
            if values.len() > crate::value_codec::MAX_VALUE_ELEMENTS {
                return Err(invalid_data("coordinator capability value limit exceeded"));
            }
            values
                .into_iter()
                .map(|value| match value {
                    Value::Symbol(symbol) => Ok(symbol),
                    _ => Err(invalid_data(
                        "coordinator capability array requires symbols",
                    )),
                })
                .collect::<io::Result<Vec<_>>>()?
        }
        _ => {
            return Err(invalid_data(
                "coordinator capabilities require a symbol or symbol array",
            ));
        }
    };
    let symbols = Array::from(symbols);
    validate_capabilities(&symbols)?;
    Ok(Some(symbols))
}

// These field-local ceilings bound new capability work before cloning it;
// they do not change the legacy frame encoder's allocation behavior.
fn validate_capabilities(symbols: &Array<Symbol>) -> io::Result<()> {
    if symbols.len() > crate::value_codec::MAX_VALUE_ELEMENTS {
        return Err(invalid_data("coordinator capability value limit exceeded"));
    }
    let mut bytes = 0_usize;
    for symbol in symbols {
        let text = symbol.as_str();
        bytes = bytes
            .checked_add(text.len())
            .filter(|bytes| *bytes <= crate::value_codec::MAX_EXPANDED_VALUE_BYTES)
            .ok_or_else(|| invalid_data("coordinator capability byte limit exceeded"))?;
        if !text.is_ascii() {
            return Err(invalid_data(
                "coordinator capabilities require ASCII symbols",
            ));
        }
    }
    Ok(())
}

impl From<TransactionCommand> for Value {
    fn from(value: TransactionCommand) -> Self {
        match value {
            TransactionCommand::Declare(declare) => described(
                DECLARE,
                list(vec![declare.global_id.unwrap_or(Value::Null)]),
            ),
            TransactionCommand::Discharge(discharge) => described(
                DISCHARGE,
                list(vec![
                    Value::Binary(discharge.txn_id.into_binary()),
                    discharge.fail.map(Value::Bool).unwrap_or(Value::Null),
                ]),
            ),
        }
    }
}

impl TryFrom<Value> for TransactionCommand {
    type Error = io::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let (descriptor, value) = take_described(value)?;
        let fields = take_list(value)?;
        match descriptor {
            DECLARE => Ok(Self::Declare(Declare {
                global_id: fields
                    .into_iter()
                    .next()
                    .filter(|value| *value != Value::Null),
            })),
            DISCHARGE => Ok(Self::Discharge(Discharge {
                txn_id: transaction_id(fields.first())?,
                fail: match fields.get(1) {
                    None | Some(Value::Null) => None,
                    Some(Value::Bool(value)) => Some(*value),
                    _ => return Err(invalid_data("discharge.fail requires a boolean")),
                },
            })),
            _ => Err(invalid_data("value is not an AMQP declare or discharge")),
        }
    }
}

fn transaction_id(value: Option<&Value>) -> io::Result<TransactionId> {
    let Some(Value::Binary(value)) = value else {
        return Err(invalid_data(
            "transaction id requires a non-null binary value",
        ));
    };
    TransactionId::new(value).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

pub(super) fn declared_to_value(declared: &Declared) -> Value {
    described(
        DECLARED,
        list(vec![Value::Binary(declared.txn_id.as_binary().clone())]),
    )
}

pub(super) fn declared_from_fields(fields: &[Value]) -> io::Result<Declared> {
    Ok(Declared {
        txn_id: transaction_id(fields.first())?,
    })
}

pub(super) fn transactional_state_to_value(state: &TransactionalState) -> Value {
    described(
        TRANSACTIONAL_STATE,
        list(vec![
            Value::Binary(state.txn_id.as_binary().clone()),
            state
                .outcome
                .as_ref()
                .map(outcome_to_value)
                .unwrap_or(Value::Null),
        ]),
    )
}

pub(super) fn transactional_state_from_fields(
    fields: Vec<Value>,
) -> io::Result<TransactionalState> {
    let txn_id = transaction_id(fields.first())?;
    Ok(TransactionalState {
        txn_id,
        outcome: match fields.into_iter().nth(1) {
            None | Some(Value::Null) => None,
            Some(value) => Some(outcome_from_value(value)?),
        },
    })
}

fn outcome_to_value(outcome: &Outcome) -> Value {
    match outcome {
        Outcome::Accepted(_) => described(ACCEPTED, Value::List(Vec::new())),
        Outcome::Rejected(rejected) => described(
            REJECTED,
            list(vec![
                rejected
                    .error
                    .as_ref()
                    .map(error_to_value)
                    .unwrap_or(Value::Null),
            ]),
        ),
        Outcome::Released(_) => described(RELEASED, Value::List(Vec::new())),
        Outcome::Modified(modified) => described(
            MODIFIED,
            list(vec![
                modified
                    .delivery_failed
                    .map(Value::Bool)
                    .unwrap_or(Value::Null),
                modified
                    .undeliverable_here
                    .map(Value::Bool)
                    .unwrap_or(Value::Null),
                fields_to_value(&modified.message_annotations),
            ]),
        ),
        Outcome::Declared(declared) => declared_to_value(declared),
    }
}

fn outcome_from_value(value: Value) -> io::Result<Outcome> {
    let (descriptor, value) = take_described(value)?;
    if !matches!(
        descriptor,
        ACCEPTED | REJECTED | RELEASED | MODIFIED | DECLARED
    ) {
        return Err(invalid_data(
            "transactional outcome must provide an AMQP outcome",
        ));
    }
    let state = delivery_state_from_fields(descriptor, take_list(value)?)?;
    Outcome::try_from(state)
        .map_err(|_| invalid_data("transactional outcome must provide an AMQP outcome"))
}

#[cfg(test)]
mod tests;
