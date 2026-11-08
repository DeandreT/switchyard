use message_content::{Text, Width};

use super::*;
use crate::{SqlProperty, SqlValue};

pub(super) fn evaluate(
    program: &SqlActionProgram,
    message: SqlMessageContext<'_>,
    budget: &mut SqlEvaluationBudget,
    limits: SqlActionLimits,
) -> Result<SqlActionPatch, SqlActionError> {
    budget.work(message.application_properties.len())?;
    // Only borrowed names/scalars are retained while checking the whole
    // program. Neither input strings nor payloads are cloned on a failed plan.
    let mut plan: Vec<(&str, Option<SqlValue<'_>>)> = Vec::new();
    let mut bytes = 0_usize;
    for statement in &program.statements {
        budget.work(1)?;
        let target = statement.target.as_str();
        let mut position = None;
        for (index, (name, _)) in plan.iter().enumerate() {
            compare_charge(name, target, budget)?;
            if *name == target {
                position = Some(index);
                break;
            }
        }
        let value = match &statement.literal {
            None => None,
            Some(literal) => {
                // A previous REMOVE means absent, not a fallback to the
                // original constructor. Repeated SET sees the prior result.
                let current = match position {
                    Some(index) => plan[index].1,
                    None => original(message.application_properties, target, budget)?,
                };
                Some(convert(literal, current)?)
            }
        };
        let old_bytes = position.map_or(0, |index| entry_bytes(target, plan[index].1));
        let next_bytes = bytes
            .saturating_sub(old_bytes)
            .saturating_add(entry_bytes(target, value));
        let count = plan.len() + usize::from(position.is_none());
        // Check every intermediate patch, not only the final reduced patch.
        if count > limits.properties {
            return Err(SqlActionError::PatchProperties {
                maximum: limits.properties,
            });
        }
        if next_bytes > limits.bytes {
            return Err(SqlActionError::PatchBytes {
                maximum: limits.bytes,
            });
        }
        match position {
            Some(index) => plan[index].1 = value,
            None => plan.push((target, value)),
        }
        bytes = next_bytes;
    }
    // Charge before allocating the owned output. Ceilings also bound the small
    // scalar plan above; compilation caps it at 32 entries and 4 KiB of source.
    budget.work(plan.len().saturating_add(bytes))?;
    budget.bytes(bytes)?;
    let entries = plan
        .into_iter()
        .map(|(name, value)| match value {
            None => SqlActionProperty::Remove {
                name: name.to_owned(),
            },
            Some(value) => SqlActionProperty::Set {
                name: name.to_owned(),
                value: materialize(value),
            },
        })
        .collect();
    Ok(SqlActionPatch {
        entries,
        charged_bytes: bytes,
    })
}

fn compare_charge(
    left: &str,
    right: &str,
    budget: &mut SqlEvaluationBudget,
) -> Result<(), SqlEvaluationError> {
    let bytes = left.len().saturating_add(right.len());
    budget.work(1_usize.saturating_add(bytes))?;
    budget.bytes(bytes)
}

fn original<'a>(
    properties: &[SqlProperty<'a>],
    target: &str,
    budget: &mut SqlEvaluationBudget,
) -> Result<Option<SqlValue<'a>>, SqlActionError> {
    let mut found = None;
    for property in properties {
        compare_charge(property.name, target, budget)?;
        if property.name == target {
            if found.is_some() {
                return Err(SqlEvaluationError::AmbiguousProperty.into());
            }
            found = Some(property.value);
        }
    }
    Ok(found)
}

fn convert<'a>(
    literal: &'a Literal,
    current: Option<SqlValue<'_>>,
) -> Result<SqlValue<'a>, SqlActionError> {
    // The existing typed SQL input preserves integer constructors. Null, an
    // absent target and a prior removal all admit a fresh literal constructor.
    let current = current.unwrap_or(SqlValue::Null);
    if matches!(
        current,
        SqlValue::Unsupported | SqlValue::Float(_) | SqlValue::Double(_)
    ) {
        return Err(SqlActionError::UnsupportedTargetType);
    }
    let overflow = |_| SqlActionError::NumericOverflow;
    Ok(match (literal, current) {
        (Literal::String(value), SqlValue::Null | SqlValue::String(_)) => SqlValue::String(value),
        (Literal::Bool(value), SqlValue::Null | SqlValue::Bool(_)) => SqlValue::Bool(*value),
        (Literal::Integer(value), SqlValue::Null | SqlValue::Long(_)) => SqlValue::Long(*value),
        (Literal::Integer(value), SqlValue::Byte(_)) => {
            SqlValue::Byte(i8::try_from(*value).map_err(overflow)?)
        }
        (Literal::Integer(value), SqlValue::Ubyte(_)) => {
            SqlValue::Ubyte(u8::try_from(*value).map_err(overflow)?)
        }
        (Literal::Integer(value), SqlValue::Short(_)) => {
            SqlValue::Short(i16::try_from(*value).map_err(overflow)?)
        }
        (Literal::Integer(value), SqlValue::Ushort(_)) => {
            SqlValue::Ushort(u16::try_from(*value).map_err(overflow)?)
        }
        (Literal::Integer(value), SqlValue::Int(_)) => {
            SqlValue::Int(i32::try_from(*value).map_err(overflow)?)
        }
        (Literal::Integer(value), SqlValue::Uint(_)) => {
            SqlValue::Uint(u32::try_from(*value).map_err(overflow)?)
        }
        (Literal::Integer(value), SqlValue::Ulong(_)) => {
            SqlValue::Ulong(u64::try_from(*value).map_err(overflow)?)
        }
        _ => return Err(SqlActionError::TypeMismatch),
    })
}

fn entry_bytes(name: &str, value: Option<SqlValue<'_>>) -> usize {
    let value_bytes = match value {
        None => 1,
        Some(SqlValue::String(text)) => 5_usize.saturating_add(text.len()),
        Some(SqlValue::Bool(_) | SqlValue::Byte(_) | SqlValue::Ubyte(_)) => 2,
        Some(SqlValue::Short(_) | SqlValue::Ushort(_)) => 3,
        Some(SqlValue::Int(_) | SqlValue::Uint(_)) => 5,
        Some(SqlValue::Long(_) | SqlValue::Ulong(_)) => 9,
        _ => unreachable!("the checked plan contains only action literal result types"),
    };
    5_usize
        .saturating_add(name.len())
        .saturating_add(value_bytes)
}

fn materialize(value: SqlValue<'_>) -> Value {
    match value {
        SqlValue::String(text) => Value::String(Text {
            text: text.to_owned(),
            width: if text.len() <= u8::MAX as usize {
                Width::One
            } else {
                Width::Four
            },
        }),
        SqlValue::Bool(value) => Value::Bool(value),
        SqlValue::Byte(value) => Value::Byte(value),
        SqlValue::Ubyte(value) => Value::UByte(value),
        SqlValue::Short(value) => Value::Short(value),
        SqlValue::Ushort(value) => Value::UShort(value),
        SqlValue::Int(value) => Value::Int(value),
        SqlValue::Uint(value) => Value::UInt(value),
        SqlValue::Long(value) => Value::Long(value),
        SqlValue::Ulong(value) => Value::ULong(value),
        _ => unreachable!("the checked plan contains only action literal result types"),
    }
}
