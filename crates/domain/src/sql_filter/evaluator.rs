use crate::MessageValue;

use super::*;

mod like;
mod numeric;
mod properties;

use numeric::{Number, NumericKind};

#[derive(Clone, Copy, Debug)]
enum Value<'a> {
    Unknown,
    Null,
    Unsupported,
    Bool(bool),
    Number(Number),
    String(&'a str),
}

impl Value<'_> {
    fn is_unknown(self) -> bool {
        matches!(self, Self::Unknown | Self::Null)
    }

    fn truth(self) -> Result<SqlTruth, SqlEvaluationError> {
        match self {
            Self::Unknown | Self::Null => Ok(SqlTruth::Unknown),
            Self::Bool(true) => Ok(SqlTruth::True),
            Self::Bool(false) => Ok(SqlTruth::False),
            Self::Unsupported => Err(SqlEvaluationError::UnsupportedValue),
            _ => Err(SqlEvaluationError::NonPredicate),
        }
    }

    fn from_truth(value: SqlTruth) -> Self {
        match value {
            SqlTruth::True => Self::Bool(true),
            SqlTruth::False => Self::Bool(false),
            SqlTruth::Unknown => Self::Unknown,
        }
    }
}

pub(super) fn evaluate<'a>(
    program: &'a SqlProgram,
    message: SqlMessageContext<'a>,
    budget: &mut SqlEvaluationBudget,
) -> Result<SqlTruth, SqlEvaluationError> {
    budget.charge_work(program.nodes.len())?;
    let summary = properties::Summary::new(message, budget)?;
    for node in &program.nodes {
        if let SqlNode::Property(property) | SqlNode::Exists(property) = node {
            summary.charge_lookup(property, budget)?;
        }
    }

    // Programs are postorder arenas. Slots borrow both source literals and
    // message properties; no message tree is copied during evaluation.
    let mut slots = Vec::with_capacity(program.nodes.len());
    for node in &program.nodes {
        let value = match node {
            SqlNode::Literal(literal) => match literal {
                SqlLiteral::Null => Value::Null,
                SqlLiteral::Bool(value) => Value::Bool(*value),
                SqlLiteral::Int64(value) => {
                    Value::Number(Number::literal(NumericKind::I64(*value)))
                }
                SqlLiteral::DoubleBits(value) => {
                    Value::Number(Number::literal(NumericKind::F64(f64::from_bits(*value))))
                }
                SqlLiteral::String(value) => Value::String(value),
            },
            SqlNode::Property(property) => properties::lookup(message, property)?.value,
            SqlNode::Exists(property) => Value::Bool(properties::lookup(message, property)?.exists),
            SqlNode::Unary { op, input } => unary(*op, slot(&slots, *input)?)?,
            SqlNode::Binary { op, left, right } => {
                binary(*op, slot(&slots, *left)?, slot(&slots, *right)?, budget)?
            }
            SqlNode::IsNull { input, negated } => {
                Value::Bool(slot(&slots, *input)?.is_unknown() != *negated)
            }
            SqlNode::In {
                input,
                start,
                len,
                negated,
            } => {
                let operands = start
                    .checked_add(*len)
                    .and_then(|end| program.in_operands.get(*start..end))
                    .ok_or(SqlEvaluationError::TypeMismatch)?;
                let input = slot(&slots, *input)?;
                let mut found = false;
                let mut unknown = false;
                for operand in operands {
                    match compare(SqlBinaryOp::Eq, input, slot(&slots, *operand)?, budget)? {
                        SqlTruth::True => found = true,
                        SqlTruth::Unknown => unknown = true,
                        SqlTruth::False => {}
                    }
                }
                Value::from_truth(if found {
                    if *negated {
                        SqlTruth::False
                    } else {
                        SqlTruth::True
                    }
                } else if unknown {
                    SqlTruth::Unknown
                } else if *negated {
                    SqlTruth::True
                } else {
                    SqlTruth::False
                })
            }
            SqlNode::Like {
                input,
                pattern,
                escape,
                negated,
            } => {
                let escape = escape.map(|id| slot(&slots, id)).transpose()?;
                Value::from_truth(like::matches(
                    slot(&slots, *input)?,
                    slot(&slots, *pattern)?,
                    escape,
                    *negated,
                    budget,
                )?)
            }
        };
        slots.push(value);
    }
    slot(&slots, program.root)?.truth()
}

fn slot<'a>(slots: &[Value<'a>], index: u16) -> Result<Value<'a>, SqlEvaluationError> {
    slots
        .get(usize::from(index))
        .copied()
        .ok_or(SqlEvaluationError::TypeMismatch)
}

fn unary(op: SqlUnaryOp, value: Value<'_>) -> Result<Value<'_>, SqlEvaluationError> {
    match op {
        SqlUnaryOp::Not => Ok(Value::from_truth(match operand_truth(value)? {
            SqlTruth::True => SqlTruth::False,
            SqlTruth::False => SqlTruth::True,
            SqlTruth::Unknown => SqlTruth::Unknown,
        })),
        SqlUnaryOp::Plus | SqlUnaryOp::Minus if value.is_unknown() => Ok(Value::Unknown),
        SqlUnaryOp::Plus | SqlUnaryOp::Minus => match value {
            Value::Number(value) => Ok(Value::Number(numeric::unary(op, value)?)),
            Value::Unsupported => Err(SqlEvaluationError::UnsupportedValue),
            _ => Err(SqlEvaluationError::TypeMismatch),
        },
    }
}

fn operand_truth(value: Value<'_>) -> Result<SqlTruth, SqlEvaluationError> {
    value.truth().map_err(|error| match error {
        SqlEvaluationError::NonPredicate => SqlEvaluationError::TypeMismatch,
        other => other,
    })
}

fn binary<'a>(
    op: SqlBinaryOp,
    left: Value<'a>,
    right: Value<'a>,
    budget: &mut SqlEvaluationBudget,
) -> Result<Value<'a>, SqlEvaluationError> {
    match op {
        SqlBinaryOp::And | SqlBinaryOp::Or => {
            let left = operand_truth(left)?;
            let right = operand_truth(right)?;
            Ok(Value::from_truth(match (op, left, right) {
                (SqlBinaryOp::And, SqlTruth::False, _) | (SqlBinaryOp::And, _, SqlTruth::False) => {
                    SqlTruth::False
                }
                (SqlBinaryOp::And, SqlTruth::True, SqlTruth::True) => SqlTruth::True,
                (SqlBinaryOp::Or, SqlTruth::True, _) | (SqlBinaryOp::Or, _, SqlTruth::True) => {
                    SqlTruth::True
                }
                (SqlBinaryOp::Or, SqlTruth::False, SqlTruth::False) => SqlTruth::False,
                _ => SqlTruth::Unknown,
            }))
        }
        SqlBinaryOp::Eq
        | SqlBinaryOp::Ne
        | SqlBinaryOp::Gt
        | SqlBinaryOp::Ge
        | SqlBinaryOp::Lt
        | SqlBinaryOp::Le => Ok(Value::from_truth(compare(op, left, right, budget)?)),
        _ if left.is_unknown() || right.is_unknown() => Ok(Value::Unknown),
        _ => match (left, right) {
            (Value::Number(left), Value::Number(right)) => {
                Ok(Value::Number(numeric::arithmetic(op, left, right)?))
            }
            (Value::Unsupported, _) | (_, Value::Unsupported) => {
                Err(SqlEvaluationError::UnsupportedValue)
            }
            _ => Err(SqlEvaluationError::TypeMismatch),
        },
    }
}

fn compare(
    op: SqlBinaryOp,
    left: Value<'_>,
    right: Value<'_>,
    budget: &mut SqlEvaluationBudget,
) -> Result<SqlTruth, SqlEvaluationError> {
    budget.charge_work(1)?;
    if left.is_unknown() || right.is_unknown() {
        return Ok(SqlTruth::Unknown);
    }
    let result = match (left, right) {
        (Value::Number(left), Value::Number(right)) => numeric::compare(op, left, right)?,
        (Value::Bool(left), Value::Bool(right)) => match op {
            SqlBinaryOp::Eq => left == right,
            SqlBinaryOp::Ne => left != right,
            _ => return Err(SqlEvaluationError::TypeMismatch),
        },
        (Value::String(left), Value::String(right)) => {
            budget.charge_bytes(left.len().saturating_add(right.len()))?;
            match op {
                SqlBinaryOp::Eq => left == right,
                SqlBinaryOp::Ne => left != right,
                _ => return Err(SqlEvaluationError::StringOrderingUnsupported),
            }
        }
        (Value::Unsupported, _) | (_, Value::Unsupported) => {
            return Err(SqlEvaluationError::UnsupportedValue);
        }
        _ => return Err(SqlEvaluationError::TypeMismatch),
    };
    Ok(if result {
        SqlTruth::True
    } else {
        SqlTruth::False
    })
}

fn message_value(value: &MessageValue) -> Result<Value<'_>, SqlEvaluationError> {
    Ok(match value {
        MessageValue::Null => Value::Null,
        MessageValue::Bool(value) => Value::Bool(*value),
        MessageValue::Ubyte(value) => Value::Number(Number::property(NumericKind::U8(*value))),
        MessageValue::Ushort(value) => Value::Number(Number::property(NumericKind::U16(*value))),
        MessageValue::Uint(value) => Value::Number(Number::property(NumericKind::U32(*value))),
        MessageValue::Ulong(value) => Value::Number(Number::property(NumericKind::U64(*value))),
        MessageValue::Byte(value) => Value::Number(Number::property(NumericKind::I8(*value))),
        MessageValue::Short(value) => Value::Number(Number::property(NumericKind::I16(*value))),
        MessageValue::Int(value) => Value::Number(Number::property(NumericKind::I32(*value))),
        MessageValue::Long(value) => Value::Number(Number::property(NumericKind::I64(*value))),
        MessageValue::Float(value) => {
            Value::Number(Number::property(NumericKind::F32(f32::from_bits(*value))))
        }
        MessageValue::Double(value) => {
            Value::Number(Number::property(NumericKind::F64(f64::from_bits(*value))))
        }
        MessageValue::String(value) => Value::String(value),
        _ => Value::Unsupported,
    })
}

#[cfg(test)]
mod tests;
