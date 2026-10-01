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

#[derive(Clone, Copy)]
struct Slot<'a> {
    value: Result<Value<'a>, SqlEvaluationError>,
    string_bound: usize,
}

impl<'a> Slot<'a> {
    fn new(value: Result<Value<'a>, SqlEvaluationError>, failed_string_bound: usize) -> Self {
        let string_bound = match value {
            Ok(Value::String(value)) => value.len(),
            Ok(_) => 0,
            Err(_) => failed_string_bound,
        };
        Self {
            value,
            string_bound,
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

    // Finite failures stay in the arena so they cannot conceal independent
    // comparisons or LIKE limits. Failed slots carry conservative string
    // bounds for dependent operations; all payloads remain borrowed.
    let mut slots = Vec::with_capacity(program.nodes.len());
    let mut first_error = None;
    for node in &program.nodes {
        let value = match node {
            SqlNode::Literal(literal) => Slot::new(
                Ok(match literal {
                    SqlLiteral::Null => Value::Null,
                    SqlLiteral::Bool(value) => Value::Bool(*value),
                    SqlLiteral::Int64(value) => {
                        Value::Number(Number::literal(NumericKind::I64(*value)))
                    }
                    SqlLiteral::DoubleBits(value) => {
                        Value::Number(Number::literal(NumericKind::F64(f64::from_bits(*value))))
                    }
                    SqlLiteral::String(value) => Value::String(value),
                }),
                0,
            ),
            SqlNode::Property(property) | SqlNode::Exists(property) => {
                let property = properties::lookup(message, property);
                let value = if matches!(node, SqlNode::Exists(_)) {
                    property.value.map(|_| Value::Bool(property.exists))
                } else {
                    property.value
                };
                Slot::new(value, property.string_bound)
            }
            SqlNode::Unary { op, input } => {
                let input = slot(&slots, *input)?;
                Slot::new(
                    input.value.and_then(|value| unary(*op, value)),
                    input.string_bound,
                )
            }
            SqlNode::Binary { op, left, right } => {
                let left = slot(&slots, *left)?;
                let right = slot(&slots, *right)?;
                Slot::new(
                    binary_slots(*op, left, right, budget),
                    left.string_bound.max(right.string_bound),
                )
            }
            SqlNode::IsNull { input, negated } => {
                let input = slot(&slots, *input)?;
                Slot::new(
                    input
                        .value
                        .map(|value| Value::Bool(value.is_unknown() != *negated)),
                    input.string_bound,
                )
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
                let mut error = None;
                let mut string_bound = input.string_bound;
                for operand in operands {
                    let operand = slot(&slots, *operand)?;
                    string_bound = string_bound.max(operand.string_bound);
                    match compare_slots(SqlBinaryOp::Eq, input, operand, budget) {
                        Ok(SqlTruth::True) => found = true,
                        Ok(SqlTruth::Unknown) => unknown = true,
                        Ok(SqlTruth::False) => {}
                        Err(failure @ SqlEvaluationError::Limit { .. }) => return Err(failure),
                        Err(failure) => {
                            error.get_or_insert(failure);
                        }
                    }
                }
                let value = error.map_or_else(
                    || {
                        Ok(Value::from_truth(if found {
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
                        }))
                    },
                    Err,
                );
                Slot::new(value, string_bound)
            }
            SqlNode::Like {
                input,
                pattern,
                escape,
                negated,
            } => {
                let escape = escape.map(|id| slot(&slots, id)).transpose()?;
                let input = slot(&slots, *input)?;
                let pattern = slot(&slots, *pattern)?;
                let string_bound = input
                    .string_bound
                    .max(pattern.string_bound)
                    .max(escape.map_or(0, |escape| escape.string_bound));
                Slot::new(
                    like_slots(input, pattern, escape, *negated, budget).map(Value::from_truth),
                    string_bound,
                )
            }
        };
        if let Err(error) = value.value {
            if matches!(error, SqlEvaluationError::Limit { .. }) {
                return Err(error);
            }
            first_error.get_or_insert(error);
        }
        slots.push(value);
    }
    match first_error {
        Some(error) => Err(error),
        None => slot(&slots, program.root)?.value?.truth(),
    }
}

fn slot<'a>(slots: &[Slot<'a>], index: u16) -> Result<Slot<'a>, SqlEvaluationError> {
    slots
        .get(usize::from(index))
        .copied()
        .ok_or(SqlEvaluationError::TypeMismatch)
}

fn binary_slots<'a>(
    op: SqlBinaryOp,
    left: Slot<'a>,
    right: Slot<'a>,
    budget: &mut SqlEvaluationBudget,
) -> Result<Value<'a>, SqlEvaluationError> {
    if matches!(
        op,
        SqlBinaryOp::Eq
            | SqlBinaryOp::Ne
            | SqlBinaryOp::Gt
            | SqlBinaryOp::Ge
            | SqlBinaryOp::Lt
            | SqlBinaryOp::Le
    ) {
        return compare_slots(op, left, right, budget).map(Value::from_truth);
    }
    binary(op, left.value?, right.value?, budget)
}

fn compare_slots(
    op: SqlBinaryOp,
    left: Slot<'_>,
    right: Slot<'_>,
    budget: &mut SqlEvaluationBudget,
) -> Result<SqlTruth, SqlEvaluationError> {
    match (left.value, right.value) {
        (Ok(left), Ok(right)) => compare(op, left, right, budget),
        _ => {
            budget.charge_work(1)?;
            budget.charge_bytes(left.string_bound.saturating_add(right.string_bound))?;
            Err(left
                .value
                .err()
                .or_else(|| right.value.err())
                .unwrap_or(SqlEvaluationError::TypeMismatch))
        }
    }
}

fn like_slots(
    input: Slot<'_>,
    pattern: Slot<'_>,
    escape: Option<Slot<'_>>,
    negated: bool,
    budget: &mut SqlEvaluationBudget,
) -> Result<SqlTruth, SqlEvaluationError> {
    match (
        input.value,
        pattern.value,
        escape.map(|escape| escape.value).transpose(),
    ) {
        (Ok(input), Ok(pattern), Ok(escape)) => {
            like::matches(input, pattern, escape, negated, budget)
        }
        _ => {
            like::charge_unresolved(
                input.string_bound,
                pattern.string_bound,
                escape.map_or(0, |escape| escape.string_bound),
                budget,
            )?;
            Err(input
                .value
                .err()
                .or_else(|| pattern.value.err())
                .or_else(|| escape.and_then(|escape| escape.value.err()))
                .unwrap_or(SqlEvaluationError::TypeMismatch))
        }
    }
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
    let string_bytes = |value| match value {
        Value::String(value) => value.len(),
        _ => 0,
    };
    budget.charge_bytes(string_bytes(left).saturating_add(string_bytes(right)))?;
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
        (Value::String(left), Value::String(right)) => match op {
            SqlBinaryOp::Eq => left == right,
            SqlBinaryOp::Ne => left != right,
            _ => return Err(SqlEvaluationError::StringOrderingUnsupported),
        },
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
