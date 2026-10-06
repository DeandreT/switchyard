use sqlparser::ast::{Expr, UnaryOperator, Value};

use crate::{MessageValue, SqlCompileBudget, SqlCompileError};

use super::evaluation::SqlActionError;

#[derive(Clone, Debug)]
pub(super) enum ActionLiteral {
    String(String),
    Bool(bool),
    Integer(i64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ActionValue {
    String(usize),
    Bool(bool),
    Byte(i8),
    Ubyte(u8),
    Short(i16),
    Ushort(u16),
    Int(i32),
    Uint(u32),
    Long(i64),
    Ulong(u64),
}

pub(super) fn parse(
    expression: &Expr,
    budget: &mut SqlCompileBudget,
) -> Result<ActionLiteral, SqlCompileError> {
    budget.charge_node()?;
    match expression {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(value) => Ok(ActionLiteral::String(value.clone())),
            Value::Boolean(value) => Ok(ActionLiteral::Bool(*value)),
            Value::Number(value, false) => integer(value, false).map(ActionLiteral::Integer),
            _ => Err(unsupported("SQL action literal")),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus | UnaryOperator::Plus,
            expr,
        } => {
            budget.charge_node()?;
            let Expr::Value(value) = expr.as_ref() else {
                return Err(unsupported("SQL action signed integer"));
            };
            let Value::Number(value, false) = &value.value else {
                return Err(unsupported("SQL action signed integer"));
            };
            integer(
                value,
                matches!(
                    expression,
                    Expr::UnaryOp {
                        op: UnaryOperator::Minus,
                        ..
                    }
                ),
            )
            .map(ActionLiteral::Integer)
        }
        _ => Err(unsupported("SQL action literal")),
    }
}

fn integer(source: &str, negative: bool) -> Result<i64, SqlCompileError> {
    if source.is_empty() || !source.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(unsupported("SQL action integer literal"));
    }
    let magnitude = source
        .parse::<u64>()
        .map_err(|_| unsupported("SQL action integer range"))?;
    if negative && magnitude == i64::MAX as u64 + 1 {
        return Ok(i64::MIN);
    }
    let value = i64::try_from(magnitude).map_err(|_| unsupported("SQL action integer range"))?;
    Ok(if negative { -value } else { value })
}

impl ActionLiteral {
    pub(super) fn bytes(&self) -> usize {
        match self {
            Self::String(value) => value.len(),
            Self::Bool(_) => 1,
            Self::Integer(_) => 8,
        }
    }

    pub(super) fn convert(
        &self,
        statement: usize,
        current: Option<ActionValue>,
        original: Option<&MessageValue>,
    ) -> Result<ActionValue, SqlActionError> {
        let family = current
            .map(Family::from)
            .map_or_else(|| original_family(original), Ok)?;
        match (self, family) {
            (Self::String(_), Family::Absent | Family::String) => {
                Ok(ActionValue::String(statement))
            }
            (Self::Bool(value), Family::Absent | Family::Bool) => Ok(ActionValue::Bool(*value)),
            (Self::Integer(value), Family::Absent | Family::Long) => Ok(ActionValue::Long(*value)),
            (Self::Integer(value), Family::Byte) => i8::try_from(*value)
                .map(ActionValue::Byte)
                .map_err(overflow),
            (Self::Integer(value), Family::Ubyte) => u8::try_from(*value)
                .map(ActionValue::Ubyte)
                .map_err(overflow),
            (Self::Integer(value), Family::Short) => i16::try_from(*value)
                .map(ActionValue::Short)
                .map_err(overflow),
            (Self::Integer(value), Family::Ushort) => u16::try_from(*value)
                .map(ActionValue::Ushort)
                .map_err(overflow),
            (Self::Integer(value), Family::Int) => i32::try_from(*value)
                .map(ActionValue::Int)
                .map_err(overflow),
            (Self::Integer(value), Family::Uint) => u32::try_from(*value)
                .map(ActionValue::Uint)
                .map_err(overflow),
            (Self::Integer(value), Family::Ulong) => u64::try_from(*value)
                .map(ActionValue::Ulong)
                .map_err(overflow),
            _ => Err(SqlActionError::TypeMismatch),
        }
    }
}

#[derive(Clone, Copy)]
enum Family {
    Absent,
    String,
    Bool,
    Byte,
    Ubyte,
    Short,
    Ushort,
    Int,
    Uint,
    Long,
    Ulong,
}

fn original_family(value: Option<&MessageValue>) -> Result<Family, SqlActionError> {
    Ok(match value {
        None | Some(MessageValue::Null) => Family::Absent,
        Some(MessageValue::String(_)) => Family::String,
        Some(MessageValue::Bool(_)) => Family::Bool,
        Some(MessageValue::Byte(_)) => Family::Byte,
        Some(MessageValue::Ubyte(_)) => Family::Ubyte,
        Some(MessageValue::Short(_)) => Family::Short,
        Some(MessageValue::Ushort(_)) => Family::Ushort,
        Some(MessageValue::Int(_)) => Family::Int,
        Some(MessageValue::Uint(_)) => Family::Uint,
        Some(MessageValue::Long(_)) => Family::Long,
        Some(MessageValue::Ulong(_)) => Family::Ulong,
        _ => return Err(SqlActionError::UnsupportedTargetType),
    })
}

impl From<ActionValue> for Family {
    fn from(value: ActionValue) -> Self {
        match value {
            ActionValue::String(_) => Self::String,
            ActionValue::Bool(_) => Self::Bool,
            ActionValue::Byte(_) => Self::Byte,
            ActionValue::Ubyte(_) => Self::Ubyte,
            ActionValue::Short(_) => Self::Short,
            ActionValue::Ushort(_) => Self::Ushort,
            ActionValue::Int(_) => Self::Int,
            ActionValue::Uint(_) => Self::Uint,
            ActionValue::Long(_) => Self::Long,
            ActionValue::Ulong(_) => Self::Ulong,
        }
    }
}

impl ActionValue {
    pub(super) fn content_size(self, program: &super::SqlActionProgram) -> usize {
        match self {
            Self::String(index) => match &program.values[index] {
                Some(ActionLiteral::String(value)) => 5_usize.saturating_add(value.len()),
                _ => unreachable!("checked string references its original literal"),
            },
            Self::Bool(_) => 2,
            Self::Byte(_) | Self::Ubyte(_) => 2,
            Self::Short(_) | Self::Ushort(_) => 3,
            Self::Int(_) | Self::Uint(_) => 5,
            Self::Long(_) | Self::Ulong(_) => 9,
        }
    }

    pub(super) fn materialize(self, program: &super::SqlActionProgram) -> MessageValue {
        match self {
            Self::String(index) => match &program.values[index] {
                Some(ActionLiteral::String(value)) => MessageValue::String(value.clone()),
                _ => unreachable!("checked string references its original literal"),
            },
            Self::Bool(value) => MessageValue::Bool(value),
            Self::Byte(value) => MessageValue::Byte(value),
            Self::Ubyte(value) => MessageValue::Ubyte(value),
            Self::Short(value) => MessageValue::Short(value),
            Self::Ushort(value) => MessageValue::Ushort(value),
            Self::Int(value) => MessageValue::Int(value),
            Self::Uint(value) => MessageValue::Uint(value),
            Self::Long(value) => MessageValue::Long(value),
            Self::Ulong(value) => MessageValue::Ulong(value),
        }
    }
}

fn overflow(_: std::num::TryFromIntError) -> SqlActionError {
    SqlActionError::NumericOverflow
}
fn unsupported(feature: &'static str) -> SqlCompileError {
    SqlCompileError::Unsupported { feature }
}
