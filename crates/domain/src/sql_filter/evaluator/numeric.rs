use std::cmp::Ordering;

use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) struct Number {
    pub(super) kind: NumericKind,
    constant: bool,
}

impl Number {
    pub(super) const fn literal(kind: NumericKind) -> Self {
        Self {
            kind,
            constant: true,
        }
    }

    pub(super) const fn property(kind: NumericKind) -> Self {
        Self {
            kind,
            constant: false,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum NumericKind {
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
}

impl NumericKind {
    fn is_signed(self) -> bool {
        matches!(
            self,
            Self::I8(_) | Self::I16(_) | Self::I32(_) | Self::I64(_)
        )
    }

    fn signed(self) -> Result<i64, SqlEvaluationError> {
        Ok(match self {
            Self::I8(value) => i64::from(value),
            Self::U8(value) => i64::from(value),
            Self::I16(value) => i64::from(value),
            Self::U16(value) => i64::from(value),
            Self::I32(value) => i64::from(value),
            Self::U32(value) => i64::from(value),
            Self::I64(value) => value,
            Self::U64(_) | Self::F32(_) | Self::F64(_) => {
                return Err(SqlEvaluationError::TypeMismatch);
            }
        })
    }

    fn unsigned(self) -> Result<u64, SqlEvaluationError> {
        Ok(match self {
            Self::U8(value) => u64::from(value),
            Self::U16(value) => u64::from(value),
            Self::U32(value) => u64::from(value),
            Self::U64(value) => value,
            Self::I8(_) | Self::I16(_) | Self::I32(_) | Self::I64(_) => {
                u64::try_from(self.signed()?).map_err(|_| SqlEvaluationError::TypeMismatch)?
            }
            Self::F32(_) | Self::F64(_) => return Err(SqlEvaluationError::TypeMismatch),
        })
    }

    fn single(self) -> Result<f32, SqlEvaluationError> {
        Ok(match self {
            Self::I8(value) => f32::from(value),
            Self::U8(value) => f32::from(value),
            Self::I16(value) => f32::from(value),
            Self::U16(value) => f32::from(value),
            Self::I32(value) => value as f32,
            Self::U32(value) => value as f32,
            Self::I64(value) => value as f32,
            Self::U64(value) => value as f32,
            Self::F32(value) => value,
            Self::F64(_) => return Err(SqlEvaluationError::TypeMismatch),
        })
    }

    fn double(self) -> f64 {
        match self {
            Self::I8(value) => f64::from(value),
            Self::U8(value) => f64::from(value),
            Self::I16(value) => f64::from(value),
            Self::U16(value) => f64::from(value),
            Self::I32(value) => f64::from(value),
            Self::U32(value) => f64::from(value),
            Self::I64(value) => value as f64,
            Self::U64(value) => value as f64,
            Self::F32(value) => f64::from(value),
            Self::F64(value) => value,
        }
    }
}

#[derive(Clone, Copy)]
enum Promotion {
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
}

fn promotion(left: Number, right: Number) -> Result<Promotion, SqlEvaluationError> {
    use NumericKind::*;
    if matches!(left.kind, F64(_)) || matches!(right.kind, F64(_)) {
        return Ok(Promotion::F64);
    }
    if matches!(left.kind, F32(_)) || matches!(right.kind, F32(_)) {
        return Ok(Promotion::F32);
    }
    if matches!(left.kind, U64(_)) || matches!(right.kind, U64(_)) {
        for value in [left, right] {
            if value.kind.is_signed() && (!value.constant || value.kind.signed()? < 0) {
                return Err(SqlEvaluationError::TypeMismatch);
            }
        }
        return Ok(Promotion::U64);
    }
    if matches!(left.kind, I64(_)) || matches!(right.kind, I64(_)) {
        return Ok(Promotion::I64);
    }
    if matches!(left.kind, U32(_)) || matches!(right.kind, U32(_)) {
        return Ok(if left.kind.is_signed() || right.kind.is_signed() {
            Promotion::I64
        } else {
            Promotion::U32
        });
    }
    Ok(Promotion::I32)
}

pub(super) fn unary(op: SqlUnaryOp, value: Number) -> Result<Number, SqlEvaluationError> {
    use NumericKind::*;
    let kind = match (op, value.kind) {
        (SqlUnaryOp::Plus, I8(value)) => I32(i32::from(value)),
        (SqlUnaryOp::Plus, U8(value)) => I32(i32::from(value)),
        (SqlUnaryOp::Plus, I16(value)) => I32(i32::from(value)),
        (SqlUnaryOp::Plus, U16(value)) => I32(i32::from(value)),
        (SqlUnaryOp::Plus, kind) => kind,
        (SqlUnaryOp::Minus, I8(value)) => I32(-i32::from(value)),
        (SqlUnaryOp::Minus, U8(value)) => I32(-i32::from(value)),
        (SqlUnaryOp::Minus, I16(value)) => I32(-i32::from(value)),
        (SqlUnaryOp::Minus, U16(value)) => I32(-i32::from(value)),
        (SqlUnaryOp::Minus, I32(value)) => I32(value
            .checked_neg()
            .ok_or(SqlEvaluationError::NumericOverflow)?),
        (SqlUnaryOp::Minus, U32(value)) => I64(-i64::from(value)),
        (SqlUnaryOp::Minus, I64(value)) => I64(value
            .checked_neg()
            .ok_or(SqlEvaluationError::NumericOverflow)?),
        (SqlUnaryOp::Minus, U64(_)) => return Err(SqlEvaluationError::TypeMismatch),
        (SqlUnaryOp::Minus, F32(value)) => F32(-value),
        (SqlUnaryOp::Minus, F64(value)) => F64(-value),
        (SqlUnaryOp::Not, _) => return Err(SqlEvaluationError::TypeMismatch),
    };
    Ok(Number {
        kind,
        constant: value.constant,
    })
}

pub(super) fn arithmetic(
    op: SqlBinaryOp,
    left: Number,
    right: Number,
) -> Result<Number, SqlEvaluationError> {
    let constant = left.constant && right.constant;
    macro_rules! integral {
        ($left:expr, $right:expr, $variant:ident) => {{
            let left = $left;
            let right = $right;
            if matches!(op, SqlBinaryOp::Divide | SqlBinaryOp::Modulo) && right == 0 {
                return Err(SqlEvaluationError::DivisionByZero);
            }
            let result = match op {
                SqlBinaryOp::Add => left.checked_add(right),
                SqlBinaryOp::Subtract => left.checked_sub(right),
                SqlBinaryOp::Multiply => left.checked_mul(right),
                SqlBinaryOp::Divide => left.checked_div(right),
                SqlBinaryOp::Modulo => left.checked_rem(right),
                _ => return Err(SqlEvaluationError::TypeMismatch),
            }
            .ok_or(SqlEvaluationError::NumericOverflow)?;
            NumericKind::$variant(result)
        }};
    }
    macro_rules! floating {
        ($left:expr, $right:expr, $variant:ident) => {{
            let left = $left;
            let right = $right;
            NumericKind::$variant(match op {
                SqlBinaryOp::Add => left + right,
                SqlBinaryOp::Subtract => left - right,
                SqlBinaryOp::Multiply => left * right,
                SqlBinaryOp::Divide => left / right,
                SqlBinaryOp::Modulo => left % right,
                _ => return Err(SqlEvaluationError::TypeMismatch),
            })
        }};
    }
    let kind = match promotion(left, right)? {
        Promotion::I32 => integral!(left.kind.signed()? as i32, right.kind.signed()? as i32, I32),
        Promotion::U32 => integral!(
            left.kind.unsigned()? as u32,
            right.kind.unsigned()? as u32,
            U32
        ),
        Promotion::I64 => integral!(left.kind.signed()?, right.kind.signed()?, I64),
        Promotion::U64 => integral!(left.kind.unsigned()?, right.kind.unsigned()?, U64),
        Promotion::F32 => floating!(left.kind.single()?, right.kind.single()?, F32),
        Promotion::F64 => floating!(left.kind.double(), right.kind.double(), F64),
    };
    Ok(Number { kind, constant })
}

pub(super) fn compare(
    op: SqlBinaryOp,
    left: Number,
    right: Number,
) -> Result<bool, SqlEvaluationError> {
    let ordering = match promotion(left, right)? {
        Promotion::I32 | Promotion::I64 => Some(left.kind.signed()?.cmp(&right.kind.signed()?)),
        Promotion::U32 | Promotion::U64 => Some(left.kind.unsigned()?.cmp(&right.kind.unsigned()?)),
        Promotion::F32 => left.kind.single()?.partial_cmp(&right.kind.single()?),
        Promotion::F64 => left.kind.double().partial_cmp(&right.kind.double()),
    };
    Ok(match op {
        SqlBinaryOp::Eq => ordering == Some(Ordering::Equal),
        SqlBinaryOp::Ne => ordering != Some(Ordering::Equal),
        SqlBinaryOp::Gt => ordering == Some(Ordering::Greater),
        SqlBinaryOp::Ge => matches!(ordering, Some(Ordering::Greater | Ordering::Equal)),
        SqlBinaryOp::Lt => ordering == Some(Ordering::Less),
        SqlBinaryOp::Le => matches!(ordering, Some(Ordering::Less | Ordering::Equal)),
        _ => return Err(SqlEvaluationError::TypeMismatch),
    })
}
