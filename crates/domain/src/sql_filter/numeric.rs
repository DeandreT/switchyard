use std::cmp::Ordering;

use super::{Binary, SqlEvaluationError};

#[derive(Clone, Copy, Debug)]
pub(super) struct Number {
    pub(super) kind: Kind,
    pub(super) constant: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Kind {
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

impl Kind {
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
            _ => return Err(SqlEvaluationError::TypeMismatch),
        })
    }

    fn unsigned(self) -> Result<u64, SqlEvaluationError> {
        Ok(match self {
            Self::U8(value) => u64::from(value),
            Self::U16(value) => u64::from(value),
            Self::U32(value) => u64::from(value),
            Self::U64(value) => value,
            _ => u64::try_from(self.signed()?).map_err(|_| SqlEvaluationError::TypeMismatch)?,
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

pub(super) fn compare(op: Binary, left: Number, right: Number) -> Result<bool, SqlEvaluationError> {
    use Kind::*;
    let ordering = if matches!(left.kind, F64(_)) || matches!(right.kind, F64(_)) {
        left.kind.double().partial_cmp(&right.kind.double())
    } else if matches!(left.kind, F32(_)) || matches!(right.kind, F32(_)) {
        left.kind.single()?.partial_cmp(&right.kind.single()?)
    } else if matches!(left.kind, U64(_)) || matches!(right.kind, U64(_)) {
        for value in [left, right] {
            if value.kind.is_signed() && (!value.constant || value.kind.signed()? < 0) {
                return Err(SqlEvaluationError::TypeMismatch);
            }
        }
        Some(left.kind.unsigned()?.cmp(&right.kind.unsigned()?))
    } else {
        // Every narrower integral combination fits the promoted signed i64.
        Some(left.kind.signed()?.cmp(&right.kind.signed()?))
    };
    Ok(match op {
        Binary::Eq => ordering == Some(Ordering::Equal),
        Binary::Ne => ordering != Some(Ordering::Equal),
        Binary::Gt => ordering == Some(Ordering::Greater),
        Binary::Ge => matches!(ordering, Some(Ordering::Greater | Ordering::Equal)),
        Binary::Lt => ordering == Some(Ordering::Less),
        Binary::Le => matches!(ordering, Some(Ordering::Less | Ordering::Equal)),
        _ => return Err(SqlEvaluationError::TypeMismatch),
    })
}
