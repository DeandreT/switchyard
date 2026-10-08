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

pub(super) fn unary(input: Number, negative: bool) -> Result<Number, SqlEvaluationError> {
    use Kind::*;
    let kind = match input.kind {
        I8(value) => I32(if negative {
            -i32::from(value)
        } else {
            i32::from(value)
        }),
        U8(value) => I32(if negative {
            -i32::from(value)
        } else {
            i32::from(value)
        }),
        I16(value) => I32(if negative {
            -i32::from(value)
        } else {
            i32::from(value)
        }),
        U16(value) => I32(if negative {
            -i32::from(value)
        } else {
            i32::from(value)
        }),
        I32(value) => I32(if negative {
            value
                .checked_neg()
                .ok_or(SqlEvaluationError::ArithmeticOverflow)?
        } else {
            value
        }),
        U32(value) if negative => I64(-i64::from(value)),
        U32(value) => U32(value),
        I64(value) => I64(if negative {
            value
                .checked_neg()
                .ok_or(SqlEvaluationError::ArithmeticOverflow)?
        } else {
            value
        }),
        U64(_) if negative => return Err(SqlEvaluationError::TypeMismatch),
        U64(value) => U64(value),
        F32(value) => F32(if negative { -value } else { value }),
        F64(value) => F64(if negative { -value } else { value }),
    };
    Ok(Number {
        kind,
        constant: false,
    })
}

pub(super) fn arithmetic(
    op: Binary,
    left: Number,
    right: Number,
) -> Result<Number, SqlEvaluationError> {
    use Kind::*;

    macro_rules! integral {
        ($left:expr, $right:expr, $kind:path) => {{
            let left = $left;
            let right = $right;
            if matches!(op, Binary::Div | Binary::Rem) && right == 0 {
                return Err(SqlEvaluationError::DivideByZero);
            }
            let result = match op {
                Binary::Add => left.checked_add(right),
                Binary::Sub => left.checked_sub(right),
                Binary::Mul => left.checked_mul(right),
                Binary::Div => left.checked_div(right),
                // MIN % -1 is deliberately refused by this local checked profile.
                Binary::Rem => left.checked_rem(right),
                _ => return Err(SqlEvaluationError::TypeMismatch),
            };
            $kind(result.ok_or(SqlEvaluationError::ArithmeticOverflow)?)
        }};
    }

    let kind = if matches!(left.kind, F64(_)) || matches!(right.kind, F64(_)) {
        let left = left.kind.double();
        let right = right.kind.double();
        F64(match op {
            Binary::Add => left + right,
            Binary::Sub => left - right,
            Binary::Mul => left * right,
            Binary::Div => left / right,
            Binary::Rem => left % right,
            _ => return Err(SqlEvaluationError::TypeMismatch),
        })
    } else if matches!(left.kind, F32(_)) || matches!(right.kind, F32(_)) {
        let left = left.kind.single()?;
        let right = right.kind.single()?;
        F32(match op {
            Binary::Add => left + right,
            Binary::Sub => left - right,
            Binary::Mul => left * right,
            Binary::Div => left / right,
            Binary::Rem => left % right,
            _ => return Err(SqlEvaluationError::TypeMismatch),
        })
    } else if matches!(left.kind, U64(_)) || matches!(right.kind, U64(_)) {
        for value in [left, right] {
            if value.kind.is_signed() && (!value.constant || value.kind.signed()? < 0) {
                return Err(SqlEvaluationError::TypeMismatch);
            }
        }
        integral!(left.kind.unsigned()?, right.kind.unsigned()?, U64)
    } else if matches!(left.kind, I64(_)) || matches!(right.kind, I64(_)) {
        integral!(left.kind.signed()?, right.kind.signed()?, I64)
    } else if matches!(left.kind, U32(_)) || matches!(right.kind, U32(_)) {
        if left.kind.is_signed() || right.kind.is_signed() {
            integral!(left.kind.signed()?, right.kind.signed()?, I64)
        } else {
            let left = u32::try_from(left.kind.unsigned()?)
                .map_err(|_| SqlEvaluationError::TypeMismatch)?;
            let right = u32::try_from(right.kind.unsigned()?)
                .map_err(|_| SqlEvaluationError::TypeMismatch)?;
            integral!(left, right, U32)
        }
    } else {
        let left =
            i32::try_from(left.kind.signed()?).map_err(|_| SqlEvaluationError::TypeMismatch)?;
        let right =
            i32::try_from(right.kind.signed()?).map_err(|_| SqlEvaluationError::TypeMismatch)?;
        integral!(left, right, I32)
    };
    Ok(Number {
        kind,
        constant: false,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn number(kind: Kind, constant: bool) -> Number {
        Number { kind, constant }
    }

    fn kind_index(kind: Kind) -> i8 {
        match kind {
            Kind::I8(_) => 0,
            Kind::U8(_) => 1,
            Kind::I16(_) => 2,
            Kind::U16(_) => 3,
            Kind::I32(_) => 4,
            Kind::U32(_) => 5,
            Kind::I64(_) => 6,
            Kind::U64(_) => 7,
            Kind::F32(_) => 8,
            Kind::F64(_) => 9,
        }
    }

    #[test]
    fn binary_arithmetic_promotion_matrix_is_explicit_for_all_ten_widths() {
        let kinds = [
            Kind::I8(2),
            Kind::U8(2),
            Kind::I16(2),
            Kind::U16(2),
            Kind::I32(2),
            Kind::U32(2),
            Kind::I64(2),
            Kind::U64(2),
            Kind::F32(2.0),
            Kind::F64(2.0),
        ];
        // Rows and columns follow `kinds`; -1 denotes the signed/ulong refusal.
        let expected: [[i8; 10]; 10] = [
            [4, 4, 4, 4, 4, 6, 6, -1, 8, 9],
            [4, 4, 4, 4, 4, 5, 6, 7, 8, 9],
            [4, 4, 4, 4, 4, 6, 6, -1, 8, 9],
            [4, 4, 4, 4, 4, 5, 6, 7, 8, 9],
            [4, 4, 4, 4, 4, 6, 6, -1, 8, 9],
            [6, 5, 6, 5, 6, 5, 6, 7, 8, 9],
            [6, 6, 6, 6, 6, 6, 6, -1, 8, 9],
            [-1, 7, -1, 7, -1, 7, -1, 7, 8, 9],
            [8, 8, 8, 8, 8, 8, 8, 8, 8, 9],
            [9, 9, 9, 9, 9, 9, 9, 9, 9, 9],
        ];
        for (op, value) in [
            (Binary::Add, 4.0),
            (Binary::Sub, 0.0),
            (Binary::Mul, 4.0),
            (Binary::Div, 1.0),
            (Binary::Rem, 0.0),
        ] {
            for (row, left) in kinds.iter().copied().enumerate() {
                for (column, right) in kinds.iter().copied().enumerate() {
                    let result = arithmetic(op, number(left, false), number(right, false));
                    if expected[row][column] < 0 {
                        assert_eq!(
                            result.err(),
                            Some(SqlEvaluationError::TypeMismatch),
                            "{op:?} at ({row}, {column})"
                        );
                    } else {
                        let result = result.expect("promoted arithmetic");
                        assert_eq!(
                            kind_index(result.kind),
                            expected[row][column],
                            "{op:?} at ({row}, {column})"
                        );
                        assert_eq!(result.kind.double(), value, "{op:?} at ({row}, {column})");
                        assert!(!result.constant);
                    }
                }
            }
        }
    }

    #[test]
    fn arithmetic_literal_origin_is_not_retained_by_results() {
        for signed in [Kind::I8(1), Kind::I16(1), Kind::I32(1), Kind::I64(1)] {
            for (left, right) in [
                (number(signed, true), number(Kind::U64(2), false)),
                (number(Kind::U64(2), false), number(signed, true)),
            ] {
                let result = arithmetic(Binary::Add, left, right).expect("nonnegative literal");
                assert!(matches!(result.kind, Kind::U64(3)));
                assert!(!result.constant);
            }
            assert_eq!(
                arithmetic(
                    Binary::Add,
                    number(signed, false),
                    number(Kind::U64(2), false)
                )
                .err(),
                Some(SqlEvaluationError::TypeMismatch)
            );
            let positive = unary(number(signed, true), false).expect("evaluated unary plus");
            assert!(!positive.constant);
            assert_eq!(
                arithmetic(Binary::Add, positive, number(Kind::U64(2), false)).err(),
                Some(SqlEvaluationError::TypeMismatch)
            );
        }
        for signed in [Kind::I8(-1), Kind::I16(-1), Kind::I32(-1), Kind::I64(-1)] {
            assert_eq!(
                arithmetic(
                    Binary::Add,
                    number(signed, true),
                    number(Kind::U64(2), false)
                )
                .err(),
                Some(SqlEvaluationError::TypeMismatch)
            );
        }
        for op in [
            Binary::Add,
            Binary::Sub,
            Binary::Mul,
            Binary::Div,
            Binary::Rem,
        ] {
            let evaluated = arithmetic(op, number(Kind::I64(1), true), number(Kind::I64(1), true))
                .expect("literal arithmetic");
            assert!(!evaluated.constant);
            assert_eq!(
                arithmetic(Binary::Add, number(Kind::U64(2), false), evaluated).err(),
                Some(SqlEvaluationError::TypeMismatch)
            );
            assert_eq!(
                compare(Binary::Eq, number(Kind::U64(2), false), evaluated),
                Err(SqlEvaluationError::TypeMismatch)
            );
        }
    }

    #[test]
    fn unary_arithmetic_promotions_overflow_and_unsigned_refusal_are_exact() {
        let kinds = [
            Kind::I8(2),
            Kind::U8(2),
            Kind::I16(2),
            Kind::U16(2),
            Kind::I32(2),
            Kind::U32(2),
            Kind::I64(2),
            Kind::U64(2),
            Kind::F32(2.0),
            Kind::F64(2.0),
        ];
        let positive = [4, 4, 4, 4, 4, 5, 6, 7, 8, 9];
        let negative = [4, 4, 4, 4, 4, 6, 6, -1, 8, 9];
        for (index, kind) in kinds.iter().copied().enumerate() {
            let result = unary(number(kind, true), false).expect("unary plus");
            assert_eq!(kind_index(result.kind), positive[index]);
            assert_eq!(result.kind.double(), 2.0);
            assert!(!result.constant);
            let result = unary(number(kind, true), true);
            if negative[index] < 0 {
                assert_eq!(result.err(), Some(SqlEvaluationError::TypeMismatch));
            } else {
                let result = result.expect("unary minus");
                assert_eq!(kind_index(result.kind), negative[index]);
                assert_eq!(result.kind.double(), -2.0);
                assert!(!result.constant);
            }
        }
        for (kind, expected) in [
            (Kind::I8(i8::MIN), 128.0),
            (Kind::I16(i16::MIN), 32768.0),
            (Kind::U16(u16::MAX), -65535.0),
            (Kind::U32(u32::MAX), -4294967295.0),
        ] {
            assert_eq!(
                unary(number(kind, false), true)
                    .expect("widened negation")
                    .kind
                    .double(),
                expected
            );
        }
        for kind in [Kind::I32(i32::MIN), Kind::I64(i64::MIN)] {
            assert_eq!(
                unary(number(kind, false), true).err(),
                Some(SqlEvaluationError::ArithmeticOverflow)
            );
        }
    }

    #[test]
    fn checked_integer_arithmetic_zero_overflow_and_remainder_are_exact() {
        for (op, left, right) in [
            (Binary::Add, Kind::I32(i32::MAX), Kind::I32(1)),
            (Binary::Sub, Kind::I32(i32::MIN), Kind::I32(1)),
            (Binary::Mul, Kind::I32(i32::MAX), Kind::I32(2)),
            (Binary::Div, Kind::I32(i32::MIN), Kind::I32(-1)),
            (Binary::Rem, Kind::I32(i32::MIN), Kind::I32(-1)),
            (Binary::Add, Kind::I64(i64::MAX), Kind::I64(1)),
            (Binary::Sub, Kind::I64(i64::MIN), Kind::I64(1)),
            (Binary::Mul, Kind::I64(i64::MAX), Kind::I64(2)),
            (Binary::Div, Kind::I64(i64::MIN), Kind::I64(-1)),
            (Binary::Rem, Kind::I64(i64::MIN), Kind::I64(-1)),
            (Binary::Add, Kind::U32(u32::MAX), Kind::U32(1)),
            (Binary::Sub, Kind::U32(0), Kind::U32(1)),
            (Binary::Mul, Kind::U32(u32::MAX), Kind::U32(2)),
            (Binary::Add, Kind::U64(u64::MAX), Kind::U64(1)),
            (Binary::Sub, Kind::U64(0), Kind::U64(1)),
            (Binary::Mul, Kind::U64(u64::MAX), Kind::U64(2)),
        ] {
            assert_eq!(
                arithmetic(op, number(left, false), number(right, false)).err(),
                Some(SqlEvaluationError::ArithmeticOverflow),
                "{op:?}, {left:?}, {right:?}"
            );
        }
        for (left, zero) in [
            (Kind::I32(1), Kind::I32(0)),
            (Kind::U32(1), Kind::U32(0)),
            (Kind::I64(1), Kind::I64(0)),
            (Kind::U64(1), Kind::U64(0)),
        ] {
            for op in [Binary::Div, Binary::Rem] {
                assert_eq!(
                    arithmetic(op, number(left, false), number(zero, false)).err(),
                    Some(SqlEvaluationError::DivideByZero)
                );
            }
        }
        for (left, right, quotient, remainder) in [
            (-7, 3, -2, -1),
            (7, -3, -2, 1),
            (-7, -3, 2, -1),
            (7, 3, 2, 1),
        ] {
            for (left, right) in [
                (Kind::I32(left), Kind::I32(right)),
                (Kind::I64(i64::from(left)), Kind::I64(i64::from(right))),
            ] {
                for (op, expected) in [(Binary::Div, quotient), (Binary::Rem, remainder)] {
                    let result = arithmetic(op, number(left, false), number(right, false))
                        .expect("truncating integer arithmetic");
                    assert_eq!(result.kind.double(), f64::from(expected));
                    assert!(!result.constant);
                }
            }
        }
        for (left, right, expected) in [
            (Kind::U32(u32::MAX), Kind::I32(1), 4294967296.0),
            (Kind::I32(1), Kind::U32(u32::MAX), 4294967296.0),
            (Kind::U16(u16::MAX), Kind::U16(u16::MAX), 131070.0),
        ] {
            let result = arithmetic(Binary::Add, number(left, false), number(right, false))
                .expect("addition after widening");
            assert_eq!(result.kind.double(), expected);
        }
    }

    #[test]
    fn floating_arithmetic_width_rounding_and_ieee_results_are_exact() {
        let single = arithmetic(
            Binary::Add,
            number(Kind::F32(16777216.0), false),
            number(Kind::I64(1), true),
        )
        .expect("single-width operation");
        assert!(
            matches!(single.kind, Kind::F32(value) if value.to_bits() == 16777216.0_f32.to_bits())
        );
        let double = arithmetic(
            Binary::Add,
            number(Kind::F32(16777216.0), false),
            number(Kind::F64(1.0), true),
        )
        .expect("double-width operation");
        assert!(
            matches!(double.kind, Kind::F64(value) if value.to_bits() == 16777217.0_f64.to_bits())
        );

        for (op, left, right, expected) in [
            (Binary::Add, -0.0, -0.0, -0.0),
            (Binary::Sub, -0.0, 0.0, -0.0),
            (Binary::Mul, -0.0, 1.0, -0.0),
            (Binary::Div, -0.0, 1.0, -0.0),
            (Binary::Rem, -0.0, 1.0, -0.0),
            (Binary::Rem, 5.0, 3.0, 2.0),
            (Binary::Rem, -5.0, 3.0, -2.0),
            (Binary::Rem, 5.0, f64::INFINITY, 5.0),
            (Binary::Div, 1.0, 0.0, f64::INFINITY),
            (Binary::Div, 1.0, -0.0, f64::NEG_INFINITY),
        ] {
            let single = arithmetic(
                op,
                number(Kind::F32(left as f32), true),
                number(Kind::F32(right as f32), true),
            )
            .expect("single IEEE operation");
            assert!(
                matches!(single.kind, Kind::F32(value)
                if value.to_bits() == (expected as f32).to_bits()),
                "{op:?}, {left}, {right}"
            );
            assert!(!single.constant);
            let double = arithmetic(
                op,
                number(Kind::F64(left), true),
                number(Kind::F64(right), true),
            )
            .expect("double IEEE operation");
            assert!(
                matches!(double.kind, Kind::F64(value)
                if value.to_bits() == expected.to_bits()),
                "{op:?}, {left}, {right}"
            );
            assert!(!double.constant);
        }
        for (op, left, right) in [
            (Binary::Div, 0.0, 0.0),
            (Binary::Rem, 1.0, 0.0),
            (Binary::Rem, f64::INFINITY, 2.0),
            (Binary::Add, f64::NAN, 1.0),
            (Binary::Mul, f64::INFINITY, 0.0),
        ] {
            let single = arithmetic(
                op,
                number(Kind::F32(left as f32), false),
                number(Kind::F32(right as f32), false),
            )
            .expect("single NaN result");
            assert!(matches!(single.kind, Kind::F32(value) if value.is_nan()));
            let double = arithmetic(
                op,
                number(Kind::F64(left), false),
                number(Kind::F64(right), false),
            )
            .expect("double NaN result");
            assert!(matches!(double.kind, Kind::F64(value) if value.is_nan()));
        }
        assert!(
            matches!(arithmetic(Binary::Mul, number(Kind::F32(f32::MAX), false),
            number(Kind::F32(2.0), false)).expect("single infinity").kind,
            Kind::F32(value) if value == f32::INFINITY)
        );
        assert!(
            matches!(arithmetic(Binary::Mul, number(Kind::F64(f64::MAX), false),
            number(Kind::F64(2.0), false)).expect("double infinity").kind,
            Kind::F64(value) if value == f64::INFINITY)
        );
        for (negative, expected) in [(false, -0.0_f64), (true, 0.0_f64)] {
            let single = unary(number(Kind::F32(-0.0), true), negative).expect("single unary zero");
            assert!(matches!(single.kind, Kind::F32(value)
                if value.to_bits() == (expected as f32).to_bits()));
            let double = unary(number(Kind::F64(-0.0), true), negative).expect("double unary zero");
            assert!(
                matches!(double.kind, Kind::F64(value) if value.to_bits() == expected.to_bits())
            );
        }
    }
}
