use super::*;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlCompileUsage {
    pub source_bytes: usize,
    pub tokens: usize,
    pub nodes: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct SqlCompileBudget {
    used: SqlCompileUsage,
    limits: SqlCompileUsage,
}

impl Default for SqlCompileBudget {
    fn default() -> Self {
        Self::with_limits(
            MAX_SQL_COMPILE_SOURCE_BYTES,
            MAX_SQL_COMPILE_TOKENS,
            MAX_SQL_COMPILE_NODES,
        )
    }
}

impl SqlCompileBudget {
    /// Larger arguments cannot raise the local ceilings. Charges are not refunded.
    pub const fn with_limits(source_bytes: usize, tokens: usize, nodes: usize) -> Self {
        Self {
            used: SqlCompileUsage {
                source_bytes: 0,
                tokens: 0,
                nodes: 0,
            },
            limits: SqlCompileUsage {
                source_bytes: smaller(source_bytes, MAX_SQL_COMPILE_SOURCE_BYTES),
                tokens: smaller(tokens, MAX_SQL_COMPILE_TOKENS),
                nodes: smaller(nodes, MAX_SQL_COMPILE_NODES),
            },
        }
    }

    pub const fn used(&self) -> SqlCompileUsage {
        self.used
    }

    pub(super) fn source(&mut self, bytes: usize) -> Result<(), SqlCompileError> {
        charge(&mut self.used.source_bytes, self.limits.source_bytes, bytes).map_err(|maximum| {
            SqlCompileError::Limit {
                kind: SqlCompileLimit::AggregateSourceBytes,
                maximum,
            }
        })
    }

    pub(super) fn tokens(&mut self, tokens: usize) -> Result<(), SqlCompileError> {
        charge(&mut self.used.tokens, self.limits.tokens, tokens).map_err(|maximum| {
            SqlCompileError::Limit {
                kind: SqlCompileLimit::AggregateTokens,
                maximum,
            }
        })
    }

    pub(super) fn node(&mut self) -> Result<(), SqlCompileError> {
        charge(&mut self.used.nodes, self.limits.nodes, 1).map_err(|maximum| {
            SqlCompileError::Limit {
                kind: SqlCompileLimit::AggregateNodes,
                maximum,
            }
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlEvaluationUsage {
    pub work: usize,
    pub comparison_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct SqlEvaluationBudget {
    used: SqlEvaluationUsage,
    limits: SqlEvaluationUsage,
}

impl Default for SqlEvaluationBudget {
    fn default() -> Self {
        Self::with_limits(MAX_SQL_EVALUATION_WORK, MAX_SQL_COMPARISON_BYTES)
    }
}

impl SqlEvaluationBudget {
    /// Larger arguments cannot raise the local ceilings. Charges are not refunded.
    pub const fn with_limits(work: usize, comparison_bytes: usize) -> Self {
        Self {
            used: SqlEvaluationUsage {
                work: 0,
                comparison_bytes: 0,
            },
            limits: SqlEvaluationUsage {
                work: smaller(work, MAX_SQL_EVALUATION_WORK),
                comparison_bytes: smaller(comparison_bytes, MAX_SQL_COMPARISON_BYTES),
            },
        }
    }

    pub const fn used(&self) -> SqlEvaluationUsage {
        self.used
    }

    pub(super) fn work(&mut self, amount: usize) -> Result<(), SqlEvaluationError> {
        charge(&mut self.used.work, self.limits.work, amount).map_err(|maximum| {
            SqlEvaluationError::Limit {
                kind: SqlEvaluationLimit::WorkUnits,
                maximum,
            }
        })
    }

    pub(super) fn bytes(&mut self, amount: usize) -> Result<(), SqlEvaluationError> {
        charge(
            &mut self.used.comparison_bytes,
            self.limits.comparison_bytes,
            amount,
        )
        .map_err(|maximum| SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            maximum,
        })
    }
}

fn charge(used: &mut usize, maximum: usize, amount: usize) -> Result<(), usize> {
    let next = used
        .checked_add(amount)
        .filter(|next| *next <= maximum)
        .ok_or(maximum)?;
    *used = next;
    Ok(())
}

const fn smaller(left: usize, right: usize) -> usize {
    if left < right { left } else { right }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_aggregate_ceiling_clamps_and_accepts_its_exact_boundary() {
        let mut compile = SqlCompileBudget::with_limits(usize::MAX, usize::MAX, usize::MAX);
        assert_eq!(
            compile.limits,
            SqlCompileUsage {
                source_bytes: MAX_SQL_COMPILE_SOURCE_BYTES,
                tokens: MAX_SQL_COMPILE_TOKENS,
                nodes: MAX_SQL_COMPILE_NODES,
            }
        );
        compile.source(MAX_SQL_COMPILE_SOURCE_BYTES).unwrap();
        compile.tokens(MAX_SQL_COMPILE_TOKENS).unwrap();
        for _ in 0..MAX_SQL_COMPILE_NODES {
            compile.node().unwrap();
        }
        for (result, kind, maximum) in [
            (
                compile.source(1),
                SqlCompileLimit::AggregateSourceBytes,
                MAX_SQL_COMPILE_SOURCE_BYTES,
            ),
            (
                compile.tokens(1),
                SqlCompileLimit::AggregateTokens,
                MAX_SQL_COMPILE_TOKENS,
            ),
            (
                compile.node(),
                SqlCompileLimit::AggregateNodes,
                MAX_SQL_COMPILE_NODES,
            ),
        ] {
            assert_eq!(result, Err(SqlCompileError::Limit { kind, maximum }));
        }
        assert_eq!(compile.used, compile.limits);

        let mut evaluation = SqlEvaluationBudget::with_limits(usize::MAX, usize::MAX);
        assert_eq!(
            evaluation.limits,
            SqlEvaluationUsage {
                work: MAX_SQL_EVALUATION_WORK,
                comparison_bytes: MAX_SQL_COMPARISON_BYTES,
            }
        );
        evaluation.work(MAX_SQL_EVALUATION_WORK).unwrap();
        evaluation.bytes(MAX_SQL_COMPARISON_BYTES).unwrap();
        assert_eq!(
            evaluation.work(1),
            Err(SqlEvaluationError::Limit {
                kind: SqlEvaluationLimit::WorkUnits,
                maximum: MAX_SQL_EVALUATION_WORK
            })
        );
        assert_eq!(
            evaluation.bytes(1),
            Err(SqlEvaluationError::Limit {
                kind: SqlEvaluationLimit::ComparisonBytes,
                maximum: MAX_SQL_COMPARISON_BYTES
            })
        );
        assert_eq!(evaluation.used, evaluation.limits);
    }

    #[test]
    fn checked_charge_cannot_wrap_and_refused_work_does_not_reset_usage() {
        let mut used = usize::MAX;
        assert_eq!(charge(&mut used, usize::MAX, 1), Err(usize::MAX));
        assert_eq!(used, usize::MAX);
        assert_eq!(charge(&mut used, usize::MAX, 0), Ok(()));
        let mut zero = SqlEvaluationBudget::with_limits(0, 0);
        assert_eq!(
            zero.work(1),
            Err(SqlEvaluationError::Limit {
                kind: SqlEvaluationLimit::WorkUnits,
                maximum: 0
            })
        );
        assert_eq!(
            zero.bytes(1),
            Err(SqlEvaluationError::Limit {
                kind: SqlEvaluationLimit::ComparisonBytes,
                maximum: 0
            })
        );
        assert_eq!(zero.used(), SqlEvaluationUsage::default());
    }
}
