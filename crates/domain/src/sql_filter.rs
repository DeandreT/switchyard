//! Bounded, ephemeral SQL predicates. Neither parser trees nor programs are stored.

use crate::{MessageEnvelope, SessionId};

#[cfg(test)]
mod budget_tests;
mod compiler;
mod evaluator;

pub const MAX_SQL_EXPRESSION_UTF16_UNITS: usize = 1_024;
pub const MAX_SQL_EXPRESSION_BYTES: usize = 4_096;
pub const MAX_SQL_EXPRESSION_TOKENS: usize = 128;
pub const MAX_SQL_PARSER_DEPTH: usize = 32;
pub const MAX_SQL_EXPRESSION_NODES: usize = 128;
pub const MAX_SQL_EXPRESSION_DEPTH: usize = 32;
pub const MAX_SQL_IN_ITEMS: usize = 32;
pub const MAX_SQL_COMPILE_SOURCE_BYTES: usize = 1_048_576;
pub const MAX_SQL_COMPILE_TOKENS: usize = 32_768;
pub const MAX_SQL_COMPILE_NODES: usize = 32_768;
pub const MAX_SQL_LIKE_PATTERN_BYTES: usize = 16_384;
pub const MAX_SQL_REGEX_ENGINE_BYTES: usize = 1_048_576;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlTruth {
    True,
    False,
    Unknown,
}

impl SqlTruth {
    pub const fn is_match(self) -> bool {
        matches!(self, Self::True)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SqlMessageContext<'a> {
    pub message_id: &'a str,
    pub session_id: Option<&'a SessionId>,
    pub envelope: Option<&'a MessageEnvelope>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlProgramMetrics {
    pub source_bytes: usize,
    pub source_utf16_units: usize,
    pub tokens: usize,
    pub nodes: usize,
    pub depth: usize,
}

#[derive(Clone, Debug)]
pub struct SqlProgram {
    nodes: Vec<SqlNode>,
    root: u16,
    in_operands: Vec<u16>,
    metrics: SqlProgramMetrics,
}

impl SqlProgram {
    pub fn compile(expression: &str) -> Result<Self, SqlCompileError> {
        Self::compile_with_budget(expression, &mut SqlCompileBudget::default())
    }

    /// The caller may share this allowance across a complete subscription topology.
    pub fn compile_with_budget(
        expression: &str,
        budget: &mut SqlCompileBudget,
    ) -> Result<Self, SqlCompileError> {
        compiler::compile(expression, budget)
    }

    pub const fn metrics(&self) -> SqlProgramMetrics {
        self.metrics
    }

    pub fn evaluate(
        &self,
        message: SqlMessageContext<'_>,
        budget: &mut SqlEvaluationBudget,
    ) -> Result<SqlTruth, SqlEvaluationError> {
        evaluator::evaluate(self, message, budget)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlCompileLimit {
    SourceUtf16Units,
    SourceBytes,
    PhysicalTokens,
    ParserDepth,
    Nodes,
    ExpressionDepth,
    InItems,
    AggregateSourceBytes,
    AggregateTokens,
    AggregateNodes,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SqlCompileError {
    #[error("invalid SQL predicate syntax")]
    Syntax,
    #[error("unsupported SQL predicate: {feature}")]
    Unsupported { feature: &'static str },
    #[error("SQL compilation exceeds {kind:?} limit {maximum}")]
    Limit {
        kind: SqlCompileLimit,
        maximum: usize,
    },
}

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
    /// Tightens the local ceilings; larger arguments cannot raise them.
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

    fn charge_source(&mut self, bytes: usize) -> Result<(), SqlCompileError> {
        Self::charge(
            &mut self.used.source_bytes,
            self.limits.source_bytes,
            bytes,
            SqlCompileLimit::AggregateSourceBytes,
        )
    }

    fn charge_tokens(&mut self, tokens: usize) -> Result<(), SqlCompileError> {
        Self::charge(
            &mut self.used.tokens,
            self.limits.tokens,
            tokens,
            SqlCompileLimit::AggregateTokens,
        )
    }

    fn charge_node(&mut self) -> Result<(), SqlCompileError> {
        Self::charge(
            &mut self.used.nodes,
            self.limits.nodes,
            1,
            SqlCompileLimit::AggregateNodes,
        )
    }

    fn charge(
        used: &mut usize,
        maximum: usize,
        amount: usize,
        kind: SqlCompileLimit,
    ) -> Result<(), SqlCompileError> {
        let next = used
            .checked_add(amount)
            .filter(|next| *next <= maximum)
            .ok_or(SqlCompileError::Limit { kind, maximum })?;
        *used = next;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlEvaluationLimit {
    WorkUnits,
    ComparisonBytes,
    LikePatternBytes,
    RegexEngineBytes,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SqlEvaluationError {
    #[error("SQL operands have incompatible types")]
    TypeMismatch,
    #[error("SQL operand uses an unsupported message value")]
    UnsupportedValue,
    #[error("SQL integer arithmetic overflow")]
    NumericOverflow,
    #[error("SQL arithmetic divides by zero")]
    DivisionByZero,
    #[error("SQL LIKE escape is invalid")]
    InvalidEscape,
    #[error("SQL property names collide under case-insensitive lookup")]
    AmbiguousProperty,
    #[error("SQL string ordering is unsupported")]
    StringOrderingUnsupported,
    #[error("SQL result is not a Boolean predicate")]
    NonPredicate,
    #[error("SQL evaluation exceeds {kind:?} limit {maximum}")]
    Limit {
        kind: SqlEvaluationLimit,
        maximum: usize,
    },
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
        Self::with_limits(
            crate::MAX_TOPIC_RULE_MATCH_WORK,
            crate::MAX_TOPIC_RULE_COMPARISON_BYTES,
        )
    }
}

impl SqlEvaluationBudget {
    /// Tightens the local ceilings; larger arguments cannot raise them.
    pub const fn with_limits(work: usize, comparison_bytes: usize) -> Self {
        Self {
            used: SqlEvaluationUsage {
                work: 0,
                comparison_bytes: 0,
            },
            limits: SqlEvaluationUsage {
                work: smaller(work, crate::MAX_TOPIC_RULE_MATCH_WORK),
                comparison_bytes: smaller(comparison_bytes, crate::MAX_TOPIC_RULE_COMPARISON_BYTES),
            },
        }
    }

    pub const fn used(&self) -> SqlEvaluationUsage {
        self.used
    }

    fn charge_work(&mut self, amount: usize) -> Result<(), SqlEvaluationError> {
        Self::charge(
            &mut self.used.work,
            self.limits.work,
            amount,
            SqlEvaluationLimit::WorkUnits,
        )
    }

    fn charge_bytes(&mut self, amount: usize) -> Result<(), SqlEvaluationError> {
        Self::charge(
            &mut self.used.comparison_bytes,
            self.limits.comparison_bytes,
            amount,
            SqlEvaluationLimit::ComparisonBytes,
        )
    }

    fn charge(
        used: &mut usize,
        maximum: usize,
        amount: usize,
        kind: SqlEvaluationLimit,
    ) -> Result<(), SqlEvaluationError> {
        let next = used
            .checked_add(amount)
            .filter(|next| *next <= maximum)
            .ok_or(SqlEvaluationError::Limit { kind, maximum })?;
        *used = next;
        Ok(())
    }
}

const fn smaller(left: usize, right: usize) -> usize {
    if left < right { left } else { right }
}

#[derive(Clone, Debug)]
enum SqlLiteral {
    Null,
    Bool(bool),
    Int64(i64),
    DoubleBits(u64),
    String(String),
}

#[derive(Clone, Debug)]
enum SqlProperty {
    User(String),
    System(SqlSystemProperty),
}

#[derive(Clone, Copy, Debug)]
enum SqlSystemProperty {
    CorrelationId,
    MessageId,
    To,
    ReplyTo,
    Subject,
    SessionId,
    ReplyToSessionId,
    ContentType,
}

#[derive(Clone, Copy, Debug)]
enum SqlUnaryOp {
    Not,
    Plus,
    Minus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlBinaryOp {
    And,
    Or,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Clone, Debug)]
enum SqlNode {
    Literal(SqlLiteral),
    Property(SqlProperty),
    Unary {
        op: SqlUnaryOp,
        input: u16,
    },
    Binary {
        op: SqlBinaryOp,
        left: u16,
        right: u16,
    },
    IsNull {
        input: u16,
        negated: bool,
    },
    Exists(SqlProperty),
    In {
        input: u16,
        start: usize,
        len: usize,
        negated: bool,
    },
    Like {
        input: u16,
        pattern: u16,
        escape: Option<u16>,
        negated: bool,
    },
}
