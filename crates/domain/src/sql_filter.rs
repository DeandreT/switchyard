//! Ephemeral, bounded SQL predicates over borrowed typed properties.
//!
//! Programs are not durable values. Only `True` matches; null and missing user
//! values produce `Unknown`. Missing supported system inputs are errors.
//! User names compare ASCII-insensitively; non-ASCII spelling stays distinct.
//! Referenced unsupported values are errors, including EXISTS and IS NULL.

mod budget;
mod compiler;
mod evaluator;
mod numeric;
mod pattern;

pub use budget::{SqlCompileBudget, SqlCompileUsage, SqlEvaluationBudget, SqlEvaluationUsage};

pub const MAX_SQL_EXPRESSION_BYTES: usize = 4_096;
pub const MAX_SQL_EXPRESSION_UTF16_UNITS: usize = 1_024;
pub const MAX_SQL_EXPRESSION_TOKENS: usize = 128;
pub const MAX_SQL_PARSER_DEPTH: usize = 32;
pub const MAX_SQL_EXPRESSION_NODES: usize = 128;
pub const MAX_SQL_EXPRESSION_DEPTH: usize = 32;
pub const MAX_SQL_COMPILE_SOURCE_BYTES: usize = 1_048_576;
pub const MAX_SQL_COMPILE_TOKENS: usize = 32_768;
pub const MAX_SQL_COMPILE_NODES: usize = 32_768;
pub const MAX_SQL_EVALUATION_WORK: usize = 1_048_576;
pub const MAX_SQL_COMPARISON_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_SQL_IN_OPERANDS: usize = 32;
pub const MAX_SQL_LIKE_PATTERN_BYTES: usize = 16 * 1024;
pub const MAX_SQL_REGEX_BYTES: usize = 1024 * 1024;

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

/// Values preserve numeric widths. Unsupported retained types must not become null.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SqlValue<'a> {
    Null,
    Bool(bool),
    Byte(i8),
    Ubyte(u8),
    Short(i16),
    Ushort(u16),
    Int(i32),
    Uint(u32),
    Long(i64),
    Ulong(u64),
    Float(f32),
    Double(f64),
    String(&'a str),
    Unsupported,
}

#[derive(Clone, Copy, Debug)]
pub struct SqlProperty<'a> {
    pub name: &'a str,
    pub value: SqlValue<'a>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlSystemProperty {
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
pub struct SqlSystemValue<'a> {
    pub property: SqlSystemProperty,
    pub value: SqlValue<'a>,
}

/// Adapters supply explicit null entries for known nullable system properties.
#[derive(Clone, Copy, Debug, Default)]
pub struct SqlMessageContext<'a> {
    pub application_properties: &'a [SqlProperty<'a>],
    pub system_properties: &'a [SqlSystemValue<'a>],
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
    nodes: Vec<Node>,
    root: u16,
    metrics: SqlProgramMetrics,
}

impl SqlProgram {
    pub fn compile(expression: &str) -> Result<Self, SqlCompileError> {
        Self::compile_with_budget(expression, &mut SqlCompileBudget::default())
    }

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
    SourceBytes,
    SourceUtf16Units,
    PhysicalTokens,
    ParserDepth,
    Nodes,
    ExpressionDepth,
    AggregateSourceBytes,
    AggregateTokens,
    AggregateNodes,
    InOperands,
    LikePatternBytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SqlCompileError {
    #[error("invalid SQL predicate syntax")]
    Syntax,
    #[error("unsupported SQL predicate: {feature}")]
    Unsupported { feature: &'static str },
    #[error("SQL LIKE escape must contain exactly one Unicode scalar")]
    InvalidLikeEscape,
    #[error("SQL LIKE pattern ends with an unpaired escape")]
    InvalidLikePattern,
    #[error("SQL compilation exceeds {kind:?} limit {maximum}")]
    Limit {
        kind: SqlCompileLimit,
        maximum: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlEvaluationLimit {
    WorkUnits,
    ComparisonBytes,
    LikePatternBytes,
    RegexBytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SqlEvaluationError {
    #[error("SQL operands have incompatible types")]
    TypeMismatch,
    #[error("SQL operand uses an unsupported input value")]
    UnsupportedValue,
    #[error("SQL property lookup is ambiguous")]
    AmbiguousProperty,
    #[error("SQL system property input is missing")]
    MissingSystemProperty,
    #[error("SQL string ordering is unsupported")]
    StringOrderingUnsupported,
    #[error("SQL result is not a Boolean predicate")]
    NonPredicate,
    #[error("SQL LIKE escape must contain exactly one Unicode scalar")]
    InvalidLikeEscape,
    #[error("SQL LIKE pattern is malformed")]
    InvalidLikePattern,
    #[error("SQL evaluation exceeds {kind:?} limit {maximum}")]
    Limit {
        kind: SqlEvaluationLimit,
        maximum: usize,
    },
}

#[derive(Clone, Debug)]
enum Literal {
    Null,
    Bool(bool),
    Integer(i64),
    Double(f64),
    String(String),
}

#[derive(Clone, Debug)]
enum Property {
    User(String),
    System(SqlSystemProperty),
}

#[derive(Clone, Copy, Debug)]
enum Binary {
    And,
    Or,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Clone, Debug)]
enum Node {
    Literal(Literal),
    Property(Property),
    Not(u16),
    Binary {
        op: Binary,
        left: u16,
        right: u16,
    },
    IsNull {
        input: u16,
        negated: bool,
    },
    Exists(Property),
    InList {
        input: u16,
        operands: [u16; MAX_SQL_IN_OPERANDS],
        len: u8,
        negated: bool,
    },
    Like {
        input: u16,
        pattern: u16,
        escape: Option<u16>,
        negated: bool,
    },
}
