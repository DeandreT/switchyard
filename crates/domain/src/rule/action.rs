//! Bounded, ephemeral SQL actions. These programs and patches are not durable
//! rule intent and do not activate topic fanout.

mod compiler;
mod evaluator;
mod lexer;

use message_content::Value;

use crate::{
    PROPERTY_PATCH_HEADROOM_BYTES, SqlCompileBudget, SqlCompileError, SqlEvaluationBudget,
    SqlEvaluationError, SqlMessageContext,
};

pub const MAX_SQL_ACTION_STATEMENTS: usize = 32;
pub const MAX_SQL_ACTION_PATCH_PROPERTIES: usize = MAX_SQL_ACTION_STATEMENTS;
pub const MAX_SQL_ACTION_PATCH_BYTES: usize = PROPERTY_PATCH_HEADROOM_BYTES;

/// Compiled, constructor-checked intent. No serde implementation can bypass
/// compilation, and no source/program version is implied by this ephemeral API.
#[derive(Clone, Debug)]
pub struct SqlActionProgram {
    statements: Vec<Statement>,
    metrics: SqlActionMetrics,
}

impl SqlActionProgram {
    pub fn compile(source: &str) -> Result<Self, SqlActionCompileError> {
        Self::compile_with_budget(source, &mut SqlCompileBudget::default())
    }

    /// Shares the existing SQL source/token/node budget across programs. Failed
    /// compilations do not refund charges. The grammar is separate from filters.
    pub fn compile_with_budget(
        source: &str,
        budget: &mut SqlCompileBudget,
    ) -> Result<Self, SqlActionCompileError> {
        compiler::compile(source, budget)
    }

    pub const fn metrics(&self) -> SqlActionMetrics {
        self.metrics
    }

    /// Builds one isolated patch, never modifying the borrowed message. System
    /// inputs are ignored. On any error no partial patch escapes; budget charges
    /// remain consumed. Each call starts from the supplied original properties.
    pub fn evaluate(
        &self,
        message: SqlMessageContext<'_>,
        budget: &mut SqlEvaluationBudget,
    ) -> Result<SqlActionPatch, SqlActionError> {
        self.evaluate_with_limits(message, budget, SqlActionLimits::default())
    }

    pub fn evaluate_with_limits(
        &self,
        message: SqlMessageContext<'_>,
        budget: &mut SqlEvaluationBudget,
        limits: SqlActionLimits,
    ) -> Result<SqlActionPatch, SqlActionError> {
        evaluator::evaluate(self, message, budget, limits)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlActionMetrics {
    pub source_bytes: usize,
    pub source_utf16_units: usize,
    /// Physical tokens, including whitespace and comments, just as for filters.
    pub tokens: usize,
    pub statements: usize,
}

/// Local output ceilings. Larger arguments cannot raise the built-in maxima.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqlActionLimits {
    properties: usize,
    bytes: usize,
}

impl SqlActionLimits {
    pub const fn with_limits(properties: usize, bytes: usize) -> Self {
        Self {
            properties: if properties < MAX_SQL_ACTION_PATCH_PROPERTIES {
                properties
            } else {
                MAX_SQL_ACTION_PATCH_PROPERTIES
            },
            bytes: if bytes < MAX_SQL_ACTION_PATCH_BYTES {
                bytes
            } else {
                MAX_SQL_ACTION_PATCH_BYTES
            },
        }
    }
}

impl Default for SqlActionLimits {
    fn default() -> Self {
        Self::with_limits(MAX_SQL_ACTION_PATCH_PROPERTIES, MAX_SQL_ACTION_PATCH_BYTES)
    }
}

/// Typed copy-local changes in first-target order, with one entry per exact
/// name. Values are the existing canonical content types, not another encoding.
/// A deletion is distinct from an AMQP null. This is not a persisted overlay;
/// consumers must enforce the effective copy's size before applying it.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlActionPatch {
    entries: Vec<SqlActionProperty>,
    charged_bytes: usize,
}

impl SqlActionPatch {
    pub fn entries(&self) -> &[SqlActionProperty] {
        &self.entries
    }

    pub fn into_entries(self) -> Vec<SqlActionProperty> {
        self.entries
    }

    /// Conservative patch charge: five framing bytes plus UTF-8 name bytes,
    /// then the widest supported scalar representation (one byte for removal).
    pub const fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SqlActionProperty {
    Remove { name: String },
    Set { name: String, value: Value },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SqlActionCompileError {
    #[error(transparent)]
    Sql(#[from] SqlCompileError),
    #[error("SQL action exceeds {maximum} statements")]
    Statements { maximum: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SqlActionError {
    #[error(transparent)]
    Evaluation(#[from] SqlEvaluationError),
    #[error("SQL action assignment has an incompatible target type")]
    TypeMismatch,
    #[error("SQL action assignment targets an unsupported property type")]
    UnsupportedTargetType,
    #[error("SQL action integer assignment overflows its target type")]
    NumericOverflow,
    #[error("SQL action patch exceeds {maximum} properties")]
    PatchProperties { maximum: usize },
    #[error("SQL action patch exceeds {maximum} charged bytes")]
    PatchBytes { maximum: usize },
}

#[derive(Clone, Debug)]
struct Statement {
    target: String,
    literal: Option<Literal>,
}

#[derive(Clone, Debug)]
enum Literal {
    String(String),
    Bool(bool),
    Integer(i64),
}
