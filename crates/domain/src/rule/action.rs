use std::fmt;

#[cfg(test)]
use crate::MessageEnvelope;
use serde::{Deserialize, Serialize, de};

use crate::{
    MAX_SQL_EXPRESSION_BYTES, MAX_SQL_EXPRESSION_UTF16_UNITS, SqlCompileBudget, SqlCompileError,
    SqlCompileLimit,
};

mod compiler;
mod evaluation;
mod literals;
pub(crate) use evaluation::{CheckedSqlAction, SqlActionError};
#[cfg(test)]
mod literal_tests;
#[cfg(test)]
mod tests;

pub const SQL_ACTION_SEMANTIC_VERSION: u32 = 2;
const MAX_SQL_ACTION_STATEMENTS: usize = 32;

/// An original bounded action source, not a stored parser tree or program.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SqlAction {
    semantic_version: u32,
    expression: String,
}

impl SqlAction {
    pub fn new(expression: impl Into<String>) -> Result<Self, SqlCompileError> {
        Self::with_semantic_version(expression, SQL_ACTION_SEMANTIC_VERSION)
    }

    /// Version 1 is REMOVE-only; version 2 adds the finite local literal SET subset.
    /// Unknown versions are refused before converting the supplied source.
    pub fn with_semantic_version(
        expression: impl Into<String>,
        semantic_version: u32,
    ) -> Result<Self, SqlCompileError> {
        require_version(semantic_version)?;
        let expression = expression.into();
        SqlActionProgram::compile_version(&expression, semantic_version)?;
        Ok(Self {
            semantic_version,
            expression,
        })
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }

    pub const fn semantic_version(&self) -> u32 {
        self.semantic_version
    }

    pub(crate) fn validate_source(&self) -> Result<(), SqlCompileError> {
        require_version(self.semantic_version)?;
        source_limits(&self.expression)
    }
}

impl<'de> Deserialize<'de> for SqlAction {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct StoredSqlAction {
            semantic_version: u32,
            #[serde(deserialize_with = "bounded_source")]
            expression: String,
        }

        let stored = StoredSqlAction::deserialize(deserializer)?;
        let action = Self {
            semantic_version: stored.semantic_version,
            expression: stored.expression,
        };
        action.validate_source().map_err(de::Error::custom)?;
        Ok(action)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SqlActionProgram {
    semantic_version: u32,
    targets: Vec<String>,
    values: Vec<Option<literals::ActionLiteral>>,
}

impl SqlActionProgram {
    #[cfg(test)]
    pub(crate) fn compile(expression: &str) -> Result<Self, SqlCompileError> {
        Self::compile_with_budget(expression, &mut SqlCompileBudget::default())
    }

    #[cfg(test)]
    pub(crate) fn compile_with_budget(
        expression: &str,
        budget: &mut SqlCompileBudget,
    ) -> Result<Self, SqlCompileError> {
        Self::compile_version_with_budget(expression, SQL_ACTION_SEMANTIC_VERSION, budget)
    }

    pub(crate) fn compile_version(
        expression: &str,
        semantic_version: u32,
    ) -> Result<Self, SqlCompileError> {
        Self::compile_version_with_budget(
            expression,
            semantic_version,
            &mut SqlCompileBudget::default(),
        )
    }

    pub(crate) fn compile_version_with_budget(
        expression: &str,
        semantic_version: u32,
        budget: &mut SqlCompileBudget,
    ) -> Result<Self, SqlCompileError> {
        require_version(semantic_version)?;
        compiler::compile(expression, semantic_version, budget)
    }

    pub(crate) fn targets(&self) -> &[String] {
        &self.targets
    }

    /// Removal is case-exact; absent keys and repeated targets are harmless.
    #[cfg(test)]
    pub(crate) fn apply(&self, envelope: &mut crate::MessageEnvelope) {
        debug_assert!(self.values.iter().all(Option::is_none));
        for target in &self.targets {
            envelope.application_properties.remove(target);
        }
    }
}

fn require_version(version: u32) -> Result<(), SqlCompileError> {
    if matches!(version, 1 | 2) {
        Ok(())
    } else {
        Err(SqlCompileError::Unsupported {
            feature: "SQL action semantic version",
        })
    }
}

fn source_limits(source: &str) -> Result<(), SqlCompileError> {
    if source.len() > MAX_SQL_EXPRESSION_BYTES {
        return Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::SourceBytes,
            maximum: MAX_SQL_EXPRESSION_BYTES,
        });
    }
    if source.encode_utf16().count() > MAX_SQL_EXPRESSION_UTF16_UNITS {
        return Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::SourceUtf16Units,
            maximum: MAX_SQL_EXPRESSION_UTF16_UNITS,
        });
    }
    Ok(())
}

fn bounded_source<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    struct SourceVisitor;

    impl<'de> de::Visitor<'de> for SourceVisitor {
        type Value = String;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded SQL action expression")
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
            source_limits(value).map_err(E::custom)?;
            Ok(value.to_owned())
        }

        fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
            self.visit_str(value)
        }

        fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
            source_limits(&value).map_err(E::custom)?;
            Ok(value)
        }
    }

    deserializer.deserialize_str(SourceVisitor)
}
