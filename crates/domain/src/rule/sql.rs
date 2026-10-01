use std::fmt;

use serde::{Deserialize, Serialize, de};

use crate::{
    MAX_SQL_EXPRESSION_BYTES, MAX_SQL_EXPRESSION_UTF16_UNITS, SqlCompileError, SqlCompileLimit,
    SqlProgram,
};

pub const SQL_FILTER_SEMANTIC_VERSION: u32 = 1;

/// An original SQL expression, not a serialized parser tree or executable arena.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SqlFilter {
    semantic_version: u32,
    expression: String,
}

impl SqlFilter {
    pub fn new(expression: impl Into<String>) -> Result<Self, SqlCompileError> {
        let expression = expression.into();
        SqlProgram::compile(&expression)?;
        Ok(Self {
            semantic_version: SQL_FILTER_SEMANTIC_VERSION,
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
        if self.semantic_version != SQL_FILTER_SEMANTIC_VERSION {
            return Err(SqlCompileError::Unsupported {
                feature: "SQL filter semantic version",
            });
        }
        source_limits(&self.expression)
    }
}

impl<'de> Deserialize<'de> for SqlFilter {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct StoredSqlFilter {
            semantic_version: u32,
            #[serde(deserialize_with = "bounded_source")]
            expression: String,
        }

        let stored = StoredSqlFilter::deserialize(deserializer)?;
        let filter = Self {
            semantic_version: stored.semantic_version,
            expression: stored.expression,
        };
        filter.validate_source().map_err(de::Error::custom)?;
        Ok(filter)
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
            formatter.write_str("a bounded SQL expression")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization_contains_only_version_and_exact_expression() -> Result<(), crate::CodecError> {
        let filter = SqlFilter::new("  colour = 'red'  ").expect("supported predicate");
        let expected = crate::codec::encode(&(1_u32, "  colour = 'red'  "))?;
        assert_eq!(crate::codec::encode(&filter)?, expected);
        assert_eq!(crate::codec::decode::<SqlFilter>(&expected)?, filter);
        Ok(())
    }

    #[test]
    fn decode_does_not_compile_but_bounds_source_and_version() -> Result<(), crate::CodecError> {
        let syntax = crate::codec::encode(&(1_u32, "broken ="))?;
        let decoded = crate::codec::decode::<SqlFilter>(&syntax)?;
        assert_eq!(decoded.expression(), "broken =");
        assert!(SqlProgram::compile(decoded.expression()).is_err());
        for (version, expression) in [
            (2, String::from("TRUE")),
            (1, "x".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS + 1)),
            (1, "x".repeat(MAX_SQL_EXPRESSION_BYTES + 1)),
        ] {
            let bytes = crate::codec::encode(&(version, expression))?;
            assert_eq!(
                crate::codec::decode::<SqlFilter>(&bytes),
                Err(crate::CodecError::Decode)
            );
        }
        Ok(())
    }

    #[test]
    fn sql_rule_cannot_be_relabelled_as_a_pre_sql_envelope() -> Result<(), crate::CodecError> {
        let rule = crate::RuleDefinition {
            name: crate::RuleName::new("sql").expect("valid rule name"),
            filter: crate::RuleFilter::Sql(SqlFilter::new("TRUE").expect("supported SQL")),
            created_at: crate::Timestamp::UNIX_EPOCH,
        };
        let mut bytes = crate::codec::encode(&rule)?;
        assert_eq!(crate::RuleDefinition::decode(&bytes)?, rule);
        for version in crate::codec::VALUE_FORMAT_V1..crate::codec::VALUE_FORMAT_V10 {
            bytes[0] = version;
            assert_eq!(
                crate::RuleDefinition::decode(&bytes),
                Err(crate::CodecError::Decode)
            );
        }
        Ok(())
    }
}
