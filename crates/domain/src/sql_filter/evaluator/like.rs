use regex::RegexBuilder;

use super::*;

pub(super) fn matches(
    input: Value<'_>,
    pattern: Value<'_>,
    escape: Option<Value<'_>>,
    negated: bool,
    budget: &mut SqlEvaluationBudget,
) -> Result<SqlTruth, SqlEvaluationError> {
    if input.is_unknown() || pattern.is_unknown() || escape.is_some_and(Value::is_unknown) {
        return Ok(SqlTruth::Unknown);
    }
    if matches!(input, Value::Unsupported)
        || matches!(pattern, Value::Unsupported)
        || matches!(escape, Some(Value::Unsupported))
    {
        return Err(SqlEvaluationError::UnsupportedValue);
    }
    let (Value::String(input), Value::String(pattern)) = (input, pattern) else {
        return Err(SqlEvaluationError::TypeMismatch);
    };
    let escape = match escape {
        None => None,
        Some(Value::String(value)) => {
            budget.charge_work(value.len())?;
            budget.charge_bytes(value.len())?;
            let mut chars = value.chars();
            let value = chars.next().ok_or(SqlEvaluationError::InvalidEscape)?;
            if chars.next().is_some() {
                return Err(SqlEvaluationError::InvalidEscape);
            }
            Some(value)
        }
        _ => return Err(SqlEvaluationError::TypeMismatch),
    };
    // Charge text traversal before counting generated syntax. Every literal is
    // escaped by the existing regex engine; counting uses only tiny scalars.
    let source_bytes = input.len().saturating_add(pattern.len());
    budget.charge_work(source_bytes)?;
    budget.charge_bytes(source_bytes)?;
    let generated = pattern_bytes(pattern, escape)?;
    let product = generated
        .checked_add(1)
        .and_then(|size| {
            input
                .len()
                .checked_add(1)
                .and_then(|text| size.checked_mul(text))
        })
        .ok_or(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            maximum: budget.limits.work,
        })?;
    budget.charge_work(product)?;
    budget.charge_bytes(product)?;

    let mut expression = String::with_capacity(generated);
    expression.push_str("\\A");
    walk_pattern(pattern, escape, |part| {
        match part {
            Part::Many => expression.push_str(".*"),
            Part::One => expression.push('.'),
            Part::Literal(value) => {
                let mut bytes = [0; 4];
                expression.push_str(&regex::escape(value.encode_utf8(&mut bytes)));
            }
        }
        Ok(())
    })?;
    expression.push_str("\\z");
    let compiled = RegexBuilder::new(&expression)
        .case_insensitive(false)
        .unicode(true)
        .dot_matches_new_line(true)
        .nest_limit(MAX_SQL_EXPRESSION_DEPTH as u32)
        .size_limit(MAX_SQL_REGEX_ENGINE_BYTES)
        .dfa_size_limit(MAX_SQL_REGEX_ENGINE_BYTES)
        .build()
        .map_err(|_| SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::RegexEngineBytes,
            maximum: MAX_SQL_REGEX_ENGINE_BYTES,
        })?;
    Ok(if compiled.is_match(input) != negated {
        SqlTruth::True
    } else {
        SqlTruth::False
    })
}

enum Part {
    Literal(char),
    One,
    Many,
}

fn walk_pattern(
    pattern: &str,
    escape: Option<char>,
    mut visit: impl FnMut(Part) -> Result<(), SqlEvaluationError>,
) -> Result<(), SqlEvaluationError> {
    let mut chars = pattern.chars();
    while let Some(value) = chars.next() {
        let part = if Some(value) == escape {
            Part::Literal(chars.next().ok_or(SqlEvaluationError::InvalidEscape)?)
        } else {
            match value {
                '%' => Part::Many,
                '_' => Part::One,
                value => Part::Literal(value),
            }
        };
        visit(part)?;
    }
    Ok(())
}

fn pattern_bytes(pattern: &str, escape: Option<char>) -> Result<usize, SqlEvaluationError> {
    let mut generated = 4_usize; // Both absolute anchors.
    walk_pattern(pattern, escape, |part| {
        generated += match part {
            Part::Many => 2,
            Part::One => 1,
            Part::Literal(value) => {
                let mut bytes = [0; 4];
                regex::escape(value.encode_utf8(&mut bytes)).len()
            }
        };
        if generated > MAX_SQL_LIKE_PATTERN_BYTES {
            return Err(SqlEvaluationError::Limit {
                kind: SqlEvaluationLimit::LikePatternBytes,
                maximum: MAX_SQL_LIKE_PATTERN_BYTES,
            });
        }
        Ok(())
    })?;
    Ok(generated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_size_is_exact_for_escaped_literals_and_wildcards() {
        assert_eq!(pattern_bytes("a%_", None), Ok(8));
        assert_eq!(pattern_bytes("a!%!_", Some('!')), Ok(7));
        assert_eq!(pattern_bytes("[a]", None), Ok(9));
        assert_eq!(pattern_bytes("\u{1f600}", None), Ok(8));
        assert_eq!(
            pattern_bytes("x!", Some('!')),
            Err(SqlEvaluationError::InvalidEscape)
        );
    }

    #[test]
    fn generated_pattern_bound_is_checked_before_compiling() {
        let exact = "a".repeat(MAX_SQL_LIKE_PATTERN_BYTES - 4);
        assert_eq!(pattern_bytes(&exact, None), Ok(MAX_SQL_LIKE_PATTERN_BYTES));
        assert_eq!(
            pattern_bytes(&(exact + "a"), None),
            Err(SqlEvaluationError::Limit {
                kind: SqlEvaluationLimit::LikePatternBytes,
                maximum: MAX_SQL_LIKE_PATTERN_BYTES,
            })
        );
    }
}
