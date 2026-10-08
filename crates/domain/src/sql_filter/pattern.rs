use std::fmt::Write;

use regex_automata::nfa::thompson::{self, State, WhichCaptures, pikevm::PikeVM};

use super::*;

pub(super) fn escape(text: &str) -> Option<char> {
    let mut chars = text.chars();
    let first = chars.next()?;
    chars.next().is_none().then_some(first)
}

pub(super) fn validate(text: &str, escape: Option<char>) -> Result<(), ()> {
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if Some(ch) == escape && chars.next().is_none() {
            return Err(());
        }
    }
    Ok(())
}

// Hex literals cannot inject regex syntax. The bound also covers both anchors.
pub(super) fn allocation_bound(pattern_bytes: usize) -> usize {
    pattern_bytes.saturating_mul(10).saturating_add(9)
}

pub(super) fn build_work_bound(pattern_bytes: usize) -> usize {
    // NFA::memory_usage includes every State's complete layout, so this remains
    // a state-count ceiling without reserving for impossible smaller states.
    allocation_bound(pattern_bytes).saturating_add(MAX_SQL_REGEX_BYTES / size_of::<State>())
}

pub(super) fn compile(text: &str, escape: Option<char>) -> Result<PikeVM, SqlEvaluationError> {
    if text.len() > MAX_SQL_LIKE_PATTERN_BYTES {
        return Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::LikePatternBytes,
            maximum: MAX_SQL_LIKE_PATTERN_BYTES,
        });
    }
    validate(text, escape).map_err(|_| SqlEvaluationError::InvalidLikePattern)?;
    let mut regex = String::with_capacity(allocation_bound(text.len()));
    regex.push_str(r"\A(?s:");
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if Some(ch) == escape {
            let literal = chars.next().ok_or(SqlEvaluationError::InvalidLikePattern)?;
            let _ = write!(regex, "\\x{{{:x}}}", u32::from(literal));
        } else {
            match ch {
                '%' => regex.push_str(".*"),
                '_' => regex.push('.'),
                literal => {
                    let _ = write!(regex, "\\x{{{:x}}}", u32::from(literal));
                }
            }
        }
    }
    regex.push_str(r")\z");
    build(&regex, MAX_SQL_REGEX_BYTES)
}

fn build(regex: &str, maximum: usize) -> Result<PikeVM, SqlEvaluationError> {
    let engine = PikeVM::builder()
        .thompson(
            thompson::Config::new()
                .which_captures(WhichCaptures::None)
                .nfa_size_limit(Some(maximum)),
        )
        .build(regex)
        .map_err(|error| {
            if error.size_limit().is_some() {
                SqlEvaluationError::Limit {
                    kind: SqlEvaluationLimit::RegexBytes,
                    maximum,
                }
            } else {
                SqlEvaluationError::InvalidLikePattern
            }
        })?;
    if engine.get_nfa().memory_usage() > maximum {
        return Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::RegexBytes,
            maximum,
        });
    }
    Ok(engine)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_escape_validation_and_translation_bounds_are_exact() {
        assert_eq!(escape(""), None);
        assert_eq!(escape("ab"), None);
        assert_eq!(escape("\u{1f600}"), Some('\u{1f600}'));
        for (text, expected) in [
            ("", Ok(())),
            ("!", Err(())),
            ("!!", Ok(())),
            ("!a", Ok(())),
            ("!!!", Err(())),
        ] {
            assert_eq!(validate(text, Some('!')), expected);
        }
        assert_eq!(allocation_bound(0), 9);
        assert_eq!(allocation_bound(usize::MAX), usize::MAX);
        assert_eq!(build_work_bound(usize::MAX), usize::MAX);
    }

    #[test]
    fn fixed_pattern_and_regex_caps_are_independent() {
        let exact = "a".repeat(MAX_SQL_LIKE_PATTERN_BYTES);
        let engine = compile(&exact, None).unwrap();
        assert!(engine.get_nfa().memory_usage() <= MAX_SQL_REGEX_BYTES);
        assert!(matches!(
            compile(&(exact + "a"), None),
            Err(SqlEvaluationError::Limit {
                kind: SqlEvaluationLimit::LikePatternBytes,
                maximum: MAX_SQL_LIKE_PATTERN_BYTES,
            })
        ));
        assert!(matches!(
            compile(&"_".repeat(MAX_SQL_LIKE_PATTERN_BYTES), None),
            Err(SqlEvaluationError::Limit {
                kind: SqlEvaluationLimit::RegexBytes,
                maximum: MAX_SQL_REGEX_BYTES,
            })
        ));
    }

    #[test]
    fn maintained_engine_accepts_its_exact_required_byte_limit_and_rejects_one_less() {
        let regex = r"\A(?s:.)\z";
        let mut low = 0;
        let mut high = MAX_SQL_REGEX_BYTES;
        while low < high {
            let middle = low + (high - low) / 2;
            if build(regex, middle).is_ok() {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        assert!(low > 0);
        let engine = build(regex, low).unwrap();
        assert!(engine.get_nfa().memory_usage() <= low);
        assert!(
            matches!(build(regex, low - 1), Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::RegexBytes, maximum,
        }) if maximum == low - 1)
        );
    }
}
