//! A borrowed lexer for the declared action subset. sqlparser exposes neither
//! its next-token method nor an abortable token callback. Its buffer/mapper
//! APIs always push the entire stream. Scan one borrowed lexeme here instead,
//! then let its existing tokenizer construct/unescape only that allowed token.

use sqlparser::{
    dialect::Dialect,
    tokenizer::{Token, Tokenizer},
};

use crate::{MAX_SQL_EXPRESSION_TOKENS, SqlCompileBudget, SqlCompileError, SqlCompileLimit};

pub(super) fn tokenize(
    source: &str,
    dialect: &dyn Dialect,
    budget: &mut SqlCompileBudget,
) -> Result<Vec<Token>, SqlCompileError> {
    let mut tokens = Vec::new();
    lex(source, budget, |lexeme| {
        // Each lexeme contains exactly one token of the action subset. Even
        // a long quoted string/comment constructs only one token, never a
        // source-sized token vector. Both quotas were checked before this call.
        let mut single = Tokenizer::new(dialect, lexeme)
            .tokenize()
            .map_err(|_| SqlCompileError::Syntax)?;
        if single.len() != 1 {
            return Err(SqlCompileError::Syntax);
        }
        tokens.push(single.pop().ok_or(SqlCompileError::Syntax)?);
        Ok(())
    })?;
    Ok(tokens)
}

/// `emit` is the sole owned-token construction/allocation point. The scanner
/// itself only advances borrowed UTF-8 slices; no strings or vectors are built.
fn lex(
    mut remaining: &str,
    budget: &mut SqlCompileBudget,
    mut emit: impl FnMut(&str) -> Result<(), SqlCompileError>,
) -> Result<(), SqlCompileError> {
    let mut count = 0;
    while !remaining.is_empty() {
        if count == MAX_SQL_EXPRESSION_TOKENS {
            return Err(SqlCompileError::Limit {
                kind: SqlCompileLimit::PhysicalTokens,
                maximum: MAX_SQL_EXPRESSION_TOKENS,
            });
        }
        budget.tokens(1)?;
        count += 1;
        let length = lexeme_len(remaining)?;
        let (lexeme, rest) = remaining.split_at(length);
        emit(lexeme)?;
        remaining = rest;
    }
    Ok(())
}

fn lexeme_len(source: &str) -> Result<usize, SqlCompileError> {
    if source.starts_with("--") {
        // Match the action dialect: CR is comment text, LF starts a new token.
        return Ok(source.find('\n').unwrap_or(source.len()));
    }
    if let Some(comment) = source.strip_prefix("/*") {
        // Like ActionDialect, comments are not nested and hints are not expanded.
        return comment
            .find("*/")
            .map(|end| end + 4)
            .ok_or(SqlCompileError::Syntax);
    }
    if source.starts_with("\r\n") {
        return Ok(2);
    }
    let first = source.chars().next().ok_or(SqlCompileError::Syntax)?;
    if matches!(first, '\'' | '"' | '[') {
        let end_quote = if first == '[' { ']' } else { first };
        let mut chars = source.char_indices().skip(1).peekable();
        while let Some((index, ch)) = chars.next() {
            if ch == end_quote {
                if chars.peek().is_some_and(|(_, next)| *next == end_quote) {
                    chars.next();
                } else {
                    return Ok(index + ch.len_utf8());
                }
            }
        }
        return Err(SqlCompileError::Syntax);
    }
    let mut length = first.len_utf8();
    if first.is_alphabetic() || first.is_ascii_digit() {
        for ch in source[length..].chars() {
            let keep = if first.is_alphabetic() {
                ch.is_alphanumeric() || ch == '_'
            } else {
                ch.is_ascii_digit()
            };
            if !keep {
                break;
            }
            length += ch.len_utf8();
        }
    }
    // Whitespace is one physical token per character except CRLF above.
    // Punctuation is one token in the declared grammar. Other SQL constructs
    // are deliberately not recognized as action expressions by the parser.
    Ok(length)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rule::action::compiler::ActionDialect;

    #[test]
    fn physical_ceiling_stops_before_constructing_or_growing_the_next_token() {
        for trivia in [" ", "\t", "\n", "\r\n", "\u{2003}", "/**/", "--\n"] {
            let source = format!("REMOVE[x]{}", trivia.repeat(200));
            let mut emitted = Vec::new();
            let mut budget = SqlCompileBudget::default();
            let result = lex(&source, &mut budget, |lexeme| {
                emitted.push(lexeme.len());
                Ok(())
            });
            assert_eq!(
                result,
                Err(SqlCompileError::Limit {
                    kind: SqlCompileLimit::PhysicalTokens,
                    maximum: MAX_SQL_EXPRESSION_TOKENS,
                }),
                "{trivia:?}"
            );
            assert_eq!(emitted.len(), MAX_SQL_EXPRESSION_TOKENS);
            assert_eq!(emitted.capacity(), MAX_SQL_EXPRESSION_TOKENS);
            assert_eq!(budget.used().tokens, MAX_SQL_EXPRESSION_TOKENS);
        }
    }

    #[test]
    fn exhausted_shared_budget_never_calls_the_owned_token_constructor() {
        let source = format!("REMOVE[x]{}", " ".repeat(1_000));
        for allowed in [0, 1, 2, 17, MAX_SQL_EXPRESSION_TOKENS - 1] {
            let mut budget = SqlCompileBudget::with_limits(4_096, allowed, 128);
            let mut emitted = Vec::new();
            let mut construct = |lexeme: &str| {
                emitted.push(lexeme.len());
                Ok(())
            };
            let expected = Err(SqlCompileError::Limit {
                kind: SqlCompileLimit::AggregateTokens,
                maximum: allowed,
            });
            assert_eq!(lex(&source, &mut budget, &mut construct), expected);
            assert_eq!(lex("REMOVE[x]", &mut budget, construct), expected);
            assert_eq!(emitted.len(), allowed);
            assert_eq!(budget.used().tokens, allowed);
            if allowed == 0 {
                assert_eq!(emitted.capacity(), 0);
            }
        }
    }

    #[test]
    fn subset_tokens_match_the_shared_tokenizer_without_splitting_long_literals() {
        let long_string = format!("SET [key]='{}'", "a;--/*' '".repeat(90).replace('\'', "''"));
        let long_name = format!("REMOVE[{}]", "n".repeat(1_000));
        let long_comment = format!("REMOVE[x]/*{}*/", "hello'[];--".repeat(90));
        for source in [
            "SET x=1; SET y=-2; SET z=+3; SET yes=TRUE; SET no=false",
            "sEt UsEr.[Odd name]='can''t; -- comment'; SET \"a\"\"b\"=true",
            "REMOVE[a]]b]; REMOVE [é雪]; SET n='\\n\\'",
            "REMOVE[x] \t\r\n\r\n\u{2003}-- one\r comment\n/**//*two*/",
            "REMOVE[x]/*! no hint expansion */; SET y='/*not a comment*/'",
            &long_string,
            &long_name,
            &long_comment,
        ] {
            let expected = Tokenizer::new(&ActionDialect, source).tokenize().unwrap();
            let actual =
                tokenize(source, &ActionDialect, &mut SqlCompileBudget::default()).unwrap();
            assert_eq!(actual, expected, "{source:?}");
        }
    }
}
