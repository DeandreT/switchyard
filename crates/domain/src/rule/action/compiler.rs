use sqlparser::{dialect::Dialect, keywords::Keyword, parser::Parser, tokenizer::Token};

use super::*;
use crate::{MAX_SQL_EXPRESSION_BYTES, MAX_SQL_EXPRESSION_UTF16_UNITS, SqlCompileLimit};

pub(super) fn compile(
    source: &str,
    budget: &mut SqlCompileBudget,
) -> Result<SqlActionProgram, SqlActionCompileError> {
    // These checks precede tokenizer allocation. UTF-16 scanning is itself
    // bounded by the byte check. No recursive expression AST is ever built.
    limit(
        source.len(),
        MAX_SQL_EXPRESSION_BYTES,
        SqlCompileLimit::SourceBytes,
    )?;
    let source_utf16_units = source.encode_utf16().count();
    limit(
        source_utf16_units,
        MAX_SQL_EXPRESSION_UTF16_UNITS,
        SqlCompileLimit::SourceUtf16Units,
    )?;
    budget.source(source.len())?;
    let dialect = ActionDialect;
    let tokens = super::lexer::tokenize(source, &dialect, budget)?;
    let mut metrics = SqlActionMetrics {
        source_bytes: source.len(),
        source_utf16_units,
        tokens: tokens.len(),
        statements: 0,
    };
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    let mut statements = Vec::new();
    loop {
        if statements.len() == MAX_SQL_ACTION_STATEMENTS {
            return Err(SqlActionCompileError::Statements {
                maximum: MAX_SQL_ACTION_STATEMENTS,
            });
        }
        let remove = parser.parse_keyword(Keyword::REMOVE);
        if !remove && !parser.parse_keyword(Keyword::SET) {
            return Err(SqlCompileError::Syntax.into());
        }
        budget.node()?;
        let first = identifier(&mut parser)?;
        let target = if parser.consume_token(&Token::Period) {
            if !first.eq_ignore_ascii_case("user") {
                return Err(unsupported("SQL action property scope").into());
            }
            identifier(&mut parser)?
        } else {
            first
        };
        if target.is_empty() || target.chars().any(char::is_control) {
            return Err(unsupported("SQL action property name").into());
        }
        let literal = if remove {
            None
        } else {
            if !parser.consume_token(&Token::Eq) {
                return Err(SqlCompileError::Syntax.into());
            }
            Some(literal(&mut parser, budget)?)
        };
        statements.push(Statement { target, literal });
        if parser.peek_token().token == Token::EOF {
            break;
        }
        if !parser.consume_token(&Token::SemiColon) {
            return Err(unsupported("SQL action expression").into());
        }
        if parser.peek_token().token == Token::EOF {
            break;
        }
    }
    metrics.statements = statements.len();
    Ok(SqlActionProgram {
        statements,
        metrics,
    })
}

// Same lexical identifier spelling as SQL filters; actions intentionally do
// not use the filter expression parser or change its grammar.
#[derive(Debug)]
pub(super) struct ActionDialect;

impl Dialect for ActionDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic()
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        matches!(ch, '"' | '[')
    }
}

fn identifier(parser: &mut Parser<'_>) -> Result<String, SqlCompileError> {
    match parser.next_token().token {
        Token::Word(word) if matches!(word.quote_style, None | Some('"' | '[')) => Ok(word.value),
        _ => Err(SqlCompileError::Syntax),
    }
}

fn literal(
    parser: &mut Parser<'_>,
    budget: &mut SqlCompileBudget,
) -> Result<Literal, SqlCompileError> {
    budget.node()?;
    let token = parser.next_token().token;
    let negative = token == Token::Minus;
    let signed = negative || token == Token::Plus;
    let token = if signed {
        budget.node()?;
        parser.next_token().token
    } else {
        token
    };
    match token {
        Token::SingleQuotedString(value) if !signed => Ok(Literal::String(value)),
        Token::Word(word) if !signed && word.quote_style.is_none() => match word.keyword {
            Keyword::TRUE => Ok(Literal::Bool(true)),
            Keyword::FALSE => Ok(Literal::Bool(false)),
            _ => Err(unsupported("SQL action literal")),
        },
        Token::Number(text, false)
            if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            let magnitude = text
                .parse::<u64>()
                .map_err(|_| unsupported("SQL action integer range"))?;
            let value = if negative && magnitude == i64::MAX as u64 + 1 {
                i64::MIN
            } else {
                let value = i64::try_from(magnitude)
                    .map_err(|_| unsupported("SQL action integer range"))?;
                if negative { -value } else { value }
            };
            Ok(Literal::Integer(value))
        }
        _ => Err(unsupported("SQL action literal")),
    }
}

fn limit(actual: usize, maximum: usize, kind: SqlCompileLimit) -> Result<(), SqlCompileError> {
    if actual > maximum {
        Err(SqlCompileError::Limit { kind, maximum })
    } else {
        Ok(())
    }
}

fn unsupported(feature: &'static str) -> SqlCompileError {
    SqlCompileError::Unsupported { feature }
}
