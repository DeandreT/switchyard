use sqlparser::{
    ast::Expr,
    dialect::Dialect,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};

use super::{MAX_SQL_ACTION_STATEMENTS, SqlActionProgram, source_limits};
use crate::{
    MAX_SQL_EXPRESSION_TOKENS, MAX_SQL_PARSER_DEPTH, SqlCompileBudget, SqlCompileError,
    SqlCompileLimit,
};

pub(super) fn compile(
    expression: &str,
    budget: &mut SqlCompileBudget,
) -> Result<SqlActionProgram, SqlCompileError> {
    source_limits(expression)?;
    budget.charge_source(expression.len())?;

    let dialect = ActionDialect;
    // The source cap bounds tokenizer allocation. Whitespace and comments count
    // toward the same physical-token allowance as predicates.
    let tokens = Tokenizer::new(&dialect, expression)
        .tokenize()
        .map_err(|_| SqlCompileError::Syntax)?;
    if tokens.len() > MAX_SQL_EXPRESSION_TOKENS {
        return Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::PhysicalTokens,
            maximum: MAX_SQL_EXPRESSION_TOKENS,
        });
    }
    budget.charge_tokens(tokens.len())?;
    let mut parser = Parser::new(&dialect)
        .with_recursion_limit(MAX_SQL_PARSER_DEPTH)
        .with_tokens(tokens);
    let mut targets = Vec::new();

    loop {
        if !parser.parse_keyword(Keyword::REMOVE) {
            return Err(match parser.peek_token().token {
                Token::Word(_) => unsupported("SQL action statement"),
                _ => SqlCompileError::Syntax,
            });
        }
        if targets.len() == MAX_SQL_ACTION_STATEMENTS {
            return Err(SqlCompileError::Limit {
                kind: SqlCompileLimit::Nodes,
                maximum: MAX_SQL_ACTION_STATEMENTS,
            });
        }
        let target = parser.parse_expr().map_err(parser_error)?;
        let name = target_name(&target)?;
        budget.charge_node()?;
        targets.push(name.to_owned());

        if parser.peek_token().token == Token::EOF {
            break;
        }
        if !parser.consume_token(&Token::SemiColon) {
            return Err(SqlCompileError::Syntax);
        }
        if parser.peek_token().token == Token::EOF {
            break;
        }
    }

    Ok(SqlActionProgram { targets })
}

#[derive(Debug)]
struct ActionDialect;

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

fn target_name(target: &Expr) -> Result<&str, SqlCompileError> {
    let name = match target {
        Expr::Identifier(name) => &name.value,
        Expr::CompoundIdentifier(parts) => {
            let [scope, name] = parts.as_slice() else {
                return Err(unsupported("SQL action property scope"));
            };
            if !scope.value.eq_ignore_ascii_case("user") {
                return Err(unsupported("SQL action property scope"));
            }
            &name.value
        }
        _ => return Err(unsupported("SQL action target")),
    };
    if name.is_empty() || name.chars().any(char::is_control) {
        return Err(unsupported("SQL action property name"));
    }
    Ok(name)
}

fn parser_error(error: ParserError) -> SqlCompileError {
    match error {
        ParserError::RecursionLimitExceeded => SqlCompileError::Limit {
            kind: SqlCompileLimit::ParserDepth,
            maximum: MAX_SQL_PARSER_DEPTH,
        },
        _ => SqlCompileError::Syntax,
    }
}

fn unsupported(feature: &'static str) -> SqlCompileError {
    SqlCompileError::Unsupported { feature }
}
