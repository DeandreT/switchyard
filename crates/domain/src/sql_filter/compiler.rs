use sqlparser::{
    ast::{BinaryOperator, Expr, Ident, ObjectName, UnaryOperator, Value},
    dialect::Dialect,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};

use super::*;

pub(super) fn compile(
    expression: &str,
    budget: &mut SqlCompileBudget,
) -> Result<SqlProgram, SqlCompileError> {
    let source_bytes = expression.len();
    limit(
        source_bytes,
        MAX_SQL_EXPRESSION_BYTES,
        SqlCompileLimit::SourceBytes,
    )?;
    let source_utf16_units = expression.encode_utf16().count();
    limit(
        source_utf16_units,
        MAX_SQL_EXPRESSION_UTF16_UNITS,
        SqlCompileLimit::SourceUtf16Units,
    )?;
    budget.source(source_bytes)?;
    let dialect = PredicateDialect;
    let tokens = Tokenizer::new(&dialect, expression)
        .tokenize()
        .map_err(|_| SqlCompileError::Syntax)?;
    let token_count = tokens.len();
    budget.tokens(token_count)?;
    limit(
        token_count,
        MAX_SQL_EXPRESSION_TOKENS,
        SqlCompileLimit::PhysicalTokens,
    )?;
    let mut parser = Parser::new(&dialect)
        .with_recursion_limit(MAX_SQL_PARSER_DEPTH)
        .with_tokens(tokens);
    let expression = parser.parse_expr().map_err(|error| match error {
        ParserError::RecursionLimitExceeded => SqlCompileError::Limit {
            kind: SqlCompileLimit::ParserDepth,
            maximum: MAX_SQL_PARSER_DEPTH,
        },
        _ => SqlCompileError::Syntax,
    })?;
    if parser.peek_token().token != Token::EOF {
        return Err(SqlCompileError::Syntax);
    }
    let mut builder = Builder {
        nodes: Vec::new(),
        depth: 0,
        budget,
    };
    let root = builder.lower(&expression, 1)?;
    let metrics = SqlProgramMetrics {
        source_bytes,
        source_utf16_units,
        tokens: token_count,
        nodes: builder.nodes.len(),
        depth: builder.depth,
    };
    Ok(SqlProgram {
        nodes: builder.nodes,
        root,
        metrics,
    })
}

#[derive(Debug)]
struct PredicateDialect;

impl Dialect for PredicateDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic()
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_'
    }
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        matches!(ch, '"' | '[')
    }

    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        let keyword = |token: &Token, expected| {
            matches!(token,
            Token::Word(word) if word.keyword == expected && word.quote_style.is_none())
        };
        let negated = keyword(&parser.peek_token().token, Keyword::NOT)
            && keyword(&parser.peek_nth_token(1).token, Keyword::EXISTS);
        if !negated && !keyword(&parser.peek_token().token, Keyword::EXISTS) {
            return None;
        }
        if negated {
            parser.next_token();
        }
        parser.next_token();
        // EXISTS takes a property, not sqlparser's subquery grammar.
        Some(
            parser
                .parse_function(ObjectName::from(vec![Ident::new("exists")]))
                .map(|expr| {
                    if negated {
                        Expr::UnaryOp {
                            op: UnaryOperator::Not,
                            expr: Box::new(expr),
                        }
                    } else {
                        expr
                    }
                }),
        )
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

struct Builder<'a> {
    nodes: Vec<Node>,
    depth: usize,
    budget: &'a mut SqlCompileBudget,
}

impl Builder<'_> {
    fn push(&mut self, node: Node, depth: usize) -> Result<u16, SqlCompileError> {
        limit(
            depth,
            MAX_SQL_EXPRESSION_DEPTH,
            SqlCompileLimit::ExpressionDepth,
        )?;
        limit(
            self.nodes.len() + 1,
            MAX_SQL_EXPRESSION_NODES,
            SqlCompileLimit::Nodes,
        )?;
        self.budget.node()?;
        let index = self.nodes.len() as u16;
        self.nodes.push(node);
        self.depth = self.depth.max(depth);
        Ok(index)
    }

    fn lower(&mut self, expr: &Expr, depth: usize) -> Result<u16, SqlCompileError> {
        limit(
            depth,
            MAX_SQL_EXPRESSION_DEPTH,
            SqlCompileLimit::ExpressionDepth,
        )?;
        let node = match expr {
            Expr::Nested(inner) => return self.lower(inner, depth),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Node::Property(property(expr)?),
            Expr::Value(value) => Node::Literal(literal(&value.value)?),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => Node::Not(self.lower(expr, depth + 1)?),
            Expr::UnaryOp {
                op: op @ (UnaryOperator::Minus | UnaryOperator::Plus),
                expr,
            } => Node::Literal(signed_literal(expr, matches!(op, UnaryOperator::Minus))?),
            Expr::BinaryOp { left, op, right } => {
                let op = match op {
                    BinaryOperator::And => Binary::And,
                    BinaryOperator::Or => Binary::Or,
                    BinaryOperator::Eq => Binary::Eq,
                    BinaryOperator::NotEq => Binary::Ne,
                    BinaryOperator::Gt => Binary::Gt,
                    BinaryOperator::GtEq => Binary::Ge,
                    BinaryOperator::Lt => Binary::Lt,
                    BinaryOperator::LtEq => Binary::Le,
                    _ => return Err(unsupported("binary operator")),
                };
                let left = self.lower(left, depth + 1)?;
                let right = self.lower(right, depth + 1)?;
                Node::Binary { op, left, right }
            }
            Expr::IsNull(input) | Expr::IsNotNull(input) => {
                // Only a property is accepted by the declared IS NULL grammar.
                let property = property(input)?;
                let input = self.push(Node::Property(property), depth + 1)?;
                Node::IsNull {
                    input,
                    negated: matches!(expr, Expr::IsNotNull(_)),
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                if list.is_empty() {
                    return Err(SqlCompileError::Syntax);
                }
                limit(list.len(), MAX_SQL_IN_OPERANDS, SqlCompileLimit::InOperands)?;
                let input = self.scalar(expr, depth + 1)?;
                let mut operands = [0; MAX_SQL_IN_OPERANDS];
                for (index, operand) in list.iter().enumerate() {
                    operands[index] = self.scalar(operand, depth + 1)?;
                }
                Node::InList {
                    input,
                    operands,
                    len: list.len() as u8,
                    negated: *negated,
                }
            }
            Expr::Like {
                expr,
                pattern,
                escape_char,
                negated,
                any,
            } => {
                if *any {
                    return Err(unsupported("LIKE ANY"));
                }
                let input = self.scalar(expr, depth + 1)?;
                let pattern = self.scalar(pattern, depth + 1)?;
                let escape = escape_char
                    .as_ref()
                    .map(|expr| self.scalar(expr, depth + 1))
                    .transpose()?;
                let known_escape = match escape.map(|index| &self.nodes[usize::from(index)]) {
                    None => Some(None),
                    Some(Node::Literal(Literal::String(text))) => Some(Some(
                        super::pattern::escape(text).ok_or(SqlCompileError::InvalidLikeEscape)?,
                    )),
                    _ => None,
                };
                if let Node::Literal(Literal::String(text)) = &self.nodes[usize::from(pattern)] {
                    limit(
                        text.len(),
                        MAX_SQL_LIKE_PATTERN_BYTES,
                        SqlCompileLimit::LikePatternBytes,
                    )?;
                    if let Some(escape) = known_escape {
                        super::pattern::validate(text, escape)
                            .map_err(|_| SqlCompileError::InvalidLikePattern)?;
                    }
                }
                Node::Like {
                    input,
                    pattern,
                    escape,
                    negated: *negated,
                }
            }
            Expr::Function(function) => {
                use sqlparser::ast::{
                    FunctionArg, FunctionArgExpr, FunctionArguments, ObjectNamePart,
                };
                let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
                    return Err(unsupported("function scope"));
                };
                if !name.value.eq_ignore_ascii_case("exists") || name.quote_style.is_some() {
                    return Err(unsupported("function"));
                }
                if function.uses_odbc_syntax
                    || !matches!(function.parameters, FunctionArguments::None)
                    || !function.within_group.is_empty()
                    || function.filter.is_some()
                    || function.null_treatment.is_some()
                    || function.over.is_some()
                {
                    return Err(unsupported("function modifiers"));
                }
                let FunctionArguments::List(arguments) = &function.args else {
                    return Err(unsupported("EXISTS operand"));
                };
                if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
                    return Err(unsupported("function modifiers"));
                }
                let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] =
                    arguments.args.as_slice()
                else {
                    return Err(unsupported("EXISTS operand"));
                };
                Node::Exists(property(argument)?)
            }
            _ => return Err(unsupported("expression")),
        };
        self.push(node, depth)
    }

    fn scalar(&mut self, expr: &Expr, depth: usize) -> Result<u16, SqlCompileError> {
        match expr {
            Expr::Nested(inner) => self.scalar(inner, depth),
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Value(_)
            | Expr::UnaryOp {
                op: UnaryOperator::Minus | UnaryOperator::Plus,
                ..
            } => self.lower(expr, depth),
            _ => Err(unsupported("scalar operand")),
        }
    }
}

fn signed_literal(expr: &Expr, negative: bool) -> Result<Literal, SqlCompileError> {
    let expr = match expr {
        Expr::Nested(expr) => return signed_literal(expr, negative),
        other => other,
    };
    let Expr::Value(value) = expr else {
        return Err(unsupported("unary arithmetic"));
    };
    let Value::Number(text, false) = &value.value else {
        return Err(unsupported("unary arithmetic"));
    };
    if negative && text.bytes().all(|byte| byte.is_ascii_digit()) {
        let magnitude = text
            .parse::<u64>()
            .map_err(|_| unsupported("integer literal range"))?;
        let value = if magnitude == (i64::MAX as u64) + 1 {
            i64::MIN
        } else {
            -i64::try_from(magnitude).map_err(|_| unsupported("integer literal range"))?
        };
        return Ok(Literal::Integer(value));
    }
    match literal(&value.value)? {
        Literal::Integer(value) => Ok(Literal::Integer(value)),
        Literal::Double(value) => Ok(Literal::Double(if negative { -value } else { value })),
        _ => Err(unsupported("numeric literal")),
    }
}

fn literal(value: &Value) -> Result<Literal, SqlCompileError> {
    match value {
        Value::Null => Ok(Literal::Null),
        Value::Boolean(value) => Ok(Literal::Bool(*value)),
        Value::SingleQuotedString(value) => Ok(Literal::String(value.clone())),
        Value::Number(value, false) if value.bytes().all(|byte| byte.is_ascii_digit()) => value
            .parse::<i64>()
            .map(Literal::Integer)
            .map_err(|_| unsupported("integer literal range")),
        Value::Number(value, false) => {
            let value = value
                .parse::<f64>()
                .map_err(|_| unsupported("numeric literal"))?;
            if !value.is_finite() {
                return Err(unsupported("nonfinite numeric literal"));
            }
            Ok(Literal::Double(value))
        }
        _ => Err(unsupported("literal")),
    }
}

fn property(expr: &Expr) -> Result<Property, SqlCompileError> {
    let user = |name: &str| {
        if name.is_empty() || name.chars().any(char::is_control) {
            Err(unsupported("property name"))
        } else {
            Ok(Property::User(name.to_owned()))
        }
    };
    match expr {
        Expr::Nested(expr) => property(expr),
        Expr::Identifier(name) => user(&name.value),
        Expr::CompoundIdentifier(parts) => {
            let [scope, name] = parts.as_slice() else {
                return Err(unsupported("property scope"));
            };
            if scope.value.eq_ignore_ascii_case("user") {
                user(&name.value)
            } else if scope.value.eq_ignore_ascii_case("sys") {
                system_property(&name.value).map(Property::System)
            } else {
                Err(unsupported("property scope"))
            }
        }
        _ => Err(unsupported("property operand")),
    }
}

fn system_property(name: &str) -> Result<SqlSystemProperty, SqlCompileError> {
    let choices = [
        ("CorrelationId", SqlSystemProperty::CorrelationId),
        ("MessageId", SqlSystemProperty::MessageId),
        ("To", SqlSystemProperty::To),
        ("ReplyTo", SqlSystemProperty::ReplyTo),
        ("Subject", SqlSystemProperty::Subject),
        ("Label", SqlSystemProperty::Subject),
        ("SessionId", SqlSystemProperty::SessionId),
        ("ReplyToSessionId", SqlSystemProperty::ReplyToSessionId),
        ("ContentType", SqlSystemProperty::ContentType),
    ];
    choices
        .into_iter()
        .find_map(|(candidate, property)| name.eq_ignore_ascii_case(candidate).then_some(property))
        .ok_or_else(|| unsupported("system property"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_arena_node_and_depth_boundaries_are_checked_before_push() {
        let mut budget = SqlCompileBudget::default();
        let mut builder = Builder {
            nodes: Vec::new(),
            depth: 0,
            budget: &mut budget,
        };
        for index in 0..MAX_SQL_EXPRESSION_NODES {
            assert_eq!(
                builder.push(Node::Literal(Literal::Bool(true)), 1).unwrap(),
                index as u16
            );
        }
        assert_eq!(
            builder.push(Node::Literal(Literal::Bool(true)), 1),
            Err(SqlCompileError::Limit {
                kind: SqlCompileLimit::Nodes,
                maximum: MAX_SQL_EXPRESSION_NODES,
            })
        );
        assert_eq!(builder.nodes.len(), MAX_SQL_EXPRESSION_NODES);
        assert_eq!(builder.budget.used().nodes, MAX_SQL_EXPRESSION_NODES);

        let mut expr = Expr::Value(Value::Boolean(true).into());
        for _ in 1..MAX_SQL_EXPRESSION_DEPTH {
            expr = Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            };
        }
        let mut budget = SqlCompileBudget::default();
        let mut builder = Builder {
            nodes: Vec::new(),
            depth: 0,
            budget: &mut budget,
        };
        assert_eq!(
            builder.lower(&expr, 1).unwrap(),
            (MAX_SQL_EXPRESSION_DEPTH - 1) as u16
        );
        assert_eq!(builder.depth, MAX_SQL_EXPRESSION_DEPTH);
        let deeper = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(expr),
        };
        assert_eq!(
            builder.lower(&deeper, 1),
            Err(SqlCompileError::Limit {
                kind: SqlCompileLimit::ExpressionDepth,
                maximum: MAX_SQL_EXPRESSION_DEPTH,
            })
        );
        assert_eq!(builder.nodes.len(), MAX_SQL_EXPRESSION_DEPTH);
    }

    #[test]
    fn lowering_keeps_children_prior_to_parents_and_metrics_exact() {
        let source = "NOT EXISTS(user.gone) AND (value=2 OR other IS NULL)";
        let program = SqlProgram::compile(source).unwrap();
        for (index, node) in program.nodes.iter().enumerate() {
            match node {
                Node::Not(child) | Node::IsNull { input: child, .. } => {
                    assert!(usize::from(*child) < index)
                }
                Node::Binary { left, right, .. } => {
                    assert!(usize::from(*left) < index);
                    assert!(usize::from(*right) < index);
                }
                _ => {}
            }
        }
        assert_eq!(usize::from(program.root) + 1, program.nodes.len());
        assert_eq!(program.metrics.nodes, program.nodes.len());
        assert_eq!(program.metrics.source_bytes, source.len());
    }
}
