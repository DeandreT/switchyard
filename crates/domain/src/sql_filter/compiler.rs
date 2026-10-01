use sqlparser::{
    ast::{
        BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
        ObjectName, ObjectNamePart, UnaryOperator, Value,
    },
    dialect::Dialect,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};

use super::{
    MAX_SQL_EXPRESSION_BYTES, MAX_SQL_EXPRESSION_DEPTH, MAX_SQL_EXPRESSION_NODES,
    MAX_SQL_EXPRESSION_TOKENS, MAX_SQL_EXPRESSION_UTF16_UNITS, MAX_SQL_IN_ITEMS,
    MAX_SQL_PARSER_DEPTH, SqlBinaryOp, SqlCompileBudget, SqlCompileError, SqlCompileLimit,
    SqlLiteral, SqlNode, SqlProgram, SqlProgramMetrics, SqlProperty, SqlSystemProperty, SqlUnaryOp,
};

pub(super) fn compile(
    expression: &str,
    budget: &mut SqlCompileBudget,
) -> Result<SqlProgram, SqlCompileError> {
    let source_bytes = expression.len();
    require_limit(
        source_bytes,
        MAX_SQL_EXPRESSION_BYTES,
        SqlCompileLimit::SourceBytes,
    )?;
    let source_utf16_units = expression.encode_utf16().count();
    require_limit(
        source_utf16_units,
        MAX_SQL_EXPRESSION_UTF16_UNITS,
        SqlCompileLimit::SourceUtf16Units,
    )?;
    budget.charge_source(source_bytes)?;

    let dialect = PredicateDialect;
    // Whitespace and comments are physical tokens too. The source cap bounds
    // the tokenizer's allocation before its result can be counted.
    let tokens = Tokenizer::new(&dialect, expression)
        .tokenize()
        .map_err(|_| SqlCompileError::Syntax)?;
    let token_count = tokens.len();
    require_limit(
        token_count,
        MAX_SQL_EXPRESSION_TOKENS,
        SqlCompileLimit::PhysicalTokens,
    )?;
    budget.charge_tokens(token_count)?;
    let mut parser = Parser::new(&dialect)
        .with_recursion_limit(MAX_SQL_PARSER_DEPTH)
        .with_tokens(tokens);
    let expression = parser.parse_expr().map_err(parser_error)?;
    if parser.peek_token().token != Token::EOF {
        return Err(SqlCompileError::Syntax);
    }

    let mut builder = Builder {
        nodes: Vec::new(),
        in_operands: Vec::new(),
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
        in_operands: builder.in_operands,
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
        let negated = is_keyword(&parser.peek_token().token, Keyword::NOT)
            && is_keyword(&parser.peek_nth_token(1).token, Keyword::EXISTS);
        if !negated && !is_keyword(&parser.peek_token().token, Keyword::EXISTS) {
            return None;
        }
        if negated {
            parser.next_token();
        }
        parser.next_token();
        // Reuse the parser's function argument grammar, not its subquery-only
        // EXISTS grammar. NOT EXISTS must be intercepted before parse_not.
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

fn is_keyword(token: &Token, keyword: Keyword) -> bool {
    matches!(token, Token::Word(word) if word.keyword == keyword && word.quote_style.is_none())
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

fn require_limit(
    actual: usize,
    maximum: usize,
    kind: SqlCompileLimit,
) -> Result<(), SqlCompileError> {
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
    nodes: Vec<SqlNode>,
    in_operands: Vec<u16>,
    depth: usize,
    budget: &'a mut SqlCompileBudget,
}

impl Builder<'_> {
    fn push(&mut self, node: SqlNode, depth: usize) -> Result<u16, SqlCompileError> {
        require_limit(
            self.nodes.len() + 1,
            MAX_SQL_EXPRESSION_NODES,
            SqlCompileLimit::Nodes,
        )?;
        self.budget.charge_node()?;
        let index = self.nodes.len() as u16;
        self.nodes.push(node);
        self.depth = self.depth.max(depth);
        Ok(index)
    }

    fn lower(&mut self, expr: &Expr, depth: usize) -> Result<u16, SqlCompileError> {
        require_limit(
            depth,
            MAX_SQL_EXPRESSION_DEPTH,
            SqlCompileLimit::ExpressionDepth,
        )?;
        let node = match expr {
            Expr::Nested(inner) => return self.lower(inner, depth),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => SqlNode::Property(property(expr)?),
            Expr::Value(value) => SqlNode::Literal(literal(&value.value)?),
            Expr::UnaryOp { op, expr } => {
                if matches!(op, UnaryOperator::Minus)
                    && let Some(value) = negative_integer(expr)?
                {
                    return self.push(SqlNode::Literal(SqlLiteral::Int64(value)), depth);
                }
                let op = match op {
                    UnaryOperator::Not => SqlUnaryOp::Not,
                    UnaryOperator::Plus => SqlUnaryOp::Plus,
                    UnaryOperator::Minus => SqlUnaryOp::Minus,
                    _ => return Err(unsupported("unary operator")),
                };
                let input = self.lower(expr, depth + 1)?;
                SqlNode::Unary { op, input }
            }
            Expr::BinaryOp { left, op, right } => {
                let op = binary_operator(op)?;
                let left = self.lower(left, depth + 1)?;
                let right = self.lower(right, depth + 1)?;
                SqlNode::Binary { op, left, right }
            }
            Expr::IsNull(input) | Expr::IsNotNull(input) => {
                let input = self.lower(input, depth + 1)?;
                SqlNode::IsNull {
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
                require_limit(list.len(), MAX_SQL_IN_ITEMS, SqlCompileLimit::InItems)?;
                let input = self.lower(expr, depth + 1)?;
                let mut operands = [0_u16; MAX_SQL_IN_ITEMS];
                for (index, item) in list.iter().enumerate() {
                    operands[index] = self.lower(item, depth + 1)?;
                }
                // Nested IN lists may append their own slices while lowering.
                // Append this slice only after all its child indices are final.
                let start = self.in_operands.len();
                self.in_operands.extend_from_slice(&operands[..list.len()]);
                SqlNode::In {
                    input,
                    start,
                    len: list.len(),
                    negated: *negated,
                }
            }
            Expr::Like {
                expr,
                pattern,
                escape_char,
                negated,
                any: false,
            } => {
                let input = self.lower(expr, depth + 1)?;
                let pattern = self.lower(pattern, depth + 1)?;
                let escape = escape_char
                    .as_ref()
                    .map(|expr| self.lower(expr, depth + 1))
                    .transpose()?;
                SqlNode::Like {
                    input,
                    pattern,
                    escape,
                    negated: *negated,
                }
            }
            Expr::Function(function) => {
                let (name, argument) = static_function(function)?;
                if name.eq_ignore_ascii_case("exists") {
                    SqlNode::Exists(property(argument)?)
                } else if name.eq_ignore_ascii_case("property") || name.eq_ignore_ascii_case("p") {
                    SqlNode::Property(static_user_property(argument)?)
                } else {
                    return Err(unsupported("function"));
                }
            }
            Expr::InSubquery { .. } | Expr::Subquery(_) | Expr::Exists { .. } => {
                return Err(unsupported("subquery"));
            }
            Expr::Cast { .. } => return Err(unsupported("cast")),
            _ => return Err(unsupported("expression")),
        };
        self.push(node, depth)
    }
}

fn binary_operator(op: &BinaryOperator) -> Result<SqlBinaryOp, SqlCompileError> {
    Ok(match op {
        BinaryOperator::And => SqlBinaryOp::And,
        BinaryOperator::Or => SqlBinaryOp::Or,
        BinaryOperator::Eq => SqlBinaryOp::Eq,
        BinaryOperator::NotEq => SqlBinaryOp::Ne,
        BinaryOperator::Gt => SqlBinaryOp::Gt,
        BinaryOperator::GtEq => SqlBinaryOp::Ge,
        BinaryOperator::Lt => SqlBinaryOp::Lt,
        BinaryOperator::LtEq => SqlBinaryOp::Le,
        BinaryOperator::Plus => SqlBinaryOp::Add,
        BinaryOperator::Minus => SqlBinaryOp::Subtract,
        BinaryOperator::Multiply => SqlBinaryOp::Multiply,
        BinaryOperator::Divide => SqlBinaryOp::Divide,
        BinaryOperator::Modulo => SqlBinaryOp::Modulo,
        _ => return Err(unsupported("binary operator")),
    })
}

fn literal(value: &Value) -> Result<SqlLiteral, SqlCompileError> {
    match value {
        Value::Null => Ok(SqlLiteral::Null),
        Value::Boolean(value) => Ok(SqlLiteral::Bool(*value)),
        Value::SingleQuotedString(value) => Ok(SqlLiteral::String(value.clone())),
        Value::Number(value, false) if value.bytes().all(|byte| byte.is_ascii_digit()) => value
            .parse::<i64>()
            .map(SqlLiteral::Int64)
            .map_err(|_| unsupported("integer literal range")),
        Value::Number(value, false) => {
            let value = value
                .parse::<f64>()
                .map_err(|_| unsupported("numeric literal"))?;
            if !value.is_finite() {
                return Err(unsupported("nonfinite numeric literal"));
            }
            Ok(SqlLiteral::DoubleBits(value.to_bits()))
        }
        _ => Err(unsupported("literal")),
    }
}

fn negative_integer(expr: &Expr) -> Result<Option<i64>, SqlCompileError> {
    match expr {
        Expr::Nested(expr) => negative_integer(expr),
        Expr::Value(value) => match &value.value {
            Value::Number(value, false) if value.bytes().all(|byte| byte.is_ascii_digit()) => {
                let magnitude = value
                    .parse::<u64>()
                    .map_err(|_| unsupported("integer literal range"))?;
                if magnitude == (i64::MAX as u64) + 1 {
                    Ok(Some(i64::MIN))
                } else {
                    let magnitude = i64::try_from(magnitude)
                        .map_err(|_| unsupported("integer literal range"))?;
                    Ok(Some(-magnitude))
                }
            }
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

fn property(expr: &Expr) -> Result<SqlProperty, SqlCompileError> {
    match expr {
        Expr::Nested(expr) => property(expr),
        Expr::Identifier(name) => user_property(&name.value),
        Expr::CompoundIdentifier(parts) => {
            let [scope, name] = parts.as_slice() else {
                return Err(unsupported("property scope"));
            };
            if scope.value.eq_ignore_ascii_case("user") {
                user_property(&name.value)
            } else if scope.value.eq_ignore_ascii_case("sys") {
                system_property(&name.value).map(SqlProperty::System)
            } else {
                Err(unsupported("property scope"))
            }
        }
        Expr::Function(function) => {
            let (name, argument) = static_function(function)?;
            if name.eq_ignore_ascii_case("property") || name.eq_ignore_ascii_case("p") {
                static_user_property(argument)
            } else {
                Err(unsupported("EXISTS operand"))
            }
        }
        _ => Err(unsupported("EXISTS operand")),
    }
}

fn user_property(name: &str) -> Result<SqlProperty, SqlCompileError> {
    if name.is_empty() || name.chars().any(char::is_control) {
        return Err(unsupported("property name"));
    }
    Ok(SqlProperty::User(name.to_owned()))
}

fn system_property(name: &str) -> Result<SqlSystemProperty, SqlCompileError> {
    let property = if name.eq_ignore_ascii_case("CorrelationId") {
        SqlSystemProperty::CorrelationId
    } else if name.eq_ignore_ascii_case("MessageId") {
        SqlSystemProperty::MessageId
    } else if name.eq_ignore_ascii_case("To") {
        SqlSystemProperty::To
    } else if name.eq_ignore_ascii_case("ReplyTo") {
        SqlSystemProperty::ReplyTo
    } else if name.eq_ignore_ascii_case("Label") || name.eq_ignore_ascii_case("Subject") {
        SqlSystemProperty::Subject
    } else if name.eq_ignore_ascii_case("SessionId") {
        SqlSystemProperty::SessionId
    } else if name.eq_ignore_ascii_case("ReplyToSessionId") {
        SqlSystemProperty::ReplyToSessionId
    } else if name.eq_ignore_ascii_case("ContentType") {
        SqlSystemProperty::ContentType
    } else {
        return Err(unsupported("system property"));
    };
    Ok(property)
}

fn static_function(function: &Function) -> Result<(&str, &Expr), SqlCompileError> {
    let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
        return Err(unsupported("function scope"));
    };
    if name.quote_style.is_some()
        || function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || !function.within_group.is_empty()
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
    {
        return Err(unsupported("function modifiers"));
    }
    if !name.value.eq_ignore_ascii_case("exists")
        && !name.value.eq_ignore_ascii_case("property")
        && !name.value.eq_ignore_ascii_case("p")
    {
        return Err(unsupported("function"));
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(unsupported("function arguments"));
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return Err(unsupported("function modifiers"));
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice() else {
        return Err(unsupported("function arguments"));
    };
    Ok((&name.value, argument))
}

fn static_user_property(argument: &Expr) -> Result<SqlProperty, SqlCompileError> {
    match argument {
        Expr::Nested(argument) => static_user_property(argument),
        Expr::Value(value) => match &value.value {
            // A literal key is never split into a scope, even if it has periods.
            Value::SingleQuotedString(name) => user_property(name),
            _ => Err(unsupported("dynamic property name")),
        },
        _ => Err(unsupported("dynamic property name")),
    }
}

#[cfg(test)]
mod tests;
