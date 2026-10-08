# SQL Predicates

The domain exposes an ephemeral typed predicate kernel, not persisted SQL rules
or subscription routing. `SqlProgram::compile` (or `compile_with_budget`) builds
a program; `evaluate(SqlMessageContext, &mut SqlEvaluationBudget)` returns
`SqlTruth` or an error. Only `True.is_match()` matches; `Unknown` does not.

```mermaid
flowchart LR
    Input["Source/token limits + shared compile budget"] --> Parser["sqlparser 0.63 + depth limit"]
    Parser --> Program["Bounded postorder SqlProgram"]
    Program --> Evaluate["Borrowed values + shared evaluation budget"]
    Evaluate --> Result["True / False / Unknown or error"]
```

## Accepted Profile

| Form | Contract |
| --- | --- |
| Boolean | `NOT`, `AND`, `OR`, parentheses; three-valued logic |
| Comparison | `=`, `<>`/`!=`, `<`, `<=`, `>`, `>=`; no implicit string/number coercion |
| Null/existence | Property `IS [NOT] NULL`, `[NOT] EXISTS(property)` |
| Properties | Bare/user-qualified names, double-quoted or bracket-quoted names; explicit `sys` whitelist |
| Literals | Boolean, null, single-quoted string, signed `i64`, finite `f64`; unary `+`/`-` only on numeric literals |

Numeric/string scalar roots can compile, but evaluate as `NonPredicate`.
IN/LIKE, arithmetic, other functions, parameters, statements/subqueries and
actions are not accepted by this increment.

## Values And Errors

`SqlMessageContext` borrows application/system entries; `SqlValue` retains
eight integral widths, `Float`/`Double`, Boolean, string, null or Unsupported.
User names compare ASCII-insensitively; non-ASCII spelling stays distinct.
Duplicate matching entries refuse as `AmbiguousProperty`.

Missing user properties are Unknown: comparisons propagate Unknown, `IS NULL`
is true and `EXISTS` is false. Explicit null also makes `IS NULL` true, but
`EXISTS` is true. Supported system inputs are `CorrelationId`, `MessageId`, `To`,
`ReplyTo`, `Subject` (`Label` alias), `SessionId`, `ReplyToSessionId`, `ContentType`.
An absent requested system entry is `MissingSystemProperty`; adapters must
supply explicit null for known nullable entries. Unknown system names refuse.

Every referenced Unsupported value errors, including EXISTS/IS NULL and
null comparisons. Boolean evaluation does not short-circuit away errors or
budget charges, even under `FALSE AND ...` or `TRUE OR ...`.
Strings use ordinal, case-sensitive equality/inequality; ordering refuses. Booleans support equality/inequality only.

Numeric comparisons use a local C#-style profile: Double promotion precedes
Float. Integral comparisons reject signed properties against Ulong, but allow
nonnegative integer literals. Narrower integral comparisons remain exact;
floating promotion can lose precision. Typed NaN compares false for equality
and ordering against numeric operands, true for inequality; typed infinities
are allowed. This subset does not claim universal Azure type behavior.

## Bounds

| Budget | Fixed ceiling |
| --- | --- |
| Each expression | 4,096 UTF-8 bytes; 1,024 UTF-16 units; 128 physical tokens including whitespace/comments; parser depth 32; 128 nodes; expression depth 32 |
| Shared compilation | 1,048,576 source bytes; 32,768 tokens; 32,768 nodes |
| Shared evaluation | 1,048,576 work units; 33,554,432 lookup/comparison bytes |

Per-expression limits are fixed. `with_limits` can lower shared ceilings, never
raise them. Reusing a budget shares accumulated charges across calls; completed
charges remain after failure. Input counts/names and all referenced lookups are
charged before evaluation allocation; string comparisons charge full operands.

See the [API](../crates/domain/src/sql_filter.rs), [compiler](../crates/domain/src/sql_filter/compiler.rs), [evaluator](../crates/domain/src/sql_filter/evaluator.rs) and [tests](../crates/domain/tests/sql_filter.rs).
Follow-ups are [#96 IN/LIKE](https://github.com/DeandreT/switchyard/issues/96), then [#97 arithmetic/static lookup](https://github.com/DeandreT/switchyard/issues/97); [#20 integration](https://github.com/DeandreT/switchyard/issues/20) remains separate.
