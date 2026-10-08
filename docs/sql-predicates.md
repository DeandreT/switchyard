# SQL Predicates

Ephemeral predicates, not persisted rules/routing. `SqlProgram::compile` /
`compile_with_budget` -> program; `evaluate(SqlMessageContext,
&mut SqlEvaluationBudget)` -> `SqlTruth` or error. Only True matches, not Unknown.

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
| Boolean | `NOT`, `AND`, `OR`, parentheses; three-valued |
| Comparison | `=`, `<>`/`!=`, `<`, `<=`, `>`, `>=` |
| Null/existence | Property `IS [NOT] NULL`, `[NOT] EXISTS(property)` |
| Membership | Scalar `[NOT] IN (scalar, ...)`; 1-32 operands |
| Pattern | Scalar `[NOT] LIKE scalar [ESCAPE scalar]`; `%`/`_` |
| Properties | Bare/user-qualified, double/bracket-quoted; `sys` whitelist |
| Literals | Boolean, null, single-quoted string, signed `i64`, finite `f64` |

```text
scalar := literal | property | static_lookup | (scalar)
        | +scalar | -scalar | scalar { + | - | * | / | % } scalar
static_lookup := { property | p } (single_quoted_static_key)
```

IN/LIKE operands are scalars, not predicates. Numeric/string roots compile but
yield `NonPredicate`. Static calls are unquoted, ASCII-insensitive and take one
single-quoted key, optionally parenthesized. No implicit string/number coercion.
Precedence: unary signs, `*`/`/`/`%`, `+`/`-`, comparison/membership/pattern,
`NOT`, `AND`, `OR`; binary arithmetic associates left.

Unsupported: dynamic keys, other functions (`newid` included), casts, parameters,
statements/subqueries, actions, LIKE ANY, ILIKE, SIMILAR TO, bracket wildcards,
regex syntax. IS NULL/EXISTS take only property syntax. Compilation refuses
qualified/quoted function names, modifiers, empty/control-containing keys.

## Values And Errors

`SqlMessageContext` borrows application/system entries; `SqlValue` retains eight
integral widths, Float/Double, Boolean, string, null and Unsupported. User lookup
is ASCII-insensitive, non-ASCII distinct; duplicates yield `AmbiguousProperty`.

| User input | Comparisons | IS NULL | EXISTS |
| --- | --- | --- | --- |
| Missing | Unknown | True | False |
| Explicit null | Unknown | True | True |

System whitelist: `CorrelationId`, `MessageId`, `To`, `ReplyTo`, `Subject`
(`Label` alias), `SessionId`, `ReplyToSessionId`, `ContentType`. Missing requested
entries yield `MissingSystemProperty`; adapters supply known nullable entries as
explicit null. Unknown system names refuse compilation.

Referenced Unsupported errors, including EXISTS/IS NULL/null comparisons.
`FALSE AND ...` / `TRUE OR ...` never hide errors/charges. Resource limits win;
otherwise the first postorder finite error wins. Strings use ordinal,
case-sensitive equality/inequality, no ordering; Booleans equality/inequality only.

Comparisons unchanged: Double before Float; Ulong rejects signed properties,
allows nonnegative integer literals. Narrower integrals stay exact; floating
promotion may lose precision. NaN: false equality/ordering, true inequality;
infinities allowed.

## Arithmetic And Static Keys

Symmetric binary promotion: first matching row. Byte/Short/Int/Long are signed;
U variants unsigned.

| Binary operands | Result type |
| --- | --- |
| Either Double | Double |
| Either Float | Float |
| Either Ulong | Ulong; signed other operand requires nonnegative literal origin |
| Either Long | Long |
| Uint with Byte/Short/Int | Long |
| Either Uint | Uint |
| Remaining Byte/Ubyte/Short/Ushort/Int | Int |

| Unary operand | `+` result | `-` result |
| --- | --- | --- |
| Byte/Ubyte/Short/Ushort | Int | Int |
| Int/Long | Same type | Checked same type |
| Uint | Uint | Long |
| Ulong | Ulong | TypeMismatch |
| Float/Double | Same width | Same width, sign inverted |

Literals: Long integers, finite Double decimals/exponents. Direct signed literals
(including Long minimum) retain origin through parentheses; all computed unary/
binary results clear it. Ulong `u+1` / `u+(1)` can pass; `u+(1+1)` refuses.
No folding or constant-expression exception, even for literal-only work.

| Arithmetic case | Result |
| --- | --- |
| Integral overflow/underflow; negated Int/Long minimum | `ArithmeticOverflow` in the promoted result type |
| Integral `/` or `%` zero | `DivideByZero` |
| Signed minimum `/ -1` or `% -1` | `ArithmeticOverflow`; remainder refusal is explicitly local, not universal C# runtime behavior |
| Integral division/remainder | Quotient truncates toward zero; nonzero remainder has dividend sign |
| Null/missing | Unknown before type/zero checks: `NULL/0` is Unknown; eager `(1/0)+NULL` errors |
| Non-null string/Boolean | `TypeMismatch`; no concatenation/coercion |

Float/Double use promoted width: overflow -> infinity, zero division -> infinity/
NaN (not `DivideByZero`). `%` is truncating, not IEEE, remainder: finite `%`
infinity -> dividend; infinity `%` finite, `%` zero or any NaN -> NaN. Native
operations/unary inversion preserve signed zero; NaN payload/sign unspecified.
Nonfinite results allowed, source literals refused.

Static calls use user lookup and raw keys after SQL doubled-apostrophe decoding.
`p('sys.MessageId')`, `p('user.Color')`, `p('[Color]')`, `p('"Color"')` read literal
application keys, never scopes/delimiters or system entries. Only normal `sys`
syntax reaches the whitelist; quoted property names decode as identifiers there.

[Service Bus syntax](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-sql-filter)
defines arithmetic, patterns/escapes, literals, unknown propagation and
`property`/`p`, with broader string-valued keys.
[C# numeric promotions](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/language-specification/expressions#1247-numeric-promotions)
and the [arithmetic reference](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/operators/arithmetic-operators)
inform widths/remainder. Checked-only arithmetic, cleared origin, raw static user
keys and error priority are local choices, not service-wide equivalence claims.

## Membership And Patterns

IN checks every entry with typed equality: any true -> True; else null/missing
-> Unknown; else False. NOT IN preserves Unknown when negating. Incompatible,
unsupported, ambiguous or missing-system operands error even after a match;
non-null types do not coerce.

| LIKE rule | Contract |
| --- | --- |
| Matching | Case-sensitive, ordinal, entire input |
| `%` / `_` | Zero-or-more / exactly one Unicode scalar, including newlines; combining sequences may have several scalars, supplementary characters one |
| Other characters | Literal, including brackets/regex punctuation; no implicit escape |
| ESCAPE | String of exactly one Unicode scalar; quotes any next scalar, including itself |
| Unpaired trailing escape | Malformed; known literal patterns/escape lengths refuse compilation, property-backed cases refuse evaluation deterministically |

Null/missing operands give Unknown, never mask referenced errors. Escape/pattern
validation precedes input null propagation: non-string pattern/escape errors with
null input; malformed patterns/engine limits cannot hide behind null/incompatible
input. Null/missing pattern/escape builds no regex.

Unicode-scalar (not UTF-16-unit) matching and malformed-pattern/error priority
are local choices, not service-wide equivalence.

## Bounds

| Budget | Fixed cap |
| --- | --- |
| Each expression | 4,096 UTF-8 bytes; 1,024 UTF-16 units; 128 physical tokens including whitespace/comments; parser depth 32; 128 nodes; expression depth 32 |
| Shared compilation | 1,048,576 source bytes; 32,768 tokens; 32,768 nodes |
| Shared evaluation | 1,048,576 work units; 33,554,432 lookup/comparison bytes |
| Membership list | 32 scalars, independent of expression ceilings |
| LIKE pattern | 16,384 UTF-8 bytes, including property-backed patterns |
| Regex engine | 1,048,576-byte Thompson NFA build limit plus final NFA size check |

`with_limits` only lowers shared ceilings; expression caps stay fixed. Charges
accumulate, survive failure, cannot wrap/refund. Larger patterns use borrowed
properties, not enlarged source/literal limits.

| Preflight, before slot allocation | Charge |
| --- | --- |
| Inputs/lookup | Counts/names, all referenced lookups; repeated bound-pass scans separately precharged before scanning |
| Arithmetic | Extra work unit/node and direct full string bounds, even null/invalid/hidden branches; zero string output bound; nested nodes reserve own operands, no recursive bound-pass arithmetic |
| Static lookup | Ordinary property-node charges |
| IN | Full input bytes per entry plus all entry string bytes, even after a match |
| LIKE | Full input/pattern/escape; `10 * pattern_bytes + 9` translation bytes, 1 MiB engine allocation allowance, transformed-source work plus `1 MiB / size_of::<State>()` work using maintained NFA state layout |

Arithmetic has no per-operation allocations or durable results. Arithmetic/IN/
LIKE preflight covers Boolean-hidden branches before regex validation/build.
Comparisons charge full strings. Maintained `regex-automata` PikeVM builds
anchored literal-safe regexes per evaluation, no retained cache. Before cache
allocation/matching charge
`(input_bytes + 1) * NFA_states` work and capture-free cache allowance
`64 * NFA_states + 32` bytes. Build/matching share the evaluation budget across
calls; matching may refuse an otherwise valid large pattern.

Engine build limits are approximate, may cover intermediate representations;
final NFA size is checked too. Auxiliary compiler allocations have fixed pattern/
translation bounds, not claimed within measured NFA size.

Sources: [API](../crates/domain/src/sql_filter.rs), [compiler](../crates/domain/src/sql_filter/compiler.rs), [evaluator](../crates/domain/src/sql_filter/evaluator.rs), [tests](../crates/domain/tests/sql_filter.rs).
Pure-kernel increment: [#97](https://github.com/DeandreT/switchyard/issues/97);
[#20 integration](https://github.com/DeandreT/switchyard/issues/20) remains separate.
