# SQL Rules

Switchyard supports a bounded, action-free SQL predicate subset through the
domain state machine and AMQP rule management. This is not complete Azure SQL
filter or action compatibility. The exact Boolean aliases `1=1` and `1=0` keep
their existing filter representation.
The trusted domain separately supports bounded [REMOVE actions](sql-actions.md);
wire and CLI action creation remain unavailable.

## Storage And Compilation

`SqlFilter` stores the original expression and semantic version 1. Expression
text is not normalized during storage or enumeration. Deserialization validates
the source bound and version without compiling. Creation validates the grammar;
each owner rule load compiles ephemeral programs once. A requested-subscription
metadata read has one allowance; publication and activation share one allowance
across the whole topic. Stored malformed expressions or unsupported versions
refuse the operation as corrupt metadata, including empty and duplicate sends.

Expression syntax is parsed by the pinned SQL parser, then lowered to a flat
postorder program with backward-only child indexes. Neither the dependency AST
nor the native program is serialized. The compiler permits Boolean logic,
comparisons, numeric arithmetic, `IS NULL`, `IN`, `LIKE`, and property existence;
queries, actions, casts, and nondeterministic functions remain unsupported.
`p('literal')` and `property('literal')` address literal user-property names,
including periods. Dynamic property-name expressions are unsupported. Explicit
`sys` scope supports the same eight retained properties as correlation rules;
unknown system names and scopes are rejected at compilation.
User-property names must be nonempty and contain no control characters.

## Predicate Semantics

SQL predicates distinguish true, false, and unknown; only true selects a
message. Missing user properties are unknown, while present Null and absent
known optional system properties are Null. `IS NULL` accepts either missing
or Null. The Boolean truth tables and missing-user behavior follow the
[SQL filter specification](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-sql-filter).
Known-system absence is an explicit local choice, not cloud-verified behavior.
Existence distinguishes missing user properties from present Null, and treats
unset known system properties as absent. Null comparisons and null/unknown
`IN` operands propagate unknown locally; a successful `IN` comparison wins over
unknown candidates, but incompatible candidates remain errors. Those additional
choices have not been compared with a cloud namespace.
Authoritative ingress message/session identifiers are borrowed from the caller.

User-property lookup compares allocation-free Unicode lowercase streams and
refuses case-colliding keys rather than selecting one by map order. Regular
identifiers use Unicode alphabetic starts and alphanumeric/underscore
continuations; quoted names preserve other characters. These Unicode policies
are local, not claims of exact .NET culture or character-category parity.
String equality and `LIKE` are case-sensitive; string ordering is unsupported.
`LIKE` uses a bounded regular-expression engine with escaped literals, full
string anchors, newline-aware wildcards, and one Unicode scalar per `_`.
The scalar-width choice is local and has not been compared with Azure.

Integer literals retain Int64 semantics and floating literals use Double.
Numeric comparison preserves integer precision instead of converting every
value to floating point. Supported numeric promotions distinguish signed and
unsigned widths. Nonnegative constant-only integral expressions can convert
to Ulong, while signed runtime properties cannot; this constant-binding choice
is local, not a cloud-verified interpretation of the SDK's Int64 literals.
Integral overflow and zero divisors are finite evaluation errors; floating
arithmetic retains IEEE-754 results. SQL values currently support Boolean,
string, and integer/floating numeric constructors. Decimal, character,
timestamp, UUID, binary, symbol, and compound values remain unsupported for
scalar operators, but existence/null checks do not traverse or reject an
otherwise present value.

## Error Routing

Action-free correlation and SQL rules OR together and retain at most one copy
per subscription; matching action rules retain independent copies as described
in [SQL Actions](sql-actions.md). Every rule is evaluated without Boolean match
shortcuts. A finite SQL error overrides successful matches on the same subscription,
independently of rule order. The first finite error in sorted rule/node order
determines its fixed description. Healthy sibling subscriptions remain usable.
This error precedence is an explicit local policy, not cloud-verified behavior.

The subscription option `dead_lettering_on_filter_evaluation_exceptions`
defaults to true, matching the documented
[subscription creation default](https://learn.microsoft.com/en-us/rest/api/servicebus/create-subscription). With
that option enabled, a finite error creates one direct dead-letter copy with
reason `SwitchyardSqlFilterError`. This is a local reason, not a claim of Azure's
exact reason text. The description is a fixed error-class string: it contains
no expression, property names, or producer values. With the option false, only
that subscription's copies are dropped. SQL errors precede missing-session routing.
Unsupported scalar types, incompatible operands, integer overflow or division
by zero, ambiguous property names, invalid escapes, unsupported string ordering,
and non-Boolean results are finite errors.

Error copies preserve original content, the shared topic sequence, and any
scheduled-enqueue annotation, but strip the normalized session identifier and
lifetime. They use ordinary shadow receive/settlement machinery. Retention
budgets account for stripped session bytes, fixed reason/description bytes, and
two projected application-property values before payload cloning. Only actual
committed backing or shadow destinations are notified.

Resource exhaustion is never a per-subscription filter error. A later resource
failure wins even after an earlier finite error, refusing the entire command
without writes, counters, history, clock advancement, or notifications. Future
publications retain only their parent record at admission and use current rules
at activation; existing active, locked, and deferred copies are not reselected.
Activation takes a fitting due prefix, or leaves the first resource-unfit head
pending and cancelable. Corrupt metadata aborts rather than being treated as an
unfit prefix.

## Resource And Diagnostic Boundaries

Local compilation limits are 1,024 UTF-16 units, 4,096 UTF-8 bytes, 128 physical
tokens, parser depth 32, 128 native nodes, native depth 32, and 32 `IN` items per
expression. Source bounds apply before tokenization; the token bound applies
before parsing and also bounds recursive AST destruction for flat chains.
The tokenizer's temporary allocation is source-bounded, not token-capped during
allocation. The complete topic load shares 1 MiB source, 32,768 tokens, and
32,768 native nodes, including rule loads for empty or duplicate sends.

All correlation and SQL evaluation shares one command allowance of 1,048,576
work units and 32 MiB comparison bytes. Property scans and potential comparisons
are charged before the corresponding work. Finite failures stay in the flat
evaluation arena so they cannot conceal independent limits; dependent operations
retain conservative possible string bounds. Generated `LIKE` patterns are
limited to 16 KiB, with 1 MiB compiled engine and DFA-cache limits. These are local
resource policies, not Azure quotas or total process-memory guarantees.
The evaluator owns no recursive message values and copies no producer payloads.

Public compiler/evaluator errors contain no rule source or producer values;
the evaluator itself emits no logs. The parser dependency can log rule text at
debug levels. The server binary blocks its log targets before dispatch even
when an operator explicitly requests parser tracing, while retaining other
diagnostics. Library embedders must independently disable those parser targets.

## Wire Contract

AMQP rule creation compiles source before broker mutation. Syntax errors map to
status 400 / `amqp:invalid-field`, unsupported constructs to status 501 /
`amqp:not-implemented`, and resource limits to status 403 /
`amqp:resource-limit-exceeded`. Enumeration uses the SDK SQL descriptor, original
source, and an AMQP `int` compatibility level of 20; that wire level is distinct
from the stored semantic version. Full-page response overflow fails instead of
returning a truncated successful enumeration.

The native subscription create/get/list/update API and `switchyardctl` preserve
an explicit false filter-error policy. Creation defaults to true; update omission
preserves the committed policy. The option does not change backing-queue or
shadow configuration projections.
An update changes policy only for new publication copies and current scheduled
activation; existing active, locked, deferred, and dead-letter copies are unchanged.
The value format and store-layout rollback guards are described in
[Durable Format](compatibility.md#durable-format).
