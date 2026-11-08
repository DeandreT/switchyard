# SQL rule actions

The domain's `SqlActionProgram` compiles and evaluates an ephemeral, transport-free
action program. It returns a typed `SqlActionPatch` for one copy without changing
the original message. The domain also persists exact `SqlRuleAction` source in
action-bearing rules and routes isolated copies. AMQP rule management still
refuses nonempty wire actions; domain-seeded copies render their removals.

## Grammar and names

An action contains one or more `REMOVE target` or `SET target = literal`
statements separated by semicolons. One trailing semicolon is allowed; empty
statements are not. Keywords and the optional `user.` scope are ASCII
case-insensitive. Targets are bare identifiers, double-quoted identifiers, or
bracket-delimited identifiers, with the same lexical spelling as SQL filter
names. Quoted names can contain spaces and punctuation. Empty names and control
characters refuse. SQL comments and doubled quote escapes are handled by the
existing SQL tokenizer.

Application-property target names are **case-exact**, including quoted names.
`SET Count = 1` and `SET count = 1` target different properties, matching the
existing copy-overlay name identity. This differs deliberately from filter
lookup's ASCII-insensitive matching. Only an unqualified property or `user.name`
is accepted; `sys.name` and other scopes refuse. A quoted literal name such as
`[sys.Label]` is an application-property key, not a system-property reference.

Literal values are single-quoted Unicode strings, `TRUE`, `FALSE`, or decimal
signed i64 integers with an optional single `+`/`-`. Both i64 endpoints are
supported. Null, decimals, exponent notation, property references, parameters,
functions, arithmetic, parenthesized expressions and generic SQL assignments
are not part of this grammar. The SQL filter grammar is unchanged.

## Assignment and removal

- Statements execute in order against a private plan. Repeated exact targets
  retain first-target order and the last value/removal in the returned patch.
- Removing an absent property succeeds. Removal does not inspect the original
  value's type. It produces a deletion, never an AMQP null.
- Missing, null and previously removed targets accept a fresh literal: string,
  Boolean, or AMQP `long` for an integer. A later assignment sees the preceding
  assignment, not the original target. Remove then SET can change its type.
- An existing string accepts only strings; an existing Boolean only Booleans.
  Existing byte/ubyte/short/ushort/int/uint/long/ulong targets accept integers
  only, retain that constructor, and refuse out-of-range assignments without
  narrowing or wrapping. In particular negative-to-unsigned refuses.
- Existing float/double and other unsupported typed SQL inputs refuse SET with
  `UnsupportedTargetType`. A supported but incompatible family gives
  `TypeMismatch`; integral overflow gives `NumericOverflow`. Unreferenced
  properties do not cause type failures. Duplicate exact input names refuse
  when a SET reads them with `Evaluation(AmbiguousProperty)`; case variants are
  distinct. Canonical ingress already collapses duplicate exact names.
- Input is the existing borrowed `SqlMessageContext`/`SqlProperty`/`SqlValue`
  projection. System inputs are ignored. Results reuse `message_content::Value`
  constructors, not a second scalar codec. String results select one-byte length
  width through 255 UTF-8 bytes, otherwise four-byte width.

`SqlActionPatch::entries()` (or `into_entries()`) exposes explicit
`SqlActionProperty::Set { name, value }` and `Remove { name }` variants. Unlike
the existing persisted `OverlayProperty`, this ephemeral patch can express a
deletion. It must not be serialized into an older overlay or silently mapped
to null. Its consumer must enforce effective-message limits and provide any
required durable deletion representation before activating actions.

## Bounds and errors

Compilation checks the existing 4,096-byte and 1,024-UTF-16-unit SQL source caps
before lexing. A borrowed action-subset lexer checks the 128 physical-token cap
(including whitespace and comments) and charges the shared aggregate token
budget **before constructing each token or growing its vector**. Quoted strings,
quoted names and whole comments remain single tokens regardless of their byte
length; CRLF is one whitespace token. The shared SQL tokenizer unescapes each
individual allowed lexeme, never the uncapped whole stream. It shares
`SqlCompileBudget` source/token/node charges with filters.
Each statement charges one node, each SET literal one more, and a sign one more.
At most 32 statements are allowed. The action parser is flat: it consumes only
identifiers, literal tokens and separators, never a recursive expression tree.
Token construction is source-bounded and the token vector is capped during
lexing; parser work and program allocation are source/token/statement-bounded.
An exhausted aggregate budget constructs no further tokens. Failed lexing keeps
charges for the consumed prefix rather than refunding them.
`SqlActionCompileError::Sql` preserves existing
typed SQL syntax/unsupported/limit errors; `Statements` names the statement cap.

Evaluation uses the existing `SqlEvaluationBudget`: it charges input property
count before scanning, statement/lookup work, both names' byte lengths before
every exact comparison, and final materialization work/bytes before cloning
output. Aggregate budgets are reusable, clamp at the existing ceilings, and
never refund consumed charges, including on failure.

The private plan and output have at most 32 distinct targets, including removal
entries, and at most 64 KiB of conservative charged patch bytes (the existing
property-patch headroom). `SqlActionLimits::with_limits` may lower either cap,
never raise it. Each intermediate patch must fit; a later REMOVE or shorter SET
cannot rescue an earlier oversized result. A name costs five framing bytes plus
its UTF-8 length. Removal costs one further byte; a string costs five plus its
UTF-8 length; bool/byte/ubyte cost two; short/ushort three; int/uint five;
long/ulong nine. These are conservative patch charges, **not** the full rendered
message size or a new encoding. The source cap also independently bounds literal
and target storage. Limit errors expose `PatchProperties`, `PatchBytes`, or the
existing typed SQL evaluation limit and its exact maximum.

Any compile/evaluation/type/result-limit failure returns no patch, including no
partially checked entries. Successful evaluations own independent patches;
evaluating another action or filter against the same borrowed original never
sees a sibling's changes. Programs and patches have no serde deserialization
entry point that could bypass compilation or output limits.

## Public API example

```rust
use domain::{SqlActionProgram, SqlEvaluationBudget, SqlMessageContext};

let action = SqlActionProgram::compile("REMOVE transient; SET route = 'west'")?;
let patch = action.evaluate(
    SqlMessageContext::default(),
    &mut SqlEvaluationBudget::default(),
)?;
assert_eq!(patch.entries().len(), 2);
# Ok::<(), Box<dyn std::error::Error>>(())
```

`crates/domain/tests/sql_actions.rs` exercises the kernel without a store.

## Durable rules and copy selection

`CreateRuleWithAction` retains exact source, including whitespace, and compiles
both the filter and the action before admission. Rule reads, fanout, content
preflight and snapshot validation compile retained actions under the same bounded
profile. Programs are never deserialized. Action-bearing rules count toward both
the 2,000-rule subscription limit and the 2,000 SQL/action-rule topic limit,
regardless of their filter kind. An actionless true/false/correlation rule does
not consume the topic SQL quota.

For each subscription, matching actionless rules produce one deduplicated copy.
Each matching action rule produces its own independent copy in canonical rule
name order. Filters inspect the original routing input; actions use the original
bounded AMQP application's canonical typed values, not a caller-supplied legacy
projection. No filter or action observes a sibling's changes. Original AMQP bytes,
duplicate identity and legacy/Azure-text ID mode remain unchanged.

An actionless filter error produces one `FilterException` copy only when no
actionless rule matches. Each action rule whose filter fails independently
produces a `FilterException` copy. A matching action that fails evaluation, type,
patch, projection or effective-copy bounds produces an unchanged original copy
with reason `ActionException` and fixed description
`SQL action failed; no changes were applied`. No partial action output escapes.
Both kinds of error copy follow the subscription's
`dead_lettering_on_filter_evaluation_exceptions` setting: false discards that
copy. A sibling's success neither hides nor duplicates an action-rule failure.
Raw domain messages can still match filters, but matching actions fail per-copy
because there are no AMQP application properties to edit. A malformed legacy
AMQP envelope on an action-enabled topology refuses the command before writes,
rather than creating an unreadable action copy. Actionless behavior is unchanged.
Retaining a raw action-error copy beyond 16 MiB refuses the command instead;
the discard policy does not retain that copy.

All ordinary, batch and scheduled-activation copies and sequences are planned
before staging message writes. Each publication still reserves its historical
base slot, including duplicates and no-match publications. Actionless copies use
that slot. Each retained action success or error copy reserves an additional
parent-topic slot; discarded attempts reserve none. A batch acknowledgement
reports only each publication's base slot. Scheduled intake reserves only the
placeholder: activation reads the then-current rules and reserves a fresh base
and any retained action-copy slots atomically. Sequence exhaustion leaves clock,
history, placeholders, counters and message indexes unchanged.

One command allows at most 4,096 action attempts and a conservative 64 MiB action
plan charge: each attempt charges original/body bytes, canonical system/application
projection text/name/value bytes, and 64 KiB patch allowance before cloning content. Failed and
discarded attempts also count. Exceeding this budget refuses the whole command
with `RuleConfigError::SqlActionPlanLimit`; it is not a per-copy action failure.
Timer scans divide their 256-copy target by the topology's maximum action-copy
fanout, with at least one atomic publication per activation command.

Effective action-copy size charges original bytes plus every SET name/value,
every removal name and five framing bytes per overlay entry. The ceiling is the
subscription message limit plus 64 KiB, capped at 16 MiB. Native parser limits
are unchanged. Later disposition patches preserve removals of unrelated names;
setting a removed name (including to AMQP null) explicitly restores it. Whole
replacement envelopes refuse copies with either SETs or removals.

Action-bearing intent uses proposal V10 (an exact V1/V6 creation envelope plus
UTF-8 source; nesting is nonrecursive), rule V3 (name, filter, creation time,
source), and message V4 (frozen V2 content followed by ID mode and distinct removal
names). Old capture paths refuse action intent and deletion-bearing envelopes.
Actionless message V1..V3, proposal V1..V9 and rule V1/V2 bytes are unchanged.
The V3 action grammar and evaluation semantics are frozen: accepting, refusing
or evaluating retained source differently requires a new rule version.
These are per-record/proposal fences, not a whole-store downgrade barrier.
Rollback needs compatible readers or a coherent pre-feature backup; never strip
the fences or turn removal into null. Finite-topic admission and accounting
remain disabled; action fanout does not introduce a placeholder overdraft.
