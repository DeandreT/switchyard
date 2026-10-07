# SQL Actions

The domain command `CreateRuleWithAction`, AMQP rule management, native gRPC,
and `switchyardctl` add a bounded SQL action to a subscription rule.
The local subset removes user properties and assigns String, Boolean or
signed-Int64 literals with checked existing-target conversions. Version 1 remains
REMOVE-only; version 2 adds this literal SET subset. System-property mutation,
dynamic names, NULL right-hand sides, property expressions, arithmetic, functions,
parameters and complete Azure/CLR action conversion compatibility are unsupported.

Rule reads must not discard action metadata. Native Get and List require explicit
`include_actions` opt-in when returning action-bearing definitions; omission
retains Unimplemented rather than a successful empty action. The CLI opts in
automatically. AMQP enumeration and trusted broker reads retain complete actions.

## AMQP Management

`com.microsoft:add-rule` accepts an optional `sql-rule-action` map containing
only a string `expression`. Omission or null means no action, not an empty SQL
program. The edge checks borrowed source bounds before copying, compiles the
supported grammar, and submits the typed command through the admitted
subscription binding. The owner independently validates and recompiles it.
Syntax refusal returns status 400 / `amqp:invalid-field`; unsupported targets or
right-hand expressions return 501 / `amqp:not-implemented`; compile limits return
403 / `amqp:resource-limit-exceeded`. Refusals do not echo source text.
Parser diagnostic logging follows the
[SQL rule privacy policy](sql-rules.md#resource-and-diagnostic-boundaries).

Enumeration uses the SQL-action descriptor `0x0000013700000006` with the exact
stored source followed by AMQP **int** 20. This wire compatibility level is not
the stored domain semantic version. Unversioned AMQP creation selects version 2,
while enumeration preserves source without encoding that local version as 20.
No-action rules retain the empty-action descriptor
`0x0000013700000005`. These shapes follow Microsoft's
[rule request/response contract](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-request-response#rule-operations).
Pagination and complete response-size checks are unchanged; an oversized page
is refused rather than returned as a truncated success.

All rule operations still require Listen on the subscription's management
endpoint. Permission checks precede parsing and owner access. Request fields and
associated-link names cannot redirect the admitted child binding, and deletion
followed by recreation does not refresh an old management link. This is separate
from the native administration API's Manage permission.

With the official .NET client, use `ServiceBusRuleManager`, not the HTTP
administration client:

```csharp
var rules = client.CreateRuleManager("events", "audit");
await rules.CreateRuleAsync(new CreateRuleOptions("RedWithoutColor",
    new SqlRuleFilter("color = 'red'"))
{
    Action = new SqlRuleAction("REMOVE user.color;"),
});
```

Delete the explicit `$Default` rule when its unchanged action-free copy is not
wanted. Creating an action rule does not replace an existing rule or transform
messages already retained by the subscription.

The following records the earlier REMOVE-only SDK baseline, not verification of
version-2 literal SET. Its results and runner scope remain historical.

Both pinned official .NET clients, 7.21.0 and 7.20.2, have separate opt-in
action gates on memory and Fjall storage. They verify exact source enumeration,
three independently settled copies, original-filter independence, exact-key
removal and final RuleName replacement, preserved body/system/footer content,
and awaited unsupported `SET` refusal followed by a healthy publication.
Defaults and empty runtime state are checked before and after reopen. Run with:

```sh
cargo test -p server --test amqp_dotnet_current --locked -j 2 -- rule_actions --ignored --test-threads=1
```

The new action gates use isolated certificate trust and bounded child-process
execution. They establish local interoperability, not live-cloud parity for
the local semantics below.

## Native And CLI

Native `RuleService.CreateRuleWithAction` requires both a filter and a
`SqlRuleAction` containing exact `expression` text and an optional
`semantic_version`. Omission selects version 2; explicit versions 1 and 2 are
accepted, and other versions return Unimplemented before source copying or
compilation. Returned actions carry their exact stored version, not AMQP
compatibility level 20. The original `CreateRule` request and method
remain action-free. A separate RPC ensures an older server refuses the method
rather than silently dropping an unknown action field and creating the wrong rule.

Get and List default `include_actions` to false. Get then refuses a selected
action rule, and List refuses the complete response if any member has an action.
A plain selected rule remains readable beside valid action siblings. With true,
the exact source and version are returned; action-free rules have no action.
All read modes validate the complete stored rule set before representation and
retain the existing clock-free reads, generation fences, and response bounds.
Manage authorization precedes action compilation and store access. Syntax,
unsupported grammar, and limits use InvalidArgument, Unimplemented, and
ResourceExhausted respectively, with static source-private errors.

Add an optional action file to CLI creation:

```text
rule create events audit RedWithoutAudit --filter-file examples/rules/red.json --action-file examples/rules/remove-audit.json
```

The strict JSON shape is `{"type":"sql","expression":"REMOVE user.audit;"}`
with an optional unsigned `semantic_version`; explicit null and unknown or
duplicate fields are refused. Action files are bounded regular files of at most
32 KiB. Both files and the complete encoded request are checked before connecting.
The CLI forwards well-typed source and versions to the authoritative server,
uses only the new RPC when an action is supplied, and never falls back to an
action-free creation. Get/List request complete action metadata automatically
and validate all output before emitting JSON. See
[Native Rule Administration](native-rules.md) for the full contract.

## Grammar And Storage

`SqlAction` retains the exact expression and semantic version, not parser trees
or executable targets. `new` selects version 2; `with_semantic_version` accepts
explicit 1 or 2. Constructor/compiler and native admission reject unknown
versions before converting, copying or tokenizing source. The stored derived
deserializer instead consumes its bounded source visitor before final version
validation; it does not provide an early-version decode guard. Creation and every
owner rule load compile according to the stored version. A stored version-1 SET
never becomes executable under version 2. Malformed stored actions refuse the
operation as corrupt metadata, not as no-op actions.

Both versions support `REMOVE identifier` and `REMOVE user.identifier`. Version 2
also supports `SET identifier = literal` and `SET user.identifier = literal`.
Literals are single-quoted strings, TRUE/FALSE and decimal signed-Int64 integers,
including unary plus/minus and `-9223372036854775808`. Fractional, exponent and
out-of-range integer literals are unsupported rather than coerced to Double.
Square-bracket and double-quoted identifiers follow the existing SQL tokenizer.
Separate statements with semicolons; a final semicolon is optional. Missing
removal targets are harmless. Microsoft documents a broader
[action language](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-sql-rule-action);
this finite local subset does not implement its full expressions or conversions.

Sources are bounded to 4,096 UTF-8 bytes, 1,024 UTF-16 units, 128 physical tokens,
and 32 statements. Whitespace and comments count toward the token bound. Actions
and filters share the existing complete-load compilation allowance of 1 MiB
source, 32,768 tokens, and 32,768 native nodes. Each target and literal charges
its compiler nodes, with an additional node for a signed integer expression.
These are local admission limits, not cloud quotas.

Value format 11 appends optional action metadata to rule definitions. Explicit
version-1-through-10 decoders add no action; pre-version-10 records still cannot
claim SQL filters. Relabeling a new rule as an old envelope is refused, including
new rules with no action. Existing rule-set accounting uses actual stored bytes,
so a valid 64-KiB legacy rule remains readable even though rewriting it in the
new format would add a byte. New rule writes retain the 64-KiB individual and
256-KiB subscription limits. The envelope remains version 11 while actions
preserve their separate version-1 or version-2 meaning. Durable layout 15
refuses earlier directories even without SET-bearing rules. Replica, catalog
and protected profile layout numbers also advance with `ACTIVE_STORE_FORMAT`;
their profile-v1 tags are unchanged. No directory migration or rollback
conversion is supplied; see [Durable Format](compatibility.md#durable-format).

## Copy Semantics

Every filter inspects the untouched publication. Matching action-free rules
combine into one ordinary copy; each matching action rule creates a separate
copy. This is the documented
[multiple-rule copy model](https://learn.microsoft.com/en-us/azure/service-bus-messaging/topic-filters).
Each action transforms only its own copy. Legacy body-only inputs acquire a
typed Data body and canonical message identifier, so adding properties does not
hide their payload from delivery projection.

Both REMOVE and SET use exact property-key bytes, including case. SET lookup
does not use SQL filters' case-folded comparison. The exact `RuleName` application
property is written last and overwrites the producer or action value, even after
`REMOVE RuleName` or `SET RuleName = ...`; differently cased keys remain
independent. These collision and case policies are local, not cloud-verified
claims. The body and system fields are otherwise unchanged.

A missing or present-Null SET target acquires the literal's canonical String,
Bool or Long constructor. An existing String accepts only a string literal, and
an existing Bool only a Boolean literal. Integer targets preserve their Byte,
Ubyte, Short, Ushort, Int, Uint, Long or Ulong width and signedness, with checked
range conversion from the signed-Int64 literal. There is no String-to-number or
Boolean coercion, float/decimal conversion, or String-to-UUID/timestamp/CLR
Guid/DateTime/DateTimeOffset/TimeSpan/Uri conversion. Symbol, binary, character,
described and compound existing targets likewise have no supported conversion.

Statements see their own preceding checked changes: SET after SET consults the
resulting type/value; REMOVE followed by SET treats the target as absent. These
small plans retain indices/scalars rather than copied producer values. Every
action still starts from the independent original publication, never a sibling
action's output.

All original input/activation sequences are allocated first, preserving
acknowledgements and action-free copy sequences. Retained action copies receive
additional sequences from the parent topic counter in sorted subscription/rule
order. An action-only match can leave its acknowledged input sequence unused by
a retained copy. This allocation order is local, not an Azure ordering guarantee.
Copies have independent receive and settlement state. Future publications still
retain only the parent record and use current rules and actions at activation.

The existing subscription-wide finite filter-error policy is unchanged: a
filter error suppresses all successful matches on that subscription and routes
one original error copy, or drops it when the policy is disabled. Missing-session
routing applies to each selected copy, including independently transformed
action copies. Removal itself has no producer-dependent failure path.

A finite SET conversion failure yields one dead-letter copy for that matched
action, with reason `SwitchyardSqlActionError` and description `TypeMismatch`,
`UnsupportedTargetType` or `NumericOverflow`. No source, property name or producer
value appears in these fields. All earlier changes from that action are discarded;
its copy preserves the original envelope plus final exact RuleName, while base
and healthy action/subscription siblings continue. The shadow copy strips
normalized session and lifetime and keeps the scheduling annotation. This
action-error route precedes missing-session routing and does not depend on
`dead_lettering_on_filter_evaluation_exceptions`; that option still governs only
filter errors. The local reasons, precedence and sibling policy are not claims
of Azure parity. Authorized rule reads intentionally return exact source; static
refusals and fixed error fields do not imply every trusted DTO or Debug value
redacts that source.

## Atomic Admission

Expanded copies, projected metadata, compatibility bodies, legacy typed bodies,
and value items are budgeted before retained payload clones. The existing
1,024-copy, 4-MiB retained-content, and 65,536-value fanout ceilings are unchanged.
Per-message and property/header limits apply to transformed copies too, including
version-2 intermediate checked measurements and the final RuleName projection.
A conversion failure cannot launder an invalid original envelope or conceal
independent limits. Failure copies account for original content, final RuleName
and fixed dead-letter fields against all fanout ceilings.

The shared 1,048,576-work-unit and 32-MiB comparison-byte allowance conservatively
precharges possible action checks and projections before those operations. It
counts section-sensitive value-node/body-container visits, property/annotation/
footer and fixed positional-property visits, map and overlay candidates, and
bounded statement/literal/target scans. Repeated original-target and final
RuleName measurements are included. All possible programs are charged even on
duplicate inputs or false filters. This is an admission model, not a bound on
every instruction, allocator operation, native buffer, RSS, CPU or wall time.

Logical refusal, counter exhaustion, and corrupt metadata leave no writes,
counter/history changes, clock advancement, or delivery notifications. Admitted
copies, counters, duplicate history, and applied time share one atomic storage
batch. A physical commit error can leave the outcome indeterminate: the entire
batch may have committed despite the error. No partial fanout is permitted by the
storage contract, but no rollback or retry-idempotency promise follows from an
error. Notifications follow only a reported successful commit.

Scheduled activation selects a fitting due prefix only for the three aggregate
admission errors `IngressBatchLimitExceeded`, `TopicFanoutTooLarge` and
`TopicRuleMatchTooLarge`. An aggregate-unfit first head remains pending and
cancelable. A later per-copy property/header/message/value or shape failure,
including malformed scheduling or rule metadata, instead refuses the entire
selected activation: no earlier fitting candidate commits. The selected source
deletions, active/dead-letter puts, counters and clock share the original batch.
This is local atomicity, not replicated durability.

## Version-2 Verification

The bounded literal SET increment passed locked default and all-feature workspace
tests: each run reported 5,199 passed and 11 ignored. The 40 added regular cases
represent 30 logical cases, including ten paired memory/Fjall cases. There are no
new compile-fail, no-run or ignored cases. Every prior owner's case/status
multiset and every ignored reason was retained; parallel completion order is not
part of that evidence. The focused compiler/evaluator, domain action, projection,
AMQP rule-management and durable-format runs passed 29, 48, 2, 33 and 2 cases.

Both default and all-feature strict Clippy and explicit workspace builds passed,
as did formatting, protobuf generation and whitespace checks. The generated
administration descriptor remained byte-identical: literal SET needs no protobuf
shape change. The verified source graph contains 40 paths, disjoint from the
preceding retained-ingress SDK source increment.

All eleven opt-in official .NET SDK gates passed. The existing rule-action gates
now extend the earlier REMOVE baseline with finite literal SET on clients
7.21.0 and 7.20.2, each against memory and Fjall: four backend/client executions.
They check exact source enumeration, typed Long/Boolean/String values, exact-key
removal, final RuleName, original-filter independence, preserved envelope content
and independent settlement. The finite
conversion failure preserves its original error copy and healthy siblings.
Including the earlier REMOVE publications, the workflow allocates 13 parent
sequences and 17 physical copies; the next sequence is exactly 14. Reopen checks
retain the final cleanup `$Default` definitions, counters and empty runtime
message state after settlement, not the deleted literal action definitions.
The management unit tests separately check the raw signed-int compatibility
level 20 field. The original SDK gate names remain unchanged.

Initial formatting, test-only import, split-module path and private return-type
lint failures were retained and corrected narrowly. Two complete scheduling test
bodies were moved to a child module; no test was disabled or weakened. One SDK
launcher failed before Cargo started because of an orchestration prefix lookup;
the subsequent actual SDK run completed successfully. These are local execution
results, not observations of live Azure conversion behavior or CLR compatibility.

Verification reused the existing build cache with two low-priority CPU cores.
Its admission assertions do not bound RSS or every native allocation, and the
durable checks do not establish power-loss survival, replicated quorum behavior,
rollback after a physical commit error or permission to reopen earlier layouts.

## Current AMQP Literal SET Revalidation

Both existing current-stable and previous-stable `rule_actions` opt-ins passed
this fresh current-source revalidation: each selected run reported one passed,
zero failed or ignored, and 77 filtered tests. The 54 frozen source paths were
unchanged before and after both serial gates. This does not replace the
historical REMOVE-only or Version-2 Verification receipts above or introduce
new test cases. Only these two opt-ins were rerun; the other 19 SDK opt-ins and
the preceding 6,042-pass workspace run remain historical, not new executions.

Each gate configures its build for `Azure.Messaging.ServiceBus` 7.21.0 or 7.20.2
and uses one `ServiceBusClient` with named-key credentials over TLS-protected
AMQP TCP with isolated private-CA trust. Its Rust runner visits memory and Fjall
sequentially. Each passing gate
required successful child exit, an exact completed success-marker line, fixture
cleanup, retained-state checks, and snapshot equality plus repeated checks after
provider reopen. These are source-bound assertions: the successful Rust output
does not print separate backend finish lines or the captured child marker. This
runner does not record loaded DLL fingerprints or loaded-file custody, so the
release names identify configured package pins, not independently measured
loaded assembly identities.

The literal workflow enumerates the exact action/filter source, sets String,
Boolean and signed-Int64 application values, and keeps the filters evaluated
against the original publication. It receives all three successful alpha copies
before settling them independently out of order, while checking the untouched
beta sibling and any error copy after each settlement. An incompatible Boolean
assignment produces `SwitchyardSqlActionError` / `TypeMismatch` in the alpha
dead-letter queue; an earlier REMOVE in that failed action is rolled back, and
healthy siblings remain independently available. The checks include final
RuleName collision handling, exact application keys/values, body and selected
system metadata, content encoding and one fixed string footer.

The complete REMOVE-plus-literal workflow checked 13 allocated parent sequences,
17 physical copies and next sequence 14. Final retained-state checks preserved
both subscriptions, only their restored no-action `$Default` rules, counters
and ten empty runtime key families before and after provider reopen; they do
not retain the deleted literal action definitions or form a transient RulePut,
action-version, timestamp or append-batch oracle. This is a local fixed-workflow
AMQP check, not both client credential constructors, an HTTPS-created-action-to-
AMQP bridge, arbitrary action grammar or cloud conversion parity. Memory reopen is not disk recovery;
Fjall reopen is not a power-loss or replicated-quorum claim.
