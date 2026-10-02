# SQL Actions

The domain command `CreateRuleWithAction`, AMQP rule management, native gRPC,
and `switchyardctl` add a bounded SQL action to a subscription rule.
This first subset removes user properties
only. It does not implement `SET`, system-property mutation, dynamic property
names, arithmetic, functions, parameters, or complete Azure action compatibility.

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
Syntax refusal returns status 400 / `amqp:invalid-field`; unsupported `SET` and
other constructs return 501 / `amqp:not-implemented`; compile limits return
403 / `amqp:resource-limit-exceeded`. Refusals do not echo source text.
Parser diagnostic logging follows the
[SQL rule privacy policy](sql-rules.md#resource-and-diagnostic-boundaries).

Enumeration uses the SQL-action descriptor `0x0000013700000006` with the exact
stored source followed by AMQP **int** 20. This wire compatibility level is not
the domain semantic version 1. No-action rules retain the empty-action descriptor
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
`semantic_version`. Omission selects version 1; other versions return
Unimplemented before compilation. Returned actions always carry version 1,
not AMQP compatibility level 20. The original `CreateRule` request and method
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

`SqlAction` retains the exact expression and semantic version 1. Programs are
ephemeral; neither parser trees nor executable targets are stored. Creation
compiles the source, and each owner rule load recompiles it alongside filters.
Deserialization bounds source and version without compiling. Malformed stored
actions refuse the operation as corrupt metadata, not as no-op actions.

Supported statements are `REMOVE identifier` and `REMOVE user.identifier`.
Square-bracket and double-quoted identifiers follow the existing SQL tokenizer.
Separate statements with semicolons; a final semicolon is optional. Missing
properties are harmless. This follows Azure's documented
[user-property removal behavior](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-sql-rule-action).

Sources are bounded to 4,096 UTF-8 bytes, 1,024 UTF-16 units, 128 physical tokens,
and 32 statements. Whitespace and comments count toward the token bound. Actions
and filters share the existing complete-load compilation allowance of 1 MiB
source, 32,768 tokens, and 32,768 native nodes; each removal charges one node.
These are local admission limits, not cloud quotas.

Value format 11 appends optional action metadata to rule definitions. Explicit
version-1-through-10 decoders add no action; pre-version-10 records still cannot
claim SQL filters. Relabeling a new rule as an old envelope is refused, including
new rules with no action. Existing rule-set accounting uses actual stored bytes,
so a valid 64-KiB legacy rule remains readable even though rewriting it in the
new format would add a byte. New rule writes retain the 64-KiB individual and
256-KiB subscription limits. Durable layout 14 refuses earlier directories;
there is no directory migration tool.

## Copy Semantics

Every filter inspects the untouched publication. Matching action-free rules
combine into one ordinary copy; each matching action rule creates a separate
copy. This is the documented
[multiple-rule copy model](https://learn.microsoft.com/en-us/azure/service-bus-messaging/topic-filters).
Each action transforms only its own copy. Legacy body-only inputs acquire a
typed Data body and canonical message identifier, so adding properties does not
hide their payload from delivery projection.

Removal uses exact property-key bytes, including case. The exact `RuleName`
application property is written last and overwrites any producer value, even
after `REMOVE RuleName`; differently cased keys remain independent. These
collision and case policies are local, not cloud-verified claims. The body and
system fields are otherwise unchanged.

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

## Atomic Admission

Expanded copies, projected metadata, compatibility bodies, legacy typed bodies,
and value items are budgeted before retained payload clones. The existing
1,024-copy, 4-MiB retained-content, and 65,536-value fanout ceilings are unchanged.
Per-message and property/header limits apply to transformed copies too.

Logical refusal, counter exhaustion, and corrupt metadata leave no writes,
counter/history changes, clock advancement, or delivery notifications. Admitted
copies, counters, duplicate history, and applied time share one atomic storage
batch. A physical commit error can leave the outcome indeterminate: the entire
batch may have committed despite the error. No partial fanout is permitted by the
storage contract, but no rollback or retry-idempotency promise follows from an
error. Notifications follow only a reported successful commit.

Scheduled activation retains its bounded fitting-prefix policy; an unfit head
remains pending and cancelable. This is local atomicity, not replicated durability.
