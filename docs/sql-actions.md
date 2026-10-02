# SQL Actions

The trusted domain command `CreateRuleWithAction` adds a bounded SQL action to a
subscription rule. This first subset removes user properties only. It does not
implement `SET`, system-property mutation, dynamic property names, arithmetic,
functions, parameters, or complete Azure action compatibility.

AMQP, native gRPC, and `switchyardctl` cannot create actions yet. Their rule reads
must not discard action metadata: native Get refuses an action-bearing rule,
native List refuses any action-bearing member, and AMQP enumeration refuses an
action-bearing set before pagination. Those refusals are Unimplemented / status
501, not successful empty actions. Native deletion can still remove a valid
action-bearing rule. The trusted broker's rule reads retain the complete action.

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
