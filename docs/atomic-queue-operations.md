# Atomic Queue Operations

The deterministic core and trusted broker API can apply a bounded group of
messaging operations on one primary non-session queue. This is a foundation
for transactions, not AMQP transaction compatibility. Coordinator links,
Declare/Discharge, transaction IDs, staged wire acknowledgments, wire timeouts,
cross-entity transactions, and successful-request retry deduplication are not
implemented by this API.
The separate [AMQP transaction types](amqp-transaction-types.md) are decoded but
refused by the connection drivers; they do not expose this trusted API on the wire.

## Scope And Admission

`AtomicMessagingCommand` carries one `EntityBinding`, one `issued_at`, and an
ordered vector of ordinary commands. Every member must have that exact
namespace, target, and timestamp. The envelope is separately serialized;
ordinary command discriminants and stored records are unchanged. There is no
storage-layout or value-format bump for this foundation.

Allowed commands are `Send`, `SendEnvelope`, immediate `SendBatch`, `Complete`,
`Abandon`, `Defer`, `DeadLetter`, and `Settle` with application-property updates.
Any scheduled timestamp in an ingress member is refused, including a past
timestamp. Session queues, session-bearing sends, topics, subscriptions, direct
DLQ targets, receives, renewal, scheduling, metadata changes, and timer commands
are outside the initial scope. An allowed settlement can still enqueue in the
queue's canonical dead-letter shadow.

Admission checks the current queue generation, primary kind, numeric
configuration, and shadow configuration before consulting the applied clock.
A stale group cannot mutate a newly recreated queue. Input validation and
current-identity checks are repeated by deterministic application, rather than
trusting proposer admission alone. An empty group still validates its binding
and configuration, but reads no clock and commits nothing.

`LocalProposer::propose_atomic_messaging` consumes owned operation intents and
stamps the nonempty group exactly once on the broker owner. The
`BrokerHandle::submit_atomic_messaging` and blocking counterpart use that same
owner queue. These Rust APIs are trusted entry points: a binding proves entity
identity, not permission. They introduce no native administration RPC or AMQP
endpoint and do not replace ordinary per-link authorization.

## Preparation And Commit

Each operation prepares its ordinary mutations against a private point-read
overlay. Later operations see earlier prepared counters, duplicate history,
message states, locks, and indexes. Preparation cannot scan, snapshot, or
commit to the backing store. Only the final normalized batch reaches the real
store, once; each key occurs once, including the global clock and counters.

Duplicate sends retain ordinary semantics: they consume acknowledged sequence
numbers even when no message survives. Duplicate history is shared across the
entire group, including between separate sends and ingress batches. Held-message
operations check their live token and deadline at the group's shared timestamp.
A lock that expired by that timestamp rejects all operations, even if it was
live when the caller first acquired it.

Any late validation, counter exhaustion, corrupt record, budget, or storage-read
failure discards preparation. Message state, counters, duplicate history, and
applied clock remain unchanged. The result contains ordered
ordinary outcomes and sorted enqueue destinations derived only from final
source/shadow ready-index Puts. The broker wakes only those destinations, after
successful storage commit. A duplicate-only group can commit bookkeeping
without waking a receiver.

A final physical commit error does not prove that nothing reached storage.
For example, a complete journal batch can be written before a failed sync is
reported, and reopening can recover the whole group. The store preserves atomic
all-or-none recovery, but the caller's decision can be unknown. An error returns
no successful outcomes or enqueue effects and publishes no receiver wakeup;
it is not proof of rollback or permission to retry blindly.

Isolation depends on the existing single broker owner. This API does not make
uncoordinated direct writes to a shared store serializable.

## Local Resource Bounds

All caps apply to the whole group, not separately to each member:

| Budget | Maximum |
| --- | --- |
| Operations | 100 |
| Logical sent messages, including duplicate drops | 100 |
| Borrowed command-content tally | 4 MiB |
| Shared command-input value nodes | 65,536 |
| Point reads, including repeated overlay hits | 4,096 |
| Cumulative point-read key bytes | 1 MiB |
| Cumulative point-read value bytes | 16 MiB |
| Unique mutation keys, including deleted keys | 4,096 |
| Unique mutation-key bytes | 1 MiB |
| Cumulative generated Put value bytes | 16 MiB |

The input tally includes compatibility bodies, conservative typed-envelope
content, normalized IDs, property-update key/value content, and dead-letter
reason/description bytes. One bounded borrowed traversal counts value nodes
across all sections and updates before ordinary content validation can clone
compound values. Empty body sections consume conservative section overhead,
so they cannot evade the work bound. Existing per-message depth, property,
header, and queue limits still apply.

Read limits cover each domain validation/application pass. Proposer admission
is a separate bounded pass, and stamping has its existing applied-clock read.
Generated Put bytes count every prepared value, even if a later operation
replaces the same key. These are deterministic discovery/work limits, not a
total memory or process-RSS guarantee. A backend point read materializes one
value before its length can be rejected; the existing record decoder is not
replaced here.

The 100-message ceiling aligns with the documented
[Service Bus send transaction quota](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-quotas).
Other ceilings, and the narrower allowed operations, are local policies, not
claims of Azure quota or transaction parity. Azure also supports lock renewal
within a transaction; see its
[transaction overview](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-transactions).

## Cancellation And Replay

For the original `submit_atomic_messaging` API, dropping a caller's future or
reply receiver does not cancel an admitted owner operation. The group can still
commit and wake receivers. A separate [guarded commit API](atomic-commit-permits.md)
adds pending-only cancellation and a compact runtime decision, without changing
this original behavior. Retrying after an
unknown result can therefore apply it again; there is no request ID or terminal
decision record in this foundation. An explicitly injected failure before the
backing apply leaves a group unchanged and can be retried; an arbitrary physical
commit error does not provide that guarantee. A successfully applied group is
not replay-idempotent. Replicated replay
still assumes each instruction is applied once in log order.

Both storage backends are exercised for read-your-writes, aggregate rejection,
late rollback, committed effects, and owner/cancellation behavior. Durable
tests also reopen committed state. Wire transaction support needs separate
protocol and official-client gates before it can be marked compatible.
