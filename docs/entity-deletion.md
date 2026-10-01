# Entity Deletion

Native administration and `switchyardctl` support synchronous destructive
deletion of queues, topics, and subscriptions. This is a local atomic cleanup
policy, not Azure Atom/XML administration compatibility.

## Request Contract

`DeleteEntityRequest` preserves namespace and path at protobuf fields 1 and 2.
The additive `kind` at field 3 selects queue, topic, or subscription. Unspecified
kind preserves generic callers: the owner resolves a primary queue or topic
inside the deletion command, while a canonical subscription path selects that
subscription. A wrong expected kind is refused atomically, not implemented as
a potentially racy get-then-delete sequence.

Manage authorization runs before path validation, kind selection, and owner
access. Native scopes retain literal parent/member spelling and case; only the
structural `/subscriptions/` separator is canonicalized. Exact-child grants
cannot delete their parent or siblings. Dead-letter paths remain reserved.
Azure's [SAS rights contract](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-sas#rights-required-for-service-bus-operations)
also identifies Manage as the topology-management permission.

A successful response is `Operation` with `state` equal to `completed` and empty
`operation_id` and `error`. The response follows the storage commit; there is no
persisted background job or operation to poll. Missing entities return NotFound,
including repeated deletion. This is consistent with the documented missing
target errors of [DeleteQueueAsync](https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.administration.servicebusadministrationclient.deletequeueasync?view=azure-dotnet)
and [DeleteTopicAsync](https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.administration.servicebusadministrationclient.deletetopicasync?view=azure-dotnet),
not a claim that every Azure control plane has identical idempotency semantics.

The CLI commands are `queue delete NAME`, `topic delete NAME`, and
`subscription delete TOPIC NAME`. They send an explicit expected kind and
report the synchronous operation as JSON. TLS, token-file handling, loopback-only
plaintext opt-in, and request deadlines are unchanged.

## Owned State

Queue deletion removes its configuration, its shadow configuration, all message
states, ready/lock/expiry indexes, session records/holds/state and session
indexes, scheduled records/indexes, and duplicate history/deadline indexes.
The same runtime cleanup applies to each subscription backing and its shadow.

Subscription deletion also removes its membership and all its rule records.
The parent topic, sibling subscriptions, parent scheduling, and parent duplicate
history are retained. Topic deletion cascades to every committed subscription,
their rules, backings, and shadows, plus the topic's configuration and parent
runtime. Namespace, prefix-neighbor, and literal-case owners are never widened
into that scope.

Deletion validates existing kinds, complete configuration projections, and
membership before staging the batch. Topic membership is bounded at 32;
canonical descendant metadata/runtime/rule keys must belong to those members.
Orphan or malformed topology is refused rather than repaired. A targeted
subscription does not validate or scan unrelated siblings. Rule keys and owner
membership are validated, but rule expressions and message payloads are purged
opaquely: deletion does not compile SQL or recursively decode message content.

## Atomic Limits

One command may retain at most 4,096 unique deletion keys and 1 MiB of their key
bytes, counting owned metadata and runtime together. Its purge/discovery scans
have a 16 MiB accounted value-byte allowance. Every returned row is charged,
including refusing overflow probes and repeated discovery of rows subsequently
scanned for removal. Fixed bounded configuration
and counter validation reads are additional. Retained counter and incarnation
tombstones and the global clock are not deletion keys. Incarnation retirement
adds at most 33 fixed owner-record reads and Puts to a topic cascade, outside
the deletion-key budget; those writes share the same atomic batch.

Runtime, rule, and orphan-discovery walks fetch one row at a time. Values are
discarded after accounting; only deletion keys are retained. A returned value
can itself exceed the limit before it is rejected, because the storage API
returns the value as a whole. These are work and retained-plan limits, not a
hard allocator or process-memory guarantee.

The whole queue, subscription, or topic shares one budget. There is no fitting
prefix, staged background cleanup, or partial success. Exceeding any limit
returns ResourceExhausted with the exact store and applied timestamp unchanged.
Clock regression and storage failures likewise leave state unchanged. Successful
cleanup and the ordinary applied-clock advance commit in one storage batch.
Large entities must be drained or otherwise reduced before retrying deletion.

## Recreation Fences

Per-scope sequence and lock counters intentionally survive as tombstones. A
recreated queue continues its source sequence and token allocation; a recreated
topic continues its shared publication sequence, while subscription/shadow
local lock counters also remain. Old receipts, scheduled handles, and session
holds cannot match normally generated new records merely because a name is
reused. New subscriptions receive a fresh default rule; old session state and
duplicate history do not survive cleanup.

Existing counters must decode to valid numeric ranges, including exhausted
sentinels. Source runtime/message evidence requires its source fence; session
records and lock/session-lock index evidence require local token fences. Fresh
subscription or shadow copies may legitimately lack local counters before
their first lock. Missing fences are never silently reconstructed. This is not
a guarantee against arbitrary manual counter tampering: a used empty entity
whose counter was externally erased cannot be distinguished from an untouched
lazy entity. Counter tombstones currently have no garbage collection and may
accumulate as names are deleted. Value format 10 remains unchanged; store layout
13 additionally requires retained incarnation records.

## Live Links

Only a committed deletion reports exact removed destinations to the broker.
All registered receivers waiting on those backings and shadows are notified;
their next broker operation refuses the retired identity and closes the link,
even if the same name was already recreated. Failed or refused deletion emits
no wakeup. Unrelated receivers and the connection remain usable.

Each admitted data or management endpoint retains its exact entity identity.
Deletion retires that identity; recreation allocates a new one. An old endpoint
cannot make fresh operations against the replacement, including pure rule reads,
management operations, cleanup, and session acceptance. Management associated
links and reply routes must have the same admitted identity as their request
endpoint. See [Entity Incarnations](entity-incarnations.md) for the fence contract.

There is no global entity-link retirement registry. An idle producer or a link
currently delivering a locked message discovers deletion on its next operation,
not through a promised immediate global detach. Replies already committed for
the old incarnation can still drain; no replacement operation is performed.
