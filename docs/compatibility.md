# Compatibility

Switchyard targets the Azure Service Bus Standard messaging model. A capability
is marked supported only after it has protocol-level tests and end-to-end
coverage with the relevant client.

## Client Gates

| Client | Data plane | Administration | Status |
| --- | --- | --- | --- |
| Official .NET SDK, current stable | Send, peek, receive, abandon/defer/dead-letter property updates, renew, complete, schedule, cancel, duplicate detection and message properties; session renew/state/scheduling | Planned | Experimental gate on 7.21.0 |
| Official .NET SDK, previous stable | Same gated workflows as current | Planned | Experimental gate on 7.20.2 |
| Sift pinned revision | Planned | Planned | Not implemented |

## Capability Matrix

A capability reaches **State machine** once the deterministic broker core
implements it with tests. That is a prerequisite for compatibility, not a form
of it: nothing below is reachable by a client until the protocol edge exists.

| Capability | Target release | Status |
| --- | --- | --- |
| AMQP 1.0 over TLS | Pre-1.0 | Protocol edge, Rust client end to end |
| AMQP over WebSockets | Pre-1.0 | Not implemented |
| SASL PLAIN and CBS SAS/JWT | Pre-1.0 | PLAIN and CBS SAS: protocol edge, Rust client end to end. JWT: not implemented |
| Queue send, receive, and settlement | Pre-1.0 | State machine |
| Message properties and AMQP body preservation | Pre-1.0 | State machine and AMQP mapping; typed properties, application values, annotations, footer and all body kinds. Rust clients on both backends and official .NET property gate |
| Peek without lock acquisition | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Receive-delete | Pre-1.0 | State machine, AMQP mapping |
| Lock expiry and redelivery | Pre-1.0 | State machine |
| Message lock renewal | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Time-to-live expiry | Pre-1.0 | State machine |
| Topics and subscriptions | Pre-1.0 | Not implemented |
| Correlation and SQL filters/actions | Pre-1.0 | Not implemented |
| Scheduling and cancellation | Pre-1.0 | State machine, AMQP management and send-annotation mappings, Rust and current .NET clients end to end |
| Deferral and deferred receive | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Dead-letter | Pre-1.0 | State machine, AMQP mapping |
| Dead-letter receive and resubmit | Pre-1.0 | Receive: state machine, AMQP mapping. Resubmit: not implemented |
| Sessions and session state | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Duplicate detection | Pre-1.0 | State machine, AMQP send/scheduling mappings, Rust and current .NET clients end to end |
| Same-placement-group transactions | Pre-1.0 | Not implemented |
| Atom/XML entity and rule administration | Pre-1.0 | Not implemented |
| Native gRPC administration | Pre-1.0 | Contract scaffolded |
| Partitioned entities | Later | Out of initial scope |
| Cross-placement-group transactions | Later | Out of initial scope |
| Geo-replication | Later | Out of initial scope |
| Premium-tier features | Uncommitted | Out of scope |

## Broker Core

The `domain` crate applies each replicated command as one atomic storage batch
and derives every deadline from the timestamp carried on that command, so a
follower replaying the log reaches the same state as the leader. Delivery
behavior it currently enforces:

- Peek-lock delivery is at-least-once: the lock commits before the message is
  handed out, and completion removes it only after settlement commits.
- Receive-delete is at-most-once: the deletion commits before the transfer.
- Peeking browses stored messages by sequence number without changing delivery
  counts, acquiring locks, or consuming from the queue.
- Deferral removes a locked message from the ready path while keeping its
  sequence number. A deferred receive by sequence can lock it again or consume
  it in receive-delete mode. A deferred message remains discoverable by peek
  after its lifetime ends; expiry is applied when a deferred receive reaches it.
- Scheduling keeps messages visible to peek but out of the ready path until
  their requested time. The timer atomically activates each due message under
  a new sequence number, appending it to the queue, and starts its lifetime at
  that activation. The requested scheduling timestamp remains attached.
  Scheduling a batch is atomic, including validation failures. Cancellation
  removes the scheduled record and deadline entry atomically; it rejects a
  missing or already-active handle. Exact Azure behavior for those rejected
  cancellation cases has not yet been verified.
- A queue can enable duplicate detection by message ID, with a 10-minute
  default history window bounded to 20 seconds through 7 days. A duplicate
  send is accepted and dropped, and history survives completion, dead-lettering,
  and schedule cancellation. Ordinary and scheduled sends share history;
  activation does not check it again. History is scoped to the namespace and
  entity, independent of the session. Anonymous messages bypass detection,
  and dropped retries do not extend the original deadline; exact Azure parity
  for these two edge cases remains unverified. A duplicate scheduling request
  receives a fresh sequence handle but stores no message, so that handle has
  nothing to cancel.
- Message identifiers are bounded to 128 UTF-16 code units, matching the
  official .NET client. Overlong identifiers are rejected before enqueue or
  duplicate-history changes, including within an atomic scheduled batch.
- A settlement is rejected unless it presents the live lock token, and rejected
  again once the lock deadline has passed.
- Abandon, defer and explicit dead-letter operations merge application-property
  updates into the held message atomically. Validation checks the complete
  resulting content before changing the lock or state. Legacy byte-body records
  gain typed content without losing their body or identifier. Reason and
  description on explicit dead-letter operations are bounded to 4,096 UTF-16
  units, matching the official client's argument limit.
- A live message lock can be renewed without changing its token. Renewal moves
  the replicated deadline record and its expiry index in one storage batch.
- Abandoning a message, or letting its lock elapse, returns it to the queue
  until it reaches the queue's maximum delivery count, after which it is
  dead-lettered as `MaxDeliveryCountExceeded`.
- A queue's default time to live is also a ceiling for an explicit message
  lifetime, including scheduled messages. Lifetime starts at enqueue/activation.
  A live message lock protects an expired message: completion and renewal
  remain valid. Abandonment or lock expiry applies the elapsed lifetime
  immediately, ahead of the delivery-count limit. Unlocked expired messages are
  dead-lettered as `TTLExpiredException` by the timer or a receive that reaches
  one first. Configurable drop-versus-dead-letter on expiry remains unfinished.
- The dead-letter queue is a queue: `entity/$deadletterqueue` is drained with
  the same receive and settlement machinery as its parent. Messages arrive
  there stripped of lifetime and session, keep their sequence numbers and the
  reason they were dead-lettered, and never dead-letter again — abandoning in
  a dead-letter queue always returns the message to it. The path is reserved:
  it cannot be created or sent to directly.
- Rejected commands write nothing, so every replica rejects at the same point.
- On a queue that requires sessions, a message carries a session identifier and
  is only delivered to a receiver holding that session's lock. Ordering is
  guaranteed within a session, which is the only FIFO guarantee made. A session
  lock is exclusive and expires on its own deadline; session state outlives the
  receiver that set it. A receiver holding the session can renew that lock and
  read, replace, or clear the opaque state through the management node.

Three session behaviors deliberately differ from Azure Service Bus, and each is
a rejection or a bound rather than a silent difference:

- A session identifier on a queue that does not require sessions is refused
  rather than carried, because it would promise an ordering that queue cannot
  keep. Azure accepts and ignores it.
- Settling a message inside a session needs the message's own lock token, not a
  live session lock. Azure fails settlement once the session lock is lost. The
  message lock is treated as the authority over that message, so a receiver that
  did the work can still settle it.
- Accepting the next available session examines a bounded number of sessions and
  reports none available if they are all held, rather than walking the entity.
  The receiver retries.

The `server` crate's timer worker proposes scheduled activation, lock,
time-to-live, session-lock, and duplicate-history sweeps on an interval, so a
running node activates what is due and releases or prunes what has elapsed.

An AMQP 1.0 client can reach a queue. The node accepts AMQP over TLS with the
socket secured before the protocol handshake, as Service Bus port 5671
requires. Plain TCP remains available only in development mode. A configured
shared-access policy accepts either SASL PLAIN credentials or SASL ANONYMOUS
or Microsoft's equivalent `MSSBCBS` mechanism followed by a CBS SAS token. CBS
grants are scoped to a namespace or entity and to Send, Listen, or Manage; they
authorize links connection-wide and close an open link when its token expires.
A connection without a valid grant gets 20 seconds to complete CBS
authorization. JWT, OIDC, and mTLS are not implemented.
The edge resolves a link's address to an entity, turns transfers into send
commands and dispositions into settlements, and answers a rejection with the
condition an SDK keys its behaviour off. A receiving link's settle mode selects
the delivery guarantee: unsettled is peek-lock, pre-settled is receive-delete.
Peeking is served through the entity's `$management` request/reply links and
returns encoded AMQP messages without touching their broker state.
Deferred receive is also served through `$management`, and locks returned that
way are settled through the management `update-disposition` operation.
Modified outcomes carry application-property updates; SDK dead-letter outcomes
carry the reason, description and updates in their error information. Management
settlement accepts `properties-to-modify` and promotes reserved dead-letter
fields when explicit reason/description fields are absent. Explicit fields win
when both forms are present. Null property values are retained, not interpreted
as deletion; these precedence and null policies have not been compared with a
live Azure namespace. A malformed or refused direct settlement closes its link
with the relevant condition and leaves the lock to expire.
Second-mode settlement is acknowledged only after the broker commits the
change. The Service Bus acknowledgement is Accepted on success or Rejected
with the actual refusal, rather than an echo of the receiver's requested
dead-letter outcome. Receivers that settle in first mode cannot await a broker
acknowledgement; a refusal still closes their link.
Scheduling and cancellation use the management node, while an ordinary send
can also schedule through the `x-opt-scheduled-enqueue-time` timestamp
annotation. Peek returns active, deferred, and scheduled state annotations.
A receiving link's `com.microsoft:session-filter` names a session or, with a
null value, asks for the next available one; the attach response echoes the
granted identifier and the initial session-lock deadline. The session is
released when that link closes; renewing its lock and reading or writing its
state use the entity's `$management` request/reply links, as does message-lock
renewal. Scheduling and cancellation require Send authorization; receiving,
peeking, settlement, and lock or session operations require Listen. Management
links accept either permission, and every request rechecks its own permission
when authentication is enabled. A transfer is accepted only after its command
committed, so the
acknowledgement means durable. One node still serves one namespace. A message
drained from a dead-letter queue carries its reason and description in the
`DeadLetterReason` and `DeadLetterErrorDescription` application properties. The
complete protocol coverage uses a Rust AMQP 1.0 client. The current and previous
stable official .NET SDKs also have opt-in gates for ordinary send, receive,
peek, deferral, deferred receive, message-lock renewal, completion, scheduling,
and cancellation, duplicate detection for ordinary and scheduled sends, plus
session state, renewal, receive, completion, and scheduling. Both gates exercise
message properties, application-property CLR types, footer data, a lifetime
longer than the AMQP header can represent, and property-preserving redelivery;
the rest of those client gates remains incomplete.

The SDK gates build into separate temporary directories and run the resulting
assemblies directly. Run them explicitly with
`cargo test -j 2 -p server --test amqp_dotnet_current -- --ignored --test-threads=1`;
each .NET build is limited to two jobs, and serial test execution preserves that
limit across the two releases.

### Message Content

Stored content has a protocol-neutral typed representation. Message and
correlation identifiers keep their AMQP types, including the distinction
between a missing identifier and an explicitly empty string or binary value.
Standard properties, application properties, message annotations and footer
entries survive receive, redelivery, deferral, scheduling and dead-lettering.
Data and sequence bodies retain every section, including empty sections; value
bodies retain nested containers, symbolic descriptors and scalar types. Empty
AMQP arrays are explicitly refused because the current value model cannot
retain their element constructor; heterogeneous arrays and duplicate map keys
are refused before storage. Application properties accept scalar values and
scalar described extensions, not compound containers. Float
and decimal values retain their raw bits. Delivery annotations are hop-local
and are consumed rather than forwarded.

Untrusted value parsing is bounded to 68 nesting levels, 132,096 value
nodes, and 4 MiB of copied binary/string/symbol data across a message. Shared
array descriptor names count once per expanded element, before allocation.
Stored message content is bounded separately to 64 levels and 65,536
nodes; the parser allowance covers envelope sections and metadata map keys.
Malformed sizes/counts, duplicate map keys and invalid symbols are refused.

Broker-owned sequence, enqueue time, state, lock and scheduling annotations
override producer values. Delivery count is broker-owned, and `first-acquirer`
is emitted as false rather than repeating an unverified producer assertion.
Dead-letter reason and description override those
application properties when draining the dead-letter queue, while the rest of
the application bag remains intact. Reserved dead-letter properties are cleared
from ordinary deliveries; exact Azure parity for that case is unverified.
The broker strips lifetime and session
from dead-letter deliveries. For active messages with finite lifetimes, the
creation/absolute-expiry timestamp pair is emitted as broker enqueue/deadline:
the official clients use that pair both to reconstruct long lifetimes and to
expose expiration. Producer creation time is otherwise retained, including in
dead letters. Pending schedules retain their effective lifetime before it
starts at activation.

These are semantic preservation guarantees, not byte-identical forwarding for
signed AMQP envelopes: map order and optional empty sections may normalize.
Duplicate detection still compares the text-normalized identifier, so distinct
AMQP identifier types with the same normalized text share history; exact Azure
behavior for that case is unverified. Rich messages enforce the configured
size limit using a conservative content tally that includes metadata and body
sections, rather than only flattened body bytes. Properties have a local 32 KiB
limit, and the header has a local 64 KiB limit, including standard properties,
application properties and message annotations. Accounting uses UTF-8 key bytes
plus conservative type/length/value overhead. New sends reserve 512 header bytes
for broker fields and ordinary dead-letter reasons; projected dead letters use
256 bytes for remaining broker fields. Footer content counts toward the message
limit, not the header limit. Settlement checks the merged property bag and
projected canonical dead-letter fields. These bounds implement the documented
quota sizes conservatively; exact Azure byte accounting and AMQP wire-size
parity remain unverified.

The current value format is version 7 and durable store layout is version 6.
Earlier message and queue-configuration shapes have tested decoders, but an
earlier store directory is refused at open because its broker contract differs.
There is no directory migration tooling yet; development directories
from older builds must be recreated. A rollback likewise refuses a newer layout.

All of it now runs on either backend. The Fjall backend fsyncs a command's batch
before reporting it applied, and the same semantics suite runs against both
backends, so a single node keeps its messages, locks, delivery counts, and
sequence numbers across a restart. Preserving them across the loss of a node
still needs replication.

Switchyard intentionally does not reproduce Azure subscription, namespace
capacity, or operations-per-second commercial quotas. It defaults to compatible
message validation, including a default 256 KiB content-size limit, while
allowing operators to configure larger limits. Full wire quota parity is not
yet claimed.
