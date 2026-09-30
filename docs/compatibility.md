# Compatibility

Switchyard targets the Azure Service Bus Standard messaging model. A capability
is marked supported only after it has protocol-level tests and end-to-end
coverage with the relevant client.

## Client Gates

| Client | Data plane | Administration | Status |
| --- | --- | --- | --- |
| Official .NET SDK, current stable | Send, both batch-send APIs, peek, receive, abandon/defer/dead-letter property updates, renew, complete, schedule, cancel, duplicate detection and message properties; session renew/state/scheduling | Planned | Experimental gate on 7.21.0 |
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
| Atomic message batch send | Pre-1.0 | State machine, AMQP producer mapping, Rust clients on both backends and both pinned .NET batch APIs |
| Message properties and AMQP body preservation | Pre-1.0 | State machine and AMQP mapping; typed properties, application values, annotations, footer and all body kinds. Rust clients on both backends and official .NET property gate |
| Peek without lock acquisition | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Receive-delete | Pre-1.0 | State machine, AMQP mapping |
| Lock expiry and redelivery | Pre-1.0 | State machine |
| Message lock renewal | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Time-to-live expiry | Pre-1.0 | State machine and timer; default drop and optional dead-lettering, official .NET deferred-expiry gate |
| Topics and subscriptions | Pre-1.0 | Not implemented |
| Correlation and SQL filters/actions | Pre-1.0 | Not implemented |
| Scheduling and cancellation | Pre-1.0 | State machine, AMQP management and send-annotation mappings, Rust and current .NET clients end to end |
| Deferral and deferred receive | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Dead-letter | Pre-1.0 | State machine, AMQP mapping |
| Dead-letter receive and resubmit | Pre-1.0 | Receive: state machine, AMQP mapping. Resubmit: not implemented |
| Sessions and session state | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Duplicate detection | Pre-1.0 | State machine, AMQP send/scheduling mappings, Rust and current .NET clients end to end |
| Queue configuration updates | Pre-1.0 | Atomic state-machine patches; native queue API |
| Same-placement-group transactions | Pre-1.0 | Not implemented |
| Atom/XML entity and rule administration | Pre-1.0 | Not implemented |
| Native gRPC administration | Pre-1.0 | Queue create/get/list/update over HTTP/2 and authenticated TLS; other services and deletion not implemented |
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
- The core has a separate atomic ingress-batch command. Every member retains
  its own properties, identifier, session, lifetime, and optional scheduling
  timestamp. The complete batch is validated before staging content, shares
  duplicate history across its members, and commits counters and records once.
  A later validation, allocation, or storage failure leaves no partial batch.
  It accepts at most 1,024 messages, 65,536 retained value items, and 4 MiB of
  retained content, including the compatibility body and normalized identifiers.
  Session queues require every member to name the same session. An empty batch
  validates its target but writes nothing. These are local resource bounds,
  not Azure batch quotas.
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
  immediately, ahead of the delivery-count limit. Expired messages are dropped
  by default. `dead_lettering_on_message_expiration` instead moves them to the
  dead-letter queue as `TTLExpiredException`, whether reached by a timer,
  receive, abandonment, or lock expiry. The flag does not affect explicit
  dead-lettering or the delivery-count limit. Logical legacy configurations
  retain their former always-dead-letter behavior when decoded.
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

Queue settings can be patched atomically with the parent and its dead-letter
shadow in the same batch. An omitted setting is unchanged; an explicit unlimited
TTL clears a finite default. Session and duplicate-detection enablement are
creation-only properties. Updates retain all existing messages, deadlines,
locks, counters, and duplicate-history entries; the new size/TTL/lock/history
limits govern new ingress or newly allocated deadlines. Expiration disposition
and maximum delivery count use the current queue policy. A no-op patch does not
write storage or advance the applied clock. The adjustable per-message size cap
is a Switchyard policy, not a verified Azure queue-update property.

Identifier allocation refuses exhaustion instead of saturating and reusing a
stored identity. Sequence numbers are limited to `i64::MAX`, preserving exact
AMQP `long` values for receive, peek, scheduling handles, and deferred receive.
That final value can be allocated once; a command requiring another allocation
is rejected atomically with `amqp:resource-limit-exceeded` (management status
403, non-retryable). Duplicate sends still consume their ordinary sequence
allocation. Lock tokens use `u64::MAX` as an exhausted sentinel, so `u64::MAX - 1`
is the final allocation. Operations that need no fresh identifier, including
receive-delete, cancellation, settlement, renewal, and cleanup, remain usable.
Counters keep their existing stored shape and exhaustion survives restart.
This is a deliberate local bound, not Azure's documented rollover behavior;
the official .NET [sequence-number property](https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebusreceivedmessage.sequencenumber)
is signed, while Azure documents rollover in its
[sequencing contract](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-sequencing).

The `server` crate's timer worker proposes scheduled activation, lock,
time-to-live, session-lock, and duplicate-history sweeps on an interval, so a
running node activates what is due and releases or prunes what has elapsed.
Queue discovery uses exclusive keyset pages of at most 1,024 configurations,
including dead-letter shadows. A retained cursor visits later pages on later
sweeps and wraps at the end; one queue's failed command does not permanently
pin the worker ahead of all following queues. Each queue index gets at most
eight bounded command rounds per sweep.

An AMQP 1.0 client can reach a queue. The node accepts AMQP over TLS with the
socket secured before the protocol handshake, as Service Bus port 5671
requires. Plain TCP remains available only in development mode. A configured
shared-access policy accepts either SASL PLAIN credentials or SASL ANONYMOUS
or Microsoft's equivalent `MSSBCBS` mechanism followed by a CBS SAS token. CBS
grants are scoped to a namespace or entity and to Send, Listen, or Manage; they
authorize links connection-wide and close an open link when its token expires.
A connection without a valid grant gets 20 seconds to complete CBS
authorization. JWT, OIDC, and mTLS are not implemented.
The listener has local defaults of 128 live connections, including unfinished
security handshakes, and one 10-second deadline covering TLS, SASL, and AMQP Open.
Excess sockets are refused; handshake progress does not restart that deadline.
Listener builders can configure both limits, with a zero handshake timeout
requesting immediate refusal. The CBS authorization deadline starts after Open.
Graceful connection Close has a two-second default deadline. Timeout or
cancellation of its caller cancels the driver and its socket reader, including
when application dispatch or a socket write is blocked. Explicit shutdown waits
for both tasks to terminate before releasing the listener's admission slot.
These are Switchyard resource policies, not Azure quotas. Connection-wide
message-allocation budgets are not yet enforced.
Open advertises a 60-second receive-idle interval by default, with an actual
120-second complete-frame silence deadline, following the
[AMQP idle-timeout recommendation](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#doc-idle-timeout).
The peer's independent interval drives outgoing heartbeats at half its value;
ordinary complete writes also satisfy that direction. An omitted or zero peer
interval disables those heartbeats, while zero in the local configuration
disables only the local receive-silence check. Positive intervals below one
second are refused. Complete valid frames, including empty frames on unbound
valid channels, refresh receive activity; partial prefixes or bodies do not.
Each frame's complete write and flush has a separate five-second local limit,
shortened by the peer's remaining interval. Local options can configure both
limits; zero write duration is immediate refusal, not unlimited. Independent
watchdogs enforce silence and Close deadlines even when dispatch or transport
writes are blocked. A partial or interrupted write drops the transport without
appending Close or retrying the partially emitted frame. No heartbeat is sent
before Open or after Close.
AMQP Open frames are limited to the initial 512-byte transport maximum. After
Open, the server advertises and enforces its own 262,144-byte receive maximum,
independent of the peer's receive limit. Oversized frames are rejected from the
four-byte size prefix before allocating or reading their bodies; an open
connection returns the framing-error Close condition. The test client can
configure its own receive maximum between 512 bytes and the codec's 4 MiB
ceiling. SASL reads use the local receive maximum as a resource policy.
Frames on channels above the locally advertised limit receive a framing-error
Close without refreshing receive activity. Asymmetric channel/handle routing
remains a separate unfinished feature.
Session windows count Transfer frames independently of link delivery counts.
Incoming windows replenish after bounded frame processing; receive links grant
32 message slots and return credit only as the application consumes a delivery
or a partial delivery is aborted. A paused receiver cannot block the connection's
other links. Fragmented sends yield when their session window closes, resume on
session Flow, and consume only one link credit per message. Flow echo, drain,
optional credit, wrapping counts, and early second-mode dispositions have raw
transport regressions. Every outgoing frame is checked against the peer's frame
cap before writing bytes. Connection-wide byte budgets and asymmetric
channel/handle routing remain unfinished.
First transfers require an explicit delivery ID, binary tag, and message format.
Tags may be empty but cannot exceed 32 bytes. Continuations may omit identity
fields, but repeated ID, tag, and format values must match the first fragment;
an invalid fragment closes only its link. Format zero always uses the standard
message decoder. The transport can opt a receiving link into at most eight
additional exact formats with trusted application decoders; zero cannot be
overridden. A format not registered on that link is refused with
`amqp:not-implemented` before reserving a delivery slot. CBS, management, and
test-client receiving links retain the standard-only default.
Incoming delivery IDs are reserved across the session, and tags across their
receiving link, from the first fragment. A live collision closes only the
offending link with `amqp:invalid-field`; a different original owner's delivery
remains available for settlement. This link-local error scope is a Switchyard
policy. A separate metadata allowance bounds incoming partial, complete, and
second-mode acknowledgement entries to 1,024 per receiving link and 4,096 per
session. Admission beyond either limit closes the offending link with
`amqp:resource-limit-exceeded`, independently of its message-slot credit.
Consumption returns message-slot credit but does not settle a delivery.
Application receipts retain opaque link and delivery generations rather than
relying on reusable numeric aliases. A foreign receiver, retired link, or
aborted receipt cannot emit a settlement. Repeating a terminal settlement on
its original open link is a no-op, even after the numeric ID has been reused.
Local outcomes are checked against the peer's frame cap before state changes;
an oversized outcome leaves the live receipt available for a smaller retry.
Receiver-first outcomes settle immediately. Receiver-second outcomes are sent
unsettled and retain their aliases until the sender acknowledges settlement,
including a state-less or ranged sender disposition. The application call
returns after writing its outcome, not after that acknowledgement. A sender
settling before the application finishes suppresses an unnecessary response;
partial deliveries retain their aliases until completion or abort. Completing
sender-settled deliveries need no outcome or retained alias.
The negotiated sender mode is enforced at completion, with aborted transfers
implicitly settled. A per-transfer receiver mode defaults to the negotiated
mode on the completing frame, rather than becoming sticky from an earlier
fragment; an illegal receiver-second override on a receiver-first link is
refused unless the delivery is sender-settled or aborted. These policies follow
the [Transfer and Disposition rules](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-transfer).
Switchyard interprets the receiver-mode exception to include a settled sender
disposition received before completion. Such a disposition never substitutes
for the explicit settled Transfer required by a negotiated settled sender link.
Detach, End, and connection teardown retire the affected generation and remove
its aliases. Link suspension and resumption remain unsupported.
Approved producer links register Service Bus batch format `0x80013700`, whose
[wire constant](https://raw.githubusercontent.com/Azure/azure-amqp/master/src/AmqpConstants.cs)
identifies one encoded standard message per outer Data section. The nonempty
wrapper uses one link delivery/credit regardless of member count, shares its
inner parsing allowance, and submits exactly one atomic ingress command before
the outer Accepted outcome. The entire wrapper must still fit the 256 KiB
producer-link limit. Inner properties, lifetimes, identifiers, and optional
scheduling timestamps remain independent; outer metadata is not inherited or
used as a batch-wide duplicate identifier. A present outer session must agree
with present inner sessions and never fills a missing one. A standard inner Data
body is ordinary content, not another batch. Empty wrappers are refused; an
empty encoded inner message is valid anonymous content. The
[pinned SDK converter](https://raw.githubusercontent.com/Azure/azure-sdk-for-net/Azure.Messaging.ServiceBus_7.21.0/sdk/servicebus/Azure.Messaging.ServiceBus/src/Amqp/AmqpMessageConverter.cs)
normally sends singleton batches in format zero, so raw transport tests also
force the nonzero singleton format. Both pinned client gates cover the enumerable
and size-safe batch APIs, session FIFO/refusals, rich content, duplicate detection,
and an oversized `TryAddMessage` refusal.
Message decoding can share one cumulative allocation allowance across embedded
messages: at most 132,096 parsed values and 4 MiB of copied string, symbol, and
binary content, with depth bounded independently for each value. Array members
and repeated named descriptors are charged before allocation. Scheduling
management requests and producer batches share that inner allowance and reject
more than 1,024 members before decoding them. These bounds do not provide a
connection-wide memory limit and are not Azure batch quotas.
If a peer detaches a link while application approval is outstanding, a bounded
canceled-approval tombstone refuses early handle reuse until that approval returns
an error. Canceled approvals count toward the 32 pending link approvals allowed
per session. A stale approval cannot reopen the canceled link. This is a local
admission policy, not a restriction imposed by AMQP.
The edge resolves a link's address to an entity, turns transfers into send
commands and dispositions into settlements, and answers a rejection with the
condition an SDK keys its behaviour off. A receiving link's settle mode selects
the delivery guarantee: unsettled is peek-lock, pre-settled is receive-delete.
Peeking is served through the entity's `$management` request/reply links and
returns encoded AMQP messages without touching their broker state.
Receiving transfers encode the number of preceding acquisitions, so the official
.NET client reports 1 on the first receive and 2 after abandonment and redelivery.
Peek retains the stored acquisition count without the client's receive increment.
The core currently counts acquisitions, not only unsuccessful settlements;
counter parity for deferral, locked-message peek, and dead-letter transfers is
not established, nor are generic AMQP Released/Modified counter semantics.
Deferred receive is also served through `$management`, and locks returned that
way are settled through the management `update-disposition` operation.
Session deferred receive requires a live session hold on the associated receiver
link in that same connection. The state machine validates the hold before
touching message records or expiry cleanup; naming a session or replaying an old
hold cannot bypass another receiver's ownership. The old identifier-only core
commands retain their serialized shape for trusted callers, but are not used
by the management edge.
Dead-letter waiters are notified from committed ready-index enqueues, including
lazy expiry during ordinary or deferred receive. Staged writes that are later
rejected, failed storage commits, and drop-on-expiry cleanup do not wake them.
This notification metadata adds no persisted or serialized command fields.
Retrieving only expired deferred messages commits their cleanup before
returning `com.microsoft:message-not-found`; missing messages are distinguished
from a missing queue. Dead-letter management paths normalize the reserved
suffix in the same way as receiving links, including the SDK's mixed-case form.
Deferred receive rejects duplicate sequence numbers before acquiring locks or
removing expired messages, so one message cannot be returned twice by a batch.
Management replies have a local 4 MiB ceiling and honor the reply receiver's
advertised maximum message size. Deferred batches must fit a conservative
content budget, including broker metadata and response fields, before any
locks, deletions, or expired-message cleanup commit. An oversized batch is
refused atomically with `amqp:link:message-size-exceeded`; a smaller retry can
still retrieve its messages. Peek returns a fitting prefix, or a size refusal
if its first eligible message cannot fit. These are local resource policies,
not Azure batch-count limits; the estimate may refuse a compact encoding that
would fit on the wire. It bounds content and staging work, not exact heap usage.
A reply link must be attached before a management
command is submitted. If it cannot be found within two seconds, the request is
rejected without changing broker state. Outgoing oversized messages detach
only their affected link and are never transferred.
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

## Native Administration

The optional `--admin-listen` endpoint serves native queue create/get/list/update
through the broker owner. It is a separate HTTP/2 listener and reuses the AMQP
TLS identity and shared-access policy when configured. Authenticated requests
require TLS and a SAS token in `authorization` metadata with Manage permission
for the requested entity; listing needs namespace scope. The configured namespace
is the only namespace accessible through that endpoint. Dead-letter shadows are
hidden and cannot be administered independently. Entity capacity and usage fields
are absent because quota accounting is not implemented; they are not reported as
zero-byte measurements.

Pages are ordered, exclusive, namespace-bound, and limited to 1,024 parent queues.
Local defaults admit 128 sockets and 128 concurrent requests across service clones,
with at most 32 HTTP/2 streams per connection, a 10-second TLS handshake deadline,
30-second request deadlines, 64 KiB decoded requests, and 1 MiB encoded replies.
HTTP/2 connections send keepalive pings after 30 seconds, allow 10 seconds for an
acknowledgment, and retire after five minutes with a 30-second graceful deadline.
These are local resource policies. Queue deletion and the cluster, namespace,
backup, and audit services return unimplemented rather than simulated success.
This endpoint is not Azure Atom/XML administration compatibility.
`switchyardctl queue create|get|list|update` exposes these operations with JSON
responses and nonzero errors. It reads SAS tokens only from bounded regular
files, marks their metadata sensitive, and verifies TLS against explicitly
supplied CA certificates. Plaintext is opt-in, loopback-only, and cannot carry a
token. Command-line settings preserve omitted, false, zero, and unlimited TTL.

## Durable Format

The current value format is version 8 and durable store layout is version 7.
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
