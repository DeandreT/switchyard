# Compatibility

Switchyard targets the Azure Service Bus Standard messaging model. A capability
is marked supported only after it has protocol-level tests and end-to-end
coverage with the relevant client.

## Client Gates

| Client | Data plane | Administration | Status |
| --- | --- | --- | --- |
| Official .NET SDK, current stable | Single and atomic batch send; duplicate detection across immediate, batch, and scheduled sends; scheduled send/batch activation and cancellation; prefetched multi-message receive; independent settlement; receive-delete; envelope fidelity; renew; abandon/redelivery; defer and deferred receive; ordered peek pagination across active, locked, scheduled, deferred, session, and DLQ messages; dead-letter and DLQ receive/complete; session renew/state; AMQP over TCP and WebSockets | Planned | Experimental gates on 7.20.2 |
| Official .NET SDK, previous stable | Planned | Planned | Not implemented |
| Sift pinned revision | Planned | Planned | Not implemented |

## Capability Matrix

A capability reaches **State machine** once the deterministic broker core
implements it with tests. That is a prerequisite for compatibility, not a form
of it: nothing below is reachable by a client until the protocol edge exists.

| Capability | Target release | Status |
| --- | --- | --- |
| AMQP 1.0 over TLS | Pre-1.0 | Protocol edge, Rust client end to end |
| AMQP over WebSockets | Pre-1.0 | Protocol edge, Rust and current .NET clients end to end |
| SASL PLAIN and CBS SAS/JWT | Pre-1.0 | PLAIN and CBS SAS: protocol edge, Rust client end to end. JWT: not implemented |
| Queue send, receive, and settlement | Pre-1.0 | State machine, AMQP mapping, Rust and current .NET clients end to end |
| Atomic message batches | Pre-1.0 | State machine, Service Bus AMQP batch format, Rust and current .NET clients end to end |
| Prefetch and concurrent settlement | Pre-1.0 | Bounded AMQP delivery pipeline, link-credit drain, Rust and current .NET clients end to end |
| AMQP message envelope fidelity | Pre-1.0 | Durable V1 envelope, broker-owned overlays, Rust and current .NET clients end to end |
| Abandon and redelivery | Pre-1.0 | State machine, AMQP mapping, Rust and current .NET clients end to end |
| Peek without lock acquisition | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Receive-delete | Pre-1.0 | State machine, AMQP mapping |
| Lock expiry and redelivery | Pre-1.0 | State machine |
| Message lock renewal | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Time-to-live expiry | Pre-1.0 | State machine |
| Topics and subscriptions | Pre-1.0 | Not implemented |
| Correlation and SQL filters/actions | Pre-1.0 | Not implemented |
| Scheduling and cancellation | Pre-1.0 | State machine, timer activation, AMQP management and annotated-send mapping, Rust and current .NET clients end to end |
| Deferral and deferred receive | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Dead-letter | Pre-1.0 | State machine, AMQP mapping, Rust and current .NET clients end to end |
| Dead-letter receive and resubmit | Pre-1.0 | Receive: state machine, AMQP mapping, Rust and current .NET clients end to end. Resubmit: not implemented |
| Sessions and session state | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Duplicate detection | Pre-1.0 | State machine, bounded timer cleanup, AMQP mapping, Rust and current .NET clients end to end |
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
- A send batch validates every child before writing anything, then allocates
  consecutive sequence slots in one storage commit. A queue without duplicate
  detection persists every child; a detecting queue persists only each
  first-copy winner. One invalid or oversized child rejects the whole batch
  without consuming a sequence number. Session batches require every child to
  name the same session.
- On a duplicate-detection queue, the first accepted non-empty message
  identifier wins for the configured entity-scoped history window. Later
  copies are acknowledged but not stored, including copies that cross
  immediate, batched, and scheduled send paths. A batch is validated before it
  changes either messages or history, and its first new identifier wins over
  later copies in that batch. Duplicate hits do not extend the deadline, while
  settlement, cancellation, expiry, and dead-lettering do not shorten it.
  Suppressed requests still consume a sequence slot so singular and scheduled
  management sends retain deterministic acknowledgements; those slots appear
  only as gaps in the stored message order.
  Missing raw-AMQP identifiers bypass detection; the official SDK supplies an
  identifier. This is send-side suppression and does not remove the normal
  possibility of receive redelivery under peek-lock.
- Receive-delete is at-most-once: the deletion commits before the transfer.
- Peek is an inclusive, sequence-ordered, read-only snapshot over active,
  locked, scheduled, and deferred records. It never increments delivery count
  or exposes a lock token. A regular receiver can browse across sessions; a
  receiver that holds a session sees only that session. Every positive signed
  request count is accepted, responses are capped at 250 messages, and scanning
  continues until that cap or the true end of the entity so an empty page is
  definitive.
- Scheduling persists an ordered placeholder immediately without making it
  receivable or making its session available. Peek exposes that placeholder as
  `Scheduled`, and cancellation removes it atomically. When the replicated
  timer command activates a due message, the placeholder is retired, a new
  active sequence number and enqueue timestamp are assigned, and its TTL starts
  from that activation timestamp.
- A settlement is rejected unless it presents the live lock token, and rejected
  again once the lock deadline has passed.
- A live message lock can be renewed without changing its token. Renewal moves
  the replicated deadline record and its expiry index in one storage batch.
- Abandoning a message, or letting its lock elapse, returns it to the queue
  until it reaches the queue's maximum delivery count, after which it is
  dead-lettered as `MaxDeliveryCountExceeded`.
- Deferring a locked message hides it from ordinary receive until a client
  requests its sequence number explicitly. Deferred receive validates a
  bounded batch atomically and returns results in caller order. Abandon and
  lock expiry restore a deferred delivery to the deferred set, while
  receive-delete removes it before replying. Defer, abandon, and dead-letter
  property updates are merged into the durable message envelope on both
  delivery-link and management dispositions.
- Messages past their time to live are dead-lettered as `TTLExpiredException`,
  both by the timer sweep and by any receive that reaches one first. A live
  message lock remains settleable until its lock deadline; TTL takes effect if
  that lock is abandoned or expires. Deferred TTL is checked on explicit
  retrieval.
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

Expiry is not merely expressible: the `server` crate's timer worker proposes the
lock, time-to-live, session-lock, and duplicate-history sweeps on an interval,
so a running node actually releases what has elapsed.

An AMQP 1.0 client can reach a queue. The node accepts AMQP over TLS with the
socket secured before the protocol handshake, as Service Bus port 5671
requires. It also accepts the `AMQPWSB10` (and standardized `amqp`) binary
WebSocket tunnel at `/$servicebus/websocket` over WSS, including the current
official .NET client.
Plain TCP remains available only in development mode. A configured
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
A transfer using the Service Bus batch message format is decoded into its
individual AMQP child envelopes and submitted as one atomic broker command;
unknown or malformed formats are rejected at the delivery without poisoning
the link. Before touching broker state, outbound delivery atomically reserves
one unit of remote link credit; an empty receive releases it. The edge keeps a
bounded set of outcomes independently identifiable, so prefetched messages may
settle out of order without ever locking or deleting beyond available credit.
When a receiver asks to drain, reservations are either consumed or released
before the remaining credit is returned through the AMQP drain handshake.
A receiving link's `com.microsoft:session-filter` names a session or, with a
null value, asks for the next available one; the attach response echoes the
granted identifier and the initial session-lock deadline. The session is
released when that link closes; renewing its lock and reading or writing its
state use the entity's `$management` request/reply links, as do message-lock
renewal, deferred receive by sequence number, and management disposition
updates. Ordered peeking uses the same management node and returns either a
bounded page or a no-content response without changing broker state. Scheduling
and cancellation use that node with Send authorization; receive-side operations
require Listen authorization. Manage includes both rights. A transfer is
accepted only after its command committed, so the acknowledgement means
durable. One node still serves one namespace. A message
keeps its encoded AMQP envelope durably, including all legal body forms,
identifier types, properties, annotations, application properties, and footer.
On delivery, reserved sequence, enqueue, expiry, lock, delivery-count, and
dead-letter fields are replaced with broker-authoritative values while custom
content remains intact across redelivery and dead-lettering. A message drained
from a dead-letter queue carries its source, reason, and description. The
complete protocol coverage uses a Rust AMQP 1.0 client. The current stable
official .NET SDK also has opt-in gates for envelope fidelity, ordinary send,
atomic enumerable and explicit SDK batches, prefetched multi-message receive,
out-of-order completion, receive-delete, message-lock renewal, abandon and
redelivery, deferral with property updates, deferred receive and settlement,
ordered peek pagination across active, locked, scheduled, deferred, session, and
dead-letter messages, management and annotated-transfer scheduling,
cancellation and timer activation, custom dead-lettering, dead-letter receive
and completion, duplicate detection across immediate, batch, and scheduled
sends, session state and renewal, and AMQP-over-TCP and WebSockets;
the rest of that client gate remains incomplete.
Dead-letter resubmission is not implemented.

All of it now runs on either backend. The Fjall backend fsyncs a command's batch
before reporting it applied, and the same semantics suite runs against both
backends, so a single node keeps its messages, locks, delivery counts, and
sequence numbers across a restart. Preserving them across the loss of a node
still needs replication.

Switchyard intentionally does not reproduce Azure subscription, namespace
capacity, or operations-per-second commercial quotas. It defaults to compatible
wire validation, including the Standard 256 KiB message-size limit, while
allowing operators to configure larger namespace storage quotas.
