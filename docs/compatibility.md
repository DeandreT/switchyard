# Compatibility

Switchyard targets the Azure Service Bus Standard messaging model. A capability
is marked supported only after it has protocol-level tests and end-to-end
coverage with the relevant client.

## Client Gates

| Client | Data plane | Administration | Status |
| --- | --- | --- | --- |
| Official .NET SDK, current stable | Send, peek, receive, defer, renew, complete, schedule and cancel; session renew/state/scheduling | Planned | Experimental gate on 7.20.2 |
| Official .NET SDK, previous stable | Planned | Planned | Not implemented |
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
| Duplicate detection | Pre-1.0 | Not implemented |
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
  it in receive-delete mode, and deferred messages still expire.
- Scheduling keeps messages visible to peek but out of the ready path until
  their requested time. The timer atomically activates each due message under
  a new sequence number, appending it to the queue, and starts its lifetime at
  that activation. The requested scheduling timestamp remains attached.
  Scheduling a batch is atomic, including validation failures. Cancellation
  removes the scheduled record and deadline entry atomically; it rejects a
  missing or already-active handle. Exact Azure behavior for those rejected
  cancellation cases has not yet been verified.
- A settlement is rejected unless it presents the live lock token, and rejected
  again once the lock deadline has passed.
- A live message lock can be renewed without changing its token. Renewal moves
  the replicated deadline record and its expiry index in one storage batch.
- Abandoning a message, or letting its lock elapse, returns it to the queue
  until it reaches the queue's maximum delivery count, after which it is
  dead-lettered as `MaxDeliveryCountExceeded`.
- Messages past their time to live are dead-lettered as `TTLExpiredException`,
  both by the timer sweep and by any receive that reaches one first.
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
time-to-live, and session-lock sweeps on an interval, so a running node
activates what is due and releases what has elapsed.

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
complete protocol coverage uses a Rust AMQP 1.0 client. The current stable
official .NET SDK also has an opt-in gate for ordinary send, receive,
peek, deferral, deferred receive, message-lock renewal, completion, scheduling,
and cancellation plus session state, renewal, receive, completion, and
scheduling; the rest of that client gate
remains incomplete.

The current message-record format is version 5 and durable store layout is
version 3. Earlier record shapes have tested decoders, but an earlier store
directory is refused at open because its broker indexes have a different
contract. There is no directory migration tooling yet; development directories
from older builds must be recreated. A rollback likewise refuses a newer layout.

All of it now runs on either backend. The Fjall backend fsyncs a command's batch
before reporting it applied, and the same semantics suite runs against both
backends, so a single node keeps its messages, locks, delivery counts, and
sequence numbers across a restart. Preserving them across the loss of a node
still needs replication.

Switchyard intentionally does not reproduce Azure subscription, namespace
capacity, or operations-per-second commercial quotas. It defaults to compatible
wire validation, including the Standard 256 KiB message-size limit, while
allowing operators to configure larger namespace storage quotas.
