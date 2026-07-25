# Compatibility

Switchyard targets the Azure Service Bus Standard messaging model. A capability
is marked supported only after it has protocol-level tests and end-to-end
coverage with the relevant client.

## Client Gates

| Client | Data plane | Administration | Status |
| --- | --- | --- | --- |
| Official .NET SDK, current stable | Planned | Planned | Not implemented |
| Official .NET SDK, previous stable | Planned | Planned | Not implemented |
| Sift pinned revision | Planned | Planned | Not implemented |

## Capability Matrix

A capability reaches **State machine** once the deterministic broker core
implements it with tests. That is a prerequisite for compatibility, not a form
of it: nothing below is reachable by a client until the protocol edge exists.

| Capability | Target release | Status |
| --- | --- | --- |
| AMQP 1.0 over TLS | Pre-1.0 | Not implemented |
| AMQP over WebSockets | Pre-1.0 | Not implemented |
| SASL PLAIN and CBS SAS/JWT | Pre-1.0 | Not implemented |
| Queue send, receive, and settlement | Pre-1.0 | State machine |
| Peek without lock acquisition | Pre-1.0 | Not implemented |
| Receive-delete | Pre-1.0 | State machine |
| Lock expiry and redelivery | Pre-1.0 | State machine |
| Time-to-live expiry | Pre-1.0 | State machine |
| Topics and subscriptions | Pre-1.0 | Not implemented |
| Correlation and SQL filters/actions | Pre-1.0 | Not implemented |
| Scheduling and cancellation | Pre-1.0 | Not implemented |
| Deferral and deferred receive | Pre-1.0 | Not implemented |
| Dead-letter | Pre-1.0 | State machine |
| Dead-letter receive and resubmit | Pre-1.0 | Not implemented |
| Sessions and session state | Pre-1.0 | State machine |
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
- A settlement is rejected unless it presents the live lock token, and rejected
  again once the lock deadline has passed.
- Abandoning a message, or letting its lock elapse, returns it to the queue
  until it reaches the queue's maximum delivery count, after which it is
  dead-lettered as `MaxDeliveryCountExceeded`.
- Messages past their time to live are dead-lettered as `TTLExpiredException`,
  both by the timer sweep and by any receive that reaches one first.
- Rejected commands write nothing, so every replica rejects at the same point.
- On a queue that requires sessions, a message carries a session identifier and
  is only delivered to a receiver holding that session's lock. Ordering is
  guaranteed within a session, which is the only FIFO guarantee made. A session
  lock is exclusive and expires on its own deadline; session state outlives the
  receiver that set it.

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

All of it now runs on either backend. The Fjall backend fsyncs a command's batch
before reporting it applied, and the same semantics suite runs against both
backends, so a single node keeps its messages, locks, delivery counts, and
sequence numbers across a restart. Preserving them across the loss of a node
still needs replication.

Switchyard intentionally does not reproduce Azure subscription, namespace
capacity, or operations-per-second commercial quotas. It defaults to compatible
wire validation, including the Standard 256 KiB message-size limit, while
allowing operators to configure larger namespace storage quotas.

