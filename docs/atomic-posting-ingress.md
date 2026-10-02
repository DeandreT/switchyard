# Atomic Posting Ingress

`AmqpListener::serve_atomic_posting_ingress()` is an explicit, posting-only Rust
entry point for brokers implementing [NativeAtomicBroker](native-atomic-broker.md).
It joins actual native control and posting receipts to the logical transaction
registry and the paired broker owner. Ordinary `serve()` and the server's normal
listener configuration remain transaction-disabled. This is not an official SDK
transaction-scope gate, transactional receiving, or a persistent recovery log.

## Admission And Correspondence

The entry point shares the ordinary listener's TCP admission, TLS, WebSocket,
SASL, handshake deadline, and engine shutdown path. It accepts coordinator links,
primary non-session queue producer links, and CBS authentication links.
Management and receiving links are explicitly unsupported on this endpoint.
Exact Send authorization precedes queue topology reads. Topics, subscriptions,
dead-letter targets, and session-enabled queues cannot become atomic producers.

One connection owner holds the registry, exact controller and receiver proofs,
and admitted queue incarnation bindings. Logical IDs are reserved before the
original native Declare response is flushed. Commands are derived only from the
actual posting receipt's message and format, never supplied separately by the
adapter's caller. Format zero produces an ordinary envelope; format `0x80013700`
expands the actual outer producer batch into one staged SendBatch action. The
outer batch still has one native posting and one provisional outcome.
Scheduled and session-bearing atomic sends are refused during bounded staging.
The final broker owner independently checks the admitted entity incarnation.

## Ordering And Decisions

Staging succeeds before provisional transactional Accepted is requested.
Discharge can arrive at the owner before an earlier posting's collector event;
its sealed receipt is parked while postings continue to drain. Native Ready is
not enough to hand off: all provisional operations must have returned their
unique prepared postings, and their exact set must be consumed with the original
sealed control receipt. One paired submission then claims native authority before
logical authority and before broker I/O. Empty transactions use the existing
zero-I/O broker path.

Collectors preserve their own link order. This adapter does not promise global
wire-order FIFO across independent producer links or sessions. Native replay
policy is unchanged: repeated terminal `fail=false` is actor-refused rather than
resubmitted. Supported terminal `fail=true` performs native cleanup without
changing an earlier logical fail flag or reapplying broker work.

Broker completion and wire completion remain separate. Recorded known success
uses final ordinary posting Accepted before the control response. Finishable
recorded Aborted or Rejected decisions use still-live original-delivery cleanup
and the negotiated control refusal. Faulted resources can instead refuse
finalization and close the exact coordinator; returning resources is not a
guarantee of a negative wire reply.
Physical storage failures can mean the whole batch committed; they remain
indeterminate, never synthetic rollback or automatic retry. Lost owner replies
also cannot establish whether work started or committed.

## Lifetime And Limits

The connection shares 32 slots between pending session admissions and running
session collectors, and supervises at most 128 live links with a 256-event owner
channel. The transaction owner separately bounds controllers and retained groups
to 32, original postings per group to 100, and owned I/O operations to 128.
Logical message, action, content, and value-item budgets still apply independently
of outer native posting counts. These are local limits, not Azure quotas or an
exact process-memory guarantee.

Normal worker cleanup waits for the owner's logical pending-cancellation
acknowledgment before releasing held original receipts and waiting for Detach.
Connection-owner destruction closes logical pending authority before dropping
its owned operations, releasing queued native receipts, or aborting collectors.
Unexpected worker failures stop the connection rather than silently abandoning
its other collectors. Native
retirement hooks independently fault pending authority; started decisions remain
irreversible.

Session admissions are owned futures in a bounded pool, polled alongside
transaction events, operation completions, and deadline ticks. Waiting for a
Begin write and flush does not hold up the connection owner's ready work. The
native actor still serializes its writes: a blocked Begin can delay other native
commands, and a failed flush can terminate the actor.

Dropping an admission future cancels its caller's wait, not an already queued
AcceptSession command. It does not add a wire End or admission-rollback
guarantee. Shutdown closes logical pending authority before draining queued
native events, dropping admission futures, or aborting collectors. Logical
tickets independently check their monotonic deadline at claim.

The local 120-second deadline runs from Declare. Expiry closes the exact native
coordinator with `amqp:transaction:timeout` and closes its logical controller,
including sibling pending groups under
that coordinator; this also frees an empty native declaration. It does not close
unrelated controllers or reverse started work. Existing native error-history
fallback can instead close the affected session.

Producer authorization is checked at admission, stage, and handoff, and grant
expiry is watched by collectors. These checks do not provide an atomic wall-clock
authorization-expiry fence against a queued owner claim under delayed runtime
scheduling. No stronger authorization-at-claim guarantee is exposed by this
experimental entry point.
