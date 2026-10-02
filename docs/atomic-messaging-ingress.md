# Atomic Messaging Ingress

`AmqpListener::serve_atomic_messaging_ingress()` is a separate, explicit Rust
entry point for [NativeAtomicBroker](native-atomic-broker.md). It joins the
[posting lifecycle](atomic-posting-ingress.md) to bounded
[native outgoing retirement](native-transactional-retirement.md) on primary
non-session queues. Ordinary `serve()`, ordinary process listeners, and the
existing posting-only listener retain their previous policies. A separate
[pinned .NET transaction-scope gate](dotnet-transaction-scopes.md) establishes
warmed same-queue send and held Complete over TLS on this endpoint. It is not
general SDK transaction compatibility or a durable transaction recovery log.

## Experimental Process Endpoint

The broker process can expose this path at a separate development-only address:

```sh
cargo run -p server -- \
  --mode development \
  --listen 127.0.0.1:5672 \
  --admin-listen 127.0.0.1:9080 \
  --experimental-atomic-messaging-listen 127.0.0.1:5673
```

The address is opt-in and does not change ordinary AMQP, WebSocket, or native
administration endpoints. It uses the same namespace, TLS identity, and
shared-access policy as the ordinary listener. Configured credentials still
require TLS. Production mode refuses the experimental flag before reading
credentials or opening storage. All configured sockets bind before any
listener starts serving.

Use an isolated development network. Plaintext development remains possible;
the flag does not make an unauthenticated or externally reachable endpoint safe.
The supported receiving profile and transaction limits below still apply.
Only the [gated warmed same-queue SDK subset](dotnet-transaction-scopes.md) is
established; cold-first scopes and management operations remain unsupported.

## Admission And Held Deliveries

The endpoint shares TCP admission, TLS, WebSocket, SASL, CBS, handshake deadlines,
and engine shutdown with the existing listeners. Producers require exact Send
authorization; receiving links require exact Listen authorization. Authorization
precedes queue topology reads. Receiving links may request Mixed or Unsettled
sender mode and must request Second receiver mode. The source must be fixed,
non-durable, and unfiltered; distribution mode must be absent or `move`.
Session queues, topics, subscriptions, dead-letter sources, management links,
and receive-delete remain unsupported here.

The endpoint explicitly selects [transaction attach defaults](transaction-attach-defaults.md):
receivers always negotiate actual Unsettled sender mode, and only a fresh
coordinator may omit its initial delivery count, interpreted locally as zero.
The latter is a documented interoperability exception. Strict native APIs,
ordinary listeners, and the posting-only endpoint retain their previous policies.
The accommodations alone do not establish SDK support; the separate
[transaction-scope gate](dotnet-transaction-scopes.md) supplies end-to-end proof
for its specific warmed same-queue subset.

Each receiving worker acquires one actual PeekLock delivery through its admitted
queue-incarnation binding. It sends the message through the dedicated native
sender and waits for the complete Transfer flush. The worker then registers the
actual sequence, lock token, and original outgoing generation with the shared
connection owner, waiting for acknowledgment before forwarding dispositions.
It retains the unique sent handle, not the original message body, while waiting.

The owner derives Complete only from this private held-delivery registration
and an actual matching retirement receipt. It does not parse a delivery tag into
settlement authority, accept separately supplied settlement commands, or treat
a clonable historical observer as proof that a delivery remains live.
The supported transactional outcome is Accepted with `settled=false`.
Ordinary outcomes retain the existing settlement mapping against the same held
sequence and lock token.

## Mixed Preparation And Completion

One connection owner and one logical registry stage both postings and held
settlements. A retirement stages its held Complete command before requesting
the native provisional flush. All native obligations share the existing
100-item group allowance. The exact mixed prepared set, an empty staging queue,
and returned preparation operations are required before handoff; observing
native Ready alone is insufficient.
Explicit rollback also accounts for the sealed native retirement origins,
posting receiver counts, and pending callbacks before notifying workers; a late
collector cannot strand an original behind an already completed control reply
or close a producer reused by a newer transaction.

The paired broker owner claims native authority first and logical authority
second, then validates the exact queue incarnation and live lock before applying
one atomic batch. Participating postings and settlements must target the same
primary queue. Stale locks and mismatched bindings cannot leave a partial send
or settlement. Logical completion precedes native finalization, and successful
native finalization precedes notification that the receiver can fetch again.

A successful explicit `fail=true` rollback rearms the same original sent handle
and held lock. It does not resend a Transfer, abandon or reacquire the message,
mint a new tag, or extend the lock deadline. The peer can use a new transaction
or ordinary settlement for that same original. Domain lock expiry remains
authoritative; this endpoint supplies no management renewal operation.

Validation refusal, controller closure, lost owner response, and indeterminate
completion conservatively close affected receiving links rather than promise
a retry-safe rollback. A physical storage failure may follow a committed batch.
Neither closure nor an unavailable response can revoke OwnerStarted work.

## Lifetime And Authorization

The existing connection bounds remain: 32 session admissions or collectors,
128 supervised links, 256 owner events, 32 transaction groups and controllers,
and 128 owned operations. Each admitted consumer retains at most one held
delivery. Logical action, message, content, and value budgets apply separately;
these are local limits, not Azure quotas or exact process-memory bounds.

Worker cleanup waits for the owner's precise pending-cancellation acknowledgment
before dropping a sent handle or receipt and waiting for Detach. Connection
shutdown invalidates pending logical authority before releasing native work or
aborting collectors. Started work retains its recorded decision.

Handoff includes every participating exact-resource Listen grant as well as
each required Send grant and the controller's any-valid-grant snapshot. Each
scope chooses its maximum currently valid expiry; the minimum required expiry
restricts the logical ticket. Only that numeric horizon travels with the owner
job. The existing sampled epoch-time check occurs before logical claim and
broker I/O; it is not an atomic wall-clock or grant-revocation guarantee.
