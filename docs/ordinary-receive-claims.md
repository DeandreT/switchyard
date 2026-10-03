# Ordinary Receive Claims

Authenticated ordinary AMQP receiving uses an owned, receive-only submission.
It carries the admitted entity binding, exact physical target, receive mode,
optional session hold, and a unique runtime claim ticket. It cannot carry a
Send, settlement, administrative command, or arbitrary command list. The ticket
is not serialized into domain commands or stored records.

## Pending And Started

The broker factory arms a pending-only cancellation guard before returning its
future, including before its first poll. Dropping that future cancels Pending
authority whether it was never polled, waiting for queue capacity, or already
queued behind other owner work. Dropping an observer clone is inert. The unique
ticket's destruction also cancels only Pending authority.

Immediately before invoking the proposer, the single broker owner claims the
ticket using current UTC. An expiry equal to the current epoch second refuses;
a pre-epoch clock sample fails closed. A winning cancellation or expiry prevents
the proposer, domain clock, and message storage from being consulted. Competing
cancellation and claim use one atomic state transition; the winning refusal's
cause is retained rather than reconstructed from a later transport observation.

Started means admission, not a storage commit, final outcome, durable receipt,
or retry identity. Cancellation cannot revoke Started work. The existing
binding-fenced proposer still validates the entity incarnation before stamping
or mutation. A stale binding may read identity metadata; it does not authorize
access to the replacement incarnation.

The portable broker API defaults to an explicit unsupported result. A broker
adapter must implement the owned operation; it cannot silently fall back to
the legacy uncancellable command submission. Bound adapters require both the
same binding and its exact target. A primary entity and its dead-letter shadow
share an incarnation owner, not interchangeable receive authority. The same
restriction applies to sibling subscriptions and foreign namespaces.

## Authorization Horizon

After claiming native outgoing capacity, the ordinary listener snapshots the
longest-lived currently matching Listen grant for its actual admitted resource.
The grant lock is held before the UTC sample. The queued owner ticket retains
only the resulting numeric expiry, not token text, a grant collection, callbacks,
or connection lifecycle objects. Renewal cannot extend a ticket already made.

This is an expiry horizon, not a live revocation lease. Replacement grants or
permission reduction are also watched by the listener; their observed loss can
drop the receive future and cancel it while Pending. The numeric horizon alone
does not synchronously revalidate the live grant list at owner claim. Nor does
an inactive native identity independently prove that the broker future has been
dropped. Pending cancellation is established by the owned future's actual
cleanup, not merely by a peer Detach acknowledgement.

The intake packet owns the armed receive future before its claimed native
reservation. On cleanup, pending broker cancellation therefore precedes return
of local native capacity. The packet remains intact through polling. Ordinary
receiving still requires usable native credit before Receive and retains the
[bounded pipeline](ordinary-receiving-pipeline.md), independent held-delivery
identities, and existing settlement-before-final-ACK ordering.

## Limits

A Started receive-delete can commit deletion after detach or authorization
loss. A Started peek-lock Receive can commit a lock whose local response is
lost; existing lock expiry remains its fallback. There is no compensating
Abandon, cancellation rollback, retry deduplication, or recovery receipt.
Owner or response loss and physical storage failures are reported with static,
redacted causes; they do not establish that Started work had no effects.

The claim adds no arbitrary receive-operation timeout. Domain timestamps remain
the existing deterministic clock, separate from this runtime UTC expiry check.
Unsecured development receiving retains its legacy submission path. Session
acceptance, renewal, and release remain separate, unchanged commands. Sends,
settlements, management and administrative operations, and the experimental
transactional receiver do not inherit this ordinary Receive guard. Neither
native send admission nor [SDK batching](dotnet-receiving-batches.md) changes
these boundaries.
