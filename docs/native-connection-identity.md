# Native Connection Identity

The native server and feature-gated test client expose an opaque
`NativeConnectionIdentity` through `connection_identity()`. Each successfully
negotiated connection actor gets a fresh runtime identity. Reusable channels,
link handles, delivery IDs, tags, and container names cannot reconstruct it.
The server and client actors on opposite ends of one socket have distinct local
identities; this is not an identifier exchanged with the peer.

## Observation And Retirement

Cloning an identity creates an inert observer. It does not own a connection,
retain transport channels or payloads, or delay cancellation. Dropping an
observer cannot retire the identity. There is no public constructor, numeric
conversion, serialization, or retirement operation. Debug output reports only
activity, not an address or identifier.

`same_connection()` compares exact origin and remains meaningful after
retirement. `is_active()` observes whether that actor has retired. Activity can
change immediately after a successful check: it is not an admission permit,
resource reservation, authorization grant, or guarantee that a future operation
will execute.

The owning connection lifecycle requests cancellation when dropped. Requesting
cancellation does not itself retire the identity. An actor-owned guard retires
it when the driver exits, before publishing the termination notification. The
guard is captured before spawning, so destruction before the first poll, task
cancellation, and unwinding also retire the identity.

Successful `close()` and awaited `shutdown()` observe actor termination and
therefore retirement. Normal driver cleanup joins its reader and tears down its
sessions before termination. On abnormal cancellation or unwinding, the reader
is aborted rather than joined; identity retirement alone does not prove that
all socket cleanup has completed. The test client's `on_close()` remains its
existing peer-close notification, not an actor-retirement barrier.

## Receipt Origin

Negotiated drivers bind session and link generations to their original native
connection. Child links inherit that proof rather than deriving it from a
channel or handle. Existing exact link and delivery checks remain separate.

A [retained ingress receipt](retained-ingress.md) exposes its delivery owner's
optional `connection_identity()`. Callers must refuse a missing origin instead
of guessing one from numeric labels. Receipts published by negotiated native
drivers carry their exact origin. `belongs_to_connection()` requires an origin,
the same runtime connection, and observed activity.

Settlement, link detach, or session End does not change connection provenance.
A receipt can still belong to its active connection after its own link or
delivery has retired. This comparison does not prove that the receipt remains
settleable, that a domain message lock is held, or that the connection has any
entity rights. After actor retirement the origin remains available for
comparison, but `belongs_to_connection()` is false.

These identities are process-local observations, not durable IDs, native
controller admission, transaction receipts, or a connection ordering barrier.
They do not bind the trusted [local transaction registry](transaction-registry.md)
to a wire coordinator. Transaction traffic remains
[explicitly unsupported](amqp-transaction-types.md).
