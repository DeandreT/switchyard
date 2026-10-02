# Native Sender Provenance

An actor-accepted native server `Sender` exposes `sender_identity()`, an opaque
`NativeSenderIdentity` observer of its exact sending-link generation. Channels,
handles, names, delivery IDs, and tags cannot reconstruct that origin. The
observer has no public constructor, serialization, numeric representation, or
retirement operation.

## Origin And Activity

`same_sender()` compares exact link origin, including after retirement. Cloning
or dropping an observer is inert: it does not detach the endpoint, keep it
active, or retain commands, watch channels, transport lifecycle, message payloads,
delivery IDs, or tags. Debug output reports only activity.

`connection_identity()` exposes the link's optional original
[native connection identity](native-connection-identity.md). `is_active()`
requires a non-retired link and a present, active connection. An inactive link,
retired connection, or test-only unbound identity fails this activity check;
numeric labels cannot supply missing provenance.

`PendingSettlement::belongs_to_sender()` requires that exact sending-link origin
and observed sender activity. It uses the existing settlement wrapper's link
identity, not an acknowledgment ID or reusable transport alias. The check also
works for First-mode and already receiver-settled outcomes, which can have no
pending second-mode acknowledgment token.

The observer and settlement wrapper are different ownership types.
`PendingSettlement` remains the existing outcome/acknowledgment wrapper and can
retain a command sender. Obtaining a sender observer or testing the wrapper's
origin does not move, clone, or settle its outcome and does not change native
content accounting.

## Limits Of The Proof

Activity can change immediately after observation. A matching sender is not an
authorization grant, owner claim, current domain binding, settlement permit, or
proof that the delivery remains usable. Settlement still performs its original
exact-identity and liveness checks.

This is link provenance, not an original outgoing-delivery generation. Different
deliveries on the same active sender share that link origin. The existing
`AckIdentity` is created only after an ordinary terminal outcome is resolved;
it is not a proof covering the delivery's entire outgoing lifetime. Reusing an
ID or tag cannot turn these comparisons into authority over a replacement
delivery.

Transactional receiver dispositions are still refused before ordinary settlement
mutation, on both ordinary native connections and the opted-in
[posting lifecycle](native-transactional-ingress.md). This observer adds no
transactional retirement, acquisition, provisional outgoing outcome, or SDK
transaction-scope activation. Default listener behavior is unchanged.
