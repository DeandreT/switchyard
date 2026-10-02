# Native Outgoing Delivery Provenance

`PendingSettlement::delivery_identity()` exposes an opaque
`NativeOutgoingDeliveryIdentity` for the original native outgoing delivery
generation. The getter is available only after the existing send operation
returns its terminal or default outcome. It is historical provenance, not a
pre-outcome send handle, live-delivery notification, or settlement authority.

## Exact Original Generation

The connection actor mints the original generation once, when it admits the
first outgoing fragment, assigns its wire delivery ID, and consumes link credit.
The active send, unsettled record, resolved outcome, returned settlement wrapper,
and any later acknowledgment metadata retain that same generation. Outcome
resolution and acknowledgment creation do not reconstruct it from an ID or tag.

`same_delivery()` compares exact original generations and remains meaningful
after settlement, alias release, link replacement, or connection retirement.
Different deliveries can reuse the same numeric ID and tag on the same link
without sharing this identity. The observer has no public constructor,
serialization, numeric representation, or retirement operation.

`belongs_to_sender()` requires the exact original
[sending-link generation](native-sender-provenance.md) and an observed active
sender and connection. It does not require an unsettled transport alias: a
completed First-mode or already-settled delivery can still match its active
sender. A retired link, retired connection, or test-only unbound origin fails
the membership check. `connection_identity()` exposes only optional original
[connection metadata](native-connection-identity.md).

## Ownership And Limits

Cloning or dropping the observer is inert. It retains only original-generation
metadata, including the link owner and private wire ID, not a body, delivery
tag, content reservation, command sender, watch channel, or transport lifecycle.
It does not delay the existing content refund after the full transfer is flushed
or keep an endpoint active. Caller-retained observer clones are not bounded by
an application-memory quota.

The observer is distinct from `PendingSettlement`, which remains the existing
outcome and acknowledgment wrapper and can retain a command sender. The late
acknowledgment token also remains a separate exact token with its original
settlement rules; retaining the original generation does not grant acknowledgment
authority.

An active-origin match is only an observation and can change immediately. It
does not establish that this delivery is still live, that settlement will
succeed, or that an authorization grant, domain binding, or owner claim remains
valid. Existing settlement checks are unchanged.

Queued-send and cancellation behavior is unchanged. Dropping a waiting send
does not undo already-admitted native work, and no successful public observer is
returned before a settlement receipt or from a failed send. This API does not
enable transactional retirement or acquisition, alter transactional-disposition
refusals, or activate clients, default listeners, or SDK transaction scopes.
The separate [transactional-work opt-in](native-transactional-retirement.md)
returns a unique fully flushed sent handle and captures peer retirement receipts.
Its disposition consumer and prepared resources, not this clonable observer,
carry that authority.
