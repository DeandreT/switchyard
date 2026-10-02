# Native Receiver Provenance

An opt-in [transactional receiver](native-transactional-ingress.md) exposes
`receiver_identity()`, an opaque `NativeReceiverIdentity` observer of its exact
actor-accepted link generation. Reusable channels, handles, names, delivery IDs,
and tags cannot reconstruct that identity. It has no public constructor,
serialization, numeric representation, or retirement operation.

## Origin And Activity

`same_receiver()` compares exact origin, including after retirement. Cloning or
dropping an observer does not keep the endpoint active, detach it, retain
payloads, or retain a transport sender. Debug output reports only activity.

`connection_identity()` exposes the link's optional original
[native connection identity](native-connection-identity.md). Missing provenance
must be refused rather than inferred from numeric labels. `is_active()` requires
both a non-retired link and a present, active connection. Closing a link or
session can therefore deactivate a receiver while its connection remains active.

Posting receipts and prepared postings implement `belongs_to_receiver()`. The
comparison requires the exact delivery owner's receiving-link generation and
observed receiver activity. Provisional acknowledgment and later sender
settlement do not replace the stored origin when a transport alias is released.
An alias reused by another delivery is not that original delivery.

Activity is an observation, not an admission reservation, authorization grant,
or settlement permit. It may change immediately after a check. Exact receiver
provenance does not prove a delivery is still unsettled, a domain queue binding
is current, or native and logical transaction work correspond. The existing
native claim and domain generation fences remain authoritative.

`TransactionalReceiver::on_detach()` exposes the existing endpoint detach
notification. A connection-close notification alone is not an actor-retirement
barrier; the separate connection lifecycle retains its documented semantics.

## Link-Local Message Formats

`ServerSession::accept_transactional_receiver_with_decoders()` accepts an
explicit `MessageFormatDecoders` registry for that receiving link. The original
acceptance method still uses the default registry. Format zero is the immutable
built-in AMQP message decoder; at most eight distinct custom formats can be
registered. Coordinator endpoints continue to accept only their default control
format. The decoder method cannot upgrade an ordinary approval into coordinator
authority or enable a connection whose transaction policy is disabled.

A Service Bus batch format can be registered with the existing AMQP message
decoder. This preserves its outer message, format, and original encoded-content
charge as one native posting obligation. It does not expand inner messages,
authorize their destination, or stage a logical SendBatch. Those operations
belong to the future serialized application adapter and must finish before a
provisional acceptance is issued.

Unknown formats, inconsistent continuation formats, and decoder failures retain
their existing refusal and native-group fault behavior. The registry adds no
unbounded decoder list, second parser, persistent format, or recovery log.
Service Bus listener transactions and SDK transaction scopes remain disabled.
