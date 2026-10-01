# AMQP Transaction Types

The native AMQP crate represents transaction control markers without silently
discarding their meaning. This is wire-type coverage, not an implemented transaction
lifecycle. Official client transaction scopes cannot yet commit or roll back
messaging operations through coordinator links.

## Representation

`Attach.target` is an optional `TargetTerminus`: either an ordinary `Target` or a
`Coordinator`. Ordinary client attach conveniences still accept a queue target;
they convert it internally. A coordinator cannot also carry an ordinary address.

The codec supports numeric and symbolic descriptors for `Coordinator`, `Declare`,
`Discharge`, `Declared`, and `TransactionalState`. `TransactionCommand` converts
Declare/Discharge values to and from an AMQP `Value`; these are control-message
bodies, not new frame performatives.

`TransactionId` checks an opaque binary identifier's 32-byte maximum before
copying it. Zero bytes are permitted by the type contract. Required IDs reject
missing, null, non-binary, and oversized fields. Declare retains an optional
global ID, including an unknown described value, without implying distributed
transaction support. Coordinator capabilities accept a scalar symbol or a
symbol array. Empty decoded arrays normalize to absent capabilities: the generic
`Value::Array` representation does not retain an empty array's constructor.
Multiplicity follows the [composite-field rules](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html#section-composite-type-representation).
The new capability field checks the native value-item ceiling and 4 MiB of
summed symbol bytes before cloning. These are field-local work limits, not a
whole-frame allocation or resident-memory guarantee.

Declared is both an outcome and a terminal delivery state. Transactional state
is a nonterminal delivery state with an optional outcome; converting it to an
ordinary outcome is refused rather than discarding its transaction identifier.
These types follow [AMQP 1.0 Part 4](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html).

## Unsupported Lifecycle

Both native connection drivers explicitly refuse coordinator admission,
transactional transfers, transactional dispositions, and transactional acquisition
through Flow properties. These control markers cannot silently enqueue a normal
message, settle a held message, consume a pending ordinary acknowledgement, or fall back
to ordinary credit handling. Existing ending-session, error-link, and error-delivery
classification takes priority where it already governs the frame.
An arbitrary described value inside an ordinary queue message remains message
content; its descriptor alone does not select a transaction lifecycle.

Fresh coordinator requests, transactional transfers to a live receiving link,
and transactional dispositions receive a scoped session End with
`amqp:not-implemented`. On an installed live link, a Flow containing
`txn-id` instead receives a closed link Detach with that condition before changing
the session window or link credit. A handleless request or a mapped pending link
without an installed owner ends its session. Unassigned handles and transfers
in the wrong direction retain their earlier routing-error classification.
Presence of the property is refused even when its value is null. This is the
local unsupported-feature policy, not a
Declare/Discharge or transactional-acquisition implementation.

Ordinary links accept only ordinary messaging outcomes as their source default.
Declared can round-trip structurally in the codec but cannot become an ordinary
link's settlement default. The Service Bus adapter independently refuses Declared
as a domain settlement.

There is no transaction registry, Declare/Discharge execution, staged message
visibility, provisional acknowledgement exchange, transaction timeout, or wire
retry contract in this increment. The separate trusted same-queue foundation is
described in [Atomic Queue Operations](atomic-queue-operations.md). Its uncertain
physical commit result must not be reported as guaranteed wire rollback.

## Verification

Codec tests cover descriptor aliases, required fields, ID boundaries, capability
forms, retained global IDs, outcome distinctions, and encoding limits. Native
driver and dual-backend socket tests verify explicit refusals, unchanged ordinary
message state, and continued use of unrelated sessions. The existing official
SDK checks exercise ordinary operations, not transaction scopes.
