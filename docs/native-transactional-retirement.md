# Native Transactional Retirement

`ServerConnection::accept_with_transactional_work()` explicitly opts a native
connection into posting and the limited retirement path described here. The
default connection and existing `accept_with_transactional_ingress()` remain
unchanged; the latter is still posting-only. No listener, server configuration,
domain adapter, or SDK transaction scope is activated by this native API.

## Dedicated Sending Endpoint

`ServerSession::accept_transactional_sender()` approves only an original peer
receiver Attach with sender mode Unsettled and receiver mode Second. A dedicated
`TransactionalSender::send_with_dispositions()` returns a unique `SentDelivery`
only after the complete outgoing Transfer has been flushed. Its
`delivery_identity()` exposes the original
[historical generation](native-outgoing-delivery-provenance.md), and
`next_disposition()` consumes its bounded, rearmable per-original inbox.

`TransactionalDisposition::Ordinary` carries the existing `PendingSettlement`.
`TransactionalDisposition::Retirement` carries a unique
`TransactionRetirementReceipt` derived from an actual peer receiver disposition,
not a caller-supplied transaction command, delivery ID, or tag.

The initial retirement subset accepts only an Accepted transactional outcome
with `settled=false` for a fully flushed, live original delivery. First-mode,
source-settled, peer-settled retirements, nonterminal Received updates,
other transactional outcomes, and retirement of an incomplete outgoing send are
unsupported. They are refused before retirement mutation. An incomplete outgoing
send is not an inbound partial posting and does not acquire the posting-specific
`PartialAtSeal` interpretation.

## Capture And Preparation

The native actor preflights the whole matched disposition range before changing
any row. It verifies dedicated endpoint policy, original generation, live
transaction and consumer, supported outcome, and available inbox capacity.
Postings and retirements share the existing 100-obligation transaction limit;
retirements do not receive a separate allowance.
Dedicated sends also retain the existing outgoing limits of 1,024 delivery tags
per link and 4,096 outstanding deliveries per session. The inbox holds one event
per original; none of these limits is an exact heap-memory quota.

Each retirement attempt has an exact identity distinct from the stable original
delivery identity. Capture registers that obligation before the application can
dequeue it and before a later decoded Discharge seals the group, including work
across sessions on the same connection.

Consuming `provisional_accept()` flushes a Sender-role Accepted transactional
disposition with `settled=false` and returns `PreparedRetirement`. This echo is
an explicit local preparation contract, not a general retirement requirement of
the AMQP standard. `NativePreparedWork` combines posting and retirement proofs;
`SealedDischargeReceipt::prepare_work()` requires the exact complete mixed set.
The existing posting-only `prepare()` API remains available.

`SealedDischargeReceipt::retirement_origins()` observes the sealed live group's
bounded exact sender and original-delivery metadata, including receipts not yet
dequeued by a collector. The snapshot is inert and grants no settlement or
commit authority. Terminal replay receipts return `None`, not a claimed empty
manifest. This lets a serialized adapter account for captured retirements before
acknowledging rollback and rearming a worker.
The companion `posting_receivers()` snapshot includes one exact receiver origin
per captured posting, including repeated origins on the same link. It has the
same terminal-replay and inert metadata boundaries; it is not a prepared manifest.

## Rollback And Lifetime

Known rollback restores the original live delivery and preserves its ID, tag,
and disposition consumer. It does not resend the payload, apply the proposed
outcome, emit Released, or use posting-style alias cleanup. The peer can then
dispose the same original ordinarily or through a new transaction. Cleanup
compares both original and current attempt, so a delayed old attempt cannot
reset a later retirement of that original.

Controller closure and revocable fault wake native actor reconciliation; cleanup
does not depend on eventual application resource finalization or terminal-history
retention. A full inbox is refused before a later update mutates state; an old
queued event is not silently overwritten.

An OwnerStarted operation is not revoked by source or controller closure.
Committed and Indeterminate decisions are not restored or made retryable.
Explicit known Aborted or Rejected decisions can restore an active exact
original. Loss of an owner response is not evidence of rollback.

Dropping a sent handle, losing its reply, or dropping a retirement receipt
faults an associated revocable attempt before its queued receipts are destroyed.
This does not cancel already admitted native transfers, create a domain
settlement, or acknowledge an ordinary delivery. Encoded content is still
refunded after the complete Transfer flush; later retirement bookkeeping retains
metadata rather than message bodies. The historical observer remains inert and
does not own this consumer-drop control.

## Remaining Scope

Early-settlement rollback requires source-default handling and is not implemented
by this subset. Transactional acquisition, prior nonterminal-state restoration,
and other retirement outcomes remain separate work. The separate
[atomic messaging listener](atomic-messaging-ingress.md) establishes bounded
correspondence to actual primary-queue PeekLock deliveries; this native API
does not activate it by itself. The
[posting-only listener](atomic-posting-ingress.md) remains posting-only.

These distinctions follow [AMQP Transactions sections 4.4.2 and
4.4.4](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html)
and [AMQP Transport sections 2.7.5, 2.7.6, and
2.8.3](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html).
