# Native Transactional Ingress

`ServerConnection::accept_with_transactional_ingress` explicitly enables the
trusted native server's transaction-posting API. Ordinary server entry points,
the test client driver, and the Service Bus listeners still refuse transaction
traffic. This is not an SDK transaction gate or a persistent transaction log.
The [transaction wire types](amqp-transaction-types.md) remain distinct from
ordinary outcomes throughout this path.

## Admission And Control

The ordered connection actor classifies the original Attach as ordinary or a
coordinator before publishing its approval. Dedicated endpoint acceptance checks
that immutable classification and the exact pending approval again. Editing a
terminus cannot upgrade an ordinary approval into coordinator authority, or
convert a coordinator into an ordinary queue receiver.

Coordinators support local transactions, multiple transactions per session, and
posting across sessions on the same connection. Distributed/global declarations,
transactional acquisition and retirement, and recovered links remain unsupported.
Coordinator source outcomes may advertise Declared; ordinary source defaults
still cannot use Declared or transactional state. Pre-settled control commands
and postings are refused by this initial local policy.
Failed control commands use Rejected only when the original source advertises
it; otherwise the coordinator link is detached. A partial posting at discharge
always requires coordinator detachment, even when Rejected was advertised.
Trusted adapters can also consume original declaration and sealed staging
receipts through the [explicit refusal API](native-transaction-refusals.md),
without minting a declaration or reversing a started owner.

A declaration is an actual control-message receipt, not registration by an
arbitrary binary ID. Its successful Declared response is written and flushed
before the given ID becomes usable under that exact coordinator. The returned
opaque transaction identity observes its state and exact origin; its clones
retain no message or transport sender, grant no claim authority, and are inert
when dropped. Native IDs
retain the codec's 32-byte bound; this API does not promise process-wide ID
uniqueness. The separate [local registry](transaction-registry.md) issues its own
checked eight-byte IDs but is not connected to these endpoints yet.

The native actor bounds active groups to 32, postings per group to 100,
control-message content to 4 KiB, and terminal metadata history to 32 groups.
History retains each group's bounded original delivery proofs for exact abort
cleanup, but no messages, transport senders, or content reservations. These are
local limits, not Azure quotas or a process-memory bound.

## Ordered Obligations

A transactional first fragment registers an obligation before it can become an
application delivery. Fragmented, queued, application-held, and provisionally
acknowledged postings remain part of that admitted set. Aborts, decode refusal,
mailbox refusal, dropped receipts, and acknowledgment failure fault the group;
they cannot silently turn it into an empty transaction.

The same connection actor decodes a completed Discharge and seals the group
before publishing the control receipt or processing later frames, including
frames on other sessions. An unfinished posting at that boundary faults the
group immediately. The actor does not wait for application staging: callers
wait outside it, so pending acknowledgment commands can still run.

Explicit continuation state must retain the first fragment's ID and outcome.
An omitted continuation state inherits the original association as a named
compatibility policy. This differs from the explicit state on every fragment
required by [AMQP Part 4](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html).

Transactional postings use unique receipts, not clonable ordinary deliveries.
Provisional acceptance sends `TransactionalState(id, Accepted)` with
`settled = false`. A posting becomes prepared only after that disposition has
been written and flushed. This does not use ordinary terminal settlement to
remove the original transport identity.

Sender settlement before a provisional outcome is a local refusal, not evidence
that its acknowledgment was communicated. It cannot produce a prepared posting
or send an acknowledgment against a replacement delivery. Later sender
settlement can release a transport ID and tag, but not the native
group's original delivery proof or its retained content. Final wire completion
checks that exact delivery generation. Already-settled original deliveries need
no new disposition; reusable numeric aliases never authorize a replacement.
After commit, the final posting state is the actual Accepted outcome, not another
provisional transactional state. Final dispositions still honor the delivery's
receiver settlement mode: Second waits for sender acknowledgment before retiring
the original transport identity.
Known abort clears original transport obligations without applying an outcome;
application-held payloads and their charges remain owned until destruction.

## Owner Handoff

Observing a sealed group become ready grants no commit authority. Consuming its
control receipt with the exact set of prepared postings creates one submission
containing a unique ready ticket and all native resources. Those resources own
the original encoded-content reservations through the handoff. Dropping them
before a claim faults pending authority rather than releasing work behind a
still-usable ticket.

The ticket's claim competes atomically with pending faults and retirement.
Link retirement synchronously faults affected pending groups before retiring
their identities. An actor-owned native book faults pending groups before
[connection retirement](native-connection-identity.md), even when public
observers remain alive. Once an owner claim starts, close cannot undo it.
The unique claim records Committed, Rejected, or Indeterminate; dropping a
started claim records Indeterminate. These separate owner parts do not enforce
destruction order by themselves: trusted owners must finalize or drop claims
before releasing resources. Native resource finalization follows the recorded
decision, not a new commit.

The trusted [paired owner handoff](native-atomic-owner-handoff.md) now moves the
native ticket, native resources, logical registry ticket, staged commands, and
shared work lease into one owner job. It claims native authority first and
logical authority second before broker clock sampling, entity validation, or
storage access, and publishes both decisions before effects or replies.
Pairing does not prove that the two submissions represent the same wire work.
The serialized connection adapter and authorization and message-conversion
bridge remain missing. A physical storage failure
remains [indeterminate](atomic-commit-permits.md#result-boundaries), never a
guaranteed wire rollback. SDK transaction scopes, transactional outgoing
settlements, and durable recovery are not enabled.
