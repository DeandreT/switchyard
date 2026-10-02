# Atomic Work Reservations

Trusted Rust callers can stage atomic messaging work under one shared
`AtomicMessagingWorkBudget` and submit it with its reservation intact. This is
ownership infrastructure for future connection transactions, not an enabled
wire transaction registry. Existing trusted and guarded APIs remain available
and do not automatically join a budget.

## Input Accounting

`domain::AtomicMessagingInputUsage` measures one new command by borrowing it.
Its counters cover actions, logical sent messages, command-content bytes, and
message value items. `try_extend` updates all counters only after the complete
candidate passes the existing allowlist, depth, and aggregate input limits.
A failed extension leaves every counter unchanged. Staging therefore need not
rescan earlier commands each time it appends another operation.

Whole-group validation retains its existing upfront action-count check and
uses that same borrowed accounting core. The
[per-group bounds](atomic-queue-operations.md#local-resource-bounds) are unchanged.
These checks do not establish message shape, queue configuration, live entity
generation, lock validity, or authorization. The owner still performs normal
proposer and deterministic application validation.

## Shared Reservations

Each budget has fixed local caps:

| Retained Work | Maximum |
| --- | --- |
| Outstanding group slots | 32 |
| Shared command-content tally | 8 MiB |
| Shared message value items | 131,072 |

`stage(binding)` reserves a slot immediately, even for an empty group. A
`StagedAtomicMessaging` group owns its commands and its non-clonable lease.
`try_push` consumes one candidate, retaining it only after both its per-group
input checks and the shared budget checks succeed. Refusal leaves existing
commands and accounting unchanged; the rejected candidate is destroyed outside
the accounting lock. Cloning or dropping a budget observer does not release
another group's reservation.

Slots also bound zero-content work, such as empty groups and settlement-only
groups. Aborting a commit permit does not refund the slot or content while its
commands still occupy the owner queue. Capacity becomes reusable only when the
work that owns the lease is destroyed. These are limits per explicitly shared
budget, not process-wide, automatically enforced per-connection, serialized
size, or process-RSS guarantees. A candidate already exists before it is
measured, and command-vector capacity can be reserved before shared admission.

## Owner Handoff

`into_submission(ticket)` moves the commands, unique commit ticket, and lease
into one `OwnedAtomicMessagingSubmission`. The
`BrokerHandle::submit_atomic_messaging_owned` and blocking counterpart queue that
whole container. The async factory arms cancellation before its first poll,
using the same [commit permit decisions](atomic-commit-permits.md) as the guarded
API.

The owner claims the ticket before external validation, storage reads, or clock
sampling. It retains `AtomicMessagingOwnerWork` through command application,
terminal decision publication, receiver wakeups, and reply delivery. A refused
claim also retains the reservation through its reply. A dropped reply or caller
does not refund an already-started operation prematurely. On cancellation,
shutdown, rejection, success, or unwind, payload destruction precedes lease
refund. A lost result can still be indeterminate; reservations do not turn
storage uncertainty into rollback or retry safety.

The owner work's `with_commands` callback consumes commands once without holding
the budget lock. This is a trusted Rust lifetime contract: custom owner code must
not retain or return those commands beyond the work's lifetime. The built-in
owner consumes them synchronously and keeps the scope through its reply.
Repeated consumption is refused, not executed again. Debug output excludes
command payloads. Poisoned accounting admission fails closed, while existing
work can still be destroyed and refunded.

No storage format, replicated command, broker-trait wire contract, controller
identity, transaction ID, provisional acknowledgment, or native ordering barrier
is added here. AMQP transaction traffic remains
[explicitly unsupported](amqp-transaction-types.md).
