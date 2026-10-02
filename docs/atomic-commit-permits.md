# Atomic Commit Permits

The trusted broker API has a separate cancellation-aware entry point,
`BrokerHandle::submit_atomic_messaging_guarded`, and a blocking counterpart.
They use the same bounded [atomic queue operations](atomic-queue-operations.md),
without changing the original API's admitted-work cancellation behavior.
This is runtime infrastructure, not a wire transaction lifecycle.

## Authority And Decisions

`AtomicCommitPermit::new(deadline)` creates a clonable observer and one
non-clonable `AtomicCommitTicket`. The observer can inspect the decision or
abort pending work. Only the ticket can submit the group and acquire owner
authority. Neither type is a durable transaction ID, authorization grant, or
retry-deduplication token. The permit retains no message content, outcomes, or
storage error strings.

The state transitions are deliberately small:

| State | Meaning |
| --- | --- |
| `Pending` | The owner has not acquired execution authority. |
| `Aborted` | Execution authority was revoked before acquisition. |
| `Started` | The owner acquired authority; cancellation cannot undo it. |
| `Committed` | Application succeeded, including a successful empty group. |
| `Rejected` | A known logical or clock refusal occurred before physical commit. |
| `Indeterminate` | The owner cannot promise either commit or rollback. |

Abort and owner acquisition compete at one atomic transition. The owner claims
the ticket before entity validation, storage reads, or broker clock sampling.
At or after the monotonic `std::time::Instant` deadline, acquisition aborts
pending work. The deadline is checked at acquisition, not by a background timer;
it does not interrupt an already-started storage operation. This clock is
separate from the deterministic timestamp stamped into domain commands.

## Cancellation And Shutdown

The async entry point constructs its cancellation guard before returning the
future. Dropping even an unpolled future aborts pending authority. Dropping a
ticket or an explicit abort-on-drop guard has the same pending-only effect;
dropping an ordinary observer clone is inert. After acquisition, caller
cancellation and a dropped reply receiver cannot revoke the owner's authority.

The private broker request queue fences new admission when its owner exits,
including unwinding. It releases queued requests and wakes callers waiting for
capacity. Destruction of an unclaimed queued ticket aborts it even while idle
broker handles remain alive. Work waiting outside the full queue remains owned
by its caller until that future resumes or is dropped; it is never applied by a
stopped owner.

Borrowed input-kind and budget validation happens before queue admission. A
local refusal returns the ordinary typed error and destroys the unclaimed
ticket, leaving `Aborted`, not an owner-issued `Rejected` decision. Entity and
content validation still run on the owner and in deterministic application.

## Result Boundaries

The owner records a terminal decision before receiver wakeups or reply delivery.
Successful application records `Committed` even if the reply is lost. Logical
and clock refusals record `Rejected`. Every storage error after acquisition is
conservatively `Indeterminate`, including an injected pre-commit error; an
unexpected outcome is also indeterminate. Dropping an unfinished owner claim,
including during unwinding, records `Indeterminate`.

`BrokerStopped` describes a lost request or reply path, not guaranteed rollback.
The observer's decision must be consulted separately. An indeterminate result
must not be retried blindly: a complete storage journal batch can be recovered
after an error was reported. A ticket prevents duplicate local submission by
ownership, but does not make a newly created ticket or a reconnect replay
idempotent. No decision survives a process restart.

These APIs add no connection transaction registry, provisional settlement,
held-message undo, AMQP Declare/Discharge acceptance, or authorization checks.
The separate [owned work API](atomic-work-reservations.md) keeps bounded resource
reservations attached to commands throughout queueing and owner completion.
The wire still [refuses unsupported transaction traffic](amqp-transaction-types.md).
