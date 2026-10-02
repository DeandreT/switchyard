# Retained Ingress

The native server receiver and feature-gated test client receiver provide an
additive `recv_retained` API. It returns a non-clonable `RetainedDelivery` that
owns the decoded message and its original native encoded-content reservation.
The ordinary `recv` and clonable `Delivery` APIs retain their existing behavior:
their content charge is released before the message reaches application code.

## Receipt Lifetime

Retained receipts expose borrowed `message()` and `message_format()` accessors.
Their [native connection origin](native-connection-identity.md) is available
separately from content accounting and ordinary settlement authority.
Their underlying ordinary delivery and content lease are private. Debug output
does not disclose payloads. A receipt cannot be cloned or converted into an
ordinary delivery that carries its lease. Application code can still explicitly
clone a borrowed message; this API does not account for such extra copies or
establish a process-RSS limit.

The original encoded-byte charge stays in the connection's existing 64 MiB
shared allowance after dequeue. Queue occupancy and link credit are still
consumed at dequeue, just as with ordinary receive. Retaining a message therefore
does not silently withhold mailbox credit until settlement or receipt drop.

The retained accept, reject, release, and modify operations use the same exact
delivery and link-generation checks as ordinary settlement. A wrong receiver or
stale generation cannot settle a replacement delivery. A successful settlement
does not release the content charge: the receipt owns it until destruction.
Dropping the receipt destroys its payload before refunding that reservation,
without sending a disposition, canceling broker work, or changing link credit.
Detach and session teardown likewise cannot refund an externally held receipt.

When a disposition is required, native settlement returns after it is written
and flushed. Sender-settled or already-settled receipts can complete without
another disposition. A second-mode sender acknowledgment may still be outstanding.
Retained receipt ownership does not add a wait for that acknowledgment. Failed
or canceled writes do not make an extant receipt's charge disappear.

## Listener Handoff

The sender-side data listener uses retained receives and retained settlements.
The receipt remains alive while ingress is parsed, the fenced broker operation
is awaited, and the terminal disposition is flushed. Parse refusal and broker
refusal keep the same lifetime through their rejection. Normal iteration,
failure, and explicit task cancellation release the receipt when it is dropped.
The native tests prove encoded-content retention; gated listener tests prove
acknowledgments still follow broker replies and preserve settlement modes.

This does not refactor listener task supervision. Socket closure alone does not
cancel an already-pending broker submission in a detached link task. Such a
receipt remains owned until that task's work returns or the task is explicitly
destroyed. Dropping a waiting task cannot undo an already-started physical commit.

The allowance remains a logical encoded-content tally, not decoded allocation,
custom-decoder expansion, transient copies, or a complete memory quota. CBS and
management receives retain their ordinary API and are not changed by this slice.

No transaction receipt, provisional acknowledgment, connection ordering barrier,
or successful wire Declare/Discharge is added here. Future transaction handoff
must move native obligations and content reservations into the same owner work
as staged commands; retaining them only in the local registry would not protect
queued or started work from registry cleanup. Wire transaction traffic remains
[explicitly unsupported](amqp-transaction-types.md).
