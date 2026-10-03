# Ordinary Receiving Pipeline

The ordinary Service Bus listener can transfer multiple deliveries on one
receiving link without waiting for the first peer outcome. Each held delivery
keeps its canonical broker identity and its exact native result independently.
The experimental transactional listener retains its separate existing path.

## Admission And Limits

Every broker Receive still requires a claimed
[native outgoing reservation](native-outgoing-admission.md) and a Listen check
before command submission. Zero usable credit does not acquire or delete a
message. Replaying an absolute Flow does not mint credit, and settling a delivery
does not itself grant fresh wire credit. A claimed slot is local admission;
the first Transfer still needs the current link credit and session window.

One ordinary link permits at most 32 work items. That count includes a pending
admission or Receive, and an acquired response parked under content pressure,
alongside held send jobs. Existing jobs are polled before the next lookup and
while admission, broker IO, or the empty-queue wait is pending. An empty lookup
returns its reservation before waiting for deliverability or the fallback.

Captured send jobs share a 4 MiB conservative projected-message content budget.
Projection borrows the actual Delivery before `write_delivery` copies content
and before the native owned future is constructed. It includes rich or legacy
content, broker metadata, session data, and canonical dead-letter properties;
the compatibility body of rich content is not counted twice. The charge stays
with its job through the outcome, canonical settlement, and final native ACK
or retirement of that transport wait, even if the native actor has already
released an earlier input or encoded copy.
This is not exact heap usage, serialized frame size, or the native connection's
separate encoded-content allowance.

Receive itself has already acquired and decoded its response before projection.
At most one such response may be parked outside the captured-message allowance.
An individually over-limit response is refused before conversion. A response
that fits alone but would exceed the current aggregate waits while other jobs
finish; the listener does not perform another Receive while it is parked.
Neither case promises pre-Receive allocation prevention or rollback. A committed
receive-delete stays deleted; an unsettled lock retains its expiry fallback.
Mutable entity limits are not used as an immutable bound on old stored rows.

## Independent Settlement

A started Receive remains alive across unrelated job completions. The worker
does not drop and recreate it merely because another delivery's outcome becomes
ready. It retains only one intake operation at a time; held native result futures
do not borrow the sender through their waits.

Each peek-lock job retains its canonical sequence, lock token, entity binding,
and exact native PendingSettlement. A peer outcome is converted to the existing
logical settlement command for that original. Listen is checked again before
submission. Only a successful canonical broker settlement is followed by the
existing caller-controlled final ACK. Reverse peer outcomes cannot redirect a
job to another held original or turn an observer identity into broker authority.
Receive-delete has already committed and requires no later broker settlement.

The associated-link management entry is owned by a synchronous cleanup guard.
Its private registration instance distinguishes replacements even if the link
name, lock token, sequence, and binding are identical. The guard remains with
the job through its final ACK or retired transport wait; explicit management
disposition may remove the entry earlier. Cancellation or task abort removes
only that owned instance,
without a detached cleanup task or an await gap. Legacy deferred-receive
registrations keep their existing explicit APIs and binding-fenced cleanup.
This guard is cleanup ownership, not authorization or settlement authority.

## Teardown Boundaries

Native detach or shutdown stops further intake and drops a pending lookup or
parked response. It drains existing send jobs while still watching Listen:
native retirement resolves pending send waiters, so this does not await another
peer outcome. An already decoded terminal outcome can still finish its canonical
settlement, preserving accept-then-immediate-close behavior. Broker IO during
that drain is not given a new time bound. A per-worker retirement signal stops
only its final transport-ACK wait after the exact native link retires; it does
not cancel the canonical settlement. Skipping that retired transport wait is
not a successful wire ACK, a credit refund, or settlement of a replacement.
Authorization loss instead cancels local work; it does not authorize a fresh
settlement after the grant is lost.

Exact management cleanup is synchronous; the worker releases its exact session
hold through the existing lifecycle before requesting its local link close.
Dropped native result waiters do not undo already admitted sends or
auto-acknowledge second-mode outcomes. Teardown does not synthesize Abandon for
every held message.

Broker Receive and settlement commands can still commit after their local
waiters are cancelled. The edge Listen checks are not a final owner-claim
authorization guard. These APIs provide no transactional admission, durable
recovery, cancellation rollback, connection-wide arbitrary-future heap cap,
or proof that SDK prefetch is an application-held-message limit. Separate
[pinned SDK batch gates](dotnet-receiving-batches.md) exercise held action copies
and rolling count-prefetch replenishment on one receiver.
