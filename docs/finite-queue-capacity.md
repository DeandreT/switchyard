# Finite Queue Capacity

Finite capacity is an opt-in logical reservation policy for a primary queue and
its dead-letter shadow. It is not physical disk usage, encoded AMQP size, a
namespace quota, or measured Azure byte accounting. Ordinary creation remains
non-finite; it still writes mandatory generation-bound capacity-mode metadata.

## API And Scope

The separate `QueueCapacityCommandV1` supports `CreateFinite` and
`SetLimitFenced`. `FiniteQueueCapacity` accepts nonzero unsigned 64-bit limits.
`StateMachine::describe_queue_capacity` is clock-free. The broker handle's
asynchronous and blocking describe methods run that read in one serialized owner
turn. The view contains the binding, queue configuration, mode, reserved bytes,
and message count. Capacity mutations stamp their time and prepare their return
view before the storage batch commits, without a post-commit reread.
Describing an owner checks its metadata and aggregate, not its entire message
ledger. Existing command, queue configuration, protobuf, and committed-entry
encodings are unchanged.

Finite queues currently exclude session-required queues and duplicate detection.
An ordinary queue may retain an optional session identifier as metadata; that
does not grant session ownership or affinity. Topics, subscriptions, session
state, duplicate-history storage, and namespace accounting are outside this
policy. A dead-letter shadow shares its primary owner's limit and cannot have a
separate capacity mode or aggregate.

A limit change requires the live primary-queue binding. Stale bindings are
refused before the proposer reads its host clock. Lowering a limit below retained
reservations is refused atomically; restating the existing limit is a no-op and
does not advance the stored command clock. Non-finite queues cannot be promoted
in place, and finite queues cannot be demoted. Delete and recreate is explicit,
with a new entity generation; there is no ledger backfill or migration API.

These methods are trusted library APIs, not authorization boundaries. Neither
native gRPC/CLI nor Atom/XML administration currently exposes capacity creation,
updates, or usage. Configuration and limit updates are not yet one combined
desired-definition operation.

## Reservation Model

Each retained message has a generation-bound charge with numeric components:

```text
C = P + S + 256 + max(256, D)
```

For legacy byte-body messages, `P` is body bytes plus five bytes and the UTF-8
identifier length. For rich messages, it is the larger of the compatibility-body
size and retained envelope content size, including any authoritative identifier
not already represented there. `S` is zero without an original session ID,
otherwise five bytes plus its UTF-8 size. `D` is zero without dead-letter details,
otherwise 81 bytes plus the UTF-8 reason and description lengths. These constants
are local model version 1, not storage-record or wire-envelope overheads.

The prepaid dead-letter reserve covers automatic lifetime and delivery-count
reasons. Explicit dead-letter details or retained property updates can increase a
charge and are refused if the resulting total exceeds the limit. A valid proposed
reservation that overflows its unsigned total is also a capacity refusal, not
stored-state corruption.

Ready, locked, deferred, scheduled, and dead-letter messages all retain their
reservation. Peek-lock receive, renewal, and lock expiry do not free it. Completion,
receive-delete, schedule cancellation, and expiry without dead-lettering refund
the original charge. Completing with unretained property updates does not charge
those discarded updates. Retained updates can grow or shrink a reservation.
Schedule activation moves the charge to the activated sequence without charging
twice. Dead-letter transfer strips the outer lifetime and session metadata but
preserves the original session-byte reservation and producer envelope.

The normal message mutations, charge sidecars, and aggregate update share one
storage batch. Validation or capacity refusal commits none of them. Ordered
atomic messaging can reuse credit released by an earlier completion in that
same operation; a later refusal rolls back the complete operation. Capacity
refusals map to AMQP `amqp:resource-limit-exceeded` and native resource exhaustion,
without exposing stored records in the error.

## Validation And Work Bounds

Private Mode, Usage, and Charge values use canonical version-11 envelopes of at
most 64 bytes. Their schema, model, generation, arithmetic, and physical owner
must agree. Finite mutations validate each original message observation against
its charge and the aggregate. Distinct observed original charges must fit the
initial aggregate, including a valid residual byte/count pair. This detects
inconsistencies encountered by that operation; it does not scan or reconcile the
entire ledger. A raw queue-config lookup is not an owner-health proof.

One capacity plan bounds 1,024 distinct source or admission message identities,
2,048 events, 2,048 ledger
keys, 4,096 added point reads, 4 MiB of added read-key bytes, and 256 KiB of
materialized read values. Ordinary finite send/receive checks add point reads,
not prefix scans. These are logical planner bounds, not total command processing,
RSS, allocation, or elapsed-time guarantees. `StateStore::get` materializes one
value before the planner can account for its size. Existing handler validation
and input-preprocessing policies remain separate. Non-finite queues retain their
existing large trusted-vector behavior.

Deletion verifies the owner's identity and capacity mode, then purges Usage and
Charge as opaque bounded runtime data. A previously obtained valid binding can
therefore delete corrupt ledger values without treating deletion as repair or
bypassing the incarnation fence. Empty finite queues do not need lazily allocated
message counters to be deletable. Recreated queues do not adopt old-generation
sidecars.

## Durable And Image Boundaries

Active durable layout 17 protects the mandatory mode and finite sidecars. Older
directories are refused, and an ordinary unversioned directory with nonempty
metadata or message records is refused before a new marker is written. There is
no automatic relabeling, migration, or rollback conversion.

Current Create/Send images use `CreateSendLayout17V1` (role 2) and include exactly
one canonical non-finite mode for each generation-1 primary queue. Current
export, bootstrap, retained catalog validation, replacement, protected-image
checking, and native snapshot metadata use that proof. Historical
`CreateSendV1` (role 1) validation and its pure planning/checking APIs remain
explicitly separate; current restore paths do not strip modes or relabel images.
Source-profile refusals precede target storage access. Finite modes and their
sidecars are outside the current image profile, and finite owners are refused by
legacy committed Create/Send work without checkpoint advancement. Supporting
finite replication needs a new payload contract, not a changed interpretation of
the existing entry or fingerprint.

Tests exercise both memory and Fjall stores, physical Fjall reopen, ordered
atomic credit reuse, ledger corruption, opaque fenced deletion, and actual AMQP
socket rejection/recovery. The socket checks use the in-tree Rust client, not an
official SDK capacity gate. Injected pre-apply backend failures establish no
partial batch in those fixtures; they do not establish the outcome of an
indeterminate physical commit.
