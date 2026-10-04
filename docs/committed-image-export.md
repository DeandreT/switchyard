# Bounded Domain Image Export

`CommittedStateMachine::export_create_send_image(&mut self)` captures one
complete bounded view and returns an immutable `EncodedCommittedImage` only
after [container checks](committed-image-container.md) and
[whole-image validation](committed-image-validation.md). It is available only
when the writer's matching reader implements `storage::BoundedStateStore`.
There is no allocating fallback for ordinary `StateStore` readers.

This is a synchronous domain API, not a native-owner operation or runtime
snapshot implementation. Constructors, application, store formats, and ordinary
snapshot behavior remain unchanged. No writer or store capability is returned.
The method grants no installation, log purge, quorum, authentication, ancestry,
anti-rollback, historical-request, or cluster-membership authority.

## One Captured View

The method refuses an already poisoned machine before any I/O. Otherwise it
calls its existing matching reader's `snapshot_bounded` exactly once, with the
container's row, key, value, and total data-byte caps. It encodes those exact rows
using the machine's stream identity, requiring the captured checkpoint to match.
The source snapshot is then dropped before decoding and semantically validating
the same encoded bytes. The returned artifact is unchanged by those checks.

Export does not mix in a separate checkpoint, point read, scan, initialization
query, clock, or write. Constructors already established initialization and the
unique writer: creation commits initialization and the initial checkpoint
together; opening validates initialization, canonical progress, and stream.
The matching reader is read-only. These are trusted storage-contract assumptions,
not a new attestation against an implementation that violates its contract or
trusted out-of-band backend changes. All exported progress comes from the
checkpoint row in the captured complete stable view.

## Error Policy

`CommittedImageExportError` contains only static causes, with no backend detail
or supplied source content:

| Cause | Effect |
| --- | --- |
| `Poisoned` | Refuse before I/O; existing poison remains |
| `ReadFailed` | Any non-quota bounded-read storage error poisons the machine |
| `LimitExceeded` | Capture or artifact quota refusal; nonfatal |
| `Allocation` | Container output reservation failed; nonfatal |
| `UnsupportedProfile` | Recognized legacy/broader profile or format; nonfatal |
| `InvalidImage` | Other captured checkpoint, container, or semantic refusals; nonfatal |

An arbitrary opened store is not known to have run only Create/Send. Relational
or unsupported-profile refusal alone cannot establish corruption, so export does
not newly poison the machine for those causes. It neither repairs nor normalizes
source records. Physical read failure does poison further export and application;
both refuse before additional I/O. Existing independent checkpoint reads after
poison remain available as before. No retry or panic containment is added.

## Memory Scope

Captured key/value data and the complete serialized artifact each have their
own 64 MiB logical bound. Source and artifact coexist during encoding, up to
128 MiB of their logical data, not a single shared memory quota. Row limits,
1,024-byte keys, and 260 KiB values also apply. Framing can make an artifact exceed
its cap even when raw source data fits; that is a refusal, never truncation.

Collection metadata, spare capacity, backend allocations or materialization,
small checkpoint metadata, caller-retained artifacts, concurrency, and RSS are
not bounded by those figures. Source rows are released before semantic metadata
maps are built. Validation borrows large bodies rather than copying them or
building another body-sized canonical encoding. Other allocations can still
fail or exhaust memory; `Allocation` covers output reservation, not all OOM paths.

## Verification Scope

Observed Memory and Fjall fixtures record all capture budgets and point reads,
scans, initialization queries, privileged commits, and ordinary write attempts.
Their ordinary snapshot method deliberately refuses any fallback. After
constructor setup, successful and refused export require exactly one bounded
capture and no other observed operation. Existing poison requires no operation.

Actual committed inputs include the maximum body/ID/session, a duplicate
sequence hole, expired history, a refusal watermark gap, initial/refusal-only
state, and opaque membership. Fault injection covers before/after physical
commit failures and a redacted physical read failure. Both backends exercise an
actual oversized-value read quota and unchanged source snapshots. Raw fixtures
check nonfatal malformed records, missing ready indexes, and captured-checkpoint
refusals. A reopened version-10 message is also verified under the ordinary
migration decoder before its conservative export refusal. Healthy later export
and application checks pin nonpoisoning behavior.

A dedicated Fjall case drops the machine and every reader, opens the same
directory, and compares the complete source and exported bytes. A compile-fail
contract requires the bounded-read capability. Static mapping tests cover output
allocation and container causes without forcing unsafe allocator exhaustion.

No full 64 MiB artifact, 65,536-row export, RSS instrumentation, forced actual
OOM, native-owner cancellation or joined-shutdown proof, durable runtime
snapshot metadata, installation, or automatic history compaction is established
by this increment.

The export checkpoint passed 16 focused checks, including the compile-fail
contract and actual Fjall reopen. The all-feature domain suite passed 1,200
tests, and the default workspace passed 4,020 with ten opt-in SDK gates ignored.
Formatting, strict Clippy and builds in both feature configurations, protocol
descriptor validation, and whitespace checks passed. Two initial test-draft
formatting failures, a delimiter and a helper module path, were corrected before
the focused and broader gates ran. The full all-feature workspace and live SDK
gates were not rerun for this domain-only increment.

The separate [pristine-store bootstrap](committed-image-bootstrap.md) requires
explicit trusted selection and a consumed unique target writer. Export alone
does not supply that authority or permit overwriting an initialized store.

The separate [exclusive retention token](committed-image-retention.md) holds the
machine borrow from this capture through one atomic opaque catalog publication.
It preserves the original image allocation and adds no live-progress or
postcommit validation read.
