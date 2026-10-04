# Sealed Owned Image Transport

`BoundedSnapshotData::from_image(image)` consumes an immutable
`domain::EncodedCommittedImage` into private read-only transport backing. It
checks actual logical length against the independent 64 MiB
`MAX_SNAPSHOT_BYTES` cap before adoption and starts the cursor at zero.

Success moves the original artifact allocation without copying or allocating
body bytes. It performs no decoding, semantic validation, source query, or
storage operation. Failure consumes the supplied image; it returns neither a
mutable buffer nor source capability. Spare capacity and aggregate memory are
not bounded by logical length.

Existing `new`, `default`, and `from_bytes` still create mutable backing.
`from_bytes` copies its bounded input rather than adopting caller capacity.
Their original writes, sparse gaps, cursor arithmetic, errors, and fallible
capacity reservation are unchanged. There is no public mutable byte reference,
clone, extraction, or unseal operation for either backing.

## I/O Contract

Both backings share bounded asynchronous reads and seeks. Reads copy requested
bytes into caller buffers. Seek changes only position, allows a bounded sparse
position past EOF, and rejects negative/overflow/over-limit positions without
changing the prior cursor or bytes. Reading at a sparse position produces EOF.

For sealed backing, actual `poll_write` rejects every input, including an empty
slice, with static `PermissionDenied`. This guard precedes all cursor, capacity,
or length operations and leaves bytes and position unchanged. The pinned Tokio
default vectored implementation routes mixed, all-empty, and empty-vector inputs
through that same guard; `is_write_vectored()` remains false.

Tokio 1.53.1's `write_all(&[])` instead succeeds without polling its writer.
That is an inert extension-method shortcut, not acceptance of a sealed write.
Flush and shutdown remain nonpersistent no-ops and do not unseal. Reads and seeks
remain available afterward. Unpolled I/O futures are inert; polled operations
finish immediately without a background worker.

Debug output contains only byte length and position, never image contents,
stream, membership, addresses, or snapshot identifiers. Static I/O errors do not
include source bytes or physical diagnostics.

## Custody And Authority

The sealed buffer owns bytes only. It keeps no domain machine, native owner,
physical writer/reader, or database handle alive. Structurally encoded images
with unsupported business rows can be transported: this constructor deliberately
does not substitute for whole-image or native metadata validation.

It grants no source health, commitment, ancestry, authenticity, installation,
machine poisoning, runtime adoption, or history-purge authority. Engine snapshot
traits and startup/configuration remain unchanged. There is no native catalog
DTO extraction/handoff API in this transport increment.

Body reads intentionally copy into callers, tests may allocate receive buffers,
and small I/O error carriers may allocate. Backend staging/cache, semantic
collections, spare capacity, aggregate buffers, allocator overhead, and RSS are
outside this logical cap. Original pointer custody and source inspection do not
establish global allocator instrumentation or universal OOM recovery.

## Verification Scope

The suite preserves all mutable cases and adds sealed original-pointer moves,
maximum-message whole-frame/checksum and chunked reads, an independent tiny-cap
refusal, structural-only acceptance, sparse/empty reads, signed/absolute/overflow
seeks, empty/nonempty actual write refusal, guard precedence, all vectored forms,
empty-write-all behavior, flush/shutdown, inert/polled futures, Send/Unpin/static
traits, and diagnostic privacy.

A real Fjall case releases every machine/writer/business-reader/catalog-reader
and both reader clones before an independent same-directory reopen. Sealed data
and original owned catalog/rows/checkpoint survive that acquisition, continued
application preserving the older catalog, and a second actual reopen after all
new physical handles drop. This is handle-release evidence, not process-crash,
power-loss, mid-sync, or fault-injection proof.

Compile-fail examples reject mutable access, cloning, extraction, mutable byte
access, and reuse of the consumed image. No exact 64 MiB successful artifact
fixture, real allocation-failure injection, engine installation, compaction, or
RSS instrumentation is added.

This checkpoint passed 40 focused checks: 35 regular cases and five compile-fail
examples. Ten repeated regular runs passed 350 executions. The all-feature
cluster suite passed 564 tests; both workspace feature configurations passed
4,323 tests with ten opt-in SDK tests ignored. Formatting, both strict workspace
lint configurations, both workspace builds, protocol descriptor validation, and
whitespace checks passed under the two-core limit. Live official-client gates
were not rerun for this transport-only change.
