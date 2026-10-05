# Standalone Catalog Snapshot Builder

`ExperimentalStateMachine::create_send_snapshot_builder(&mut self)` returns an
owning, non-Clone `CreateSendSnapshotBuilder` implementing the pinned
`RaftSnapshotBuilder<LogTypes>` trait. This factory performs no source I/O or
admission and retains no facade borrow or physical storage handle. An unpolled
build is inert. Explicit catalog constructors enable the existing catalog path;
ordinary constructors and the independent export capability still refuse builds
without source I/O.

## Build And Custody

First polling submits exactly the existing native owner's catalog-build
operation. It captures one bounded complete business view, validates the whole
CreateSend image, derives exact native metadata and projection, and retains the
original pair with one combined catalog commit. There is no second capture,
retention write, request charge, source query, or worker.

A private consuming handoff moves the original immutable image allocation into
[sealed transport](sealed-image-transport.md) and moves the owned `SnapshotMeta`
into `Snapshot<LogTypes>`. The original encoded metadata is dropped at this
handoff; it remains in the persisted catalog, not in the returned transport DTO.
No public catalog split, mutable byte reference, extraction, clone, or writer is
added. The data starts at cursor zero and refuses actual writes, including empty
writes, while preserving bounded reads and seeks.

A compile-time assertion checks that the domain image limit fits the independent
64 MiB transport limit; the actual transport length check remains. Retention has
already succeeded if the defensive handoff fails. The trait returns a static
storage error, not rollback evidence or retry authorization. Small Box,
projection, channel, library, and allocator allocations remain outside a global
fallible-allocation or RSS guarantee.

The existing owner reserves its full 64 MiB budget through result publication
and capacity refund. Encoded catalog metadata adds at most 8 KiB of explicitly
separate overhead. Small captured checkpoints and projections, backend staging,
spare capacity, semantic collections, and caller receive buffers are excluded.
Losing the build waiter does not cancel capture, durable retention, or cleanup.
Explicit facade shutdown closes admission, drains accepted work, and actually
joins the native owner. An inert builder or unpolled build retains only a request
handle and cannot retain the database after that join.

Builder/buffer diagnostics and the trait refusal are content-redacted. The
returned library `SnapshotMeta` intentionally exposes IDs and membership;
library-level `Snapshot` diagnostics are not covered by that redaction claim.

## Engine Boundary

This concrete builder is deliberately not the adapter's associated engine
builder. The existing `RaftStateMachine` associated type, `get_snapshot_builder`,
`get_current_snapshot`, receiving, and installation methods are unchanged.
Building through the standalone path does not make those methods succeed or
advertise a current engine snapshot. It does not alter configuration, network
transport, startup, preflight, log storage, runtime policy, or purge.

A successful immutable image/native pair establishes neither source health nor
quorum commitment, ancestry, anti-rollback, engine adoption, or permission to
delete history. Engine integration still requires a durable state/catalog-before-
purge barrier across independently owned stores, prefix reconstruction or a
trusted purged baseline, recovery policy for cross-owner crash windows, trusted
stream and membership adoption, bounded RPC custody, and finite retention policy.
Snapshot policy alone cannot establish these contracts.

## Verification Scope

Paired memory/Fjall scenarios cover inert and disabled factories, a maximum-body
whole-frame checksum and original-pointer handoff, exact native projection and
retained pair, and unchanged unsupported engine snapshot methods. They cover
nonfatal capture quotas and native incompatibility, physical capture poisoning,
before/after real catalog commit errors mapped to unknown, and a caught capture
panic with capacity refund and actual failed native join.

A real capture gate checks lost-waiter custody, the full accepted charge, sibling
Busy refusal without another read, and shutdown remaining pending until release.
Every cleanup path releases the gate before joining and saves an unexpectedly
completed shutdown rather than polling it twice.

Another actual Fjall case keeps an inert builder and unpolled trait build while
joining the original owner and dropping every store-bearing control. It then
opens the same directory independently, checks exact catalog and transport
bytes, continues application, releases all new handles, and opens it a second
time while owned outputs remain. This proves selected handle-release boundaries,
not process-crash, mid-sync, power-loss, or arbitrary persistence-failure safety.

Two compile-fail examples reject cloning and private request-handle extraction.
No exact 64 MiB success artifact, real allocator/OS exhaustion, engine
installation, network snapshot flow, compaction, or runtime adoption is claimed.

The final focused suite passed 20 regular tests and two compile-fail examples;
ten regular-suite repetitions passed another 200 tests. The all-feature cluster
suite passed 586 tests, and the default workspace suite passed 4,345 tests with
10 intentionally ignored tests. Formatting, strict default/all-feature workspace
Clippy, both workspace builds, the administrative protobuf check, and whitespace
checks passed. Two test-only async cleanup blocks needed explicit result types
before these final runs. The all-feature workspace test suite and live SDK gates
were not rerun for this isolated builder increment.
