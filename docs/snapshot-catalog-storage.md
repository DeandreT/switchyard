# Opt-In Snapshot Catalog Storage

## Current Boundary (2026-10-06)

The global business layout is 18; profile tags and opaque catalog framing are
unchanged. Current domain/native catalog consumers require role-2
`CreateSendLayout17V1` images, while this storage layer still neither validates
nor rewrites artifact roles. A legacy catalog does not gain current restore
authority merely because its bytes can be stored. The verification receipts
below describe the original increment, not new layout-18 execution.

`MemoryCatalogReplicaStore` and `FjallCatalogReplicaStore` add one opaque catalog
slot beside isolated committed business records. Both remain unique writers;
their ordinary business readers refuse every mutation, including an empty batch.
The separate `SnapshotCatalogReader` capability returns an immutable owned copy
of both catalog components. No writer or mutable buffer can be recovered from
either reader or the returned value.

This is storage only, not a validated state image, native snapshot implementation,
populated-state replacement, transport, runtime adoption, or history-purge
authority. No default constructor or server startup path changes. Existing
snapshot trait and no-purge runtime restrictions remain unchanged.

## Explicit Profile

The durable layout uses
`ACTIVE_CATALOG_REPLICA_STORE_FORMAT = 0xc000_0000 | ACTIVE_STORE_FORMAT` and the
exact profile `committed-state-catalog-v1`. Its record-layout version advances
with existing formats; a catalog-header change also needs an explicit profile
version change. The current global base is 18, so this profile uses
`0xc0000012`; standalone and ordinary replica layouts are 18 and `0x80000012`.
The catalog profile tag remains version 1. This profile does not bypass the
shared base-format guard or convert an earlier directory.

A fresh directory is stamped with three headers in one synchronous batch:
`format_version`, `replica_profile`, and `replica_initialized`. Only those headers
and the two catalog keys, `snapshot_meta` and `snapshot_image`, are accepted in
the metadata keyspace. Unknown, missing, malformed, or incompatible metadata is
refused rather than rewritten. An uninitialized profile must contain neither
business records nor a catalog.

Existing standalone and ordinary replica directories are not adopted, even if
their records are empty. Their normal open paths also refuse catalog directories,
including pristine and initialized-empty ones. There is no automatic migration
or conversion. All earlier base-format directories, including ordinary layouts
and catalog directories with no action records, are refused by the current
build. Recreating development directories is a consequence of that global
version boundary, not adoption by the catalog API; see
[Durable Format](compatibility.md#durable-format).

All three generic durable openers also [refuse reserved paired metadata
markers](paired-marker-refusal.md) before record-keyspace acquisition or
application stamps, regardless of marker validity. Database recovery and
metadata-keyspace acquisition still precede that guard; this is not a guarantee
of unchanged physical files or an arbitrary existing-only inspection API.

## One Atomic Publication

`CatalogCommittedStore::commit_with_catalog(batch, record)` atomically publishes
every ordered business mutation, the initialized flag, and both supplied catalog
components. Fjall uses one `SyncAll` batch. Memory protects all three kinds of
state with one shared write lock and is not durable across process exit.

Ordinary `CommittedStore::commit` preserves the existing catalog unchanged, even
after later business writes or deletion of every business row. A retained catalog
may legitimately be older than current business progress. Storage neither
decodes its bytes nor compares it with a business checkpoint. These low-level
capabilities do not certify consensus, authorization, image validity, authenticity,
ancestry, anti-rollback, or source history.

Both catalog components live outside the business record keyspace. Ordinary
gets, scans, snapshots, and bounded complete snapshots include only caller
records, even when a caller key has the same spelling as a reserved metadata key.
Separately requested business and catalog reads still have no shared transaction
view. The [complete legacy state capture](complete-catalog-state-capture.md)
returns bounded business/init/catalog data from one retained backend view,
without supplying a fence or publication authority.
The separate [protected Memory publication](protected-memory-state.md) couples
complete replacement with an opaque fence in a fresh object. It does not wrap
or promote either legacy catalog writer.

The separate [controlled paired-storage prototype](paired-storage-prototype.md)
tests private physical publication with an opaque fence. It adds no selection-
fence capability or cross-directory transaction to this public catalog API.

## Complete Bounded Reads

`SnapshotCatalogRecord::new(metadata, artifact)` borrows immutable inputs and
checks their independent fixed limits before copying or writing: 8 KiB for
metadata and 64 MiB for the artifact. Storage treats empty components as legal
opaque values. Two present empty values are a catalog; two absent values mean
`None`; one present value is corrupt. Memory represents the pair with a single
optional tuple, so a half-present slot is unrepresentable there.

Fjall catalog reads pin one snapshot for header validation, initialization,
component presence, both measured lengths, value retrieval, and exact-length
checks. Both limits are checked before caller-owned copies. Memory holds one
read lock across validation and both copies. No partial, truncated, cross-view,
or unbounded fallback result is returned.

Metadata-key validation stops at the sixth entry and does not accumulate
arbitrary metadata. Header values have their exact lengths checked before get.
Backend iteration, size lookup, value materialization/cache, and staging are
outside caller-copy limits. Component limits do not bound spare capacity,
concurrent returned values, aggregate heap use, or RSS.

`StoredSnapshotCatalog` is non-Clone and exposes only borrowed immutable slices.
Its explicit result copies reserve fallibly; an allocation refusal returns no
partial value. Borrowed/owned catalog Debug output shows lengths, not contents.
Returned bytes contain no backend handle. Catalog-reader clones, like business
readers, can keep a physical database open until dropped.

## Errors And Recovery

`CatalogBoundsError` is a static constructor refusal. `CatalogReadError` separates
`LimitExceeded`, explicit output `Allocation`, and `Storage(StorageError)`.
Storage causes preserve existing backend and corrupt-metadata details, except
the existing read-limit cause maps to `LimitExceeded`. This is not a sanitized
external error interface; domain/protocol adapters must map causes before
publishing external diagnostics.

The separate [pure native metadata codec](native-snapshot-metadata.md) validates
complete metadata/image agreement. Storage remains opaque: it does not call that
codec, enforce image semantics, or grant native adoption authority.

Catalog commits retain the existing unknown physical commit-decision contract:
an error can follow the entire successful durable publication. Do not publish
success effects, assume rollback, or automatically retry. Release every physical
writer and reader, reopen, and inspect actual complete state before choosing the
next action. The low-level store adds no domain/native poison policy. Backend
writes may use ordinary allocations; fallible read-result copies are not an
all-allocations-fallible or OOM-safety guarantee.

## Verification Scope

Paired conformance cases exercise empty versus absent slots, ordered mutations,
business-only bounded views, originating-reader isolation, immutable replacement,
older-catalog preservation, static bounds, and redacted value diagnostics.
Private fixtures inspect complete business/init/catalog views under the same
Memory lock or pinned Fjall snapshot, including old views retained across new
commits and live-header changes.

Malformed headers, unknown metadata, either half-slot, uninitialized orphan
state, incompatible formats, and actual overlong stored components are refused
without repair. The oversized artifact fixtures allocate and, for Fjall, write
64 MiB plus one byte to exercise the real fixed limit. They do not establish a
successful exact-64-MiB durable roundtrip or induce allocator exhaustion.

Controlled errors immediately before or after actual complete publication pin
the two possible decisions without retry. Dedicated durable tests drop every
writer, business reader, and catalog-reader clone before reopening the same
directory while returned owned bytes remain alive. Separate child processes
exit before commit or after a real synchronous commit for both fresh and already
initialized targets; recovery checks complete old/new state and another reopen.
The child runner uses null streams, a child-only deadline, and guard kill/reap
cleanup. It does not cancel in-process storage I/O or promise a hard kernel
cleanup bound.

Compile-fail coverage enforces unique writers, nonwritable catalog readers, and
immutable non-Clone owned results. No native metadata/image agreement, trusted
selection, installation, runtime snapshot activation, history compaction,
mid-sync failure, torn-journal recovery, power-loss coverage, or RSS proof is
established by these storage tests.

This increment passed 47 focused tests, including six compile-fail cases, and
ten additional crash-suite runs: 30 regular test executions and 40 actual child
exits followed by recovery. The full storage suite passed 138 tests; the default
workspace suite passed 4,146 with ten ignored. Formatting, strict workspace
Clippy with default and all features, both workspace builds, protobuf generation,
and diff checks passed. The full all-feature workspace suite and live SDK gates
were not rerun for this isolated storage-capability change.
