# Complete Catalog-Replica State Capture

`CompleteCatalogReplicaStateReader::capture_complete_state()` returns business
records, initialization and the absent-or-present opaque catalog pair together.
The existing `MemoryCatalogReader` and `FjallCatalogReader` implement this
separate read-only capability. It does not compose independently requested
business and catalog reads or change any existing writer or constructor.

## One Retained View

Memory holds one existing read guard through validation, preflight and copying.
Fjall creates one native cross-keyspace snapshot and uses that same snapshot
through every phase. Its original validator checks the catalog-replica format,
exact profile, initialized flag and closed five-key metadata dictionary before
business preflight. Capture opens no database or keyspace and mutates none.

The complete numeric preflight checks all row counts, key/value lengths, strict
key order, business byte totals, catalog presence/component limits and the
checked combined sum before any caller-result reservation or copy. The copy
pass repeats shape/order checks and reconciles row and byte totals. Fjall repeats
`size_of` and `get` inside that same pinned snapshot and verifies actual lengths;
there is no per-row preflight vector, refreshed view or unbounded fallback.

Uninitialized orphan business or catalog data is refused. Both present empty
catalog components are a catalog, not absence; either missing durable component
is corrupt. An initialized store may have no catalog, and an older catalog may
legitimately coexist with newer business records. No semantic comparison occurs.

## Owned Result

`StoredCatalogReplicaState` is non-Clone, privately constructed and immutable.
Its accessors expose `is_initialized()`, `records()`, `catalog()` and
`logical_payload_bytes()`. It owns all returned bytes but no backend handle,
reader, writer, source borrow, expectation or self-reference. Originating handles
may go away while the result remains usable; reader clones can still retain an
already-open database.

Wrapper Debug shows only counts and presence. Explicit record entries remain
readable through their existing API. `CatalogReadError` preserves existing
`LimitExceeded`, explicit output `Allocation` and unsanitized low-level storage
causes; this is not a universal redacted-diagnostics interface.

## Fixed Logical Limits

`COMPLETE_CATALOG_STATE_RECORD_LIMITS` permits 65,536 rows, 1,024-byte keys,
266,240-byte values and 67,108,864 total business key/value bytes. Empty keys and
values are allowed by the numeric capture policy; actual Fjall insertion still
requires nonempty keys. Existing opaque catalog caps remain 8,192 metadata bytes
and 67,108,864 artifact bytes. `MAX_COMPLETE_CATALOG_STATE_BYTES` is the combined
caller-copy ceiling of 134,225,920 bytes.

Explicit output reservations are fallible. These limits do not cover native
iteration/get/header materialization, snapshot-retained history, caches,
allocator bookkeeping, spare capacity, concurrent results, encoded image size
or RSS. Numeric maxima and impossible-capacity refusal do not demonstrate heap
exhaustion or a successful maximum-size allocation.

## Evidence And Authority

Tests cover actual public capture, opaque and empty data, older catalogs, owned
output lifetime, closed malformed controls and actual over-limit refusal. A
locked Memory helper view blocks a finite writer thread; a pinned Fjall helper
reads old complete state after newer business/catalog and header changes. These
are helper-view interleavings, not deterministic interruption of the synchronous
public method. Memory releases the guard and actually joins its writer thread
before assertions or error propagation. Durable fixtures use one worker per
case, not a process-wide thread bound or an original native-worker join receipt.

A result may become stale immediately. It is neither a validated image nor a
checkpoint, digest, provenance, current-target certificate, paired role, fence,
mutation batch, writer, CAS or publication/adoption permission. Legacy fence
absence is not a selection identity. The [borrowed replacement planner](committed-image-replacement-plan.md)
and [private physical prototype](paired-storage-prototype.md) remain separate;
neither accepts this result as authority or is promoted by this API.
The [protected Memory store](protected-memory-state.md) is a separate unique
writer with its own complete read-only capture. This legacy result cannot be
converted into that writer or used as a publication permit.

No format bump, repair, poison/retry policy, durable intent, history purge,
native physical retirement, safe reopen, runtime snapshot activation, SDK
compatibility fix or explanation of earlier failures follows from this capture.

## Verification

The initial focused run passed 27 regular tests: 24 new capture tests and three
existing complete-read tests. The explicit documentation run passed one
compile-only positive example and six compile-fail examples. The first broad
gate then stopped at strict-default Clippy's single `type_complexity` finding,
reported for both library targets; no broad tests ran in that attempt.

A private record-entry type alias resolved that finding. No function or test
body, public API, allocation policy or lint suppression changed. Fresh focused
runs again passed all 27 regular tests and all seven documentation checks.

The complete revised 23-check gate passed on Rust 1.97.1: formatting, both strict
workspace/all-target lint configurations, ten repeated all-feature combined
focuses, explicit documentation checks, storage/domain/cluster suites, both
workspace configurations, both builds, protobuf validation and whitespace
checks. The repeats passed 270 executions: 240 new tests and 30 preserved tests.
The storage suite passed 204 checks across six groups, the domain suite 1,371
across 37, and the cluster suite 797 across eight. Each workspace configuration
passed 4,906 across 134 groups, with ten existing ignored tests and no failures.
Against the preceding checkpoint, both workspace logs contain exactly 31 new
identities: the 24 regular tests and seven documentation checks, with no removed
or changed statuses.

All nine source files remained byte-identical throughout that complete revised
gate. Builds used two low-priority cores and one existing shared build cache;
post-build checks found sufficient disk and memory headroom. Existing catalog
and replacement-plan verification histories are unchanged. This increment ran
no live SDK gate and established no native-worker retirement, safe reopen,
runtime snapshot activation or explanation of the earlier SDK/cluster failures.
