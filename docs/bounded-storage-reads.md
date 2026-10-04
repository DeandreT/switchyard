# Bounded Storage Reads

`storage::BoundedStateStore` is an opt-in capability implemented by `MemoryStore`,
`FjallStore`, and their committed-store read-only views. Its
`snapshot_bounded(ReadLimits)` method returns every business record from one
stable view, in ascending key order, or returns an error. It never returns a
truncated view as a successful snapshot.

`ReadLimits` requires four explicit budgets:

| Field | Bound |
| --- | --- |
| `max_rows` | Number of returned records |
| `max_key_bytes` | Length of each returned key |
| `max_value_bytes` | Length of each returned value |
| `max_total_bytes` | Sum of every returned key and value length |

All length and row accounting uses checked arithmetic. A limit violation returns
the static `StorageError::ReadLimitExceeded` error, without exposing a record's
key or contents. Zero budgets accept an empty view. A record with an empty key
and value still consumes one row; empty index values consume no value bytes.
An error discards any earlier copied rows and does not mutate the store.

There is no default implementation that calls an ordinary allocating snapshot.
Third-party stores must explicitly implement this stronger contract. Existing
`StateStore::get`, `snapshot`, and prefix scans retain their existing behavior.
A committed-store reader delegates bounded reads when its underlying backend
supports them, but still refuses every ordinary write. This capability exposes
neither its unique writer nor backend replica metadata.

## Stable Views and Memory Scope

The memory backend holds its read lock for the entire ordered walk. It checks
each record against all budgets before cloning its key and value into the
result. A poisoned lock remains a storage error.

The disk backend pins one Fjall snapshot for iteration, size lookup, and value
lookup. Row and key limits are checked before the value-size lookup; all budgets
are checked before the caller-owned key/value copies. A missing record or a
different value size within that same view is an error, not a partial success.
A concurrent live commit cannot alter the returned old view.

These are **logical caller-owned data limits**, not a process-memory bound. They
exclude collection metadata, allocator spare capacity and overhead, backend
allocations, decompression, caches, and other simultaneous reads. In particular,
Fjall's size lookup can internally materialize a value; this API does not promise
to prevent that allocation. A permitted allocation may still exhaust memory.
Callers must choose suitable finite budgets and separately bound concurrency.

## Scope and Verification

This capability is a prerequisite for bounded state-image work, not a validated
domain image. It establishes no business-record validity, committed-history
provenance, snapshot install authority, anti-rollback rule, or compaction safety.
It changes no store-format version, replica initialization metadata, commit
durability, or unknown-commit-error behavior. Snapshot transport, export,
installation, and automatic history purging remain unsupported.

The focused suite covers exact boundaries, all four refusal paths, arithmetic
overflow, empty and zero-byte records, unchanged records after refusal, read-only
replica authority, and owned results. Disk cases also use actual directory
reopening and a real concurrent batch after capturing the old snapshot, before
its iteration and size/value lookups. A compile-fail contract prevents an
ordinary generic `StateStore` from gaining an allocating fallback.

The bounded-read checkpoint passed all 22 focused cases and all 89 storage
tests, including compile-fail contracts. Both default and all-feature workspace
runs passed 3,936 tests each, with ten opt-in SDK gates ignored in each run.
Both strict workspace lint configurations, both workspace builds, formatting,
protocol descriptor validation, and whitespace checks passed. No live SDK gate
was rerun for this storage-only increment; the earlier unexplained SDK failure
is not classified or fixed by these results.
