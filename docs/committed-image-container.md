# Committed Image Container

## Current Boundary (2026-10-06)

The schema-1 container recognizes historical role 1, `CreateSendV1`, and
current role 2, `CreateSendLayout17V1`. Current export and restore paths use
role 2; recognizing role 1 structurally does not make it a current restore
input. The role-specific pure validators remain separate. The verification
receipts below describe the original increment, not new layout-17 execution.

`domain::EncodedCommittedImage` packages an already-owned `StoreSnapshot` with
an explicit `CommittedImageRole` declaration and committed stream.
`DecodedCommittedImage` checks the container and borrows its encoded bytes.
Neither API reads a store, obtains a writer, exports from a healthy owner,
installs records, or purges history.

The role is a **declaration, not business-record certification**. Unknown
business tags and malformed business values can be structurally packaged.
Queue topology, counters, message state, indexes, lifetime, duplicate history,
and active value schemas require the separate
[whole-image validator](committed-image-validation.md). Neither check enables an
install path. A valid checksum proves neither commitment
nor authentication, source health, ancestry, anti-rollback safety, or durability.

## Canonical Framing

All integer fields are fixed-width, unsigned, and big-endian:

| Field | Encoding |
| --- | --- |
| Magic | Four bytes: `SWYI` |
| Container schema | `u16`, currently `1` |
| Declared role | `u16`: `1` for CreateSendV1 or `2` for CreateSendLayout17V1 |
| Committed stream | Sixteen bytes; the all-zero identity is refused |
| Row count | `u32` |
| Each row | `u32` key length, `u32` value length, exact key and value bytes |
| Checksum | SHA-256 of the complete preceding header and rows |

Keys must be nonempty, strictly ascending, and unique. Exactly one row must
have the checkpoint key `[0x12]`. Its value must pass the existing bounded,
canonical domain checkpoint decoder, and its stream must match the container.
Membership remains bounded opaque domain metadata; these checks do not validate
the replication adapter's membership schema or log correspondence.

Unknown magic, schema, or role is refused. Declared lengths and counts must fit
the actual bytes exactly; trailing rows, extra bytes, missing bytes, duplicate
keys, or a missing checkpoint are errors. Encoding does not sort, deduplicate,
normalize, or omit source records.

## Bounds and Ownership

The complete serialized artifact is at most 64 MiB, including the 28-byte
header, eight bytes of framing per row, and 32-byte checksum. This version
permits at most 65,536 rows, 1,024 bytes per key, and 260 KiB per value. The
checkpoint retains its separate 8 KiB bound and 4 KiB membership-payload bound.
These limits do not promise that every legitimate domain state will fit.

Encoding checks the entire source, checked length arithmetic, ordering, and
checkpoint before reserving the large output. Output reservation failure is a
static error. The owned carrier has immutable byte access and no `Clone` or
mutable-buffer escape. Obtaining the source snapshot is outside this pure API;
callers can use [bounded storage reads](bounded-storage-reads.md).

Decoding rejects oversized input before parsing. It uses checked slices without
allocating a row list or copying large values. The checksum is verified before
the existing checkpoint codec copies its small metadata. The decoded view and
row iterator borrow immutable artifact bytes; image, row, and iterator diagnostics
report lengths rather than keys, values, streams, checksums, or membership data.
Errors are static and carry no source payload.

The cap bounds logical artifact length, not allocator capacity, process memory,
the source snapshot, caller-created copies, backend memory, or concurrent images.
Hashing provides integrity only; an untrusted sender can recompute the checksum.

## Verification Scope

Focused tests exercise exact framing, borrowing, ordering, inclusive limits,
overflow, hostile lengths and counts, every truncated prefix, exact exhaustion,
unsupported formats, stream agreement, checksum coverage, canonical checkpoint
bytes, opaque membership, and content-private diagnostics. The non-Clone API
has a compile-fail contract.

Real Memory and Fjall cases start from committed domain work and compare every
source row and the full checkpoint. Inputs include the maximum committed body,
message ID and session, lifetime and duplicate-history indexes, a genuine
duplicate sequence hole, and a known refusal that advances the checkpoint
watermark without advancing the business clock. Both readers still refuse empty
and nonempty ordinary writes. A Fjall case drops all old handles and reopens the
actual directory before comparing recovered records and encoded bytes. A
separate no-allocation check ties the artifact cap to the bounded transport buffer.

These inputs establish the test fixtures, not semantic certification by the
container. Snapshot installation, durable snapshot metadata, log baselines,
authenticated transport, runtime recovery, and automatic purging remain disabled.

The container checkpoint passed all 26 focused checks, including its compile-fail
contract. The all-feature domain suite passed 1,148 tests, the all-feature cluster
suite passed 409, and the default workspace passed 3,962 with ten opt-in SDK gates
ignored. Both strict workspace lint configurations, both builds, formatting,
protocol descriptor validation, and whitespace checks passed. The full
all-feature workspace and live SDK gates were not rerun for this increment.
Boundary tests use the same implementation with small private limits; a complete
64 MiB allocating roundtrip and an exact 65,536-row fixture were not exercised.
