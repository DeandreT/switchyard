# Pure Native Snapshot Metadata

## Current Boundary (2026-10-06)

Both public entry points require a role-2 `CreateSendLayout17V1` SWYI image,
using `ValidatedCreateSendLayout17Image`; role 1 or a finite capacity profile
is refused as `InvalidImage`. This changes image admission, not SWYM framing:
its schema 1, metadata role 1, payload fields, checksum, and snapshot-ID prefix
are unchanged. Agreement still names the complete original artifact. The
verification receipts below describe the original increment, not new layout-17
execution.

`EncodedNativeSnapshotMetadata::encode(artifact)` derives immutable metadata
from one complete, fully validated CreateSendLayout17V1 image. Both this entrypoint and
`DecodedNativeSnapshotPair::decode(metadata, artifact)` require container
framing, canonical encoding, checksum, business consistency, and compatible
native checkpoint recovery. A declared container role alone is insufficient.

The decoded pair borrows both inputs and retains the validated image. There is
no public metadata-only validated result. The encoder owns only its metadata
bytes, is non-Clone, and exposes no mutable buffer. Neither interface queries a
store, current applied checkpoint, clock, counter, or writer.

This is pure agreement checking, not catalog persistence, source-health
certification, snapshot installation, quorum, authorization, authenticity,
ancestry, anti-rollback, or history-purge authority. Existing owner constructors,
snapshot traits, runtime startup, and no-purge restrictions are unchanged.

The separate [candidate preparer](aligned-seed-candidates.md) generates desired
empty-log controls from checked image data and caller expectations. It does not
persist a catalog or certify historical voting or adoption authority.

## Frozen Format

The complete metadata limit is 8 KiB. Its 12-byte frame contains `SWYM`,
big-endian `u16` schema 1, big-endian `u16` metadata role 1, and big-endian
`u32` payload length. A 32-byte SHA-256 trailer covers the frame and payload.

The payload uses explicit versioned postcard fields in this order:

1. Stream identifier, exactly 16 bytes.
2. Complete artifact length as `u64` and complete artifact SHA-256 as 32 bytes.
3. Optional last mark and optional previous mark. Each includes full term,
   node identifier, and index as `u64`, followed by a 32-byte fingerprint.
4. Highest timestamp as `u64`.
5. Optional membership: full source entry identifier, `u16` membership schema,
   and canonical borrowed payload bytes.

This is not the replication library's serde layout or a serialized domain
command. Accepted pairs use the existing native membership schema and canonical
decoder. The independently frozen membership payload cap is 4 KiB. Changing
these fields, meanings, framing, or bounds requires explicit format review.

The conservative maximum representation is 4,370 bytes, including worst-case
integer encodings even where their values cannot describe a valid image:

```text
12 frame + 16 stream + 10 artifact length + 32 artifact digest
+ 2 * (1 option + 30 full entry ID + 32 fingerprint)
+ 10 timestamp
+ (1 option + 30 source ID + 3 schema + 2 length + 4096 membership)
+ 32 checksum = 4370
```

Metadata framing, declared length, and checksum are checked before borrowed
payload decoding. Trailing input, trailing payload, nonminimal integers, invalid
option tags, and other noncanonical encodings are refused. Canonical comparison
streams into a comparison writer without allocating a second metadata buffer.

## Exact Captured Agreement

Pair decoding derives expected fields from the image's captured checkpoint and
the complete artifact, then compares every field. The artifact digest includes
the image's existing checksum footer, not just its preceding body. No live
checkpoint is queried or compared: an older retained pair can remain valid after
later business progress.

`snapshot_meta()` projects only the recovered captured checkpoint into owned
native IDs and membership. Its deterministic snapshot identifier is
`swyi-v1-sha256:` followed by all 64 lowercase hexadecimal digest characters.
Different valid bodies at the same full checkpoint produce different identifiers
subject to SHA-256 collision resistance, not an absolute uniqueness proof.
Neither this identifier nor pair agreement attests source history.

## Bounds And Diagnostics

The existing complete artifact cap remains 64 MiB. Container and business
validation borrow image rows and bodies; this codec does not make another full
image copy. Metadata output uses checked counting, fallible exact reservation,
and serialization into the reserved buffer. Snapshot identifier output also
reserves fallibly.

Native recovery and projection copy bounded membership metadata; library
collection clones use normal allocations. Semantic validation may allocate
row-bounded metadata collections. Limits do not bound spare capacity, aggregate
concurrent output, allocator staging, heap, or RSS, and do not promise recovery
from every allocation failure.

`NativeSnapshotMetadataError` contains only static refusal variants. Wrapper
Debug output reports byte counts, not identities, digests, membership, source
paths, or payloads. The intentionally requested `SnapshotMeta` projection does
expose native identity and membership. Pure validation refusals do not grant
authority to poison a source owner.

## Verification Scope

Paired Memory/Fjall cases export actual maximum-body, session, TTL, and duplicate
detection records with a duplicate sequence hole and refusal-watermark gap. They
verify deterministic metadata, original borrowed input pointers, captured native
projection, unchanged source records, initial checkpoints, and older pairs after
later applied progress.

Focused cases cover every isolated field mismatch with a fresh canonical
checksum, optional fields in both directions, same-checkpoint/different-body
identifiers, complete-footer hashing, structurally valid but business-invalid
images, and domain-valid but native-incompatible checkpoints. A real canonical
4,096-byte native membership exercises public encoding, borrowed decoding, and
native projection.

The corpus includes explicit golden bytes, every truncated prefix, unsupported
schema/role, framing/checksum damage, malformed lengths, noncanonical payloads,
component bounds, the conservative frozen maximum, an actual 64 MiB-plus-one
artifact refusal, and static diagnostics. Two compile-fail examples enforce
non-Clone and immutable metadata output. No allocator-failure injection, physical
snapshot transaction, crash installation, owner retirement, power-loss coverage,
runtime adoption, or heap/RSS proof is established by this pure codec corpus.

The final source passed 25 focused tests, including two compile-fail cases, and
ten further regular-corpus runs totaling 230 passes. All 482 cluster tests and
4,171 default workspace tests passed, with ten workspace tests ignored.
Formatting, strict default/all-feature workspace Clippy, both workspace builds,
protobuf generation, and diff checks passed. An initial strict lint run identified
duplicate test-fixture registration, an unnecessary explicit drop, and a constant
assertion; those were corrected before rerunning the final gates. The full
all-feature workspace suite and live SDK gates were not rerun for this isolated
pure-codec change.

## Later Retention

The separate [catalog storage profile](snapshot-catalog-storage.md) can retain
opaque metadata and image bytes. This pure codec does not write that slot or
prevent apply between capture and publication; a later unique-owner retention
capability must enforce that ordering. Owner admission for combined results
also needs an explicit metadata-overhead policy rather than silently clamping
the charge or reducing the complete artifact limit.

The separate [aligned seed inspector](aligned-seed-inspection.md) combines this
pair check with an exactly aligned empty log and independent caller expectations.
It supplies checked data, not storage, source-health or installation authority.
