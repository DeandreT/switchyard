# Aligned Seed Inspection

`inspect_aligned_seed` is a synchronous read-only check of offered canonical
snapshot/catalog bytes and an exactly aligned empty local-compaction log. It
extends [pure native metadata agreement](native-snapshot-metadata.md), not
[sealed compaction](sealed-local-compaction.md) or the private
[physical prototype](paired-storage-prototype.md) into remote adoption.

## Independent Expectation

`BorrowedAlignedSeed` supplies metadata, artifact and three borrowed log rows.
`AlignedSeedExpectation` supplies a separate exact log profile, full checkpoint,
artifact length and complete SHA-256, native `SnapshotMeta`, committed vote and
positive baseline ordinal. The checker establishes equality to these inputs,
not their trustworthiness. Deriving expectations from the same offered bytes
does not create independent trusted selection, authentication or provenance.

`InspectedAlignedSeed` has private fields and is not `Clone`. It exposes only
the checked borrowed native image pair, baseline ordinal and vote. It has no
writer, backend, source-health certificate, seal, install permit, mutation or
authority conversion. Its lifetime cannot outlive the borrowed inputs.

The accepted policy requires a present catalog with a last mark and membership,
an exact captured baseline, and no retained entry or unknown log row. Initial,
membership-free, absent, stale, ahead or continuation-tail selection is not
implemented. Inspection does not choose between old and selected state or infer
destructive reset from omitted rows.

## Validate Observations First

All top-level sizes and exact sorted keys are checked before new owned copies or
existing allocating semantic recovery. The caller's checkpoint membership cap,
profile/checkpoint stream consistency and native membership shape are also
checked first. Those checks concern expectation shape, not observed identity.

The existing `DecodedNativeSnapshotPair` validates SWYM framing and canonical
metadata, the full SWYI artifact, business consistency and native recovery.
The offered log dictionary must contain exactly keys `[1]`, `[2]` and `[3]`,
in that order, for canonical SWLQ profile, SWLS progress and SWLF baseline.
No private SWAI model record or paired SWAP physical record is interpreted.

A private observed-profile decoder validates the bounded SWLQ outer frame,
borrows its bounded inner SWLP bytes and reuses existing profile semantics and
canonical reencoding. It does not compare offered identity to the caller's
expected profile. Progress is decoded independently; the baseline is decoded
using that observed profile before independent expectation or policy shortcuts.
Existing legacy profile checks, baseline methods, callers and encoded bytes
remain unchanged.

Thus malformed progress or nested baseline metadata is refused even when the
caller expects a foreign identity. A valid but different external node or stream
is an identity mismatch, not an unsupported encoding. Internally inconsistent
observations remain invalid log data.

## Exact Aligned Agreement

The full checkpoint comparison includes stream, last/previous identities and
fingerprints, refusal timestamp watermark and exact membership. Artifact length
and SHA-256 cover the entire immutable image, including its checksum footer.
The native projection comparison includes last ID, membership source, complete
configurations, node addresses and the deterministic 79-byte snapshot ID.

Baseline ordinal must equal the positive expected ordinal. Its summary must
match the complete captured checkpoint and independent artifact identity.
Different valid bodies at one checkpoint are not interchangeable. Baseline
boundary and progress `last_purged` must equal the image's last native ID;
`last_present` is absent and retained entry/byte counts are zero.

The observed vote must be exactly the expected committed vote and cover the
required last leader according to the native leader order, including node ID.
Term-only comparison is insufficient. Even a sufficient changed vote is refused.
Pinned OpenRaft uses the advanced total-order leader mode; the defensive
incomparable-order refusal is not evidence for another feature configuration.

## Bounds And Diagnostics

| Supplied Component | Limit |
| --- | --- |
| Complete artifact | 64 MiB |
| Native metadata | 8 KiB |
| SWLQ profile | 8 KiB |
| SWLS progress | 8 KiB |
| SWLF baseline | 16 KiB |
| Logical keys | Exactly three one-byte keys |

The resulting supplied key/blob ceiling is 67,149,827 bytes. The nested
checkpoint/native membership limit remains 4 KiB; it is not the whole 8-KiB
checkpoint limit. Expected native membership is guarded without allocation
before semantic processing: at most two configurations, 32 members per
configuration, 32 nodes, 512-byte addresses and a 4-KiB encoded membership.

The new explicit baseline copy reserves fallibly. Existing semantic metadata
trees, native membership clones and small canonical buffers use their established
bounded allocation paths. Large artifact bodies remain borrowed. No aggregate
RSS, spare-capacity, universal fallible-allocation, OOM, admission-charge or
timing guarantee follows from these logical caps.

Errors are static, distinguish unsupported profile/policy, malformed image,
native pair or log, external identity mismatch, unsupported tail, finite limits
and explicit allocation refusal. Redacted Debug reports lengths/counts, never
checkpoints, identities, digests, votes or addresses. Deliberate data getters are
separate from diagnostic formatting.

## No Physical Authority

No storage opener, filesystem operation, source mutation, cache, clock, task,
thread or commit is involved. A pure refusal cannot poison a source or diagnose
an unknown write outcome. Repeating inspection grants no stronger authority.
The dictionary is complete only as supplied; this cannot prove that the caller
included every on-disk row or captured one atomic physical view.

Canonical checked data supplies neither both-owner joins nor physical role,
selection-fence, durable-intent, history, ancestry, quorum, safe-reopen or engine
adoption authority. A later paired writer needs separately reviewed protected
atomic fence publication, domain plan handoff, bounded physical inventories,
unique source custody, actual joins and stable fallback policy. Neither the
private physical prototype nor this result grants those capabilities.

The separate [candidate preparer](aligned-seed-candidates.md) generates canonical
image/catalog and empty-log controls from an artifact and requested expectations.
Generated profile, vote and ordinal are desired data, not prior durable history.

## Verification

The focused suite passed all 27 tests. Ten unchanged serial repetitions passed
270 tests in total. Four compile-fail examples and one positive `no_run` example
also passed in the normal-library documentation configuration; the positive
example was compiled, not executed.

The final serial verification group completed all 20 checks: formatting, strict
workspace lint in default and all-feature configurations, the ten focused
repetitions, all-feature cluster tests (765 passed), both full workspace suites
(4,702 passed across 132 result groups, with 10 existing ignored tests each),
both workspace builds, protobuf validation and whitespace checks. All nine
source digests remained unchanged after verification.

Compilation used two cores at low priority, tests ran serially, and disk and
memory headroom were checked before and after the substantial builds. The
existing shared build cache was reused; no additional build cache was created.

No live SDK gate or physical publication ran for this increment. Earlier SDK
and cluster failures remain unexplained; these green checks neither explain
nor repair them. The evidence establishes only the pure inspection contracts
above, not installation, runtime custody or safe-reopen authority.
