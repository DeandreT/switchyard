# Borrowed Create/Send Replacement Counts

`domain::plan_create_send_replacement` checks two offered CreateSendV1 business
images and returns `PlannedCreateSendReplacement`. Each independent expectation
contains a full `CommittedCheckpoint`, complete artifact length and whole-artifact
SHA-256. These expectations are caller-provided identity data, not trust,
provenance, ancestry, source health, quorum evidence or authority to publish.

The result borrows only the two complete immutable artifacts. Expectation values
and their checkpoint storage need not remain alive. Its private, non-Clone fields
expose original artifact slices, immutable `ValidatedCreateSendImage` views and
`CreateSendReplacementCounts`. Existing row keys and values remain borrowed.
Explicit checkpoint/row access is intentionally readable; result and expectation
Debug output is redacted, and errors are static without underlying raw causes.

## Validation And Policy

Both offered and expected artifact sizes and both typed member payloads are
checked before recovery or hashing. Expected streams are checked next. OLD
container/business validation, then SELECTED container/business validation,
precede external identity and paired policy. Identity comparison is lazy full
checkpoint, complete length and whole digest for OLD, then SELECTED.

Full checkpoint comparison includes the stream, last/previous marks including
leader IDs and fingerprints, highest timestamp and complete opaque membership.
Two different valid bodies with the same full checkpoint and length are not
interchangeable under an independent whole-artifact digest expectation.

The first paired-facing policy requires BOTH images to be noninitial and have
membership. Initial or noninitial/no-member images are validated but refused as
`UnsupportedPairPolicy`, not called corrupt. Domain membership remains opaque;
nonzero domain schema/payload data is not reinterpreted as native membership.

An exact earlier SELECTED checkpoint can produce read-only counts. That is not
permission to roll back, adopt an image, alter a vote, purge logs or discard an
unrelated suffix. Semantically unsupported broader business profiles have a
distinct `UnsupportedProfile` refusal; pure data refusal does not diagnose a
physical store or justify poisoning a source owner.

## Counts And Limits

Every SELECTED row contributes a Put, including exact-body no-ops. Only keys
present in OLD and absent from SELECTED contribute a Delete. Counts report
Delete rows, Put rows, total mutations and logical key/value payload bytes.
They are not public mutation commands, allocated bytes or committed bytes.
Framing, initialization, catalog, selection-fence and log writes are excluded.

Each artifact is at most 64 MiB. Expected member payloads are at most 4 KiB;
the existing parser separately caps whole checkpoints at 8 KiB. Existing row
bounds remain 65,536 rows, 1..1,024-byte keys and 266,240-byte values. The private
count core allows at most 131,072 mutations and 128 MiB logical payload. These
are conservative ceilings, not exact attainable maxima: both images contain
the same checkpoint key. Temporary semantic-validation collections are bounded
by the existing parser, not an RSS/OOM or universal allocation guarantee.

There is no public iterator of mutations, WriteBatch conversion, backend, commit
method or selection-fence capability. No store is read, opened or written. The
result does not prove complete physical capture or that the offered OLD is a
current target. Paired role protection, same-view state/control capture, protected
atomic fence commit, durable intent, canonical seed provenance and native owner
custody/joins/reopens remain separate prerequisites. The private
[physical prototype](paired-storage-prototype.md) is not promoted or copied.

The existing [trusted replacement writer](committed-image-replacement.md) retains
its own target capture, copy order and one catalog commit. This new data API does
not accept its counts as a replacement permit or alter the legacy selection path.
The [aligned seed candidate](aligned-seed-candidates.md) remains a separate
canonical data preparation boundary, not a publication capability.

## Verification

The 30 new regular tests passed, and the combined replacement filter passed all
36 tests, including the six unchanged legacy tests. Ten serial repeats passed
360 tests. A separate documentation run passed 15 checks: six new compile-fail
examples, one new positive `no_run` compile check and eight unchanged legacy
compile-fail examples. The positive example was compiled, not executed.

All 22 final checks passed: formatting, strict default/all-feature workspace
lint, the ten repeats, domain (1,371), storage (173), cluster (797), both workspace
test passes, both workspace builds, protobuf generation and whitespace checks.
Each workspace pass reported 4,849 passed and ten ignored across 133 result
groups. The initially expected 132 groups was corrected during log auditing;
this was an audit expectation correction, not a test failure or source change.

The initial focused compile stopped on one test-fixture `E0596` before any tests
ran. Four mutable-reference setup lines in that fixture were corrected, with no
production change. The ten source digests remained unchanged throughout the
subsequent focused checks and final gate; no later compiler, lint or test fixes
were needed. Original failure and completed verification logs were retained.

Coverage includes full checkpoint identity, independently identified equal-size
bodies, interleaved keys, exact-body no-ops, opaque membership and earlier-image
counts. The 64 MiB-plus-one refusal fixture and private checked-count tests do
not prove maximum-size successful artifacts, a full 128 MiB batch, allocator
failure handling, native custody or physical publication/reopen safety.

Checks ran serially in one shared build cache, limited to two cores at low
priority. Disk space was checked before and after substantial builds. No live
SDK gate ran. Existing SDK and cluster failures remain unexplained; these green
checks neither explain those failures nor establish a fix or source health.
