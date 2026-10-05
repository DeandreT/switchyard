# Canonical Aligned Seed Candidates

The synchronous cluster preparer generates canonical candidate data
from a borrowed immutable artifact and separately supplied expectations.
It uses the existing SWYM/SWYI and SWLQ/SWLS/SWLF codecs. It does not create a
paired physical directory, publish a seed, activate an engine or grant adoption
authority. Successful equality does not establish independent trust.

## API

`prepare_aligned_seed_candidate` borrows the complete artifact for its output
lifetime and borrows `AlignedSeedExpectation` only during the call. The returned
`EncodedAlignedSeedCandidate` retains existing encoded native metadata and three
private control buffers; it does not retain checkpoint/native expectation
references, a backend handle, runtime, callback or owner.

`artifact_bytes` returns the original artifact borrow. `metadata_bytes` and
`log_rows` borrow immutable small buffers from the candidate. Bind the fixed
three-row array locally before calling `inspect_aligned_seed` with those rows.
Keep the candidate and array alive through the resulting inspection.
`reinspect` returns only `Result<()>` so no view borrowing a local array escapes.
No self-reference, unsafe lifetime extension, `Clone`, public constructor,
mutable buffer, owned extraction or writer conversion is exposed.

Readable bytes may be copied by a caller. Changing those copies does not mutate
the candidate and does not produce a capability. Public Debug reports lengths;
the static error type does not traverse body, names, addresses, IDs, digest
or vote. Intentional read-only byte access is not content redaction.

## Identity And Policy

Caller/artifact caps and borrowed native membership shape checks precede
generating output or existing allocating semantic recovery. Expected profile
stream must agree with expected full checkpoint. The complete artifact is
validated by the existing native metadata encoder and pair decoder.

Initial or noninitial membership-free images return no candidate. Independent
full checkpoint, artifact length, complete SHA-256 and every recovered native
field must match. Native equality includes last ID, membership source,
configurations, node IDs and addresses and the deterministic 79-byte snapshot ID.

The requested baseline ordinal must be positive. The requested vote must be
committed and cover the image's last native leader by native partial comparison,
accepting only `Equal` or `Greater`. The requested profile node, positive ordinal
and sufficient vote are desired encoded data; they need not equal controls from
an earlier source fixture. Their generation does not prove a source persisted
them, voted, replayed the image, remained healthy or obtained quorum.

Generated progress has `last_purged` equal to the image's last native ID,
`last_present` absent and retained entries/bytes zero. The exact generated profile,
progress and baseline rows are temporarily reinspected using the original
expectations before returning the candidate. Reinspection grants no stronger
authority or provenance.

The existing inspector's refusal precedence and lazy comparison order stay
unchanged. Observed row caps/shape still precede expected stream/native
consistency; observed image validation and control decoding precede external
identity comparisons. Baseline-summary, progress and vote agreement checks
retain their later positions. The external profile check still precedes full CP,
artifact length, complete digest and native equality.

## Limits

The artifact remains borrowed with the existing 64-MiB cap. Native metadata has
its existing 8-KiB envelope and conservative frozen encoding ceiling of 4,370
bytes. Profile and progress controls each have an 8-KiB envelope; baseline has
16 KiB. There are exactly three existing one-byte keys and no retained-entry rows.

The conservative retained small-output ceiling is 40,960 bytes excluding the
borrowed artifact. The existing supplied logical ceiling is 67,149,827 bytes,
including artifact and keys. These are conservative logical ceilings, not exact
attainable encoder maxima, admission quotas, allocator capacity or RSS bounds.
Whole checkpoint parsing remains 8 KiB; its membership payload cap remains 4 KiB.

Existing explicit SWYM/SWLF reservations keep their fallible behavior. Existing
profile/progress vectors, bounded image/native collections and temporary
reinspection allocations are not universally fallible. Coexisting buffers,
spare capacity and OOM recovery are not bounded or guaranteed. The heavy
64-MiB+1 caller-fixture case requires serial root execution and resource checks.

## Not Publication

The candidate contains offered/requested canonical data only. It is not proof
of complete physical capture, an independently trusted seed manifest, history,
source health, ancestry, quorum, a protected selection fence, a publication
permit, a storage writer or actual native/task retirement.

The private B2 physical prototype is not promoted. Existing catalog commits
lack a protected selection-fence field; domain plan handoff remains separate.
A future paired owner must separately establish stable complete role views,
quiescent unique custody, durable intent before state write, known selected
catalog before destructive log finalization and a distinct phase fence for
no-op content. Every entered physical commit error must become Unknown and
poison BOTH owners without postquery, retry, rollback or journal clearing.

Actual BOTH native-owner joins, complete handle release, live fallback custody
and independent BOTH-role controlled reopens remain separate prerequisites.
No engine/runtime/network, source cache, automatic recovery, SDK cause fix or
historical failure explanation follows from generating canonical bytes.

## Verification

The focused preparer suite passed 26 regular tests; the separate unchanged
inspector suite passed 27. Normal-library documentation checks passed nine
compile-fail examples and two positive `no_run` examples: five negative and one
positive are new, while four negative and one positive belong to the inspector.
The positive examples were compiled, not executed.

Ten unchanged serial preparer repetitions passed 260 tests. All 20 final checks
completed: formatting, strict default/all-feature workspace lint, the ten
repetitions, all-feature cluster tests (797 passed), both full workspace suites
(4,778 passed across 132 result groups, with 10 existing ignored SDK tests each),
both workspace builds, protobuf validation and whitespace checks. All eight
implementation digests remained unchanged throughout final verification.

No compiler, test or lint correction was needed for this increment. Review
narrowed one documentation sentence about the inspector's existing validation
order; that editorial correction changed no source behavior.

Boundary coverage includes the serial 64-MiB+1 caller fixture. Unconstructible
typed whole-checkpoint oversize and the native comparison's defensive `None`
branch were source-audited, not demonstrated as reachable runtime cases. No
exact 64-MiB success artifact, universal allocation-failure injection or process
memory guarantee is claimed.

Compilation used two cores at low priority, tests ran serially, and disk/memory
headroom was checked before and after substantial builds. The existing shared
build cache was reused. No live SDK gate ran. Earlier SDK and cluster failures
remain unexplained; these green checks neither explain nor repair them.
