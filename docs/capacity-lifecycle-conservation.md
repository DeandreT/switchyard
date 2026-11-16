# Ordinary queue lifecycle conservation test support

The nondefault `capacity-conservation-test-support` feature exposes explicit,
test-named domain, indexed, replay and Broker facades. Normal `StateMachine`,
`LocalProposer`, `Broker::spawn`, indexed/replay open, snapshots, anchors,
compaction and offline upgrade retain their capacity-owned refusal policies,
including when the feature is compiled into the shipped binary. No runtime flag
selects the test path. Finite creation, adoption/migration, resize and production
admission remain a separate activation contract.

Historical finite queue profile V3 and application proposal V7 remain
metadata-only. They never acquire a ledger by detection, open or replay.

## Owner image and quantities

A live ordinary parent and its DLQ share one generation-bound aggregate. Both
profiles use queue profile V4; the shadow remains publicly non-finite. Data and
counters use `13 || original canonical key`. Original tags are counter `02`,
message `03`, ready `04`, lock `05`, expiry `06`, deferred `07` and scheduled `0B`.
These domain profiles/tags are distinct from consensus outer store format 4/F2;
ordinary Fjall outer format 3 is unchanged.

Usage is one tag12, 72-byte V1/model1 owner-bound row. Actual `A` sums physical
message/index key and value lengths. Every primary and index contributes its
extra tag13 byte. Counter, configuration, catalog, usage, Clock, F0 and F1 bytes
are excluded. This is not disk allocation, engine overhead, RSS or process memory.

For each message, model1 bounds the maximum encoded automatic future footprint
`C`. Its prepaid growth is `R = C - A`; it is not already stored content. The
parent limit bounds the aggregate actual plus reserved total, with equality
allowed and no overdraft. Each automatic transition must independently keep that
message's commitment nonincreasing; unrelated free capacity cannot fund an
automatic-growth proof failure. Deletion refunds actual plus remaining reserve
once. A DLQ move remains inside the same aggregate, not a whole-message refund.

Before each owned preparation, owner-local paged scans validate both profiles,
live metadata/generation, usage and its recomputed totals, canonical messages,
exact forward/reverse companion sets and allocation frontiers. Parent/DLQ
sequence claims are exclusive under the parent frontier; lock tokens are
endpoint-local. Mixed legacy/isolated rows, missing usage, unsupported scopes,
orphan/stale companions and destination collisions refuse rather than repair.
Unrelated ordinary runtime commands do not acquire a global owner-health gate.
Paging bounds individual reads, not total work or hostile-value allocation.

The validated physical image supplies a private logical read view to the same
ordinary handler dispatcher. There is no second lifecycle evaluator and no raw
fallback to legacy message/counter rows. Complete affected before/final physical
images and activation old/new-sequence provenance drive one finalizer. Counter,
usage and Clock publication follows all candidate proofs in the same atomic
StateStore apply; indexed application appends its F1 checkpoint to that batch.

## External changes and automatic transitions

Abandon, defer and application-DLQ whole replacements and sparse patches first
validate the original held authority, then prove the rewritten **still-locked**
candidate against the owner's existing aggregate. This happens before abandon's
TTL-delete/TTL-DLQ/max-delivery branches; final-state accounting alone would let
those branches hide an inadmissible rewrite. Application DLQ also proves its
arbitrary final reason/description and shadow future reserve. The two hypothetical
proofs replace the same original commitment; they are not billed together.

Refusal preserves original lock/message bytes, delivery count, companions,
counters, usage and domain Clock. Ready and Deferred lock origins remain
distinct. Scheduled activation removes a placeholder, allocates a new sequence
and rebases TTL under the same automatic commitment. Ready TTL and deferred TTL
retrieval follow their existing delete/DLQ policies; locks suspend expiry until
settlement, abandon or lock expiry. Existing removal APIs are unchanged.

## Message V4 remains unsupported

Reservation model1 explicitly accepts numeric message versions 1–3 only. The
general codec's knowledge of V4 removal overlays is not an accounting proof.
Either V4 ID mode, including an empty removal list, refuses owned operations
without deletion, downgrade or refund. Ordinary sparse disposition patches do
not introduce V4; legacy topic rule actions do, and their behavior is unchanged.
Supporting finite-owned V4 requires a separately coordinated model/version
dispatch and real encoded automatic-state proof preserving ID/removal semantics.
No new message version or reservation model is allocated by test support.

## Typed replay and execution custody

`CapacityIntent` remains separate outer V11 with one complete canonical inner
ordinary proposal of versions 1–10. Scope and retained bound authority must
agree with the live parent/target/generation. There is no nesting or conversion
that erases the owner before execution. Indexed identity hashes the complete
canonical outer envelope. Exact latest duplicates are read-free AlreadyApplied
results, not recovered acknowledgements or command outcomes.

The sealed `TestGenesis` builds only an exact empty canonical fixture. It cannot
install into an existing store. The test indexed no-checkpoint opener requires
that exact domain baseline, excluding actual F0 journal rows; populated arbitrary
state is not adopted and no checkpoint is fabricated. Later opens validate the
actual image/checkpoint. The direct unindexed harness refuses F0/F1 ownership.

Typed test replay uses the same total committed-schema preflight and advance
core as ordinary replay. All committed originals, including applied history,
validate before any prefix executes. Only a privately minted healthy capacity
refusal can checkpoint an over-limit decision without domain effects. Unsupported
messages, corrupt images and arithmetic faults do not become healthy quota
refusals. A failed or panicking attempted apply retires its owner; reopen resolves
the actual durable image rather than assuming the failed return meant rollback.

The test Broker constructor accepts only a concrete validated test proposer and
Clock. A private adapter selects the same owner loop as ordinary spawn, preserving
captured request/delivery/session authority, notifications and original joins.
Native staged receive-delete first commits PeekLock and only a successful actual
Complete refunds. Size refusal, detach or registry cleanup retains the durable
message/lock and charge. Deferred response sizing similarly retains pending locks;
per-message Complete success/failure is not whole-response atomic deletion.

Production activation must define a coherent creation/admission and recovery
matrix. Test unanchored lifecycle replay does not authorize V11 anchors, snapshot
capture/install/compaction, generic recovery or offline upgrade. Their existing
refusals remain in force until their validators, retained-original identity and
atomic publication contracts evolve together.
