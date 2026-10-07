# Trusted Image And Catalog Replacement

## Current Boundary (2026-10-06)

Both selected source and captured target must pass the role-2
`CreateSendLayout17V1` proof. Historical role-1 sources and unsupported finite
capacity profiles refuse before target I/O; target profile refusals remain
nonfatal after the one bounded capture. Exact NonFinite Mode rows are retained
or removed by the existing ordered replacement, never synthesized. The
verification receipts below describe the original increment, not new layout-17
execution.

`CommittedStateMachine::replace_create_send_image_with_catalog(request,
metadata)` is a separate trusted mutation capability for an initialized target.
It requires `CatalogCommittedStore` and a bounded business reader. Ordinary
constructors, pristine target rules, dependencies, and runtime startup remain
unchanged; current artifact admission uses the separate layout-17 proof.

The separate [borrowed count planner](committed-image-replacement-plan.md)
validates both offered images and their independent identities without target
access. Its immutable counts grant no writer capability and do not alter the
trusted mutation path described here.

The non-Clone `TrustedCreateSendReplacement` borrows immutable artifact bytes
and two complete checkpoints: the expected old target and selected new source.
It also pins stream identity and SHA-256 of the entire artifact, including the
footer. These expectations come from caller policy, not authorization inferred
from supplied bytes. Its checkpoint getters are immutable and Debug is redacted;
it exposes neither artifact fields nor a writer. Domain metadata remains opaque.

## Fixed Ordering

An already-poisoned machine refuses first. The 8 KiB metadata and 64 MiB artifact
bounds then precede stream, full selected checkpoint, complete digest, container,
and whole CreateSendLayout17V1 semantic checks. Every source or metadata refusal precedes
the operation's first target access.

The existing bounded exporter supplies one complete target capture and validates
its container and business semantics. Raw target rows drop before semantic
validation. Pure decoding of that returned artifact supplies the old full
checkpoint; every component must match the trusted expectation. There is no
separate live checkpoint/init query, catalog read, ordinary snapshot, allocating
fallback, reader factory, or mixed-view comparison. Initialization relies on the
existing constructor and matching-reader/exclusive-writer contract, not a new
attestation or CAS against privileged out-of-band mutation.

Two linear ordered merge walks count mutations and copy stale-only Delete keys.
Every selected row is then Put, including overlapping keys and the original
checkpoint/membership bytes. There are no repeated per-key source scans or extra
large key set. Counts and logical payload are checked against 131,072 mutations
and 128 MiB. Exact mutation/key/value reservations are fallible. The old decoded
view and encoded target drop after Delete copies and before source Put copies.

Exactly one `commit_with_catalog` publishes all Deletes, selected Puts,
initialization, and the original borrowed metadata/artifact pair atomically.
Success leaves the private machine, stream, and already-held live reader
unchanged; that reader observes the new rows. There is no ordinary commit,
separate retention, postcommit query, refresh, reopen, constructor validation,
clock operation, normalization, or automatic retry. Later ordinary application
preserves the older catalog.

## Failure And Authority

Every returned combined-commit error poisons and becomes static `CommitUnknown`,
including low-level limit/profile errors. Complete new state may already be
durable. No success or rollback assumption is published. Physical target capture
failure also poisons through the exporter. Quota/allocation, exact-selection,
target-expectation, and arbitrary source/target semantic/profile refusals are
nonfatal conservative refusals, not certified corruption. Existing diagnostic
checkpoint behavior is unchanged.

An initialized initial or refusal-only image needs no invented business clock.
Explicit trusted policy may select earlier progress. This is not ancestry,
anti-rollback, source authentication, quorum, native pair compatibility, engine
installation, runtime adoption, or history-purge authority. Future native
installation requires independent owner admission and a durable
state/catalog-before-purge barrier across owners.

The separate [owned native replacement API](native-image-replacement.md) supplies
that domain operation through an explicitly enabled state owner. It checks the
whole snapshot carrier and every actual native metadata field before target
access. Its known-commit result still grants no engine or log-purge authority.

Selected source, raw target, and encoded target can coexist during export, each
with a separate 64 MiB logical bound. Source, Delete/Put copies, and backend
staging can coexist during commit. Metadata, collection overhead, spare capacity,
semantic/backend allocation, aggregate heap, and RSS are excluded. An encoded
framing limit can conservatively refuse a raw target within its capture quota.
Fallible explicit row copies are not universal OOM recovery.

## Verification Scope

Paired Memory/Fjall cases cover exact stale-only Deletes and every selected Put,
overlapping changed values, stale queue/shadow/index/history removal, original
catalog pointer, maximum message body, exact-limit metadata, live readers without
refresh, complete source/target checkpoints, and original opaque membership.
They exercise initial/refusal/no-clock and explicitly earlier selection,
source-before-target ordering, legacy/unsupported/invalid/overlong preserved
targets, nonfatal capture limits, physical capture poison, every commit error
mapped to unknown, and static source-private diagnostics.

Private units cover checkpoint components, linear merge shapes, exact budget
guards, and deterministic mutation-capacity overflow. Eight compile-fail examples
enforce capability bounds, request immutability/non-Clone, exclusive borrow,
reader-as-target refusal, and no private artifact/writer escape.

A dedicated Fjall case returns errors before and after a real completed combined
commit. It releases the machine, writer-holding control, BOTH business/catalog
readers and all clones before independent same-directory opens, checks exact old
or selected records/pair, continues application, and opens again while owned
byte-only outputs remain. Same-handle controlled recovery is separate evidence.

Two abrupt-child cases exit before the physical call or after completed
`SyncAll`, then recover exact complete old or selected records/init/catalog.
The reused runner bounds deadline/output, kills and reaps as necessary, and joins
both pipe readers. These are selected process boundaries, not mid-sync,
power-loss, torn-I/O, or actual persistence-failure proof. No exact 64 MiB success
artifact, assembled 128 MiB batch, per-copy heap-allocation fault, global allocator
instrumentation, native owner integration, or runtime installation is claimed.

The final focused suite passed 27 regular tests and eight compile-fail examples.
Ten further integration runs passed 210 tests, including 20 actual abrupt child
exits followed by recovery. All 1,334 domain tests, 138 storage tests, and 4,380
default workspace tests passed; ten opt-in SDK tests were ignored.
Formatting, strict default/all-feature workspace Clippy, both workspace builds,
administrative protobuf generation, and whitespace checks also passed.
The initial formatting pass needed three explicit test-module paths. Four paired
failures used an unsupported clock format instead of a malformed current-format
payload; those fixtures were corrected without changing production behavior,
then the whole focused suite was rerun. Full all-feature workspace tests and
live SDK gates were not rerun for this isolated domain mutation capability.
