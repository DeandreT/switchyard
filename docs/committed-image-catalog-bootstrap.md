# Combined Image And Catalog Bootstrap

`CommittedStateMachine::bootstrap_create_send_image_with_catalog(writer,
selection, metadata)` is an opt-in pristine-target constructor requiring
`CatalogCommittedStore`. It needs no bounded business reader and does not broaden
the existing [ordinary bootstrap](committed-image-bootstrap.md), constructors,
formats, profiles, dependencies, or runtime behavior.

The trusted selection still pins stream, complete checkpoint, and SHA-256 of the
entire selected artifact, including its checksum. Expectations come from an
explicit caller policy, not authority inferred from untrusted bytes. Metadata is
opaque and bounded; empty metadata is legal. This domain operation does not
interpret [native metadata](native-snapshot-metadata.md).

## Fixed Ordering

Catalog component limits are checked first: 8 KiB metadata and 64 MiB artifact.
An overlong component takes precedence over invalid selection or source bytes.
The unchanged pure selection validator then checks the exact stream, all
checkpoint fields, complete-artifact digest, container, and full CreateSendV1
business semantics. Every source/metadata refusal precedes this function's first
target operation. Opening or stamping the writer earlier is outside that claim.

The originating reader is obtained once. Initialization is checked once; an
initialized target refuses without scanning. Otherwise one ordinary empty-prefix
scan with limit one refuses any existing business record. This probe bounds row
count, not value bytes or backend materialization, as in ordinary bootstrap.

Catalog writers have a specific trusted profile obligation: initialization reads
validate profile/init invariants, and uninitialized state must contain neither
business rows nor a catalog. Malformed state must return an error, not false.
Both built-in backends already enforce this. The narrow trait documentation makes
it explicit without broadening `CommittedStore`. A conforming unique-writer
profile therefore needs no extra allocating catalog read for pristine checking.
Generic bounds are not proof against a lying implementation or trusted
out-of-band mutation, and the probe/commit sequence is not CAS.

The existing fallible row copier assembles exactly the selected ascending Put
rows, including original checkpoint bytes. One `commit_with_catalog` atomically
publishes these business records, initialization, and both supplied slot
components. The catalog borrows the exact selected artifact slice; it is not
re-encoded or copied into another domain-owned artifact.

Ordinary bootstrap followed by separate retention is not equivalent: a crash
between two commits could leave initialized business state without its catalog.
This constructor uses neither an ordinary commit nor a second retention write.

Success directly constructs the machine using the originating reader and consumed
private writer. There is no target get, ordinary/bounded snapshot, reader apply,
catalog read, postcommit query, constructor revalidation, normalization, or retry.
Later ordinary application preserves the older catalog through the
[storage profile](snapshot-catalog-storage.md).

## Failure And Authority

The existing static `CommittedImageBootstrapError` variants are reused. Source,
target, and assembly refusals consume the supplied writer without returning a
machine or a writer escape. Target read failures discard backend diagnostics.

Every error returned by the sole combined commit becomes `CommitUnknown`, even a
low-level limit/profile error. The complete business/init/catalog may already be
durable. No success result, rollback assumption, or automatic retry is published.
Release every writer/control/business-reader/catalog-reader handle and inspect an
actual reopen before choosing a newly authorized recovery attempt.

Source artifact, copied Put batch, catalog/backend staging, and bounded metadata
can coexist. Logical byte limits do not bound combined heap/RSS or spare capacity.
Explicit row and mutation copies reserve fallibly; semantic collections and
backend allocations do not acquire universal allocator-failure recovery.

This API grants no populated-state replacement, native pair compatibility,
source authenticity, ancestry, anti-rollback, quorum, runtime adoption,
replication-library installation, or history-purge authority. The catalog remains
opaque until separately checked. Existing native owner and engine snapshot
operations are unchanged by this domain constructor.

## Verification Scope

Paired Memory/Fjall cases pin exact maximum message/ID/session/TTL rows, duplicate
sequence holes, opaque membership, all selected Put bytes, original artifact
pointer, exact-limit and empty metadata, one reader/init/limit-one scan/combined
commit, and zero ordinary commits or other target operations. Replay and continued
allocation preserve source progress; later application leaves the original pair
older rather than replacing it.

Bounds precedence, exact trusted stream/full checkpoint/full-artifact digest,
different valid bodies with the same checkpoint, legacy/unsupported/malformed
records, missing indexes, checksum/truncation, initialized empty/catalog-only
targets, and an intentionally lying orphan-record adapter are covered. That last
adapter tests the empty-record probe, not certified catalog absence. Physical
and read-limit target refusals never commit; all before/after/limit/profile commit
errors remain unknown. Initial/refusal-only images preserve their exact watermark
without inventing a business clock.

A positive custom reader without `BoundedStateStore` proves this constructor adds
no capture requirement. Private units pin every checkpoint component and static
diagnostic privacy; compile-fail examples refuse ordinary writers/readers lacking
catalog target authority.

Dedicated Fjall cases release every actual machine, control, business reader,
catalog reader, and clone before independent same-directory opens. They recover
successful publication and errors before/after actual `SyncAll` as exactly
pristine or completely installed records/init/pair. Original owned source and
catalog outputs survive additional real reopens and continued application.

The existing bounded child runner is reused without changing its original test
identity/environment. It bounds time/output, reaps the child, and joins both pipe
readers before recovery opens. Abrupt exits before commit and after completed
`SyncAll` recover the exact old pristine or new complete state. Completed targets
refuse another bootstrap; only an inspected pristine result permits a separately
selected new attempt. This is not mid-sync, power-loss, or populated-replacement
evidence.

No exact 64 MiB successful artifact fixture, forced allocator exhaustion, native
owner integration, engine activation, or RSS instrumentation is added here.

The final source passed 33 focused checks, including two compile-fail examples.
Ten further integration-suite runs passed 270 regular cases and included 20
actual abrupt child exits followed by recovery. All 1,299 domain tests, 138
storage tests, and 4,272 default workspace tests passed; ten opt-in SDK tests
were ignored in the workspace run. Formatting, strict workspace Clippy and
builds in both configurations, protobuf descriptor generation, and diff checks
passed. Full all-feature workspace tests and live SDK gates were not rerun for
this isolated domain constructor and storage-contract clarification.
