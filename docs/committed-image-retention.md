# Exclusive Domain Image Retention

`CommittedStateMachine::prepare_create_send_catalog()` captures one complete,
validated CreateSendV1 image and returns a non-Clone token that exclusively
borrows the machine. It requires `CatalogCommittedStore` and a bounded business
reader. This is an opt-in domain API; existing constructors, generic bounds,
formats, profiles, dependencies, and runtime startup are unchanged.

The token exposes only immutable image bytes and its small captured checkpoint.
It grants no raw writer, mutable image, public construction, or second machine
handle. The mutable borrow prevents intervening application until the token is
consumed or dropped. Dropping it performs no write.

This is catalog retention, not populated-state replacement, pristine-target
bootstrap, native snapshot construction, installation, source-history
certification, authorization, authenticity, ancestry, or history-purge authority.
Replication-library snapshot traits and the no-purge runtime remain unchanged.

## One Capture, One Publication

Preparation reuses the [bounded domain exporter](committed-image-export.md): one
complete bounded source snapshot supplies both business rows and checkpoint.
The exact encoded bytes pass container and full business-profile validation.
A further pure decode owns the small captured checkpoint; no extra storage
capture, live checkpoint/init query, or allocating fallback is added.

Callers derive independently owned metadata before consuming the token with
`retain(metadata)`. Domain retention deliberately treats metadata as opaque;
the separate [native metadata codec](native-snapshot-metadata.md) can bind it to
the exact image without obtaining mutation authority. Metadata borrowed from
the token cannot accompany its move into `retain`.

Retention first checks poison and the 8 KiB metadata/64 MiB artifact limits, then
performs exactly one empty-business `commit_with_catalog` through the same
private writer. It changes neither business rows, clock, nor applied progress.
Success returns the original owned image with the same allocation/pointer.
There is no second large image copy, ordinary commit, progress/init query,
postcommit validation read, retry, or source fallback.

The [catalog storage profile](snapshot-catalog-storage.md) atomically publishes
both components and initialization. Source records and the encoded artifact can
coexist during preparation, each with their existing 64 MiB logical limits.
Small checkpoint cloning, semantic collections, collection capacity, backend
commit copies/staging, concurrent returned bytes, and RSS are outside those
limits. No universal allocator-failure recovery is promised.

## Reading A Retained Image

`read_create_send_catalog()` requires only `CatalogCommittedStore`, not a
bounded business reader. It makes one originating catalog-reader factory call
and one complete read. An absent slot returns `None`.

The exact stored artifact passes container, stream, and full CreateSendV1
business validation. The returned non-Clone `RetainedCreateSendCatalog` wraps
the original immutable owned storage result and a small captured checkpoint,
without another image copy, self-referential decoded view, or backend handle.
Its getters expose immutable metadata, image bytes, and captured checkpoint.
Metadata is bounded but remains uninterpreted.

The domain adds no separate current-checkpoint/init query, clock query, ordinary
snapshot, bounded business snapshot, or live-progress comparison. The catalog
backend still validates its own profile and initialization within the complete
catalog view. A retained image may legitimately be older than applied business
state, and ordinary commits preserve it. Agreement checking alone cannot make a
retained image an installable frontier or authorize history removal.

## Failure Boundary

`CommittedCatalogError` contains only static variants: `Poisoned`, `ReadFailed`,
`LimitExceeded`, `Allocation`, `UnsupportedProfile`, `InvalidImage`, `WrongStream`,
and `CommitUnknown`. Backend-rich diagnostics are discarded, and wrapper Debug
output reports lengths rather than image contents or checkpoint identities.

Failure to obtain a trusted complete physical capture/catalog view poisons the
machine. Low-level catalog profile/header failures also map to `ReadFailed` and
poison. Quota/allocation refusals, including a defensively nested storage read
limit, remain nonfatal. Arbitrary opaque image semantic/profile/stream refusals
also remain nonfatal: declaring a role or carrying a checksum is not proof of
source provenance or certified corruption.

An overlong metadata input consumes its token without writing or poisoning.
Every error returned by the actual catalog commit instead poisons and becomes
`CommitUnknown`, regardless of its low-level variant. The entire publication
may already be durable. No success image is returned and no retry or rollback
assumption is made. Drop every physical writer, test/control handle, business
reader, and catalog-reader clone before reopening and inspecting actual state.

Poison blocks apply, export, preparation, and catalog read before their storage
I/O. Existing diagnostic checkpoint behavior is deliberately unchanged. A
pristine image-plus-catalog bootstrap requires one combined business/init/catalog
commit; ordinary bootstrap followed by separate retention is not equivalent.

## Verification Scope

Paired Memory/Fjall cases observe exact bounded-capture limits, one empty-business
publication, original image/storage-result pointers, no extra source reads,
maximum message bodies, exact-limit metadata, opaque membership, absent/drop
behavior, consumed overlong metadata, and older catalogs after later apply.
They exercise nonfatal injected quota/allocation refusals, an actual overlong
business value without fallback, physical-read poison, arbitrary legacy/invalid
rows/checksums/streams, and every before/after/limit/profile commit error treated
as unknown. Static private units check exhaustive mappings and poison precedence.

A dedicated durable case injects errors before actual commit and after actual
`SyncAll` publication. It releases the machine, writer-holding control, and every
originating business/catalog reader, observes database-lock refusal while readers
remain, then performs two real same-directory reopens and continued application.
Original image/catalog outputs remain alive across reopen as byte-only values.
Same-handle recovery used in paired controlled fixtures is not substituted for
this physical evidence.

A custom ordinary-only business reader demonstrates that catalog read has no
bounded-business-reader requirement. Compile-fail examples enforce required
capabilities, token exclusivity, immutable image/metadata/checkpoint getters,
non-Clone results, no private writer escape, and the consuming-token self-borrow
boundary.

Allocation refusal is injected, not allocator exhaustion. This slice adds no
exact-64-MiB successful image fixture, child-process crash runner, mid-sync
failure, power-loss proof, native owner integration, installation, or RSS bound.
The storage layer's earlier lifecycle/crash evidence remains separate.

The final source passed 37 focused checks, including twelve compile-fail cases,
and ten further backend-suite runs totaling 200 passes. All 1,266 domain tests
and 4,208 default workspace tests passed, with ten workspace tests ignored.
Formatting, strict default/all-feature workspace Clippy, both workspace builds,
protobuf generation, and diff checks passed. An initial integration-test compile
failure called a crate-private checkpoint-key helper; the fixture was corrected
to locate its captured key/value without exposing a production API. The full
all-feature workspace suite and live SDK gates were not rerun for this isolated
domain-capability change.
