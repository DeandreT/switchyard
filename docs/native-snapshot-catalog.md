# Owned Native Snapshot Catalogs

`ExperimentalStateMachine::create_with_snapshot_catalog` and
`open_with_snapshot_catalog` explicitly enable native catalog build/read work on
the existing state owner. They require `CatalogCommittedStore` and a bounded
business reader. Ordinary constructors and both existing pristine bootstrap
variants leave catalog work disabled. Catalog constructors do not independently
enable the separate image-export operation.

These APIs combine the [exclusive domain retention token](committed-image-retention.md)
with the [pure native metadata codec](native-snapshot-metadata.md). They do not
implement the replication library's snapshot builder, current-snapshot,
receiving, or installation traits. No runtime uses these constructors; startup,
formats, profiles, dependencies, production refusal, and no-purge behavior are
unchanged.

## Owned Admission

`build_create_send_catalog(&mut self)` and `read_create_send_catalog(&mut self)`
return owned `Send + 'static` futures. Factory creation performs no admission or
I/O and retains only the private owner handle, not the facade borrow. First poll
requests work through the existing owner; no public cloneable writer, reader,
handle, or mutable image capability is added.

Each accepted catalog operation reserves the complete existing 64 MiB owner
budget. The budget is not raised, the artifact limit is not reduced, and no
charge is clamped to a smaller size. An accepted catalog job excludes all other
accepted packets until publication/refund; competing work can return `Busy`.
There is no waiting, automatic retry, or unbounded source fallback.

`MAX_NATIVE_CATALOG_METADATA_OVERHEAD_BYTES` explicitly permits up to 8 KiB of
encoded metadata outside that artifact charge. Small captured checkpoints and
native membership projections, source captures, spare collection capacity,
backend materialization/staging, and RSS are also outside admission accounting.
Source records and encoded image bytes can coexist, each with its own 64 MiB
logical bound. This is not a combined heap limit or universal OOM guarantee.

Accepted caller loss does not cancel capture, retention, read, or result cleanup.
The original job and full charge remain owned until actual completion. Existing
publication/refund ordering ensures a returning caller observes refunded
capacity. Returned immutable bytes become caller-owned and carry no reservation
or backend handle.

Shutdown closes admission, drains accepted work, and joins the native owner even
while an unpolled factory remains alive. Such a factory cannot keep the database
open and refuses admission after shutdown. This is a state-owner guarantee,
not a new combined log/state/connection shutdown guarantee.

## Build And Read

Build prepares one fully validated domain image from one complete bounded
capture. While the exclusive token still prevents intervening apply, the native
codec derives owned encoded metadata, validates the exact image/metadata pair,
and derives an owned `SnapshotMeta`. A small boxed carrier is allocated before
the token's sole empty-business catalog commit.

After successful retention, build only moves the original image and prepared
carrier into `BuiltNativeSnapshotCatalog`. There is no second image allocation,
postcommit source query, validation, constructor open, or retry. This ordering
does not guarantee that owner reply channels, library internals, or the allocator
perform no further allocation. The result exposes immutable image/metadata bytes
and a borrowed native projection; explicit projection access intentionally
reveals IDs and membership, while Debug reports only byte counts.

Read obtains one originating catalog reader and one complete owned domain
catalog result. The domain validates the exact stored image; native code checks
every metadata field against that same image and derives an owned projection in
a scoped borrowed decode. `RetainedNativeSnapshotCatalog` then moves the original
domain result and projection, without copying either component or storing a
self-referential decoded view. Getters expose immutable bytes, captured
checkpoint, and native projection.

Neither operation adds a separate live checkpoint/init query or ordinary
snapshot. The catalog backend still validates its own profile/init invariants
inside its complete view. Constructor and separately armed retirement reads are
outside these operations.

A retained pair older than applied state is legitimate. Even a valid pair ahead
of current state can be returned as a DTO: read does not compare live progress or
prove retained-log ancestry. Pair consistency is not engine-adoption,
installation, rollback, source-health, source-authentication, quorum, or purge
authority. Any future engine consumer needs a separate explicit policy.

## Failure Policy

`StateMachineCatalogError` has only static `Owner`, `Domain`, `Metadata`, and
`Disabled` variants, with no backend cause chain. A healthy disabled owner
performs no catalog source I/O. Adapter poison is checked before capability
selection.

Domain `Poisoned`, `ReadFailed`, and `CommitUnknown` also poison the native
adapter. Later apply, export, catalog, and trait progress work refuses before
source I/O. Quota/allocation, arbitrary semantic/profile/stream, and pure native
pair refusals remain nonfatal; these nested replies bypass the ordinary blanket
trait-error poisoning path. Validating arbitrary bytes cannot certify corruption
or source provenance.

Every retention commit error is unknown, even when the complete catalog is
already durable. No successful image, automatic retry, or rollback assumption is
published. Release every writer/control/business-reader/catalog-reader handle,
join the owner, and inspect a real reopen before selecting recovery work.

Actual backend capture/read panics use existing owner unwind handling: static
panic replies, reservation refund, closed admission, and an actual failed-thread
join. Shutdown can race reply publication, so an old factory can observe
`Panicked` or `Closed`. Static diagnostic privacy does not cover panic hooks or
backend logs.

## Verification Scope

Paired Memory/Fjall cases pin disabled defaults, independent export capability,
exact one-capture/one-publication and one-reader/one-read counts, no fallback or
postcommit reads, maximum message/ID/session sizes, TTL, duplicate sequence holes,
membership, refusal watermark gaps, absent catalogs, and older preserved pairs.
Pointer checks prove original image/metadata custody through build and original
storage-result custody through read.

Tests cover malformed, mismatched, unsupported and ahead-of-live pairs, nonfatal
injected quota/allocation refusals, physical-read poison, before/after actual
commit errors, and injected panics after real backend captures/reads, with refund
and actual failed-owner joins. Controlled
recovery fixtures are not substituted for physical reopen evidence.

Real gates pause after complete captures/catalog reads exist and outside backend
or observation locks. Lost waiters retain one job and the full charge; sibling
work is refused without source I/O, and shutdown remains pending until release.
All paths release gates before awaiting actual joins, without repolling a
completed shutdown future. A durable case releases every physical handle and
performs two actual same-directory reopens while original results and an old
unpolled factory remain alive.

Compile-time checks enforce owned Send futures, non-Clone results, immutable
bytes, catalog writer capability, and bounded capture capability. This slice
adds no forced real allocator failure, exact-full-size success fixture,
child-process crash or mid-sync/power-loss evidence, engine snapshot activation,
history policy, installation, or RSS bound.

The final source passed 31 focused checks, including four compile-fail examples,
and ten further regular-suite runs totaling 270 passes. All 513 cluster tests
and 4,239 workspace tests passed in both default and all-feature configurations,
with ten opt-in SDK tests ignored in each workspace run. Formatting, strict
workspace Clippy and builds in both configurations, protobuf descriptor
generation, and diff checks passed. The descriptor check's initial command used
a nonexistent abbreviated path; the corrected command used the unchanged CI
protocol path. Live SDK gates were not rerun for this isolated owner capability.
