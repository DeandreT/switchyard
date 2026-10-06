# Durable Protected Publication

`storage::FjallProtectedStateStore` is an explicitly selected local durable
profile for complete business, initialization, opaque catalog and fence
publication. It uses the same borrowed input and owned capture types as the
[protected Memory store](protected-memory-state.md), without changing ordinary
stores or enabling server startup, consensus, snapshots or runtime adoption.

## Selection And Ownership

The unique writer is not `Clone` and implements none of `StateStore`,
`CommittedStore` or `CatalogCommittedStore`. It exposes no writable native
handle, legacy conversion, repair, retry or receipt API. `reader()` returns a
`FjallProtectedStateReader`; clones share that exact read-only generation and
can keep its directory locked after the writer is dropped.

`ProtectedStateReader::capture_protected_state` returns one complete owned
`StoredProtectedState`. Its ordinary byte accessors confer no backend handle,
write permission, authenticated provenance or installation authority. The view
can outlive every originating handle and become stale immediately after capture.
Debug for the wrappers and capture exposes only flags, presence and counts,
not paths, keys, values, catalog content or fences.

The disjoint format is `0xd0000000 | ACTIVE_STORE_FORMAT`, with profile
`protected-state-publication-v1`. Ordinary standalone, replica and catalog
openers refuse this profile rather than adopt it. Their native acquisition or
recovery can still have physical effects; refusal is not read-only inspection.
No existing directory is migrated or relabeled into this profile.

## Acquisition

`create_new(path)` reserves only an absent child under an already durably
established trusted parent. It refuses an existing file, empty directory or
store, never recursively creates parents, and never deletes a failure prefix.
The caller must keep the parent live, prevent outside writes or path replacement,
and serialize external preparation. Hostile aliases, symlink protection,
ancestor durability and an `openat` authority model are outside this contract.

The profile supports Unix only. `UnsupportedPlatform` is returned before
filesystem access elsewhere. Successful file/directory synchronization and
atomic filesystem operations must be honored by the filesystem, OS and hardware;
Unix configuration alone does not establish those conditions.

Creation retains and syncs the parent, creates and syncs the fixed empty lock
file, then closes that descriptor before taking the native lock. Native creation
uses one worker and default standard trees, creating `records` before application
`meta`. The pinned native layout has internal metadata tree `0`, records tree
`1` and application metadata tree `2`.

After both application keyspaces exist, retained `tables/` and tree-directory
handles for `0/1/2` are synced child-to-parent, followed by `keyspaces/`, the
selected database and the caller parent. The handles remain live through known
success of one pristine `SyncAll` stamp. These barriers rely on the pinned
backend's file-data-before-reference ordering: directory sync cannot make
unsynced file data durable. They are acquisition barriers, not a second boundary
for subsequent complete publications or a backend-wide correctness proof.

Open failures use static `FjallProtectedStateOpenError` categories:
`UnsupportedPlatform`, `DirectoryUnavailable`, `Backend`, `InvalidLayout`,
`LimitExceeded` and `StampUnknown`. They expose no path or recursive native
cause; discarded cause detail is a diagnostic tradeoff, not original-cause
custody. A pristine stamp error is `StampUnknown`, not absence or rollback.
Failures leave any created prefix in place. Destructor unwinds propagate.

`open_existing(path)` explicitly performs native recovery, requires exactly
the two application keyspaces before acquiring their handles, and validates
the complete closed pinned state before returning a writer. It does not
deliberately recreate missing application keyspaces, stamp, relabel or repair
application dictionaries. Recovery can replay, truncate or sync journals,
create internal data, remove stray native keyspaces and start workers. Arbitrary
failed acquisition prefixes or damaged native files are not supported reopen
inputs; this is not `SafeReopen` or a portable power-loss contract.

## Capture And Publication

Capture holds one shared admission guard and one cross-keyspace snapshot through
closed header/key/presence/order/count/length validation, bounded output copying
and reconciliation. Full logical preflight precedes explicit caller-output/body
copies. A second pass uses the same snapshot, reconciling row count, order,
lengths, key bytes, business bytes, total bytes and fence. Native materialization
can precede that copy boundary and is not bounded by the caller-output cap.

Every native temporary is released while the capture guard remains armed; the
final shared-poison check precedes returning the owned result. There is no
truncated result, unrelated-read composition or unbounded fallback.

`publish(ProtectedStatePublication)` checks offered shape and valid current
closure/fence before owned preparation. It preflights delete/put arithmetic,
prepares every old key and the complete native batch off entry, releases
preparation snapshots, then takes the exclusive admission gate and rechecks
current closure/fence. No snapshot remains at commit entry.

One batch deletes every old business row and puts every new row together with
initialization, both live catalog components and the nonempty fence. Native
commit requires `SyncAll`; only returned native `Ok` is normal success. There
is no separate buffered-commit/persist sequence, postcommit query, reconstructed
success, rollback or automatic retry. Acquisition syncs do not add another
per-publication durability boundary. This is local durability, not a quorum
acknowledgment or proof for arbitrary crash timing, torn files or machine power
loss.

## Refusals And Poison

Invalid offered shape, offered limits, unchanged fence and explicit preparation
allocation refusal are known off-entry refusals. Explicit capture allocation
refusal also leaves a healthy generation usable. Fence equality is checked only
after current closure validation. Inequality permits later reuse of old bytes;
it is not monotonicity, anti-rollback, canonical selection or an expected-old CAS.

Current native/invariant failures and Rust lock poison terminally mark the
shared generation before releasing admission. Depending on the failure, the
first call returns `Poisoned`, `InvalidState` or `LimitExceeded`; a current limit
failure is not the non-poisoning offered-limit refusal. Entered publication
errors return `PublishUnknown` and mark the same shared poison. Entered unwinds
also mark poison and propagate the original unwind, rather than report rollback.
Later publication and capture calls refuse with `Poisoned` before native work.

Poison is process-shared per generation, not a durable poison record. There is
no reset, extra poison batch or retry permit. After every prior native handle
is released, a separate explicitly validated reopen starts a new generation.
It does not resolve the old unknown publication decision, prove the batch absent
or authenticate a chosen state. Native cleanup can block; dropping a wrapper is
not an original native-worker join certificate or whole-process custody proof.

## Logical Limits

| Component | Limit |
| --- | --- |
| Business rows | 65,536 |
| Each nonempty key | 1,024 bytes |
| Each value, including empty values | 266,240 bytes |
| All business key and value bytes | 67,108,864 bytes |
| Opaque catalog metadata | 8,192 bytes |
| Opaque catalog artifact | 67,108,864 bytes |
| Nonempty opaque fence | 256 bytes |
| Business key/value plus catalog components and fence | 134,226,176 bytes |

Keys are strictly ascending, with no duplicates. Present empty catalog
components differ from absent components. A pristine capture has no rows, live
catalog or fence and is uninitialized; an initialized state has both catalog
components and a nonempty fence, even when there are no business rows.

Mutation preflight allows at most 131,076 operations and 201,335,108 logical
mutation bytes, with checked arithmetic. The mutation-byte count includes old
delete keys, new business key/value bytes, catalog/fence bytes, one initialized
flag byte and the four changed metadata keys. Capture payload accounting
excludes profile/header bytes and metadata keys. Neither cap bounds allocation
capacity, collection metadata, native batches/materialization/history/caches,
coexisting inputs/outputs, disk amplification or aggregate process RSS.
Explicit output/key reservations are fallible; native insertion and
materialization allocations remain ordinary, not universal OOM handling.

## Image Agreement

The existing [protected image agreement check](protected-image-agreement.md)
borrows one completed owned capture. It independently checks the full expected
checkpoint, artifact length and whole SHA-256, exact expected opaque fence, and
every business key/value against the validated artifact. It performs no store
read, publication, poisoning, authentication or adoption, and does not interpret
opaque catalog metadata as native metadata.

New business beside an older opaque catalog can be legal storage state.
`BusinessMismatch` is descriptive, not corruption or a poison decision; it does
not prevent a later distinct-fence complete publication. Matching caller-supplied
expectations proves agreement, not authenticated source history or write,
snapshot-installation, source-health or history-purge authority.

## Verification

Verification used Rust 1.97.1 on Unix, the pinned native backend, one reused
build cache, two low-priority CPUs and two Cargo build jobs. The final source
adds 29 regular cases and seven documentation cases: six compile-fail checks
and one compile-only `no_run` example. Ten paired repetitions of the storage
and domain filters executed all 29 regular cases each time, for 290 executions.
The final source stayed byte-identical through those repetitions and broad gates.

Full all-feature crate suites passed: storage 272, domain 1,407, AMQP 857,
protocol-AMQP 521 and cluster 797, with no ignored cases in those suites.
Default and all-feature workspace suites each passed 5,096 cases with the same
10 ignored gates. Each retained all 5,060 prior passing cases, their identities
and ordering, and every old ignored status and reason; only these 36 approved
regular/documentation cases were added. Both runs contained 139 physical harness
groups and 134 logical owners, with no old documentation location changes.

Strict default and all-feature workspace Clippy, both workspace builds,
formatting, protobuf generation and whitespace checks passed. The repeated
documentation filter passed all seven cases. The generated 11,579-byte protobuf
descriptor remained identical to the preceding increment. The 45 protected
storage files, three complete registration inverses and all 21 preceding
pending-Attach refusal source files retained their exact bytes.

The first storage filter retained 24 passes and two failures: new fixtures
tried to insert an empty native key, which the pinned public batch constructor
rejects before writing. Only that unsupported fixture arm and its two new
table entries were removed. The production current-empty-key defenses remain
present but directly unexercised; offered empty-key refusal is still tested.
All 29 regular case identities and seven documentation cases were preserved.
The first strict Clippy run also rejected one private test-table type for
complexity. A private alias corrected it without an allowance or production
change; both strict retries passed. These retained failures are not evidence
of a backend fix or a resolved unknown publication.

Controlled child exits immediately before native commit and after a returned
real `SyncAll` commit recovered the exact old and new generations respectively,
each through two separate reopens. Original children were reaped before checks.
Private test-only error/unwind probes exercised entry and shared-poison paths;
they are not induced native disk faults. No in-call process kill, torn-file,
machine power-loss, arbitrary-corruption, hard kernel-reap deadline or native
worker-health theorem follows from these cases. No official SDK gate was run
for this increment, and no earlier SDK, authentication or cluster failure is
claimed fixed by these results.
