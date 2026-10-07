# Owned Native Image Replacement

## Current Boundary (2026-10-06)

Actual native pair validation and domain source/target admission now require
the role-2 `CreateSendLayout17V1` proof. The narrow profile includes exact
generation-1 NonFinite Modes and excludes finite Modes and Usage/Charge. A
role-1 offered image is not upgraded or adopted. Source refusals remain before
target access, and the existing owned known-commit boundary does not become
engine installation authority. The verification receipts below describe the
original increment, not new layout-17 execution.

`OwnedTrustedNativeReplacement` and the separately enabled
`ExperimentalStateMachine::replace_create_send_image_with_catalog` operation
replace initialized CreateSendLayout17V1 business state and its catalog through the
existing serial owner. This is explicit trusted mutation, not engine snapshot
installation, runtime adoption, history repair, or log-purge authorization.
Existing capability defaults, dependencies, engine traits, and
server startup remain unchanged.

The separate [paired-image model](native-image-paired-model.md) explores bounded
cross-role records and conservative classification only; it grants no adoption
authority or paired physical writer to this replacement API.

## Owned Selection

The non-Clone request consumes a whole `Snapshot<LogTypes>` and independently
trusted stream, full expected old checkpoint, full selected checkpoint, and
SHA-256 of the complete selected artifact. Its fields are private and immutable;
Debug exposes only artifact length. No mutable carrier, extracted body, writer,
or second writable adapter escapes.

Synchronous packaging checks the body against both existing 64-MiB bounds,
snapshot ID against 79 bytes, both checkpoint membership payloads against 4 KiB,
and actual native membership against finite shape and canonical wire limits.
There are at most two configurations, 32 IDs per configuration, 32 nodes, and
512 bytes per address; exact membership encoding remains at most 4 KiB.
Borrowed serialization counts bytes without collecting or copying membership.
These allocation-free checks precede operation Box/channel allocations and may
refuse before owner poison is checked. Already caller-owned allocation and spare
capacity are not reclaimed or counted as aggregate memory.

## Owner Ordering

`create_with_snapshot_replacement` and `open_with_snapshot_replacement` require
`CatalogCommittedStore` with a bounded business reader. They install only the
private replacement capability; export and catalog build/read are independent
constructor choices. All older constructors and bootstrap variants leave
replacement disabled. The sealed local-compaction allowlist also refuses this
new generic mutation, without backend work or closing its frontier.

The owning future is inert until first poll. Accepted work reserves the existing
full 64-MiB owner admission charge, excluding other accepted work until result
publication and refund. Healthy/disabled checks precede source preflight.
The worker borrows the original privately owned body regardless of cursor,
derives canonical native metadata, and compares every supplied `SnapshotMeta`
field against the actual validated body projection. This agreement does not
derive authorization from an offered snapshot.

The [domain replacement](committed-image-replacement.md) then checks the
independently trusted complete selection before its one bounded target capture.
It compares the full old checkpoint, prepares stale-only Deletes and every
selected Put, and performs exactly one combined business/init/catalog commit.
The original artifact pointer is retained unchanged. No body clone, source
store query, postcommit read, factory, validation, reader refresh, or reopen is
needed to manufacture success.

`CommittedNativeReplacement` is a private-construction, non-Clone, zero-sized
known-commit result. It is prepared before commit and only moved after success;
it contains no image, IDs, source-health certification, historical receipt,
ancestry, anti-rollback, quorum, engine-adoption, or destructive log authority.
An explicitly trusted choice may select earlier progress or an initial image.

## Failure And Custody

Source/native disagreement, selection/target mismatch, and quota/allocation or
semantic refusals are nonfatal conservative refusals. Physical target capture
failure, domain poison, and every returned combined-commit error poison the
owner. Even a backend limit error becomes static `Domain(CommitUnknown)` because
the complete new state may already be durable. No successful result, automatic
retry, or rollback inference follows. Nested errors expose no backend details
or caller metadata.

Busy/Closed before admission differs from a lost response after acceptance.
Losing an accepted waiter does not cancel work or release its charge early.
Shutdown waits for the actual native owner, including gated accepted work;
panic/lost response cannot certify rollback. Caller-owned unpolled futures hold
data/admission handles, not physical database handles.

The source, raw and encoded target captures each have separate 64-MiB bounds;
Delete/Put logical payload is at most 128 MiB over 131,072 mutations. Metadata
is at most 8 KiB. These objects, projections, ordinary allocation/channel
internals, spare capacity, semantic/backend staging, and MVCC may coexist.
Admission accounting is not aggregate heap/RSS, universal OOM recovery, or a
successful full-capacity-artifact guarantee.

## Verification Scope

Paired Memory/Fjall cases check exact mutation contents, original artifact
pointer and ignored cursor, standalone-builder interoperability with a maximum
message body, independent complete selection, same-full-checkpoint/different
valid body, every actual native metadata field, earlier/initial selection,
default capability refusal, nonfatal quotas, capture failures, and injected
before/after commit errors plus a backend limit refusal. A retained private
catalog handle confirms sealing denies the
new mutation without backend activity while permitted reads remain unchanged.

Custody tests gate actual capture, lose the waiter, check Busy and full charge,
delay joined shutdown, then verify exact publication and refund. A backend panic
also requires actual join. Synchronous bounds and compile-fail examples cover
capability bounds, immutable/non-Clone ownership, private construction, no body
extraction, and move-versus-live-borrow exclusivity.

A dedicated durable test checks success and returned errors before or after a
completed physical commit. All originating writers, controls, business/catalog
readers, and clones are released before two independent same-directory opens
while owned images, catalog outputs, inert futures, and static results survive.
It verifies exact old or selected records and both catalog components.

These are not mid-sync, power-loss, real allocator exhaustion, valid exactly
64-MiB artifact success, independent history/authenticity, or engine activation
proof. Live SDK behavior and cross-directory adoption are outside this API.

The final focused run passed 37 regular tests and eight compile-fail examples.
Ten consecutive repetitions passed all 370 regular checks. The full cluster
suite passed 695 checks; default and all-feature workspace runs each passed
4,489 checks with ten opt-in SDK gates ignored. Formatting, both strict workspace
lint configurations, both workspace builds, administrative protocol descriptor
generation, and whitespace checks passed. All Rust verification used two
low-priority cores and the existing shared build cache.

Initial compilation found two tests comparing errors from different API layers
and unnecessary mutable bindings. Those test-only corrections were made before
the complete final focused and broad runs above; production behavior did not
change. No live SDK gate was rerun for this isolated owner capability.
