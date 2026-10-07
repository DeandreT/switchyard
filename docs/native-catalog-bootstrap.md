# Selected Native Catalog Bootstrap

## Current Boundary (2026-10-06)

Actual pair validation requires a role-2 `CreateSendLayout17V1` artifact before
domain target access. Domain bootstrap independently requires the same narrow
generation-1 NonFinite profile and trusted selection. Role 1 and finite Modes
are refused, not upgraded. SWYM metadata framing is unchanged, and publication
still uses one combined commit before owner startup. The verification receipts
below describe the original increment, not new layout-17 execution.

`ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(writer,
selection, metadata)` restores an exactly selected image and its matching native
metadata into a pristine catalog store, then starts its unique state owner. It
requires `CatalogCommittedStore`, not a bounded business reader. It uses the
[combined domain bootstrap](committed-image-catalog-bootstrap.md), not ordinary
bootstrap followed by a separate retention write.

The `bootstrap_create_send_image_with_catalog_operations` variant additionally
requires `W::Reader: BoundedStateStore` and enables the existing
[owned catalog build/read capability](native-snapshot-catalog.md). Plain bootstrap
leaves that capability disabled. Both variants leave the independent image-export
operation disabled. Existing constructors and their bounds are unchanged.

## Before Target Access

The immutable borrowed selection pins trusted stream, full checkpoint, and
SHA-256 of the entire artifact. These expectations must come from an independent
caller policy, not authorization inferred from incoming bytes. Its readonly
`artifact_bytes()` getter returns the original unvalidated slice; it supplies no
mutation, provenance, commitment, or installation authority.

The actual metadata/artifact pair is checked first by the
[native pair codec](native-snapshot-metadata.md). This validates framing,
checksums, full CreateSendLayout17V1 business semantics, all captured checkpoint fields,
complete artifact digest, and native identity/membership recovery. Its owned
`SnapshotMeta` is derived in the same pure preflight scope, then dropped before
the first domain target API. Checking the caller's expected checkpoint alone is
not a substitute for checking the actual pair.

Domain bootstrap remains independently mandatory. It enforces exact trusted
stream, full checkpoint, complete-artifact digest, and source semantics before
obtaining the target reader. A valid actual pair with a wrong selected expectation
still refuses before target access. Opening/stamping the consumed writer earlier
is outside these functions' source-before-target guarantee.

The target is checked pristine under the catalog profile's trusted unique-writer
contract. One combined commit publishes exactly the selected Put rows, including
checkpoint bytes, initialization, and the original metadata/artifact pair.
Original input pointers are preserved through that call; there is no re-encoding
or second domain-owned artifact. The limit-one record probe bounds returned rows,
not value materialization, and the sequence is not CAS against out-of-band writes.

## Known Success And Failure

After known domain success, native state is constructed directly around the
returned machine. The optional variant installs the existing private catalog
capability before real owner startup. Neither variant reopens, gets progress,
captures, reads the catalog, revalidates, normalizes, or writes again after commit.

`StateMachineCatalogBootstrapError` distinguishes three static boundaries:

| Cause | Meaning |
| --- | --- |
| `Metadata(NativeSnapshotMetadataError)` | Actual pair/native preflight refused before target APIs |
| `Domain(CommittedImageBootstrapError)` | Exact domain refusal, including indeterminate `CommitUnknown` |
| `OwnerStartAfterCommit` | Domain commit succeeded but owner startup failed |

These errors contain no physical diagnostic source. Every result consumes the
writer; no error returns a usable machine or raw writer. Every combined-commit
error is unknown, even when the complete new target may already be durable.
Startup refusal instead follows known commit success. Neither permits blind retry
or implies rollback. Release all physical writers, controls, and readers, then
inspect an independent reopen before selecting a recovery action. A complete
target uses ordinary open, not another bootstrap.

After successful startup, shutdown closes admission, drains accepted work, and
joins the actual owner. Original owned images and catalog results hold no backend
handle. This constructor is synchronous before the owner exists; it adds no new
owner queue, admission policy, or combined log/state lifecycle guarantee.

Source bytes, copied business batch, backend catalog staging, bounded metadata,
and small native projections can coexist. Logical limits are not a combined heap,
spare-capacity, RSS, or universal allocator-failure guarantee.

Pair agreement and trusted pristine restoration grant no populated replacement,
source authenticity, ancestry, anti-rollback, quorum, engine adoption, snapshot
network flow, or history-purge authority. Engine builder, receive, install, and
get-current behavior, runtime configuration, and dependencies
remain unchanged.

## Verification Scope

Paired Memory/Fjall cases cover maximum body/ID/session sizes, TTL and duplicate
sequence holes, exact original rows and pair pointers, one combined Put-only
commit, and no postcommit reads. They pin plain disabled capabilities, a positive
unbounded reader, initial checkpoint-only restoration, optional catalog reads,
continued native sequence allocation, and later catalogs legitimately older than
current progress. Capture occurs only when explicitly requested after bootstrap.

Pure rejection tests use a writer whose every target API panics. Cases include
malformed/oversized/unsupported/checksum-invalid metadata, full checkpoint and
native identity/membership differences, a different valid body at the same full
checkpoint, unsupported business rows, and wrong trusted stream/checkpoint/digest.
Native-compatible pair agreement never overrides exact trusted selection.

Initialized targets refuse replacement. Precommit target read errors remain
static, and before/after actual commit errors never invoke the starter. A private
consuming fake starter drops known-committed state and returns `ThreadStart`;
this tests the distinct startup boundary, not real OS thread exhaustion.

Dedicated durable cases join actual owners and release every physical control
and reader before independent same-directory Fjall opens. They inspect exact
pristine or complete decisions without retry, continue application, and reopen
again while original owned outputs remain alive. Unexpected successful negative
test results are also joined before reporting failure.

Compile-fail examples reject ordinary writers and the optional capability without
a bounded reader. Static diagnostic checks cover error display/debug and absent
error sources. No native-bootstrap process crash, mid-sync/power-loss injection,
real allocator/OS exhaustion, exact-full-size artifact fixture, engine activation,
live node adoption, or compaction proof is established here.

The final source passed all 31 focused checks, including two compile-fail
examples, and ten additional regular-suite runs totaling 290 passes. All 1,299
domain tests, 544 all-feature cluster tests, and 4,303 default workspace tests
passed; ten opt-in SDK tests were ignored in the workspace run. Formatting,
strict workspace Clippy and builds in both configurations, protobuf descriptor
generation, and diff checks passed. Full all-feature workspace tests and live
SDK gates were not rerun for this isolated native constructor and readonly
selection accessor.
