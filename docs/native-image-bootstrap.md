# Native Pristine Image Bootstrap

`ExperimentalStateMachine::bootstrap_create_send_image(writer, selection)`
restores an exactly selected CreateSendV1 image into a pristine committed store
and starts its unique native state owner. The export-enabled variant,
`bootstrap_create_send_image_with_export`, additionally requires the matching
reader to implement `storage::BoundedStateStore` and installs the existing
[owned export capability](native-image-export.md). Plain bootstrap leaves
export disabled, even on a bounded backend. Existing create/open constructors
and their bounds are unchanged.

This is not the replication library's snapshot installation or populated-target
replacement. No runtime or server startup path uses these constructors. Snapshot
build, receive, install, and current-snapshot behavior, disk formats, retained
history requirements, transport, and production refusal remain unchanged.

## Validation And Commit

The request remains the immutable, borrowed
[`TrustedCreateSendBootstrap`](committed-image-bootstrap.md). Its new
`expected_checkpoint()` getter borrows only the caller's selected expectation;
it establishes neither artifact validity nor a commit decision. Expected stream,
complete checkpoint, and complete-artifact digest must come from independently
trusted selection policy, not be inferred as authorization from untrusted bytes.

Pure native recovery checks that selected checkpoint first. Full current and
predecessor identities must be compatible with native ordering, and membership
source, schema, and canonical payload must satisfy the existing native recovery
rules. An incompatible expectation is refused before domain source validation
or any target API call in these functions. This is deliberate error precedence,
not a claim that the caller's earlier writer acquisition performed no I/O.

Domain bootstrap then remains mandatory. It checks container bounds, exact
stream/full-checkpoint/complete-digest selection, and current whole-image business
semantics before creating the matching target reader. It requires the target to
be uninitialized and empty, prepares exact ascending Put-only rows, and performs
one atomic commit of records and initialization. The limit-one pristine probe
bounds returned rows, not value bytes or backend allocation.

On domain success, the native adapter directly wraps the returned domain machine
and starts the existing owner thread. It does not reopen, query the checkpoint,
or validate again after commit. This avoids obscuring a known successful write
with a later read failure. Export opt-in changes only the private capability
field before owner startup; it adds no second commit or capture.

Compatible native metadata is not fixed-cluster preflight, membership provenance,
source history, quorum, authenticity, ancestry, anti-rollback, historical-result
proof, or authority to purge retained logs. Source and target use the same stream.

## Failure And Ownership

`StateMachineImageBootstrapError` has only static causes:

| Cause | Meaning |
| --- | --- |
| `IncompatibleMetadata` | Pure native recovery refused the selected expectation; no target operation |
| `Domain(CommittedImageBootstrapError)` | Exact domain refusal, including indeterminate `CommitUnknown` |
| `OwnerStartAfterCommit` | Domain commit succeeded, but native owner startup failed |

Every result consumes the unique writer. No failure returns a usable machine.
Before-commit refusals, unknown physical commit decisions, and a known successful
commit followed by startup failure are distinct. None grants rollback or blind
retry permission. Release every physical writer and matching reader, reopen the
directory, and inspect actual initialization, checkpoint, and records before
choosing the next action. A completely restored target uses ordinary open, not
another bootstrap.

After successful startup, explicit shutdown closes admission, drains accepted
work, and joins the owner. Returned image bytes do not retain a physical store
handle. This constructor does not establish a combined log/state-owner lifecycle
or a new runtime adoption guarantee.

Logical artifact and copied batch-data limits remain separate and may coexist.
Semantic metadata, spare capacity, target probe values, backend staging/cache,
and RSS are outside those limits. The constructor adds no owner admission charge:
it runs synchronously before an owner exists. Later opted-in export uses its
existing full-cap owner admission policy.

## Verification Scope

Paired Memory/Fjall fixtures pin one reader factory, one initialized query, one
empty-prefix limit-one scan, one commit, and no postcommit get, snapshot, bounded
capture, or reader apply. They compare every attempted Put against exact source
rows. Plain bootstrap refuses export without I/O; explicit bootstrap exports
byte-identical validated source bytes with one bounded capture.

Cases cover native membership and full-identity recovery, initial checkpoints,
maximum message body, sessions, duplicate sequence holes, TTL, refusal watermark
separation, and resumed sequence allocation. Domain-valid but native-incompatible
checkpoints refuse before target access. Native-compatible wrong selections,
malformed containers, and unsupported business profiles preserve the target.
Initialized-empty and forced-uninitialized orphan targets are not replaced.
Precommit read errors remain static and perform no commit.

Before/after actual commit errors preserve `CommitUnknown` and never invoke the
starter. A private consuming starter seam deliberately drops known-committed
state and returns a startup refusal to test `OwnerStartAfterCommit`. This is a
staged refusal, not induced OS thread exhaustion. Real successful owners are
joined, and durable cases scope out every observation control and physical reader
before reopening the same directory. Further reopen and resumed-write checks pin
exact recovered decisions without bootstrap retry.

Compile-fail coverage rejects the export-enabled constructor without the
bounded-reader associated-type guarantee. A test-only SHA-256 dependency reuses
the workspace's existing package to pin complete artifact digests; no production
hashing dependency or package version changes were added to the cluster crate.

All 24 focused checks passed. The 23 regular cases also passed ten complete
repeat runs, for 230 additional executions. The full all-feature cluster suite
passed 457 tests, and the default workspace passed 4,099 with 10 intentionally
ignored. Formatting, strict default and all-feature workspace Clippy, both
workspace builds, admin descriptor validation, and whitespace checks passed.
An initial test-draft compile failure named a nonexistent temporary-directory
helper; it was corrected to the existing fixture before the focused tests,
repeats, and broader checks passed. The full all-feature workspace tests and
live SDK gates were not rerun for this isolated constructor increment.

No real allocator failure, OS thread-exhaustion fault, mid-sync or power-loss
injection, fully populated 64 MiB/65,536-row restore, or RSS proof is established
here. The domain bootstrap's separate child-process tests cover abrupt exits
before and after a synchronous commit, not torn-journal recovery. Snapshot
catalog publication, populated-state installation, engine adoption, and history
compaction remain unsupported.
