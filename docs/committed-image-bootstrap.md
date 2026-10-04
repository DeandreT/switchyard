# Pristine Committed Image Bootstrap

`CommittedStateMachine::bootstrap_create_send_image(writer, selection)` restores
one explicitly selected CreateSendV1 image into a pristine replica store. It
consumes the unique `storage::CommittedStore` writer and returns a machine only
after one successful atomic commit. Ordinary create/open, record formats, native
owners, server startup, and replication runtime behavior remain unchanged.

This is not populated-target replacement or the replication library's snapshot
installation. It provides no snapshot catalog, current-snapshot result, transport,
log baseline, history purge, or runtime adoption.

## Explicit Selection

`TrustedCreateSendBootstrap::new` borrows immutable artifact bytes and a
separately supplied full expected checkpoint. It also takes the expected stream
and SHA-256 digest of the complete artifact, including its embedded checksum.
Its fields are private and diagnostics omit their contents. Creating the request
does not validate the source or touch the target.

The expected stream, checkpoint, and digest must come from the caller's trusted
selection decision. Deriving them from the same untrusted bytes establishes no
authorization. The unique writer supplies the existing low-level mutation
authority; neither this request nor image consistency is a provenance, quorum,
authenticity, ancestry, anti-rollback, or historical-result certificate.

Bootstrap checks the selected stream, the captured complete checkpoint, and
the complete artifact digest. Checkpoint equality includes the current and
predecessor full identities and fingerprints, timestamp watermark, and opaque
membership source/schema/payload. A changed valid business row at the same
checkpoint is refused when its digest differs. Source and target use the same
stream; relabeling or checkpoint rewriting is not supported.

Container limits and canonical structure are checked before full-artifact
digest work. Current [whole-image semantics](committed-image-validation.md)
must also pass. All source checks precede even the target reader factory call
inside this function. The caller has already opened the writer, so this is not
a claim that target acquisition performed no earlier I/O.

## Pristine Target And One Commit

The writer's initialized flag must be false and a single empty-prefix probe
with limit one must return no records. An initialized target is refused even
when its record view is empty or its checkpoint already matches. An
uninitialized but nonempty target is also refused. The caller owns the trusted
unique writer and must serialize its use; no compare-and-swap or defense against
contract violations and trusted out-of-band mutation is introduced.

The ordinary target probe bounds row count, not returned value bytes or backend
allocation. No complete target snapshot, bounded-reader requirement, or source
read fallback is added. This preserves the existing constructor contract.

Mutation-vector capacity and every key/value copy are reserved fallibly before
the sole write. `WriteBatch::try_reserve_mutations` reserves capacity without
changing existing mutations or granting write authority. The prepared batch
contains exactly the source's ascending Put rows, including the original
checkpoint bytes. There are no deletes, migrations, clock stamps, business
replays, or normalized values.

The existing committed-store contract atomically commits all records and the
initialized flag, with durable persistence before success. Bootstrap then
constructs the machine directly using the already obtained matching reader.
It does not open again or perform a postcommit checkpoint, initialization, or
validation read which could obscure a known successful commit.

## Refusals And Recovery

`CommittedImageBootstrapError` is a flat static enum with no backend detail or
supplied source bytes:

| Cause | Decision |
| --- | --- |
| `InvalidSelection` | Invalid selected stream or expected-checkpoint stream disagreement |
| `SelectionMismatch` | Captured stream, complete checkpoint, or full artifact digest differs |
| `LimitExceeded` | Container bounds exceeded |
| `Allocation` | An explicit batch/output reservation was refused |
| `UnsupportedProfile` | Unsupported format or business profile |
| `InvalidImage` | Malformed container or inconsistent supported image |
| `TargetNotPristine` | Initialized or nonempty target; no commit attempted |
| `TargetReadFailed` | Pristine proof failed physically; no commit attempted |
| `CommitUnknown` | Sole commit failed; complete durable restoration may have occurred |

Every failure consumes the writer and returns no usable machine. In particular,
a commit error is not proof of rollback or permission to retry. Release every
physical store-bearing handle, reopen, and inspect actual initialization,
checkpoint, and rows before explicitly choosing the next action. A completely
restored target must use ordinary open, not another bootstrap. The API does not
automatically retry, repair, or discard data.

## Memory Scope

Borrowed artifact bytes and copied batch key/value data may coexist, each under
a separate 64 MiB logical bound. Temporary semantic maps are released before
batch copies. Row/mutation metadata, spare capacity, target probe values,
backend staging/cache, and RSS are excluded. Fallible copies do not make every
existing semantic-validator or backend allocation fallible.

## Verification Scope

Observed Memory/Fjall writers count reader factories, initialization, gets,
exact scans, ordinary snapshots, reader applies, commits, and complete attempted
batches. Successful bootstrap requires one reader factory, one initialized
query, one `([], [], 1)` scan, one commit, and no other operation before
post-return inspection. Every mutation must match one source row exactly and
be Put. Source refusals require no target API call; private checkpoint/digest
fixtures use a target which panics on any access.

Cases include maximum body/ID/session, TTL, a duplicate sequence hole, opaque
membership, refusal watermark/business-clock separation, exact latest replay,
resumed sequence allocation, and initial/refusal-only images without a business
clock. Wrong selection, changed valid rows at the same checkpoint, unsupported,
malformed, inconsistent, and oversized images refuse before target access.
Initialized-empty and forced-uninitialized orphan stores are never adopted.
Target read failures remain static and perform no commit.

Before/after-commit errors share the same unknown-result error but recover
different exact actual states. Dedicated Fjall cases drop every old physical
handle before reopening the same directory. Separate child processes exit
immediately before or after a real synchronous commit; reopening checks either
the pristine flag/empty records or the complete flag/checkpoint/image, then
releases all handles and opens again. The child runner retains bounded output
tails and kills/reaps on its child-only deadline; it does not time out in-process
domain storage I/O.

The storage reservation tests exercise deterministic capacity overflow without
attempting allocator exhaustion. Compile-fail coverage requires the unique
committed writer rather than an ordinary store capability.

All 31 focused tests passed. Ten repeated crash-boundary runs passed another 30
tests, including 20 actual child-process abrupt exits and recoveries. The full
domain suite passed 1,229 tests and the storage suite passed 91. The default
workspace suite passed 4,075 tests with 10 intentionally ignored. Formatting,
strict default and all-feature workspace Clippy, both workspace builds, admin
descriptor validation, and whitespace checks also passed. The full all-feature
workspace tests and live SDK gates were not rerun for this isolated storage and
domain increment.

No induced real key/value allocation failure, mid-sync or power-loss fault,
fully populated 64 MiB/65,536-row restore, RSS proof, populated-target replacement,
native membership interpretation, snapshot catalog, engine installation, runtime
adoption, or history compaction is established here. Existing no-snapshot
preflight/runtime still require matching complete retained history and refuse
purged or application-ahead pairings.

The separate opt-in [combined catalog bootstrap](committed-image-catalog-bootstrap.md)
publishes these same selected rows, initialization, and an opaque image/metadata
pair in one commit. This ordinary constructor remains catalog-independent and
keeps its original generic bounds, source ordering, and target refusal behavior.
