# Experimental State Machine

`cluster::ExperimentalStateMachine` implements the pinned OpenRaft 0.9.25
`RaftStateMachine` interface over the existing
[committed queue machine](committed-queue-apply.md). It is an isolated storage
adapter, not a running Raft node or a replicated command proposer. Existing
listeners, timers, standalone formats, and production startup remain unchanged.
There is no network, quorum proof, leader clock/read barrier, snapshot support,
or new deployment mode in this increment.

## Ownership And Admission

Creation consumes one trusted `CommittedStore` writer and initializes only a
pristine store. Open validates the existing stream, checkpoint, complete leader
identities, and effective membership. Missing, malformed, or incompatible
progress is not adopted or repaired. The log and state machine use separate
replica directories; a log-role directory is not a committed queue store.
A failed initialization may still have persisted its complete baseline before
returning an error. Reopen validates the actual state; another creation attempt
does not reset initialized progress.

The adapter is not Clone and exposes no writer, raw batch, or writable inner
conversion. Synchronous creation/open precede one blocking owner. After that,
all accepted apply and applied-state queries run through its FIFO. No task is
spawned for each request, and synchronous backend I/O does not run on the Tokio
executor.

Admission retains at most 32 queued/in-flight jobs and 64 MiB of charged work.
An apply packet is limited to 256 entries and 64 MiB of canonical encoded-entry
bytes; scalar queries carry a conservative 64-byte charge. These are finite
work/input bounds, not exact heap, caller-result, or process RSS limits.
Encoding checks consume at most one entry beyond the count limit and reject a
late invalid entry before any packet is admitted or business state is written.
The input's adjacent full log identities must be contiguous and increase under
the pinned library's `(term, node, index)` ordering.

The full apply bound intentionally differs from the log adapter's smaller
append bound. The library can submit an entire committed retained range in one
apply call; silently truncating it or imposing the append chunk size would
violate that contract.

After admission, dropping the caller removes only its waiter. The owner retains
the whole packet and its charge through actual application and result
publication, then refunds capacity before the method can return. Explicit
`shutdown` closes admission, drains accepted work, and joins. Canceling that
wait does not cancel its accepted work or join task. Adapter Drop requests a
drain without synchronously joining. Owner unwind fails current and queued
obligations rather than reporting success.

## Apply And Recovery

Only the log codec's blank, membership, primary queue creation, and immediate
byte-body Send entries are representable. Queue data moves into the restricted
domain work without another body clone. Membership uses the same frozen
canonical membership fields as the log codec, as a bare schema-1 payload capped
at 4 KiB. It does not add an envelope header that could push a valid payload
beyond the domain's membership limit. Unsupported schemas, malformed shapes,
noncanonical bytes, or inconsistent source identities fail recovery.

`applied_state` returns the exact current full identity and `StoredMembership`
with its original source identity, including joint configurations and learners.
Absent membership returns the library's initial empty membership, not a made-up
voter set. The adapter checks full leader ordering in checkpoint predecessor
and membership-source relationships, beyond the domain's term-only ordering.

The complete packet is converted and its starting checkpoint relationship is
validated before its first entry is committed. Each entry then performs one
atomic business/progress commit using the existing domain machine. Blank and
membership entries update progress; deterministic business refusals update only
progress and the committed timestamp watermark.

This is not one atomic multi-entry transaction. A later physical failure can
leave a durable prefix, including the failing entry if its commit persisted
before returning an error. The method reports a fatal storage error, not a
partial-success response vector or proof of rollback. Further apply and
applied-state operations fail on that poisoned adapter. Reopening validates the
actual checkpoint and resumes from that position; no suffix is automatically
resubmitted or silently skipped.

## Typed Results

Each completed entry has one bounded `LogApplication` response:

- `CheckpointOnly` for blank or membership progress.
- `QueueCreated` for successful primary creation.
- `Sent { sequence }` for an acknowledged sequence allocation. Duplicate
  detection can discard the body, so this is not proof of a newly enqueued row.
- `Refused` with a finite cause and bounded numeric metadata, not a serialized
  broker error or backend detail.
- `AlreadyApplied` with the exact entry identity and no reconstructed original
  outcome.

Latest-position replay requires the original predecessor and canonical content
fingerprint. It does not reapply business work, allocate another sequence, or
invent a send result. Older history and conflicting replay are not certified
by the single checkpoint. A future client owner must treat replay metadata as
original-result unavailable, not as an original success receipt or request
deduplication key.

Only the current Create/Send refusal kinds and expected result/effect shapes
map to normal responses. Unexpected outcomes, effects, or refusal variants are
fatal even if progress has already persisted. Public trait errors are static
and sanitized; they never disclose message bodies, entity names, backend paths,
or decoder details.

## No Snapshots Or Runtime

Snapshot building, receiving, and installation explicitly return unsupported
storage errors without writing state. They do not create an empty or
metadata-only snapshot of queue data. A healthy `get_current_snapshot` returns
`None` after an owner health/recovery query; a poisoned adapter returns an error.
The `Cursor<Vec<u8>>` associated type is not a snapshot implementation.

A later no-snapshot runtime must use `SnapshotPolicy::Never`, reject purged
history without snapshots and applied-state-ahead-of-log pairings before
`Raft::new`, and forbid manual snapshot/purge paths. The pinned startup helper
can rebuild or purge regardless of that policy. Finite retention must not be
mistaken for automatic compaction.
The separate [owned storage-pair preflight](experimental-replica-preparation.md)
now checks those pairings and the full applied fingerprint chain, membership,
watermark, and votes without applying or repairing history. Private retirement
tokens preserve storage-thread join ownership; no runtime is activated.

The [pinned state-machine contract](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/storage/v2.rs)
defines applied identity, membership, and one result per entry. Its
[core apply path](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/core/raft_core.rs)
can materialize the full committed range, and its
[startup helper](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/storage/helper.rs)
performs recovery repair. Those library contracts do not authenticate a direct
adapter caller or establish quorum commitment on their own.

## Verification

At the state-machine adapter checkpoint, the cluster suite passed 70 unit
tests, 48 log integration cases, 41 state-machine integration cases, and three
compile-fail examples. Its new
public cases cover both memory and durable replica stores, full retained-range
application, exact membership recovery, whole-input refusal before I/O,
caller-loss admission leases, executor responsiveness, joined shutdown, and
poisoning after physical errors or owner unwind.

Four subprocess cases exit before a middle entry's backing commit or after its
`SyncAll` completion. Two of those recover through the actual persisted log
reader in a separate directory. They verify exact durable prefixes,
provenance-only latest replay, explicit suffix continuation, sequence allocation,
and subsequent reopening. These are selected persistence boundaries, not
arbitrary power-loss or quorum evidence.

Ten repeated cluster runs passed, totaling 1,620 test executions. Both workspace
feature configurations passed 3,659 tests, and all ten serial official-client
checks passed. Formatting, both lint configurations, both builds, and protobuf
validation also passed with builds restricted to two cores.
