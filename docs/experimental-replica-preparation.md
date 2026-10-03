# Experimental Replica Preparation

`cluster::ExperimentalReplicaStores::prepare` consumes a unique log adapter and
a unique committed state-machine adapter, validates their pairing, and seals
them behind frozen diagnostic observations. It does not call `Raft::new`, start
a network, elect a leader, propose client work, or enable production startup.
Existing listeners, standalone formats, and local proposers are unchanged.

## Pairing Without Repair

Preparation queries the actual owners for their validated log profile and
healthy committed checkpoint. The intended node must match the log profile,
and both stores must have the same stream. No caller-supplied checkpoint or
profile is accepted as recovery evidence. The log profile query rereads its
canonical record and rejects changes to the owner's immutable profile.

Purged history is refused because the state machine has no snapshots. Applied
state ahead of the retained log is refused rather than prompting a repair or
purge. Empty history requires an empty applied checkpoint, membership, and
timestamp watermark. Nonempty history must begin with the pinned library's
genuine initial membership at the exact default `(term 0, node 0, index 0)`
identity. Other manually populated storage-adapter histories are not adopted
as initialized runtime history.

All retained rows are checked across the existing count/byte-limited read
chunks, under the total 256-entry/64 MiB bound. Rows must be contiguous with
increasing full identities and the exact recorded tail. The applied position
must name the same full identity in that history. A legitimate unapplied
suffix is left untouched; the tail is not treated as a committed position.

For the applied prefix, preparation reconstructs the frozen canonical
fingerprint chain from index zero. Both computed current and predecessor marks
must match the checkpoint. This detects changed work at the same full identity
and divergence in older applied content, not only a different final entry.
The latest applied membership must match checkpoint source, schema, canonical
payload, and recovered `StoredMembership`. The checkpoint watermark must equal
the maximum stamped queue timestamp in that applied prefix, including commands
that produced deterministic refusals. Unapplied timestamps do not advance it.

`CommittedQueueWork::entry_mark` is the pure, bounded fingerprint helper used
for this comparison. It neither writes nor prepares business state, authorizes
a predecessor, reconstructs an outcome, nor proves commitment. Business-invalid
queue configurations and message IDs remain hashable because their committed
refusals are valid log content. No encoded body is allocated by that helper.

These checks are log/checkpoint consistency, not proof that a trusted direct
caller obtained quorum commitment. They do not exhaustively audit unrelated
business records, authenticate a stream, or provide client retry receipts.
Preparation performs no apply, vote, append, truncation, purge, adoption, or
format migration.

## Voting And Configuration

For noninitial history, the persisted vote must compare at least equal to the
committed vote for the retained tail's leader under the pinned library order.
A newer uncommitted candidate is legitimate; the same nonzero leader with an
uncommitted vote, an older leader, or a missing vote is refused. The narrow
initialization exception permits a missing/default vote only with the lone
default-identity membership still unapplied. It does not permit unvoted queue
work or an already applied initial membership.

The sealed pair exposes read-only fixed replication settings, not a running
node. `SnapshotPolicy::Never` prevents policy-triggered snapshots. The count-only
payload limit is 15, so even 15 maximum-sized 260 KiB entries fit the log
adapter's 4 MiB append bound. Setting the limit to 32 would satisfy only the
entry-count cap, not the byte cap. Snapshot/purge commands are not exposed.

The finite retained cap includes initialization membership, election no-ops,
and later memberships as well as queue commands. Preparation reserves no future
election or write capacity and is not an indefinite no-compaction availability
guarantee. A later runtime must check startup headroom and treat post-submission
storage exhaustion as fatal/ambiguous, never as proof of business rollback.

## Owned Cleanup

The adapters privately transfer their unique native thread join handles into
retirement tokens. Tokens have no reader, writer, operation, or admission-close
authority. An adapter closes admission and then signals retirement on Drop;
the token waits for that signal before joining. Missing retirement does not
skip the actual join, and owner failures remain distinguishable internally.

Under a live Tokio runtime, the first poll of preparation transfers both
adapters into one owned supervisor before awaiting validation. Losing that
waiter cannot cancel accepted queries or preparation. If success publication
is lost, the resulting owned pair dispatches retirement and cleanup on the
captured runtime. Validation failure waits for both owners to be joined before
returning; one failed owner never skips cleanup of the other.

Explicit `shutdown` retires both adapters and joins both owners. Its first poll
dispatches cleanup before awaiting, so caller loss does not cancel it. Dropping
a prepared pair requests that cleanup without providing an observable joined
result. An unpolled preparation future retains the original adapters' ordinary
close/drain-only Drop behavior. No-runtime failure and shutdown of the entire
Tokio runtime likewise do not supply a joined-cleanup guarantee. There is no
forceful thread termination, backend I/O timeout, or automatic retry.

The distinction matters for a future running node: the pinned
[Raft shutdown](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/raft/mod.rs)
joins its core/ticker, while its
[state-machine handle](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/core/sm/handle.rs)
does not await the worker's join handle. The
[worker](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/core/sm/worker.rs)
can continue draining queued committed apply. A future node owner must wait
for actual adapter retirement rather than closing its storage owner immediately
after core shutdown. This increment joins only the storage threads it owns,
not all library background work.

The pinned [startup helper](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/storage/helper.rs)
can purge when application is ahead and rebuild a missing snapshot after purge,
even with the snapshot policy disabled. Strict pairing rejects those inputs
before a later runtime can enter that helper. The
[initialization path](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/engine/engine_impl.rs)
and [vote rules](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/vote/vote.rs)
define the narrowly accepted initial-membership recovery case.

## Verification

At this storage-pairing checkpoint, both default and all-feature workspace
runs pass 3,720 tests, with 10 SDK scenarios ignored in those runs and all 10
passing separately. Both warnings-as-errors lint configurations, both workspace
builds, formatting, the admin protocol descriptor, and whitespace checks pass.

The cluster suite passes 215 tests: 81 unit tests, 48 log-storage cases, 41
state-machine cases, 41 storage-pairing cases, and four compile-fail examples.
Ten additional complete cluster runs pass all 2,150 test executions.
Pairing coverage includes both memory and Fjall storage, strict whole-history
checks, cancellation, owner failure, and cleanup of the sibling owner after
one fails. A direct durable case drops the matching readers and immediately
reopens both directories after explicit joined shutdown.

A fixture writer's Drop notification is not independently proof of a native
thread join. The private retirement tests exercise the actual join boundary;
the durable reopen case checks the externally observable directory lifecycle.
This increment does not start a Raft node or establish quorum durability.
