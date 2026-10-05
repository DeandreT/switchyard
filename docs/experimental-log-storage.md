# Experimental Log Storage

This isolated `cluster` API persists votes and bounded log entries for exactly
OpenRaft 0.9.25 with default features disabled and `serde`/`storage-v2` enabled.
That version's storage-v2 interface is explicitly unstable, so the dependency
is pinned. This is not a running Raft node, network, replicated proposer,
snapshot implementation, or production deployment mode. The separate
[state-machine adapter](experimental-state-machine.md) now uses the same typed
entries, without activating a consensus runtime.
Existing listeners and local proposers remain unchanged; production startup
still refuses before opening storage.

## Separate Owner And Profile

`ExperimentalLogStore` consumes one trusted `storage::CommittedStore` writer
and internally derives its matching reader. It is not Clone. Its cloneable
`ReadOnlyLogReader` exposes only the replication library's read interface, not
the writer, a raw batch operation, or a writable backend conversion.

Use separate replica directories for a node's log and its
[committed queue state](committed-queue-apply.md). Both use the already isolated
replica layout, but the log has a mandatory immutable inner profile and a
mandatory progress envelope containing its optional vote, exact purged/present
log identities, retained entry count, and encoded byte count. Creation requires
an uninitialized empty store; open requires a complete matching profile.
Node, stream, codec, leader-identity configuration, and durable limits must
match exactly. Initialized directories with missing records are corruption,
not fresh stores. Neither state/log directory swaps nor standalone directories
are adopted or converted.

Startup scans at most the retained entry cap plus three records and checks
canonical records, key/index agreement, contiguous history, bookkeeping, and
unknown keys. It does not repair gaps or reconstruct missing progress.

After synchronous initialization/open validation, one blocking owner serializes
vote, log, read, truncation, and purge work. Admission and FIFO publication
share a gate with shutdown. At most 32 queued/in-flight jobs and 4 MiB of
charged accepted work are retained: encoded append rows plus a conservative
64-byte charge for each fixed-size scalar/read job. This is not exact heap or
response size. Capacity exhaustion returns a sanitized
storage error, not ordinary broker backpressure. No task is spawned per job.
The work lease remains owned through storage and completion publication;
dropping a caller removes only its waiter, never accepted I/O or its lease.
The returning method also waits for the owner's capacity refund, so an
immediate next append cannot race the previous completed packet's charge.

Explicit `shutdown` closes admission, completes accepted FIFO work, and joins
the owner. Dropping the adapter requests the same drain without synchronously
joining; read-handle clones cannot reopen admission. Owner unwind fails the
active and queued completion obligations rather than inventing success.
No arbitrary I/O timeout, forceful thread termination, or automatic retry is
introduced.

## Bounded Frozen Records

Own version-1 envelopes preserve complete `(term, node, index)` log identities
and the vote's committed bit. They do not depend on raw library serde layouts
or the evolving domain `CommandKind` enum. Only blank, bounded membership,
primary queue creation, and immediate byte-body Send entries are representable.
Queue commands carry the leader-supplied timestamp; this adapter does not
establish leadership or stamp it.

Bodies are limited to 256 KiB, encoded entries to 260 KiB, membership to 4 KiB,
and metadata to 8 KiB. An append is limited to 32 entries and 4 MiB; the entire
append is checked before storage writes. Only identical retained overlap is
accepted. Conflicting replacement requires prior truncation; holes and
resurrection at or before the purged boundary refuse.

Queue payloads additionally leave 128 bytes within the entry limit for the
committed-apply wrapper's stream, predecessor fingerprint, and full identities.
An entry admitted by this codec therefore cannot fail apply merely because
that wrapper needs more space than the log wrapper. This reserve is part of
the immutable profile; it does not reduce the 256 KiB body allowance.

Decoding checks raw envelope size, borrows strings/body before owned copies,
caps membership collections, validates structural identifiers, requires exact
consumption, and rejects noncanonical encodings. Queue-configuration and message
ID business refusals remain representable for deterministic committed apply.
The backend may already copy a raw stored value before these codec checks;
this is not an allocation bound for arbitrary oversized disk corruption.

At most 256 entries and 64 MiB of encoded history are retained. Full range reads
return the entire requested retained range under that finite global bound;
they never silently truncate it to a replication chunk. Only
`limited_get_log_entries` returns a contiguous prefix capped at 32 entries and
4 MiB, nonempty for a valid nonempty retained range. Missing/outside history and
maximum-index bounds have explicit handling.

These are encoded-record, accepted-work, and output-shape bounds, not a process
RSS guarantee. Caller-owned inputs/results, runtime/library queues, backend
buffers, and staging copies are not covered by the accepted-work lease.

## Durability And Failure

Rows and exact progress bookkeeping change in one privileged atomic commit;
Fjall performs `SyncAll` before reporting success. Vote/log I/O is serialized.
Append completion invokes the actual library `LogFlushed` callback only after
the backing commit succeeds. This initial adapter waits for that completion
before its append method returns; it does not claim a pipelined fsync throughput
target. Callbacks and reply cleanup run outside admission locks.

Truncation removes an inclusive suffix. Purge removes an inclusive prefix and
persists the supplied full purged identity atomically, including when its
boundary is beyond the present tail. An empty log reports that retained purge
boundary, not the domain applied index. The adapter does not infer authority to
purge from application progress; its trusted caller owns that decision.

A physical write failure may occur before persistence or after the entire batch
is durable. Both fail completion, poison subsequent stored-state operations,
and require reopening; neither is proof of rollback. Public library errors
and callback diagnostics contain static sanitized causes, not backend paths,
message bodies, or decoder details. Under the pinned library contract, a storage
refusal is node-fatal, never a checkpoint-only domain business refusal.

The optional library committed-index persistence methods retain their documented
no-op defaults. The separate durable apply adapter persists each applied entry
before returning, but no running node or client acknowledgement path is wired
to these stores. Optional committed-index persistence is not a quorum receipt.

There is no automatic compaction, snapshot, or purge. The finite retained cap
fails safely rather than deleting merely-applied history. The intended first
runtime must use `SnapshotPolicy::Never` until a real crash-safe snapshot path
exists, and must account for its resulting retention limit.
That policy is necessary but insufficient: the pinned
[startup helper](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/storage/helper.rs)
rebuilds a missing snapshot after purge and can purge when applied state is
ahead of the log. A future no-snapshot runtime must reject those pairings
before starting the library, rather than relying on the policy to suppress
startup repair.
The separate [owned storage-pair preflight](experimental-replica-preparation.md)
now rejects those inputs, validates applied fingerprints/membership/votes, and
retains private storage-owner retirement/join tokens. It still starts no node
or network.

A disjoint [sealed local compaction API](sealed-local-compaction.md) now performs
explicit catalog-backed deletion in a standalone quiescent pair. It does not
implement this adapter's storage traits, change its ordinary role, or enable
automatic engine/runtime compaction.

## Verification

At the log-storage checkpoint, the cluster suite had 40 unit tests, 48 public
integration cases, and two
compile-fail examples. Public cases exercise both memory and durable replica
stores, the real append-completion callback, immediate near-capacity appends,
caller loss, FIFO shutdown, corruption refusal, and retained-range limits.

Eight subprocess cases exit immediately before a backing commit or after its
`SyncAll` completion for append, vote, truncation, and purge. A further recovery
case reads a durable entry after callback loss and applies it into a separate
committed queue store, checking exact replay and sequence continuation. These
prove selected persistence boundaries, not arbitrary mid-journal power loss,
quorum commitment, or a running consensus state machine.

Ten repeated cluster runs passed, totaling 900 test executions. Both workspace
feature configurations passed 3,587 tests, and all ten serial official-client
checks passed. Formatting, both lint configurations, both builds, and protobuf
validation also passed with builds restricted to two cores.

The library's [pinned storage contract](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/storage/v2.rs)
defines serialized I/O and durable append completion. Its
[reader contract](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/storage/mod.rs)
distinguishes full reads from limited prefixes. These storage requirements do
not provide quorum commitment, leadership/read barriers, request retry
deduplication, or end-to-end replicated message durability on their own.
