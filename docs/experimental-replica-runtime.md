# Experimental Replica Runtime

`cluster::ExperimentalRaftCluster` runs three actual OpenRaft 0.9.25 nodes in
one process, over separate unique log and committed-state writers. It exposes
only fixed membership, bounded primary-queue Create/Send intents, diagnostic
routing hints, and joined stop/shutdown. Existing server listeners, development
proposers, timers, and production startup remain unchanged. No socket transport,
deployment activation, snapshot, purge, arbitrary membership change, or raw
writable engine is exposed.

## Startup And Recovery

`create` consumes three sealed [prepared pairs](experimental-replica-preparation.md)
with distinct node IDs and one stream. All pairs must have pristine retained
history and no prior vote. Every pair is revalidated before any node starts.
The runtime additionally checks every retained membership, including unapplied
suffixes, against its exact three-voter configuration and stable in-process
addresses. Joint membership, foreign addresses, and learners are refused.

All endpoints exist before the lowest-ID node performs the library's real
initialization once. No fabricated membership, vote, or leadership result is
used. `open` requires existing initialized membership somewhere in the retained
histories and never initializes, adopts, repairs, or resubmits queue work. Other
empty followers can catch up through normal election and replication.

Startup reserves room for an election no-op and one maximum command on every
replica. Creation also reserves its initial membership. The existing total
retention limit remains 256 entries and 64 MiB, including memberships and
no-ops. This is a finite no-compaction experiment, not indefinite availability.
Repeated elections or concurrent native work can still exhaust retained
capacity. A post-submission failure is ambiguous, never rollback evidence.

Node startup initially pauses ticks. After every endpoint is attached and
initialization completes, the runtime activates normal election/heartbeat
ticks. Strict preflight prevents the pinned
[startup helper](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/storage/helper.rs)
from entering unsupported snapshot rebuild or application-ahead purge paths.

## Owned Clients And Time

`QueueIntent` represents primary queue creation or immediate byte-body Send;
callers cannot supply the committed timestamp. Structural/encoding limits are
checked before admission. Business-invalid configurations and message IDs
remain representable because their normal committed refusals are valid results.

Each node admits at most 16 queued/running client jobs and 4 MiB of conservatively
charged encoded work. One owner submits them serially, without forwarding or
automatic retry. An unpolled submission is inert. Once admitted, caller loss
removes only its waiter: work and its count/byte charge remain owned through
completion or joined retirement. Refund occurs before a successful waiter can
return. A cloned public handle retains ingress, not the writable engine or
storage reader, and becomes permanently closed when its generation retires.

Before timestamping, the owner calls the library's
[linearizable barrier](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/raft/mod.rs)
and then queries the actual healthy applied checkpoint. A metric or leader hint
is not commitment or clock evidence. The checkpoint must cover the barrier's
full log identity. UTC is sampled after that check; a committed watermark up to
500 ms ahead is clamped, while a larger lead or invalid UTC refuses this intent
before submission. This is bounded clock handling, not a distributed-time proof.

Actual healthy log retention is checked before attempting the one native
`client_write`. A `KnownRejected` result concerns this unsubmitted intent, not
an assertion that elections, barriers, or other replica state made no progress.
After the submission-attempt boundary, failures are `Unknown`, including a
leadership change. The engine can report forwarding after accepted work is
truncated; that error is not permission to retry automatically.

A successful reply includes the exact committed identity and bounded typed
application outcome. The real engine gates it on quorum replication and local
application; normal business refusals advance committed progress without a
successful business mutation. `AlreadyApplied` metadata means the original
result is unavailable, not a reconstructed success receipt or deduplication key.

## Transport Bounds

The private in-process route registry binds every accepted RPC to exact source
and target generations. A stale source cannot retarget after retirement.
Once forwarded, work is held until the actual target API finishes, even if a
caller times out or drops. Never-forwarded admitted packets can be refused
during retirement. Retiring an old generation cannot close a replacement.

Transport admits at most 32 queued/running jobs and 12 MiB globally, with a
4 MiB per-target cap. Charges include active work and the gap between reservation
and publication. Whole AppendEntries requests are validated before touching the
remote engine: malformed tails, full-identity ordering, membership, and byte
limits are checked without partial acceptance. Configured chunks hold at most
15 maximum entries within the 4 MiB storage append bound. Oversized splittable
requests return a nonzero payload hint; offline, busy, stale, or unsupported
routes fail rather than inventing success. Snapshots are refused before queuing.

These are bounds on owned admitted requests, not process RSS. The pinned engine
has internal unbounded API/notification/state-machine queues; the runtime does
not certify those queues or caller-owned results as a finite heap bound.
Network chunks also do not bound native application batches: a committed jump
can apply the whole retained prefix. Full log reads and state-machine applies
share the same 256-entry/64 MiB limits and canonical size accounting, so an
individual valid retained range fits both. This does not guarantee aggregate
queue admission or availability under native work pressure.

## Joined Retirement

First polling startup or cluster shutdown transfers cleanup to an owned task
under the live captured Tokio runtime. Losing a waiter does not cancel accepted
startup or cleanup. Partial startup failure joins every started node and every
remaining pair; a completed failed startup future is removed before cleanup,
never polled a second time.

Retirement synchronously closes public admission and exact-generation routes,
then starts core shutdown, client drainage, transport drainage, and owned admin
completion together. Waiting for pending minority work before stopping the
core would deadlock. The pinned
[Raft shutdown](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/raft/mod.rs)
does not itself join the separate
[state-machine worker](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/core/sm/worker.rs).
Storage admission therefore remains open while that worker drains already
committed work. Both unique native storage owners are joined before a held
terminal client result is published and its charge refunded.

Stopping one node retains its completion record in the cluster before the first
await. Canceling that waiter cannot exclude its still-draining storage from a
later whole-cluster shutdown. Repeated stops observe the same final result;
shutdown starts all remaining stops before joining every retained completion,
including failures. Routing hints exclude stopped IDs, but still prove neither
leadership nor quorum.

Fatal idle-core metrics also initiate this retirement without requiring another
client request. Worker unwind retains its active completion obligation rather
than releasing it early. Backend failures remain static public causes; one
failed owner does not skip joining its healthy sibling.

No-runtime failure, unpolled Drop, or shutdown of the whole Tokio runtime does
not provide an observable joined-cleanup guarantee. A failed `Raft::new` starts
a ticker before fallible storage recovery; the pinned public API exposes no
ticker join handle on that failure path. The runtime joins its actual storage
owners and signals normal cancellation, but does not claim every library task
has been joined. There is no forced thread termination or backend I/O timeout.

## Verification

Verified with Rust 1.97.1, two build jobs, two Rust test threads, and the shared
build directory. Both all-feature and default-feature workspace runs passed
3,821 tests each, with ten SDK tests intentionally ignored in each ordinary run.
The explicit current .NET SDK interoperability run passed all ten tests.
Formatting, both strict workspace lint configurations, both workspace builds,
the administration protocol descriptor, and whitespace checks also passed.

The complete cluster suite passed 316 tests in each of ten consecutive runs
(3,160 test executions). It includes 144 internal tests, 48 log-adapter tests,
41 committed-state tests, 41 preparation tests, 17 public runtime tests,
19 public startup tests, and six compile-fail API checks.

Memory and durable tests exercise actual quorum persistence, local application,
caller loss, bounded ingress, minority refusal, late native storage drainage,
canceled stop retention, partial startup failure, and joined cleanup. Direct
durable recovery reopens all six paths after releasing prior store controls and
readers. Recovery checks preserve original records and membership; a subsequent
leadership-change unknown is neither retried nor counted as acknowledgement.
Separate quorum tests require confirmed acknowledgements from the real engine.
