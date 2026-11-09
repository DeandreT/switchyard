# Persisted Consensus Driver

`cluster::consensus::driver` is an opt-in library combining pinned raft-rs 0.7
`RawNode`, the exclusive [consensus store](consensus-persistence.md), and the real
[peer TCP transport](consensus-transport.md). It runs elections and replicated-log
commit for one immutable three-voter namespace group. It does not project domain
state, return client outcomes, supply primary routing or consumers, implement
consistent reads, or enable production startup.

## Construction And Authority

`ConsensusDriver::bind` checks fixed identity, local voter, transport limits,
retention and driver/core options, then binds exactly the configured endpoint,
before calling a synchronous backend factory. Explicit fresh bootstrap requires
an empty backend. Existing stores must pass full profile-4 identity/history open
validation. There is no migration from domain profile 3 or local F0/F1 history.
The RawNode owns the sole non-Clone `ConsensusStore<B>`; no raw backend, ordinary
StateStore, second opener or independent persistence authority escapes.

Pre-vote, check-quorum and `ReadOnlyOption::Safe` are forced. The defaults use
heartbeat/election ticks 1/10 and a 100 ms timer. Automatic timers skip missed
ticks instead of queuing catch-up work; `None` allows explicitly bounded manual
tick admissions. These core settings do not establish a linearizable read lease.
Voting freshness, one vote per term, higher-term stepdown, distinct voter tracking,
current-term commit and divergent-tail reconciliation are the maintained core's
rules, not a second quorum implementation in this adapter.

## Original Ready Ordering

One owner serializes all core mutations. Before the next step, proposal, campaign,
tick or unreachable report, it completely handles the original Ready:

1. Refuse snapshots/read state and unsupported configuration work. Persist the
   complete Ready entries and hard state using the original synchronous atomic
   store operation. A real native write uses SyncAll even when `must_sync` is false.
2. Hold **all** associated messages, including leader messages the core would
   permit before persistence. Only successful persistence permits
   `advance_append(original_ready)`.
3. If its LightReady supplies a new commit index, persist that hard-state commit
   in a second atomic operation before releasing messages or the new observed
   durable frontier. Both first- and second-stage messages stay local until this
   processing pass succeeds.
4. Submit messages nonwaitingly to TCP. A local send receipt only records actual
   original write/task join, never a peer durable vote or a commitment outcome.

Any persistence/core/bound error or escaping synchronous panic retires the driver.
It does not retry a failed Ready, continue stepping, or send held durable replies.
The original Ready/LightReady, failure and panic payload remain owned until the
driver is destroyed or its finished store is explicitly recovered. A backend may
have committed a complete batch before returning an error or panicking; observed
metadata deliberately does not guess that result. After real original completion,
consume the finished owner and `reopen` its actual store to discover disk state,
then construct a fresh RawNode. The retired core is never reused.

## Admission Is Not Application

`DriverHandle::propose(data, context, expected_term)` validates nonempty bounded
bytes using the existing canonical `DurableProposal::decode` and matching namespace
before enqueueing, changing RawNode or proposing. It refuses malformed framing,
unsupported versions (including V11), empty/control-shaped input, foreign namespace
and oversized data/context individually. Such rejection does not enter the unstable
log, fail persistence, retire the group or block the next valid admission. It does
not inspect the catalog, apply business rules or authenticate encoded authority.
Valid but business-invalid intent remains admissible for deterministic application
refusal by a future projection owner.

Queued proposals recheck actual core leadership and the expected term at execution;
a higher-term stepdown fences old local admission capabilities. Successful
`ProposalAdmission { term, raft_index }` means local admission only. The entry may
never commit and may later be overwritten. No receipt reports committed/applied
success. `DriverStatus` is bounded latest diagnostic metadata, not readiness or
consistent-read authority, and must not be used to acknowledge client commands.

The driver never calls convenience `advance`, async advance or `advance_apply_to`.
Actual `raft_log.applied` stays zero. In pinned raft-rs, emitting committed entries
advances `commit_since_index`, an independent delivery cursor used by `has_ready`;
replication therefore continues without fictional application. Committed bytes
remain in the retained log for future application recovery. Blanks consume Raft
indices only, never application ordinals, clocks or domain sequences. Projection,
durable application receipts and client outcomes require their own separate contract.

## Bounds And Backpressure

Input credits cover queued, active and completed-but-unobserved originals. Defaults
allow 16 events/64 MiB logical bytes; supported bounds are 1..=64 events and at most
512 MiB. A receipt keeps its credit until observation or drop; dropping a caller's
receipt cannot cancel or free a queued/begun original's credit. Admission never
waits for queue space. There is one latest status, no completion history map and
no timer/retry queue.

Retention is 4..=65,536 entries and at most 1 GiB logical stored entry bytes. Its
default remains 4,096 entries/256 MiB. No application/compaction progress exists;
capacity exhaustion is a retirement/recovery condition, not permission to discard
unapplied or committed history. Peer input is separately frame/count/byte bounded
by TCP before dispatch, and only append/heartbeat/vote/pre-vote request/response
messages reach the core. Network proposals, read-index, transfer, snapshot and
configuration operations cannot bypass local admission or create pending read state.

The core allows 1..=4 inflight messages per peer (default two) and bounds uncommitted
data bytes (default 8 MiB, maximum 64 MiB). Pinned `max_size_per_msg = 0` means **at
most one entry**, not zero-byte truncation; `batch_append = false` prevents coalescing.
A legal full 1 MiB proposal plus 1 MiB entry context fits a transport frame unchanged,
including the configured maximum message context allowance. No payload splitting,
rewriting or smaller data ceiling is introduced. Retention additionally bounds
entry contexts, control entries and recovered core history beyond the core's
uncommitted-data accounting. Committed delivery uses at most one entry per page.

Ready staging and retained send receipts each have a separate 16..=128 item cap,
default 32, and logical-byte cap, default 128 MiB, maximum 1 GiB. Their combined
diagnostic count/bytes are bounded by twice those limits, in addition to one original
Ready/LightReady and bounded core state. Message charge is protobuf size plus fixed
overhead. Full codec validation uses one additional bounded temporary frame, then
TCP retains its original frame/task under its own quotas. These are logical
retention bounds, not physical RSS, disk-space or hostile allocator guarantees.

`PeerSender::try_send` Busy is a known non-admission failure. The adapter drops that
attempt and reports unreachable to the standard core using at most one flag per
configured voter per flush. It similarly reports actual local send failures after
the original receipt completes. It does not await send space or remote ACK while
holding an inbound callback credit, manufacture a vote, recursively retry, or
resubmit client proposals. Ordinary core ticks/peer responses provide later retries.

## Cancellation And Finish

`run(&mut self)` and `finish(&mut self)` borrow a retained owner. Canceling either
borrower preserves the RawNode, original pending work, transport task owners and
send receipts. Synchronous backend work runs on the calling thread with no await
inside a Ready/persistence pass; cancellation cannot detach a begun atomic write.
Callers should dedicate an appropriate owner thread when native IO blocking matters;
the adapter does not spawn an unowned blocking operation.

Retirement closes admissions before any drain. Queued inputs are refused, begun
synchronous work has already returned, and `finish` awaits the original TCP owner
and all retained send receipts through actual join. It adds no reset deadline and
does not treat transport deadline, task kill or process exit as graceful success.
`into_finished_store` refuses extraction before real finish, returning custody of
the original driver. Dropping the owner/ancestor fences admission but is explicitly
**not graceful drain**; no Drop-time join or reopen authorization is claimed.
Reopen a directory only after actual original owner/store release, including PID
join when an external process owns it.

## Fault Fixtures And Limits

`consensus-driver-testkit` adds bounded outgoing drop/hold/first-capture/replay
controls for local fixtures only: eight held/released messages capped at 64 MiB,
plus one configured-frame-bounded capture. Delivered packets still traverse the
unaltered generated protobuf codec, original TCP owner and RawNode. No test link
implements elections, rewrites replication semantics or creates voting authority.

A single fixed-size observation can record a selected successful current-term
append response after the original event has actually stepped RawNode and
persisted its Ready. It reads the core's three progress values and records whether
the preceding matching probe was rejected; it cannot seed or change progress.
Current-term commit tests use real shorter-follower rejection/backfill and hold
new-term acknowledgements, rather than infer leader knowledge from disk possession.
A separate single-use, one-permit dispatch gate can hold one genuine transport
handler after its driver work but before handler return. Retirement cannot release
that original; canceled-finish tests require Pending at this named frontier and
then release and join the same original before store extraction or reopen.
Both facilities exist only under the testkit feature and expose no writable core,
store or application authority.

The focused fixtures cover real three-process elections with distinct profile-4
stores, isolated admission without commitment, duplicate earlier responses,
old-term-majority/current-term-commit ordering, higher-term stepdown, healed
uncommitted-tail replacement, preserved committed prefix, voting freshness and
one persisted vote across original PID exits/restarts. Full-size entry contexts
are checked by length and digest against the original expected bytes on every
replica and on post-exit disk reopen, not just by replica agreement. Native and
volatile backend fault fixtures pause actual apply, inject errors/panics before or after atomic
persistence, and check that failure cannot release durable replies or advance the
failed original. Separate tests cover LightReady second-stage ambiguity, input
credit observation and canceled run/finish borrowers. Use the checked-in bounded
test launcher against exact compiled originals; expiry is failure, not drain.

Loopback identity/fingerprint checks are **not authenticated voters**, Byzantine
tolerance, three physical machines, a hardware power-cut test, performance
qualification or production security. Memory is volatile. Outer profile 4/F2,
the raft/protobuf/Fjall pins and all existing V11/configuration/snapshot refusals
remain intact. Server production guards, application projection, primary APIs,
consumer integration and final release qualification remain outside this library.
