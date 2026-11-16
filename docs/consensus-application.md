# Committed Consensus Application

`cluster::consensus::driver::application::ApplicationOwner` is the explicit
application mode of the [persisted driver](consensus-driver.md). It reuses the
same private RawNode/TCP event owner with a sealed application policy. One
exclusive physical `ConsensusBackend` holds F2 consensus, F0 journal and ordinary
domain/F1 state. Private capability views share that owner; no public backend,
StateStore, writer, generic reader, callback or applied-index setter escapes.

This library does not provide primary routing, client authentication, automatic
mutation retry, consumers, consistent reads or production activation. Ordinary
`ConsensusDriver` still reports actual applied zero and its proposal receipts
still mean local admission only. Ordinary `ConsensusStore::open` still refuses
mixed images.

## Construction And Recovery

`ApplicationOwner::bind` validates limits, identity and endpoint before calling
the backend factory. Binding does not serve peer work. Before writable
application views or admission are exposed, the owner validates the entire
physical image: ordered bounded records; canonical F2 identity/history; unanchored
F0 V1 structure and exact tail; the ordinary F1 V1 owner/checkpoint pair; domain
catalog, counters, indexes, message content, namespaces and Clock coherence.
`CommittedReplay::open` is used only for read-only startup validation, not live
dispatch. A consuming private token binds the validated image and derived
progress to that same physical owner.

Every appended F0 payload, including its uncommitted structural tail, must equal
the corresponding committed F2 proposal byte-for-byte. A conflict, malformed
committed tail, unknown record, missing companion or frontier ahead of authority
refuses startup before any prefix effect. No repair, truncation or overwrite
makes an invalid image agree. An F2-only image can enter this mode after full
validation; local domain/F0/F1 history cannot be adopted as quorum history.

An F1 checkpoint without domain rows or Clock is valid: a first deterministic
refusal or successful no-effect command may publish only its checkpoint. A
present Clock must satisfy ordinary complete-state validation and cannot exceed
the maximum stamped time among applied originals. It need not equal the latest
original's time. Validation never reexecutes applied history to reconstruct an
outcome or invents a Clock row.

Missing work is recovered before admission. Recovery resumes after F2 commitment,
F0 append, F0 commit or the complete atomic domain/F1 write. The latest persisted
checkpoint and exact identity determine progress, not an old in-memory phase.
Recovery restores state, never historical business results.

## Durable Pipeline

Let C be persisted F2 commitment, L the F0 appended application ordinal, J its
committed ordinal, A the atomic F1 applied ordinal and P(C) the number of proposal
entries through C. The invariant is `0 <= A <= J <= L <= P(C) <= C`.

After completely persisting the original Ready and LightReady, the serialized
owner services one bounded application phase per turn:

1. Read the next committed original from F2. Blanks consume only a Raft index.
2. Append its exact `Entry.data` to F0, or verify an already-present exact copy.
3. Advance the F0 committed frontier over that exact prefix.
4. Call ordinary `IndexedWriter::apply` for the next ordinal. Domain effects and
   the F1 checkpoint share the existing single atomic batch. A real deterministic
   refusal checkpoints normally; an infrastructure/corruption error is fatal.
5. Consume a private actual-applied proof to advance RawNode. Only then can a
   matching live original's real outcome be published.

Raft index and application ordinal are distinct. Actual Raft progress R is the
longest committed prefix containing only validated blanks and proposals through
A. It includes trailing blanks but never skips an unapplied proposal. Startup
initializes the core only from privately validated R. Blanks do not call the
domain machine, create F0/F1 application rows or tick Clock.

Projection never reencodes/restamps data or recaptures authority/binding. Entry
term, index, data and independent context remain exact in F2. F0 retains only the
original proposal bytes, in its unchanged format. Existing domain digest rules
remain the authority for the latest F1 identity. `AlreadyApplied` cannot produce
a cached business result and is an invariant failure for a purported next apply.

## Live Original Results

`ApplicationHandle::submit(data, context, expected_term)` queues one original;
successful submission is not business success. Direct InvalidProposal, Busy or
Unavailable means definite non-admission. Execution also checks actual leadership,
the expected term and catch-up through persisted commitment before proposing.
NotLeader/Busy returned by the receipt before proposal acceptance is not an apply
decision.

`ApplicationReceipt::wait(&mut self)` yields the actual `Applied` or `Refused`
decision only for that retained original, after real majority-durable commitment
and actual leader apply. Publication matches term/index/data/context and requires
the original's submitted term still to be the leader's current term. Admission,
status, follower apply and local send receipts cannot substitute for those facts.

Stepdown permanently fences unpublished originals as Unknown, whether their
entries were uncommitted, replaced or already committed. Later follower apply or
leadership cannot resurrect their results. A result legally published before a
later stepdown remains a completed fact about that operation, not a leader lease.
Unknown after admission can mean the command did commit/apply: do not automatically
retry or interpret it as rollback/no effects. Status is diagnostic, not readiness
or consistent-read authority.

## Logical Bounds And Backpressure

Defaults allow a 1 GiB logical physical image, 65,536 rows, 4 KiB keys, 16 MiB
values and one live original. Up to four originals can be configured, subject to
the independent original byte budget. Construction requires at least:

- Value bytes: `MAX_CONSENSUS_ENTRY_BYTES + 40`, preserving legal independent
  1 MiB proposal/context, protobuf allowance and existing F2 record framing.
- Image bytes: twice retained F2 entry bytes, plus a full F2 framed entry, full
  F0 proposal/envelope and 16 KiB root/checkpoint headroom. This is construction
  headroom, not a guarantee that arbitrary future domain growth will fit.
- Key bytes: namespace, entity, session, four-byte-per-character rule-name
  maxima and 64 bytes for tags, separators and scalar suffixes.
- Four peer/control event slots and four full frame charges beyond the maximum
  queued application originals. Waiting business results do not hold event slots.

The public checked `reply_reservation()` charges:

`2 * image_bytes + 16 * MAX_PROPOSAL_BYTES + TIMER_SCAN_LIMIT *
MAX_TOPIC_SUBSCRIPTIONS * (MAX_ENTITY_PATH_BYTES + 32) + max_rows * 256`.

This is a conservative logical reply bound, not a heap-layout calculation:

- Delivery pages, deferred/received messages, rules and session data select
  distinct persisted rows. Twice image bytes covers retained variable data and
  normalized/content representations; row wrappers have a separate charge.
- Batch sequences and scalar-vector expansion are bounded by proposal bytes;
  sixteen bytes per encoded input byte is reserved independently of image data.
- Published/activated destination paths have bounded timer/subscription/path
  cardinalities, including dead-letter destinations. Their charge is independent
  of the small triggering timer proposal.
- Scalar outcomes/refusals fit the fixed/row/input allowances. The existing
  domain classifier, not this wrapper, decides deterministic refusal versus fatal
  preparation/storage failure.

`original_charge()` adds a full legal data/context pair and 512 bytes of identity
metadata to that reply reservation **before** enqueue/RawNode admission. Queued,
waiting-commit, active, fenced-but-unfinished and completed-unobserved phases use
one shared count/byte credit. Observation/drop releases a completed receipt;
dropping a receiver cannot refund work still held by its original owner. A
consuming same-owner reopen preserves that pool, so old unobserved receipts cannot
bypass the bound. No response history or unbounded retry map exists.

Private logical scans gather requests such as 256 or 2,001 rows through physical
pages of at most 64. Exclusive cursors preserve ordering and advance across
filtered rows; requests are neither forwarded oversized nor silently clipped.
The complete image and live atomic batches enforce checked row/byte accounting,
including overwrites, deletes and last-mutation-wins semantics. Exhaustion of a
committed image retires the composition as infrastructure failure, not a business
quota refusal. Committed history is never evicted to make room. Provisioning and
future compaction remain operational constraints.

Original domain/backend reads may allocate before logical checks. The combined
finite envelope additionally includes bounded validation facts, current stage,
core, Ready, outbox/send and transport budgets. These limits are not hostile
allocation/RSS, physical disk, hardware-power-loss or performance certification.

## Cancellation, Retirement And Finish

`run` and `finish` borrow the same retained owner. Synchronous atomic phases have
no await; a cancellation request cannot detach or interrupt a begun backend write.
Each successful phase transition is recorded before yielding. A canceled borrower
resumes the original, not a replay/retry worker.

An error or escaping panic after an atomic attempt can mean the whole batch
landed. It poisons all private views and serving, retains the actual phase/error/
panic and original credits, and prevents further core/application work or
business publication. No cached sibling frontier authorizes a retry.

`ApplicationHandle::retire` fences result publication with the same short lock
that transfers a result to its receipt, then closes driver admission. It does not
take the physical-backend lock or interrupt a begun write. If retirement wins,
that original remains Unknown even if its whole atomic batch subsequently lands;
if publication wins, the already transferred result survives retirement and
delayed observation. Unfinished work keeps its count/byte credit in either case.

Retirement closes admission before drain. Not-begun phases remain valid recovery
windows; finish does not wait for quorum or start new writes to claim drain.
`finish` joins the actual TCP tasks and send receipts without resetting their
shutdown budget. `into_closed` refuses extraction until real finish and returns
the same owner on refusal. `ClosedApplicationOwner::reopen` consumes its opaque
backend custody, validates the entire image again and derives fresh progress.
Dropping an owner, a deadline or a killed process is not graceful drain or reopen
permission. Native directory reopen requires actual original store release/PID
join. Memory remains volatile.

No new native/record/proposal format is allocated. Profile 4, F2 V1, unanchored F0
V1, ordinary F1 V1 and dependency pins are unchanged. V11/finite-owned state,
0x12/0x13, anchors, snapshots, install, upgrade and production fences stay closed.
The optional `consensus-driver-testkit` exposes only bounded pre-phase pause and
fixed latest-contextual-entry diagnostics alongside existing transport controls;
it introduces no raw reader/writer or voting authority. Trusted custom backends
must uphold atomicity, durability, ordering and exclusive-writer preconditions.
