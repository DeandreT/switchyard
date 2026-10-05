# Sealed Local Compaction

`ExperimentalCompactionLogStore` and `ExperimentalLocalCompactionPair` provide
explicit quiescent local compaction beside the existing native catalog API.
They are standalone owning facades, not replication-library storage adapters.
No server startup, engine snapshot method, runtime configuration, network,
preflight, ordinary log profile, or production policy changes.

## Disjoint Log Role

The log uses the existing trusted `CommittedStore` atomic batch contract and
backend format, with the separate logical role `queue-log-local-compaction-v1`.
Creation requires authoritative uninitialized and empty storage. One commit
publishes initialized state and mandatory profile, progress, and baseline records
at keys 1, 2, and 3. Open refuses old roles, missing/partial records, unknown keys,
and inconsistent bookkeeping; it does not migrate, adopt, or repair storage.

The canonical `SWLF` baseline is schema 1/role 1, length-framed, postcard encoded,
SHA-256 checked, and bounded to 16 KiB. It is either empty with ordinal zero or
captured with a nonzero ordinal, exact full boundary, stream/node identity, and
the original at-most-8-KiB native metadata. Its boundary equals persisted purge
progress. An empty retained log still includes that boundary in vote coverage.

Before preparation, borrowed methods support bounded append, vote, and read
work. The limited reader uses an inclusive upper index, including `u64::MAX`,
and returns at most 32 entries/4 MiB. Complete retained history remains bounded
to 256 entries/64 MiB; creation and open do not grant arbitrary purge authority.

## Sealed Preparation

`ExperimentalLocalCompactionPair::prepare` consumes that unique log facade and
one explicitly catalog-enabled native state facade. Without a current Tokio
runtime, synchronous admission returns both sources unchanged in
`LocalCompactionAdmissionError`. Accepted preparation synchronously arms both
retirement tokens without querying, capturing, or writing either source.

First poll uses the current runtime for work. The runtime captured at factory
creation is the stable cleanup fallback, not replacement execution authority.
Dropping an accepted unpolled preparation schedules whole-resource cleanup on
that fallback. Accepted first poll remains rescue-armed even before its worker
polls. An absent current runtime starts no new source operation and cleans up on
the fallback. Failure of that required fallback itself is outside the guarantee.

Preparation seals the state owner's FIFO loop with an explicit allowlist.
Read-only operations and exactly tagged private capture remain allowed; generic
apply, catalog build, an older standalone snapshot builder, and future generic
mutation variants are refused. No apply, append, raw writable engine, public
owner handle, or reusable deletion permit escapes the pair.

Proof folds complete bounded retained history from genesis or the fully checked
baseline. It checks exact succession, full identities, chained fingerprints,
canonical membership/source/schema/payload, and the refused-command timestamp
watermark against the complete current checkpoint. The private native summary
uses the same recovery checks as the state adapter, not framing alone.
Older catalogs, catalogs ahead without a bridge, and identical checkpoints with
different artifact digests refuse. This is not independent business-row replay,
source authentication, quorum evidence, or remote-image ancestry.

## Known Retention Before Deletion

`compact` is inert until first poll. A tagged state job performs one bounded
capture through the existing catalog path. It prepares native metadata,
projection, and the small receipt carrier before the sole catalog commit.
Only known-successful retention publishes a private generation/attempt receipt;
no postcommit artifact copy or source query is added.

Both permit preparation and the final destructive operation reconstruct the
checked receipt and full history fold from fresh authoritative rows. The final
bounded batch is prepared before a one-shot frontier claim. No I/O, await, or
callback runs under that short claim lock. Terminal-before-claim refuses; a
claim-before-terminal may finish and is neither canceled nor rolled back.

One log commit deletes exactly the covered prefix and atomically publishes the
incremented baseline ordinal, full progress, and initialized state. Cache updates
occur only after success. Every physical commit error is unknown and fatal,
including an error returned after the complete batch is durable. No successful
receipt, rollback assumption, automatic retry, or cross-directory atomicity is
claimed. A retained catalog can legitimately survive a failed log compaction.

Permitted state reads that poison the owner terminal-mark the frontier before
reply publication. Dispatch panic marks it before active-packet unwind and again
before queued cleanup. Attempt or ordinal exhaustion has a distinct terminal
`Exhausted` error, never wrapping or becoming an ordinary retryable quota refusal.
Ordinary size/allocation/semantic refusals remain nonfatal.

An exact already-compacted observation is cached, including after validated
reopen. It requires no additional capture, catalog retention, or log write, but
is not a fresh source-health certification.

## Custody And Limits

The pair admits one accepted job; competing work returns `Busy` without native
work. Losing a waiter does not cancel accepted capture, retention, or a claimed
log commit, nor release its capacity before completion. `shutdown` synchronously
closes destructive admission and returns an owned completion future.

Cleanup drops both facades and explicitly starts both retirement joins on the
captured stable fallback before awaiting either. Both actual native joins are
attempted even if one fails. Rescue retains their original handles, completion,
and refund obligations if the distinct worker runtime stops before its first
poll or during cleanup. Process/allocator termination and fallback shutdown are
excluded; no forced termination or backend-I/O timeout is introduced.

State capture reserves the existing 64-MiB owner budget, with at most 8 KiB
encoded metadata overhead. Log admission retains its existing finite quotas.
Backend scan materialization, staging, ordinary collection/Box/channel
allocation, allocator spare capacity, caller-owned results, and aggregate RSS
are outside those charges. These are not universal OOM or full-capacity-success
guarantees.

The pair remains quiescent throughout its lifetime. A second different cycle
requires joined shutdown, release of all physical handles, actual reopen,
explicit standalone suffix application, and preparation of a fresh generation.
Remote replacement requires separate durable adoption policy; this API does not
use populated image replacement or activate engine installation/compaction.

## Verification

The final focused run passed 57 local-compaction tests, five native-summary
tests, and two compile-fail documentation tests. Ten consecutive repetitions
passed all 570 local-compaction checks. Each repetition included four actual
child-process exits before a commit call or after completed `SyncAll`, for 40
such boundaries. These are not mid-sync or power-loss tests.

Coverage includes full-identity and fresh-history proof, unknown outcomes,
terminal/claim ordering, caller loss, stale builders, role and codec rejection,
finite exhaustion, two separate compaction cycles, and actual Fjall reopens after
all physical handles are released. Separate runtime tests stop a distinct
execution runtime before worker polling and during cleanup while the captured
fallback remains live; both native joins must finish before reopening.

The final cluster suite passed 650 checks. Default and all-feature workspace
runs each passed 4,444 checks with ten opt-in SDK checks ignored. Formatting,
strict workspace linting in both configurations, both workspace builds, the
administrative protocol descriptor, and patch whitespace checks passed.
Initial execution found missing test imports and lint issues; these were
corrected before the complete final focused and broad runs reported here.

No live SDK gate was rerun for this increment. This evidence does not establish
engine activation, cross-directory atomicity, a successful full-capacity
64-MiB capture, heap-exhaustion behavior, universal allocation safety, or
termination of the required cleanup fallback.
