# Isolated Task Retirement Model

The protocol crate contains a private `cfg(test)` model for retiring one existing
homogeneous `JoinSet<T>`. It is not a production task registry, listener adapter,
connection policy, fixture owner, or diagnostic producer. No normal-build path,
public export, dependency, existing spawn, deadline, or SDK gate changes.

The creator must retain its unique external `JoinRoot<T, A>` and drive borrowed
`finish` on the captured live fallback runtime until completion. Every disposable
starter/status observer may disappear, but premature loss of that root or runtime
is excluded. A Tokio Handle is availability, not ownership or proof of runtime
liveness. This is not autonomous cleanup after the final owner disappears.

## Finite Admission And Ownership

Admission consumes one caller-owned set and a synthetic resource anchor. Its
32-task limit is per model batch, not a connection/session/link or full-tree
bound. The factory checks the supplied fallback and count and fallibly reserves
the original-result vector before acceptance. Refusal returns the entire untouched
set and anchor; the caller still has to join every member.

Acceptance roots the complete set and result storage without spawning, aborting,
or joining. The external anchor need not be Send. Child results must be Send and
static, but their payload size is not bounded by the task count. Tokio, Arc, task,
payload, and allocator bookkeeping are not aggregate RSS or universal OOM bounds.
The tests do not inject allocator exhaustion.

A starter's first poll or unpolled Drop, or the root's borrowed finish, can submit
the single Original worker on the captured fallback. A guarded no-await handoff
installs its actual handle before submission returns. The worker owns only a
whole-job lease, never its own handle slots or the external root. Creator and
alternate-runtime host handles remain outside this graph. There is no completion
task, self-join, ownership cycle, global slot, leak, or dedicated runtime.

## Actual Join Barrier

The whole-job lease holds the entire child set and every original result already
collected. It aborts members without removing them, then drains every `join_next`
result. Returned T values and child JoinError objects go directly into reserved
storage before a model checkpoint or callback can run. There is no first-error
return, arbitrary result formatting, panic-payload inspection, or child respawn.

Cancellation or unwind restores the complete job. Borrowed supervisor waiting
uses a guarded actual-handle lease, restoring Pending handles and observed Ready
results to the same fixed slots. Polls, wakes, and injected checkpoints run outside
custody locks; poison recovery moves the same fixed fields without dropping user
outputs under a lock. No owned join handle is moved into a timeout.

Only after Original actually joins may an incomplete job start one Rescue worker
on the same fallback. Only after Rescue actually joins may the retained external
root drain the same job inline. There is no third worker or recursive rescue.
Inline execution is wherever the external root is polled; driver-on-fallback is
an explicit private caller obligation, not automatic API rebinding. Dropping or
unwinding borrowed inline finish restores its job to the still-live root, which
must continue driving it. Repeated root failure is not guaranteed away.

The report requires an actually empty child set, every created Original/Rescue
handle actually joined with its original result retained, and no outstanding
lease. It transfers the untouched child/supervisor results and synthetic anchor
only after all those barriers. Arbitrary result or panic-payload destruction can
still panic when the report is dropped, but cannot then lose an unfinished sibling
or supervisor. Sealed slots refuse a stale starter; a second finish does not
produce another report or prove current health.

Body-exit, child-drained, and status signals do not establish an actual task join.
Joining a Tokio task does not establish termination of a runtime OS worker or the
creator. Process/OOM abort, fallback/root loss, and an uncooperative task or
destructor remain exclusions; a retained incomplete join is not completed cleanup.

## Focused Evidence

The 20 focused tests exercise empty/exact-capacity admission, untouched
over-capacity/missing-fallback refusal with caller cleanup, a non-Send external
anchor, unpolled starter Drop, all-observer loss, borrowed-handle waiting loss,
custody-lock poison, and actual joins following controlled worker interruption.

Blocking final-destructor gates keep child and supervisor handles unready after
their markers. Collected panic-on-Drop T values and child JoinError panic payloads
survive both supervisor interruptions while a sibling remains unjoined; report
disposal is caught only after all actual barriers. Mixed return/panic/cancellation
preserves original values and typed panic identity.

Two tests destroy a distinct observer runtime before Original first poll or while
cleanup is gated. The captured fallback and external root stay live; original
children and supervisor, the observer task, and its separate OS host are joined.
Three multithreaded tests use two workers; the others use a current-thread runtime.
Every controlled gate is released and cleanup driven before observation/setup
errors or result assertions are propagated.

Supervisor interruptions here are controlled first-polled panic checkpoints,
not actual queued Original/Rescue abort experiments. Observer-runtime death does
not supply that missing evidence. These tests are not arbitrary engine-failure,
allocator, process-crash, power-loss, live SDK, or whole-fixture evidence.

## Still Separate

The current server still has detached connection/session/link/actor descendants,
signal-only actor shutdown, and fixture stop windows that bound observation, not
guaranteed physical completion. This model does not retain Broker, provider,
store, or certificates and does not make incomplete fixture stop safe for reopen.
No cleanup timeout, error precedence, native-owner health, or whole-run coverage
claim changes. The [typed recorder and test writer observations](server-diagnostics.md)
remain separate; this model publishes no trace rows.
The separate [actual connection pair](server-owned-connection-tasks.md) retains
the original socket-task handles privately; it does not activate fixture cleanup.

The historical provisional-Complete SDK timeout remains unexplained as recorded
in [client diagnostic evidence](atomic-sdk-evidence.md). Whole-resource custody
and actual listener/engine integration require separate ownership and verification.

## Verification

The final source passed all 20 focused tests and ten additional repeated runs
(200 passes). The complete all-feature protocol crate passed 380 tests across
four targets, with none ignored. The normal default-feature workspace passed
4,556 tests across 131 targets, with ten existing tests ignored.

Formatting, strict all-target workspace linting with both default and all
features, both workspace builds, the administrative protocol descriptor, and
diff checks passed. Source hashes remained unchanged throughout verification.
No all-feature workspace test run or live SDK gate was selected for this private,
unconditional test model; neither fixture activation nor a queued-supervisor
abort result is implied by these checks.
