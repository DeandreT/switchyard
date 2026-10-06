# Retained Server Connection Ownership

The AMQP engine offers an explicit public boundary for retaining the actual actor
and optional reader of one accepted connection. It uses the same fixed-role
custody implementation as the [earlier isolated connection path](server-owned-connection-tasks.md),
not a generic task registry or the separate synthetic JoinSet retirement model.

Existing `ServerConnection` acceptance methods keep their legacy launch behavior.
Ordinary, atomic and WebSocket protocol listeners, SDK fixtures, client behavior
and server startup do not automatically use the new owner. The caller must
explicitly opt in and retain the external owner and its captured live runtime.

## Split Factory And Acceptance

`ServerConnectionOwner<A>::new(runtime, anchor)` returns the owner and one
`ServerConnectionAcceptor`. The factory spawns nothing. Construct and retain the
owner before creating an acceptance future or spawning its parent.

The acceptor is non-Clone and each acceptance method consumes it. It captures
launch/reader capabilities and the captured runtime Handle, not the owner or
anchor. Its three methods preserve the existing acceptance policies:

| Method | Policy |
| --- | --- |
| `accept_with_options` | Ordinary acceptance; native transactions remain disabled. |
| `accept_with_transactional_ingress` | The existing trusted native posting policy. |
| `accept_with_transactional_work_defaults` | The existing trusted native posting/outgoing-retirement policy and narrow fresh-coordinator Attach default. |

All three take the same transport, container identifier, optional SASL
authenticator and connection options as their corresponding legacy methods.
Option validation, SASL/header/Open sequence, frame limits, write deadlines,
wire order and original error conversions remain unchanged. This does not
enable SDK transaction scopes or broaden ordinary acceptance.

Acceptance returns `Result<ScopedConnectionAcceptance<Io>, EngineError>`.
A real negotiation failure remains the original typed EngineError, including
the original I/O cause. Stop does not cancel negotiation or replace a later
original failure with an owner-stopped error.

Successful negotiation yields either `Accepted(ServerConnection)` or
`Refused(RefusedServerConnection<Io>)`. Refusal means that sealing won before the
actor claim: no connection identity, actor or reader was created by that claim.
The refusal retains the original negotiated transport. Its `into_transport`
accessor returns advanced I/O: negotiation bytes have already been exchanged.
It is not a fresh transport and does not resume or restart negotiation.
Its disposal belongs to the acceptance caller/parent, not the socket-task owner.

## Installation And Sticky Stop

The owner retains two fixed role cells, sticky stop state, the captured runtime
Handle and the caller's anchor. Private test observations and controls remain
absent in normal dependency builds; the native report observations below are
separate data-only public views.

Actor claim serializes against sealing. An accepted claim retains its no-await
installation obligation even if stop wins immediately afterward. The original
actual actor token is rooted before synchronous launch returns. Stop before
lifecycle binding remains sticky and reaches the eventual cancellation sender.
The actor exit guard is captured before spawn, including queued unpolled task
destruction; identity retirement and exit notification are still only observations.

The actor holds the reader registration/view capability, never its own handle
cell or the whole owner. Reader claim likewise serializes against sealing.
An accepted reader claim installs its original token even when stop arrives
between claim and spawn. The reader future does not own its handle cell.
There is no self-join, owner cycle, cleanup supervisor or automatic rescuer.

`owner.stop()` seals future actor/reader claims and requests existing lifecycle
cancellation when bound. It does not await completion, cancel an outstanding
negotiation, add a new handshake select, or introduce a hard actor-abort policy.
An already-accepted installation remains an obligation rather than invented absence.

## Actual Join Report

`owner.finish()` borrows the retained owner. It seals claims, requests existing
cancellation and actually joins the original actor token first. Only after that
barrier does it externally acquire and abort/join an unfinished original reader.
The actor's existing normal reader epilogue remains in its original position;
its restoring reader loan retains the same pending token or original Ready
result if actor cancellation/unwind interrupts that await.

Actor-first external order eliminates the reader's creator and competing
borrower before reader acquisition. Every created role must actually join;
a genuinely never-created role is represented by `None`, not fabricated `Ok(())`.
Exit notification, identity retirement, body return and abort request are not
substitutes for retaining and joining the original token.

Borrowed finish uses restoring actual-handle leases. Cancellation, timeout or
unwind of that waiter restores its pending handles and original observed results
to the still-retained owner. Drive it again on the captured live runtime.
Observation deadlines must borrow finish rather than consume a handle.
There is no first-error early return, termination deadline or promise that
uncooperative actor work/destructors can finish.

The first completed finish returns `Some(ServerConnectionJoinReport<A>)` only
after all created actor/reader barriers. Subsequent completed calls return inert
`None`: no new report, fresh I/O observation or runtime-health proof.

The report's `actor()` and `reader()` return `Option<&Result<(), JoinError>>`;
`anchor()` borrows the anchor. `into_parts()` moves the results into named
`ServerConnectionTaskJoins` fields `actor` and `reader`, alongside the anchor.
Those fields contain owned `Option<Result<(), JoinError>>`; the public
`observations` field moves the complete native observation carrier with them.
The tuple remains `(ServerConnectionTaskJoins, A)`. These named parts are plain
raw data, not a forgeable completion report or a new authority token.

Original JoinError objects and their panic payloads remain retained until both
covered barriers. The actual tasks return unit; actor I/O failures already
handled internally are not newly surfaced as join failures or classified by
cause guessing. Owner, acceptor, acceptance outcome, refusal, report and named
parts use opaque Debug output. Formatting does not inspect or render the
transport, anchor, raw error source or task panic payload.

Post-report raw-result or anchor destruction can still panic. It cannot strand
an unfinished covered sibling because all created covered roles already joined.
It does not imply that unrelated descendants or native work have completed.

Public `ServerConnection::shutdown` is unchanged: it requests driver cancellation
and observes the actor's exit notification, not an actual actor-task join.
The owner's separate borrowed finish is the actual socket-token barrier.

## Native Close And Abort Observations

`report.observations()` borrows `ServerConnectionObservations`. Its optional
`peer_close()` retains the original decoded Close, actual channel and payload,
native connection identity and whether the actor was already locally closing.
The retained Close, its error and payload are not cloned or normalized.
`reply_state()` distinguishes NotRequired, Pending, Ready and
AbandonedBeforeReady; `reply_result()` borrows
the original optional reply-write `io::Result`. Ready is installed inside the
original write future's Ready poll, before that completed future can drop.
Received-only or abandoned work is not a successful write. Local acknowledgment
needs no second reply and supplies no invented write result.

Optional `actor()` and `reader()` observations expose `id()`, `abort_requested()`
and `requested_by(source)` data for their original tokens. Its two sources,
ActorReaderShutdown and OwnerFinish, record requests immediately before the
corresponding original Reader abort;
cached joins receive no retroactive facts. A request can race already-completed
work and establishes neither cancellation cause nor a general Close-call order.

The new public `ServerConnectionTaskJoins.observations` field is a source break
for external exhaustive struct literals or patterns. Constructors remain private
on the report and observation carriers, and their Debug remains opaque. Tuple
shapes, original raw errors, legacy launch and abort mechanics are unchanged.
These finite observations are not source health, native-resource completion or
safe-reopen authority. The separate [retained SDK ingress](retained-atomic-sdk-ingress.md)
uses an explicit consumer policy; no default listener or SDK activation follows.

## External Root, Runtime And Anchor

The anchor need not be Send or 'static. It stays in the external owner/report
and never enters an actor, reader or acceptance future. Holding it through the
covered barriers does not establish that a Broker, provider, store, certificate
or other object has no uncovered users.

Retain the owner and runtime A itself, and drive finish on A until it completes.
Cloning a Tokio Handle does not keep the Runtime alive. A current-thread A must
also be actively driven for its I/O and timers. Acceptance/status work may run
on a distinct B while A stays live; actual socket-task ownership stays on A.
That is custody support, not a promise that I/O bound to a destroyed B continues
to function.

Dropping an unfinished owner only requests sticky stop; Drop cannot await.
Stored tokens can detach, so premature external-root loss, root unwinding
without retained custody, A shutdown, process/OOM abort and uncooperative
work/destructors are unsupported. There is no global quarantine, leak slot,
dedicated runtime, recursive rescue or autonomous final-owner cleanup.

An accepting parent and its output, refused transport or negotiation future
remain outside the pair report. The caller must retain and actually join that
parent separately, preserving its original errors/panic payloads until the
appropriate barriers. An empty pair report does not certify an unpolled
acceptance future's or its transport's destructor.

## Evidence And Limits

The public-boundary tests cover factory/anchor behavior, cancellation
before/during negotiation, original typed errors after sealing, advanced
transport refusal, exact wire bytes, all three coordinator admission policies,
signal observations versus actual joins, cancellation-restoring finish, opaque
formatting, separately retained parent/host results and distinct-runtime B loss
while the external owner and A remain live. Compile-fail examples cover
non-Clone acceptance, private controls and non-forgeable report fields.

The tests retain the setup/observation outcomes actually observed and the
particular injected original payloads until released controlled gates and all
created covered joins. They do not establish custody of arbitrary or unobserved
temporary values inside timeout-wrapped joined setup futures. Controlled
payload formatting/counters and separately retained parent/host panic-on-Drop
disposal are not universal destructor-panic coverage. After-barrier disposal
tests cannot prove cleanup after loss of A or the root.

The pair report does not retire accepting wrappers, protocol session/link tasks,
listeners, atomic collectors/workers, broker/native jobs or fixture resources.
It is not a safe-reopen receipt, whole-run coverage statement, new SDK gate or
explanation/fix for the historical provisional-Complete timeout in
[client diagnostic evidence](atomic-sdk-evidence.md). Existing fixture stop
windows still bound observation, not guaranteed physical completion.
Protocol wrapper and finite descendant ownership require separate design,
source review and actual verification; none is activated by this public API.

The separate [retained protocol connection](retained-protocol-connection.md)
explicitly adds one accepted socket's wrapper and original protocol outcomes.
It uses this pair's joins without retiring sessions, links or fixture resources.

## Verification

The public source passed all 38 focused regular tests, including the 23 inherited
isolated-path cases, and three external compile-fail examples. Ten additional
complete focused runs passed 380 regular cases. The all-feature engine suite
passed 840 tests across three targets; the protocol suite passed 380 across four.
Both default and all-feature workspace runs passed 4,643 tests across 131 targets,
with ten existing ignored tests in each run.

The first broad lint check rejected two test-only explicit drops of an empty
non-Drop anchor. Keeping that anchor in a named unused binding removed those
redundant calls without changing production behavior. The next all-feature
workspace attempt failed the existing live durable-partition test with
`Unknown(LeadershipChanged)`; its cluster library had 497 passes and one failure.
The unchanged test then passed ten serial isolated runs, followed by the complete
fresh verification group above. No partition assertions, deadlines, retry policy
or production error classification changed. Its bare error does not identify
the failed scenario stage or establish a root cause; passing reruns do not
explain or fix that retained failure.

Formatting, strict all-target workspace linting in both feature configurations,
both workspace builds, the administration protocol descriptor and whitespace
checks passed. Source hashes remained unchanged throughout the final group.
Initial lint and workspace failure logs were retained. No live SDK gate was
selected, and the earlier provisional-Complete SDK failure remains unexplained.
