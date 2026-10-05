# Retained Protocol Connection

The protocol listener's explicit retained path covers one already accepted
socket. It adds the protocol wrapper to the AMQP engine's
[connection owner](server-connection-owner.md), retaining original outcomes and
the actual task tokens for exactly Wrapper, Actor and Reader. It does not change
the ordinary listener, semaphore, ingress defaults or SDK fixtures.

## Construct Before Start

`RetainedConnectionOwner<A>::new(runtime, anchor)` returns a unique external
owner and a consuming `RetainedConnectionStarter`. The root, wrapper slot,
engine owner and two outcome cells exist before any connection task is spawned.
The anchor need not be `Send` or `'static`; it never enters a task.

Retain the owner and the captured live runtime separately. A runtime `Handle`
does not keep its runtime alive. Drive borrowed finish on that same live runtime;
a current-thread runtime also needs its timers and I/O driven.

The configured `AmqpListener` consumes itself, an already accepted `TcpStream`
and the starter through exactly one explicit entrypoint:

| Entrypoint | Existing Ingress Policy |
| --- | --- |
| `start_retained_connection` | Ordinary connection driver |
| `start_retained_atomic_posting_ingress` | Existing posting ingress |
| `start_retained_atomic_messaging_ingress` | Existing messaging ingress |

The caller owns socket acceptance and admission across roots. These methods do
not apply `max_connections` across independently constructed owners. There is no
automatic activation from `serve`, a semaphore path or an SDK fixture.

Setup validates the existing connection options, sets `TCP_NODELAY` and computes
one checked absolute handshake deadline, in that order, before launch refusal.
A setup failure keeps its original `io::Error`, including its typed cause.
`RetainedConnectionStartError` returns that cause, or `Stopped`, and the original
configured listener/socket through `into_parts`. These caller-owned requests are
not included in the owner's created-task report. `TCP_NODELAY` may already be set.

The returned request occupies one private heap allocation. `into_parts` moves
out the original listener and socket without cloning. This allocation is not a
universal fallible-allocation or OOM guarantee.

Claim, original spawn and token installation have no asynchronous gap. The
wrapper captures its transport, configuration, driver, consuming engine acceptor
and data publishers, never its own task token, root or anchor. Publisher cells
contain data only, not runtime, task, transport or custody capabilities.

## Original Outcomes

`RetainedConnectionOutcomes` keeps independently optional primary and WebSocket
close values. Missing data means unobserved, not success or a guessed cause.
The primary is one of these dispositions:

| Disposition | Meaning |
| --- | --- |
| `SkippedExpiredDeadline` | Existing first-poll deadline short-circuit; no handshake error was constructed |
| `LaunchRefused` | Negotiation completed, but the engine launch was sealed; advanced I/O is not restored |
| `Finished(result)` | Original result returned by the handshake, driver or timeout boundary |

The original boxed primary error and independently returned WebSocket close
error remain separate. No `result.and(closed)` discards one on this path.
Wrapper, Actor and Reader each retain their own original `JoinError`, including
panic payloads, separately from these protocol outcomes.

Publication happens synchronously inside the actual `Ready` observation before
any later await, callback, completed-future Drop or advanced refused-I/O Drop.
The driver result is therefore already rooted when shutdown first becomes
pending. A close result is rooted before a later wrapper callback can unwind.
This is custody of observed values, not every temporary hidden inside a provider.

The existing absolute TLS, HTTP upgrade and AMQP Open/SASL deadline is preserved,
including inner-first timeout polling and strict deadline checks. Authentication
remains inside the timed Open path; CBS authorization retains its existing
separate timer and grace behavior. Transport adapters can already erase an
underlying error before returning; this path preserves the returned boundary
object, not an earlier cause that no longer exists.

Post-Open expiry retains the existing order: await signal-only engine shutdown,
then construct the timeout error. Aborting during that await legitimately leaves
primary absent because no timeout error has yet been produced. WebSocket close
still runs after an observed primary error using its existing close deadline,
with no additional pump task. Existing default result precedence and logging
remain unchanged outside the explicit retained path.

## Actual Joins

`stop()` seals wrapper and engine launch claims and forwards existing engine
cancellation. It is cooperative: no new production wrapper/Actor abort policy or
hard termination bound is added. Sealing does not turn pending negotiation into
an immediate cancellation acknowledgment.

`finish()` borrows the owner, stops it and awaits the actual created task tokens
in order: Wrapper, then Actor, then Reader. An absent role is not invented. Only
after every created covered role actually joins can original protocol outcomes,
task results and anchor leave the root in `RetainedConnectionJoinReport<A>`.
Canceling or unwinding a borrowed finish restores its pending token or already
observed original task result; retain the root and drive finish again. Later
cached calls return `None`, not a new health or physical-state check.

Report fields are private. Borrowed task, outcome and anchor accessors permit
inspection; `into_parts` returns all original results and anchor after the joins.
Root, report, requests and outcome diagnostics are redacted without formatting
raw errors, payloads or anchors. Deliberate access to those originals is separate
from wrapper formatting. Post-report disposal can still panic.

## Exclusions

Exactly three covered roles are not the whole connection's descendant tree.
Socket acceptance, listener/session/link tasks, Broker/provider jobs, native
workers, certificates and fixture resources need their own external custody and
actual joins. A report certifies neither source health, physical termination nor
safe reopen, and cannot explain an internally handled engine I/O failure.

Dropping an unfinished owner cannot await and may detach covered work. Root or
runtime loss, uncooperative tasks/destructors, process abort and OOM remain
unsupported. There is no automatic rescue, fixture cleanup, adopted storage
authority or historical SDK-timeout diagnosis. Tests of the explicit path do not
activate or replace live SDK lifecycle gates.

Panicking `tokio_unstable` task-spawn hooks are unsupported: a token that Tokio
never returns cannot be retained or joined by this owner. No completion-report
guarantee is claimed for that configuration.

## Verification

The final focused suite passed 41 regular tests and three compile-fail examples.
Ten unchanged serial repetitions passed 410 regular tests. The final serial
verification group completed all 21 checks: formatting, strict workspace lint
in default and all-feature configurations, the ten repetitions, all-feature
engine tests (840 passed), all-feature protocol tests (424 passed), both full
workspace suites (4,746 passed across 132 result groups, with 10 existing ignored
tests each), both workspace builds, protobuf validation and whitespace checks.
All 18 implementation digests remained unchanged through final verification.

Earlier failed attempts remain retained separately. Initial compilation produced
17 lifetime diagnostics before execution; the correction added the missing
`'static` trait-object bound to one borrowed test-error view, without making its
reference static. The revised focused binary recorded five failures and two
passes before double-panic disposal aborted it; it produced no completed-suite
count. Unchanged diagnostics separately recorded one failure and then two passes
with five failures. Their common assertion required Reader success, although
the existing engine epilogue aborts and actually joins that reader. Tests now
accept its original success or cancelled result, never a missing or panic result.
Fault-bearing holders are catch-disposed after all covered joins and before
assertions. These were test-only corrections, not an engine policy change.

The first broad lint attempt reported four oversized returned-error diagnostics.
The narrow production correction boxes the original refused/setup request
privately, preserving public signatures, object identities and successful paths.
The complete focused and 21-check groups above were rerun after that correction.

Actual socket and WebSocket pending-close cases are separate from helper-only
Ready-capability cases. TLS refusal/deadline fixtures use an empty certificate
resolver and establish pre-certificate behavior, not successful TLS identity.
Original primary/close retention uses counters; deliberately panicking Drop
payloads belong to wrapper and OS-host results and are disposed after the joins.

Compilation used two cores at low priority, tests ran serially, and disk and
memory headroom were checked before and after substantial builds. The existing
shared build cache was reused; no additional build cache was created.

No live SDK gate ran for this increment. Earlier SDK and cluster failures remain
unexplained; these green checks neither explain nor repair them. Covered joins
and observed outcomes are not whole-descendant, native-resource or safe-reopen
authority.
