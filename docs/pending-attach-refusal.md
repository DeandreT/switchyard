# Pending Attach Refusal

`ServerSession::reject_attach(IncomingAttach, Error)` refuses an original pending
ordinary link without installing it in the session's active link map or granting
link credit. It uses the received admission receipt, not a raw wire `Attach`,
and applies to both peer roles.

## Receipt And Wire Contract

The public call validates receipt ownership and ordinary attach kind before
queueing. The actor rechecks the current session generation, original pending
approval, local handle, peer handle, name and role. Foreign receipts, changed
identity fields, replaced approvals and accepted or retired receipts do not
authorize a new refusal. Recovery and transactional attaches retain their
existing refusal paths.

The normal wire sequence is the original approval's minimal null-terminus
`Attach`, followed by a closed `Detach` carrying the supplied error. Both source
and target are null. A sender-role response supplies its required initial
delivery count; no permissive negotiated defaults or ordinary endpoint are
installed. Existing local Begin publication is preserved when needed.

`Ok(())` reports completed refusal writes and flushes, not acknowledgment by the
peer. Frame-size, history-capacity and transport failures retain their existing
session or connection failure paths; the pair is not an unconditional guarantee
under those failures.

Server retirement deliberately uses the existing closing-handle machinery.
It removes and retires the pending approval immediately. The alias and closing
handle remain owned until the real peer acknowledgment or existing session or
connection cleanup. Error-owned name, peer-handle and delivery histories keep
their original rules and can outlive that acknowledgment. Errant Flow, Transfer,
Attach and Disposition traffic is not made permissive.

## Native Client Contract

The feature-gated `test-client` native helpers recognize a peer sender's null
source or a peer receiver's null target as a refusal response. The optional
opposite terminus alone is not a refusal. Positive attach negotiation, source
filter echo, session routing and ordinary credit behavior remain unchanged.

A valid null-operative-terminus response is parked on the original directional
pending request. Its response and peer alias are recorded, but its existing
pending link state is not installed as an active endpoint. Pending early Flow
is not applied to an ordinary link. No link-scoped Flow, refill or Transfer is
emitted for that refused link.

The native client keeps the original pending identity, directional name and
alias until a correctly associated peer `Detach`. It stops and drops the pending
link state, closes its original delivery channel, flushes the real Detach
acknowledgment and removes the alias before completing the attach waiter.
The existing attach-then-detach contract returns `Ok` with the actual echoed
response and an already-retired handle, not a live or authorized link.
Sending or receiving on that handle reports `RemoteDetached`; closing the
terminal handle is harmless. No fabricated active endpoint row is introduced.

A missing Detach does not manufacture completion. Wrong-channel or wrong-handle
traffic follows the existing routing and session/connection refusal rules; it
cannot stand in for the original peer acknowledgment. Session or connection
retirement fails unfinished attach waiters rather than inventing a successful
refusal exchange.

Role, sender delivery-count, buffered count consistency, recovery and
transaction/default-outcome checks still precede parking. A Source default of
nonterminal `Received` is invalid even beside a null operative terminus.
The ordinary codec rejects it both before encoding and while decoding.
A test-only structured-value writer injects those malformed bytes into the
real reader. That reader stops the connection, the original pending caller
fails and the peer observes EOF. This ordinary InvalidData path does not emit
an actor End or framing-error Close.

## Protocol Planning

Ordinary entity and management planning errors call the pending-refusal API
before positive acceptance. A denied link has no accepted data endpoint or
data worker and receives no ordinary credit. Authorization denial still
precedes topology access. Later binding or session-planning errors can retain
their existing broker read or command effects; wire refusal does not roll
those back.

Successful plans keep their existing fenced admission, session-filter echo
and worker behavior. CBS routing, authentication policy, transaction ingress
and post-acceptance detach handling are not replaced. A pending denial does
not itself end a valid session or change a later CBS grant.

## Ownership And Limits

An unpolled refusal future enqueues nothing. Once queued, canceling its waiter
does not undo the actor-owned refusal or peer-acknowledgment ownership.
Canceling a native client's attach waiter likewise leaves the original pending
request owned until Detach or ordinary session/connection cleanup.

No per-link Detach deadline is added. A live peer can retain pending ownership
while withholding its acknowledgment, subject to the existing limits. These
include 32 pending attaches per session and the unchanged frame, value, link,
closing-handle and error-history bounds. Pending response data uses the
existing owned request and codec representations, not an unbounded new registry.
These are logical protocol limits, not a total allocator-capacity or RSS bound.

The new cases use bounded controlled transports for refusal publication,
acknowledgment, cancellation, malformed input and continued authorized traffic.
State-only cases create no actor or reader. Native client fixtures retain the
original actor and caller outcomes; cooperative actor return includes its
existing reader shutdown, not a separate retained reader report. Server and
protocol socket fixtures retain original acceptance and actor/reader reports.
Direct protocol workflows retain the original workers they start and observe
the exact CBS and data-link replenishment before later denial. Their observation
cleanup releases gates and joins retained outcomes before rethrowing a panic.

Those fixture boundaries do not establish custody of every descendant of a
production listener, native whole-node health or SDK cleanup. This change adds
no SDK compatibility receipt, durable ownership, storage format, migration,
replication or production-startup authority.

## Verification

The completed default and all-features workspace suites each passed 5,060 tests
with ten unchanged ignored cases. Against the preceding checkpoint, all 5,036
passing cases and ten ignored identities/reasons retain exact per-harness order.
The only additions are eight server refusal cases, seven native-client cases,
seven protocol cases, one compile-fail example and one compiled no-run example.
The no-run example was compiled, not executed.

Ten serial paired AMQP/protocol focus runs passed all 22 new regular cases each
time (220 case executions). Complete AMQP and protocol suites passed 857 and 521
checks, respectively; domain, storage and cluster registries remained unchanged.
Default and all-features workspace builds and strict Clippy passed, as did
formatting, protobuf compilation and diff checks. The generated descriptor
remained byte-identical. All Rust gates used two low-priority CPU cores and the
same shared build cache.

Initial failed checks remain retained separately: formatter module paths, an
old test initializer for the new pending-response field, malformed-value test
injection, two fixtures that had not consumed legitimate refill Flow, and two
fixture lint findings. Corrections were followed by complete successful gates;
no production codec or CBS behavior was relaxed to make those fixtures pass.
A mistaken package-selection invocation failed before any Rust test ran and
was replaced by the actual workspace AMQP package command.

Independent source, documentation and closed-log reviews preserved prior test
bodies and API fences, except for the one required existing initializer field
addition. Four updated modules retain exact complete inverses of their scoped
changes, and the other 39 protected repository files remain unchanged. The
outside draft's captured UNEXECUTED statuses were not rewritten into test
receipts. No fresh SDK suite was run, and earlier authentication or SDK failure
causes are not resolved by these passing gates.
