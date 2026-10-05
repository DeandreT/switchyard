# Private Accepted-Socket Collector

The protocol crate contains a private `cfg(test)` composition of the
[retained socket owner](retained-protocol-connection.md) and the
[two-session atomic collector](retained-atomic-collector.md). It retains the
original Wrapper, Actor, Reader, two Session tasks and their shared routing
workers through two actual report barriers. This is not a public lifecycle API,
default listener activation or SDK fixture cleanup.

## Construct Before Start

The caller creates the external aggregate root and unique consuming launch
capability before any Wrapper spawn. The root owns a captured runtime handle,
the separate socket owner, two fixed restoring handoff packets and the external
anchor. The anchor may be borrowed and non-Send; it never enters a task.

Keep the root and Runtime A separately alive and drive borrowed operations on A.
A handle does not keep its runtime alive. Current-thread runtimes need their
timers and I/O driven. There is no autonomous cleanup after root/runtime loss.

Launch carries one socket starter, one consuming Open publisher and exactly two
dormant ports into the existing Wrapper. Beyond the existing Wrapper, Actor and
Reader, the composition adds no supervisor, duplicate socket task, WebSocket
pump or aggregate-owning task.

After the existing Open negotiation, the Wrapper publishes data-only connection
identity, namespace, broker, authorization and mode information. The root binds
one logical atomic Owner and event receiver, reserves both Session tickets and
arms the ports before acknowledging binding. Canceled binding closes unbound
rather than constructing another Owner. Stop before Open creates no collector.

The existing private Copy-driver adapter and the consuming experiment driver
share the same retained Wrapper body. TLS, SASL, HTTP upgrade, Open negotiation,
absolute deadline and transport shutdown behavior are not duplicated or changed.
The full production atomic driver's initial authorization grace, discovery,
MAX_SESSIONS and connection Close policy are not activated by this experiment.

## Original Handoffs

Each port discovers the actual incoming Session using a whole-packet loan.
Incoming Ready is stored before the completed discovery future or its captures
can drop. After that borrow ends, the Wrapper constructs the unchanged original
cold acceptance future and immediately arms it in the restoring packet. Private
test wrapping occurs only after the original future is stored.

A final storage claim serializes against seal. An accepted claim must finish
storage installation even if sealing follows; it does not authorize acceptance
polling or Session launch after seal. Refusal, cancellation and unwind keep the
original cold future in its packet, not a replacement error or reconstructed
future. Refunds and raw disposal occur outside custody locks.

The same two ports are never reissued. An unused Session ticket can be refunded,
but the two admission-attempt histories remain sticky. Session launches and the
connection-wide worker history also never replenish after completion. Both
Sessions reuse the existing scoped routing body and share worker limit K, with
`1 <= K <= 128`, independently of live-link permits.

The identity bound is three socket roles, two Session roles and at most K
original workers, plus two original admission histories. It is not an allocation,
payload-byte or RSS bound. Port packets contain real endpoint capabilities;
unlike the Open publication, they are not data-only observations.

## Authority And Barriers

Stop first seals claims, closes the sole logical Owner outside locks, publishes
authority closure and requests existing Session/socket stop. A seal, wake or
actor-exit notification is not an actual join or native-resource completion.
New packet/control locks only move fixed states. They do not poll, wake, format,
invoke callbacks or dispose raw endpoint/error/panic objects.

During socket finish, the same collector continues pumping closed-owner events,
WorkerStopped acknowledgments and original Session observations. It neither
applies owner operations nor ticks owner state after close. No second event-pump
task is introduced.

An observed Session join result enters the root before its Ready callback. The
callback's pending index also remains in the root across borrowed cancellation,
so resumption cannot skip its existing failure classification. It is cleared
only after that callback returns; the completed original join is never repolled.

The first barrier actually joins Wrapper, then Actor, then Reader. Its original
report is rooted directly in the Ready arm before another callback, await or
completed-waiter Drop. Wrapper join ends all remaining handoff-creator claims;
the root can then reconcile accepted storage and extract the original histories.

The second barrier finishes the same collector, joining both original Sessions
and every remaining worker while pumping their stop acknowledgments. Whole
packets and unique final rows survive borrowed cancellation or unwind. Only
after these joins does the collector close its receiver, drop the root sender,
drain buffered events through the closed Owner and tear down receiver/Owner.

The second original report is also rooted at Ready. Only after both barriers
can the aggregate's one-shot report transfer both reports, original histories,
Open context and anchor. Earlier observations remain rooted when a later
borrowed observation disappears. No uncertain operation is resubmitted.

## Evidence Boundaries

Tests use real TCP posting producer/controller links, messaging consumer/
controller links, and a WebSocket connection with two Begin handshakes. Existing
registration and stop acknowledgment paths use one logical Owner. The third
Begin check is a finite bounded no-echo observation, not an invented limit error
or proof of an unlimited refusal policy.

Real pending admission and real TCP shutdown are separate cases. The shutdown
case holds actual transport flush Pending and observes the socket owner's inner
shutdown boundary. The borrowed Wrapper-join cancellation case instead holds
a private gate in the original Wrapper after logical authority closure. It
establishes actual original task-join Pending, not native shutdown Pending.
No new controlled WebSocket pending-close claim follows.

Session/worker errors and Wrapper returned errors or original panic payloads
remain separate and unique through blocked sibling cleanup. Future-drop tests
wrap actual discovery/admission futures safely; they do not recover arbitrary
earlier errors erased inside native/provider code.

A separate observer Runtime B holds data-only control, not the aggregate or its
anchor. Its original host and observer results are retained through cleanup
while live A continues launch and joins. This does not establish cleanup after
root/A loss or a supervisor's forced abort.

Fixtures retain observed setup, transport, host and worker results until gate
release, stop and original task joins. Deliberate fault containers are separately
catch-disposed after those barriers and before assertions or error propagation.
The original peer and pending WebSocket upgrade stay in the fixture across
borrowed observations. Arbitrary unobserved I/O temporaries are not covered.

This graph excludes sibling listener connections, certificates, provider jobs,
Started broker work and arbitrary retained native operation futures. Zero native
submissions is not a native-buffer disposal or physical termination proof.
Uncooperative work/destructors, undriven/lost runtimes, root loss, OOM/process
abort, double panic and spawn hooks that prevent return of the original token
remain unsupported. No source-health, safe-reopen, production adoption or
historical SDK-failure explanation follows.

## Verification

The first focused run passed 25 of 30 new cases and failed five. A fixture
correction gave controller links distinct connection-wide names; the next run
passed 28 and failed two cancellation checkpoints. The final correction retained
the pending Session Ready callback index across borrowed cancellation and used
an explicit original-Wrapper return gate for the Wrapper-join checkpoint. The
30-case focused run then passed. This controlled join gate does not claim real
native shutdown Pending; the separate actual TCP shutdown case is unchanged.

An initial strict Clippy run failed on four new experiment/fixture lints. Narrow
match, boxed WebSocket variant, direct-return and async-function corrections
resolved them without lint allowances. Original lifecycle tests and their
compile-fail blocks remained byte-identical.

With the final 16 source files unchanged, ten full retained-filter runs each
passed all 136 registered checks: 1,360 executions, comprising 300 executions of
the 30 new cases, 1,010 of the existing lifecycle cases and 50 other existing
filtered checks. The three existing retained compile-fail examples also passed.

The required all-feature engine run passed 840 checks, exactly preserving its
previous identities and statuses. The protocol suite passed 514, exactly adding
the 30 new cases; storage passed 239, domain 1,371 and cluster 797, all unchanged.
Both default and all-feature workspace runs passed 4,971 tests in 134 groups,
with ten existing ignored tests and exactly those 30 additions. Full group-aware
comparison found no removed checks or changed statuses. An extra default-feature
engine run passed 775 checks with two existing test warnings in untouched engine
files; it is supplementary evidence, not the warning-free 840-check gate.

Formatting, strict workspace Clippy and workspace builds passed in both feature
configurations. Protobuf descriptor generation and whitespace checks passed.
Verification reused one target directory, ran serially on two low-priority
cores, and left about 151 GiB free at home and 907 GiB on the build volume; the
target directory occupied about 97 GiB. No SDK gate ran for this increment.
These checks do not activate a default listener or expand the evidence boundaries
above.
