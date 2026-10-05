# Isolated Connection Task Ownership

The AMQP engine's earlier private `cfg(test)` path retained the actual actor and
optional reader task for one negotiated connection. It is now promoted behind an
explicit [public owner and consuming acceptor](server-connection-owner.md).
This page preserves the earlier isolated-path evidence and corrections; the
linked public contract records the current opt-in boundary. Unlike the separate
[retirement model](server-task-retirement.md), these are the original socket
futures, not synthetic children. Default ordinary, atomic, or WebSocket listeners,
SDK fixtures, and production startup still do not activate this custody.

The unique external owner must remain retained and drive borrowed `finish` on
its captured live runtime A. Disposable connection parents and status observers
may disappear. Loss of that owner or A remains excluded: a Tokio Handle neither
owns the runtime nor proves its health. There is no detached rescuer, supervisor,
global quarantine, dedicated runtime, or autonomous last-owner cleanup.

## Compatibility Boundary

A private negotiation/launch extraction preserves the existing option validation,
SASL/header/Open sequence, frame limits, deadline helpers, caller conversion
timing, and original EngineError/cause objects. Existing `ServerConnection`
acceptance immediately uses legacy launch in the same poll. That path still
discards the actor handle and uses the original ConnectionReader; client behavior
is unchanged.

Public `shutdown` still cancels driver work and observes actor exit notification,
not an actual actor join. Its implementation, close/error precedence, session
epilogue, diagnostics, transport, and default timeouts are unchanged. Only the
scoped launch activates the new custody. The earlier private checkpoint changed
no public exports; the later public API remains opt-in. Dependencies, storage
formats, and default admission policies are unchanged.

## Ownership And Installation

Two fixed role cells retain the original handles and results. The owner also
holds sticky stop state and a caller anchor that need not be Send or 'static.
Logical observations and injected controls remain test-only. It does not own
Broker, provider, certificates, or a whole-fixture resource bundle. The two roles
are not a full descendant-tree or
aggregate allocation bound.

Synchronous launch claims an unused actor slot atomically against sealing.
Sealed or private-test duplicate launch returns the original advanced negotiated
transport without creating an identity or task. An accepted no-await installation guard stays
outside the actor future and roots its actual handle before launch returns.
Stop arriving before lifecycle binding remains sticky and reaches the eventual
cancellation sender.

The actor captures only reader registration, stop, runtime, and test-only controls,
never its own handle cell or the whole external owner. The reader captures its
original transport/frame/activity future and controls, not a role cell. Reader
claim also serializes against sealing; claim-before-seal retains the installation
obligation even if stop arrives before spawn. There is no self-join or owning cycle.

The existing actor exit guard is captured before spawn, including queued
unpolled task destruction. Native identity retirement and exit notification are
logical observations, not proof that the actor handle has actually joined.

## Actual Join Barrier

In normal scoped teardown, the actor borrows, aborts, and actually joins the
reader in the original reader-before-session-stop/final-reply position. An
unfinished handle or observed Ready result restores to the same role cell when
that borrower is cancelled or unwinds. Earlier logical Close reply locations
are not moved or strengthened.

Borrowed owner finish seals admission and sends existing cancellation. It
actually joins the actor first, then acquires, aborts, and actually joins the
reader. This order eliminates the reader's creator and competing borrower before
external reader acquisition. Never-created roles become explicitly absent only
after their creator is gone; a claimed installation is still an obligation.
There is no new hard actor-abort policy or bounded termination promise.

Waiting uses restoring actual-handle leases, not owned handles in timeouts.
Pending tokens and original Ready results remain retained on waiter loss.
Spawns, polls, watch sends, wakes, and injected callbacks occur outside custody
locks. Poison recovery moves fixed fields rather than invoking arbitrary output
destruction under a lock. There is no first-error return.

Only both created roles' actual joins, with no outstanding lease or possible
creator, permit transfer of the original results and anchor. Original JoinError
objects, including arbitrary panic payloads, remain rooted until then. Task
output is unit; engine I/O errors already absorbed/logged are not newly forwarded
as join failures. A second finish returns no new report or health proof.
Joining either task does not join its runtime's OS workers or its caller.

## Earlier Focused Evidence

The earlier 23 focused tests cover empty/cached finish, a non-Send Rc anchor,
original sealed/duplicate refusal, and unpolled/pending negotiation loss without
new roles.
They preserve a typed private I/O cause through extracted and public acceptance,
the original invalid-timeout variant, and exact header/Open/Close wire bytes.

Actual queued actor abort, parent Drop before/after reader creation, controlled
actor unwind after reader installation, and claim/stop races exercise real
handles. A separately retained panicked connection parent is actually joined,
not treated as the pair's actor. Exit notification and identity retirement are
observed before a gated actor destructor completes. Lost borrowed finish retains
the token and non-Send anchor.

The cancellation handoff holds the reader at its first poll, observes the
actor's actual reader loan, aborts the actor, and checks restoration of that same
pending token. A lost external borrower restores it again. Separate destructor
gates keep actor/reader joins unready, retaining each original panic result until
the other role's barrier. Those pair-payload counters do not inject panic-on-Drop
report payloads.

Two tests destroy a distinct observer runtime B before actor first poll or during
observed cleanup while A and the external owner stay live. Actual observer and
OS host handles are retained/joined separately. An additional real OS-host panic
payload stays undisposed through both pair barriers; its controlled disposal
panic is caught only after cleanup. Original non-success host/build results also
remain retained rather than being discarded during cleanup.

All controlled gates release and pair cleanup completes before observation
errors or assertions propagate. Observation deadlines bound test observation,
not task termination. Test runtimes use at most two workers. Injected actor
aborts and panics are test evidence, not a new public shutdown policy.

## Corrections And Limits

Pre-integration review corrected premature disposal of a non-success OS-host
result and added the host-payload regression. The first compile found two test
expectations calling a private header helper; explicit golden wire bytes replaced
those calls without changing helper visibility.

The first executed suite passed 22 tests and timed out observing the cancelled
actor in the reader-loan test. A labelled isolated rerun reproduced that stage.
The old test blocked a worker during reader destruction. Pinned scheduler and
channel source admit a starvation path: dropping the reader's frame sender wakes
the actor into that worker's nonstealable local slot before the destructor gate
blocks it; an already-notified actor is not reinjected by abort. The exact failed
run's queue ordering was not instrumented. The corrected first-poll gate retains
that sender while observing cancellation, without extra workers, longer waits,
or changes to production ownership.

Arbitrary I/O destructor panics, custody-lock poison, allocator/scheduler failures,
and panic-on-Drop pair-report payloads are not separately injected here. Process
or OOM abort, uncooperative tasks/destructors, premature A/root loss, and repeated
external-owner failure remain exclusions.

This pair does not retire accepting workers, protocol session/link wrappers,
listener descendants, broker/native jobs, or fixture resources. Existing stop
windows still bound observation rather than physical completion. It supplies no
safe-reopen receipt, whole-run coverage, live SDK evidence, or explanation/fix
for the historical provisional-Complete timeout in
[client diagnostic evidence](atomic-sdk-evidence.md). Public engine ownership is
opt-in; full descendant custody, default listener integration, and fixture
activation remain separate work.

The separate [retained protocol connection](retained-protocol-connection.md)
offers explicit one-socket Wrapper/Actor/Reader ownership without activating
default listeners or changing this page's earlier isolated-path evidence.

## Earlier Verification

The corrected source passed all 23 focused tests, ten additional complete
focused runs (230 passes), and 30 additional isolated cancellation-handoff runs.
The all-feature AMQP engine passed 822 tests across three targets; the protocol
crate passed 380 across four. Both default and all-feature workspace runs passed
4,617 tests across 131 targets, with ten existing tests ignored in each run.

Formatting, strict all-target workspace linting with default and all features,
both workspace builds, the administrative protocol descriptor, and diff checks
passed. Final source hashes stayed unchanged throughout the complete verification
group. The initial compile and execution failures were retained; the complete
final focused and broad checks ran on the corrected source. No live SDK gate was
selected.
