# Private Two-Session Atomic Collector

The protocol crate contains a private `cfg(test)` experiment that retains two
actual atomic-ingress sessions and their original routing workers under one
logical owner. It does not activate a listener, public lifecycle API, default
connection driver or SDK fixture cleanup.

The caller first opens and retains the actual `ServerConnection` and its
independent engine owner. It then creates the collector with settings, a worker
history limit `K` and an external anchor before constructing either original
session-admission future. Invalid `K` returns the same settings and anchor in a
boxed refusal without creating a collector task.

## Fixed Graph

```text
external collector root
  one Owner, event receiver and root sender
  two original pinned admission futures and observed results
  two original Session handles and final results
  two restoring worker packets, shared histories and link semaphore
  runtime handle, fixed stop acknowledgments and external anchor

each actual Session future
  one restoring packet loan and actual routing inputs
  original routing workers and their returned task tokens
```

Two total admission attempts are distinct from two returned Session launches.
Each offered `IncomingSession` reserves a session ticket, constructs the actual
owned acceptance future and installs a fixed record without an await. A refused
offer returns the same original incoming session in a private box, without
cloning or starting its acceptance. That allocation is not a universal OOM or
fallible-allocation guarantee.

A remotely ended attempt refunds its unused session ticket, not its attempt
slot. There is no unlimited replacement loop. Returned session launches never
refund lifetime history. Both sessions share one logical registry, the actual
existing routing body, a 128-permit live-link semaphore and the connection-wide
worker launch history `K`, where `1 <= K <= 128`.

Each preallocated packet has capacity for `K` launch identities and `K` final
rows; at most `K` workers launch across both packets together. The bound is on
two Session and `K` worker task/outcome identities plus two admission futures,
not total objects, arbitrary payload sizes or process memory.

## Original Handoffs

Original admission Ready moves directly into its root record before subsequent
callbacks, awaits, classification or completed-future disposal. The original
pinned future remains retained and is never repolled after Ready. An accepted
session sealed before launch stays rooted and unlaunched through the barriers.

Accepted `ServerSession` moves once into an armed restoring Session future
before spawn. A final accepted claim retains its installation obligation across
seal. The original returned handle is installed before its history commits,
without an await or callback gap. Worker ordinals are assigned under the shared
budget's installation-commit lock, not cached during concurrent reservations;
they describe serialized returned installations, not physical task-start order.

The existing single-session experiment reuses the same scoped Session helper.
Its original tests and default routing policies remain unchanged. No production
driver, authorization grace, discovery, deadline or connection Close policy is
copied into the collector.

## Borrowed Operations

Active drive pumps admissions, Session joins, owner events, operations and ticks.
Normal completion of one Session does not close its sibling. Each observed
original Session or worker Ready result is retained before later Ready-triggered
actions; an earlier explicit stop may already have closed the owner.

Stop seals all histories, closes logical owner authority and then requests
original Session cancellation. StopConnection acknowledgment senders occupy two
fixed root slots across a borrowed observation await, rather than disappearing
with a canceled local observer. Events continue through every actual Session and
worker join, including WorkerStopped acknowledgments. After close, owner
operations and ticks are no longer applied; retained operations are not native
completion receipts.

Finish stops, actually joins both original Session tokens, takes their restored
packets and collects every remaining worker completion with that event pump.
Canceling or unwinding borrowed finish restores whole sets, launch metadata and
raw rows to their cells. Resuming observation does not resubmit uncertain work.
After all joins and identity/history checks, finish closes the receiver, drops
the root sender and drains through None using the closed owner. It disposes
receiver and owner outside custody locks, then extracts its one-shot report and
anchor. Report extraction is neither cached cleanup nor a health check.

## Evidence Boundaries

The caller must retain the external root and captured live Runtime A through
finish. A handle is not its runtime owner; current-thread A needs I/O and timers
driven. The anchor may be non-Send and borrowed, and never enters a child task.
There is no autonomous rescue after root or runtime loss.

Tests use actual Open, two Begin approvals and original producer, controller and
messaging-consumer routing. The report retains original admission futures and
observed outputs plus FINAL Session and worker results. Stopped pending original
admissions are not polled again or dropped before the report. Dropping one does
not cancel already-enqueued native acceptance.

The fixture separately joins the original engine Actor and Reader. The collector
is not integrated into the [retained protocol connection](retained-protocol-connection.md)
and cannot certify Wrapper or engine barriers. Deliberate panic-on-Drop results
are catch-disposed after collector and separately covered socket/host barriers,
before assertions or early error propagation.

Earlier internal locals, canceled internal stop/detach errors, secondary close
errors and arbitrary hidden owner/native temporaries remain excluded. Teardown
flags and zero broker submissions show task ordering, not held-native-buffer
disposal or Started-job completion. Payload size and destructor time are unbounded.

Stable Tokio without panicking unstable spawn hooks is the supported model.
Unreturned spawn tokens, root/runtime loss, OOM/process abort, double panic and
uncooperative work remain unsupported. No native physical completion, storage
fence, safe reopen, whole-listener/descendant cleanup, certificate/provider
release, source-health verdict or historical SDK-timeout explanation follows.

## Verification

The first focused attempt stopped at one ambiguous test-result binding; no
tests ran. After an explicit test-only type annotation, all 26 new tests passed
with three private-interface warnings. Three visibility narrowings removed
those warnings, and the new, preserved and combined focuses passed 26, 34 and
60 tests respectively.

The first broad gate then stopped at strict-default Clippy: seven diagnostics
identified the same large private refusal payload, and one identified replacing
an array with its default. The corrections boxed the original refused incoming
session in both refusal branches and used `mem::take`. They changed no test body,
routing policy or successful admission path and introduced no lint suppression.
Fresh post-correction focuses passed 26, 34 and 60 tests without warnings.

The complete revised 21-check gate passed on Rust 1.97.1: formatting, both strict
workspace/all-target lint configurations, ten repeated all-feature combined
focuses, engine and protocol suites, both workspace configurations, both builds,
protobuf validation and whitespace checks. The repeats passed 600 executions:
260 new collector tests and 340 preserved single-session tests. The engine suite
passed 840 tests across three groups; the protocol suite passed 484 across four.
Each workspace run passed 4,875 tests across 133 groups, with ten existing ignored
tests and no failures. Against the preceding checkpoint, both workspace logs
contain exactly the 26 new test identities and no removed or changed statuses.

All 15 source files remained byte-identical throughout that complete revised
gate. Builds used two low-priority cores and one existing shared build cache;
post-build checks found sufficient disk and memory headroom. The existing
single-session and protocol-connection verification histories are unchanged.
This increment did not run a live SDK gate, activate the experiment, establish
native-buffer disposal or explain the earlier SDK and cluster failures.
