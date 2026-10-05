# Private Retained Atomic Session

The protocol crate contains a private `cfg(test)` experiment for one actual
atomic-ingress session and its original routing workers. It is not a public
connection or fixture lifecycle API. Default listeners, ingress policy and SDK
fixtures remain unchanged.

The unique external root and captured runtime must stay live. Drive borrowed
finish on that runtime; a runtime handle does not keep it alive. An observation
deadline or abort request is not completion. There is no rescuer after root or
runtime loss, and the external anchor need not be `Send` or `'static`.

## Fixed Graph

```text
external Root<anchor, broker>
  original Session handle and final result
  Owner, event receiver and root sender
  whole worker packet, lifetime budget, runtime handle, anchor

actual Session future, captured before spawn
  restoring packet loan: JoinSet, reserved launch rows and raw final rows
  refundable admission or accepted installation claim
  actual session, routing inputs, event sender and runtime handle

original routing workers
  existing endpoints, broker, authorization, event sender and live permit
```

The root joins the original Session, takes its whole worker packet and joins
every remaining original token. Live and peer-End joins already retained by the
Session remain in that packet. No task captures its own token, root, event
receiver or anchor. A configured 1-to-128 lifetime worker budget is separate from
the existing live-link limit.
Completed workers never replenish lifetime history. Reserved row capacity bounds
task and outcome identities, not arbitrary payload sizes or process memory.

## Shared Routing Body

The adapter uses the actual existing routing body and preserves its validation,
admission, settlement and decoder ordering. Default workers retain unit tickets,
infallible launch admission, current-runtime spawning and original first-error
behavior; their added drain-phase hook is a no-op.

Scoped tickets are reserved after preceding validation and before actual link
acceptance. They survive registration awaits and refund unused, remotely detached,
invalid-CBS or cancelled admission. An accepted claim carries its installation
obligation through sealing. Original spawn and installation have no await gap;
history commits only after the original returned token is stored in the JoinSet.
A refused claim returns the same unspawned future, retained through owner close
and acknowledgment before destruction outside custody locks. Spawn hooks that
prevent Tokio from returning the original token remain excluded.

Each Ready row keeps the unique original worker result or JoinError. A Session
worker-failure result refers to that retained row rather than replacing its error.
Live, peer-End and external-collection phases are test metadata, not authority.

## Finish Order

1. Borrow the original Session token. Store an observed Ready result before its
   subsequent checkpoint or Ready-triggered close action, and never poll the
   completed token again. Earlier stop closure may already have occurred.
2. Drive owner events, operations and ticks while active. StopConnection seals
   and logically closes before acknowledging, without replacing the final result.
3. After close, keep consuming events through the closed owner while retaining
   operation futures. Their presence is not a native-job completion barrier.
4. After actual Session join, take the whole packet, request remaining worker
   cancellation and actually join every original token. Store each raw Ready row
   before classification, another await or a caller callback.
5. Close the event receiver, drop the root sender and drain its buffer through
   None. Drop receiver and owner fields outside locks after logical close.
6. Transfer original Session and worker results, metadata and anchor exactly once.

Cancelling or unwinding borrowed finish restores the complete set/results packet
to the still-live root. Custody locks only move fixed Option and data states;
they never poll or wake tasks, format or drop raw results, or run callbacks.
Report extraction is one-shot, not cached repeatable cleanup or a health check.

## Evidence Boundaries

The focused tests use real AMQP frames and original routing workers, plus a
separately retained public engine socket pair. Atomic joins do not replace its
Actor and Reader joins. Deliberately fault-bearing results are retained through
both sets of barriers and catch-disposed before assertions or error propagation.

The 34 tests cover routing and lifetime history, acceptance refunds and final
sealing, original errors, whole-packet custody, borrowed-observation cancellation,
and owner-close ordering. Pending CBS registration and impossible fixed-decoder
refusal remain source-only audits. Teardown flags and zero broker submissions do
not prove disposal of held native operations, buffered receipts or Started work.
A post-join worker Drop counter is not a capture-destructor ordering barrier.

Only final task outputs are retained. Earlier internal locals and secondary
errors already erased by existing close handling remain outside this slice.
Root/runtime loss, process or OOM abort, unreturned spawn tokens and uncooperative
tasks or destructors remain exclusions. There is no universal memory bound.

This is not integrated into the [retained protocol connection](retained-protocol-connection.md),
which still covers Wrapper, Actor and Reader only. Multiple Session collectors,
their shared owner, listener descendants, native jobs and fixture resources need
separate custody and actual barriers. No native physical completion, safe reopen,
source-health verdict, SDK compatibility fix or wider lifecycle activation follows.

## Verification

The final focused suite passed all 34 regular tests. Ten unchanged serial repeats
passed another 340. All 21 checks in the final serial verification group passed:
formatting, strict all-target workspace lint in default and all-feature modes,
the ten repeats, all-feature engine tests (840 passed), all-feature protocol tests
(458 passed), both full workspace suites (4,812 passed across 132 result groups,
with 10 existing ignored tests each), both workspace builds, protobuf validation
and whitespace checks. All 13 implementation digests remained unchanged through
this final verification. This increment adds no documentation examples.

Earlier attempts remain retained separately. The first focused suite passed all
34 tests but emitted four private-interface warnings. Six test-only visibility
lines were narrowed; the revised focused suite passed without warnings. The first
broad attempt then stopped on one oversized temporary-enum lint diagnostic.
The narrow correction moved the same handlers directly into the private worker
drain's select branches, without a new allocation, lint exemption or production
policy change. The focused suite and entire 21-check group were rerun afterward.

Compilation was capped at two cores at low priority, tests ran serially, and disk
and memory headroom were checked before and after substantial builds. The existing
shared build cache was reused; no additional cache was created.

No live SDK gate ran for this increment. Earlier SDK and cluster failures remain
unexplained; these green checks neither explain nor repair them. Final task joins
are not native-job completion, whole-descendant cleanup or safe-reopen authority.
