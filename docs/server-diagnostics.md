# Typed Server Diagnostic Recorder

`ServerDiagnosticRecorder` is an explicitly allocated, finite, data-only recorder
in the AMQP crate. It stores caller-supplied typed observations; no live listener,
connection, reader, actor, collector, transaction, writer, fixture, or retirement
producer is attached. A private test-only writer binding exercises observations
without activating a production path. The recorder spawns nothing and retains no
socket, message, native protocol identity, broker, or store handle. Existing behavior,
retry policy, deadlines, SDK gates, dependencies, and startup are unchanged.

## Finite Correlation And Publication

`try_new` fallibly reserves the full 4,096-row event limit before publication.
Fresh recorder-local `DiagnosticScope` ordinals use checked atomic increment;
the last valid ordinal is `u64::MAX`, and exhaustion cannot wrap or resume.
Parent correlation must belong to the same recorder. Scope cloning preserves
private provenance and local correlation, not a wire ID or protocol authority.
No caller-chosen ordinal or native identifier constructor is exposed.

`record` accepts closed `Copy` event enums and a supplied elapsed `Duration`.
It converts elapsed milliseconds outside the lock, saturating at `u64::MAX`;
it reads no clock. Publication uses a nonwaiting lock attempt, checks the fixed
limit, and copies one row without allocation, formatting, callbacks, I/O, or
await under that lock. Full storage never grows, overwrites, clears, or resumes.
Sequence describes publication order, not causality or a monotonic time proof.

The closed schema includes nine scope kinds and 80 unique event labels for
fixture, connection, decoded-frame, actor-dispatch, end, retirement, write, and
task observations. Declaring an event family is not attaching a producer.
No variant carries strings, raw IDs, addresses, delivery tags, transaction IDs,
tokens, credentials, bodies, descriptions, arbitrary errors, or raw frames.

Six separately observed saturating loss counters distinguish full buffer,
contention, poison, exhausted ordinals, foreign provenance, and capture allocation
refusal. Their snapshot is not atomic across counters. Recording/capture can
refuse instead of waiting; a zero counter does not certify external health or
coverage. Errors are fixed typed refusals without caller/backend detail.

## Immutable Capture

`capture` fallibly reserves bounded output storage before taking the same
nonwaiting lock, copies at most 4,096 rows, observes the independent counters,
then releases the lock. The returned `DiagnosticCapture` owns only immutable
rows and numeric losses; it keeps neither recorder provenance nor source
resources alive. Later publications and losses do not mutate it.

`format_bounded` reserves output fallibly before formatting outside all recorder
locks. Each row uses a 224-byte fixed line, and the summary uses 512 bytes;
maximum output is 918,016 bytes, below 1 MiB. Only fixed ASCII labels and numbers
are rendered. No caller `Display`, error formatting, or output callback runs.
Overflow refuses before fixed-line growth. Scope Debug shows only kind and local
ordinal; recorder/capture Debug exposes no external content.

The event-vector, capture-vector, and output-string reservations are fallible.
Arc and ordinary allocator bookkeeping, spare capacity, aggregate RSS, and
process-wide OOM behavior are excluded. These limits apply to this added typed
storage/formatter, not all existing SDK logs or primary exception rendering.

## Evidence And Limits

Focused tests cover exact capacity, immutable captures, same/foreign provenance,
fresh parents, concurrent publishers and scope minting, maximum ordinals and
durations, all saturating counters, contention and lock poison, every label and
scope kind at maximal numeric width, full output limits, fixed-line refusal,
data-only ownership, and static diagnostics. Both spawned test threads are
actually joined before concurrent-result assertions. Six compile-fail examples
check private scope/row construction, immutable captures, and no arbitrary
string event payload.

The allocation-refusal unit uses deterministic private capacity overflow, not
an injected real heap failure or proof of every allocation path. Structural
typed privacy is not a captured live socket trace. Empty capture, event absence,
zero losses, or a manually recorded `ActuallyJoined` event cannot establish that
a boundary ran, a task joined, or an entire lifecycle was observed.

Whole-task ownership, safe fixture-resource retirement, live producer attachment,
and complete coverage require separate implementation and evidence. This
foundation does not explain or fix the historical provisional-Complete SDK
timeout described in [client diagnostic evidence](atomic-sdk-evidence.md).

## Recorder Verification

The recorder-only checkpoint passed 26 focused unit tests and six compile-fail
examples. Ten additional focused runs passed all 260 executions. The AMQP
all-feature suite passed 784 tests; the normal CI default-feature workspace suite
passed 4,521 tests with ten existing ignored tests across 131 targets. Formatting,
strict workspace Clippy for both feature configurations, both workspace builds,
the administrative protocol descriptor, and whitespace checks passed.

An initial strict lint run required replacing an error-preserving `map_err` with
`inspect_err` at capture reservation. It still increments the same loss counter
only on refusal and returns the original typed error. The focused tests and
complete selected gate set were rerun after this equivalent correction.

This increment did not rerun the all-feature workspace suite or live SDK gates.
The selected scope combines normal CI workspace coverage, all-feature AMQP tests,
and both feature configurations for linting and builds. It adds no live producer,
complete lifecycle coverage, deadline change, or historical timeout explanation.

## Test-Only Writer Observations

Every existing `FrameWriter` constructor leaves its optional private binding
disabled. Only a test-only factory enables it. The disabled path reads no
diagnostic clock, allocates no diagnostic scope, and accesses no recorder.
This is source-audited, not a test that counts clock reads. A bound test writer
retains only the recorder, an optional local parent, and its monotonic origin;
the observation helper keeps no frame, error, socket, Activity, or task reference.

First poll creates a fresh local Write scope. An unpolled future publishes
nothing. Fixed events distinguish preflight, write-all, flush, local acceptance,
underlying write/flush errors, outer deadline expiry, late completed flush, and
abandonment at the last unfinished socket stage. Underlying `TimedOut` errors
remain distinct from the enclosing timer expiring. A late successful flush can
produce `FlushDone` followed by `TimeoutAfterFlush`, never `WriteAccepted`.

The observation guard is outside the original single timed write-all/flush
future. It preserves preflight order, encoded bytes, peer limits, Close handling,
deadline selection and strict acceptance boundary. Original typed errors and
custom causes are returned without formatting, inspection, or replacement.
Existing Activity completion/failure updates run before disarming the guard.
Dropping a pending attempt reports its unfinished stage once; it does not repair
Activity, clear taint, retry, write Close, perform I/O, or spawn cleanup.

Full storage, contention, poison, exhausted ordinals, and foreign parent
provenance remain separate recording losses, not replacement I/O errors.
Opt-in clock/atomic/nonwaiting publication work can consume real time; unchanged
deadline policy is not an identical real-world scheduling guarantee.

`WriteAllDone` and `FlushDone` describe local API completion. `WriteAccepted`
describes original local writer-policy acceptance. None establishes peer or SDK
receipt, transaction preparation, durability, remote settlement, task joins, or
fixture retirement. Direct encoding, protocol headers, and negotiation helpers
remain unobserved. Stage abandonment does not identify its cancellation cause.

The 15 focused writer tests use controlled direct AsyncWrite implementations and
paused/manual polling, not live sockets or SDK fixtures. They compare enabled
and disabled outcomes, bytes, polls, deadlines, Close exceptions, taint, original
custom error identity, pending Drop, and all five writer recording-refusal modes.
Hostile error formatting is never called. Actual writer-produced captures omit
private sentinel values and remain unchanged after writer/source references are
dropped. This is bounded typed test evidence, not production-wide privacy,
complete trace coverage, allocator-failure injection, or physical cleanup proof.

The final writer checkpoint passed all 15 new writer tests, the combined 41
recorder/writer unit tests, and the six existing compile-fail examples (47 unique
focused cases; the writer-only run overlaps the combined run). Ten additional
combined runs passed 410 executions. The AMQP all-feature suite passed 799 tests;
the normal CI default-feature workspace suite passed 4,536 tests with ten existing
ignored tests across 131 targets. Formatting, strict workspace Clippy in both
feature configurations, both workspace builds, the administrative protocol
descriptor, and whitespace checks passed without source corrections.

The all-feature workspace suite and live SDK gates were not rerun for this
test-only binding. This evidence adds no live writer attachment, complete task
coverage, SDK receipt proof, pending-Drop behavior repair, stronger fixture cleanup,
or explanation of the historical provisional-Complete timeout.
