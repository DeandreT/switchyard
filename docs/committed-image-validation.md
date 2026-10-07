# Committed Image Validation

## Current Boundary (2026-10-06)

`ValidatedCreateSendLayout17Image::validate` is the current role-2 proof. It
retains the closed record and relation rules below and additionally requires
exactly one canonical generation-1 `NonFinite` capacity Mode (`0x16`) per primary
queue. Missing, orphan, or shadow Modes are refused. Finite Modes, Usage (`0x17`),
Charge (`0x18`), and broader lifecycle state are outside this narrow profile;
layout 17 is not a finite-capacity image installer. Checkpoint-only images need
no invented Mode. Current operations neither synthesize Modes nor migrate rows.

`ValidatedCreateSendImage` remains the historical role-1 pure proof, with its
original bytes and semantics. It does not accept role 2 or capacity sidecars.
The verification receipts below belong to that original role-1 increment, not
new layout-17 execution.

`domain::ValidatedCreateSendImage::validate` consumes a structurally decoded
[committed image container](committed-image-container.md) and checks its complete
business-record consistency against the closed CreateSendV1 profile. The opaque,
non-Clone result exposes immutable rows, the captured checkpoint and stream, and
row, primary-queue, and retained-message counts. Shadow queues are not included
in the primary count.

This pure API reads no store or clock and owns no writer. It does not certify
source health, actual command history, quorum commitment, authentication,
ancestry, anti-rollback safety, or export/install authority. Checkpoint membership
remains opaque; its cluster schema and log correspondence are not interpreted.

## Closed Profile

Business values must use exactly envelope version 11. The validator does not
run the ordinary migration decoder or silently adopt later value formats.
Supported records must consume their complete bytes and reproduce those bytes
canonically. Borrowed message mirrors retain body, identifier, and session slices;
a streaming serializer compares canonical bytes without a second body-sized
encoding buffer. Recognized broader states and unsupported optional metadata
are refused before decoding their inner contents.

Only the current exact clock, queue configuration/counters, incarnation,
message, ordinary/session ready, expiry, duplicate-history/deadline, and checkpoint
key families are accepted. Names are literal, case-sensitive domain identifiers,
not AMQP-normalized addresses. Full key tails are consumed. Duplicate-history
IDs occupy the whole UTF-8 remainder, including embedded NUL, and obey the
message-ID UTF-16 limit.

Every primary queue must have its exact dead-letter shadow configuration and
one active Queue incarnation at generation 1. Shadow runtime state, shadow
counters/incarnations, topics, subscriptions, deletion, and recreation are
outside this profile. Primary counters cover every retained sequence and keep
the lock counter at 1. Sequence holes and the exhausted next-sequence sentinel
are allowed; validation does not invent the requests responsible for each hole.

Messages must be Ready with delivery count zero and no dead-letter, scheduling,
or rich-envelope metadata. Their sequence keys, body and identifier bounds,
queue size limit, session requirement, and retained enqueue ordering must agree.
Every message has exactly its required ordinary/session ready index and optional
expiry index, with empty marker values and no orphan or duplicate indexes.

An expiry may equal enqueue time, including zero requested TTL or saturated
timestamp arithmetic. A finite default TTL requires expiry and supplies a
known upper bound. The original requested TTL is not stored and is not
reconstructed. Expired messages remain valid because this role has no timer sweep.

Dedup-enabled queues require exactly the latest retained nonempty ID's history
value and deadline index, selected in sequence order. Retained copies may not
overlap the configured history window. The strict deadline-greater-than-enqueue
suppression rule permits two originals at the saturated maximum timestamp.
Anonymous empty IDs have no history. Expired final histories remain valid
because this role has no history-expiration operation.

A business clock is required with queue state and covers retained enqueue times
without exceeding the checkpoint watermark. Equality is not required: a known
business refusal can advance the checkpoint without changing ordinary state.
Initial, refusal-only, blank, and membership-only images can have no business
clock.

## Refusals And Bounds

Errors are static and do not retain supplied keys, names, bodies, or metadata.
Recognized legacy/broader formats, record families, states, delivery history,
optional metadata, and later or retired incarnations return `UnsupportedProfile`.
Malformed supported bytes and inconsistent keys or relations are refused
separately. Inputs are never normalized, repaired, or omitted.

No refusal alone authorizes poisoning an arbitrary opened source. Legitimate
broader operations can leave relations outside this role, including removed
messages or expired duplicate history. A source-role guarantee and any owning
exporter's fatal/nonfatal policy are separate responsibilities.

The container's 65,536-row maximum bounds temporary metadata collections. The
validator allocates metadata maps and small temporary shadow names, not copies
of large message bodies. This is neither allocation-free validation nor a
global memory, allocator-capacity, or concurrent-image bound. The existing
64 MiB artifact and per-key/value caps remain unchanged.

## Verification Scope

Private tests freeze current message-wire compatibility and borrowing, canonical
comparison, unsupported metadata preflight, strict keys and tails, scalar and
identifier limits, and source-private diagnostics. Actual committed Memory and
Fjall fixtures cover maximum body/ID/session sizes, sequence holes, reaccepted
IDs and expired final history, literal independent scopes, zero TTL, timestamp
saturation, opaque membership, and refusal watermark gaps. Raw record mutations
exercise refusal without claiming that those rows arose from valid commands.

A separate plain-queue fixture isolates retained timestamp regression from
TTL and history errors. Real repeated nonempty-ID sends at the maximum timestamp
check both retained messages, counters, and the exact sole history/deadline pair.
The existing Fjall test releases every old handle, reopens the actual directory,
and now semantically validates the recovered image as well as comparing its rows
and encoded bytes.

No fully populated 65,536-row or 64 MiB semantic fixture, allocator/RSS
instrumentation, source export, physical installation, durable snapshot metadata,
or automatic history purge is established by this increment.
The separate [bounded domain exporter](committed-image-export.md) applies these
checks to one captured view; it does not add installation authority.

The validator checkpoint passed 37 focused checks, including the compile-fail
contract and actual Fjall reopen. The all-feature domain suite passed 1,184
tests, and the default workspace passed 4,004 with ten opt-in SDK gates ignored.
Both strict workspace lint configurations and builds, formatting, protocol
descriptor validation, and whitespace checks passed. One initial lint-only
conditional-style finding was corrected before the focused and final gates
were rerun. The full all-feature workspace and live SDK gates were not rerun for
this pure validator increment; the preceding SDK diagnostic checkpoint's live
results and unexplained earlier timeout remain separate evidence.
