# Retained-byte accounting kernel

`domain::retained_accounting` is an opt-in pure calculation library. Existing
finite queue profiles remain metadata only: no command reads or writes a usage
row, reserves capacity, or rejects a send based on retained bytes. Existing
message V1/V2/V3, profile and proposal formats (including V7 finite intent and
V9 proposals) are unchanged.

## Charge and ownership

Actual retained charge is the checked sum of each stored key length plus its
value length for tags `03` message, `04` ready, `05` lock, `06` expiry, `07`
deferred and `0B` scheduled. One ordinary parent and its exact DLQ shadow share
an owner and generation. Profiles, counters, owner metadata, session and
duplicate state, usage rows, journal, indexed checkpoints, LSM and filesystem
overhead are excluded. Session/duplicate finite admission is not supported by
this kernel; callers must reject those profiles, not assume excluded rows are
free supported functionality. Topic and subscription ownership are not covered.

`calculate_batch` accepts a caller-owned immutable before image and an atomic
`WriteBatch`. Every original key is unique; each final key is visited once.
Repeated mutations coalesce by last-write-wins, including put/delete/put and
deleting an absent key. Before-image absence must mean truly absent, not an
omitted read. The caller must serialize writes while capturing and using that
image. All charged scopes in the supplied image/batch require explicit owners;
malformed charged keys and unknown tags refuse even if later overwritten.
Known excluded tags are not semantic validation of their key/value bytes.
The calculator checks row ownership and exact lengths, not message/index
health; reservation validates a complete individual message's canonical rows.

The result separates removed/added byte and primary-message totals per owner.
Applying these to a usage refuses arithmetic underflow/overflow and generation
mismatch. `ReservationDelta` is a separate input: raw retained-byte deltas cannot
infer future growth reservations. No signed counters, saturation or overdrafts
are used. A complete-image independent recomputation can use an empty before
image and a batch containing each resulting raw row.

## Reservation model 1

`reserve_message` requires exactly one canonical unframed V1/V2/V3 message and
all its current companion indexes. V1's duplicated body/envelope, V2/V3 raw or
AMQP content, projections and overlays are measured by the real message codec,
not a payload-size approximation. AMQP originals and all copy-local content are
unchanged by calculation. Session-bearing records and malformed/missing/extra
indexes refuse. Ordinary rows must have no DLQ provenance; DLQ rows must have
provenance, no TTL and no scheduled state.

The deterministic ceiling is the maximum key-plus-value size among:

- parent ready (plus expiry if present), deferred, scheduled (if the original
  has a scheduled time), and locked from ready/deferred;
- automatic ordinary/deferred TTL and max-delivery DLQ copies in ready,
  deferred and locked states, using the longer shadow path, stripped TTL and
  the exact currently emitted reason/description strings;
- the same states for an existing DLQ copy, preserving its already-admitted
  application reason/description rather than creating additional metadata.

For these candidates, sequence, enqueue time, optional scheduled/expiry times,
lock token, lock deadline and dead-letter time use `u64::MAX` (ten-byte Postcard
varints); delivery count uses `u32::MAX` (five bytes). Fixed enum discriminants
and Option tags are encoded by the current codec. Immutable variable-length
fields retain their exact contents and length prefixes. Keys use their real
fixed eight-byte numbers; no key-prefix estimate is substituted.

Lock/renew and activation therefore cannot exceed a candidate's numeric bound.
Ready/lock/deferred/scheduled companion sets are each explicitly measured.
Automatic TTL/max-delivery can create only one of the bounded DLQ descriptions;
DLQ lock/renew preserves it and cannot recursively dead-letter. Deletion releases
the whole commitment. Recomputing a reservation through these automatic states
cannot increase its ceiling; a DLQ transition can reduce it. Property patches,
content replacements and application DLQ metadata are external rewrites: they
require recomputing and checking the new reservation **before** writing. The
model does not authorize unbounded partial metadata updates against old headroom.
Changes to automatic metadata or encoding require updating the model/proof.

`reserved_growth_bytes = ceiling_bytes - actual_retained_bytes`. A strict cap
must check the sum of actual and reserved bytes, including on shrink; internal
transitions do not receive an overdraft. This library does not activate that
check in the state machine.

## Capacity framing and follow-on integration

Every reservation call explicitly chooses `CurrentUnframedEncoding` or
`AdditionalMessageValueBytes(maximum)`. The latter adds a caller-specified maximum
capacity-owned message header/trailer length to every candidate, on both parent
and shadow paths. There is one retained primary value per message. It must cover
**all** additional framing bytes, not just a semantic owner identifier. The
current-only choice proves only today's encoding and is not an activation gate.

The writer/recovery fence work must define its actual capacity-owned framing,
freeze/verify its allowance as part of the reservation model, and add canonical
decoding/accounting for those framed inputs before using them. Framed bytes
currently refuse canonical message decoding. Framing that grows keys, indexes,
or separate charged rows needs an explicit model extension, not an implicit
zero-byte allowance. No future message/proposal/profile version is allocated here.

The ordered integration is writer/recovery fences (#342), lifecycle conservation
(#343), then finite creation/admission/resize (#344). Generation authority, legacy
writer refusal (including profile-bypassing Complete and shadow writers), missing
usage refusal, snapshot/reopen verification and atomic reservation updates belong
to those integrations. The kernel does not adopt existing finite queues, stamp
populated stores or reinterpret V7 intent. Do not persist its usage in a live
store until the recovery and writer fences are in place.

## Usage key and codec

The new, otherwise unallocated domain tag `12` holds
`12 || canonical namespace || 00 || ordinary parent path || 00`. It is separate
from metadata tag `11`, message/index tags and external `F0`/`F1`. Current snapshot
catalog/full-state validation refuses `12` as unsupported; accepting these rows
requires coordinated recovery validation, not silently skipping the new tag.
The key stores the owner, not the shadow or a generation-specific orphan key.

Usage V1 is exactly 72 bytes: `SYBU` (4), format version (big-endian u16, 1),
reservation model (big-endian u16, 1), then generation, actual retained bytes,
reserved automatic growth and primary-message count (four big-endian u64s), and
a 32-byte SHA-256 digest. The digest covers
`switchyard retained usage v1\0 || exact usage key || preceding 40 value bytes`.
It detects accidental corruption and cross-owner substitution, not hostile
forgery or authority. Generation must be nonzero and match the supplied owner.
An empty owner has zero actual/reserved/count; nonempty usage requires positive
actual bytes and cannot claim more messages than actual bytes. Combined
commitment must fit u64. Unsupported versions/models,
wrong size/checksum, stale generation and impossible/overflowing totals refuse.
There is no implicit migration or backwards-writer compatibility claim.
