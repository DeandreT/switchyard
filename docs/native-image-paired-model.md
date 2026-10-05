# Paired Image Transition Model

The cluster crate contains a private, unconditional `cfg(test)` model for paired
state/log observations. It is a pure codec, identity, policy and classification
experiment, not a physical store format, creation path, adoption operation,
recovery tool or engine feature. Normal constructors, storage profiles, frozen
domain codecs, native metadata, dependencies and runtime behavior are unchanged.
No record or classification grants a receipt, writer, purge permit or mutation.

## Bounded Records

Seven closed record kinds describe headers, progress, baseline, intent, state
selection fence, log final fence and stage binding. The private SWAI frame has
schema 1, an explicit kind/owner role, a fixed big-endian length and SHA-256 of
the complete header plus payload. Borrow-only visitors reject owned sequence
shapes; canonical re-encoding, remaining-input checks and nested limits precede
copies.
No new parser deserializes arbitrary owned native collections.

The full frozen checkpoint limit is 8 KiB; its membership payload is separately
limited to 4 KiB. Native fields and existing canonical metadata are at most
8 KiB. Header/progress/fence/stage-binding controls are at most 256 bytes,
baseline is at most 16 KiB, and intent is at most 128 KiB. The intent budget
counts every nested old/selected/final checkpoint and native projection.
Worst-shape framing tests do not claim a semantically valid maximum image.

State observations contain at most seven fixed controls/components plus a
complete canonical business image. Log observations contain five closed controls
and at most 256 exactly keyed entries, with at most 64 MiB of entry values.
Existing business and artifact bounds remain 64 MiB. Unknown, duplicate,
wrong-role or old-prefix
controls are refused. Entry identity hashes count and byte totals plus every
full key/value with explicit length framing; there is no second collected body.
These are logical limits, not aggregate RSS, backend materialization, disk,
spare-capacity or universal allocator-failure guarantees. Existing bounded
semantic/native validation can still allocate.

## Independent Selection

Trusted inputs separately supply the seed manifest hash, exact old seed rows,
selected complete checkpoint and whole-artifact hash, every actual native field,
explicit tail choice and exact final inventory hash. Offered metadata is not
allowed to invent that authority. Existing native snapshot ID spelling and
metadata bytes are unchanged. Checkpoint agreement alone does not establish
whole-body identity, authentication, ancestry or source health.

Initial and membership-free old or selected images remain report-only policy
refusals, including valid noninitial membership-free images. A private absent
membership sentinel accepts only no source plus the exact native default value;
it does not widen the shared membership codec or turn that image into a candidate.

A present catalog must match the full actual metadata/image pair. A stale catalog
requires the exact old baseline and bounded checkpoint recurrence over the
independently supplied seed rows. This is an identity check, not ancestry proof.
Absent catalog is allowed only with explicitly trusted absence, an exact
noninitial old baseline and a valid suffix. Same-checkpoint/different-body,
ahead, conflicting or unbound catalog observations cannot be inferred safe.

The committed vote is preserved exactly and compared using the native partial
order against the old baseline, selected image and every seed-row leader.
Missing, uncommitted, insufficient, incomparable or changed votes refuse.
Exact continuation retains every contiguous suffix byte after selection; an
unrelated or gapped suffix is not repaired. Only an independently explicit empty
reset may remove the complete exact old entry inventory.

## Classification Only

One serial-1 intent binds the exact old state/catalog and persisted Ready fence,
old log controls/inventory, selected checkpoint/body/native fields and exact
final log controls/inventory. Fence recipes omit their own intent digest; the
actual final fences bind the complete canonical intent hash, avoiding circular
identity. There is no journal clearing, reuse, retry, rollback or automatic repair.

Observed business/catalog/log semantics are validated before foreign-binding or
missing/mismatched-fence classification shortcuts. Malformed input therefore
remains a typed observation error rather than being laundered into Neither.
Valid foreign observations can still classify Neither/Halt. A partial or changed
stage is Invalid/Halt without erasing exact state/log facts.

State and log independently classify Old, Selected or Neither; stage independently
classifies Absent, ExactSelected or Invalid. Cross-role facts distinguish pending
old pair, pending finalization, retained finalized, ordering inconsistency and
halt. Absent staging supports only the pending Old/Old case; it cannot justify
selected/finalized cleanup. Exact data no-ops still require distinct state and
log phase fences. A missing/prior intent or absent old fence cannot be reconstructed
from matching business bytes.

These facts cannot tell whether a task ran, commit was entered, a write returned
an error or its outcome is unknown. They authorize no physical transition.

## Focused Evidence

The 38 tests cover golden frames, every record kind, canonical/length/checksum/
schema refusal, borrow-only shapes, independent checkpoint/membership bounds,
every native field, redacted static errors, role/stage ordering, no-op fences,
complete intent identity, malformed observations despite missing/foreign fences,
stale/absent/conflicting catalogs, initial and membership-free refusal, vote
partial ordering including the old-baseline leader, complete suffix identity,
explicit reset, inventory caps and separately supplied trust.

Fixtures use existing in-memory domain reference images and canonical bytes.
There are no paired physical stores, native owner tasks, actual retirement joins,
all-handle-drop/reopen experiments, child-process crashes or live SDK results.
Physical fencing, atomic state/catalog/selection-fence publication, complete
same-view backend capture, creation ordering, sealed writer admission and BOTH
owner custody require separately reviewed implementation and evidence.

## Verification

The final source passed all 38 focused tests and ten additional repeated runs
(380 passes). The complete all-feature cluster suite passed 733 tests across
seven targets, with none ignored. The normal default-feature workspace passed
4,594 tests across 131 targets, with ten existing opt-in SDK tests ignored.
Formatting, both strict all-target workspace lint configurations, both workspace
builds, the administrative protocol descriptor and diff checks passed. All Rust
verification used two low-priority cores and the existing shared cache; source
hashes remained unchanged. No all-feature workspace or live SDK run was selected.

Pre-import review corrected three model findings: the committed vote must also
cover the old baseline leader, malformed observed bytes must be validated before
binding/fence shortcuts, and exact absent native membership needs its private
report-only sentinel rather than the shared membership encoder. The reviewed
source included those corrections before its first Rust execution. No source
correction was needed during verification. Documentation review also made the
header-plus-payload checksum wording exact; none of this is physical adoption,
creation, reopening, runtime custody or engine activation evidence.
