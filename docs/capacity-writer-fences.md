# Inert capacity writer and recovery fences

Finite ordinary queue operation is not activated. Historical queue V3 profiles
and proposal V7 keep their exact metadata-only meaning and bytes. They do not
acquire usage, reservations or enforcement on open, replay or upgrade. No runtime
command emits the new framing, and there is no migration or raw-writer-safe
administration API. Lifecycle conservation and admission remain separate work.

## Selected formats

- Queue profile V4 wraps **both** the finite parent and its publicly non-finite
  dead-letter shadow. Bytes are `04 || generation:u64be || inner_length:u32be ||
  exact historical profile || SHA256`. The digest covers
  `switchyard capacity profile v4\0`, the exact profile key and all preceding
  value bytes. The 45 added profile bytes are excluded from retained charge.
  The shadow must equal the parent's exact derived shadow. Sessions and duplicate
  detection are unsupported.
- Isolated data keyspace V1 is `13 || original canonical key`, for counters and
  message/ready/lock/expiry/deferred/scheduled rows. Every charged key grows by
  **one byte**. Values remain the exact canonical message V1–V3 values or empty
  index values. There is no new message outer frame and no message V5 allocation.
  Message V4 is explicitly outside the reservation proof, even when a general
  message decoder understands it. Session/duplicate keys are unsupported.
- Usage remains tag `12`, fixed 72-byte usage V1, reservation model 1, generation
  and exact owner-key-bound checksum. It is not a framing escape or an adoption
  signal for legacy rows.
- Capacity-owned proposal V11 is a separate `CapacityIntent` envelope. Its SWDP
  header contains version 11 and payload length as u32 big-endian. The payload is
  generation u64be, namespace length u16be, parent path length u16be, exact UTF-8
  namespace and parent, then one complete canonical historical proposal. Scope
  and any retained binding must agree with the capacity owner and generation.
  Nested V11 is refused. Ordinary proposal readers explicitly refuse V11, including
  current replay: only the inert capacity-specific codec can inspect it. V7 is
  never rewritten or reinterpreted. V10 belongs to durable SQL action intent.

The codecs detect accidental corruption and cross-owner substitution, not hostile
forgery. Pure codec inputs are not evidence of live authenticated authority.

## Writer boundary

Every direct or bound command preflights the selected ordinary parent and exact
shadow before Clock or handler preparation, including Complete, held locks,
session-state writers and empty timer maintenance. New profiles require both
canonical profiles, a live matching owner generation and valid usage. Missing
usage refuses; malformed, stale or mismatched state refuses; valid new state also
explicitly refuses until lifecycle conservation is implemented. Usage or isolated
rows without the new profile pair refuse, never rebuild or relabel a populated
store. These errors are not checkpointed as successful deterministic commands.
Unrelated queues remain usable through ordinary runtime application.

Bound scope mismatch still precedes every read. A point-read-only profile/usage
preflight identifies new ownership before the legacy binding reader. Historical
stale/corrupt authority keeps its scan-free refusal; a successful legacy binding
then runs the full owner-local orphan-data check before Clock or handlers.
The local proposer also runs compatibility preflight **before timestamp
selection**, so unsupported/missing/corrupt new ownership never reads host time
or persisted Clock just to construct a refused command. Its bound path uses
point preflight, existing binding validation, then full preflight before stamping;
the state machine rechecks under the same exclusive writer. The broker emits
watcher permits only for successful outcomes, not capacity refusals. Unrelated
owners continue through the normal proposer and notification path.

Pre-ledger handlers cannot see the isolated data through any old prefix. Their
profile-dependent paths refuse V4, Complete cannot find a primary, and config-free
empty maintenance has no rows to mutate and commits no Clock. All mutable owned
rows, including counters, must be isolated; mixing old and new data is not an
accepted representation. Raw/cloned StateStore writes still require external
exclusion and are not runtime administration. Removing all ownership fences by
raw corruption is not a supported downgrade operation.

The pre-ledger dispatcher audit includes create/update queue/topic/subscription,
create/delete/list rule, all send/batch/content forms, cancellation/activation,
peek, both receive modes, deferred receive, Complete, abandon/defer/dead-letter
and their patches, lock renewal, named/next session acceptance, release/renew/
set/get session state, and every timer family. Profile-dependent operations fail
on V4 or the absent appropriate entity; held-lock/session paths cannot locate
isolated records. ExpireMessages and ExpireSessionLocks can return successful
empty maintenance outcomes, but stage no owned row and no Clock. They are not
reported as refusals. Incomplete or corrupt isolated indexes remain invisible
to those old prefixes, not opportunities to create unprefixed repair rows.

## Charge and recovery

`IsolatedKeyspaceV1` reservation requires already-isolated input rows, unwraps
only keys for semantic validation, and charges the additional byte separately on
**each** candidate key. Parent and longer-shadow ready/TTL, scheduled, deferred,
locked and automatic DLQ candidates retain the same exact numeric-varint and
metadata bounds as model 1, including deferred-TTL descriptions. Actual batch
accounting charges the real wrapped keys and unchanged values. It is not journal,
LSM, filesystem or profile overhead, and does not authorize overdrafts.

Recovery/indexed open refuses new profile V4 or tags 12/13 before writes. Snapshot
catalog/full-state validation, capture and installation refuse unsupported new
state rather than certify or discard reservations. Committed V11 proposals refuse
during complete replay preflight, before any prefix is applied. Offline V2-to-V3
store upgrade explicitly checks for unsupported capacity state before changing
the marker, including profile-only incomplete fixtures. The outer store format
stays V3: old raw storage can open it, but old domain writers cannot discover its
owned rows or decode its profiles. This is an owner-local compatibility fence,
not a blanket old-binary open prohibition. Supported non-finite bytes do not change.
