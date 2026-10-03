# Committed Queue Apply

This is a trusted, synchronous durability prerequisite. It is not a consensus
runtime and does not enable production startup, quorum acknowledgements,
replicated listeners, or a new deployment mode. Existing listeners, timers,
local proposers, and standalone storage constructors remain unchanged.

## One Writer

`storage::CommittedStore` has a matching read-only `StateStore` reader, one
privileged batch commit, and an authoritative initialized flag. Concrete
`MemoryReplicaStore` and `FjallReplicaStore` writers are not Clone. Their readers
can be cloned, but `apply` always refuses, including an empty batch. Readers
have no writable inner-handle conversion. This guard survives existing wrappers
that forward ordinary writes to their backend.

`CommittedStateMachine` consumes the writer and derives its matching reader;
callers cannot independently pair preparation on one store with commit on
another. It exposes no writer or prepared-batch getter. All preparation,
checkpoint validation, and commit happen in one synchronous owner call.
The low-level storage trait remains a trusted capability, not an authenticated
service: custom implementations and privileged raw batch writers must preserve
the single-writer contract. Expected-previous checks are not storage CAS.

Replica directories use `0x80000000 | ACTIVE_STORE_FORMAT`, currently
`0x8000000e`, and exact `committed-state-v1` metadata. Fresh creation stamps the
format, profile, and initialized-zero flag in one fsynced metadata batch.
Every privileged commit sets initialized-one in the same atomic batch as its
record mutations. Standalone format 14 is unchanged. Standalone open refuses
replica metadata and replica layouts; old format-14 binaries refuse the
disjoint version. Replica open rejects standalone directories, partial or
malformed headers, and populated unversioned data. There is no implicit
adoption, rollback conversion, or migration.

`create` requires an uninitialized, empty replica and durably writes its baseline
checkpoint. `open` requires initialized, valid progress for the exact stream.
Missing progress after initialization is corruption, even if no business
records exist; it cannot reset sequence allocation to a pristine baseline.

## Typed Work

The domain API supports only:

- Primary queue creation with the existing queue configuration.
- Primary queue immediate byte-body Send, with message ID, optional TTL, and
  optional session ID.
- Blank progress entries.
- Effective membership replacement with bounded, versioned opaque bytes.

Restricted command constructors cannot carry arbitrary `CommandKind`, fenced
work, receives, settlements, atomic groups, rich envelopes, scheduled work,
deletion, or topic publications. A valid topic target is a normal queue-only
refusal; it never routes through fanout. Membership interpretation belongs to
the [separate library adapter](experimental-state-machine.md). The domain does
not infer voter validity from those opaque bytes.

The new path limits bodies to 256 KiB, canonical encoded entries to that limit
plus 4 KiB, membership payloads to 4 KiB, and checkpoint envelopes to 8 KiB.
These limits do not change `QueueConfig` or existing standalone limits.
Decoded identifiers are revalidated before hashing or business preparation.
Hashing streams a borrowed, frozen version-1 entry schema through a byte-limited
SHA-256 writer, without allocating another encoded body. Entry size limits are
trusted-log admission requirements; an invalid or oversized committed entry
fails without advancing progress.

The pure `CommittedQueueWork::entry_mark` helper computes the same bounded
canonical mark for [owned storage-pair validation](experimental-replica-preparation.md).
It does not apply work, authorize a predecessor, reconstruct an outcome, or
prove quorum commitment. Hashable business refusals remain representable.

## Atomic Progress

The fixed checkpoint key is `0x12`. Its independent `SWYC` version-1 envelope
records the stream identity, full current entry identity (term, node, index),
canonical entry fingerprint, exact predecessor mark, committed timestamp
watermark, and effective membership with its source entry identity.
Decoding rejects unsupported headers, trailing bytes, oversized payloads,
noncanonical encodings, impossible predecessor positions, and inconsistent
membership sources for retained positions.

The first entry has index zero. Later entries require the exact previous mark
and a checked contiguous index; terms cannot move backward. Current-position
replay requires the same full identity, original predecessor, and fingerprint.
It returns `AlreadyApplied` without rerunning business preparation, allocating
a sequence, committing again, or reconstructing earlier outcomes and effects.
Changed work or identity at that position is a conflict. Older history cannot
be independently replay-certified from this single checkpoint.

Successful queue work stages existing business mutations and its business clock,
then appends progress before one privileged atomic commit. Blank and membership
entries commit only progress. A normal deterministic business refusal also
commits only progress: no business clock, counter, or message changes survive.
This differs deliberately from standalone refusal, which writes nothing.

Refusals are classified by operation and stage, not by a broad error-variant
allowlist. Invalid caller configuration is a normal refusal; the same invalid
configuration decoded from storage is fatal. Truly absent queues differ from
orphaned live identities. The valid exhausted sequence sentinel differs from
corrupt counters. Errors after staging a counter discard the entire private
business batch. Bounded probes reject sequence reuse and orphan runtime records;
fresh creation cannot adopt records whose retained owner identity is missing.
Storage, codec, malformed, or inconsistent touched metadata
never become successful checkpoint-only refusals. Unrelated untouched records
are not exhaustively audited by each entry.

The committed watermark includes queue timestamps even when work is refused;
the ordinary business clock stays tied to business mutations. A subsequent
queue command cannot move behind that watermark. A business clock ahead of
progress fails closed. A future leader must recover this watermark after a
leadership/read barrier, not use only the ordinary business clock to stamp work.

## Failure Boundaries

A physical commit error may occur before persistence or after the entire batch
is durable. Both produce an outer error and no successful application effects.
Further application on that machine is poisoned. Reopening reads business state
and progress together; it does not infer rollback from the lost response.
Read-only inspection remains possible after poisoning.

Memory and Fjall tests cover atomic batches, refusal origins, replay and
conflicts, membership, malformed progress, and failure before versus after
commit. Fjall tests additionally terminate a bounded test child immediately
before commit or after the real fsynced batch returns but before application
returns, then reopen and verify complete state and replay without duplicate
sequence allocation. Those tests prove abrupt exit at the selected boundaries,
not arbitrary mid-journal power-loss behavior.

This is committed-log replay protection, not client retry deduplication. A new
Send intent after a lost response may allocate another sequence. There are no
durable reply receipts, live authorization leases, automatic retries, snapshots,
log/vote persistence, network consensus, leader read barriers, or process-memory
bounds supplied by this API. Existing whole-store snapshots remain unsuitable
as a streaming production replication snapshot. All those runtime requirements
remain separate work before quorum durability or production can be claimed.
