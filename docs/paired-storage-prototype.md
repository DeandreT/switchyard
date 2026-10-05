# Controlled Paired Storage Prototype

The storage crate contains a private, unconditional `cfg(test)` physical
prototype for two new state/log directories. It is not a production API, storage
trait, format producer, generic raw writer, engine adoption path or recovery tool.
Existing public constructors, ordinary profiles, domain codecs, runtime behavior
and dependencies remain unchanged.

The [paired transition model](native-image-paired-model.md) remains a separate
pure semantic experiment. The [generic marker guard](paired-marker-refusal.md)
is a separate production refusal, not a paired constructor. The existing
[opaque catalog profile](snapshot-catalog-storage.md) does not itself supply a
selection fence or a cross-directory transaction.

## Closed Physical Records

Unique private Memory/Fjall capsules represent one State and one Log role. Their
inventories contain owned bytes, not database, keyspace, snapshot or reader
handles. Neither capsule nor returned inventory is a public capability or an
adoption, completion, purge, safe-reopen or resource-custody receipt.

The private fixture formats are `0xa000_0000 | ACTIVE_STORE_FORMAT` for State and
`0xb000_0000 | ACTIVE_STORE_FORMAT` for Log. Their exact profiles are
`committed-state-paired-adoption-v1` and `committed-log-paired-adoption-v1`.
They are test encodings, not supported production profiles or conversion targets.
The fixture does not remove or relabel a permanent binding or rewrite its profile
after first publication. Generic openers remain fenced by marker presence.

Common metadata comprises format, profile and initialized headers, permanent
binding `[0x22, 0x01]` and creation stamp `[0x22, 0x02]`. Log additionally stores
an opaque manifest at `[0x22, 0x03]`. Binding and stamp are exactly 112 bytes,
with closed `SWAP`/`SWCR` magic, schema 1, explicit role and reserved/phase byte,
three distinct nonzero IDs, a node field, nonzero stream and caller-supplied seed
bytes. Manual fixed-size decoding rejects wrong length, schema, role, reserved
bits, phase or binding. It does not deserialize arbitrary collections.

State has seven closed logical metadata keys for header, fence, stage binding,
stage metadata/artifact and live metadata/artifact. Header and fence are required
after seeding. Stage components may be independently present: partial staging
is a bounded fact, not an instruction to repair it. Live components must be
both absent or both present; a composite requires both present. Present empty
components remain distinct from absence.

Log has five closed record controls for header, progress, baseline, optional
intent and optional final fence, plus exactly keyed entries. Header, progress
and baseline are required after seeding. Unknown or duplicate logical controls,
wrong roles, incompatible profiles and malformed fixed records are refused.

Seed manifest, digest, business, catalog, selection/stage/header and log-control
bytes remain opaque. Presence and exact equality do not establish a computed
digest, canonical seed/image/native fields, sufficient vote, trusted suffix,
authentication, ancestry, anti-rollback or source history. The prototype parses
neither SWAI model records nor frozen checkpoints, native images or log entries.
A changed opaque fence is physical byte evidence, not a valid semantic phase.

## Bounded Complete Views

Borrowed inputs undergo all closed-shape, count, component and checked-aggregate
checks before owned preparation or commit entry. Role capture first checks all
metadata and record keys/counts/sizes and required fixed controls before any
caller-owned inventory/body copies. Fixed bounded stack/scalar control decoding
is allowed during validation; it is not an owned inventory/body copy.

Memory validates and copies under one read lock. Fjall uses one pinned snapshot
across both keyspaces for iteration, `size_of`, fixed-control retrieval and final
copying, checking exact returned lengths. There is no live-read substitution,
prefix-only capture or unbounded fallback. Each role has one stable view, not an
atomic snapshot spanning the two separate directories.

| Logical Component | Fixed Limit |
| --- | --- |
| Business rows | 65,536; nonempty keys at most 1,024 bytes, values at most 266,240 bytes; 64 MiB aggregate keys plus values |
| State metadata | At most 12 closed keys; 256-byte header/fence/stage binding, 8 KiB stage/live metadata, 64 MiB stage/live artifacts |
| Log metadata | Six closed keys; manifest at most 128 KiB |
| Log records | Five controls plus at most 256 increasing unique 9-byte entry keys; 64 MiB aggregate entry values |
| Log controls | 256-byte header/progress/final fence, 16 KiB baseline, 128 KiB intent |

Conservative closed-shape ceilings for captured logical keys plus values are
201,344,103 bytes for State and 67,390,821 for Log. These use a 64-byte profile
allowance; admitted fixed profiles are 34/32 bytes. The planned state seed is
capped at 65,545 Puts and 134,226,944 logical bytes, log seed at 265 Puts and
67,272,704 bytes, and the
composite at 131,076 mutations and 201,335,808 bytes. Pure shape/budget cases
exercise maxima and overflow without requiring every maximum to coexist.

Explicit output buffers reserve fallibly, but backend lookup, iterator/Slice/get
materialization, staging, tree/collection allocations and spare capacity are
outside caller-copy budgets. These limits do not bound aggregate RSS, disk or
write amplification, concurrent retained inventories or every allocation path.
They establish neither allocator-exhaustion handling nor OOM safety.

## Controlled Acquisition And Creation

Private fixtures exclusively reserve two new children beneath a test-owned
parent, retaining prefix paths on failure. They never adopt, convert, migrate,
relabel or automatically delete an existing role. Opaque input preflight precedes
native acquisition. Distinct controlled children and simultaneously acquired
database locks provide fixture custody, not hostile-path alias protection.

The controlled EmptyNative boundary exists only after both databases and each
role's known metadata and records keyspaces have successfully been acquired.
A partial/native acquisition failure retains its private original cause and
paths with redacted formatting, releases acquired caller handles and performs
zero application writes. Backend effects remain possible. Such a failure is
not a guaranteed valid-layout reopen fixture, EmptyNative diagnosis or repair
claim.

One fixed creation attempt publishes these steps in order:

1. Log `Prepared`: one Fjall `SyncAll` batch publishes its common headers,
   permanent binding, Prepared stamp, opaque manifest/controls and complete seed
   entries. State is still empty.
2. Only known Log success permits State `Ready`: one `SyncAll` batch publishes
   its common headers, permanent binding, Ready stamp, complete business, optional
   live pair, header and opaque Ready fence atomically.
3. Only known State success permits Log `Ready`: one `SyncAll` batch changes
   only its creation stamp.

Memory mirrors the same publication order with single-lock role replacement;
it is not durable across process exit. Retained physical prefixes are both empty,
Log Prepared/State empty, Log Prepared/State Ready, and both Ready. No selected
business is published under an earlier ordinary/catalog stamp. There is no
creation resume, journal completion, clearing, retry or rollback.

Every marked role refuses all three generic durable openers before application
stamps or record-keyspace acquisition. Empty roles can still receive an ordinary
generic stamp. Separate successfully acquired empty fixtures test all three
openers on each role, without seed business; once ordinary-stamped, those
fixtures supply raw small-dictionary observations only and are never reused as
paired mutation or reopen authority. Those private raw helpers are not a bounded
production inspector.

## One-Shot State Composite

After both roles are Ready, one private composite attempt validates complete
replacement rows and a present live pair, captures complete bounded old business,
then plans Delete for every old business key and Put for every replacement
row. Initialized state, both live components and the opaque fence join those
mutations in one Fjall `SyncAll` batch. Memory prepares the whole candidate away
from the write lock and publishes it under one write lock.

The new fence must differ byte-for-byte even when business and catalog bytes
are unchanged; a no-op does not elide the fence publication. Permanent controls,
creation stamp, header, partial staging and the entire Log remain unchanged.
This is a physical fixture batch, not a semantic selection, adoption journal,
tail deletion policy or recovery mutation. The attempt is one-shot even when a
known pre-entry refusal prevents its commit.

Every error after entry to a creation/composite commit poisons both capsules and
returns `CommitUnknown`. Closed faults immediately before the backend call or
after a real successful `SyncAll` exercise that same conservative policy.
A genuine backend commit error has the same contract. Neither presumed rollback
nor a postcommit query manufactures a success result. Poisoned capsules refuse
later capture and mutation before backend access; invariant/read failures also
end mutation admission. Counters describe observed entry/calls, not persisted
outcome or task-completion proof.

## Reopen And Evidence

The 27 regular tests cover fixed controls and cap arithmetic, opaque inputs,
one-view old/new atomicity,
live absence/empty/partial distinctions, unchanged staging/log, distinct no-op
fences, every creation/composite before/after fault, poisoned no-postquery
behavior, marked generic refusal, separate ordinary EmptyNative admission,
redacted acquisition failures and swapped roles/bindings.

Controlled durable observations first drop all original capsules, batches,
database, keyspace, snapshot, reader and clone handles. Reopen acquires only
privately created successful native layouts, checks exact known keyspace
presence/count before acquiring application handles, and returns inspection-only
owned inventories. Valid outcomes independently reopen both roles twice, closing
all physical handles between calls while the first complete owned inventory
survives. Refused logical dictionaries are observed without stamping or repair;
logical damage is injected by normal backend batches, never by editing native
files.

Pinned Fjall recovery may replay journals, sync files, create internal metadata
and delete stray/uninitialized native keyspaces before caller validation. This
is not arbitrary existing-only inspection, noncreating open, filesystem
immutability, malformed native-file recovery or repair prevention. No missing
application keyspace is recreated by the controlled reopen helper. Partial
acquisition failures remain outside its supported provenance.

Child-process cases exit before a real backend call or after returned
`SyncAll` for each creation step and the composite. Parents retain paths and
actually reap the child, then independently reopen both roles twice. The runner
uses null streams, a 20-second observation deadline and kill/wait fallback on
unsuccessful paths. It supplies no hard kernel-cleanup deadline or mid-sync,
torn-journal, power-loss or all-crash-timing proof.

The heavy stored-component case includes actual 64 MiB plus one-byte live/stage
values on both backends. It must run serially after resource checks; input,
backend and retained output copies can coexist. Each controlled database requests
one Fjall worker and a pair opens at most two concurrently per case. Generic
openers retain their defaults. Neither setting is a process-wide thread/resource
cap. No new application/native-owner thread or Tokio task is introduced, but
Fjall starts backend workers; Database Drop's counter wait is not an actual
native-worker join receipt.

Canonical trusted seed/image/log/native/vote/tail admission, real durable-intent
ordering and recovery policy, domain selection-fence capability, sealed writer
handoff, both application-owner joins and stable live fallback custody remain
separate prerequisites. There is no runtime, engine, network or SDK activation,
whole-tree custody, safe-reopen authority or explanation/fix for historical SDK
timeouts.

The separate [aligned seed inspector](aligned-seed-inspection.md) checks canonical
image/catalog and empty-log agreement. It does not interpret this prototype's
opaque physical records or grant either capsule's publication authority.

## Verification

The initial focused compile failed because private test-module imports shadowed
the backend crate name. Eight test-only paths were made explicitly absolute;
the next focused run passed all 27 tests. The first broader strict-lint run then
refused one indexed test loop. Its three creation-prefix cases were rewritten
as an explicit table, without changing assertions or production storage logic.
Both failed logs were retained, and the final source was frozen after formatting.

The final focused run passed 27 tests; ten serial repeats passed another 270.
The storage suite passed 173 tests. Default and all-feature workspace runs each
passed 4,670 tests across 131 result groups, with the existing 10 ignored tests.
All 20 final checks passed: formatting, both strict-lint modes, ten focused
repeats, storage, both workspace test/build modes, protobuf and whitespace checks.
All 14 source digests remained unchanged through verification. Compilation was
limited to two low-priority cores, and tests ran one at a time so large component
fixtures did not overlap. Disk, shared target and memory headroom were checked
before and after the broader builds; no additional build cache was created.

No live SDK gate was selected. Passing these checks neither explains nor fixes
the historical SDK timeout or earlier cluster leadership-change failure, and
does not establish production paired adoption or whole-resource custody.
