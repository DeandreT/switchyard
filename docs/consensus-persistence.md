# Consensus Persistence

The opt-in `cluster::consensus` API provides local persistence for one fixed
three-voter group. It does not run elections, transport, quorum commitment,
application projection or server routing. Production startup remains refused.
Its caller is a trusted future consensus driver: writing a local commit index
does not prove that a majority agreed.

## Core And Dependency Contract

`raft` is pinned to **0.7.0**, with default features disabled and only
`protobuf-codec` enabled. `protobuf` is pinned to **2.28.0**; `raft-proto` 0.7.0
and the complete dependency resolution are retained in `Cargo.lock`. Use the
repository's Rust 1.97.1 toolchain. Fjall stays pinned to 3.1.13.

The core and `raft-proto` are Apache-2.0. `protobuf`/`protobuf-codegen` and
`getset` are MIT. Select the MIT option for `slog` (not its MPL alternative),
`fxhash`, `bytes`, byteorder, rand/rand_core/rand_chacha, getrandom, libc,
cfg-if, ppv-lite86, zerocopy, serde/erased-serde, anyhow, thiserror, rustversion,
proc-macro2/quote/syn, regex and bitflags where offered. `unicode-ident` also
requires the permissive Unicode-3.0 terms. WASI offers an Apache-2.0 option.
The build-only `protobuf-build` helper is Apache-2.0; its bundled Protobuf
compiler is BSD-3-Clause licensed upstream. Redistribution must preserve the
applicable notices; build tools are not runtime broker dependencies.

`protobuf-build` uses a compatible 3.x `protoc`, including its packaged 3.9.0
fallback when the host compiler is incompatible. No host installation or global
tool replacement is required. Review registry manifests, enabled features,
license files and upstream compiler notices when updating the lockfile; a
package's alternative license expression is not a requirement to select every
alternative. `cargo metadata --locked --format-version 1` and
`cargo tree --locked -p raft --edges normal,build` expose the resolved dependency
closure. This is dependency/license selection, not a security or release audit.

### Future Driver Ordering

The exact `RawNode` contract is `ready()` followed by `advance_append(Ready)`
and later `advance_apply_to(actual_applied_raft_index)`. Consume a Ready before
stepping, campaigning or proposing again. Persist its entries and hard state
before releasing `persisted_messages`; a conservative driver can persist before
releasing every message. Snapshot input is unsupported, never silently ignored.

`advance_append` returns a `LightReady` that can contain a new commit index,
messages and committed entries. Persist this second-stage commit authority too,
even though the core permits asynchronous commit-only updates. No projection,
application or client acknowledgement may precede that authority. Do not call
the convenience `advance` as a substitute for real application completion.
The driver must respect retained-log admission bounds before accepting more
work and must not advance the core after failed persistence.

`read_index(context)` returns `ReadState` with the same request context; a future
consistent read additionally needs current-term/quorum authority and application
through its returned Raft index. This foundation does not implement that read
barrier or a clock-based leader lease.

## Exclusive Backend And Outer Fence

Ordinary `ACTIVE_STORE_FORMAT` remains **3**. The separate
`ConsensusFjallStore` opener creates **storage profile 4** only in an absent or
empty directory. This is not a queue/profile encoding version. Existing
directories are recovered without creating missing keyspaces and require exactly
`meta`/`records` and the sole four-byte format-4 marker. Legacy format 3, missing
markers, foreign inventories and extra metadata refuse without logical writes.
Native Fjall recovery may still perform internal IO; this is not a byte-identical
directory guarantee. An interrupted initial outer-marker creation is not repaired
by the existing-directory opener.

`ConsensusMemoryStore` is an independently owned volatile backend. Neither
backend implements `Clone`, `StateStore` or the sealed image-install interface.
Their inner handles are private, so moving a backend into `ConsensusStore` leaves
no normal raw/domain writer alias. Atomic persistence uses the existing Fjall
SyncAll batching and directory owner, not a second database opener. A trusted
custom `ConsensusBackend` must enforce the same atomicity, ordering, durability
and exclusion of all external/raw/topology writers; the trait cannot prove them.

Normal Fjall opening still refuses profile 4. Native snapshot/image inspection
and offline format upgrade also refuse it; consensus wrappers cannot enter the
legacy generic snapshot/install/anchor/compaction APIs. There is no profile-3 to
profile-4 migration, downgrade, marker-stripping rollback or snapshot conversion.
Local F0/F1 history is never adopted as quorum history. Opening a populated legacy
directory with the new opener does not stamp it as fresh.

## Versioned Records And Bootstrap

`0xF2 || "switchyard/consensus" || 0x00` owns root/identity (tag 0), hard state
(tag 1), and big-endian indexed entries (tag 2 plus eight bytes). Each value has
version 1, a four-byte payload length and a separately scoped SHA-256 digest.
Canonical protobuf bytes retain entry term/index/type/data/context and hard
state term/vote/commit. Unsupported versions, unknown fields, malformed records,
holes, invalid/foreign-namespace proposals, term regressions, unknown keys and foreign namespaces
refuse on open without repair. Checksums are not authentication or anti-rollback.

The immutable root contains cluster/group identifiers, canonical namespace,
exactly three sorted nonzero unique voter IDs and concrete unique socket
endpoints, local replica ID and a configuration fingerprint. Every open supplies
the expected identity. Changing endpoints, membership, namespace, cluster/group
or local replica refuses rather than reinterpreting stored history. Endpoint
declarations and fingerprints are not peer authentication or proof of three
physical machines. One store belongs to one group/namespace.

Bootstrap accepts only an empty record space and atomically writes root and
empty hard state. Initial membership comes from this validated identity, not
synthetic domain commands. All namespace entities initially share this group;
there is no dynamic membership, placement move or multi-group key rewriting.

## Append, Recovery And Admission

`persist(entries, hard_state)` validates one complete update before one atomic
batch. Terms and commit indexes cannot decrease. A persisted nonzero vote cannot
change or clear in the same term; a new term may begin with vote zero. Votes must
name configured voters; commit cannot exceed the resulting contiguous log.
Entries must have ordered nonzero terms/indexes within the persisted term.
Exhausted `u64::MAX` term/index values refuse rather than risking core overflow.

Identical overlapping entries are no-ops. A matching shorter append preserves
its existing later suffix. Only an actual differing-term conflict strictly after
the old committed prefix atomically replaces that suffix. A same-term byte
conflict or any committed-entry conflict refuses. There is no committed-history
truncation, compaction or automatic promotion of an uncommitted tail.

An owner retires before invoking backend apply. A returned error or original
escaping panic can follow persistence of the entire batch. No retry, rollback or
compensation occurs. All subsequent IO refuses until explicit consuming `reopen`
or full directory close/reopen validates the actual state. A bootstrap failure
returns no owner; reopening determines whether its original batch persisted.
Successful local persistence is not a recovered command outcome or quorum proof.

Tail reads return at most 64 entries. Full open validates in bounded record pages;
the core Storage adapter honors its requested range/size and at-least-one-entry
rule over the caller-capped retained log. The default retained bounds are 4,096
entries and 256 MiB of logical entry keys/framed values. A full update can span
more than one read page. Bound exhaustion refuses admission, never evicts history.
Changing bounds on reopen does not rewrite history; a now-overbound log refuses.
These are logical limits, not physical disk, backend allocation or fsync hardware
certification. Snapshot support requires a separate Raft-aware checkpoint format.

The proposal-data ceiling is the existing `MAX_DURABLE_PROPOSAL_BYTES` (1 MiB),
checked to fit `MAX_JOURNAL_PAYLOAD_BYTES`. Context has its own bounded 1 MiB
budget; canonical protobuf overhead is additional. Consensus framing therefore
does not reduce the application payload ceiling. Original proposal bytes,
timestamp and retained entity authority are decoded for validation but never
re-encoded or replaced on persistence/reopen.

## Raft And Application Indexes

Raft indexes count every entry. Application ordinals count only nonempty normal
entries containing exact `DurableProposal` bytes, starting at one. Normal empty
entries are consensus blanks. Configuration entries are typed controls, not
domain expiry commands. Fixed membership comes from bootstrap's ConfState;
all V1/V2 configuration log entries refuse, including empty V2: the selected
core interprets that as leaving joint configuration, not a blank in this
non-joint group. Membership changes require their own coordinated implementation.

`application_ordinal(raft_index)` is defined only through the persisted committed
frontier and returns `None` for controls. For proposals it counts proposals in the
retained prefix. An uncommitted ordinal is not a projection identity because
tail reconciliation can change it. The complete log remains retained, making
the committed mapping reproducible after reopen. Future checkpoints must retain
the last-included Raft term/index, immutable configuration and application-prefix
count before any Raft prefix can be removed.

The future application owner must project each committed proposal once, in this
order, into the existing proposal-only F0 journal and indexed F1 effects. Raft
blanks/configuration controls must never be passed to the old proposal decoder
or consume fabricated domain timestamps. This API writes no F0/F1/domain rows
and currently refuses such mixed records on open; adding projection requires
an explicit coordinated validation contract. Existing indexed checkpoints do
not recover historical outcomes or provide transparent exactly-once retries.
