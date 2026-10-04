# Owned Committed Image Export

`ExperimentalStateMachine::create_with_image_export` and
`open_with_image_export` explicitly enable the bounded domain exporter on the
existing native state owner. Both require the matching reader to implement
`storage::BoundedStateStore`. Ordinary `create` and `open` leave this capability
disabled, even on backends that implement bounded reads. Their constructor and
recovery behavior are unchanged.

This is not the replication library's snapshot implementation. Snapshot build,
receive, install, and current-snapshot behavior remain unchanged and unsupported
as described in [the state-machine adapter](experimental-state-machine.md).
No runtime uses the new constructors. Server startup, transport, disk formats,
history retention, and production refusal remain unchanged.

## Owned Work

`export_create_send_image(&mut self)` returns a `Send + 'static` future which
does not retain a facade borrow. Creating it performs no admission or I/O.
First poll attempts one request through the existing owner. No public cloneable
export handle, physical reader, writer, or mutable artifact buffer is added.

Every accepted export reserves the complete 64 MiB artifact limit, not a small
scalar-query charge. That equals the current owner byte budget: an accepted
export excludes other accepted packets until it completes, and other packets
can make export admission return `Busy`. No waiting, automatic retry, partial
capture, or unbounded fallback is added.

Accepted work retains its job and byte reservation through actual owner
completion, even when its caller disappears. Result publication uses the
existing completion barrier: a returning caller observes capacity already
refunded. The returned immutable image becomes caller-owned and does not retain
an owner reservation or store handle.

An unpolled factory holds only the private channel/admission handle. Facade
shutdown closes admission, drains accepted work, and joins the native owner even
while that factory remains alive. Polling it after shutdown refuses admission;
it cannot keep the physical backend open.

## One View And Failure Policy

The owner checks adapter poison before invoking the capability. The
[domain exporter](committed-image-export.md) performs exactly one complete
bounded capture, structural encoding, and current CreateSend semantic validation.
The native adapter then decodes that returned artifact and applies its existing
full-identity and membership recovery checks to the captured checkpoint only.
Export issues no separate checkpoint query, point read, scan, initialization
query, ordinary snapshot, or write. Constructor and separately armed retirement
report reads are outside the export operation.

`StateMachineImageExportError` contains only static enum causes:

| Cause | Policy |
| --- | --- |
| `Owner(StateMachineError)` | Existing admission, owner loss, or adapter poison |
| `Domain(CommittedImageExportError)` | Exact bounded domain export refusal |
| `Disabled` | A healthy owner lacks the explicit capability; no source I/O |
| `IncompatibleMetadata` | Captured native progress or membership is incompatible; nonfatal |

Domain `ReadFailed` or `Poisoned` also poisons the adapter. Later export, apply,
and trait progress queries refuse before source I/O. Limits, output reservation
failure, unsupported profiles, consistency refusals, and native metadata
incompatibility do not by themselves prove corruption of an arbitrary opened
source and remain conservative nonfatal export refusals. They do not pass
through the adapter's blanket trait-error poisoning path.

A capture panic uses the owner's existing unwind handling: the active packet
publishes a static panic failure and refunds its reservation, admission closes,
and actual shutdown joins the failed thread. Packet publication can race the
caller's shutdown close, so a later inert factory may observe `Panicked` or
`Closed`. Static reply privacy does not cover panic-hook stderr or backend logs.

## Bounds And Authority

The owner charge bounds accepted operation capacity, not combined heap use or
RSS. Captured raw key/value data and the complete artifact each have separate
64 MiB logical limits and may coexist during encoding. Backend cache and
materialization, metadata, spare allocation capacity, ordinary constructor
reads, and caller-retained images are outside the single admission charge.

Successful checks grant no source authentication, log ancestry, anti-rollback,
historical operation result, quorum, cluster-membership provenance, installation,
or history-purge authority. This operation owns only the state-machine owner;
it does not establish a new combined log/state-owner shutdown guarantee.

## Verification Scope

Observed matching readers on Memory and Fjall pin the exact four capture caps
and count every other read and write independently. Ordinary snapshots refuse
any fallback. Tests cover disabled constructors, explicit opt-in reopening,
maximum body/ID/session, TTL, a duplicate sequence hole, membership, and a
refusal watermark gap. Quota, legacy, malformed record, missing index, and
native metadata refusals preserve source bytes and permit healthy later work.
Physical read failures and before/after commit faults pin no-I/O poison behavior.

Real complete captures pause after the stable view exists and outside backend,
writer, and observation locks. Tests change a later checkpoint to prove native
recovery uses only captured progress; lose an accepted caller to prove full
charge and `Busy` behavior; and keep shutdown pending until the actual capture
is released. The export-specific panic test checks the charge before failure,
publication/refund, actual failed join, unchanged source, and healthy reopening.
Gate waits have a deadline and are released before join; no whole owning scenario
or native storage operation is cancelled by a timeout.

A dedicated Fjall case joins a lost capture, drops every old physical store
handle, and opens the same directory while an old unpolled factory is alive.
The recovered image is byte-identical; a second real directory reopen checks
the complete source. Compile-fail coverage enforces the bounded-reader constructor
requirement, and compilation checks exported futures are owned and `Send`.

No forced real allocator failure, fully populated 64 MiB/65,536-row owner export,
RSS instrumentation, runtime snapshot metadata, installation, transport activation,
automatic compaction, or broader source-profile support is established here.

The owner-export checkpoint passed all 24 focused checks, including its
compile-fail contract. The 23 regular export cases also passed ten complete
repeat runs, for 230 additional executions. The all-feature cluster suite
passed 433 tests, and the default workspace passed 4,044 with ten opt-in SDK
gates ignored. Formatting, strict Clippy and builds in both feature
configurations, protocol descriptor validation, and whitespace checks passed.
The full all-feature workspace and live SDK gates were not rerun for this
isolated opt-in owner increment.
