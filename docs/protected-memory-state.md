# Protected Memory State

This opt-in library API provides a fresh separate Memory object whose
only writer operation replaces business records, initialization, both live
catalog components and one nonempty bounded opaque fence together. It is a
low-level atomic coupling capability, not a canonical source-selection protocol,
domain adoption permission, consensus proof or durable state.

## Explicit Writer And Reader

MemoryProtectedStateStore::new (and Default) makes a separate private object.
The unique writer is not Clone and implements no ordinary or replica storage
trait. It does not wrap, convert or share the inner object of an existing
MemoryStore, MemoryReplicaStore or MemoryCatalogReplicaStore. Existing defaults
and admission contracts are unchanged; no WriteBatch/backend escape is exposed.

ProtectedStatePublication::new borrows sorted rows, a SnapshotCatalogRecord and
an opaque fence. Input construction checks shape only. Arbitrary in-bound bytes
remain untrusted; no image parser, digest, checkpoint or expected-old receipt
is supplied. publish consumes the borrowed descriptor, not caller-owned generic
payloads. It retains no input borrow after success.

reader returns a cloneable MemoryProtectedStateReader. ProtectedStateReader
provides capture_protected_state, with no default composing unrelated reads.
The owned non-Clone StoredProtectedState exposes is_initialized, records,
live_catalog, fence and logical_payload_bytes. It holds no originating handle
and can outlive every writer/reader. It can immediately be stale and has no
writer, mutation, conversion, authenticity or publication-permit meaning.

## Closed Complete View

Pristine is false, empty rows, absent catalog and absent fence. Successful
publication is initialized with both catalog components present and a nonempty
fence; empty rows and present empty catalog vectors are legal. Half-catalog
and malformed bool encodings are not representable in this Memory object.

Limits are 65,536 rows; each nonempty key <=1,024 bytes; each value <=266,240;
business total <=67,108,864; catalog metadata/artifact <=8,192/67,108,864; fence
1..=256; and combined logical payload <=134,226,176. These count byte lengths,
not Vec capacity, collection metadata, backend/internal allocations or total
process memory. Nonempty-key policy is new here; existing reader policies are unchanged.

All offered shape checks precede explicit candidate copies. capture holds one
read lock through complete current-view validation/preflight and all owned
copies. Its second pass reconciles numeric totals and row count/order against
that same view. No per-row first-pass inventory or ordinary allocating snapshot
fallback is used. Candidate/result Vec reservations are fallible, but Arc setup,
old/candidate/output coexistence, spare capacity and aggregate RSS/OOM are not
covered. No exact-maximum success or all-allocations-fallible claim is made.

## Publication Outcomes

publish checks poison/current closure and complete shape before immediate fence
inequality, prepares off-lock, and rechecks before one write-locked whole-object
move. Displaced ordinary buffers drop after unlock. Fence inequality means only
different bytes from the current value: canonical phase, monotonicity, history
and anti-rollback are not established. Body/catalog no-op still needs a distinct
fence; a later return to an older fence is allowed.

ProtectedStateError is fixed/static: LimitExceeded, Allocation, InvalidInput,
InvalidState, UnchangedFence, Poisoned or PublishUnknown. Known input,
allocation or unchanged-fence refusal leaves prior state readable. Invalid
current state refuses without repair. An entered returned error or unwind marks
shared poison before unlocking; unwind retains its original propagation and
Rust lock poison is terminal. Later mutation/capture refuses before copying.
No postquery, rollback, retry, clearing or invented success is provided.
Previously owned views remain ordinary descriptive data, never active permits.

Debug for new wrappers is numeric/presence-only and errors have no source cause.
Explicit immutable accessors intentionally expose bytes; existing StoreSnapshot
Debug is not a secrecy guarantee. Implementing the reader trait is a trusted
contract, not compiler-certified provenance.

## Evidence Boundaries

The suite contains 26 regular tests, eight compile-fail examples and one
compile-only public API example. Coverage includes closed
pristine/published state, complete replacement and lifetimes, refusal before
entry, entered poison/unwind and finite old-or-new reader concurrency. Actual
finite test threads join before errors/assertions. Helper-lock blocking is not
a public phase callback, and private faults are Memory-only control evidence.

One serial 64-MiB+1 catalog vector tests the existing borrowed factory refusal.
Scalar capacities/overflow do not stand for maximal successful data or heap-OOM.
No native file, paired role, intent, physical opener, actual native join,
SafeReopen, backend power-loss, SDK, listener, production default or historical
timeout fix is implemented. Canonical binding and semantic publication/adoption
remain separate work. Existing catalog readers, the replacement planner and
private paired formats supply no conversion into this mutation capability.

## Verification

The initial focused run passed all 26 new regular tests. Ten additional serial
runs passed the same 26 identities each (260 additional focused executions).
All eight compile-fail examples passed and the public API example compiled;
the no-run example was not executed as a runtime test.

Storage passed 239 checks, domain 1,371 and cluster 797. The default workspace
run and the completed all-feature recheck each passed 4,941 checks across 134
groups, with ten existing ignored tests. Exact name/status multiset comparisons
against the preceding checkpoint add only these 26 regular and nine documentation
checks, with no removed or changed results.

The first all-feature attempt stopped after 57 completed groups at the existing
cbs_grants_send_but_not_listen test with Error: RemoteDetached (3,728 passed,
one failed). The unchanged test passed ten narrower package-only isolated runs,
and its complete eight-test package suite passed. Those runs used a different
feature-unified binary from the failed broad attempt. The fresh full-workspace
all-feature recheck passed, but no cause or fix for the original failure is
established; the failed log is retained.

Formatting, default and all-feature warning-denying Clippy, both workspace
builds, protobuf descriptor generation and diff checks passed. Verification
used one shared Cargo target, serial commands and two low-priority CPU cores.
The eight storage source files stayed byte-identical through the final gates.
Existing catalog/capture verification sections were preserved unchanged.

These are library and finite controlled-test results, not SDK verification,
native-role join/adoption evidence or production activation.
