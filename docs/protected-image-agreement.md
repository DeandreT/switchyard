# Protected Image Agreement

## Current Boundary (2026-10-06)

`check_protected_create_send_layout17_image` returns
`CheckedProtectedCreateSendLayout17Image` using the role-2 proof, with the same
expectation, fence, exhaustive row agreement, and non-authority boundaries.
Its current profile requires canonical generation-1 NonFinite Modes and
refuses finite Modes and Usage/Charge. The role-1 checker described below is
preserved as a separate pure API. Its historical verification receipts remain
unchanged and are not new layout-17 execution evidence.

`check_protected_create_send_image` checks one already-owned
`storage::StoredProtectedState` against an independently selected
`CreateSendImageExpectation` and expected opaque fence. It returns a borrowed
`CheckedProtectedCreateSendImage`, not publication or installation authority.
It reads no originating store and cannot mutate or poison one.

## Agreement

A successful check establishes these descriptive agreements:

- The capture is initialized, with a complete live catalog and nonempty fence.
- The artifact passes the unchanged complete container decoder and
  `CreateSendV1` business validator.
- Its complete checkpoint equals the expected checkpoint, including stream,
  last and previous full marks, timestamp and complete optional membership.
- Its complete serialized length and whole-artifact SHA-256 match the
  expectation. The digest includes the container checksum bytes.
- Its opaque fence equals the independently selected expected bytes.
- Every captured business row equals every validated artifact row, with equal
  cardinality, ordered keys and values, and simultaneous exhaustion.

The checked view is non-Clone and has private fields. Its sole constructor is
the checked function. `protected_state()` exposes the immutable borrowed capture;
`image()` exposes the existing validated image. Artifact and business-row bytes
stay borrowed; existing decoding owns bounded checkpoint metadata. No second
artifact copy or self-referential object is introduced.

The view's lifetime depends only on the capture. Neither expectation nor
expected-fence inputs are retained. The originating handles can be released
after producing the owned capture; using that capture does not inspect a
current backend or retain its physical ownership.

## Refusals

Expected artifact-length, membership-payload and fence bounds are checked
before image recovery. Oversized inputs return `LimitExceeded`; an empty expected
fence or zero expected stream returns `InvalidExpectation`. Initialization and
complete capture shape follow. Existing complete container and business
validation precede lazy full-checkpoint, complete-length and whole-digest
comparison. Fence comparison then precedes exhaustive row equality.

Earlier refusals can mask later mismatches. No partially checked view escapes.
`ProtectedCreateSendImageError` uses copied fixed variants and static messages,
without recursive sources. The checked view's Debug contains only row and byte
counts, not artifact, metadata, fence or checkpoint contents.

`IncompleteState` is defensive: the current private storage DTO cannot publicly
construct an initialized half-state. `Allocation` preserves the existing
container error mapping; it does not add a recoverable allocator for every
decoder or semantic-validation operation.

## Limits And Trust

The checker reuses the existing logical limits: artifact at most 64 MiB,
checkpoint membership payload at most 4 KiB and expected fence at most 256 bytes.
The capture's shape and business bounds rely on the existing private storage DTO
contract. Backend buffers, allocator capacity, validation collections and RSS
are not bounded by this view. Borrowing artifact bytes is not zero-allocation
validation or a universal fallible-allocation guarantee.

Catalog metadata and fence bytes remain opaque. No native metadata decoding,
canonical fence encoding or authenticity check is introduced. Valid initial
checkpoint-only and noninitial memberless images remain descriptive data, not
native eligibility.

A store may legally publish newer business rows beside an older catalog. That
capture can return `BusinessMismatch`; this does not make the owner corrupt or
poisoned and does not block a later real publication. Previously captured data
can become stale: agreement over that owned value does not assert that a current
store still agrees.

Matching caller-selected expectations proves agreement only. Expectations
derived from the same untrusted bytes do not establish independent provenance,
authenticity, committed ancestry or canonical selection. The view supplies no
expected-old CAS, write batch, receipt, adoption authority, backend capability
or native reader/writer adapter. No production store I/O, runtime/default
activation, migration tooling, SDK behavior or durable-custody claim is added.

## Verification

The frozen nine-file source package was imported exactly. Formatting normalized
eight files; inverse removal of the three new registration blocks restores
the complete original Update bases. All 54 protected existing test bodies and
21 existing documentation fences remain exact. The only old documentation
location changes are image.rs line 76 to 82 and committed.rs line 160 to 165.

The first focused run passed all 26 regular cases. The seven new documentation
checks also passed: six compile-fail cases and one compile-only no-run example.
Ten further focused runs passed the same 26 cases each, totaling 260 repeated
new-case executions. Both strict workspace Clippy configurations passed, and no
Rust source correction or failed Rust gate was needed for this slice.

All 25 serial verification commands completed successfully, including full
domain, storage, cluster, AMQP engine and protocol suites; default and all-feature
workspace tests and builds; formatting; protobuf generation; and diff checks.
Domain passed 1,404 cases, exactly 33 more than the prior checkpoint. Storage
remained at 239, cluster at 797, engine at 840 and protocol at 514, with their
existing case identities and statuses unchanged.

Both full workspace configurations passed 5,036 cases with ten unchanged
ignored cases, 138 physical result groups and 134 logical harnesses. Exact
per-harness comparisons preserve all 5,013 old rows and their relative order
after only the two approved documentation location changes, adding precisely
26 regular and seven documentation checks. No SDK gate ran for this slice.

Verification used two low-priority CPU cores and one reused target directory.
After the builds, the build volume had 893 GB free and the target remained
112 GB. The README adds only the three-line scope cross-reference; removing it
restores the previous published README exactly.
