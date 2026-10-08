# Contributing to Switchyard

Switchyard is pre-alpha. Keep current design in [ARCHITECTURE.md](ARCHITECTURE.md),
progress/dependencies in [the roadmap](docs/roadmap.md), and behavior in
[compatibility](docs/compatibility.md). Do not add decision-history archives.

## Issue And PR Workflow

- Create or choose an issue before implementation. Define scope, exclusions,
  dependencies, and observable acceptance checks; file newly discovered gaps
  as separate issues instead of expanding active work.
- Assign the issue to yourself before starting. Keep issue comments succinct:
  result, next action, or blocker, not a development transcript. Use first person
  (`I`/`me`) when commenting on someone's behalf.
- Use `feat/<description>` and one cohesive, independently reviewable PR per
  increment. Link its issue and state verification and remaining limitations.
- Plan larger work as dependent small PRs. Merge verified prerequisites first,
  then start the next increment from current `main`; do not merge catch-all
  reference branches or mix unrelated goals.
- Coordinate shared-path ownership before parallel work. Divide tasks by module
  or contract, and do not change another task's inputs during verification.

## Development And Verification

Use the pinned Rust toolchain. Reuse compatible Cargo artifacts, serialize shared
builds, and limit compilation to two jobs. Check free space and target-directory
size before large builds and after substantial runs. Never remove uncertain
user files or active build artifacts to reclaim space.

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -j2 -- -D warnings
cargo test --locked --workspace -j2
cargo build --locked --workspace -j2
```

Run focused tests first and broaden verification with the change's risk.
Protocol/broker changes need relevant client or end-to-end coverage and a
compatibility update; report skipped gates rather than imply they passed.

### Shared Target Handoffs

Opt-in Linux/Python 3.11+ guard: [cargo_handoff.py](tools/cargo_handoff.py).
Initialize only a new, empty external parent with an absent `target` child,
never an existing target/cache. All writers must use the guard and freeze inputs.

```sh
mkdir /mnt/scratch/owned
python3 tools/cargo_handoff.py init --target /mnt/scratch/owned/target
taskset -c 14,15 nice -n 10 python3 tools/cargo_handoff.py run \
  --target /mnt/scratch/owned/target --receipt /mnt/scratch/check-01 \
  -- cargo test --workspace --locked
python3 -m unittest discover -s tools/tests
```

The pinned 1.97.1 helper records worktree/content/configuration/toolchain fingerprints,
disk headroom, package-scoped invalidation, exact commands and original output.
Changed local packages and owned dependents are cleaned; third-party caches and
retained receipts are not. Interrupted/failed runs force conservative invalidation.
Cargo clean ignores path qualifiers: each owned name must have exactly one
source-less version-4 lock entry matching its explicit or workspace-inherited
manifest version. Same-name registry/git/other-version entries refuse cleanup.
Unmapped inputs (including docs), configuration or Cargo command-graph changes
conservatively clean all approved local packages, trading build time for refusal
to guess which package consumes a shared input.
Ownership or observed active-lock uncertainty refuses, not silently adopts.
The sentinel/guard live in that parent; Cargo alone creates the target and cache
tag on the first run. Preexisting targets and foreign parent entries refuse.
An interrupted initial run requires Cargo-tagged cleanup or refuses; the helper
never creates a cache tag or adopts an older sentinel layout.
Cargo lock probes are advisory and released before Cargo; they cannot exclude
non-cooperating writers. Exit zero is not corpus/coverage certification.
External include/build-script inputs and unrelated environment variables must
remain frozen; the helper does not fingerprint them.

Run [the tiny fixture](tools/tests/cargo_handoff_fixture.py) on two CPUs with a new
external directory. It compares exact known test IDs/executables against a fresh
dependency-free reference; only fixture timestamps change. A non-reproduction
does not prove universal Cargo cache correctness. Review source-bound expected
test identities/statuses separately for real workspace gates.

## Durable Compatibility

Changes to stored keys/values, replicated commands, snapshots, or wire encoding
need an explicit compatibility fence: versions, supported readers/writers,
migration or intentional refusal, and rollback behavior. Do not silently reuse
a key tag or format version for incompatible bytes, or stamp a populated
unversioned store as fresh. Include old/new/corrupt fixtures, persistence/reopen,
and rejection-without-mutation checks appropriate to the boundary. Formats may
evolve; the currently implemented version is not a permanent schema promise.

## Engineering Policy

- Keep the state machine deterministic and independent of networking.
- Production acknowledgement must require quorum durability; local apply is
  not a substitute for the planned replicated write path.
- Parse structured formats with structured parsers; avoid `unsafe` in owned crates.
- Keep production runtime dependencies permissively licensed; do not copy
  proprietary implementations or undocumented reference code.
- Never include credentials, customer messages, or private fixtures.

Contributions are licensed under the [MIT License](LICENSE).
