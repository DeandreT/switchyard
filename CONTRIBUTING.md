# Contributing to Switchyard

Switchyard is pre-alpha. Keep the current design in `ARCHITECTURE.md` and the
focused documents under `docs/`; do not add decision-history archives.

## Development

Use the pinned Rust toolchain and run:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace
```

Changes to protocol or broker behavior must include focused tests and update
`docs/compatibility.md`. Changes to durable types update the V1 format and must
include focused persistence and restart coverage.

## Engineering Policy

- Keep the broker state machine deterministic and independent of networking.
- Never acknowledge a production mutation before quorum durability.
- Parse structured formats with structured parsers.
- Avoid `unsafe` in Switchyard-owned crates.
- Keep production runtime dependencies under permissive licenses.
- Do not copy proprietary implementations or undocumented reference code.
- Do not include credentials, captured customer messages, or private fixtures.

By contributing, you agree that your contribution is licensed under the MIT
License.
