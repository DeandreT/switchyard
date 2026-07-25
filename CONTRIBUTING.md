# Contributing to Switchyard

Switchyard is pre-alpha. Design changes should begin with an issue or a short
architecture decision record when they affect wire compatibility, durable
formats, consensus, security boundaries, or public APIs.

## Development

Use the pinned Rust toolchain and run:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace
```

Changes to protocol or broker behavior must include focused tests and update
`docs/compatibility.md`. Changes to durable types require an explicit version
and an upgrade/rollback test.

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
