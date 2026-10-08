# Switchyard

Switchyard is a Rust message broker targeting the Azure Service Bus Standard
messaging model and official clients, without an Azure subscription.

> [!IMPORTANT]
> Pre-alpha. The implemented broker is single-node, with memory or Fjall storage
> and an AMQP TCP/TLS or WSS edge. Raft replication, entity administration, and
> production security and recovery are not implemented. Do not use it for
> production workloads; the `production` startup profile is not a readiness claim.

## Current Progress

- [x] Atomic queue send/batch, receive, settlement, expiry, DLQ, deferral, and peek
- [x] Queue sessions, scheduling/cancellation, and duplicate detection
- [x] Immediate/scheduled topics, subscriptions, and actionless correlation rules
- [x] SASL PLAIN/CBS SAS and experimental .NET TCP/WSS loaded-file custody gates
- [x] Fsynced single-node storage and bounded topic routing integrity checks
- [ ] Administration, remaining Service Bus semantics, and broader client gates
- [ ] Quorum durability, tenant isolation, encryption/audit, and recovery

See the [roadmap](docs/roadmap.md) for issue ownership, dependencies, and merge
order. [Compatibility](docs/compatibility.md) records implemented behavior and
known differences. [Architecture](ARCHITECTURE.md) separates the current runtime
from the production design.

## Development

Use the pinned Rust toolchain. Keep builds bounded and reuse a compatible Cargo
target directory; check disk headroom before a full workspace build.

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -j2 -- -D warnings
cargo test --locked --workspace -j2
cargo build --locked --workspace -j2
cargo run --locked -p server -j2 -- --mode development --storage memory --voters 1
```

That starts an empty development node at `127.0.0.1:5672`. Entity creation is
currently a domain command, not a working administration endpoint. For local
persistence, select `--storage fjall --data-dir <directory>`.

The [server arguments](crates/server/src/main.rs) expose TLS certificate/key
files, shared-access policy files, namespace, and transport selection. TLS
defaults to port 5671; `--transport amqp-websockets` requires TLS and defaults to
443. Plaintext is development-only. These checks do not provide replication.

## Workspace

| Crates | Responsibility |
| --- | --- |
| `domain`, `storage` | Deterministic commands and atomic memory/Fjall state |
| `amqp` | Repository-owned framing, sessions, links, SASL, and AMQP types |
| `protocol-amqp`, `server` | Service Bus adaptation, listener/runtime, local proposer, timers |
| `auth` | Implemented SAS policy; future identity/security boundary |
| `cluster` | Configuration invariants; replication remains planned |
| `admin-api`, `switchyardctl` | [Native contract](proto/switchyard/admin/v1/admin.proto) and CLI scaffold |
| `testkit`, `conformance` | Fixtures and client gates |

Follow [CONTRIBUTING.md](CONTRIBUTING.md). Use one issue and one focused feature
PR per independently mergeable change; do not merge reference mega branches.

## License

[MIT](LICENSE). Azure and Azure Service Bus are Microsoft trademarks. Switchyard
is independent and is not affiliated with or endorsed by Microsoft.
